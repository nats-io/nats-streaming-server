// Copyright 2018-2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/stores"
	bolt "go.etcd.io/bbolt"
)

// Bucket names
var (
	logsBucket = []byte("logs")
	confBucket = []byte("conf")
	chanBucket = []byte("channels")
)

// When a key is not found. Raft checks the error text, and it needs to be exactly "not found"
var errKeyNotFound = errors.New("not found")

// raftLog implements both the raft LogStore and Stable interfaces. This is used
// by raft to store logs and configuration changes.
type raftLog struct {
	sync.RWMutex
	log      logger.Logger
	conn     *bolt.DB
	fileName string
	codec    *codec.MsgpackHandle
	closed   bool

	// If the store is using encryption
	encryption    bool
	eds           *stores.EDStore
	encryptBuf    []byte
	encryptOffset int
}

func newRaftLog(log logger.Logger, fileName string, sync bool, _ int, encrypt bool, encryptionCipher string, encryptionKey []byte) (*raftLog, error) {
	r := &raftLog{
		log:      log,
		fileName: fileName,
		codec:    &codec.MsgpackHandle{},
	}
	db, err := bolt.Open(fileName, 0600, nil)
	if err != nil {
		return nil, err
	}
	db.NoSync = !sync
	db.NoFreelistSync = true
	db.FreelistType = bolt.FreelistMapType
	r.conn = db
	if err := r.init(); err != nil {
		r.conn.Close()
		return nil, err
	}
	if encrypt {
		lastIndex, err := r.getIndex(false)
		if err != nil {
			r.conn.Close()
			return nil, err
		}
		eds, err := stores.NewEDStore(encryptionCipher, encryptionKey, lastIndex)
		if err != nil {
			r.conn.Close()
			return nil, err
		}
		r.eds = eds
		r.encryption = true
		r.encryptBuf = make([]byte, 100)
		r.encryptOffset = eds.EncryptionOffset()
	}
	return r, nil
}

func (r *raftLog) init() error {
	tx, err := r.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create the configuration and logs buckets
	if _, err := tx.CreateBucketIfNotExists(confBucket); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(logsBucket); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(chanBucket); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *raftLog) encrypt(data []byte) ([]byte, error) {
	// Here we can reuse a buffer to encrypt because we know
	// that the underlying is going to make a copy of the
	// slice.
	return r.eds.Encrypt(&r.encryptBuf, data)
}

func (r *raftLog) encodeRaftLog(in *raft.Log) ([]byte, error) {
	var orgData []byte
	if r.encryption && len(in.Data) > 0 && in.Type == raft.LogCommand {
		orgData = in.Data
		ed, err := r.encrypt(in.Data)
		if err != nil {
			return nil, err
		}
		in.Data = ed
	}
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, r.codec)
	err := enc.Encode(in)
	if orgData != nil {
		in.Data = orgData
	}
	return buf, err
}

func (r *raftLog) decodeRaftLog(buf []byte, log *raft.Log) error {
	dec := codec.NewDecoderBytes(buf, r.codec)
	err := dec.Decode(log)
	if r.encryption && err == nil && len(log.Data) > 0 && log.Type == raft.LogCommand {
		// My understanding is that log.Data is empty at beginning of this
		// function and dec.Decode(log) will make a copy from buffer that
		// comes from boltdb. So we can decrypt in place since we "own" log.Data.
		var dst []byte
		if len(log.Data) > r.encryptOffset {
			dst = log.Data[r.encryptOffset:]
		}
		dd, err := r.eds.Decrypt(dst[:0], log.Data)
		if err != nil {
			return err
		}
		log.Data = dd
	}
	return err
}

// Close implements the LogStore interface
func (r *raftLog) Close() error {
	r.Lock()
	if r.closed {
		r.Unlock()
		return nil
	}
	r.closed = true
	err := r.conn.Close()
	r.Unlock()
	return err
}

// FirstIndex implements the LogStore interface
func (r *raftLog) FirstIndex() (uint64, error) {
	r.RLock()
	idx, err := r.getIndex(true)
	r.RUnlock()
	return idx, err
}

// LastIndex implements the LogStore interface
func (r *raftLog) LastIndex() (uint64, error) {
	r.RLock()
	idx, err := r.getIndex(false)
	r.RUnlock()
	return idx, err
}

// returns either the first (if first is true) or the last
// index of the logs bucket.
func (r *raftLog) getIndex(first bool) (uint64, error) {
	tx, err := r.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	var (
		key []byte
		idx uint64
	)
	curs := tx.Bucket(logsBucket).Cursor()
	if first {
		key, _ = curs.First()
	} else {
		key, _ = curs.Last()
	}
	if key != nil {
		idx = binary.BigEndian.Uint64(key)
	}
	tx.Rollback()
	return idx, nil
}

// GetLog implements the LogStore interface
func (r *raftLog) GetLog(idx uint64, log *raft.Log) error {
	r.RLock()
	tx, err := r.conn.Begin(false)
	if err != nil {
		r.RUnlock()
		return err
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], idx)
	bucket := tx.Bucket(logsBucket)
	val := bucket.Get(key[:])
	if val == nil {
		err = raft.ErrLogNotFound
	} else {
		err = r.decodeRaftLog(val, log)
	}
	tx.Rollback()
	r.RUnlock()
	return err
}

// StoreLog implements the LogStore interface
func (r *raftLog) StoreLog(log *raft.Log) error {
	return r.StoreLogs([]*raft.Log{log})
}

// StoreLogs implements the LogStore interface
func (r *raftLog) StoreLogs(logs []*raft.Log) error {
	r.Lock()
	tx, err := r.conn.Begin(true)
	if err != nil {
		r.Unlock()
		return err
	}
	bucket := tx.Bucket(logsBucket)
	for _, log := range logs {
		var (
			key [8]byte
			val []byte
		)
		binary.BigEndian.PutUint64(key[:], log.Index)
		val, err = r.encodeRaftLog(log)
		if err != nil {
			break
		}
		err = bucket.Put(key[:], val)
		if err != nil {
			break
		}
	}
	if err != nil {
		tx.Rollback()
	} else {
		err = tx.Commit()
	}
	r.Unlock()
	return err
}

// DeleteRange implements the LogStore interface
func (r *raftLog) DeleteRange(min, max uint64) (retErr error) {
	r.Lock()
	defer r.Unlock()

	start := time.Now()
	r.log.Noticef("Deleting raft logs from %v to %v", min, max)
	err := r.deleteRange(min, max)
	dur := time.Since(start)
	durTxt := fmt.Sprintf("Deletion took %v", dur)
	if dur > 2*time.Second {
		r.log.Errorf(durTxt)
	} else {
		r.log.Noticef(durTxt)
	}
	return err
}

// Delete logs from the "logs" bucket starting at the min index
// and up to max index (included).
// Lock is held on entry
func (r *raftLog) deleteRange(min, max uint64) error {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], min)
	tx, err := r.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	curs := tx.Bucket(logsBucket).Cursor()
	for k, _ := curs.Seek(key[:]); k != nil; k, _ = curs.Next() {
		// If we reach the max, we are done
		if binary.BigEndian.Uint64(k) > max {
			break
		}
		if err := curs.Delete(); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Set implements the Stable interface
func (r *raftLog) Set(k, v []byte) error {
	return r.set(confBucket, k, v)
}

func (r *raftLog) set(bucketName, k, v []byte) error {
	r.Lock()
	tx, err := r.conn.Begin(true)
	if err != nil {
		r.Unlock()
		return err
	}
	bucket := tx.Bucket(bucketName)
	err = bucket.Put(k, v)
	if err != nil {
		tx.Rollback()
	} else {
		err = tx.Commit()
	}
	r.Unlock()
	return err
}

// Get implements the Stable interface
func (r *raftLog) Get(k []byte) ([]byte, error) {
	return r.get(confBucket, k)
}

func (r *raftLog) get(bucketName, k []byte) ([]byte, error) {
	r.RLock()
	tx, err := r.conn.Begin(false)
	if err != nil {
		r.RUnlock()
		return nil, err
	}
	var v []byte
	bucket := tx.Bucket(bucketName)
	val := bucket.Get(k)
	if val == nil {
		err = errKeyNotFound
	} else {
		// Make a copy
		v = append([]byte(nil), val...)
	}
	tx.Rollback()
	r.RUnlock()
	return v, err
}

// SetUint64 implements the Stable interface
func (r *raftLog) SetUint64(k []byte, v uint64) error {
	return r.setUint64(confBucket, k, v)
}

func (r *raftLog) setUint64(bucket, k []byte, v uint64) error {
	var vbytes [8]byte
	binary.BigEndian.PutUint64(vbytes[:], v)
	err := r.set(bucket, k, vbytes[:])
	return err
}

// GetUint64 implements the Stable interface
func (r *raftLog) GetUint64(k []byte) (uint64, error) {
	return r.getUint64(confBucket, k)
}

func (r *raftLog) getUint64(bucket, k []byte) (uint64, error) {
	var v uint64
	vbytes, err := r.get(bucket, k)
	if err == nil {
		v = binary.BigEndian.Uint64(vbytes)
	}
	return v, err
}

func (r *raftLog) SetChannelID(name string, id uint64) error {
	return r.setUint64(chanBucket, []byte(name), id)
}

func (r *raftLog) DeleteChannelID(name string) error {
	r.Lock()
	tx, err := r.conn.Begin(true)
	if err != nil {
		r.Unlock()
		return err
	}
	bucket := tx.Bucket(chanBucket)
	err = bucket.Delete([]byte(name))
	if err != nil {
		tx.Rollback()
	} else {
		err = tx.Commit()
	}
	r.Unlock()
	return err
}

func (r *raftLog) GetChannelID(name string) (uint64, error) {
	id, err := r.getUint64(chanBucket, []byte(name))
	if err == errKeyNotFound {
		err = nil
		id = 0
	}
	return id, err
}
