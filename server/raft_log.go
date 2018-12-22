// Copyright 2018 The NATS Authors
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
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
)

// Bucket names
var (
	logsBucket = []byte("logs")
	confBucket = []byte("conf")
)

// When a key is not found. Raft checks the error text, and it needs to be exactly "not found"
var errKeyNotFound = errors.New("not found")

// raftLog implements both the raft LogStore and Stable interfaces. This is used
// by raft to store logs and configuration changes.
type raftLog struct {
	sync.RWMutex
	log                    logger.Logger
	conn                   *bolt.DB
	fileName               string
	noSync                 bool
	trailingLogs           int
	ratioThreshold         int
	simpleDelThresholdHigh int
	simpleDelThresholdLow  int
	codec                  *codec.MsgpackHandle
	closed                 bool

	// Crypto specific fields
	cipherCode     byte
	gcm            cipher.AEAD
	aesgcm         cipher.AEAD
	chachagcm      cipher.AEAD
	cryptoOverhead int
	encryptBuf     []byte
	nonce          []byte
	nonceSize      int
	nonceUsed      int64
	nonceLimit     int64
}

func newRaftLog(log logger.Logger, fileName string, sync bool, trailingLogs int, encrypt bool, encryptionCipher string, encryptionKey []byte) (*raftLog, error) {
	r := &raftLog{
		log:                    log,
		fileName:               fileName,
		noSync:                 !sync,
		trailingLogs:           trailingLogs,
		ratioThreshold:         50,
		simpleDelThresholdLow:  1000,
		simpleDelThresholdHigh: 100000,
		codec:                  &codec.MsgpackHandle{},
	}
	if encrypt {
		code, ciphers, err := stores.CreateGCMs(encryptionCipher, encryptionKey)
		if err != nil {
			return nil, err
		}
		r.cipherCode = code
		r.aesgcm = ciphers[stores.CryptoCodeAES]
		r.chachagcm = ciphers[stores.CryptoCodeChaCha]
		switch r.cipherCode {
		case stores.CryptoCodeAES:
			r.gcm = r.aesgcm
		case stores.CryptoCodeChaCha:
			r.gcm = r.chachagcm
		}
		// These values are same for the 2 ciphers we support.
		r.cryptoOverhead = r.gcm.Overhead()
		r.nonceSize = r.gcm.NonceSize()
		if err := r.generateNewNonce(); err != nil {
			return nil, err
		}
	}
	conn, err := r.openAndSetOptions(fileName)
	if err != nil {
		return nil, err
	}
	r.conn = conn
	if err := r.init(); err != nil {
		r.conn.Close()
		return nil, err
	}
	return r, nil
}

func (r *raftLog) generateNewNonce() error {
	r.nonce = make([]byte, r.nonceSize)
	if _, err := io.ReadFull(rand.Reader, r.nonce); err != nil {
		return err
	}
	r.nonceUsed = 0
	r.nonceLimit = mrand.Int63n(1e6) + 100000
	return nil
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
	return tx.Commit()
}

func (r *raftLog) openAndSetOptions(fileName string) (*bolt.DB, error) {
	db, err := bolt.Open(fileName, 0600, nil)
	if err != nil {
		return nil, err
	}
	db.NoSync = r.noSync
	return db, nil
}

func (r *raftLog) encrypt(data []byte) ([]byte, error) {
	// Here we can reuse a buffer to encrypt because we know
	// that the underlying is going to make a copy of the
	// slice.
	r.encryptBuf = util.EnsureBufBigEnough(r.encryptBuf, 1+r.nonceSize+r.cryptoOverhead+len(data))
	r.encryptBuf[0] = r.cipherCode
	copy(r.encryptBuf[1:], r.nonce)
	copy(r.encryptBuf[1+r.nonceSize:], data)
	dst := r.encryptBuf[1+r.nonceSize : 1+r.nonceSize+len(data)]
	ret := r.gcm.Seal(dst[:0], r.nonce, dst, nil)
	r.nonceUsed++
	if r.nonceUsed >= r.nonceLimit {
		if err := r.generateNewNonce(); err != nil {
			return nil, err
		}
	}
	return r.encryptBuf[:1+r.nonceSize+len(ret)], nil
}

func (r *raftLog) encodeRaftLog(in *raft.Log) ([]byte, error) {
	var orgData []byte
	if r.gcm != nil && len(in.Data) > 0 && in.Type == raft.LogCommand {
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
	if err == nil && len(log.Data) > 0 && r.gcm != nil && log.Type == raft.LogCommand {
		var gcm cipher.AEAD
		if len(log.Data) > 0 {
			switch log.Data[0] {
			case stores.CryptoCodeAES:
				gcm = r.aesgcm
			case stores.CryptoCodeChaCha:
				gcm = r.chachagcm
			default:
				// Anything else, assume no algo or something we don't know how to decrypt.
				return nil
			}
		}
		if len(log.Data) <= 1+r.nonceSize {
			return fmt.Errorf("trying to decrypt data that is not (len=%v)", len(log.Data))
		}
		// My understanding is that log.Data is empty at beginning of this
		// function and dec.Decode(log) will make a copy from buffer that
		// comes from boltdb. So we can decrypt in place since we "own" log.Data.
		cipherText := log.Data[1+r.nonceSize:]
		dd, err := gcm.Open(cipherText[:0], log.Data[1:1+r.nonceSize], cipherText, nil)
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
	if r.gcm != nil {
		r.Lock()
	} else {
		r.RLock()
	}
	tx, err := r.conn.Begin(true)
	if err != nil {
		if r.gcm != nil {
			r.Unlock()
		} else {
			r.RUnlock()
		}
		return err
	}
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
		bucket := tx.Bucket(logsBucket)
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
	if r.gcm != nil {
		r.Unlock()
	} else {
		r.RUnlock()
	}
	return err
}

// DeleteRange implements the LogStore interface
func (r *raftLog) DeleteRange(min, max uint64) (retErr error) {
	r.Lock()
	defer r.Unlock()

	start := time.Now()
	r.log.Noticef("Deleting raft logs from %v to %v", min, max)
	defer func() {
		dur := time.Since(start)
		durTxt := fmt.Sprintf("Deletion took %v", dur)
		if dur > 2*time.Second {
			r.log.Errorf(fmt.Sprintf("%s. This is too long, consider lowering TrailingLogs value currently set to %v",
				durTxt, r.trailingLogs))
		} else {
			r.log.Noticef(durTxt)
		}
	}()

	// We know that RAFT is calling DeleteRange leaving at least
	// trailingLogs number of logs at the end.

	// If the selected number of trailingLogs (value set when RAFT is created)
	// is too big, perform a simple delete range that removes logs from the DB.
	if r.trailingLogs > r.simpleDelThresholdHigh {
		return r.simpleDeleteRange(min, max)
	}
	// If the number of logs to delete is small, remove in place.
	toRemove := int(max-min) + 1
	if toRemove <= r.simpleDelThresholdLow {
		return r.simpleDeleteRange(min, max)
	}
	r.log.Noticef("Compaction in progress...")

	newfileName := r.fileName + ".new"
	newdb, err := r.openAndSetOptions(newfileName)
	if err != nil {
		return err
	}
	removeNewFile := true
	defer func() {
		if removeNewFile {
			newdb.Close()
			os.Remove(newfileName)
		}
	}()

	curDBConn := r.conn
	newDBConn := newdb

	// First, transfer all confLogs, there should not be that many
	if err := r.transferLogs(curDBConn, newDBConn, confBucket, 1); err != nil {
		return err
	}

	if err := r.transferLogs(curDBConn, newDBConn, logsBucket, max+1); err != nil {
		return err
	}

	// Close new db
	if err := newdb.Close(); err != nil {
		return err
	}
	// Close current db
	if err := curDBConn.Close(); err != nil {
		// We got an error, try to reopen
		db, cerr := r.openAndSetOptions(r.fileName)
		if cerr != nil {
			// At this point, panic...
			panic(fmt.Errorf("error closing bolt db after compaction: %v - error re-opening: %v", err, cerr))
		}
		r.conn = db
		return err
	}
	// Rename new db file to the name of old one
	os.Rename(newfileName, r.fileName)
	// Reopen the compacted db file
	db, err := r.openAndSetOptions(r.fileName)
	if err != nil {
		// At this point, panic...
		panic(fmt.Errorf("error compacting bolt db: %v", err))
	}
	// We now point to the compact db file
	r.conn = db
	// Success, skip cleanup code
	removeNewFile = false
	return nil
}

// Delete logs from the "logs" bucket starting at the min index
// and up to max index (included).
// Lock is held on entry
func (r *raftLog) simpleDeleteRange(min, max uint64) error {
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

func (r *raftLog) transferLogs(curDB, newDB *bolt.DB, bucketName []byte, startKey uint64) error {
	readTX, err := curDB.Begin(false)
	if err != nil {
		return err
	}
	// Read transactions must be rollback (not committed)
	defer readTX.Rollback()

	var (
		key         [8]byte
		count       int
		limit       = 1000
		writeTX     *bolt.Tx
		writeBucket *bolt.Bucket
	)
	binary.BigEndian.PutUint64(key[:], startKey)

	curs := readTX.Bucket(bucketName).Cursor()

	for k, v := curs.Seek(key[:]); k != nil; k, v = curs.Next() {
		if count == 0 {
			writeTX, err = newDB.Begin(true)
			if err != nil {
				return err
			}
			writeBucket = writeTX.Bucket(bucketName)
			if writeBucket == nil {
				b, err := writeTX.CreateBucket(bucketName)
				if err != nil {
					writeTX.Rollback()
					return err
				}
				writeBucket = b
			}
		}
		if err := writeBucket.Put(k, v); err != nil {
			writeTX.Rollback()
			return err
		}
		count++
		if count == limit {
			count = 0
			if err := writeTX.Commit(); err != nil {
				return err
			}
			writeTX = nil
		}
	}
	if writeTX != nil {
		return writeTX.Commit()
	}
	return nil
}

// Set implements the Stable interface
func (r *raftLog) Set(k, v []byte) error {
	r.RLock()
	tx, err := r.conn.Begin(true)
	if err != nil {
		r.RUnlock()
		return err
	}
	bucket := tx.Bucket(confBucket)
	err = bucket.Put(k, v)
	if err != nil {
		tx.Rollback()
	} else {
		err = tx.Commit()
	}
	r.RUnlock()
	return err
}

// Get implements the Stable interface
func (r *raftLog) Get(k []byte) ([]byte, error) {
	r.RLock()
	tx, err := r.conn.Begin(false)
	if err != nil {
		r.RUnlock()
		return nil, err
	}
	var v []byte
	bucket := tx.Bucket(confBucket)
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
	var vbytes [8]byte
	binary.BigEndian.PutUint64(vbytes[:], v)
	err := r.Set(k, vbytes[:])
	return err
}

// GetUint64 implements the Stable interface
func (r *raftLog) GetUint64(k []byte) (uint64, error) {
	var v uint64
	vbytes, err := r.Get(k)
	if err == nil {
		v = binary.BigEndian.Uint64(vbytes)
	}
	return v, err
}
