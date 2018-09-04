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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats-streaming-server/logger"
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
}

func newRaftLog(log logger.Logger, fileName string, sync bool, trailingLogs int) (*raftLog, error) {
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

func (r *raftLog) encodeRaftLog(in *raft.Log) ([]byte, error) {
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, r.codec)
	err := enc.Encode(in)
	return buf, err
}

func (r *raftLog) decodeRaftLog(buf []byte, log *raft.Log) error {
	dec := codec.NewDecoderBytes(buf, r.codec)
	return dec.Decode(log)
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
	r.RLock()
	tx, err := r.conn.Begin(true)
	if err != nil {
		r.RUnlock()
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
	r.RUnlock()
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
