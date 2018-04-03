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
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
)

func createTestRaftLog(t tLogger, sync bool, trailingLogs int) *raftLog {
	if err := os.MkdirAll(defaultRaftLog, os.ModeDir+os.ModePerm); err != nil {
		stackFatalf(t, "Unable to create raft log directory: %v", err)
	}
	fileName := filepath.Join(defaultRaftLog, raftLogFile)
	store, err := newRaftLog(testLogger, fileName, sync, trailingLogs)
	if err != nil {
		stackFatalf(t, "Error creating store: %v", err)
	}
	return store
}

func TestRaftLogDeleteRange(t *testing.T) {
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// No sync (will check that conn's NoSync value is correct after a recreating the file)
	store := createTestRaftLog(t, false, 0)
	defer store.Close()

	// Save reference to bolt connection
	store.RLock()
	orgConn := store.conn
	store.RUnlock()

	// Store in dbConf bucket
	k1 := []byte("1")
	if err := store.SetUint64(k1, 1); err != nil {
		t.Fatalf("Error on set: %v", err)
	}
	k2 := []byte("2")
	if err := store.SetUint64(k2, 2); err != nil {
		t.Fatalf("Error on set: %v", err)
	}

	// Store in dbLogs bucket
	var logs []*raft.Log
	for i := 0; i < 10; i++ {
		log := &raft.Log{
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte(fmt.Sprintf("log%d", (i + 1))),
		}
		logs = append(logs, log)
		if err := store.StoreLog(log); err != nil {
			t.Fatalf("Error on store log: %v", err)
		}
	}

	// Call DeleteRange 1..5 and make sure we have all the conf entries
	// and the 5 last logs
	if err := store.DeleteRange(1, 5); err != nil {
		t.Fatalf("Error on delete range: %v", err)
	}

	if v, err := store.GetUint64(k1); v != 1 || err != nil {
		t.Fatalf("Error on get of key k1: %v - %v", v, err)
	}
	if v, err := store.GetUint64(k2); v != 2 || err != nil {
		t.Fatalf("Error on get of key k2: %v - %v", v, err)
	}
	// Logs 1 to 5 should not exist
	for i := 1; i <= 5; i++ {
		var log *raft.Log
		if err := store.GetLog(uint64(i), log); err == nil {
			t.Fatalf("Expected log %v to not exist", (i + 1))
		}
	}
	// Logs 6 to 10 should exist
	if first, err := store.FirstIndex(); first != 6 || err != nil {
		t.Fatalf("Unexpected first index: %v - %v", first, err)
	}
	if last, err := store.LastIndex(); last != 10 || err != nil {
		t.Fatalf("Unexpected last index: %v - %v", last, err)
	}
	for i := 6; i <= 10; i++ {
		log := &raft.Log{}
		if err := store.GetLog(uint64(i), log); err != nil {
			t.Fatalf("Error on GetLog for index %v: %v", i, err)
		}
		if !reflect.DeepEqual(log, logs[i-1]) {
			t.Fatalf("Unexpected log at index %v: %v", i, log)
		}
	}

	// Now change the limit to cause re-creation of the bolt DB store.
	store.Lock()
	store.simpleDelThresholdLow = 0
	store.Unlock()

	// Delete just one element
	if err := store.DeleteRange(6, 6); err != nil {
		t.Fatalf("Error on delete: %v", err)
	}
	// Keys should still be present
	if v, err := store.GetUint64(k1); v != 1 || err != nil {
		t.Fatalf("Error on get of key k1: %v - %v", v, err)
	}
	if v, err := store.GetUint64(k2); v != 2 || err != nil {
		t.Fatalf("Error on get of key k2: %v - %v", v, err)
	}
	// Logs 7 to 10 should exist
	if first, err := store.FirstIndex(); first != 7 || err != nil {
		t.Fatalf("Unexpected first index: %v - %v", first, err)
	}
	if last, err := store.LastIndex(); last != 10 || err != nil {
		t.Fatalf("Unexpected last index: %v - %v", last, err)
	}
	for i := 7; i <= 10; i++ {
		log := &raft.Log{}
		if err := store.GetLog(uint64(i), log); err != nil {
			t.Fatalf("Error on GetLog: %v", err)
		}
		if !reflect.DeepEqual(log, logs[i-1]) {
			t.Fatalf("Unexpected log at index %v: %v", i, log)
		}
	}
	// The store should have been replaced
	store.RLock()
	newConn := store.conn
	noSyncValue := store.conn.NoSync
	store.RUnlock()
	if newConn == orgConn {
		t.Fatalf("Looks like store was not recreated")
	}
	if !noSyncValue {
		t.Fatalf("bolt conn NoSync should be true")
	}

	// Check close:
	if err := store.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
}

func TestRaftLogEncodeDecodeLogs(t *testing.T) {
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	store := createTestRaftLog(t, false, 0)
	defer store.Close()

	total := 50000
	logs := make([]*raft.Log, 0, total)
	for i := 0; i < total; i++ {
		log := &raft.Log{
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte(fmt.Sprintf("%d", (i + 1))),
		}
		logs = append(logs, log)
	}
	encoded := make([][]byte, 0, total)
	for _, l := range logs {
		b, err := store.encodeRaftLog(l)
		if err != nil {
			t.Fatalf("Error encoding: %v", err)
		}
		encoded = append(encoded, b)
	}
	decoded := make([]*raft.Log, 0, total)
	for _, e := range encoded {
		var l raft.Log
		if err := store.decodeRaftLog(e, &l); err != nil {
			t.Fatalf("Error decoding: %v", err)
		}
		decoded = append(decoded, &l)
	}
	for i, d := range decoded {
		if !reflect.DeepEqual(d, logs[i]) {
			t.Fatalf("Wrong decoding at index %v. Expected %v, got %v", i+1, logs[i], d)
		}
	}

	// Perform concurrent encoding...
	wg := sync.WaitGroup{}
	encode := func(wg *sync.WaitGroup, min, max int) {
		defer wg.Done()
		for i := min; i < max; i++ {
			l := logs[i]
			if _, err := store.encodeRaftLog(l); err != nil {
				t.Fatalf("Error during encoding")
			}
		}
	}
	wg.Add(4)
	go encode(&wg, 0, total/4)
	go encode(&wg, total/4, 2*(total/4))
	go encode(&wg, 2*(total/4), 3*(total/4))
	go encode(&wg, 3*(total/4), total)
	wg.Wait()

	// Concurrent decoding...
	decode := func(wg *sync.WaitGroup, min, max int) {
		defer wg.Done()
		for i := min; i < max; i++ {
			e := encoded[i]
			var l raft.Log
			if err := store.decodeRaftLog(e, &l); err != nil {
				t.Fatalf("Error during encoding")
			}
		}
	}
	wg.Add(4)
	go decode(&wg, 0, total/4)
	go decode(&wg, total/4, 2*(total/4))
	go decode(&wg, 2*(total/4), 3*(total/4))
	go decode(&wg, 3*(total/4), total)
	wg.Wait()
}
