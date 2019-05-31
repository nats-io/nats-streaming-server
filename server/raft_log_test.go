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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats-streaming-server/stores"
)

func createTestRaftLog(t tLogger, sync bool, trailingLogs int) *raftLog {
	if err := os.MkdirAll(defaultRaftLog, os.ModeDir+os.ModePerm); err != nil {
		stackFatalf(t, "Unable to create raft log directory: %v", err)
	}
	fileName := filepath.Join(defaultRaftLog, raftLogFile)
	store, err := newRaftLog(testLogger, fileName, sync, trailingLogs, false, stores.CryptoCipherAutoSelect, nil)
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

	errCh := make(chan error, 1)
	// Perform concurrent encoding...
	wg := sync.WaitGroup{}
	encode := func(wg *sync.WaitGroup, min, max int) {
		defer wg.Done()
		for i := min; i < max; i++ {
			l := logs[i]
			if _, err := store.encodeRaftLog(l); err != nil {
				select {
				case errCh <- fmt.Errorf("Error during encoding: %v", err):
					return
				default:
				}
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
				select {
				case errCh <- fmt.Errorf("Error during decoding: %v", err):
					return
				default:
				}
			}
		}
	}
	wg.Add(4)
	go decode(&wg, 0, total/4)
	go decode(&wg, total/4, 2*(total/4))
	go decode(&wg, 2*(total/4), 3*(total/4))
	go decode(&wg, 3*(total/4), total)
	wg.Wait()

	select {
	case e := <-errCh:
		t.Fatal(e.Error())
	default:
	}
}

func TestRaftLogWithEncryption(t *testing.T) {
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	store := createTestRaftLog(t, false, 0)
	defer store.Close()
	// Plain text log
	store.StoreLog(&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("abcd")})
	// Log with marker that says AES but content is not encrypted
	store.StoreLog(&raft.Log{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte{stores.CryptoCodeAES, 'a', 'b', 'c', 'd'}})
	store.RLock()
	fileName := store.fileName
	store.RUnlock()
	store.Close()

	// Re-open as encrypted store
	store, err := newRaftLog(testLogger, fileName, false, 0, true, stores.CryptoCipherAES, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error opening store: %v", err)
	}
	defer store.Close()
	// Attempt to get the log, it should be ok
	rl := &raft.Log{}
	if err := store.GetLog(uint64(1), rl); err != nil {
		t.Fatalf("Error getting log: %v", err)
	}
	if string(rl.Data) != "abcd" {
		t.Fatalf("Expected %q, got %q", "abcd", rl.Data)
	}
	// Attempt to get the log, it should fail
	rl = &raft.Log{}
	if err := store.GetLog(uint64(2), rl); err == nil || !strings.Contains(err.Error(), "trying") {
		t.Fatalf("Expected error about trying to decrypt data that is not, got %v", err)
	}
	store.Close()
	cleanupRaftLog(t)

	if err := os.MkdirAll(defaultRaftLog, os.ModeDir+os.ModePerm); err != nil {
		t.Fatalf("Unable to create raft log directory: %v", err)
	}
	fileName = filepath.Join(defaultRaftLog, raftLogFile)

	key := []byte("testkey")
	store, err = newRaftLog(testLogger, fileName, false, 0, true, stores.CryptoCipherAES, key)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()
	if string(key) == "testkey" {
		t.Fatalf("Key should have been erased")
	}

	expected := []*raft.Log{
		&raft.Log{
			Type:  raft.LogCommand,
			Index: 1,
			Term:  1,
			Data:  []byte("msg1"),
		},
		&raft.Log{
			Type:  raft.LogCommand,
			Index: 2,
			Term:  1,
			Data:  []byte("msg2"),
		},
		&raft.Log{
			Type:  raft.LogCommand,
			Index: 3,
			Term:  1,
			Data:  []byte("msg3"),
		},
	}
	if err := store.StoreLogs(expected); err != nil {
		t.Fatalf("Error storing logs")
	}
	for i := 0; i < len(expected); i++ {
		log := &raft.Log{}
		if err := store.GetLog(uint64(i+1), log); err != nil {
			t.Fatalf("Error getting log: %v", err)
		}
		if !reflect.DeepEqual(log, expected[i]) {
			t.Fatalf("Expected %v, got %v", expected[i], log)
		}
	}
	store.Close()

	// Re-open with using env variable
	os.Unsetenv(stores.CryptoStoreEnvKeyName)
	defer os.Unsetenv(stores.CryptoStoreEnvKeyName)

	if err := os.Setenv(stores.CryptoStoreEnvKeyName, "testkey"); err != nil {
		t.Fatalf("Unable to set environment variable: %v", err)
	}
	store, err = newRaftLog(testLogger, fileName, false, 0, true, stores.CryptoCipherAES, nil)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()
	log := &raft.Log{}
	if err := store.GetLog(uint64(1), log); err != nil {
		t.Fatalf("Error getting log: %v", err)
	}
	if string(log.Data) != "msg1" {
		t.Fatalf("Expected %q, got %q", "msg1", log.Data)
	}
	store.Close()

	// Ensure that env key override config by providing a wrong key
	// and notice that we have correct decrypt.
	store, err = newRaftLog(testLogger, fileName, false, 0, true, stores.CryptoCipherAES, []byte("wrongkey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()
	log = &raft.Log{}
	if err := store.GetLog(uint64(1), log); err != nil {
		t.Fatalf("Error getting log: %v", err)
	}
	if string(log.Data) != "msg1" {
		t.Fatalf("Expected %q, got %q", "msg1", log.Data)
	}
	store.Close()

	// Now unset env variable and re-open with wrong key
	os.Unsetenv(stores.CryptoStoreEnvKeyName)
	store, err = newRaftLog(testLogger, fileName, false, 0, true, stores.CryptoCipherAES, []byte("wrongkey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()
	log = &raft.Log{}
	if err := store.GetLog(uint64(1), log); err == nil || !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("Expected error about auth failure, got %v", err)
	}
	store.Close()

	// Re-open with encryption but no key, this should fail.
	store, err = newRaftLog(testLogger, fileName, false, 0, true, stores.CryptoCipherAES, nil)
	if err == nil || !strings.Contains(err.Error(), stores.ErrCryptoStoreRequiresKey.Error()) {
		if store != nil {
			store.Close()
		}
		t.Fatalf("Expected error about missing key, got %v", err)
	}
}

func TestRaftLogMultipleCiphers(t *testing.T) {
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	if err := os.MkdirAll(defaultRaftLog, os.ModeDir+os.ModePerm); err != nil {
		t.Fatalf("Unable to create raft log directory: %v", err)
	}
	fileName := filepath.Join(defaultRaftLog, raftLogFile)

	store, err := newRaftLog(testLogger, fileName, false, 0, false, stores.CryptoCipherAutoSelect, nil)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()

	payloads := [][]byte{
		[]byte("this is a plain text message"),
		[]byte("this is a message encrypted with AES cipher"),
		[]byte("this is a message encrypted with CHACHA cipher"),
	}

	if err := store.StoreLog(&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: payloads[0]}); err != nil {
		t.Fatalf("Error storing log: %v", err)
	}
	store.Close()

	storeWithEncryption := func(t *testing.T, encryptionCipher string, payloadIdx int) {
		t.Helper()
		store, err := newRaftLog(testLogger, fileName, false, 0, true, encryptionCipher, []byte("mykey"))
		if err != nil {
			t.Fatalf("Error creating store: %v", err)
		}
		defer store.Close()

		if err := store.StoreLog(&raft.Log{Index: uint64(payloadIdx + 1), Term: 1, Type: raft.LogCommand, Data: payloads[payloadIdx]}); err != nil {
			t.Fatalf("Error storing log: %v", err)
		}
		store.Close()
	}
	storeWithEncryption(t, stores.CryptoCipherAES, 1)
	storeWithEncryption(t, stores.CryptoCipherChaChaPoly, 2)

	// Now re-open with any cipher, use the auto-select one.
	// We should be able to get all 3 messages correctly.
	store, err = newRaftLog(testLogger, fileName, false, 0, true, stores.CryptoCipherAutoSelect, []byte("mykey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()
	for i := 0; i < 3; i++ {
		l := &raft.Log{}
		if err := store.GetLog(uint64(i+1), l); err != nil {
			t.Fatalf("Error getting log: %v", err)
		}
		if !bytes.Equal(l.Data, payloads[i]) {
			t.Fatalf("Expected message %q, got %q", payloads[i], l.Data)
		}
	}
}
