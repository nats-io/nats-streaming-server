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

package stores

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/go-nats-streaming/pb"
)

func TestCryptoStoreKeyIsCleared(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	orgStr := "thisisthekey"
	key := []byte(orgStr)
	cs, err := NewCryptoStore(s, key)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer cs.Close()
	if string(key) == orgStr {
		t.Fatalf("The key was not cleared")
	}
}

func TestCryptoStore(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	cs, err := NewCryptoStore(s, nil)
	if cs != nil || err != ErrCryptoStoreRequiresKey {
		t.Fatalf("Expected no store and error %q, got %v - %v", ErrCryptoStoreRequiresKey.Error(), cs, err)
	}
	goodKey := []byte("testkey")
	cs, err = NewCryptoStore(s, goodKey)
	if err != nil {
		t.Fatalf("Unable to create crypto store: %v", err)
	}
	defer cs.Close()

	c := storeCreateChannel(t, cs, "foo")
	for i := 0; i < 10; i++ {
		storeMsg(t, c, "foo", uint64(i+1), []byte(fmt.Sprintf("msg%d", i)))
	}
	cs.Close()

	// Reopen the file and do Recover() without crypto. the
	// content of messages should be encrypted.
	s, rs := openDefaultFileStore(t)
	rc := getRecoveredChannel(t, rs, "foo")
	for i := 0; i < 10; i++ {
		msg, err := rc.Msgs.Lookup(uint64(i + 1))
		if err != nil {
			t.Fatalf("Error looking up message: %v", err)
		}
		if reflect.DeepEqual(msg.Data, []byte(fmt.Sprintf("msg%d", i))) {
			t.Fatalf("Unexpected message: %v", string(msg.Data))
		}
	}
	s.Close()

	// Now create the file store and wrap with CryptoStore.
	// First use wrong key and lookup should fail, then
	// correct key and all should be good.
	keys := [][]byte{[]byte("wrongkey"), []byte("testkey")}
	for ki, k := range keys {
		s, err = NewFileStore(testLogger, testFSDefaultDatastore, nil)
		if err != nil {
			t.Fatalf("Error opening store: %v", err)
		}
		defer s.Close()
		cs, err = NewCryptoStore(s, k)
		if err != nil {
			t.Fatalf("Error creating crypto store: %v", err)
		}
		rs, err = cs.Recover()
		if err != nil {
			t.Fatalf("Error recovering state: %v", err)
		}
		rc = getRecoveredChannel(t, rs, "foo")
		for i := 0; i < 10; i++ {
			msg, err := rc.Msgs.Lookup(uint64(i + 1))
			if ki == 0 {
				if msg != nil || err == nil {
					t.Fatalf("Expected failure to lookup message, got m=%v err=%v", msg, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Error looking up message: %v", err)
				}
				if !reflect.DeepEqual(msg.Data, []byte(fmt.Sprintf("msg%d", i))) {
					t.Fatalf("Unexpected message: %v", string(msg.Data))
				}
			}
		}
		if ki == 1 {
			fm := msgStoreFirstMsg(t, rc.Msgs)
			if !reflect.DeepEqual(fm.Data, []byte("msg0")) {
				t.Fatalf("Unexpected message: %v", string(fm.Data))
			}
			lm := msgStoreLastMsg(t, rc.Msgs)
			if !reflect.DeepEqual(lm.Data, []byte("msg9")) {
				t.Fatalf("Unexpected message: %v", string(fm.Data))
			}
		}
		cs.Close()
	}

	s.Close()
	// Cleanup and create a file store
	cleanupFSDatastore(t)
	s = createDefaultFileStore(t)
	defer s.Close()
	// Add some messages
	c = storeCreateChannel(t, s, "foo")
	// Use small payload less than nonceSize
	storeMsg(t, c, "foo", 1, []byte("abcd"))
	s.Close()

	// Now re-open and use crypto store
	s, err = NewFileStore(testLogger, testFSDefaultDatastore, nil)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer s.Close()
	cs, err = NewCryptoStore(s, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error creating crypto store: %v", err)
	}
	state, err := cs.Recover()
	if err != nil {
		t.Fatalf("Error recovering store: %v", err)
	}
	rc = getRecoveredChannel(t, state, "foo")
	rm, err := rc.Msgs.Lookup(1)
	if err == nil || rm != nil {
		t.Fatalf("Expected error and no message, but got %v", rm)
	}
	if !strings.Contains(err.Error(), "trying") {
		t.Fatalf("Expected error about trying to decrypt data that is not, got %v", err)
	}
}

func TestCryptoStoreEmptyMsg(t *testing.T) {
	s := createDefaultMemStore(t)
	defer s.Close()

	cs, err := NewCryptoStore(s, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer cs.Close()

	c := storeCreateChannel(t, cs, "foo")
	msg := &pb.MsgProto{Sequence: 1}
	seq, err := c.Msgs.Store(msg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m := msgStoreLookup(t, c.Msgs, seq)
	if m.Data != nil {
		t.Fatalf("Unexpected content: %s", m.Data)
	}
	m = msgStoreFirstMsg(t, c.Msgs)
	if m.Data != nil {
		t.Fatalf("Unexpected content: %s", m.Data)
	}
	m = msgStoreLastMsg(t, c.Msgs)
	if m.Data != nil {
		t.Fatalf("Unexpected content: %s", m.Data)
	}
}

func TestCryptoStoreUseEnvKey(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	os.Unsetenv(CryptoStoreEnvKeyName)
	defer os.Unsetenv(CryptoStoreEnvKeyName)

	if err := os.Setenv(CryptoStoreEnvKeyName, "testkey"); err != nil {
		t.Fatalf("Unable to set environment variable: %v", err)
	}

	s := createDefaultFileStore(t)
	defer s.Close()

	cs, err := NewCryptoStore(s, nil)
	if err != nil {
		t.Fatalf("Unable to create crypto store: %v", err)
	}
	defer cs.Close()

	c := storeCreateChannel(t, cs, "foo")
	msg := storeMsg(t, c, "foo", 1, []byte("msg"))

	ms := c.Msgs.(*CryptoMsgStore).MsgStore.(*FileMsgStore)
	ms.RLock()
	datFile := ms.files[1].file.name
	ms.RUnlock()

	cs.Close()

	datContent, err := ioutil.ReadFile(datFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if bytes.Contains(datContent, msg.Data) {
		t.Fatalf("Should have found content of message in plaintext, got %s", datContent)
	}

	// Make sure that the env variable overrides a given key,
	// which we will verify by passing a wrong key and making sure
	// that we get the proper message back.
	s, err = NewFileStore(testLogger, testFSDefaultDatastore, nil)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer s.Close()
	cs, err = NewCryptoStore(s, []byte("wrongkey"))
	if err != nil {
		t.Fatalf("Unable to create crypto store: %v", err)
	}
	defer cs.Close()

	state, err := cs.Recover()
	if err != nil {
		t.Fatalf("Error recovering data: %v", err)
	}
	c = getRecoveredChannel(t, state, "foo")
	lm := msgStoreLookup(t, c.Msgs, 1)
	if !reflect.DeepEqual(msg, lm) {
		t.Fatalf("Expected message %v, got %v", msg, lm)
	}
}

func TestCryptoStoreRenewNonce(t *testing.T) {
	s := createDefaultMemStore(t)
	defer s.Close()

	cs, err := NewCryptoStore(s, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer cs.Close()

	c := storeCreateChannel(t, cs, "foo")
	cms := c.Msgs.(*CryptoMsgStore)
	cms.Lock()
	oldNonce := cms.nonce
	oldLimit := cms.nonceLimit
	cms.nonceLimit = 5
	cms.Unlock()

	if len(oldNonce) == 0 || oldLimit == 0 {
		t.Fatalf("Unexpected nonce and/or limit: %v - %v", oldNonce, oldLimit)
	}

	nr := 20
	wg := sync.WaitGroup{}
	wg.Add(nr)
	for i := 0; i < nr; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				storeMsg(t, c, "foo", uint64(i+1), []byte("hello"))
			}
		}()
	}
	wg.Wait()

	cms.Lock()
	currentNonce := cms.nonce
	currentLimit := cms.nonceLimit
	cms.Unlock()

	if bytes.Equal(currentNonce, oldNonce) {
		t.Fatal("Expected nonce to have changed, it did not")
	}
	if currentLimit == 0 || currentLimit == oldLimit {
		t.Fatalf("Expected limit to be different, was %v is now %v", oldLimit, currentLimit)
	}
}

func TestCryptoStoreCheckEncryptedStore(t *testing.T) {
	for _, test := range []struct {
		name       string
		encrypt    bool
		shouldFind bool
	}{
		{
			name:       "no encryption",
			encrypt:    false,
			shouldFind: true,
		},
		{
			name:       "encryption",
			encrypt:    true,
			shouldFind: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanupFSDatastore(t)
			defer cleanupFSDatastore(t)

			fs, err := NewFileStore(testLogger, testFSDefaultDatastore, nil)
			if err != nil {
				t.Fatalf("Error creating store: %v", err)
			}
			var s Store
			if test.encrypt {
				s, err = NewCryptoStore(fs, []byte("testkey"))
				if err != nil {
					t.Fatalf("Error creating crypto store: %v", err)
				}
			} else {
				s = fs
			}
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")

			msg := storeMsg(t, cs, "foo", 1, []byte("This is a message"))
			var ms *FileMsgStore
			if test.encrypt {
				ms = cs.Msgs.(*CryptoMsgStore).MsgStore.(*FileMsgStore)
			} else {
				ms = cs.Msgs.(*FileMsgStore)
			}

			ms.RLock()
			datFile := ms.files[1].file.name
			ms.RUnlock()

			subID := storeSub(t, cs, "foo")

			ss := cs.Subs.(*FileSubStore)
			ss.RLock()
			subFile := ss.file.name
			sub := ss.subs[subID].(*subscription)
			ss.RUnlock()

			s.Close()

			datContent, err := ioutil.ReadFile(datFile)
			if err != nil {
				t.Fatalf("Error reading file: %v", err)
			}
			found := bytes.Contains(datContent, msg.Data)
			if test.shouldFind && !found {
				t.Fatalf("Should have found content of message in plaintext, got %s", datContent)
			} else if !test.shouldFind && found {
				t.Fatalf("Should not have found content of message in plaintext, got %s", datContent)
			}
			// Only message store is encrypted, so subscription file
			// should be in plain text.
			subContent, err := ioutil.ReadFile(subFile)
			if err != nil {
				t.Fatalf("Error reading file: %v", err)
			}
			found = bytes.Contains(subContent, []byte(sub.sub.AckInbox))
			if !found {
				t.Fatalf("Should have found content of subscription in plaintext, got %s", subContent)
			}
		})
	}
}
