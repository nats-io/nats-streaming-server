// Copyright 2018-2019 The NATS Authors
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
	"testing"
	"time"

	"github.com/nats-io/stan.go/pb"
)

func TestCryptoStoreKeyIsCleared(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	orgStr := "thisisthekey"
	key := []byte(orgStr)
	cs, err := NewCryptoStore(s, CryptoCipherAutoSelect, key)
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

	cs, err := NewCryptoStore(s, CryptoCipherAutoSelect, nil)
	if cs != nil || err != ErrCryptoStoreRequiresKey {
		t.Fatalf("Expected no store and error %q, got %v - %v", ErrCryptoStoreRequiresKey.Error(), cs, err)
	}
	cs, err = NewCryptoStore(s, "not supported cipher", []byte("mykey"))
	if cs != nil || err != ErrCipherNotSupported {
		t.Fatalf("Expected no store and error %q, got %v - %v", ErrCipherNotSupported.Error(), cs, err)
	}

	goodKey := []byte("testkey")
	cs, err = NewCryptoStore(s, CryptoCipherAES, goodKey)
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
		cs, err = NewCryptoStore(s, CryptoCipherAES, k)
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
	// Add a plain text message
	c = storeCreateChannel(t, s, "foo")
	storeMsg(t, c, "foo", 1, []byte("abcd"))
	// Add a message with payload that matches one of the algo,
	// but is not encrypted
	storeMsg(t, c, "foo", 2, []byte{CryptoCodeAES, 'a', 'b', 'c', 'd'})
	s.Close()

	// Now re-open and use crypto store
	s, err = NewFileStore(testLogger, testFSDefaultDatastore, nil)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer s.Close()
	cs, err = NewCryptoStore(s, CryptoCipherAES, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error creating crypto store: %v", err)
	}
	state, err := cs.Recover()
	if err != nil {
		t.Fatalf("Error recovering store: %v", err)
	}
	rc = getRecoveredChannel(t, state, "foo")
	rm, err := rc.Msgs.Lookup(1)
	if err != nil {
		t.Fatalf("Expected to get plain text message, got %v", err)
	}
	if string(rm.Data) != "abcd" {
		t.Fatalf("Expected message data to be %q, got %q", "abcd", rm.Data)
	}
	rm, err = rc.Msgs.Lookup(2)
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

	cs, err := NewCryptoStore(s, CryptoCipherAES, []byte("testkey"))
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

	cs, err := NewCryptoStore(s, CryptoCipherAES, nil)
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
	cs, err = NewCryptoStore(s, CryptoCipherAES, []byte("wrongkey"))
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
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	cs, err := NewCryptoStore(s, CryptoCipherAES, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer cs.Close()

	c := storeCreateChannel(t, cs, "foo")

	nr := 200
	for i := 0; i < nr; i++ {
		storeMsg(t, c, "foo", uint64(i+1), []byte("hello"))
	}
	c.Msgs.Flush()

	cms := c.Msgs.(*CryptoMsgStore)
	cms.Lock()
	val := cms.eds.nonce[cms.eds.nonceSize-1]
	cms.Unlock()
	if val != byte(nr+1) {
		t.Fatalf("Unexpected nonce counter: %v", val)
	}

	cs.Close()

	s, err = NewFileStore(testLogger, testFSDefaultDatastore, nil)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()
	cs, err = NewCryptoStore(s, CryptoCipherAES, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer cs.Close()

	rs, err := cs.Recover()
	if err != nil {
		t.Fatalf("Error recovering file: %v", err)
	}
	c = getRecoveredChannel(t, rs, "foo")
	cms = c.Msgs.(*CryptoMsgStore)
	cms.Lock()
	val = cms.eds.nonce[cms.eds.nonceSize-1]
	cms.Unlock()
	if val != byte(nr+1) {
		t.Fatalf("Unexpected nonce counter: %v", val)
	}
	for i := 0; i < 100; i++ {
		storeMsg(t, c, "foo", uint64(i+1), []byte("hello"))
	}
	cms.Lock()
	val1 := cms.eds.nonce[cms.eds.nonceSize-2]
	val2 := cms.eds.nonce[cms.eds.nonceSize-1]
	cms.Unlock()
	if val1 != 1 || val2 != 45 {
		t.Fatalf("Expected values %v, %v", val1, val2)
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
				s, err = NewCryptoStore(fs, CryptoCipherAES, []byte("testkey"))
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

func TestCryptoStoreMultipleCiphers(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	payloads := [][]byte{
		[]byte("this is a plain text message"),
		[]byte("this is a message encrypted with AES cipher"),
		[]byte("this is a message encrypted with CHACHA cipher"),
	}

	c := storeCreateChannel(t, s, "foo")
	storeMsg(t, c, "foo", 1, payloads[0])

	s.Close()

	storeWithEncryption := func(t *testing.T, encryptionCipher string, payloadIdx int) {
		t.Helper()
		s, err := NewFileStore(testLogger, testFSDefaultDatastore, nil)
		if err != nil {
			t.Fatalf("Error opening store: %v", err)
		}
		defer s.Close()

		cs, err := NewCryptoStore(s, encryptionCipher, []byte("mykey"))
		if err != nil {
			t.Fatalf("Error creating crypto store: %v", err)
		}
		defer cs.Close()

		state, err := cs.Recover()
		if err != nil {
			t.Fatalf("Error recovering store: %v", err)
		}
		c = getRecoveredChannel(t, state, "foo")
		storeMsg(t, c, "foo", uint64(payloadIdx+1), payloads[payloadIdx])
		s.Close()
	}
	storeWithEncryption(t, CryptoCipherAES, 1)
	storeWithEncryption(t, CryptoCipherChaChaPoly, 2)

	// Now re-open with any cipher, use the auto-select one.
	// We should be able to get all 3 messages correctly.
	s, err := NewFileStore(testLogger, testFSDefaultDatastore, nil)
	if err != nil {
		t.Fatalf("Error opening store: %v", err)
	}
	defer s.Close()

	cs, err := NewCryptoStore(s, CryptoCipherAutoSelect, []byte("mykey"))
	if err != nil {
		t.Fatalf("Error creating crypto store: %v", err)
	}
	defer cs.Close()
	state, err := cs.Recover()
	if err != nil {
		t.Fatalf("Error recovering store: %v", err)
	}
	c = getRecoveredChannel(t, state, "foo")
	for i := 0; i < 3; i++ {
		rm, err := c.Msgs.Lookup(uint64(i + 1))
		if err != nil {
			t.Fatalf("Error getting message: %v", err)
		}
		if !bytes.Equal(rm.Data, payloads[i]) {
			t.Fatalf("Expected message %q, got %q", payloads[i], rm.Data)
		}
	}
}

func TestCryptoFileAutoSync(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs, _ := newFileStore(t, testFSDefaultDatastore, nil, AutoSync(15*time.Millisecond))
	s, err := NewCryptoStore(fs, CryptoCipherAES, []byte("testkey"))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	// Add some state
	cs := storeCreateChannel(t, s, "foo")
	storeMsg(t, cs, "foo", 1, []byte("msg"))
	storeSub(t, cs, "foo")

	// Wait for auto sync to kick in
	time.Sleep(50 * time.Millisecond)

	// Server should not have panic'ed.
}
