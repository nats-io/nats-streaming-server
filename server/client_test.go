// Copyright 2016-2018 The NATS Authors
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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nuid"
)

func createClientStore() *clientStore {
	store, _ := stores.NewMemoryStore(testLogger, nil)
	cs := newClientStore(store)
	return cs
}

func createClientInfo() *spb.ClientInfo {
	return &spb.ClientInfo{
		ID:      "me",
		HbInbox: nuid.Next(),
		ConnID:  []byte(nuid.Next()),
	}
}

func TestClientRegister(t *testing.T) {
	cs := createClientStore()

	info := createClientInfo()
	clientID := info.ID
	hbInbox := info.HbInbox

	// Register a new one
	sc, err := cs.register(info)
	if err != nil {
		t.Fatalf("Error on register: %v", err)
	}
	if sc == nil {
		t.Fatal("Unable to register client")
	}
	// Verify it's in the list of clients
	c := cs.lookup(clientID)
	if c == nil {
		t.Fatal("Expected client to be registered")
	}
	// Verify the created client
	func() {
		c.RLock()
		defer c.RUnlock()
		if c.info.ID != clientID {
			t.Fatalf("Expected client id to be %v, got %v", clientID, c.info.ID)
		}
		if c.info.HbInbox != hbInbox {
			t.Fatalf("Expected client hbInbox to be %v, got %v", hbInbox, c.info.HbInbox)
		}
		if c.hbt != nil {
			t.Fatal("Did not expect timer to be set")
		}
		if c.fhb != 0 {
			t.Fatalf("Expected fhb to be 0, got %v", c.fhb)
		}
		if len(c.subs) != 0 {
			t.Fatalf("Expected subs count to be 0, got %v", len(c.subs))
		}
	}()

	// Try to register with same clientID, should get an error
	secondCli, err := cs.register(&spb.ClientInfo{ID: clientID, HbInbox: hbInbox})
	if secondCli != nil || err != ErrInvalidClient {
		t.Fatalf("Expected to get no client and an error, got %v err=%v", secondCli, err)
	}
}

func TestClientUnregister(t *testing.T) {
	cs := createClientStore()

	info := createClientInfo()
	clientID := info.ID
	connID := info.ConnID

	// Unregistering one that does not exist should not cause a crash
	cs.unregister(clientID)

	// Now register a client
	cs.register(info)

	// Verify it's in the list of clients
	if !cs.isValid(clientID, nil) {
		t.Fatal("Expected client to be registered")
	}
	if !cs.isValid("", connID) {
		t.Fatal("Expected client to be registered")
	}
	if !cs.isValid(clientID, connID) {
		t.Fatal("Expected client to be registered")
	}

	// Unregistering now
	cs.unregister(clientID)

	// Verify it's gone.
	if cs.isValid(clientID, nil) {
		t.Fatal("Expected client to be unregistered")
	}
	if cs.isValid("", connID) {
		t.Fatal("Expected client to be unregistered")
	}
	if cs.isValid(clientID, connID) {
		t.Fatal("Expected client to be unregistered")
	}
}

func TestClientLookup(t *testing.T) {
	cs := createClientStore()

	info := createClientInfo()
	clientID := info.ID

	// Looks-up one that does not exist
	if c := cs.lookup("not-registered"); c != nil {
		t.Fatalf("Got unexpected client: %v", c)
	}

	// Registers one
	cs.register(info)

	// Lookup again
	if c := cs.lookup(clientID); c == nil {
		t.Fatal("Should have looked-up the client")
	}

	// Unregistering
	cs.unregister(clientID)

	// Lookup, should not be there
	if c := cs.lookup(clientID); c != nil {
		t.Fatalf("Got unexpected client: %v", c)
	}
}

func TestClientGetClientIDs(t *testing.T) {
	cs := createClientStore()

	if count := cs.count(); count != 0 {
		t.Fatalf("Expected no client, got %v", count)
	}

	nuid := nuid.New()

	cs.register(&spb.ClientInfo{ID: "me", HbInbox: nuid.Next()})

	cs.register(&spb.ClientInfo{ID: "me2", HbInbox: nuid.Next()})

	clients := cs.getClients()
	if clients == nil || len(clients) != 2 {
		t.Fatalf("Expected to get 2 clients, got %v", len(clients))
	}

	for _, c := range clients {
		if c.info.ID != "me" && c.info.ID != "me2" {
			t.Fatalf("Unexpected client ID: %v", c.info.ID)
		}
	}
}

func TestClientAddSub(t *testing.T) {
	cs := createClientStore()

	info := createClientInfo()
	clientID := info.ID

	sub := &subState{}

	// Try to add a sub with client ID not registered
	if cs.addSub(clientID, sub) {
		t.Fatal("Expected AddSub to return false")
	}

	// Now register the client
	sc, _ := cs.register(info)

	// Now this should work
	if !cs.addSub(clientID, sub) {
		t.Fatal("Expected AddSub to return true")
	}

	// Check the sub is properly added to the client's subs list.
	c := cs.lookup(clientID)
	func() {
		c.RLock()
		defer c.RUnlock()

		if len(c.subs) != 1 {
			t.Fatalf("Expected to have 1 sub, got %v", len(c.subs))
		}
		if c.subs[0] != sub {
			t.Fatalf("Got unexpected sub: %v", c.subs[0])
		}
	}()

	// Unregister
	cs.unregister(clientID)

	// Again, this should fail since the clientID is not registered
	// anymore.
	if cs.addSub(clientID, sub) {
		t.Fatalf("Expected AddSub to return nil, got %v", c)
	}

	// Try to test the case where we are trying to add a subscription while
	// the client is being unregistered.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	added := int32(0)
	done := make(chan bool)
	// Try to add subscriptions while the client is being unregistered
	go func() {
		defer wg.Done()
		for {
			if cs.addSub(clientID, sub) {
				atomic.AddInt32(&added, 1)
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	total := 10000
	insubs := 0
	for i := 0; i < total; i++ {
		// Register the client
		cs.register(info)
		runtime.Gosched()
		c, _ := cs.unregister(clientID)
		if sc == nil {
			t.Fatal("Client should have been found")
		}
		c.RLock()
		subs := c.subs
		c.RUnlock()
		insubs += len(subs)
	}
	done <- true
	wg.Wait()
	if a := int(atomic.LoadInt32(&added)); a != insubs {
		t.Fatalf("Unexpected counts: added=%v insubs=%v", a, insubs)
	}
}

func TestClientRemoveSub(t *testing.T) {
	cs := createClientStore()

	info := createClientInfo()
	clientID := info.ID

	sub := &subState{}

	// Try to remove a sub with client ID not registered
	if cs.removeSub(clientID, sub) {
		t.Fatal("Expected RemoveSub to return false")
	}

	// Now register the client
	cs.register(info)

	// Add a subscription
	if !cs.addSub(clientID, sub) {
		t.Fatal("Expected AddSub to return true")
	}

	// And remove it..
	if !cs.removeSub(clientID, sub) {
		t.Fatal("Expected RemoveSub to return true")
	}

	// Unregister
	cs.unregister(clientID)

	// Again, this should fail since the clientID is not registered
	// anymore.
	if cs.removeSub(clientID, sub) {
		t.Fatal("Expected Remove to return false")
	}

	// Try to test the case where we are trying to remove a subscription while
	// the client is being unregistered.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	removed := int32(0)
	done := make(chan bool)
	// Try to remove subscriptions while the client is being unregistered
	go func() {
		defer wg.Done()
		for {
			if cs.removeSub(clientID, sub) {
				atomic.AddInt32(&removed, 1)
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	total := 10000
	insubs := 0
	for i := 0; i < total; i++ {
		// Register the client
		cs.register(info)
		cs.addSub(clientID, sub)
		runtime.Gosched()
		c, _ := cs.unregister(clientID)
		if c == nil {
			t.Fatal("Client should have been found")
		}
		c.RLock()
		subs := c.subs
		c.RUnlock()
		insubs += len(subs)
	}
	done <- true
	wg.Wait()
	if r := int(atomic.LoadInt32(&removed)); r != total-insubs {
		t.Fatalf("Unexpected counts: removed=%v insubs=%v missing=%v", r, insubs, total-insubs)
	}
}

func TestClientGetSubs(t *testing.T) {
	cs := createClientStore()

	info := createClientInfo()
	clientID := info.ID

	if subs := cs.getSubs(clientID); len(subs) != 0 {
		t.Fatalf("Expected 0 subs, got: %v", len(subs))
	}

	// Now register the client
	cs.register(info)

	// Add a subscription
	if !cs.addSub(clientID, &subState{subject: "foo"}) {
		t.Fatal("Expected AddSub to return true")
	}

	// or two
	// Add a subscription
	if !cs.addSub(clientID, &subState{subject: "bar"}) {
		t.Fatal("Expected AddSub to return true")
	}

	subs := cs.getSubs(clientID)
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subs, got: %v", len(subs))
	}

	// Make sure subs is a copy by switching the 2 subscriptions in the
	// client's subs array
	c := cs.lookup(clientID)
	c.Lock()
	sub2 := c.subs[1]
	c.subs[1] = c.subs[0]
	c.subs[0] = sub2
	c.Unlock()

	for idx, s := range subs {
		// The subs copy should still have "foo" first, and "bar" second.
		switch idx {
		case 0:
			if s.subject != "foo" {
				t.Fatalf("First subject should be \"foo\", got %q", s.subject)
			}
		case 1:
			if s.subject != "bar" {
				t.Fatalf("Second subject should be \"bar\", got %q", s.subject)
			}
		}
	}
}

func TestClientSetClientHBForNonExistentClient(t *testing.T) {
	cs := createClientStore()

	ch := make(chan struct{})
	cs.setClientHB("me", time.Millisecond, func() { ch <- struct{}{} })
	// function should not be invoked
	select {
	case <-ch:
		t.Fatal("Timer should not have fired!")
	case <-time.After(25 * time.Millisecond):
		// ok
	}
}

type clientStoreErrorsStore struct{ stores.Store }

func (s *clientStoreErrorsStore) AddClient(_ *spb.ClientInfo) (*stores.Client, error) {
	return nil, errOnPurpose
}
func (s *clientStoreErrorsStore) DeleteClient(id string) error {
	return errOnPurpose
}

func TestClientStoreErrors(t *testing.T) {
	cs := createClientStore()

	info := createClientInfo()

	// Register a client
	rc, err := cs.register(info)
	if err != nil {
		t.Fatalf("Error during registration: %v", err)
	}
	// Make sure it is registered.
	if c := cs.lookup("me"); c == nil || c != rc {
		t.Fatal("Client should have been registered")
	}

	// Now replace store with a store failing the Add/Remove client apis.
	cs.Lock()
	cs.store = &clientStoreErrorsStore{Store: cs.store}
	cs.Unlock()

	// Register: store will fail the AddClient call
	if _, err := cs.register(&spb.ClientInfo{ID: "me2", HbInbox: "hbInbox"}); err == nil {
		t.Fatal("Expected register to fail")
	}
	// Make sure client is not registered
	if c := cs.lookup("me2"); c != nil {
		t.Fatalf("Unexpected registered client: %v", c)
	}

	// Try to unregister the one that was successfully registered
	c, err := cs.unregister("me")
	if err == nil {
		t.Fatal("Should have failed")
	}
	// However, client should have been removed
	if c == nil || c != rc {
		t.Fatalf("Unexpected registered client: %v", c)
	}
}
