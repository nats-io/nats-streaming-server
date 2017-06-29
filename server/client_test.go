// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nuid"
)

func createClientStore() *clientStore {
	store, _ := stores.NewMemoryStore(testLogger, nil)
	cs := &clientStore{store: store}
	return cs
}

func createClientInfo() (string, string) {
	clientID := "me"
	hbInbox := nuid.Next()

	return clientID, hbInbox
}

func TestClientRegister(t *testing.T) {
	cs := createClientStore()

	clientID, hbInbox := createClientInfo()

	// Register a new one
	sc, isNew, _ := cs.Register(clientID, hbInbox)
	if sc == nil || !isNew {
		t.Fatal("Expected client to be new")
	}
	// Verify it's in the list of clients
	c := cs.Lookup(clientID)
	if c == nil {
		t.Fatal("Expected client to be registered")
	}
	// Verify the created client
	func() {
		c.RLock()
		defer c.RUnlock()
		if sc.ID != clientID {
			t.Fatalf("Expected client id to be %v, got %v", clientID, sc.ID)
		}
		if sc.HbInbox != hbInbox {
			t.Fatalf("Expected client hbInbox to be %v, got %v", hbInbox, sc.HbInbox)
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

	// Register with same info
	secondCli, isNew, _ := cs.Register(clientID, hbInbox)
	if secondCli != sc || isNew {
		t.Fatal("Expected to get the same client")
	}
}

func TestClientParallelRegister(t *testing.T) {
	cs := createClientStore()

	_, hbInbox := createClientInfo()

	var wg sync.WaitGroup
	wg.Add(2)

	totalClients := 100
	errors := make(chan error, 2)

	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < totalClients; j++ {
				clientID := fmt.Sprintf("clientID-%v", j)
				c, isNew, _ := cs.Register(clientID, hbInbox)
				if c == nil {
					errors <- fmt.Errorf("client should not be nil")
					return
				}
				if !isNew && cs.Lookup(clientID) == nil {
					errors <- fmt.Errorf("Register returned isNew false, but clientID %v can't be found", clientID)
				}
				runtime.Gosched()
			}

		}()
	}

	wg.Wait()

	// Fail with the first error found.
	select {
	case e := <-errors:
		t.Fatalf("%v", e)
	default:
	}

	// We should not get more than totalClients
	count := cs.store.GetClientsCount()
	if count != totalClients {
		t.Fatalf("Expected %v clients, got %v", totalClients, count)
	}
}

func TestClientUnregister(t *testing.T) {
	cs := createClientStore()

	clientID, hbInbox := createClientInfo()

	// Unregistering one that does not exist should not cause a crash
	cs.Unregister(clientID)

	// Now register a client
	cs.Register(clientID, hbInbox)

	// Verify it's in the list of clients
	if !cs.IsValid(clientID) {
		t.Fatal("Expected client to be registered")
	}

	// Unregistering now
	cs.Unregister(clientID)

	// Verify it's gone.
	if cs.IsValid(clientID) {
		t.Fatal("Expected client to be unregistered")
	}
}

func TestClientLookup(t *testing.T) {
	cs := createClientStore()

	clientID, hbInbox := createClientInfo()

	// Looks-up one that does not exist
	if c := cs.Lookup("not-registered"); c != nil {
		t.Fatalf("Got unexpected client: %v", c)
	}

	// Registers one
	cs.Register(clientID, hbInbox)

	// Lookup again
	if c := cs.Lookup(clientID); c == nil {
		t.Fatal("Should have looked-up the client")
	}

	// Unregistering
	cs.Unregister(clientID)

	// Lookup, should not be there
	if c := cs.Lookup(clientID); c != nil {
		t.Fatalf("Got unexpected client: %v", c)
	}
}

func TestClientGetClients(t *testing.T) {
	cs := createClientStore()

	if count := cs.store.GetClientsCount(); count != 0 {
		t.Fatalf("Expected no client, got %v", count)
	}

	nuid := nuid.New()

	clientID := "me"
	hbInbox := nuid.Next()

	cs.Register(clientID, hbInbox)

	clientID = "me2"
	hbInbox = nuid.Next()

	cs.Register(clientID, hbInbox)

	clients := cs.store.GetClients()
	if clients == nil || len(clients) != 2 {
		t.Fatalf("Expected to get 2 clients, got %v", len(clients))
	}

	for cID := range clients {
		if cID != "me" && cID != "me2" {
			t.Fatalf("Unexpected client ID: %v", cID)
		}
	}
}

func TestClientAddSub(t *testing.T) {
	cs := createClientStore()

	clientID, hbInbox := createClientInfo()

	sub := &subState{}

	// Try to add a sub with client ID not registered
	if cs.AddSub(clientID, sub) {
		t.Fatal("Expected AddSub to return false")
	}

	// Now register the client
	sc, _, _ := cs.Register(clientID, hbInbox)

	// Now this should work
	if !cs.AddSub(clientID, sub) {
		t.Fatal("Expected AddSub to return true")
	}

	// Check the sub is properly added to the client's subs list.
	c := sc.UserData.(*client)
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
	cs.Unregister(clientID)

	// Again, this should fail since the clientID is not registered
	// anymore.
	if cs.AddSub(clientID, sub) {
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
			if cs.AddSub(clientID, sub) {
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
		cs.Register(clientID, hbInbox)
		runtime.Gosched()
		sc = cs.Unregister(clientID)
		if sc == nil {
			t.Fatal("Client should have been found")
		}
		c = sc.UserData.(*client)
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

	clientID, hbInbox := createClientInfo()

	sub := &subState{}

	// Try to remove a sub with client ID not registered
	if cs.RemoveSub(clientID, sub) {
		t.Fatal("Expected RemoveSub to return false")
	}

	// Now register the client
	cs.Register(clientID, hbInbox)

	// Add a subscription
	if !cs.AddSub(clientID, sub) {
		t.Fatal("Expected AddSub to return true")
	}

	// And remove it..
	if !cs.RemoveSub(clientID, sub) {
		t.Fatal("Expected RemoveSub to return true")
	}

	// Unregister
	cs.Unregister(clientID)

	// Again, this should fail since the clientID is not registered
	// anymore.
	if cs.RemoveSub(clientID, sub) {
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
			if cs.RemoveSub(clientID, sub) {
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
		cs.Register(clientID, hbInbox)
		cs.AddSub(clientID, sub)
		runtime.Gosched()
		sc := cs.Unregister(clientID)
		if sc == nil {
			t.Fatal("Client should have been found")
		}
		c := sc.UserData.(*client)
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

	clientID, hbInbox := createClientInfo()

	if subs := cs.GetSubs(clientID); len(subs) != 0 {
		t.Fatalf("Expected 0 subs, got: %v", len(subs))
	}

	// Now register the client
	cs.Register(clientID, hbInbox)

	// Add a subscription
	if !cs.AddSub(clientID, &subState{subject: "foo"}) {
		t.Fatal("Expected AddSub to return true")
	}

	// or two
	// Add a subscription
	if !cs.AddSub(clientID, &subState{subject: "bar"}) {
		t.Fatal("Expected AddSub to return true")
	}

	subs := cs.GetSubs(clientID)
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subs, got: %v", len(subs))
	}

	// Make sure subs is a copy by switching the 2 subscriptions in the
	// client's subs array
	c := cs.Lookup(clientID)
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
