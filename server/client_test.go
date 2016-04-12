// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/nats-io/nuid"
)

func createClientStore() *clientStore {
	cs := &clientStore{
		clients: make(map[string]*client),
	}
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
	c, isNew := cs.Register(clientID, hbInbox)
	if c == nil || !isNew {
		t.Fatal("Expected client to be new")
	}
	// Verify it's in the list of clients
	func() {
		cs.RLock()
		defer cs.RUnlock()

		if cs.clients[clientID] == nil {
			t.Fatal("Expected client to be registered")
		}
	}()
	// Verify the created client
	func() {
		c.RLock()
		defer c.RUnlock()
		if c.clientID != clientID {
			t.Fatalf("Expected client id to be %v, got %v", clientID, c.clientID)
		}
		if c.hbInbox != hbInbox {
			t.Fatalf("Expected client hbInbox to be %v, got %v", hbInbox, c.hbInbox)
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
	secondCli, isNew := cs.Register(clientID, hbInbox)
	if secondCli != c || isNew {
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
				c, isNew := cs.Register(clientID, hbInbox)
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
	func() {
		cs.RLock()
		defer cs.RUnlock()

		if len(cs.clients) != totalClients {
			t.Fatalf("Expected %v clients, got %v", totalClients, len(cs.clients))
		}
	}()
}

func TestClientUnregister(t *testing.T) {
	cs := createClientStore()

	clientID, hbInbox := createClientInfo()

	// Unregistering one that does not exist should not cause a crash
	cs.Unregister(clientID)

	// Now register a client
	cs.Register(clientID, hbInbox)

	// Verify it's in the list of clients
	func() {
		cs.RLock()
		defer cs.RUnlock()

		if cs.clients[clientID] == nil {
			t.Fatal("Expected client to be registered")
		}
	}()

	// Unregistering now
	cs.Unregister(clientID)

	// Verify it's gone.
	func() {
		cs.RLock()
		defer cs.RUnlock()

		if cs.clients[clientID] != nil {
			t.Fatal("Expected client to be unregistered")
		}
	}()
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

	clients := cs.GetClients()
	if len(clients) != 0 {
		t.Fatalf("Expected no client, got %v", len(clients))
	}

	nuid := nuid.New()

	clientID := "me"
	hbInbox := nuid.Next()

	cs.Register(clientID, hbInbox)

	clientID = "me2"
	hbInbox = nuid.Next()

	cs.Register(clientID, hbInbox)

	clients = cs.GetClients()
	if clients == nil || len(clients) != 2 {
		t.Fatalf("Expected to get 2 clients, got %v", len(clients))
	}

	for _, c := range clients {
		func() {
			c.RLock()
			defer c.RUnlock()

			if c.clientID != "me" && c.clientID != "me2" {
				t.Fatalf("Unexpected client ID: %v", c.clientID)
			}
		}()
	}
}

func TestClientAddSub(t *testing.T) {
	cs := createClientStore()

	clientID, hbInbox := createClientInfo()

	sub := &subState{}

	// Try to add a sub with client ID not registered
	if c := cs.AddSub(clientID, sub); c != nil {
		t.Fatalf("Expected AddSub to return nil, got %v", c)
	}

	// Now register the client
	cs.Register(clientID, hbInbox)

	// Now this should work
	c := cs.AddSub(clientID, sub)
	if c == nil {
		t.Fatal("Expected AddSub to return c")
	}

	// Check the sub is properly added to the client's subs list.
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
	if c := cs.AddSub(clientID, sub); c != nil {
		t.Fatalf("Expected AddSub to return nil, got %v", c)
	}
}

func TestClientRemoveSub(t *testing.T) {
	cs := createClientStore()

	clientID, hbInbox := createClientInfo()

	sub := &subState{}

	// Try to remove a sub with client ID not registered
	if c := cs.RemoveSub(clientID, sub); c != nil {
		t.Fatalf("Expected RemoveSub to return nil, got %v", c)
	}

	// Now register the client
	cs.Register(clientID, hbInbox)

	// Add a subscription
	c := cs.AddSub(clientID, sub)
	if c == nil {
		t.Fatal("Expected AddSub to return c")
	}

	// And remove it..
	if c := cs.RemoveSub(clientID, sub); c == nil {
		t.Fatal("Expected RemoveSub to return c")
	}

	// Unregister
	cs.Unregister(clientID)

	// Again, this should fail since the clientID is not registered
	// anymore.
	if c := cs.RemoveSub(clientID, sub); c != nil {
		t.Fatalf("Expected Remove to return nil, got %v", c)
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
	c := cs.AddSub(clientID, &subState{subject: "foo"})
	if c == nil {
		t.Fatal("Expected AddSub to return c")
	}

	// or two
	// Add a subscription
	if c := cs.AddSub(clientID, &subState{subject: "bar"}); c == nil {
		t.Fatal("Expected AddSub to return c")
	}

	subs := cs.GetSubs(clientID)
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subs, got: %v", len(subs))
	}

	for _, s := range subs {
		if s.subject != "foo" && s.subject != "bar" {
			t.Fatalf("Unexpected subject: %v", s.subject)
		}
	}
}
