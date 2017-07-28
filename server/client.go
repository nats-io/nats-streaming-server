// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"github.com/nats-io/nats-streaming-server/stores"
	"sync"
	"time"
)

// This is a proxy to the store interface.
type clientStore struct {
	sync.RWMutex
	clients map[string]*client
	store   stores.Store
}

// client has information needed by the server. A client is also
// stored in a stores.Client object (which contains ID and HbInbox).
type client struct {
	sync.RWMutex
	info *stores.Client
	hbt  *time.Timer
	fhb  int
	subs []*subState
}

// newClientStore creates a new clientStore instance using `store` as the backing storage.
func newClientStore(store stores.Store) *clientStore {
	return &clientStore{clients: make(map[string]*client), store: store}
}

// getSubsCopy returns a copy of the client's subscribers array.
// At least Read-lock must be held by the caller.
func (c *client) getSubsCopy() []*subState {
	subs := make([]*subState, len(c.subs))
	copy(subs, c.subs)
	return subs
}

// Register a client if new, otherwise returns the client already registered
// and `false` to indicate that the client is not new.
func (cs *clientStore) register(ID, hbInbox string) (*client, bool, error) {
	cs.Lock()
	defer cs.Unlock()
	c := cs.clients[ID]
	if c != nil {
		return c, false, nil
	}
	sc, err := cs.store.AddClient(ID, hbInbox)
	if err != nil {
		return nil, false, err
	}
	c = &client{info: sc, subs: make([]*subState, 0, 4)}
	cs.clients[ID] = c
	return c, true, nil
}

// Unregister a client.
func (cs *clientStore) unregister(ID string) (*client, error) {
	cs.Lock()
	defer cs.Unlock()
	c := cs.clients[ID]
	if c == nil {
		return nil, nil
	}
	c.Lock()
	if c.hbt != nil {
		c.hbt.Stop()
		c.hbt = nil
	}
	c.Unlock()
	delete(cs.clients, ID)
	err := cs.store.DeleteClient(ID)
	return c, err
}

// IsValid returns true if the client is registered, false otherwise.
func (cs *clientStore) isValid(ID string) bool {
	return cs.lookup(ID) != nil
}

// Lookup a client
func (cs *clientStore) lookup(ID string) *client {
	cs.RLock()
	c := cs.clients[ID]
	cs.RUnlock()
	return c
}

// GetSubs returns the list of subscriptions for the client identified by ID,
// or nil if such client is not found.
func (cs *clientStore) getSubs(ID string) []*subState {
	cs.RLock()
	defer cs.RUnlock()
	c := cs.clients[ID]
	if c == nil {
		return nil
	}
	c.RLock()
	subs := c.getSubsCopy()
	c.RUnlock()
	return subs
}

// AddSub adds the subscription to the client identified by clientID
// and returns true only if the client has not been unregistered,
// otherwise returns false.
func (cs *clientStore) addSub(ID string, sub *subState) bool {
	cs.RLock()
	defer cs.RUnlock()
	c := cs.clients[ID]
	if c == nil {
		return false
	}
	c.Lock()
	c.subs = append(c.subs, sub)
	c.Unlock()
	return true
}

// RemoveSub removes the subscription from the client identified by clientID
// and returns true only if the client has not been unregistered and that
// the subscription was found, otherwise returns false.
func (cs *clientStore) removeSub(ID string, sub *subState) bool {
	cs.RLock()
	defer cs.RUnlock()
	c := cs.clients[ID]
	if c == nil {
		return false
	}
	c.Lock()
	removed := false
	c.subs, removed = sub.deleteFromList(c.subs)
	c.Unlock()
	return removed
}

// recoverClients recreates the content of the client store based on clients
// information recovered from the Store.
func (cs *clientStore) recoverClients(clients []*stores.Client) {
	cs.Lock()
	for _, sc := range clients {
		client := &client{info: sc, subs: make([]*subState, 0, 4)}
		cs.clients[client.info.ID] = client
	}
	cs.Unlock()
}

// setClientHB will lookup the client `ID` and, if present, set the
// client's timer with the given interval and function.
func (cs *clientStore) setClientHB(ID string, interval time.Duration, f func()) {
	cs.RLock()
	defer cs.RUnlock()
	c := cs.clients[ID]
	if c == nil {
		return
	}
	c.Lock()
	if c.hbt == nil {
		c.hbt = time.AfterFunc(interval, f)
	}
	c.Unlock()
}

// getClients returns a snapshot of the registered clients.
// The map itself is a copy (can be iterated safely), but
// the clients objects returned are the one stored in the clientStore.
func (cs *clientStore) getClients() map[string]*client {
	cs.RLock()
	defer cs.RUnlock()
	clients := make(map[string]*client, len(cs.clients))
	for _, c := range cs.clients {
		clients[c.info.ID] = c
	}
	return clients
}

// count returns the number of registered clients
func (cs *clientStore) count() int {
	cs.RLock()
	total := len(cs.clients)
	cs.RUnlock()
	return total
}
