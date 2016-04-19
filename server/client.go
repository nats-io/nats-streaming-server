// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"github.com/nats-io/stan-server/stores"
	"sync"
	"time"
)

// Hold current clients.
type clientStore struct {
	sync.RWMutex
	clients map[string]*client
	store   stores.Store
}

// Hold for client
type client struct {
	sync.RWMutex
	clientID string
	hbInbox  string
	hbt      *time.Timer
	fhb      int
	subs     []*subState
}

// Track subscriptions
func (c *client) addSub(sub *subState) {
	c.Lock()
	defer c.Unlock()
	c.subs = append(c.subs, sub)
}

// Remove a subscription
func (c *client) removeSub(sub *subState) bool {
	c.Lock()
	defer c.Unlock()

	removed := false
	c.subs, removed = sub.deleteFromList(c.subs)
	return removed
}

// Register a client if new, otherwise returns the client already registered
// and 'isNew' is set to false.
func (cs *clientStore) Register(ID, hbInbox string) (c *client, isNew bool, err error) {
	c = cs.Lookup(ID)
	if c != nil {
		return c, false, nil
	}

	cs.Lock()
	defer cs.Unlock()
	c = cs.clients[ID]
	if c != nil {
		return c, false, nil
	}
	// Create a new client here...
	// Store the client first.
	if cs.store != nil {
		if err := cs.store.AddClient(ID, hbInbox); err != nil {
			return nil, false, err
		}
	}
	// Add to the map
	c = &client{
		clientID: ID,
		hbInbox:  hbInbox,
		subs:     make([]*subState, 0, 4),
	}
	cs.clients[c.clientID] = c

	// Return the client and 'true' to indicate that the client is new.
	return c, true, nil
}

// Unregister a client
func (cs *clientStore) Unregister(ID string) {
	cs.Lock()
	defer cs.Unlock()
	client := cs.clients[ID]
	if client != nil {
		client.subs = nil
		delete(cs.clients, ID)
		if cs.store != nil {
			cs.store.DeleteClient(ID)
		}
	}

}

// Check validity of a client.
func (s *StanServer) isValidClient(ID string) bool {
	return s.clients.Lookup(ID) != nil
}

// Lookup a client
func (cs *clientStore) Lookup(ID string) *client {
	cs.RLock()
	defer cs.RUnlock()
	return cs.clients[ID]
}

// GetClients returns the list of clients
func (cs *clientStore) GetClients() []*client {
	cs.RLock()
	defer cs.RUnlock()

	clients := make([]*client, 0, len(cs.clients))
	for _, c := range cs.clients {
		clients = append(clients, c)
	}
	return clients
}

// GetSubs returns the list of subscriptions for the client identified by ID,
// or nil if such client is not found.
func (cs *clientStore) GetSubs(ID string) []*subState {
	cs.RLock()
	defer cs.RUnlock()

	c := cs.clients[ID]
	if c == nil {
		return nil
	}
	c.RLock()
	subs := c.subs
	c.RUnlock()
	return subs
}

// AddSub atomically adds the subscription to the client identified by
// clientID. If the client is not found in the list, the subscription is not
// added and nil is returned.
func (cs *clientStore) AddSub(ID string, sub *subState) *client {
	cs.RLock()
	defer cs.RUnlock()
	if c := cs.clients[ID]; c != nil {
		c.addSub(sub)
		return c
	}
	return nil
}

// RemoveSub atomically removes the subscription from the client identified
// by clientID. If the client is not found in the list, the subscription is
// not removed and nil is returned.
func (cs *clientStore) RemoveSub(ID string, sub *subState) *client {
	cs.RLock()
	defer cs.RUnlock()
	if c := cs.clients[ID]; c != nil {
		if !c.removeSub(sub) {
			return nil
		}
		return c
	}
	return nil
}

// SetStore sets the store to be used when registering/unregistering clients.
func (cs *clientStore) SetStore(store stores.Store) {
	cs.Lock()
	cs.store = store
	cs.Unlock()
}
