// Copyright 2016 Apcera Inc. All rights reserved.

package stan

import (
	"sync"
	"time"
)

// Hold current clients.
type clientStore struct {
	sync.RWMutex
	clients map[string]*client
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
func (c *client) AddSub(sub *subState) {
	c.Lock()
	defer c.Unlock()
	c.subs = append(c.subs, sub)
}

// Remove a subscription
func (c *client) RemoveSub(sub *subState) {
	c.Lock()
	defer c.Unlock()
	c.subs = sub.deleteFromList(c.subs)
}

// Register a client
func (cs *clientStore) Register(c *client) {
	cs.Lock()
	defer cs.Unlock()
	cs.clients[c.clientID] = c
}

// Unregister a client
func (cs *clientStore) Unregister(ID string) {
	cs.Lock()
	defer cs.Unlock()
	client := cs.clients[ID]
	if client != nil {
		client.subs = nil
	}
	delete(cs.clients, ID)
}

// Check validity of a client.
func (s *stanServer) isValidClient(ID string) bool {
	return s.clients.Lookup(ID) != nil
}

// Lookup a client
func (cs *clientStore) Lookup(ID string) *client {
	cs.RLock()
	defer cs.RUnlock()
	return cs.clients[ID]
}
