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
	"sync"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
)

// This is a proxy to the store interface.
type clientStore struct {
	sync.RWMutex
	clients        map[string]*client
	connIDs        map[string]*client
	waitOnRegister map[string]chan struct{}
	store          stores.Store
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
	return &clientStore{
		clients: make(map[string]*client),
		connIDs: make(map[string]*client),
		store:   store,
	}
}

// getSubsCopy returns a copy of the client's subscribers array.
// At least Read-lock must be held by the caller.
func (c *client) getSubsCopy() []*subState {
	subs := make([]*subState, len(c.subs))
	copy(subs, c.subs)
	return subs
}

// Register a new client. Returns ErrInvalidClient if client is already registered.
func (cs *clientStore) register(info *spb.ClientInfo) (*client, error) {
	cs.Lock()
	defer cs.Unlock()
	c := cs.clients[info.ID]
	if c != nil {
		return nil, ErrInvalidClient
	}
	sc, err := cs.store.AddClient(info)
	if err != nil {
		return nil, err
	}
	c = &client{info: sc, subs: make([]*subState, 0, 4)}
	cs.clients[c.info.ID] = c
	if len(c.info.ConnID) > 0 {
		cs.connIDs[string(c.info.ConnID)] = c
	}
	if cs.waitOnRegister != nil {
		ch := cs.waitOnRegister[c.info.ID]
		if ch != nil {
			ch <- struct{}{}
			delete(cs.waitOnRegister, c.info.ID)
		}
	}
	return c, nil
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
	connID := c.info.ConnID
	c.Unlock()
	delete(cs.clients, ID)
	if len(connID) > 0 {
		delete(cs.connIDs, string(connID))
	}
	if cs.waitOnRegister != nil {
		delete(cs.waitOnRegister, ID)
	}
	err := cs.store.DeleteClient(ID)
	return c, err
}

// IsValid returns true if the client is registered, false otherwise.
func (cs *clientStore) isValid(ID string, connID []byte) bool {
	cs.RLock()
	valid := cs.lookupByConnIDOrID(ID, connID) != nil
	cs.RUnlock()
	return valid
}

// isValidWithTimeout will return true if the client is registered,
// false if not.
// When the client is not yet registered, this call sets up a go channel
// and waits up to `timeout` for the register() call to send the newly
// registered client to the channel.
// On timeout, this call return false to indicate that the client
// has still not registered.
func (cs *clientStore) isValidWithTimeout(ID string, connID []byte, timeout time.Duration) bool {
	cs.Lock()
	c := cs.lookupByConnIDOrID(ID, connID)
	if c != nil {
		cs.Unlock()
		return true
	}
	if cs.waitOnRegister == nil {
		cs.waitOnRegister = make(map[string]chan struct{})
	}
	ch := make(chan struct{}, 1)
	cs.waitOnRegister[ID] = ch
	cs.Unlock()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		// We timed out, remove the entry in the map
		cs.Lock()
		delete(cs.waitOnRegister, ID)
		cs.Unlock()
		return false
	}
}

// Lookup client by ConnID if not nil, otherwise by clientID.
// Assume at least clientStore RLock is held on entry.
func (cs *clientStore) lookupByConnIDOrID(ID string, connID []byte) *client {
	var c *client
	if len(connID) > 0 {
		c = cs.connIDs[string(connID)]
	} else {
		c = cs.clients[ID]
	}
	return c
}

// Lookup a client
func (cs *clientStore) lookup(ID string) *client {
	cs.RLock()
	c := cs.clients[ID]
	cs.RUnlock()
	return c
}

// Lookup a client by connection ID
func (cs *clientStore) lookupByConnID(connID []byte) *client {
	cs.RLock()
	c := cs.connIDs[string(connID)]
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

// removeClientHB will stop and remove the client's heartbeat timer, if
// present.
func (cs *clientStore) removeClientHB(c *client) {
	if c == nil {
		return
	}
	c.Lock()
	if c.hbt != nil {
		c.hbt.Stop()
		c.hbt = nil
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
