// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"github.com/nats-io/nats-streaming-server/spb"
)

// DelegateStore delegates all the Store's APIs calls to the
// store it was given when created.
// This is used by tests programs trying to create a mocked
// store with the ability to override some of the APIs.
// To do so, they would create an object that embeds a
// DelegateStore and simply override the API of their choice.
type DelegateStore struct {
	S Store
}

// GetExclusiveLock implements the Store interface
func (ms *DelegateStore) GetExclusiveLock() (bool, error) {
	return ms.S.GetExclusiveLock()
}

// Init implements the Store interface
func (ms *DelegateStore) Init(si *spb.ServerInfo) error {
	return ms.S.Init(si)
}

// Name implements the Store interface
func (ms *DelegateStore) Name() string {
	return ms.S.Name()
}

// Recover implements the Store interface
func (ms *DelegateStore) Recover() (*RecoveredState, error) {
	return ms.S.Recover()
}

// SetLimits implements the Store interface
func (ms *DelegateStore) SetLimits(limits *StoreLimits) error {
	return ms.S.SetLimits(limits)
}

// CreateChannel implements the Store interface
func (ms *DelegateStore) CreateChannel(channel string, userData interface{}) (*ChannelStore, bool, error) {
	return ms.S.CreateChannel(channel, userData)
}

// LookupChannel implements the Store interface
func (ms *DelegateStore) LookupChannel(channel string) *ChannelStore {
	return ms.S.LookupChannel(channel)
}

// HasChannel implements the Store interface
func (ms *DelegateStore) HasChannel() bool {
	return ms.S.HasChannel()
}

// MsgsState implements the Store interface
func (ms *DelegateStore) MsgsState(channel string) (numMessages int, byteSize uint64, err error) {
	return ms.S.MsgsState(channel)
}

// AddClient implements the Store interface
func (ms *DelegateStore) AddClient(clientID, hbInbox string, userData interface{}) (*Client, bool, error) {
	return ms.S.AddClient(clientID, hbInbox, userData)
}

// GetClient implements the Store interface
func (ms *DelegateStore) GetClient(clientID string) *Client {
	return ms.S.GetClient(clientID)
}

// GetClients implements the Store interface
func (ms *DelegateStore) GetClients() map[string]*Client {
	return ms.S.GetClients()
}

// GetClientsCount implements the Store interface
func (ms *DelegateStore) GetClientsCount() int {
	return ms.S.GetClientsCount()
}

// DeleteClient implements the Store interface
func (ms *DelegateStore) DeleteClient(clientID string) *Client {
	return ms.S.DeleteClient(clientID)
}

// Close implements the Store interface
func (ms *DelegateStore) Close() error {
	return ms.S.Close()
}
