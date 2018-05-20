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
	"sync"

	"github.com/nats-io/nats-streaming-server/spb"
)

// RaftStore is an hybrid store for server running in clustering mode.
// This store persists/recovers ServerInfo and messages, but is a no-op
// for clients and subscriptions since we rely on raft log for that.
// It still creates/deletes subscriptions so that we on recovery we
// can ensure that we don't reuse any subscription ID.
type RaftStore struct {
	sync.Mutex
	Store
}

// RaftSubStore implements the SubStore interface
type RaftSubStore struct {
	SubStore
}

// NewRaftStore returns an instarce of a RaftStore
func NewRaftStore(s Store) *RaftStore {
	return &RaftStore{Store: s}
}

////////////////////////////////////////////////////////////////////////////
// RaftStore methods
////////////////////////////////////////////////////////////////////////////

// CreateChannel implements the Store interface
func (s *RaftStore) CreateChannel(channel string) (*Channel, error) {
	s.Lock()
	defer s.Unlock()
	c, err := s.Store.CreateChannel(channel)
	if err != nil {
		return nil, err
	}
	c.Subs = &RaftSubStore{SubStore: c.Subs}
	return c, nil
}

// Name implements the Store interface
func (s *RaftStore) Name() string {
	return TypeRaft + "_" + s.Store.Name()
}

// Recover implements the Store interface
func (s *RaftStore) Recover() (*RecoveredState, error) {
	s.Lock()
	defer s.Unlock()
	state, err := s.Store.Recover()
	if err != nil {
		return nil, err
	}
	if state != nil {
		for _, rc := range state.Channels {
			rc.Subscriptions = nil
		}
		state.Clients = nil
	}
	return state, nil
}

// AddClient implements the Store interface
func (s *RaftStore) AddClient(info *spb.ClientInfo) (*Client, error) {
	// No need for storage
	return &Client{*info}, nil
}

// DeleteClient implements the Store interface
func (s *RaftStore) DeleteClient(clientID string) error {
	// Make this a no-op
	return nil
}

////////////////////////////////////////////////////////////////////////////
// RaftSubStore methods
////////////////////////////////////////////////////////////////////////////

// UpdateSub implements the SubStore interface
func (ss *RaftSubStore) UpdateSub(*spb.SubState) error {
	// Make this a no-op
	return nil
}

// AddSeqPending adds the given message 'seqno' to the subscription 'subid'.
func (ss *RaftSubStore) AddSeqPending(subid, seqno uint64) error {
	// Make this a no-op
	return nil
}

// AckSeqPending records that the given message 'seqno' has been acknowledged
// by the subscription 'subid'.
func (ss *RaftSubStore) AckSeqPending(subid, seqno uint64) error {
	// Make this a no-op
	return nil
}
