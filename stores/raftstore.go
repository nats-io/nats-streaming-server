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

	"github.com/nats-io/nats-streaming-server/logger"
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
	log logger.Logger
}

// RaftSubStore implements the SubStore interface
type RaftSubStore struct {
	genericSubStore
}

// NewRaftStore returns an instarce of a RaftStore
func NewRaftStore(log logger.Logger, s Store, limits *StoreLimits) *RaftStore {
	return &RaftStore{Store: s, log: log}
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
	c.Subs = s.replaceSubStore(channel, c.Subs, 0)
	return c, nil
}

func (s *RaftStore) replaceSubStore(channel string, realSubStore SubStore, maxSubID uint64) *RaftSubStore {
	// Close underlying sub store.
	realSubStore.Close()
	// We need the subs limits for this channel
	cl := s.Store.GetChannelLimits(channel)
	// Create and initialize our sub store.
	rss := &RaftSubStore{}
	rss.init(s.log, &cl.SubStoreLimits)
	rss.maxSubID = maxSubID
	return rss
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
		for channel, rc := range state.Channels {
			// Note that this is when recovering the underlying sub store
			// that would be the case for a RaftSubStore prior to 0.14.1
			var maxSubID uint64
			for _, rs := range rc.Subscriptions {
				if rs.Sub.ID > maxSubID {
					maxSubID = rs.Sub.ID
				}
			}
			rc.Channel.Subs = s.replaceSubStore(channel, rc.Channel.Subs, maxSubID)
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

// CreateSub implements the SubStore interface
func (ss *RaftSubStore) CreateSub(sub *spb.SubState) error {
	gss := &ss.genericSubStore

	gss.Lock()
	defer gss.Unlock()

	// This store does not persist subscriptions, since it is done
	// in the actual RAFT log. This is just a wrapper to the streaming
	// sub store. We still need to apply limits.

	// If sub.ID is provided, check if already present, in which case
	// don't check limit.
	if sub.ID > 0 {
		if _, ok := gss.subs[sub.ID]; ok {
			return nil
		}
	}
	// Check limits
	if gss.limits.MaxSubscriptions > 0 && len(gss.subs) >= gss.limits.MaxSubscriptions {
		return ErrTooManySubs
	}

	// With new server, the sub.ID is set before this call is invoked,
	// and if that is the case, this is what we use. But let's support
	// not having one (in case we recover an existing store, or run a
	// mix of servers with different versions where the leader would
	// not be at a version that sets the sub.ID).
	if sub.ID == 0 {
		gss.maxSubID++
		sub.ID = gss.maxSubID
	} else if sub.ID > gss.maxSubID {
		gss.maxSubID = sub.ID
	}
	gss.subs[sub.ID] = emptySub

	return nil
}

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
