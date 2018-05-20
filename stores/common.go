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

package stores

import (
	"sync"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

// format string used to report that limit is reached when storing
// messages.
var droppingMsgsFmt = "WARNING: Reached limits for store %q (msgs=%v/%v bytes=%v/%v), " +
	"dropping old messages to make room for new ones"

// commonStore contains everything that is common to any type of store
type commonStore struct {
	sync.RWMutex
	closed bool
	log    logger.Logger
}

// genericStore is the generic store implementation with a map of channels.
type genericStore struct {
	commonStore
	limits   *StoreLimits
	sublist  *util.Sublist
	name     string
	channels map[string]*Channel
}

// Used as the value for the genericSubStore's subs map.
var emptySub = struct{}{}

// genericSubStore is the generic store implementation that manages subscriptions
// for a given channel.
type genericSubStore struct {
	commonStore
	limits   SubStoreLimits
	subs     map[uint64]interface{}
	maxSubID uint64
}

// genericMsgStore is the generic store implementation that manages messages
// for a given channel.
type genericMsgStore struct {
	commonStore
	limits     MsgStoreLimits
	subject    string // Can't be wildcard
	first      uint64
	last       uint64
	totalCount int
	totalBytes uint64
	hitLimit   bool // indicates if store had to drop messages due to limit
}

////////////////////////////////////////////////////////////////////////////
// genericStore methods
////////////////////////////////////////////////////////////////////////////

// init initializes the structure of a generic store
func (gs *genericStore) init(name string, log logger.Logger, limits *StoreLimits) error {
	gs.name = name
	if limits == nil {
		limits = &DefaultStoreLimits
	}
	if err := gs.setLimits(limits); err != nil {
		return err
	}
	gs.log = log
	// Do not use limits values to create the map.
	gs.channels = make(map[string]*Channel)
	return nil
}

// GetExclusiveLock implements the Store interface.
func (gs *genericStore) GetExclusiveLock() (bool, error) {
	// Need to be implementation specific.
	return false, ErrNotSupported
}

// Init can be used to initialize the store with server's information.
func (gs *genericStore) Init(info *spb.ServerInfo) error {
	return nil
}

// Name returns the type name of this store
func (gs *genericStore) Name() string {
	return gs.name
}

// Recover implements the Store interface.
func (gs *genericStore) Recover() (*RecoveredState, error) {
	// Implementations that can recover their state need to
	// override this.
	return nil, nil
}

// setLimits makes a copy of the given StoreLimits,
// validates the limits and if ok, applies the inheritance.
func (gs *genericStore) setLimits(limits *StoreLimits) error {
	// Make a copy.
	gs.limits = limits.Clone()
	// Build will validate and apply inheritance if no error.
	if err := gs.limits.Build(); err != nil {
		return err
	}
	// We don't need the PerChannel map and the sublist. So replace
	// the map with the sublist instead.
	gs.sublist = util.NewSublist()
	for key, val := range gs.limits.PerChannel {
		// val is already a copy of the original limits.PerChannel[key],
		// so don't need to make a copy again, we own this.
		gs.sublist.Insert(key, val)
	}
	// Get rid of the map now.
	gs.limits.PerChannel = nil
	return nil
}

// Returns the appropriate limits for this channel based on inheritance.
// The channel is assumed to be a literal, and the store lock held on entry.
func (gs *genericStore) getChannelLimits(channel string) *ChannelLimits {
	r := gs.sublist.Match(channel)
	if len(r) == 0 {
		// If there is no match, that means we need to use the global limits.
		return &gs.limits.ChannelLimits
	}
	// If there is a match, use the limits from the last element because
	// we know that the returned array is ordered from widest to narrowest,
	// and the only literal that there is would be the channel we are
	// looking up.
	return r[len(r)-1].(*ChannelLimits)
}

// GetChannelLimits implements the Store interface
func (gs *genericStore) GetChannelLimits(channel string) *ChannelLimits {
	gs.RLock()
	defer gs.RUnlock()
	c := gs.channels[channel]
	if c == nil {
		return nil
	}
	// Return a copy
	cl := *gs.getChannelLimits(channel)
	return &cl
}

// SetLimits sets limits for this store
func (gs *genericStore) SetLimits(limits *StoreLimits) error {
	gs.Lock()
	err := gs.setLimits(limits)
	gs.Unlock()
	return err
}

// CreateChannel implements the Store interface
func (gs *genericStore) CreateChannel(channel string) (*Channel, error) {
	return nil, nil
}

// DeleteChannel implements the Store interface
func (gs *genericStore) DeleteChannel(channel string) error {
	gs.Lock()
	err := gs.deleteChannel(channel)
	gs.Unlock()
	return err
}

func (gs *genericStore) deleteChannel(channel string) error {
	c := gs.channels[channel]
	if c == nil {
		return ErrNotFound
	}
	err := c.Msgs.Close()
	if lerr := c.Subs.Close(); lerr != nil && err == nil {
		err = lerr
	}
	if err != nil {
		return err
	}
	delete(gs.channels, channel)
	return nil
}

// canAddChannel returns true if the current number of channels is below the limit.
// If a channel named `channelName` alreadt exists, an error is returned.
// Store lock is assumed to be locked.
func (gs *genericStore) canAddChannel(name string) error {
	if gs.channels[name] != nil {
		return ErrAlreadyExists
	}
	if gs.limits.MaxChannels > 0 && len(gs.channels) >= gs.limits.MaxChannels {
		return ErrTooManyChannels
	}
	return nil
}

// AddClient implements the Store interface
func (gs *genericStore) AddClient(info *spb.ClientInfo) (*Client, error) {
	return &Client{*info}, nil
}

// DeleteClient implements the Store interface
func (gs *genericStore) DeleteClient(clientID string) error {
	return nil
}

// Close closes all stores
func (gs *genericStore) Close() error {
	gs.Lock()
	defer gs.Unlock()
	if gs.closed {
		return nil
	}
	gs.closed = true
	return gs.close()
}

// close closes all stores. Store lock is assumed held on entry
func (gs *genericStore) close() error {
	var err error
	var lerr error

	for _, cs := range gs.channels {
		lerr = cs.Subs.Close()
		if lerr != nil && err == nil {
			err = lerr
		}
		lerr = cs.Msgs.Close()
		if lerr != nil && err == nil {
			err = lerr
		}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////
// genericMsgStore methods
////////////////////////////////////////////////////////////////////////////

// init initializes this generic message store
func (gms *genericMsgStore) init(subject string, log logger.Logger, limits *MsgStoreLimits) {
	gms.subject = subject
	gms.limits = *limits
	gms.log = log
}

// State returns some statistics related to this store
func (gms *genericMsgStore) State() (numMessages int, byteSize uint64, err error) {
	gms.RLock()
	c, b := gms.totalCount, gms.totalBytes
	gms.RUnlock()
	return c, b, nil
}

// Store implements the MsgStore interface
func (gms *genericMsgStore) Store(msg *pb.MsgProto) (uint64, error) {
	// no-op
	return 0, nil
}

// FirstSequence returns sequence for first message stored.
func (gms *genericMsgStore) FirstSequence() (uint64, error) {
	gms.RLock()
	first := gms.first
	gms.RUnlock()
	return first, nil
}

// LastSequence returns sequence for last message stored.
func (gms *genericMsgStore) LastSequence() (uint64, error) {
	gms.RLock()
	last := gms.last
	gms.RUnlock()
	return last, nil
}

// FirstAndLastSequence returns sequences for the first and last messages stored.
func (gms *genericMsgStore) FirstAndLastSequence() (uint64, uint64, error) {
	gms.RLock()
	first, last := gms.first, gms.last
	gms.RUnlock()
	return first, last, nil
}

// Lookup returns the stored message with given sequence number.
func (gms *genericMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	return nil, nil
}

// FirstMsg returns the first message stored.
func (gms *genericMsgStore) FirstMsg() (*pb.MsgProto, error) {
	return nil, nil
}

// LastMsg returns the last message stored.
func (gms *genericMsgStore) LastMsg() (*pb.MsgProto, error) {
	return nil, nil
}

func (gms *genericMsgStore) Flush() error {
	return nil
}

// GetSequenceFromTimestamp returns the sequence of the first message whose
// timestamp is greater or equal to given timestamp.
func (gms *genericMsgStore) GetSequenceFromTimestamp(timestamp int64) (uint64, error) {
	return 0, nil
}

// Empty implements the MsgStore interface
func (gms *genericMsgStore) Empty() error {
	return nil
}

func (gms *genericMsgStore) empty() {
	gms.first, gms.last, gms.totalCount, gms.totalBytes, gms.hitLimit = 0, 0, 0, 0, false
}

// Close closes this store.
func (gms *genericMsgStore) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////
// genericSubStore methods
////////////////////////////////////////////////////////////////////////////

// init initializes the structure of a generic sub store
func (gss *genericSubStore) init(log logger.Logger, limits *SubStoreLimits) {
	gss.limits = *limits
	gss.log = log
	gss.subs = make(map[uint64]interface{})
}

// CreateSub records a new subscription represented by SubState. On success,
// it records the subscription's ID in SubState.ID. This ID is to be used
// by the other SubStore methods.
func (gss *genericSubStore) CreateSub(sub *spb.SubState) error {
	gss.Lock()
	err := gss.createSub(sub)
	gss.Unlock()
	return err
}

// UpdateSub updates a given subscription represented by SubState.
func (gss *genericSubStore) UpdateSub(sub *spb.SubState) error {
	return nil
}

// createSub checks that the number of subscriptions is below the max
// and if so, assigns a new subscription ID and keep track of it in a map.
// Lock is assumed to be held on entry.
func (gss *genericSubStore) createSub(sub *spb.SubState) error {
	if gss.limits.MaxSubscriptions > 0 && len(gss.subs) >= gss.limits.MaxSubscriptions {
		return ErrTooManySubs
	}

	// Bump the max value before assigning it to the new subscription.
	gss.maxSubID++

	// This new subscription has the max value.
	sub.ID = gss.maxSubID
	// Store anything. Some implementations may replace with specific
	// object.
	gss.subs[sub.ID] = emptySub

	return nil
}

// DeleteSub invalidates this subscription.
func (gss *genericSubStore) DeleteSub(subid uint64) error {
	gss.Lock()
	delete(gss.subs, subid)
	gss.Unlock()
	return nil
}

// AddSeqPending adds the given message seqno to the given subscription.
func (gss *genericSubStore) AddSeqPending(subid, seqno uint64) error {
	return nil
}

// AckSeqPending records that the given message seqno has been acknowledged
// by the given subscription.
func (gss *genericSubStore) AckSeqPending(subid, seqno uint64) error {
	return nil
}

// Flush is for stores that may buffer operations and need them to be persisted.
func (gss *genericSubStore) Flush() error {
	return nil
}

// Close closes this store
func (gss *genericSubStore) Close() error {
	return nil
}
