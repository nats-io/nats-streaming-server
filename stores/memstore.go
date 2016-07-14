// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
)

// MemoryStore is a factory for message and subscription stores.
type MemoryStore struct {
	genericStore
}

// MemorySubStore is a subscription store in memory
type MemorySubStore struct {
	genericSubStore
}

// MemoryMsgStore is a per channel message store in memory
type MemoryMsgStore struct {
	genericMsgStore
}

////////////////////////////////////////////////////////////////////////////
// MemoryStore methods
////////////////////////////////////////////////////////////////////////////

// NewMemoryStore returns a factory for stores held in memory.
// If not limits are provided, the store will be created with
// DefaultChannelLimits.
func NewMemoryStore(limits *ChannelLimits) (*MemoryStore, error) {
	ms := &MemoryStore{}
	ms.init(TypeMemory, limits)
	return ms, nil
}

// CreateChannel creates a ChannelStore for the given channel, and returns
// `true` to indicate that the channel is new, false if it already exists.
func (ms *MemoryStore) CreateChannel(channel string, userData interface{}) (*ChannelStore, bool, error) {
	ms.Lock()
	defer ms.Unlock()
	channelStore := ms.channels[channel]
	if channelStore != nil {
		return channelStore, false, nil
	}

	if err := ms.canAddChannel(); err != nil {
		return nil, false, err
	}

	msgStore := &MemoryMsgStore{}
	msgStore.init(channel, ms.limits)

	subStore := &MemorySubStore{}
	subStore.init(channel, ms.limits)

	channelStore = &ChannelStore{
		Subs:     subStore,
		Msgs:     msgStore,
		UserData: userData,
	}

	ms.channels[channel] = channelStore

	return channelStore, true, nil
}

////////////////////////////////////////////////////////////////////////////
// MemoryMsgStore methods
////////////////////////////////////////////////////////////////////////////

// Store a given message.
func (ms *MemoryMsgStore) Store(reply string, data []byte) (*pb.MsgProto, error) {
	ms.Lock()
	defer ms.Unlock()

	if ms.first == 0 {
		ms.first = 1
	}
	ms.last++
	m := &pb.MsgProto{
		Sequence:  ms.last,
		Subject:   ms.subject,
		Reply:     reply,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}
	ms.msgs[ms.last] = m
	ms.totalCount++
	ms.totalBytes += uint64(len(data))

	// Check if we need to remove any (but leave at least the last added)
	for ms.totalCount > ms.limits.MaxNumMsgs ||
		((ms.totalCount > 1) && (ms.totalBytes > ms.limits.MaxMsgBytes)) {
		firstMsg := ms.msgs[ms.first]
		ms.totalBytes -= uint64(len(firstMsg.Data))
		ms.totalCount--
		if !ms.hitLimit {
			ms.hitLimit = true
			Noticef(droppingMsgsFmt, ms.subject, ms.totalCount, ms.limits.MaxNumMsgs, ms.totalBytes, ms.limits.MaxMsgBytes)
		}
		delete(ms.msgs, ms.first)
		ms.first++
	}

	return m, nil
}

////////////////////////////////////////////////////////////////////////////
// MemorySubStore methods
////////////////////////////////////////////////////////////////////////////

// AddSeqPending adds the given message seqno to the given subscription.
func (*MemorySubStore) AddSeqPending(subid, seqno uint64) error {
	// Overrides in case genericSubStore does something. For the memory
	// based store, we want to minimize the cost of this to a minimum.
	return nil
}

// AckSeqPending records that the given message seqno has been acknowledged
// by the given subscription.
func (*MemorySubStore) AckSeqPending(subid, seqno uint64) error {
	// Overrides in case genericSubStore does something. For the memory
	// based store, we want to minimize the cost of this to a minimum.
	return nil
}
