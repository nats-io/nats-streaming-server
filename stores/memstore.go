// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"time"

	"github.com/nats-io/stan/pb"
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
	ms.init("MEMORY", limits)
	return ms, nil
}

// LookupOrCreateChannel returns a ChannelStore for the given channel,
// creates one if no such channel exists. In this case, the returned
// boolean will be true.
func (ms *MemoryStore) LookupOrCreateChannel(channel string) (*ChannelStore, bool, error) {
	channelStore := ms.LookupChannel(channel)
	if channelStore != nil {
		return channelStore, false, nil
	}

	// Two "threads" could make this call and end-up deciding to create the
	// channel. So we need to test again, this time under the write lock.
	ms.Lock()
	defer ms.Unlock()
	channelStore = ms.channels[channel]
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
		Subs: subStore,
		Msgs: msgStore,
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

	// Check if we need to remove any.
	if ms.totalCount > ms.limits.MaxNumMsgs || ms.totalBytes > ms.limits.MaxMsgBytes {
		firstMsg := ms.msgs[ms.first]
		ms.totalBytes -= uint64(len(firstMsg.Data))
		ms.totalCount--
		Errorf("WARNING: Removing message[%d] from the store for [`%s`]", ms.first, ms.subject)
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
