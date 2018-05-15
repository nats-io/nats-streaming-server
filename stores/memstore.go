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
	"sort"
	"sync"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/util"
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
	msgs     map[uint64]*pb.MsgProto
	ageTimer *time.Timer
	wg       sync.WaitGroup
}

////////////////////////////////////////////////////////////////////////////
// MemoryStore methods
////////////////////////////////////////////////////////////////////////////

// NewMemoryStore returns a factory for stores held in memory.
// If not limits are provided, the store will be created with
// DefaultStoreLimits.
func NewMemoryStore(log logger.Logger, limits *StoreLimits) (*MemoryStore, error) {
	ms := &MemoryStore{}
	if err := ms.init(TypeMemory, log, limits); err != nil {
		return nil, err
	}
	return ms, nil
}

// CreateChannel implements the Store interface
func (ms *MemoryStore) CreateChannel(channel string) (*Channel, error) {
	ms.Lock()
	defer ms.Unlock()

	// Verify that it does not already exist or that we did not hit the limits
	if err := ms.canAddChannel(channel); err != nil {
		return nil, err
	}

	channelLimits := ms.genericStore.getChannelLimits(channel)

	msgStore := &MemoryMsgStore{msgs: make(map[uint64]*pb.MsgProto, 64)}
	msgStore.init(channel, ms.log, &channelLimits.MsgStoreLimits)

	subStore := &MemorySubStore{}
	subStore.init(ms.log, &channelLimits.SubStoreLimits)

	c := &Channel{
		Subs: subStore,
		Msgs: msgStore,
	}
	ms.channels[channel] = c

	return c, nil
}

////////////////////////////////////////////////////////////////////////////
// MemoryMsgStore methods
////////////////////////////////////////////////////////////////////////////

// Store a given message.
func (ms *MemoryMsgStore) Store(m *pb.MsgProto) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()

	if m.Sequence <= ms.last {
		// We've already seen this message.
		return m.Sequence, nil
	}

	if ms.first == 0 {
		ms.first = m.Sequence
	}
	ms.last = m.Sequence
	ms.msgs[ms.last] = m
	ms.totalCount++
	ms.totalBytes += uint64(m.Size())
	// If there is an age limit and no timer yet created, do so now
	if ms.limits.MaxAge > time.Duration(0) && ms.ageTimer == nil {
		ms.wg.Add(1)
		ms.ageTimer = time.AfterFunc(ms.limits.MaxAge, ms.expireMsgs)
	}

	// Check if we need to remove any (but leave at least the last added)
	maxMsgs := ms.limits.MaxMsgs
	maxBytes := ms.limits.MaxBytes
	if maxMsgs > 0 || maxBytes > 0 {
		for ms.totalCount > 1 &&
			((maxMsgs > 0 && ms.totalCount > maxMsgs) ||
				(maxBytes > 0 && (ms.totalBytes > uint64(maxBytes)))) {
			ms.removeFirstMsg()
			if !ms.hitLimit {
				ms.hitLimit = true
				ms.log.Noticef(droppingMsgsFmt, ms.subject, ms.totalCount, ms.limits.MaxMsgs,
					util.FriendlyBytes(int64(ms.totalBytes)), util.FriendlyBytes(ms.limits.MaxBytes))
			}
		}
	}

	return ms.last, nil
}

// Lookup returns the stored message with given sequence number.
func (ms *MemoryMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	ms.RLock()
	m := ms.msgs[seq]
	ms.RUnlock()
	return m, nil
}

// FirstMsg returns the first message stored.
func (ms *MemoryMsgStore) FirstMsg() (*pb.MsgProto, error) {
	ms.RLock()
	m := ms.msgs[ms.first]
	ms.RUnlock()
	return m, nil
}

// LastMsg returns the last message stored.
func (ms *MemoryMsgStore) LastMsg() (*pb.MsgProto, error) {
	ms.RLock()
	m := ms.msgs[ms.last]
	ms.RUnlock()
	return m, nil
}

// GetSequenceFromTimestamp returns the sequence of the first message whose
// timestamp is greater or equal to given timestamp.
func (ms *MemoryMsgStore) GetSequenceFromTimestamp(timestamp int64) (uint64, error) {
	ms.RLock()
	defer ms.RUnlock()

	// No message ever stored
	if ms.first == 0 {
		return 0, nil
	}
	// All messages have expired
	if ms.first > ms.last {
		return ms.last + 1, nil
	}
	if ms.msgs[ms.first].Timestamp >= timestamp {
		return ms.first, nil
	}
	if timestamp >= ms.msgs[ms.last].Timestamp {
		return ms.last + 1, nil
	}

	index := sort.Search(len(ms.msgs), func(i int) bool {
		return ms.msgs[uint64(i)+ms.first].Timestamp >= timestamp
	})

	return uint64(index) + ms.first, nil
}

// expireMsgs ensures that messages don't stay in the log longer than the
// limit's MaxAge.
func (ms *MemoryMsgStore) expireMsgs() {
	ms.Lock()
	defer ms.Unlock()
	if ms.closed {
		ms.wg.Done()
		return
	}

	now := time.Now().UnixNano()
	maxAge := int64(ms.limits.MaxAge)
	for {
		m, ok := ms.msgs[ms.first]
		if !ok {
			if ms.first < ms.last {
				ms.first++
				continue
			}
			ms.ageTimer = nil
			ms.wg.Done()
			return
		}
		elapsed := now - m.Timestamp
		if elapsed >= maxAge {
			ms.removeFirstMsg()
		} else {
			if elapsed < 0 {
				ms.ageTimer.Reset(time.Duration(m.Timestamp - now + maxAge))
			} else {
				ms.ageTimer.Reset(time.Duration(maxAge - elapsed))
			}
			return
		}
	}
}

// removeFirstMsg removes the first message and updates totals.
func (ms *MemoryMsgStore) removeFirstMsg() {
	firstMsg := ms.msgs[ms.first]
	ms.totalBytes -= uint64(firstMsg.Size())
	ms.totalCount--
	delete(ms.msgs, ms.first)
	ms.first++
}

// Empty implements the MsgStore interface
func (ms *MemoryMsgStore) Empty() error {
	ms.Lock()
	if ms.ageTimer != nil {
		if ms.ageTimer.Stop() {
			ms.wg.Done()
		}
		ms.ageTimer = nil
	}
	ms.empty()
	ms.msgs = make(map[uint64]*pb.MsgProto)
	ms.Unlock()
	return nil
}

// Close implements the MsgStore interface
func (ms *MemoryMsgStore) Close() error {
	ms.Lock()
	if ms.closed {
		ms.Unlock()
		return nil
	}
	ms.closed = true
	if ms.ageTimer != nil {
		if ms.ageTimer.Stop() {
			ms.wg.Done()
		}
	}
	ms.Unlock()

	ms.wg.Wait()
	return nil
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
