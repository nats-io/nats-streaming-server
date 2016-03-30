package stores

import (
	"sort"
	"sync"

	"github.com/nats-io/stan-server/spb"
	"github.com/nats-io/stan/pb"
)

type GenericStore struct {
	sync.RWMutex
	name     string
	closed   bool
	channels map[string]*ChannelStore
	limits   ChannelLimits
}

type GenericSubStore struct {
	sync.RWMutex
	limits ChannelLimits
}

type GenericMsgStore struct {
	sync.RWMutex
	subject    string // Can't be wildcard
	first      uint64
	last       uint64
	msgs       map[uint64]*pb.MsgProto
	limits     ChannelLimits
	totalCount int
	totalBytes uint64
	closed     bool
}

// Init initializes the structure of a generic store
func (gs *GenericStore) Init(name string, limits ChannelLimits) {
	gs.name = name
	gs.limits = limits
	gs.channels = make(map[string]*ChannelStore, 1)
}

// Name returns the type name of this store
func (gs *GenericStore) Name() string {
	return gs.name
}

// SetChannelLimits sets the limit for the messages and subscriptions stores.
func (gs *GenericStore) SetChannelLimits(limits ChannelLimits) error {
	gs.Lock()
	defer gs.Unlock()
	gs.limits = limits
	return nil
}

// LookupChannel returns a ChannelStore for the given channel.
func (gs *GenericStore) LookupChannel(channel string) *ChannelStore {
	gs.RLock()
	defer gs.RUnlock()

	return gs.channels[channel]
}

// HasChannel returns true if this store has any channel
func (gs *GenericStore) HasChannel() bool {
	gs.RLock()
	defer gs.RUnlock()

	return len(gs.channels) > 0
}

// State returns message store statistics for a given channel ('*' for all)
func (gs *GenericStore) MsgsState(channel string) (numMessages int, byteSize uint64, err error) {
	numMessages = 0
	byteSize = 0
	err = nil

	if channel == AllChannels {
		gs.RLock()
		cs := gs.channels
		gs.RUnlock()

		for _, c := range cs {
			n, b, lerr := c.Msgs.State()
			if lerr != nil {
				err = lerr
				return
			}
			numMessages += n
			byteSize += b
		}
	} else {
		cs := gs.LookupChannel(channel)
		if cs != nil {
			numMessages, byteSize, err = cs.Msgs.State()
		}
	}
	return
}

// Close closes all stores
func (gs *GenericStore) Close() error {
	gs.Lock()
	defer gs.Unlock()

	if gs.closed {
		return nil
	}

	gs.closed = true

	var err error
	var lerr error

	for _, cs := range gs.channels {
		lerr = cs.Subs.Close()
		if err == nil && lerr != nil {
			err = lerr
		}
		lerr = cs.Msgs.Close()
		if err == nil && lerr != nil {
			err = lerr
		}
	}

	return err
}

// Init initializes this generic message store
func (gms *GenericMsgStore) Init(subject string, limits ChannelLimits) {
	gms.subject = subject
	gms.limits = limits
	gms.first = 1
	// FIXME(ik) - Long term, msgs map should probably not be part of the
	// generic store.
	gms.msgs = make(map[uint64]*pb.MsgProto, gms.limits.MaxNumMsgs)
}

// State returns some statistics related to this store
func (gms *GenericMsgStore) State() (numMessages int, byteSize uint64, err error) {
	gms.RLock()
	defer gms.RUnlock()

	return gms.totalCount, gms.totalBytes, nil
}

// FirstSequence returns sequence for first message stored.
func (gms *GenericMsgStore) FirstSequence() uint64 {
	gms.RLock()
	defer gms.RUnlock()
	return gms.first
}

// LastSequence returns sequence for last message stored.
func (gms *GenericMsgStore) LastSequence() uint64 {
	gms.RLock()
	defer gms.RUnlock()
	return gms.last
}

// FirstAndLastSequence returns sequences for the first and last messages stored.
func (gms *GenericMsgStore) FirstAndLastSequence() (uint64, uint64) {
	gms.RLock()
	defer gms.RUnlock()
	return gms.first, gms.last
}

// Lookup returns the stored message with given sequence number.
func (gms *GenericMsgStore) Lookup(seq uint64) *pb.MsgProto {
	gms.RLock()
	defer gms.RUnlock()
	return gms.msgs[seq]
}

// FirstMsg returns the first message stored.
func (gms *GenericMsgStore) FirstMsg() *pb.MsgProto {
	gms.RLock()
	defer gms.RUnlock()
	return gms.msgs[gms.first]
}

// LastMsg returns the last message stored.
func (gms *GenericMsgStore) LastMsg() *pb.MsgProto {
	gms.RLock()
	defer gms.RUnlock()
	return gms.msgs[gms.last]
}

// GetSequenceFromStartTime returns the sequence of the first message whose
// timestamp is greater or equal to given startTime.
func (gms *GenericMsgStore) GetSequenceFromStartTime(startTime int64) uint64 {
	gms.RLock()
	defer gms.RUnlock()

	index := sort.Search(len(gms.msgs), func(i int) bool {
		m := gms.msgs[uint64(i)+gms.first]
		if m.Timestamp >= startTime {
			return true
		}
		return false
	})

	return uint64(index) + gms.first
}

// CreateSub records a new subscription represented by SubState. On success,
// it returns an id that is used by the other methods.
func (gss *GenericSubStore) CreateSub(sub *spb.SubState) (uint64, error) {
	return 0, nil
}

// DeleteSub invalidates this subscription.
func (gss *GenericSubStore) DeleteSub(subid uint64) {
}

// AddSeqPending adds the given message seqno to the given subscription.
func (gss *GenericSubStore) AddSeqPending(subid, seqno uint64) error {
	return nil
}

// AckSeqPending records that the given message seqno has been acknowledged
// by the given subscription.
func (gss *GenericSubStore) AckSeqPending(subid, seqno uint64) error {
	return nil
}

// Close closes this store
func (gss *GenericSubStore) Close() error {
	return nil
}
