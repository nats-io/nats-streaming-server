// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"errors"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/stan-server/spb"
	"github.com/nats-io/stan/pb"
)

const (
	// AllChannels allows to get state for all channels.
	AllChannels = "*"
)

// Errors.
var (
	ErrTooManyChannels = errors.New("too many channels")
	ErrTooManySubs     = errors.New("too many subscriptions per channel")
)

// Errorf generates error message
func Errorf(format string, v ...interface{}) {
	server.Errorf(format, v...)
}

// ChannelLimits defines some limits on the store interface
type ChannelLimits struct {
	MaxChannels int
	MaxNumMsgs  int
	MaxMsgBytes uint64
	MaxMsgAge   time.Duration
	MaxSubs     int
}

// ChannelStore contains a reference to both Subscription and Message stores.
type ChannelStore struct {
	UserData interface{}
	Subs     SubStore
	Msgs     MsgStore
}

// Store is the storage interface for STAN servers.
type Store interface {
	// Name returns the name type of this store (e.g: MEMORY, FILESTORE, etc...).
	Name() string

	// SetChannelLimits sets limits per channel.
	SetChannelLimits(limits ChannelLimits) error

	// LookupOrCreateChannel returns a ChannelStore for the given channel,
	// creates one if no such channel exists. In this case, the returned
	// boolean will be true.
	LookupOrCreateChannel(channel string) (*ChannelStore, bool, error)

	// LookupChannel returns a ChannelStore for the given channel, nil if channel
	// does not exist
	LookupChannel(channel string) *ChannelStore

	// HasChannel returns true if this store has any channel
	HasChannel() bool

	// State returns message store statistics for a given channel ('*' for all)
	MsgsState(channel string) (numMessages int, byteSize uint64, err error)

	// Close closes all stores
	Close() error
}

// SubStore stores a subscription state.
type SubStore interface {
	// CreateSub records a new subscription represented by SubState. On success,
	// it returns an id that is used by the other methods.
	CreateSub(*spb.SubState) (uint64, error)

	// DeleteSub invalidates this subscription.
	DeleteSub(subid uint64)

	// AddSeqPending adds the given message seqno to the given subscription.
	AddSeqPending(subid, seqno uint64) error

	// AckSeqPending records that the given message seqno has been acknowledged
	// by the given subscription.
	AckSeqPending(subid, seqno uint64) error

	// Close closes the store.
	Close() error
}

// MsgStore is a per channel message store
type MsgStore interface {
	// State returns some statistics related to this store
	State() (numMessages int, byteSize uint64, err error)

	// Store stores a message.
	Store(reply string, data []byte) (*pb.MsgProto, error)

	// Lookup returns the stored message with given sequence number.
	Lookup(seq uint64) *pb.MsgProto

	// FirstSequence returns sequence for first message stored.
	FirstSequence() uint64

	// LastSequence returns sequence for last message stored.
	LastSequence() uint64

	// FirstAndLastSequence returns sequences for the first and last messages stored.
	FirstAndLastSequence() (uint64, uint64)

	// GetSequenceFromStartTime returns the sequence of the first message whose
	// timestamp is greater or equal to given startTime.
	GetSequenceFromStartTime(startTime int64) uint64

	// FirstMsg returns the first message stored.
	FirstMsg() *pb.MsgProto

	// LastMsg returns the last message stored.
	LastMsg() *pb.MsgProto

	// Close closes the store.
	Close() error
}
