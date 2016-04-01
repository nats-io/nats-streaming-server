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
	// How many channels are allowed.
	MaxChannels int
	// How many messages per channel are allowed.
	MaxNumMsgs int
	// How many bytes (messages payloads) per channel are allowed.
	MaxMsgBytes uint64
	// How old messages on a channel can be before being removed.
	MaxMsgAge time.Duration
	// How many subscriptions per channel are allowed.
	MaxSubs int
}

// DefaultChannelLimits are the default channel limits that a Store must
// use when created. Use Store.SetChannelLimits() to change the limits.
var DefaultChannelLimits = ChannelLimits{
	MaxChannels: 100,
	MaxNumMsgs:  1000000,
	MaxMsgBytes: 1000000 * 1024,
	MaxSubs:     1000,
}

// ChannelStore contains a reference to both Subscription and Message stores.
type ChannelStore struct {
	// UserData allows the user of a ChannelStore to store private data.
	UserData interface{}
	// Subs is the Subscriptions Store.
	Subs SubStore
	// Msgs is the Messages Store.
	Msgs MsgStore
}

// Store is the storage interface for STAN servers.
//
// Implementations of this interface should create a Store with DefaultChannelLimits.
// Note that limits should not be applied on recovery (for implementations that
// support recovery). The user can specify limits before using the store by using
// the SetChannelLimits() method.
//
// When calling the method LookupOrCreateChannel(), if the channel does not exist,
// the implementation should create an instance of SubStore and MsgStore, passing
// the Store's channel limits. Then, it should create a ChannelStore structure,
// which holds reference to those two stores, and return it, along with a boolean
// indicating the the channel has been created during this call. If the channel
// does exist, then LookupOrCreateChannel() behaves like LookupChannel() and
// the boolean returned is false.
//
// The LookupChannel() method should only return a ChannelStore that has been
// previously created, and nil if it does not exist.
type Store interface {
	// Name returns the name type of this store (e.g: MEMORY, FILESTORE, etc...).
	Name() string

	// SetChannelLimits sets limits per channel.
	SetChannelLimits(limits ChannelLimits)

	// LookupOrCreateChannel returns a ChannelStore for the given channel,
	// or creates one if the channel doesn't exist. In this case, the returned
	// boolean will be true.
	LookupOrCreateChannel(channel string) (*ChannelStore, bool, error)

	// LookupChannel returns a ChannelStore for the given channel, nil if channel
	// does not exist.
	LookupChannel(channel string) *ChannelStore

	// HasChannel returns true if this store has any channel.
	HasChannel() bool

	// State returns message store statistics for a given channel, or all
	// if 'channel' is AllChannels.
	MsgsState(channel string) (numMessages int, byteSize uint64, err error)

	// Close closes all stores.
	Close() error
}

// SubStore is the interface for storage of Subscriptions on a given channel.
//
// Implementations of this interface should not attempt to validate that
// a subscription is valid (that is, has not been deleted) when processing
// updates.
type SubStore interface {
	// CreateSub records a new subscription represented by SubState. On success,
	// it records the subscription's ID in SubState.ID. This ID is to be used
	// by the other SubStore methods.
	CreateSub(*spb.SubState) error

	// DeleteSub invalidates the subscription 'subid'.
	DeleteSub(subid uint64)

	// AddSeqPending adds the given message 'seqno' to the subscription 'subid'.
	AddSeqPending(subid, seqno uint64) error

	// AckSeqPending records that the given message 'seqno' has been acknowledged
	// by the subscription 'subid'.
	AckSeqPending(subid, seqno uint64) error

	// Close closes the subscriptions store.
	Close() error
}

// MsgStore is the interface for storage of Messages on a given channel.
type MsgStore interface {
	// State returns some statistics related to this store.
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
