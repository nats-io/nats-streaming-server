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
	"errors"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
)

const (
	// TypeMemory is the store type name for memory based stores
	TypeMemory = "MEMORY"
	// TypeFile is the store type name for file based stores
	TypeFile = "FILE"
	// TypeSQL is the store type name for sql based stores
	TypeSQL = "SQL"
	// TypeRaft is the store type name for the raft stores
	TypeRaft = "RAFT"
)

// Errors.
var (
	ErrTooManyChannels = errors.New("too many channels")
	ErrTooManySubs     = errors.New("too many subscriptions per channel")
	ErrNotSupported    = errors.New("not supported")
	ErrAlreadyExists   = errors.New("already exists")
	ErrNotFound        = errors.New("not found")
)

// StoreLimits define limits for a store.
type StoreLimits struct {
	// How many channels are allowed.
	MaxChannels int `json:"max_channels"`
	// Global limits. Any 0 value means that the limit is ignored (unlimited).
	ChannelLimits
	// Per-channel limits. Special values for limits in this map:
	// - == 0 means that the corresponding global limit is used.
	// -  < 0 means that limit is ignored (unlimited).
	PerChannel map[string]*ChannelLimits `json:"channels,omitempty"`
}

// ChannelLimits defines limits for a given channel
type ChannelLimits struct {
	// Limits for message stores
	MsgStoreLimits
	// Limits for subscriptions stores
	SubStoreLimits
	// How long without any active subscription and no new message
	// before this channel can be deleted.
	MaxInactivity time.Duration
}

// MsgStoreLimits defines limits for a MsgStore.
// For global limits, a value of 0 means "unlimited".
// For per-channel limits, it means that the corresponding global
// limit is used.
type MsgStoreLimits struct {
	// How many messages are allowed.
	MaxMsgs int `json:"max_msgs"`
	// How many bytes are allowed.
	MaxBytes int64 `json:"max_bytes"`
	// How long messages are kept in the log (unit is seconds)
	MaxAge time.Duration `json:"max_age"`
}

// SubStoreLimits defines limits for a SubStore
type SubStoreLimits struct {
	// How many subscriptions are allowed.
	MaxSubscriptions int `json:"max_subscriptions"`
}

// DefaultStoreLimits are the limits that a Store must
// use when none are specified to the Store constructor.
// Store limits can be changed with the Store.SetLimits() method.
var DefaultStoreLimits = StoreLimits{
	100,
	ChannelLimits{
		MsgStoreLimits{
			MaxMsgs:  1000000,
			MaxBytes: 1000000 * 1024,
		},
		SubStoreLimits{
			MaxSubscriptions: 1000,
		},
		0,
	},
	nil,
}

// RecoveredState allows the server to reconstruct its state after a restart.
type RecoveredState struct {
	Info     *spb.ServerInfo
	Clients  []*Client
	Channels map[string]*RecoveredChannel
}

// RecoveredChannel represents a channel that has been recovered, with all its subscriptions
type RecoveredChannel struct {
	Channel       *Channel
	Subscriptions []*RecoveredSubscription
}

// PendingAcks is a set of message sequences waiting to be acknowledged.
type PendingAcks map[uint64]struct{}

// RecoveredSubscription represents a recovered Subscription with a map
// of pending messages.
type RecoveredSubscription struct {
	Sub     *spb.SubState
	Pending PendingAcks
}

// Client represents a client with ID and Heartbeat Inbox.
type Client struct {
	spb.ClientInfo
}

// Channel contains a reference to both Subscription and Message stores.
type Channel struct {
	// Subs is the Subscriptions Store.
	Subs SubStore
	// Msgs is the Messages Store.
	Msgs MsgStore
}

// Store is the storage interface for NATS Streaming servers.
//
// If an implementation has a Store constructor with StoreLimits, it should be
// noted that the limits don't apply to any state being recovered, for Store
// implementations supporting recovery.
//
type Store interface {
	// GetExclusiveLock is an advisory lock to prevent concurrent
	// access to the store from multiple instances.
	// This is not to protect individual API calls, instead, it
	// is meant to protect the store for the entire duration the
	// store is being used. This is why there is no `Unlock` API.
	// The lock should be released when the store is closed.
	//
	// If an exclusive lock can be immediately acquired (that is,
	// it should not block waiting for the lock to be acquired),
	// this call will return `true` with no error. Once a store
	// instance has acquired an exclusive lock, calling this
	// function has no effect and `true` with no error will again
	// be returned.
	//
	// If the lock cannot be acquired, this call will return
	// `false` with no error: the caller can try again later.
	//
	// If, however, the lock cannot be acquired due to a fatal
	// error, this call should return `false` and the error.
	//
	// It is important to note that the implementation should
	// make an effort to distinguish error conditions deemed
	// fatal (and therefore trying again would invariably result
	// in the same error) and those deemed transient, in which
	// case no error should be returned to indicate that the
	// caller could try later.
	//
	// Implementations that do not support exclusive locks should
	// return `false` and `ErrNotSupported`.
	GetExclusiveLock() (bool, error)

	// Init can be used to initialize the store with server's information.
	Init(info *spb.ServerInfo) error

	// Name returns the name type of this store (e.g: MEMORY, FILESTORE, etc...).
	Name() string

	// Recover returns the recovered state.
	// Implementations that do not persist state and therefore cannot
	// recover from a previous run MUST return nil, not an error.
	// However, an error must be returned for implementations that are
	// attempting to recover the state but fail to do so.
	Recover() (*RecoveredState, error)

	// SetLimits sets limits for this store. The action is not expected
	// to be retroactive.
	// The store implementation should make a deep copy as to not change
	// the content of the structure passed by the caller.
	// This call may return an error due to limits validation errors.
	SetLimits(limits *StoreLimits) error

	// GetChannelLimits returns the limit for this channel. If the channel
	// does not exist, returns nil.
	GetChannelLimits(name string) *ChannelLimits

	// CreateChannel creates a Channel.
	// Implementations should return ErrAlreadyExists if the channel was
	// already created.
	// Limits defined for this channel in StoreLimits.PeChannel map, if present,
	// will apply. Otherwise, the global limits in StoreLimits will apply.
	CreateChannel(channel string) (*Channel, error)

	// DeleteChannel deletes a Channel.
	// Implementations should make sure that if no error is returned, the
	// channel would not be recovered after a restart, unless CreateChannel()
	// with the same channel is invoked.
	// If processing is expecting to be time consuming, work should be done
	// in the background as long as the above condition is guaranteed.
	// It is also acceptable for an implementation to have CreateChannel()
	// return an error if background deletion is still happening for a
	// channel of the same name.
	DeleteChannel(channel string) error

	// AddClient stores information about the client identified by `clientID`.
	AddClient(info *spb.ClientInfo) (*Client, error)

	// DeleteClient removes the client identified by `clientID` from the store.
	DeleteClient(clientID string) error

	// Close closes this store (including all MsgStore and SubStore).
	// If an exclusive lock was acquired, the lock shall be released.
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

	// UpdateSub updates a given subscription represented by SubState.
	UpdateSub(*spb.SubState) error

	// DeleteSub invalidates the subscription 'subid'.
	DeleteSub(subid uint64) error

	// AddSeqPending adds the given message 'seqno' to the subscription 'subid'.
	AddSeqPending(subid, seqno uint64) error

	// AckSeqPending records that the given message 'seqno' has been acknowledged
	// by the subscription 'subid'.
	AckSeqPending(subid, seqno uint64) error

	// Flush is for stores that may buffer operations and need them to be persisted.
	Flush() error

	// Close closes the subscriptions store.
	Close() error
}

// MsgStore is the interface for storage of Messages on a given channel.
type MsgStore interface {
	// State returns some statistics related to this store.
	State() (numMessages int, byteSize uint64, err error)

	// Store stores a message and returns the message sequence.
	Store(msg *pb.MsgProto) (uint64, error)

	// Lookup returns the stored message with given sequence number.
	Lookup(seq uint64) (*pb.MsgProto, error)

	// FirstSequence returns sequence for first message stored, 0 if no
	// message is stored.
	FirstSequence() (uint64, error)

	// LastSequence returns sequence for last message stored, 0 if no
	// message is stored.
	LastSequence() (uint64, error)

	// FirstAndLastSequence returns sequences for the first and last messages stored,
	// 0 if no message is stored.
	FirstAndLastSequence() (uint64, uint64, error)

	// GetSequenceFromTimestamp returns the sequence of the first message whose
	// timestamp is greater or equal to given timestamp.
	GetSequenceFromTimestamp(timestamp int64) (uint64, error)

	// FirstMsg returns the first message stored.
	FirstMsg() (*pb.MsgProto, error)

	// LastMsg returns the last message stored.
	LastMsg() (*pb.MsgProto, error)

	// Flush is for stores that may buffer operations and need them to be persisted.
	Flush() error

	// Empty removes all messages from the store
	Empty() error

	// Close closes the store.
	Close() error
}
