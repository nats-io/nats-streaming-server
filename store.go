// Copyright 2016 Apcera Inc. All rights reserved.

package stan

import (
	"time"
)

type MsgStore interface {
	// Limits
	SetLimits(maxMessages, maxSize int, maxAge time.Duration) error

	// Current State
	State() (numMessages, byteSize int, oldest time.Duration, err error)

	// Basic Storage and Retrieval
	Store(m *MsgProto, optSerial []byte) error
	Retrieve(seq uint64) (*MsgProto, error)
	RetrieveN(seq []uint64) ([]*MsgProto, error)
}

type SubscriptionStore interface {
	Store(sub *subState) error
	Load() (subs []*subState, err error)
	Size() int
	Compact() error
}
