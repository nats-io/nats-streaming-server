// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"sync"
	"time"

	"github.com/nats-io/nats"
	"github.com/nats-io/stan/pb"
)

type channelMap struct {
	sync.RWMutex
	channels map[string]*channelStore
}

// channelStore holds our known state of all messages and subscribers for a given channel/subject.
type channelStore struct {
	subs *subStore // All subscribers
	msgs *msgStore // All messages
}

// subStore holds all known state for all subscriptions
type subStore struct {
	sync.RWMutex
	psubs    []*subState            // plain subscribers
	qsubs    map[string]*queueState // queue subscribers
	durables map[string]*subState   // durables lookup
	acks     map[string]*subState   // ack inbox lookup
}

// Per channel/subject message store
type msgStore struct {
	sync.RWMutex
	subject string // Can't be wildcard
	first   uint64
	last    uint64
	msgs    map[uint64]*pb.MsgProto
}

// Holds all queue subsribers for a subject/group and
// tracks lastSent for the group.
type queueState struct {
	sync.RWMutex
	lastSent uint64
	subs     []*subState
	stalled  bool
}

// Holds Subscription state
// FIXME(dlc) - Use embedded proto
type subState struct {
	sync.RWMutex
	clientID      string
	subject       string
	qgroup        string
	inbox         string
	ackInbox      string
	durableName   string
	qstate        *queueState
	lastSent      uint64
	ackWaitInSecs time.Duration
	ackTimer      *time.Timer
	ackSub        *nats.Subscription
	acksPending   map[uint64]*pb.MsgProto
	maxInFlight   int
	stalled       bool
}
