// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"errors"
	"time"

	"github.com/nats-io/stan/pb"
)

const (
	AllChannels = "*"
)

// MemoryStore will hold messages and subscription state in memory.
type MemoryStore struct {
	channels *channelMap
	limits   ChannelLimits
}

type ChannelLimits struct {
	MaxNumMsgs  int
	MaxMsgBytes uint64
	MaxMsgAge   time.Duration
	MaxSubs     int
}

func NewMemoryStore(limits ChannelLimits) *MemoryStore {
	ms := &MemoryStore{}
	ms.SetChannelLimits(limits)
	ms.limits = limits
	ms.channels = &channelMap{channels: make(map[string]*channelStore)}
	return ms
}

func (ms *MemoryStore) SetChannelLimits(limits ChannelLimits) error {
	ms.limits = limits
	return nil
}

func (ms *MemoryStore) State(channel string) (numMessages, byteSize int, err error) {
	return 0, 0, errors.New("not implemented")
}

func (ms *MemoryStore) Store(channel, reply string, data []byte) (*pb.MsgProto, error) {
	return nil, errors.New("not implemented")
}

func (ms *MemoryStore) Lookup(channel string, seq uint64) (*pb.MsgProto, error) {
	return nil, errors.New("not implemented")
}
