// Copyright 2016 Apcera Inc. All rights reserved.

package stan

import (
	"github.com/nats-io/stan/pb"
)

// Interface to storage for STAN servers.
type Store interface {
	// Limits per channel
	//	SetChannelLimits(limits stores.ChannelLimits) error

	// Current State, "*" returns all channels combined.
	State(channel string) (numMessages, byteSize int, err error)

	// Basic Message Storage and Lookup
	Store(channel, reply string, data []byte) (*pb.MsgProto, error)
	Lookup(channel string, seq uint64) (*pb.MsgProto, error)
}
