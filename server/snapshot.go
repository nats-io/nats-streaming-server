// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"encoding/binary"
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats-streaming/pb"
)

// channelSnapshot implements the raft.FSMSnapshot interface.
type channelSnapshot struct {
	channel *channel
}

func newChannelSnapshot(c *channel) raft.FSMSnapshot {
	return &channelSnapshot{channel: c}
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (c *channelSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	// TODO: this is very expensive and might repeatedly fail if messages are
	// constantly being truncated. Is there a way we can optimize this, e.g.
	// handling Restore() out-of-band from Raft?

	defer func() {
		if err != nil {
			sink.Cancel()
		}
	}()

	var (
		first uint64
		last  uint64
		msg   *pb.MsgProto
		data  []byte
	)
	first, last, err = c.channel.store.Msgs.FirstAndLastSequence()
	if err != nil {
		return err
	}

	buf := make([]byte, 4)
	for seq := first; seq <= last; seq++ {
		msg, err = c.channel.store.Msgs.Lookup(seq)
		if err != nil {
			return
		}
		// If msg is nil, channel truncation has occurred while snapshotting.
		if msg == nil {
			// Channel truncation has occurred while snapshotting.
			return fmt.Errorf("channel %q was truncated while snapshotting", c.channel.name)
		}
		data, err = msg.Marshal()
		if err != nil {
			return
		}
		binary.BigEndian.PutUint32(buf, uint32(len(data)))
		_, err = sink.Write(buf)
		if err != nil {
			return
		}
		_, err = sink.Write(data)
		if err != nil {
			return
		}
	}
	return sink.Close()
}

// Release is a no-op.
func (c *channelSnapshot) Release() {}
