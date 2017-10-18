// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats-streaming/pb"

	"github.com/nats-io/nats-streaming-server/spb"
)

const batchSize = 200

var (
	fragmentPool    = &sync.Pool{New: func() interface{} { return &spb.RaftSnapshotFragment{} }}
	batchPool       = &sync.Pool{New: func() interface{} { return &spb.Batch{} }}
	subSnapshotPool = &sync.Pool{New: func() interface{} { return &spb.SubSnapshot{} }}
)

// channelSnapshot implements the raft.FSMSnapshot interface.
type channelSnapshot struct {
	*channel
}

func newChannelSnapshot(c *channel) raft.FSMSnapshot {
	return &channelSnapshot{channel: c}
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (c *channelSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer func() {
		if err != nil {
			sink.Cancel()
		}
	}()

	if err := c.snapshotMessages(sink); err != nil {
		return err
	}

	if err := c.snapshotSubscriptions(sink); err != nil {
		return err
	}

	if err := c.snapshotAcks(sink); err != nil {
		return err
	}

	return sink.Close()
}

// Release is a no-op.
func (c *channelSnapshot) Release() {}

func (c *channelSnapshot) snapshotMessages(sink raft.SnapshotSink) error {
	// TODO: this is very expensive and might repeatedly fail if messages are
	// constantly being truncated. Is there a way we can optimize this, e.g.
	// handling Restore() out-of-band from Raft?

	first, last, err := c.store.Msgs.FirstAndLastSequence()
	if err != nil {
		return err
	}

	var (
		buf   [4]byte
		batch = batchPool.Get().(*spb.Batch)
	)
	batch.Messages = make([]*pb.MsgProto, 0, batchSize)

	for seq := first; seq <= last; seq++ {
		msg, err := c.store.Msgs.Lookup(seq)
		if err != nil {
			batchPool.Put(batch)
			return err
		}
		// If msg is nil, channel truncation has occurred while snapshotting.
		if msg == nil {
			// Channel truncation has occurred while snapshotting.
			batchPool.Put(batch)
			return fmt.Errorf("channel %q was truncated while snapshotting", c.name)
		}

		// Previous batch is full, ship it.
		if len(batch.Messages) == batchSize {
			fragment := newFragment()
			fragment.FragmentType = spb.RaftSnapshotFragment_Messages
			fragment.MessageBatch = batch
			if err := writeFragment(sink, fragment, buf); err != nil {
				return err
			}

			// Create a new batch.
			batch = batchPool.Get().(*spb.Batch)
			batch.Messages = make([]*pb.MsgProto, 0, batchSize)
		}

		batch.Messages = append(batch.Messages, msg)
	}

	// Ship any partial batch.
	if len(batch.Messages) > 0 {
		fragment := newFragment()
		fragment.FragmentType = spb.RaftSnapshotFragment_Messages
		fragment.MessageBatch = batch
		if err := writeFragment(sink, fragment, buf); err != nil {
			return err
		}
	}

	return nil
}

func (c *channelSnapshot) snapshotSubscriptions(sink raft.SnapshotSink) error {
	var buf [4]byte
	for _, sub := range c.ss.getAllSubs() {
		fragment := newFragment()
		fragment.FragmentType = spb.RaftSnapshotFragment_Subscription
		subSnap := subSnapshotPool.Get().(*spb.SubSnapshot)
		subSnap.Subject = sub.subject
		subSnap.State = &sub.SubState
		fragment.Sub = subSnap
		if err := writeFragment(sink, fragment, buf); err != nil {
			return err
		}
	}
	return nil
}

func (c *channelSnapshot) snapshotAcks(sink raft.SnapshotSink) error {
	// TODO
	return nil
}

// writeFragment writes the marshalled RaftSnapshotFragment to the
// SnapshotSink. The fragment should not be used after this is called.
func writeFragment(sink raft.SnapshotSink, fragment *spb.RaftSnapshotFragment, sizeBuf [4]byte) error {
	data, err := fragment.Marshal()
	if fragment.MessageBatch != nil {
		batchPool.Put(fragment.MessageBatch)
	}
	if fragment.Sub != nil {
		subSnapshotPool.Put(fragment.Sub)
	}
	fragmentPool.Put(fragment)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(len(data)))
	_, err = sink.Write(sizeBuf[:])
	if err != nil {
		return err
	}
	_, err = sink.Write(data)
	return err
}

func newFragment() *spb.RaftSnapshotFragment {
	fragment := fragmentPool.Get().(*spb.RaftSnapshotFragment)
	fragment.MessageBatch = nil
	return fragment
}

// restoreFromSnapshot restores a channel from a snapshot. This is not called
// concurrently with any other Raft commands.
func (c *channel) restoreFromSnapshot(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	// TODO: this needs to be fully implemented to support restoring the
	// entire Raft log, not just channel messages.
	// TODO: restore acks

	// Drop all existing subs. These will be restored from the snapshot.
	for _, sub := range c.ss.getAllSubs() {
		c.stan.unsubscribe(c, sub.ClientID, sub, true)
	}

	sizeBuf := make([]byte, 4)
	for {
		// Read the fragment size.
		if _, err := io.ReadFull(snapshot, sizeBuf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Read the fragment.
		size := binary.BigEndian.Uint32(sizeBuf)
		buf := make([]byte, size)
		if _, err := io.ReadFull(snapshot, buf); err != nil {
			return err
		}

		// Unmarshal the fragment.
		fragment := newFragment()
		if err := fragment.Unmarshal(buf); err != nil {
			return err
		}

		// Apply the fragment.
		switch fragment.FragmentType {
		case spb.RaftSnapshotFragment_Messages:
			// Channel messages.
			for _, msg := range fragment.MessageBatch.Messages {
				if _, err := c.store.Msgs.Store(msg); err != nil {
					batchPool.Put(fragment.MessageBatch)
					fragmentPool.Put(fragment)
					return err
				}
			}
			batchPool.Put(fragment.MessageBatch)
			fragmentPool.Put(fragment)
		case spb.RaftSnapshotFragment_Subscription:
			// Channel subscription.
			sub := &subState{
				SubState:    *fragment.Sub.State,
				subject:     fragment.Sub.Subject,
				ackWait:     computeAckWait(fragment.Sub.State.AckWaitInSecs),
				acksPending: make(map[uint64]int64),
				store:       c.store.Subs,
			}
			if err := c.stan.addSubscription(c.ss, sub); err != nil {
				subSnapshotPool.Put(fragment.Sub)
				fragmentPool.Put(fragment)
				return err
			}
			subSnapshotPool.Put(fragment.Sub)
			fragmentPool.Put(fragment)
		default:
			panic(fmt.Sprintf("unknown snapshot fragment type %s", fragment.FragmentType))
		}
	}
	return c.store.Msgs.Flush()
}
