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

const snapshotBatchSize = 200

var (
	fragmentPool       = &sync.Pool{New: func() interface{} { return &spb.RaftSnapshotFragment{} }}
	batchPool          = &sync.Pool{New: func() interface{} { return &spb.Batch{} }}
	msgsSnapshotPool   = &sync.Pool{New: func() interface{} { return &spb.MessagesSnapshot{} }}
	subSnapshotPool    = &sync.Pool{New: func() interface{} { return &spb.SubSnapshot{} }}
	clientSnapshotPool = &sync.Pool{New: func() interface{} { return &spb.ClientSnapshot{} }}
)

// serverSnapshot implements the raft.FSMSnapshot interface by snapshotting
// StanServer state.
type serverSnapshot struct {
	*StanServer
}

func newServerSnapshot(s *StanServer) raft.FSMSnapshot {
	return &serverSnapshot{s}
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *serverSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer func() {
		if err != nil {
			sink.Cancel()
		}
	}()

	// Snapshot clients. We are only snapshotting the client metadata here
	// (heartbeat inbox and client ID). Subscriptions are replicated through
	// the channel Raft groups, so upon snapshot restore, the server will
	// load any of the client's subscriptions from the store.
	var buf [4]byte
	for _, client := range s.clients.getClients() {
		fragment := newFragment()
		fragment.FragmentType = spb.RaftSnapshotFragment_Conn
		clientSnap := clientSnapshotPool.Get().(*spb.ClientSnapshot)
		client.RLock()
		clientSnap.Info = &client.info.ClientInfo
		client.RUnlock()
		fragment.Client = clientSnap
		if err := writeFragment(sink, fragment, buf); err != nil {
			return err
		}
	}

	return sink.Close()
}

// Release is a no-op.
func (s *serverSnapshot) Release() {}

// restoreFromSnapshot restores a server from a snapshot. This is not called
// concurrently with any other Raft commands.
func (s *StanServer) restoreFromSnapshot(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	// Drop all existing clients. These will be restored from the snapshot.
	// However, keep a copy so we can recover subs.
	oldClients := s.clients.getClients()
	for _, client := range oldClients {
		if _, err := s.clients.unregister(client.info.ID); err != nil {
			return err
		}
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
		case spb.RaftSnapshotFragment_Conn:
			// Client.
			err := s.restoreClientFromSnapshot(fragment.Client, oldClients)
			clientSnapshotPool.Put(fragment.Client)
			fragmentPool.Put(fragment)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unknown snapshot fragment type %s", fragment.FragmentType))
		}
	}
	return nil
}

func (s *StanServer) restoreClientFromSnapshot(clientSnapshot *spb.ClientSnapshot, oldClients map[string]*client) error {
	client, _, err := s.clients.register(clientSnapshot.Info.ID, clientSnapshot.Info.HbInbox)
	if err != nil {
		return err
	}

	// Restore any subscriptions.
	// QUESTION: are there potential race conditions due to how subscriptions
	// and clients are handled on different Raft groups?
	oldClient, ok := oldClients[client.info.ID]
	if ok {
		for _, sub := range oldClient.getSubsCopy() {
			s.clients.addSub(client.info.ID, sub)
		}
	}

	return nil
}

// channelSnapshot implements the raft.FSMSnapshot interface by snapshotting
// channel state.
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

	return sink.Close()
}

// Release is a no-op.
func (c *channelSnapshot) Release() {}

func (c *channelSnapshot) snapshotMessages(sink raft.SnapshotSink) error {
	// TODO: this is very expensive and might repeatedly fail if messages are
	// constantly being truncated. Is there a way we can optimize this, e.g.
	// handling Restore() out-of-band from Raft?
	// See issue #410.

	first, last, err := c.store.Msgs.FirstAndLastSequence()
	if err != nil {
		return err
	}

	var (
		buf              [4]byte
		batch            = batchPool.Get().(*spb.Batch)
		writeMsgFragment = func(b *spb.Batch) error {
			fragment := newFragment()
			fragment.FragmentType = spb.RaftSnapshotFragment_Messages
			msgsSnap := msgsSnapshotPool.Get().(*spb.MessagesSnapshot)
			msgsSnap.MessageBatch = b
			fragment.Msgs = msgsSnap
			return writeFragment(sink, fragment, buf)
		}
	)
	batch.Messages = make([]*pb.MsgProto, 0, snapshotBatchSize)

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
		if len(batch.Messages) == snapshotBatchSize {
			if err := writeMsgFragment(batch); err != nil {
				return err
			}

			// Create a new batch.
			batch = batchPool.Get().(*spb.Batch)
			batch.Messages = make([]*pb.MsgProto, 0, snapshotBatchSize)
		}

		batch.Messages = append(batch.Messages, msg)
	}

	// Ship any partial batch.
	if len(batch.Messages) > 0 {
		if err := writeMsgFragment(batch); err != nil {
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
		sub.Lock()
		subSnap.Subject = sub.subject
		subSnap.State = &sub.SubState
		subSnap.AcksPending = make([]uint64, len(sub.acksPending))
		i := 0
		for seq, _ := range sub.acksPending {
			subSnap.AcksPending[i] = seq
			i++
		}
		sub.Unlock()
		fragment.Sub = subSnap
		if err := writeFragment(sink, fragment, buf); err != nil {
			return err
		}
	}
	return nil
}

// writeFragment writes the marshaled RaftSnapshotFragment to the
// SnapshotSink. The fragment should not be used after this is called.
func writeFragment(sink raft.SnapshotSink, fragment *spb.RaftSnapshotFragment, sizeBuf [4]byte) error {
	data, err := fragment.Marshal()
	if fragment.Msgs != nil {
		batchPool.Put(fragment.Msgs.MessageBatch)
		msgsSnapshotPool.Put(fragment.Msgs)
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
	fragment.Msgs = nil
	fragment.Sub = nil
	return fragment
}

// restoreFromSnapshot restores a channel from a snapshot. This is not called
// concurrently with any other Raft commands.
func (c *channel) restoreFromSnapshot(snapshot io.ReadCloser) error {
	defer snapshot.Close()

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
			err := c.restoreMsgsFromSnapshot(fragment.Msgs)
			batchPool.Put(fragment.Msgs.MessageBatch)
			msgsSnapshotPool.Put(fragment.Msgs)
			fragmentPool.Put(fragment)
			if err != nil {
				return err
			}
		case spb.RaftSnapshotFragment_Subscription:
			// Channel subscription.
			err := c.restoreSubFromSnapshot(fragment.Sub)
			subSnapshotPool.Put(fragment.Sub)
			fragmentPool.Put(fragment)
			if err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("unknown snapshot fragment type %s", fragment.FragmentType))
		}
	}
	return c.store.Msgs.Flush()
}

func (c *channel) restoreMsgsFromSnapshot(msgsSnapshot *spb.MessagesSnapshot) error {
	for _, msg := range msgsSnapshot.MessageBatch.Messages {
		if _, err := c.store.Msgs.Store(msg); err != nil {
			return err
		}
	}
	return nil
}

func (c *channel) restoreSubFromSnapshot(subSnapshot *spb.SubSnapshot) error {
	acksPending := make(map[uint64]int64, len(subSnapshot.AcksPending))
	for _, seq := range subSnapshot.AcksPending {
		// Set 0 for expiration time. This will be computed
		// when the follower becomes leader and attempts to
		// redeliver messages.
		acksPending[seq] = 0
	}
	sub := &subState{
		SubState:    *subSnapshot.State,
		subject:     subSnapshot.Subject,
		ackWait:     computeAckWait(subSnapshot.State.AckWaitInSecs),
		acksPending: acksPending,
		store:       c.store.Subs,
	}
	// Store the subscription.
	if err := c.stan.addSubscription(c.ss, sub); err != nil {
		return err
	}
	// Store pending acks for the subscription.
	for seq, _ := range sub.acksPending {
		if err := sub.store.AddSeqPending(sub.ID, seq); err != nil {
			return err
		}
	}
	return nil
}
