// Copyright 2017-2018 The NATS Authors
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

package server

import (
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

// serverSnapshot implements the raft.FSMSnapshot interface by snapshotting
// StanServer state.
type serverSnapshot struct {
	*StanServer
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (r *raftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &serverSnapshot{r.server}, nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *serverSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer func() {
		if err != nil {
			sink.Cancel()
		}
	}()

	snap := &spb.RaftSnapshot{}

	s.snapshotClients(snap, sink)

	if err := s.snapshotChannels(snap); err != nil {
		return err
	}

	var b []byte
	for i := 0; i < 2; i++ {
		b, err = snap.Marshal()
		if err != nil {
			return err
		}
		// Raft assumes that the follower will restore the snapshot from the leader using
		// a timeout that is equal to:
		// the transport timeout (we provide 2*time.Second) * (snapshot size / TimeoutScale).
		// We can't provide an infinite timeout to the nats-log transport otherwise some
		// Raft operations will block forever.
		// However, we persist only the first/last sequence for a channel snapshot, however,
		// the follower will request the leader to send all those messages as part of the
		// restore. Since the snapshot size may be small compared to the amout of messages
		// to recover, the timeout may be too small.
		// To trick the system, we first set the transport's TimeoutScale to 1 (the default
		// is 256KB). Then, if we want an overall timeout of 1 hour (3600 seconds), we need
		// the size to be at least 1800 bytes. If it is less than that, then we add some
		// padding to the snapshot.
		if len(b) < 1800 {
			snap.Padding = make([]byte, 1800-len(b))
			continue
		}
		break
	}

	var sizeBuf [4]byte
	util.ByteOrder.PutUint32(sizeBuf[:], uint32(len(b)))
	if _, err := sink.Write(sizeBuf[:]); err != nil {
		return err
	}
	if _, err := sink.Write(b); err != nil {
		return err
	}

	return sink.Close()
}

func (s *serverSnapshot) snapshotClients(snap *spb.RaftSnapshot, sink raft.SnapshotSink) {
	s.clients.RLock()
	defer s.clients.RUnlock()

	numClients := len(s.clients.clients)
	if numClients == 0 {
		return
	}

	snap.Clients = make([]*spb.ClientInfo, numClients)
	i := 0
	for _, client := range s.clients.clients {
		// Make a copy
		info := client.info.ClientInfo
		snap.Clients[i] = &info
		i++
	}
}

func (s *serverSnapshot) snapshotChannels(snap *spb.RaftSnapshot) error {
	s.channels.RLock()
	defer s.channels.RUnlock()

	numChannels := len(s.channels.channels)
	if numChannels == 0 {
		return nil
	}

	snapshotASub := func(sub *subState) *spb.SubscriptionSnapshot {
		// Make a copy
		state := sub.SubState
		snapSub := &spb.SubscriptionSnapshot{State: &state}
		if len(sub.acksPending) > 0 {
			snapSub.AcksPending = make([]uint64, len(sub.acksPending))
			i := 0
			for seq := range sub.acksPending {
				snapSub.AcksPending[i] = seq
				i++
			}
		}
		return snapSub
	}

	snap.Channels = make([]*spb.ChannelSnapshot, numChannels)
	numChannel := 0
	for _, c := range s.channels.channels {
		first, last, err := c.store.Msgs.FirstAndLastSequence()
		if err != nil {
			return err
		}
		snapChannel := &spb.ChannelSnapshot{
			Channel: c.name,
			First:   first,
			Last:    last,
		}
		c.ss.RLock()

		// Start with count of all plain subs...
		snapSubs := make([]*spb.SubscriptionSnapshot, len(c.ss.psubs))
		i := 0
		for _, sub := range c.ss.psubs {
			sub.RLock()
			snapSubs[i] = snapshotASub(sub)
			sub.RUnlock()
			i++
		}

		// Now need to close durables
		for _, dur := range c.ss.durables {
			dur.RLock()
			if dur.IsClosed {
				// We need to persist a SubState with a ClientID
				// so that we can reconstruct the durable key
				// on recovery. So set to the saved value here
				// and then clear it after that.
				dur.ClientID = dur.savedClientID
				snapSubs = append(snapSubs, snapshotASub(dur))
				dur.ClientID = ""
			}
			dur.RUnlock()
		}

		// Snapshot the queue subscriptions
		for _, qsub := range c.ss.qsubs {
			qsub.RLock()
			for _, sub := range qsub.subs {
				sub.RLock()
				snapSubs = append(snapSubs, snapshotASub(sub))
				sub.RUnlock()
			}
			// If all members of a durable queue group left the group,
			// we need to persist the "shadow" queue member.
			if qsub.shadow != nil {
				qsub.shadow.RLock()
				snapSubs = append(snapSubs, snapshotASub(qsub.shadow))
				qsub.shadow.RUnlock()
			}
			qsub.RUnlock()
		}
		if len(snapSubs) > 0 {
			snapChannel.Subscriptions = snapSubs
		}

		c.ss.RUnlock()
		snap.Channels[numChannel] = snapChannel
		numChannel++
	}

	return nil
}

// Release is a no-op.
func (s *serverSnapshot) Release() {}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (r *raftFSM) Restore(snapshot io.ReadCloser) (retErr error) {
	defer snapshot.Close()

	r.Lock()
	defer r.Unlock()

	// This function may be invoked directly from raft.NewRaft() when
	// the node is initialized and if there were exisiting local snapshots,
	// or later, when catching up with a leader. We behave differently
	// depending on the situation. So we need to know if we are called
	// from NewRaft().
	//
	// To do so, we first look at the number of local snapshots before
	// calling NewRaft(). If the number is > 0, it means that Raft will
	// call us within NewRaft(). Raft will restore the latest snapshot
	// first, and only in case of Restore() returning an error will move
	// to the next (earliest) one. When there are none and Restore() still
	// returns an error raft.NewRaft() will return an error.
	//
	// So on error we decrement the number of snapshots, on success we set
	// it to 0. This means that next time Restore() is invoked, we know it
	// is restoring from a leader, not from the local snapshots.
	inNewRaftCall := r.snapshotsOnInit != 0
	if inNewRaftCall {
		defer func() {
			if retErr != nil {
				r.snapshotsOnInit--
			} else {
				r.snapshotsOnInit = 0
			}
		}()
	}
	s := r.server

	// We need to drop current state. The server will recover from snapshot
	// and all newer Raft entry logs (basically the entire state is being
	// reconstructed from this point on).
	for _, c := range s.channels.getAll() {
		for _, sub := range c.ss.getAllSubs() {
			sub.RLock()
			clientID := sub.ClientID
			sub.RUnlock()
			if err := s.unsubscribeSub(c, clientID, "unsub", sub, false); err != nil {
				return err
			}
		}
	}
	for clientID := range s.clients.getClients() {
		if _, err := s.clients.unregister(clientID); err != nil {
			return err
		}
	}

	sizeBuf := make([]byte, 4)
	// Read the snapshot size.
	if _, err := io.ReadFull(snapshot, sizeBuf); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	// Read the snapshot.
	size := util.ByteOrder.Uint32(sizeBuf)
	buf := make([]byte, size)
	if _, err := io.ReadFull(snapshot, buf); err != nil {
		return err
	}

	serverSnap := &spb.RaftSnapshot{}
	if err := serverSnap.Unmarshal(buf); err != nil {
		panic(err)
	}
	if err := r.restoreClientsFromSnapshot(serverSnap); err != nil {
		return err
	}
	return r.restoreChannelsFromSnapshot(serverSnap, inNewRaftCall)
}

func (r *raftFSM) restoreClientsFromSnapshot(serverSnap *spb.RaftSnapshot) error {
	s := r.server
	for _, sc := range serverSnap.Clients {
		if _, err := s.clients.register(sc); err != nil {
			return err
		}
	}
	return nil
}

func (r *raftFSM) restoreChannelsFromSnapshot(serverSnap *spb.RaftSnapshot, inNewRaftCall bool) error {
	s := r.server

	var channelsBeforeRestore map[string]*channel
	if !inNewRaftCall {
		channelsBeforeRestore = s.channels.getAll()
	}
	for _, sc := range serverSnap.Channels {
		c, err := s.lookupOrCreateChannel(sc.Channel)
		if err != nil {
			return err
		}
		// Do not restore messages from snapshot if the server
		// just started and is recovering from its own snapshot.
		if !inNewRaftCall {
			if err := r.restoreMsgsFromSnapshot(c, sc.First, sc.Last); err != nil {
				return err
			}
			delete(channelsBeforeRestore, sc.Channel)
		}
		for _, ss := range sc.Subscriptions {
			s.recoverOneSub(c, ss.State, nil, ss.AcksPending)
		}
	}
	if !inNewRaftCall {
		// Now delete channels that we had before the restore.
		// This is possible if channels have been deleted while
		// this node was not running and snapshot occurred. The
		// channels would not be in the snapshot, so we can remove
		// them now.
		for name := range channelsBeforeRestore {
			s.processDeleteChannel(name)
		}
	}
	return nil
}

func (r *raftFSM) restoreMsgsFromSnapshot(c *channel, first, last uint64) error {
	storeFirst, storeLast, err := c.store.Msgs.FirstAndLastSequence()
	if err != nil {
		return err
	}
	// If the leader's first sequence is more than our lastSequence+1,
	// then we need to empty the store. We don't want to have gaps.
	// Same if our first is strictly greater than the leader, or our
	// last sequence is more than the leader
	if first > storeLast+1 || storeFirst > first || storeLast > last {
		if err := c.store.Msgs.Empty(); err != nil {
			return err
		}
	} else if storeLast == last {
		// We may have a message with lower sequence than the leader,
		// but our last sequence is the same, so nothing to do.
		return nil
	} else if storeLast > 0 {
		// first is less than what we already have, just started
		// at our next sequence.
		first = storeLast + 1
	}
	inbox := nats.NewInbox()
	sub, err := c.stan.ncsr.SubscribeSync(inbox)
	if err != nil {
		return err
	}
	sub.SetPendingLimits(-1, -1)
	defer sub.Unsubscribe()

	subject := fmt.Sprintf("%s.%s.%s", defaultSnapshotPrefix, c.stan.info.ClusterID, c.name)

	var (
		reqBuf    [16]byte
		reqNext   = first
		reqStart  = first
		reqEnd    uint64
		batch     = uint64(100)
		halfBatch = batch / 2
	)
	for seq := first; seq <= last; seq++ {
		if seq == reqNext {
			reqEnd = reqStart + batch
			if reqEnd > last {
				reqEnd = last
			}
			util.ByteOrder.PutUint64(reqBuf[:8], reqStart)
			util.ByteOrder.PutUint64(reqBuf[8:16], reqEnd)
			if err := c.stan.ncsr.PublishRequest(subject, inbox, reqBuf[:16]); err != nil {
				return err
			}
			if reqEnd != last {
				reqNext = reqStart + halfBatch
				reqStart = reqEnd + 1
			}
		}
		resp, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			return err
		}
		// It is possible that the leader does not have this message because of
		// channel limits. If resp.Data is empty, we are in this situation and
		// we are done recovering snapshot.
		if len(resp.Data) == 0 {
			break
		}
		msg := &pb.MsgProto{}
		if err := msg.Unmarshal(resp.Data); err != nil {
			panic(err)
		}
		if _, err := c.store.Msgs.Store(msg); err != nil {
			return err
		}
		select {
		case <-r.server.shutdownCh:
			return fmt.Errorf("server shutting down")
		default:
		}
	}
	return c.store.Msgs.Flush()
}
