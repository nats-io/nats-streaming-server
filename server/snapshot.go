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
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go/pb"
)

const (
	defaultRestoreMsgsAttempts             = 10
	defaultRestoreMsgsRcvTimeout           = 2 * time.Second
	defaultRestoreMsgsSleepBetweenAttempts = time.Second
)

var (
	restoreMsgsAttempts             = defaultRestoreMsgsAttempts
	restoreMsgsRcvTimeout           = defaultRestoreMsgsRcvTimeout
	restoreMsgsSleepBetweenAttempts = defaultRestoreMsgsSleepBetweenAttempts
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

	// We don't want Persit() and Apply() to be invoked concurrently,
	// so use common lock.
	s.raft.fsm.Lock()
	defer s.raft.fsm.Unlock()

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
		// Flush msg and sub stores before persisting snapshot
		if err := c.store.Subs.Flush(); err != nil {
			return err
		}
		if err := c.store.Msgs.Flush(); err != nil {
			return err
		}
		first, last, err := s.getChannelFirstAndlLastSeq(c)
		if err != nil {
			return err
		}
		c.ss.RLock()
		snapChannel := &spb.ChannelSnapshot{
			Channel:   c.name,
			First:     first,
			Last:      last,
			NextSubID: c.nextSubID,
		}

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
	s := r.server
	defer snapshot.Close()

	r.Lock()
	defer r.Unlock()

	shouldSnapshot := false
	defer func() {
		if retErr == nil && shouldSnapshot {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				// s.raft.Snapshot() is actually s.raft.Raft.Snapshot()
				// (our raft structure embeds Raft). But it is set after
				// the call to NewRaft() - which invokes this function on
				// startup. However, this runs under the server's lock, so
				// simply grab the lock to ensure that Snapshot() will be
				// able to execute.
				// Also don't use startGoRoutine() here since it needs the
				// server lock, which we already have.
				s.mu.Lock()
				s.raft.Snapshot().Error()
				s.mu.Unlock()
			}()
		}
	}()

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
	} else {
		s.log.Noticef("restoring from snapshot")
		defer func() {
			if retErr == nil {
				s.log.Noticef("done restoring from snapshot")
			} else {
				s.log.Errorf("error restoring from snapshot: %v", retErr)
			}
		}()
	}

	// We need to drop current state. The server will recover from snapshot
	// and all newer Raft entry logs (basically the entire state is being
	// reconstructed from this point on).
	for _, c := range s.channels.getAll() {
		for _, sub := range c.ss.getAllSubs() {
			sub.RLock()
			clientID := sub.ClientID
			sub.RUnlock()
			if err := s.unsubscribeSub(c, clientID, "unsub", sub, false, false); err != nil {
				return err
			}
		}
		c.store.Subs.Flush()
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
	var err error
	shouldSnapshot, err = r.restoreChannelsFromSnapshot(serverSnap, inNewRaftCall)
	return err
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

func (r *raftFSM) restoreChannelsFromSnapshot(serverSnap *spb.RaftSnapshot, inNewRaftCall bool) (bool, error) {
	s := r.server

	shouldSnapshot := false

	var channelsBeforeRestore map[string]*channel
	if !inNewRaftCall {
		channelsBeforeRestore = s.channels.getAll()
	}
	for _, sc := range serverSnap.Channels {
		c, err := s.lookupOrCreateChannel(sc.Channel)
		if err != nil {
			return false, err
		}
		// Keep track of first/last sequence in this channel's snapshot.
		// It can help to detect that all messages in the leader have expired
		// and this is what we should report/use as our store first/last.
		atomic.StoreUint64(&c.firstSeq, sc.First)

		// Even on startup (when inNewRaftCall==true), we need to call
		// restoreMsgsFromSnapshot() to make sure our store is consistent.
		// If we skip it (like we used to), then it is possible that this
		// node misses some messages from the snapshot and will then start
		// to apply remaining logs. It could even then become leader in
		// the process with missing messages. So if we need to restore some
		// messages and fail, keep trying until a leader is avail and
		// able to serve this node.
		ok := false
		for i := 0; i < restoreMsgsAttempts; i++ {
			if err := r.restoreMsgsFromSnapshot(c, sc.First, sc.Last, false); err != nil {
				s.log.Errorf("channel %q - unable to restore messages, can't start until leader is available",
					sc.Channel)
				time.Sleep(restoreMsgsSleepBetweenAttempts)
			} else {
				ok = true
				break
			}
		}
		if !ok {
			err = fmt.Errorf("channel %q - unable to restore messages, aborting", sc.Channel)
			s.log.Fatalf(err.Error())
			// In tests, we use a "dummy" logger, so process will not exit
			// (and we would not want that anyway), so make sure we return
			// an error.
			return false, err
		}
		if !inNewRaftCall {
			delete(channelsBeforeRestore, sc.Channel)
		}
		// restoreMsgsFromSnapshot may have updated c.firstSeq. If that is the
		// case it means that when restoring, we realized that some messages
		// have expired/removed. If so, try to do a snapshot once we have
		// finished with restoring.
		shouldSnapshot = atomic.LoadUint64(&c.firstSeq) != sc.First

		// Ensure we check store last sequence before storing msgs in Apply().
		c.lSeqChecked = false

		for _, ss := range sc.Subscriptions {
			s.recoverOneSub(c, ss.State, nil, ss.AcksPending)
			c.ss.Lock()
			if ss.State.ID >= c.nextSubID {
				c.nextSubID = ss.State.ID + 1
			}
			c.ss.Unlock()
		}
		c.ss.Lock()
		if sc.NextSubID > c.nextSubID {
			c.nextSubID = sc.NextSubID
		}
		c.ss.Unlock()
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
	return shouldSnapshot, nil
}

func (r *raftFSM) restoreMsgsFromSnapshot(c *channel, first, last uint64, fromApply bool) error {
	storeLast, err := c.store.Msgs.LastSequence()
	if err != nil {
		return err
	}

	// We are here in several situations:
	// - on startup, we may have a snapshot with a channel first/last index.
	//   It does not mean that there should not be more messages in the channel,
	//   and there may be raft logs to apply following this snapshot. But
	//   we still want to make sure that our store is consistent.
	// - at runtime, we have fallen behind and the leader sent us a snapshot.
	//   we care about restoring/fetching only messages past `storeLast`+1 up
	//   to `last`
	// - in Apply(), we are storing a message and this function is invoked with
	//   `last` equal to that message sequence - 1. We need to possibly fetch
	//   messages from `storeLast`+1 up to `last`.

	// If we are invoked from Apply(), we are about to store a message
	// at sequence `last`+1, so we want to ensure that we have all messages
	// from `storeLast`+1 to `last` included. Set `first` to `storeLast`+1.
	if fromApply {
		first = storeLast + 1
	}

	// If the first sequence in the channel is past our store last sequence + 1,
	// then we need to empty our store.
	if first > storeLast+1 {
		if storeLast != 0 {
			if err := c.store.Msgs.Empty(); err != nil {
				return err
			}
		}
	} else if first <= storeLast {
		// Start to store at storeLast+1
		first = storeLast + 1
	}

	// With the above done, we now need to make sure that we don't try
	// to restore from a too old position. If `first` is smaller than
	// snapFSeq, then set the floor at that value.
	// Note that c.firstSeq is only set from a thread invoking this
	// function or within this function itself. However, it is read
	// from other threads, so use atomic operation here for consistency,
	// but not really required.
	if fseq := atomic.LoadUint64(&c.firstSeq); fseq > first {
		first = fseq
	}

	// Now check if we have anything to do. This condition will be true
	// if we try to restore from a channel where all messages have expired
	// or if we have all messages we need.
	if first > last {
		return nil
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
		stored    = false
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
		resp, err := sub.NextMsg(restoreMsgsRcvTimeout)
		if err != nil {
			return err
		}
		// It is possible that the leader does not have this message because of
		// channel limits. If resp.Data is empty, we are in this situation.
		// We need to continue to see if more recent messages are available though.
		if len(resp.Data) != 0 {
			msg := &pb.MsgProto{}
			if err := msg.Unmarshal(resp.Data); err != nil {
				panic(err)
			}
			if _, err := c.store.Msgs.Store(msg); err != nil {
				return err
			}
			stored = true
		} else {
			// Since this message is not in the leader, it means that
			// messages may have expired or be removed due to limit,
			// so we need to empty our store before storing the next
			// valid message.
			if seq == first || stored {
				if err := c.store.Msgs.Empty(); err != nil {
					return err
				}
				stored = false
			}
			atomic.StoreUint64(&c.firstSeq, seq+1)
		}
		select {
		case <-r.server.shutdownCh:
			return fmt.Errorf("server shutting down")
		default:
		}
	}
	return c.store.Msgs.Flush()
}
