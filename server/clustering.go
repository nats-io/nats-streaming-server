// Copyright 2017-2020 The NATS Authors
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
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats.go"
)

const (
	defaultJoinRaftGroupTimeout = time.Second
	defaultRaftHBTimeout        = 2 * time.Second
	defaultRaftElectionTimeout  = 2 * time.Second
	defaultRaftLeaseTimeout     = time.Second
	defaultRaftCommitTimeout    = 100 * time.Millisecond
	defaultTPortTimeout         = 10 * time.Second
)

var (
	runningInTests              bool
	joinRaftGroupTimeout        = defaultJoinRaftGroupTimeout
	testPauseAfterNewRaftCalled bool
	tportTimeout                = defaultTPortTimeout
)

func clusterSetupForTest() {
	runningInTests = true
	lazyReplicationInterval = 250 * time.Millisecond
	joinRaftGroupTimeout = 250 * time.Millisecond
	tportTimeout = 250 * time.Millisecond
}

// ClusteringOptions contains STAN Server options related to clustering.
type ClusteringOptions struct {
	Clustered    bool     // Run the server in a clustered configuration.
	NodeID       string   // ID of the node within the cluster.
	Bootstrap    bool     // Bootstrap the cluster as a seed node if there is no existing state.
	Peers        []string // List of cluster peer node IDs to bootstrap cluster state.
	RaftLogPath  string   // Path to Raft log store directory.
	LogCacheSize int      // Number of Raft log entries to cache in memory to reduce disk IO.
	LogSnapshots int      // Number of Raft log snapshots to retain.
	TrailingLogs int64    // Number of logs left after a snapshot.
	Sync         bool     // Do a file sync after every write to the Raft log and message store.
	RaftLogging  bool     // Enable logging of Raft library (disabled by default since really verbose).

	// When a node processes a snapshot (either on startup or if falling behind) and its is
	// not in phase with the message store's state, it is required to reconcile its state
	// with the current leader. If it is unable, the node will fail to start or exit.
	// If all nodes are starting and there is no way to have a leader at this point,
	// then if this boolean is set to true, then the node will attempt to reconcile but
	// if it can't it will still proceed.
	ProceedOnRestoreFailure bool

	// These will be set to some sane defaults. Change only if experiencing raft issues.
	RaftHeartbeatTimeout time.Duration
	RaftElectionTimeout  time.Duration
	RaftLeaseTimeout     time.Duration
	RaftCommitTimeout    time.Duration
}

// raftNode is a handle to a member in a Raft consensus group.
type raftNode struct {
	leader int64
	sync.Mutex
	closed bool
	*raft.Raft
	store     *raftLog
	transport *raft.NetworkTransport
	logInput  io.WriteCloser
	joinSub   *nats.Subscription
	notifyCh  <-chan bool
	fsm       *raftFSM
}

type replicatedSub struct {
	sub *subState
	err error
}

type raftFSM struct {
	sync.Mutex
	server          *StanServer
	restoreFromInit bool
}

// shutdown attempts to stop the Raft node.
func (r *raftNode) shutdown() error {
	r.Lock()
	if r.closed {
		r.Unlock()
		return nil
	}
	r.closed = true
	r.Unlock()
	if r.transport != nil {
		if err := r.transport.Close(); err != nil {
			return err
		}
	}
	if r.Raft != nil {
		if err := r.Raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	if r.store != nil {
		if err := r.store.Close(); err != nil {
			return err
		}
	}
	if r.logInput != nil {
		if err := r.logInput.Close(); err != nil {
			return err
		}
	}
	return nil
}

// createRaftNode creates and starts a new Raft node.
func (s *StanServer) createServerRaftNode(hasStreamingState bool) error {
	var (
		name               = s.info.ClusterID
		addr               = s.getClusteringAddr(name)
		existingState, err = s.createRaftNode(name)
	)
	if err != nil {
		return err
	}
	if !existingState && hasStreamingState {
		return fmt.Errorf("streaming state was recovered but cluster log path %q is empty", s.opts.Clustering.RaftLogPath)
	}
	node := s.raft

	// Bootstrap if there is no previous state and we are starting this node as
	// a seed or a cluster configuration is provided.
	bootstrap := !existingState && (s.opts.Clustering.Bootstrap || len(s.opts.Clustering.Peers) > 0)
	if bootstrap {
		if err := s.bootstrapCluster(name, node.Raft); err != nil {
			node.shutdown()
			return err
		}
	} else if !existingState {
		// Attempt to join the cluster if we're not bootstrapping.
		req, err := (&spb.RaftJoinRequest{NodeID: s.opts.Clustering.NodeID, NodeAddr: addr}).Marshal()
		if err != nil {
			panic(err)
		}
		var (
			joined = false
			resp   = &spb.RaftJoinResponse{}
		)
		s.log.Debugf("Joining Raft group %s", name)
		// Attempt to join up to 5 times before giving up.
		for i := 0; i < 5; i++ {
			r, err := s.ncr.Request(fmt.Sprintf("%s.%s.join", defaultRaftPrefix, name), req, joinRaftGroupTimeout)
			if err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if err := resp.Unmarshal(r.Data); err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if resp.Error != "" {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			joined = true
			break
		}
		if !joined {
			node.shutdown()
			return fmt.Errorf("failed to join Raft group %s", name)
		}
	}
	if s.opts.Clustering.Bootstrap {
		// If node is started with bootstrap, regardless if state exist or not, try to
		// detect (and report) other nodes in same cluster started with bootstrap=true.
		s.wg.Add(1)
		go func() {
			s.detectBootstrapMisconfig(name)
			s.wg.Done()
		}()
	}
	return nil
}

func (s *StanServer) detectBootstrapMisconfig(name string) {
	srvID := []byte(s.serverID)
	subj := fmt.Sprintf("%s.%s.bootstrap", defaultRaftPrefix, name)
	s.ncr.Subscribe(subj, func(m *nats.Msg) {
		if m.Data != nil && m.Reply != "" {
			// Ignore message to ourself
			if string(m.Data) != s.serverID {
				s.ncr.Publish(m.Reply, srvID)
				s.log.Fatalf("Server %s was also started with -cluster_bootstrap", string(m.Data))
			}
		}
	})
	inbox := nats.NewInbox()
	s.ncr.Subscribe(inbox, func(m *nats.Msg) {
		s.log.Fatalf("Server %s was also started with -cluster_bootstrap", string(m.Data))
	})
	if err := s.ncr.Flush(); err != nil {
		s.log.Errorf("Error setting up bootstrap misconfiguration detection: %v", err)
		return
	}
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-s.shutdownCh:
			ticker.Stop()
			return
		case <-ticker.C:
			s.ncr.PublishRequest(subj, inbox, srvID)
		}
	}
}

type raftLogger struct {
	*StanServer
}

func (rl *raftLogger) Write(b []byte) (int, error) {
	if !rl.raftLogging {
		return len(b), nil
	}
	levelStart := bytes.IndexByte(b, '[')
	if levelStart != -1 {
		// RAFT has various "headers", sometimes it is "[xxxx] raft:",
		// sometimes "[xxx]  raft:" or "[xxx]  raft-net:", etc..
		// So look for the closing ']' and skip spaces to determine the offset.
		offset := levelStart + 1 + bytes.IndexByte(b[levelStart+1:], ']')
		for offset = offset + 1; offset < len(b); offset++ {
			if b[offset] != ' ' {
				break
			}
		}
		if offset == len(b) {
			return len(b), nil
		}
		switch b[levelStart+1] {
		case 'D': // [DEBUG]
			rl.log.Debugf("%s", b[offset:])
		case 'I': // [INFO]
			rl.log.Noticef("%s", b[offset:])
		case 'W': // [WARN]
			rl.log.Warnf("%s", b[offset:])
		case 'E': // [ERROR]
			rl.log.Errorf("%s", b[offset:])
		default:
			rl.log.Noticef("%s", b)
		}
	}
	return len(b), nil
}

func (rl *raftLogger) Close() error { return nil }

// createRaftNode creates and starts a new Raft node with the given name and FSM.
func (s *StanServer) createRaftNode(name string) (bool, error) {
	path := filepath.Join(s.opts.Clustering.RaftLogPath, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModeDir+os.ModePerm); err != nil {
			return false, err
		}
	}

	// We create s.raft early because once NewRaft() is called, the
	// raft code may asynchronously invoke FSM.Apply() and FSM.Restore()
	// So we want the object to exist so we can check on leader atomic, etc..
	s.raft = &raftNode{}

	raftLogFileName := filepath.Join(path, raftLogFile)
	store, err := newRaftLog(s.log, raftLogFileName, s.opts.Clustering.Sync, int(s.opts.Clustering.TrailingLogs),
		s.opts.Encrypt, s.opts.EncryptionCipher, s.opts.EncryptionKey)
	if err != nil {
		return false, err
	}

	// Go through the list of channels that we have recovered from streaming store
	// and set their corresponding UID.
	s.channels.Lock()
	for cname, c := range s.channels.channels {
		id, err := store.GetChannelID(cname)
		if err != nil {
			s.channels.Unlock()
			return false, err
		}
		c.id = id
	}
	s.channels.Unlock()

	cacheStore, err := raft.NewLogCache(s.opts.Clustering.LogCacheSize, store)
	if err != nil {
		store.Close()
		return false, err
	}

	addr := s.getClusteringAddr(name)
	config := raft.DefaultConfig()
	// For tests
	if runningInTests {
		config.ElectionTimeout = 100 * time.Millisecond
		config.HeartbeatTimeout = 100 * time.Millisecond
		config.LeaderLeaseTimeout = 100 * time.Millisecond
	} else {
		if s.opts.Clustering.RaftHeartbeatTimeout == 0 {
			s.opts.Clustering.RaftHeartbeatTimeout = defaultRaftHBTimeout
		}
		if s.opts.Clustering.RaftElectionTimeout == 0 {
			s.opts.Clustering.RaftElectionTimeout = defaultRaftElectionTimeout
		}
		if s.opts.Clustering.RaftLeaseTimeout == 0 {
			s.opts.Clustering.RaftLeaseTimeout = defaultRaftLeaseTimeout
		}
		if s.opts.Clustering.RaftCommitTimeout == 0 {
			s.opts.Clustering.RaftCommitTimeout = defaultRaftCommitTimeout
		}
		config.HeartbeatTimeout = s.opts.Clustering.RaftHeartbeatTimeout
		config.ElectionTimeout = s.opts.Clustering.RaftElectionTimeout
		config.LeaderLeaseTimeout = s.opts.Clustering.RaftLeaseTimeout
		config.CommitTimeout = s.opts.Clustering.RaftCommitTimeout
	}
	config.LocalID = raft.ServerID(s.opts.Clustering.NodeID)
	config.TrailingLogs = uint64(s.opts.Clustering.TrailingLogs)

	logWriter := &raftLogger{s}
	config.LogOutput = logWriter

	snapshotStore, err := raft.NewFileSnapshotStore(path, s.opts.Clustering.LogSnapshots, logWriter)
	if err != nil {
		store.Close()
		return false, err
	}
	sl, err := snapshotStore.List()
	if err != nil {
		store.Close()
		return false, err
	}

	// TODO: using a single NATS conn for every channel might be a bottleneck. Maybe pool conns?
	transport, err := newNATSTransport(addr, s.ncr, tportTimeout, logWriter)
	if err != nil {
		store.Close()
		return false, err
	}
	// Make the snapshot process never timeout... check (s *serverSnapshot).Persist() for details
	transport.TimeoutScale = 1

	// Set up a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	config.NotifyCh = raftNotifyCh

	fsm := &raftFSM{server: s}
	// We want to know in snapshot.go:Restore() if we are called from NewRaft() or
	// at runtime when catching up with the leader. To do so we will set this boolean
	// if there were more than one snapshot before the call. The boolean will be cleared
	// in Restore() itself.
	if len(sl) > 0 {
		fsm.Lock()
		fsm.restoreFromInit = true
		fsm.Unlock()
	}
	s.raft.fsm = fsm
	node, err := raft.NewRaft(config, fsm, cacheStore, store, snapshotStore, transport)
	if err != nil {
		transport.Close()
		store.Close()
		return false, err
	}
	if testPauseAfterNewRaftCalled {
		time.Sleep(time.Second)
	}
	existingState, err := raft.HasExistingState(cacheStore, store, snapshotStore)
	if err != nil {
		node.Shutdown()
		transport.Close()
		store.Close()
		return false, err
	}

	if existingState {
		s.log.Debugf("Loaded existing state for Raft group %s", name)
	}

	// Handle requests to join the cluster.
	sub, err := s.ncr.Subscribe(fmt.Sprintf("%s.%s.join", defaultRaftPrefix, name), func(msg *nats.Msg) {
		// Drop the request if we're not the leader. There's no race condition
		// after this check because even if we proceed with the cluster add, it
		// will fail if the node is not the leader as cluster changes go
		// through the Raft log.
		if node.State() != raft.Leader {
			return
		}
		req := &spb.RaftJoinRequest{}
		if err := req.Unmarshal(msg.Data); err != nil {
			s.log.Errorf("Invalid join request for Raft group %s", name)
			return
		}

		// Add the node as a voter. This is idempotent. No-op if the request
		// came from ourselves.
		resp := &spb.RaftJoinResponse{}
		if req.NodeID != s.opts.Clustering.NodeID {
			future := node.AddVoter(
				raft.ServerID(req.NodeID),
				raft.ServerAddress(req.NodeAddr), 0, 0)
			if err := future.Error(); err != nil {
				resp.Error = err.Error()
			}
		}

		// Send the response.
		r, err := resp.Marshal()
		if err != nil {
			panic(err)
		}
		s.ncr.Publish(msg.Reply, r)
	})
	if err != nil {
		node.Shutdown()
		transport.Close()
		store.Close()
		return false, err
	}
	s.raft.Raft = node
	s.raft.store = store
	s.raft.transport = transport
	s.raft.logInput = logWriter
	s.raft.notifyCh = raftNotifyCh
	s.raft.joinSub = sub
	return existingState, nil
}

// bootstrapCluster bootstraps the node for the provided Raft group either as a
// seed node or with the given peer configuration, depending on configuration
// and with the latter taking precedence.
func (s *StanServer) bootstrapCluster(name string, node *raft.Raft) error {
	var (
		addr = s.getClusteringAddr(name)
		// Include ourself in the cluster.
		servers = []raft.Server{{
			ID:      raft.ServerID(s.opts.Clustering.NodeID),
			Address: raft.ServerAddress(addr),
		}}
	)
	if len(s.opts.Clustering.Peers) > 0 {
		// Bootstrap using provided cluster configuration.
		s.log.Debugf("Bootstrapping Raft group %s using provided configuration", name)
		for _, peer := range s.opts.Clustering.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(s.getClusteringPeerAddr(name, peer)),
			})
		}
	} else {
		// Bootstrap as a seed node.
		s.log.Debugf("Bootstrapping Raft group %s as seed node", name)
	}
	config := raft.Configuration{Servers: servers}
	return node.BootstrapCluster(config).Error()
}

func (s *StanServer) getClusteringAddr(raftName string) string {
	return s.getClusteringPeerAddr(raftName, s.opts.Clustering.NodeID)
}

func (s *StanServer) getClusteringPeerAddr(raftName, nodeID string) string {
	return fmt.Sprintf("%s.%s.%s", s.opts.ID, nodeID, raftName)
}

// Returns the message store first and last sequence.
// When in clustered mode, if the first and last are 0, returns the value of
// the last sequence that we possibly got from the last snapshot. If a node
// restores a snapshot that let's say has first=1 and last=100, but when it
// tries to get these messages from the leader, the leader does not send them
// back because they have all expired, the node will not store anything.
// If we just rely on store's first/last, this node would use and report 0
// for channel's first and last while when all messages have expired, it should
// be last+1/last.
func (s *StanServer) getChannelFirstAndlLastSeq(c *channel) (uint64, uint64, error) {
	first, last, err := c.store.Msgs.FirstAndLastSequence()
	if !s.isClustered {
		return first, last, err
	}
	if err != nil {
		return 0, 0, err
	}
	if first == 0 && last == 0 {
		if fseq := atomic.LoadUint64(&c.firstSeq); fseq != 0 {
			first = fseq
			last = fseq - 1
		}
	}
	return first, last, nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (r *raftFSM) Apply(l *raft.Log) interface{} {
	s := r.server
	op := &spb.RaftOperation{}
	if err := op.Unmarshal(l.Data); err != nil {
		panic(err)
	}
	// We don't want snapshot Persist() and Apply() to execute concurrently,
	// so use common lock.
	r.Lock()
	defer r.Unlock()
	switch op.OpType {
	case spb.RaftOperation_Publish:
		// Message replication.
		if len(op.PublishBatch.Messages) == 0 {
			return nil
		}
		// This is a batch for a given channel, so lookup channel once.
		msg := op.PublishBatch.Messages[0]
		c, err := r.lookupOrCreateChannel(msg.Subject, op.ChannelID)
		if err != nil {
			goto FATAL_ERROR
		}
		// `c` will be nil if the existing channel has an ID > than op.ChannelID.
		// This will be the case in RAFT log replay if we have several "versions"
		// of the same channel. In that case, simply ignore the log replay.
		if c == nil {
			return nil
		}
		if !c.lSeqChecked {
			// If msg.Sequence is > 1, then make sure we have no gap.
			if msg.Sequence > 1 {
				// We pass `1` for the `first` sequence. The function we call
				// will do the right thing when it comes to restore possible
				// missing messages.
				if err = s.raft.fsm.restoreMsgsFromSnapshot(c, 1, msg.Sequence-1, true); err != nil {
					goto FATAL_ERROR
				}
			}
			c.lSeqChecked = true
		}
		for _, msg = range op.PublishBatch.Messages {
			if _, err = c.store.Msgs.Store(msg); err != nil {
				goto FATAL_ERROR
			}
		}
		return c.store.Msgs.Flush()
	FATAL_ERROR:
		panic(fmt.Errorf("failed to store replicated message %d on channel %s: %v",
			msg.Sequence, msg.Subject, err))
	case spb.RaftOperation_Connect:
		// Client connection create replication.
		return s.processConnect(op.ClientConnect.Request, op.ClientConnect.Refresh)
	case spb.RaftOperation_Disconnect:
		// Client connection close replication.
		return s.closeClient(op.ClientDisconnect.ClientID)
	case spb.RaftOperation_Subscribe:
		// Subscription replication.
		c, err := r.lookupOrCreateChannel(op.Sub.Request.Subject, op.ChannelID)
		if c == nil && err == nil {
			err = fmt.Errorf("unable to process subscription on channel %q, wrong ID %v",
				op.Sub.Request.Subject, op.ChannelID)
		}
		if err != nil {
			return &replicatedSub{sub: nil, err: err}
		}
		sub, err := s.processSub(c, op.Sub.Request, op.Sub.AckInbox, op.Sub.ID)
		return &replicatedSub{sub: sub, err: err}
	case spb.RaftOperation_RemoveSubscription:
		fallthrough
	case spb.RaftOperation_CloseSubscription:
		// Close/Unsub subscription replication.
		isSubClose := op.OpType == spb.RaftOperation_CloseSubscription
		s.closeMu.Lock()
		err := s.unsubscribe(op.Unsub, isSubClose)
		s.closeMu.Unlock()
		return err
	case spb.RaftOperation_SendAndAck:
		if !s.isLeader() {
			s.processReplicatedSendAndAck(op.SubSentAck)
		}
		return nil
	case spb.RaftOperation_DeleteChannel:
		// Delete only if the channel exists and has the same ID.
		if r.lookupChannel(op.Channel, op.ChannelID) == nil {
			return nil
		}
		s.processDeleteChannel(op.Channel)
		return nil
	default:
		panic(fmt.Sprintf("unknown op type %s", op.OpType))
	}
}

// This returns a channel object only if it is found with the proper ID.
func (r *raftFSM) lookupChannel(name string, id uint64) *channel {
	s := r.server
	cs := s.channels
	cs.RLock()
	defer cs.RUnlock()

	c := cs.channels[name]
	// Consider no ID (either in channel or from param) to be a match.
	// See note in raftFSM.lookupOrCreateChannel() regarding id == 0
	// when dealing with channels created by older versions.
	if c != nil && (id == 0 || c.id == 0 || c.id == id) {
		return c
	}
	// No channel, or wrong ID
	return nil
}

// Returns the channel with this name and ID.
// If channel exists and has an ID that is greater than the given `id`, then
// this function will return `nil` to indicate that the streaming version
// is more recent than the asked version. Otherwise, if `id` is greater,
// the channel is first deleted then recreated with the given `id`.
//
// Note that to support existing streaming and RAFT stores, the given `id` may
// be empty when processing existing RAFT snapshots/logs, or the streaming
// channel may not have an ID. In any of those cases, the existing channel
// object is returned.
func (r *raftFSM) lookupOrCreateChannel(name string, id uint64) (*channel, error) {
	s := r.server
	cs := s.channels
	cs.Lock()
	defer cs.Unlock()

	c := cs.channels[name]
	if c != nil {
		// Consider no ID (either in channel or from param) to be a match.
		// See note above regarding meaning of id == 0.
		if id == 0 || c.id == 0 || c.id == id {
			return c, nil
		}
		// If this channel is a more recent version than the asked `id` return
		// nil to indicate this to the caller.
		if c.id > id {
			return nil, nil
		}
		// Here the existing channel has an older ID (version) so replace.
		err := cs.store.DeleteChannel(name)
		if err == nil {
			err = s.raft.store.DeleteChannelID(name)
		}
		if err != nil {
			s.log.Errorf("Error deleting channel %q: %v", name, err)
			if s.isLeader() && c.activity != nil {
				c.activity.deleteInProgress = false
				c.startDeleteTimer()
			}
			return nil, err
		}
		delete(cs.channels, name)
	}
	// Channel does exist or has been deleted. Create now with given ID.
	return cs.createChannelLocked(s, name, id)
}
