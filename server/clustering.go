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
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nats-streaming-server/spb"
)

const (
	defaultJoinRaftGroupTimeout = time.Second
	defaultRaftHBTimeout        = 2 * time.Second
	defaultRaftElectionTimeout  = 2 * time.Second
	defaultRaftLeaseTimeout     = time.Second
	defaultRaftCommitTimeout    = 100 * time.Millisecond
)

var (
	runningInTests              bool
	joinRaftGroupTimeout        = defaultJoinRaftGroupTimeout
	testPauseAfterNewRaftCalled bool
)

func clusterSetupForTest() {
	runningInTests = true
	lazyReplicationInterval = 250 * time.Millisecond
	joinRaftGroupTimeout = 250 * time.Millisecond
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
	snapshotsOnInit int
	server          *StanServer
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
	if r.Raft != nil {
		if err := r.Raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	if r.transport != nil {
		if err := r.transport.Close(); err != nil {
			return err
		}
	}
	if r.store != nil {
		if err := r.store.Close(); err != nil {
			return err
		}
	}
	if r.joinSub != nil {
		if err := r.joinSub.Unsubscribe(); err != nil {
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
		switch b[levelStart+1] {
		case 'D': // [DEBUG]
			rl.log.Tracef("%s", b[levelStart+8:])
		case 'I': // [INFO]
			rl.log.Noticef("%s", b[levelStart+7:])
		case 'W': // [WARN]
			rl.log.Warnf("%s", b[levelStart+7:])
		case 'E': // [ERR]
			rl.log.Errorf("%s", b[levelStart+6:])
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
		config.LeaderLeaseTimeout = 50 * time.Millisecond
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
	transport, err := newNATSTransport(addr, s.ncr, 2*time.Second, logWriter)
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
	fsm.Lock()
	fsm.snapshotsOnInit = len(sl)
	fsm.Unlock()
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
		servers = []raft.Server{raft.Server{
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
func (s *StanServer) getChannelFirstAndlLastSeqWithLock(c *channel, lock bool) (uint64, uint64, error) {
	first, last, err := c.store.Msgs.FirstAndLastSequence()
	if !s.isClustered {
		return first, last, err
	}
	if err != nil {
		return 0, 0, err
	}
	if first == 0 && last == 0 {
		if lock {
			s.raft.fsm.Lock()
		}
		if c.snapLastSeq != 0 {
			last = c.snapLastSeq
			first = last + 1
		}
		if lock {
			s.raft.fsm.Unlock()
		}
	}
	return first, last, nil
}

func (s *StanServer) getChannelFirstAndlLastSeq(c *channel) (uint64, uint64, error) {
	return s.getChannelFirstAndlLastSeqWithLock(c, true)
}

func (s *StanServer) getChannelFirstAndlLastSeqLocked(c *channel) (uint64, uint64, error) {
	return s.getChannelFirstAndlLastSeqWithLock(c, false)
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
		var (
			c       *channel
			err     error
			lastSeq uint64
		)
		for _, msg := range op.PublishBatch.Messages {
			// This is a batch for a given channel, so lookup channel once.
			if c == nil {
				c, err = s.lookupOrCreateChannel(msg.Subject)
				// That should not be the case, but if it happens,
				// just bail out.
				if err == ErrChanDelInProgress {
					return nil
				}
				lastSeq, err = c.store.Msgs.LastSequence()
			}
			if err == nil && lastSeq < msg.Sequence-1 {
				err = s.raft.fsm.restoreMsgsFromSnapshot(c, lastSeq+1, msg.Sequence-1)
			}
			if err == nil {
				_, err = c.store.Msgs.Store(msg)
			}
			if err != nil {
				return fmt.Errorf("failed to store replicated message %d on channel %s: %v",
					msg.Sequence, msg.Subject, err)
			}
		}
		return nil
	case spb.RaftOperation_Connect:
		// Client connection create replication.
		return s.processConnect(op.ClientConnect.Request, op.ClientConnect.Refresh)
	case spb.RaftOperation_Disconnect:
		// Client connection close replication.
		return s.closeClient(op.ClientDisconnect.ClientID)
	case spb.RaftOperation_Subscribe:
		// Subscription replication.
		sub, err := s.processSub(nil, op.Sub.Request, op.Sub.AckInbox, op.Sub.ID)
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
		s.processDeleteChannel(op.Channel)
		return nil
	default:
		panic(fmt.Sprintf("unknown op type %s", op.OpType))
	}
}
