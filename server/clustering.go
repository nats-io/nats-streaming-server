// Copyright 2017 Apcera Inc. All rights reserved.
// Copyright 2018 Synadia Communications Inc. All rights reserved.

package server

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nats-on-a-log"
	"github.com/nats-io/nats-streaming-server/spb"
)

var runningInTests bool

func clusterSetupForTest() {
	runningInTests = true
	lazyReplicationInterval = 250 * time.Millisecond
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
}

// raftNode is a handle to a member in a Raft consensus group.
type raftNode struct {
	leader uint32
	*raft.Raft
	store     *raftboltdb.BoltStore
	transport *raft.NetworkTransport
	logInput  io.WriteCloser
	joinSub   *nats.Subscription
	notifyCh  <-chan bool
}

// shutdown attempts to stop the Raft node.
func (r *raftNode) shutdown() error {
	if err := r.Raft.Shutdown().Error(); err != nil {
		return err
	}
	if err := r.transport.Close(); err != nil {
		return err
	}
	if err := r.store.Close(); err != nil {
		return err
	}
	if err := r.joinSub.Unsubscribe(); err != nil {
		return err
	}
	return r.logInput.Close()
}

// createRaftNode creates and starts a new Raft node.
func (s *StanServer) createServerRaftNode(fsm raft.FSM) (*raftNode, error) {
	var (
		name                     = "stan"
		addr                     = s.getClusteringAddr(name)
		node, existingState, err = s.createRaftNode(name, fsm)
	)
	if err != nil {
		return nil, err
	}

	// Bootstrap if there is no previous state and we are starting this node as
	// a seed or a cluster configuration is provided.
	bootstrap := !existingState && (s.opts.Clustering.Bootstrap || len(s.opts.Clustering.Peers) > 0)
	if bootstrap {
		if err := s.bootstrapCluster(name, node.Raft); err != nil {
			node.shutdown()
			return nil, err
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
			r, err := s.ncr.Request(fmt.Sprintf("raft.%s.join", name), req, time.Second)
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
			return nil, fmt.Errorf("failed to join Raft group %s", name)
		}
	}
	return node, nil
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
			rl.log.Noticef("%s", b[levelStart+7:])
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
func (s *StanServer) createRaftNode(name string, fsm raft.FSM) (*raftNode, bool, error) {
	path := filepath.Join(s.opts.Clustering.RaftLogPath, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModeDir+os.ModePerm); err != nil {
			return nil, false, err
		}
	}
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:   filepath.Join(path, raftLogFile),
		NoSync: !s.opts.Clustering.Sync,
	})
	if err != nil {
		return nil, false, err
	}
	cacheStore, err := raft.NewLogCache(s.opts.Clustering.LogCacheSize, store)
	if err != nil {
		store.Close()
		return nil, false, err
	}

	addr := s.getClusteringAddr(name)
	config := raft.DefaultConfig()
	// For tests
	if runningInTests {
		config.ElectionTimeout = 100 * time.Millisecond
		config.HeartbeatTimeout = 100 * time.Millisecond
		config.LeaderLeaseTimeout = 50 * time.Millisecond
	}
	config.LocalID = raft.ServerID(s.opts.Clustering.NodeID)
	config.TrailingLogs = uint64(s.opts.Clustering.TrailingLogs)

	logWriter := &raftLogger{s}
	config.LogOutput = logWriter

	snapshotStore, err := raft.NewFileSnapshotStore(path, s.opts.Clustering.LogSnapshots, logWriter)
	if err != nil {
		store.Close()
		return nil, false, err
	}

	// TODO: using a single NATS conn for every channel might be a bottleneck. Maybe pool conns?
	transport, err := natslog.NewNATSTransport(addr, s.ncr, 2*time.Second, logWriter)
	if err != nil {
		store.Close()
		return nil, false, err
	}
	// Make the snapshot process never timeout... check (s *serverSnapshot).Persist() for details
	transport.TimeoutScale = 1

	// Set up a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	config.NotifyCh = raftNotifyCh

	node, err := raft.NewRaft(config, fsm, cacheStore, store, snapshotStore, transport)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, false, err
	}

	existingState, err := raft.HasExistingState(cacheStore, store, snapshotStore)
	if err != nil {
		return nil, false, err
	}

	if existingState {
		s.log.Debugf("Loaded existing state for Raft group %s", name)
	}

	// Handle requests to join the cluster.
	sub, err := s.ncr.Subscribe(fmt.Sprintf("raft.%s.join", name), func(msg *nats.Msg) {
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
		transport.Close()
		store.Close()
		return nil, false, err
	}

	return &raftNode{
		Raft:      node,
		store:     store,
		transport: transport,
		logInput:  logWriter,
		notifyCh:  raftNotifyCh,
		joinSub:   sub,
	}, existingState, nil
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
