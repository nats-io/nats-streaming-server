// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
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

// ClusteringOptions contains STAN Server options related to clustering.
type ClusteringOptions struct {
	Clustered      bool          // Run the server in a clustered configuration.
	NodeID         string        // ID of the node within the cluster.
	Bootstrap      bool          // Bootstrap the cluster as a seed node if there is no existing state.
	RaftLogPath    string        // Path to Raft log store directory.
	LogCacheSize   int           // Number of Raft log entries to cache in memory to reduce disk IO.
	LogSnapshots   int           // Number of Raft log snapshots to retain.
	TrailingLogs   int64         // Number of logs left after a snapshot.
	Sync           bool          // Do a file sync after every write to the Raft log and message store.
	GossipInterval time.Duration // Interval in which to gossip channels to the cluster (plus some random delay).
}

// raftNode is a handle to a member in a Raft consensus group.
type raftNode struct {
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
	if err := r.store.Close(); err != nil {
		return err
	}
	if err := r.transport.Close(); err != nil {
		return err
	}
	if err := r.joinSub.Unsubscribe(); err != nil {
		return err
	}
	return r.logInput.Close()
}

// createRaftNode creates and starts a new Raft node with the given name and FSM.
func (s *StanServer) createRaftNode(name string, fsm raft.FSM) (*raftNode, error) {
	path := filepath.Join(s.opts.Clustering.RaftLogPath, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModeDir+os.ModePerm); err != nil {
			return nil, err
		}
	}
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:   filepath.Join(path, raftLogFile),
		NoSync: !s.opts.Clustering.Sync,
	})
	if err != nil {
		return nil, err
	}
	cacheStore, err := raft.NewLogCache(s.opts.Clustering.LogCacheSize, store)
	if err != nil {
		store.Close()
		return nil, err
	}

	addr := s.getClusteringAddr(name)
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.opts.Clustering.NodeID)
	config.TrailingLogs = uint64(s.opts.Clustering.TrailingLogs)

	// FIXME: Send output of Raft logger
	// Raft expects a *log.Logger so can not swap that with another one,
	// but we can modify the outputs to which the Raft logger is writing.
	//config.Logger = s.log
	logReader, logWriter := io.Pipe()
	config.LogOutput = logWriter
	bufr := bufio.NewReader(logReader)
	go func() {
		for {
			line, _, err := bufr.ReadLine()
			if err != nil {
				if err != io.EOF {
					s.log.Errorf("error while reading piped output from Raft log: %s", err)
				}
				return
			}

			fields := bytes.Fields(line)
			level := string(fields[2])
			raftLogFields := fields[4:]
			raftLog := string(bytes.Join(raftLogFields, []byte(" ")))

			switch level {
			case "[DEBUG]":
				s.log.Tracef("%v", raftLog)
			case "[INFO]":
				s.log.Noticef("%v", raftLog)
			case "[WARN]":
				s.log.Noticef("%v", raftLog)
			case "[ERROR]":
				s.log.Fatalf("%v", raftLog)
			default:
				s.log.Noticef("%v", raftLog)
			}
		}
	}()

	snapshotStore, err := raft.NewFileSnapshotStore(path, s.opts.Clustering.LogSnapshots, logWriter)
	if err != nil {
		store.Close()
		return nil, err
	}

	// TODO: using a single NATS conn for every channel might be a bottleneck. Maybe pool conns?
	transport, err := natslog.NewNATSTransport(addr, s.ncr, 2*time.Second, logWriter)
	if err != nil {
		store.Close()
		return nil, err
	}

	// Set up a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	config.NotifyCh = raftNotifyCh

	node, err := raft.NewRaft(config, fsm, cacheStore, store, snapshotStore, transport)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, err
	}

	// Check if we need to bootstrap the cluster.
	existingState, err := raft.HasExistingState(cacheStore, store, snapshotStore)
	if err != nil {
		return nil, err
	}
	if s.opts.Clustering.Bootstrap && !existingState {
		config := raft.Configuration{
			Servers: []raft.Server{
				raft.Server{
					ID:      raft.ServerID(s.opts.Clustering.NodeID),
					Address: raft.ServerAddress(addr),
				},
			},
		}
		s.log.Debugf("Bootstrapping Raft group %s", name)
		if err := node.BootstrapCluster(config).Error(); err != nil {
			return nil, err
		}
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
		return nil, err
	}

	// Attempt to join the cluster if we're not bootstrapping.
	if !s.opts.Clustering.Bootstrap {
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
				s.log.Errorf("Error attempting to join Raft group %s: %v", name, err.Error())
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if err := resp.Unmarshal(r.Data); err != nil {
				s.log.Errorf("Error attempting to join Raft group %s: %v", name, err.Error())
				time.Sleep(20 * time.Millisecond)
				continue
			}
			if resp.Error != "" {
				s.log.Errorf("Error attempting to join Raft group %s: %s", name, resp.Error)
				time.Sleep(20 * time.Millisecond)
				continue
			}
			joined = true
			break
		}
		if !joined {
			transport.Close()
			store.Close()
			sub.Unsubscribe()
			return nil, fmt.Errorf("failed to join Raft group %s", name)
		}
	}

	return &raftNode{
		Raft:      node,
		store:     store,
		transport: transport,
		logInput:  logWriter,
		notifyCh:  raftNotifyCh,
		joinSub:   sub,
	}, nil
}

func (s *StanServer) getClusteringAddr(raftName string) string {
	return fmt.Sprintf("%s.%s.%s", s.opts.ID, s.opts.Clustering.NodeID, raftName)
}
