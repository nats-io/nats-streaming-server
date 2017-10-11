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
	"github.com/nats-io/nats-on-a-log"
)

// raftNode is a handle to a member in a Raft consensus group.
type raftNode struct {
	*raft.Raft
	store     *raftboltdb.BoltStore
	transport *raft.NetworkTransport
	logInput  io.WriteCloser
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
	return r.logInput.Close()
}

// createRaftNode creates and starts a new Raft node with the given name and FSM.
func (s *StanServer) createRaftNode(name string, fsm raft.FSM) (*raftNode, error) {
	path := filepath.Join(s.opts.RaftLogPath, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModeDir+os.ModePerm); err != nil {
			return nil, err
		}
	}
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:   filepath.Join(path, raftLogFile),
		NoSync: !s.opts.FileStoreOpts.DoSync,
	})
	if err != nil {
		return nil, err
	}
	cacheStore, err := raft.NewLogCache(s.opts.LogCacheSize, store)
	if err != nil {
		store.Close()
		return nil, err
	}

	config := raft.DefaultConfig()
	config.TrailingLogs = uint64(s.opts.TrailingLogs)

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

	snapshotStore, err := raft.NewFileSnapshotStore(path, s.opts.LogSnapshots, logWriter)
	if err != nil {
		store.Close()
		return nil, err
	}

	// TODO: using a single NATS conn for every channel might be a bottleneck. Maybe pool conns?
	transport, err := natslog.NewNATSTransport(
		fmt.Sprintf("%s.%s", s.opts.ClusterNodeID, name), s.ncr, 2*time.Second, logWriter)
	if err != nil {
		store.Close()
		return nil, err
	}

	peers := make([]string, len(s.opts.ClusterPeers))
	for i, p := range s.opts.ClusterPeers {
		peers[i] = fmt.Sprintf("%s.%s", p, name)
	}
	peersStore := &raft.StaticPeers{StaticPeers: peers}

	// Set up a channel for reliable leader notifications.
	raftNotifyCh := make(chan bool, 1)
	config.NotifyCh = raftNotifyCh

	node, err := raft.NewRaft(config, fsm, cacheStore, store, snapshotStore, peersStore, transport)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, err
	}

	return &raftNode{
		Raft:      node,
		store:     store,
		transport: transport,
		logInput:  logWriter,
		notifyCh:  raftNotifyCh,
	}, nil
}
