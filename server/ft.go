// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/graft"
	"github.com/nats-io/nats-streaming-server/stores"
)

// Some go-routine will panic, which we can't recover in test.
// So the tests will set this to true to be able to test the
// correct behavior.
var noPanic = false

// ftWaitToBeLeader will return only when the RAFT algorithm has
// elected this node as the leader and that the node can get
// the store's exclusive lock.
// This is running in a separate go-routine so if server state
// changes, take care of using the server's lock.
func (s *StanServer) ftWaitToBeLeader() error {
	Noticef("STAN: Waiting for leader election...")
	graftOpts := s.nc.Opts
	graftOpts.Name = fmt.Sprintf("_NSS-%s-graft", s.opts.ID)
graft_create_node:
	node, stateChangeChan, errChan, err := s.ftCreateRAFTNode(&graftOpts)
	if err != nil {
		return fmt.Errorf("ft: error creating graft node: %v", err)
	}
	// The server was shutdown
	if node == nil {
		return nil
	}
	if node.State() != graft.LEADER {
	waitForLeaderLoop:
		for {
			select {
			case <-s.ftQuit:
				node.Close()
				return nil
			case sc := <-stateChangeChan:
				if sc.To == graft.LEADER {
					break waitForLeaderLoop
				}
			case err := <-errChan:
				node.Close()
				return fmt.Errorf("ft: error: %v", err)
			}
		}
	}
	Noticef("STAN: Server elected leader, getting store exclusive lock...")
	// Normally, the store would be set early and is immutable, but some
	// FT tests do set a mock store after the server is created, so use
	// locking here to avoid race reports.
	s.mu.Lock()
	store := s.store
	s.mu.Unlock()
	if ok, err := store.GetExclusiveLock(); !ok || err != nil {
		// If there is an error, we have to stop now.
		if err != nil {
			node.Close()
			return fmt.Errorf("ft: error getting the store lock: %v", err)
		}
		// If ok is false, it means that we did not get the lock
		// so we should be going back to standby.
		Noticef("STAN: ft: unable to get store lock at this time, going back to standby")
		node.Close()
		goto graft_create_node
	}
	// Server could have been shutdown
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}
	s.wg.Add(1)
	s.mu.Unlock()
	Noticef("STAN: Server confirmed leader")
	go func() {
		// On shutdown, server is waiting on s.wg, so release when
		// we exit this function.
		defer s.wg.Done()
		// We are going to care only about state changes here, and not the
		// error channel. The reason is that for a leader, the only case
		// where we would get an error is writting the state, which would
		// happen if the leader gets demoted, which we handle in the state
		// change case. So we will simply dequeue the error channel and
		// print, but nothing else.
		// Note that when we call "Fatalf", it is possible that the server
		// was started programmatically, and that no logger is set, in which
		// case "Fatalf" does nothing, so each "Fatalf" call is followed by
		// a panic.
		for {
			select {
			case sc := <-stateChangeChan:
				if sc.To != graft.LEADER {
					err = fmt.Errorf("ft: server demoted to %v, aborting", sc.To.String())
					Fatalf("STAN: %v", err)
					if noPanic {
						node.Close()
						s.setFTError(err)
					} else {
						panic(err)
					}
				}
				Noticef("STAN: ft: state change, from %v to %v", sc.From.String(), sc.To.String())
			case err := <-errChan:
				Errorf("STAN: ft: error: %v", err)
			case <-s.ftQuit:
				node.Close()
				return
			}
		}
	}()
	// Start the recovery process, etc..
	return s.start(FTActive)
}

// ftCreateRAFTNode creates a RAFT node and returns required channels.
// Since there is no step down feature, if a node is elected leader but
// the lock file checks shows that there is another leader currently running,
// the server will close and create a new node. State and error channels
// (handler) could be reused, but creating new ones to avoid possible updates
// from previous instance to interfere with new one.
func (s *StanServer) ftCreateRAFTNode(opts *nats.Options) (*graft.Node, chan graft.StateChange, chan error, error) {
	// Loop until we can get a valid connection...
	var (
		err error
		rpc *graft.NatsRpcDriver
	)
	// We need to have a NATS RPC in order for RAFT to work,
	// so try until we succeed.
	printCount := 0
	for {
		rpc, err = graft.NewNatsRpc(opts)
		if err != nil {
			if printCount%10 == 0 {
				Errorf("STAN: ft: creating RPC failed (%v), trying again...", err)
			}
			printCount++
			select {
			case <-s.ftQuit:
				return nil, nil, nil, nil
			case <-time.After(500 * time.Millisecond):
			}
		} else {
			break
		}
	}
	groupSize := s.opts.FTQuorum*2 - 1
	ci := graft.ClusterInfo{
		Name: s.opts.ID,
		Size: groupSize,
	}
	errChan := make(chan error)
	stateChangeChan := make(chan graft.StateChange)
	handler := graft.NewChanHandler(stateChangeChan, errChan)
	node, err := graft.New(ci, handler, rpc, s.opts.FTLogFile)
	return node, stateChangeChan, errChan, err
}

// ftSetup checks that all required FT parameters have been specified and
// create the channel required for shutdown.
// Note that FTGroupName has to be set before server invokes this function,
// so this parameter is not checked here.
func (s *StanServer) ftSetup() error {
	// Check that we have an FT log file
	if s.opts.FTLogFile == "" {
		return fmt.Errorf("ft: FTLogFile needs to be specified")
	}
	// Check that store type is ok. So far only support for FileStore
	if s.opts.StoreType != stores.TypeFile {
		return fmt.Errorf("ft: only %v stores supported in FT mode", stores.TypeFile)
	}
	// We need to have at least quorum of 1
	if s.opts.FTQuorum <= 0 {
		return fmt.Errorf("ft: FTQuorum needs to be at least 1")
	}
	// Create channel to notify FT go routine to quit.
	s.ftQuit = make(chan struct{}, 1)
	// Set the state as standby initially
	s.state = FTStandby
	return nil
}
