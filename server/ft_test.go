// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/graft"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
)

func getTestFTDefaultOptions(member int) *Options {
	opts := getTestDefaultOptsForFileStore()
	opts.FTGroupName = "ft"
	opts.FTQuorum = 1
	opts.FTLogFile = filepath.Join(defaultDataStore, fmt.Sprintf("ft%d.log", member))
	return opts
}

func TestFTConfig(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.FTGroupName = "ft"
	opts.FTLogFile = filepath.Join(defaultDataStore, "ft.log")
	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Server should have failed to start with FT mode and MemStore")
	}

	// Test without logfile
	opts = getTestDefaultOptsForFileStore()
	opts.FTGroupName = "ft"
	s, err = RunServerWithOpts(opts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail due to missing log file")
	}

	// Test with negative size
	opts = getTestFTDefaultOptions(1)
	opts.FTQuorum = -1
	s, err = RunServerWithOpts(opts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail due to negative quorum size")
	}
	// Try with size 0 too.
	opts.FTQuorum = 0
	s, err = RunServerWithOpts(opts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail due to invalid quorum size")
	}
}

func getFTActiveServer(t *testing.T, servers ...*StanServer) *StanServer {
	var active *StanServer
	for l := 0; l < 5; l++ {
		for i := 0; i < len(servers); i++ {
			s := servers[i]
			if s.State() == FTActive {
				if active != nil {
					stackFatalf(t, "Found more than one active servers")
				}
				active = s
			}
		}
		if active != nil {
			break
		}
		time.Sleep(time.Second)
	}
	if active == nil {
		stackFatalf(t, "Unable to find the active server")
	}
	return active
}

func TestFTBasic(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Configure first server
	s1nOpts := natsdTest.DefaultTestOptions
	s1nOpts.Cluster.ListenStr = "nats://localhost:6222"
	s1nOpts.RoutesStr = "nats://localhost:6223"
	s1sOpts := getTestDefaultOptsForFileStore()
	s1sOpts.FTGroupName = "ft"
	s1sOpts.FTLogFile = filepath.Join(defaultDataStore, "ft1.log")
	s1, err := RunServerWithOpts(s1sOpts, &s1nOpts)
	if err != nil {
		t.Fatalf("Error starting server: %v", err)
	}
	defer s1.Shutdown()

	// Configure second server
	s2nOpts := natsdTest.DefaultTestOptions
	s2nOpts.Port = 4223
	s2nOpts.Cluster.ListenStr = "nats://localhost:6223"
	s2nOpts.RoutesStr = "nats://localhost:6222"
	s2sOpts := getTestDefaultOptsForFileStore()
	s2sOpts.FTGroupName = "ft"
	s2sOpts.FTLogFile = filepath.Join(defaultDataStore, "ft2.log")
	s2, err := RunServerWithOpts(s2sOpts, &s2nOpts)
	if err != nil {
		t.Fatalf("Error starting server: %v", err)
	}
	defer s2.Shutdown()

	s := getFTActiveServer(t, s1, s2)

	// Create a client connection
	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Create a subscription and notify the go channel when we get the message
	ch := make(chan bool)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) { ch <- true },
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// For test, make sure subscriber is registered.
	waitForNumSubs(t, s, clientName, 1)
	// Now shutdown the active
	s.Shutdown()
	// Get the reference to the new active server
	s = getFTActiveServer(t, s1, s2)
	// Publish a message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Check that we have received our message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Done!
	sc.Close()
	s.Shutdown()
}

func checkState(t *testing.T, s *StanServer, expectedState State) {
	if state := s.State(); state != expectedState {
		stackFatalf(t, "Expected server state to be %v, got %v (ft error=%v)",
			expectedState.String(), state.String(), s.FTStartupError())
	}
	// Repeat test with String() too...
	if stateStr := s.State().String(); stateStr != expectedState.String() {
		stackFatalf(t, "Expected server state to be %v, got %v (ft error=%v)",
			expectedState.String(), stateStr, s.FTStartupError())
	}
}

func waitForElection() {
	time.Sleep(graft.MAX_ELECTION_TIMEOUT + 100*time.Millisecond)
}

func TestFTCanStopFTStandby(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Start only 1 server with a quorum of 2, so it stays in standby mode.
	opts := getTestFTDefaultOptions(1)
	opts.FTQuorum = 2
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	// Make sure that it did not become active, even after more than the
	// leader election timeout
	waitForElection()
	checkState(t, s, FTStandby)
	// Make sure we can shut it down
	err := make(chan error, 1)
	ok := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ok:
			ok <- struct{}{}
		case <-time.After(5 * time.Second):
			err <- fmt.Errorf("Failed to shutdown")
		}
	}()
	go func() {
		s.Shutdown()
		ok <- struct{}{}
	}()
	select {
	case <-ok:
		ok <- struct{}{}
	case e := <-err:
		t.Fatal(e)
	}
	wg.Wait()
	// Check state, should be shutdown
	checkState(t, s, Shutdown)
	// Since server did not try to activate, there should not be any FT startup error
	if err := s.FTStartupError(); err != nil {
		t.Fatalf("FT startup error should be nil, got: %v", err)
	}
}

func TestFTQuorum(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// For this test, use a central NATS server
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Set quorum to 3, which means we need at least 3 servers running to have an election.
	// Start with starting only 2
	startServer := func(memberID int) *StanServer {
		opts := getTestFTDefaultOptions(memberID)
		opts.FTQuorum = 3
		opts.NATSServerURL = "nats://localhost:4222"
		return runServerWithOpts(t, opts, nil)
	}
	s1 := startServer(1)
	defer s1.Shutdown()
	s2 := startServer(2)
	defer s2.Shutdown()

	// Make sure that none become active, even after more than the
	// leader election timeout
	waitForElection()
	checkState(t, s1, FTStandby)
	checkState(t, s2, FTStandby)

	// Now start a 3rd server
	s3 := startServer(5)
	defer s3.Shutdown()

	// There should be an active server. This call will fail if no
	// active server can be found in a given period of time.
	active := getFTActiveServer(t, s1, s2, s3)
	// Then shut it down, the 2 remaning should not become active
	active.Shutdown()
	waitForElection()
	if s1.State() != Shutdown {
		checkState(t, s1, FTStandby)
	}
	if s2.State() != Shutdown {
		checkState(t, s2, FTStandby)
	}
	if s3.State() != Shutdown {
		checkState(t, s3, FTStandby)
	}
	// Start a new server
	s4 := startServer(4)
	defer s4.Shutdown()
	getFTActiveServer(t, s1, s2, s3, s4)
}

var ftPartitionDB = flag.String("ft_partition", "", "")

func TestFTPartition(t *testing.T) {
	ds := *ftPartitionDB
	parentProcess := ds == ""
	nOpts := natsdTest.DefaultTestOptions
	memberID := 1
	var natsURL string
	if parentProcess {
		ds = defaultDataStore
		cleanupDatastore(t, ds)
		defer cleanupDatastore(t, ds)

		nOpts.Cluster.ListenStr = "nats://localhost:6222"
		nOpts.RoutesStr = "nats://localhost:6223"
		natsURL = "nats://localhost:4222"
	} else {
		nOpts.Port = 4223
		nOpts.Cluster.ListenStr = "nats://localhost:6223"
		nOpts.RoutesStr = "nats://localhost:6222"
		memberID = 2
		natsURL = "nats://localhost:4223"
	}

	// Start NATS server independently
	ns := natsdTest.RunServer(&nOpts)
	defer ns.Shutdown()

	sOpts := getTestDefaultOptsForFileStore()
	sOpts.FTGroupName = "ft"
	sOpts.FTQuorum = 1
	sOpts.FTLogFile = filepath.Join(ds, fmt.Sprintf("ft%d.log", memberID))
	sOpts.NATSServerURL = natsURL
	sOpts.FilestoreDir = ds
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	// Wait for election and check state
	waitForElection()

	if parentProcess {
		// We should be the active server
		checkState(t, s, FTActive)

		wg := sync.WaitGroup{}
		wg.Add(1)
		errCh := make(chan error, 1)
		go func() {
			defer wg.Done()
			// Start a process that will be the standby
			out, err := exec.Command(os.Args[0], "-ft_partition", ds,
				"-test.v", "-test.run=TestFTPartition$").CombinedOutput()
			if err != nil {
				errCh <- fmt.Errorf("Standby error: %v - %v", err, string(out))
			}
		}()

		// Give a bit of chance for child process to start and be the standby
		waitForElection()
		// Kill our NATS server, the standby should try to become active but
		// fail due to file lock
		ns.Shutdown()
		waitForElection()
		ns = natsdTest.RunServer(&nOpts)
		defer ns.Shutdown()
		waitForElection()
		checkState(t, s, FTActive)
		wg.Wait()
		select {
		case e := <-errCh:
			t.Fatal(e)
		default:
		}
	} else {
		// Wait this process to be the standby server
		checkState(t, s, FTStandby)
		// The active server's NATS server will be killed in the parent
		// process. The standby is going to try to become active, but should
		// fail.
		time.Sleep(2 * graft.MAX_ELECTION_TIMEOUT)
		checkState(t, s, FTStandby)
	}
}

// This is same that TestFTPartition, but roles are reversed. This is mainly for
// code coverage report.
func TestFTPartitionReversed(t *testing.T) {
	ds := *ftPartitionDB
	parentProcess := ds == ""
	nOpts := natsdTest.DefaultTestOptions
	memberID := 1
	var natsURL string
	if parentProcess {
		ds = defaultDataStore
		cleanupDatastore(t, ds)
		defer cleanupDatastore(t, ds)

		nOpts.Cluster.ListenStr = "nats://localhost:6222"
		nOpts.RoutesStr = "nats://localhost:6223"
		natsURL = "nats://localhost:4222"
	} else {
		nOpts.Port = 4223
		nOpts.Cluster.ListenStr = "nats://localhost:6223"
		nOpts.RoutesStr = "nats://localhost:6222"
		memberID = 2
		natsURL = "nats://localhost:4223"
	}

	// Start NATS server independently
	ns := natsdTest.RunServer(&nOpts)
	defer ns.Shutdown()

	if parentProcess {
		wg := sync.WaitGroup{}
		wg.Add(1)
		errCh := make(chan error, 1)
		go func() {
			defer wg.Done()
			// Start a process that will be the standby
			out, err := exec.Command(os.Args[0], "-ft_partition", ds,
				"-test.v", "-test.run=TestFTPartitionReversed$").CombinedOutput()
			if err != nil {
				errCh <- fmt.Errorf("Active error: %v - %v", err, string(out))
			}
		}()

		// Wait for the child process to become active server.
		waitForElection()

		sOpts := getTestDefaultOptsForFileStore()
		sOpts.FTGroupName = "ft"
		sOpts.FTQuorum = 1
		sOpts.FTLogFile = filepath.Join(ds, fmt.Sprintf("ft%d.log", memberID))
		sOpts.NATSServerURL = natsURL
		sOpts.FilestoreDir = ds
		s := runServerWithOpts(t, sOpts, nil)
		defer s.Shutdown()

		waitForElection()
		checkState(t, s, FTStandby)

		// The active server's NATS server is going to be killed.
		// The standby here will try to become active, but should fail
		// to do so and stay standby
		waitForElection()
		checkState(t, s, FTStandby)

		wg.Wait()
		select {
		case e := <-errCh:
			t.Fatal(e)
		default:
		}
	} else {
		sOpts := getTestDefaultOptsForFileStore()
		sOpts.FTGroupName = "ft"
		sOpts.FTQuorum = 1
		sOpts.FTLogFile = filepath.Join(ds, fmt.Sprintf("ft%d.log", memberID))
		sOpts.NATSServerURL = natsURL
		sOpts.FilestoreDir = ds
		s := runServerWithOpts(t, sOpts, nil)
		defer s.Shutdown()

		// Wait this process to be the active server
		waitForElection()
		checkState(t, s, FTActive)

		// Shutdown NATS server to cause standby to try to become active
		ns.Shutdown()
		waitForElection()
		ns = natsdTest.RunServer(&nOpts)
		defer ns.Shutdown()
		waitForElection()
		checkState(t, s, FTActive)
	}
}

func TestFTFailedStartup(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestFTDefaultOptions(1)
	s := runServerWithOpts(t, opts, nil)
	// Wait for it to become active
	getFTActiveServer(t, s)
	// Now shut it down
	s.Shutdown()
	// And restart with wrong cluster name.
	opts.ID = "wrongClusterName"
	// We should not get an error until it becomes active
	// because until then, it cannot know that it's cluster ID
	// does not match the one stored.
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	waitForElection()
	checkState(t, s, FTFailed)
	if err := s.FTStartupError(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("Expected error regarding non matching cluster ID, got %v", err)
	}
}

func TestFTImmediateShutdownOnStartup(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestFTDefaultOptions(1)
	s := runServerWithOpts(t, opts, nil)
	s.Shutdown()
	checkState(t, s, Shutdown)
}

func TestFTUnableToCreateLogfile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Fail to start due to error creating FT log
	opts := getTestFTDefaultOptions(1)
	opts.FTLogFile = "dummy/ft.log"
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	waitForElection()
	checkState(t, s, FTFailed)
	if err := s.FTStartupError(); err == nil || !strings.Contains(err.Error(), "graft") {
		t.Fatalf("Expected error about creating graft node, got: %v", err)
	}
}

// A mock store that we use to override GetExclusiveLock() behavior.
type mockStore struct {
	// We embed a store so that we don't have to implement all store APIs.
	stores.FileStore
	result bool
	err    error
}

func (ms *mockStore) GetExclusiveLock() (bool, error) {
	return ms.result, ms.err
}

// There is one test (so far) that proceeds with trying to recover and then
// init the store. So we need to override those, but do nothing in them.

func (ms *mockStore) Recover() (*stores.RecoveredState, error) {
	return nil, nil
}

func (ms *mockStore) Init(_ *spb.ServerInfo) error {
	return nil
}

func setMockedStore(s *StanServer, ms *mockStore) {
	s.mu.Lock()
	s.store = ms
	s.mu.Unlock()
}

func TestFTGetStoreLockReturnsError(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a mock store that will return an error in GetExclusiveLock
	ms := &mockStore{err: fmt.Errorf("on purpose")}

	opts := getTestFTDefaultOptions(1)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	setMockedStore(s, ms)
	waitForElection()
	checkState(t, s, FTFailed)
	// Should get an error about getting an error getting the store lock
	if err := s.FTStartupError(); err == nil || !strings.Contains(err.Error(), "store lock") {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestFTStayStandbyIfStoreAlreadyLocked(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a mock store that will return that store is locked
	ms := &mockStore{result: false}

	opts := getTestFTDefaultOptions(1)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	setMockedStore(s, ms)
	waitForElection()
	checkState(t, s, FTStandby)
	// Now shutdown and check state
	s.Shutdown()
	checkState(t, s, Shutdown)
}

func TestFTFailureToCreateRPCWhenInStandby(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a mock store that will return that store is locked
	ms := &mockStore{result: false}

	// For this test, use separate NATS server
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestFTDefaultOptions(1)
	opts.NATSServerURL = "nats://localhost:4222"
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	setMockedStore(s, ms)
	waitForElection()
	// Shutdown NATS Server
	ns.Shutdown()
	// Wait: the standby should not be able to become active
	// due to the lack of NATS server
	time.Sleep(2 * graft.MAX_ELECTION_TIMEOUT)
	checkState(t, s, FTStandby)
}

func TestFTFailureToUpdateLogfile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Cause an error trying to update the FT log file.
	opts := getTestFTDefaultOptions(1)
	opts.FTQuorum = 2
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	waitForElection()
	ftLogName := filepath.Join(defaultDataStore, "ft1.log")
	os.Chmod(ftLogName, 0400)
	defer os.Chmod(ftLogName, 0666)
	waitForElection()
	checkState(t, s, FTFailed)
	if err := s.FTStartupError(); err == nil || !strings.Contains(err.Error(), "ft: error") {
		t.Fatalf("Expected error about ft error, got: %v", err)
	}
}

func TestFTSteppingDown(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// On Windows, the second server would not be able to get the store
	// lock, even though we are in the same process. So use a mocked store
	// for the second server.
	ms := &mockStore{result: true}

	ns1Opts := natsdTest.DefaultTestOptions
	// Prevent streaming servers to reconnect to other NATS server
	ns1Opts.Cluster.NoAdvertise = true
	ns1Opts.Cluster.Port = 6222
	ns1Opts.RoutesStr = "nats://localhost:6223"
	ns1Opts.Routes = natsd.RoutesFromStr(ns1Opts.RoutesStr)
	ns1 := natsdTest.RunServer(&ns1Opts)
	defer ns1.Shutdown()

	ns2Opts := natsdTest.DefaultTestOptions
	ns2Opts.Port = 4223
	// Prevent streaming servers to reconnect to other NATS server
	ns2Opts.Cluster.NoAdvertise = true
	ns2Opts.Cluster.Port = 6223
	ns2Opts.RoutesStr = "nats://localhost:6222"
	ns2Opts.Routes = natsd.RoutesFromStr(ns2Opts.RoutesStr)
	ns2 := natsdTest.RunServer(&ns2Opts)
	defer ns2.Shutdown()

	// Start first streaming server
	s1Opts := getTestFTDefaultOptions(1)
	s1Opts.NATSServerURL = "nats://localhost:4222"
	s1 := runServerWithOpts(t, s1Opts, nil)
	defer s1.Shutdown()
	// Wait for this server to be active
	getFTActiveServer(t, s1)
	// Start 2nd server
	s2Opts := getTestFTDefaultOptions(2)
	s2Opts.NATSServerURL = "nats://localhost:4223"
	s2 := runServerWithOpts(t, s2Opts, nil)
	defer s2.Shutdown()
	setMockedStore(s2, ms)
	waitForElection()
	checkState(t, s2, FTStandby)

	// We can't recover from panics in other go routines, so instruct
	// the FT code to not panic for the test and check for the failed state.
	noPanic = true
	defer func() {
		noPanic = false
	}()
	// Kill the NATS Server of the active server
	ns1.Shutdown()
	// Both server will become active because we are in the same
	// process, the file lock is not helping
	waitForElection()
	checkState(t, s1, FTActive)
	checkState(t, s2, FTActive)
	// Now restart ns1
	ns1 = natsdTest.RunServer(&ns1Opts)
	defer ns1.Shutdown()
	// Wait for route to be re-established and an election to succeed.
	// So for reliability, let's wait for 2 elections.
	waitForElection()
	waitForElection()
	// At this point, it is possible that if both servers have the same
	// RAFT term, they vote for themselves and reject each other vote
	// and become followers? Running on Travis seem to show that this
	// can be the case. So we can't be sure who is going to win and
	// both may be possibly abort.
	srvInExpectedState := func(s *StanServer) {
		if s.State() == FTFailed {
			if err := s.FTStartupError(); err == nil || !strings.Contains(err.Error(), "demoted") {
				stackFatalf(t, "Expected error about server being demoted, got %v", err)
			}
		} else if s.State() != FTActive {
			stackFatalf(t, "Should be either active or failed, got: %v", s.State().String())
		}
	}
	srvInExpectedState(s1)
	srvInExpectedState(s2)
}
