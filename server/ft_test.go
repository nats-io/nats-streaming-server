// Copyright 2017-2019 The NATS Authors
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
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

// A mock store that we use to override GetExclusiveLock() behavior.
type ftMockStore struct {
	// We need to embed Store
	stores.Store
	sync.Mutex
	result bool
	err    error
}

func (ms *ftMockStore) GetExclusiveLock() (bool, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.result, ms.err
}

func replaceWithMockedStore(s *StanServer, result bool, err error) {
	s.mu.Lock()
	ms := &ftMockStore{
		Store:  s.store,
		result: result,
		err:    err,
	}
	s.store = ms
	s.mu.Unlock()
}

func setMockedStoreVals(s *StanServer, result bool, err error) {
	s.mu.Lock()
	ms := s.store.(*ftMockStore)
	s.mu.Unlock()
	ms.Lock()
	ms.result = result
	ms.err = err
	ms.Unlock()
}

func setFTTestsHBInterval() {
	ftHBInterval = 50 * time.Millisecond
	ftHBMissedInterval = 75 * time.Millisecond
}

func getTestFTDefaultOptions() *Options {
	// We support only FileStore for now, so set the options directly here.
	opts := GetDefaultOptions()
	opts.StoreType = persistentStoreType
	if persistentStoreType == stores.TypeFile {
		opts.FilestoreDir = defaultDataStore
		opts.FileStoreOpts.BufferSize = 1024
	} else {
		opts.SQLStoreOpts.Driver = testSQLDriver
		opts.SQLStoreOpts.Source = testSQLSource
		opts.SQLStoreOpts.NoCaching = true
		opts.SQLStoreOpts.MaxOpenConns = 5
	}
	opts.FTGroupName = "ft"
	return opts
}

func cleanupFTDatastore(t tLogger) {
	cleanupDatastore(t)
}

func delayFirstLockAttempt() {
	ftPauseBeforeFirstAttempt = true
}

func cancelFirstLockAttemptDelay() {
	ftPauseBeforeFirstAttempt = false
}

func TestFTConfig(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	opts := GetDefaultOptions()
	opts.FTGroupName = "ft"
	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Server should have failed to start with FT mode and MemStore")
	}

	opts = getTestFTDefaultOptions()
	// set wrong ftHBMissedInterval and ftHBInterval values
	defer setFTTestsHBInterval()
	ftHBMissedInterval = time.Duration(float64(ftHBInterval) * 1.05)
	s, err = RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Server should have failed to start due to incorrect HB values")
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
		time.Sleep(ftHBMissedInterval)
	}
	if active == nil {
		stackFatalf(t, "Unable to find the active server")
	}
	return active
}

func TestFTBasic(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	// For this test, use a central NATS server
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	delayFirstLockAttempt()
	defer cancelFirstLockAttemptDelay()

	// Configure first server
	s1sOpts := getTestFTDefaultOptions()
	s1sOpts.NATSServerURL = "nats://localhost:4222"
	s1, err := RunServerWithOpts(s1sOpts, nil)
	if err != nil {
		t.Fatalf("Error starting server: %v", err)
	}
	ftReleasePause()
	defer s1.Shutdown()

	// Configure second server
	s2sOpts := getTestFTDefaultOptions()
	s2sOpts.NATSServerURL = "nats://localhost:4222"
	s2, err := RunServerWithOpts(s2sOpts, nil)
	if err != nil {
		t.Fatalf("Error starting server: %v", err)
	}
	replaceWithMockedStore(s2, false, nil)
	ftReleasePause()
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
	setMockedStoreVals(s2, true, nil)
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
	timeout := time.Now().Add(5 * time.Second)
	ok := false
	var state State
	var stateStr string
	for time.Now().Before(timeout) {
		state = s.State()
		stateStr = s.State().String()
		if state == expectedState && stateStr == expectedState.String() {
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		if state != expectedState {
			stackFatalf(t, "Expected server state to be %v, got %v (ft error=%v)",
				expectedState.String(), state.String(), s.LastError())
		}
		// Repeat test with String() too...
		if stateStr != expectedState.String() {
			stackFatalf(t, "Expected server state to be %v, got %v (ft error=%v)",
				expectedState.String(), stateStr, s.LastError())
		}
	}
}

func waitForGetLockAttempt() {
	time.Sleep(time.Duration(float64(ftHBMissedInterval)*1.1) + 25*time.Millisecond)
}

func TestFTCanStopFTStandby(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	delayFirstLockAttempt()
	defer cancelFirstLockAttemptDelay()

	opts := getTestFTDefaultOptions()
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	replaceWithMockedStore(s, false, nil)
	ftReleasePause()

	// Make sure that it did not become active, even after more than the
	// attempt to get the store lock.
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
	if err := s.LastError(); err != nil {
		t.Fatalf("FT startup error should be nil, got: %v", err)
	}
}

var ftPartitionDB = flag.String("ft_partition", "", "")

func TestFTPartition(t *testing.T) {
	ds := *ftPartitionDB
	parentProcess := ds == ""
	nOpts := natsdTest.DefaultTestOptions
	var natsURL string
	if parentProcess {
		ds = defaultDataStore
		cleanupFTDatastore(t)
		defer cleanupFTDatastore(t)

		nOpts.Cluster.ListenStr = "nats://localhost:6222"
		nOpts.RoutesStr = "nats://localhost:6223"
		natsURL = "nats://localhost:4222"

		// Use a separate NATS server for communication between
		// processes for the test
		ipcOpts := natsdTest.DefaultTestOptions
		ipcOpts.Port = 5222
		ipcNATS := natsdTest.RunServer(&ipcOpts)
		defer ipcNATS.Shutdown()
	} else {
		nOpts.Port = 4223
		nOpts.Cluster.ListenStr = "nats://localhost:6223"
		nOpts.RoutesStr = "nats://localhost:6222"
		natsURL = "nats://localhost:4223"
	}
	// Create NATS client just for synchronization between the
	// two processes.
	syncNC, err := nats.Connect("nats://localhost:5222")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer syncNC.Close()
	psubj := "test.sync.p"
	csubj := "test.sync.c"
	var syncSub *nats.Subscription
	if parentProcess {
		syncSub, err = syncNC.SubscribeSync(psubj)
	} else {
		syncSub, err = syncNC.SubscribeSync(csubj)
	}
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Start NATS server independently
	ns := natsdTest.RunServer(&nOpts)
	defer shutdownRestartedNATSServerOnTestExit(&ns)

	sOpts := getTestFTDefaultOptions()
	sOpts.NATSServerURL = natsURL
	if persistentStoreType == stores.TypeFile {
		sOpts.FilestoreDir = ds
	}
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	if parentProcess {
		// We should be the active server
		checkState(t, s, FTActive)

		wg := sync.WaitGroup{}
		wg.Add(1)
		errCh := make(chan error, 1)
		go func() {
			defer wg.Done()
			params := []string{
				"-ft_partition", ds,
				"-test.v",
				"-test.run=TestFTPartition$",
				"-persistent_store", persistentStoreType,
			}
			// Start a process that will be the standby
			if persistentStoreType == stores.TypeSQL {
				params = append(params,
					"-sql_create_db=false",
					"-sql_delete_db=false",
					"-sql_driver", testSQLDriver,
					"-sql_source", testSQLSource,
					"-sql_source_admin", testSQLSourceAdmin,
					"-sql_db_name", testSQLDatabaseName)
			}
			out, err := exec.Command(os.Args[0], params...).CombinedOutput()
			if err != nil {
				errCh <- fmt.Errorf("Standby error: %v - %v", err, string(out))
			}
		}()

		// Wait for the other process to start as standby
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}
		// Kill our NATS server, the standby should try to become active but
		// fail due to file lock
		ns.Shutdown()
		// Notify the other process
		syncNC.Publish(csubj, nil)
		// Wait for the other process being done with checking
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}
		ns = natsdTest.RunServer(&nOpts)
		waitForGetLockAttempt()
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
		// Notify other process that we are standby
		syncNC.Publish(psubj, nil)
		// The active server's NATS server will be killed in the parent
		// process. The standby is going to try to become active, but should
		// fail. Wait for the notification.
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}
		waitForGetLockAttempt()
		checkState(t, s, FTStandby)
		// Notify that we are done.
		syncNC.Publish(psubj, nil)
	}
}

// This is same that TestFTPartition, but roles are reversed. This is mainly for
// code coverage report.
func TestFTPartitionReversed(t *testing.T) {
	ds := *ftPartitionDB
	parentProcess := ds == ""
	nOpts := natsdTest.DefaultTestOptions
	var natsURL string
	if parentProcess {
		ds = defaultDataStore
		cleanupFTDatastore(t)
		defer cleanupFTDatastore(t)

		nOpts.Cluster.ListenStr = "nats://localhost:6222"
		nOpts.RoutesStr = "nats://localhost:6223"
		natsURL = "nats://localhost:4222"

		// Use a separate NATS server for communication between
		// processes for the test
		ipcOpts := natsdTest.DefaultTestOptions
		ipcOpts.Port = 5222
		ipcNATS := natsdTest.RunServer(&ipcOpts)
		defer ipcNATS.Shutdown()
	} else {
		nOpts.Port = 4223
		nOpts.Cluster.ListenStr = "nats://localhost:6223"
		nOpts.RoutesStr = "nats://localhost:6222"
		natsURL = "nats://localhost:4223"
	}
	// Create NATS client just for synchronization between the
	// two processes.
	syncNC, err := nats.Connect("nats://localhost:5222")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer syncNC.Close()
	psubj := "test.sync.p"
	csubj := "test.sync.c"
	var syncSub *nats.Subscription
	if parentProcess {
		syncSub, err = syncNC.SubscribeSync(psubj)
	} else {
		syncSub, err = syncNC.SubscribeSync(csubj)
	}
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Start NATS server independently
	ns := natsdTest.RunServer(&nOpts)
	defer shutdownRestartedNATSServerOnTestExit(&ns)

	if parentProcess {
		wg := sync.WaitGroup{}
		wg.Add(1)
		errCh := make(chan error, 1)
		go func() {
			defer wg.Done()
			params := []string{
				"-ft_partition", ds,
				"-test.v",
				"-test.run=TestFTPartitionReversed$",
				"-persistent_store", persistentStoreType,
			}
			// Start a process that will act as the active server
			if persistentStoreType == stores.TypeSQL {
				params = append(params,
					"-sql_create_db=false",
					"-sql_delete_db=false",
					"-sql_driver", testSQLDriver,
					"-sql_source", testSQLSource,
					"-sql_source_admin", testSQLSourceAdmin,
					"-sql_db_name", testSQLDatabaseName)
			}
			out, err := exec.Command(os.Args[0], params...).CombinedOutput()
			if err != nil {
				errCh <- fmt.Errorf("Active error: %v - %v", err, string(out))
			}
		}()

		// Wait for the active server to tell us that it is active
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}

		// Now start our streaming server, it should be a standby
		sOpts := getTestFTDefaultOptions()
		sOpts.NATSServerURL = natsURL
		if persistentStoreType == stores.TypeFile {
			sOpts.FilestoreDir = ds
		}
		s := runServerWithOpts(t, sOpts, nil)
		defer s.Shutdown()
		checkState(t, s, FTStandby)

		// Let the other process know that we have checked that
		// we are standby on startup.
		syncNC.Publish(csubj, nil)

		// Wait for the signal that NATS server was stopped.
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}
		// Try to become active, but it should fail and so we should still be standby
		waitForGetLockAttempt()
		checkState(t, s, FTStandby)

		// Notify that it can restart the NATS server now
		syncNC.Publish(csubj, nil)

		// Wait for signal
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}

		// Ok, we are done, so notify the other process that it can stop.
		syncNC.Publish(csubj, nil)

		wg.Wait()
		select {
		case e := <-errCh:
			t.Fatal(e)
		default:
		}
	} else {
		sOpts := getTestFTDefaultOptions()
		sOpts.NATSServerURL = natsURL
		if persistentStoreType == stores.TypeFile {
			sOpts.FilestoreDir = ds
		}
		s := runServerWithOpts(t, sOpts, nil)
		defer s.Shutdown()

		// Wait for this process to be the active server
		checkState(t, s, FTActive)

		// Let the other process know that we are active, so it can start
		// its own streaming server
		syncNC.Publish(psubj, nil)

		// And wait to receive a notification so we can shutdown the NATS server
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}

		// Shutdown NATS server to cause standby to try to become active
		ns.Shutdown()

		// Notify the other process
		syncNC.Publish(psubj, nil)

		// Wait the green light for us to restart the NATS server
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}

		ns = natsdTest.RunServer(&nOpts)
		waitForGetLockAttempt()
		checkState(t, s, FTActive)

		// Again, notify that it was restarted.
		syncNC.Publish(psubj, nil)

		// Wait for the green light that we can stop.
		if _, err := syncSub.NextMsg(5 * time.Second); err != nil {
			t.Fatal("Did not receive notification")
		}
	}
}

func TestFTFailedStartup(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	opts := getTestFTDefaultOptions()
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
	checkState(t, s, Failed)
	if err := s.LastError(); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("Expected error regarding non matching cluster ID, got %v", err)
	}
}

func TestFTImmediateShutdownOnStartup(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	opts := getTestFTDefaultOptions()
	s := runServerWithOpts(t, opts, nil)
	s.Shutdown()
	checkState(t, s, Shutdown)
}

func TestFTGetStoreLockReturnsError(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	delayFirstLockAttempt()
	defer cancelFirstLockAttemptDelay()

	dl := &dummyLogger{}

	opts := getTestFTDefaultOptions()
	opts.CustomLogger = dl
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	replaceWithMockedStore(s, false, fmt.Errorf("on purpose"))
	ftReleasePause()
	// Wait for the firs lock attempt
	waitForGetLockAttempt()
	checkState(t, s, FTStandby)
	// We should get an error about not being able to get the store lock
	dl.Lock()
	msg := dl.msg
	dl.Unlock()
	if msg == "" || !strings.Contains(msg, "store lock") {
		t.Fatalf("Unexpected error: %v", msg)
	}
}

func TestFTStayStandbyIfStoreAlreadyLocked(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	delayFirstLockAttempt()
	defer cancelFirstLockAttemptDelay()

	opts := getTestFTDefaultOptions()
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	replaceWithMockedStore(s, false, nil)
	ftReleasePause()
	checkState(t, s, FTStandby)
	// Now shutdown and check state
	s.Shutdown()
	checkState(t, s, Shutdown)
}

func TestFTSteppingDown(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	delayFirstLockAttempt()
	defer cancelFirstLockAttemptDelay()

	ftNoPanic = true
	defer func() {
		ftNoPanic = false
	}()

	// For this test, run a central NATS server
	ns := natsdTest.RunDefaultServer()
	defer shutdownRestartedNATSServerOnTestExit(&ns)

	// Start first server
	opts1 := getTestFTDefaultOptions()
	opts1.NATSServerURL = "nats://localhost:4222"
	s1 := runServerWithOpts(t, opts1, nil)
	defer s1.Shutdown()
	ftReleasePause()
	// Wait for it to be active
	getFTActiveServer(t, s1)

	// Start 2nd server, give it a mock store that says it can get the lock
	opts2 := getTestFTDefaultOptions()
	opts2.NATSServerURL = "nats://localhost:4222"
	s2 := runServerWithOpts(t, opts2, nil)
	defer s2.Shutdown()
	replaceWithMockedStore(s2, true, nil)
	ftReleasePause()
	// Shutdown the NATS server
	ns.Shutdown()
	// Wait the next attempt to grab the lock
	waitForGetLockAttempt()
	// And restart it
	ns = natsdTest.RunDefaultServer()
	// Make sure that streaming has time to reconnect and wait for HBs
	// exchange to realize that there are 2 actives.
	time.Sleep(2 * ftHBMissedInterval)
	// Since s1 activated before s2, we want s1 to stay and s2 to exit.
	checkState(t, s1, FTActive)
	checkState(t, s2, Failed)
	if err := s2.LastError(); err == nil || !strings.Contains(err.Error(), "aborting") {
		t.Fatalf("Expected server to have exited due to both servers being active, got %v", err)
	}
}

func TestFTActiveSendsHB(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	// For this test, make the HB interval very small so that we have more
	// chance to get actual failure when we disconnect from NATS.
	ftHBInterval = time.Millisecond
	defer setFTTestsHBInterval()

	ns := natsdTest.RunDefaultServer()
	defer shutdownRestartedNATSServerOnTestExit(&ns)

	// Start Streaming server
	opts := getTestFTDefaultOptions()
	opts.NATSServerURL = "nats://localhost:4222"
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	// Wait for it to be active
	getFTActiveServer(t, s)

	s.mu.RLock()
	subj := s.ftSubject
	reconnDelay := s.ftnc.Opts.ReconnectWait
	s.mu.RUnlock()

	// setup bare NATS subscriber that checks FT hbs.
	// We use the same reconnect delay than the Streaming server
	// uses.
	rch := make(chan bool)
	nc, err := nats.Connect(nats.DefaultURL,
		nats.ReconnectWait(reconnDelay),
		nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			rch <- true
		}))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	hbarch := make(chan bool)
	reconnected := int32(0)
	hbBefore := int32(0)
	if _, err = nc.Subscribe(subj, func(m *nats.Msg) {
		if atomic.LoadInt32(&reconnected) == 1 {
			hbarch <- true
			m.Sub.Unsubscribe()
		} else {
			atomic.AddInt32(&hbBefore, 1)
		}
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	// Wait for some HBs to be sent before the disconnect
	time.Sleep(50 * time.Millisecond)
	// Shutdown the NATS server
	ns.Shutdown()
	time.Sleep(2 * ftHBMissedInterval)
	// Start again
	ns = natsdTest.RunDefaultServer()
	atomic.StoreInt32(&reconnected, 1)
	// Wait that we know we have reconnected.
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get reconnected")
	}
	// Wait to receive an HB after the reconnect
	if err := Wait(hbarch); err != nil {
		t.Fatal("Did not get our HB after reconnect")
	}
	// We should have received some before the disconnect
	if atomic.LoadInt32(&hbBefore) == 0 {
		t.Fatal("Should have received HB before the disconnect")
	}
}

func TestFTActiveReceivesInvalidHBMessages(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Start Streaming server
	opts := getTestFTDefaultOptions()
	opts.NATSServerURL = "nats://localhost:4222"
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	// Wait for it to be active
	getFTActiveServer(t, s)

	s.mu.RLock()
	subj := s.ftSubject
	s.mu.RUnlock()

	// use bare NATS connection to send fake FT HBs.
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Sending invalid HBs, waiting a bit to make sure
	// server processes it and check that it is still FTActive

	// Send nil message
	nc.Publish(subj, nil)
	time.Sleep(50 * time.Millisecond)
	checkState(t, s, FTActive)

	// Send invalid protobuf
	nc.Publish(subj, []byte("wrong msg"))
	time.Sleep(50 * time.Millisecond)
	checkState(t, s, FTActive)

	// Send valid protobuf but with invalid activation time
	hb := spb.CtrlMsg{
		MsgType:  spb.CtrlMsg_FTHeartbeat,
		ServerID: "otherserver",
		Data:     []byte("wrong time"),
	}
	bytes, _ := hb.Marshal()
	nc.Publish(subj, bytes)
	time.Sleep(50 * time.Millisecond)
	checkState(t, s, FTActive)
}
