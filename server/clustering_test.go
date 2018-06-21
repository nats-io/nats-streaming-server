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
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	natsd "github.com/nats-io/gnatsd/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/stores"
)

var defaultRaftLog string

func init() {
	tmpDir, err := ioutil.TempDir("", "raft_logs_")
	if err != nil {
		panic("Could not create tmp dir")
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp dir: %v", err))
	}
	defaultRaftLog = tmpDir
	clusterSetupForTest()
}

func cleanupRaftLog(t *testing.T) {
	if err := os.RemoveAll(defaultRaftLog); err != nil {
		stackFatalf(t, "Error cleaning up raft log: %v", err)
	}
}

func getTestDefaultOptsForClustering(id string, bootstrap bool) *Options {
	opts := getTestDefaultOptsForPersistentStore()
	if persistentStoreType == stores.TypeFile {
		opts.FilestoreDir = filepath.Join(defaultDataStore, id)
		opts.FileStoreOpts.BufferSize = 1024
	} else if persistentStoreType == stores.TypeSQL {
		// Since we need to have the databases created for all possible
		// IDs, make sure that if someone adds a test with a new ID
		// he/she adds it to the list of database names to create on
		// test startup.
		ok := false
		suffix := "_" + id
		for _, n := range testDBSuffixes {
			if suffix == n {
				ok = true
				break
			}
		}
		if !ok {
			panic(fmt.Errorf("Clustering test with node ID %q, need to create the database for that id", id))
		}
		opts.SQLStoreOpts.Source = testSQLSource + suffix
	}
	opts.Clustering.Clustered = true
	opts.Clustering.Bootstrap = bootstrap
	opts.Clustering.RaftLogPath = filepath.Join(defaultRaftLog, id)
	opts.Clustering.LogCacheSize = DefaultLogCacheSize
	opts.Clustering.LogSnapshots = 1
	opts.Clustering.RaftLogging = true
	opts.NATSServerURL = "nats://localhost:4222"
	return opts
}

func getLeader(t *testing.T, timeout time.Duration, servers ...*StanServer) *StanServer {
	var (
		leader   *StanServer
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if s.state == Shutdown || s.raft == nil {
				continue
			}
			if s.isLeader() {
				if leader != nil {
					stackFatalf(t, "Found more than one leader")
				}
				leader = s
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if leader == nil {
		for _, s := range servers {
			s.mu.Lock()
			if s.raft == nil {
				fmt.Printf("  server:%p state:%v raft state: nil lastErr=%v\n", s, s.state, s.lastError)
			} else {
				fmt.Printf("  server:%p state:%v raft state: %v lastErr=%v\n", s, s.state, s.raft.State(), s.lastError)
			}
			s.mu.Unlock()
		}
		printAllStacks()
		stackFatalf(t, "Unable to find the leader")
	}
	return leader
}

func verifyNoLeader(t *testing.T, timeout time.Duration, servers ...*StanServer) {
	deadline := time.Now().Add(timeout)
	var leader *StanServer
	for time.Now().Before(deadline) {
		for _, server := range servers {
			if server.raft == nil {
				continue
			}
			if server.isLeader() {
				leader = server
				time.Sleep(100 * time.Millisecond)
				break
			}
		}
		return
	}
	stackFatalf(t, "Found unexpected leader %q", leader.info.NodeID)
}

func checkClientsInAllServers(t *testing.T, expected int, servers ...*StanServer) {
	for _, srv := range servers {
		waitForNumClients(t, srv, expected)
	}
}

func checkChannelsInAllServers(t *testing.T, channels []string, timeout time.Duration, servers ...*StanServer) {
	deadline := time.Now().Add(timeout)
OUTER:
	for time.Now().Before(deadline) {
		for _, server := range servers {
			server.channels.RLock()
			if len(server.channels.channels) != len(channels) {
				server.channels.RUnlock()
				time.Sleep(100 * time.Millisecond)
				continue OUTER
			}
			for _, c := range channels {
				if server.channels.get(c) == nil {
					server.channels.RUnlock()
					time.Sleep(100 * time.Millisecond)
					continue OUTER
				}
			}
			server.channels.RUnlock()
		}
		return
	}
	stackFatalf(t, "Channels are inconsistent")
}

type msg struct {
	sequence uint64
	data     []byte
}

func verifyChannelConsistency(t *testing.T, channel string, timeout time.Duration,
	expectedFirstSeq, expectedLastSeq uint64, expectedMsgs map[uint64]msg, servers ...*StanServer) {
	deadline := time.Now().Add(timeout)
OUTER:
	for time.Now().Before(deadline) {
		for _, server := range servers {
			c := server.channels.get(channel)
			if c == nil {
				time.Sleep(15 * time.Millisecond)
				continue OUTER
			}
			store := c.store.Msgs
			first, last, err := store.FirstAndLastSequence()
			if err != nil {
				stackFatalf(t, "Error getting sequence numbers: %v", err)
			}
			if first != expectedFirstSeq {
				time.Sleep(15 * time.Millisecond)
				continue OUTER
			}
			if last != expectedLastSeq {
				time.Sleep(15 * time.Millisecond)
				continue OUTER
			}
			for i := first; i <= last; i++ {
				msg, err := store.Lookup(i)
				if err != nil {
					stackFatalf(t, "Error getting message %d: %v", i, err)
				}
				if msg == nil {
					stackFatalf(t, "No stored message for i=%v expected=%v", i, expectedMsgs[i])
				}
				assertMsg(t, *msg, expectedMsgs[i].data, expectedMsgs[i].sequence)
			}
		}
		return
	}
	stackFatalf(t, "Message stores are inconsistent")
}

func removeServer(servers []*StanServer, s *StanServer) []*StanServer {
	for i, srv := range servers {
		if srv == s {
			servers = append(servers[:i], servers[i+1:]...)
		}
	}
	return servers
}

func assertMsg(t *testing.T, msg pb.MsgProto, expectedData []byte, expectedSeq uint64) {
	if msg.Sequence != expectedSeq {
		stackFatalf(t, "Msg sequence incorrect, expected: %d, got: %d", expectedSeq, msg.Sequence)
	}
	if !bytes.Equal(msg.Data, expectedData) {
		stackFatalf(t, "Msg data incorrect, expected: %s, got: %s", expectedData, msg.Data)
	}
}

func TestClusteringMemoryStoreNotSupported(t *testing.T) {
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// Configure the server in non-clustered mode.
	opts := getTestDefaultOptsForClustering("a", true)
	opts.NATSServerURL = ""
	opts.StoreType = stores.TypeMemory
	s, err := RunServerWithOpts(opts, nil)
	if err == nil {
		if s != nil {
			s.Shutdown()
		}
		t.Fatal("Expected error got none")
	}
	if !strings.Contains(err.Error(), stores.TypeMemory) {
		t.Fatalf("Expected error about MEMORY store not supported, got %v", err)
	}
}

// Ensure restarting a non-clustered server in clustered mode fails.
func TestClusteringRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure the server in non-clustered mode.
	s1sOpts := getTestDefaultOptsForClustering("a", false)
	s1sOpts.Clustering.Clustered = false
	s1 := runServerWithOpts(t, s1sOpts, nil)

	// Restart in clustered mode. This should fail.
	s1.Shutdown()
	s1sOpts.Clustering.Clustered = true
	_, err := RunServerWithOpts(s1sOpts, nil)
	if err == nil {
		t.Fatal("Expected error on server start")
	}
	if err != ErrClusteredRestart {
		t.Fatalf("Incorrect error, expected: ErrClusteredRaftRestart, got: %v", err)
	}
}

// Ensure starting a clustered node fails when there is no seed node to join.
func TestClusteringNoSeed(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server. Starting this should fail because there is no
	// seed node.
	s1sOpts := getTestDefaultOptsForClustering("a", false)
	if _, err := RunServerWithOpts(s1sOpts, nil); err == nil {
		t.Fatal("Expected error on server start")
	}
}

// Ensure clustering node ID is assigned when not provided and stored/recovered
// on server restart.
func TestClusteringAssignedDurableNodeID(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Wait to elect self as leader.
	leader := getLeader(t, 10*time.Second, s1)

	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	id := future.Configuration().Servers[0].ID

	if id == "" {
		t.Fatal("Expected non-empty cluster node id")
	}

	// Restart server without setting node ID.
	s1.Shutdown()
	s1sOpts.Clustering.NodeID = ""
	s1 = runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Wait to elect self as leader.
	leader = getLeader(t, 10*time.Second, s1)

	future = leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	newID := future.Configuration().Servers[0].ID
	if id != newID {
		t.Fatalf("Incorrect cluster node id, expected: %s, got: %s", id, newID)
	}
}

// Ensure clustering node ID is stored and recovered on server restart.
func TestClusteringDurableNodeID(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure server.
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.NodeID = "a"
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Wait to elect self as leader.
	leader := getLeader(t, 10*time.Second, s1)

	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	id := future.Configuration().Servers[0].ID

	if id != "a" {
		t.Fatalf("Incorrect cluster node id, expected: a, got: %s", id)
	}

	// Restart server without setting node ID.
	s1.Shutdown()
	s1sOpts.Clustering.NodeID = ""
	s1 = runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Wait to elect self as leader.
	leader = getLeader(t, 10*time.Second, s1)

	future = leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	newID := future.Configuration().Servers[0].ID
	if newID != "a" {
		t.Fatalf("Incorrect cluster node id, expected: a, got: %s", newID)
	}
}

// Ensure starting a cluster with auto configuration works when we start one
// node in bootstrap mode.
func TestClusteringBootstrapAutoConfig(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server as a seed.
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server which should automatically join the first.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	var (
		servers = []*StanServer{s1, s2}
		leader  = getLeader(t, 10*time.Second, servers...)
	)

	// Verify configuration.
	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	configServers := future.Configuration().Servers
	if len(configServers) != 2 {
		t.Fatalf("Expected 2 servers, got %d", len(configServers))
	}
}

// Ensure starting a cluster with manual configuration works when we provide
// the cluster configuration to each server.
func TestClusteringBootstrapManualConfig(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server.
	s1sOpts := getTestDefaultOptsForClustering("a", false)
	s1sOpts.Clustering.NodeID = "a"
	s1sOpts.Clustering.Peers = []string{"b"}
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.NodeID = "b"
	s2sOpts.Clustering.Peers = []string{"a"}
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	var (
		servers = []*StanServer{s1, s2}
		leader  = getLeader(t, 10*time.Second, servers...)
	)

	// Verify configuration.
	future := leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	configServers := future.Configuration().Servers
	if len(configServers) != 2 {
		t.Fatalf("Expected 2 servers, got %d", len(configServers))
	}

	// Ensure new servers can automatically join once the cluster is formed.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	future = leader.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("Unexpected error on GetConfiguration: %v", err)
	}
	configServers = future.Configuration().Servers
	if len(configServers) != 3 {
		t.Fatalf("Expected 3 servers, got %d", len(configServers))
	}
}

func TestClusteringBootstrapMisconfiguration(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// The first server will create route to second server.
	// This is to test that a server may not be able to detect
	// the misconfiguration right away.

	// Configure first server as a seed.
	n1Opts := natsdTest.DefaultTestOptions
	n1Opts.Host = "127.0.0.1"
	n1Opts.Port = 4222
	n1Opts.Cluster.Host = "127.0.0.1"
	n1Opts.Cluster.Port = 6222
	n1Opts.Routes = natsd.RoutesFromStr("nats://127.0.0.1:6223")
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.NATSServerURL = ""
	s1Logger := &captureFatalLogger{}
	s1sOpts.CustomLogger = s1Logger
	s1sOpts.EnableLogging = true
	s1 := runServerWithOpts(t, s1sOpts, &n1Opts)
	defer s1.Shutdown()

	getLeader(t, 10*time.Second, s1)

	// Configure second server on same cluster as a seed too.
	// Servers should stop.
	n2Opts := natsdTest.DefaultTestOptions
	n2Opts.Host = "127.0.0.1"
	n2Opts.Port = 4223
	n2Opts.Cluster.Host = "localhost"
	n2Opts.Cluster.Port = 6223
	s2sOpts := getTestDefaultOptsForClustering("b", true)
	s2sOpts.NATSServerURL = ""
	s2Logger := &captureFatalLogger{}
	s2sOpts.CustomLogger = s2Logger
	s2sOpts.EnableLogging = true
	s2 := runServerWithOpts(t, s2sOpts, &n2Opts)
	defer s2.Shutdown()

	// After a little while, servers should detect that they were
	// both started with the bootstrap flag and exit.
	ok := false
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		check := func(otherServer *StanServer, l *captureFatalLogger) bool {
			l.Lock()
			defer l.Unlock()
			if l.fatal != "" {
				if strings.Contains(l.fatal, otherServer.serverID) {
					return true
				}
			}
			return false
		}
		ok = check(s1, s2Logger)
		if ok {
			ok = check(s2, s1Logger)
		}
		if ok {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !ok {
		t.Fatal("Servers should have reported fatal error")
	}
}

// Ensure basic replication works as expected. This test starts three servers
// in a cluster, publishes messages to the cluster, kills the leader, publishes
// more messages, kills the new leader, verifies progress cannot be made when
// there is no leader, then brings the cluster back online and verifies
// catchup and consistency.
func TestClusteringBasic(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message (this will create the channel and form the Raft group).
	channel := "foo"
	if err := sc.Publish(channel, []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	ch := make(chan *stan.Msg, 100)
	sub, err := sc.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DeliverAllAvailable(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}

	select {
	case msg := <-ch:
		assertMsg(t, msg.MsgProto, []byte("hello"), 1)
	case <-time.After(2 * time.Second):
		t.Fatal("expected msg")
	}

	sub.Unsubscribe()

	stopped := []*StanServer{}

	// Take down the leader.
	leader := getLeader(t, 10*time.Second, servers...)
	leader.Shutdown()
	stopped = append(stopped, leader)
	servers = removeServer(servers, leader)

	// Wait for the new leader to be elected.
	leader = getLeader(t, 10*time.Second, servers...)

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("Unexpected error on publish %d: %v", i, err)
		}
	}

	// Read everything back from the channel.
	sub, err = sc.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DeliverAllAvailable(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	select {
	case msg := <-ch:
		assertMsg(t, msg.MsgProto, []byte("hello"), 1)
	case <-time.After(2 * time.Second):
		t.Fatal("expected msg")
	}
	for i := 0; i < 5; i++ {
		select {
		case msg := <-ch:
			assertMsg(t, msg.MsgProto, []byte(strconv.Itoa(i)), uint64(i+2))
		case <-time.After(2 * time.Second):
			t.Fatal("expected msg")
		}
	}

	sub.Unsubscribe()

	// Take down the leader.
	leader.Shutdown()
	stopped = append(stopped, leader)
	servers = removeServer(servers, leader)

	// Creating a new connection should fail since there should not be a leader.
	_, err = stan.Connect(clusterName, clientName+"-2", stan.PubAckWait(time.Second), stan.ConnectWait(time.Second))
	if err == nil {
		t.Fatal("Expected error on connect")
	}

	// Bring one node back up.
	s := stopped[0]
	stopped = stopped[1:]
	s = runServerWithOpts(t, s.opts, nil)
	servers = append(servers, s)
	defer s.Shutdown()

	// Wait for the new leader to be elected.
	getLeader(t, 10*time.Second, servers...)

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte("foo-"+strconv.Itoa(i))); err != nil {
			t.Fatalf("Unexpected error on publish %d: %v", i, err)
		}
	}

	// Bring the last node back up.
	s = stopped[0]
	s = runServerWithOpts(t, s.opts, nil)
	servers = append(servers, s)
	defer s.Shutdown()

	// Ensure there is still a leader.
	getLeader(t, 10*time.Second, servers...)

	// Publish one more message.
	if err := sc.Publish(channel, []byte("goodbye")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify the server stores are consistent.
	expected := make(map[uint64]msg, 12)
	expected[1] = msg{sequence: 1, data: []byte("hello")}
	for i := uint64(0); i < 5; i++ {
		expected[i+2] = msg{sequence: uint64(i + 2), data: []byte(strconv.Itoa(int(i)))}
	}
	for i := uint64(0); i < 5; i++ {
		expected[i+7] = msg{sequence: uint64(i + 7), data: []byte("foo-" + strconv.Itoa(int(i)))}
	}
	expected[12] = msg{sequence: 12, data: []byte("goodbye")}
	verifyChannelConsistency(t, channel, 10*time.Second, 1, 12, expected, servers...)

	sc.Close()
}

func TestClusteringNoPanicOnShutdown(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName, stan.PubAckWait(time.Second))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()

	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Unsubscribe since this is not about that
	sub.Unsubscribe()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			if err := sc.Publish("foo", []byte("msg")); err != nil {
				return
			}
		}
	}()

	// Wait so that go-routine is in middle of sending messages
	time.Sleep(time.Duration(rand.Intn(500)+100) * time.Millisecond)

	// We shutdown the follower, it should not panic.
	follower := s1
	if s1 == leader {
		follower = s2
	}
	follower.Shutdown()
	wg.Wait()

	// Restart follower to speed up client disconnect.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()

	// Close client explicitly otherwise follower will be shutdown
	// first and then client close will timeout since only 1 node
	// in cluster (no leader)
	sc.Close()
}

func TestClusteringLeaderFlap(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName, stan.PubAckWait(2*time.Second))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()

	// Publish a message (this will create the channel and form the Raft group).
	channel := "foo"
	if err := sc.Publish(channel, []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Kill the follower.
	var follower *StanServer
	if s1 == leader {
		s2.Shutdown()
		follower = s2
	} else {
		s1.Shutdown()
		follower = s1
	}
	servers = removeServer(servers, follower)

	// Ensure there is no leader now.
	verifyNoLeader(t, 5*time.Second, s1, s2)

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)

	// Ensure there is a new leader.
	getLeader(t, 10*time.Second, servers...)
}

func TestClusteringDontRecoverFSClientsAndSubs(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}
	getLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName, stan.ConnectWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {},
		stan.DurableName("du")); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	s1.Shutdown()
	s2.Shutdown()

	s1 = runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	clients := s1.clients.getClients()
	if len(clients) != 0 {
		t.Fatalf("Should not have recovered clients from store, got %v", clients)
	}

	c := s1.channels.get("foo")
	c.ss.RLock()
	dur := c.ss.durables
	c.ss.RUnlock()
	if len(dur) != 0 {
		t.Fatalf("Should not have recovered subscription from store, got %v", dur)
	}
	sc.Close()
}

func TestClusteringLogSnapshotRestore(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.Clustering.TrailingLogs = 0
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message.
	channel := "foo"
	if err := sc.Publish(channel, []byte("1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Create a subscription.
	sub, err := sc.Subscribe(channel, func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i+2))); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	// Kill a follower.
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
	servers = removeServer(servers, follower)
	follower.Shutdown()

	// Publish some more messages.
	moreMsgsCount := 200
	for i := 0; i < moreMsgsCount; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i+7))); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	// Create two more subscriptions.
	if _, err := sc.Subscribe(channel, func(_ *stan.Msg) {}, stan.DurableName("durable")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if _, err := sc.Subscribe(channel, func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// And a queue subscription
	if _, err := sc.QueueSubscribe(channel, "queue", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Unsubscribe the previous subscription.
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}

	// Force a log compaction on the leader.
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)

	// Ensure there is a leader before publishing.
	getLeader(t, 10*time.Second, servers...)

	// Publish a message to force a timely catch up.
	if err := sc.Publish(channel, []byte(strconv.Itoa(moreMsgsCount+7))); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify the server stores are consistent.
	totalMsgs := uint64(moreMsgsCount + 7)
	expected := make(map[uint64]msg, totalMsgs)
	for i := uint64(0); i < totalMsgs; i++ {
		expected[i+1] = msg{sequence: uint64(i + 1), data: []byte(strconv.Itoa(int(i + 1)))}
	}
	verifyChannelConsistency(t, channel, 10*time.Second, 1, totalMsgs, expected, servers...)

	// Verify subscriptions are consistent.
	for _, srv := range servers {
		waitForNumSubs(t, srv, clientName, 3)
	}
}

func TestClusteringLogSnapshotRestoreAfterChannelLimitHit(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1sOpts.MaxMsgs = 20
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2sOpts.MaxMsgs = 20
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.Clustering.TrailingLogs = 0
	s3sOpts.MaxMsgs = 20
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	channel := "foo"
	// Publish some messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i+1))); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	// Kill a follower.
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
	follower.Shutdown()
	servers = removeServer(servers, follower)

	// Publish 5 more messages before doing a raft log snapshot
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i+6))); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	// Force a log compaction on the leader.
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Now publish more messages so that we cause the 10 first messages to be
	// discarded due to channel limits
	for i := 0; i < 30; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i+11))); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	// At this point, the follower should only have messages 1..5 on its log,
	// the leader snapshot will have 1..10, but the message log has 20..40.

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)
	// Force another log compaction on the leader.
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Verify the server stores are consistent.
	totalMsgs := uint64(s1sOpts.MaxMsgs)
	expected := make(map[uint64]msg, totalMsgs)
	for i := uint64(0); i < totalMsgs; i++ {
		expected[i+21] = msg{sequence: uint64(i + 21), data: []byte(strconv.Itoa(int(i + 21)))}
	}
	verifyChannelConsistency(t, channel, 10*time.Second, 21, 40, expected, servers...)
}

func TestClusteringLogSnapshotRestoreSubAcksPending(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.Clustering.TrailingLogs = 0
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Create a subscription. (this will create the channel and form the Raft group).
	var (
		ch      = make(chan *stan.Msg, 1)
		channel = "foo"
	)
	_, err = sc.Subscribe(channel, func(msg *stan.Msg) {
		if !msg.Redelivered {
			// Do not ack.
			ch <- msg
		}
	}, stan.DeliverAllAvailable(), stan.SetManualAckMode(), stan.AckWait(time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Kill a follower.
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
	servers = removeServer(servers, follower)
	follower.Shutdown()

	// Publish a message.
	if err := sc.Publish(channel, []byte("1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify we received the message.
	select {
	case msg := <-ch:
		assertMsg(t, msg.MsgProto, []byte("1"), 1)
	case <-time.After(2 * time.Second):
		t.Fatal("expected msg")
	}

	// Force a log compaction on the leader.
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)

	// Ensure there is a leader before publishing.
	getLeader(t, 10*time.Second, servers...)

	// Publish a message to force a timely catch up.
	if err := sc.Publish(channel, []byte("2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	select {
	case msg := <-ch:
		assertMsg(t, msg.MsgProto, []byte("2"), 2)
	case <-time.After(2 * time.Second):
		t.Fatal("expected msg")
	}

	// Verify the server stores are consistent.
	totalMsgs := uint64(2)
	expected := make(map[uint64]msg, totalMsgs)
	for i := uint64(0); i < totalMsgs; i++ {
		expected[i+1] = msg{sequence: uint64(i + 1), data: []byte(strconv.Itoa(int(i + 1)))}
	}
	verifyChannelConsistency(t, channel, 10*time.Second, 1, totalMsgs, expected, servers...)

	waitForAcks(t, follower, clientName, 1, 2)

	sc.Close()
}

func TestClusteringLogSnapshotRestoreConnections(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.Clustering.TrailingLogs = 0
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc1, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc1.Close()

	// Create a subscription.
	var (
		ch      = make(chan *stan.Msg, 1)
		channel = "foo"
	)
	sub1, err := sc1.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DeliverAllAvailable(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub1.Unsubscribe()

	// Publish a message.
	if err := sc1.Publish(channel, []byte("1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify we received the message.
	select {
	case msg := <-ch:
		assertMsg(t, msg.MsgProto, []byte("1"), 1)
	case <-time.After(2 * time.Second):
		t.Fatal("expected msg")
	}

	// Create another subscription.
	sub2, err := sc1.Subscribe("bar", func(msg *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// Create another client connection.
	sc2, err := stan.Connect(clusterName, "bob")
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()

	// Ensure clients are consistent across servers.
	checkClientsInAllServers(t, 2, servers...)

	// Ensure subs are consistent across servers.
	for _, srv := range servers {
		waitForNumSubs(t, srv, clientName, 2)
		waitForNumSubs(t, srv, "bob", 0)
	}

	// Kill a follower.
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
	follower.Shutdown()
	servers = removeServer(servers, follower)

	// Create one more client connection.
	sc3, err := stan.Connect(clusterName, "alice")
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc3.Close()

	// Close one client.
	if err := sc2.Close(); err != nil {
		t.Fatalf("Unexpected error on close: %v", err)
	}

	// Force a log compaction on the leader.
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)

	// Ensure there is a leader.
	getLeader(t, 10*time.Second, servers...)

	// Create one last client connection to force a timely catch up.
	sc4, err := stan.Connect(clusterName, "tyler")
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc4.Close()

	// Ensure clients are consistent across servers.
	checkClientsInAllServers(t, 3, servers...)

	// Ensure subs are consistent across servers.
	for _, srv := range servers {
		waitForNumSubs(t, srv, clientName, 2)
		waitForNumSubs(t, srv, "alice", 0)
		waitForNumSubs(t, srv, "tyler", 0)
	}
}

func TestClusteringLogSnapshotDoNotRestoreMsgsFromOwnSnapshot(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)
	follower := s1
	if leader == s1 {
		follower = s2
	}

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	for i := 0; i < 100; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Error on publish")
		}
	}
	sc.Close()

	// Cause a log compaction on the follower
	if err := follower.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Error during snapshot: %v", err)
	}

	// Shutdown both servers.
	s1.Shutdown()
	s2.Shutdown()

	// If we are able to restart, it means that we were able to
	// recover from our own snapshot without error (the issue
	// would be if server was trying to recover through NATS
	// the snapshot from a peer).
	follower = runServerWithOpts(t, follower.opts, nil)
	follower.Shutdown()
}

func TestClusteringLogSnapshotRestoreClosedDurables(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}
	leader := getLeader(t, 10*time.Second, servers...)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("1")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	ch := make(chan bool, 2)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) { ch <- true },
		stan.DurableName("dur"), stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if _, err := sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) { ch <- true },
		stan.DurableName("dur"), stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Wait for each to receive the message
	for i := 0; i < 2; i++ {
		if err := Wait(ch); err != nil {
			t.Fatal("Did not receive our message")
		}
	}
	// Close the subs by closing the connection
	sc.Close()

	// Force a snapshot
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Error during snapshot: %v", err)
	}

	s1.Shutdown()
	s2.Shutdown()

	// Restart them
	s1 = runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	s2 = runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers = []*StanServer{s1, s2}
	getLeader(t, 10*time.Second, servers...)

	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Send the second message
	if err := sc.Publish("foo", []byte("2")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	msgChan := make(chan *stan.Msg, 1)
	// Re-open the durable
	if _, err := sc.Subscribe("foo", func(m *stan.Msg) { msgChan <- m },
		stan.DurableName("dur"), stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	select {
	case m := <-msgChan:
		assertMsg(t, m.MsgProto, []byte("2"), 2)
	case <-time.After(2 * time.Second):
		t.Fatalf("Should have received a message")
	}
	// Re-open the queue durable
	if _, err := sc.QueueSubscribe("foo", "queue", func(m *stan.Msg) { msgChan <- m },
		stan.DurableName("dur"), stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	select {
	case m := <-msgChan:
		assertMsg(t, m.MsgProto, []byte("2"), 2)
	case <-time.After(2 * time.Second):
		t.Fatalf("Should have received a message")
	}

	sc.Close()
}

func TestClusteringLogSnapshotRestoreNoSubIDCollision(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}
	leader := getLeader(t, 10*time.Second, servers...)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Create 1 regular and 1 durable subscriptions
	sub1, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	sub2, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Close them
	sub1.Close() // this one will disappear since it was not durable
	sub2.Close() // this one will stay (but closed)

	sc.Close()

	// Get the durable subscription ID
	var durSubID uint64
	c := leader.channels.get("foo")
	c.ss.RLock()
	for _, dur := range c.ss.durables {
		dur.RLock()
		durSubID = dur.ID
		dur.RUnlock()
	}
	c.ss.RUnlock()

	// Force a snapshot
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Error during snapshot: %v", err)
	}

	s1.Shutdown()
	s2.Shutdown()

	// Restart them
	s1 = runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	s2 = runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers = []*StanServer{s1, s2}
	leader = getLeader(t, 10*time.Second, servers...)

	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Create 2 new subscriptions
	for i := 0; i < 2; i++ {
		if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
	}

	// Get these new subscriptions
	subs := checkSubs(t, leader, clientName, 2)
	for _, sub := range subs {
		sub.RLock()
		sid := sub.ID
		sub.RUnlock()
		if sid == durSubID {
			t.Fatalf("One of the new subscription got an ID same as the closed durable: %v", sid)
		}
	}

	sc.Close()
}

func verifyChannelExist(t *testing.T, s *StanServer, channel string, expectedToExist bool, deadline time.Duration) {
	timeout := time.Now().Add(deadline)
	for time.Now().Before(timeout) {
		c := s.channels.get(channel)
		if expectedToExist && c != nil {
			return
		} else if !expectedToExist && c == nil {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	expStr := "expected"
	if !expectedToExist {
		expStr = "not expected"
	}
	stackFatalf(t, "Channel %q was %s to exist on server %v", channel, expStr, s.info.NodeID)
}

func TestClusteringLogSnapshotRestoreAfterChannelDeleted(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	maxInactivity := 250 * time.Millisecond

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1sOpts.MaxInactivity = maxInactivity
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2sOpts.MaxInactivity = maxInactivity
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.Clustering.TrailingLogs = 0
	s3sOpts.MaxInactivity = maxInactivity
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Send a message, which will create the channel
	channel := "foo"
	expectedMsg := make(map[uint64]msg)
	expectedMsg[1] = msg{sequence: 1, data: []byte("first")}
	if err := sc.Publish(channel, expectedMsg[1].data); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Wait for channel to be replicated in all servers
	verifyChannelConsistency(t, channel, 5*time.Second, 1, 1, expectedMsg, servers...)

	// Kill a follower.
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
	servers = removeServer(servers, follower)
	follower.Shutdown()

	// Wait for more than the MaxInactivity
	time.Sleep(2 * maxInactivity)
	// Check channel is no longer in leader
	verifyChannelExist(t, leader, channel, false, 5*time.Second)
	// Perform a snapshot after the channel has been deleted
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Error on snapshot: %v", err)
	}

	// Restart the follower
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)

	getLeader(t, 10*time.Second, servers...)

	// Produce some activity to speed up snapshot recovery
	sc.Publish("bar", []byte("hello"))

	// The follower will have recovered foo, but then from
	// the snapshot should realize that the channel no longer
	// exits and should delete it.
	verifyChannelExist(t, follower, channel, false, 5*time.Second)
	sc.Close()
}

func TestClusteringLogSnapshotRestoreOnInit(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForClustering("a", true)
	opts.Clustering.TrailingLogs = 0
	opts.Clustering.LogSnapshots = 5
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	getLeader(t, 10*time.Second, s)

	sc := NewDefaultConnection(t)
	defer sc.Close()
	for i := 0; i < 10; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}
	sc.Close()

	if err := s.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	sc = NewDefaultConnection(t)
	if err := s.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Snapshot error: %v", err)
	}
	sc.Close()

	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	getLeader(t, 10*time.Second, s)

	s.raft.fsm.Lock()
	soi := s.raft.fsm.snapshotsOnInit
	s.raft.fsm.Unlock()
	if soi != 0 {
		t.Fatalf("snapshotsOnInit should be 0, got %v", soi)
	}
}

type checkRestoreLogger struct {
	dummyLogger
	ch chan error
}

func (l *checkRestoreLogger) Errorf(format string, args ...interface{}) {
	m := fmt.Errorf(format, args...)
	l.ch <- m
}

func TestClusteringLogSnapshotRestoreBatching(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.TrailingLogs = 0
	s1sOpts.MaxMsgs = 234
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.TrailingLogs = 0
	s2sOpts.MaxMsgs = 234
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.Clustering.TrailingLogs = 0
	s3sOpts.MaxMsgs = 234
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish 100 messages
	firstCount := 100
	for i := 0; i < firstCount; i++ {
		if err := sc.Publish("foo", []byte(strconv.Itoa(i+1))); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Kill a follower
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
	servers = removeServer(servers, follower)
	follower.Shutdown()

	// Publish more messages while the follower is down.
	moreMsgs := 500
	for i := 0; i < moreMsgs; i++ {
		if err := sc.Publish("foo", []byte(strconv.Itoa(i+firstCount+1))); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Force snapshot
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Error during snapshot: %v", err)
	}

	// Restart follower
	cl := &checkRestoreLogger{ch: make(chan error, 10)}
	follower.opts.Clustering.RaftLogging = true
	follower.opts.CustomLogger = cl
	follower = runServerWithOpts(t, follower.opts, nil)
	servers = append(servers, follower)

	getLeader(t, 10*time.Second, servers...)

	expected := make(map[uint64]msg, s1sOpts.MaxMsgs)
	total := uint64(firstCount + moreMsgs)
	start := total - uint64(s1sOpts.MaxMsgs) + 1
	for i := start; i <= total; i++ {
		expected[i] = msg{sequence: i, data: []byte(strconv.Itoa(int(i)))}
	}
	verifyChannelConsistency(t, "foo", 10*time.Second, start, total, expected, servers...)

	select {
	case e := <-cl.ch:
		t.Fatalf("Error on restore: %v", e)
	default:
	}

	sc.Close()
}

// Ensures subscriptions are replicated such that when a leader fails over, the
// subscription continues to deliver messages.
func TestClusteringSubscriberFailover(t *testing.T) {
	var (
		channel  = "foo"
		queue    = "queue"
		sc1, sc2 stan.Conn
		err      error
		ch       = make(chan *stan.Msg, 100)
		cb       = func(msg *stan.Msg) { ch <- msg }
	)
	testCases := []struct {
		name      string
		subscribe func() error
	}{
		{
			"normal",
			func() error {
				_, err := sc1.Subscribe(channel, cb,
					stan.DeliverAllAvailable(),
					stan.MaxInflight(1),
					stan.AckWait(2*time.Second))
				return err
			},
		},
		{
			"durable",
			func() error {
				_, err := sc1.Subscribe(channel, cb,
					stan.DeliverAllAvailable(),
					stan.DurableName("durable"),
					stan.MaxInflight(1),
					stan.AckWait(2*time.Second))
				return err
			},
		},
		{
			"queue",
			func() error {
				_, err := sc1.QueueSubscribe(channel, queue, cb,
					stan.DeliverAllAvailable(),
					stan.MaxInflight(1),
					stan.AckWait(2*time.Second))
				if err != nil {
					return err
				}
				_, err = sc2.QueueSubscribe(channel, queue, cb,
					stan.DeliverAllAvailable(),
					stan.MaxInflight(1),
					stan.AckWait(2*time.Second))
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cleanupDatastore(t)
			defer cleanupDatastore(t)
			cleanupRaftLog(t)
			defer cleanupRaftLog(t)

			// For this test, use a central NATS server.
			ns := natsdTest.RunDefaultServer()
			defer ns.Shutdown()

			// Configure first server
			s1sOpts := getTestDefaultOptsForClustering("a", true)
			s1 := runServerWithOpts(t, s1sOpts, nil)
			defer s1.Shutdown()

			// Configure second server.
			s2sOpts := getTestDefaultOptsForClustering("b", false)
			s2 := runServerWithOpts(t, s2sOpts, nil)
			defer s2.Shutdown()

			// Configure third server.
			s3sOpts := getTestDefaultOptsForClustering("c", false)
			s3 := runServerWithOpts(t, s3sOpts, nil)
			defer s3.Shutdown()

			servers := []*StanServer{s1, s2, s3}
			for _, s := range servers {
				checkState(t, s, Clustered)
			}

			// Wait for leader to be elected.
			leader := getLeader(t, 10*time.Second, servers...)

			// Create client connections.
			sc1, err = stan.Connect(clusterName, clientName)
			if err != nil {
				t.Fatalf("Expected to connect correctly, got err %v", err)
			}
			defer sc1.Close()
			sc2, err = stan.Connect(clusterName, clientName+"-2")
			if err != nil {
				t.Fatalf("Expected to connect correctly, got err %v", err)
			}
			defer sc2.Close()

			// Publish a message (this will create the channel and form the Raft group).
			if err := sc1.Publish(channel, []byte("hello")); err != nil {
				t.Fatalf("Unexpected error on publish: %v", err)
			}

			if err := tc.subscribe(); err != nil {
				t.Fatalf("Error subscribing: %v", err)
			}

			select {
			case msg := <-ch:
				assertMsg(t, msg.MsgProto, []byte("hello"), 1)
			case <-time.After(2 * time.Second):
				t.Fatal("expected msg")
			}

			// Take down the leader.
			leader.Shutdown()
			servers = removeServer(servers, leader)

			// Wait for the new leader to be elected.
			getLeader(t, 10*time.Second, servers...)

			// Publish some more messages.
			for i := 0; i < 5; i++ {
				if err := sc1.Publish(channel, []byte(strconv.Itoa(i))); err != nil {
					t.Fatalf("Unexpected error on publish %d: %v", i, err)
				}
			}

			// Ensure we received the new messages.
			for i := 0; i < 5; i++ {
				select {
				case msg := <-ch:
					if i == 0 && msg.Sequence == 1 {
						assertMsg(t, msg.MsgProto, []byte("hello"), 1)
						i--
						continue
					}
					assertMsg(t, msg.MsgProto, []byte(strconv.Itoa(i)), uint64(i+2))
				case <-time.After(2 * time.Second):
					t.Fatal("expected msg")
				}
			}

			sc1.Close()
			sc2.Close()
		})
	}
}

// Ensures durable subscription updates are replicated (i.e. closing/reopening
// subscription).
func TestClusteringUpdateDurableSubscriber(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message (this will create the channel and form the Raft group).
	channel := "foo"
	if err := sc.Publish(channel, []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	ch := make(chan *stan.Msg, 100)
	sub, err := sc.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DeliverAllAvailable(), stan.DurableName("durable"), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case msg := <-ch:
		assertMsg(t, msg.MsgProto, []byte("hello"), 1)
	case <-time.After(2 * time.Second):
		t.Fatal("expected msg")
	}

	// Close (but don't remove) the subscription.
	if err := sub.Close(); err != nil {
		t.Fatalf("Unexpected error on close: %v", err)
	}

	// Take down the leader.
	leader.Shutdown()
	servers = removeServer(servers, leader)

	// Wait for the new leader to be elected.
	getLeader(t, 10*time.Second, servers...)

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("Unexpected error on publish %d: %v", i, err)
		}
	}

	// Reopen subscription.
	sub, err = sc.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DurableName("durable"), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	// Ensure we received the new messages.
	for i := 0; i < 5; i++ {
		select {
		case msg := <-ch:
			assertMsg(t, msg.MsgProto, []byte(strconv.Itoa(i)), uint64(i+2))
		case <-time.After(2 * time.Second):
			t.Fatal("expected msg")
		}
	}

	sc.Close()
}

// Ensure unsubscribes are replicated such that when a leader fails over, the
// subscription does not continue delivering messages.
func TestClusteringReplicateUnsubscribe(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message (this will create the channel and form the Raft group).
	channel := "foo"
	if err := sc.Publish(channel, []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	ch := make(chan *stan.Msg, 100)
	sub, err := sc.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DeliverAllAvailable(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case msg := <-ch:
		assertMsg(t, msg.MsgProto, []byte("hello"), 1)
	case <-time.After(2 * time.Second):
		t.Fatal("expected msg")
	}

	// Unsubscribe.
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}

	// Take down the leader.
	leader.Shutdown()
	servers = removeServer(servers, leader)

	// Wait for the new leader to be elected.
	getLeader(t, 10*time.Second, servers...)

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("Unexpected error on publish %d: %v", i, err)
		}
	}

	// Ensure we don't receive new messages.
	time.Sleep(200 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("Unexpected msg")
	default:
	}

	sc.Close()
}

func TestClusteringRaftLogReplay(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message.
	channel := "foo"
	if err := sc.Publish(channel, []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	ch := make(chan bool, 1)
	doAckMsg := int32(0)
	if _, err := sc.Subscribe(channel, func(m *stan.Msg) {
		if atomic.LoadInt32(&doAckMsg) == 1 {
			m.Ack()
		}
		if !m.Redelivered {
			ch <- true
		}
	}, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(2*time.Second)); err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	// Wait for message to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}

	// Make sure that the message is pending in leader
	waitForAcks(t, leader, clientName, 1, 1)
	subs := leader.clients.getSubs(clientName)
	// Flush the replication of SentMsg
	leader.flushReplicatedSentAndAckSeqs(subs[0], true)

	atomic.StoreInt32(&doAckMsg, 1)
	servers = removeServer(servers, leader)
	leader.Shutdown()
	servers = removeServer(servers, leader)
	getLeader(t, 10*time.Second, servers...)

	// Publish one more message and wait for message to be received
	if err := sc.Publish(channel, []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}

	// Restart original leader
	rs := runServerWithOpts(t, leader.opts, nil)
	defer rs.Shutdown()

	// Make sure we have a new leader before publishing a new
	// message to speed up the replay
	servers = append(servers, rs)
	getLeader(t, 5*time.Second, servers...)
	// Speed up the replay
	if err := sc.Publish(channel, []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}

	numSubs := 0
	lastSent := uint64(0)
	acksPending := 0
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		// There should be only 1 sub
		subs := rs.clients.getSubs(clientName)
		numSubs = len(subs)
		if numSubs == 1 {
			sub := subs[0]
			sub.RLock()
			lastSent = sub.LastSent
			acksPending = len(sub.acksPending)
			sub.RUnlock()
			if lastSent == 3 && acksPending == 0 {
				// All is as expected, we are done
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if numSubs != 1 {
		t.Fatalf("Expected 1 sub, got %v", numSubs)
	}
	if lastSent != 3 {
		t.Fatalf("Expected lastSent to be 3, got %v", lastSent)
	}
	if acksPending != 0 {
		t.Fatalf("Expected 0 pending msgs, got %v", acksPending)
	}

	sc.Close()
}

func TestClusteringConnClose(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()
	// Create a subscription
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	checkClientsInAllServers(t, 1, servers...)

	// Wait for subscription to be registered in all 3 servers
	for _, srv := range servers {
		waitForNumSubs(t, srv, clientName, 1)
	}

	// Close client connection
	sc.Close()
	// Now clients should be removed from all nodes
	checkClientsInAllServers(t, 0, servers...)
}

func TestClusteringClientCrashAndReconnect(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create NATS connection so we can simulate client stopping
	// responding to HBs.
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()
	// Get the connected client's inbox
	clients := leader.clients.getClients()
	if cc := len(clients); cc != 1 {
		t.Fatalf("There should be 1 client, got %v", cc)
	}
	cli := clients[clientName]
	if cli == nil {
		t.Fatalf("Expected client %q to exist, did not", clientName)
	}
	hbInbox := cli.info.HbInbox

	// should get a duplicate clientID error
	if sc2, err := stan.Connect(clusterName, clientName); err == nil {
		sc2.Close()
		t.Fatal("Expected to be unable to connect")
	}

	// kill the NATS conn
	nc.Close()

	// Since the original client won't respond to a ping, we should
	// be able to connect, and it should not take too long.
	start := time.Now()

	// should succeed
	if sc2, err := stan.Connect(clusterName, clientName); err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	} else {
		defer sc2.Close()
	}

	duration := time.Since(start)
	if duration > 5*time.Second {
		t.Fatalf("Took too long to be able to connect: %v", duration)
	}

	// Now kill the leader and ensure connection is known
	// to the new leader.
	leader.Shutdown()
	servers = removeServer(servers, leader)
	// Wait for new leader
	leader = getLeader(t, 10*time.Second, servers...)
	clients = leader.clients.getClients()
	if cc := len(clients); cc != 1 {
		t.Fatalf("There should be 1 client, got %v", cc)
	}
	cli = clients[clientName]
	if cli == nil {
		t.Fatalf("Expected client %q to exist, did not", clientName)
	}
	// Check we have registered the "new" client which should have
	// a different HbInbox
	if hbInbox == cli.info.HbInbox {
		t.Fatalf("Looks like restarted client was not properly registered")
	}

	sc.Close()
}

func TestClusteringHeartbeatFailover(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.ClientHBInterval = 50 * time.Millisecond
	s1sOpts.ClientHBTimeout = 10 * time.Millisecond
	s1sOpts.ClientHBFailCount = 5
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.ClientHBInterval = 50 * time.Millisecond
	s2sOpts.ClientHBTimeout = 10 * time.Millisecond
	s2sOpts.ClientHBFailCount = 5
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.ClientHBInterval = 50 * time.Millisecond
	s3sOpts.ClientHBTimeout = 10 * time.Millisecond
	s3sOpts.ClientHBFailCount = 5
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Wait for client to be registered
	checkClientsInAllServers(t, 1, servers...)

	// Check that client is not incorrectly purged
	dur := (leader.opts.ClientHBInterval + leader.opts.ClientHBTimeout)
	dur *= time.Duration(leader.opts.ClientHBFailCount + 1)
	dur += 100 * time.Millisecond
	time.Sleep(dur)
	// Client should still be there
	checkClientsInAllServers(t, 1, servers...)

	// Take down the leader.
	leader.Shutdown()
	servers = removeServer(servers, leader)

	// Wait for leader to be elected.
	getLeader(t, 10*time.Second, servers...)

	// Client should still be there
	checkClientsInAllServers(t, 1, servers...)

	// kill the NATS conn
	nc.Close()

	// Check that the server closes the connection
	checkClientsInAllServers(t, 0, servers...)

	sc.Close()
}

func TestClusteringChannelCreatedOnLogReplay(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message (this will create the channel and form the Raft group).
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Ensure channel is replicated.
	checkChannelsInAllServers(t, []string{"foo"}, 10*time.Second, servers...)

	// Kill a follower.
	var follower *StanServer
	for i, s := range servers {
		if leader != s {
			follower = s
			servers = append(servers[:i], servers[i+1:]...)
			break
		}
	}
	follower.Shutdown()

	// Implicitly create two more channels.
	if err := sc.Publish("bar", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("baz", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Ensure channels are replicated amongst remaining cluster members.
	checkChannelsInAllServers(t, []string{"foo", "bar", "baz"}, 10*time.Second, servers...)

	// Restart the follower.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)

	// Ensure follower reconciles channels.
	checkChannelsInAllServers(t, []string{"foo", "bar", "baz"}, 10*time.Second, servers...)
}

func TestClusteringAckTimerOnlyOnLeader(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	leader := getLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	// Create durable subscription that does not ack
	cliDur, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if !m.Redelivered {
			ch <- true
		}
	},
		stan.DurableName("dur"),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(500)))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}

	checkTimer := func(s *StanServer, shouldBeSet bool) {
		waitForNumSubs(t, s, clientName, 1)
		subs := checkSubs(t, s, clientName, 1)
		dur := subs[0]
		dur.RLock()
		timerSet := dur.ackTimer != nil
		dur.RUnlock()
		if !timerSet && shouldBeSet {
			stackFatalf(t, "AckTimer should be set, was not")
		} else if timerSet && !shouldBeSet {
			stackFatalf(t, "AckTimer should not be set, it was")
		}
	}

	for _, s := range servers {
		shouldBeSet := s.isLeader()
		checkTimer(s, shouldBeSet)
	}

	cliDur.Close()
	// Re-open it, since it has an unack'ed message, the
	// leader should re-create the timer.
	if _, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if !m.Redelivered {
			ch <- true
		}
	},
		stan.DurableName("dur"),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(500))); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	for _, s := range servers {
		shouldBeSet := s.isLeader()
		checkTimer(s, shouldBeSet)
	}

	// Shutdown the followers, the leader should lose
	// leadership and timer should be stopped.
	followers := removeServer(servers, leader)
	var oneFollower *StanServer
	for _, f := range followers {
		oneFollower = f
		f.Shutdown()
	}
	verifyNoLeader(t, 10*time.Second, leader)

	// The old leader should now have cancel the sub's timer.
	checkTimer(leader, false)

	// Restart one follower to speed up teardown of test
	s := runServerWithOpts(t, oneFollower.opts, nil)
	defer s.Shutdown()
	sc.Close()
}

func TestClusteringAndChannelsPartitioning(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	opts := getTestDefaultOptsForClustering("a", true)
	opts.Partitioning = true
	opts.AddPerChannel("foo", &stores.ChannelLimits{})
	s, err := RunServerWithOpts(opts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected error, got none")
	}
}

func TestClusteringGetProperErrorFromFSMApply(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	sc1, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc1.Close()

	if _, err := sc1.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur")); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// This one should fail
	if _, err := sc1.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur")); err == nil {
		t.Fatal("Creation of durable should have failed")
	}

	// We will send crafted unsubscribe protocol with wrong clientID
	waitForNumSubs(t, leader, clientName, 1)
	// Get the sub, so we can get required info for unsub request
	sub := leader.clients.getSubs(clientName)[0]
	unsubReq := &pb.UnsubscribeRequest{
		Inbox:       sub.Inbox,
		Subject:     sub.subject,
		DurableName: sub.DurableName,
		ClientID:    "wrongcid",
	}
	data, _ := unsubReq.Marshal()
	reply, err := sc1.NatsConn().Request(leader.info.Unsubscribe, data, 2*time.Second)
	if err != nil {
		t.Fatalf("Error on request: %v", err)
	}
	unsubReply := &pb.SubscriptionResponse{}
	unsubReply.Unmarshal(reply.Data)
	if unsubReply.Error == "" {
		t.Fatal("Unsubscribe should have returned an error")
	}

	leader.clients.Lock()
	orgClientStore := leader.clients.store
	leader.clients.store = &clientStoreErrorsStore{Store: orgClientStore}
	leader.clients.Unlock()

	// These operations should fail

	// Server should fail to store client
	sc2, err := stan.Connect(clusterName, clientName+"2")
	if err == nil {
		sc2.Close()
		t.Fatal("Second connect should have failed")
	}
	// Server should fail to delete this unknown client
	closeReq := &pb.CloseRequest{ClientID: "wrongid"}
	data, _ = closeReq.Marshal()
	reply, err = sc1.NatsConn().Request(leader.info.Close, data, 2*time.Second)
	if err != nil {
		t.Fatalf("Error on request: %v", err)
	}
	closeReply := &pb.CloseResponse{}
	closeReply.Unmarshal(reply.Data)
	if closeReply.Error == "" {
		t.Fatal("Close should have returned an error")
	}
	// Restore client store to speed up end of test
	leader.clients.Lock()
	leader.clients.store = orgClientStore
	leader.clients.Unlock()
}

func TestClusteringNoMsgSeqGapOnApplyError(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}

	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName,
		stan.MaxPubAcksInflight(100),
		stan.PubAckWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()

	follower := s2
	if s2 == leader {
		follower = s1
	}

	stopCh := make(chan struct{}, 1)
	errCount := uint32(0)
	errLeadeshipLost := uint32(0)
	okCount := uint32(0)
	sent := uint32(0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 1
		for {
			select {
			case <-stopCh:
				return
			default:
				if _, err := sc.PublishAsync("foo", []byte(fmt.Sprintf("%d", i)), func(guid string, err error) {
					if err != nil {
						atomic.AddUint32(&errCount, 1)
						if err.Error() == raft.ErrLeadershipLost.Error() {
							atomic.AddUint32(&errLeadeshipLost, 1)
						}
					} else {
						atomic.AddUint32(&okCount, 1)
					}
				}); err != nil {
					return
				}
				atomic.AddUint32(&sent, 1)
				i++
			}
		}
	}()
	// Wait so that go-routine is in middle of sending messages
	time.Sleep(time.Duration(rand.Intn(500)+100) * time.Millisecond)
	// Shutdow follower
	follower.Shutdown()
	// The leader should step down since there is only 1 node
	verifyNoLeader(t, 2*time.Second, servers...)
	// stop the publisher loop
	stopCh <- struct{}{}
	wg.Wait()

	timeout := time.Now().Add(5 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		total := atomic.LoadUint32(&okCount) + atomic.LoadUint32(&errCount)
		if total == atomic.LoadUint32(&sent) {
			ok = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Timedout waiting for total sent messages")
	}

	okInStore := uint64(atomic.LoadUint32(&okCount))

	ch1, ch2 := leader.sendSynchronziationRequest()
	<-ch1
	// Get the old leader's store first/last sequence
	c := leader.channels.get("foo")
	first, last := msgStoreFirstAndLastSequence(t, c.store.Msgs)
	if first != 1 || last != okInStore {
		close(ch2)
		t.Fatalf("Expected first/last to be %v/%v got %v/%v", 1, okInStore, first, last)
	}
	if c.nextSequence != last+1 {
		close(ch2)
		t.Fatalf("Expected channel nextSequence to be %v, got %v", last+1, c.nextSequence)
	}
	close(ch2)

	// Restart the follower
	servers = removeServer(servers, follower)
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	servers = append(servers, follower)

	leader = getLeader(t, 10*time.Second, servers...)

	// Publish last message
	if err := sc.Publish("foo", []byte("last")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	leader.raft.Barrier(0).Error()

	okInStore = uint64(atomic.LoadUint32(&okCount) + atomic.LoadUint32(&errLeadeshipLost) + 1)
	c = leader.channels.get("foo")
	timeout = time.Now().Add(10 * time.Second)
	ok = false
	for time.Now().Before(timeout) {
		first, last = msgStoreFirstAndLastSequence(t, c.store.Msgs)
		if first == 1 && last == okInStore {
			ok = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Timedout wait for all messages to be stored... expected %v, got %v", okInStore, last)
	}
	for i := first; i <= last; i++ {
		m, err := c.store.Msgs.Lookup(i)
		if err != nil {
			t.Fatalf("Error on lookup for seq=%v: %v", i, err)
		}
		if m == nil {
			t.Fatalf("Nil message for seq: %v", i)
		}
		if i != last {
			mi, _ := strconv.Atoi(string(m.Data))
			if i != uint64(mi) {
				t.Fatalf("Expected seq %v, got %v (m=%v)", i, mi, m)
			}
		} else if string(m.Data) != "last" {
			t.Fatalf("Unexpected last message: %v", m)
		}
	}
	sc.Close()
}

func TestClusteringDifferentClusters(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}

	// Wait for leader to be elected.
	getLeader(t, 10*time.Second, servers...)

	// Configure this server but to be part of another cluster.
	// It should not be joining the above cluster and since it
	// is started with boostrap false, it should fail to start.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.ID = s1sOpts.ID + "2"
	s3, err := RunServerWithOpts(s3sOpts, nil)
	if err == nil {
		s3.Shutdown()
		t.Fatal("Server s3 should have failed to start")
	}
}

func TestClusteringDeleteChannel(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	maxInactivity := 500 * time.Millisecond

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.MaxInactivity = maxInactivity
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.MaxInactivity = maxInactivity
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}
	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Send a message, which will create the channel
	channel := "foo"

	// Pick the follower
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}

	// Keep sending to the channel to keep it alive.
	ch := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.After(50 * time.Millisecond):
				sc.Publish(channel, []byte("hello"))
			case <-ch:
				return
			}
		}
	}()
	// Check that channel is not removed in follower
	time.Sleep(2 * maxInactivity)
	verifyChannelExist(t, follower, channel, true, 2*time.Second)
	ch <- struct{}{}
	wg.Wait()

	// Ensure channel is removed from both leader and follower
	verifyChannelExist(t, leader, channel, false, 2*maxInactivity)
	verifyChannelExist(t, follower, channel, false, 2*maxInactivity)

	checkLeaderLossChan := "bar"
	sc.Publish(checkLeaderLossChan, []byte("hello"))
	verifyChannelExist(t, leader, checkLeaderLossChan, true, 2*time.Second)
	verifyChannelExist(t, follower, checkLeaderLossChan, true, 2*time.Second)
	// Now lose leadership
	follower.Shutdown()
	verifyNoLeader(t, 5*time.Second, servers...)
	// Make sure that channel is not removed
	time.Sleep(2 * maxInactivity)
	verifyChannelExist(t, leader, checkLeaderLossChan, true, 2*time.Second)

	// Restart follower
	servers = removeServer(servers, follower)
	restartedServer := runServerWithOpts(t, follower.opts, nil)
	defer restartedServer.Shutdown()
	servers = append(servers, restartedServer)
	getLeader(t, 10*time.Second, servers...)

	// Now wait for this channel to be removed
	for _, s := range servers {
		verifyChannelExist(t, s, checkLeaderLossChan, false, 2*maxInactivity)
	}

	// Send a message
	sc.Publish(channel, []byte("new"))
	sc.Close()

	// Stop the servers
	for _, s := range servers {
		s.Shutdown()
	}

	// Remove the MaxInactivity limit and restart
	s1sOpts.MaxInactivity = 0
	s1 = runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()
	s2sOpts.MaxInactivity = 0
	s2 = runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()
	servers = []*StanServer{s1, s2}

	time.Sleep(2 * maxInactivity)

	// Channels should exist.
	for _, s := range servers {
		verifyChannelExist(t, s, channel, true, 2*time.Second)
	}

	leader = getLeader(t, 5*time.Second, servers...)
	c := leader.channels.get(channel)
	m := msgStoreFirstMsg(t, c.store.Msgs)
	assertMsg(t, *m, []byte("new"), 1)
}

func TestClusteringCrashOnRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForClustering("a", true)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	getLeader(t, 10*time.Second, s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	ch := make(chan bool, 1)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {
		ch <- true
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	sc.Close()

	s.Shutdown()

	testPauseAfterNewRaftCalled = true
	defer func() { testPauseAfterNewRaftCalled = false }()
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
}

func TestClusteringNoRaftStateButStreamingState(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForClustering("a", true)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	getLeader(t, 10*time.Second, s)

	sc := NewDefaultConnection(t)
	defer sc.Close()
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	sc.Close()

	s.Shutdown()
	cleanupRaftLog(t)
	s, err := RunServerWithOpts(opts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected error, got none")
	}
	// Start again and still should fail
	s, err = RunServerWithOpts(opts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected error, got none")
	}
}

func TestClusteringNodeIDInPeersArray(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	s1Opts := getTestDefaultOptsForClustering("a", true)
	s1Opts.Clustering.NodeID = "a"
	s1Opts.Clustering.Peers = []string{"a", "b", "c"}
	s1 := runServerWithOpts(t, s1Opts, nil)
	defer s1.Shutdown()

	s2Opts := getTestDefaultOptsForClustering("b", false)
	s2Opts.Clustering.NodeID = "b"
	s2Opts.Clustering.Peers = []string{"a", "b", "c"}
	s2 := runServerWithOpts(t, s2Opts, nil)
	defer s2.Shutdown()

	s3Opts := getTestDefaultOptsForClustering("c", false)
	s3Opts.Clustering.NodeID = "c"
	s3Opts.Clustering.Peers = []string{"a", "b", "c"}
	s3 := runServerWithOpts(t, s3Opts, nil)
	defer s3.Shutdown()

	getLeader(t, 10*time.Second, s1, s2, s3)
}

func TestClusteringUnableToContactPeer(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	logger := &checkErrorLogger{checkErrorStr: "dial failed"}

	s1Opts := getTestDefaultOptsForClustering("a", true)
	s1Opts.Clustering.NodeID = "a"
	s1Opts.Clustering.Peers = []string{"b"}
	s1Opts.Clustering.RaftLogging = true
	s1Opts.CustomLogger = logger
	s1 := runServerWithOpts(t, s1Opts, nil)
	defer s1.Shutdown()

	s2Opts := getTestDefaultOptsForClustering("b", false)
	s2Opts.Clustering.NodeID = "b"
	s2Opts.Clustering.Peers = []string{"a"}
	s2 := runServerWithOpts(t, s2Opts, nil)
	defer s2.Shutdown()

	getLeader(t, 10*time.Second, s1, s2)

	// Kill the NATS Server
	ns.Shutdown()

	// Wait that we get the expected error message
	gotIt := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		// Check for error
		logger.Lock()
		gotIt = logger.gotError
		logger.Unlock()
		if gotIt {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !gotIt {
		t.Fatalf("Did not get the expected error")
	}

	// Check that server shutdown in a timely manner
	checkShutdown := func(s *StanServer) {
		ch := make(chan bool, 1)
		go func() {
			s.Shutdown()
			ch <- true
		}()
		if err := Wait(ch); err != nil {
			stackFatalf(t, "Server took too long to shutdown")
		}
	}
	checkShutdown(s2)
	checkShutdown(s1)
}

func TestClusteringClientPings(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	clientCheckTimeout = 150 * time.Millisecond
	defer func() { clientCheckTimeout = defaultClientCheckTimeout }()

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	servers := []*StanServer{s1, s2}
	// Wait for leader to be elected.
	leader := getLeader(t, 10*time.Second, servers...)

	testClientPings(t, leader)
}
