// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
}

func cleanupRaftLog(t *testing.T) {
	if err := os.RemoveAll(defaultRaftLog); err != nil {
		stackFatalf(t, "Error cleaning up raft log: %v", err)
	}
}

func getTestDefaultOptsForClustering(id string, bootstrap bool) *Options {
	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = filepath.Join(defaultDataStore, id)
	opts.FileStoreOpts.BufferSize = 1024
	opts.Clustering.Clustered = true
	opts.Clustering.Bootstrap = bootstrap
	opts.Clustering.RaftLogPath = filepath.Join(defaultRaftLog, id)
	opts.Clustering.LogCacheSize = DefaultLogCacheSize
	opts.Clustering.LogSnapshots = 1
	opts.NATSServerURL = "nats://localhost:4222"
	return opts
}

func getChannelLeader(t *testing.T, channel string, timeout time.Duration, servers ...*StanServer) *StanServer {
	var (
		leader   *StanServer
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if s.state == Shutdown {
				continue
			}
			c := s.channels.get(channel)
			if c == nil || c.raft == nil {
				continue
			}
			if c.isLeader() {
				if leader != nil {
					stackFatalf(t, "Found more than one channel leader")
				}
				leader = s
			}
		}
		if leader != nil {
			break
		}
	}
	if leader == nil {
		stackFatalf(t, "Unable to find the channel leader")
	}
	return leader
}

func getMetadataLeader(t *testing.T, timeout time.Duration, servers ...*StanServer) *StanServer {
	var (
		leader   *StanServer
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for _, s := range servers {
			if s.state == Shutdown || s.raft == nil {
				continue
			}
			if s.isMetadataLeader() {
				if leader != nil {
					stackFatalf(t, "Found more than one metadata leader")
				}
				leader = s
			}
		}
		if leader != nil {
			break
		}
	}
	if leader == nil {
		stackFatalf(t, "Unable to find the metadata leader")
	}
	return leader
}

func verifyNoLeader(t *testing.T, channel string, timeout time.Duration, servers ...*StanServer) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, server := range servers {
			c := server.channels.get(channel)
			if c == nil || c.raft == nil {
				continue
			}
			if c.isLeader() {
				time.Sleep(100 * time.Millisecond)
				break
			}
		}
		return
	}
	stackFatalf(t, "Found unexpected leader for channel %s", channel)
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
			store := server.channels.get(channel).store.Msgs
			first, last, err := store.FirstAndLastSequence()
			if err != nil {
				stackFatalf(t, "Error getting sequence numbers: %v", err)
			}
			if first != expectedFirstSeq {
				time.Sleep(100 * time.Millisecond)
				continue OUTER
			}
			if last != expectedLastSeq {
				time.Sleep(100 * time.Millisecond)
				continue OUTER
			}
			for i := first; i <= last; i++ {
				msg, err := store.Lookup(i)
				if err != nil {
					t.Fatalf("Error getting message %d: %v", i, err)
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

	// Wait to elect self as leader.
	leader := getMetadataLeader(t, 10*time.Second, s1)

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
	leader = getMetadataLeader(t, 10*time.Second, s1)

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

	// Wait to elect self as leader.
	leader := getMetadataLeader(t, 10*time.Second, s1)

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
	leader = getMetadataLeader(t, 10*time.Second, s1)

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
		leader  = getMetadataLeader(t, 10*time.Second, servers...)
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
		leader  = getMetadataLeader(t, 10*time.Second, servers...)
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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

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
	leader := getChannelLeader(t, channel, 10*time.Second, servers...)
	leader.Shutdown()
	stopped = append(stopped, leader)
	servers = removeServer(servers, leader)

	// Wait for the new leader to be elected.
	leader = getChannelLeader(t, channel, 10*time.Second, servers...)

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

	// Creating a new connection should fail since there should not be a
	// metadata leader.
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
	getChannelLeader(t, channel, 10*time.Second, servers...)

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
	getChannelLeader(t, channel, 10*time.Second, servers...)

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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName, stan.PubAckWait(time.Second), stan.ConnectWait(10*time.Second))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()

	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	leader := getChannelLeader(t, "foo", 10*time.Second, servers...)

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
	if s1 == leader {
		s2.Shutdown()
	} else {
		s1.Shutdown()
	}
	wg.Wait()
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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

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

	// Wait for leader to be elected.
	leader := getChannelLeader(t, channel, 10*time.Second, servers...)

	// Kill the follower.
	var follower *StanServer
	if s1 == leader {
		s2.Shutdown()
		follower = s2
	} else {
		s1.Shutdown()
		follower = s1
	}

	// Ensure there is no leader now.
	verifyNoLeader(t, channel, 5*time.Second, s1, s2)

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	servers = []*StanServer{leader, follower}
	defer follower.Shutdown()

	// Ensure there is a new leader.
	getChannelLeader(t, channel, 10*time.Second, servers...)
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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message (this will create the channel and form the Raft group).
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

	// Wait for leader to be elected.
	leader := getChannelLeader(t, channel, 10*time.Second, servers...)

	// Kill a follower.
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
	follower.Shutdown()

	// Publish some more messages.
	for i := 0; i < snapshotBatchSize; i++ {
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

	// Unsubscribe the previous subscription.
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}

	// Force a log compaction on the leader.
	if err := leader.channels.get(channel).raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	for i, server := range servers {
		if server.opts.Clustering.NodeID == follower.opts.Clustering.NodeID {
			servers[i] = follower
			break
		}
	}

	// Ensure there is a leader before publishing.
	getChannelLeader(t, channel, 10*time.Second, servers...)

	// Publish a message to force a timely catch up.
	if err := sc.Publish(channel, []byte(strconv.Itoa(snapshotBatchSize+7))); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify the server stores are consistent.
	totalMsgs := uint64(snapshotBatchSize + 7)
	expected := make(map[uint64]msg, totalMsgs)
	for i := uint64(0); i < totalMsgs; i++ {
		expected[i+1] = msg{sequence: uint64(i + 1), data: []byte(strconv.Itoa(int(i + 1)))}
	}
	verifyChannelConsistency(t, channel, 10*time.Second, 1, totalMsgs, expected, servers...)

	// Verify subscriptions are consistent.
	for _, srv := range servers {
		waitForNumSubs(t, srv, clientName, 2)
	}
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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

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
		// Do not ack.
		ch <- msg
	}, stan.DeliverAllAvailable(), stan.SetManualAckMode(), stan.AckWait(time.Second), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Wait for leader to be elected.
	leader := getChannelLeader(t, channel, 10*time.Second, servers...)

	// Kill a follower.
	var follower *StanServer
	for _, s := range servers {
		if leader != s {
			follower = s
			break
		}
	}
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
	if err := leader.channels.get(channel).raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	for i, server := range servers {
		if server.opts.Clustering.NodeID == follower.opts.Clustering.NodeID {
			servers[i] = follower
			break
		}
	}

	// Ensure there is a leader before publishing.
	getChannelLeader(t, channel, 10*time.Second, servers...)

	// Publish a message to force a timely catch up.
	if err := sc.Publish(channel, []byte("2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify the server stores are consistent.
	totalMsgs := uint64(2)
	expected := make(map[uint64]msg, totalMsgs)
	for i := uint64(0); i < totalMsgs; i++ {
		expected[i+1] = msg{sequence: uint64(i + 1), data: []byte(strconv.Itoa(int(i + 1)))}
	}
	verifyChannelConsistency(t, channel, 10*time.Second, 1, totalMsgs, expected, servers...)

	// Kill all the servers.
	for _, s := range servers {
		s.Shutdown()
	}

	// Restart the follower who caught up in standalone mode.
	follower.opts.Clustering.Clustered = false
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()

	// Verify the message is redelivered and, after acking, we receive the next
	// message.
	select {
	case msg := <-ch:
		msg.Ack()
		assertMsg(t, msg.MsgProto, []byte("1"), 1)
	case <-time.After(10 * time.Second):
		t.Fatal("expected msg")
	}
	var (
		deadline = time.Now().Add(10 * time.Second)
		m        *stan.Msg
	)
LOOP:
	for time.Now().Before(deadline) {
		select {
		case msg := <-ch:
			msg.Ack()
			// It's possible the first message was redelivered multiple times.
			if msg.Sequence == 1 {
				continue
			}
			assertMsg(t, msg.MsgProto, []byte("2"), 2)
			m = msg
			break LOOP
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if m == nil {
		t.Fatal("expected msg")
	}
}

func TestClusteringLogSnapshotRestoreMetadata(t *testing.T) {
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

	// Wait for metadata leader to be elected.
	leader := getMetadataLeader(t, 10*time.Second, servers...)

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

	// Wait for channel leader to be elected.
	getChannelLeader(t, channel, 10*time.Second, servers...)

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

	// Force a log compaction on the metadata leader.
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatalf("Unexpected error on snapshot: %v", err)
	}

	// Bring the follower back up.
	follower = runServerWithOpts(t, follower.opts, nil)
	defer follower.Shutdown()
	for i, server := range servers {
		if server.opts.Clustering.NodeID == follower.opts.Clustering.NodeID {
			servers[i] = follower
			break
		}
	}

	// Ensure there is a leader.
	getMetadataLeader(t, 10*time.Second, servers...)

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

			// Wait for metadata leader to be elected.
			getMetadataLeader(t, 10*time.Second, servers...)

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
			leader := getChannelLeader(t, channel, 10*time.Second, servers...)
			leader.Shutdown()
			servers = removeServer(servers, leader)

			// Wait for the new leader to be elected.
			getChannelLeader(t, channel, 10*time.Second, servers...)

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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

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
	}, stan.DeliverAllAvailable(), stan.DurableName("durable"), stan.MaxInflight(1), stan.AckWait(2*time.Second))
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
	leader := getChannelLeader(t, channel, 10*time.Second, servers...)
	leader.Shutdown()
	servers = removeServer(servers, leader)

	// Wait for the new leader to be elected.
	getChannelLeader(t, channel, 10*time.Second, servers...)

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("Unexpected error on publish %d: %v", i, err)
		}
	}

	// Reopen subscription.
	sub, err = sc.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DurableName("durable"), stan.MaxInflight(1), stan.AckWait(2*time.Second))
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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

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
	leader := getChannelLeader(t, channel, 10*time.Second, servers...)
	leader.Shutdown()
	servers = removeServer(servers, leader)

	// Wait for the new leader to be elected.
	getChannelLeader(t, channel, 10*time.Second, servers...)

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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

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
	// And for the ack to be replicated
	time.Sleep(time.Second)
	leader := getChannelLeader(t, channel, 10*time.Second, servers...)
	leader.Shutdown()
	servers = removeServer(servers, leader)
	getChannelLeader(t, channel, 10*time.Second, servers...)

	atomic.StoreInt32(&doAckMsg, 1)
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
			if lastSent == 2 && acksPending == 0 {
				// All is as expected, we are done
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if numSubs != 1 {
		t.Fatalf("Expected 1 sub, got %v", numSubs)
	}
	if lastSent != 2 {
		t.Fatalf("Expected lastSent to be 2, got %v", lastSent)
	}
	if acksPending != 0 {
		t.Fatalf("Expected 0 pending msgs, got %v", acksPending)
	}
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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

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
	// Wait for subscription to be registered in all 3 servers
	for _, srv := range servers {
		waitForNumSubs(t, srv, clientName, 1)
	}

	checkClientsInAllServers(t, 1, servers...)
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

	// Wait for metadata leader to be elected.
	leader := getMetadataLeader(t, 10*time.Second, servers...)

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

	// Now kill the metadata leader and ensure connection is known
	// to the new leader.
	leader.Shutdown()
	servers = removeServer(servers, leader)
	// Wait for new leader
	leader = getMetadataLeader(t, 10*time.Second, servers...)
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

	// Wait for metadata leader to be elected.
	leader := getMetadataLeader(t, 10*time.Second, servers...)

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

	// Wait for metadata leader to be elected.
	getMetadataLeader(t, 10*time.Second, servers...)

	// Client should still be there
	checkClientsInAllServers(t, 1, servers...)

	// kill the NATS conn
	nc.Close()

	// Check that the server closes the connection
	checkClientsInAllServers(t, 0, servers...)
}

func TestClusteringChannelGossip(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	// For this test, use a central NATS server.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	// Configure first server
	s1sOpts := getTestDefaultOptsForClustering("a", true)
	s1sOpts.Clustering.GossipInterval = time.Second
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", false)
	s2sOpts.Clustering.GossipInterval = time.Second
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", false)
	s3sOpts.Clustering.GossipInterval = time.Second
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Wait for metadata leader to be elected.
	leader := getMetadataLeader(t, 10*time.Second, servers...)

	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Publish a message (this will create the channel and form the Raft group).
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for channel leader to be elected.
	getChannelLeader(t, "foo", 10*time.Second, servers...)

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
