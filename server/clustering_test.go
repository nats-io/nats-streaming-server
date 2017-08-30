// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"

	"github.com/nats-io/nats-streaming-server/stores"
)

var defaultRaftLog string

func init() {
	tmpDir, err := ioutil.TempDir(".", "raft_logs_")
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

func getTestDefaultOptsForClustering(id string, peers []string) *Options {
	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = filepath.Join(defaultDataStore, id)
	opts.FileStoreOpts.BufferSize = 1024
	opts.ClusterPeers = peers
	opts.ClusterNodeID = id
	opts.RaftLogPath = filepath.Join(defaultRaftLog, id)
	opts.NATSServerURL = "nats://localhost:4222"
	return opts
}

func getChannelLeader(t *testing.T, channel string, timeout time.Duration, servers ...*StanServer) *StanServer {
	var (
		leader   *StanServer
		deadline = time.Now().Add(timeout)
	)
	for time.Now().Before(deadline) {
		for i := 0; i < len(servers); i++ {
			s := servers[i]
			if s.state == Shutdown {
				continue
			}
			s.channels.Lock()
			c := s.channels.channels[channel]
			s.channels.Unlock()
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

func removeServer(servers []*StanServer, s *StanServer) []*StanServer {
	for i, srv := range servers {
		if srv == s {
			servers = append(servers[:i], servers[i+1:]...)
		}
	}
	return servers
}

func assertMsg(t *testing.T, msg pb.MsgProto, expectedData []byte, expectedSeq uint64) {
	if !bytes.Equal(msg.Data, expectedData) {
		stackFatalf(t, "Msg data incorrect, expected: %s, got: %s", expectedData, msg.Data)
	}
	if msg.Sequence != expectedSeq {
		stackFatalf(t, "Msg sequence incorrect, expected: %d, got: %d", expectedSeq, msg.Sequence)
	}
}

func TestClusteringConfig(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	opts := GetDefaultOptions()
	opts.ClusterPeers = []string{"a", "b"}
	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		if s != nil {
			s.Shutdown()
		}
		t.Fatal("Server should have failed to start with cluster peers and no cluster node id")
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
	s1sOpts := getTestDefaultOptsForClustering("a", []string{"b", "c"})
	s1 := runServerWithOpts(t, s1sOpts, nil)
	defer s1.Shutdown()

	// Configure second server.
	s2sOpts := getTestDefaultOptsForClustering("b", []string{"a", "c"})
	s2 := runServerWithOpts(t, s2sOpts, nil)
	defer s2.Shutdown()

	// Configure third server.
	s3sOpts := getTestDefaultOptsForClustering("c", []string{"a", "b"})
	s3 := runServerWithOpts(t, s3sOpts, nil)
	defer s3.Shutdown()

	servers := []*StanServer{s1, s2, s3}
	for _, s := range servers {
		checkState(t, s, Clustered)
	}

	// Create a client connection.
	sc, err := stan.Connect(clusterName, clientName, stan.PubAckWait(2*time.Second))
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
	}, stan.DeliverAllAvailable())
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
	leader := getChannelLeader(t, channel, 5*time.Second, servers...)
	leader.Shutdown()
	stopped = append(stopped, leader)
	servers = removeServer(servers, leader)

	// Wait for the new leader to be elected.
	leader = getChannelLeader(t, channel, 5*time.Second, servers...)

	// Publish some more messages.
	for i := 0; i < 5; i++ {
		if err := sc.Publish(channel, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("Unexpected error on publish %d: %v", i, err)
		}
	}

	// Read everything back from the channel.
	sub, err = sc.Subscribe(channel, func(msg *stan.Msg) {
		ch <- msg
	}, stan.DeliverAllAvailable())
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

	// Create a new connection with lower timeouts to test when we don't have a leader.
	sc2, err := stan.Connect(clusterName, clientName+"-2", stan.PubAckWait(time.Second), stan.ConnectWait(time.Second))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()

	// New subscriptions should be rejected since no leader can be established.
	_, err = sc2.Subscribe(channel, func(msg *stan.Msg) {}, stan.DeliverAllAvailable())
	if err == nil {
		t.Fatal("Expected error on subscribe")
	}

	// Publishes should also fail.
	if err := sc2.Publish(channel, []byte("foo")); err == nil {
		t.Fatal("Expected error on publish")
	}

	// Bring one node back up.
	s := stopped[0]
	stopped = stopped[1:]
	s = runServerWithOpts(t, s.opts, nil)
	servers = append(servers, s)
	defer s.Shutdown()

	// Wait for the new leader to be elected.
	getChannelLeader(t, channel, 5*time.Second, servers...)

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
	getChannelLeader(t, channel, 5*time.Second, servers...)

	// Publish one more message.
	if err := sc.Publish(channel, []byte("goodbye")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	type msg struct {
		sequence uint64
		data     []byte
	}
	expected := make([]msg, 12)
	expected[0] = msg{sequence: 1, data: []byte("hello")}
	for i := 0; i < 5; i++ {
		expected[i+1] = msg{sequence: uint64(i + 2), data: []byte(strconv.Itoa(i))}
	}
	for i := 0; i < 5; i++ {
		expected[i+6] = msg{sequence: uint64(i + 7), data: []byte("foo-" + strconv.Itoa(i))}
	}
	expected[11] = msg{sequence: 12, data: []byte("goodbye")}

	// Wait for the cluster to quiesce.
	time.Sleep(2 * time.Second)

	// Verify the server stores are consistent.
	for _, server := range servers {
		server.channels.Lock()
		store := server.channels.channels[channel].store.Msgs
		server.channels.Unlock()
		first, last, err := store.FirstAndLastSequence()
		if err != nil {
			t.Fatalf("Error getting sequence numbers: %v", err)
		}
		if first != 1 {
			t.Fatalf("Unexpected first sequence: expected 1, got %d", first)
		}
		if last != 12 {
			t.Fatalf("Unexpected last sequence: expected 12, got %d", last)
		}
		for i := first; i <= last; i++ {
			msg, err := store.Lookup(i)
			if err != nil {
				t.Fatalf("Error getting message %d: %v", i, err)
			}
			assertMsg(t, *msg, expected[i-1].data, expected[i-1].sequence)
		}
	}
}
