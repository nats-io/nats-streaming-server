// Copyright 2018 Synadia Communications Inc. All rights reserved.

package stores

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

var testRSDefaultDatastore string

func init() {
	tmpDir, err := ioutil.TempDir(".", "raft_data_stores_")
	if err != nil {
		panic("Could not create tmp dir")
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp directory: %v", err))
	}
	testRSDefaultDatastore = tmpDir
}

func cleanupRaftDatastore(t tLogger) {
	if err := os.RemoveAll(testRSDefaultDatastore); err != nil {
		stackFatalf(t, "Error cleaning up datastore: %v", err)
	}
}

func createDefaultRaftStore(t tLogger) *RaftStore {
	limits := testDefaultStoreLimits
	fs, err := NewFileStore(testLogger, testRSDefaultDatastore, &limits)
	if err != nil {
		stackFatalf(t, "Error creating raft store: %v", err)
	}
	rs := NewRaftStore(fs)
	state, err := rs.Recover()
	if err != nil {
		rs.Close()
		stackFatalf(t, "Error recovering file store: %v", err)
	}
	if state == nil {
		info := testDefaultServerInfo
		if err := rs.Init(&info); err != nil {
			stackFatalf(t, "Unexpected error durint Init: %v", err)
		}
	}
	return rs
}

func TestRSRecover(t *testing.T) {
	cleanupRaftDatastore(t)
	defer cleanupRaftDatastore(t)

	limits := testDefaultStoreLimits
	fs, err := NewFileStore(testLogger, testRSDefaultDatastore, &limits)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	s := NewRaftStore(fs)
	defer s.Close()

	info := testDefaultServerInfo
	info.ClusterID = "testRaftStore"
	if err := s.Init(&info); err != nil {
		t.Fatalf("Error on init: %v", err)
	}

	// Add some clients activity
	storeAddClient(t, s, "me", "mehbinbox")
	storeAddClient(t, s, "me2", "me2hbinbox")
	storeAddClient(t, s, "me3", "me3hbinbox")
	storeDeleteClient(t, s, "me")

	// Add some messages
	cs := storeCreateChannel(t, s, "foo")
	for i := 0; i < 10; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), []byte("msg"))
	}

	// Add some subscriptions activity
	sub1 := storeSub(t, cs, "foo")
	sub2 := storeSub(t, cs, "foo")
	storeSubPending(t, cs, "foo", sub1, 1, 2, 3)
	storeSubAck(t, cs, "foo", sub1, 1, 3)
	storeSubPending(t, cs, "foo", sub2, 1, 2, 3, 4, 5)
	storeSubAck(t, cs, "foo", sub2, 1, 2, 3, 4)
	storeSubDelete(t, cs, "foo", sub2)

	// Close store and re-open it
	s.Close()

	fs, err = NewFileStore(testLogger, testRSDefaultDatastore, &limits)
	if err != nil {
		stackFatalf(t, "Error creating raft store: %v", err)
	}
	s = NewRaftStore(fs)
	state, err := s.Recover()
	if err != nil {
		t.Fatalf("Error on recover: %v", err)
	}
	if state == nil {
		t.Fatal("Expected a state, did not get one")
	}
	if !reflect.DeepEqual(*state.Info, info) {
		t.Fatalf("Expected ServerInfo to be %v, got %v", *state.Info, info)
	}
	// Expect single channel "foo"
	if len(state.Channels) != 1 {
		t.Fatalf("Expected only 1 channel, got %v", len(state.Channels))
	}
	cs = getRecoveredChannel(t, state, "foo")
	// Expect no client
	if len(state.Clients) != 0 {
		t.Fatalf("Expected no client, got %v", len(state.Clients))
	}
	// Expect no subscription
	for _, rc := range state.Channels {
		if len(rc.Subscriptions) != 0 {
			t.Fatalf("Should have no subscription, got %v", len(rc.Subscriptions))
		}
	}
	// Expect 10 messages
	count, _ := msgStoreState(t, cs.Msgs)
	if count != 10 {
		t.Fatalf("Expected 10 messages, got %v", count)
	}
	// Adding a new subscription should give us ID 3
	sub3 := storeSub(t, cs, "foo")
	if sub3 != 3 {
		t.Fatalf("Expected sub3 to have ID 3, got %v", sub3)
	}
}
