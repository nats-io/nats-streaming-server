// Copyright 2018 The NATS Authors
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

package stores

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
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
	rs := NewRaftStore(testLogger, fs, &limits)
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
	s := NewRaftStore(testLogger, fs, &limits)
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
	s = NewRaftStore(testLogger, fs, &limits)
	defer s.Close()
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
}

func TestRSRecoverOldStore(t *testing.T) {
	cleanupRaftDatastore(t)
	defer cleanupRaftDatastore(t)

	limits := testDefaultStoreLimits
	fs, err := NewFileStore(testLogger, testRSDefaultDatastore, &limits)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}

	info := testDefaultServerInfo
	info.ClusterID = "testRaftStore"
	if err := fs.Init(&info); err != nil {
		t.Fatalf("Error on init: %v", err)
	}

	// Add some messages
	cs := storeCreateChannel(t, fs, "foo")
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

	// Close store and re-open it
	fs.Close()

	fs, err = NewFileStore(testLogger, testRSDefaultDatastore, &limits)
	if err != nil {
		stackFatalf(t, "Error creating raft store: %v", err)
	}
	s := NewRaftStore(testLogger, fs, &limits)
	defer s.Close()
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

func TestRSUseSubID(t *testing.T) {
	cleanupRaftDatastore(t)
	defer cleanupRaftDatastore(t)

	limits := testDefaultStoreLimits
	limits.MaxSubscriptions = 2
	ms, err := NewMemoryStore(testLogger, &limits)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	s := NewRaftStore(testLogger, ms, &limits)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	sub := &spb.SubState{
		ID:            10,
		ClientID:      "me",
		Inbox:         "ibx",
		AckInbox:      "ackibx",
		AckWaitInSecs: 10,
	}
	if err := cs.Subs.CreateSub(sub); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	if sub.ID != 10 {
		t.Fatalf("Store did not use the provided sub ID, expected 10, got %v", sub.ID)
	}
	// Store another with different sub.ID
	newSub := *sub
	newSub.ID = 11
	if err := cs.Subs.CreateSub(&newSub); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	if newSub.ID != 11 {
		t.Fatalf("Store did not use the provided sub ID, expected 11, got %v", newSub.ID)
	}

	// Now if we call with one of existing ID it should not count toward max subs.
	newSub2 := *sub
	newSub2.ID = 10
	if err := cs.Subs.CreateSub(&newSub2); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	if newSub2.ID != 10 {
		t.Fatalf("Store did not use the provided sub ID, expected 10, got %v", newSub2.ID)
	}

	// Now, a new one should fail.
	newSub3 := *sub
	newSub3.ID = 12
	if err := cs.Subs.CreateSub(&newSub3); err != ErrTooManySubs {
		t.Fatalf("Expected too many subs error, got %v", err)
	}

	// Delete the first (ID=10)
	if err := cs.Subs.DeleteSub(10); err != nil {
		t.Fatalf("Error deleting sub: %v", err)
	}

	// Check that we support sub.ID == 0 and take the max+1.
	// So if we change newSub3.ID to 0, after create it should be
	// set to 12 (since last max is 11)
	newSub3.ID = 0
	if err := cs.Subs.CreateSub(&newSub3); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	if newSub3.ID != 12 {
		t.Fatalf("Expected newSub3.ID to be 12, got %v", newSub3.ID)
	}
}

func TestRSFileAutoSync(t *testing.T) {
	cleanupRaftDatastore(t)
	defer cleanupRaftDatastore(t)

	limits := testDefaultStoreLimits
	fs, err := NewFileStore(testLogger, testRSDefaultDatastore, &limits, AutoSync(15*time.Millisecond))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}

	s := NewRaftStore(testLogger, fs, &limits)
	defer s.Close()

	info := testDefaultServerInfo
	info.ClusterID = "testRaftStore"
	if err := s.Init(&info); err != nil {
		t.Fatalf("Error on init: %v", err)
	}

	// Add some state
	cs := storeCreateChannel(t, s, "foo")
	storeMsg(t, cs, "foo", 1, []byte("msg"))
	storeSub(t, cs, "foo")

	// Wait for auto sync to kick in
	time.Sleep(50 * time.Millisecond)

	// Server should not have panic'ed.
}
