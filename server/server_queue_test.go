// Copyright 2016-2019 The NATS Authors
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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
)

// As of now, it is possible for members of the same group to have different
// AckWait values. This test checks that if a member with an higher AckWait
// than the other member leaves, the message with an higher expiration time
// is set to the remaining member's AckWait value.
// It also checks that on the opposite case, if a member leaves and the
// remaining member has a higher AckWait, the original expiration time is
// maintained.
func TestQueueSubsWithDifferentAckWait(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	var qsub1, qsub2, qsub3 stan.Subscription
	var err error

	dch := make(chan bool)
	rch2 := make(chan bool)
	rch3 := make(chan bool)

	cb := func(m *stan.Msg) {
		if !m.Redelivered {
			dch <- true
		} else {
			if m.Sub == qsub2 {
				rch2 <- true
			} else if m.Sub == qsub3 {
				rch3 <- true
				// stop further redeliveries, test is done.
				qsub3.Close()
			}
		}
	}
	// Create first queue member with high AckWait
	qsub1, err = sc.QueueSubscribe("foo", "bar", cb,
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(1000)))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Send the single message used in this test
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for message to be received
	if err := Wait(dch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Create the second member with low AckWait
	qsub2, err = sc.QueueSubscribe("foo", "bar", cb,
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(15)))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check we have the two members
	checkQueueGroupSize(t, s, "foo", "bar", true, 2)
	// Close the first, message should be redelivered within
	// qsub2's AckWait, which is 1 second.
	qsub1.Close()
	// Check we have only 1 member
	checkQueueGroupSize(t, s, "foo", "bar", true, 1)
	// Wait for redelivery
	select {
	case <-rch2:
	// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Message should have been redelivered")
	}
	// Create 3rd member with higher AckWait than the 2nd
	qsub3, err = sc.QueueSubscribe("foo", "bar", cb,
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(150)))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Close qsub2
	qsub2.Close()
	// Check we have only 1 member
	checkQueueGroupSize(t, s, "foo", "bar", true, 1)
	// Wait for redelivery. It should happen after the remaining
	// of the first redelivery to qsub2 and its AckWait, which
	// should be less than 15 ms.
	select {
	case <-rch3:
	// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Message should have been redelivered")
	}
}

func TestQueueMaxInFlight(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	total := 100
	payload := []byte("hello")
	for i := 0; i < total; i++ {
		sc.Publish("foo", payload)
	}

	ch := make(chan bool)
	received := 0
	cb := func(m *stan.Msg) {
		if !m.Redelivered {
			received++
			if received == total {
				ch <- true
			}
		}
	}
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(5)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not get all our messages")
	}
}

func TestQueueGroupRemovedOnLastMemberLeaving(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Sequence == 1 {
			ch <- true
		}
	}
	// Create a queue subscriber
	if _, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close the connection which will remove the queue subscriber
	sc.Close()

	// Create a new connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Send a new message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a queue subscriber. The group should have been destroyed
	// when the last member left, so even with a new name, this should
	// be a new group and start from msg seq 1
	qsub, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Test with unsubscribe
	if err := qsub.Unsubscribe(); err != nil {
		t.Fatalf("Error during Unsubscribe: %v", err)
	}
	// Recreate a queue subscriber, it should again receive from msg1
	if _, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestQueueSubscriberTransferPendingMsgsOnClose(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	var sub1 stan.Subscription
	var sub2 stan.Subscription
	var err error
	ch := make(chan bool)
	qsetup := make(chan bool)
	cb := func(m *stan.Msg) {
		<-qsetup
		if m.Sub == sub1 && m.Sequence == 1 && !m.Redelivered {
			ch <- true
		} else if m.Sub == sub2 && m.Sequence == 1 && m.Redelivered {
			ch <- true
		}
	}
	// Create a queue subscriber with MaxInflight == 1 and manual ACK
	// so that it does not ack it and see if it will be redelivered.
	sub1, err = sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	qsetup <- true
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Start 2nd queue subscriber on same group
	sub2, err = sc.QueueSubscribe("foo", "group", cb, stan.AckWait(ackWaitInMs(15)))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Unsubscribe the first member
	sub1.Unsubscribe()
	qsetup <- true
	// The second queue subscriber should receive the first message.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestPersistentStoreQueueSubLeavingUpdateQGroupLastSent(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(50*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		ch <- true
	}
	// Create a queue subscriber with MaxInflight == 1 and manual ACK
	// so that it does not ack it and see if it will be redelivered.
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// send second message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start 2nd queue subscriber on same group
	sub2, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// The second queue subscriber should receive the second message.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Unsubscribe the second member
	sub2.Unsubscribe()
	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Send a third message
	if err := sc.Publish("foo", []byte("msg3")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a third queue subscriber, it should receive message 3
	msgCh := make(chan *stan.Msg)
	lastMsgCb := func(m *stan.Msg) {
		msgCh <- m
	}
	if _, err := sc.QueueSubscribe("foo", "group", lastMsgCb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for msg3 or an error to occur
	gotIt := false
	select {
	case m := <-msgCh:
		if m.Sequence != 3 {
			t.Fatalf("Unexpected message: %v", m)
		} else {
			gotIt = true
			break
		}
	case <-time.After(250 * time.Millisecond):
		// Wait for a bit to see if we receive extraneous messages
		break
	}
	if !gotIt {
		t.Fatal("Did not get message 3")
	}
}

func checkQueueGroupSize(t *testing.T, s *StanServer, channelName, groupName string, expectedExist bool, expectedSize int) {
	cs := channelsGet(t, s.channels, channelName)
	s.mu.RLock()
	groupSize := 0
	group, exist := cs.ss.qsubs[groupName]
	if exist {
		groupSize = len(group.subs)
	}
	s.mu.RUnlock()
	if expectedExist && !exist {
		stackFatalf(t, "Expected group to still exist, does not")
	}
	if !expectedExist && exist {
		stackFatalf(t, "Expected group to not exist, it does")
	}
	if expectedExist {
		if groupSize != expectedSize {
			stackFatalf(t, "Expected group size to be %v, got %v", expectedSize, groupSize)
		}
	}
}

func TestBasicDurableQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()
	if err := sc1.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if count == 1 && m.Sequence == 1 {
			ch <- true
		} else if count == 2 && m.Sequence == 2 {
			ch <- true
		}
	}
	// Create a durable queue subscriber.
	sc2, err := stan.Connect(clusterName, "sc2cid")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group exists
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// For this test, make sure we wait for ack to be processed.
	waitForAcks(t, s, "sc2cid", 1, 0)
	// Close the durable queue sub's connection.
	// This should not close the queue group
	sc2.Close()
	// Check queue group still exist, but size 0
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 0)
	// Send another message
	if err := sc1.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Create a durable queue subscriber on the group.
	sub, err := sc1.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// It should receive message 2 only
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Now unsubscribe the sole member
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Error during Unsubscribe: %v", err)
	}
	// Group should be gone.
	checkQueueGroupSize(t, s, "foo", "qsub:group", false, 0)
}

func TestDurableAndNonDurableQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Expect failure if durable name contains ':'
	_, err := sc.QueueSubscribe("foo", "group", func(_ *stan.Msg) {},
		stan.DurableName("qsub:"))
	if err == nil {
		t.Fatal("Expected error on subscribe")
	}
	if err.Error() != ErrInvalidDurName.Error() {
		t.Fatalf("Expected error %v, got %v", ErrInvalidDurName, err)
	}

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		ch <- true
	}
	// Create a durable queue subscriber.
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a regular one
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check groups exists
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	checkQueueGroupSize(t, s, "foo", "group", true, 1)
	// Wait to receive the messages
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Non durable group should be gone
	checkQueueGroupSize(t, s, "foo", "group", false, 0)
	// Other should exist, but empty
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 0)
}

func TestPersistentStoreDurableQueueSub(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc1, nc1 := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(50*time.Millisecond))
	defer nc1.Close()
	defer sc1.Close()
	if err := sc1.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if count == 1 && m.Sequence == 1 {
			ch <- true
		} else if count == 2 && m.Sequence == 2 {
			ch <- true
		}
	}
	// Create a durable queue subscriber.
	sc2, err := stan.Connect(clusterName, "sc2cid")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// For this test, make sure ack is processed
	waitForAcks(t, s, "sc2cid", 1, 0)
	// Close the durable queue sub's connection.
	// This should not close the queue group
	sc2.Close()
	// Check queue group still exist, but size 0
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 0)
	// Send another message
	if err := sc1.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Stop and restart the server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Create another client connection
	sc2, err = stan.Connect(clusterName, "sc2cid")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	// Create a durable queue subscriber on the group.
	if _, err := sc2.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// It should receive message 2 only
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestPersistentStoreQMemberRemovedFromStore(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()

	ch := make(chan bool)
	// Create the group (adding the first member)
	if _, err := sc1.QueueSubscribe("foo", "group",
		func(_ *stan.Msg) {
			ch <- true
		},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	sc2, err := stan.Connect(clusterName, "othername")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	// Add a second member to the group
	if _, err := sc2.QueueSubscribe("foo", "group", func(_ *stan.Msg) {},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Remove it by closing the connection.
	sc2.Close()

	// Send a message and verify it is received
	if err := sc1.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Have the first leave the group too.
	sc1.Close()

	// Restart the server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Send a new message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Have a member rejoin the group
	if _, err := sc.QueueSubscribe("foo", "group",
		func(_ *stan.Msg) {
			ch <- true
		},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// It should receive the second message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Check server state
	s.mu.RLock()
	cs := channelsLookupOrCreate(t, s, "foo")
	ss := cs.ss
	s.mu.RUnlock()
	ss.RLock()
	qs := ss.qsubs["dur:group"]
	ss.RUnlock()
	if len(qs.subs) != 1 {
		t.Fatalf("Expected only 1 member, got %v", len(qs.subs))
	}
}

func TestPersistentStoreMultipleShadowQSubs(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	s.Shutdown()

	var (
		store stores.Store
		err   error
	)
	limits := stores.DefaultStoreLimits
	switch persistentStoreType {
	case stores.TypeFile:
		store, err = stores.NewFileStore(testLogger, defaultDataStore, &limits)
	case stores.TypeSQL:
		store, err = stores.NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits)
	}
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer store.Close()
	cs, err := store.CreateChannel("foo")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	sub := spb.SubState{
		ID:            1,
		AckInbox:      nats.NewInbox(),
		Inbox:         nats.NewInbox(),
		AckWaitInSecs: 30,
		MaxInFlight:   10,
		LastSent:      1,
		IsDurable:     true,
		QGroup:        "dur:queue",
	}
	cs.Subs.CreateSub(&sub)
	sub.ID = 2
	sub.LastSent = 2
	cs.Subs.CreateSub(&sub)
	store.Close()

	// Should not panic
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	scs := channelsLookupOrCreate(t, s, "foo")
	ss := scs.ss
	ss.RLock()
	qs := ss.qsubs["dur:queue"]
	ss.RUnlock()
	if qs == nil {
		t.Fatal("Should have recovered queue group")
	}
	qs.RLock()
	shadow := qs.shadow
	lastSent := qs.lastSent
	qs.RUnlock()
	if shadow == nil {
		t.Fatal("Should have recovered a shadow queue sub")
	}
	if shadow.ID != 2 || lastSent != 2 {
		t.Fatalf("Recovered shadow queue sub should be ID 2, lastSent 2, got %v, %v", shadow.ID, lastSent)
	}
}

func TestQueueWithOneStalledMemberDoesNotStallGroup(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	ch := make(chan bool, 1)
	// Create a member with low MaxInflight and Manual ack mode
	if _, err := sc.QueueSubscribe("foo", "queue",
		func(_ *stan.Msg) {
			ch <- true
		},
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Check message is received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Create another member with higher MaxInFlight and manual ack mode too.
	count := 0
	total := 5
	if _, err := sc.QueueSubscribe("foo", "queue",
		func(_ *stan.Msg) {
			count++
			if count == total {
				ch <- true
			}
		},
		stan.MaxInflight(10),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Send messages and ensure they are received by 2nd member
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
}

type queueGroupStalledMsgStore struct {
	stores.MsgStore
	lookupCh chan struct{}
}

func (s *queueGroupStalledMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	select {
	case s.lookupCh <- struct{}{}:
	default:
	}
	return s.MsgStore.Lookup(seq)
}

func TestQueueGroupStalledSemantics(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(50*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		ch <- true
	}
	// Create a member with manual ack and MaxInFlight of 1
	if _, err := sc.QueueSubscribe("foo", "queue", cb,
		stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// This member is stalled, and since there is only one member, the
	// group itself should be stalled.
	checkStalled := func(expected bool) {
		var stalled bool
		timeout := time.Now().Add(time.Second)
		for time.Now().Before(timeout) {
			c := channelsGet(t, s.channels, "foo")
			c.ss.RLock()
			qs := c.ss.qsubs["queue"]
			c.ss.RUnlock()
			qs.RLock()
			stalled = qs.stalledSubCount == len(qs.subs)
			qs.RUnlock()
			if stalled != expected {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			break
		}
		if stalled != expected {
			stackFatalf(t, "Expected stalled to be %v, got %v", expected, stalled)
		}
	}
	checkStalled(true)

	// Create another member that has a higher MaxInFlight
	if _, err := sc.QueueSubscribe("foo", "queue", cb,
		stan.SetManualAckMode(), stan.MaxInflight(3)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// That should make the queue sub not stalled
	checkStalled(false)
	// Publish 3 messages, check state for each iteration
	for i := 0; i < 3; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
		if err := Wait(ch); err != nil {
			t.Fatal("Did not get our message")
		}
		stalled := i == 2
		checkStalled(stalled)
	}
	checkStalled(true)
	// Replace store with one that will report if Lookup was used
	s.channels.Lock()
	c := s.channels.channels["foo"]
	ms := &queueGroupStalledMsgStore{c.store.Msgs, make(chan struct{}, 2)}
	c.store.Msgs = ms
	s.channels.Unlock()
	// Publish a message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Check that no Lookup was done
	select {
	case <-ms.lookupCh:
		t.Fatalf("Lookup should not have been invoked")
	case <-time.After(50 * time.Millisecond):
	}

	// Restart server...
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Serve will check if messages need to be redelivered, but since it
	// is less than the AckWait of 30 seconds, it won't send them. Still,
	// they are looked up at this point. So wait a little before swapping
	// with mock store.
	time.Sleep(100 * time.Millisecond)

	// Replace store with one that will report if Lookup was used
	s.channels.Lock()
	c = s.channels.channels["foo"]
	orgMS := c.store.Msgs
	ms = &queueGroupStalledMsgStore{c.store.Msgs, make(chan struct{}, 2)}
	c.store.Msgs = ms
	s.channels.Unlock()

	// Publish a message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	select {
	case <-ms.lookupCh:
		t.Fatalf("Lookup should not have been invoked")
	case <-time.After(50 * time.Millisecond):
	}

	// Then replace store with original one before exit.
	s.channels.Lock()
	c = s.channels.channels["foo"]
	c.store.Msgs = orgMS
	s.channels.Unlock()
}

func TestQueueGroupUnStalledOnAcks(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Produce messages to a channel
	msgCount := 10000
	msg := []byte("hello")
	for i := 0; i < msgCount; i++ {
		if i == msgCount-1 {
			if err := sc.Publish("foo", msg); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
		} else {
			if _, err := sc.PublishAsync("foo", msg, func(guid string, puberr error) {
				if puberr != nil {
					t.Fatalf("Error on publish %q: %v", guid, puberr)
				}
			}); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
		}
	}

	count := int32(0)
	doneCh := make(chan bool)
	mcb := func(m *stan.Msg) {
		if c := atomic.AddInt32(&count, 1); c == int32(msgCount) {
			doneCh <- true
		}
	}

	// Create multiple queue subs
	for i := 0; i < 4; i++ {
		if _, err := sc.QueueSubscribe("foo", "bar", mcb, stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}
	// Wait for all messages to be received
	if err := Wait(doneCh); err != nil {
		t.Fatal("Did not get our messages")
	}
}

func TestQueueGroupAckTimerStoppedWhenMemberLeavesGroup(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	for i := 0; i < 1024; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	ch := make(chan error, 1)
	if _, err := sc.QueueSubscribe("foo", "bar", func(m *stan.Msg) {
		subs := s.clients.getSubs(clientName)
		if len(subs) != 1 {
			ch <- fmt.Errorf("Wrong subs len=%v", len(subs))
			return
		}
		sub := subs[0]
		m.Sub.Close()
		// Wait for sub close to be processed...
		waitForNumSubs(t, s, clientName, 0)
		// Then check that ackTimer is not set
		sub.RLock()
		timerSet := sub.ackTimer != nil
		sub.RUnlock()
		if timerSet {
			ch <- fmt.Errorf("Timer should not be set")
		} else {
			ch <- nil
		}
	}, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	select {
	case e := <-ch:
		if e != nil {
			t.Fatalf("Error: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for callback to fire")
	}
}

func TestPersistentStoreNewOnHoldClearedAfterRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Create durable queue sub that does not ack messages so we
	// get a pending ack on server restart.
	ch := make(chan bool, 1)
	if _, err := sc.QueueSubscribe("foo", "bar", func(_ *stan.Msg) {
		ch <- true
	}, stan.DurableName("dur"), stan.SetManualAckMode()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Publish a message
	if err := sc.Publish("foo", []byte("1")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection/sub
	sc.Close()

	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	// Create connection and publish a new message
	sc = NewDefaultConnection(t)
	defer sc.Close()
	if err := sc.Publish("foo", []byte("2")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Restart the queue durable sub, it should get the redelivered message
	// and the new message.
	msgCh := make(chan *stan.Msg, 1)
	if _, err := sc.QueueSubscribe("foo", "bar", func(m *stan.Msg) {
		msgCh <- m
	}, stan.DurableName("dur")); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Check received messages
	for i := uint64(1); i < 3; i++ {
		select {
		case m := <-msgCh:
			assertMsg(t, m.MsgProto, []byte(fmt.Sprintf("%v", i)), i)
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get expected message %v", i)
		}
	}
}

func TestQueueGroupNotStalledOnMemberLeaving(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()

	sc2, err := stan.Connect(clusterName, clientName+"2")
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer sc2.Close()

	total := 1000
	for i := 0; i < total; i++ {
		sc1.Publish("foo", []byte("hello"))
	}

	sub1GotIt := make(chan struct{}, 1)
	if _, err := sc1.QueueSubscribe("foo", "bar", func(m *stan.Msg) {
		select {
		case sub1GotIt <- struct{}{}:
		default:
		}
	}, stan.DeliverAllAvailable(), stan.MaxInflight(5)); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	ch := make(chan struct{}, 1)
	waitForSub1 := true
	if _, err := sc2.QueueSubscribe("foo", "bar", func(m *stan.Msg) {
		if waitForSub1 {
			waitForSub1 = false
			<-sub1GotIt
			sc1.Close()
		}
		if m.Sequence == uint64(total) {
			ch <- struct{}{}
		}
	}, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all msgs")
	}
}

type checkStoreLookup struct {
	stores.MsgStore
	errCh chan error
}

func (s *checkStoreLookup) Lookup(seq uint64) (*pb.MsgProto, error) {
	buf := make([]byte, 10000)
	n := runtime.Stack(buf, false)
	if strings.Contains(string(buf[:n]), "Remove") {
		select {
		case s.errCh <- fmt.Errorf("Lookup invoked from Remove"):
		default:
		}
	}
	return s.MsgStore.Lookup(seq)
}

func TestQueueNoRedeliveryDuringSubClose(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	errCh := make(chan error, 1)

	c, err := s.lookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	c.store.Msgs = &checkStoreLookup{MsgStore: c.store.Msgs, errCh: errCh}

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()

	ch := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	if _, err := sc1.QueueSubscribe("foo", "bar", func(m *stan.Msg) {
		<-ch
		if m.Sequence == 10 {
			wg.Done()
		}
	}); err != nil {
		t.Fatalf("Error on queue subscribe: %v", err)
	}

	sc2, err := stan.Connect(clusterName, clientName+"2")
	if err != nil {
		t.Fatalf("Error on connect")
	}
	defer sc2.Close()

	if _, err := sc2.QueueSubscribe("foo", "bar", func(_ *stan.Msg) {},
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Error on queue subscribe: %v", err)
	}

	// Send some messages, since sub1 does not ack due to channel
	// being blocked and sub2 does not ack due to manual ack mode,
	// messages should be distributed.
	for i := 0; i < 10; i++ {
		if err := sc2.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Error on publish")
		}
	}

	if err := sc2.Close(); err != nil {
		t.Fatalf("Error on publish")
	}

	close(ch)
	wg.Wait()

	select {
	case e := <-errCh:
		t.Fatal(e.Error())
	default:
	}
}

func TestPersistentStoreDurableQueueSubRaceBetweenCreateAndClose(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	qsub1, err := sc.QueueSubscribe("foo", "bar", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	cs := channelsGet(t, s.channels, "foo")
	ss := &mockedSubStore{SubStore: cs.store.Subs}
	cs.store.Subs = ss

	// Will make the store create sub pause so that the addition of the new
	// queue sub in the go routine will be delayed.
	ss.Lock()
	ss.ch = make(chan bool, 1)
	ss.Unlock()
	closeErrCh := make(chan error)
	createErrCh := make(chan error)

	sc2, err := stan.Connect(clusterName, clientName+"2")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc2.Close()

	// Since close operation uses a barrier that ensures that
	// other operations complete, we need to start processing
	// of qsub1.Close() start before we start the new queue sub,
	// but ensure that it stops at one point. We will use the
	// closeMu mutex to artificially block it.
	s.closeMu.Lock()
	go func() {
		closeErrCh <- qsub1.Close()
	}()
	// Let the Close() proceed a bit...
	time.Sleep(100 * time.Millisecond)
	go func() {
		_, err := sc2.QueueSubscribe("foo", "bar", func(_ *stan.Msg) {}, stan.DurableName("dur"))
		createErrCh <- err
	}()

	// Let the QueueSubscribe() proceed a bit..
	time.Sleep(100 * time.Millisecond)

	// Now release the qsub1 Close()
	s.closeMu.Unlock()

	// Let the Close() complete, but we have to release the CreateSub()
	// blockage from a different go routine otherwise with the fix,
	// the Close() would still be blocked.
	go func() {
		time.Sleep(100 * time.Millisecond)
		// Now release the addition to the subStore of the second queue sub
		ss.ch <- true
	}()

	// Wait for the close to complete
	if err := <-closeErrCh; err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// Wait for create to complete
	if err := <-createErrCh; err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Now close 2nd queue sub (connection close will do that for us)
	sc2.Close()
	// We don't need that connection either.
	sc.Close()

	// Shutdown and restart the server and make sure that we did not
	// introduce a 2nd shadow subscription.
	s.Shutdown()
	l := &captureWarnLogger{}
	opts.CustomLogger = l
	s = runServerWithOpts(t, opts, nil)

	cs = channelsGet(t, s.channels, "foo")
	qs := cs.ss.qsubs["dur:bar"]
	qs.RLock()
	hasShadow := qs.shadow != nil
	qs.RUnlock()
	if !hasShadow {
		t.Fatalf("Should have a shadow subscription")
	}
	var hasDuplicate bool
	l.Lock()
	for _, w := range l.warnings {
		if strings.Contains(w, "Duplicate shadow durable") {
			hasDuplicate = true
			break
		}
	}
	l.Unlock()
	if hasDuplicate {
		t.Fatalf("Duplicate shadow subscription found!")
	}
}

func TestQueueRedeliveryCount(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	errCh := make(chan error, 2)
	ch := make(chan bool, 1)
	var mu sync.Mutex
	var prev uint32
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			mu.Lock()
			if m.RedeliveryCount != prev+1 {
				m.Sub.Close()
				errCh <- fmt.Errorf("previous was %v, current %v", prev, m.RedeliveryCount)
				mu.Unlock()
				return
			}
			prev = m.RedeliveryCount
			if m.RedeliveryCount == 5 {
				m.Ack()
				ch <- true
			}
			mu.Unlock()
		}
	}

	for i := 0; i < 2; i++ {
		if _, err := sc.QueueSubscribe("foo", "bar", cb, stan.AckWait(ackWaitInMs(50)), stan.SetManualAckMode()); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
	}

	sc.Publish("foo", []byte("msg"))

	select {
	case e := <-errCh:
		t.Fatal(e.Error())
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("Timedout")
	}

	// Make sure that deliver count map gets cleaned-up once messages are acknowledged.
	sub := s.clients.getSubs(clientName)[0]
	sub.RLock()
	qs := sub.qstate
	sub.RUnlock()
	waitForCount(t, 0, func() (string, int) {
		qs.RLock()
		l := len(qs.rdlvCount)
		qs.RUnlock()
		return "queue redelivery map size", l
	})
}
