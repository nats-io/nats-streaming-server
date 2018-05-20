// Copyright 2016-2018 The NATS Authors
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
)

func TestSubscribeShrink(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	nsubs := 10
	subs := make([]stan.Subscription, 0, nsubs)
	for i := 1; i <= nsubs; i++ {
		sub, err := sc.Subscribe("foo", nil)
		if err != nil {
			t.Fatalf("Got an error on subscribe: %v\n", err)
		}
		subs = append(subs, sub)
	}
	// Check number of subs
	waitForNumSubs(t, s, clientName, nsubs)
	// Now unsubscribe them all
	for _, sub := range subs {
		err := sub.Unsubscribe()
		if err != nil {
			t.Fatalf("Got an error on unsubscribe: %v\n", err)
		}
	}
	// Check number of subs
	waitForNumSubs(t, s, clientName, 0)
	// Make sure that array size reduced
	client := s.clients.lookup(clientName)
	if client == nil {
		t.Fatal("Client should exist")
	}
	client.RLock()
	defer client.RUnlock()
	if cap(client.subs) >= nsubs {
		t.Fatalf("Expected array capacity to have gone down, got %v", len(client.subs))
	}
}

func TestSubStartPositionNewOnly(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(_ *stan.Msg) {
		rch <- true
	}

	// Start a subscriber with "NewOnly" as start position.
	// Since there was no message previously sent, it should
	// not receive anything yet.
	sub, err := sc.Subscribe("foo", cb, stan.StartAt(pb.StartPosition_NewOnly))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait a little bit and ensure no message was received
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}

	// Send a message now.
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}

	// Start another subscriber with "NewOnly" as start position.
	sub2, err := sc.Subscribe("foo", cb, stan.StartAt(pb.StartPosition_NewOnly))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// It should not receive anything
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}
}

func TestSubStartPositionLastReceived(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(_ *stan.Msg) {
		rch <- true
	}

	// Start a subscriber with "LastReceived" as start position.
	// Since there was no message previously sent, it should
	// not receive anything yet.
	sub, err := sc.Subscribe("foo", cb, stan.StartWithLastReceived())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait a little bit and ensure no message was received
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}

	// Send a message now.
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}

	rch = make(chan bool)

	cb = func(m *stan.Msg) {
		if string(m.Data) == "msg2" {
			rch <- true
		}
	}

	// Send two messages
	if err := sc.Publish("bar", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("bar", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Start a subscriber with "LastReceived" as start position.
	sub2, err := sc.Subscribe("bar", cb, stan.StartWithLastReceived())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// The second message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
}

func TestSubStartPositionFirstSequence(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(_ *stan.Msg) {
		rch <- true
	}

	// Start a subscriber with "FirstSequence" as start position.
	// Since there was no message previously sent, it should
	// not receive anything yet.
	sub, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait a little bit and ensure no message was received
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}

	// Send a message now.
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}

	mch := make(chan *stan.Msg, 2)

	cb = func(m *stan.Msg) {
		mch <- m
	}

	// Send two messages
	if err := sc.Publish("bar", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("bar", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Start a subscriber with "FirstPosition" as start position.
	sub2, err := sc.Subscribe("bar", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	first := true
	for {
		select {
		case m := <-mch:
			if first {
				if string(m.Data) != "msg1" {
					t.Fatalf("Expected msg1 first, got %v", string(m.Data))
				}
				first = false
			} else {
				if string(m.Data) != "msg2" {
					t.Fatalf("Expected msg2 second, got %v", string(m.Data))
				}
				// We are done!
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not get our message")
		}
	}
}

func TestSubStartPositionSequenceStart(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	opts.MaxMsgs = 10
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Send more messages than max so that first is not 1
	for i := 0; i < opts.MaxMsgs+10; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Check first/last
	firstSeq, lastSeq := msgStoreFirstAndLastSequence(t, channelsGet(t, s.channels, "foo").store.Msgs)
	if firstSeq != uint64(opts.MaxMsgs+1) {
		t.Fatalf("Expected first sequence to be %v, got %v", uint64(opts.MaxMsgs+1), firstSeq)
	}
	if lastSeq != uint64(opts.MaxMsgs+10) {
		t.Fatalf("Expected last sequence to be %v, got %v", uint64(opts.MaxMsgs+10), lastSeq)
	}

	rch := make(chan bool)
	first := int32(1)
	// Create subscriber with sequence below firstSeq, it should not fail
	// and receive message with sequence == firstSeq
	sub, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == firstSeq {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", firstSeq, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtSequence(1), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure correct msg is received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our message")
	}
	sub.Unsubscribe()

	// Create subscriber with sequence higher than lastSeq, it should not
	// fail, but not receive until we send a new message.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == lastSeq+1 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", lastSeq+1, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtSequence(lastSeq+1), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Make sure correct msg is received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our message")
	}
	sub.Unsubscribe()

	// Create a subscriber with sequence somewhere in range.
	// It should receive that message.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == firstSeq+3 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", firstSeq+3, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtSequence(firstSeq+3), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure correct msg is received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our message")
	}
	sub.Unsubscribe()
}

func TestSubStartPositionTimeDelta(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	// Send a message.
	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait 1.5 seconds.
	time.Sleep(1500 * time.Millisecond)
	// Sends a second message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Create subscriber with TimeDelta in the past, should
	// not fail and get first message.
	first := int32(1)
	sub, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == 1 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", 1, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtTimeDelta(10*time.Second), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Message 1 should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
	sub.Unsubscribe()

	// Start a subscriber with "TimeDelta" as start position.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == 2 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", 2, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtTimeDelta(1*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Message 2 should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
	sub.Unsubscribe()

	// Wait a bit
	time.Sleep(250 * time.Millisecond)
	// Create a subscriber with delta that would point to
	// after the end of the log.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == 3 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", 3, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtTimeDelta(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("msg3")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Message 3 should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
	sub.Unsubscribe()
}

func TestSubStartPositionWithDurable(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	ch := make(chan bool)
	expected := uint64(1)
	cb := func(m *stan.Msg) {
		if m.Sequence == atomic.LoadUint64(&expected) {
			sc.Close()
			ch <- true
		}
	}
	// Start a durable at first index
	if _, err := sc.Subscribe("foo", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message to be received and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Restart the durable with different start position. It should
	// be ignored and resume from where it left of.
	// Since connection is closed in the callback last message is not
	// acked, but that's fine. We still wait for the next message.
	atomic.StoreUint64(&expected, 2)
	if _, err := sc.Subscribe("foo", cb,
		stan.StartAtSequence(uint64(total-5)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start sequence above total message
	atomic.StoreUint64(&expected, 3)
	if _, err := sc.Subscribe("foo", cb,
		stan.StartAtSequence(uint64(total+10)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start time in future, which should not fail if
	// start position is ignored for a resumed durable.
	atomic.StoreUint64(&expected, 4)
	if _, err := sc.Subscribe("foo", cb,
		stan.StartAtTime(time.Now().Add(10*time.Second)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestSubStartPositionWithDurableQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	ch := make(chan bool)
	expected := uint64(1)
	cb := func(m *stan.Msg) {
		if m.Sequence == atomic.LoadUint64(&expected) {
			sc.Close()
			ch <- true
		}
	}
	// Start a durable queue subscriber at first index
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message to be received and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Restart the queue durable with different start position. It should
	// be ignored and resume from where it left of.
	// Since connection is closed in the callback last message is not
	// acked, but that's fine. We still wait for the next message.
	atomic.StoreUint64(&expected, 2)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total-5)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start sequence above total message
	atomic.StoreUint64(&expected, 3)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total+10)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start time in future, which should not fail if
	// start position is ignored for a resumed durable.
	atomic.StoreUint64(&expected, 4)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtTime(time.Now().Add(10*time.Second)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestSubStartPositionWithQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	ch := make(chan bool)
	expected := uint64(1)
	cb := func(m *stan.Msg) {
		if m.Sequence == atomic.LoadUint64(&expected) {
			ch <- true
		}
	}
	// Start a queue subsriber at first index
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	atomic.StoreUint64(&expected, 2)
	// Add a member to the group with start sequence
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total-5)),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Try with start sequence above total message
	atomic.StoreUint64(&expected, 3)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total+10)),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Try with start time in future, which should not fail if
	// start position is ignored for queue group
	atomic.StoreUint64(&expected, 4)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtTime(time.Now().Add(10*time.Second)),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestAckTimerSetOnStalledSub(t *testing.T) {

	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	createDur := func() *subState {
		durName := "mydur"

		// Create a durable with MaxInFlight==1 and ack mode (don't ack the message)
		if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {},
			stan.DurableName(durName),
			stan.DeliverAllAvailable(),
			stan.SetManualAckMode(),
			stan.MaxInflight(1)); err != nil {
			stackFatalf(t, "Unexpected error on subscribe: %v", err)
		}
		// Make sure durable is created
		subs := checkSubs(t, s, clientName, 1)
		if len(subs) != 1 {
			stackFatalf(t, "Should be only 1 durable, got %v", len(subs))
		}
		return subs[0]
	}

	// Create durable
	createDur()

	// Wait for durable to be created
	waitForNumSubs(t, s, clientName, 1)

	// Close
	sc.Close()

	// Recreate connection
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Restart durable
	dur := createDur()

	// Wait for durable to be created
	waitForNumSubs(t, s, clientName, 1)

	// Now check that the timer is set
	dur.RLock()
	timerSet := dur.ackTimer != nil
	dur.RUnlock()
	if !timerSet {
		t.Fatal("Timer should have been set")
	}
}

func TestPersistentStoreNonDurableSubRemovedOnConnClose(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Create a subscription
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Close the client connection
	sc.Close()
	// Shutdown the server
	s.Shutdown()
	// Open the store directly and verify that the sub record is not even found.
	limits := stores.DefaultStoreLimits
	var (
		store stores.Store
		err   error
	)
	switch persistentStoreType {
	case stores.TypeFile:
		store, err = stores.NewFileStore(testLogger, defaultDataStore, &limits)
	case stores.TypeSQL:
		store, err = stores.NewSQLStore(testLogger, testSQLDriver, testSQLSource, &limits)
	}
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer store.Close()
	recoveredState, err := store.Recover()
	if err != nil {
		t.Fatalf("Error recovering state: %v", err)
	}
	if recoveredState == nil {
		t.Fatal("Expected to recover state, got none")
	}
	rc := recoveredState.Channels["foo"]
	if rc == nil {
		t.Fatalf("Channel foo should have been recovered")
	}
	rsubsArray := rc.Subscriptions
	if len(rsubsArray) > 0 {
		t.Fatalf("Expected no subscription to be recovered from store, got %v", len(rsubsArray))
	}
}

func TestPersistentStoreIgnoreRecoveredSubForUnknownClientID(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(50*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// For delete the client
	s.clients.unregister(clientName)

	// Shutdown the server
	s.Shutdown()

	// Restart the server
	s = runServerWithOpts(t, opts, nil)

	// Check that client does not exist
	if s.clients.lookup(clientName) != nil {
		t.Fatal("Client should not have been recovered")
	}
	// Channel would be recovered
	cs := channelsGet(t, s.channels, "foo")
	// But there should not be any subscription
	ss := cs.ss
	ss.RLock()
	numSubs := len(ss.psubs)
	ss.RUnlock()
	if numSubs > 0 {
		t.Fatalf("Should not have restored subscriptions, got %v", numSubs)
	}
}

func TestTraceSubCreateCloseUnsubscribeRequests(t *testing.T) {
	// This logger captures any log.XXX() call into a single logger.msg string.
	// Will use that to compare the expected debug trace.
	logger := &dummyLogger{}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	type optAndText struct {
		opt    stan.SubscriptionOption
		txt    string
		suffix bool
	}
	subOpts := []optAndText{
		optAndText{stan.StartAt(pb.StartPosition_NewOnly), "new-only, seq=1", true},
		optAndText{stan.StartWithLastReceived(), "last message, seq=1", true},
		optAndText{stan.StartAtSequence(10), "from sequence, asked_seq=10 actual_seq=1", true},
		optAndText{stan.StartAt(pb.StartPosition_First), "from beginning, seq=1", true},
		optAndText{stan.StartAtTimeDelta(time.Hour), "from time time=", false},
	}
	for _, o := range subOpts {
		sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, o.opt)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		waitForNumSubs(t, s, clientName, 1)
		logger.Lock()
		msg := logger.msg
		logger.Unlock()
		if o.suffix {
			if !strings.HasSuffix(msg, o.txt) {
				t.Fatalf("Execpected suffix %q, got %q", o.txt, msg)
			}
		} else {
			if !strings.Contains(msg, o.txt) {
				t.Fatalf("Execpected to contain %q, got %q", o.txt, msg)
			}
			if !strings.HasSuffix(msg, "seq=1") {
				t.Fatalf("Execpected suffix \"seq=1\", got %q", msg)
			}
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Fatalf("Error on unsubscribe: %v", err)
		}
	}
	checkTrace := func(trace string) {
		logger.Lock()
		msg := logger.msg
		logger.Unlock()
		trace = "[Client:me] " + trace
		if !strings.Contains(msg, trace) {
			stackFatalf(t, "Expected trace %q, got %q", trace, msg)
		}
	}
	type startSub struct {
		start      func() (stan.Subscription, error)
		startTrace string
		end        func(sub stan.Subscription) error
		endTrace   string
	}
	ssubs := []startSub{
		// New plain subscription followed by Unsubscribe should remove the subscription
		startSub{
			start:      func() (stan.Subscription, error) { return sc.Subscribe("foo", func(_ *stan.Msg) {}) },
			startTrace: "Started new subscription",
			end:        func(sub stan.Subscription) error { return sub.Unsubscribe() },
			endTrace:   "Removed subscription",
		},
		// New plain subscription followed by Close should remove the subscription
		startSub{
			start:      func() (stan.Subscription, error) { return sc.Subscribe("foo", func(_ *stan.Msg) {}) },
			startTrace: "Started new subscription",
			end:        func(sub stan.Subscription) error { return sub.Close() },
			endTrace:   "Removed subscription",
		},
		// New durable subscription followed by Close should suspend the subscription
		startSub{
			start: func() (stan.Subscription, error) {
				return sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
			},
			startTrace: "Started new durable subscription",
			end:        func(sub stan.Subscription) error { return sub.Close() },
			endTrace:   "Suspended durable subscription",
		},
		// Resuming the durable subscription, followed by Unsubscribe should removed the subscription
		startSub{
			start: func() (stan.Subscription, error) {
				return sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
			},
			startTrace: "Resumed durable subscription",
			end:        func(sub stan.Subscription) error { return sub.Unsubscribe() },
			endTrace:   "Removed durable subscription",
		},
		// Non durable queue subscribption
		startSub{
			start:      func() (stan.Subscription, error) { return sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}) },
			startTrace: "Started new queue subscription",
			end:        func(sub stan.Subscription) error { return nil }, endTrace: "",
		},
		// Adding a member followed by Unsubscribe should simply remove this member.
		startSub{
			start:      func() (stan.Subscription, error) { return sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}) },
			startTrace: "Added member to queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Unsubscribe() },
			endTrace:   "Removed member from queue subscription",
		},
		// Adding a member followed by Close should simply remove this member.
		startSub{
			start:      func() (stan.Subscription, error) { return sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}) },
			startTrace: "Added member to queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Close() },
			endTrace:   "Removed member from queue subscription",
		},
		// New queue subscription followed by Unsubscribe should remove the queue subscription
		startSub{
			start:      func() (stan.Subscription, error) { return sc.QueueSubscribe("foo", "queue2", func(_ *stan.Msg) {}) },
			startTrace: "Started new queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Unsubscribe() },
			endTrace:   "Removed queue subscription",
		},
		// New queue subscription followed by Close should remove the queue subscription
		startSub{
			start:      func() (stan.Subscription, error) { return sc.QueueSubscribe("foo", "queue2", func(_ *stan.Msg) {}) },
			startTrace: "Started new queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Close() },
			endTrace:   "Removed queue subscription",
		},
		// New durable queue subscription followed by Close should suspend the subscription
		startSub{
			start: func() (stan.Subscription, error) {
				return sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}, stan.DurableName("dur"))
			},
			startTrace: "Started new durable queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Close() },
			endTrace:   "Suspended durable queue subscription",
		},
		// Resuming durable queue subscription
		startSub{
			start: func() (stan.Subscription, error) {
				return sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}, stan.DurableName("dur"))
			},
			startTrace: "Resumed durable queue subscription",
			end:        func(sub stan.Subscription) error { return nil }, endTrace: "",
		},
		// Adding a member followed by Close should remove this member only
		startSub{
			start: func() (stan.Subscription, error) {
				return sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}, stan.DurableName("dur"))
			},
			startTrace: "Added member to durable queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Close() },
			endTrace:   "Removed member from durable queue subscription",
		},
		// Adding a member followed by Unsubscribe should remove this member only
		startSub{
			start: func() (stan.Subscription, error) {
				return sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}, stan.DurableName("dur"))
			},
			startTrace: "Added member to durable queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Unsubscribe() },
			endTrace:   "Removed member from durable queue subscription",
		},
		// New durable subscription followed by Unsubscribe should remove the subscription
		startSub{
			start: func() (stan.Subscription, error) {
				return sc.QueueSubscribe("foo", "queue2", func(_ *stan.Msg) {}, stan.DurableName("dur"))
			},
			startTrace: "Started new durable queue subscription",
			end:        func(sub stan.Subscription) error { return sub.Unsubscribe() },
			endTrace:   "Removed durable queue subscription",
		},
	}
	for _, s := range ssubs {
		sub, err := s.start()
		if err != nil {
			t.Fatalf("Error starting subscription: %v", err)
		}
		checkTrace(s.startTrace)
		if err := s.end(sub); err != nil {
			t.Fatalf("Error ending subscription: %v", err)
		}
		if s.endTrace != "" {
			checkTrace(s.endTrace)
		}
	}
}

func TestSubStalledSemantics(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	ch := make(chan bool, 1)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {
		ch <- true
	}, stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Replace store with one that will report if Lookup was used
	s.channels.Lock()
	c := s.channels.channels["foo"]
	orgMS := c.store.Msgs
	ms := &queueGroupStalledMsgStore{c.store.Msgs, make(chan struct{}, 1)}
	c.store.Msgs = ms
	s.channels.Unlock()

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

func TestSubAckInboxFromOlderStore(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForPersistentStore()
	opts.NATSServerURL = "nats://127.0.0.1:4222"
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	if _, err := s.clients.register(&spb.ClientInfo{ID: "me2", HbInbox: nats.NewInbox()}); err != nil {
		t.Fatalf("Error registering client: %v", err)
	}
	c, err := s.lookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	sub := &spb.SubState{
		ClientID:      "me2",
		AckInbox:      nats.NewInbox(),
		Inbox:         nats.NewInbox(),
		MaxInFlight:   10,
		AckWaitInSecs: 1,
	}
	if err := c.store.Subs.CreateSub(sub); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	s.Shutdown()

	// Setup manual sub on Inbox
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	ch := make(chan bool, 1)
	errCh := make(chan error, 1)
	if _, err := nc.Subscribe(sub.Inbox, func(raw *nats.Msg) {
		msg := &pb.MsgProto{}
		err := msg.Unmarshal(raw.Data)
		if err != nil {
			panic(fmt.Errorf("Error processing unmarshal for msg: %v", err))
		}
		if !msg.Redelivered {
			// Send an ack back
			ack := pb.Ack{
				Subject:  "foo",
				Sequence: msg.Sequence,
			}
			ackBytes, err := ack.Marshal()
			if err != nil {
				panic(fmt.Errorf("Error marshaling ack: %v", err))
			}
			nc.Publish(sub.AckInbox, ackBytes)
			ch <- true
		} else {
			errCh <- fmt.Errorf("Msg %v was redelivered", msg)
		}
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	s = runServerWithOpts(t, opts, nil)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish")
	}
	// Wait for the message to be received and ack sent back.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Now wait for more than AckWait to be sure that message is not redelivered
	select {
	case e := <-errCh:
		t.Fatalf(e.Error())
	case <-time.After(1500 * time.Millisecond):
		// ok...
	}
}
