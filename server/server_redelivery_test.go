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
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

func TestRedelivery(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer sc.Close()

	rch := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
			rch <- true
		}
	}

	// Create a plain sub
	if _, err := sc.Subscribe("foo", cb, stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(200))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Send first message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Add a delay before the next message
	time.Sleep(100 * time.Millisecond)
	// Send second message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	subs := checkSubs(t, s, clientName, 1)
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if sub.acksPending == nil || len(sub.acksPending) != 2 {
			t.Fatalf("Expected to have two ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer == nil {
			t.Fatalf("Expected timer to be set")
		}
	}(subs[0])

	for i := 0; i < 2; i++ {
		if err := Wait(rch); err != nil {
			t.Fatalf("Messages not redelivered")
		}
	}

	// Wait for another ackWait to check if timer is cleared
	time.Sleep(250 * time.Millisecond)

	// Check state
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if len(sub.acksPending) != 0 {
			t.Fatalf("Expected to have no ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer != nil {
			t.Fatalf("Expected timer to be nil")
		}
	}(subs[0])
}

func TestMultipleRedeliveries(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	dlvTimes := make(map[uint64]int64)
	mu := &sync.Mutex{}
	sent := 5
	count := 0
	ch := make(chan bool)
	ackWait := int64(15 * time.Millisecond)
	lowBound := int64(float64(ackWait) * 0.5)
	highBound := int64(float64(ackWait) * 1.7)
	errCh := make(chan error)
	cb := func(m *stan.Msg) {
		now := time.Now().UnixNano()
		mu.Lock()
		lastDlv := dlvTimes[m.Sequence]
		if lastDlv != 0 && (now < lastDlv-lowBound || now > lastDlv+highBound) {
			if len(errCh) == 0 {
				errCh <- fmt.Errorf("Message %d redelivered %v instead of [%v,%v] after last (re)delivery",
					m.Sequence, time.Duration(now-lastDlv), time.Duration(lowBound), time.Duration(highBound))
				mu.Unlock()
				return
			}
		} else {
			dlvTimes[m.Sequence] = now
		}
		if m.Redelivered {
			count++
			if count == 2*sent*4 {
				// we want at least 4 redeliveries
				ch <- true
			}
		}
		mu.Unlock()
	}
	// Create regular subscriber
	if _, err := sc.Subscribe("foo", cb,
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(15))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// And two queue subscribers from same group
	for i := 0; i < 2; i++ {
		if _, err := sc.QueueSubscribe("foo", "bar", cb,
			stan.SetManualAckMode(),
			stan.AckWait(ackWaitInMs(15))); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}

	for i := 0; i < 5; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Wait for all redeliveries or errors
	select {
	case e := <-errCh:
		t.Fatal(e)
	case <-ch: // all good
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get all our redeliveries")
	}
}

func TestRedeliveryRace(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.AckWait(ackWaitInMs(15)), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	sub.Unsubscribe()
}

func TestQueueRedelivery(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer sc.Close()

	rch := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
			rch <- true
		}
	}

	// Create a queue subscriber
	if _, err := sc.QueueSubscribe("foo", "group", cb, stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(50))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Send first message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Add a delay before the next message
	time.Sleep(25 * time.Millisecond)
	// Send second message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	subs := checkSubs(t, s, clientName, 1)
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if sub.acksPending == nil || len(sub.acksPending) != 2 {
			t.Fatalf("Expected to have two ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer == nil {
			t.Fatalf("Expected timer to be set")
		}
	}(subs[0])

	for i := 0; i < 2; i++ {
		if err := Wait(rch); err != nil {
			t.Fatalf("Messages not redelivered")
		}
	}

	// Wait for another ackWait to check if timer is cleared
	time.Sleep(75 * time.Millisecond)

	// Check state
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if len(sub.acksPending) != 0 {
			t.Fatalf("Expected to have no ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer != nil {
			t.Fatalf("Expected timer to be nil")
		}
	}(subs[0])
}

func TestDurableRedelivery(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	ch := make(chan bool)
	rch := make(chan bool)
	errors := make(chan error, 5)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		switch count {
		case 1:
			ch <- true
		case 2:
			rch <- true
		default:
			errors <- fmt.Errorf("Unexpected message %v", m)
		}
	}

	sc := NewDefaultConnection(t)
	defer sc.Close()

	_, err := sc.Subscribe("foo", cb, stan.DurableName("dur"), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for first message to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Failed to receive first message")
	}

	// Report error if any
	if len(errors) > 0 {
		t.Fatalf("%v", <-errors)
	}

	// Close the client
	sc.Close()

	// Restart client
	sc2 := NewDefaultConnection(t)
	defer sc2.Close()

	sub2, err := sc2.Subscribe("foo", cb, stan.DurableName("dur"), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// Wait for redelivered message
	if err := Wait(rch); err != nil {
		t.Fatal("Messages were not redelivered to durable")
	}

	// Report error if any
	if len(errors) > 0 {
		t.Fatalf("%v", <-errors)
	}
}

func testStalledRedelivery(t *testing.T, typeSub string) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	recv := int32(0)
	rdlv := int32(0)
	toSend := int32(1)
	numSubs := 1

	ch := make(chan bool)
	rch := make(chan bool, 2)
	errors := make(chan error)

	cb := func(m *stan.Msg) {
		if !m.Redelivered {
			r := atomic.AddInt32(&recv, 1)
			if r > toSend {
				errors <- fmt.Errorf("Should have received only 1 message, got %v", r)
				return
			} else if r == toSend {
				ch <- true
			}
		} else {
			m.Ack()
			// We have received our redelivered message(s), we're done
			if atomic.AddInt32(&rdlv, 1) == toSend {
				rch <- true
			}
		}
	}
	if typeSub == "queue" {
		// Create 2 queue subs with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(ackWaitInMs(15))); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(ackWaitInMs(15))); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		numSubs = 2
		toSend = 2
	} else if typeSub == "durable" {
		// Create a durable with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur"),
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(ackWaitInMs(15))); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	} else {
		// Create a sub with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.Subscribe("foo", cb, stan.SetManualAckMode(),
			stan.MaxInflight(1), stan.AckWait(ackWaitInMs(15))); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}
	// Wait for subscriber to be registered before starting publish
	waitForNumSubs(t, s, clientName, numSubs)
	// Send
	for i := int32(0); i < toSend; i++ {
		msg := fmt.Sprintf("msg_%d", (i + 1))
		if err := sc.Publish("foo", []byte(msg)); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Make sure the message is received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Wait for completion or error
	select {
	case <-rch:
		break
	case e := <-errors:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our redelivered message")
	}
	// Same test but with server restart.
	atomic.StoreInt32(&recv, 0)
	atomic.StoreInt32(&rdlv, 0)
	// Send
	for i := int32(0); i < toSend; i++ {
		msg := fmt.Sprintf("msg_%d", (i + 1))
		if err := sc.Publish("foo", []byte(msg)); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Make sure the message is received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Wait for completion or error
	select {
	case <-rch:
		break
	case e := <-errors:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our redelivered message")
	}
}

func TestPersistentStoreStalledRedelivery(t *testing.T) {
	testStalledRedelivery(t, "sub")
}

func TestPersistentStoreStalledQueueRedelivery(t *testing.T) {
	testStalledRedelivery(t, "queue")
}

func TestPersistentStoreStalledDurableRedelivery(t *testing.T) {
	testStalledRedelivery(t, "durable")
}

func TestPersistentStoreRedeliveredPerSub(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Send one message on "foo"
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Message should not be marked as redelivered
	cs := channelsGet(t, s.channels, "foo")
	if m := msgStoreFirstMsg(t, cs.store.Msgs); m == nil || m.Redelivered {
		t.Fatal("Message should have been recovered as not redelivered")
	}

	first := make(chan bool)
	ch := make(chan bool)
	rch := make(chan bool)
	errors := make(chan error, 10)
	delivered := int32(0)
	redelivered := int32(0)

	var sub1 stan.Subscription

	cb := func(m *stan.Msg) {
		if m.Redelivered && m.Sub == sub1 {
			m.Ack()
			if atomic.AddInt32(&redelivered, 1) == 1 {
				rch <- true
			}
		} else if !m.Redelivered {
			d := atomic.AddInt32(&delivered, 1)
			switch d {
			case 1:
				first <- true
			case 2:
				ch <- true
			}
		} else {
			errors <- fmt.Errorf("Unexpected redelivered message to sub1")
		}
	}

	// Start a subscriber that consumes the message but does not ack it.
	sub1, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(), stan.AckWait(ackWaitInMs(15)))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for 1st msg to be received
	if err := Wait(first); err != nil {
		t.Fatal("Did not get our first message")
	}
	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Client should have been recovered
	checkClients(t, s, 1)

	// There should be 1 subscription
	checkSubs(t, s, clientName, 1)

	// Now start a second subscriber that will receive the old message
	if _, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for that message to be received.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
	// Wait for the redelivered message.
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our redelivered message")
	}
	// Report error if any
	if len(errors) > 0 {
		t.Fatalf("%v", <-errors)
	}
	// There should be only 1 redelivered message
	if c := atomic.LoadInt32(&redelivered); c != 1 {
		t.Fatalf("Expected 1 redelivered message, got %v", c)
	}
}

func TestPersistentStoreRedeliveryCbPerSub(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Send one message on "foo"
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	rch := make(chan bool)
	errors := make(chan error, 10)
	sub1Redel := int32(0)
	sub2Redel := int32(0)

	var sub1 stan.Subscription
	var sub2 stan.Subscription

	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
		}
		if m.Redelivered {
			if m.Sub == sub1 {
				if atomic.AddInt32(&sub1Redel, 1) > 1 {
					errors <- fmt.Errorf("More than one redeliverd msg for sub1")
					return
				}
			} else if m.Sub == sub2 {
				if atomic.AddInt32(&sub2Redel, 1) > 1 {
					errors <- fmt.Errorf("More than one redeliverd msg for sub1")
					return
				}
			} else {
				errors <- fmt.Errorf("Redelivered msg for unknown subscription")
			}
		}
		s1 := atomic.LoadInt32(&sub1Redel)
		s2 := atomic.LoadInt32(&sub2Redel)
		total := s1 + s2
		if total == 2 {
			rch <- true
		}
	}

	// Start 2 subscribers that consume the message but do not ack it.
	var err error
	if sub1, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(), stan.AckWait(ackWaitInMs(15))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if sub2, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(), stan.AckWait(ackWaitInMs(15))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Make sure the messages are added to the subscriptions before shutting down.
	waitForCount(t, 2, func() (string, int) {
		subs := s.clients.getSubs(clientName)
		good := 0
		for _, sub := range subs {
			sub.Lock()
			if len(sub.acksPending) == 1 {
				good++
			}
		}
		return "pending message per sub", good
	})

	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Client should have been recovered
	checkClients(t, s, 1)

	// There should be 2 subscriptions
	checkSubs(t, s, clientName, 2)

	// Wait for all redelivered messages.
	select {
	case e := <-errors:
		t.Fatalf("%v", e)
		break
	case <-rch:
		break
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our redelivered messages")
	}
}

func TestPersistentStorePersistMsgRedeliveredToDifferentQSub(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	var err error
	var sub2 stan.Subscription

	errs := make(chan error, 10)
	sub2Recv := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			if m.Sub != sub2 {
				errs <- fmt.Errorf("Expected redelivered msg to be sent to sub2")
				return
			}
			sub2Recv <- true
		}
	}

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Create two queue subscribers with manual ackMode that will
	// not ack the message.
	if _, err := sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(ackWaitInMs(15)),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	sub2, err = sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(ackWaitInMs(150)),
		stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure these are registered.
	waitForNumSubs(t, s, clientName, 2)
	// Send a message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for sub2 to receive the message.
	select {
	case <-sub2Recv:
		waitForAcks(t, s, clientName, 1, 0)
		break
	case e := <-errs:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get out message")
	}

	// Stop server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Get subs
	subs := s.clients.getSubs(clientName)
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subscriptions to be recovered, got %v", len(subs))
	}
	// Message should be in sub2's ackPending
	for _, s := range subs {
		s.RLock()
		na := len(s.acksPending)
		sID := s.ID
		s.RUnlock()
		if sID == 1 && na != 0 {
			t.Fatal("Unexpected un-acknowledged message for sub1")
		} else if sID == 2 && na != 1 {
			t.Fatal("Unacknowledged message should have been recovered for sub2")
		}
	}
}

func TestPersistentStoreAckMsgRedeliveredToDifferentQueueSub(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	var (
		err    error
		sub2   stan.Subscription
		sub2Mu sync.Mutex
	)

	errs := make(chan error, 10)
	sub2Recv := make(chan bool)
	redelivered := int32(0)
	trackDelivered := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			if atomic.AddInt32(&redelivered, 1) != 1 {
				errs <- fmt.Errorf("Message redelivered after restart")
				return
			}
			sub2Mu.Lock()
			isSub2 := m.Sub == sub2
			sub2Mu.Unlock()
			if !isSub2 {
				// We want the message to be redelivered to sub2, so wait
				// for that to happen
				return
			}
			sub2Recv <- true
		} else {
			if atomic.LoadInt32(&trackDelivered) == 1 {
				errs <- fmt.Errorf("Unexpected non redelivered message: %v", m)
				return
			}
		}
	}

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Create a queue subscriber with manual ackMode that will
	// not ack the message.
	if _, err := sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(ackWaitInMs(100)),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)
	// Send a message that we know is going to go to the first (and only)
	// queue sub that will not ack this message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	sub2Mu.Lock()
	// Create this subscriber that will receive and ack the message
	sub2, err = sc.QueueSubscribe("foo", "g1", cb)
	sub2Mu.Unlock()
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure these are registered.
	waitForNumSubs(t, s, clientName, 2)
	// Wait for sub2 to receive the message.
	select {
	case <-sub2Recv:
		break
	case e := <-errs:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get out message")
	}
	// Wait for msg sent to sub2 to be ack'ed
	waitForAcks(t, s, clientName, 2, 0)

	// Stop server
	s.Shutdown()
	// Track unexpected delivery of non redelivered message
	atomic.StoreInt32(&trackDelivered, 1)
	// Restart server
	s = runServerWithOpts(t, opts, nil)

	// Get subs
	subs := s.clients.getSubs(clientName)
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subscriptions to be recovered, got %v", len(subs))
	}
	// No ackPending should be recovered
	for _, s := range subs {
		s.RLock()
		na := len(s.acksPending)
		sID := s.ID
		s.RUnlock()
		if na != 0 {
			t.Fatalf("Unexpected un-acknowledged message for sub: %v", sID)
		}
	}
	// Wait for possible redelivery
	select {
	case e := <-errs:
		t.Fatalf("%v", e)
	case <-time.After(30 * time.Millisecond):
		break
	}
}

func TestDurableQueueSubRedeliveryOnRejoin(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg1")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	dlv := 0
	dch := make(chan bool)
	cb1 := func(m *stan.Msg) {
		if !m.Redelivered {
			dlv++
			if dlv == total {
				dch <- true
			}
		}
	}
	rdlv := 0
	rdch := make(chan bool)
	sigCh := make(chan bool)
	signaled := false
	cb2 := func(m *stan.Msg) {
		if m.Redelivered && int(m.Sequence) <= total {
			rdlv++
			if rdlv == total {
				rdch <- true
			}
		} else if !m.Redelivered && int(m.Sequence) > total {
			if !signaled {
				signaled = true
				sigCh <- true
			}
		}
	}
	// Create a durable queue subscriber with manual ack
	if _, err := sc.QueueSubscribe("foo", "group", cb1,
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.MaxInflight(total),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Wait for it to receive the message
	if err := Wait(dch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Create new one
	sc2, err := stan.Connect(clusterName, "sc2cid")
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer sc2.Close()
	// Rejoin the group
	if _, err := sc2.QueueSubscribe("foo", "group", cb2,
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(15)),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 2)
	// Send one more message, which should go to sub2
	if err := sc.Publish("foo", []byte("last")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for it to be received
	if err := Wait(sigCh); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Message should be redelivered
	if err := Wait(rdch); err != nil {
		t.Fatal("Did not get our redelivered message")
	}
}

func TestPersistentStoreDurableQueueSubRedeliveryOnRejoin(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()
	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if (count == 1 && !m.Redelivered) || (count == 2 && m.Redelivered) {
			ch <- true
		}
	}
	// Create a durable queue subscriber with manual ack
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Wait for it to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Stop server
	s.Shutdown()
	// Restart it
	s = runServerWithOpts(t, opts, nil)
	// Connect
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Rejoin the group
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.AckWait(ackWaitInMs(15)),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Message should be redelivered
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestDroppedMessagesOnRedelivery(t *testing.T) {
	opts := GetDefaultOptions()
	opts.MaxMsgs = 3
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Produce 3 messages
	for i := 0; i < 3; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Start a subscriber with manual ack, don't ack the first
	// delivered messages.
	ch := make(chan bool)
	ready := make(chan bool)
	expectedSeq := uint64(2)
	good := 0
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
			if m.Sequence == expectedSeq {
				good++
				if good == 3 {
					ch <- true
				}
			}
			expectedSeq++
		} else if m.Sequence == 3 {
			ready <- true
		}
	}
	if _, err := sc.Subscribe("foo", cb,
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(15)),
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive 3rd message, then send one more
	if err := Wait(ready); err != nil {
		t.Fatal("Did not get our message")
	}
	// Send one more, this should cause 1st message to be dropped
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for redelivery of message 2, 3 and 4.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
}

func TestIgnoreFailedHBInAckRedeliveryForQGroup(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	opts.ClientHBInterval = 100 * time.Millisecond
	opts.ClientHBTimeout = time.Millisecond
	opts.ClientHBFailCount = 100000
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	count := 0
	ch := make(chan bool)
	var mu sync.Mutex
	cb := func(m *stan.Msg) {
		mu.Lock()
		defer mu.Unlock()
		count++
		if count == 4 {
			ch <- true
		}
	}
	// Create first queue member. Use NatsConn so we can close the NATS
	// connection to produced failed HB
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	sc1, err := stan.Connect(clusterName, "client1", stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	if _, err := sc1.QueueSubscribe("foo", "group", cb, stan.AckWait(ackWaitInMs(15))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Create 2nd member.
	sc2 := NewDefaultConnection(t)
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "group", cb, stan.AckWait(ackWaitInMs(15))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Send 2 messages, expecting to go to sub1 then sub2
	for i := 0; i < 2; i++ {
		sc1.Publish("foo", []byte("hello"))
	}
	// Wait for those messages to be ack'ed
	waitForAcks(t, s, "client1", 1, 0)
	// Close connection of sub1
	nc.Close()
	// Send 2 more messages
	for i := 0; i < 2; i++ {
		sc2.Publish("foo", []byte("hello"))
	}
	// Wait for messages to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
}

type trackDeliveredMsgs struct {
	dummyLogger
	newSeq   int
	redelSeq int
	errCh    chan error
}

func (l *trackDeliveredMsgs) Tracef(format string, args ...interface{}) {
	l.dummyLogger.Lock()
	l.msg = fmt.Sprintf(format, args...)
	if strings.Contains(l.msg, "Redelivering") {
		l.redelSeq++
	} else if strings.Contains(l.msg, "Delivering") {
		if l.newSeq != l.redelSeq+1 {
			l.errCh <- fmt.Errorf("Got %q while there were only %d redelivered messages", l.msg, l.redelSeq)
		} else {
			l.errCh <- nil
		}
	}
	l.dummyLogger.Unlock()
}

func TestQueueRedeliveryOnStartup(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	ch := make(chan bool, 1)
	errCh := make(chan error, 4)
	skipCh := make(chan bool, 2)
	restarted := int32(0)
	totalMsgs := int32(10)
	delivered := int32(0)
	redelivered := int32(0)
	type qinfo struct {
		r    int
		msgs map[uint64]struct{}
	}
	newCb := func(id int) func(m *stan.Msg) {
		q := qinfo{msgs: make(map[uint64]struct{}, 2)}
		return func(m *stan.Msg) {
			if !m.Redelivered {
				if atomic.LoadInt32(&restarted) == 0 {
					q.msgs[m.Sequence] = struct{}{}
					if atomic.AddInt32(&delivered, 1) == totalMsgs {
						ch <- true
					}
				} else {
					m.Ack()
					if q.r != len(q.msgs) {
						errCh <- fmt.Errorf("Unexpected new message %v into sub %d before getting all undelivered first", m.Sequence, id)
					}
				}
			} else if atomic.LoadInt32(&restarted) == 1 {
				// This is a redelivered message after server restart
				if _, present := q.msgs[m.Sequence]; !present {
					errCh <- fmt.Errorf("Unexpected message %v into sub %d", m.Sequence, id)
				} else {
					m.Ack()
					q.r++
					if atomic.AddInt32(&redelivered, 1) == totalMsgs {
						ch <- true
					}
				}
			} else {
				select {
				case skipCh <- true:
				default:
				}
				m.Sub.Unsubscribe()
			}
		}
	}
	if _, err := sc.QueueSubscribe("foo", "queue",
		newCb(1),
		stan.MaxInflight(int(totalMsgs/2)),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(500))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if _, err := sc.QueueSubscribe("foo", "queue",
		newCb(2),
		stan.MaxInflight(int(totalMsgs/2)),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(500))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Send more messages that can be accepted, both member should stall
	for i := 0; i < int(totalMsgs+1); i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Wait for all messages to be received
	select {
	case <-ch:
	case <-skipCh:
		// If we have a redelivery before the server restart
		// (can happen on Travis because of timing), no point
		// in continuing this test.
		return
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive all our messages")
	default:
	}
	// Now stop server and wait more than AckWait before resarting.
	s.Shutdown()
	// We need to  make sure that the first redelivery on startup will
	// actually send messages to original qsub. This happens only if
	// the AckWait has elapsed. So make sure that we wait long enough.
	time.Sleep(800 * time.Millisecond)
	l := &trackDeliveredMsgs{newSeq: int(totalMsgs + 1), errCh: make(chan error, 1)}
	opts.Trace = true
	opts.CustomLogger = l
	atomic.StoreInt32(&restarted, 1)
	s = runServerWithOpts(t, opts, nil)
	// Check that messages are delivered to members that
	// originally got them. Wait for all messages to be redelivered
	select {
	case e := <-errCh:
		t.Fatalf(e.Error())
	case <-ch:
	// All messages were redelivered, we are ok
	case <-skipCh:
		// If we have a redelivery before the server restart
		// (can happen on Travis because of timing), no point
		// in continuing this test.
		return
	case <-time.After(time.Second):
		t.Fatal("Did not get all redelivered messages")
	}
	// Check that we find in log that all messages were redelivered before
	// the new message was delivered.
	select {
	case e := <-l.errCh:
		if e != nil {
			t.Fatalf(e.Error())
		}
	case <-time.After(250 * time.Millisecond):
	}
}

type delayReconnectDialer struct {
	fail int
}

func (d *delayReconnectDialer) Dial(network, address string) (net.Conn, error) {
	d.fail++
	if d.fail >= 10 {
		return net.Dial(network, address)
	}
	return nil, errOnPurpose
}

func TestNoDuplicateRedeliveryDueToDisconnect(t *testing.T) {
	ns := natsdTest.RunServer(nil)
	defer shutdownRestartedNATSServerOnTestExit(&ns)

	opts := GetDefaultOptions()
	opts.NATSServerURL = nats.DefaultURL
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	// If the server's NATS connections were to use a reconnect buffer,
	// in case of redeliveries, the server will Publish() them while
	// being disconnected. On reconnect, the redeliveries would be flushed,
	// which would result in client getting the same message redelivered
	// multiple times, even after being ack'ed.
	// Use a custom dialer for the server's send connection to ensure that
	// the streaming client lib has a chance to reconnect before the server's
	// connection reconnects, otherwise, even with a reconnect buffer, the
	// pending redeliveries may be flushed before the client's subs are
	// recreated on the NATS server and the issue would not be observed.
	s.mu.Lock()
	s.ncs.Opts.CustomDialer = &delayReconnectDialer{}
	s.mu.Unlock()

	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.MaxReconnects(-1), nats.ReconnectWait(15*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	ch := make(chan bool, 1)
	rdc := int32(0)
	ok := int32(0)
	if _, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if !m.Redelivered {
			ch <- true
			return
		}
		// Do not ack until we know that the NATS server has not
		// been stopped and restarted.
		if atomic.LoadInt32(&ok) != 1 {
			return
		}
		m.Ack()
		if atomic.AddInt32(&rdc, 1) == 1 {
			ch <- true
		}
	}, stan.SetManualAckMode(), stan.AckWait(ackWaitInMs(100))); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get out message")
	}
	// Shutdown NATS Server and wait for several redelivery attempts
	ns.Shutdown()
	time.Sleep(500 * time.Millisecond)
	ns = natsdTest.RunServer(nil)
	atomic.StoreInt32(&ok, 1)
	// Now wait for the redelivered message to be ack'ed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get redeliverd")
	}
	// Wait a bit and check that the rdc count is 1.
	time.Sleep(400 * time.Millisecond)
	if c := atomic.LoadInt32(&rdc); c != 1 {
		t.Fatalf("Message redelivered after being ack'ed: %v", c)
	}
}

func TestPersistentStoreNotStalledIfBelowMaxInflight(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	ch := make(chan *stan.Msg, 1)
	if _, err := sc.Subscribe("foo", func(m *stan.Msg) {
		ch <- m
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("first")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	select {
	case m := <-ch:
		assertMsg(t, m.MsgProto, []byte("first"), 1)
	case <-time.After(time.Second):
		t.Fatalf("Did not get our message")
	}

	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Send one more message
	if err := sc.Publish("foo", []byte("second")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Second message should get delivered
	select {
	case m := <-ch:
		assertMsg(t, m.MsgProto, []byte("second"), 2)
	case <-time.After(time.Second):
		t.Fatalf("Did not get our second message")
	}
}

func TestQueueAckRemovesFromOwningMember(t *testing.T) {
	resetPreviousHTTPConnections()
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	nopts := defaultMonitorOptions
	s := runServerWithOpts(t, opts, &nopts)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	redelivered := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			atomic.AddInt32(&redelivered, 1)
		} else {
			time.Sleep(750 * time.Millisecond)
			m.Ack()
		}
	}
	if _, err := sc.QueueSubscribe("foo", "bar", cb,
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(500))); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if _, err := sc.QueueSubscribe("foo", "bar",
		func(_ *stan.Msg) {
			atomic.AddInt32(&redelivered, 1)
		},
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(500))); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	waitForAcks(t, s, clientName, 1, 0)
	waitForAcks(t, s, clientName, 2, 0)

	if n := atomic.LoadInt32(&redelivered); n != 1 {
		t.Fatalf("Expected message to be redelivered only once, was %v times", n)
	}

	resp, body := getBody(t, ChannelsPath+"?channel=foo&subs=1", expectedJSON)
	defer resp.Body.Close()
	cz := Channelz{}
	if err := json.Unmarshal(body, &cz); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v", err)
	}
	resp.Body.Close()
	subs := cz.Subscriptions
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subscriptions, got %v", len(subs))
	}
	for _, sub := range subs {
		if pc := sub.PendingCount; pc != 0 {
			t.Fatalf("Pending count of subID: %v should be 0, got %v", sub.AckInbox, pc)
		}
	}

	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	waitForAcks(t, s, clientName, 1, 0)
	waitForAcks(t, s, clientName, 2, 0)
}

func TestQueueRedeliveryAfterMembersCrash(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	// Need to create NATS connections to simulate crash
	// by closing NATS connection and preventing connection close
	// to be sent.
	sc1, nc1 := createConnectionWithNatsOpts(t, "member1")
	defer nc1.Close()
	defer sc1.Close()

	total := 20

	if _, err := sc1.QueueSubscribe("foo", "bar", func(_ *stan.Msg) {},
		stan.SetManualAckMode(),
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1),
		stan.AckWait(60*time.Second)); err != nil {
		t.Fatalf("Error on queue subscribe: %v", err)
	}

	sc2, nc2 := createConnectionWithNatsOpts(t, "member2")
	defer nc2.Close()
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "bar", func(_ *stan.Msg) {},
		stan.SetManualAckMode(),
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1),
		stan.AckWait(60*time.Second)); err != nil {
		t.Fatalf("Error on queue subscribe: %v", err)
	}

	// Wait for 2 queue members to be registered
	waitForNumSubs(t, s, "member1", 1)
	waitForNumSubs(t, s, "member2", 1)

	for i := 0; i < 2; i++ {
		if err := sc1.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Simulate crash by closing the NATS connections.
	nc1.Close()
	nc2.Close()

	// Produce messages
	sc := NewDefaultConnection(t)
	defer sc.Close()
	for i := 0; i < total-2; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}
	sc.Close()

	// Recreate the 2 queue members
	sc1, err := stan.Connect(clusterName, "member1")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc1.Close()

	sc2, err = stan.Connect(clusterName, "member2")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc2.Close()

	count := int32(0)
	ch := make(chan bool, 1)
	cb := func(m *stan.Msg) {
		if n := int(atomic.AddInt32(&count, 1)); n == total {
			ch <- true
		}
	}

	if _, err := sc1.QueueSubscribe("foo", "bar", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1),
		stan.AckWait(60*time.Second)); err != nil {
		t.Fatalf("Error on queue subscribe: %v", err)
	}
	if _, err := sc2.QueueSubscribe("foo", "bar", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1),
		stan.AckWait(60*time.Second)); err != nil {
		t.Fatalf("Error on queue subscribe: %v", err)
	}

	// Wait for messages. It should not take the 60 seconds of AckWait.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get redelivered messages on time")
	}
}

func TestQueueRedeliveryWithMsgsReassignedToMemberWithAcksPending(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// First check that the queue sub leaving is the one that
	// had a msg with sequence smaller than the pending msg
	// of the queue sub to which first message is reassigned.
	for i := 0; i < 2; i++ {
		sc.Publish("foo", []byte("msg"))
	}

	subCh := make(chan stan.Subscription, 1)
	if _, err := sc.QueueSubscribe("foo", "bar",
		func(m *stan.Msg) {
			if m.Sequence == 1 && !m.Redelivered {
				subCh <- m.Sub
			}
		},
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.MaxInflight(1),
	); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Wait for qsub1 to receive message
	qsub1 := <-subCh

	ch := make(chan bool, 1)
	if _, err := sc.QueueSubscribe("foo", "bar",
		func(m *stan.Msg) {
			if m.Sequence == 2 && !m.Redelivered {
				qsub1.Close()
			} else if m.Sequence == 1 && m.Redelivered {
				ch <- true
			}
		},
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode(),
	); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("Message 2 was not immediately redelivered")
	}

	// Now test with situation where the leaving queue sub
	// has a message with seq higher than the pending msg
	// of the queue sub the message is reassigned to.

	for i := 0; i < 2; i++ {
		sc.Publish("baz", []byte("msg"))
	}

	if _, err := sc.QueueSubscribe("baz", "bar",
		func(m *stan.Msg) {
			if m.Redelivered && m.Sequence == 2 {
				ch <- true
			} else if m.Sequence == 1 && !m.Redelivered {
				subCh <- m.Sub
			}
		},
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.MaxInflight(1),
	); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Wait for first qsub to get message seq1
	<-subCh

	// Now that we got the first message to the first queue sub,
	// start a new queue sub that will get message 2 and close.
	// We want message 2 to be redelivered right away to qsub 1
	if _, err := sc.QueueSubscribe("baz", "bar",
		func(m *stan.Msg) {
			if m.Sequence == 2 {
				m.Sub.Close()
			}
		},
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode(),
	); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("Message 2 was not immediately redelivered")
	}
}
