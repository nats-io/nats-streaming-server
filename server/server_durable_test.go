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
	"sync"
	"testing"
	"time"

	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
)

func TestDurableRestartWithMaxInflight(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	maxAckPending := 100
	stopDurAt := 10
	total := stopDurAt + maxAckPending + 100

	// Send all messages
	sc := NewDefaultConnection(t)
	defer sc.Close()
	msg := []byte("hello")
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	// Have a cb that stop the connection after a certain amount of
	// messages received
	count := 0
	ch := make(chan bool)
	cb := func(_ *stan.Msg) {
		count++
		if count == stopDurAt || count == total {
			sc.Close()
			ch <- true
		}
	}
	// Start the durable
	_, err := sc.Subscribe("foo", cb, stan.DurableName("dur"),
		stan.MaxInflight(maxAckPending), stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for the connection to be closed after receiving
	// a certain amount of messages.
	if err := Wait(ch); err != nil {
		t.Fatal("Waited too long for the messages to be received")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Restart the durable
	_, err = sc.Subscribe("foo", cb, stan.DurableName("dur"),
		stan.MaxInflight(maxAckPending), stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for all messages to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Waited too long for the messages to be received")
	}
}

func checkDurable(t *testing.T, s *StanServer, channel, durName, durKey string) {
	c := s.clients.lookup(clientName)
	if c == nil {
		stackFatalf(t, "Expected client %v to be registered", clientName)
	}
	c.RLock()
	subs := c.subs
	c.RUnlock()
	if len(subs) != 1 {
		stackFatalf(t, "Expected 1 sub, got %v", len(subs))
	}
	sub := subs[0]
	if sub.DurableName != durName {
		stackFatalf(t, "Expected durable name %v, got %v", durName, sub.DurableName)
	}
	// Check that durable is also in subStore
	cs := channelsGet(t, s.channels, channel)
	ss := cs.ss
	ss.RLock()
	durInSS := ss.durables[durKey]
	ss.RUnlock()
	if durInSS == nil || durInSS.DurableName != durName {
		stackFatalf(t, "Expected durable to be in subStore")
	}
}

func TestDurableCanReconnect(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	cb := func(_ *stan.Msg) {}

	durName := "mydur"
	sr := &pb.SubscriptionRequest{
		ClientID:    clientName,
		Subject:     "foo",
		DurableName: durName,
	}
	durKey := durableKey(sr)

	// Create durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is created
	checkDurable(t, s, "foo", durName, durKey)

	// We should not be able to create a second durable on same subject
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err == nil {
		t.Fatal("Expected to fail to create a second durable with same name")
	}

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)
}

func TestPersistentStoreRecoveredDurableCanReconnect(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	cb := func(_ *stan.Msg) {}

	durName := "mydur"
	sr := &pb.SubscriptionRequest{
		ClientID:    clientName,
		Subject:     "foo",
		DurableName: durName,
	}
	durKey := durableKey(sr)

	// Create durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is created
	checkDurable(t, s, "foo", durName, durKey)

	// We should not be able to create a second durable on same subject
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err == nil {
		t.Fatal("Expected to fail to create a second durable with same name")
	}

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Close the connection
	sc.Close()

	// Restart the server
	s.Shutdown()

	// Recover
	s = runServerWithOpts(t, opts, nil)

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)
}

func TestDurableAckedMsgNotRedelivered(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Make a channel big enough so that we don't block
	msgs := make(chan *stan.Msg, 10)

	cb := func(m *stan.Msg) {
		msgs <- m
	}

	durName := "mydur"
	sr := &pb.SubscriptionRequest{
		ClientID:    clientName,
		Subject:     "foo",
		DurableName: durName,
	}
	durKey := durableKey(sr)

	// Create durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is created
	checkDurable(t, s, "foo", durName, durKey)

	// We verified that there is 1 sub, and this is our durable.
	subs := s.clients.getSubs(clientName)
	durable := subs[0]
	durable.RLock()
	// Get the AckInbox.
	ackInbox := durable.AckInbox
	// Get the ack subscriber
	ackSub := durable.ackSub
	durable.RUnlock()

	// Send a message
	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify message is acked.
	checkDurableNoPendingAck(t, s, true, ackInbox, ackSub, 1, "foo")

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Send a second message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify that we have different AckInbox and ackSub and message is acked.
	checkDurableNoPendingAck(t, s, false, ackInbox, ackSub, 2, "foo")

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Verify that we have different AckInbox and ackSub and message is acked.
	checkDurableNoPendingAck(t, s, false, ackInbox, ackSub, 2, "foo")

	numMsgs := len(msgs)
	if numMsgs > 2 {
		t.Fatalf("Expected only 2 messages to be delivered, got %v", numMsgs)
	}
	for i := 0; i < numMsgs; i++ {
		m := <-msgs
		if m.Redelivered {
			t.Fatal("Unexpected redelivered message")
		}
		if m.Sequence != uint64(i+1) {
			t.Fatalf("Expected message %v's sequence to be %v, got %v", (i + 1), (i + 1), m.Sequence)
		}
	}
}

func TestDurableRemovedOnUnsubscribe(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	cb := func(_ *stan.Msg) {}

	durName := "mydur"
	sr := &pb.SubscriptionRequest{
		ClientID:    clientName,
		Subject:     "foo",
		DurableName: durName,
	}
	durKey := durableKey(sr)

	// Create durable
	sub, err := sc.Subscribe("foo", cb, stan.DurableName(durName))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is created
	checkDurable(t, s, "foo", durName, durKey)

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}

	// Check that durable is removed
	cs := channelsGet(t, s.channels, "foo")
	ss := cs.ss
	ss.RLock()
	durInSS := ss.durables[durKey]
	ss.RUnlock()
	if durInSS != nil {
		t.Fatal("Durable should have been removed")
	}
}

func checkDurableNoPendingAck(t *testing.T, s *StanServer, isSame bool,
	ackInbox string, ackSub *nats.Subscription, expectedSeq uint64, channel string) {

	// When called, we know that there is 1 sub, and the sub is a durable.
	subs := s.clients.getSubs(clientName)
	durable := subs[0]
	durable.RLock()
	durAckInbox := durable.AckInbox
	durAckSub := durable.ackSub
	durable.RUnlock()

	if isSame {
		if durAckInbox != ackInbox {
			stackFatalf(t, "Expected ackInbox %v, got %v", ackInbox, durAckInbox)
		}
		if durAckSub != ackSub {
			stackFatalf(t, "Expected subscriber on ack to be %p, got %p", ackSub, durAckSub)
		}

	} else {
		if durAckInbox == ackInbox {
			stackFatalf(t, "Expected different ackInbox'es")
		}
		if durAckSub == ackSub {
			stackFatalf(t, "Expected different ackSub")
		}
	}

	limit := time.Now().Add(5 * time.Second)
	for time.Now().Before(limit) {
		durable.RLock()
		lastSent := durable.LastSent
		acks := len(durable.acksPending)
		durable.RUnlock()

		if lastSent != expectedSeq || acks > 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// We are ok
		return
	}
	stackFatalf(t, "Message was not acknowledged")
}

func TestPersistentStoreDurableCanReceiveAfterRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		ch <- true
	}

	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Create our durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure this is registered
	waitForNumSubs(t, s, clientName, 1)
	// Close the connection
	sc.Close()

	// Restart durable
	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()
	if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure it is registered
	waitForNumSubs(t, s, clientName, 1)

	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Send 1 message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for message to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestPersistentStoreDontSendToOfflineDurablesAfterServerRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	// Run a standalone NATS Server
	gs := natsdTest.RunServer(nil)
	defer gs.Shutdown()

	opts := getTestDefaultOptsForPersistentStore()
	opts.NATSServerURL = nats.DefaultURL
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	clientName2 := clientName + "2"
	// Have another client connection that will stay opened while
	// server is restarted
	sc2, nc2 := createConnectionWithNatsOpts(t, clientName2, nats.ReconnectWait(50*time.Millisecond))
	defer nc2.Close()
	defer sc2.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	durName := "mydur"
	gotFirstCh := make(chan bool, 4)
	gotFirstCb := func(m *stan.Msg) {
		if !m.Redelivered {
			gotFirstCh <- true
		}
	}
	// Create a durable with manual ack mode (don't ack the message)
	if _, err := sc.Subscribe("foo", gotFirstCb,
		stan.DurableName(durName),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(100))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a durable queue sub too.
	if _, err := sc.QueueSubscribe("foo", "queue", gotFirstCb,
		stan.DurableName(durName),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(100))); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a durable and durable queue sub (from sc2) that we will
	// explicitly close
	dur, err := sc2.Subscribe("foo", gotFirstCb,
		stan.DurableName(durName+"2"),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(100)))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	durqsub, err := sc2.QueueSubscribe("foo", "queue", gotFirstCb,
		stan.DurableName(durName+"2"),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(ackWaitInMs(100)))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	inboxes := []string{}

	// Make sure durables are created
	getInboxes := func(cname string) {
		waitForNumSubs(t, s, cname, 2)
		subs := checkSubs(t, s, cname, 2)
		if len(subs) != 2 {
			stackFatalf(t, "Should be only 2 durables, got %v", len(subs))
		}
		for _, dur := range subs {
			dur.RLock()
			inboxes = append(inboxes, dur.Inbox)
			dur.RUnlock()
		}
	}
	getInboxes(clientName)
	getInboxes(clientName2)

	// Ensure original messages are received
	for i := 0; i < 4; i++ {
		if err := Wait(gotFirstCh); err != nil {
			t.Fatal("Did not get our ")
		}
	}
	// Close explicitly some of the durables/qsubs
	dur.Close()
	durqsub.Close()
	// Close the client
	sc.Close()

	newSender := NewDefaultConnection(t)
	defer newSender.Close()
	if err := newSender.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	newSender.Close()

	// Create a raw NATS connection
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	failCh := make(chan *pb.MsgProto, 10)
	// Setup a consumer on the durable inboxes
	for _, inbox := range inboxes {
		sub, err := nc.Subscribe(inbox, func(m *nats.Msg) {
			sm := &pb.MsgProto{}
			sm.Unmarshal(m.Data)
			failCh <- sm
		})
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		defer sub.Unsubscribe()
	}
	nc.Flush()

	// Stop the Streaming server
	s.Shutdown()
	// Wait a bit more than the redelivery wait time (30ms)
	time.Sleep(110 * time.Millisecond)
	// Restart the Streaming server
	s = runServerWithOpts(t, opts, nil)

	newSender = NewDefaultConnection(t)
	defer newSender.Close()
	if err := newSender.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	newSender.Close()

	select {
	case m := <-failCh:
		t.Fatalf("Server sent this message: %v", m)
	case <-time.After(250 * time.Millisecond):
		// Did not receive message, we are ok.
	}
}

func TestDurableClosedNotUnsubscribed(t *testing.T) {
	closeSubscriber(t, "sub")
	closeSubscriber(t, "queue")
}

func closeSubscriber(t *testing.T, subType string) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg")); err != nil {
		stackFatalf(t, "Unexpected error on publish: %v", err)
	}
	// Create a durable
	durName := "dur"
	groupName := "group"
	durKey := fmt.Sprintf("%s-%s-%s", clientName, "foo", durName)
	if subType == "queue" {
		durKey = fmt.Sprintf("%s:%s", durName, groupName)
	}
	ch := make(chan bool)
	errCh := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if m.Sequence != uint64(count) {
			errCh <- true
			return
		}
		ch <- true
	}
	var sub stan.Subscription
	var err error
	if subType == "sub" {
		sub, err = sc.Subscribe("foo", cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	} else {
		sub, err = sc.QueueSubscribe("foo", groupName, cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	}
	if err != nil {
		stackFatalf(t, "Unexpected error on subscribe: %v", err)
	}
	wait := func() {
		select {
		case <-errCh:
			stackFatalf(t, "Unexpected message received")
		case <-ch:
		case <-time.After(5 * time.Second):
			stackFatalf(t, "Did not get our message")
		}
	}
	wait()

	s.mu.RLock()
	cs := channelsGet(t, s.channels, "foo")
	ss := cs.ss
	var dur *subState
	if subType == "sub" {
		dur = ss.durables[durKey]
	} else {
		dur = ss.qsubs[durKey].subs[0]
	}
	s.mu.RUnlock()
	if dur == nil {
		stackFatalf(t, "Durable should have been found")
	}
	// Make sure ACKs are processed before closing to avoid redelivery
	waitForAcks(t, s, clientName, dur.ID, 0)
	// Close durable, don't unsubscribe it
	if err := sub.Close(); err != nil {
		stackFatalf(t, "Error on subscriber close: %v", err)
	}
	// Durable should still be present
	ss.RLock()
	var there bool
	if subType == "sub" {
		_, there = ss.durables[durKey]
	} else {
		_, there = ss.qsubs[durKey]
	}
	ss.RUnlock()
	if !there {
		stackFatalf(t, "Durable should still be present")
	}
	// Send second message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		stackFatalf(t, "Unexpected error on publish: %v", err)
	}
	// Restart the durable
	if subType == "sub" {
		sub, err = sc.Subscribe("foo", cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	} else {
		sub, err = sc.QueueSubscribe("foo", groupName, cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	}
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	wait()
	// Unsubscribe for good
	if err := sub.Unsubscribe(); err != nil {
		stackFatalf(t, "Unexpected error on unsubscribe")
	}
	// Wait for unsub to be fully processed
	waitForNumSubs(t, s, clientName, 0)
	// Should have been removed
	ss.RLock()
	if subType == "sub" {
		_, there = ss.durables[durKey]
	} else {
		_, there = ss.qsubs[durKey]
	}
	ss.RUnlock()
	if there {
		stackFatalf(t, "Durable should not be present")
	}
}

func TestNewOnHoldSetOnDurableRestart(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	var (
		dur stan.Subscription
		err error
	)
	total := 50
	count := 0
	ch := make(chan bool)
	dur, err = sc.Subscribe("foo", func(_ *stan.Msg) {
		count++
		if count == total {
			dur.Close()
			ch <- true
		}
	}, stan.SetManualAckMode(), stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}

	// Start a go routine that will pump messages to the channel
	// and make sure that the very first message we receive is
	// the redelivered message.
	failed := false
	mu := sync.Mutex{}
	count = 0
	cb := func(m *stan.Msg) {
		count++
		if m.Sequence != uint64(count) && !m.Redelivered {
			failed = true
		}
		if count == total {
			mu.Lock()
			dur.Unsubscribe()
			mu.Unlock()
			ch <- true
		}
	}
	stop := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := sc.Publish("foo", []byte("hello")); err != nil {
					t.Fatalf("Unexpected error on publish: %v", err)
				}
			}
		}
	}()
	// Restart durable
	mu.Lock()
	dur, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(), stan.DurableName("dur"))
	mu.Unlock()
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Make publishers stop
	close(stop)
	// Wait for go routine to finish
	wg.Wait()
	// Check our success
	if failed {
		t.Fatal("Did not receive the redelivered messages first")
	}
}

func TestPersistentStoreDurableClosedStatusOnRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(50*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	dur, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)
	// Close durable
	dur.Close()
	waitForNumSubs(t, s, clientName, 0)

	// Function that restart a durable and checks that IsClosed is false
	restartDurable := func() stan.Subscription {
		dur, err = sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
		if err != nil {
			stackFatalf(t, "Unexpected error on subscribe: %v", err)
		}
		waitForNumSubs(t, s, clientName, 1)
		subs := checkSubs(t, s, clientName, 1)
		dsub := subs[0]
		dsub.RLock()
		isClosed := dsub.IsClosed
		dsub.RUnlock()
		if isClosed {
			t.Fatal("Durable's IsClosed should be false")
		}
		return dur
	}
	// Restart durable
	dur = restartDurable()
	// Close durable again
	dur.Close()
	waitForNumSubs(t, s, clientName, 0)
	// Keep client connection opened, but restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// THere should be no sub recovered for this client
	checkSubs(t, s, clientName, 0)
	// Restart one last time
	dur = restartDurable()
}
