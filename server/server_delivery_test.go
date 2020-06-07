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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
)

func testStalledDelivery(t *testing.T, typeSub string) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	lastMsgSentTime := int64(0)
	ackDelay := 700 * time.Millisecond
	toSend := int32(2)
	numSubs := 1

	ch := make(chan bool)
	errors := make(chan error)
	acked := int32(0)

	cb := func(m *stan.Msg) {
		// No message should be redelivered because of MaxInFlight==1
		if m.Redelivered {
			errors <- fmt.Errorf("Unexpected message redelivered")
			return
		}
		go func() {
			time.Sleep(ackDelay)
			m.Ack()
			if atomic.AddInt32(&acked, 1) == toSend {
				ch <- true
			}
		}()
		if int32(m.Sequence) == toSend {
			now := time.Now().UnixNano()
			sent := atomic.LoadInt64(&lastMsgSentTime)
			elapsed := time.Duration(now - sent)
			if elapsed < ackDelay-20*time.Millisecond {
				errors <- fmt.Errorf("Second message received too soon: %v", elapsed)
				return
			}
		}
	}
	if typeSub == "queue" {
		// Create 2 queue subs with manual ack mode and maxInFlight of 1.
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		numSubs = 2
		toSend = 3
	} else if typeSub == "durable" {
		// Create a durable with manual ack mode and maxInFlight of 1.
		if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur"),
			stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	} else {
		// Create a sub with manual ack mode and maxInFlight of 1.
		if _, err := sc.Subscribe("foo", cb, stan.SetManualAckMode(),
			stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}
	// Wait for subscriber to be registered before starting publish
	waitForNumSubs(t, s, clientName, numSubs)
	// Send our messages
	for i := int32(0); i < toSend; i++ {
		if i == toSend-1 {
			atomic.AddInt64(&lastMsgSentTime, time.Now().UnixNano())
		}
		msg := fmt.Sprintf("msg_%d", (i + 1))
		if err := sc.Publish("foo", []byte(msg)); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Wait for completion or error
	select {
	case <-ch:
		break
	case e := <-errors:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our messages")
	}
}

func TestStalledDelivery(t *testing.T) {
	testStalledDelivery(t, "sub")
}

func TestStalledQueueDelivery(t *testing.T) {
	testStalledDelivery(t, "queue")
}

func TestStalledDurableDelivery(t *testing.T) {
	testStalledDelivery(t, "durable")
}

func TestPersistentStoreAutomaticDeliveryOnRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	toSend := int32(10)
	msg := []byte("msg")

	// Get our STAN connection
	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(50*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Send messages
	for i := int32(0); i < toSend; i++ {
		if err := sc.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	blocked := make(chan bool)
	ready := make(chan bool)
	done := make(chan bool)
	received := int32(0)

	// Message callback
	cb := func(m *stan.Msg) {
		count := atomic.AddInt32(&received, 1)
		if count == 2 {
			// Notify we are ready
			ready <- true
			// Wait to be unblocked
			<-blocked
		} else if count == toSend {
			done <- true
		}
	}
	// Create a subscriber that will block after receiving 2 messages.
	if _, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.MaxInflight(2)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for callback to block
	if err := Wait(ready); err != nil {
		t.Fatal("Did not get our messages")
	}

	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Release 	the consumer
	close(blocked)
	// Wait for messages to be delivered
	if err := Wait(done); err != nil {
		t.Fatal("Messages were not automatically delivered")
	}
	sc.Close()
	nc.Close()
}

type mockedGapInSeqStore struct {
	stores.Store
}

func (ms *mockedGapInSeqStore) CreateChannel(name string) (*stores.Channel, error) {
	cs, err := ms.Store.CreateChannel(name)
	if err != nil {
		return nil, err
	}
	cs.Msgs = &mockedGapInSeqMsgStore{MsgStore: cs.Msgs}
	return cs, nil
}

type mockedGapInSeqMsgStore struct {
	stores.MsgStore
}

func (mms *mockedGapInSeqMsgStore) Store(m *pb.MsgProto) (uint64, error) {
	if m.Sequence == 3 {
		return 3, nil
	}
	return mms.MsgStore.Store(m)
}

func TestDeliveryWithGapsInSequence(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedGapInSeqStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	for i := 0; i < 10; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unable to publish: %v", err)
		}
	}

	ch := make(chan bool)
	errCh := make(chan error, 1)
	count := 0
	sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == 3 {
			errCh <- fmt.Errorf("Should not have received message 3")
		} else {
			count++
			if count == 9 {
				ch <- true
			}
		}

	}, stan.DeliverAllAvailable())

	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
	select {
	case e := <-errCh:
		t.Fatalf(e.Error())
	default:
	}
}

func TestPersistentStoreSQLSubsPendingRows(t *testing.T) {
	if !doSQL {
		t.SkipNow()
	}
	source := testSQLSource
	if persistentStoreType != stores.TypeSQL {
		// If not running tests with `-persistent_store sql`,
		// initialize few things and default to MySQL.
		source = testDefaultMySQLSource
		sourceAdmin := testDefaultMySQLSourceAdmin
		if err := test.CreateSQLDatabase(testSQLDriver, sourceAdmin,
			source, testSQLDatabaseName); err != nil {
			t.Fatalf("Error setting up test for SQL: %v", err)
		}
		defer test.DeleteSQLDatabase(testSQLDriver, sourceAdmin, testSQLDatabaseName)
	}

	cleanupDatastore(t)
	defer cleanupDatastore(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := GetDefaultOptions()
	opts.NATSServerURL = "nats://127.0.0.1:4222"
	opts.StoreType = stores.TypeSQL
	opts.SQLStoreOpts.Driver = testSQLDriver
	opts.SQLStoreOpts.Source = source
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Create a regular sub and a durable.
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {},
		stan.SetManualAckMode(),
		stan.MaxInflight(5000)); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	ch := make(chan bool, 1)
	dur, err := sc.Subscribe("foo",
		func(_ *stan.Msg) {
			ch <- true
		},
		stan.SetManualAckMode(),
		stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Publish a message
	sc.Publish("foo", []byte("hello"))
	if err := Wait(ch); err != nil {
		t.Fatalf("Did not get our message")
	}
	dur.Close()
	// Produce another message
	sc.Publish("foo", []byte("hello"))

	// Restart the server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Bombard the running subscriber with messages.
	for i := 0; i < 3000; i++ {
		sc.PublishAsync("foo", []byte("hello"), nil)
	}
	waitForAcks(t, s, clientName, 1, 3002)
}

func TestDeliveryRaceBetweenNextMsgAndStoring(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	prev := uint64(0)
	errCh := make(chan error, 1)
	doneCh := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Sequence != prev+1 {
			errCh <- fmt.Errorf("Previous was %v, now got %v", prev, m.Sequence)
			m.Sub.Close()
			return
		}
		prev = m.Sequence
		if m.Sequence == 4 {
			doneCh <- true
		}
	}
	if _, err := sc.Subscribe("foo", cb, stan.MaxInflight(1)); err != nil {
		t.Fatalf("Erro on subscribe: %v", err)
	}

	sc.Publish("foo", []byte("msg1"))

	ch1 := make(chan struct{})
	ch2 := make(chan bool)
	s.channels.Lock()
	c := s.channels.channels["foo"]
	c.store.Msgs = &blockingLookupStore{MsgStore: c.store.Msgs, inLookupCh: ch1, releaseCh: ch2}
	s.channels.Unlock()

	sub := s.clients.getSubs(clientName)[0]
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s.sendAvailableMessages(c, sub)
		wg.Done()
	}()
	<-ch1
	sc.PublishAsync("foo", []byte("msg2"), nil)
	sc.PublishAsync("foo", []byte("msg3"), nil)
	time.Sleep(50 * time.Millisecond)
	ch2 <- true
	wg.Wait()

	sc.Publish("foo", []byte("msg4"))

	select {
	case <-doneCh:
	case e := <-errCh:
		t.Fatal(e.Error())
	case <-time.After(time.Second):
		t.Fatal("Timeout!")
	}
}

func TestDeliveryStopsWhenSubClosed(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	// Use a bare NATS connection to send incorrect requests
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	// Get the connect subject
	connSubj := fmt.Sprintf("%s.%s", s.opts.DiscoverPrefix, clusterName)
	connReq := &pb.ConnectRequest{
		ClientID:       clientName,
		HeartbeatInbox: nats.NewInbox(),
	}
	crb, _ := connReq.Marshal()
	respMsg, err := nc.Request(connSubj, crb, 5*time.Second)
	if err != nil {
		t.Fatalf("Request error: %v", err)
	}
	connResponse := &pb.ConnectResponse{}
	connResponse.Unmarshal(respMsg.Data)

	subSubj := connResponse.SubRequests
	subCloseSubj := connResponse.SubCloseRequests

	subReq := &pb.SubscriptionRequest{
		ClientID:      clientName,
		MaxInFlight:   1024,
		Subject:       "foo",
		StartPosition: pb.StartPosition_NewOnly,
		AckWaitInSecs: 1,
	}
	subCloseReq := &pb.UnsubscribeRequest{
		ClientID: clientName,
		Subject:  "foo",
	}

	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("Unable to create nats subscriber: %v", err)
	}
	// Send a subscription request
	subReq.Inbox = inbox
	bytes, _ := subReq.Marshal()
	msg, err := nc.Request(subSubj, bytes, time.Second)
	if err != nil {
		t.Fatalf("Error sending subscription request: %v", err)
	}
	// Parse the response
	subReqResp := &pb.SubscriptionResponse{}
	if err := subReqResp.Unmarshal(msg.Data); err != nil {
		t.Fatalf(" Invalid subscription create response: %v", err)
	}
	if subReqResp.Error != "" {
		t.Fatalf("Received response error: %q", subReqResp.Error)
	}

	ssub := s.clients.getSubs(clientName)[0]

	// Close
	subCloseReq.Inbox = subReqResp.AckInbox
	bytes, _ = subCloseReq.Marshal()
	msg, err = nc.Request(subCloseSubj, bytes, time.Second)
	if err != nil {
		t.Fatalf("Unable to sending unsub request: %v", err)
	}
	// Parse the response
	subCloseResp := &pb.SubscriptionResponse{}
	if err := subCloseResp.Unmarshal(msg.Data); err != nil {
		t.Fatalf("Invalid subscription close response: %v", err)
	}
	if subCloseResp.Error != "" {
		t.Fatalf("Error on close: %q", subCloseResp.Error)
	}

	// Now check that if the server was trying to send a message, the
	// message would not be sent and ack timer not setup.
	m := &pb.MsgProto{
		Subject:   "foo",
		Sequence:  1,
		Data:      []byte("hello"),
		Timestamp: time.Now().UnixNano(),
	}
	sent, sendMore := s.sendMsgToSub(ssub, m, false)
	if sent || sendMore {
		t.Fatalf("Should have returned false and false, but returned %v and %v", sent, sendMore)
	}
	// Expect to no receive anything on the sub
	if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || err == nil {
		t.Fatalf("No message expected, got %+v", msg)
	}
	ssub.RLock()
	timerSet := ssub.ackTimer != nil
	ssub.RUnlock()
	if timerSet {
		t.Fatal("Ack timer should not be set, but it was")
	}
}
