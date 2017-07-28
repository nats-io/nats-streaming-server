// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"flag"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nuid"
)

func testPerChannelLimits(t *testing.T, opts *Options) {
	opts.MaxMsgs = 10
	opts.MaxAge = time.Hour
	clfoo := stores.ChannelLimits{}
	clfoo.MaxMsgs = 2
	clbar := stores.ChannelLimits{}
	clbar.MaxBytes = 1000
	clbaz := stores.ChannelLimits{}
	clbaz.MaxSubscriptions = 1
	clbaz.MaxAge = time.Second
	sl := &opts.StoreLimits
	sl.AddPerChannel("foo", &clfoo)
	sl.AddPerChannel("bar", &clbar)
	sl.AddPerChannel("baz", &clbaz)

	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Sending on foo should be limited to 2 messages
	for i := 0; i < 10; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Check messages count
	s.mu.RLock()
	n, _, err := s.channels.msgsState("foo")
	s.mu.RUnlock()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	if n != clfoo.MaxMsgs {
		t.Fatalf("Expected only %v messages, got %v", clfoo.MaxMsgs, n)
	}

	// Sending on bar should be limited by the size, or the count of global
	// setting
	for i := 0; i < 100; i++ {
		if err := sc.Publish("bar", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Check messages count
	s.mu.RLock()
	n, b, err := s.channels.msgsState("bar")
	s.mu.RUnlock()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	// There should be more than for foo, but no more than opts.MaxMsgs
	if n <= clfoo.MaxMsgs {
		t.Fatalf("Expected more messages than %v", n)
	}
	if n > opts.MaxMsgs {
		t.Fatalf("Should be limited by parent MaxMsgs of %v, got %v", opts.MaxMsgs, n)
	}
	// The size should be lower than clbar.MaxBytes
	if b > uint64(clbar.MaxBytes) {
		t.Fatalf("Expected less than %v bytes, got %v", clbar.MaxBytes, b)
	}

	// Number of subscriptions on baz should be limited to 1
	for i := 0; i < 2; i++ {
		if _, err := sc.Subscribe("baz", func(_ *stan.Msg) {}); i == 1 && err == nil {
			t.Fatal("Expected max subscriptions to be 1")
		}
	}
	// Max Age for baz should be 1sec.
	if err := sc.Publish("baz", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait more than max age
	time.Sleep(1500 * time.Millisecond)
	// Check state
	s.mu.RLock()
	n, _, err = s.channels.msgsState("baz")
	s.mu.RUnlock()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	if n != 0 {
		t.Fatalf("Message should have expired")
	}
}

func TestPerChannelLimits(t *testing.T) {
	opts := GetDefaultOptions()
	testPerChannelLimits(t, opts)
}

func TestProtocolOrder(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	for i := 0; i < 10; i++ {
		if err := sc.Publish("bar", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	ch := make(chan bool)
	errCh := make(chan error)

	recv := int32(0)
	mode := 0
	var sc2 stan.Conn
	cb := func(m *stan.Msg) {
		count := atomic.AddInt32(&recv, 1)
		if err := m.Ack(); err != nil {
			errCh <- err
			return
		}
		if count == 10 {
			var err error
			switch mode {
			case 1:
				err = m.Sub.Unsubscribe()
			case 2:
				err = m.Sub.Close()
			case 3:
				err = sc2.Close()
			case 4:
				err = m.Sub.Unsubscribe()
				if err == nil {
					err = sc2.Close()
				}
			case 5:
				err = m.Sub.Close()
				if err == nil {
					err = sc2.Close()
				}
			}
			if err != nil {
				errCh <- err
			} else {
				ch <- true
			}
		}
	}

	total := 50
	// Unsubscribe should not be processed before last processed Ack
	mode = 1
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		if _, err := sc.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Subscription close should not be processed before last processed Ack
	mode = 2
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		if _, err := sc.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Connection close should not be processed before last processed Ack
	mode = 3
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		conn, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		sc2 = conn
		if _, err := sc2.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Connection close should not be processed before unsubscribe and last processed Ack
	mode = 4
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		conn, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		sc2 = conn
		if _, err := sc2.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Connection close should not be processed before sub close and last processed Ack
	mode = 5
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		conn, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		sc2 = conn
		if _, err := sc2.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}

	// Mix pub and subscribe calls
	ch = make(chan bool)
	errCh = make(chan error)
	startSubAt := 50
	var sub stan.Subscription
	var err error
	for i := 1; i <= 100; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
		if i == startSubAt {
			sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
				if m.Sequence == uint64(startSubAt)+1 {
					ch <- true
				} else if len(errCh) == 0 {
					errCh <- fmt.Errorf("Received message %v instead of %v", m.Sequence, startSubAt+1)
				}
			})
			if err != nil {
				t.Fatalf("Unexpected error on subscribe: %v", err)
			}
		}
	}
	// Wait confirmation of received message
	select {
	case <-ch:
	case e := <-errCh:
		t.Fatal(e)
	case <-time.After(5 * time.Second):
		t.Fatal("Timed-out waiting for messages")
	}
	sub.Unsubscribe()

	// Acks should be processed before Connection close
	for i := 0; i < total; i++ {
		rcv := int32(0)
		sc2, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		// Create durable and close connection in cb when
		// receiving 10th message.
		if _, err := sc2.Subscribe("foo", func(m *stan.Msg) {
			if atomic.AddInt32(&rcv, 1) == 10 {
				sc2.Close()
				ch <- true
			}
		}, stan.DurableName("dur"), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := Wait(ch); err != nil {
			t.Fatal("Did not get our messages")
		}
		// Connection is closed at this point. Recreate one
		sc2, err = stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		// Recreate durable and first message should be 10.
		first := true
		sub, err = sc2.Subscribe("foo", func(m *stan.Msg) {
			if first {
				if m.Redelivered && m.Sequence == 10 {
					ch <- true
				} else {
					errCh <- fmt.Errorf("Unexpected message: %v", m)
				}
				first = false
			}
		}, stan.DurableName("dur"), stan.DeliverAllAvailable(),
			stan.MaxInflight(1), stan.SetManualAckMode())
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		// Wait for ok or error
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatalf("Error: %v", e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Fatalf("Unexpected error on unsubscribe: %v", err)
		}
		sc2.Close()
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

func TestProcessCommandLineArgs(t *testing.T) {
	var host string
	var port int
	cmd := flag.NewFlagSet("nats-streaming-server", flag.ExitOnError)
	cmd.StringVar(&host, "a", "0.0.0.0", "Host.")
	cmd.IntVar(&port, "p", 4222, "Port.")

	cmd.Parse([]string{"-a", "127.0.0.1", "-p", "9090"})
	showVersion, showHelp, err := natsd.ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion || showHelp {
		t.Errorf("Expected not having to handle subcommands")
	}

	cmd.Parse([]string{"version"})
	showVersion, showHelp, err = natsd.ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if !showVersion {
		t.Errorf("Expected having to handle version command")
	}
	if showHelp {
		t.Errorf("Expected not having to handle help command")
	}

	cmd.Parse([]string{"help"})
	showVersion, showHelp, err = natsd.ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion {
		t.Errorf("Expected not having to handle version command")
	}
	if !showHelp {
		t.Errorf("Expected having to handle help command")
	}

	cmd.Parse([]string{"foo", "-p", "9090"})
	_, _, err = natsd.ProcessCommandLineArgs(cmd)
	if err == nil {
		t.Errorf("Expected an error handling the command arguments")
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
	cb := func(m *stan.Msg) {
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
	if _, err := sc1.QueueSubscribe("foo", "group", cb, stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Create 2nd member.
	sc2 := NewDefaultConnection(t)
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "group", cb, stan.AckWait(time.Second)); err != nil {
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

func TestAckPublisherBufSize(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	errCh := make(chan error)
	inbox := nats.NewInbox()
	iopm := &ioPendingMsg{m: &nats.Msg{Reply: inbox}}
	nc.Subscribe(inbox, func(m *nats.Msg) {
		pubAck := pb.PubAck{}
		if err := pubAck.Unmarshal(m.Data); err != nil {
			errCh <- err
			return
		}
		if !reflect.DeepEqual(pubAck, iopm.pa) {
			errCh <- fmt.Errorf("Expected PubAck: %v, got: %v", iopm.pa, pubAck)
			return
		}
		errCh <- nil
	})
	nc.Flush()

	checkErr := func() {
		select {
		case e := <-errCh:
			if e != nil {
				t.Fatalf("%v", e)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not get our message")
		}
	}

	iopm.pm.Guid = nuid.Next()
	s.ackPublisher(iopm)
	checkErr()

	iopm.pm.Guid = "this is a very very very very very very very very very very very very very very very very very very long guid"
	s.ackPublisher(iopm)
	checkErr()
}

func TestDontSendEmptyMsgProto(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.NoReconnect())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	// Since server is expected to crash, do not attempt to close sc
	// because it would delay test by 2 seconds.

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	waitForNumSubs(t, s, clientName, 1)

	subs := s.clients.getSubs(clientName)
	sub := subs[0]

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Server should have panic'ed")
		}
	}()

	m := &pb.MsgProto{}
	sub.Lock()
	s.sendMsgToSub(sub, m, false)
	sub.Unlock()
}

func TestMsgsNotSentToSubBeforeSubReqResponse(t *testing.T) {
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
	unsubSubj := connResponse.UnsubRequests
	pubSubj := connResponse.PubPrefix + ".foo"

	pubMsg := &pb.PubMsg{
		ClientID: clientName,
		Subject:  "foo",
		Data:     []byte("hello"),
	}
	subReq := &pb.SubscriptionRequest{
		ClientID:      clientName,
		MaxInFlight:   1024,
		Subject:       "foo",
		StartPosition: pb.StartPosition_First,
		AckWaitInSecs: 30,
	}
	unsubReq := &pb.UnsubscribeRequest{
		ClientID: clientName,
		Subject:  "foo",
	}
	for i := 0; i < 100; i++ {
		// Use the same subscriber for subscription request response and data,
		// so we can reliably check if data comes before response.
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("Unable to create nats subscriber: %v", err)
		}
		subReq.Inbox = inbox
		bytes, _ := subReq.Marshal()
		// Send the request with inbox as the Reply
		if err := nc.PublishRequest(subSubj, inbox, bytes); err != nil {
			t.Fatalf("Error sending request: %v", err)
		}
		// Followed by a data message
		pubMsg.Guid = nuid.Next()
		bytes, _ = pubMsg.Marshal()
		if err := nc.Publish(pubSubj, bytes); err != nil {
			t.Fatalf("Error sending msg: %v", err)
		}
		nc.Flush()
		// Dequeue
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Did not get our message: %v", err)
		}
		// It should always be the subscription response!!!
		msgProto := &pb.MsgProto{}
		err = msgProto.Unmarshal(msg.Data)
		if err == nil && msgProto.Sequence != 0 {
			t.Fatalf("Iter=%v - Did not receive valid subscription response: %#v - %v", (i + 1), msgProto, err)
		}
		subReqResp := &pb.SubscriptionResponse{}
		subReqResp.Unmarshal(msg.Data)
		unsubReq.Inbox = subReqResp.AckInbox
		bytes, _ = unsubReq.Marshal()
		if err := nc.Publish(unsubSubj, bytes); err != nil {
			t.Fatalf("Unable to send unsub request: %v", err)
		}
		sub.Unsubscribe()
	}
}

func TestAckSubsSubjectsInPoolUseUniqueSubject(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	opts.AckSubsPoolSize = 1
	s1 := runServerWithOpts(t, opts, nil)
	defer s1.Shutdown()

	opts.NATSServerURL = nats.DefaultURL
	opts.ID = "otherCluster"
	s2 := runServerWithOpts(t, opts, nil)
	defer s2.Shutdown()

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()

	if _, err := sc1.Subscribe("foo", func(m *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	sc2, err := stan.Connect("otherCluster", "otherClient")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc2.Close()

	// Create a subscription with manual ack, and ack after message is
	// redelivered.
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
		}
	}
	if _, err := sc2.Subscribe("foo", cb,
		stan.SetManualAckMode(),
		stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Produce 1 message for each connection
	if err := sc1.Publish("foo", []byte("hello s1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc2.Publish("foo", []byte("hello s2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for ack of sc2 to be processed by s2
	waitForAcks(t, s2, "otherClient", 1, 0)

	s1.mu.Lock()
	s1AcksReceived, _ := s1.acksSubs[0].Delivered()
	s1.mu.Unlock()
	if s1AcksReceived != 1 {
		t.Fatalf("Expected pooled ack sub to receive only 1 message, got %v", s1AcksReceived)
	}
	s2.mu.Lock()
	s2AcksReceived, _ := s2.acksSubs[0].Delivered()
	s2.mu.Unlock()
	if s2AcksReceived != 1 {
		t.Fatalf("Expected pooled ack sub to receive only 1 message, got %v", s2AcksReceived)
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

func TestUnlimitedPerChannelLimits(t *testing.T) {
	opts := GetDefaultOptions()
	opts.StoreLimits.MaxChannels = 2
	// Set very small global limits
	opts.StoreLimits.MaxMsgs = 1
	opts.StoreLimits.MaxBytes = 1
	opts.StoreLimits.MaxAge = time.Millisecond
	opts.StoreLimits.MaxSubscriptions = 1
	// Add a channel that has all unlimited values
	cl := &stores.ChannelLimits{}
	cl.MaxMsgs = -1
	cl.MaxBytes = -1
	cl.MaxAge = -1
	cl.MaxSubscriptions = -1
	opts.StoreLimits.AddPerChannel("foo", cl)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Check that we can send more than 1 message of more than 1 byte
	total := 10
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	count := int32(0)
	ch := make(chan bool)
	cb := func(_ *stan.Msg) {
		if c := atomic.AddInt32(&count, 1); c == int32(2*total) {
			ch <- true
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
	// Wait for more than the global limit MaxAge and verify messages
	// are still there
	time.Sleep(15 * time.Millisecond)
	s.mu.RLock()
	n, _, _ := s.channels.msgsState("foo")
	s.mu.RUnlock()
	if n != total {
		t.Fatalf("Should be %v messages, store reports %v", total, n)
	}
	// Now use a channel not defined in PerChannel and we should be subject
	// to global limits.
	for i := 0; i < total; i++ {
		if err := sc.Publish("bar", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	if _, err := sc.Subscribe("bar", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// This one should fail
	if _, err := sc.Subscribe("bar", func(_ *stan.Msg) {}); err == nil {
		t.Fatal("Expected to fail to subscribe, did not")
	}
	// Wait more than MaxAge
	time.Sleep(15 * time.Millisecond)
	// Messages should have all disappear
	s.mu.RLock()
	n, _, _ = s.channels.msgsState("bar")
	s.mu.RUnlock()
	if n != 0 {
		t.Fatalf("Expected 0 messages, store reports %v", n)
	}
}

type mockedStore struct {
	stores.Store
}

type mockedMsgStore struct {
	stores.MsgStore
	sync.RWMutex
	fail bool
}

type mockedSubStore struct {
	stores.SubStore
	sync.RWMutex
	fail bool
}

func (ms *mockedStore) CreateChannel(name string) (*stores.Channel, error) {
	cs, err := ms.Store.CreateChannel(name)
	if err != nil {
		return nil, err
	}
	cs.Msgs = &mockedMsgStore{MsgStore: cs.Msgs}
	cs.Subs = &mockedSubStore{SubStore: cs.Subs}
	return cs, nil
}

func (ms *mockedMsgStore) Lookup(seq uint64) (*pb.MsgProto, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return nil, errOnPurpose
	}
	return ms.MsgStore.Lookup(seq)
}

func (ms *mockedMsgStore) FirstSequence() (uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, errOnPurpose
	}
	return ms.MsgStore.FirstSequence()
}

func (ms *mockedMsgStore) LastSequence() (uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, errOnPurpose
	}
	return ms.MsgStore.LastSequence()
}

func (ms *mockedMsgStore) FirstAndLastSequence() (uint64, uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, 0, errOnPurpose
	}
	return ms.MsgStore.FirstAndLastSequence()
}

func (ms *mockedMsgStore) GetSequenceFromTimestamp(startTime int64) (uint64, error) {
	ms.RLock()
	fail := ms.fail
	ms.RUnlock()
	if fail {
		return 0, errOnPurpose
	}
	return ms.MsgStore.GetSequenceFromTimestamp(startTime)
}

func TestStartPositionFailures(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unable to publish: %v", err)
	}

	cs := channelsGet(t, s.channels, "foo")
	mms := cs.store.Msgs.(*mockedMsgStore)
	mms.Lock()
	mms.fail = true
	mms.Unlock()

	// New only
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// Last received
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartWithLastReceived()); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// Time delta
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartAtTimeDelta(time.Second)); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// Sequence start
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartAtSequence(1)); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
	// First
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.StartAt(pb.StartPosition_First)); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Not failed as expected: %v", err)
	}
}

type checkErrorLogger struct {
	dummyLogger
	checkErrorStr string
	gotError      bool
}

func (l *checkErrorLogger) Errorf(format string, args ...interface{}) {
	l.log(format, args...)
	l.Lock()
	if strings.Contains(l.msg, l.checkErrorStr) {
		l.gotError = true
	}
	l.Unlock()
}

func TestMsgLookupFailures(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "looking up"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rcvCh := make(chan bool)
	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {
		rcvCh <- true
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	cs := channelsGet(t, s.channels, "foo")
	mms := cs.store.Msgs.(*mockedMsgStore)
	mms.Lock()
	mms.fail = true
	mms.Unlock()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unable to publish: %v", err)
	}

	select {
	case <-rcvCh:
		t.Fatal("Should not have received the message")
	case <-time.After(500 * time.Millisecond):
		// we waited "long enoug" and did not receive anything, which is good
	}
	logger.Lock()
	gotErr := logger.gotError
	logger.Unlock()
	if !gotErr {
		t.Fatalf("Did not capture error about lookup")
	}
	mms.Lock()
	mms.fail = false
	mms.Unlock()
	sub.Unsubscribe()

	// Create subscription, manual ack mode, don't ack, wait for redelivery
	sub, err = sc.Subscribe("foo", func(_ *stan.Msg) {
		rcvCh <- true
	}, stan.DeliverAllAvailable(), stan.SetManualAckMode(), stan.AckWait(time.Second))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := Wait(rcvCh); err != nil {
		t.Fatal("Did not get our message")
	}
	// Activate store failure
	mms.Lock()
	mms.fail = true
	logger.Lock()
	logger.checkErrorStr = "Error getting message for redelivery"
	logger.gotError = false
	logger.Unlock()
	mms.Unlock()
	// Make sure message is not redelivered and we capture the error
	select {
	case <-rcvCh:
		t.Fatal("Should not have received the message")
	case <-time.After(1500 * time.Millisecond):
		// we waited more than redelivery time and did not receive anything, which is good
	}
	logger.Lock()
	gotErr = logger.gotError
	logger.Unlock()
	if !gotErr {
		t.Fatalf("Did not capture error about redelivery")
	}
	mms.Lock()
	mms.fail = false
	mms.Unlock()
	sub.Unsubscribe()

	// Check that removal of qsub with pending message

	// One queue member receives and acks
	if _, err := sc.QueueSubscribe("bar", "queue", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Another member does not ack.
	qsub2, err := sc.QueueSubscribe("bar", "queue", func(_ *stan.Msg) {
		rcvCh <- true
	}, stan.SetManualAckMode(), stan.AckWait(time.Second))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	cs = channelsGet(t, s.channels, "bar")
	mms = cs.store.Msgs.(*mockedMsgStore)

	// Publish messages until qsub2 receives one
forLoop:
	for {
		if err := sc.Publish("bar", []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		select {
		case <-rcvCh:
			break forLoop
		case <-time.After(500 * time.Millisecond):
			// send a new message
		}
	}
	// Activate store failures
	mms.Lock()
	mms.fail = true
	logger.Lock()
	logger.checkErrorStr = "Unable to update subscription"
	logger.gotError = false
	logger.Unlock()
	mms.Unlock()
	// Close qsub2, server should try to move unack'ed message to qsub1
	if err := qsub2.Close(); err != nil {
		t.Fatalf("Error closing qsub: %v", err)
	}
	logger.Lock()
	gotErr = logger.gotError
	logger.Unlock()
	if !gotErr {
		t.Fatalf("Did not capture error about updating subscription")
	}
}

func (ss *mockedSubStore) AddSeqPending(subid, seq uint64) error {
	ss.RLock()
	fail := ss.fail
	ss.RUnlock()
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.AddSeqPending(subid, seq)
}

func (ss *mockedSubStore) UpdateSub(sub *spb.SubState) error {
	ss.RLock()
	fail := ss.fail
	ss.RUnlock()
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.UpdateSub(sub)
}

func (ss *mockedSubStore) DeleteSub(subid uint64) error {
	ss.RLock()
	fail := ss.fail
	ss.RUnlock()
	if fail {
		return fmt.Errorf("On purpose")
	}
	return ss.SubStore.DeleteSub(subid)
}

func TestDeleteSubFailures(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "deleting subscription"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Create a plain sub
	psub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a queue sub
	qsub, err := sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a durable queue sub with manual ack and does not ack message
	ch := make(chan bool)
	dqsub1, err := sc.QueueSubscribe("foo", "dqueue", func(_ *stan.Msg) {
		ch <- true
	}, stan.DurableName("dur"), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Produce a message to this durable queue sub
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Create 2 more durable queue subs
	dqsub2, err := sc.QueueSubscribe("foo", "dqueue", func(_ *stan.Msg) {},
		stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if _, err := sc.QueueSubscribe("foo", "dqueue", func(_ *stan.Msg) {},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Ensure subscription is processed
	waitForNumSubs(t, s, clientName, 5)

	cs := channelsGet(t, s.channels, "foo")
	mss := cs.store.Subs.(*mockedSubStore)
	mss.Lock()
	mss.fail = true
	mss.Unlock()

	// Check that server reported an error
	checkError := func() {
		logger.Lock()
		gotIt := logger.gotError
		logger.gotError = false
		logger.Unlock()
		if !gotIt {
			stackFatalf(t, "Server did not log error on unsubscribe")
		}
	}

	// Now unsubscribe
	if err := psub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}
	// Wait for unsubscribe to be processed
	waitForNumSubs(t, s, clientName, 4)
	checkError()

	// Unsubscribe queue sub
	if err := qsub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}
	// Wait for unsubscribe to be processed
	waitForNumSubs(t, s, clientName, 3)
	checkError()

	// Close 1 durable queue sub
	if err := dqsub2.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// Wait for close to be processed
	waitForNumSubs(t, s, clientName, 2)
	checkError()

	// Now check that when closing qsub1 that has an unack message,
	// server logs an error when trying to move the message to remaining
	// queue member
	logger.Lock()
	logger.checkErrorStr = "update subscription"
	logger.Unlock()
	if err := dqsub1.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// Wait for close to be processed
	waitForNumSubs(t, s, clientName, 1)
	checkError()
}

func TestUpdateSubFailure(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "add subscription"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	dur, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)
	dur.Close()
	waitForNumSubs(t, s, clientName, 0)

	cs := channelsGet(t, s.channels, "foo")
	mss := cs.store.Subs.(*mockedSubStore)
	mss.Lock()
	mss.fail = true
	mss.Unlock()
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur")); err == nil {
		t.Fatal("Expected subscription to fail")
	}
	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatalf("Server did not log error on subscribe")
	}
}

func TestQueueSubStoreFailure(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "update subscription"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	s.channels.Lock()
	s.channels.store = &mockedStore{Store: s.channels.store}
	s.channels.Unlock()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)

	// Cause failure on AddSeqPending
	cs := channelsGet(t, s.channels, "foo")
	mss := cs.store.Subs.(*mockedSubStore)
	mss.Lock()
	mss.fail = true
	mss.Unlock()
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Check error was logged.
	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatal("Server did not log error about updating subscription")
	}
}

func TestClientStoreError(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "deleting client"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	s.clients.Lock()
	s.clients.store = &clientStoreErrorsStore{Store: s.clients.store}
	s.clients.Unlock()

	// Client should not fail to close
	if err := sc.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// However, server should have logged something about an error closing client
	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatal("Server did not report error about closing client")
	}
	// Verify that client is gone though
	if c := s.clients.lookup(clientName); c != nil {
		t.Fatalf("Unexpected client in server: %v", c)
	}

	logger.Lock()
	logger.gotError = false
	logger.checkErrorStr = "registering client"
	logger.Unlock()

	if _, err := stan.Connect(clusterName, clientName); err == nil || !strings.Contains(err.Error(), errOnPurpose.Error()) {
		t.Fatalf("Expected error on connect, got %v", err)
	}
	logger.Lock()
	gotIt = logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatal("Server did not report error about registering client")
	}
	if c := s.clients.lookup(clientName); c != nil {
		t.Fatalf("Unexpected client in server: %v", c)
	}
}

func TestAckForUnknownChannel(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "not found"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)
	c := channelsGet(t, s.channels, "foo")
	sub := c.ss.psubs[0]

	ack := pb.Ack{
		Subject:  "bar",
		Sequence: 1,
	}
	ackBytes, err := ack.Marshal()
	if err != nil {
		t.Fatalf("Error during marshaling: %v", err)
	}
	sc.NatsConn().Publish(sub.AckInbox, ackBytes)
	time.Sleep(100 * time.Millisecond)
	logger.Lock()
	gotIt := logger.gotError
	logger.Unlock()
	if !gotIt {
		t.Fatalf("Server did not log error about not finding channel")
	}
}
