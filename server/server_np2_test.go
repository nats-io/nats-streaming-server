// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

func TestStartPositionLastReceived(t *testing.T) {
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

func TestStartPositionFirstSequence(t *testing.T) {
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

func TestStartPositionSequenceStart(t *testing.T) {
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

func TestStartPositionTimeDelta(t *testing.T) {
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

func TestStartPositionWithDurable(t *testing.T) {
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

func TestStartPositionWithDurableQueueSub(t *testing.T) {
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

func TestStartPositionWithQueueSub(t *testing.T) {
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

func TestCheckClientHealth(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	// Override HB settings
	opts.ClientHBInterval = 50 * time.Millisecond
	opts.ClientHBTimeout = 10 * time.Millisecond
	opts.ClientHBFailCount = 5
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

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
	waitForNumClients(t, s, 1)

	// Check that client is not incorrectly purged
	dur := (s.opts.ClientHBInterval + s.opts.ClientHBTimeout)
	dur *= time.Duration(s.opts.ClientHBFailCount + 1)
	dur += 100 * time.Millisecond
	time.Sleep(dur)
	// Client should still be there
	waitForNumClients(t, s, 1)

	// kill the NATS conn
	nc.Close()

	// Check that the server closes the connection
	waitForNumClients(t, s, 0)
}

func TestCheckClientHealthDontKeepClientLock(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	// Override HB settings
	opts.ClientHBInterval = 50 * time.Millisecond
	opts.ClientHBTimeout = 3 * time.Second
	opts.ClientHBFailCount = 1
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

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
	waitForNumClients(t, s, 1)

	// Kill the NATS Connection
	nc.Close()

	// Check that when the server sends a HB request,
	// the client is not blocked for the duration of the
	// HB Timeout
	start := time.Now()

	// Since we can't reliably know when the server is performing
	// the HB request, we are going to wait for at least 2 HB intervals
	// before checking.
	time.Sleep(2 * opts.ClientHBInterval)

	c := s.clients.lookup(clientName)
	c.RLock()
	// This is to avoid staticcheck "empty critical section (SA2001)" report
	_ = c.fhb
	c.RUnlock()
	dur := time.Since(start)
	// This should have taken less than HB Timeout
	if dur >= opts.ClientHBTimeout {
		t.Fatalf("Client may be locked for the duration of the HB request: %v", dur)
	}
}

func TestConnectsWithDupCID(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	// Not too small to avoid flapping tests.
	s.dupCIDTimeout = 1 * time.Second
	s.dupMaxCIDRoutines = 5
	total := int(s.dupMaxCIDRoutines)

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	dupCIDName := "dupCID"

	sc, err := stan.Connect(clusterName, dupCIDName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Close the nc connection
	nc.Close()

	var wg sync.WaitGroup

	// Channel large enough to hold all possible errors.
	errors := make(chan error, 3*total)

	dupTimeoutMin := time.Duration(float64(s.dupCIDTimeout) * 0.9)
	dupTimeoutMax := time.Duration(float64(s.dupCIDTimeout) * 1.1)

	wg.Add(1)

	connect := func(cid string, shouldFail bool) (stan.Conn, time.Duration, error) {
		start := time.Now()
		c, err := stan.Connect(clusterName, cid, stan.ConnectWait(3*s.dupCIDTimeout))
		duration := time.Since(start)
		if shouldFail {
			if c != nil {
				c.Close()
			}
			if err == nil || err == stan.ErrConnectReqTimeout {
				return nil, 0, fmt.Errorf("Connect should have failed")
			}
			return nil, duration, nil
		} else if err != nil {
			return nil, 0, err
		}
		return c, duration, nil
	}

	getErrors := func() string {
		errorsStr := ""
		numErrors := len(errors)
		for i := 0; i < numErrors; i++ {
			e := <-errors
			oneErr := fmt.Sprintf("%d: %s\n", (i + 1), e.Error())
			if i == 0 {
				errorsStr = "\n"
			}
			errorsStr = errorsStr + oneErr
		}
		return errorsStr
	}

	// Start this go routine that will try to connect 2*total-1
	// times. These all should fail (quickly) since the one
	// connecting below should be the one that connects.
	go func() {
		defer wg.Done()
		time.Sleep(s.dupCIDTimeout / 2)
		for i := 0; i < 2*total-1; i++ {
			_, duration, err := connect(dupCIDName, true)
			if err != nil {
				errors <- err
				continue
			}
			// These should fail "immediately", so consider it a failure if
			// it is close to the dupCIDTimeout
			if duration >= dupTimeoutMin {
				errors <- fmt.Errorf("Connect took too long to fail: %v", duration)
			}
		}
	}()

	// This connection on different client ID should not take long
	newConn, duration, err := connect("newCID", false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer newConn.Close()
	if duration >= dupTimeoutMin {
		t.Fatalf("Connect expected to be fast, took %v", duration)
	}

	// This one should connect, and it should take close to dupCIDTimeout
	replaceConn, duration, err := connect(dupCIDName, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer replaceConn.Close()
	if duration < dupTimeoutMin || duration > dupTimeoutMax {
		t.Fatalf("Connect expected in the range [%v-%v], took %v",
			dupTimeoutMin, dupTimeoutMax, duration)
	}

	// Wait for all other connects to complete
	wg.Wait()

	// Report possible errors
	if errs := getErrors(); errs != "" {
		t.Fatalf("Test failed: %v", errs)
	}

	// We don't need those anymore.
	newConn.Close()
	replaceConn.Close()

	// Now, let's create (total + 1) connections with different CIDs
	// and close their NATS connection. Then try to "reconnect".
	// The first (total) connections should each take about dupCIDTimeout to
	// complete.
	// The last (total + 1) connection should be delayed waiting for
	// a go routine to finish. So the expected duration - assuming that
	// they all start roughly at the same time - would be 2 * dupCIDTimeout.
	for i := 0; i < total+1; i++ {
		nc, err := nats.Connect(nats.DefaultURL)
		if err != nil {
			t.Fatalf("Unexpected error on connect: %v", err)
		}
		defer nc.Close()

		cid := fmt.Sprintf("%s_%d", dupCIDName, i)
		sc, err := stan.Connect(clusterName, cid, stan.NatsConn(nc))
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		defer sc.Close()

		// Close the nc connection
		nc.Close()
	}

	wg.Add(total + 1)

	// Need to close the connections only after the test is done
	conns := make([]stan.Conn, total+1)

	// Cleanup function
	cleanupConns := func() {
		wg.Wait()
		for _, c := range conns {
			c.Close()
		}
	}

	var delayedGuard sync.Mutex
	delayed := false

	// Connect 1 more than the max number of allowed go routines.
	for i := 0; i < total+1; i++ {
		go func(idx int) {
			defer wg.Done()
			cid := fmt.Sprintf("%s_%d", dupCIDName, idx)
			c, duration, err := connect(cid, false)
			if err != nil {
				errors <- err
				return
			}
			conns[idx] = c
			ok := false
			if duration >= dupTimeoutMin && duration <= dupTimeoutMax {
				ok = true
			}
			if !ok && duration >= 2*dupTimeoutMin && duration <= 2*dupTimeoutMax {
				delayedGuard.Lock()
				if delayed {
					delayedGuard.Unlock()
					errors <- fmt.Errorf("Failing %q, only one connection should take that long", cid)
					return
				}
				delayed = true
				delayedGuard.Unlock()
				return
			}
			if !ok {
				if duration < dupTimeoutMin || duration > dupTimeoutMax {
					errors <- fmt.Errorf("Connect with cid %q expected in the range [%v-%v], took %v",
						cid, dupTimeoutMin, dupTimeoutMax, duration)
				}
			}
		}(i)
	}

	// Wait for all routines to return
	wg.Wait()

	// Wait for other connects to complete, and close them.
	cleanupConns()

	// Report possible errors
	if errs := getErrors(); errs != "" {
		t.Fatalf("Test failed: %v", errs)
	}
}

func TestStoreTypeUnknown(t *testing.T) {
	opts := GetDefaultOptions()
	opts.StoreType = "MyType"

	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestSubscribeShrink(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	nsubs := 1000
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
	// Now unsubsribe them all
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

func TestGetSubStoreRace(t *testing.T) {
	numChans := 10000

	opts := GetDefaultOptions()
	opts.MaxChannels = numChans + 1
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	errs := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	chanNames := make([]string, numChans)
	// Create the channel names in advance to increase concurrency
	// of critical code in the function below.
	for i := 0; i < numChans; i++ {
		chanNames[i] = fmt.Sprintf("channel_%v", i)
	}
	// Perform lookup of channel and access subStore
	f := func() {
		defer wg.Done()
		for i := 0; i < numChans; i++ {
			cs, err := s.lookupOrCreateChannel(chanNames[i])
			if err != nil {
				errs <- err
				return
			}
			if cs.ss == nil {
				errs <- fmt.Errorf("subStore is nil")
				return
			}
		}
	}
	// Run two go routines to cause parallel execution
	go f()
	go f()
	// Wait for them to return
	wg.Wait()
	// Report possible errors
	if len(errs) > 0 {
		t.Fatalf("%v", <-errs)
	}
}

// TestEnsureStandAlone tests to ensure the server
// panics if it detects another instance running in
// a cluster.
func TestEnsureStandAlone(t *testing.T) {

	// Start a streaming server, and setup a route
	nOpts := DefaultNatsServerOptions
	nOpts.Cluster.ListenStr = "nats://127.0.0.1:5550"
	nOpts.RoutesStr = "nats://127.0.0.1:5551"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()

	// Start a second streaming server and route to the first, while using the
	// same cluster ID.  It should fail
	nOpts2 := DefaultNatsServerOptions
	nOpts2.Port = 4333
	nOpts2.Cluster.ListenStr = "nats://127.0.0.1:5551"
	nOpts2.RoutesStr = "nats://127.0.0.1:5550"
	s2, err := RunServerWithOpts(sOpts, &nOpts2)
	if s2 != nil || err == nil {
		s2.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestAuthenticationUserPass(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Username = "colin"
	nOpts.Password = "alpine"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()

	_, err := nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Server allowed a plain connection")
	}

	_, err = nats.Connect(fmt.Sprintf("nats://%s:badpass@%s:%d", nOpts.Username, nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Server allowed invalid credentials")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s@%s:%d", nOpts.Username, nOpts.Password, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestAuthenticationUserOnly(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Username = "colin"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()

	_, err := nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Server allowed a plain connection")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:@%s:%d", nOpts.Username, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestAuthenticationToken(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Authorization = "0ffw1dth"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()

	_, err := nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Authentication allowed a plain connection")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s@%s:%d", nOpts.Authorization, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestAuthenticationMultiUser(t *testing.T) {

	nOpts, err := natsd.ProcessConfigFile("../test/configs/multi_user.conf")
	if err != nil {
		t.Fatalf("Unable to parse configuration file: %v", err)
	}
	nOpts.Username = "alice"
	nOpts.Password = "foo"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := runServerWithOpts(t, sOpts, nOpts)
	defer s.Shutdown()

	_, err = nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Authentication allowed a plain connection")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s@%s:%d", nOpts.Username, nOpts.Password, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestTLSSuccess(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.ClientCert = "../test/certs/client-cert.pem"
	sOpts.ClientCA = "../test/certs/ca.pem"
	sOpts.ClientKey = "../test/certs/client-key.pem"

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()
}

func TestTLSSuccessSecure(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.Secure = true

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()
}

func TestTLSFailServerTLSClientPlain(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s, err := RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestTLSFailClientTLSServerPlain(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.ClientCert = "../test/certs/client-cert.pem"
	sOpts.ClientCA = "../test/certs/ca.pem"
	sOpts.ClientKey = "../test/certs/client-key.pem"

	s, err := RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestIOChannel(t *testing.T) {
	// TODO: When running tests on my Windows VM, looks like we are getting
	// a slow consumer scenario (the NATS Streaming server being the slow
	// consumer). So skip for now.
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}

	run := func(opts *Options) {
		s := runServerWithOpts(t, opts, nil)
		defer s.Shutdown()

		sc := NewDefaultConnection(t)
		defer sc.Close()

		ackCb := func(guid string, ackErr error) {
			if ackErr != nil {
				panic(fmt.Errorf("%v - Ack for %v: %v", time.Now(), guid, ackErr))
			}
		}

		total := s.opts.IOBatchSize + 100
		msg := []byte("Hello")
		var err error
		for i := 0; i < total; i++ {
			if i < total-1 {
				_, err = sc.PublishAsync("foo", msg, ackCb)
			} else {
				err = sc.Publish("foo", msg)
			}
			if err != nil {
				stackFatalf(t, "Unexpected error on publish: %v", err)
			}
		}

		// Make sure we have all our messages stored in the server
		checkCount(t, total, func() (string, int) {
			n, _, _ := s.channels.msgsState("foo")
			return "Messages", n
		})
		// For IOBatchSize > 0, check that the actual limit was never crossed.
		if opts.IOBatchSize > 0 {
			// Check that the server's ioChannel did not grow bigger than expected
			ioChannelSize := int(atomic.LoadInt64(&(s.ioChannelStatsMaxBatchSize)))
			if ioChannelSize > opts.IOBatchSize {
				stackFatalf(t, "Expected max channel size to be smaller than %v, got %v", opts.IOBatchSize, ioChannelSize)
			}
		}
	}

	sOpts := GetDefaultOptions()
	run(sOpts)

	sOpts = GetDefaultOptions()
	sOpts.IOBatchSize = 50
	run(sOpts)

	sOpts = GetDefaultOptions()
	sOpts.IOBatchSize = 0
	run(sOpts)

	sOpts = GetDefaultOptions()
	sOpts.IOSleepTime = 500
	run(sOpts)
}

func TestDontEmbedNATSNotRunning(t *testing.T) {
	sOpts := GetDefaultOptions()
	// Make sure that with empty string (normally the default), we
	// can run the streaming server (will embed NATS)
	sOpts.NATSServerURL = ""
	s := runServerWithOpts(t, sOpts, nil)
	s.Shutdown()

	// Point to a NATS Server that will not be running
	sOpts.NATSServerURL = "nats://localhost:5223"

	// Don't start a NATS Server, starting streaming server
	// should fail.
	s, err := RunServerWithOpts(sOpts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestDontEmbedNATSRunning(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.NATSServerURL = "nats://localhost:5223"

	nOpts := DefaultNatsServerOptions
	nOpts.Host = "localhost"
	nOpts.Port = 5223
	natsd := natsdTest.RunServer(&nOpts)
	defer natsd.Shutdown()

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()
}

func TestDontEmbedNATSMultipleURLs(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Host = "localhost"
	nOpts.Port = 5223
	nOpts.Username = "ivan"
	nOpts.Password = "pwd"
	natsServer := natsdTest.RunServer(&nOpts)
	defer natsServer.Shutdown()

	sOpts := GetDefaultOptions()

	workingURLs := []string{
		"nats://localhost:5223",
		"nats://ivan:pwd@localhost:5223",
		"nats://ivan:pwd@localhost:5223, nats://ivan:pwd@localhost:5224",
	}
	for _, url := range workingURLs {
		sOpts.NATSServerURL = url
		s := runServerWithOpts(t, sOpts, &nOpts)
		s.Shutdown()
	}

	notWorkingURLs := []string{
		"nats://ivan:incorrect@localhost:5223",
		"nats://localhost:5223,nats://ivan:pwd@localhost:5224",
		"nats://localhost",
		"localhost:5223",
		"localhost",
		"nats://ivan:pwd@:5224",
		" ",
	}
	for _, url := range notWorkingURLs {
		sOpts.NATSServerURL = url
		s, err := RunServerWithOpts(sOpts, &nOpts)
		if s != nil || err == nil {
			s.Shutdown()
			t.Fatalf("Expected streaming server to fail to start with url=%v", url)
		}
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
	sub2, err = sc.QueueSubscribe("foo", "group", cb, stan.AckWait(time.Second))
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
		stan.AckWait(time.Second),
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

func TestDroppedMessagesOnSendToSub(t *testing.T) {
	opts := GetDefaultOptions()
	opts.MaxMsgs = 3
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Produce 1 message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a durable, it should receive the message
	ch := make(chan bool)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {
		ch <- true
	}, stan.DurableName("dur"),
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Recreate a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Send messages 2, 3, 4 and 5. Messages 1 and 2 should be dropped.
	for i := 2; i <= 5; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Start the durable, it should receive messages 3, 4 and 5
	expectedSeq := uint64(3)
	good := 0
	cb := func(m *stan.Msg) {
		if m.Sequence == expectedSeq {
			good++
			if good == 3 {
				ch <- true
			}
		}
		expectedSeq++
	}
	if _, err := sc.Subscribe("foo", cb,
		stan.DurableName("dur"),
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for messages:
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
}

func TestDroppedMessagesOnSendToQueueSub(t *testing.T) {
	opts := GetDefaultOptions()
	opts.MaxMsgs = 3
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Produce 1 message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a queue subscriber, it should receive the message
	ch := make(chan bool)
	blocked := make(chan bool)
	if _, err := sc.QueueSubscribe("foo", "bar", func(m *stan.Msg) {
		ch <- true
		// Block
		<-blocked
		m.Ack()
	}, stan.MaxInflight(1), stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Send messages 2, 3, 4 and 5. Messages 1 and 2 should be dropped.
	for i := 2; i <= 5; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Start another member, it should receive messages 3, 4 and 5
	expectedSeq := uint64(3)
	good := 0
	cb := func(m *stan.Msg) {
		if m.Sequence == expectedSeq {
			good++
			if good == 3 {
				ch <- true
			}
		}
		expectedSeq++
	}
	if _, err := sc.QueueSubscribe("foo", "bar", cb,
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for messages:
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
	// Unlock first member
	close(blocked)
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
		stan.AckWait(time.Second),
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
