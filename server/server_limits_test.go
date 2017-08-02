// Copyright 2016-2017 Apcera Inc. All rights reserved.
package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/stores"
)

func TestTooManyChannelsOnCreateSub(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxChannels = 1
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// That should create channel foo
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// This should fail because we reached the limit
	if _, err := sc.Subscribe("bar", func(_ *stan.Msg) {}); err == nil {
		t.Fatalf("Expected error due to too many channels, got none")
	}
}

func TestTooManyChannelsOnPublish(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxChannels = 1
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// That should create channel foo
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// This should fail since we reached the max channels limit
	if err := sc.Publish("bar", []byte("hello")); err == nil {
		t.Fatalf("Expected error due to too many channels, got none")
	}

	// Check that channel bar was not created
	if s.channels.get("bar") != nil {
		t.Fatal("Channel bar should not have been created")
	}
}

func TestTooManySubs(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxSubscriptions = 1
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// This should be ok
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// We should get an error here
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err == nil {
		t.Fatal("Expected error on subscribe, go none")
	}
	cs := channelsGet(t, s.channels, "foo")
	ss := cs.ss
	func() {
		ss.RLock()
		defer ss.RUnlock()
		if ss.psubs == nil || len(ss.psubs) != 1 {
			t.Fatalf("Expected only one subscription, got %v", len(ss.psubs))
		}
	}()
}

func TestMaxMsgs(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxMsgs = 10
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	for i := 0; i < 2*sOpts.MaxMsgs; i++ {
		sc.Publish("foo", []byte("msg"))
	}

	// We should not have more than MaxMsgs
	cs := channelsGet(t, s.channels, "foo")
	if n, _ := msgStoreState(t, cs.store.Msgs); n != sOpts.MaxMsgs {
		t.Fatalf("Expected msgs count to be %v, got %v", sOpts.MaxMsgs, n)
	}
}

func TestMaxBytes(t *testing.T) {
	payload := []byte("hello")
	m := pb.MsgProto{Data: payload, Subject: "foo", Sequence: 1, Timestamp: time.Now().UnixNano()}
	msgSize := m.Size()
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxBytes = int64(msgSize * 10)
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	count := 2 * (int(sOpts.MaxBytes) / len(payload))
	for i := 0; i < count; i++ {
		sc.Publish("foo", payload)
	}

	// We should not have more than MaxMsgs
	cs := channelsGet(t, s.channels, "foo")
	if _, b := msgStoreState(t, cs.store.Msgs); b != uint64(sOpts.MaxBytes) {
		t.Fatalf("Expected msgs size to be %v, got %v", sOpts.MaxBytes, b)
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

func TestPerChannelLimits(t *testing.T) {
	var opts *Options
	f := func(idx int) {
		if idx == 0 {
			opts = GetDefaultOptions()
		} else {
			cleanupDatastore(t)
			defer cleanupDatastore(t)

			opts = getTestDefaultOptsForPersistentStore()
			if opts.StoreType == stores.TypeFile {
				stores.FileStoreTestSetBackgroundTaskInterval(15 * time.Millisecond)
				defer stores.FileStoreTestResetBackgroundTaskInterval()
			}
		}
		opts.MaxMsgs = 10
		opts.MaxAge = time.Hour
		clfoo := stores.ChannelLimits{}
		clfoo.MaxMsgs = 2
		clbar := stores.ChannelLimits{}
		clbar.MaxBytes = 1000
		clbaz := stores.ChannelLimits{}
		clbaz.MaxSubscriptions = 1
		clbaz.MaxAge = 15 * time.Millisecond
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
		time.Sleep(50 * time.Millisecond)
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
	for i := 0; i < 2; i++ {
		f(i)
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
