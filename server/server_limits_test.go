// Copyright 2016-2017 Apcera Inc. All rights reserved.
package server

import (
	"strings"
	"sync"
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
	check := func() {
		cs := channelsGet(t, s.channels, "foo")
		ss := cs.ss
		ss.RLock()
		if ss.psubs == nil || len(ss.psubs) != 1 {
			stackFatalf(t, "Expected only one subscription, got %v", len(ss.psubs))
		}
		ss.RUnlock()
		subs := s.clients.getSubs(clientName)
		if len(subs) != 1 {
			stackFatalf(t, "Expected 1 subscription for client %q, got %+v", clientName, subs)
		}
	}
	// We should get an error here
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err == nil {
		t.Fatal("Expected error on subscribe, go none")
	}
	check()
	// Try with a durable
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur")); err == nil {
		t.Fatal("Expected error on subscribe, go none")
	}
	check()
	// And a queue sub
	if _, err := sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}); err == nil {
		t.Fatal("Expected error on subscribe, go none")
	}
	check()
	// Finally a durable queue sub
	if _, err := sc.QueueSubscribe("foo", "queue", func(_ *stan.Msg) {}, stan.DurableName("dur")); err == nil {
		t.Fatal("Expected error on subscribe, go none")
	}
	check()
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

func TestPerChannelLimitsSetToUnlimitedPrintedCorrectly(t *testing.T) {
	opts := GetDefaultOptions()
	opts.MaxSubscriptions = 10
	opts.MaxMsgs = 10
	opts.MaxBytes = 64 * 1024
	opts.MaxAge = time.Hour

	clfoo := stores.ChannelLimits{}
	clfoo.MaxSubscriptions = -1
	clfoo.MaxMsgs = -1
	clfoo.MaxBytes = -1
	clfoo.MaxAge = -1

	sl := &opts.StoreLimits
	sl.AddPerChannel("foo", &clfoo)

	l := &captureNoticesLogger{}
	opts.EnableLogging = true
	opts.CustomLogger = l

	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	var notices []string
	l.Lock()
	for _, line := range l.notices {
		if strings.Contains(line, "-1") {
			notices = l.notices
			break
		}
	}
	l.Unlock()
	if notices != nil {
		t.Fatalf("There should not be -1 values, got %v", notices)
	}
}

func TestMaxInactivity(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	opts.MaxInactivity = 100 * time.Millisecond
	fooStarLimits := &stores.ChannelLimits{MaxInactivity: 500 * time.Millisecond}
	opts.AddPerChannel("foo.*", fooStarLimits)
	opts.AddPerChannel("foo.bar", &stores.ChannelLimits{MaxInactivity: -1})
	recoveredLimits := &stores.ChannelLimits{MaxInactivity: 800 * time.Millisecond}
	opts.AddPerChannel("recovered", recoveredLimits)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	wg := sync.WaitGroup{}
	// If a subscription exists, channel should not be removed.
	checkChannelNotDeletedDueToSub := func(channel, queue, dur string) {
		defer wg.Done()
		var (
			sub stan.Subscription
			err error
		)
		if dur != "" {
			sub, err = sc.QueueSubscribe(channel, queue, func(_ *stan.Msg) {}, stan.DurableName(dur))
		} else {
			sub, err = sc.QueueSubscribe(channel, queue, func(_ *stan.Msg) {})
		}
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		defer sub.Unsubscribe()
		time.Sleep(opts.MaxInactivity + 50*time.Millisecond)
		verifyChannelExist(t, s, channel, true, time.Second)
	}
	wg.Add(4)
	go checkChannelNotDeletedDueToSub("c1", "", "")
	go checkChannelNotDeletedDueToSub("c2", "queue", "")
	go checkChannelNotDeletedDueToSub("c3", "", "dur")
	go checkChannelNotDeletedDueToSub("c4", "queue", "dur")
	wg.Wait()
	// Since last subscription is closed, wait more than
	// MaxInactivity, all those channels should have been deleted
	time.Sleep(opts.MaxInactivity + 50*time.Millisecond)
	verifyChannelExist(t, s, "c1", false, time.Second)
	verifyChannelExist(t, s, "c2", false, time.Second)
	verifyChannelExist(t, s, "c3", false, time.Second)
	verifyChannelExist(t, s, "c4", false, time.Second)

	// Keep sending to keep a channel alive.
	ch := make(chan struct{}, 1)
	wg = sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.After(50 * time.Millisecond):
				sc.Publish("c5", []byte("hello"))
			case <-ch:
				return
			}
		}
	}()
	// Check that channel is not removed
	time.Sleep(2 * opts.MaxInactivity)
	verifyChannelExist(t, s, "c5", true, time.Second)
	ch <- struct{}{}
	wg.Wait()
	// wait more... and it should be deleted
	verifyChannelExist(t, s, "c5", false, 2*opts.MaxInactivity)

	// Verify that offline durables do not prevent removal.
	dur, err := sc.Subscribe("c6", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Close durable
	dur.Close()
	time.Sleep(2 * opts.MaxInactivity)
	verifyChannelExist(t, s, "c6", false, time.Second)

	qdur, err := sc.QueueSubscribe("c7", "queue", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Close durable
	qdur.Close()
	time.Sleep(2 * opts.MaxInactivity)
	verifyChannelExist(t, s, "c7", false, time.Second)

	// Check that last activity bumps the next check
	if err := sc.Publish("c8", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Wait half MaxInactivity
	time.Sleep(opts.MaxInactivity / 2)
	// Publish another
	if err := sc.Publish("c8", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	start := time.Now()
	// Sleep the other half (and a bit more)
	time.Sleep((opts.MaxInactivity / 2) + 15*time.Millisecond)
	// Channel should still exist
	verifyChannelExist(t, s, "c8", true, time.Second)
	// After at least MaxInflight after the second send the channel should
	// have disappeared
	verifyChannelExist(t, s, "c8", false, opts.MaxInactivity-time.Since(start)+50*time.Millisecond)

	// Create store foo.baz, it should be deleted after 500ms
	if err := sc.Publish("foo.baz", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	time.Sleep(fooStarLimits.MaxInactivity / 2)
	verifyChannelExist(t, s, "foo.baz", true, 2*time.Second)
	// Wait more than 500ms (total), channel should be gone
	verifyChannelExist(t, s, "foo.baz", false, fooStarLimits.MaxInactivity*2/3)

	// Channel foo.bar should override the MaxInactivity to
	// unlimited, so channel is not expected to be deleted.
	if err := sc.Publish("foo.bar", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Wait for more than 500ms
	time.Sleep(fooStarLimits.MaxInactivity + 100*time.Millisecond)
	verifyChannelExist(t, s, "foo.bar", true, 2*time.Second)

	// Check that if a channel with higher MaxInactivity is added
	// first, then one with lower MaxInactivity is added next,
	// then it will be deleted at appropriate time
	if err := sc.Publish("foo.baz", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := sc.Publish("c9", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	time.Sleep(2 * opts.MaxInactivity)
	verifyChannelExist(t, s, "c9", false, time.Second)
	verifyChannelExist(t, s, "foo.baz", true, time.Second)
	verifyChannelExist(t, s, "foo.baz", false, fooStarLimits.MaxInactivity+100*time.Millisecond)

	// Check removal of a channel not first in the list
	if err := sc.Publish("foo.one", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := sc.Publish("foo.two", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	sub, err := sc.Subscribe("foo.two", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	s.channels.Lock()
	ok := len(s.channels.delWatchList) == 1 && s.channels.delWatchList[0].name == "foo.one"
	s.channels.Unlock()
	if !ok {
		t.Fatal("Delete channel watch list should contain only foo.one")
	}
	sub.Unsubscribe()

	// Send a message on a new channel and restart server
	if err := sc.Publish("recovered", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Before shutdown, verify foo.bar still exists
	verifyChannelExist(t, s, "foo.bar", true, 2*time.Second)
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	// Check channels exist
	verifyChannelExist(t, s, "recovered", true, time.Second)
	verifyChannelExist(t, s, "foo.bar", true, 2*time.Second)
	// But without any activity, it should go away
	verifyChannelExist(t, s, "recovered", false, recoveredLimits.MaxInactivity+50*time.Millisecond)
	// This one should always be there
	verifyChannelExist(t, s, "foo.bar", true, 2*time.Second)
	sc.Close()
}
