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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
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
	if testUseEncryption {
		t.SkipNow()
	}
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

func checkChannelActivity(t tLogger, s *StanServer, name string, expectedToExist bool, created time.Time, maxInactivity time.Duration) {
	if expectedToExist {
		// Check that we are not invoked too late
		if time.Since(created) >= maxInactivity {
			// assume ok, return
			return
		}
		if s.channels.get(name) == nil {
			stackFatalf(t, "Channel %q was expected to exist", name)
		}
		return
	}
	// Here, we wait for the channel to be removed.
	// We will wait up to the channel max inactivity before reporting failure
	timeout := time.Now().Add(maxInactivity + 60*time.Millisecond)
	for time.Now().Before(timeout) {
		// If channel is no longer there, we are good.
		if s.channels.get(name) == nil {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	stackFatalf(t, "Channel %q was not expected to exist", name)
}

func TestMaxInactivity(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForPersistentStore()
	opts.NATSServerURL = "nats://127.0.0.1:4222"
	opts.MaxInactivity = 100 * time.Millisecond
	fooStarLimits := &stores.ChannelLimits{MaxInactivity: 500 * time.Millisecond}
	opts.AddPerChannel("foo.*", fooStarLimits)
	opts.AddPerChannel("foo.bar", &stores.ChannelLimits{MaxInactivity: -1})
	recoveredLimits := &stores.ChannelLimits{MaxInactivity: 800 * time.Millisecond}
	opts.AddPerChannel("recovered", recoveredLimits)
	pubLimits := &stores.ChannelLimits{MaxInactivity: 1000 * time.Millisecond}
	opts.AddPerChannel("pub", pubLimits)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	wg := sync.WaitGroup{}
	errCh := make(chan error, 1)
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
			select {
			case errCh <- fmt.Errorf("Error on subscribe: %v", err):
				return
			default:
			}
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
	select {
	case e := <-errCh:
		t.Fatal(e.Error())
	default:
	}
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
	if err := sc.Publish("pub", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	c := channelsGet(t, s.channels, "pub")
	created := c.activity.last
	// Wait half MaxInactivity
	time.Sleep(pubLimits.MaxInactivity / 2)
	// Publish another
	if err := sc.Publish("pub", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Sleep the other half (and a bit more)
	time.Sleep((pubLimits.MaxInactivity / 2) + 50*time.Millisecond)
	// Channel should still exist
	checkChannelActivity(t, s, "pub", true, created, pubLimits.MaxInactivity)
	// Wait for at least MaxInactivity since last publish and now channel
	// should be gone.
	checkChannelActivity(t, s, "pub", false, created, pubLimits.MaxInactivity)

	// Create store foo.baz, it should be deleted after 500ms
	if err := sc.Publish("foo.baz", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	c = s.channels.get("foo.baz")
	if c != nil {
		created = c.activity.last
		time.Sleep(fooStarLimits.MaxInactivity / 2)
		checkChannelActivity(t, s, "foo.baz", true, created, fooStarLimits.MaxInactivity)
		// Wait more than 500ms (total), channel should be gone
		checkChannelActivity(t, s, "foo.baz", false, created, fooStarLimits.MaxInactivity)
	}

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
	c = s.channels.get("foo.baz")
	if c != nil {
		c = channelsGet(t, s.channels, "foo.baz")
		created = c.activity.last
		if err := sc.Publish("c9", []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		time.Sleep(2 * opts.MaxInactivity)
		verifyChannelExist(t, s, "c9", false, time.Second)
		checkChannelActivity(t, s, "foo.baz", true, created, fooStarLimits.MaxInactivity)
		checkChannelActivity(t, s, "foo.baz", false, created, fooStarLimits.MaxInactivity)
	}

	// Send a message on a new channel and restart server
	if err := sc.Publish("recovered", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Before shutdown, verify foo.bar still exists
	verifyChannelExist(t, s, "foo.bar", true, 2*time.Second)
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	c = s.channels.get("recovered")
	if c != nil {
		created = c.activity.last
		// Check channels exist
		checkChannelActivity(t, s, "recovered", true, created, recoveredLimits.MaxInactivity)
		verifyChannelExist(t, s, "foo.bar", true, 2*time.Second)
		// But without any activity, it should go away
		checkChannelActivity(t, s, "recovered", false, created, recoveredLimits.MaxInactivity)
		// This one should always be there
		verifyChannelExist(t, s, "foo.bar", true, 2*time.Second)
	}

	// Send to a new channel
	if err := sc.Publish("c10", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	// Block the ioLoop
	ch1, ch2 := s.sendSynchronziationRequest()
	select {
	case <-ch1:
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get notification from ioLoop")
	}
	// Wait for timer to fire and post delete channel request to ioLoop
	// which will be delayed since we did not release the ioLoop yet.
	time.Sleep(2 * opts.MaxInactivity)
	// Create a subscription
	sub, err := sc.Subscribe("c10", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Release ioLoop
	close(ch2)
	// Check that channel was not deleted
	verifyChannelExist(t, s, "c10", true, 2*opts.MaxInactivity)
	c = s.channels.get("c10")
	s.channels.RLock()
	tset := c.activity.timerSet
	s.channels.RUnlock()
	if tset {
		t.Fatalf("Timer should not be set")
	}
	sub.Unsubscribe()
	// Now should be removed
	verifyChannelExist(t, s, "c10", false, 2*opts.MaxInactivity)

	sc.Close()
}

func TestMaxInactivityOnConnectionClose(t *testing.T) {
	o := GetDefaultOptions()
	o.MaxInactivity = 250 * time.Millisecond
	o.ClientHBInterval = 100 * time.Millisecond
	o.ClientHBTimeout = 50 * time.Millisecond
	o.ClientHBFailCount = 2
	s := runServerWithOpts(t, o, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if s.channels.get("foo") == nil {
		t.Fatalf("Channel should exit")
	}
	// Close connection without closing subscription
	sc.Close()
	// Wait for server to remove...
	waitForNumClients(t, s, 0)
	// Wait for channel to be removed
	waitFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if s.channels.get("foo") != nil {
			return fmt.Errorf("Channel should have been removed")
		}
		return nil
	})
	sc.Close()

	sc = NewDefaultConnection(t)
	defer sc.Close()
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if s.channels.get("foo") == nil {
		t.Fatalf("Channel should exit")
	}
	// Close NATS connection to cause server to remove it
	sc.NatsConn().Close()
	// Wait for server to remove...
	waitForNumClients(t, s, 0)
	// Wait for channel to be removed
	waitFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if s.channels.get("foo") != nil {
			return fmt.Errorf("Channel should have been removed")
		}
		return nil
	})
}
