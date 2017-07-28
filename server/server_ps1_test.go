// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

var (
	benchStoreType      = stores.TypeMemory
	persistentStoreType = stores.TypeFile
)

func TestMain(m *testing.M) {
	var (
		bst string
		pst string
	)
	flag.StringVar(&bst, "bench_store", "", "store type for bench tests (mem, file)")
	flag.StringVar(&pst, "persistent_store", "", "store type for server recovery related tests (file)")
	flag.Parse()
	bst = strings.ToLower(bst)
	pst = strings.ToLower(pst)
	// Will add DB store and others when avail
	switch bst {
	case "":
		// use default
	case "mem":
		benchStoreType = stores.TypeMemory
	case "file":
		benchStoreType = stores.TypeFile
	default:
		fmt.Printf("Unknown store %q for bench tests\n", bst)
		os.Exit(2)
	}
	// Will add DB store and others when avail
	switch pst {
	case "":
		// use default
	default:
		fmt.Printf("Unknown or unsupported store %q for persistent store server tests\n", pst)
		os.Exit(2)
	}
	os.Exit(m.Run())
}

func cleanupDatastore(t *testing.T, dir string) {
	if persistentStoreType == stores.TypeFile {
		if err := os.RemoveAll(dir); err != nil {
			stackFatalf(t, "Error cleaning up datastore: %v", err)
		}
	}
}

func getTestDefaultOptsForPersistentStore() *Options {
	opts := GetDefaultOptions()
	opts.StoreType = persistentStoreType
	if persistentStoreType == stores.TypeFile {
		opts.FilestoreDir = defaultDataStore
		opts.FileStoreOpts.BufferSize = 1024
	}
	return opts
}

func testStalledRedelivery(t *testing.T, typeSub string) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		numSubs = 2
		toSend = 2
	} else if typeSub == "durable" {
		// Create a durable with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur"),
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	} else {
		// Create a sub with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.Subscribe("foo", cb, stan.SetManualAckMode(),
			stan.MaxInflight(1), stan.AckWait(time.Second),
			stan.AckWait(time.Second)); err != nil {
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

func TestPersistentStoreRunServer(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	// Create our own NATS connection to control reconnect wait
	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(50*time.Millisecond), nats.MaxReconnects(500))
	defer nc.Close()
	defer sc.Close()

	rch := make(chan bool)
	delivered := int32(0)
	redelivered := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			atomic.AddInt32(&redelivered, 1)
		} else {
			if atomic.AddInt32(&delivered, 1) == 3 {
				rch <- true
			}
		}
	}

	// 2 Queue subscribers on bar
	if _, err := sc.QueueSubscribe("bar", "group", cb); err != nil {
		t.Fatalf("Unexpected error on queue subscribe: %v", err)
	}
	if _, err := sc.QueueSubscribe("bar", "group", cb); err != nil {
		t.Fatalf("Unexpected error on queue subscribe: %v", err)
	}
	// 1 Durable on baz
	if _, err := sc.Subscribe("baz", cb, stan.DurableName("mydur")); err != nil {
		t.Fatalf("Unexpected error on durable subscribe: %v", err)
	}
	// 1 Plain subscriber on foo
	if _, err := sc.Subscribe("foo", cb); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Wait for all subscriptions to be processed by the server
	waitForNumSubs(t, s, clientName, 4)

	// Publish some messages.
	if err := sc.Publish("bar", []byte("Msg for bar")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("baz", []byte("Msg for baz")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("foo", []byte("Msg for foo")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for all 3 messages
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our messages")
	}
	// There should be no redelivered message
	if r := atomic.LoadInt32(&redelivered); r != 0 {
		t.Fatalf("There should be no redelivered message, got %v", r)
	}

	// Wait a bit for the acks to be processed
	time.Sleep(50 * time.Millisecond)

	// Shutdown server
	s.Shutdown()

	// Reset delivered count
	atomic.StoreInt32(&delivered, 0)

	// Recover
	s = runServerWithOpts(t, opts, nil)

	// Check server recovered state
	// Should be 1 client
	checkClients(t, s, 1)

	// Should be 4 subscriptions
	checkSubs(t, s, clientName, 4)

	// helper to check that there is no ack pending
	checkNoAckPending := func(sub *subState) {
		sub.RLock()
		lap := len(sub.acksPending)
		sub.RUnlock()
		if lap != 0 {
			t.Fatalf("Server shutdown too soon? Unexpected un-ack'ed messages: %v", lap)
		}
	}

	// Check details now.
	// 2 Queue subscribers on bar
	cs := channelsGet(t, s.channels, "bar")
	func() {
		ss := cs.ss
		ss.RLock()
		defer ss.RUnlock()
		if len(ss.durables) != 0 {
			t.Fatalf("Unexpected durables for bar: %v", len(ss.durables))
		}
		if len(ss.psubs) != 0 {
			t.Fatalf("Unexpected plain subscribers for bar: %v", len(ss.psubs))
		}
		if len(ss.qsubs) != 1 {
			t.Fatalf("Expected one queue group for bar, got: %v", len(ss.qsubs))
		}
		qs := ss.qsubs["group"]
		if qs == nil {
			t.Fatal("Expected to get a queue state")
		}
		qs.RLock()
		qsubs := qs.subs
		qs.RUnlock()
		if qsubs == nil || len(qsubs) != 2 {
			t.Fatalf("Unexpected number of queue subscribers of group 'group' for channel bar, got: %v", len(qsubs))
		}
		// Check for the two queue subscribers
		for _, sub := range qsubs {
			checkNoAckPending(sub)
		}
	}()

	// One durable on baz
	cs = channelsGet(t, s.channels, "baz")
	func() {
		ss := cs.ss
		ss.RLock()
		defer ss.RUnlock()
		if len(ss.durables) != 1 {
			t.Fatalf("Expected one durable for baz: %v", len(ss.durables))
		}
		// Durables are both in plain subs and durables
		if len(ss.psubs) != 1 {
			t.Fatalf("Unexpected plain subscribers for baz: %v", len(ss.psubs))
		}
		if len(ss.qsubs) != 0 {
			t.Fatalf("Unexpected queue groups for baz, got: %v", len(ss.qsubs))
		}
		checkNoAckPending(ss.psubs[0])
	}()

	// One plain subscriber on foo
	cs = channelsGet(t, s.channels, "foo")
	func() {
		ss := cs.ss
		ss.RLock()
		defer ss.RUnlock()
		if len(ss.durables) != 0 {
			t.Fatalf("Unexpected durables for foo: %v", len(ss.durables))
		}
		if len(ss.psubs) != 1 {
			t.Fatalf("Expected 1 plain subscriber for foo: %v", len(ss.psubs))
		}
		if len(ss.qsubs) != 0 {
			t.Fatalf("Unexpected queue subscribers for foo, got: %v", len(ss.qsubs))
		}
		checkNoAckPending(ss.psubs[0])
	}()

	// Since we use the same connection to send new messages,
	// we don't have to explicitly wait that the client has
	// reconnected (sends are buffered and flushed on reconnect)

	// Send new messages, should be received.
	if err := sc.Publish("bar", []byte("New Msg for bar")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("baz", []byte("New Msg for baz")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("foo", []byte("New Msg for foo")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for the new messages
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our messages")
	}
	// There should be no redelivered message
	if r := atomic.LoadInt32(&redelivered); r != 0 {
		t.Fatalf("There should be no redelivered message, got %v", r)
	}
}

func TestPersistentStoreRecoveredDurableCanReconnect(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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

func TestPersistentStoreIgnoreRecoveredSubForUnknownClientID(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
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

func TestPersistentStoreMissingDirectory(t *testing.T) {
	if persistentStoreType != stores.TypeFile {
		t.SkipNow()
	}
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = ""

	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestPersistentStoreChangedClusterID(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	s.Shutdown()

	// Change cluster ID, running the server should fail with a panic
	opts.ID = "differentID"
	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestPersistentStoreRedeliveredPerSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
		stan.SetManualAckMode(), stan.AckWait(time.Second))
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

func TestPersistentStoreDurableCanReceiveAfterRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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

func TestPersistentStoreCheckClientHealthAfterRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	// Create 2 clients
	sc1, nc1 := createConnectionWithNatsOpts(t, "c1",
		nats.ReconnectWait(10*time.Second))
	defer nc1.Close()
	defer sc1.Close()

	sc2, nc2 := createConnectionWithNatsOpts(t, "c2",
		nats.ReconnectWait(10*time.Second))
	defer nc2.Close()
	defer sc2.Close()

	// Make sure they are registered
	waitForNumClients(t, s, 2)
	// Restart
	s.Shutdown()
	// Change server's hb settings
	opts.ClientHBInterval = 100 * time.Millisecond
	opts.ClientHBTimeout = 10 * time.Millisecond
	opts.ClientHBFailCount = 2
	s = runServerWithOpts(t, opts, nil)
	// Check that there are 2 clients
	checkClients(t, s, 2)
	// Tweak their hbTimer interval to make the test short
	clients := s.clients.getClients()
	for cID, c := range clients {
		c.Lock()
		if c.hbt == nil {
			c.Unlock()
			t.Fatalf("HeartBeat Timer of client %q should have been set", cID)
		}
		c.Unlock()
	}
	// Both clients should quickly timed-out
	waitForNumClients(t, s, 0)
}

func TestPersistentStoreRedeliveryCbPerSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
		stan.SetManualAckMode(), stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if sub2, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(), stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

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
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Create two queue subscribers with manual ackMode that will
	// not ack the message.
	if _, err := sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(time.Second),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	sub2, err = sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(10*time.Second),
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
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	var err error
	var sub2 stan.Subscription

	errs := make(chan error, 10)
	sub2Recv := make(chan bool)
	redelivered := int32(0)
	trackDelivered := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			if m.Sub != sub2 {
				errs <- fmt.Errorf("Expected redelivered msg to be sent to sub2")
				return
			}
			if atomic.AddInt32(&redelivered, 1) != 1 {
				errs <- fmt.Errorf("Message redelivered after restart")
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
	if _, err := sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(time.Second),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create this subscriber that will receive and ack the message
	sub2, err = sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(time.Second))
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
	case <-time.After(1500 * time.Millisecond):
		break
	}
}

func TestPersistentStoreAutomaticDeliveryOnRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	toSend := int32(10)
	msg := []byte("msg")

	// Get our STAN connection
	sc := NewDefaultConnection(t)

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
	s = nil
	s2 := runServerWithOpts(t, opts, nil)
	// defer s.Shutdown()

	// Release 	the consumer
	close(blocked)
	// Wait for messages to be delivered
	if err := Wait(done); err != nil {
		t.Fatal("Messages were not automatically delivered")
	}
	sc.Close()
	s2.Shutdown()
	s2 = nil
}

func TestPersistentStoreDontSendToOfflineDurablesOnRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Run a standalone NATS Server
	gs := natsdTest.RunServer(nil)
	defer gs.Shutdown()

	opts := getTestDefaultOptsForPersistentStore()
	opts.NATSServerURL = nats.DefaultURL
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	durName := "mydur"
	// Create a durable with manual ack mode (don't ack the message)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {},
		stan.DurableName(durName),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode()); err != nil {
		stackFatalf(t, "Unexpected error on subscribe: %v", err)
	}
	// Make sure durable is created
	subs := checkSubs(t, s, clientName, 1)
	if len(subs) != 1 {
		stackFatalf(t, "Should be only 1 durable, got %v", len(subs))
	}
	dur := subs[0]
	dur.RLock()
	inbox := dur.Inbox
	dur.RUnlock()

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

	failCh := make(chan bool, 10)
	// Setup a consumer on the durable inbox
	sub, err := nc.Subscribe(inbox, func(_ *nats.Msg) {
		failCh <- true
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Stop the Streaming server
	s.Shutdown()
	// Restart the Streaming server
	s = runServerWithOpts(t, opts, nil)

	// We should not get any message, if we do, this is an error
	if err := WaitTime(failCh, time.Second); err == nil {
		t.Fatal("Consumer got a message")
	}
}

func TestPersistentStoreNoPanicOnShutdown(t *testing.T) {
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForPersistentStore()
	opts.NATSServerURL = nats.DefaultURL

	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	// Start a go routine that keeps sending messages
	sendQuit := make(chan bool)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		sc, nc := createConnectionWithNatsOpts(t, clientName, nats.NoReconnect())
		defer sc.Close()
		defer nc.Close()

		payload := []byte("hello")
		for {
			select {
			case <-sendQuit:
				return
			default:
				sc.PublishAsync("foo", payload, nil)
			}
		}
	}()
	// Wait for some messages to have been sent
	time.Sleep(100 * time.Millisecond)
	// Shutdown the server, it should not panic
	s.Shutdown()
	// Stop and wait for go routine to end
	sendQuit <- true
	wg.Wait()
}

func TestPersistentStoreNonDurableRemovedFromStoreOnConnClose(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
	if persistentStoreType == stores.TypeFile {
		store, err = stores.NewFileStore(testLogger, defaultDataStore, &limits)
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

func TestPersistentStoreQueueSubLeavingUpdateQGroupLastSent(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
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
	case <-time.After(time.Second):
		// Wait for a bit to see if we receive extraneous messages
		break
	}
	if !gotIt {
		t.Fatal("Did not get message 3")
	}
}

func TestPersistentStoreDurableQueueSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

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

func TestPersistentStoreDurableQueueSubRedeliveryOnRejoin(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
		stan.AckWait(time.Second),
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

func TestPersistentStoreQMemberRemovedFromStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
	cs, _ := s.lookupOrCreateChannel("foo")
	ss := cs.ss
	s.mu.RUnlock()
	ss.RLock()
	qs := ss.qsubs["dur:group"]
	ss.RUnlock()
	if len(qs.subs) != 1 {
		t.Fatalf("Expected only 1 member, got %v", len(qs.subs))
	}
}

func TestPersistentStoreAcksPool(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	opts.AckSubsPoolSize = 5
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	var allSubs []stan.Subscription

	// Check server's ackSub pool
	checkPoolSize := func() {
		s.mu.RLock()
		poolSize := len(s.acksSubs)
		s.mu.RUnlock()
		if poolSize != opts.AckSubsPoolSize {
			stackFatalf(t, "Expected acksSubs pool size to be %v, got %v", opts.AckSubsPoolSize, poolSize)
		}
	}
	checkPoolSize()

	// Check total subs and each sub's ackSub is pooled or not as expected.
	checkAckSubs := func(total int32, checkSubFunc func(sub *subState) error) {
		subs := s.clients.getSubs(clientName)
		if len(subs) != int(total) {
			stackFatalf(t, "Expected %d subs, got %v", total, len(subs))
		}
		for _, sub := range subs {
			sub.RLock()
			err := checkSubFunc(sub)
			sub.RUnlock()
			if err != nil {
				stackFatalf(t, err.Error())
			}
		}
	}

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	totalSubs := int32(10)
	errCh := make(chan error, totalSubs)
	ch := make(chan bool)
	count := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			errCh <- fmt.Errorf("Unexpected redelivered message: %v", m)
		} else if atomic.AddInt32(&count, 1) == atomic.LoadInt32(&totalSubs) {
			ch <- true
		}
	}
	// Create 10 subs
	for i := 0; i < int(totalSubs); i++ {
		sub, err := sc.Subscribe("foo", cb, stan.AckWait(time.Second))
		if err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		allSubs = append(allSubs, sub)
	}
	checkReceived := func(subs int32) {
		atomic.StoreInt32(&count, 0)
		atomic.StoreInt32(&totalSubs, subs)
		// Send 1 message
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			stackFatalf(t, "Unexpected error on publish: %v", err)
		}
		// Wait for all messages to be received
		if err := Wait(ch); err != nil {
			stackFatalf(t, "Did not get our messages")
		}
		// Wait for more than redelivery time
		time.Sleep(1500 * time.Millisecond)
		// Check that there was no error
		select {
		case e := <-errCh:
			stackFatalf(t, e.Error())
		default:
		}
	}
	checkReceived(totalSubs)
	// Check that server's subs have nil ackSub
	checkAckSubs(totalSubs, func(sub *subState) error {
		if sub.ackSub != nil {
			return fmt.Errorf("Expected ackSub for sub %v to be nil, was not", sub.ID)
		}
		return nil
	})
	// Stop server and restart with lower pool size
	s.Shutdown()
	opts.AckSubsPoolSize = 2
	s = runServerWithOpts(t, opts, nil)
	// Check server's ackSub pool
	checkPoolSize()
	// Check that AckInbox with ackSubIndex > AcksPoolSize-1 have
	// individual ackSub
	checkAckSubs(totalSubs, func(sub *subState) error {
		wantsNil := false
		ackSubIndexEnd := strings.Index(sub.AckInbox, ".")
		if n, _ := strconv.Atoi(sub.AckInbox[:ackSubIndexEnd]); n <= 1 {
			wantsNil = true
		}
		if wantsNil && sub.ackSub != nil {
			return fmt.Errorf("Expected ackSub for sub %v to be nil, was not", sub.ID)
		} else if !wantsNil && sub.ackSub == nil {
			return fmt.Errorf("Expected ackSub for sub %v to be not nil, was nil", sub.ID)
		}
		return nil
	})
	checkReceived(totalSubs)
	// Restart server with no acksSub pool
	s.Shutdown()
	opts.AckSubsPoolSize = 0
	s = runServerWithOpts(t, opts, nil)
	// Check server's ackSub pool
	checkPoolSize()
	// Check that all subs have an individual ackSub
	checkAckSubs(totalSubs, func(sub *subState) error {
		if sub.ackSub == nil {
			return fmt.Errorf("Expected ackSub for sub %v to be not nil, was nil", sub.ID)
		}
		return nil
	})
	checkReceived(totalSubs)
	// Add another subscriber
	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	allSubs = append(allSubs, sub)
	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Check that the new sub's AckInbox is _INBOX as usual.
	checkAckSubs(totalSubs+1, func(sub *subState) error {
		if int(sub.ID) > int(totalSubs) {
			if sub.AckInbox[:len(nats.InboxPrefix)] != nats.InboxPrefix {
				return fmt.Errorf("Unexpected AckInbox: %v", sub)
			}
		}
		if sub.ackSub == nil {
			return fmt.Errorf("Expected ackSub for sub %v to be not nil, was nil", sub.ID)
		}
		return nil
	})
	// Restart server with ackPool
	s.Shutdown()
	opts.AckSubsPoolSize = 2
	s = runServerWithOpts(t, opts, nil)
	// Check server's ackSub pool
	checkPoolSize()
	// Check that unsubscribe work ok
	for _, sub := range allSubs {
		if err := sub.Unsubscribe(); err != nil {
			t.Fatalf("Error on unsubscribe: %v", err)
		}
	}
	// Create a subscription without call unsubscribe and make
	// sure it does not prevent closing of connection.
	if _, err := sc.Subscribe("foo", cb, stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	checkReceived(1)
	// Close the client connection
	sc.Close()
	nc.Close()
	// Make sure connection close is correctly processed.
	waitForNumClients(t, s, 0)
}

func TestPersistentStoreMultipleShadowQSubs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	s.Shutdown()

	limits := stores.DefaultStoreLimits
	var (
		store stores.Store
		err   error
	)
	if persistentStoreType == stores.TypeFile {
		store, err = stores.NewFileStore(testLogger, defaultDataStore, &limits)
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
	scs, err := s.lookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Error looking up channel: %v", err)
	}
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

func TestPersistentStoreMonitorServerzAfterRestart(t *testing.T) {
	resetPreviousHTTPConnections()
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)
	opts := getTestDefaultOptsForPersistentStore()

	s := runMonitorServer(t, opts)
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.ReconnectWait(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()
	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	totalMsgs := 10
	msg := []byte("hello")
	for i := 0; i < totalMsgs; i++ {
		if err := sc.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	cs := channelsGet(t, s.channels, "foo").store
	_, totalBytes := msgStoreState(t, cs.Msgs)

	for i := 0; i < 2; i++ {
		resp, body := getBody(t, ServerPath, expectedJSON)
		defer resp.Body.Close()

		sz := Serverz{}
		if err := json.Unmarshal(body, &sz); err != nil {
			t.Fatalf("Got an error unmarshalling the body: %v", err)
		}
		if sz.ClusterID != s.ClusterID() {
			t.Fatalf("Expected ClusterID to be %v, got %v", s.ClusterID(), sz.ClusterID)
		}
		if sz.ServerID != s.serverID {
			t.Fatalf("Expected ServerID to be %v, got %v", s.serverID, sz.ServerID)
		}
		if sz.Now.IsZero() {
			t.Fatalf("Expected Now to be set, was not")
		}
		if sz.Start.IsZero() {
			t.Fatalf("Expected Start to be set, was not")
		}
		if sz.Uptime == "" {
			t.Fatalf("Expected Uptime to be set, was not")
		}
		if sz.Version != VERSION {
			t.Fatalf("Expected version to be %v, got %v", VERSION, sz.Version)
		}
		if sz.GoVersion != runtime.Version() {
			t.Fatalf("Expected GoVersion to be %v, got %v", runtime.Version(), sz.Version)
		}
		if sz.Clients != 1 {
			t.Fatalf("Expected 1 client, got %v", sz.Clients)
		}
		if sz.Channels != 1 {
			t.Fatalf("Expected 1 channel, got %v", sz.Channels)
		}
		if sz.Subscriptions != 1 {
			t.Fatalf("Expected 1 subscription, got %v", sz.Subscriptions)
		}
		if sz.TotalMsgs != totalMsgs {
			t.Fatalf("Expected %d messages, got %v", totalMsgs, sz.TotalMsgs)
		}
		if sz.TotalBytes != totalBytes {
			t.Fatalf("Expected %v bytes, got %v", totalBytes, sz.TotalBytes)
		}
		resp.Body.Close()

		// Restart server
		s.Shutdown()
		resetPreviousHTTPConnections()
		s = runMonitorServer(t, opts)
		defer s.Shutdown()
	}
	sc.Close()
	nc.Close()
	s.Shutdown()
}

func TestPersistentStoreMonitorz(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	s := runMonitorServer(t, opts)
	testMonitorStorez(t, s, stores.TypeFile)
}

func TestPersistentStorePerChannelLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForPersistentStore()
	testPerChannelLimits(t, opts)
}
