package stan

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats"
	"github.com/nats-io/stan/pb"

	natsd "github.com/nats-io/gnatsd/test"
)

// Dumb wait program to sync on callbacks, etc... Will timeout
func Wait(ch chan bool) error {
	return WaitTime(ch, 5*time.Second)
}

func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func TestNoNats(t *testing.T) {
	if _, err := Connect("someNonExistantServerID", "myTestClient"); err != nats.ErrNoServers {
		t.Fatalf("Expected NATS: No Servers err, got %v\n", err)
	}
}

func TestUnreachable(t *testing.T) {
	s := natsd.RunDefaultServer()
	defer s.Shutdown()

	// Non-Existant or Unreachable
	connectTime := 25 * time.Millisecond
	start := time.Now()
	if _, err := Connect("someNonExistantServerID", "myTestClient", ConnectWait(connectTime)); err != ErrConnectReqTimeout {
		t.Fatalf("Expected Unreachable err, got %v\n", err)
	}
	if delta := time.Since(start); delta < connectTime {
		t.Fatalf("Expected to wait at least %v, but only waited %v\n", connectTime, delta)
	}
}

const (
	clusterName = "my_test_cluster"
	clientName  = "me"
)

func TestBasicConnect(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()
	if sc, err := Connect(clusterName, clientName); err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	} else {
		sc.Close()
	}
}

func TestBasicPublish(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()
	sc, err := Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	defer sc.Close()
	if err := sc.Publish("foo", []byte("Hello World!")); err != nil {
		t.Fatalf("Expected no errors on publish, got %v\n", err)
	}
}

func TestBasicPublishAsync(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()
	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	ch := make(chan bool)
	var glock sync.Mutex
	var guid string
	acb := func(lguid string, err error) {
		glock.Lock()
		defer glock.Unlock()
		if lguid != guid {
			t.Fatalf("Expected a matching guid in ack callback, got %s vs %s\n", lguid, guid)
		}
		ch <- true
	}
	glock.Lock()
	guid, _ = sc.PublishAsync("foo", []byte("Hello World!"), acb)
	glock.Unlock()
	if guid == "" {
		t.Fatalf("Expected non-empty guid to be returned.")
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our ack callback")
	}
}

func TestTimeoutPublishAsync(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	ch := make(chan bool)
	var glock sync.Mutex
	var guid string
	acb := func(lguid string, err error) {
		glock.Lock()
		defer glock.Unlock()
		if lguid != guid {
			t.Fatalf("Expected a matching guid in ack callback, got %s vs %s\n", lguid, guid)
		}
		if err != ErrTimeout {
			t.Fatalf("Expected a timeout error")
		}
		ch <- true
	}
	// Kill the STAN server so we timeout.
	s.Shutdown()
	glock.Lock()
	guid, _ = sc.PublishAsync("foo", []byte("Hello World!"), acb)
	glock.Unlock()
	if guid == "" {
		t.Fatalf("Expected non-empty guid to be returned.")
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our ack callback with a timeout err")
	}
}

func TestBasicSubscription(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	sub, err := sc.Subscribe("foo", func(m *Msg) {})
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()
}

func TestBasicQueueSubscription(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	sub, err := sc.QueueSubscribe("foo", "bar", func(m *Msg) {})
	if err != nil {
		t.Fatalf("Expected no error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	// Test that we can not set durable status on queue subscribers.
	_, err = sc.QueueSubscribe("foo", "bar", func(m *Msg) {}, DurableName("durable-queue-sub"))
	if err == nil {
		t.Fatalf("Expected non-nil error on QueueSubscribe with DurableName\n")
	}
}

func TestBasicPubSub(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	toSend := int32(500)
	hw := []byte("Hello World")
	msgMap := make(map[uint64]struct{})

	sub, err := sc.Subscribe("foo", func(m *Msg) {
		if m.Subject != "foo" {
			t.Fatalf("Expected subject of 'foo', got '%s'\n", m.Subject)
		}
		if !bytes.Equal(m.Data, hw) {
			t.Fatalf("Wrong payload, got %q\n", m.Data)
		}
		// Make sure Seq and Timestamp are set
		if m.Sequence == 0 {
			t.Fatalf("Expected Sequence to be set\n")
		}
		if m.Timestamp == 0 {
			t.Fatalf("Expected timestamp to be set\n")
		}

		if _, ok := msgMap[m.Sequence]; ok {
			t.Fatalf("Detected duplicate for sequence: %d\n", m.Sequence)
		}
		msgMap[m.Sequence] = struct{}{}

		if nr := atomic.AddInt32(&received, 1); nr >= int32(toSend) {
			ch <- true
		}
	})
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	for i := int32(0); i < toSend; i++ {
		if err := sc.Publish("foo", hw); err != nil {
			t.Fatalf("Received error on publish: %v\n", err)
		}
	}
	if err := WaitTime(ch, 1*time.Second); err != nil {
		t.Fatal("Did not receive our messages")
	}
}

func TestBasicPubSubFlowControl(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	toSend := int32(500)
	hw := []byte("Hello World")

	sub, err := sc.Subscribe("foo", func(m *Msg) {
		if nr := atomic.AddInt32(&received, 1); nr >= int32(toSend) {
			ch <- true
		}
	}, MaxInflight(25))
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	for i := int32(0); i < toSend; i++ {
		if err := sc.Publish("foo", hw); err != nil {
			t.Fatalf("Received error on publish: %v\n", err)
		}
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our messages")
	}
}

func TestBasicPubQueueSub(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	toSend := int32(100)
	hw := []byte("Hello World")

	sub, err := sc.QueueSubscribe("foo", "bar", func(m *Msg) {
		if m.Subject != "foo" {
			t.Fatalf("Expected subject of 'foo', got '%s'\n", m.Subject)
		}
		if !bytes.Equal(m.Data, hw) {
			t.Fatalf("Wrong payload, got %q\n", m.Data)
		}
		// Make sure Seq and Timestamp are set
		if m.Sequence == 0 {
			t.Fatalf("Expected Sequence to be set\n")
		}
		if m.Timestamp == 0 {
			t.Fatalf("Expected timestamp to be set\n")
		}
		if nr := atomic.AddInt32(&received, 1); nr >= int32(toSend) {
			ch <- true
		}
	})
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	for i := int32(0); i < toSend; i++ {
		sc.Publish("foo", hw)
	}
	if err := WaitTime(ch, 1*time.Second); err != nil {
		t.Fatal("Did not receive our messages")
	}
}

func TestBasicPubSubWithReply(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	hw := []byte("Hello World")

	inbox := nats.NewInbox()

	sub, err := sc.Subscribe("foo", func(m *Msg) {
		if m.Subject != "foo" {
			t.Fatalf("Expected subject of 'foo', got '%s'\n", m.Subject)
		}
		if !bytes.Equal(m.Data, hw) {
			t.Fatalf("Wrong payload, got %q\n", m.Data)
		}
		if m.Reply != inbox {
			t.Fatalf("Expected reply subject of '%s', got '%s'\n", inbox, m.Reply)
		}
		ch <- true
	})
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	sc.PublishWithReply("foo", inbox, hw)

	if err := WaitTime(ch, 1*time.Second); err != nil {
		t.Fatal("Did not receive our messages")
	}
}

func TestAsyncPubSubWithReply(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	hw := []byte("Hello World")

	inbox := nats.NewInbox()

	sub, err := sc.Subscribe("foo", func(m *Msg) {
		if m.Subject != "foo" {
			t.Fatalf("Expected subject of 'foo', got '%s'\n", m.Subject)
		}
		if !bytes.Equal(m.Data, hw) {
			t.Fatalf("Wrong payload, got %q\n", m.Data)
		}
		if m.Reply != inbox {
			t.Fatalf("Expected reply subject of '%s', got '%s'\n", inbox, m.Reply)
		}
		ch <- true
	})
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	sc.PublishAsyncWithReply("foo", inbox, hw, nil)

	if err := WaitTime(ch, 1*time.Second); err != nil {
		t.Fatal("Did not receive our messages")
	}
}

func TestSubscriptionStartPositionLast(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	defer sc.Close()

	// Publish ten messages
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}

	ch := make(chan bool)
	received := int32(0)

	mcb := func(m *Msg) {
		atomic.AddInt32(&received, 1)
		if m.Sequence != 10 {
			t.Fatalf("Wrong sequence received: got %d vs. %d\n", m.Sequence, 10)
		}
		ch <- true
	}
	// Now subscribe and set start position to last received.
	sub, err := sc.Subscribe("foo", mcb, StartWithLastReceived())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	// Check for sub setup
	rsub := sub.(*subscription)
	if rsub.opts.StartAt != pb.StartPosition_LastReceived {
		t.Fatalf("Incorrect StartAt state: %s\n", rsub.opts.StartAt)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our message")
	}

	if received > int32(1) {
		t.Fatalf("Should have received only 1 message, but got %d\n", received)
	}
}

func TestSubscriptionStartAtSequence(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	// Publish ten messages
	for i := 1; i <= 10; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}

	// Check for illegal sequences
	_, err = sc.Subscribe("foo", nil, StartAtSequence(500))
	if err == nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	shouldReceive := int32(5)

	// Capture the messages that are delivered.
	savedMsgs := make([]*Msg, 0, 5)

	mcb := func(m *Msg) {
		savedMsgs = append(savedMsgs, m)
		if nr := atomic.AddInt32(&received, 1); nr >= int32(shouldReceive) {
			ch <- true
		}
	}

	// Now subscribe and set start position to #6, so should received 6-10.
	sub, err := sc.Subscribe("foo", mcb, StartAtSequence(6))
	if err != nil {
		t.Fatalf("Expected no error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	// Check for sub setup
	rsub := sub.(*subscription)
	if rsub.opts.StartAt != pb.StartPosition_SequenceStart {
		t.Fatalf("Incorrect StartAt state: %s\n", rsub.opts.StartAt)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our messages")
	}

	// Check we received them in order.
	for i, seq := 0, uint64(6); i < 5; i++ {
		m := savedMsgs[i]
		// Check Sequence
		if m.Sequence != seq {
			t.Fatalf("Expected seq: %d, got %d\n", seq, m.Sequence)
		}
		// Check payload
		dseq, _ := strconv.Atoi(string(m.Data))
		if dseq != int(seq) {
			t.Fatalf("Expected payload: %d, got %d\n", seq, dseq)
		}
		seq++
	}
}

func TestSubscriptionStartAtTime(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	// Publish first 5
	for i := 1; i <= 5; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}

	// Buffer each side so slow tests still work.
	time.Sleep(250 * time.Millisecond)
	startTime := time.Now()
	time.Sleep(250 * time.Millisecond)

	// Publish last 5
	for i := 6; i <= 10; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}

	// Check for illegal configuration
	_, err = sc.Subscribe("foo", nil, StartAtTime(time.Time{}))
	if err == nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	shouldReceive := int32(5)

	// Capture the messages that are delivered.
	savedMsgs := make([]*Msg, 0, 5)

	mcb := func(m *Msg) {
		savedMsgs = append(savedMsgs, m)
		if nr := atomic.AddInt32(&received, 1); nr >= int32(shouldReceive) {
			ch <- true
		}
	}

	// Now subscribe and set start position to #6, so should received 6-10.
	sub, err := sc.Subscribe("foo", mcb, StartAtTime(startTime))
	if err != nil {
		t.Fatalf("Expected no error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	// Check for sub setup
	rsub := sub.(*subscription)
	if rsub.opts.StartAt != pb.StartPosition_TimeDeltaStart {
		t.Fatalf("Incorrect StartAt state: %s\n", rsub.opts.StartAt)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our messages")
	}

	// Check we received them in order.
	for i, seq := 0, uint64(6); i < 5; i++ {
		m := savedMsgs[i]
		// Check time is always greater than startTime
		if m.Timestamp < startTime.UnixNano() {
			t.Fatalf("Expected all messages to have timestamp > startTime.")
		}
		// Check Sequence
		if m.Sequence != seq {
			t.Fatalf("Expected seq: %d, got %d\n", seq, m.Sequence)
		}
		// Check payload
		dseq, _ := strconv.Atoi(string(m.Data))
		if dseq != int(seq) {
			t.Fatalf("Expected payload: %d, got %d\n", seq, dseq)
		}
		seq++
	}

	// Now test Ago helper
	delta := time.Now().Sub(startTime)

	sub, err = sc.Subscribe("foo", mcb, StartAtTimeDelta(delta))
	if err != nil {
		t.Fatalf("Expected no error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our messages")
	}
}

func TestSubscriptionStartAtFirst(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	// Publish ten messages
	for i := 1; i <= 10; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}

	ch := make(chan bool)
	received := int32(0)
	shouldReceive := int32(10)

	// Capture the messages that are delivered.
	savedMsgs := make([]*Msg, 0, 10)

	mcb := func(m *Msg) {
		savedMsgs = append(savedMsgs, m)
		if nr := atomic.AddInt32(&received, 1); nr >= int32(shouldReceive) {
			ch <- true
		}
	}

	// Now subscribe and set start position to #6, so should received 6-10.
	sub, err := sc.Subscribe("foo", mcb, DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Expected no error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	// Check for sub setup
	rsub := sub.(*subscription)
	if rsub.opts.StartAt != pb.StartPosition_First {
		t.Fatalf("Incorrect StartAt state: %s\n", rsub.opts.StartAt)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our messages")
	}

	if received != shouldReceive {
		t.Fatalf("Expected %d msgs but received %d\n", shouldReceive, received)
	}

	// Check we received them in order.
	for i, seq := 0, uint64(1); i < 10; i++ {
		m := savedMsgs[i]
		// Check Sequence
		if m.Sequence != seq {
			t.Fatalf("Expected seq: %d, got %d\n", seq, m.Sequence)
		}
		// Check payload
		dseq, _ := strconv.Atoi(string(m.Data))
		if dseq != int(seq) {
			t.Fatalf("Expected payload: %d, got %d\n", seq, dseq)
		}
		seq++
	}
}

func TestUnsubscribe(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	// test nil
	var nsub *subscription
	err = nsub.Unsubscribe()
	if err == nil || err != ErrBadSubscription {
		t.Fatalf("Expected a bad subscription err, got %v\n", err)
	}

	// Create a valid one
	sc.Subscribe("foo", nil)

	// Now subscribe, but we will unsubscribe before sending any messages.
	sub, err := sc.Subscribe("foo", func(m *Msg) {
		t.Fatalf("Did not expect to receive any messages\n")
	})
	if err != nil {
		t.Fatalf("Expected no error on Subscribe, got %v\n", err)
	}
	// Create another valid one
	sc.Subscribe("foo", nil)

	// Unsubscribe middle one.
	err = sub.Unsubscribe()
	if err != nil {
		t.Fatalf("Expected no errors from unsubscribe: got %v\n", err)
	}
	// Do it again, should not dump, but should get error.
	err = sub.Unsubscribe()
	if err == nil || err != ErrBadSubscription {
		t.Fatalf("Expected a bad subscription err, got %v\n", err)
	}

	// Publish ten messages
	for i := 1; i <= 10; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}
}

func TestSubscribeShrink(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	nsubs := 1000
	subs := make([]Subscription, 0, nsubs)
	for i := 1; i <= nsubs; i++ {
		// Create a valid one
		sub, err := sc.Subscribe("foo", nil)
		if err != nil {
			t.Fatalf("Got an error on subscribe: %v\n", err)
		}
		subs = append(subs, sub)
	}
	// Now unsubsribe them all
	for _, sub := range subs {
		err := sub.Unsubscribe()
		if err != nil {
			t.Fatalf("Got an error on unsubscribe: %v\n", err)
		}
	}
}

func TestDupClientID(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	_, err = Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err == nil {
		t.Fatalf("Expected to get an error for duplicate clientID\n")
	}
}

func TestClose(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	sub, err := sc.Subscribe("foo", func(m *Msg) {
		t.Fatalf("Did not expect to receive any messages\n")
	})
	if err != nil {
		t.Fatalf("Expected no errors when subscribing, got %v\n", err)
	}

	err = sc.Close()
	if err != nil {
		t.Fatalf("Did not expect error on Close(), got %v\n", err)
	}

	for i := 0; i < 10; i++ {
		sc.Publish("foo", []byte("ok"))
	}

	if err := sc.Publish("foo", []byte("Hello World!")); err == nil || err != ErrConnectionClosed {
		t.Fatalf("Expected an ErrConnectionClosed error on publish to a closed connection, got %v\n", err)
	}

	if err := sub.Unsubscribe(); err == nil || err != ErrConnectionClosed {
		t.Fatalf("Expected an ErrConnectionClosed error on unsubscribe to a closed connection, got %v\n", err)
	}
}

func TestManualAck(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	toSend := int32(100)
	hw := []byte("Hello World")

	for i := int32(0); i < toSend; i++ {
		sc.PublishAsync("foo", hw, nil)
	}
	sc.Publish("foo", hw)

	fch := make(chan bool)

	// Test that we can't Ack if not in manual mode.
	sub, err := sc.Subscribe("foo", func(m *Msg) {
		if err := m.Ack(); err != ErrManualAck {
			t.Fatalf("Expected an error trying to ack an auto-ack subscription")
		}
		fch <- true
	}, DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}

	if err := Wait(fch); err != nil {
		t.Fatal("Did not receive our first message")
	}
	sub.Unsubscribe()

	ch := make(chan bool)
	sch := make(chan bool)
	received := int32(0)

	msgs := make([]*Msg, 0, 101)

	// Test we only receive MaxInflight if we do not ack
	sub, err = sc.Subscribe("foo", func(m *Msg) {
		msgs = append(msgs, m)
		if nr := atomic.AddInt32(&received, 1); nr == int32(10) {
			ch <- true
		} else if nr > 10 {
			m.Ack()
			if nr >= toSend+1 { // sync Publish +1
				sch <- true
			}
		}
	}, DeliverAllAvailable(), MaxInflight(10), SetManualAckMode())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive at least 10 messages")
	}
	// Wait a bit longer for other messages which would be an error.
	time.Sleep(50 * time.Millisecond)

	if nr := atomic.LoadInt32(&received); nr != 10 {
		t.Fatalf("Only expected to get 10 messages to match MaxInflight without Acks, got %d\n", nr)
	}

	// Now make sure we get the rest of them. So ack the ones we have so far.
	for _, m := range msgs {
		if err := m.Ack(); err != nil {
			t.Fatalf("Unexpected error on Ack: %v\n", err)
		}
	}
	if err := Wait(sch); err != nil {
		t.Fatal("Did not receive all our messages")
	}

	if nr := atomic.LoadInt32(&received); nr != toSend+1 {
		t.Fatalf("Did not receive correct number of messages: %d vs %d\n", nr, toSend+1)
	}
}

func TestRedelivery(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	toSend := int32(100)
	hw := []byte("Hello World")

	for i := int32(0); i < toSend; i++ {
		sc.Publish("foo", hw)
	}

	// Make sure we get an error on bad ackWait
	if _, err := sc.Subscribe("foo", nil, AckWait(20*time.Millisecond)); err == nil {
		t.Fatalf("Expected an error for back AckWait time under 1 second\n")
	}

	ch := make(chan bool)
	sch := make(chan bool)
	received := int32(0)

	ackRedeliverTime := 1 * time.Second

	sub, err := sc.Subscribe("foo", func(m *Msg) {
		if nr := atomic.AddInt32(&received, 1); nr == toSend {
			ch <- true
		} else if nr == 2*toSend {
			sch <- true
		}

	}, DeliverAllAvailable(), MaxInflight(100), AckWait(ackRedeliverTime), SetManualAckMode())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive first delivery of all messages")
	}
	if nr := atomic.LoadInt32(&received); nr != toSend {
		t.Fatalf("Expected to get 100 messages, got %d\n", nr)
	}
	if err := Wait(sch); err != nil {
		t.Fatal("Did not receive second re-delivery of all messages")
	}
	if nr := atomic.LoadInt32(&received); nr != 2*toSend {
		t.Fatalf("Expected to get 200 messages, got %d\n", nr)
	}
}

func TestDurableSubscriber(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	toSend := int32(100)
	hw := []byte("Hello World")

	// Capture the messages that are delivered.
	savedMsgs := make([]*Msg, 0, toSend)

	for i := int32(0); i < toSend; i++ {
		sc.Publish("foo", hw)
	}

	ch := make(chan bool)
	received := int32(0)

	_, err = sc.Subscribe("foo", func(m *Msg) {
		if nr := atomic.AddInt32(&received, 1); nr == 10 {
			sc.Close()
			ch <- true
		} else {
			savedMsgs = append(savedMsgs, m)
		}
	}, DeliverAllAvailable(), DurableName("durable-foo"))
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive first delivery of all messages")
	}
	// Reset in case we get more messages in the above callback
	ch = make(chan bool)

	if nr := atomic.LoadInt32(&received); nr != 10 {
		t.Fatalf("Expected to get only 10 messages, got %d\n", nr)
	}
	// This is auto-ack, so undo received for check.
	// Close will prevent ack from going out, so #10 will be redelivered
	atomic.AddInt32(&received, -1)

	// sc is closed here from above..

	// Recreate the connection
	sc, err = Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	defer sc.Close()

	// Create the same durable subscription.
	_, err = sc.Subscribe("foo", func(m *Msg) {
		savedMsgs = append(savedMsgs, m)
		if nr := atomic.AddInt32(&received, 1); nr == toSend {
			ch <- true
		}
	}, DeliverAllAvailable(), DurableName("durable-foo"))
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}

	// Check that durables can not be subscribed to again by same client.
	_, err = sc.Subscribe("foo", nil, DurableName("durable-foo"))
	if err == nil || err.Error() != ErrDupDurable.Error() {
		t.Fatalf("Expected ErrDupSubscription error, got %v\n", err)
	}

	// Check that durables with same name, but subscribed to differen subject are ok.
	_, err = sc.Subscribe("bar", nil, DurableName("durable-foo"))
	if err != nil {
		t.Fatalf("Expected no error, got %v\n", err)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive delivery of all messages")
	}

	if nr := atomic.LoadInt32(&received); nr != toSend {
		t.Fatalf("Expected to get %d messages, got %d\n", toSend, nr)
	}
	if len(savedMsgs) != int(toSend) {
		t.Fatalf("Expected len(savedMsgs) to be %d, got %d\n", toSend, len(savedMsgs))
	}
	// Check we received them in order
	for i, m := range savedMsgs {
		seqExpected := uint64(i + 1)
		if m.Sequence != seqExpected {
			t.Fatalf("Got wrong seq, expected %d, got %d\n", seqExpected, m.Sequence)
		}
	}
}

func TestPubMultiQueueSub(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	s1Received := int32(0)
	s2Received := int32(0)
	toSend := int32(1000)

	var s1, s2 Subscription

	msgMapLock := &sync.Mutex{}
	msgMap := make(map[uint64]struct{})

	mcb := func(m *Msg) {
		// Remember the message sequence.
		msgMapLock.Lock()
		if _, ok := msgMap[m.Sequence]; ok {
			t.Fatalf("Detected duplicate for sequence: %d\n", m.Sequence)
		}
		msgMap[m.Sequence] = struct{}{}
		msgMapLock.Unlock()
		// Track received for each receiver.
		if m.Sub == s1 {
			atomic.AddInt32(&s1Received, 1)
		} else if m.Sub == s2 {
			atomic.AddInt32(&s2Received, 1)
		} else {
			t.Fatalf("Received message on unknown subscription")
		}
		// Track total
		if nr := atomic.AddInt32(&received, 1); nr == int32(toSend) {
			ch <- true
		}
	}

	s1, err = sc.QueueSubscribe("foo", "bar", mcb)
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s1.Unsubscribe()

	s2, err = sc.QueueSubscribe("foo", "bar", mcb)
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s2.Unsubscribe()

	// Publish out the messages.
	for i := int32(0); i < toSend; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}
	if err := WaitTime(ch, 10*time.Second); err != nil {
		t.Fatal("Did not receive our messages")
	}

	if nr := atomic.LoadInt32(&received); nr != toSend {
		t.Fatalf("Did not receive correct number of messages: %d vs %d\n", nr, toSend)
	}

	s1r := atomic.LoadInt32(&s1Received)
	s2r := atomic.LoadInt32(&s2Received)

	v := uint(float32(toSend) * 0.25) // 25 percent
	expected := toSend / 2
	d1 := uint(math.Abs(float64(expected - s1r)))
	d2 := uint(math.Abs(float64(expected - s2r)))
	if d1 > v || d2 > v {
		t.Fatalf("Too much variance in totals: %d, %d > %d", d1, d2, v)
	}
}

func TestPubMultiQueueSubWithSlowSubscriber(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	s1Received := int32(0)
	s2Received := int32(0)
	toSend := int32(100)

	var s1, s2 Subscription

	msgMapLock := &sync.Mutex{}
	msgMap := make(map[uint64]struct{})

	mcb := func(m *Msg) {
		// Remember the message sequence.
		msgMapLock.Lock()
		if _, ok := msgMap[m.Sequence]; ok {
			t.Fatalf("Detected duplicate for sequence: %d\n", m.Sequence)
		}
		msgMap[m.Sequence] = struct{}{}
		msgMapLock.Unlock()
		// Track received for each receiver.
		if m.Sub == s1 {
			atomic.AddInt32(&s1Received, 1)
		} else if m.Sub == s2 {
			// Slow down this subscriber
			time.Sleep(500 * time.Millisecond)
			atomic.AddInt32(&s2Received, 1)
		} else {
			t.Fatalf("Received message on unknown subscription")
		}
		// Track total
		if nr := atomic.AddInt32(&received, 1); nr == int32(toSend) {
			ch <- true
		}
	}

	s1, err = sc.QueueSubscribe("foo", "bar", mcb)
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s1.Unsubscribe()

	s2, err = sc.QueueSubscribe("foo", "bar", mcb)
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s2.Unsubscribe()

	// Publish out the messages.
	for i := int32(0); i < toSend; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}
	if err := WaitTime(ch, 10*time.Second); err != nil {
		t.Fatal("Did not receive our messages")
	}

	if nr := atomic.LoadInt32(&received); nr != toSend {
		t.Fatalf("Did not receive correct number of messages: %d vs %d\n", nr, toSend)
	}

	s1r := atomic.LoadInt32(&s1Received)
	s2r := atomic.LoadInt32(&s2Received)

	// Since we slowed down sub2, we should receive all but 1 or 2 messages on sub1
	if s2r != 1 && s2r != 2 {
		t.Fatalf("Expected 1 or 2 msgs for sub2, got %d\n", s2r)
	}

	if s1r != toSend-s2r {
		t.Fatalf("Expected %d msgs for sub1, got %d\n", toSend-s2r, s1r)
	}
}

func TestPubMultiQueueSubWithRedelivery(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	s1Received := int32(0)
	toSend := int32(50)

	var s1, s2 Subscription

	mcb := func(m *Msg) {
		// Track received for each receiver.

		if m.Sub == s1 {
			m.Ack()
			atomic.AddInt32(&s1Received, 1)

			// Track total only for sub1
			if nr := atomic.AddInt32(&received, 1); nr == int32(toSend) {
				ch <- true
			}
		} else if m.Sub == s2 {
			// We will not ack this subscriber
		} else {
			t.Fatalf("Received message on unknown subscription")
		}
	}

	s1, err = sc.QueueSubscribe("foo", "bar", mcb, SetManualAckMode())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s1.Unsubscribe()

	s2, err = sc.QueueSubscribe("foo", "bar", mcb, SetManualAckMode(), AckWait(1*time.Second))
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s2.Unsubscribe()

	// Publish out the messages.
	for i := int32(0); i < toSend; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}
	if err := WaitTime(ch, 30*time.Second); err != nil {
		t.Fatal("Did not receive our messages")
	}

	if nr := atomic.LoadInt32(&received); nr != toSend {
		t.Fatalf("Did not receive correct number of messages: %d vs %d\n", nr, toSend)
	}
}

func TestPubMultiQueueSubWithDelayRedelivery(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	toSend := int32(500)
	ackCount := int32(0)

	var s1, s2 Subscription

	mcb := func(m *Msg) {
		// Track received for each receiver.
		if m.Sub == s1 {

			m.Ack()

			// if we've acked everything, signal
			nr := atomic.AddInt32(&ackCount, 1)

			if nr == int32(toSend) {
				ch <- true
			}

			if nr > 0 && nr%(toSend/2) == 0 {

				// This depends on the internal algorithm where the
				// best resend subscriber is the one with the least number
				// of outstanding acks.
				//
				// Sleep to allow the acks to back up, so s2 will look
				// like a better subscriber to send messages to.
				time.Sleep(time.Millisecond * 200)
			}
		} else if m.Sub == s2 {
			// We will not ack this subscriber
		} else {
			t.Fatalf("Received message on unknown subscription")
		}
	}

	s1, err = sc.QueueSubscribe("foo", "bar", mcb, SetManualAckMode())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s1.Unsubscribe()

	s2, err = sc.QueueSubscribe("foo", "bar", mcb, SetManualAckMode(), AckWait(1*time.Second))
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer s2.Unsubscribe()

	// Publish out the messages.
	for i := int32(0); i < toSend; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}

	if err := WaitTime(ch, 30*time.Second); err != nil {
		t.Fatalf("Did not ack expected count of messages: %v", toSend)
	}

	if nr := atomic.LoadInt32(&ackCount); nr != toSend {
		t.Fatalf("Did not ack the correct number of messages: %d vs %d\n", nr, toSend)
	}
}

func TestRedeliveredFlag(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	toSend := int32(100)
	hw := []byte("Hello World")

	for i := int32(0); i < toSend; i++ {
		if err := sc.Publish("foo", hw); err != nil {
			t.Fatalf("Error publishing message: %v\n", err)
		}
	}

	ch := make(chan bool)
	received := int32(0)

	msgsLock := &sync.Mutex{}
	msgs := make(map[uint64]*Msg)

	// Test we only receive MaxInflight if we do not ack
	sub, err := sc.Subscribe("foo", func(m *Msg) {
		// Remember the message.
		msgsLock.Lock()
		msgs[m.Sequence] = m
		msgsLock.Unlock()

		// Only Ack odd numbers
		if m.Sequence%2 != 0 {
			if err := m.Ack(); err != nil {
				t.Fatalf("Unexpected error on Ack: %v\n", err)
			}
		}

		if nr := atomic.AddInt32(&received, 1); nr == toSend {
			ch <- true
		}
	}, DeliverAllAvailable(), AckWait(1*time.Second), SetManualAckMode())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive at least 10 messages")
	}
	time.Sleep(1500 * time.Millisecond) // Wait for redelivery

	msgsLock.Lock()
	defer msgsLock.Unlock()

	for _, m := range msgs {
		// Expect all even msgs to have been redelivered.
		if m.Sequence%2 == 0 && !m.Redelivered {
			t.Fatalf("Expected a redelivered flag to be set on msg %d\n", m.Sequence)
		}
	}
}

func TestMaxChannels(t *testing.T) {
	t.Skip("Skipping test for now around channel limits")

	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName)
	defer sc.Close()
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	hw := []byte("Hello World")
	var subject string

	// FIXME(dlc) - Eventually configurable, but wanted test in place.
	// Send to DefaultChannelLimit + 1
	// These all should work fine
	for i := 0; i < DefaultChannelLimit; i++ {
		subject = fmt.Sprintf("CHAN-%d", i)
		sc.PublishAsync(subject, hw, nil)
	}
	// This one should error
	if err := sc.Publish("CHAN_MAX", hw); err == nil {
		t.Fatalf("Expected an error signalling too many channels\n")
	}
}
