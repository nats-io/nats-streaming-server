package stan

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/test"
	nats "github.com/nats-io/nats"
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
	if _, err := Connect(clusterName, clientName); err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
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
	if err := sc.Publish("foo", []byte("Hello World!")); err != nil {
		t.Fatalf("Expected no errors on publish, got %v\n", err)
	}
}

func TestBasicPublishAsync(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()
	sc, err := Connect(clusterName, clientName)
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	sub, err := sc.Subscribe("foo", func(m *Msg) {})
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()
}

func TestBasicPubSub(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	received := int32(0)
	toSend := int32(100)
	hw := []byte("Hello World")

	sub, err := sc.Subscribe("foo", func(m *Msg) {
		if m.Subject != "foo" {
			t.Fatalf("Expected subject of 'foo', got '%s'\n", m.Subject)
		}
		if !bytes.Equal(m.Data, hw) {
			t.Fatalf("Wrong payload, got %q\n", m.Data)
		}
		// Make sure Seq and Timestamp are set
		if m.Seq == 0 {
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	hw := []byte("Hello World")

	inbox := newInbox()

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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}

	ch := make(chan bool)
	hw := []byte("Hello World")

	inbox := newInbox()

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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	// Publish ten messages
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}

	// Now subscribe and set start position to last received.
	ch := make(chan bool)
	mcb := func(m *Msg) {
		ch <- true
	}
	sub, err := sc.Subscribe("foo", mcb, StartWithLastReceived())
	if err != nil {
		t.Fatalf("Expected non-nil error on Subscribe, got %v\n", err)
	}
	defer sub.Unsubscribe()

	// Check for sub setup
	rsub := sub.(*subscription)
	if rsub.opts.StartAt != StartPosition_LastReceived {
		t.Fatalf("Incorrect StartAt state: %s\n", rsub.opts.StartAt)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our message")
	}
}

func TestSubscriptionStartAtSequence(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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
	if rsub.opts.StartAt != StartPosition_SequenceStart {
		t.Fatalf("Incorrect StartAt state: %s\n", rsub.opts.StartAt)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our messages")
	}

	// Check we received them in order.
	for i, seq := 0, uint64(6); i < 5; i++ {
		m := savedMsgs[i]
		// Check Sequence
		if m.Seq != seq {
			t.Fatalf("Expected seq: %d, got %d\n", seq, m.Seq)
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	// Publish ten messages all together

	// Publish first 5
	for i := 1; i <= 5; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		sc.Publish("foo", data)
	}
	time.Sleep(200 * time.Millisecond)
	startTime := time.Now()

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
	if rsub.opts.StartAt != StartPosition_TimeStart {
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
		if m.Seq != seq {
			t.Fatalf("Expected seq: %d, got %d\n", seq, m.Seq)
		}
		// Check payload
		dseq, _ := strconv.Atoi(string(m.Data))
		if dseq != int(seq) {
			t.Fatalf("Expected payload: %d, got %d\n", seq, dseq)
		}
		seq++
	}
}

func TestSubscriptionStartAtFirst(t *testing.T) {
	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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
	if rsub.opts.StartAt != StartPosition_First {
		t.Fatalf("Incorrect StartAt state: %s\n", rsub.opts.StartAt)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our messages")
	}

	// Check we received them in order.
	for i, seq := 0, uint64(1); i < 10; i++ {
		m := savedMsgs[i]
		// Check Sequence
		if m.Seq != seq {
			t.Fatalf("Expected seq: %d, got %d\n", seq, m.Seq)
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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

	_, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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
			fmt.Printf("err is %v\n", err)
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

	sc, err := Connect(clusterName, clientName, PubAckWait(50*time.Millisecond))
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

////////////////////////////////////////////////////////////////////////////////
// Benchmarks
////////////////////////////////////////////////////////////////////////////////

func BenchmarkPublish(b *testing.B) {
	b.StopTimer()

	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()
	sc, err := Connect(clusterName, clientName)
	if err != nil {
		b.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	hw := []byte("Hello World")

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := sc.Publish("foo", hw); err != nil {
			b.Fatalf("Got error on publish: %v\n", err)
		}
	}
}

func BenchmarkPublishAsync(b *testing.B) {
	b.StopTimer()

	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()
	sc, err := Connect(clusterName, clientName)
	if err != nil {
		b.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	hw := []byte("Hello World")

	ch := make(chan bool)
	received := int32(0)

	ah := func(guid string, err error) {
		if err != nil {
			b.Fatalf("Received an error in ack callback: %v\n", err)
		}
		if nr := atomic.AddInt32(&received, 1); nr >= int32(b.N) {
			ch <- true
		}
	}
	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sc.PublishAsync("foo", hw, ah)
	}

	err = WaitTime(ch, 10*time.Second)
	if err != nil {
		b.Fatal("Timed out waiting for ack messages")
	} else if atomic.LoadInt32(&received) != int32(b.N) {
		b.Fatalf("Received: %d", received)
	}

	//	msgs, bytes, _ := sc.(*conn).ackSubscription.MaxPending()
	//	fmt.Printf("max pending msgs:%d bytes:%d\n", msgs, bytes)
}

func BenchmarkPublishSubscribe(b *testing.B) {
	b.StopTimer()

	// Run a STAN server
	s := RunServer(clusterName)
	defer s.Shutdown()
	sc, err := Connect(clusterName, clientName)
	if err != nil {
		b.Fatalf("Expected to connect correctly, got err %v\n", err)
	}
	hw := []byte("Hello World")

	ch := make(chan bool)
	received := int32(0)

	// Subscribe callback, counts msgs received.
	sc.Subscribe("foo", func(m *Msg) {
		if nr := atomic.AddInt32(&received, 1); nr >= int32(b.N) {
			ch <- true
		}
	})

	b.StartTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sc.PublishAsync("foo", hw, func(guid string, err error) {
			if err != nil {
				b.Fatalf("Received an error in publish ack callback: %v\n", err)
			}
		})
	}

	err = WaitTime(ch, 10*time.Second)
	nr := atomic.LoadInt32(&received)
	if err != nil {
		b.Fatalf("Timed out waiting for messages, received only %d of %d\n", nr, b.N)
	} else if nr != int32(b.N) {
		b.Fatalf("Only Received: %d of %d", received, b.N)
	}
}

func BenchmarkTimeNow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		now := time.Now()
		now.Add(10 * time.Nanosecond)
	}
}
