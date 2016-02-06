package stan

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

import (
	"bytes"
	"errors"
	"fmt"
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
	if _, err := Connect("someNonExistantServerID", "myTestClient", ConnectWait(connectTime)); err != ErrClusterUnreachable {
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
	guid = sc.PublishAsync("foo", []byte("Hello World!"), acb)
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
	guid = sc.PublishAsync("foo", []byte("Hello World!"), acb)
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
	sub, err := sc.Subscribe("foo", func(m *Msg) {
	})
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

	//	msgs, bytes, _ := sc.(*conn).ackSubscription.MaxPending()
	//	fmt.Printf("max pending msgs:%d bytes:%d\n", msgs, bytes)
}

func BenchmarkTimeNow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		now := time.Now()
		now.Add(10 * time.Nanosecond)
	}
}
