// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"flag"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats-streaming-server/stores"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var storeType string

func benchCleanupDatastore(b *testing.B, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		stackFatalf(b, "Error cleaning up datastore: %v", err)
	}
}

func benchRunServer(b *testing.B) *StanServer {
	opts := GetDefaultOptions()
	opts.Debug = false
	opts.Trace = false
	opts.StoreType = storeType
	if storeType == stores.TypeFile {
		opts.FilestoreDir = defaultDataStore
	}
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		b.Fatalf("Unable to start server: %v", err)
	}
	return s
}

func BenchmarkPublish(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b, defaultDataStore)
	defer benchCleanupDatastore(b, defaultDataStore)

	s := benchRunServer(b)
	defer s.Shutdown()

	sc := NewDefaultConnection(b)
	defer sc.Close()

	hw := []byte("Hello World")

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := sc.Publish("foo", hw); err != nil {
			b.Fatalf("Got error on publish: %v\n", err)
		}
	}

	b.StopTimer()
}

func BenchmarkPublishAsync(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b, defaultDataStore)
	defer benchCleanupDatastore(b, defaultDataStore)

	s := benchRunServer(b)
	defer s.Shutdown()

	sc := NewDefaultConnection(b)
	defer sc.Close()

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

	for i := 0; i < b.N; i++ {
		if _, err := sc.PublishAsync("foo", hw, ah); err != nil {
			b.Fatalf("Error from PublishAsync: %v\n", err)
		}
	}

	err := WaitTime(ch, 10*time.Second)

	b.StopTimer()

	if err != nil {
		b.Fatal("Timed out waiting for ack messages")
	} else if atomic.LoadInt32(&received) != int32(b.N) {
		b.Fatalf("Received: %d", received)
	}
}

func BenchmarkSubscribe(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b, defaultDataStore)
	defer benchCleanupDatastore(b, defaultDataStore)

	s := benchRunServer(b)
	defer s.Shutdown()

	sc := NewDefaultConnection(b)
	defer sc.Close()

	hw := []byte("Hello World")
	pch := make(chan bool)

	// Queue up all the messages. Keep this outside of the timing.
	for i := 0; i < b.N; i++ {
		if i == b.N-1 {
			// last one
			sc.PublishAsync("foo", hw, func(lguid string, err error) {
				if err != nil {
					b.Fatalf("Got an error from ack handler, %v", err)
				}
				pch <- true
			})
		} else {
			sc.PublishAsync("foo", hw, nil)
		}
	}

	// Wait for published to finish
	if err := WaitTime(pch, 10*time.Second); err != nil {
		b.Fatalf("Error waiting for publish to finish\n")
	}

	ch := make(chan bool)
	received := int32(0)

	b.StartTimer()

	sc.Subscribe("foo", func(m *stan.Msg) {
		if nr := atomic.AddInt32(&received, 1); nr >= int32(b.N) {
			ch <- true
		}
	}, stan.DeliverAllAvailable())

	err := WaitTime(ch, 10*time.Second)

	b.StopTimer()

	nr := atomic.LoadInt32(&received)
	if err != nil {
		b.Fatalf("Timed out waiting for messages, received only %d of %d\n", nr, b.N)
	} else if nr != int32(b.N) {
		b.Fatalf("Only Received: %d of %d", received, b.N)
	}
}

func BenchmarkQueueSubscribe(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b, defaultDataStore)
	defer benchCleanupDatastore(b, defaultDataStore)

	s := benchRunServer(b)
	defer s.Shutdown()

	sc := NewDefaultConnection(b)
	defer sc.Close()

	hw := []byte("Hello World")
	pch := make(chan bool)

	// Queue up all the messages. Keep this outside of the timing.
	for i := 0; i < b.N; i++ {
		if i == b.N-1 {
			// last one
			sc.PublishAsync("foo", hw, func(lguid string, err error) {
				if err != nil {
					b.Fatalf("Got an error from ack handler, %v", err)
				}
				pch <- true
			})
		} else {
			sc.PublishAsync("foo", hw, nil)
		}
	}

	// Wait for published to finish
	if err := WaitTime(pch, 10*time.Second); err != nil {
		b.Fatalf("Error waiting for publish to finish\n")
	}

	ch := make(chan bool)
	received := int32(0)

	mcb := func(m *stan.Msg) {
		if nr := atomic.AddInt32(&received, 1); nr >= int32(b.N) {
			ch <- true
		}
	}

	b.StartTimer()

	sc.QueueSubscribe("foo", "bar", mcb, stan.DeliverAllAvailable())
	sc.QueueSubscribe("foo", "bar", mcb, stan.DeliverAllAvailable())
	sc.QueueSubscribe("foo", "bar", mcb, stan.DeliverAllAvailable())
	sc.QueueSubscribe("foo", "bar", mcb, stan.DeliverAllAvailable())

	err := WaitTime(ch, 20*time.Second)

	b.StopTimer()

	nr := atomic.LoadInt32(&received)
	if err != nil {
		b.Fatalf("Timed out waiting for messages, received only %d of %d\n", nr, b.N)
	} else if nr != int32(b.N) {
		b.Fatalf("Only Received: %d of %d", received, b.N)
	}
}

func BenchmarkPublishSubscribe(b *testing.B) {
	b.StopTimer()

	benchCleanupDatastore(b, defaultDataStore)
	defer benchCleanupDatastore(b, defaultDataStore)

	s := benchRunServer(b)
	defer s.Shutdown()

	sc := NewDefaultConnection(b)
	defer sc.Close()

	hw := []byte("Hello World")

	ch := make(chan bool)
	received := int32(0)

	// Subscribe callback, counts msgs received.
	_, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if nr := atomic.AddInt32(&received, 1); nr >= int32(b.N) {
			ch <- true
		}
	}, stan.DeliverAllAvailable())

	if err != nil {
		b.Fatalf("Error subscribing, %v", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := sc.PublishAsync("foo", hw, func(guid string, err error) {
			if err != nil {
				b.Fatalf("Received an error in publish ack callback: %v\n", err)
			}
		})
		if err != nil {
			b.Fatalf("Error publishing %v\n", err)
		}
	}

	err = WaitTime(ch, 30*time.Second)

	b.StopTimer()

	nr := atomic.LoadInt32(&received)
	if err != nil {
		b.Fatalf("Timed out waiting for messages, received only %d of %d\n", nr, b.N)
	} else if nr != int32(b.N) {
		b.Fatalf("Only Received: %d of %d", received, b.N)
	}
}

func TestMain(m *testing.M) {
	storeType = stores.TypeMemory
	var st string
	flag.StringVar(&st, "store", "", "store type to test: \"file\" for File stores")
	flag.Parse()
	if strings.ToLower(st) == "file" {
		storeType = stores.TypeFile
	}
	os.Exit(m.Run())
}
