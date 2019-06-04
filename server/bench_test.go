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
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/test"
	"github.com/nats-io/stan.go"
)

func benchCleanupDatastore(b *testing.B, dir string) {
	switch benchStoreType {
	case stores.TypeFile:
		if err := os.RemoveAll(dir); err != nil {
			stackFatalf(b, "Error cleaning up datastore: %v", err)
		}
	case stores.TypeSQL:
		test.CleanupSQLDatastore(b, testSQLDriver, testSQLSource)
	}
}

func benchRunServer(b *testing.B) *StanServer {
	opts := GetDefaultOptions()
	opts.Debug = false
	opts.Trace = false
	opts.StoreType = benchStoreType
	switch benchStoreType {
	case stores.TypeFile:
		opts.FilestoreDir = defaultDataStore
	case stores.TypeSQL:
		opts.SQLStoreOpts.Driver = testSQLDriver
		opts.SQLStoreOpts.Source = testSQLSource
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
