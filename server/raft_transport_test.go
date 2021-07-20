// Copyright 2017-2021 The NATS Authors
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
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats-server/v2/server"
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type testAddrProvider struct {
	addr string
}

func (t *testAddrProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	return raft.ServerAddress(t.addr), nil
}

// This can be used as the destination for a logger and it'll
// cause the test to fail if the low level code is issuing an [ERR]
// log message.
type testLoggerAdapter struct {
	t *testing.T
}

func (a *testLoggerAdapter) Write(d []byte) (int, error) {
	if d[len(d)-1] == '\n' {
		d = d[:len(d)-1]
	}
	str := string(d)
	if strings.Contains(str, "[ERROR]") {
		stackFatalf(a.t, "NetTransport error: %v", str)
	}
	return len(d), nil
}

func newTestLogger(t *testing.T) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   natsLogAppName,
		Level:  hclog.Debug,
		Output: &testLoggerAdapter{t: t},
	})
}

func runRaftTportServer() *server.Server {
	return natsdTest.RunDefaultServer()
}

func newNatsConnection(t tLogger) *nats.Conn {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	return nc
}

func TestRAFTTransportStartStop(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	trans, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	trans.Close()
}

func TestRAFTTransportHeartbeatFastPath(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	// Transport 1 is consumer
	trans1, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	// Make the RPC request
	args := raft.AppendEntriesRequest{
		Term:   10,
		Leader: []byte("cartman"),
	}
	resp := raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	invoked := false
	fastpath := func(rpc raft.RPC) {
		// Verify the command
		req := rpc.Command.(*raft.AppendEntriesRequest)
		if !reflect.DeepEqual(req, &args) {
			t.Fatalf("command mismatch: %#v %#v", *req, args)
		}

		rpc.Respond(&resp, nil)
		invoked = true
	}
	trans1.SetHeartbeatHandler(fastpath)

	// Transport 2 makes outbound request
	nc2 := newNatsConnection(t)
	defer nc2.Close()
	trans2, err := newNATSTransportWithLogger("b", nc2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	var out raft.AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}

	// Ensure fast-path is used
	if !invoked {
		t.Fatalf("fast-path not used")
	}
}

func TestRAFTTransportAppendEntries(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	// Transport 1 is consumer
	trans1, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.AppendEntriesRequest{
		Term:         10,
		Leader:       []byte("cartman"),
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*raft.Log{
			{
				Index: 101,
				Term:  4,
				Type:  raft.LogNoop,
			},
		},
		LeaderCommitIndex: 90,
	}
	resp := raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	ch := make(chan error, 1)

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.AppendEntriesRequest)
			if !reflect.DeepEqual(req, &args) {
				ch <- fmt.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			ch <- errors.New("timeout")
			return
		}
		close(ch)
	}()

	// Transport 2 makes outbound request
	nc2 := newNatsConnection(t)
	defer nc2.Close()
	trans2, err := newNATSTransportWithLogger("b", nc2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	var out raft.AppendEntriesResponse
	if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	err = <-ch
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

func TestRAFTTransportAppendEntriesPipeline(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	// Transport 1 is consumer
	trans1, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.AppendEntriesRequest{
		Term:         10,
		Leader:       []byte("cartman"),
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*raft.Log{
			{
				Index: 101,
				Term:  4,
				Type:  raft.LogNoop,
			},
		},
		LeaderCommitIndex: 90,
	}
	resp := raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	ch := make(chan error, 1)

	// Listen for a request
	go func() {
		for i := 0; i < 10; i++ {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*raft.AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					ch <- fmt.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				ch <- errors.New("timeout")
				return
			}
		}
		close(ch)
	}()

	// Transport 2 makes outbound request
	nc2 := newNatsConnection(t)
	defer nc2.Close()
	trans2, err := newNATSTransportWithLogger("b", nc2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	pipeline, err := trans2.AppendEntriesPipeline("id1", trans1.LocalAddr())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer pipeline.Close()
	for i := 0; i < 10; i++ {
		out := new(raft.AppendEntriesResponse)
		if _, err := pipeline.AppendEntries(&args, out); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	err = <-ch
	if err != nil {
		t.Fatal(err)
	}

	respCh := pipeline.Consumer()
	for i := 0; i < 10; i++ {
		select {
		case ready := <-respCh:
			// Verify the response
			if !reflect.DeepEqual(&resp, ready.Response()) {
				t.Fatalf("command mismatch: %#v %#v", &resp, ready.Response())
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("timeout")
		}
	}
}

func TestRAFTTransportRequestVote(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	// Transport 1 is consumer
	trans1, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.RequestVoteRequest{
		Term:         20,
		Candidate:    []byte("butters"),
		LastLogIndex: 100,
		LastLogTerm:  19,
	}
	resp := raft.RequestVoteResponse{
		Term:    100,
		Peers:   []byte("blah"),
		Granted: false,
	}

	ch := make(chan error, 1)

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.RequestVoteRequest)
			if !reflect.DeepEqual(req, &args) {
				ch <- fmt.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			ch <- errors.New("timeout")
			return
		}
		close(ch)
	}()

	// Transport 2 makes outbound request
	nc2 := newNatsConnection(t)
	defer nc2.Close()
	trans2, err := newNATSTransportWithLogger("b", nc2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	var out raft.RequestVoteResponse
	if err := trans2.RequestVote("id1", trans1.LocalAddr(), &args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	err = <-ch
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

func TestRAFTTransportInstallSnapshot(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	// Transport 1 is consumer
	trans1, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.InstallSnapshotRequest{
		Term:         10,
		Leader:       []byte("kyle"),
		LastLogIndex: 100,
		LastLogTerm:  9,
		Peers:        []byte("blah blah"),
		Size:         10,
	}
	resp := raft.InstallSnapshotResponse{
		Term:    10,
		Success: true,
	}

	ch := make(chan error, 1)

	// Listen for a request
	go func() {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*raft.InstallSnapshotRequest)
			if !reflect.DeepEqual(req, &args) {
				ch <- fmt.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}

			// Try to read the bytes
			buf := make([]byte, 10)
			rpc.Reader.Read(buf)

			// Compare
			if !bytes.Equal(buf, []byte("0123456789")) {
				ch <- fmt.Errorf("bad buf %v", buf)
				return
			}

			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			ch <- errors.New("timeout")
			return
		}
		close(ch)
	}()

	// Transport 2 makes outbound request
	nc2 := newNatsConnection(t)
	defer nc2.Close()
	trans2, err := newNATSTransportWithLogger("b", nc2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	// Create a buffer
	buf := bytes.NewBuffer([]byte("0123456789"))

	var out raft.InstallSnapshotResponse
	if err := trans2.InstallSnapshot("id1", trans1.LocalAddr(), &args, &out, buf); err != nil {
		t.Fatalf("err: %v", err)
	}

	err = <-ch
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response
	if !reflect.DeepEqual(resp, out) {
		t.Fatalf("command mismatch: %#v %#v", resp, out)
	}
}

func TestRAFTTransportEncodeDecode(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	// Transport 1 is consumer
	trans1, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != local {
		t.Fatalf("enc/dec fail: %v %v", dec, local)
	}
}

func TestRAFTTransportEncodeDecodeAddressProvider(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	addressOverride := "b"
	config := &raft.NetworkTransportConfig{MaxPool: 2, Timeout: time.Second, Logger: newTestLogger(t), ServerAddressProvider: &testAddrProvider{addressOverride}}
	trans1, err := newNATSTransportWithConfig("a", nc, config)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()

	local := trans1.LocalAddr()
	enc := trans1.EncodePeer("id1", local)
	dec := trans1.DecodePeer(enc)

	if dec != raft.ServerAddress(addressOverride) {
		t.Fatalf("enc/dec fail: %v %v", dec, addressOverride)
	}
}

func TestRAFTTransportPooledConn(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()
	nc := newNatsConnection(t)
	defer nc.Close()

	// Transport 1 is consumer
	trans1, err := newNATSTransportWithLogger("a", nc, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := raft.AppendEntriesRequest{
		Term:         10,
		Leader:       []byte("cartman"),
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*raft.Log{
			{
				Index: 101,
				Term:  4,
				Type:  raft.LogNoop,
			},
		},
		LeaderCommitIndex: 90,
	}
	resp := raft.AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	ch1 := make(chan error, 1)

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*raft.AppendEntriesRequest)
				if !reflect.DeepEqual(req, &args) {
					ch1 <- fmt.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				close(ch1)
				return
			}
		}
	}()

	// Transport 2 makes outbound request, 3 conn pool
	nc2 := newNatsConnection(t)
	defer nc2.Close()
	trans2, err := newNATSTransportWithLogger("b", nc2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer trans2.Close()

	// Create wait group
	wg := &sync.WaitGroup{}
	wg.Add(5)

	ch2 := make(chan error, 1)

	appendFunc := func() {
		defer wg.Done()
		var out raft.AppendEntriesResponse
		if err := trans2.AppendEntries("id1", trans1.LocalAddr(), &args, &out); err != nil {
			ch2 <- fmt.Errorf("err: %v", err)
			return
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			ch2 <- fmt.Errorf("command mismatch: %#v %#v", resp, out)
			return
		}
	}

	// Try to do parallel appends, should stress the conn pool
	for i := 0; i < 5; i++ {
		go appendFunc()
	}

	// Wait for the routines to finish
	wg.Wait()
	close(ch2)

	err = <-ch1
	if err != nil {
		t.Fatal(err)
	}

	err = <-ch2
	if err != nil {
		t.Fatal(err)
	}
}

func TestRAFTTransportConnReader(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()

	nc1 := newNatsConnection(t)
	defer nc1.Close()
	stream1, err := newNATSStreamLayer("a", nc1, newTestLogger(t), 2*time.Second, nil)
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	defer stream1.Close()

	// Check dial timeout:
	start := time.Now()
	if _, err := stream1.Dial("b", 50*time.Millisecond); err == nil {
		t.Fatal("Expected failure")
	}
	dur := time.Since(start)
	if dur > 250*time.Millisecond {
		t.Fatalf("Should have timed out sooner than %v", dur)
	}

	ch := make(chan *natsConn, 3)
	go func() {
		for {
			c, err := stream1.Accept()
			if err != nil {
				ch <- nil
				return
			}
			ch <- c.(*natsConn)
		}
	}()

	nc2 := newNatsConnection(t)
	defer nc2.Close()
	stream2, err := newNATSStreamLayer("b", nc2, newTestLogger(t), 2*time.Second, nil)
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	defer stream2.Close()

	bToA, err := stream2.Dial("a", time.Second)
	if err != nil {
		t.Fatalf("Error dialing: %v", err)
	}
	defer bToA.Close()

	var fromB *natsConn
	select {
	case fromB = <-ch:
		if fromB == nil {
			t.Fatal("Error accepting connection")
		}
	case <-time.After(time.Second):
		t.Fatal("Failed to get connection from B")
	}
	defer fromB.Close()

	if _, err := bToA.Write([]byte("Hello from A!")); err != nil {
		t.Fatalf("Error on write: %v", err)
	}

	var buf [1024]byte
	for _, test := range []struct {
		nb       int
		expected string
	}{
		{5, "Hello"},
		{6, " from "},
		{2, "A!"},
	} {
		t.Run("", func(t *testing.T) {
			n, err := fromB.Read(buf[:test.nb])
			if err != nil || n != test.nb {
				t.Fatalf("Unexpected error on read, n=%v err=%v", n, err)
			}
			if got := string(buf[:n]); got != test.expected {
				t.Fatalf("Unexpected result: %q", got)
			}
		})
	}

	firstPart := "Partial"
	secondPart := " and then the rest"
	if _, err := bToA.Write([]byte(firstPart + secondPart)); err != nil {
		t.Fatalf("Error on write: %v", err)
	}
	n, err := fromB.Read(buf[:7])
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if string(buf[:n]) != firstPart {
		t.Fatalf("Unexpected result: %q", buf[:n])
	}
	// Now pass a buffer to Read() that is larger than what is left in pending
	n, err = fromB.Read(buf[:])
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if string(buf[:n]) != secondPart {
		t.Fatalf("Unexpected result: %q", buf[:n])
	}

	// Another test with a partial...
	if _, err := bToA.Write([]byte("ab")); err != nil {
		t.Fatalf("Error on write: %v", err)
	}
	n, err = fromB.Read(buf[:1])
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if string(buf[:n]) != "a" {
		t.Fatalf("Unexpected result: %q", buf[:n])
	}
	// There is only 1 byte that should be pending, but call with a large buffer.
	n, err = fromB.Read(buf[:])
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if string(buf[:n]) != "b" {
		t.Fatalf("Unexpected result: %q", buf[:n])
	}

	// Write empty message should not go out
	if n, err := bToA.Write(nil); err != nil || n != 0 {
		t.Fatalf("Write nil should return 0, nil, got %v and %v", n, err)
	}

	// Write something else
	if _, err := bToA.Write([]byte("msg")); err != nil {
		t.Fatalf("Error on write: %v", err)
	}

	// Consume all at once
	n, err = fromB.Read(buf[:])
	if err != nil || n != 3 {
		t.Fatalf("Unexpected error on read, n=%v err=%v", n, err)
	}
	if got := string(buf[:3]); got != "msg" {
		t.Fatalf("Unexpected result: %q", got)
	}

	// Now wait for a timeout
	fromB.SetDeadline(time.Now().Add(100 * time.Millisecond))
	start = time.Now()
	n, err = fromB.Read(buf[:])
	if err == nil || n != 0 {
		t.Fatalf("Expected timeout, got err=%v n=%v", err, n)
	}

	// Clear timeout
	fromB.SetDeadline(time.Time{})

	// Close the stream1's connection that should send an empty
	// message to fromB connection to signal that it is closed.
	if err := bToA.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
	// Call Write on close connection should fail too.
	if _, err := bToA.Write([]byte("msg")); err != io.EOF {
		t.Fatalf("Expected EOF on write, got: %v", err)
	}

	// Expect an EOF error
	for i := 0; i < 2; i++ {
		n, err = fromB.Read(buf[:])
		if err != io.EOF || n != 0 {
			t.Fatalf("Expected EOF, got err=%v n=%v", err, n)
		}
	}

	// Create a "new" connection. The way the stream was created,
	// we actually reuse the connection from the stream, so no
	// new connection is created.
	tmp, err := stream1.newNATSConn("c")
	if err != nil {
		t.Fatalf("Error on create: %v", err)
	}
	// Now close and ensure that we did not close the stream connection.
	tmp.Close()

	stream1.mu.Lock()
	count := len(stream1.conns)
	connected := stream1.conn.IsConnected()
	stream1.mu.Unlock()
	if count != 0 {
		t.Fatalf("Expected stream to have no connection, got %v", count)
	}
	if !connected {
		t.Fatal("Stream connection should not have been closed")
	}

	// Again, create a temp connection and make sure what we break out of
	// a Read() if we close the connection.
	tmp, err = stream1.newNATSConn("c")
	if err != nil {
		t.Fatalf("Error on create: %v", err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(150 * time.Millisecond)
		tmp.Close()
		wg.Done()
	}()
	if n, err := tmp.Read(buf[:]); err != io.EOF || n > 0 {
		t.Fatalf("Expected n=0 err=io.EOF, got n=%v err=%v", n, err)
	}
	wg.Wait()

	// Now create a stream that will create a new NATS connection.
	nc3 := newNatsConnection(t)
	defer nc3.Close()
	stream3, err := newNATSStreamLayer("c", nc3, newTestLogger(t), 2*time.Second, func(name string) (*nats.Conn, error) {
		return nats.Connect(nats.DefaultURL)
	})
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	defer stream3.Close()

	cToA, err := stream3.Dial("a", time.Second)
	if err != nil {
		t.Fatalf("Error dialing: %v", err)
	}
	defer cToA.Close()

	var fromC *natsConn
	select {
	case fromC = <-ch:
		if fromC == nil {
			t.Fatal("Error accepting connection")
		}
	case <-time.After(time.Second):
		t.Fatal("Failed to get connection from C")
	}
	defer fromC.Close()

	if _, err := cToA.Write([]byte("from C")); err != nil {
		t.Fatalf("Error on write: %v", err)
	}

	n, err = fromC.Read(buf[:])
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if got := string(buf[:n]); got != "from C" {
		t.Fatalf("Unexpected read: %q", got)
	}

	// Close cToA
	if err := cToA.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}

	// The connection "fromC" should be closed too.
	n, err = fromC.Read(buf[:])
	if n != 0 || err != io.EOF {
		t.Fatalf("Expected fromC connection to close, got n=%v err=%v", n, err)
	}

	// Close all streams.
	stream3.Close()
	stream2.Close()
	stream1.Close()

	// The Accept should return and we should get nil
	select {
	case c := <-ch:
		if c != nil {
			t.Fatalf("Should have gotten nil, got %v", c)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept() did not exit")
	}
}

func TestRAFTTransportDialAcceptCloseConnOnTransportClosed(t *testing.T) {
	s := runRaftTportServer()
	defer s.Shutdown()

	nc1 := newNatsConnection(t)
	defer nc1.Close()
	stream1, err := newNATSStreamLayer("a", nc1, newTestLogger(t), 2*time.Second, nil)
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	defer stream1.Close()

	nc2 := newNatsConnection(t)
	defer nc2.Close()
	stream2, err := newNATSStreamLayer("b", nc2, newTestLogger(t), 2*time.Second, nil)
	if err != nil {
		t.Fatalf("Error creating stream: %v", err)
	}
	defer stream2.Close()

	accepted := make(chan *natsConn, 101)
	go func() {
		for {
			c, err := stream2.Accept()
			if err != nil {
				accepted <- nil
				return
			}
			accepted <- c.(*natsConn)
		}
	}()

	ch := make(chan bool)
	dialed := make(chan net.Conn, 101)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c, err := stream1.Dial("b", 250*time.Millisecond)
			if err != nil {
				return
			}
			dialed <- c
			select {
			case <-ch:
				return
			default:
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	stream1.Close()
	stream2.Close()
	close(ch)
	wg.Wait()

	stream1.mu.Lock()
	l1 := len(stream1.conns)
	stream1.mu.Unlock()
	stream2.mu.Lock()
	l2 := len(stream2.conns)
	stream2.mu.Unlock()

	for i := 0; i < 100; i++ {
		select {
		case c := <-dialed:
			if c != nil {
				c.Close()
			}
		default:
		}
		select {
		case c := <-accepted:
			if c != nil {
				c.Close()
			}
		default:
		}
	}
	if l1 > 0 || l2 > 0 {
		t.Fatalf("Connections were added after streams were closed: %v/%v", l1, l2)
	}
}
