// Copyright 2017-2019 The NATS Authors
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
	"log"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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
	if strings.Contains(str, "[ERR]") {
		stackFatalf(a.t, "NetTransport error: %v", str)
	}
	return len(d), nil
}

func newTestLogger(t *testing.T) *log.Logger {
	return log.New(&testLoggerAdapter{t: t}, "", log.Lmicroseconds)
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
			&raft.Log{
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
			&raft.Log{
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
			&raft.Log{
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
