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

// RAFT Transport implementation using NATS

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/nats-io/nats.go"
)

const (
	natsConnectInbox       = "raft.%s.accept"
	natsRequestInbox       = "raft.%s.request.%s"
	timeoutForDialAndFlush = 2 * time.Second
	natsLogAppName         = "raft-nats"
)

var errTransportShutdown = errors.New("raft-nats: transport is being shutdown")

type natsRaftConnCreator func(name string) (*nats.Conn, error)

// natsAddr implements the net.Addr interface. An address for the NATS
// transport is simply a node id, which is then used to construct an inbox.
type natsAddr string

func (n natsAddr) Network() string {
	return "nats"
}

func (n natsAddr) String() string {
	return string(n)
}

type connectRequestProto struct {
	ID    string `json:"id"`
	Inbox string `json:"inbox"`
}

type connectResponseProto struct {
	Inbox string `json:"inbox"`
}

// natsConn implements the net.Conn interface by simulating a stream-oriented
// connection between two peers. It does this by establishing a unique inbox at
// each endpoint which the peers use to stream data to each other.
type natsConn struct {
	mu         sync.RWMutex
	conn       *nats.Conn
	streamConn bool
	localAddr  natsAddr
	remoteAddr natsAddr
	sub        *nats.Subscription
	subTimeout time.Duration
	pendingH   *pendingBuf   // head of pending buffers list
	pendingT   *pendingBuf   // tail of pending buffers list
	ch         chan struct{} // to send notification that a buffer is available
	tmr        *time.Timer
	outbox     string
	closed     bool
	parent     *natsStreamLayer
}

type pendingBuf struct {
	buf  []byte
	next *pendingBuf
}

var pendingBufPool = &sync.Pool{
	New: func() interface{} {
		return &pendingBuf{}
	},
}

func (n *natsConn) onMsg(msg *nats.Msg) {
	pb := pendingBufPool.Get().(*pendingBuf)
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	pb.buf = msg.Data
	if n.pendingT != nil {
		n.pendingT.next = pb
	} else {
		n.pendingH = pb
		// Need to notify under the lock since this channel gets closed
		// on natsConn.close(), which could then cause a panic of
		// "send on closed channel".
		select {
		case n.ch <- struct{}{}:
		default:
		}
	}
	n.pendingT = pb
	n.mu.Unlock()
}

func (n *natsConn) Read(b []byte) (int, error) {
	var subTimeout time.Duration

	n.mu.RLock()
	if n.closed {
		n.mu.RUnlock()
		return 0, io.EOF
	}
	// Reference, but do not remove the pending buffer in case we
	// cannot copy the whole buffer, we will update the buffer slice.
	pb := n.pendingH
	// We will wait only if there is no pending buffer.
	if pb == nil {
		subTimeout = n.subTimeout
		if subTimeout == 0 {
			subTimeout = time.Duration(0x7FFFFFFFFFFFFFFF)
		}
		n.tmr.Reset(subTimeout)
	}
	n.mu.RUnlock()

	// There was no buffer, so we need to wait.
	if pb == nil {
	WAIT_FOR_BUFFER:
		select {
		case <-n.tmr.C:
			return 0, nats.ErrTimeout
		case _, ok := <-n.ch:
			if !ok {
				return 0, io.EOF
			}
		}
		n.mu.RLock()
		// We notify when adding the first pending buffer, but if Read() is called
		// after, we will detect that there is a pending and skip the whole select.
		// So after consuming the pending, the next Read() would get the past
		// notification. If that is the case, go back to the select.
		if n.pendingH == nil {
			n.mu.RUnlock()
			goto WAIT_FOR_BUFFER
		}
		// We have been notified, so get the reference to the head of the list.
		pb = n.pendingH
		n.tmr.Stop()
		n.mu.RUnlock()
	}

	buf := pb.buf
	bufSize := len(buf)
	// A buf of size 0 means that the remote closed
	if bufSize == 0 {
		n.close(false)
		return 0, io.EOF
	}
	limit := bufSize
	if limit > len(b) {
		limit = len(b)
	}
	nb := copy(b, buf[:limit])
	// If we did not copy everything, reduce size by what we copied.
	if nb != bufSize {
		buf = buf[nb:]
	} else {
		buf = nil
	}
	var release bool
	n.mu.Lock()
	if buf != nil {
		pb.buf = buf
	} else {
		// We are done with this pending buffer, remove from the pending list.
		n.pendingH = n.pendingH.next
		if n.pendingH == nil {
			n.pendingT = nil
		}
		release = true
	}
	n.mu.Unlock()
	if release {
		pb.buf, pb.next = nil, nil
		pendingBufPool.Put(pb)
	}
	return nb, nil
}

func (n *natsConn) Write(b []byte) (int, error) {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return 0, io.EOF
	}

	if len(b) == 0 {
		return 0, nil
	}

	// Send data in chunks to avoid hitting max payload.
	for i := 0; i < len(b); {
		chunkSize := min(int64(len(b[i:])), n.conn.MaxPayload())
		if err := n.conn.Publish(n.outbox, b[i:int64(i)+chunkSize]); err != nil {
			return i, err
		}
		i += int(chunkSize)
	}

	return len(b), nil
}

func (n *natsConn) Close() error {
	return n.close(true)
}

func (n *natsConn) close(signalRemote bool) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}

	if signalRemote {
		// Send empty message to signal EOF for a graceful disconnect. Not
		// concerned with errors here as this is best effort.
		n.conn.Publish(n.outbox, nil)
		// Best effort, don't block for too long and don't check returned error.
		n.conn.FlushTimeout(500 * time.Millisecond)
	}

	// If connection is owned by stream, simply unsubscribe. Note that we
	// check for sub != nil because this can be called during setup where
	// sub has not been attached.
	var err error
	if n.streamConn {
		if n.sub != nil {
			err = n.sub.Unsubscribe()
		}
	} else {
		n.conn.Close()
	}

	n.closed = true
	stream := n.parent
	close(n.ch)
	n.tmr.Stop()
	n.mu.Unlock()

	stream.mu.Lock()
	delete(stream.conns, n)
	stream.mu.Unlock()

	return err
}

func (n *natsConn) LocalAddr() net.Addr {
	return n.localAddr
}

func (n *natsConn) RemoteAddr() net.Addr {
	return n.remoteAddr
}

func (n *natsConn) SetDeadline(t time.Time) error {
	n.mu.Lock()
	if t.IsZero() {
		n.subTimeout = 0
	} else {
		n.subTimeout = time.Until(t)
	}
	n.mu.Unlock()
	return nil
}

func (n *natsConn) SetReadDeadline(t time.Time) error {
	return n.SetDeadline(t)
}

func (n *natsConn) SetWriteDeadline(t time.Time) error {
	return n.SetDeadline(t)
}

// natsStreamLayer implements the raft.StreamLayer interface.
type natsStreamLayer struct {
	conn      *nats.Conn
	makeConn  natsRaftConnCreator
	localAddr natsAddr
	sub       *nats.Subscription
	logger    hclog.Logger
	conns     map[*natsConn]struct{}
	mu        sync.Mutex
	closed    bool
	// This is the timeout we will use for flush and dial (request timeout),
	// not the timeout that RAFT will use to call SetDeadline.
	dfTimeout time.Duration
}

func newNATSStreamLayer(id string, conn *nats.Conn, logger hclog.Logger, timeout time.Duration, makeConn natsRaftConnCreator) (*natsStreamLayer, error) {
	n := &natsStreamLayer{
		localAddr: natsAddr(id),
		conn:      conn,
		makeConn:  makeConn,
		logger:    logger,
		conns:     map[*natsConn]struct{}{},
		dfTimeout: timeoutForDialAndFlush,
	}
	// Could be the case in tests...
	if timeout < n.dfTimeout {
		n.dfTimeout = timeout
	}
	sub, err := conn.SubscribeSync(fmt.Sprintf(natsConnectInbox, id))
	if err != nil {
		return nil, err
	}
	if err := conn.FlushTimeout(n.dfTimeout); err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	n.sub = sub
	return n, nil
}

func (n *natsStreamLayer) newNATSConn(address string) (*natsConn, error) {
	var conn *nats.Conn
	var err error

	c := &natsConn{
		localAddr:  n.localAddr,
		remoteAddr: natsAddr(address),
		parent:     n,
		ch:         make(chan struct{}, 1),
		tmr:        time.NewTimer(time.Hour),
	}
	if n.makeConn == nil {
		c.conn = n.conn
		c.streamConn = true
	} else {
		conn, err = n.makeConn(address)
		if err != nil {
			return nil, err
		}
		c.conn = conn
	}
	return c, nil
}

// Dial creates a new net.Conn with the remote address. This is implemented by
// performing a handshake over NATS which establishes unique inboxes at each
// endpoint for streaming data.
func (n *natsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if !n.conn.IsConnected() {
		return nil, errors.New("raft-nats: dial failed, not connected")
	}

	connect := &connectRequestProto{
		ID:    n.localAddr.String(),
		Inbox: fmt.Sprintf(natsRequestInbox, n.localAddr.String(), nats.NewInbox()),
	}
	data, err := json.Marshal(connect)
	if err != nil {
		panic(err)
	}

	// When creating the transport, we pass a 10s timeout, but for Dial, we want
	// to use a different timeout, unless the one provided is smaller.
	if timeout > n.dfTimeout {
		timeout = n.dfTimeout
	}

	// Make connect request to peer.
	msg, err := n.conn.Request(fmt.Sprintf(natsConnectInbox, address), data, timeout)
	if err != nil {
		return nil, err
	}
	var resp connectResponseProto
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}

	// Success, so now create a new NATS connection...
	peerConn, err := n.newNATSConn(string(address))
	if err != nil {
		return nil, fmt.Errorf("raft-nats: unable to create connection to %q: %v", string(address), err)
	}

	// Setup inbox.
	peerConn.mu.Lock()
	sub, err := peerConn.conn.Subscribe(connect.Inbox, peerConn.onMsg)
	if err != nil {
		peerConn.mu.Unlock()
		peerConn.Close()
		return nil, err
	}
	sub.SetPendingLimits(-1, -1)
	peerConn.sub = sub
	peerConn.outbox = resp.Inbox
	peerConn.mu.Unlock()

	if err := peerConn.conn.FlushTimeout(timeout); err != nil {
		peerConn.Close()
		return nil, err
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		peerConn.Close()
		return nil, errTransportShutdown
	}
	n.conns[peerConn] = struct{}{}
	n.mu.Unlock()
	return peerConn, nil
}

// Accept waits for and returns the next connection to the listener.
func (n *natsStreamLayer) Accept() (net.Conn, error) {
	for {
		msg, err := n.sub.NextMsgWithContext(context.TODO())
		if err != nil {
			return nil, err
		}
		if msg.Reply == "" {
			n.logger.Error("Invalid connect message (missing reply inbox)")
			continue
		}

		var connect connectRequestProto
		if err := json.Unmarshal(msg.Data, &connect); err != nil {
			n.logger.Error("Invalid connect message (invalid data)")
			continue
		}

		peerConn, err := n.newNATSConn(connect.ID)
		if err != nil {
			n.logger.Error("Unable to create connection to %q: %v", connect.ID, err)
			continue
		}

		// Setup inbox for peer.
		inbox := fmt.Sprintf(natsRequestInbox, n.localAddr.String(), nats.NewInbox())
		peerConn.mu.Lock()
		sub, err := peerConn.conn.Subscribe(inbox, peerConn.onMsg)
		if err != nil {
			peerConn.mu.Unlock()
			n.logger.Error("Failed to create inbox for remote peer", "error", err)
			peerConn.Close()
			continue
		}
		sub.SetPendingLimits(-1, -1)
		peerConn.outbox = connect.Inbox
		peerConn.sub = sub
		shouldFlush := !peerConn.streamConn
		peerConn.mu.Unlock()

		if shouldFlush {
			if err := peerConn.conn.FlushTimeout(n.dfTimeout); err != nil {
				peerConn.Close()
				continue
			}
		}

		// Reply to peer.
		resp := &connectResponseProto{Inbox: inbox}
		data, err := json.Marshal(resp)
		if err != nil {
			panic(err)
		}
		if err := n.conn.Publish(msg.Reply, data); err != nil {
			n.logger.Error("Failed to send connect response to remote peer", "error", err)
			peerConn.Close()
			continue
		}
		if err := n.conn.FlushTimeout(n.dfTimeout); err != nil {
			n.logger.Error("Failed to flush connect response to remote peer", "error", err)
			peerConn.Close()
			continue
		}
		n.mu.Lock()
		if n.closed {
			n.mu.Unlock()
			peerConn.Close()
			return nil, errTransportShutdown
		}
		n.conns[peerConn] = struct{}{}
		n.mu.Unlock()
		return peerConn, nil
	}
}

func (n *natsStreamLayer) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	nc := n.conn
	// Do not set nc.conn to nil since it is accessed in some functions
	// without the stream layer lock
	conns := make(map[*natsConn]struct{}, len(n.conns))
	for conn, s := range n.conns {
		conns[conn] = s
	}
	n.mu.Unlock()
	for c := range conns {
		c.Close()
	}
	if nc != nil {
		nc.Close()
	}
	return nil
}

func (n *natsStreamLayer) Addr() net.Addr {
	return n.localAddr
}

// newNATSTransport creates a new raft.NetworkTransport implemented with NATS
// as the transport layer.
func newNATSTransport(id string, conn *nats.Conn, timeout time.Duration, logOutput io.Writer, makeConn natsRaftConnCreator) (*raft.NetworkTransport, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   natsLogAppName,
		Level:  hclog.Debug,
		Output: logOutput,
	})
	return createNATSTransport(id, conn, timeout, makeConn, logger, nil)
}

// newNATSTransportWithLogger creates a new raft.NetworkTransport implemented
// with NATS as the transport layer using the provided Logger.
func newNATSTransportWithLogger(id string, conn *nats.Conn, timeout time.Duration, logger hclog.Logger) (*raft.NetworkTransport, error) {
	return createNATSTransport(id, conn, timeout, nil, logger, nil)
}

// newNATSTransportWithConfig returns a raft.NetworkTransport implemented
// with NATS as the transport layer, using the given config struct.
func newNATSTransportWithConfig(id string, conn *nats.Conn, config *raft.NetworkTransportConfig) (*raft.NetworkTransport, error) {
	return createNATSTransport(id, conn, 0, nil, nil, config)
}

func createNATSTransport(id string, conn *nats.Conn, timeout time.Duration, makeConn natsRaftConnCreator,
	logger hclog.Logger, config *raft.NetworkTransportConfig) (*raft.NetworkTransport, error) {

	if config != nil {
		if config.Timeout == 0 {
			config.Timeout = defaultTPortTimeout
		}
		timeout = config.Timeout
		logger = config.Logger
	}
	stream, err := newNATSStreamLayer(id, conn, logger, timeout, makeConn)
	if err != nil {
		return nil, err
	}
	if config != nil {
		config.Stream = stream
		return raft.NewNetworkTransportWithConfig(config), nil
	}
	return raft.NewNetworkTransportWithLogger(stream, 3, timeout, logger), nil
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
