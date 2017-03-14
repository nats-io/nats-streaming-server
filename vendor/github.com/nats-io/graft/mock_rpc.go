// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/nats-io/graft/pb"
)

var mu sync.Mutex
var peers map[string]*Node

func init() {
	peers = make(map[string]*Node)
}

func mockPeerCount() int {
	mu.Lock()
	defer mu.Unlock()
	return len(peers)
}

func mockPeers() []*Node {
	mu.Lock()
	defer mu.Unlock()
	nodes := make([]*Node, 0, len(peers))
	for _, p := range peers {
		nodes = append(nodes, p)
	}
	return nodes
}

func mockRegisterPeer(n *Node) {
	mu.Lock()
	defer mu.Unlock()
	peers[n.id] = n
}

func mockUnregisterPeer(id string) {
	mu.Lock()
	defer mu.Unlock()
	delete(peers, id)
}

func mockResetPeers() {
	mu.Lock()
	defer mu.Unlock()
	peers = make(map[string]*Node)
}

// Membership designations for split network simulations.
const (
	NO_MEMBERSHIP = int32(iota)
	GRP_A
	GRP_B
)

// Handle a simulation of a split network.
func mockSplitNetwork(grp []*Node) {
	if len(grp) <= 0 {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	// Reset all to other group, GrpB
	for _, p := range peers {
		rpc := p.rpc.(*MockRpcDriver)
		atomic.StoreInt32(&rpc.membership, GRP_B)
	}
	// Set passed in nodes to GrpA
	for _, p := range grp {
		rpc := p.rpc.(*MockRpcDriver)
		atomic.StoreInt32(&rpc.membership, GRP_A)
	}
}

// Restore network from a split.
func mockRestoreNetwork() {
	mu.Lock()
	defer mu.Unlock()
	for _, p := range peers {
		rpc := p.rpc.(*MockRpcDriver)
		atomic.StoreInt32(&rpc.membership, NO_MEMBERSHIP)
	}
}

type MockRpcDriver struct {
	mu   sync.Mutex
	node *Node

	// For testing
	shouldFailInit bool
	closeCalled    bool
	shouldFailComm bool
	membership     int32
}

func NewMockRpc() *MockRpcDriver {
	return &MockRpcDriver{}
}

func (rpc *MockRpcDriver) Init(n *Node) error {
	if rpc.shouldFailInit {
		return errors.New("RPC Failed to Init")
	}
	// Redo the channels to be buffered since we could be
	// sending and block the select loops.
	cSize := n.ClusterInfo().Size
	n.VoteRequests = make(chan *pb.VoteRequest, cSize)
	n.VoteResponses = make(chan *pb.VoteResponse, cSize)
	n.HeartBeats = make(chan *pb.Heartbeat, cSize)

	mockRegisterPeer(n)
	rpc.node = n
	return nil
}

func (rpc *MockRpcDriver) Close() {
	rpc.closeCalled = true
	if rpc.node != nil {
		mockUnregisterPeer(rpc.node.id)
	}
}

// Test if we can talk to a peer
func (rpc *MockRpcDriver) commAllowed(peer *Node) bool {
	// Faked nodes
	if peer == nil || peer.rpc == nil {
		return true
	}
	// Might be fake node, so allow if not a MockRpcDriver
	peerRpc, ok := peer.rpc.(*MockRpcDriver)
	if !ok {
		return true
	}

	// Check to see if we are in same group as peer
	m1 := atomic.LoadInt32(&rpc.membership)
	m2 := atomic.LoadInt32(&peerRpc.membership)
	return m1 == m2
}

func (rpc *MockRpcDriver) RequestVote(vr *pb.VoteRequest) error {
	if rpc.isCommBlocked() {
		// Silent failure
		return nil
	}
	for _, p := range mockPeers() {
		if p.id != rpc.node.id && rpc.commAllowed(p) {
			p.VoteRequests <- vr
		}
	}
	return nil
}

func (rpc *MockRpcDriver) HeartBeat(hb *pb.Heartbeat) error {
	if rpc.isCommBlocked() {
		// Silent failure
		return nil
	}

	for _, p := range mockPeers() {
		if p.id != rpc.node.id && rpc.commAllowed(p) {
			p.HeartBeats <- hb
		}
	}
	return nil
}

func (rpc *MockRpcDriver) SendVoteResponse(candidate string, vresp *pb.VoteResponse) error {
	if rpc.isCommBlocked() {
		// Silent failure
		return nil
	}

	mu.Lock()
	p := peers[candidate]
	mu.Unlock()

	if p != nil && p.isRunning() && rpc.commAllowed(p) {
		p.VoteResponses <- vresp
	}
	return nil
}

func (rpc *MockRpcDriver) setCommBlocked(block bool) {
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	rpc.shouldFailComm = block
}

func (rpc *MockRpcDriver) isCommBlocked() bool {
	rpc.mu.Lock()
	defer rpc.mu.Unlock()
	return rpc.shouldFailComm
}
