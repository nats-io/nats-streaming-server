// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats/encoders/protobuf"
	"github.com/nats-io/graft/pb"
)

// The subject space for the nats rpc driver is based on the
// cluster name, which is filled in below on the heartbeats
// and vote requests. The vote responses are directed by
// using the node.Id().
const (
	HEARTBEAT_SUB = "graft.%s.heartbeat"
	VOTE_REQ_SUB  = "graft.%s.vote_request"
	VOTE_RESP_SUB = "graft.%s.vote_response"
)

var (
	NotInitializedErr = errors.New("graft(nats_rpc): Driver is not properly initialized")
)

// NatsRpcDriver is an implementation of the RPCDriver using NATS.
type NatsRpcDriver struct {
	sync.Mutex

	// NATS encoded connection.
	ec *nats.EncodedConn

	// Heartbeat subscription.
	hbSub *nats.Subscription

	// Vote request subscription.
	vreqSub *nats.Subscription

	// Vote response subscription.
	vrespSub *nats.Subscription

	// Graft node.
	node *Node
}

// NewNatsRpc creates a new instance of the driver. The NATS connection
// will use the options passed in.
func NewNatsRpc(opts *nats.Options) (*NatsRpcDriver, error) {
	nc, err := opts.Connect()
	if err != nil {
		return nil, err
	}
	ec, err := nats.NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER)
	if err != nil {
		return nil, err
	}
	return &NatsRpcDriver{ec: ec}, nil
}

// Init initializes the driver via the Graft node.
func (rpc *NatsRpcDriver) Init(n *Node) (err error) {
	rpc.node = n

	// Create the heartbeat subscription.
	hbSub := fmt.Sprintf(HEARTBEAT_SUB, n.ClusterInfo().Name)
	rpc.hbSub, err = rpc.ec.Subscribe(hbSub, rpc.HeartbeatCallback)
	if err != nil {
		return err
	}
	// Create the voteRequest subscription.
	rpc.vreqSub, err = rpc.ec.Subscribe(rpc.vreqSubject(), rpc.VoteRequestCallback)
	if err != nil {
		return err
	}
	return nil
}

// Close down the subscriptions and the NATS encoded connection.
// Will nil everything out.
func (rpc *NatsRpcDriver) Close() {
	rpc.Lock()
	defer rpc.Unlock()

	if rpc.hbSub != nil {
		rpc.hbSub.Unsubscribe()
		rpc.hbSub = nil
	}
	if rpc.vreqSub != nil {
		rpc.vreqSub.Unsubscribe()
		rpc.vreqSub = nil
	}
	if rpc.vrespSub != nil {
		rpc.vrespSub.Unsubscribe()
		rpc.vrespSub = nil
	}
	if rpc.ec != nil {
		rpc.ec.Close()
	}
}

// Convenience function for generating the directed response
// subject for vote requests. We will use the candidate's id
// to form a directed response
func (rpc *NatsRpcDriver) vrespSubject(candidate string) string {
	return fmt.Sprintf(VOTE_RESP_SUB, candidate)
}

// Convenience funstion for generating the vote request subject.
func (rpc *NatsRpcDriver) vreqSubject() string {
	return fmt.Sprintf(VOTE_REQ_SUB, rpc.node.ClusterInfo().Name)
}

// HeartbeatCallback will place the heartbeat on the Graft
// node's appropriate channel.
func (rpc *NatsRpcDriver) HeartbeatCallback(hb *pb.Heartbeat) {
	rpc.node.HeartBeats <- hb
}

// VoteRequestCallback will place the request on the Graft
// node's appropriate channel.
func (rpc *NatsRpcDriver) VoteRequestCallback(vreq *pb.VoteRequest) {
	// Don't respond to our own request.
	if vreq.Candidate != rpc.node.Id() {
		rpc.node.VoteRequests <- vreq
	}
}

// VoteResponseCallback will place the response on the Graft
// node's appropriate channel.
func (rpc *NatsRpcDriver) VoteResponseCallback(vresp *pb.VoteResponse) {
	rpc.node.VoteResponses <- vresp
}

// RequestVote is sent from the Graft node when it has become a
// candidate.
func (rpc *NatsRpcDriver) RequestVote(vr *pb.VoteRequest) error {
	rpc.Lock()
	defer rpc.Unlock()

	// Create a new response subscription for each outstanding
	// RequestVote and cancel the previous.
	if rpc.vrespSub != nil {
		rpc.vrespSub.Unsubscribe()
		rpc.vrespSub = nil
	}
	inbox := rpc.vrespSubject(rpc.node.Id())
	sub, err := rpc.ec.Subscribe(inbox, rpc.VoteResponseCallback)
	if err != nil {
		return err
	}
	// If we can auto-unsubscribe to max number of expected responses
	// which will be the cluster size.
	if size := rpc.node.ClusterInfo().Size; size > 0 {
		sub.AutoUnsubscribe(size)
	}
	// hold to cancel later.
	rpc.vrespSub = sub
	// Fire off the request.
	return rpc.ec.PublishRequest(rpc.vreqSubject(), inbox, vr)
}

// HeartBeat is called from the Graft node to send out a heartbeat
// while it is a LEADER.
func (rpc *NatsRpcDriver) HeartBeat(hb *pb.Heartbeat) error {
	rpc.Lock()
	defer rpc.Unlock()

	if rpc.hbSub == nil {
		return NotInitializedErr
	}
	return rpc.ec.Publish(rpc.hbSub.Subject, hb)
}

// SendVoteResponse is called from the Graft node to respond to a vote request.
func (rpc *NatsRpcDriver) SendVoteResponse(id string, vresp *pb.VoteResponse) error {
	rpc.Lock()
	defer rpc.Unlock()

	return rpc.ec.Publish(rpc.vrespSubject(id), vresp)
}
