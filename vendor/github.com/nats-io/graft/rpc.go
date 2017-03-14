// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"github.com/nats-io/graft/pb"
)

// An RPCDriver allows multiple transports to be utilized for the
// RAFT protocol RPCs. An instance of RPCDriver will use the *Node
// passed to Init() to call back into the Node when VoteRequests,
// VoteResponses and Heartbeat RPCs are received. They will be
// placed on the appropriate node's channels.
type RPCDriver interface {
	// Used to initialize the driver
	Init(*Node) error
	// Used to close down any state
	Close()
	// Used to respond to VoteResponses to candidates
	SendVoteResponse(candidate string, vresp *pb.VoteResponse) error
	// Used by Candidate Nodes to issue a new vote for a leader.
	RequestVote(*pb.VoteRequest) error
	// Used by Leader Nodes to Heartbeat
	HeartBeat(*pb.Heartbeat) error
}
