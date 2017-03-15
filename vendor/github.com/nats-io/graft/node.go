// Copyright 2013-2016 Apcera Inc. All rights reserved.

// Graft is a RAFT implementation.
// Currently only the election functionality is supported.
package graft

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/nats-io/graft/pb"
)

type Node struct {
	// Lock
	mu sync.Mutex

	// UUID
	id string

	// Info for the cluster
	info ClusterInfo

	// Current state
	state State

	// The RPC Driver
	rpc RPCDriver

	// Where we store the persistent state
	logPath string

	// Async handler
	handler Handler

	// Pending StateChange events
	stateChg []*StateChange

	// Pending Error events
	errors []error

	// Current leader
	leader string

	// Current term
	term uint64

	// Who we voted for in the current term.
	vote string

	// Election timer.
	electTimer *time.Timer

	// Channel to receive VoteRequests.
	VoteRequests chan *pb.VoteRequest

	// Channel to receive the VoteResponses.
	VoteResponses chan *pb.VoteResponse

	// Channel to receive Heartbeats.
	HeartBeats chan *pb.Heartbeat

	// quit channel for shutdown on Close().
	quit chan chan struct{}
}

// ClusterInfo expresses the name and expected
// size of the cluster.
type ClusterInfo struct {
	// The cluster's name
	Name string

	// Expected members
	Size int
}

// A Handler can process async callbacks from a Graft node.
type Handler interface {
	// Process async errors that are encountered by the node.
	AsyncError(error)

	// Process state changes.
	StateChange(from, to State)
}

// New will create a new Graft node. All arguments are required.
func New(info ClusterInfo, handler Handler, rpc RPCDriver, logPath string) (*Node, error) {

	// Check for correct Args
	if err := checkArgs(info, handler, rpc, logPath); err != nil {
		return nil, err
	}

	// Assign an Id() and start us as a FOLLOWER with no known LEADER.
	node := &Node{
		id:            genUUID(),
		info:          info,
		state:         FOLLOWER,
		rpc:           rpc,
		handler:       handler,
		leader:        NO_LEADER,
		quit:          make(chan chan struct{}),
		VoteRequests:  make(chan *pb.VoteRequest),
		VoteResponses: make(chan *pb.VoteResponse),
		HeartBeats:    make(chan *pb.Heartbeat),
	}

	// Init the log file and update our state.
	if err := node.initLog(logPath); err != nil {
		return nil, err
	}

	// Init the rpc driver
	if err := rpc.Init(node); err != nil {
		return nil, err
	}

	// Setup Timers
	node.setupTimers()

	// Loop
	go node.loop()

	return node, nil
}

func genUUID() string {
	u := make([]byte, 13)
	io.ReadFull(rand.Reader, u)
	return hex.EncodeToString(u)
}

// Convenience function for accessing the ClusterInfo.
func (n *Node) ClusterInfo() ClusterInfo {
	return n.info
}

// Convenience function for accessing the node's Id().
func (n *Node) Id() string {
	return n.id
}

func (n *Node) setupTimers() {
	// Election timer
	n.electTimer = time.NewTimer(randElectionTimeout())
}

func (n *Node) clearTimers() {
	if n.electTimer != nil {
		n.electTimer.Stop()
		n.electTimer = nil
	}
}

// Make sure we have all the arguments to create the Graft node.
func checkArgs(info ClusterInfo, handler Handler, rpc RPCDriver, logPath string) error {
	// Check ClusterInfo
	if info.Name == "" {
		return ClusterNameErr
	}
	if info.Size == 0 {
		return ClusterSizeErr
	}
	// Make sure we have non-nil args
	if handler == nil {
		return HandlerReqErr
	}
	if rpc == nil {
		return RpcDriverReqErr
	}
	if logPath == "" {
		return LogReqErr
	}
	return nil
}

// Mainloop that switches states and reacts to voteRequests and Heartbeats.
func (n *Node) loop() {
	for n.isRunning() {
		switch n.State() {
		case FOLLOWER:
			n.runAsFollower()
		case CANDIDATE:
			n.runAsCandidate()
		case LEADER:
			n.runAsLeader()
		}
	}
}

// isRunning returns whether we are still running.
// When Close() has been called this returns false.
func (n *Node) isRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state != CLOSED
}

// Process loop for a LEADER.
func (n *Node) runAsLeader() {
	// Setup our heartbeat ticker
	hb := time.NewTicker(HEARTBEAT_INTERVAL)
	defer hb.Stop()

	for {
		select {

		// Request to quit
		case q := <-n.quit:
			n.processQuit(q)
			return

		// Heartbeat tick. Send an HB each time.
		case <-hb.C:
			// Send a heartbeat
			n.rpc.HeartBeat(&pb.Heartbeat{Term: n.term, Leader: n.id})

		// A Vote Request.
		case vreq := <-n.VoteRequests:
			// We will stepdown if needed. This can happen if the
			// request is from a newer term than ours.
			if stepDown := n.handleVoteRequest(vreq); stepDown {
				n.switchToFollower(NO_LEADER)
				return
			}

		// Process another LEADER's heartbeat.
		case hb := <-n.HeartBeats:
			// If they are newer, we will step down.
			if stepDown := n.handleHeartBeat(hb); stepDown {
				n.switchToFollower(hb.Leader)
				return
			}
		}
	}
}

// Process loop for a CANDIDATE.
func (n *Node) runAsCandidate() {

	// Drain and previous responses.
	n.drainPreviousVoteResponses()

	// Initiate an Election
	vreq := &pb.VoteRequest{
		Term:      n.term,
		Candidate: n.id,
	}
	// Collect the votes.
	// We will vote for ourselves, so start at 1.
	votes := 1

	// Vote for ourself.
	n.setVote(n.id)

	// Save our state.
	if err := n.writeState(); err != nil {
		n.handleError(err)
		n.switchToFollower(NO_LEADER)
		return
	}

	// Send the vote request to other members
	n.rpc.RequestVote(vreq)

	// Check to see if we have already won.
	if n.wonElection(votes) {
		// Become LEADER if we have won.
		n.switchToLeader()
		return
	}

	for {
		select {

		// Request to quit
		case q := <-n.quit:
			n.processQuit(q)
			return

		// An ElectionTimeout causes us to go back into a Candidate
		// state and start a new election.
		case <-n.electTimer.C:
			n.switchToCandidate()
			return

		// A response to our votes.
		case vresp := <-n.VoteResponses:
			// We have a VoteResponse. Only process if
			// it is for our term and Granted is true.
			if vresp.Granted && vresp.Term == n.term {
				votes++
				if n.wonElection(votes) {
					// Become LEADER if we have won.
					n.switchToLeader()
					return
				}
			}

		// A Vote Request.
		case vreq := <-n.VoteRequests:
			// We will stepdown if needed. This can happen if the
			// request is from a newer term than ours.
			if stepDown := n.handleVoteRequest(vreq); stepDown {
				n.switchToFollower(NO_LEADER)
				return
			}

		// Process a LEADER's heartbeat.
		case hb := <-n.HeartBeats:
			// If they are newer, we will step down.
			if stepDown := n.handleHeartBeat(hb); stepDown {
				n.switchToFollower(hb.Leader)
				return
			}
		}
	}
}

// Process loop for a FOLLOWER.
func (n *Node) runAsFollower() {
	for {
		select {

		// Request to quit
		case q := <-n.quit:
			n.processQuit(q)
			return

		// An ElectionTimeout causes us to go into a Candidate state
		// and start a new election.
		case <-n.electTimer.C:
			n.switchToCandidate()
			return

		// A Vote Request.
		case vreq := <-n.VoteRequests:
			if shouldReturn := n.handleVoteRequest(vreq); shouldReturn {
				return
			}

		// Process a LEADER's heartbeat.
		case hb := <-n.HeartBeats:
			// Set the Leader regardless if we currently have none set.
			if n.leader == NO_LEADER {
				n.setLeader(hb.Leader)
			}
			// Just set Leader if asked to stepdown.
			if stepDown := n.handleHeartBeat(hb); stepDown {
				n.setLeader(hb.Leader)
			}
		}
	}
}

// postError invokes handler.AsyncError() in a go routine.
// When the handler call returns, and if there are still pending errors,
// this function will recursively call itself with the first element in
// the list.
func (n *Node) postError(err error) {
	go func() {
		n.handler.AsyncError(err)
		n.mu.Lock()
		n.errors = n.errors[1:]
		if len(n.errors) > 0 {
			err := n.errors[0]
			n.postError(err)
		}
		n.mu.Unlock()
	}()
}

// Send the error to the async handler.
func (n *Node) handleError(err error) {
	n.mu.Lock()
	n.errors = append(n.errors, err)
	// Call postError only for the first error added.
	// Check postError for details.
	if len(n.errors) == 1 {
		n.postError(err)
	}
	n.mu.Unlock()
}

// handleHeartBeat is called to process a heartbeat from a LEADER.
// We will indicate to the controlling process loop if we should
// "stepdown" from our current role.
func (n *Node) handleHeartBeat(hb *pb.Heartbeat) bool {

	// Ignore old term
	if hb.Term < n.term {
		return false
	}

	// Save state flag
	saveState := false

	// This will trigger a return from the current runAs loop.
	stepDown := false

	// Newer term
	if hb.Term > n.term {
		n.term = hb.Term
		n.vote = NO_VOTE
		stepDown = true
		saveState = true
	}

	// If we are candidate and someone asserts they are leader for an equal or
	// higher term, step down.
	if n.State() == CANDIDATE && hb.Term >= n.term {
		n.term = hb.Term
		n.vote = NO_VOTE
		stepDown = true
		saveState = true
	}

	// Reset the election timer.
	n.resetElectionTimeout()

	// Write our state if needed.
	if saveState {
		if err := n.writeState(); err != nil {
			n.handleError(err)
			stepDown = true
		}
	}

	return stepDown
}

// handleVoteRequest will process a vote request and either
// deny or grant our own vote to the caller.
func (n *Node) handleVoteRequest(vreq *pb.VoteRequest) bool {

	deny := &pb.VoteResponse{Term: n.term, Granted: false}

	// Old term, reject
	if vreq.Term < n.term {
		n.rpc.SendVoteResponse(vreq.Candidate, deny)
		return false
	}

	// Save state flag
	saveState := false

	// This will trigger a return from the current runAs loop.
	stepDown := false

	// Newer term
	if vreq.Term > n.term {
		n.term = vreq.Term
		n.vote = NO_VOTE
		n.leader = NO_LEADER
		stepDown = true
		saveState = true
	}

	// If we are the Leader, deny request unless we have seen
	// a newer term and must step down.
	if n.State() == LEADER && !stepDown {
		n.rpc.SendVoteResponse(vreq.Candidate, deny)
		return stepDown
	}

	// If we have already cast a vote for this term, reject.
	if n.vote != NO_VOTE && n.vote != vreq.Candidate {
		n.rpc.SendVoteResponse(vreq.Candidate, deny)
		return stepDown
	}

	// We will vote for this candidate.

	n.setVote(vreq.Candidate)

	// Write our state if needed.
	if saveState {
		if err := n.writeState(); err != nil {
			// We have failed to update our state. Process the error
			// and deny the vote.
			n.handleError(err)
			n.setVote(NO_VOTE)
			n.rpc.SendVoteResponse(vreq.Candidate, deny)
			n.resetElectionTimeout()
			return true
		}
	}

	// Send our acceptance.
	accept := &pb.VoteResponse{Term: n.term, Granted: true}
	n.rpc.SendVoteResponse(vreq.Candidate, accept)

	// Reset ElectionTimeout
	n.resetElectionTimeout()

	return stepDown
}

// wonElection returns a bool to determine if we have a
// majority of the votes.
func (n *Node) wonElection(votes int) bool {
	return votes >= quorumNeeded(n.info.Size)
}

// Return the quorum size for a given cluster config.
func quorumNeeded(clusterSize int) int {
	switch clusterSize {
	// Handle 0, but 0 is really an invalid cluster size.
	case 0:
		return 0
	default:
		return clusterSize/2 + 1
	}
}

// Switch to a FOLLOWER.
func (n *Node) switchToFollower(leader string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leader = leader
	n.switchState(FOLLOWER)
}

// Switch to a LEADER.
func (n *Node) switchToLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leader = n.id
	n.switchState(LEADER)
}

// Switch to a CANDIDATE.
func (n *Node) switchToCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()
	// Increment the term.
	n.term++
	// Clear current Leader.
	n.leader = NO_LEADER
	n.resetElectionTimeout()
	n.switchState(CANDIDATE)
}

// postStateChange invokes handler.StateChange() in a go routine.
// When the handler call returns, and if there are still pending state
// changes, this function will recursively call itself with the first
// element in the list.
func (n *Node) postStateChange(sc *StateChange) {
	go func() {
		n.handler.StateChange(sc.From, sc.To)
		n.mu.Lock()
		n.stateChg = n.stateChg[1:]
		if len(n.stateChg) > 0 {
			sc := n.stateChg[0]
			n.postStateChange(sc)
		}
		n.mu.Unlock()
	}()
}

// Process a state transistion. Assume lock is held on entrance.
// Call the async handler in a separate Go routine.
func (n *Node) switchState(state State) {
	if state == n.state {
		return
	}
	old := n.state
	n.state = state
	sc := &StateChange{From: old, To: state}
	n.stateChg = append(n.stateChg, sc)
	// Invoke postStateChange only for the first state change added.
	// Check postStateChange for details.
	if len(n.stateChg) == 1 {
		n.postStateChange(sc)
	}
}

// Reset the election timeout with a random value.
func (n *Node) resetElectionTimeout() {
	n.electTimer.Reset(randElectionTimeout())
}

// Generate a random timeout between MIN and MAX Election timeouts.
// The randomness is required for the RAFT algorithm to be stable.
func randElectionTimeout() time.Duration {
	delta := mrand.Int63n(int64(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT))
	return (MIN_ELECTION_TIMEOUT + time.Duration(delta))
}

// processQuit will change or internal state to CLOSED and will close the
// received channel to release anyone waiting on it.
func (n *Node) processQuit(q chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = CLOSED
	close(q)
}

// waitOnLoopFinish will block until the loops are exiting.
func (n *Node) waitOnLoopFinish() {
	q := make(chan struct{})
	n.quit <- q
	<-q
}

// drainPreviousVoteResponses will remove any previous responses
// that were queued up and waiting.
func (n *Node) drainPreviousVoteResponses() {
	select {
	case <-n.VoteResponses:
	default:
		return
	}
}

// Close will shutdown the Graft node and wait until the
// state is processed. We will clear timers, channels, etc.
// and close the log.
func (n *Node) Close() {
	if n.State() == CLOSED {
		return
	}
	n.rpc.Close()
	n.waitOnLoopFinish()
	n.clearTimers()
	n.closeLog()
}

// Return the current state.
func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) setLeader(newLeader string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leader = newLeader
}

func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

func (n *Node) setTerm(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.term = term
}

func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

func (n *Node) setVote(candidate string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.vote = candidate
}

func (n *Node) CurrentVote() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.vote
}

func (n *Node) LogPath() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.logPath
}
