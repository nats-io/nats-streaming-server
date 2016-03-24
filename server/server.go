// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"
	"github.com/nats-io/nuid"
	"github.com/nats-io/stan/pb"

	natsd "github.com/nats-io/gnatsd/test"
)

// A single STAN server

const (
	DefaultDiscoverPrefix = "_STAN.discover"
	DefaultPubPrefix      = "_STAN.pub"
	DefaultSubPrefix      = "_STAN.sub"
	DefaultUnSubPrefix    = "_STAN.unsub"
	DefaultClosePrefix    = "_STAN.close"

	// How many messages per channel will we store?
	DefaultMsgStoreLimit = 1000000
	// How many channels (literal subjects) do we allow?
	DefaultChannelLimit = 100

	// Heartbeat intervals.
	DefaultHeartBeatInterval   = 30 * time.Second
	DefaultClientHBTimeout     = 10 * time.Second
	DefaultMaxFailedHeartBeats = int((5 * time.Minute) / DefaultHeartBeatInterval)
)

// Errors.
var (
	ErrBadPubMsg       = errors.New("stan: malformed publish message envelope")
	ErrBadSubRequest   = errors.New("stan: malformed subscription request")
	ErrInvalidSubject  = errors.New("stan: invalid subject")
	ErrInvalidSequence = errors.New("stan: invalid start sequence")
	ErrInvalidTime     = errors.New("stan: invalid start time")
	ErrInvalidSub      = errors.New("stan: invalid subscription")
	ErrInvalidConnReq  = errors.New("stan: invalid connection request")
	ErrInvalidClient   = errors.New("stan: clientID already registered")
	ErrInvalidCloseReq = errors.New("stan: invalid close request")
	ErrInvalidAckWait  = errors.New("stan: invalid ack wait time, should be >= 1s")
	ErrDupDurable      = errors.New("stan: duplicate durable registration")
	ErrDurableQueue    = errors.New("stan: queue subscribers can't be durable")
)

type StanServer struct {
	clusterID     string
	serverID      string
	pubPrefix     string // Subject prefix we received published messages on.
	subRequests   string // Subject we receive subscription requests on.
	unsubRequests string // Subject we receive unsubscribe requests on.
	closeRequests string // Subject we receive close requests on.
	natsServer    *server.Server
	opts          *ServerOptions
	nc            *nats.Conn

	// Clients
	clients *clientStore

	// Channels
	channels *channelMap
}

// Map from subject to channelStore
type channelMap struct {
	sync.RWMutex
	channels map[string]*channelStore
}

// channelStore holds our known state of all messages and subscribers for a given channel/subject.
type channelStore struct {
	subs *subStore // All subscribers
	msgs *msgStore // All messages
}

// subStore holds all known state for all subscriptions
type subStore struct {
	sync.RWMutex
	psubs    []*subState            // plain subscribers
	qsubs    map[string]*queueState // queue subscribers
	durables map[string]*subState   // durables lookup
	acks     map[string]*subState   // ack inbox lookup
}

// Holds all queue subsribers for a subject/group and
// tracks lastSent for the group.
type queueState struct {
	sync.RWMutex
	lastSent uint64
	subs     []*subState
	stalled  bool
}

// Holds Subscription state
// FIXME(dlc) - Use embedded proto
type subState struct {
	sync.RWMutex
	clientID      string
	subject       string
	qgroup        string
	inbox         string
	ackInbox      string
	durableName   string
	qstate        *queueState
	lastSent      uint64
	ackWaitInSecs time.Duration
	ackTimer      *time.Timer
	ackTimeFloor  int64
	ackSub        *nats.Subscription
	acksPending   map[uint64]*pb.MsgProto
	maxInFlight   int
	stalled       bool
}

// Lookup or create a channel by subject
func (cm *channelMap) LookupOrCreate(subject string) *channelStore {
	cs := cm.Lookup(subject)
	if cs == nil {
		cs = cm.New(subject)
	}
	return cs
}

// Lookup a channel by subject
func (cm *channelMap) Lookup(subject string) *channelStore {
	cm.RLock()
	defer cm.RUnlock()
	return cm.channels[subject]
}

// Create a new channel for the given subject
func (cm *channelMap) New(subject string) *channelStore {
	cm.Lock()
	defer cm.Unlock()
	cs := &channelStore{
		msgs: &msgStore{
			subject: subject,
			first:   1,
			last:    0,
			msgs:    make(map[uint64]*pb.MsgProto, DefaultMsgStoreLimit),
		},
		subs: &subStore{
			psubs:    make([]*subState, 0, 4),
			qsubs:    make(map[string]*queueState),
			durables: make(map[string]*subState),
			acks:     make(map[string]*subState),
		},
	}
	cm.channels[subject] = cs
	return cs
}

func (ss *subStore) Store(sub *subState) {
	if sub == nil {
		return
	}
	sub.RLock()
	ackInbox := sub.ackInbox
	qgroup := sub.qgroup
	isDurable := sub.isDurable()
	sub.RUnlock()

	ss.Lock()
	defer ss.Unlock()

	// First store by ackInbox for ack direct lookup
	ss.acks[ackInbox] = sub

	// Store by type
	if qgroup != "" {
		// Queue subscriber.
		qs := ss.qsubs[qgroup]
		if qs == nil {
			qs = &queueState{
				subs: make([]*subState, 0, 4),
			}
			ss.qsubs[qgroup] = qs
		}
		qs.subs = append(qs.subs, sub)
		sub.qstate = qs
	} else {
		// Plain subscriber.
		ss.psubs = append(ss.psubs, sub)
	}

	// Hold onto durables in special lookup.
	if isDurable {
		ss.durables[sub.durableKey()] = sub
	}
}

// Remove
func (ss *subStore) Remove(sub *subState) {
	if sub == nil {
		return
	}

	sub.Lock()
	// Clear the subscriptions clientID
	sub.clientID = ""
	sub.ackSub.Unsubscribe()
	ackInbox := sub.ackInbox
	qs := sub.qstate
	durable := sub.durableName
	sub.Unlock()

	ss.Lock()
	defer ss.Unlock()

	// Delete from ackInbox lookup.
	delete(ss.acks, ackInbox)

	// Delete from durable if needed
	if durable != "" {
		delete(ss.durables, durable)
	}

	// Delete ourselves from the list
	if qs != nil {
		qs.subs = sub.deleteFromList(qs.subs)
	} else {
		ss.psubs = sub.deleteFromList(ss.psubs)
	}
}

// Lookup by durable name.
func (ss *subStore) LookupByDurable(durableName string) *subState {
	ss.RLock()
	defer ss.RUnlock()
	return ss.durables[durableName]
}

// Lookup by ackInbox name.
func (ss *subStore) LookupByAckInbox(ackInbox string) *subState {
	ss.RLock()
	defer ss.RUnlock()
	return ss.acks[ackInbox]
}

// Per channel/subject message store
type msgStore struct {
	sync.RWMutex
	subject string // Can't be wildcard
	first   uint64
	last    uint64
	msgs    map[uint64]*pb.MsgProto
}

// Store a given message
func (ms *msgStore) Store(subject, reply string, data []byte) (*pb.MsgProto, error) {
	ms.Lock()
	defer ms.Unlock()

	ms.last++
	m := &pb.MsgProto{
		Sequence:  ms.last,
		Subject:   subject,
		Reply:     reply,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}
	ms.msgs[ms.last] = m

	// Check if we need to remove any.
	if len(ms.msgs) > DefaultMsgStoreLimit {
		Errorf("WARNING: Removing message[%d] from the store for [`%s`]", ms.first, subject)
		delete(ms.msgs, ms.first)
		ms.first++
	}

	return m, nil
}

// Return sequence for first message stored.
func (ms *msgStore) FirstSequence() uint64 {
	ms.RLock()
	defer ms.RUnlock()
	return ms.first
}

// Return sequence for last message stored.
func (ms *msgStore) LastSequence() uint64 {
	ms.RLock()
	defer ms.RUnlock()
	return ms.last
}

// Lookup by sequence number.
func (ms *msgStore) Lookup(seq uint64) *pb.MsgProto {
	ms.RLock()
	defer ms.RUnlock()
	return ms.msgs[seq]
}

func (ms *msgStore) FirstMsg() *pb.MsgProto {
	ms.RLock()
	defer ms.RUnlock()
	return ms.msgs[ms.first]
}

func (ms *msgStore) LastMsg() *pb.MsgProto {
	ms.RLock()
	defer ms.RUnlock()
	return ms.msgs[ms.last]
}

// ServerOptions
type ServerOptions struct {
	DiscoverPrefix string
}

// Set the default discover prefix.
var DefaultServerOptions = ServerOptions{
	DiscoverPrefix: DefaultDiscoverPrefix,
}

func stanDisconnectedHandler(nc *nats.Conn) {
	Errorf("STAN: connection has been disconnected: %s.", nc.LastError())
}

func stanClosedHandler(nc *nats.Conn) {
	Debugf("STAN: connection has been closed.")
}

func stanErrorHandler(nc *nats.Conn, sub *nats.Subscription, err error) {
	Errorf("STAN: Asynchronous error on subject %s: %s.", sub.Subject, err)
}

// Convenience API to set the default logger.
func EnableDefaultLogger(opts *server.Options) {
	//	var log natsd.Logger
	colors := true
	// Check to see if stderr is being redirected and if so turn off color
	// Also turn off colors if we're running on Windows where os.Stderr.Stat() returns an invalid handle-error
	stat, err := os.Stderr.Stat()
	if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
		colors = false
	}
	log := logger.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, colors, true)

	var s *server.Server
	s.SetLogger(log, opts.Debug, opts.Trace)
}

// RunServer will startup and embedded STAN server and a nats-server to support it.
func RunServer(ID string, optsA ...*server.Options) *StanServer {
	// Run a nats server by default
	s := StanServer{clusterID: ID, serverID: nuid.Next(), opts: &DefaultServerOptions}

	// Create clientStore
	s.clients = &clientStore{clients: make(map[string]*client)}

	// Create channelMap
	s.channels = &channelMap{channels: make(map[string]*channelStore)}

	// Generate Subjects
	// FIXME(dlc) guid needs to be shared in cluster mode
	s.pubPrefix = fmt.Sprintf("%s.%s", DefaultPubPrefix, nuid.Next())
	s.subRequests = fmt.Sprintf("%s.%s", DefaultSubPrefix, nuid.Next())
	s.unsubRequests = fmt.Sprintf("%s.%s", DefaultUnSubPrefix, nuid.Next())
	s.closeRequests = fmt.Sprintf("%s.%s", DefaultClosePrefix, nuid.Next())

	// hack
	var opts *server.Options
	if len(optsA) > 0 {
		opts = optsA[0]
	} else {
		opts = &natsd.DefaultTestOptions
	}
	noLog = opts.NoLog

	if opts.Host == "" {
		opts.Host = "localhost"
	}

	s.natsServer = natsd.RunServer(opts)

	natsURL := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	var err error
	if s.nc, err = nats.Connect(natsURL); err != nil {
		panic(fmt.Sprintf("Can't connect to NATS server: %v\n", err))
	}

	nats.DisconnectHandler(stanDisconnectedHandler)
	nats.ErrorHandler(stanErrorHandler)
	nats.ClosedHandler(stanClosedHandler)

	s.initSubscriptions()

	Noticef("STAN: Message store is MEMORY")
	Noticef("STAN: Maximum of %d will be stored", DefaultMsgStoreLimit)

	return &s
}

// initSubscriptions will setup initial subscriptions for discovery etc.
func (s *StanServer) initSubscriptions() {
	// Listen for connection requests.
	discoverSubject := fmt.Sprintf("%s.%s", s.opts.DiscoverPrefix, s.clusterID)
	_, err := s.nc.Subscribe(discoverSubject, s.connectCB)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to discover subject, %v\n", err))
	}
	// Receive published messages from clients.
	pubSubject := fmt.Sprintf("%s.>", s.pubPrefix)
	_, err = s.nc.Subscribe(pubSubject, s.processClientPublish)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to publish subject, %v\n", err))
	}
	// Receive subscription requests from clients.
	_, err = s.nc.Subscribe(s.subRequests, s.processSubscriptionRequest)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to subscribe request subject, %v\n", err))
	}
	// Receive unsubscribe requests from clients.
	_, err = s.nc.Subscribe(s.unsubRequests, s.processUnSubscribeRequest)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to unsubscribe request subject, %v\n", err))
	}
	// Receive close requests from clients.
	_, err = s.nc.Subscribe(s.closeRequests, s.processCloseRequest)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to close request subject, %v\n", err))
	}
	// Flush to make sure those subscriptions are processed before the upper layer
	// returns to the user.
	err = s.nc.Flush()
	if err != nil {
		panic(fmt.Sprintf("Could not flush the request subscriptions, %v\n", err))
	}

	Debugf("STAN: discover subject: %s", discoverSubject)
	Debugf("STAN: publish subject:  %s", pubSubject)
	Debugf("STAN: subcribe subject: %s", s.subRequests)
	Debugf("STAN: unsub subject:    %s", s.unsubRequests)
	Debugf("STAN: close subject:    %s", s.closeRequests)

}

// Process a client connect request
func (s *StanServer) connectCB(m *nats.Msg) {
	req := &pb.ConnectRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil || req.ClientID == "" || req.HeartbeatInbox == "" {
		cr := &pb.ConnectResponse{Error: ErrInvalidConnReq.Error()}
		b, _ := cr.Marshal()
		s.nc.Publish(m.Reply, b)
		return
	}

	// Check if already connected.
	if c := s.clients.Lookup(req.ClientID); c != nil {
		cr := &pb.ConnectResponse{Error: ErrInvalidClient.Error()}
		b, _ := cr.Marshal()
		s.nc.Publish(m.Reply, b)
		Debugf("STAN: [Client:%s] Connect failed; already connected.", c.clientID)
		return
	}

	// Register the new connection.
	client := &client{
		clientID: req.ClientID,
		hbInbox:  req.HeartbeatInbox,
		subs:     make([]*subState, 0, 4),
	}
	s.clients.Register(client)

	// Respond with our ConnectResponse
	cr := &pb.ConnectResponse{
		PubPrefix:     s.pubPrefix,
		SubRequests:   s.subRequests,
		UnsubRequests: s.unsubRequests,
		CloseRequests: s.closeRequests,
	}
	b, _ := cr.Marshal()
	s.nc.Publish(m.Reply, b)

	// Heartbeat timer.
	client.Lock()
	client.hbt = time.AfterFunc(DefaultHeartBeatInterval, func() { s.checkClientHealth(client.clientID) })
	client.Unlock()

	Debugf("STAN: [Client:%s] connected.", client.clientID)
}

// Send a heartbeat call to the client.
func (s *StanServer) checkClientHealth(clientID string) {
	client := s.clients.Lookup(clientID)
	if client == nil {
		return
	}
	client.Lock()
	defer client.Unlock()

	_, err := s.nc.Request(client.hbInbox, nil, DefaultClientHBTimeout)
	if err != nil {
		client.fhb++
		if client.fhb > DefaultMaxFailedHeartBeats { // 5 minutes
			Debugf("STAN: [Client:%s]  Timed out on hearbeats.", client.clientID)
			defer s.closeClient(client.clientID)
		}
	} else {
		client.fhb = 0
	}
	client.hbt.Reset(DefaultHeartBeatInterval)
}

// Close a client
func (s *StanServer) closeClient(clientID string) {
	// Remove all non-durable subscribers.
	s.removeAllNonDurableSubscribers(clientID)

	// Remove from our clientStore
	s.clients.Unregister(clientID)

	Debugf("STAN: [Client:%s] Closed.", clientID)
}

// processCloseRequest process inbound messages from clients.
func (s *StanServer) processCloseRequest(m *nats.Msg) {
	req := &pb.CloseRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		Errorf("STAN: Received invalid close request, subject=%s.", m.Subject)
		resp := &pb.CloseResponse{Error: ErrInvalidCloseReq.Error()}
		if b, err := resp.Marshal(); err != nil {
			s.nc.Publish(m.Reply, b)
		}
	}

	s.closeClient(req.ClientID)

	resp := &pb.CloseResponse{}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)
}

// processClientPublish process inbound messages from clients.
func (s *StanServer) processClientPublish(m *nats.Msg) {
	pm := &pb.PubMsg{}
	pm.Unmarshal(m.Data)

	// TODO (cls) error check.

	// Make sure we have a clientID, guid, etc.
	if pm.Guid == "" || !s.isValidClient(pm.ClientID) || !isValidSubject(pm.Subject) {
		Errorf("STAN: Received invalid client publish message %v.", pm)
		badMsgAck := &pb.PubAck{Guid: pm.Guid, Error: ErrBadPubMsg.Error()}
		if b, err := badMsgAck.Marshal(); err == nil {
			s.nc.Publish(m.Reply, b)
		}
		return
	}

	////////////////////////////////////////////////////////////////////////////
	// This is where we will store the message and wait for others in the
	// potential cluster to do so as well, once we have a quorom someone can
	// ack the publisher. We simply do so here for now.
	////////////////////////////////////////////////////////////////////////////

	s.ackPublisher(pm, m.Reply)

	////////////////////////////////////////////////////////////////////////////
	// Once we have ack'd the publisher, we need to assign this a sequence ID.
	// This will be done by a master election within the cluster, for now we
	// assume we are the master and assign the sequence ID here.
	////////////////////////////////////////////////////////////////////////////

	cs := s.assignAndStore(pm)

	////////////////////////////////////////////////////////////////////////////
	// Now trigger sends to any active subscribers
	////////////////////////////////////////////////////////////////////////////

	s.processMsg(cs)
}

// FIXME(dlc) - place holder to pick sub that has least outstanding, should just sort,
// or use insertion sort, etc.
func findBestQueueSub(sl []*subState) (rsub *subState) {
	for _, sub := range sl {

		if rsub == nil {
			rsub = sub
			continue
		}

		rsub.RLock()
		rOut := len(rsub.acksPending)
		rsub.RUnlock()

		sub.Lock()
		sOut := len(sub.acksPending)
		sub.Unlock()

		if sOut < rOut {
			rsub = sub
		}
	}
	return
}

// Send a message to the queue group
// Assumes subStore lock is held
// Assumes qs lock held for write
func (s *StanServer) sendMsgToQueueGroup(qs *queueState, m *pb.MsgProto) bool {
	if qs == nil {
		return false
	}
	sub := findBestQueueSub(qs.subs)
	if sub == nil {
		return false
	}
	sub.Lock()
	didSend := s.sendMsgToSubAndUpdateLastSent(sub, m)
	lastSent := sub.lastSent
	sub.Unlock()
	if !didSend {
		qs.stalled = true
		return false
	}
	if lastSent > qs.lastSent {
		qs.lastSent = lastSent
	}
	return true
}

// processMsg will proces a message, and possibly send to clients, etc.
func (s *StanServer) processMsg(cs *channelStore) {
	ss := cs.subs

	// Since we iterate through them all.
	ss.RLock()
	defer ss.RUnlock()

	// Walk the plain subscribers and deliver to each one
	for _, sub := range ss.psubs {
		s.sendAvailableMessages(cs, sub)
	}

	// Check the queue subscribers
	for _, qs := range ss.qsubs {
		s.sendAvailableMessagesToQueue(cs, qs)
	}
}

// Used for sorting by sequence
type bySeq []*pb.MsgProto

func (a bySeq) Len() int           { return (len(a)) }
func (a bySeq) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySeq) Less(i, j int) bool { return a[i].Sequence < a[j].Sequence }

func makeSortedMsgs(msgs map[uint64]*pb.MsgProto) []*pb.MsgProto {
	results := make([]*pb.MsgProto, 0, len(msgs))
	for _, m := range msgs {
		mCopy := *m // copy since we need to set redelivered flag.
		results = append(results, &mCopy)
	}
	sort.Sort(bySeq(results))
	return results
}

// Redeliver all outstanding messages to a durable subscriber, used on resubscribe.
func (s *StanServer) performDurableRedelivery(sub *subState) {
	Debugf("STAN:[Client:%s] Redelivering to durable %s.", sub.clientID, sub.durableName)
	s.performRedelivery(sub, false)
}

// Redeliver all outstanding messages that have expired.
func (s *StanServer) performAckExpirationRedelivery(sub *subState) {
	Debugf("STAN: [Client:%s] Redelivering on ack expiration. subject=%s, inbox=%s.",
		sub.clientID, sub.subject, sub.inbox)
	s.performRedelivery(sub, true)
}

// Performs redelivery, takes a flag on whether to honor expiration.
func (s *StanServer) performRedelivery(sub *subState, checkExpiration bool) {
	// Sort our messages outstanding from acksPending, grab some state and unlock.
	sub.RLock()
	expTime := int64(sub.ackWaitInSecs * time.Second)
	sortedMsgs := makeSortedMsgs(sub.acksPending)
	subject := sub.subject
	qs := sub.qstate
	clientID := sub.clientID
	floorTimestamp := sub.ackTimeFloor
	sub.RUnlock()

	// If the client has some failed heartbeats, ignore this request.
	client := s.clients.Lookup(clientID)
	if client == nil {
		return
	}
	client.RLock()
	fhbs := client.fhb
	client.RUnlock()
	if fhbs != 0 {
		return
	}

	var pick *subState

	now := time.Now().UnixNano()

	// We will move through acksPending(sorted) and see what needs redelivery.
	for _, m := range sortedMsgs {
		if checkExpiration {
			// Ignore messages with a timestamp below our floor
			if floorTimestamp > 0 && floorTimestamp > m.Timestamp {
				continue
			}

			if m.Timestamp+expTime > now {
				// the messages are ordered by seq so the expiration
				// times are ascending.  Once we've get here, we've hit an
				// unexpired message, and we're done. Reset the sub's ack
				// timer to fire on the next message expiration.
				Tracef("STAN: [Client:%s] redelivery, skipping seqno=%d.", clientID, m.Sequence)
				sub.adjustAckTimer(m.Timestamp)
				return
			}
		}

		Tracef("STAN: [Client:%s] redelivery, sending seqno=%d.", clientID, m.Sequence)

		// Flag as redelivered.
		m.Redelivered = true

		// Handle QueueSubscribers differently, since we will choose best subscriber
		// to redeliver to, not necessarily the same one.
		if qs != nil {
			// Remove from current subs acksPending.
			sub.Lock()
			delete(sub.acksPending, m.Sequence)
			sub.Unlock()

			cs := s.channels.Lookup(subject)
			ss := cs.subs

			ss.RLock()
			pick = findBestQueueSub(qs.subs)
			ss.RUnlock()

			if pick == nil {
				Errorf("STAN: [Client:%s] Unable to find queue subscriber.", clientID)
				break
			}
		} else {
			pick = sub
		}

		pick.Lock()
		s.sendMsgToSub(pick, m)
		pick.Unlock()
	}

	if checkExpiration {
		// The messages from sortedMsgs above may have all been acknowledged
		// by now, but we are going to set the timer based on the oldest on
		// that list, which is the sooner the timer should fire anyway.
		// The timer will correctly be adjusted.

		firstUnacked := int64(0)

		// Because of locking and timing, it's possible that the sortedMsgs
		// map was empty even on entry.
		if len(sortedMsgs) > 0 {
			firstUnacked = sortedMsgs[0].Timestamp
		}

		// Adjust the timer
		sub.adjustAckTimer(firstUnacked)
	}
}

// Sends the message to the subscriber
// Sub lock should be held before calling.
func (s *StanServer) sendMsgToSub(sub *subState, m *pb.MsgProto) bool {
	if sub == nil || m == nil {
		return false
	}

	Tracef("STAN: [Client:%s] Sending msg subject=%s inbox=%s seqno=%d.",
		sub.clientID, m.Subject, sub.inbox, m.Sequence)

	// Don't send if we have too many outstanding already.
	if len(sub.acksPending) >= sub.maxInFlight {
		sub.stalled = true
		Debugf("STAN: [Client:%s] Stalled msgseq %s:%d to %s.",
			sub.clientID, m.Subject, m.Sequence, sub.inbox)
		return false
	}

	b, _ := m.Marshal()
	if err := s.nc.Publish(sub.inbox, b); err != nil {
		Errorf("STAN: [Client:%s] Failed Sending msgseq %s:%d to %s (%s).",
			sub.clientID, m.Subject, m.Sequence, sub.inbox, err)
		return false
	}
	// Store in ackPending.
	sub.acksPending[m.Sequence] = m

	// Setup the ackTimer as needed.
	if sub.ackTimer == nil {
		sub.ackTimer = time.AfterFunc(sub.ackWaitInSecs*time.Second, func() {
			s.performAckExpirationRedelivery(sub)
		})
		sub.ackTimeFloor = m.Timestamp
	}

	return true
}

// Sends the message to the subscriber and updates the subscriber's lastSent field
// Sub lock should be held before calling.
func (s *StanServer) sendMsgToSubAndUpdateLastSent(sub *subState, m *pb.MsgProto) bool {
	if s.sendMsgToSub(sub, m) {
		sub.lastSent = m.Sequence
		return true
	}
	return false
}

// assignAndStore will assign a sequence ID and then store the message.
func (s *StanServer) assignAndStore(pm *pb.PubMsg) *channelStore {
	cs := s.channels.LookupOrCreate(pm.Subject)
	// FIXME(dlc) - check for errors.
	cs.msgs.Store(pm.Subject, pm.Reply, pm.Data)
	return cs
}

// ackPublisher sends the ack for a message.
func (s *StanServer) ackPublisher(pm *pb.PubMsg, reply string) {
	msgAck := &pb.PubAck{Guid: pm.Guid}
	var buf [32]byte
	b := buf[:]
	n, _ := msgAck.MarshalTo(b)
	Tracef("STAN: [Client:%s] Acking Publisher subj=%s guid=%s", pm.ClientID, pm.Subject, pm.Guid)
	s.nc.Publish(reply, b[:n])
}

// Delete a sub from a given list.
func (sub *subState) deleteFromList(sl []*subState) []*subState {
	for i := 0; i < len(sl); i++ {
		if sl[i] == sub {
			sl[i] = sl[len(sl)-1]
			sl[len(sl)-1] = nil
			sl = sl[:len(sl)-1]
			return shrinkSubListIfNeeded(sl)
		}
	}
	return sl
}

// Checks if we need to do a resize. This is for very large growth then
// subsequent return to a more normal size.
func shrinkSubListIfNeeded(sl []*subState) []*subState {
	lsl := len(sl)
	csl := cap(sl)
	// Don't bother if list not too big
	if csl <= 8 {
		return sl
	}
	pFree := float32(csl-lsl) / float32(csl)
	if pFree > 0.50 {
		return append([]*subState(nil), sl...)
	}
	return sl
}

// removeAllNonDurableSubscribers will remove all non-durable subscribers for the client.
func (s *StanServer) removeAllNonDurableSubscribers(clientID string) {
	client := s.clients.Lookup(clientID)
	if client == nil {
		return
	}
	client.RLock()
	defer client.RUnlock()

	for _, sub := range client.subs {
		sub.Lock()
		sub.clearAckTimer()
		subject := sub.subject
		isDurable := sub.isDurable()
		sub.clientID = ""
		sub.Unlock()

		// Skip removal if durable.
		if isDurable {
			continue
		}
		cs := s.channels.Lookup(subject)
		if cs == nil {
			continue
		}
		cs.subs.Remove(sub)
	}
}

// processUnSubscribeRequest will process a unsubscribe request.
func (s *StanServer) processUnSubscribeRequest(m *nats.Msg) {
	req := &pb.UnsubscribeRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		Errorf("STAN: Invalid unsub request from %s.", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}

	cs := s.channels.Lookup(req.Subject)
	if cs == nil {
		Errorf("STAN: [Client:%s] unsub request missing subject %s.",
			req.ClientID, req.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}
	sub := cs.subs.LookupByAckInbox(req.Inbox)
	if sub == nil {
		Errorf("STAN: [Client:%s] unsub request for missing inbox %s.",
			req.ClientID, req.Inbox)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}
	// Remove the subscription.
	cs.subs.Remove(sub)

	// Remove from Client
	if client := s.clients.Lookup(req.ClientID); client != nil {
		Debugf("STAN: [Client:%s] Unsubscribing subject=%s.",
			req.ClientID, sub.subject)
		client.RemoveSub(sub)
	}

	// Create a non-error response
	resp := &pb.SubscriptionResponse{AckInbox: req.Inbox}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)
}

func (s *StanServer) sendSubscriptionResponseErr(reply string, err error) {
	resp := &pb.SubscriptionResponse{Error: err.Error()}
	b, _ := resp.Marshal()
	s.nc.Publish(reply, b)
}

// Check for valid subjects
func isValidSubject(subject string) bool {
	tokens := strings.Split(subject, ".")
	if len(tokens) == 0 {
		return false
	}
	for _, token := range tokens {
		if strings.ContainsAny(token, ">*") {
			return false
		}
	}
	return true
}

// Clear the ackTimer
func (sub *subState) clearAckTimer() {
	if sub.ackTimer != nil {
		sub.ackTimer.Stop()
		sub.ackTimer = nil
	}
}

// adjustAckTimer adjusts the timer based on a given timestamp
// The timer will be stopped if there is no more pending ack.
// If there are pending acks, the timer will be reset to the
// default sub.ackWaitInSecs value if the given timestamp is
// 0 or in the past. Otherwise, it is set to the remaining time
// between the given timestamp and now.
func (sub *subState) adjustAckTimer(firstUnackedTimestamp int64) {
	sub.Lock()
	defer sub.Unlock()

	// Reset the floor (it will be set if needed)
	sub.ackTimeFloor = 0

	// Check if there are still pending acks
	if len(sub.acksPending) > 0 {

		// Capture time
		now := time.Now().UnixNano()

		// If it is in the past (which will happen when a message is
		// redelivered more than once), or 0, use the default ackWait
		if firstUnackedTimestamp <= now {
			sub.ackTimer.Reset(sub.ackWaitInSecs * time.Second)
		} else {
			// Compute the time the ackTimer should fire, which is the
			// ack timeout less the duration the message has been in
			// the server.
			fireIn := (firstUnackedTimestamp - now) + int64(sub.ackWaitInSecs*time.Second)

			sub.ackTimer.Reset(time.Duration(fireIn))

			// Skip redelivery of messages before this one.
			sub.ackTimeFloor = firstUnackedTimestamp
		}
	} else {
		// No more pending acks, clear the timer.
		sub.clearAckTimer()
	}
}

// Test if a subscription is a queue subscriber.
func (sub *subState) isQueueSubscriber() bool {
	return sub != nil && sub.qgroup != ""
}

// Test if a subscription is durable.
func (sub *subState) isDurable() bool {
	return sub != nil && sub.durableName != ""
}

// Used to generate durable key. This should not be called on non-durables.
func (sub *subState) durableKey() string {
	if sub.durableName == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s", sub.clientID, sub.subject, sub.durableName)
}

// Used to generate durable key. This should not be called on non-durables.
func durableKey(sr *pb.SubscriptionRequest) string {
	if sr.DurableName == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s", sr.ClientID, sr.Subject, sr.DurableName)
}

func (s *StanServer) addSubscription(cs *channelStore, sub *subState) {

	// Store this subscription
	cs.subs.Store(sub)

	// Also store in client
	if client := s.clients.Lookup(sub.clientID); client != nil {
		client.AddSub(sub)
	}

	Debugf("STAN: [Client:%s] Subscribed. subject=%s, inbox=%s.",
		sub.clientID, sub.subject, sub.inbox)
}

// processSubscriptionRequest will process a subscription request.
func (s *StanServer) processSubscriptionRequest(m *nats.Msg) {
	sr := &pb.SubscriptionRequest{}
	err := sr.Unmarshal(m.Data)
	if err != nil {
		Errorf("STAN:  Invalid Subscription request from %s.", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}

	// FIXME(dlc) check for multiple errors, mis-configurations, etc.

	// AckWait must be >= 1s
	if sr.AckWaitInSecs <= 0 {
		Debugf("STAN: [Client:%s] Invalid AckWait in subscription request from %s.",
			sr.ClientID, m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidAckWait)
		return
	}

	// Make sure subject is valid
	if !isValidSubject(sr.Subject) {
		Debugf("STAN: [Client:%s] Invalid subject <%s> in subscription request from %s.",
			sr.ClientID, sr.Subject, m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSubject)
		return
	}

	// ClientID must not be empty.
	if sr.ClientID == "" {
		Debugf("STAN: missing clientID in subscription request from %s", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply,
			errors.New("stan: malformed subscription request, clientID missing."))
		return
	}

	// Grab channel state, create a new one if needed.
	cs := s.channels.LookupOrCreate(sr.Subject)

	var sub *subState

	// Check for DurableSubscriber status
	if sr.DurableName != "" {
		// Can't be durable and a queue subscriber
		if sr.QGroup != "" {
			Debugf("STAN: [Client:%s] Invalid subscription request; cannot be both durable and a queue subscriber.",
				sr.ClientID)
			s.sendSubscriptionResponseErr(m.Reply, ErrDurableQueue)
			return
		}

		if sub = cs.subs.LookupByDurable(durableKey(sr)); sub != nil {
			sub.RLock()
			clientID := sub.clientID
			sub.RUnlock()
			if clientID != "" {
				Debugf("STAN: [Client:%s] Invalid client id in subscription request from %s.",
					sr.ClientID, m.Subject)
				s.sendSubscriptionResponseErr(m.Reply, ErrDupDurable)
				return
			}
			// ok we have a remembered subscription
			// FIXME(dlc) - Do we error on options? They should be ignored if the new conflicts with old.
			sub.Lock()
			// Set new clientID and reset lastSent
			sub.clientID = sr.ClientID
			// Also grab a new ackInbox and the sr's inbox.
			sub.ackInbox = nats.NewInbox()
			sub.inbox = sr.Inbox
			sub.Unlock()
		}
	}

	// Check SequenceStart out of range
	if sr.StartPosition == pb.StartPosition_SequenceStart {
		if !s.startSequenceValid(sr.Subject, sr.StartSequence) {
			Debugf("STAN: [Client:%s] Invalid start sequence in subscription request from %s.",
				sr.ClientID, m.Subject)
			s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSequence)
			return
		}
	}
	// Check for SequenceTime out of range
	if sr.StartPosition == pb.StartPosition_TimeDeltaStart {
		startTime := time.Now().UnixNano() - sr.StartTimeDelta
		if !s.startTimeValid(sr.Subject, startTime) {
			Debugf("STAN: [Client:%s] Invalid start time in subscription request from %s.",
				sr.ClientID, m.Subject)
			s.sendSubscriptionResponseErr(m.Reply, ErrInvalidTime)
			return
		}
	}

	// Create a subState if not retrieved from durable lookup above.
	if sub == nil {
		sub = &subState{
			clientID:      sr.ClientID,
			subject:       sr.Subject,
			qgroup:        sr.QGroup,
			inbox:         sr.Inbox,
			ackInbox:      nats.NewInbox(),
			durableName:   sr.DurableName,
			maxInFlight:   int(sr.MaxInFlight),
			ackWaitInSecs: time.Duration(sr.AckWaitInSecs),
			acksPending:   make(map[uint64]*pb.MsgProto),
		}

		// set the start sequence of the subscriber.
		s.setSubStartSequence(sub, sr, cs)

		// add the subscription to stan
		s.addSubscription(cs, sub)
	}

	// Subscribe to acks
	sub.ackSub, err = s.nc.Subscribe(sub.ackInbox, s.processAckMsg)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to ack subject, %v\n", err))
	}

	// Create a non-error response
	resp := &pb.SubscriptionResponse{AckInbox: sub.ackInbox}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)

	// If we are a durable and have state
	if sr.DurableName != "" {
		// Redeliver any oustanding.
		s.performDurableRedelivery(sub)
	}

	// publish messages to this subscriber
	sub.RLock()
	qs := sub.qstate
	sub.RUnlock()

	if qs != nil {
		s.sendAvailableMessagesToQueue(cs, qs)
	} else {
		s.sendAvailableMessages(cs, sub)
	}

}

// processAckMsg processes inbound acks from clients for delivered messages.
func (s *StanServer) processAckMsg(m *nats.Msg) {
	ack := &pb.Ack{}
	ack.Unmarshal(m.Data)
	cs := s.channels.Lookup(ack.Subject)
	if cs == nil {
		Errorf("STAN: [Client:?] Ack received, invalid channel (%s)", ack.Subject)
		return
	}
	s.processAck(cs, cs.subs.LookupByAckInbox(m.Subject), ack)
}

// processAck processes an ack and if needed sends more messages.
func (s *StanServer) processAck(cs *channelStore, sub *subState, ack *pb.Ack) {
	if sub == nil || ack == nil {
		return
	}

	sub.Lock()

	// Clear the ack
	Tracef("STAN: [Client:%s] removing pending ack, subj=%s, seq=%d.",
		sub.clientID, sub.subject, ack.Sequence)

	delete(sub.acksPending, ack.Sequence)
	stalled := sub.stalled
	if len(sub.acksPending) < sub.maxInFlight {
		sub.stalled = false
	}

	// Leave the reset/cancel of the ackTimer to the redelivery cb.

	qs := sub.qstate
	sub.Unlock()

	if qs != nil {
		qs.Lock()
		stalled = qs.stalled
		qs.stalled = false
		qs.Unlock()
	}

	if !stalled {
		return
	}

	if qs != nil {
		s.sendAvailableMessagesToQueue(cs, qs)
	} else {
		s.sendAvailableMessages(cs, sub)
	}
}

// Send any messages that are ready to be sent that have been queued to the group.
func (s *StanServer) sendAvailableMessagesToQueue(cs *channelStore, qs *queueState) {
	if cs == nil || qs == nil {
		return
	}

	qs.Lock()
	defer qs.Unlock()

	for nextSeq := qs.lastSent + 1; ; nextSeq++ {
		nextMsg := cs.msgs.Lookup(nextSeq)
		if nextMsg == nil || s.sendMsgToQueueGroup(qs, nextMsg) == false {
			break
		}
	}
}

// Send any messages that are ready to be sent that have been queued.
func (s *StanServer) sendAvailableMessages(cs *channelStore, sub *subState) {
	sub.Lock()
	defer sub.Unlock()

	for nextSeq := sub.lastSent + 1; ; nextSeq++ {
		nextMsg := cs.msgs.Lookup(nextSeq)
		if nextMsg == nil || s.sendMsgToSubAndUpdateLastSent(sub, nextMsg) == false {
			break
		}
	}
}

// Check if a startTime is valid.
func (s *StanServer) startTimeValid(subject string, start int64) bool {
	cs := s.channels.Lookup(subject)
	firstMsg := cs.msgs.FirstMsg()
	lastMsg := cs.msgs.LastMsg()
	if start > lastMsg.Timestamp || start < firstMsg.Timestamp {
		return false
	}
	return true
}

// Check if a startSequence is valid.
func (s *StanServer) startSequenceValid(subject string, seq uint64) bool {
	cs := s.channels.Lookup(subject)
	cs.msgs.RLock()
	defer cs.msgs.RUnlock()
	if seq > cs.msgs.last || seq < cs.msgs.first {
		return false
	}
	return true
}

func (s *StanServer) getSequenceFromStartTime(cs *channelStore, sub *subState, startTime int64) uint64 {

	cs.msgs.RLock()
	defer cs.msgs.RUnlock()

	index := sort.Search(len(cs.msgs.msgs), func(i int) bool {
		m := cs.msgs.msgs[uint64(i)+cs.msgs.first]
		if m.Timestamp >= startTime {
			return true
		}
		return false
	})

	return uint64(index) + cs.msgs.first
}

// Setup the start position for the subscriber.
func (s *StanServer) setSubStartSequence(sub *subState, sr *pb.SubscriptionRequest, cs *channelStore) {
	sub.Lock()
	defer sub.Unlock()

	switch sr.StartPosition {
	case pb.StartPosition_NewOnly:
		sub.lastSent = cs.msgs.LastSequence()
		Debugf("STAN: [Client:%s] Sending new-only subject=%s, seq=%d.",
			sub.clientID, sub.subject, sub.lastSent)
	case pb.StartPosition_LastReceived:
		sub.lastSent = cs.msgs.LastSequence() - 1
		Debugf("STAN: [Client:%s] Sending last message, subject=%s.",
			sub.clientID, sub.subject)
	case pb.StartPosition_TimeDeltaStart:
		startTime := time.Now().UnixNano() - sr.StartTimeDelta
		sub.lastSent = s.getSequenceFromStartTime(cs, sub, startTime) - 1
		Debugf("STAN: [Client:%s] Sending from time, subject=%s time=%d seq=%d",
			sub.clientID, sub.subject, startTime, sub.lastSent)
	case pb.StartPosition_SequenceStart:
		sub.lastSent = sr.StartSequence - 1
		Debugf("STAN: [Client:%s] Sending from sequence, subject=%s seq=%d",
			sub.clientID, sub.subject, sub.lastSent)
	case pb.StartPosition_First:
		sub.lastSent = cs.msgs.FirstSequence() - 1
		Debugf("STAN: [Client:%s] Sending from beginngin, subject=%s seq=%d",
			sub.clientID, sub.subject, sub.lastSent)
	}
}

// Shutdown will close our NATS connection and shutdown any embedded NATS server.
func (s *StanServer) Shutdown() {
	Debugf("STAN: Shutting down.")
	if s.nc != nil {
		s.nc.Close()
	}
	if s.natsServer != nil {
		s.natsServer.Shutdown()
		s.natsServer = nil
	}
}
