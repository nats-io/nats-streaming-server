// Copyright 2016 Apcera Inc. All rights reserved.

package stan

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"

	natsd "github.com/nats-io/gnatsd/test"
)

// A mock single STAN server

const (
	DefaultPubPrefix = "_STAN.pub" // We will add on GUID here.
	DefaultSubPrefix = "_STAN.sub" // We will add on GUID here.

	DefaultMsgStoreLimit = 1024 * 1024
)

type stanServer struct {
	clusterID   string
	serverID    string
	pubPrefix   string
	subRequests string
	natsServer  *server.Server
	opts        *ServerOptions
	nc          *nats.Conn

	// Msg Storage
	msgStoreLock  sync.RWMutex
	msgStores     map[string]*msgStore
	msgStoreLimit int

	// Subscription Storage
	subLock sync.RWMutex
	// Used to lookup subscriptions by subject.
	subStore map[string][]*serverSubscription
	// Used to lookup subscriptions by the INBOX they came in on.
	subAckInboxStore map[string]*serverSubscription
}

// Per channel/subject store
type msgStore struct {
	sync.RWMutex
	subject string // Can't be wildcard
	cur     uint64
	first   uint64
	last    uint64
	msgs    map[uint64]*Msg
}

// Holds Subscription state
type serverSubscription struct {
	sync.Mutex
	subject       string
	queue         string
	inbox         string
	ackInbox      string
	name          string
	lastQueued    uint64
	lastSent      uint64
	lastAck       uint64
	maxInFlight   uint64
	ackWaitInSecs int32
	ackSub        *nats.Subscription
}

var (
	ErrBadPubMsg       = errors.New("stan: malformed message")
	ErrBadSubscription = errors.New("stan: malformed subscription request")
	ErrInvalidSequence = errors.New("stan: invalid start sequence")
	ErrInvalidTime     = errors.New("stan: invalid start time")
)

// ServerOptions
type ServerOptions struct {
	DiscoverPrefix string
}

// Set the default discover prefix.
var DefaultServerOptions = ServerOptions{
	DiscoverPrefix: DefaultDiscoverPrefix,
}

// RunServer will startup and embedded STAN server and a nats-server to support it.
func RunServer(ID string, optsA ...*server.Options) *stanServer {
	// Run a nats server by default
	s := stanServer{clusterID: ID, serverID: newGUID(), opts: &DefaultServerOptions}

	// Create fake msgStores
	s.msgStores = make(map[string]*msgStore)
	s.msgStoreLimit = DefaultMsgStoreLimit

	// Setup Subscription Stores
	s.subStore = make(map[string][]*serverSubscription)
	s.subAckInboxStore = make(map[string]*serverSubscription)

	// Generate pubPrefix
	// FIXME(dlc) guid needs to be shared in cluster mode
	s.pubPrefix = fmt.Sprintf("%s.%s", DefaultPubPrefix, newGUID())
	s.subRequests = fmt.Sprintf("%s.%s", DefaultSubPrefix, newGUID())

	// hack
	var opts *server.Options
	if len(optsA) > 0 {
		opts = optsA[0]
	} else {
		opts = &natsd.DefaultTestOptions
	}
	s.natsServer = natsd.RunServer(opts)
	natsURL := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	var err error
	if s.nc, err = nats.Connect(natsURL); err != nil {
		panic(fmt.Sprintf("Can't connect to embedded NATS server: %v\n", err))
	}
	s.initSubscriptions()

	return &s
}

// initSubscriptions will setup initial subscriptions for discovery etc.
func (s *stanServer) initSubscriptions() {
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
}

// Process a client connect request
func (s *stanServer) connectCB(m *nats.Msg) {
	// Respond with our ConnectResponse
	cr := &ConnectResponse{PubPrefix: s.pubPrefix, SubRequests: s.subRequests}
	b, _ := cr.Marshal()
	s.nc.Publish(m.Reply, b)
}

// processClientPublish process inbound messages from clients.
func (s *stanServer) processClientPublish(m *nats.Msg) {
	pe := &PubMsg{}
	err := pe.Unmarshal(m.Data)
	//	err := json.Unmarshal(m.Data, &pe)
	if err != nil {
		badMsgAck := &PubAck{Error: ErrBadPubMsg.Error()}
		if b, err := badMsgAck.Marshal(); err != nil {
			s.nc.Publish(m.Reply, b)
		}
	}

	////////////////////////////////////////////////////////////////////////////
	// This is where we will store the message and wait for others in the
	// potential cluster to do so as well, once we have a quorom someone can
	// ack the publisher. We simply do so here for now.
	////////////////////////////////////////////////////////////////////////////

	s.ackPublisher(pe, m.Reply)

	////////////////////////////////////////////////////////////////////////////
	// Once we have ack'd the publisher, we need to assign this a sequence ID.
	// This will be done by a master election within the cluster, for now we
	// assume we are the master and assign the sequence ID here.
	////////////////////////////////////////////////////////////////////////////

	msg, _ := s.assignAndStore(pe)

	////////////////////////////////////////////////////////////////////////////
	// Now trigger sends to any active subscribers
	////////////////////////////////////////////////////////////////////////////

	s.processMsg(msg)
}

// processMsg will proces a message, and possibly send to clients, etc.
func (s *stanServer) processMsg(m *Msg) {
	// Grab active subscriptions
	s.subLock.RLock()
	sl := s.subStore[m.Subject]
	if sl == nil || len(sl) == 0 {
		s.subLock.RUnlock()
		return
	}
	// Walk the subscribers
	for _, sub := range sl {
		sub.Lock()
		sub.lastQueued = m.Seq
		// Check if we have too many outstanding. Ack receipt will unblock
		if sub.lastSent-sub.lastAck <= sub.maxInFlight {
			// We can send direct here.
			sub.lastSent = m.Seq
			s.sendMsgToSub(sub.inbox, m)
		}
		sub.Unlock()
	}
	s.subLock.RUnlock()
}

func (s *stanServer) sendMsgToSub(inbox string, m *Msg) {
	b, _ := m.Marshal()
	s.nc.Publish(inbox, b)
}

// assignAndStore will assign a sequencId and then store the message.
func (s *stanServer) assignAndStore(pm *PubMsg) (*Msg, error) {
	s.msgStoreLock.RLock()
	store := s.msgStores[pm.Subject]
	s.msgStoreLock.RUnlock()
	if store == nil {
		store = &msgStore{subject: pm.Subject, cur: 1, first: 1, last: 1}
		store.msgs = make(map[uint64]*Msg, s.msgStoreLimit)
		s.msgStoreLock.Lock()
		s.msgStores[pm.Subject] = store
		s.msgStoreLock.Unlock()
	}
	m := &Msg{Seq: store.cur, Subject: pm.Subject, Reply: pm.Reply, Data: pm.Data, Timestamp: time.Now().UnixNano()}
	store.Lock()
	store.msgs[store.cur] = m
	store.last = store.cur
	store.cur++

	// Check if we need to remove any.
	if len(store.msgs) >= s.msgStoreLimit {
		fmt.Printf("Removing message[%d] from the store for [`%s`]\n", store.first, pm.Subject)
		delete(store.msgs, store.first)
		store.first++
	}
	store.Unlock()
	return m, nil
}

// ackPublisher sends the ack for a message.
func (s *stanServer) ackPublisher(pm *PubMsg, reply string) {
	msgAck := &PubAck{Id: pm.Id}
	var buf [32]byte
	b := buf[:]
	n, _ := msgAck.MarshalTo(b)
	s.nc.Publish(reply, b[:n])
}

// storeSubscription will store the subscription.
func (s *stanServer) storeSubscription(sub *serverSubscription) {
	s.subLock.Lock()
	sl := s.subStore[sub.subject]
	if sl == nil {
		sl = make([]*serverSubscription, 0, 8)
	}
	s.subStore[sub.subject] = append(sl, sub)
	s.subAckInboxStore[sub.ackInbox] = sub
	s.subLock.Unlock()
}

// processSubscriptionRequest will process a subscription request
func (s *stanServer) processSubscriptionRequest(m *nats.Msg) {
	sr := &SubscriptionRequest{}
	err := sr.Unmarshal(m.Data)
	if err != nil {
		resp := &SubscriptionResponse{Error: err.Error()}
		b, _ := resp.Marshal()
		s.nc.Publish(m.Reply, b)
		return
	}

	// FIXME(dlc) check for multiple errors, mis-configurations, etc.

	// Check SequenceStart out of range
	if sr.StartPosition == StartPosition_SequenceStart {
		if !s.startSeqValid(sr.Subject, sr.StartSequence) {
			resp := &SubscriptionResponse{Error: ErrInvalidSequence.Error()}
			b, _ := resp.Marshal()
			s.nc.Publish(m.Reply, b)
			return
		}
	}
	// Check for SequenceTime out of range
	if sr.StartPosition == StartPosition_TimeStart {
		if !s.startTimeValid(sr.Subject, sr.StartTime) {
			resp := &SubscriptionResponse{Error: ErrInvalidTime.Error()}
			b, _ := resp.Marshal()
			s.nc.Publish(m.Reply, b)
			return
		}
	}

	// Create a serverSubscription
	sub := &serverSubscription{
		subject:       sr.Subject,
		queue:         sr.Queue,
		inbox:         sr.Inbox,
		ackInbox:      newInbox(),
		name:          sr.DurableName,
		maxInFlight:   uint64(sr.MaxInFlight),
		ackWaitInSecs: sr.AckWaitInSecs,
	}

	// Store this subscription
	s.storeSubscription(sub)

	// Subscribe to acks
	sub.ackSub, err = s.nc.Subscribe(sub.ackInbox, s.processAckMsg)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to ack subject, %v\n", err))
	}

	// Create a response
	resp := &SubscriptionResponse{AckInbox: sub.ackInbox}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)

	// Initialize the subscription and see if StartPosition dictates we have messages to send.
	switch sr.StartPosition {
	case StartPosition_NewOnly:
		// No-Op
	case StartPosition_LastReceived:
		// Send the last message received.
		s.sendLastMessageToSub(sub)
	case StartPosition_TimeStart:
		s.sendMessagesToSubFromTime(sub, sr.StartTime)
	case StartPosition_SequenceStart:
		s.sendMessagesToSubFromSequence(sub, sr.StartSequence)
	}
}

// processAckMsg processes inbound acks from clients for delivered messages.
func (s *stanServer) processAckMsg(m *nats.Msg) {
	ack := &Ack{}
	ack.Unmarshal(m.Data)
	s.processAck(s.subAckInboxStore[m.Subject], ack)
}

// processAck processes an ack and if needed sends more messages.
func (s *stanServer) processAck(sub *serverSubscription, ack *Ack) {
	if sub == nil {
		return
	}

	sub.Lock()
	// Update our notion of lastAck.
	if ack.Seq > sub.lastAck {
		sub.lastAck = ack.Seq
	}
	sub.Unlock()

	// Check to see if we should send more messages. Acks unblock the queue.
	s.sendQueuedMessages(sub)
}

// Send any messages that are ready to be sent that have been queued.
// Lock for sub should be held.
func (s *stanServer) sendQueuedMessages(sub *serverSubscription) {
	sub.Lock()
	defer sub.Unlock()

	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	for d := sub.lastQueued - sub.lastSent; d > 0; d = sub.lastQueued - sub.lastSent {
		nextSeq := sub.lastSent + 1
		store.RLock()
		nextMsg := store.msgs[nextSeq]
		store.RUnlock()
		if nextMsg != nil {
			sub.lastSent++
			s.sendMsgToSub(sub.inbox, nextMsg)
		}
	}
}

// Check if a startTime is valid.
func (s *stanServer) startTimeValid(subject string, start int64) bool {
	s.msgStoreLock.RLock()
	store := s.msgStores[subject]
	s.msgStoreLock.RUnlock()
	store.RLock()
	defer store.RUnlock()
	firstMsg := store.msgs[store.first]
	lastMsg := store.msgs[store.last]
	if start > lastMsg.Timestamp || start < firstMsg.Timestamp {
		return false
	}
	return true
}

// Check if a startSequence is valid.
func (s *stanServer) startSeqValid(subject string, seq uint64) bool {
	s.msgStoreLock.RLock()
	store := s.msgStores[subject]
	s.msgStoreLock.RUnlock()
	store.RLock()
	defer store.RUnlock()
	if seq > store.last || seq < store.first {
		return false
	}
	return true
}

// Send messages to the subscriber starting at startSeq.
func (s *stanServer) sendMessagesToSubFromSequence(sub *serverSubscription, startSeq uint64) {
	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	store.RLock()
	sub.lastSent = startSeq - 1 // FIXME(dlc) - wrap?
	sub.lastQueued = store.last
	store.RUnlock()

	s.sendQueuedMessages(sub)
}

// Send messages to the subscriber starting at startTime. Assumes startTime is valid.
func (s *stanServer) sendMessagesToSubFromTime(sub *serverSubscription, startTime int64) {
	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	// Do binary search to find starting sequence.
	store.RLock()
	index := sort.Search(len(store.msgs), func(i int) bool {
		m := store.msgs[uint64(i)+store.first]
		if m.Timestamp >= startTime {
			return true
		}
		return false
	})
	store.RUnlock()
	startSeq := uint64(index) + store.first
	s.sendMessagesToSubFromSequence(sub, startSeq)
}

// Send the last message we have to the subscriber
func (s *stanServer) sendLastMessageToSub(sub *serverSubscription) {
	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	store.RLock()
	last := store.last
	store.RUnlock()

	s.sendMessagesToSubFromSequence(sub, last)
}

// Shutdown will close our NATS connection and shutdown any embedded NATS server.
func (s *stanServer) Shutdown() {
	if s.nc != nil {
		s.nc.Close()
	}
	if s.natsServer != nil {
		s.natsServer.Shutdown()
	}
}
