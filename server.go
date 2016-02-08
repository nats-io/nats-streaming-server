// Copyright 2016 Apcera Inc. All rights reserved.

package stan

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"

	natsd "github.com/nats-io/gnatsd/test"
)

// A single STAN server

const (
	DefaultPubPrefix   = "_STAN.pub"
	DefaultSubPrefix   = "_STAN.sub"
	DefaultUnSubPrefix = "_STAN.unsub"
	DefaultClosePrefix = "_STAN.close"

	DefaultMsgStoreLimit = 1024 * 1024
)

// Errors.
var (
	ErrBadPubMsg       = errors.New("stan: malformed message")
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
)

type stanServer struct {
	clusterID     string
	serverID      string
	pubPrefix     string // Subject prefix we received published messages on.
	subRequests   string // Subject we receive subscription requests on.
	unsubRequests string // Subject we receive unsubscribe requests on.
	closeRequests string // Subject we receive close requests on.
	natsServer    *server.Server
	opts          *ServerOptions
	nc            *nats.Conn

	// Track clients
	clientStoreLock sync.RWMutex
	clientStore     map[string]*client

	// Message Storage
	msgStoreLock  sync.RWMutex
	msgStores     map[string]*msgStore
	msgStoreLimit int

	// Subscription Storage
	subLock sync.RWMutex
	// Used to lookup subscriptions by subject.
	subStore map[string][]*serverSubscription
	// Used to lookup subscriptions by the INBOX they came in on.
	subAckInboxStore map[string]*serverSubscription
	// Used for Durables, note key is tuple, ClientId+Subject+Durable
	durableStore map[string]*serverSubscription
}

type client struct {
	clientID string
}

// Per channel/subject message store
type msgStore struct {
	sync.RWMutex
	subject string // Can't be wildcard
	cur     uint64
	first   uint64
	last    uint64
	msgs    map[uint64]*MsgProto
}

// Holds Subscription state
type serverSubscription struct {
	sync.RWMutex
	clientID      string
	subject       string
	queue         string
	inbox         string
	ackInbox      string
	durableName   string
	lastQueued    uint64
	lastSent      uint64
	lastAck       uint64
	maxInFlight   uint64
	ackWaitInSecs time.Duration
	ackTimer      *time.Timer
	ackSub        *nats.Subscription
}

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

	// Create clientStore
	s.clientStore = make(map[string]*client)

	// Create msgStores
	s.msgStores = make(map[string]*msgStore)
	s.msgStoreLimit = DefaultMsgStoreLimit

	// Setup Subscription Stores
	s.subStore = make(map[string][]*serverSubscription)
	s.subAckInboxStore = make(map[string]*serverSubscription)
	s.durableStore = make(map[string]*serverSubscription)

	// Generate Subjects
	// FIXME(dlc) guid needs to be shared in cluster mode
	s.pubPrefix = fmt.Sprintf("%s.%s", DefaultPubPrefix, newGUID())
	s.subRequests = fmt.Sprintf("%s.%s", DefaultSubPrefix, newGUID())
	s.unsubRequests = fmt.Sprintf("%s.%s", DefaultUnSubPrefix, newGUID())
	s.closeRequests = fmt.Sprintf("%s.%s", DefaultClosePrefix, newGUID())

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
}

// Process a client connect request
func (s *stanServer) connectCB(m *nats.Msg) {
	req := &ConnectRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil || req.ClientID == "" {
		cr := &ConnectResponse{Error: ErrInvalidConnReq.Error()}
		b, _ := cr.Marshal()
		s.nc.Publish(m.Reply, b)
		return
	}
	s.clientStoreLock.RLock()
	c := s.clientStore[req.ClientID]
	s.clientStoreLock.RUnlock()
	if c != nil {
		cr := &ConnectResponse{Error: ErrInvalidClient.Error()}
		b, _ := cr.Marshal()
		s.nc.Publish(m.Reply, b)
		return
	}

	// Register the new connection.
	s.clientStoreLock.Lock()
	s.clientStore[req.ClientID] = &client{clientID: req.ClientID}
	s.clientStoreLock.Unlock()

	// Respond with our ConnectResponse
	cr := &ConnectResponse{
		PubPrefix:     s.pubPrefix,
		SubRequests:   s.subRequests,
		UnsubRequests: s.unsubRequests,
		CloseRequests: s.closeRequests,
	}
	b, _ := cr.Marshal()
	s.nc.Publish(m.Reply, b)
}

// processCloseRequest process inbound messages from clients.
func (s *stanServer) processCloseRequest(m *nats.Msg) {
	req := &CloseRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		resp := &CloseResponse{Error: ErrInvalidCloseReq.Error()}
		if b, err := resp.Marshal(); err != nil {
			s.nc.Publish(m.Reply, b)
		}
	}

	// Remove from our clientStore
	s.clientStoreLock.Lock()
	delete(s.clientStore, req.ClientID)
	s.clientStoreLock.Unlock()

	// Remove non-durable subscribers.
	s.removeAllNonDurableSubscribers(req.ClientID)

	resp := &CloseResponse{}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)
}

// processClientPublish process inbound messages from clients.
func (s *stanServer) processClientPublish(m *nats.Msg) {
	pe := &PubMsg{}
	err := pe.Unmarshal(m.Data)
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
func (s *stanServer) processMsg(m *MsgProto) {
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
		if sub.lastSent-sub.lastAck < sub.maxInFlight {
			// We can send direct here.
			s.sendMsgToSub(sub, m)
		}
		sub.Unlock()
	}
	s.subLock.RUnlock()
}

// Sends the message to the subscriber, assumes sub lock is held.
func (s *stanServer) sendMsgToSub(sub *serverSubscription, m *MsgProto) {
	sub.lastSent = m.Seq
	b, _ := m.Marshal()
	s.nc.Publish(sub.inbox, b)

	// Setup the ackTimer as needed.
	if sub.ackTimer == nil {
		sub.ackTimer = time.AfterFunc(sub.ackWaitInSecs*time.Second, func() {
			sub.Lock()
			sub.ackTimer = nil
			needsRetransmit := sub.lastSent > sub.lastAck
			sub.Unlock()
			if needsRetransmit {
				// Reaqcuire lock and reset lastSent to lastAck
				sub.Lock()
				sub.lastSent = sub.lastAck
				sub.Unlock()
				s.sendQueuedMessages(sub)
			}
		})
	}
}

// assignAndStore will assign a sequencId and then store the message.
func (s *stanServer) assignAndStore(pm *PubMsg) (*MsgProto, error) {
	s.msgStoreLock.RLock()
	store := s.msgStores[pm.Subject]
	s.msgStoreLock.RUnlock()
	if store == nil {
		store = &msgStore{subject: pm.Subject, cur: 1, first: 1, last: 1}
		store.msgs = make(map[uint64]*MsgProto, s.msgStoreLimit)
		s.msgStoreLock.Lock()
		s.msgStores[pm.Subject] = store
		s.msgStoreLock.Unlock()
	}
	m := &MsgProto{Seq: store.cur, Subject: pm.Subject, Reply: pm.Reply, Data: pm.Data, Timestamp: time.Now().UnixNano()}
	store.Lock()
	store.msgs[store.cur] = m
	store.last = store.cur
	store.cur++

	// Check if we need to remove any.
	if len(store.msgs) >= s.msgStoreLimit {
		fmt.Printf("WARNING: Removing message[%d] from the store for [`%s`]\n", store.first, pm.Subject)
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

// Check for the existence of a durable.
func (s *stanServer) checkForDurable(key string) *serverSubscription {
	s.subLock.RLock()
	defer s.subLock.RUnlock()
	return s.durableStore[key]
}

// storeSubscription will store the subscription.
func (s *stanServer) storeSubscription(sub *serverSubscription, optDurable string) {
	if sub == nil {
		return
	}
	sub.RLock()
	subject := sub.subject
	ackInbox := sub.ackInbox
	sub.RUnlock()

	s.subLock.Lock()
	defer s.subLock.Unlock()

	sl := s.subStore[subject]
	if sl == nil {
		sl = make([]*serverSubscription, 0, 8)
	}
	s.subStore[subject] = append(sl, sub)
	s.subAckInboxStore[ackInbox] = sub
	if optDurable != "" {
		s.durableStore[optDurable] = sub
	}
}

// removeSubscription will remove references to the subscription.
func (s *stanServer) removeSubscription(sub *serverSubscription) {
	if sub == nil {
		return
	}
	sub.Lock()
	sub.ackSub.Unsubscribe()
	subject := sub.subject
	ackInbox := sub.ackInbox
	sub.Unlock()

	s.subLock.Lock()
	defer s.subLock.Unlock()

	// Delete from ackInbox lookup.
	delete(s.subAckInboxStore, ackInbox)

	// Delete ourselves from the list
	sl := s.subStore[subject]
	for i := 0; i < len(sl); i++ {
		if sl[i] == sub {
			sl[i] = sl[len(sl)-1]
			sl[len(sl)-1] = nil
			sl = sl[:len(sl)-1]
			s.subStore[subject] = sl
			s.checkForResizeOfSubscriberList(subject)
			break
		}
	}
}

// Checks if we need to do a resize. Assume lock is held for subStore.
func (s *stanServer) checkForResizeOfSubscriberList(subject string) {
	sl := s.subStore[subject]
	lsl := len(sl)
	csl := cap(sl)
	// Don't bother if list not too big
	if csl <= 8 {
		return
	}
	pFree := float32(csl-lsl) / float32(csl)
	if pFree > 0.50 {
		nsl := append([]*serverSubscription(nil), sl...)
		s.subStore[subject] = nsl
	}
}

// removeAllNonDurableSubscribers will remove all non-durable subscribers for the client.
func (s *stanServer) removeAllNonDurableSubscribers(clientID string) {
	dlist := []*serverSubscription{}

	s.subLock.RLock()
	for _, sub := range s.subAckInboxStore {
		sub.Lock()
		if sub.ackTimer != nil {
			sub.ackTimer.Stop()
			sub.ackTimer = nil
		}
		sub.Unlock()
		if sub.clientID == clientID {
			if sub.durableName == "" {
				dlist = append(dlist, sub)
			} else {
				// Clear the subscriptions clientID
				sub.clientID = ""
			}
		}
	}
	s.subLock.RUnlock()

	// Now delete them
	for _, sub := range dlist {
		s.removeSubscription(sub)
	}
}

// processUnSubscribeRequest will process a unsubscribe request.
func (s *stanServer) processUnSubscribeRequest(m *nats.Msg) {
	usr := &UnsubscribeRequest{}
	err := usr.Unmarshal(m.Data)
	if err != nil {
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}

	s.subLock.RLock()
	sub := s.subAckInboxStore[usr.Inbox]
	s.subLock.RUnlock()

	if sub == nil {
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}

	// Remove the subscription.
	s.removeSubscription(sub)

	// Create a non-error response
	resp := &SubscriptionResponse{AckInbox: usr.Inbox}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)
}

func (s *stanServer) sendSubscriptionResponseErr(reply string, err error) {
	resp := &SubscriptionResponse{Error: err.Error()}
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

// processSubscriptionRequest will process a subscription request.
func (s *stanServer) processSubscriptionRequest(m *nats.Msg) {
	sr := &SubscriptionRequest{}
	err := sr.Unmarshal(m.Data)
	if err != nil {
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}

	// FIXME(dlc) check for multiple errors, mis-configurations, etc.

	// AckWait must be >= 1s
	if sr.AckWaitInSecs <= 0 {
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidAckWait)
		return
	}

	// Make sure subject is valid
	if !isValidSubject(sr.Subject) {
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSubject)
		return
	}

	// ClientID must not be empty.
	if sr.ClientID == "" {
		s.sendSubscriptionResponseErr(m.Reply,
			errors.New("stan: malformed subscription request, clientID missing"))
		return
	}

	var sub *serverSubscription
	var durableKey string

	// Check for DurableSubscriber status
	if sr.DurableName != "" {
		durableKey = fmt.Sprintf("%s-%s-%s", sr.ClientID, sr.Subject, sr.DurableName)
		if sub = s.checkForDurable(durableKey); sub != nil {
			sub.RLock()
			clientID := sub.clientID
			sub.RUnlock()
			if clientID != "" {
				s.sendSubscriptionResponseErr(m.Reply, ErrDupDurable)
				return
			}
			// ok we have a remembered subscription
			// FIXME(dlc) - Do we error on options? They should be ignored
			sub.Lock()
			// Set new clientID and reset lastSent to lastAck
			sub.clientID = sr.ClientID
			sub.lastSent = sub.lastAck
			sub.Unlock()
		}
	}

	// Check SequenceStart out of range
	if sr.StartPosition == StartPosition_SequenceStart {
		if !s.startSeqValid(sr.Subject, sr.StartSequence) {
			s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSequence)
			return
		}
	}
	// Check for SequenceTime out of range
	if sr.StartPosition == StartPosition_TimeStart {
		if !s.startTimeValid(sr.Subject, sr.StartTime) {
			s.sendSubscriptionResponseErr(m.Reply, ErrInvalidTime)
			return
		}
	}

	// Create a serverSubscription
	if sub == nil {
		sub = &serverSubscription{
			clientID:      sr.ClientID,
			subject:       sr.Subject,
			queue:         sr.Queue,
			inbox:         sr.Inbox,
			ackInbox:      newInbox(),
			durableName:   sr.DurableName,
			maxInFlight:   uint64(sr.MaxInFlight),
			ackWaitInSecs: time.Duration(sr.AckWaitInSecs),
		}
		// Store this subscription
		s.storeSubscription(sub, durableKey)
	} else {
		sub.Lock()
		sub.ackInbox = newInbox()
		sub.inbox = sr.Inbox
		sub.Unlock()
	}

	// Subscribe to acks
	sub.ackSub, err = s.nc.Subscribe(sub.ackInbox, s.processAckMsg)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to ack subject, %v\n", err))
	}

	// Create a non-error response
	resp := &SubscriptionResponse{AckInbox: sub.ackInbox}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)

	// If we are a durable and have state
	if sr.DurableName != "" {
		sub.RLock()
		lastSent := sub.lastSent
		sub.RUnlock()
		if lastSent > 0 {
			s.sendQueuedMessages(sub)
			return
		}
	}

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
	case StartPosition_First:
		s.sendMessagesFromBeginning(sub)
	}
}

// processAckMsg processes inbound acks from clients for delivered messages.
func (s *stanServer) processAckMsg(m *nats.Msg) {
	ack := &Ack{}
	ack.Unmarshal(m.Data)
	s.subLock.RLock()
	sub := s.subAckInboxStore[m.Subject]
	s.subLock.RUnlock()
	s.processAck(sub, ack)
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
	// If we have the ackTimer running, either reset or cancel it.
	if sub.ackTimer != nil {
		if sub.lastAck == sub.lastSent {
			sub.ackTimer.Stop()
			sub.ackTimer = nil
		} else {
			sub.ackTimer.Reset(sub.ackWaitInSecs * time.Second)
		}
	}
	sub.Unlock()

	// Check to see if we should send more messages. Acks unblock the queue.
	s.sendQueuedMessages(sub)
}

// Send any messages that are ready to be sent that have been queued.
func (s *stanServer) sendQueuedMessages(sub *serverSubscription) {
	sub.Lock()
	defer sub.Unlock()

	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	for d := sub.lastQueued - sub.lastSent; d > 0; d = sub.lastQueued - sub.lastSent {
		// Throttle based on maxInflight
		if sub.lastSent-sub.lastAck >= sub.maxInFlight {
			break
		}
		nextSeq := sub.lastSent + 1
		store.RLock()
		nextMsg := store.msgs[nextSeq]
		store.RUnlock()
		if nextMsg != nil {
			s.sendMsgToSub(sub, nextMsg)
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
	sub.Lock()
	sub.lastSent = startSeq - 1 // FIXME(dlc) - wrap?
	sub.lastQueued = store.last
	sub.Unlock()
	store.RUnlock()

	s.sendQueuedMessages(sub)
}

// Send messages to the subscriber starting at startTime. Assumes startTime is valid.
func (s *stanServer) sendMessagesToSubFromTime(sub *serverSubscription, startTime int64) {
	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	if store == nil {
		return
	}

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

// Send all messages to the subscriber.
func (s *stanServer) sendMessagesFromBeginning(sub *serverSubscription) {
	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	if store == nil {
		return
	}

	// Do binary search to find starting sequence.
	store.RLock()
	first := store.first
	store.RUnlock()
	s.sendMessagesToSubFromSequence(sub, first)
}

// Send the last message we have to the subscriber
func (s *stanServer) sendLastMessageToSub(sub *serverSubscription) {
	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	if store == nil {
		return
	}

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
