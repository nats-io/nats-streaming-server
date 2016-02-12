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
	ErrDurableQueue    = errors.New("stan: queue subscribers can't be durable")
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
	// Used to lookup channel state
	subStore map[string]*channelState

	// Used to lookup subscriptions by the INBOX they came in on.
	subAckInboxStore map[string]*subState

	// Used for Durables, note key is tuple, ClientId+Subject+Durable
	durableStore map[string]*subState
}

// Hold for client
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

// channelState holds our known state of all subscribers for a given channel/subject.
type channelState struct {
	subs  []*subState            // plain subscribers
	qsubs map[string][]*subState // queue subscribers
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
	lastSent      uint64
	ackWaitInSecs time.Duration
	ackTimer      *time.Timer
	ackSub        *nats.Subscription
	maxInFlight   int
	acksPending   map[uint64]*MsgProto
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
	s.subStore = make(map[string]*channelState)
	s.subAckInboxStore = make(map[string]*subState)
	s.durableStore = make(map[string]*subState)

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

// processMsg will proces a message, and possibly send to clients, etc.
func (s *stanServer) processMsg(m *MsgProto) {
	// Grab channel state
	s.subLock.RLock()
	chs := s.subStore[m.Subject]
	if chs == nil {
		s.subLock.RUnlock()
		return
	}

	// Walk the plain subscribers
	for _, sub := range chs.subs {
		sub.Lock()
		s.sendMsgToSub(sub, m)
		sub.Unlock()
	}

	// Check the queue subscribers
	for _, sl := range chs.qsubs {
		if sub := findBestQueueSub(sl); sub != nil {
			sub.Lock()
			s.sendMsgToSub(sub, m)
			sub.Unlock()
		}
	}

	s.subLock.RUnlock()
}

// Used for sorting by sequence
type bySeq []*MsgProto

func (a bySeq) Len() int           { return (len(a)) }
func (a bySeq) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySeq) Less(i, j int) bool { return a[i].Sequence < a[j].Sequence }

func makeSortedMsgs(msgs map[uint64]*MsgProto) []*MsgProto {
	results := make([]*MsgProto, 0, len(msgs))
	for _, m := range msgs {
		mCopy := *m // copy since we need to set redelivered flag.
		results = append(results, &mCopy)
	}
	sort.Sort(bySeq(results))
	return results
}

// Redeliver all outstanding messages to a durable subscriber, used on resubscribe.
func (s *stanServer) performDurableRedelivery(sub *subState) {
	s.performRedelivery(sub, false)
}

// Redeliver all outstanding messages that have expired.
func (s *stanServer) performAckExpirationRedelivery(sub *subState) {
	s.performRedelivery(sub, true)
}

// Performs redlivery, takes a flag on whether to honor expiration.
func (s *stanServer) performRedelivery(sub *subState, checkExpiration bool) {
	// Sort our messages outstanding from acksPending, grab some state and unlock.
	sub.Lock()
	expTime := int64(sub.ackWaitInSecs * time.Second)
	sortedMsgs := makeSortedMsgs(sub.acksPending)
	sub.ackTimer = nil
	inbox := sub.inbox
	subject := sub.subject
	qgroup := sub.qgroup
	isQueueSubsriber := qgroup != ""
	sub.Unlock()

	now := time.Now().UnixNano()

	// We will move through acksPending(sorted) and see what needs redelivery.
	for _, m := range sortedMsgs {
		if m.Timestamp+expTime > now && checkExpiration {
			continue
		}

		// Flag as redelivered.
		m.Redelivered = true

		// Handle QueueSubscribers differently, since we will choose best subscriber
		// to redeliver to, not necessarily the same one.
		if isQueueSubsriber {
			// Remove from current subs acksPending.
			sub.Lock()
			delete(sub.acksPending, m.Sequence)
			sub.Unlock()

			s.subLock.RLock()
			chs := s.subStore[subject]
			sl := chs.qsubs[qgroup]
			qsub := findBestQueueSub(sl)
			s.subLock.RUnlock()
			if qsub == nil {
				break
			}
			qsub.Lock()
			s.sendMsgToSub(qsub, m)
			qsub.Unlock()
		} else {
			b, _ := m.Marshal()
			if err := s.nc.Publish(inbox, b); err != nil {
				// Break on error. FIXME(dlc) reset timer?
				break
			}
		}
	}
}

// Sends the message to the subscriber
// Sub lock should be held before calling.
func (s *stanServer) sendMsgToSub(sub *subState, m *MsgProto) bool {
	if sub == nil || m == nil {
		return false
	}
	// Don't send if we have too many outstanding already.
	if len(sub.acksPending) > sub.maxInFlight {
		return false
	}

	sub.lastSent = m.Sequence
	b, _ := m.Marshal()
	if err := s.nc.Publish(sub.inbox, b); err != nil {
		return false
	}
	// Store in ackPending.
	sub.acksPending[m.Sequence] = m

	// Setup the ackTimer as needed.
	if sub.ackTimer == nil {
		sub.ackTimer = time.AfterFunc(sub.ackWaitInSecs*time.Second, func() {
			s.performAckExpirationRedelivery(sub)
		})
	}

	return true
}

// assignAndStore will assign a sequencId and then store the message.
func (s *stanServer) assignAndStore(pm *PubMsg) (*MsgProto, error) {
	s.msgStoreLock.RLock()
	store := s.msgStores[pm.Subject]
	s.msgStoreLock.RUnlock()

	// Create store if needed.
	if store == nil {
		store = &msgStore{subject: pm.Subject, cur: 1, first: 1, last: 1}
		store.msgs = make(map[uint64]*MsgProto, s.msgStoreLimit)
		s.msgStoreLock.Lock()
		s.msgStores[pm.Subject] = store
		s.msgStoreLock.Unlock()
	}

	store.Lock()

	m := &MsgProto{
		Sequence:  store.cur,
		Subject:   pm.Subject,
		Reply:     pm.Reply,
		Data:      pm.Data,
		Timestamp: time.Now().UnixNano(),
	}
	store.msgs[store.cur] = m
	store.last = store.cur
	store.cur++

	// FIXME(dlc) - Check if we need to remove any.
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
func (s *stanServer) checkForDurable(key string) *subState {
	s.subLock.RLock()
	defer s.subLock.RUnlock()
	return s.durableStore[key]
}

// storeSubscription will store the subscription.
func (s *stanServer) storeSubscription(sub *subState) {
	if sub == nil {
		return
	}
	sub.RLock()
	subject := sub.subject
	ackInbox := sub.ackInbox
	qgroup := sub.qgroup
	sub.RUnlock()

	s.subLock.Lock()
	defer s.subLock.Unlock()

	// First store by ackInbox for ack direct lookup
	s.subAckInboxStore[ackInbox] = sub

	// Now store in the channelState by type
	chs := s.subStore[subject]
	if chs == nil {
		chs = &channelState{
			subs:  make([]*subState, 0, 4),
			qsubs: make(map[string][]*subState),
		}
		s.subStore[subject] = chs
	}

	if qgroup != "" {
		// Queue subscriber.
		sl := chs.qsubs[qgroup]
		if sl == nil {
			sl = make([]*subState, 0, 4)
		}
		chs.qsubs[qgroup] = append(sl, sub)
	} else {
		// Plain subscriber.
		chs.subs = append(chs.subs, sub)
	}

	// Hold onto durables in special lookup.
	if sub.isDurable() {
		s.durableStore[sub.durableKey()] = sub
	}
}

// Delete a sub from our list.
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

// removeSubscription will remove references to the subscription.
func (s *stanServer) removeSubscription(sub *subState) {
	if sub == nil {
		return
	}
	sub.Lock()
	sub.ackSub.Unsubscribe()
	subject := sub.subject
	ackInbox := sub.ackInbox
	qgroup := sub.qgroup
	sub.Unlock()

	s.subLock.Lock()
	defer s.subLock.Unlock()

	// Delete from ackInbox lookup.
	delete(s.subAckInboxStore, ackInbox)

	// Delete ourselves from the list
	chs := s.subStore[subject]
	if chs == nil {
		return
	}
	if qgroup != "" {
		chs.qsubs[qgroup] = sub.deleteFromList(chs.qsubs[qgroup])
	} else {
		chs.subs = sub.deleteFromList(chs.subs)
	}
}

// removeAllNonDurableSubscribers will remove all non-durable subscribers for the client.
func (s *stanServer) removeAllNonDurableSubscribers(clientID string) {
	dlist := []*subState{}

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
func (sr *SubscriptionRequest) durableKey() string {
	if sr.DurableName == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s", sr.ClientID, sr.Subject, sr.DurableName)
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

	var sub *subState

	// Check for DurableSubscriber status
	if sr.DurableName != "" {
		// Can't be durable and a queue subscriber
		if sr.QGroup != "" {
			s.sendSubscriptionResponseErr(m.Reply, ErrDurableQueue)
			return
		}

		durableKey := sr.durableKey()
		if sub = s.checkForDurable(durableKey); sub != nil {
			sub.RLock()
			clientID := sub.clientID
			sub.RUnlock()
			if clientID != "" {
				s.sendSubscriptionResponseErr(m.Reply, ErrDupDurable)
				return
			}
			// ok we have a remembered subscription
			// FIXME(dlc) - Do we error on options? They should be ignored if the new conflicts with old.
			sub.Lock()
			// Set new clientID and reset lastSent
			sub.clientID = sr.ClientID
			// Also grab a new ackInbox and the sr's inbox.
			sub.ackInbox = newInbox()
			sub.inbox = sr.Inbox
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

	// Create a subState if non-durable
	if sub == nil {
		sub = &subState{
			clientID:      sr.ClientID,
			subject:       sr.Subject,
			qgroup:        sr.QGroup,
			inbox:         sr.Inbox,
			ackInbox:      newInbox(),
			durableName:   sr.DurableName,
			maxInFlight:   int(sr.MaxInFlight),
			ackWaitInSecs: time.Duration(sr.AckWaitInSecs),
			acksPending:   make(map[uint64]*MsgProto),
		}
		// Store this subscription
		s.storeSubscription(sub)
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
		// Redeliver any oustanding.
		s.performDurableRedelivery(sub)
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
func (s *stanServer) processAck(sub *subState, ack *Ack) {
	if sub == nil {
		return
	}

	sub.Lock()

	wasBlocked := len(sub.acksPending) >= sub.maxInFlight

	// Clear the ack
	delete(sub.acksPending, ack.Sequence)

	// If we have the ackTimer running, either reset or cancel it.
	if sub.ackTimer != nil {
		if len(sub.acksPending) == 0 {
			sub.ackTimer.Stop()
			sub.ackTimer = nil
		} else {
			// FIXME(dlc) - This should be to next expiration, not simply +delta
			sub.ackTimer.Reset(sub.ackWaitInSecs * time.Second)
		}
	}

	sub.Unlock()

	// Check to see if we should send more messages. Acks unblock the queue.
	if wasBlocked {
		s.sendQueuedMessages(sub)
	}
}

// Send any messages that are ready to be sent that have been queued.
func (s *stanServer) sendQueuedMessages(sub *subState) {
	sub.Lock()
	defer sub.Unlock()

	s.msgStoreLock.RLock()
	store := s.msgStores[sub.subject]
	s.msgStoreLock.RUnlock()

	store.RLock()
	last := store.last
	store.RUnlock()

	for d := last - sub.lastSent; d > 0; d = last - sub.lastSent {
		// Throttle based on maxInflight
		if len(sub.acksPending) >= sub.maxInFlight {
			break
		}
		nextSeq := sub.lastSent + 1
		store.RLock()
		nextMsg := store.msgs[nextSeq]
		store.RUnlock()
		if s.sendMsgToSub(sub, nextMsg) == false {
			break
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
func (s *stanServer) sendMessagesToSubFromSequence(sub *subState, startSeq uint64) {
	sub.Lock()
	sub.lastSent = startSeq - 1 // FIXME(dlc) - wrap?
	sub.Unlock()

	s.sendQueuedMessages(sub)
}

// Send messages to the subscriber starting at startTime. Assumes startTime is valid.
func (s *stanServer) sendMessagesToSubFromTime(sub *subState, startTime int64) {
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
	startSeq := uint64(index) + store.first
	store.RUnlock()

	s.sendMessagesToSubFromSequence(sub, startSeq)
}

// Send all messages to the subscriber.
func (s *stanServer) sendMessagesFromBeginning(sub *subState) {
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
func (s *stanServer) sendLastMessageToSub(sub *subState) {
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
		s.natsServer = nil
	}
}
