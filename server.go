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

	DefaultMsgStoreLimit = 1000000
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

	// Clients
	clients *clientStore

	// Channels
	channels *channelMap
}

// Hold current clients.
type clientStore struct {
	sync.RWMutex
	clients map[string]*client
}

// Hold for client
type client struct {
	sync.RWMutex
	clientID string
	subs     []*subState
}

// Track subscriptions
func (c *client) AddSub(sub *subState) {
	c.Lock()
	defer c.Unlock()
	c.subs = append(c.subs, sub)
}

// Remove a subscription
func (c *client) RemoveSub(sub *subState) {
	c.Lock()
	defer c.Unlock()
	c.subs = sub.deleteFromList(c.subs)
}

// Register a client
func (cs *clientStore) Register(c *client) {
	cs.Lock()
	defer cs.Unlock()
	cs.clients[c.clientID] = c
}

// Unregister a client
func (cs *clientStore) Unregister(ID string) {
	cs.Lock()
	defer cs.Unlock()
	client := cs.clients[ID]
	if client != nil {
		client.subs = nil
	}
	delete(cs.clients, ID)
}

// Lookup a client
func (cs *clientStore) Lookup(ID string) *client {
	cs.RLock()
	defer cs.RUnlock()
	return cs.clients[ID]
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
			msgs:    make(map[uint64]*MsgProto, DefaultMsgStoreLimit),
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
	qgroup := sub.qgroup
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
	if qgroup != "" {
		if qs := ss.qsubs[qgroup]; qs != nil {
			qs.subs = sub.deleteFromList(qs.subs)
		}
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

// Get queueState for qgroup name.
func (ss *subStore) LookupQueueState(qgroup string) *queueState {
	ss.RLock()
	defer ss.RUnlock()
	return ss.qsubs[qgroup]
}

// Per channel/subject message store
type msgStore struct {
	sync.RWMutex
	subject string // Can't be wildcard
	first   uint64
	last    uint64
	msgs    map[uint64]*MsgProto
}

// Store a given message
func (ms *msgStore) Store(subject, reply string, data []byte) (*MsgProto, error) {
	ms.Lock()
	defer ms.Unlock()

	ms.last++
	m := &MsgProto{
		Sequence:  ms.last,
		Subject:   subject,
		Reply:     reply,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}
	ms.msgs[ms.last] = m

	// Check if we need to remove any.
	if len(ms.msgs) > DefaultMsgStoreLimit {
		Errorf("WARNING: Removing message[%d] from the store for [`%s`]\n", ms.first, subject)
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
func (ms *msgStore) Lookup(seq uint64) *MsgProto {
	ms.RLock()
	defer ms.RUnlock()
	return ms.msgs[seq]
}

func (ms *msgStore) FirstMsg() *MsgProto {
	ms.RLock()
	defer ms.RUnlock()
	return ms.msgs[ms.first]
}

func (ms *msgStore) LastMsg() *MsgProto {
	ms.RLock()
	defer ms.RUnlock()
	return ms.msgs[ms.last]
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
	s.clients = &clientStore{clients: make(map[string]*client)}

	// Create channelMap
	s.channels = &channelMap{channels: make(map[string]*channelStore)}

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
	noLog = opts.NoLog
	s.natsServer = natsd.RunServer(opts)
	natsURL := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	var err error
	if s.nc, err = nats.Connect(natsURL); err != nil {
		panic(fmt.Sprintf("Can't connect to NATS server: %v\n", err))
	}

	/*
		s.nc.SetDisconnectHandler(func(_ *nats.Conn) {
			fmt.Printf("NATS DISCONNECTED THE CONNECTION!\n")
		})
		s.nc.SetClosedHandler(func(_ *nats.Conn) {
			fmt.Printf("NATS CLOSED THE CONNECTION!\n")
		})
		s.nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			fmt.Printf("NATS GOT ERR: %v\n", err)
		})
	*/

	s.initSubscriptions()

	Noticef("Message store is MEMORY")
	Noticef("Maximum of %d will be stored", DefaultMsgStoreLimit)

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

	// Check if already connected.
	if c := s.clients.Lookup(req.ClientID); c != nil {
		cr := &ConnectResponse{Error: ErrInvalidClient.Error()}
		b, _ := cr.Marshal()
		s.nc.Publish(m.Reply, b)
		return
	}

	// Register the new connection.
	client := &client{
		clientID: req.ClientID,
		subs:     make([]*subState, 0, 4),
	}
	s.clients.Register(client)

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

	// Remove all non-durable subscribers.
	s.removeAllNonDurableSubscribers(req.ClientID)

	// Remove from our clientStore
	s.clients.Unregister(req.ClientID)

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

	cs, msg := s.assignAndStore(pe)

	////////////////////////////////////////////////////////////////////////////
	// Now trigger sends to any active subscribers
	////////////////////////////////////////////////////////////////////////////

	s.processMsg(cs, msg)
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
func (s *stanServer) sendMsgToQueueGroup(qs *queueState, m *MsgProto) bool {
	if qs == nil {
		return false
	}
	sub := findBestQueueSub(qs.subs)
	if sub == nil {
		return false
	}
	sub.Lock()
	didSend := s.sendMsgToSub(sub, m)
	lastSent := sub.lastSent
	sub.Unlock()
	if !didSend {
		return false
	}
	if lastSent > qs.lastSent {
		qs.lastSent = lastSent
	}
	return true
}

// processMsg will proces a message, and possibly send to clients, etc.
func (s *stanServer) processMsg(cs *channelStore, m *MsgProto) {
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

			cs := s.channels.Lookup(subject)
			ss := cs.subs

			var qsub *subState
			ss.RLock()
			if qs := ss.qsubs[qgroup]; qs != nil {
				qsub = findBestQueueSub(qs.subs)
			}
			ss.RUnlock()

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
	if len(sub.acksPending) >= sub.maxInFlight {
		return false
	}

	oldLast := sub.lastSent
	sub.lastSent = m.Sequence
	b, _ := m.Marshal()
	if err := s.nc.Publish(sub.inbox, b); err != nil {
		sub.lastSent = oldLast
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

// assignAndStore will assign a sequence ID and then store the message.
func (s *stanServer) assignAndStore(pm *PubMsg) (*channelStore, *MsgProto) {
	cs := s.channels.LookupOrCreate(pm.Subject)
	// FIXME(dlc) - check for errors.
	m, _ := cs.msgs.Store(pm.Subject, pm.Reply, pm.Data)
	return cs, m
}

// ackPublisher sends the ack for a message.
func (s *stanServer) ackPublisher(pm *PubMsg, reply string) {
	msgAck := &PubAck{Id: pm.Id}
	var buf [32]byte
	b := buf[:]
	n, _ := msgAck.MarshalTo(b)
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
func (s *stanServer) removeAllNonDurableSubscribers(clientID string) {
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
func (s *stanServer) processUnSubscribeRequest(m *nats.Msg) {
	req := &UnsubscribeRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}

	cs := s.channels.Lookup(req.Subject)
	if cs == nil {
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}
	sub := cs.subs.LookupByAckInbox(req.Inbox)
	if sub == nil {
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}
	// Remove the subscription.
	cs.subs.Remove(sub)

	// Remove from Client
	if client := s.clients.Lookup(req.ClientID); client != nil {
		client.RemoveSub(sub)
	}

	// Create a non-error response
	resp := &SubscriptionResponse{AckInbox: req.Inbox}
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

// Clear the ackTimer
func (sub *subState) clearAckTimer() {
	if sub.ackTimer != nil {
		sub.ackTimer.Stop()
		sub.ackTimer = nil
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

	// Grab channel state, create a new one if needed.
	cs := s.channels.LookupOrCreate(sr.Subject)

	var sub *subState

	// Check for DurableSubscriber status
	if sr.DurableName != "" {
		// Can't be durable and a queue subscriber
		if sr.QGroup != "" {
			s.sendSubscriptionResponseErr(m.Reply, ErrDurableQueue)
			return
		}

		if sub = cs.subs.LookupByDurable(sr.durableKey()); sub != nil {
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
		if !s.startSequenceValid(sr.Subject, sr.StartSequence) {
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
		cs.subs.Store(sub)
		// Also store in client
		if client := s.clients.Lookup(sr.ClientID); client != nil {
			client.AddSub(sub)
		}
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
		s.sendAvailableMessages(cs, sub)
		return
	}

	// Initialize the subscription and see if StartPosition dictates we have messages to send.
	switch sr.StartPosition {
	case StartPosition_NewOnly:
		s.sendNewOnly(cs, sub)
	case StartPosition_LastReceived:
		s.sendLastMessage(cs, sub)
	case StartPosition_TimeStart:
		s.sendMessagesToSubFromTime(cs, sub, sr.StartTime)
	case StartPosition_SequenceStart:
		s.sendMessagesFromSequence(cs, sub, sr.StartSequence)
	case StartPosition_First:
		s.sendMessagesFromBeginning(cs, sub)
	}
}

// processAckMsg processes inbound acks from clients for delivered messages.
func (s *stanServer) processAckMsg(m *nats.Msg) {
	ack := &Ack{}
	ack.Unmarshal(m.Data)
	cs := s.channels.Lookup(ack.Subject)
	if cs == nil {
		// FIXME(dlc) - log
		return
	}
	s.processAck(cs, cs.subs.LookupByAckInbox(m.Subject), ack)
}

// processAck processes an ack and if needed sends more messages.
func (s *stanServer) processAck(cs *channelStore, sub *subState, ack *Ack) {
	if sub == nil || ack == nil {
		return
	}

	sub.Lock()
	// Clear the ack
	delete(sub.acksPending, ack.Sequence)

	// If we have the ackTimer running, either reset or cancel it.
	if sub.ackTimer != nil {
		if len(sub.acksPending) == 0 {
			sub.clearAckTimer()
		} else {
			// FIXME(dlc) - This should be to next expiration, not simply +delta
			sub.ackTimer.Reset(sub.ackWaitInSecs * time.Second)
		}
	}
	qgroup := sub.qgroup
	sub.Unlock()

	if qgroup != "" {
		s.sendAvailableMessagesToQueue(cs, cs.subs.LookupQueueState(qgroup))
	} else {
		s.sendAvailableMessages(cs, sub)
	}
}

// Send any messages that are ready to be sent that have been queued to the group.
func (s *stanServer) sendAvailableMessagesToQueue(cs *channelStore, qs *queueState) {
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
func (s *stanServer) sendAvailableMessages(cs *channelStore, sub *subState) {
	sub.Lock()
	defer sub.Unlock()

	for nextSeq := sub.lastSent + 1; ; nextSeq++ {
		nextMsg := cs.msgs.Lookup(nextSeq)
		if nextMsg == nil || s.sendMsgToSub(sub, nextMsg) == false {
			break
		}
	}
}

// Check if a startTime is valid.
func (s *stanServer) startTimeValid(subject string, start int64) bool {
	cs := s.channels.Lookup(subject)
	firstMsg := cs.msgs.FirstMsg()
	lastMsg := cs.msgs.LastMsg()
	if start > lastMsg.Timestamp || start < firstMsg.Timestamp {
		return false
	}
	return true
}

// Check if a startSequence is valid.
func (s *stanServer) startSequenceValid(subject string, seq uint64) bool {
	cs := s.channels.Lookup(subject)
	cs.msgs.RLock()
	defer cs.msgs.RUnlock()
	if seq > cs.msgs.last || seq < cs.msgs.first {
		return false
	}
	return true
}

// Send messages to the subscriber starting at startSeq.
func (s *stanServer) sendMessagesFromSequence(cs *channelStore, sub *subState, startSeq uint64) {
	sub.Lock()
	sub.lastSent = startSeq - 1 // FIXME(dlc) - wrap?
	sub.Unlock()

	s.sendAvailableMessages(cs, sub)
}

// Send messages to the subscriber starting at startTime. Assumes startTime is valid.
func (s *stanServer) sendMessagesToSubFromTime(cs *channelStore, sub *subState, startTime int64) {
	// Do binary search to find starting sequence.
	cs.msgs.RLock()
	index := sort.Search(len(cs.msgs.msgs), func(i int) bool {
		m := cs.msgs.msgs[uint64(i)+cs.msgs.first]
		if m.Timestamp >= startTime {
			return true
		}
		return false
	})
	startSeq := uint64(index) + cs.msgs.first
	cs.msgs.RUnlock()

	s.sendMessagesFromSequence(cs, sub, startSeq)
}

// Send all messages to the subscriber.
func (s *stanServer) sendMessagesFromBeginning(cs *channelStore, sub *subState) {
	s.sendMessagesFromSequence(cs, sub, cs.msgs.FirstSequence())
}

// Send the last message we have to the subscriber
func (s *stanServer) sendLastMessage(cs *channelStore, sub *subState) {
	s.sendMessagesFromSequence(cs, sub, cs.msgs.LastSequence())
}

// Setup to send only new messages.
func (s *stanServer) sendNewOnly(cs *channelStore, sub *subState) {
	lastSeq := cs.msgs.LastSequence()
	sub.Lock()
	sub.lastSent = lastSeq
	sub.Unlock()
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
