// Copyright 2016 Apcera Inc. All rights reserved.

package stan

import (
	"errors"
	"fmt"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"

	natsd "github.com/nats-io/gnatsd/test"
)

// A mock single STAN server

const (
	DefaultPubPrefix = "stan.pub" // We will add on GUID here.
	DefaultSubPrefix = "stan.sub" // We will add on GUID here.

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
	msgStores     map[string]*msgStore
	msgStoreLimit int

	// Subscription Storage
	subStore map[string][]*serverSubscription
	// Used to lookup subscriptions by the INBOX they came in on.
	ackStore map[string]*serverSubscription
}

// What is stored in the log/store
type msg struct {
	seq   uint64
	reply string
	data  []byte
}

// Per channel/subject store
type msgStore struct {
	subject string // Can't be wildcard
	cur     uint64
	first   uint64
	last    uint64
	msgs    map[uint64]*msg
}

// Holds Subscription state
type serverSubscription struct {
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
	s.ackStore = make(map[string]*serverSubscription)

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
	pe := &PubEnvelope{}
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

	s.processMsg(pe.Subject, msg)
}

// processMsg will proces a message, and possibly send to clients, etc.
func (s *stanServer) processMsg(subject string, m *msg) {
	// Grab active subscriptions
	sl := s.subStore[subject]
	if sl == nil || len(sl) == 0 {
		return
	}
	// Walk the subscribers
	for _, sub := range sl {
		sub.lastQueued = m.seq
		// Check if we have too many outstanding. Ack receipt will unblock
		if sub.lastSent-sub.lastAck <= sub.maxInFlight {
			// We can send direct here.
			sub.lastSent = m.seq
			s.sendMsgToSub(sub.inbox, m)
		}
	}
}

func (s *stanServer) sendMsgToSub(inbox string, m *msg) {
	pmsg := &Msg{Seq: m.seq, Reply: m.reply, Data: m.data}
	b, _ := pmsg.Marshal()
	s.nc.Publish(inbox, b)
}

// assignAndStore will assign a sequencId and then store the message.
func (s *stanServer) assignAndStore(pe *PubEnvelope) (*msg, error) {
	store := s.msgStores[pe.Subject]
	if store == nil {
		store = &msgStore{subject: pe.Subject, cur: 1, first: 1, last: 1}
		store.msgs = make(map[uint64]*msg, s.msgStoreLimit)
		s.msgStores[pe.Subject] = store
	}
	m := &msg{seq: store.cur, reply: pe.Reply, data: pe.Data}
	store.msgs[store.cur] = m
	store.cur++
	store.last = store.cur

	// Check if we need to remove any.
	if len(store.msgs) >= s.msgStoreLimit {
		fmt.Printf("Removing message[%d] from the store for [`%s`]\n", store.first, pe.Subject)
		delete(store.msgs, store.first)
		store.first++
	}
	return m, nil
}

// ackPublisher sends the ack for a message.
func (s *stanServer) ackPublisher(pe *PubEnvelope, reply string) {
	msgAck := &PubAck{Id: pe.Id}
	var buf [32]byte
	b := buf[:]
	n, _ := msgAck.MarshalTo(b)
	s.nc.Publish(reply, b[:n])
}

// storeSubscription will store the subscription.
func (s *stanServer) storeSubscription(sub *serverSubscription) {
	sl := s.subStore[sub.subject]
	if sl == nil {
		sl = make([]*serverSubscription, 0, 8)
	}
	s.subStore[sub.subject] = append(sl, sub)
	s.ackStore[sub.ackInbox] = sub
}

// processSubscriptionRequest will process a subscription request
func (s *stanServer) processSubscriptionRequest(m *nats.Msg) {
	sr := &SubscriptionRequest{}
	err := sr.Unmarshal(m.Data)
	if err != nil {
		resp := &SubscriptionResponse{Error: err.Error()}
		b, _ := resp.Marshal()
		s.nc.Publish(m.Reply, b)
	}

	// FIXME(dlc) check for errors, mis-configurations, etc.

	// Create a serverSubscription
	ss := &serverSubscription{
		subject:       sr.Subject,
		queue:         sr.Queue,
		inbox:         sr.Inbox,
		ackInbox:      newInbox(),
		name:          sr.DurableName,
		maxInFlight:   uint64(sr.MaxInFlight),
		ackWaitInSecs: sr.AckWaitInSecs,
	}
	// Store this subscription
	s.storeSubscription(ss)

	// Subscribe to acks
	ss.ackSub, err = s.nc.Subscribe(ss.ackInbox, s.processAck)
	if err != nil {
		panic(fmt.Sprintf("Could not subscribe to ack subject, %v\n", err))
	}

	// Create a response
	r := &SubscriptionResponse{AckInbox: ss.ackInbox}
	b, _ := r.Marshal()
	s.nc.Publish(m.Reply, b)
}

// processAck processes inbound acks from clients for delivered messages.
func (s *stanServer) processAck(m *nats.Msg) {
	ack := &Ack{}
	ack.Unmarshal(m.Data)
	sub := s.ackStore[m.Subject]
	if ack.Seq > sub.lastAck {
		sub.lastAck = ack.Seq
	}
	store := s.msgStores[sub.subject]
	// Check to see if we should send more messages
	for d := sub.lastQueued - sub.lastSent; d > 0; d = sub.lastQueued - sub.lastSent {
		nextSeq := sub.lastSent + 1
		if nextMsg := store.msgs[nextSeq]; nextMsg != nil {
			sub.lastSent++
			s.sendMsgToSub(sub.inbox, nextMsg)
		}
	}
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
