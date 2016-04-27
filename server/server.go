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
	"github.com/nats-io/go-stan/pb"
	"github.com/nats-io/nats"
	"github.com/nats-io/nuid"
	"github.com/nats-io/stan-server/spb"

	natsd "github.com/nats-io/gnatsd/test"

	stores "github.com/nats-io/stan-server/stores"
	"regexp"
)

// A single STAN server

// Server defaults.
const (
	DefaultClusterID      = "test-cluster"
	DefaultDiscoverPrefix = "_STAN.discover"
	DefaultPubPrefix      = "_STAN.pub"
	DefaultSubPrefix      = "_STAN.sub"
	DefaultUnSubPrefix    = "_STAN.unsub"
	DefaultClosePrefix    = "_STAN.close"
	DefaultStoreType      = stores.TypeMemory

	// DefaultChannelLimit defines how many channels (literal subjects) we allow
	DefaultChannelLimit = 100
	// DefaultSubStoreLimit defines how many subscriptions per channel we allow
	DefaultSubStoreLimit = 1000
	// DefaultMsgStoreLimit defines how many messages per channel we allow
	DefaultMsgStoreLimit = 1000000
	// DefaultMsgSizeStoreLimit defines how many bytes per channel we allow
	DefaultMsgSizeStoreLimit = DefaultMsgStoreLimit * 1024

	// Heartbeat intervals.
	DefaultHeartBeatInterval   = 30 * time.Second
	DefaultClientHBTimeout     = 10 * time.Second
	DefaultMaxFailedHeartBeats = int((5 * time.Minute) / DefaultHeartBeatInterval)

	// Max number of outstanding go-routines handling connect requests for
	// duplicate client IDs.
	defaultMaxDupCIDRoutines = 100
	// Timeout used to ping the known client when processing a connection
	// request for a duplicate client ID.
	defaultCheckDupCIDTimeout = 500 * time.Millisecond
)

// Errors.
var (
	ErrInvalidSubject  = errors.New("stan: invalid subject")
	ErrInvalidSequence = errors.New("stan: invalid start sequence")
	ErrInvalidTime     = errors.New("stan: invalid start time")
	ErrInvalidSub      = errors.New("stan: invalid subscription")
	ErrInvalidClient   = errors.New("stan: clientID already registered")
	ErrInvalidAckWait  = errors.New("stan: invalid ack wait time, should be >= 1s")
	ErrInvalidConnReq  = errors.New("stan: invalid connection request")
	ErrInvalidPubReq   = errors.New("stan: invalid publish request")
	ErrInvalidSubReq   = errors.New("stan: invalid subscription request")
	ErrInvalidUnsubReq = errors.New("stan: invalid unsubscribe request")
	ErrInvalidCloseReq = errors.New("stan: invalid close request")
	ErrDupDurable      = errors.New("stan: duplicate durable registration")
	ErrDurableQueue    = errors.New("stan: queue subscribers can't be durable")
	ErrUnknownClient   = errors.New("stan: unkwown clientID")
)

// Shared regular expression to check clientID validity.
// No lock required since from doc: https://golang.org/pkg/regexp/
// A Regexp is safe for concurrent use by multiple goroutines.
var clientIDRegEx *regexp.Regexp

func init() {
	if re, err := regexp.Compile("^[a-zA-Z0-9_-]+$"); err != nil {
		panic("Unable to compile regular expression")
	} else {
		clientIDRegEx = re
	}
}

// StanServer structure represents the STAN server
type StanServer struct {
	sync.RWMutex
	shutdown      bool
	clusterID     string
	serverID      string
	pubPrefix     string // Subject prefix we received published messages on.
	subRequests   string // Subject we receive subscription requests on.
	unsubRequests string // Subject we receive unsubscribe requests on.
	closeRequests string // Subject we receive close requests on.
	natsServer    *server.Server
	opts          *Options
	nc            *nats.Conn
	wg            sync.WaitGroup // Wait on go routines during shutdown

	// For now, these will be set to the constants DefaultHeartBeatInterval, etc...
	// but allow to override in tests.
	hbInterval  time.Duration
	hbTimeout   time.Duration
	maxFailedHB int

	// Used when processing connect requests for client ID already registered
	dupCIDGuard       sync.RWMutex
	dupCIDMap         map[string]struct{}
	dupCIDwg          sync.WaitGroup // To wait for one routine to end when we have reached the max.
	dupCIDswg         bool           // To instruct one go routine to decrement the wait group.
	dupCIDTimeout     time.Duration
	dupMaxCIDRoutines int

	// Clients
	clients *clientStore

	// Store
	store stores.Store
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
type subState struct {
	sync.RWMutex
	spb.SubState // Embedded protobuf. Used for storage.
	subject      string
	qstate       *queueState
	// ackWait expressed as a duration so that we don't need
	// to multiply by time.Second anytime we use this.
	ackWait      time.Duration
	ackTimer     *time.Timer
	ackTimeFloor int64
	ackSub       *nats.Subscription
	acksPending  map[uint64]*pb.MsgProto
	stalled      bool
	store        stores.SubStore // for easy access to the store interface
}

// Looks up, or create a new channel if it does not exist
func (s *StanServer) lookupOrCreateChannel(channel string) (*stores.ChannelStore, error) {
	cs, isNew, err := s.store.LookupOrCreateChannel(channel)
	if err != nil {
		return nil, err
	}
	if isNew {
		s.addNewSubStoreToChannel(cs)
	}
	return cs, nil
}

func (s *StanServer) addNewSubStoreToChannel(cs *stores.ChannelStore) *subStore {
	subs := &subStore{
		psubs:    make([]*subState, 0, 4),
		qsubs:    make(map[string]*queueState),
		durables: make(map[string]*subState),
		acks:     make(map[string]*subState),
	}
	// Keep a reference to the subStore in the ChannelStore.
	cs.UserData = subs
	return subs
}

func (ss *subStore) Store(sub *subState) error {
	if sub == nil {
		return nil
	}
	sub.RLock()
	ackInbox := sub.AckInbox
	qgroup := sub.QGroup
	isDurable := sub.isDurable()
	subStateProto := &sub.SubState
	store := sub.store
	sub.RUnlock()

	// Adds to storage.
	err := store.CreateSub(subStateProto)
	if err != nil {
		sub.RLock()
		clientID := sub.ClientID
		inbox := sub.Inbox
		subject := sub.subject
		sub.RUnlock()
		Errorf("Unable to store subscription [%v:%v] on [%s]: %v", clientID, inbox, subject, err)
		return err
	}

	ss.Lock()
	ss.updateState(sub, ackInbox, qgroup, isDurable)
	ss.Unlock()

	return nil
}

// Updates the subStore state with this sub.
// The fields have been acquired from the sub's under its lock, so as not
// to require sub's lock in this function.
// The subStore is locked on entry (or does not need to - during startup -).
func (ss *subStore) updateState(sub *subState, ackInbox, qgroup string, isDurable bool) {
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
	sub.ClientID = ""
	if sub.ackSub != nil {
		sub.ackSub.Unsubscribe()
		sub.ackSub = nil
	}
	ackInbox := sub.AckInbox
	qs := sub.qstate
	durable := sub.DurableName
	subid := sub.ID
	store := sub.store
	sub.Unlock()

	// Delete from storage
	store.DeleteSub(subid)

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
		qs.subs, _ = sub.deleteFromList(qs.subs)
	} else {
		ss.psubs, _ = sub.deleteFromList(ss.psubs)
	}
}

// Lookup by durable name.
func (ss *subStore) LookupByDurable(durableName string) *subState {
	ss.RLock()
	defer ss.RUnlock()
	sub := ss.durables[durableName]
	return sub
}

// Lookup by ackInbox name.
func (ss *subStore) LookupByAckInbox(ackInbox string) *subState {
	ss.RLock()
	defer ss.RUnlock()
	return ss.acks[ackInbox]
}

// Options for STAN Server
type Options struct {
	ID               string
	DiscoverPrefix   string
	StoreType        string
	FilestoreDir     string
	MaxChannels      int
	MaxMsgs          int    // Maximum number of messages per channel
	MaxBytes         uint64 // Maximum number of bytes used by messages per channel
	MaxSubscriptions int    // Maximum number of subscriptions per channel
}

// DefaultOptions are default options for the STAN server
var DefaultOptions = Options{
	ID:             DefaultClusterID,
	DiscoverPrefix: DefaultDiscoverPrefix,
	StoreType:      DefaultStoreType,
}

// DefaultNatsServerOptions are default options for the NATS server
var DefaultNatsServerOptions = server.Options{
	Host:  "localhost",
	Port:  4222,
	NoLog: true,
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

// EnableDefaultLogger is a convenience API to set a default logger.
func EnableDefaultLogger(opts *server.Options) {
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

// RunServer will startup an embedded STAN server and a nats-server to support it.
func RunServer(ID string) *StanServer {
	sOpts := DefaultOptions
	sOpts.ID = ID
	return RunServerWithOpts(&sOpts, &DefaultNatsServerOptions)
}

// RunServerWithOpts will startup an embedded STAN server and a nats-server to support it.
func RunServerWithOpts(stanOpts *Options, natsOpts *server.Options) *StanServer {
	// Run a nats server by default
	var sOpts *Options
	var nOpts *server.Options

	if stanOpts == nil {
		sOpts = &DefaultOptions
	} else {
		sOpts = stanOpts
	}

	if natsOpts == nil {
		nOpts = &DefaultNatsServerOptions
	} else {
		nOpts = natsOpts
	}

	s := StanServer{
		clusterID:         sOpts.ID,
		serverID:          nuid.Next(),
		opts:              sOpts,
		hbInterval:        DefaultHeartBeatInterval,
		hbTimeout:         DefaultClientHBTimeout,
		maxFailedHB:       DefaultMaxFailedHeartBeats,
		dupCIDMap:         make(map[string]struct{}),
		dupMaxCIDRoutines: defaultMaxDupCIDRoutines,
		dupCIDTimeout:     defaultCheckDupCIDTimeout,
	}

	// Create clientStore
	s.clients = &clientStore{clients: make(map[string]*client)}

	// Set limits
	limits := &stores.ChannelLimits{
		MaxChannels: DefaultChannelLimit,
		MaxNumMsgs:  DefaultMsgStoreLimit,
		MaxMsgBytes: DefaultMsgStoreLimit * 1024,
		MaxSubs:     DefaultSubStoreLimit,
	}

	// Override with Options if needed
	overrideLimits(limits, sOpts)

	var err error
	var recoveredState *stores.RecoveredState
	var recoveredSubs []*subState

	// Ensure store type option is in upper-case
	sOpts.StoreType = strings.ToUpper(sOpts.StoreType)

	// Create the store. So far either memory or file-based.
	switch sOpts.StoreType {
	case stores.TypeFile:
		// The dir must be specified
		if sOpts.FilestoreDir == "" {
			err = fmt.Errorf("for %v stores, root directory must be specified", stores.TypeFile)
			break
		}
		s.store, recoveredState, err = stores.NewFileStore(sOpts.FilestoreDir, limits)
	case stores.TypeMemory:
		s.store, err = stores.NewMemoryStore(limits)
	default:
		err = fmt.Errorf("unsupported store type: %v", sOpts.StoreType)
	}
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}

	if recoveredState != nil {
		// Restore clients state
		s.processRecoveredClients(recoveredState.Clients)

		// Process recovered channels (if any).
		recoveredSubs = s.processRecoveredChannels(recoveredState.Subs)
	}

	// Generate Subjects
	// FIXME(dlc) guid needs to be shared in cluster mode
	s.pubPrefix = fmt.Sprintf("%s.%s", DefaultPubPrefix, nuid.Next())
	s.subRequests = fmt.Sprintf("%s.%s", DefaultSubPrefix, nuid.Next())
	s.unsubRequests = fmt.Sprintf("%s.%s", DefaultUnSubPrefix, nuid.Next())
	s.closeRequests = fmt.Sprintf("%s.%s", DefaultClosePrefix, nuid.Next())

	if nOpts.Host == "" {
		nOpts.Host = "localhost"
	}
	s.natsServer = natsd.RunServer(nOpts)

	natsURL := fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port)
	if s.nc, err = nats.Connect(natsURL); err != nil {
		panic(fmt.Sprintf("Can't connect to NATS server: %v\n", err))
	}

	nats.DisconnectHandler(stanDisconnectedHandler)
	nats.ErrorHandler(stanErrorHandler)
	nats.ClosedHandler(stanClosedHandler)

	s.initSubscriptions()

	// Do some post recovery processing (create subs on AckInbox, setup
	// some timers, etc...)
	if err := s.postRecoveryProcessing(recoveredSubs); err != nil {
		panic(fmt.Sprintf("Could not subscribe to ack subject, %v\n", err))
	}

	// Flush to make sure all subscriptions are processed before
	// we return control to the user.
	if err := s.nc.Flush(); err != nil {
		panic(fmt.Sprintf("Could not flush the subscriptions, %v\n", err))
	}

	Noticef("STAN: Message store is %s", s.store.Name())
	Noticef("STAN: Maximum of %d will be stored", DefaultMsgStoreLimit)

	return &s
}

func overrideLimits(limits *stores.ChannelLimits, opts *Options) {
	if opts.MaxChannels != 0 {
		limits.MaxChannels = opts.MaxChannels
	}
	if opts.MaxMsgs != 0 {
		limits.MaxNumMsgs = opts.MaxMsgs
	}
	if opts.MaxBytes != 0 {
		limits.MaxMsgBytes = opts.MaxBytes
	}
	if opts.MaxSubscriptions != 0 {
		limits.MaxSubs = opts.MaxSubscriptions
	}
}

// Reconstruct the clients map.
func (s *StanServer) processRecoveredClients(clients []*stores.RecoveredClient) {
	for _, c := range clients {
		// Note that s.clients does not have a reference to a store yet,
		// so this call cannot fail.
		if _, isNew, _ := s.clients.Register(c.ClientID, c.HbInbox); !isNew {
			Errorf("Ignoring duplicate clientID: %v", c.ClientID)
		}
	}
}

// Reconstruct the subscription state on restart.
// We don't use locking in there because there is no communication
// with the NATS server and/or clients, so no chance that the state
// changes while we are doing this.
func (s *StanServer) processRecoveredChannels(subscriptions stores.RecoveredSubscriptions) []*subState {
	// We will return the recovered subscriptions
	allSubs := make([]*subState, 0, 16)

	for channelName, recoveredSubs := range subscriptions {
		// Lookup the ChannelStore from the store
		channel := s.store.LookupChannel(channelName)
		// Create the subStore for this channel
		ss := s.addNewSubStoreToChannel(channel)
		// Get the recovered subscriptions for this channel.
		for _, recSub := range recoveredSubs {
			// Create a subState
			sub := &subState{
				subject:     channelName,
				ackWait:     time.Duration(recSub.Sub.AckWaitInSecs) * time.Second,
				acksPending: recSub.Pending,
				store:       channel.Subs,
			}
			// Ensure acksPending is not nil
			if sub.acksPending == nil {
				// Create an empty map
				sub.acksPending = make(map[uint64]*pb.MsgProto)
			}
			// Copy over fields from SubState protobuf
			sub.SubState = *recSub.Sub
			// Add the subscription to the corresponding client
			if s.clients.AddSub(sub.ClientID, sub) == nil {
				Errorf("Client %q not found, skipping subscription on ['%s']", sub.ClientID, channelName)
			} else {
				// Update subStore based on the following information.
				ss.updateState(sub, sub.AckInbox, sub.QGroup, sub.isDurable())
				// Add to the array
				allSubs = append(allSubs, sub)
			}
		}
	}
	return allSubs
}

// Do some final setup. Be minded of locking here since the server
// has started communication with NATS server/clients.
func (s *StanServer) postRecoveryProcessing(recoveredSubs []*subState) error {
	var err error
	for _, sub := range recoveredSubs {
		sub.Lock()
		// To be on the safe side, just check that the ackSub has not
		// been created (may happen with durables that may reconnect maybe?)
		if sub.ackSub == nil {
			// Subscribe to acks
			sub.ackSub, err = s.nc.Subscribe(sub.AckInbox, s.processAckMsg)
			if err != nil {
				sub.Unlock()
				return err
			}
		}
		if sub.ackTimer == nil {
			// Setup the redelivery timer. The callback will figure out
			// if there are messages to redeliver or not and adjust the
			// timer for us.
			sub.ackTimer = time.AfterFunc(sub.ackWait, func() {
				s.performAckExpirationRedelivery(sub)
			})
			// Set the floor to 0 since we don't know (or want to find
			// out) the lowest msg's timestamp for this subscription.
			sub.ackTimeFloor = 0
		}
		sub.Unlock()
	}
	// Set the store reference so that further Register/Unregister are stored.
	s.clients.SetStore(s.store)
	// Go through the list of clients and ensure their Hb timer is set.
	clients := s.clients.GetClients()
	for _, c := range clients {
		c.Lock()
		if c.hbt == nil {
			c.hbt = time.AfterFunc(s.hbInterval, func() { s.checkClientHealth(c.clientID) })
		}
		c.Unlock()
	}
	return nil
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
	if err != nil || !clientIDRegEx.MatchString(req.ClientID) || req.HeartbeatInbox == "" {
		Debugf("STAN: [Client:?] Invalid conn request: ClientID=%s, HBInbox=%s, err=%v.",
			req.ClientID, req.HeartbeatInbox, err)
		s.sendConnectErr(m.Reply, ErrInvalidConnReq.Error())
		return
	}

	// Try to register
	client, isNew, err := s.clients.Register(req.ClientID, req.HeartbeatInbox)
	if err != nil {
		Debugf("STAN: [Client:%s] Error registering client: %v", req.ClientID, err)
		s.sendConnectErr(m.Reply, err.Error())
		return
	}
	// Handle duplicate IDs in a dedicated go-routine
	if !isNew {
		// Do we have a routine in progress for this client ID?
		s.dupCIDGuard.RLock()
		_, inProgress := s.dupCIDMap[req.ClientID]
		s.dupCIDGuard.RUnlock()

		// Yes, fail this request here.
		if inProgress {
			Debugf("STAN: [Client:%s] Connect failed; already connected", req.ClientID)
			s.sendConnectErr(m.Reply, ErrInvalidClient.Error())
			return
		}

		// If server has started shutdown, we can't call wg.Add() so we need
		// to check on shutdown status. Note that s.wg is for all server's
		// go routines, not specific to duplicate CID handling. Use server's
		// lock here.
		s.Lock()
		shutdown := s.shutdown
		if !shutdown {
			// Assume we are going to start a go routine.
			s.wg.Add(1)
		}
		s.Unlock()

		if shutdown {
			// The client will timeout on connect
			return
		}

		// If we have exhausted the max number of go routines, we will have
		// to wait that one finishes.
		needToWait := false

		s.dupCIDGuard.Lock()
		s.dupCIDMap[req.ClientID] = struct{}{}
		if len(s.dupCIDMap) > s.dupMaxCIDRoutines {
			s.dupCIDswg = true
			s.dupCIDwg.Add(1)
			needToWait = true
		}
		s.dupCIDGuard.Unlock()

		// If we need to wait for a go routine to return...
		if needToWait {
			s.dupCIDwg.Wait()
		}
		// Start a go-routine to handle this connect request
		go func() {
			s.processConnectRequestWithDupID(client, req, m.Reply)
		}()
		return
	}

	// Here, we accept this client's incoming connect request.
	s.finishConnectRequest(client, m.Reply)
}

func (s *StanServer) finishConnectRequest(client *client, replyInbox string) {
	cr := &pb.ConnectResponse{
		PubPrefix:     s.pubPrefix,
		SubRequests:   s.subRequests,
		UnsubRequests: s.unsubRequests,
		CloseRequests: s.closeRequests,
	}
	b, _ := cr.Marshal()
	s.nc.Publish(replyInbox, b)

	s.RLock()
	hbInterval := s.hbInterval
	s.RUnlock()

	// Heartbeat timer.
	client.Lock()
	clientID := client.clientID
	client.hbt = time.AfterFunc(hbInterval, func() { s.checkClientHealth(clientID) })
	client.Unlock()

	Debugf("STAN: [Client:%s] connected.", clientID)
}

func (s *StanServer) processConnectRequestWithDupID(client *client, req *pb.ConnectRequest, replyInbox string) {
	sendErr := true

	client.RLock()
	hbInbox := client.hbInbox
	clientID := client.clientID
	client.RUnlock()

	defer s.wg.Done()
	defer func() {
		s.dupCIDGuard.Lock()
		delete(s.dupCIDMap, clientID)
		if s.dupCIDswg {
			s.dupCIDswg = false
			s.dupCIDwg.Done()
		}
		s.dupCIDGuard.Unlock()
	}()

	// This is the HbInbox from the "old" client. See if it is up and
	// running by sending a ping to that inbox.
	if _, err := s.nc.Request(hbInbox, nil, s.dupCIDTimeout); err != nil {
		// The old client didn't reply, assume it is dead, close it and continue.
		s.closeClient(client)

		// Between the close and the new registration below, it is possible
		// that a connection request came in (in connectCB) and since the
		// client is now unregistered, the new connection was accepted there.
		// The registration below will then fail, in which case we will fail
		// this request.

		// Need to re-register now based on the new request info.
		var isNew bool
		client, isNew, err = s.clients.Register(req.ClientID, req.HeartbeatInbox)
		if err == nil && isNew {
			// We could register the new client.
			sendErr = false
		}
	}
	// The currently registered client is responding, or we failed to register,
	// so fail the request of the incoming client connect request.
	if sendErr {
		Debugf("STAN: [Client:%s] Connect failed; already connected", clientID)
		s.sendConnectErr(replyInbox, ErrInvalidClient.Error())
		return
	}
	// We have replaced the old with the new.
	s.finishConnectRequest(client, replyInbox)
}

func (s *StanServer) sendConnectErr(replyInbox, err string) {
	cr := &pb.ConnectResponse{Error: err}
	b, _ := cr.Marshal()
	s.nc.Publish(replyInbox, b)
}

// Send a heartbeat call to the client.
func (s *StanServer) checkClientHealth(clientID string) {
	client := s.clients.Lookup(clientID)
	if client == nil {
		return
	}
	// Capture these under lock (as of now, there are not configurable,
	// but we tweak them in tests and maybe they will be settable in
	// the future)
	s.RLock()
	hbInterval := s.hbInterval
	hbTimeout := s.hbTimeout
	maxFailedHB := s.maxFailedHB
	s.RUnlock()

	client.Lock()
	_, err := s.nc.Request(client.hbInbox, nil, hbTimeout)
	if err != nil {
		client.fhb++
		if client.fhb > maxFailedHB {
			Debugf("STAN: [Client:%s]  Timed out on hearbeats.", client.clientID)
			client.Unlock()
			s.closeClient(client)
			return
		}
	} else {
		client.fhb = 0
	}
	client.hbt.Reset(hbInterval)
	client.Unlock()
}

// Close a client
func (s *StanServer) closeClient(client *client) {
	client.Lock()
	if client.hbt != nil {
		client.hbt.Stop()
	}
	clientID := client.clientID
	client.Unlock()

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
		s.sendCloseErr(m.Reply, ErrInvalidCloseReq.Error())
		return
	}

	client := s.clients.Lookup(req.ClientID)
	if client == nil {
		Errorf("STAN: Unknown client %q in close request", req.ClientID)
		s.sendCloseErr(m.Reply, ErrUnknownClient.Error())
		return
	}

	s.closeClient(client)

	resp := &pb.CloseResponse{}
	b, _ := resp.Marshal()
	s.nc.Publish(m.Reply, b)
}

func (s *StanServer) sendCloseErr(subj, err string) {
	resp := &pb.CloseResponse{Error: err}
	if b, err := resp.Marshal(); err == nil {
		s.nc.Publish(subj, b)
	}
}

// processClientPublish process inbound messages from clients.
func (s *StanServer) processClientPublish(m *nats.Msg) {
	pm := &pb.PubMsg{}
	pm.Unmarshal(m.Data)

	// TODO (cls) error check.

	// Make sure we have a clientID, guid, etc.
	if pm.Guid == "" || !s.isValidClient(pm.ClientID) || !isValidSubject(pm.Subject) {
		Errorf("STAN: Received invalid client publish message %v", pm)
		s.sendPublishErr(m.Reply, pm.Guid, ErrInvalidPubReq)
		return
	}

	////////////////////////////////////////////////////////////////////////////
	// This is where we will store the message and wait for others in the
	// potential cluster to do so as well, once we have a quorom someone can
	// ack the publisher. We simply do so here for now.
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	// Once we have ack'd the publisher, we need to assign this a sequence ID.
	// This will be done by a master election within the cluster, for now we
	// assume we are the master and assign the sequence ID here.
	////////////////////////////////////////////////////////////////////////////

	// Store first, and check for error.
	cs, err := s.assignAndStore(pm)
	if err != nil {
		Errorf("STAN: [Client:%s] Error processing message for subject %q: %v", pm.ClientID, pm.Subject, err)
		s.sendPublishErr(m.Reply, pm.Guid, err)
		return
	}

	s.ackPublisher(pm, m.Reply)

	////////////////////////////////////////////////////////////////////////////
	// Now trigger sends to any active subscribers
	////////////////////////////////////////////////////////////////////////////

	s.processMsg(cs)
}

func (s *StanServer) sendPublishErr(subj, guid string, err error) {
	badMsgAck := &pb.PubAck{Guid: guid, Error: err.Error()}
	if b, err := badMsgAck.Marshal(); err == nil {
		s.nc.Publish(subj, b)
	}
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

		sub.RLock()
		sOut := len(sub.acksPending)
		sub.RUnlock()

		if sOut < rOut {
			rsub = sub
		}
	}

	len := len(sl)
	if len > 1 && rsub == sl[0] {
		copy(sl, sl[1:len])
		sl[len-1] = rsub
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
	lastSent := sub.LastSent
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
func (s *StanServer) processMsg(cs *stores.ChannelStore) {
	ss := cs.UserData.(*subStore)

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
	Debugf("STAN:[Client:%s] Redelivering to durable %s.", sub.ClientID, sub.DurableName)
	s.performRedelivery(sub, false)
}

// Redeliver all outstanding messages that have expired.
func (s *StanServer) performAckExpirationRedelivery(sub *subState) {
	Debugf("STAN: [Client:%s] Redelivering on ack expiration. subject=%s, inbox=%s.",
		sub.ClientID, sub.subject, sub.Inbox)
	s.performRedelivery(sub, true)
}

// Performs redelivery, takes a flag on whether to honor expiration.
func (s *StanServer) performRedelivery(sub *subState, checkExpiration bool) {
	// Sort our messages outstanding from acksPending, grab some state and unlock.
	sub.RLock()
	expTime := int64(sub.ackWait)
	sortedMsgs := makeSortedMsgs(sub.acksPending)
	subject := sub.subject
	qs := sub.qstate
	clientID := sub.ClientID
	floorTimestamp := sub.ackTimeFloor
	sub.RUnlock()

	// If we don't find the client, we are done.
	client := s.clients.Lookup(clientID)
	if client == nil {
		return
	}
	// If the client has some failed heartbeats, ignore this request.
	client.RLock()
	fhbs := client.fhb
	client.RUnlock()
	if fhbs != 0 {
		// Reset the timer.
		sub.Lock()
		if sub.ackTimer != nil {
			sub.ackTimer.Reset(sub.ackWait)
		}
		sub.Unlock()
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

			cs := s.store.LookupChannel(subject)
			ss := cs.UserData.(*subStore)

			ss.Lock()
			pick = findBestQueueSub(qs.subs)
			ss.Unlock()

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
		sub.ClientID, m.Subject, sub.Inbox, m.Sequence)

	// Don't send if we have too many outstanding already.
	if int32(len(sub.acksPending)) >= sub.MaxInFlight {
		sub.stalled = true
		Debugf("STAN: [Client:%s] Stalled msgseq %s:%d to %s.",
			sub.ClientID, m.Subject, m.Sequence, sub.Inbox)
		return false
	}

	b, _ := m.Marshal()
	if err := s.nc.Publish(sub.Inbox, b); err != nil {
		Errorf("STAN: [Client:%s] Failed Sending msgseq %s:%d to %s (%s).",
			sub.ClientID, m.Subject, m.Sequence, sub.Inbox, err)
		return false
	}

	// Store in storage (only if not a redelivery)
	if !m.Redelivered {
		if err := sub.store.AddSeqPending(sub.ID, m.Sequence); err != nil {
			Errorf("STAN: [Client:%s] Unable to update subscription for %s:%v (%v)",
				sub.ClientID, m.Subject, m.Sequence, err)
			return false
		}
	}

	// Store in ackPending.
	sub.acksPending[m.Sequence] = m

	// Setup the ackTimer as needed.
	if sub.ackTimer == nil {
		sub.ackTimer = time.AfterFunc(sub.ackWait, func() {
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
		sub.LastSent = m.Sequence
		return true
	}
	return false
}

// assignAndStore will assign a sequence ID and then store the message.
func (s *StanServer) assignAndStore(pm *pb.PubMsg) (*stores.ChannelStore, error) {
	cs, err := s.lookupOrCreateChannel(pm.Subject)
	if err != nil {
		return nil, err
	}
	if _, err := cs.Msgs.Store(pm.Reply, pm.Data); err != nil {
		return nil, err
	}
	return cs, nil
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
func (sub *subState) deleteFromList(sl []*subState) ([]*subState, bool) {
	for i := 0; i < len(sl); i++ {
		if sl[i] == sub {
			sl[i] = sl[len(sl)-1]
			sl[len(sl)-1] = nil
			sl = sl[:len(sl)-1]
			return shrinkSubListIfNeeded(sl), true
		}
	}
	return sl, false
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
	subs := s.clients.GetSubs(clientID)
	for _, sub := range subs {
		sub.Lock()
		sub.clearAckTimer()
		subject := sub.subject
		isDurable := sub.isDurable()
		sub.ClientID = ""
		ackInbox := sub.AckInbox
		// Unsubscribe the ackSub, even for a durable: a new one will be
		// created on durable restart.
		if sub.ackSub != nil {
			sub.ackSub.Unsubscribe()
			sub.ackSub = nil
		}
		sub.Unlock()

		cs := s.store.LookupChannel(subject)
		if cs == nil {
			continue
		}

		// Get the subStore from the ChannelStore
		ss := cs.UserData.(*subStore)

		// Special handling of durables
		if isDurable {
			// Delete from ackInbox lookup. When a durable restarts, a new
			// ackInbox is created (and stored in ss.acks)
			ss.Lock()
			delete(ss.acks, ackInbox)
			ss.Unlock()

			// Skip removal if durable.
			continue
		}

		// Remove from subStore
		ss.Remove(sub)
	}
}

// processUnSubscribeRequest will process a unsubscribe request.
func (s *StanServer) processUnSubscribeRequest(m *nats.Msg) {
	req := &pb.UnsubscribeRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		Errorf("STAN: Invalid unsub request from %s.", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidUnsubReq)
		return
	}

	cs := s.store.LookupChannel(req.Subject)
	if cs == nil {
		Errorf("STAN: [Client:%s] unsub request missing subject %s.",
			req.ClientID, req.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}

	// Get the subStore
	ss := cs.UserData.(*subStore)

	sub := ss.LookupByAckInbox(req.Inbox)
	if sub == nil {
		Errorf("STAN: [Client:%s] unsub request for missing inbox %s.",
			req.ClientID, req.Inbox)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}

	// Remove from Client
	if s.clients.RemoveSub(req.ClientID, sub) == nil {
		Errorf("STAN: [Client:%s] unsub request for missing client", req.ClientID)
		s.sendSubscriptionResponseErr(m.Reply, ErrUnknownClient)
		return
	}

	// Remove the subscription.
	ss.Remove(sub)

	Debugf("STAN: [Client:%s] Unsubscribing subject=%s.", req.ClientID, sub.subject)

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
// default sub.ackWait value if the given timestamp is
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

		// ackWait in int64
		expTime := int64(sub.ackWait)

		// If the message timestamp + expiration is in the past
		// (which will happen when a message is redelivered more
		// than once), or if timestamp is 0, use the default ackWait
		if firstUnackedTimestamp+expTime <= now {
			sub.ackTimer.Reset(sub.ackWait)
		} else {
			// Compute the time the ackTimer should fire, which is the
			// ack timeout less the duration the message has been in
			// the server.
			fireIn := (firstUnackedTimestamp + expTime - now)

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
	return sub != nil && sub.QGroup != ""
}

// Test if a subscription is durable.
func (sub *subState) isDurable() bool {
	return sub != nil && sub.DurableName != ""
}

// Used to generate durable key. This should not be called on non-durables.
func (sub *subState) durableKey() string {
	if sub.DurableName == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s", sub.ClientID, sub.subject, sub.DurableName)
}

// Used to generate durable key. This should not be called on non-durables.
func durableKey(sr *pb.SubscriptionRequest) string {
	if sr.DurableName == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s", sr.ClientID, sr.Subject, sr.DurableName)
}

func (s *StanServer) addSubscription(cs *stores.ChannelStore, sub *subState) error {

	// Store in client
	if client := s.clients.AddSub(sub.ClientID, sub); client != nil {
		// Store this subscription
		ss := cs.UserData.(*subStore)
		if err := ss.Store(sub); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("can't find clientID: %v", sub.ClientID)
	}

	Debugf("STAN: [Client:%s] Subscribed. subject=%s, inbox=%s.",
		sub.ClientID, sub.subject, sub.Inbox)

	return nil
}

// processSubscriptionRequest will process a subscription request.
func (s *StanServer) processSubscriptionRequest(m *nats.Msg) {
	sr := &pb.SubscriptionRequest{}
	err := sr.Unmarshal(m.Data)
	if err != nil {
		Errorf("STAN:  Invalid Subscription request from %s.", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSubReq)
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
			errors.New("stan: malformed subscription request, clientID missing"))
		return
	}

	// Grab channel state, create a new one if needed.
	cs, err := s.lookupOrCreateChannel(sr.Subject)
	if err != nil {
		Errorf("STAN: Unable to create store for subject %s.", sr.Subject)
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}

	var sub *subState

	ackInbox := nats.NewInbox()

	// Check for DurableSubscriber status
	if sr.DurableName != "" {
		// Can't be durable and a queue subscriber
		if sr.QGroup != "" {
			Debugf("STAN: [Client:%s] Invalid subscription request; cannot be both durable and a queue subscriber.",
				sr.ClientID)
			s.sendSubscriptionResponseErr(m.Reply, ErrDurableQueue)
			return
		}

		ss := cs.UserData.(*subStore)

		if sub = ss.LookupByDurable(durableKey(sr)); sub != nil {
			sub.RLock()
			clientID := sub.ClientID
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
			// Set clientID and new AckInbox, reset lastSent
			sub.AckInbox = ackInbox
			sub.ClientID = sr.ClientID
			sub.Inbox = sr.Inbox
			sub.Unlock()

			// We need to add the subscription to the client's state here.
			if s.clients.AddSub(sr.ClientID, sub) == nil {
				Debugf("STAN: [Client:%s] Couldn't add durable %s",
					sr.ClientID, sr.DurableName)
				s.sendSubscriptionResponseErr(m.Reply, ErrUnknownClient)
				return
			}

			// Store by ackInbox for ack direct lookup. This was removed
			// when the previous client went away.
			ss.Lock()
			ss.acks[sub.AckInbox] = sub
			ss.Unlock()
		}
	}

	// Check SequenceStart out of range
	if sr.StartPosition == pb.StartPosition_SequenceStart {
		if !s.startSequenceValid(cs, sr.Subject, sr.StartSequence) {
			Debugf("STAN: [Client:%s] Invalid start sequence in subscription request from %s.",
				sr.ClientID, m.Subject)
			s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSequence)
			return
		}
	}
	// Check for SequenceTime out of range
	if sr.StartPosition == pb.StartPosition_TimeDeltaStart {
		startTime := time.Now().UnixNano() - sr.StartTimeDelta
		if !s.startTimeValid(cs, sr.Subject, startTime) {
			Debugf("STAN: [Client:%s] Invalid start time in subscription request from %s.",
				sr.ClientID, m.Subject)
			s.sendSubscriptionResponseErr(m.Reply, ErrInvalidTime)
			return
		}
	}

	// Create a subState if not retrieved from durable lookup above.
	if sub == nil {
		sub = &subState{
			SubState: spb.SubState{
				ClientID:      sr.ClientID,
				QGroup:        sr.QGroup,
				Inbox:         sr.Inbox,
				AckInbox:      ackInbox,
				MaxInFlight:   sr.MaxInFlight,
				AckWaitInSecs: sr.AckWaitInSecs,
				DurableName:   sr.DurableName,
			},
			subject:     sr.Subject,
			ackWait:     time.Duration(sr.AckWaitInSecs) * time.Second,
			acksPending: make(map[uint64]*pb.MsgProto),
			store:       cs.Subs,
		}

		// set the start sequence of the subscriber.
		s.setSubStartSequence(cs, sub, sr)

		// add the subscription to stan
		if err := s.addSubscription(cs, sub); err != nil {
			Errorf("STAN: Unable to persist subscription for %s: %v", sr.Subject, err)
			s.sendSubscriptionResponseErr(m.Reply, err)
			return
		}
	}

	// In case this is a durable, sub already exists so we need to protect access
	sub.Lock()
	// Subscribe to acks
	sub.ackSub, err = s.nc.Subscribe(ackInbox, s.processAckMsg)
	if err != nil {
		sub.Unlock()
		panic(fmt.Sprintf("Could not subscribe to ack subject, %v\n", err))
	}
	sub.Unlock()

	// Create a non-error response
	resp := &pb.SubscriptionResponse{AckInbox: ackInbox}
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
	cs := s.store.LookupChannel(ack.Subject)
	if cs == nil {
		Errorf("STAN: [Client:?] Ack received, invalid channel (%s)", ack.Subject)
		return
	}
	s.processAck(cs, cs.UserData.(*subStore).LookupByAckInbox(m.Subject), ack)
}

// processAck processes an ack and if needed sends more messages.
func (s *StanServer) processAck(cs *stores.ChannelStore, sub *subState, ack *pb.Ack) {
	if sub == nil || ack == nil {
		return
	}

	sub.Lock()

	// Clear the ack
	Tracef("STAN: [Client:%s] removing pending ack, subj=%s, seq=%d.",
		sub.ClientID, sub.subject, ack.Sequence)

	if err := sub.store.AckSeqPending(sub.ID, ack.Sequence); err != nil {
		Errorf("STAN: [Client:%s] Unable to persist ack for %s:%v (%v)",
			sub.ClientID, sub.subject, ack.Sequence, err)
		sub.Unlock()
		return
	}

	delete(sub.acksPending, ack.Sequence)
	stalled := sub.stalled
	if int32(len(sub.acksPending)) < sub.MaxInFlight {
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
func (s *StanServer) sendAvailableMessagesToQueue(cs *stores.ChannelStore, qs *queueState) {
	if cs == nil || qs == nil {
		return
	}

	qs.Lock()
	defer qs.Unlock()

	for nextSeq := qs.lastSent + 1; ; nextSeq++ {
		nextMsg := cs.Msgs.Lookup(nextSeq)
		if nextMsg == nil || s.sendMsgToQueueGroup(qs, nextMsg) == false {
			break
		}
	}
}

// Send any messages that are ready to be sent that have been queued.
func (s *StanServer) sendAvailableMessages(cs *stores.ChannelStore, sub *subState) {
	sub.Lock()
	defer sub.Unlock()

	for nextSeq := sub.LastSent + 1; ; nextSeq++ {
		nextMsg := cs.Msgs.Lookup(nextSeq)
		if nextMsg == nil || s.sendMsgToSubAndUpdateLastSent(sub, nextMsg) == false {
			break
		}
	}
}

// Check if a startTime is valid.
func (s *StanServer) startTimeValid(cs *stores.ChannelStore, subject string, start int64) bool {
	firstMsg := cs.Msgs.FirstMsg()
	// simply no messages to return
	if firstMsg == nil {
		return false
	}
	lastMsg := cs.Msgs.LastMsg()
	if start > lastMsg.Timestamp || start < firstMsg.Timestamp {
		return false
	}
	return true
}

// Check if a startSequence is valid.
func (s *StanServer) startSequenceValid(cs *stores.ChannelStore, subject string, seq uint64) bool {
	first, last := cs.Msgs.FirstAndLastSequence()
	if first == 0 || seq > last || seq < first {
		return false
	}
	return true
}

func (s *StanServer) getSequenceFromStartTime(cs *stores.ChannelStore, startTime int64) uint64 {
	return cs.Msgs.GetSequenceFromTimestamp(startTime)
}

// Setup the start position for the subscriber.
func (s *StanServer) setSubStartSequence(cs *stores.ChannelStore, sub *subState, sr *pb.SubscriptionRequest) {
	sub.Lock()
	defer sub.Unlock()

	lastSent := uint64(0)

	// In all start position cases, if there is no message, ensure
	// lastSent stays at 0.

	switch sr.StartPosition {
	case pb.StartPosition_NewOnly:
		lastSent = cs.Msgs.LastSequence()
		Debugf("STAN: [Client:%s] Sending new-only subject=%s, seq=%d.",
			sub.ClientID, sub.subject, lastSent)
	case pb.StartPosition_LastReceived:
		lastSeq := cs.Msgs.LastSequence()
		if lastSeq > 0 {
			lastSent = lastSeq - 1
		}
		Debugf("STAN: [Client:%s] Sending last message, subject=%s.",
			sub.ClientID, sub.subject)
	case pb.StartPosition_TimeDeltaStart:
		startTime := time.Now().UnixNano() - sr.StartTimeDelta
		seq := s.getSequenceFromStartTime(cs, startTime)
		if seq > 0 {
			lastSent = seq - 1
		}
		Debugf("STAN: [Client:%s] Sending from time, subject=%s time=%d seq=%d",
			sub.ClientID, sub.subject, startTime, lastSent)
	case pb.StartPosition_SequenceStart:
		if sr.StartSequence > 0 {
			lastSent = sr.StartSequence - 1
		}
		Debugf("STAN: [Client:%s] Sending from sequence, subject=%s seq=%d",
			sub.ClientID, sub.subject, lastSent)
	case pb.StartPosition_First:
		firstSeq := cs.Msgs.FirstSequence()
		if firstSeq > 0 {
			lastSent = firstSeq - 1
		}
		Debugf("STAN: [Client:%s] Sending from beginning, subject=%s seq=%d",
			sub.ClientID, sub.subject, lastSent)
	}
	sub.LastSent = lastSent
}

// ClusterID returns the STAN Server's ID.
func (s *StanServer) ClusterID() string {
	return s.clusterID
}

// Shutdown will close our NATS connection and shutdown any embedded NATS server.
func (s *StanServer) Shutdown() {
	Debugf("STAN: Shutting down.")

	s.Lock()
	if s.shutdown {
		s.Unlock()
		return
	}

	// Allows Shutdown() to be idempotent
	s.shutdown = true

	// Capture under lock
	store := s.store
	ns := s.natsServer
	// Do not set s.nc to nil since it is used in many place without locking.
	// Once closed, s.nc.xxx() calls will simply fail, but we won't panic.
	nc := s.nc
	s.Unlock()

	// Close/Shutdown resources. Note that unless one instantiates StanServer
	// directly (instead of calling RunServer() and the like), these should
	// not be nil.

	// Wait for go-routines to return
	s.wg.Wait()

	if store != nil {
		store.Close()
	}
	if nc != nil {
		nc.Close()
	}
	if ns != nil {
		ns.Shutdown()
	}
}
