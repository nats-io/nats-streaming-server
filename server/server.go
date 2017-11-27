// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	natsdLogger "github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
	"github.com/nats-io/nuid"
)

// A single STAN server

// Server defaults.
const (
	// VERSION is the current version for the NATS Streaming server.
	VERSION = "0.7.0"

	DefaultClusterID      = "test-cluster"
	DefaultDiscoverPrefix = "_STAN.discover"
	DefaultPubPrefix      = "_STAN.pub"
	DefaultSubPrefix      = "_STAN.sub"
	DefaultSubClosePrefix = "_STAN.subclose"
	DefaultUnSubPrefix    = "_STAN.unsub"
	DefaultClosePrefix    = "_STAN.close"
	DefaultStoreType      = stores.TypeMemory

	// The prefixes should not have been made public (since we do not expose ways
	// to change them). Add the new ones as private.
	acksSubsPoolPrefix = "_STAN.subacks"

	// Prefix of subject active server is sending HBs to
	ftHBPrefix = "_STAN.ft"

	// DefaultHeartBeatInterval is the interval at which server sends heartbeat to a client
	DefaultHeartBeatInterval = 30 * time.Second
	// DefaultClientHBTimeout is how long server waits for a heartbeat response
	DefaultClientHBTimeout = 10 * time.Second
	// DefaultMaxFailedHeartBeats is the number of failed heartbeats before server closes
	// the client connection (total= (heartbeat interval + heartbeat timeout) * (fail count + 1)
	DefaultMaxFailedHeartBeats = int((5 * time.Minute) / DefaultHeartBeatInterval)

	// Max number of outstanding go-routines handling connect requests for
	// duplicate client IDs.
	defaultMaxDupCIDRoutines = 100
	// Timeout used to ping the known client when processing a connection
	// request for a duplicate client ID.
	defaultCheckDupCIDTimeout = 500 * time.Millisecond

	// DefaultIOBatchSize is the maximum number of messages to accumulate before flushing a store.
	DefaultIOBatchSize = 1024

	// DefaultIOSleepTime is the duration (in micro-seconds) the server waits for more messages
	// before starting processing. Set to 0 (or negative) to disable the wait.
	DefaultIOSleepTime = int64(0)

	// Length of the channel used to schedule subscriptions start requests.
	// Subscriptions requests are processed from the same NATS subscription.
	// When a subscriber starts and it has pending messages, the server
	// processes the new subscription request and sends avail messages out
	// (up to MaxInflight). When done in place, this can cause other
	// new subscriptions requests to timeout. Server uses a channel to schedule
	// start (that is sending avail messages) of new subscriptions. This is
	// the default length of that channel.
	defaultSubStartChanLen = 2048

	// Length of the NATS Inbox prefix
	natsInboxPrefixLen = len(nats.InboxPrefix)
	// First character of a NATS Inbox. When using the ackSub pool,
	// ackInboxes will start with a number.
	natsInboxFirstChar = '_'
	// Length of a NATS inbox
	natsInboxLen = 29 // _INBOX.<nuid: 22 characters>
)

// Constant to indicate that sendMsgToSub() should check number of acks pending
// against MaxInFlight to know if message should be sent out.
const (
	forceDelivery    = true
	honorMaxInFlight = false
)

// Errors.
var (
	ErrInvalidSubject     = errors.New("stan: invalid subject")
	ErrInvalidStart       = errors.New("stan: invalid start position")
	ErrInvalidSub         = errors.New("stan: invalid subscription")
	ErrInvalidClient      = errors.New("stan: clientID already registered")
	ErrMissingClient      = errors.New("stan: clientID missing")
	ErrInvalidClientID    = errors.New("stan: invalid clientID: only alphanumeric and `-` or `_` characters allowed")
	ErrInvalidAckWait     = errors.New("stan: invalid ack wait time, should be >= 1s")
	ErrInvalidMaxInflight = errors.New("stan: invalid MaxInflight, should be >= 1")
	ErrInvalidConnReq     = errors.New("stan: invalid connection request")
	ErrInvalidPubReq      = errors.New("stan: invalid publish request")
	ErrInvalidSubReq      = errors.New("stan: invalid subscription request")
	ErrInvalidUnsubReq    = errors.New("stan: invalid unsubscribe request")
	ErrInvalidCloseReq    = errors.New("stan: invalid close request")
	ErrDupDurable         = errors.New("stan: duplicate durable registration")
	ErrInvalidDurName     = errors.New("stan: durable name of a durable queue subscriber can't contain the character ':'")
	ErrUnknownClient      = errors.New("stan: unknown clientID")
	ErrNoChannel          = errors.New("stan: no configured channel")
)

// Shared regular expression to check clientID validity.
// No lock required since from doc: https://golang.org/pkg/regexp/
// A Regexp is safe for concurrent use by multiple goroutines.
var clientIDRegEx *regexp.Regexp

var testAckWaitIsInMillisecond bool

func computeAckWait(wait int32) time.Duration {
	unit := time.Second
	if testAckWaitIsInMillisecond && wait < 0 {
		wait = wait * -1
		unit = time.Millisecond
	}
	return time.Duration(wait) * unit
}

func init() {
	if re, err := regexp.Compile("^[a-zA-Z0-9_-]+$"); err != nil {
		panic("Unable to compile regular expression")
	} else {
		clientIDRegEx = re
	}
}

// ioPendingMsg is a record that embeds the pointer to the incoming
// NATS Message, the PubMsg and PubAck structures so we reduce the
// number of memory allocations to 1 when processing a message from
// producer.
type ioPendingMsg struct {
	m  *nats.Msg
	pm pb.PubMsg
	pa pb.PubAck
}

// Constant that defines the size of the channel that feeds the IO thread.
const ioChannelSize = 64 * 1024

const (
	scheduleRequest = true
	processRequest  = false
)

// subStartInfo contains information used when a subscription request
// is successful and the start (sending avail messages) is scheduled.
type subStartInfo struct {
	c         *channel
	sub       *subState
	qs        *queueState
	isDurable bool
}

// State represents the possible server states
type State int8

// Possible server states
const (
	Standalone State = iota
	FTActive
	FTStandby
	Failed
	Shutdown
)

func (state State) String() string {
	switch state {
	case Standalone:
		return "STANDALONE"
	case FTActive:
		return "FT_ACTIVE"
	case FTStandby:
		return "FT_STANDBY"
	case Failed:
		return "FAILED"
	case Shutdown:
		return "SHUTDOWN"
	default:
		return "UNKNOW STATE"
	}
}

type channelStore struct {
	sync.RWMutex
	channels map[string]*channel
	store    stores.Store
}

func newChannelStore(s stores.Store) *channelStore {
	cs := &channelStore{
		channels: make(map[string]*channel),
		store:    s,
	}
	return cs
}

func (cs *channelStore) get(name string) *channel {
	cs.RLock()
	c := cs.channels[name]
	cs.RUnlock()
	return c
}

func (cs *channelStore) createChannel(s *StanServer, name string) (*channel, error) {
	cs.Lock()
	defer cs.Unlock()
	// It is possible that there were 2 concurrent calls to lookupOrCreateChannel
	// which first uses `channelStore.get()` and if not found, calls this function.
	// So we need to check now that we have the write lock that the channel has
	// not already been created.
	c := cs.channels[name]
	if c != nil {
		return c, nil
	}
	sc, err := cs.store.CreateChannel(name)
	if err != nil {
		return nil, err
	}
	return cs.create(s, name, sc), nil
}

// low-level creation and storage in memory of a *channel
// Lock is held on entry or not needed.
func (cs *channelStore) create(s *StanServer, name string, sc *stores.Channel) *channel {
	c := &channel{name: name, store: sc, ss: s.createSubStore()}
	cs.channels[name] = c
	return c
}

func (cs *channelStore) getAll() map[string]*channel {
	cs.RLock()
	m := make(map[string]*channel, len(cs.channels))
	for k, v := range cs.channels {
		m[k] = v
	}
	cs.RUnlock()
	return m
}

func (cs *channelStore) msgsState(channelName string) (int, uint64, error) {
	cs.RLock()
	defer cs.RUnlock()
	if channelName != "" {
		c := cs.channels[channelName]
		if c == nil {
			return 0, 0, fmt.Errorf("channel %q not found", channelName)
		}
		return c.store.Msgs.State()
	}
	var (
		count int
		bytes uint64
	)
	for _, c := range cs.channels {
		m, b, err := c.store.Msgs.State()
		if err != nil {
			return 0, 0, err
		}
		count += m
		bytes += b
	}
	return count, bytes, nil
}

func (cs *channelStore) count() int {
	cs.RLock()
	count := len(cs.channels)
	cs.RUnlock()
	return count
}

type channel struct {
	name  string
	store *stores.Channel
	ss    *subStore
}

// StanServer structure represents the STAN server
type StanServer struct {
	// Keep all members for which we use atomic at the beginning of the
	// struct and make sure they are all 64bits (or use padding if necessary).
	// atomic.* functions crash on 32bit machines if operand is not aligned
	// at 64bit. See https://github.com/golang/go/issues/599
	ioChannelStatsMaxBatchSize int64 // stats of the max number of messages than went into a single batch

	mu         sync.RWMutex
	shutdown   bool
	serverID   string
	info       spb.ServerInfo // Contains cluster ID and subjects
	natsServer *server.Server
	opts       *Options
	startTime  time.Time

	// For scalability, a dedicated connection is used to publish
	// messages to subscribers.
	nc  *nats.Conn // used for most protocol messages
	ncs *nats.Conn // used for sending to subscribers and acking publishers

	wg sync.WaitGroup // Wait on go routines during shutdown

	// Used when processing connect requests for client ID already registered
	dupCIDGuard       sync.RWMutex
	dupCIDMap         map[string]struct{}
	dupCIDwg          sync.WaitGroup // To wait for one routine to end when we have reached the max.
	dupCIDswg         bool           // To instruct one go routine to decrement the wait group.
	dupCIDTimeout     time.Duration
	dupMaxCIDRoutines int

	// Clients
	clients *clientStore

	// channels
	channels *channelStore

	// Store
	store stores.Store

	// Monitoring
	monMu   sync.RWMutex
	numSubs int

	// IO Channel
	ioChannel     chan *ioPendingMsg
	ioChannelQuit chan struct{}
	ioChannelWG   sync.WaitGroup

	// Used to fix out-of-order processing of requests due to use of
	// different internal NATS subscriptions.
	ctrlMsgMu     sync.Mutex
	ctrlMsgRefIDs map[string]int // Key: CtrlMsg's Ref ID, Value: ref count

	// To protect some close related requests
	closeMu sync.Mutex

	tmpBuf []byte // Used to marshal protocols (right now, only PubAck)

	subStartCh   chan *subStartInfo
	subStartQuit chan struct{}

	// By default we create a subscription on AckInbox per subscription,
	// but still use a single connection to receive all ACKs. So
	// effectively, this means having a go-routine per subscription for
	// processing of client's ACKs. The more subscriptions there are,
	// the more go-routines the system will need. To curb this growth,
	// there is an option to use a pool of ack subscribers.
	acksSubsPoolSize  int
	acksSubsIndex     int
	acksSubs          []*nats.Subscription
	acksSubsPrefix    string
	acksSubsPrefixLen int

	// For FT mode
	ftnc               *nats.Conn
	ftSubject          string
	ftHBInterval       time.Duration
	ftHBMissedInterval time.Duration
	ftHBCh             chan *nats.Msg
	ftQuit             chan struct{}

	state State
	// This is in cases where a fatal error occurs after the server was
	// started. We call Fatalf, but for users starting the server
	// programmatically, it is a way to report what the error was.
	lastError error

	// Will be created only when running in partitioning mode.
	partitions *partitions

	// Use these flags for Debug/Trace in places where speed matters.
	// Normally, Debugf and Tracef will check an internal variable to
	// figure out if the statement should be logged, however, the
	// cost of calling Debugf/Tracef is still significant since there
	// may be memory allocations to format the string passed to these
	// calls. So in those situations, use these flags to surround the
	// calls to Debugf/Tracef.
	trace bool
	debug bool
	log   *logger.StanLogger
}

// subStore holds all known state for all subscriptions
type subStore struct {
	sync.RWMutex
	psubs    []*subState            // plain subscribers
	qsubs    map[string]*queueState // queue subscribers
	durables map[string]*subState   // durables lookup
	acks     map[string]*subState   // ack inbox lookup
	stan     *StanServer            // back link to Stan server
}

// Holds all queue subsribers for a subject/group and
// tracks lastSent for the group.
type queueState struct {
	sync.RWMutex
	lastSent        uint64
	subs            []*subState
	shadow          *subState // For durable case, when last member leaves and group is not closed.
	stalledSubCount int       // number of stalled members
	newOnHold       bool
}

// When doing message redelivery due to ack expiration, the function
// makeSortedPendingMsgs return an array of pendingMsg objects,
// ordered by their expiration date.
type pendingMsg struct {
	seq    uint64
	expire int64
}

// Holds Subscription state
type subState struct {
	sync.RWMutex
	spb.SubState // Embedded protobuf. Used for storage.
	subject      string
	qstate       *queueState
	ackWait      time.Duration // SubState.AckWaitInSecs expressed as a time.Duration
	ackTimer     *time.Timer
	ackSub       *nats.Subscription
	acksPending  map[uint64]int64 // key is message sequence, value is expiration time.
	store        stores.SubStore  // for easy access to the store interface

	// So far, compacting these booleans into a byte flag would not save space.
	// May change if we need to add more.
	initialized bool // false until the subscription response has been sent to prevent data to be sent too early.
	stalled     bool
	newOnHold   bool // Prevents delivery of new msgs until old are redelivered (on restart)
	hasFailedHB bool // This is set when server sends heartbeat to this subscriber's client.
	removed     bool // This is true when subStore.Remove() has been invoked for this subscription.
}

// Looks up, or create a new channel if it does not exist
func (s *StanServer) lookupOrCreateChannel(name string) (*channel, error) {
	c := s.channels.get(name)
	if c != nil {
		return c, nil
	}
	return s.channels.createChannel(s, name)
}

// createSubStore creates a new instance of `subStore`.
func (s *StanServer) createSubStore() *subStore {
	subs := &subStore{
		psubs:    make([]*subState, 0, 4),
		qsubs:    make(map[string]*queueState),
		durables: make(map[string]*subState),
		acks:     make(map[string]*subState),
		stan:     s,
	}
	return subs
}

// Store adds this subscription to the server's `subStore` and also in storage
func (ss *subStore) Store(sub *subState) error {
	if sub == nil {
		return nil
	}
	// `sub` has just been created and can't be referenced anywhere else in
	// the code, so we don't need locking.

	// Adds to storage.
	err := sub.store.CreateSub(&sub.SubState)
	if err != nil {
		ss.stan.log.Errorf("Unable to store subscription [%v:%v] on [%s]: %v", sub.ClientID, sub.Inbox, sub.subject, err)
		return err
	}

	ss.Lock()
	ss.updateState(sub)
	ss.Unlock()

	return nil
}

// Updates the subStore state with this sub.
// The subStore is locked on entry (or does not need, as during server restart).
// However, `sub` does not need locking since it has just been created.
func (ss *subStore) updateState(sub *subState) {
	// Store by type
	if sub.isQueueSubscriber() {
		// Queue subscriber.
		qs := ss.qsubs[sub.QGroup]
		if qs == nil {
			qs = &queueState{
				subs: make([]*subState, 0, 4),
			}
			ss.qsubs[sub.QGroup] = qs
		}
		qs.Lock()
		// The recovered shadow queue sub will have ClientID=="",
		// keep a reference to it until a member re-joins the group.
		if sub.ClientID == "" {
			// There should be only one shadow queue subscriber, but
			// we found in https://github.com/nats-io/nats-streaming-server/issues/322
			// that some datastore had 2 of those (not sure how this happened except
			// maybe due to upgrades from much older releases that had bugs?).
			// So don't panic and use as the shadow the one with the highest LastSent
			// value.
			if qs.shadow == nil || sub.LastSent > qs.lastSent {
				qs.shadow = sub
			}
		} else {
			// Store by ackInbox for ack direct lookup
			ss.acks[sub.AckInbox] = sub

			qs.subs = append(qs.subs, sub)
		}
		// Needed in the case of server restart, where
		// the queue group's last sent needs to be updated
		// based on the recovered subscriptions.
		if sub.LastSent > qs.lastSent {
			qs.lastSent = sub.LastSent
		}
		// If the added sub has newOnHold it means that we are doing recovery and
		// that this member had unacknowledged messages. Mark the queue group
		// with newOnHold
		if sub.newOnHold {
			qs.newOnHold = true
		}
		// Update stalled (on recovery)
		if sub.stalled {
			qs.stalledSubCount++
		}
		qs.Unlock()
		sub.qstate = qs
	} else {
		// First store by ackInbox for ack direct lookup
		ss.acks[sub.AckInbox] = sub

		// Plain subscriber.
		ss.psubs = append(ss.psubs, sub)

		// Hold onto durables in special lookup.
		if sub.isDurableSubscriber() {
			ss.durables[sub.durableKey()] = sub
		}
	}
}

// Remove a subscriber from the subscription store, leaving durable
// subscriptions unless `unsubscribe` is true.
func (ss *subStore) Remove(c *channel, sub *subState, unsubscribe bool) {
	if sub == nil {
		return
	}

	sub.Lock()
	subject := sub.subject
	clientID := sub.ClientID
	sub.removed = true
	sub.clearAckTimer()
	durableKey := ""
	// Do this before clearing the sub.ClientID since this is part of the key!!!
	if sub.isDurableSubscriber() {
		durableKey = sub.durableKey()
	}
	// Clear the subscriptions clientID
	sub.ClientID = ""
	if sub.ackSub != nil {
		sub.ackSub.Unsubscribe()
		sub.ackSub = nil
	}
	ackInbox := sub.AckInbox
	qs := sub.qstate
	isDurable := sub.IsDurable
	subid := sub.ID
	store := sub.store
	qgroup := sub.QGroup
	sub.Unlock()

	reportError := func(err error) {
		ss.stan.log.Errorf("Error deleting subscription subid=%d, subject=%s, err=%v", subid, subject, err)
	}

	// Delete from storage non durable subscribers on either connection
	// close or call to Unsubscribe(), and durable subscribers only on
	// Unsubscribe(). Leave durable queue subs for now, they need to
	// be treated differently.
	if !isDurable || (unsubscribe && durableKey != "") {
		if err := store.DeleteSub(subid); err != nil {
			reportError(err)
		}
	}

	var (
		log               logger.Logger
		queueGroupIsEmpty bool
	)

	ss.Lock()
	if ss.stan.debug {
		log = ss.stan.log
	}
	// Delete from ackInbox lookup.
	delete(ss.acks, ackInbox)

	// Delete from durable if needed
	if unsubscribe && durableKey != "" {
		delete(ss.durables, durableKey)
	}

	var qsubs map[uint64]*subState

	// Delete ourselves from the list
	if qs != nil {
		storageUpdate := false
		// For queue state, we need to lock specifically,
		// because qs.subs can be modified by findBestQueueSub,
		// for which we don't have substore lock held.
		qs.Lock()
		qs.subs, _ = sub.deleteFromList(qs.subs)
		if len(qs.subs) == 0 {
			queueGroupIsEmpty = true
			// If it was the last being removed, also remove the
			// queue group from the subStore map, but only if
			// non durable or explicit unsubscribe.
			if !isDurable || unsubscribe {
				delete(ss.qsubs, qgroup)
				// Delete from storage too.
				if err := store.DeleteSub(subid); err != nil {
					reportError(err)
				}
			} else {
				// Group is durable and last member just left the group,
				// but didn't call Unsubscribe(). Need to keep a reference
				// to this sub to maintain the state.
				qs.shadow = sub
				// Clear the number of stalled members
				qs.stalledSubCount = 0
				// Will need to update the LastSent and clear the ClientID
				// with a storage update.
				storageUpdate = true
			}
		} else {
			if sub.stalled && qs.stalledSubCount > 0 {
				qs.stalledSubCount--
			}
			now := time.Now().UnixNano()
			// If there are pending messages in this sub, they need to be
			// transferred to remaining queue subscribers.
			numQSubs := len(qs.subs)
			idx := 0
			sub.RLock()
			// Need to update if this member was the one with the last
			// message of the group.
			storageUpdate = sub.LastSent == qs.lastSent
			sortedPendingMsgs := makeSortedPendingMsgs(sub.acksPending)
			for _, pm := range sortedPendingMsgs {
				m, err := c.store.Msgs.Lookup(pm.seq)
				if err != nil {
					ss.stan.log.Errorf("Unable to update subscription for %s:%v (%v)", sub.subject, pm.seq, err)
					continue
				}
				// This is possible if message has expired or removed from channel
				// due to limits. No need to ack it since we are destroying this
				// subscription.
				if m == nil {
					continue
				}
				// Get one of the remaning queue subscribers.
				qsub := qs.subs[idx]
				qsub.Lock()
				// Store in storage
				if err := qsub.store.AddSeqPending(qsub.ID, m.Sequence); err != nil {
					ss.stan.log.Errorf("[Client:%s] Unable to transfer message to subid=%d, subject=%s, seq=%d, err=%v",
						clientID, subid, subject, m.Sequence, err)
					qsub.Unlock()
					continue
				}
				// We don't need to update if the sub's lastSent is transferred
				// to another queue subscriber.
				if storageUpdate && m.Sequence == qs.lastSent {
					storageUpdate = false
				}
				// Update LastSent if applicable
				if m.Sequence > qsub.LastSent {
					qsub.LastSent = m.Sequence
				}
				// As of now, members can have different AckWait values.
				expirationTime := pm.expire
				// If the member the message is transferred to has a higher AckWait,
				// keep original expiration time, otherwise check that it is smaller
				// than the new AckWait.
				if sub.ackWait > qsub.ackWait && expirationTime-now > 0 {
					expirationTime = now + int64(qsub.ackWait)
				}
				// Store in ackPending.
				qsub.acksPending[m.Sequence] = expirationTime
				// Keep track of this qsub
				if qsubs == nil {
					qsubs = make(map[uint64]*subState)
				}
				if _, tracked := qsubs[qsub.ID]; !tracked {
					qsubs[qsub.ID] = qsub
				}
				qsub.Unlock()
				// Move to the next queue subscriber, going back to first if needed.
				idx++
				if idx == numQSubs {
					idx = 0
				}
			}
			sub.RUnlock()
			// Even for durable queue subscribers, if this is not the last
			// member, we need to delete from storage (we did that higher in
			// that function for non durable case). Issue #215.
			if isDurable {
				if err := store.DeleteSub(subid); err != nil {
					reportError(err)
				}
			}
		}
		if storageUpdate {
			// If we have a shadow sub, use that one, othewise any queue subscriber
			// will do, so use the first.
			qsub := qs.shadow
			if qsub == nil {
				qsub = qs.subs[0]
			}
			qsub.Lock()
			qsub.LastSent = qs.lastSent
			qsub.store.UpdateSub(&qsub.SubState)
			qsub.Unlock()
		}
		qs.Unlock()
	} else {
		ss.psubs, _ = sub.deleteFromList(ss.psubs)
		// When closing a durable subscription (calling sub.Close(), not sub.Unsubscribe()),
		// we need to update the record on store to prevent the server from adding
		// this durable to the list of active subscriptions. This is especially important
		// if the client closing this durable is itself not closed when the server is
		// restarted. The server would have no way to detect if the durable subscription
		// is offline or not.
		if isDurable && !unsubscribe {
			sub.Lock()
			// ClientID is required on store because this is used on recovery to
			// "compute" the durable key (clientID+subject+durable name).
			sub.ClientID = clientID
			sub.IsClosed = true
			store.UpdateSub(&sub.SubState)
			// After storage, clear the ClientID.
			sub.ClientID = ""
			sub.Unlock()
		}
	}
	ss.Unlock()

	// Calling this will sort current pending messages and ensure
	// that the ackTimer is properly set. It does not necessarily
	// mean that messages are going to be redelivered on the spot.
	for _, qsub := range qsubs {
		ss.stan.performAckExpirationRedelivery(qsub, false)
	}

	if log != nil {
		traceCtx := subStateTraceCtx{clientID: clientID, isRemove: true, isUnsubscribe: unsubscribe, isGroupEmpty: queueGroupIsEmpty}
		traceSubState(log, sub, &traceCtx)
	}
}

// Lookup by durable name.
func (ss *subStore) LookupByDurable(durableName string) *subState {
	ss.RLock()
	sub := ss.durables[durableName]
	ss.RUnlock()
	return sub
}

// Lookup by ackInbox name.
func (ss *subStore) LookupByAckInbox(ackInbox string) *subState {
	aiLen := len(ackInbox)
	ss.RLock()
	if aiLen != natsInboxLen {
		if aiLen <= ss.stan.acksSubsPrefixLen {
			ss.RUnlock()
			return nil
		}
		// Extract the actual AckInbox
		ackInbox = ackInbox[ss.stan.acksSubsPrefixLen:]
	}
	sub := ss.acks[ackInbox]
	ss.RUnlock()
	return sub
}

// Options for STAN Server
type Options struct {
	ID                 string
	DiscoverPrefix     string
	StoreType          string
	FilestoreDir       string
	FileStoreOpts      stores.FileStoreOptions
	SQLStoreOpts       stores.SQLStoreOptions
	stores.StoreLimits               // Store limits (MaxChannels, etc..)
	EnableLogging      bool          // Enables logging
	CustomLogger       logger.Logger // Server will start with the provided logger
	Trace              bool          // Verbose trace
	Debug              bool          // Debug trace
	HandleSignals      bool          // Should the server setup a signal handler (for Ctrl+C, etc...)
	Secure             bool          // Create a TLS enabled connection w/o server verification
	ClientCert         string        // Client Certificate for TLS
	ClientKey          string        // Client Key for TLS
	ClientCA           string        // Client CAs for TLS
	IOBatchSize        int           // Maximum number of messages collected from clients before starting their processing.
	IOSleepTime        int64         // Duration (in micro-seconds) the server waits for more message to fill up a batch.
	NATSServerURL      string        // URL for external NATS Server to connect to. If empty, NATS Server is embedded.
	ClientHBInterval   time.Duration // Interval at which server sends heartbeat to a client.
	ClientHBTimeout    time.Duration // How long server waits for a heartbeat response.
	ClientHBFailCount  int           // Number of failed heartbeats before server closes client connection.
	AckSubsPoolSize    int           // Number of internal subscriptions handling incoming ACKs (0 means one per client's subscription).
	FTGroupName        string        // Name of the FT Group. A group can be 2 or more servers with a single active server and all sharing the same datastore.
	Partitioning       bool          // Specify if server only accepts messages/subscriptions on channels defined in StoreLimits.
}

// Clone returns a deep copy of the Options object.
func (o *Options) Clone() *Options {
	// A simple copy covers pretty much everything
	clone := *o
	// But we have the problem of the PerChannel map that needs
	// to be copied.
	clone.PerChannel = (&o.StoreLimits).ClonePerChannelMap()
	return &clone
}

// DefaultOptions are default options for the STAN server
var defaultOptions = Options{
	ID:                DefaultClusterID,
	DiscoverPrefix:    DefaultDiscoverPrefix,
	StoreType:         DefaultStoreType,
	FileStoreOpts:     stores.DefaultFileStoreOptions,
	IOBatchSize:       DefaultIOBatchSize,
	IOSleepTime:       DefaultIOSleepTime,
	NATSServerURL:     "",
	ClientHBInterval:  DefaultHeartBeatInterval,
	ClientHBTimeout:   DefaultClientHBTimeout,
	ClientHBFailCount: DefaultMaxFailedHeartBeats,
}

// GetDefaultOptions returns default options for the STAN server
func GetDefaultOptions() (o *Options) {
	opts := defaultOptions
	opts.StoreLimits = stores.DefaultStoreLimits
	return &opts
}

// DefaultNatsServerOptions are default options for the NATS server
var DefaultNatsServerOptions = server.Options{
	Host:   "localhost",
	Port:   4222,
	NoLog:  true,
	NoSigs: true,
}

func (s *StanServer) stanDisconnectedHandler(nc *nats.Conn) {
	if nc.LastError() != nil {
		s.log.Errorf("connection %q has been disconnected: %v",
			nc.Opts.Name, nc.LastError())
	}
}

func (s *StanServer) stanReconnectedHandler(nc *nats.Conn) {
	s.log.Noticef("connection %q reconnected to NATS Server at %q",
		nc.Opts.Name, nc.ConnectedUrl())
}

func (s *StanServer) stanClosedHandler(nc *nats.Conn) {
	s.log.Debugf("connection %q has been closed", nc.Opts.Name)
}

func (s *StanServer) stanErrorHandler(nc *nats.Conn, sub *nats.Subscription, err error) {
	s.log.Errorf("Asynchronous error on connection %s, subject %s: %s",
		nc.Opts.Name, sub.Subject, err)
}

func (s *StanServer) buildServerURLs(sOpts *Options, opts *server.Options) ([]string, error) {
	var hostport string
	natsURL := sOpts.NATSServerURL
	// If the URL to an external NATS is provided...
	if natsURL != "" {
		// If it has user/pwd info or is a list of urls...
		if strings.Contains(natsURL, "@") || strings.Contains(natsURL, ",") {
			// Return the array
			urls := strings.Split(natsURL, ",")
			for i, s := range urls {
				urls[i] = strings.Trim(s, " ")
			}
			return urls, nil
		}
		// Otherwise, prepare the host and port and continue to see
		// if user/pass needs to be added.

		// First trim the protocol.
		parts := strings.Split(natsURL, "://")
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed url: %v", natsURL)
		}
		natsURL = parts[1]
		host, port, err := net.SplitHostPort(natsURL)
		if err != nil {
			return nil, err
		}
		// Use net.Join to support IPV6 addresses.
		hostport = net.JoinHostPort(host, port)
	} else {
		// We embed the server, so it is local. If host is "any",
		// use 127.0.0.1 or ::1 for host address (important for
		// Windows since connect with 0.0.0.0 or :: fails).
		sport := strconv.Itoa(opts.Port)
		if opts.Host == "0.0.0.0" {
			hostport = net.JoinHostPort("127.0.0.1", sport)
		} else if opts.Host == "::" || opts.Host == "[::]" {
			hostport = net.JoinHostPort("::1", sport)
		} else {
			hostport = net.JoinHostPort(opts.Host, sport)
		}
	}
	var userpart string
	if opts.Authorization != "" {
		userpart = opts.Authorization
	} else if opts.Username != "" {
		userpart = fmt.Sprintf("%s:%s", opts.Username, opts.Password)
	}
	if userpart != "" {
		return []string{fmt.Sprintf("nats://%s@%s", userpart, hostport)}, nil
	}
	return []string{fmt.Sprintf("nats://%s", hostport)}, nil
}

// createNatsClientConn creates a connection to the NATS server, using
// TLS if configured.  Pass in the NATS server options to derive a
// connection url, and for other future items (e.g. auth)
func (s *StanServer) createNatsClientConn(name string, sOpts *Options, nOpts *server.Options) (*nats.Conn, error) {
	var err error
	ncOpts := nats.DefaultOptions

	ncOpts.Servers, err = s.buildServerURLs(sOpts, nOpts)
	if err != nil {
		return nil, err
	}
	ncOpts.Name = fmt.Sprintf("_NSS-%s-%s", sOpts.ID, name)

	if err = nats.ErrorHandler(s.stanErrorHandler)(&ncOpts); err != nil {
		return nil, err
	}
	if err = nats.ReconnectHandler(s.stanReconnectedHandler)(&ncOpts); err != nil {
		return nil, err
	}
	if err = nats.ClosedHandler(s.stanClosedHandler)(&ncOpts); err != nil {
		return nil, err
	}
	if err = nats.DisconnectHandler(s.stanDisconnectedHandler)(&ncOpts); err != nil {
		return nil, err
	}
	if sOpts.Secure {
		if err = nats.Secure()(&ncOpts); err != nil {
			return nil, err
		}
	}
	if sOpts.ClientCA != "" {
		if err = nats.RootCAs(sOpts.ClientCA)(&ncOpts); err != nil {
			return nil, err
		}
	}
	if sOpts.ClientCert != "" {
		if err = nats.ClientCert(sOpts.ClientCert, sOpts.ClientKey)(&ncOpts); err != nil {
			return nil, err
		}
	}
	// Shorten the time we wait to try to reconnect.
	// Don't make it too often because it may exhaust the number of FDs.
	ncOpts.ReconnectWait = 250 * time.Millisecond
	// Make it try to reconnect for ever.
	ncOpts.MaxReconnect = -1
	// For FT make the reconnect buffer as small as possible since
	// we don't really want FT HBs to be buffered while we are disconnected
	// and be sent as a burst on reconnect.
	if name == "ft" {
		ncOpts.ReconnectBufSize = 128
	}

	s.log.Tracef(" NATS conn opts: %v", ncOpts)

	var nc *nats.Conn
	if nc, err = ncOpts.Connect(); err != nil {
		return nil, err
	}
	return nc, err
}

func (s *StanServer) createNatsConnections(sOpts *Options, nOpts *server.Options) error {
	var err error
	s.ncs, err = s.createNatsClientConn("send", sOpts, nOpts)
	if err == nil {
		s.nc, err = s.createNatsClientConn("general", sOpts, nOpts)
	}
	if err == nil && sOpts.FTGroupName != "" {
		s.ftnc, err = s.createNatsClientConn("ft", sOpts, nOpts)
	}
	return err
}

// RunServer will startup an embedded STAN server and a nats-server to support it.
func RunServer(ID string) (*StanServer, error) {
	sOpts := GetDefaultOptions()
	sOpts.ID = ID
	nOpts := DefaultNatsServerOptions
	return RunServerWithOpts(sOpts, &nOpts)
}

// RunServerWithOpts will startup an embedded STAN server and a nats-server to support it.
func RunServerWithOpts(stanOpts *Options, natsOpts *server.Options) (newServer *StanServer, returnedError error) {
	var sOpts *Options
	var nOpts *server.Options
	// Make a copy of the options so we own them.
	if stanOpts == nil {
		sOpts = GetDefaultOptions()
	} else {
		sOpts = stanOpts.Clone()
	}
	if natsOpts == nil {
		no := DefaultNatsServerOptions
		nOpts = &no
	} else {
		nOpts = natsOpts.Clone()
	}

	s := StanServer{
		serverID:          nuid.Next(),
		opts:              sOpts,
		dupCIDMap:         make(map[string]struct{}),
		dupMaxCIDRoutines: defaultMaxDupCIDRoutines,
		dupCIDTimeout:     defaultCheckDupCIDTimeout,
		ioChannelQuit:     make(chan struct{}, 1),
		ctrlMsgRefIDs:     make(map[string]int),
		trace:             sOpts.Trace,
		debug:             sOpts.Debug,
		subStartCh:        make(chan *subStartInfo, defaultSubStartChanLen),
		subStartQuit:      make(chan struct{}, 1),
		acksSubsPoolSize:  sOpts.AckSubsPoolSize,
		startTime:         time.Now(),
		log:               logger.NewStanLogger(),
	}

	// If a custom logger is provided, use this one, otherwise, check
	// if we should configure the logger or not.
	if sOpts.CustomLogger != nil {
		s.log.SetLogger(sOpts.CustomLogger, sOpts.Debug, sOpts.Trace)
	} else if sOpts.EnableLogging {
		s.configureLogger(nOpts)
	}

	s.log.Noticef("Starting nats-streaming-server[%s] version %s", sOpts.ID, VERSION)

	testInbox := nats.NewInbox()
	// We rely heavily on the format of a NATS inbox.
	// Since we vendor nats and nuid, it should not change without our knowledge.
	if testInbox[0] != natsInboxFirstChar || len(testInbox) != natsInboxLen {
		err := fmt.Errorf("invalid inbox format: %v", testInbox)
		s.log.Errorf("%v", err)
		return nil, err
	}

	// ServerID is used to check that a brodcast protocol is not ours,
	// for instance with FT. Some err/warn messages may be printed
	// regarding other instance's ID, so print it on startup.
	s.log.Noticef("ServerID: %v", s.serverID)
	s.log.Noticef("Go version: %v", runtime.Version())

	// Ensure that we shutdown the server if there is a panic/error during startup.
	// This will ensure that stores are closed (which otherwise would cause
	// issues during testing) and that the NATS Server (if started) is also
	// properly shutdown. To do so, we recover from the panic in order to
	// call Shutdown, then issue the original panic.
	defer func() {
		// We used to issue panic for common errors but now return error
		// instead. Still we want to log the reason for the panic.
		if r := recover(); r != nil {
			s.Shutdown()
			s.log.Noticef("Failed to start: %v", r)
			panic(r)
		} else if returnedError != nil {
			s.Shutdown()
			// Log it as a fatal error, process will exit (if
			// running from executable or logger is configured).
			s.log.Fatalf("Failed to start: %v", returnedError)
		}
	}()

	storeLimits := &s.opts.StoreLimits

	var (
		err   error
		store stores.Store
	)

	// Ensure store type option is in upper-case
	sOpts.StoreType = strings.ToUpper(sOpts.StoreType)

	// Create the store. So far either memory or file-based.
	switch sOpts.StoreType {
	case stores.TypeFile:
		store, err = stores.NewFileStore(s.log, sOpts.FilestoreDir, storeLimits,
			stores.AllOptions(&sOpts.FileStoreOpts))
	case stores.TypeSQL:
		store, err = stores.NewSQLStore(s.log, sOpts.SQLStoreOpts.Driver, sOpts.SQLStoreOpts.Source,
			storeLimits, stores.SQLAllOptions(&sOpts.SQLStoreOpts))
	case stores.TypeMemory:
		store, err = stores.NewMemoryStore(s.log, storeLimits)
	default:
		err = fmt.Errorf("unsupported store type: %v", sOpts.StoreType)
	}
	if err != nil {
		return nil, err
	}
	// StanServer.store (s.store here) is of type stores.Store, which is an
	// interface. If we assign s.store in the call of the constructor and there
	// is an error, although the call returns "nil" for the store, we can no
	// longer have a test such as "if s.store != nil" (as we do in shutdown).
	// This is because the constructors return a store implementention.
	// We would need to use reflection such as reflect.ValueOf(s.store).IsNil().
	// So to not do that, we simply delay the setting of s.store when we know
	// that it was successful.
	s.store = store

	s.clients = newClientStore(s.store)
	s.channels = newChannelStore(s.store)

	// If no NATS server url is provided, it means that we embed the NATS Server
	if sOpts.NATSServerURL == "" {
		if err := s.startNATSServer(nOpts); err != nil {
			return nil, err
		}
	}
	// Check for monitoring
	if nOpts.HTTPPort != 0 || nOpts.HTTPSPort != 0 {
		if err := s.startMonitoring(nOpts); err != nil {
			return nil, err
		}
	}
	// Create our connections
	if err := s.createNatsConnections(sOpts, nOpts); err != nil {
		return nil, err
	}

	// If using partitioning, try our best to find out that on startup that
	// no other server with same cluster ID has any channel that we own.
	if sOpts.Partitioning {
		if err := s.initPartitions(sOpts, nOpts, storeLimits.PerChannel); err != nil {
			return nil, err
		}
	}

	// In FT mode, server cannot recover the store until it is elected leader.
	if s.opts.FTGroupName != "" {
		if err := s.ftSetup(); err != nil {
			return nil, err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.ftStart(); err != nil {
				s.setLastError(err)
			}
		}()
	} else {
		if err := s.start(Standalone); err != nil {
			return nil, err
		}
	}
	if s.opts.HandleSignals {
		s.handleSignals()
	}
	return &s, nil
}

// Logging in STAN
//
// The STAN logger is an instance of a NATS logger, (basically duplicated
// from the NATS server code), and is passed into the NATS server.
//
// A note on Debugf and Tracef:  These will be enabled within the log if
// either STAN or the NATS server enables them.  However, STAN will only
// trace/debug if the local STAN debug/trace flags are set.  NATS will do
// the same with it's logger flags.  This enables us to use the same logger,
// but differentiate between STAN and NATS debug/trace.
func (s *StanServer) configureLogger(nOpts *server.Options) {
	var newLogger logger.Logger

	sOpts := s.opts

	enableDebug := nOpts.Debug || sOpts.Debug
	enableTrace := nOpts.Trace || sOpts.Trace

	if nOpts.LogFile != "" {
		newLogger = natsdLogger.NewFileLogger(nOpts.LogFile, nOpts.Logtime, enableDebug, enableTrace, true)
	} else if nOpts.RemoteSyslog != "" {
		newLogger = natsdLogger.NewRemoteSysLogger(nOpts.RemoteSyslog, enableDebug, enableTrace)
	} else if nOpts.Syslog {
		newLogger = natsdLogger.NewSysLogger(enableDebug, enableTrace)
	} else {
		colors := true
		// Check to see if stderr is being redirected and if so turn off color
		// Also turn off colors if we're running on Windows where os.Stderr.Stat() returns an invalid handle-error
		stat, err := os.Stderr.Stat()
		if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
			colors = false
		}
		newLogger = natsdLogger.NewStdLogger(nOpts.Logtime, enableDebug, enableTrace, colors, true)
	}

	s.log.SetLogger(newLogger, sOpts.Debug, sOpts.Trace)
}

// This is either running inside RunServerWithOpts() and before any reference
// to the server is returned, so locking is not really an issue, or it is
// running from a go-routine when the server has been elected the FT active.
// Therefore, this function grabs the server lock for the duration of this
// call and so care must be taken to not invoke - directly or indirectly -
// code that would attempt to grab the server lock.
func (s *StanServer) start(runningState State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shutdown {
		return nil
	}

	var (
		err            error
		recoveredState *stores.RecoveredState
		recoveredSubs  []*subState
		callStoreInit  bool
	)

	// Recover the state.
	s.log.Noticef("Recovering the state...")
	recoveredState, err = s.store.Recover()
	if err != nil {
		return err
	}
	if recoveredState != nil {
		s.log.Noticef("Recovered %v channel(s)", len(recoveredState.Channels))
	} else {
		s.log.Noticef("No recovered state")
	}
	subjID := s.opts.ID
	// In FT or with static channels (aka partitioning), we use the cluster ID
	// as part of the subjects prefix, not a NUID.
	if runningState == Standalone && s.partitions == nil {
		subjID = nuid.Next()
	}
	if recoveredState != nil {
		// Copy content
		s.info = *recoveredState.Info
		// Check cluster IDs match
		if s.opts.ID != s.info.ClusterID {
			return fmt.Errorf("cluster ID %q does not match recovered value of %q",
				s.opts.ID, s.info.ClusterID)
		}
		// Check to see if SubClose subject is present or not.
		// If not, it means we recovered from an older server, so
		// need to update.
		if s.info.SubClose == "" {
			s.info.SubClose = fmt.Sprintf("%s.%s", DefaultSubClosePrefix, subjID)
			// Update the store with the server info
			callStoreInit = true
		}
		// Same for AcksSubs (use of pool of subscriptions for subscriptions' acks)
		if s.info.AcksSubs == "" {
			s.info.AcksSubs = fmt.Sprintf("%s.%s", acksSubsPoolPrefix, subjID)
			callStoreInit = true
		}

		// Restore clients state
		s.processRecoveredClients(recoveredState.Clients)

		// Process recovered channels (if any).
		recoveredSubs = s.processRecoveredChannels(recoveredState.Channels)
	} else {
		s.info.ClusterID = s.opts.ID
		// Generate Subjects
		s.info.Discovery = fmt.Sprintf("%s.%s", s.opts.DiscoverPrefix, s.info.ClusterID)
		s.info.Publish = fmt.Sprintf("%s.%s", DefaultPubPrefix, subjID)
		s.info.Subscribe = fmt.Sprintf("%s.%s", DefaultSubPrefix, subjID)
		s.info.SubClose = fmt.Sprintf("%s.%s", DefaultSubClosePrefix, subjID)
		s.info.Unsubscribe = fmt.Sprintf("%s.%s", DefaultUnSubPrefix, subjID)
		s.info.Close = fmt.Sprintf("%s.%s", DefaultClosePrefix, subjID)
		s.info.AcksSubs = fmt.Sprintf("%s.%s", acksSubsPoolPrefix, subjID)

		callStoreInit = true
	}
	if callStoreInit {
		// Initialize the store with the server info
		if err := s.store.Init(&s.info); err != nil {
			return fmt.Errorf("unable to initialize the store: %v", err)
		}
	}

	// We don't do the check if we are running FT and/or if
	// static channels (partitioning) is in play.
	if runningState == Standalone && s.partitions == nil {
		if err := s.ensureRunningStandAlone(); err != nil {
			return err
		}
	}

	// Start the go-routine responsible to start sending messages to newly
	// started subscriptions. We do that before opening the gates in
	// s.initSupscriptions() (which is where the internal subscriptions
	// are created).
	s.wg.Add(1)
	go s.processSubscriptionsStart()

	if err := s.initSubscriptions(); err != nil {
		return err
	}

	if recoveredState != nil {
		// Do some post recovery processing (create subs on AckInbox, setup
		// some timers, etc...)
		if err := s.postRecoveryProcessing(recoveredState.Clients, recoveredSubs); err != nil {
			return fmt.Errorf("error during post recovery processing: %v", err)
		}
	}

	// Flush to make sure all subscriptions are processed before
	// we return control to the user.
	if err := s.nc.Flush(); err != nil {
		return fmt.Errorf("could not flush the subscriptions, %v", err)
	}

	s.log.Noticef("Message store is %s", s.store.Name())
	storeLimitsLines := (&s.opts.StoreLimits).Print()
	for _, l := range storeLimitsLines {
		s.log.Noticef(l)
	}

	// Execute (in a go routine) redelivery of unacknowledged messages,
	// and release newOnHold
	s.wg.Add(1)
	go s.performRedeliveryOnStartup(recoveredSubs)
	s.state = runningState
	return nil
}

// TODO:  Explore parameter passing in gnatsd.  Keep separate for now.
func (s *StanServer) configureClusterOpts(opts *server.Options) error {
	// If we don't have cluster defined in the configuration
	// file and no cluster listen string override, but we do
	// have a routes override, we need to report misconfiguration.
	if opts.Cluster.ListenStr == "" && opts.Cluster.Host == "" &&
		opts.Cluster.Port == 0 {
		if opts.RoutesStr != "" {
			err := fmt.Errorf("solicited routes require cluster capabilities, e.g. --cluster")
			s.log.Fatalf(err.Error())
			// Also return error in case server is started from application
			// and no logger has been set.
			return err
		}
		return nil
	}

	// If cluster flag override, process it
	if opts.Cluster.ListenStr != "" {
		clusterURL, err := url.Parse(opts.Cluster.ListenStr)
		if err != nil {
			return err
		}
		h, p, err := net.SplitHostPort(clusterURL.Host)
		if err != nil {
			return err
		}
		opts.Cluster.Host = h
		_, err = fmt.Sscan(p, &opts.Cluster.Port)
		if err != nil {
			return err
		}

		if clusterURL.User != nil {
			pass, hasPassword := clusterURL.User.Password()
			if !hasPassword {
				return fmt.Errorf("expected cluster password to be set")
			}
			opts.Cluster.Password = pass

			user := clusterURL.User.Username()
			opts.Cluster.Username = user
		} else {
			// Since we override from flag and there is no user/pwd, make
			// sure we clear what we may have gotten from config file.
			opts.Cluster.Username = ""
			opts.Cluster.Password = ""
		}
	}

	// If we have routes but no config file, fill in here.
	if opts.RoutesStr != "" && opts.Routes == nil {
		opts.Routes = server.RoutesFromStr(opts.RoutesStr)
	}

	return nil
}

// configureNATSServerTLS sets up TLS for the NATS Server.
// Additional TLS parameters (e.g. cipher suites) will need to be placed
// in a configuration file specified through the -config parameter.
func (s *StanServer) configureNATSServerTLS(opts *server.Options) error {
	tlsSet := false
	tc := server.TLSConfigOpts{}
	if opts.TLSCert != "" {
		tc.CertFile = opts.TLSCert
		tlsSet = true
	}
	if opts.TLSKey != "" {
		tc.KeyFile = opts.TLSKey
		tlsSet = true
	}
	if opts.TLSCaCert != "" {
		tc.CaFile = opts.TLSCaCert
		tlsSet = true
	}

	if opts.TLSVerify {
		tc.Verify = true
		tlsSet = true
	}

	var err error
	if tlsSet {
		if opts.TLSConfig, err = server.GenTLSConfig(&tc); err != nil {
			// The connection will fail later if the problem is severe enough.
			return fmt.Errorf("unable to setup NATS Server TLS:  %v", err)
		}
	}
	return nil
}

// startNATSServer massages options as necessary, and starts the embedded
// NATS server.
func (s *StanServer) startNATSServer(opts *server.Options) error {
	if err := s.configureClusterOpts(opts); err != nil {
		return err
	}
	if err := s.configureNATSServerTLS(opts); err != nil {
		return err
	}
	s.natsServer = server.New(opts)
	if s.natsServer == nil {
		return fmt.Errorf("no NATS Server object returned")
	}
	if stanLogger := s.log.GetLogger(); stanLogger != nil {
		s.natsServer.SetLogger(stanLogger, opts.Debug, opts.Trace)
	}
	// Run server in Go routine.
	go s.natsServer.Start()
	// Wait for accept loop(s) to be started
	if !s.natsServer.ReadyForConnections(10 * time.Second) {
		return fmt.Errorf("unable to start a NATS Server on %s:%d", opts.Host, opts.Port)
	}
	return nil
}

// ensureRunningStandAlone prevents this streaming server from starting
// if another is found using the same cluster ID - a possibility when
// routing is enabled.
// This runs under sever's lock so nothing should grab the server lock here.
func (s *StanServer) ensureRunningStandAlone() error {
	clusterID := s.info.ClusterID
	hbInbox := nats.NewInbox()
	timeout := time.Millisecond * 250

	// We cannot use the client's API here as it will create a dependency
	// cycle in the streaming client, so build our request and see if we
	// get a response.
	req := &pb.ConnectRequest{ClientID: clusterID, HeartbeatInbox: hbInbox}
	b, _ := req.Marshal()
	reply, err := s.nc.Request(s.info.Discovery, b, timeout)
	if err == nats.ErrTimeout {
		s.log.Debugf("Did not detect another server instance")
		return nil
	}
	if err != nil {
		return fmt.Errorf("request error detecting another server instance: %v", err)
	}
	// See if the response is valid and can be unmarshalled.
	cr := &pb.ConnectResponse{}
	err = cr.Unmarshal(reply.Data)
	if err != nil {
		// Something other than a compatible streaming server responded.
		// This may cause other problems in the long run, so better fail
		// the startup early.
		return fmt.Errorf("unmarshall error while detecting another server instance: %v", err)
	}
	// Another streaming server was found, cleanup then return error.
	clreq := &pb.CloseRequest{ClientID: clusterID}
	b, _ = clreq.Marshal()
	s.nc.Request(cr.CloseRequests, b, timeout)
	return fmt.Errorf("discovered another streaming server with cluster ID %q", clusterID)
}

// Binds server's view of a client with stored Client objects.
func (s *StanServer) processRecoveredClients(clients []*stores.Client) {
	s.clients.recoverClients(clients)
}

// Reconstruct the subscription state on restart.
// We don't use locking in there because there is no communication
// with the NATS server and/or clients, so no chance that the state
// changes while we are doing this.
func (s *StanServer) processRecoveredChannels(channels map[string]*stores.RecoveredChannel) []*subState {
	// We will return the recovered subscriptions
	allSubs := make([]*subState, 0, 16)

	// Offline durable subscribers need to be added to the durables
	// map, but nowhere else.
	processOfflineSub := func(channel *channel, sub *subState) {
		channel.ss.durables[sub.durableKey()] = sub
		// Now that the key is computed, clear ClientID otherwise
		// durable would not be able to be restarted.
		sub.ClientID = ""
	}

	for channelName, recoveredChannel := range channels {
		channel := s.channels.create(s, channelName, recoveredChannel.Channel)
		// Get the recovered subscriptions for this channel.
		for _, recSub := range recoveredChannel.Subscriptions {
			// Create a subState
			sub := &subState{
				subject: channel.name,
				ackWait: computeAckWait(recSub.Sub.AckWaitInSecs),
				store:   channel.store.Subs,
			}
			sub.acksPending = make(map[uint64]int64, len(recSub.Pending))
			for seq := range recSub.Pending {
				sub.acksPending[seq] = 0
			}
			if len(sub.acksPending) > 0 {
				// Prevent delivery of new messages until resent of old ones
				sub.newOnHold = true
				// We may not need to set this because this would be set
				// during the initial redelivery attempt, but does not hurt.
				if int32(len(sub.acksPending)) >= sub.MaxInFlight {
					sub.stalled = true
				}
			}
			// Copy over fields from SubState protobuf
			sub.SubState = *recSub.Sub
			// When recovering older stores, IsDurable may not exist for
			// durable subscribers. Set it now.
			durableSub := sub.isDurableSubscriber() // not a durable queue sub!
			if durableSub {
				sub.IsDurable = true
				// Special handling if this is an offline durable subscriber.
				// Note that durable subscribers have always ClientID on store.
				// This is because we use ClientID+subject+durableName to construct
				// the durable key used in the subStore's durables map.
				// Note that even if the client connection is recovered, we should
				// not attempt to add the offline durable back to the clients and
				// regular state. We need to wait for the durable to be restarted.
				if sub.IsClosed {
					processOfflineSub(channel, sub)
					continue
				}
			}
			// Add the subscription to the corresponding client
			added := s.clients.addSub(sub.ClientID, sub)
			if added || sub.IsDurable {
				// Repair for issue https://github.com/nats-io/nats-streaming-server/issues/215
				// Do not recover a queue durable subscriber that still
				// has ClientID but for which connection was closed (=>!added)
				if !added && sub.isQueueDurableSubscriber() && !sub.isShadowQueueDurable() {
					s.log.Noticef("WARN: Not recovering ghost durable queue subscriber: [%s]:[%s] subject=%s inbox=%s", sub.ClientID, sub.QGroup, sub.subject, sub.Inbox)
					continue
				}
				// Fix for older offline durable subscribers. Newer offline durable
				// subscribers have IsClosed set to true and therefore are handled aboved.
				if durableSub && !added {
					processOfflineSub(channel, sub)
				} else {
					// Add this subscription to subStore.
					channel.ss.updateState(sub)
					// Add to the array, unless this is the shadow durable queue sub that
					// was left in the store in order to maintain the group's state.
					if !sub.isShadowQueueDurable() {
						allSubs = append(allSubs, sub)
						s.monMu.Lock()
						s.numSubs++
						s.monMu.Unlock()
					}
				}
			}
		}
	}
	return allSubs
}

// Do some final setup. Be minded of locking here since the server
// has started communication with NATS server/clients.
func (s *StanServer) postRecoveryProcessing(recoveredClients []*stores.Client, recoveredSubs []*subState) error {
	var err error
	for _, sub := range recoveredSubs {
		sub.Lock()
		// To be on the safe side, just check that the ackSub has not
		// been created (may happen with durables that may reconnect maybe?)
		if sub.ackSub == nil {
			if len(sub.AckInbox) <= natsInboxPrefixLen {
				err = fmt.Errorf("invalid ack inbox: %s", sub.AckInbox)
			} else {
				// If the recovered AckInbox starts with nats.InboxPrefix, we
				// need to create an individual subscription for that,
				// regardless if the server is using ackSub pool or not.
				ackSubject := sub.AckInbox
				createSub := ackSubject[0] == natsInboxFirstChar
				if !createSub {
					// The saved AckInbox indicates that this was for a pool
					// of acksSubs. If the server is currently not running
					// in that mode, we need to create the subscription.
					createSub = s.acksSubsPoolSize == 0
					// If not, check that the recovered AckInbox index is
					// below the pool size, otherwise we need to create an
					// individual subscription.
					if !createSub {
						ackSubIndex := 0
						ackSubIndexEnd := strings.Index(sub.AckInbox, ".")
						if ackSubIndexEnd != -1 {
							ackSubIndexStr := sub.AckInbox[:ackSubIndexEnd]
							ackSubIndex, err = strconv.Atoi(ackSubIndexStr)
							if err == nil {
								createSub = ackSubIndex >= s.acksSubsPoolSize
							}
						}
						if ackSubIndexEnd == -1 || err != nil {
							err = fmt.Errorf("invalid ack inbox: %s", sub.AckInbox)
						}
					}
					// If we need to create an individual subscription,
					// set the appropriate ack subject.
					if err == nil && createSub {
						ackSubject = s.acksSubsPrefix + sub.AckInbox
					}
				}
				if err == nil && createSub {
					// Subscribe to acks
					// Actual subject may be AckInbox prefixed with s.acksSubsPrefix.
					sub.ackSub, err = s.nc.Subscribe(ackSubject, s.processAckMsg)
					if err == nil {
						sub.ackSub.SetPendingLimits(-1, -1)
					}
				}
			}
			if err != nil {
				sub.Unlock()
				return err
			}
		}
		// Consider this subscription initialized. Note that it may
		// still have newOnHold == true, which would prevent incoming
		// messages to be delivered before we attempt to redeliver
		// unacknowledged messages in performRedeliveryOnStartup.
		sub.initialized = true
		sub.Unlock()
	}
	// Go through the list of clients and ensure their Hb timer is set.
	for _, sc := range recoveredClients {
		// Because of the loop, we need to make copy for the closure
		cID := sc.ID
		s.clients.setClientHB(cID, s.opts.ClientHBTimeout, func() {
			s.checkClientHealth(cID)
		})
	}
	return nil
}

// Redelivers unacknowledged messages and release the hold for new messages delivery
func (s *StanServer) performRedeliveryOnStartup(recoveredSubs []*subState) {
	defer s.wg.Done()

	queues := make(map[*queueState]*channel)

	for _, sub := range recoveredSubs {
		// Ignore subs that did not have any ack pendings on startup.
		sub.Lock()
		if !sub.newOnHold {
			sub.Unlock()
			continue
		}
		// If this is a durable and it is offline, then skip the rest.
		if sub.isOfflineDurableSubscriber() {
			sub.newOnHold = false
			sub.Unlock()
			continue
		}
		// Unlock in order to call function below
		sub.Unlock()
		// Send old messages (lock is acquired in that function)
		s.performAckExpirationRedelivery(sub, true)
		// Regrab lock
		sub.Lock()
		// Allow new messages to be delivered
		sub.newOnHold = false
		subject := sub.subject
		qs := sub.qstate
		sub.Unlock()
		c := s.channels.get(subject)
		if c == nil {
			continue
		}
		// Kick delivery of (possible) new messages
		if qs != nil {
			queues[qs] = c
		} else {
			s.sendAvailableMessages(c, sub)
		}
	}
	// Kick delivery for queues that had members with newOnHold
	for qs, c := range queues {
		qs.Lock()
		qs.newOnHold = false
		qs.Unlock()
		s.sendAvailableMessagesToQueue(c, qs)
	}
}

// initSubscriptions will setup initial subscriptions for discovery etc.
func (s *StanServer) initSubscriptions() error {

	s.startIOLoop()

	// Listen for connection requests.
	_, err := s.nc.Subscribe(s.info.Discovery, s.connectCB)
	if err != nil {
		return fmt.Errorf("could not subscribe to discover subject, %v", err)
	}
	if s.partitions != nil {
		// Receive published messages from clients, but only on the list
		// of static channels.
		if err := s.partitions.initSubscriptions(); err != nil {
			return err
		}
	} else {
		// Receive published messages from clients.
		pubSubject := fmt.Sprintf("%s.>", s.info.Publish)
		pubSub, err := s.nc.Subscribe(pubSubject, s.processClientPublish)
		if err != nil {
			return fmt.Errorf("could not subscribe to publish subject, %v", err)
		}
		pubSub.SetPendingLimits(-1, -1)
	}
	// Receive subscription requests from clients.
	_, err = s.nc.Subscribe(s.info.Subscribe, s.processSubscriptionRequest)
	if err != nil {
		return fmt.Errorf("could not subscribe to subscribe request subject, %v", err)
	}
	// Receive unsubscribe requests from clients.
	_, err = s.nc.Subscribe(s.info.Unsubscribe, s.processUnsubscribeRequest)
	if err != nil {
		return fmt.Errorf("could not subscribe to unsubscribe request subject, %v", err)
	}
	// Receive subscription close requests from clients.
	_, err = s.nc.Subscribe(s.info.SubClose, s.processSubCloseRequest)
	if err != nil {
		return fmt.Errorf("could not subscribe to subscription close request subject, %v", err)
	}
	// Receive close requests from clients.
	_, err = s.nc.Subscribe(s.info.Close, s.processCloseRequest)
	if err != nil {
		return fmt.Errorf("could not subscribe to close request subject, %v", err)
	}
	// We need to set this regardless if server is currently running
	// with the pool or not (since we may need those when recovering subscriptions)
	s.acksSubsPrefix = s.info.AcksSubs + "."
	s.acksSubsPrefixLen = len(s.acksSubsPrefix)
	// Optionally receive ACKs from clients using this pool of ack subscribers.
	if s.acksSubsPoolSize > 0 {
		for i := 0; i < s.acksSubsPoolSize; i++ {
			ackSub, err := s.nc.Subscribe(fmt.Sprintf("%s%d.>", s.acksSubsPrefix, i),
				s.processAckMsg)
			if err != nil {
				return fmt.Errorf("Could not subscribe to subscriptions acks subject: %v", err)
			}
			ackSub.SetPendingLimits(-1, -1)
			s.acksSubs = append(s.acksSubs, ackSub)
		}
	}
	s.log.Debugf("Discover subject:           %s", s.info.Discovery)
	// For partitions, we actually print the list of channels
	// in the startup banner, so we don't need to repeat them here.
	if s.partitions != nil {
		s.log.Debugf("Publish subjects root:      %s", s.info.Publish)
	} else {
		s.log.Debugf("Publish subject:            %s.>", s.info.Publish)
	}
	s.log.Debugf("Subscribe subject:          %s", s.info.Subscribe)
	s.log.Debugf("Subscription Close subject: %s", s.info.SubClose)
	s.log.Debugf("Unsubscribe subject:        %s", s.info.Unsubscribe)
	s.log.Debugf("Close subject:              %s", s.info.Close)
	return nil
}

// Process a client connect request
func (s *StanServer) connectCB(m *nats.Msg) {
	req := &pb.ConnectRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil || req.HeartbeatInbox == "" {
		s.log.Errorf("[Client:?] Invalid conn request: ClientID=%s, Inbox=%s, err=%v",
			req.ClientID, req.HeartbeatInbox, err)
		s.sendConnectErr(m.Reply, ErrInvalidConnReq.Error())
		return
	}
	if !clientIDRegEx.MatchString(req.ClientID) {
		s.log.Errorf("[Client:%s] Invalid ClientID, only alphanumeric and `-` or `_` characters allowed", req.ClientID)
		s.sendConnectErr(m.Reply, ErrInvalidClientID.Error())
		return
	}

	// Try to register
	client, isNew, err := s.clients.register(req.ClientID, req.HeartbeatInbox)
	if err != nil {
		s.log.Errorf("[Client:%s] Error registering client: %v", req.ClientID, err)
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
			s.log.Errorf("[Client:%s] Connect failed; already connected", req.ClientID)
			s.sendConnectErr(m.Reply, ErrInvalidClient.Error())
			return
		}

		// If server has started shutdown, we can't call wg.Add() so we need
		// to check on shutdown status. Note that s.wg is for all server's
		// go routines, not specific to duplicate CID handling. Use server's
		// lock here.
		s.mu.Lock()
		shutdown := s.shutdown
		if !shutdown {
			// Assume we are going to start a go routine.
			s.wg.Add(1)
		}
		s.mu.Unlock()

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
	s.finishConnectRequest(client, req, m.Reply)
}

func (s *StanServer) finishConnectRequest(client *client, req *pb.ConnectRequest, replyInbox string) {
	cr := &pb.ConnectResponse{
		PubPrefix:        s.info.Publish,
		SubRequests:      s.info.Subscribe,
		UnsubRequests:    s.info.Unsubscribe,
		SubCloseRequests: s.info.SubClose,
		CloseRequests:    s.info.Close,
	}
	b, _ := cr.Marshal()
	s.nc.Publish(replyInbox, b)

	clientID := req.ClientID
	hbInbox := req.HeartbeatInbox
	// Heartbeat timer.
	s.clients.setClientHB(clientID, s.opts.ClientHBInterval, func() { s.checkClientHealth(clientID) })

	s.log.Debugf("[Client:%s] Connected (Inbox=%v)", clientID, hbInbox)
}

func (s *StanServer) processConnectRequestWithDupID(c *client, req *pb.ConnectRequest, replyInbox string) {
	sendErr := true

	c.RLock()
	hbInbox := c.info.HbInbox
	clientID := c.info.ID
	c.RUnlock()

	defer func() {
		s.dupCIDGuard.Lock()
		delete(s.dupCIDMap, clientID)
		if s.dupCIDswg {
			s.dupCIDswg = false
			s.dupCIDwg.Done()
		}
		s.dupCIDGuard.Unlock()
		s.wg.Done()
	}()

	// This is the HbInbox from the "old" client. See if it is up and
	// running by sending a ping to that inbox.
	if _, err := s.nc.Request(hbInbox, nil, s.dupCIDTimeout); err != nil {
		// The old client didn't reply, assume it is dead, close it and continue.
		s.closeClient(clientID)

		// Between the close and the new registration below, it is possible
		// that a connection request came in (in connectCB) and since the
		// client is now unregistered, the new connection was accepted there.
		// The registration below will then fail, in which case we will fail
		// this request.

		// Need to re-register now based on the new request info.
		var isNew bool
		c, isNew, err = s.clients.register(req.ClientID, req.HeartbeatInbox)
		if err == nil && isNew {
			// We could register the new client.
			s.log.Debugf("[Client:%s] Replaced old client (Inbox=%v)", req.ClientID, hbInbox)
			sendErr = false
		}
	}
	// The currently registered client is responding, or we failed to register,
	// so fail the request of the incoming client connect request.
	if sendErr {
		s.log.Debugf("[Client:%s] Connect failed; already connected", clientID)
		s.sendConnectErr(replyInbox, ErrInvalidClient.Error())
		return
	}
	// We have replaced the old with the new.
	s.finishConnectRequest(c, req, replyInbox)
}

func (s *StanServer) sendConnectErr(replyInbox, err string) {
	cr := &pb.ConnectResponse{Error: err}
	b, _ := cr.Marshal()
	s.nc.Publish(replyInbox, b)
}

// Send a heartbeat call to the client.
func (s *StanServer) checkClientHealth(clientID string) {
	client := s.clients.lookup(clientID)
	if client == nil {
		return
	}

	client.RLock()
	hbInbox := client.info.HbInbox
	client.RUnlock()

	var subs []*subState
	hasFailedHB := false
	// Sends the HB request. This call blocks for ClientHBTimeout,
	// do not hold the lock for that long!
	_, err := s.nc.Request(hbInbox, nil, s.opts.ClientHBTimeout)
	// Grab the lock now.
	client.Lock()
	// Client could have been unregistered, in which case
	// client.hbt will be nil.
	if client.hbt == nil {
		client.Unlock()
		return
	}
	// If we did not get the reply, increase the number of
	// failed heartbeats.
	if err != nil {
		client.fhb++
		// If we have reached the max number of failures
		if client.fhb > s.opts.ClientHBFailCount {
			s.log.Debugf("[Client:%s] Timed out on heartbeats", clientID)
			// close the client (connection). This locks the
			// client object internally so unlock here.
			client.Unlock()
			s.closeClient(clientID)
			return
		}
	} else {
		// We got the reply, reset the number of failed heartbeats.
		client.fhb = 0
	}
	// Get a copy of subscribers and client.fhb while under lock
	subs = client.getSubsCopy()
	hasFailedHB = client.fhb > 0
	// Reset the timer to fire again.
	client.hbt.Reset(s.opts.ClientHBInterval)
	client.Unlock()
	if len(subs) > 0 {
		// Push the info about presence of failed heartbeats down to
		// subscribers, so they have easier access to that info in
		// the redelivery attempt code.
		for _, sub := range subs {
			sub.Lock()
			sub.hasFailedHB = hasFailedHB
			sub.Unlock()
		}
	}
}

// Close a client
func (s *StanServer) closeClient(clientID string) bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	// Remove from our clientStore.
	client, err := s.clients.unregister(clientID)
	// The above call may return an error (due to storage) but still return
	// the client that is being unregistered. So log error an proceed.
	if err != nil {
		s.log.Errorf("Error deleting client %q: %v", clientID, err)
	}
	// This would mean that the client was already unregistered or was never
	// registered.
	if client == nil {
		return false
	}

	// Remove all non-durable subscribers.
	s.removeAllNonDurableSubscribers(client)

	if s.debug {
		client.RLock()
		hbInbox := client.info.HbInbox
		client.RUnlock()
		s.log.Debugf("[Client:%s] Closed (Inbox=%v)", clientID, hbInbox)
	}
	return true
}

func (s *StanServer) createCtrlMsg(reqType spb.CtrlMsg_Type, useRefID bool, reply string, data []byte) (*spb.CtrlMsg, *nats.Msg) {
	ctrlMsg := &spb.CtrlMsg{MsgType: reqType, ServerID: s.serverID, Data: data}
	if useRefID {
		ctrlMsg.RefID = nuid.Next()
	}
	ctrlBytes, _ := ctrlMsg.Marshal()
	ctrlMsgNatsMsg := &nats.Msg{Reply: reply, Data: ctrlBytes}
	return ctrlMsg, ctrlMsgNatsMsg
}

// processCloseRequest will process connection close requests from clients.
func (s *StanServer) processCloseRequest(m *nats.Msg) {
	req := &pb.CloseRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		s.log.Errorf("Received invalid close request, subject=%s", m.Subject)
		s.sendCloseErr(m.Reply, ErrInvalidCloseReq.Error())
		return
	}

	// Create the control and corresponding NATS message
	ctrlMsg, ctrlNatsMsg := s.createCtrlMsg(spb.CtrlMsg_ConnClose, true, m.Reply, []byte(req.ClientID))

	// Lock for the remainder of the function
	s.ctrlMsgMu.Lock()
	// When using static channels, the server listens to a specific
	// sets of subjects. We have created a special one that we can
	// send those control messages to.
	if s.partitions != nil {
		ctrlNatsMsg.Subject = s.partitions.ctrlMsgSubject
	} else {
		// Server is listening to s.info.Publish + ".>", so use any
		// dummy subject suffix.
		ctrlNatsMsg.Subject = s.info.Publish + ".close"
	}
	refs := 0
	if s.ncs.PublishMsg(ctrlNatsMsg) == nil {
		refs++
	}
	subs := s.clients.getSubs(req.ClientID)
	if len(subs) > 0 {
		// There are subscribers, we will schedule the connection
		// close request to subscriber's ackInbox subscribers.
		for _, sub := range subs {
			sub.Lock()
			if !sub.removed {
				// Don't use sub.AckInbox directly since it may
				// need to be prefixed with s.acksSubsPrefix
				ctrlNatsMsg.Subject = s.getAckSubject(sub)
				if s.ncs.PublishMsg(ctrlNatsMsg) == nil {
					refs++
				}
			}
			sub.Unlock()
		}
	}
	if refs > 0 {
		// Store our reference count and wait for performConnClose to
		// be invoked...
		s.ctrlMsgRefIDs[ctrlMsg.RefID] = refs
	}
	s.ctrlMsgMu.Unlock()
	// If were unable to schedule a single proto, then execute
	// performConnClose from here.
	if refs == 0 {
		s.performConnClose(m, req.ClientID)
	}
}

// performConnClose performs a connection close operation after all
// client's pubMsg or client acks have been processed.
func (s *StanServer) performConnClose(m *nats.Msg, clientID string) {
	// The function or the caller is already locking, so do not use
	// locking in that function.
	if !s.closeClient(clientID) {
		s.log.Errorf("Unknown client %q in close request", clientID)
		s.sendCloseErr(m.Reply, ErrUnknownClient.Error())
		return
	}

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
	iopm := &ioPendingMsg{m: m}
	pm := &iopm.pm
	if pm.Unmarshal(m.Data) != nil {
		if s.processCtrlMsg(m) {
			return
		}
		// else we will report an error below...
	}

	// Make sure we have a clientID, guid, etc.
	if pm.Guid == "" || !s.clients.isValid(pm.ClientID) || !util.IsChannelNameValid(pm.Subject, false) {
		s.log.Errorf("Received invalid client publish message %v", pm)
		s.sendPublishErr(m.Reply, pm.Guid, ErrInvalidPubReq)
		return
	}

	s.ioChannel <- iopm
}

// processCtrlMsg processes the incoming message has a CtrlMsg.
// If this is not a CtrlMsg, returns false to indicate an error.
// If the CtrlMsg's ServerID is not this server, the request is simply
// ignored and this function returns true (so the caller does not fail).
// Based on the CtrlMsg type, invokes appropriate function to
// do final processing of unsub/subclose/conn close/etc requests.
func (s *StanServer) processCtrlMsg(m *nats.Msg) bool {
	cm := &spb.CtrlMsg{}
	if cm.Unmarshal(m.Data) != nil {
		return false
	}
	// If this control message is not intended for us, simply
	// ignore the request and does not return a failure.
	if cm.ServerID != s.serverID {
		return true
	}
	if cm.RefID != "" {
		var process bool
		s.ctrlMsgMu.Lock()
		// If present, the ref count should be > 0 at this stage.
		// If not present, ignore the request, there is a bug somewhere.
		if refs := s.ctrlMsgRefIDs[cm.RefID]; refs > 0 {
			refs--
			if refs == 0 {
				delete(s.ctrlMsgRefIDs, cm.RefID)
				process = true
			} else {
				s.ctrlMsgRefIDs[cm.RefID] = refs
			}
		} else {
			s.log.Errorf("Unexpected ref count for CtrlMsg %q", cm.MsgType.String())
		}
		s.ctrlMsgMu.Unlock()
		if !process {
			return true
		}
	}
	switch cm.MsgType {
	case spb.CtrlMsg_SubUnsubscribe:
		// SubUnsub and SubClose use same function, using cm.MsgType
		// to differentiate between unsubscribe and close.
		fallthrough
	case spb.CtrlMsg_SubClose:
		req := &pb.UnsubscribeRequest{}
		req.Unmarshal(cm.Data)
		s.performSubUnsubOrClose(cm.MsgType, processRequest, m, req)
	case spb.CtrlMsg_ConnClose:
		clientID := string(cm.Data)
		s.performConnClose(m, clientID)
	default:
		return false // Valid ctrl message, but unexpected type, return failure.
	}
	return true
}

func (s *StanServer) sendPublishErr(subj, guid string, err error) {
	badMsgAck := &pb.PubAck{Guid: guid, Error: err.Error()}
	if b, err := badMsgAck.Marshal(); err == nil {
		s.ncs.Publish(subj, b)
	}
}

// FIXME(dlc) - place holder to pick sub that has least outstanding, should just sort,
// or use insertion sort, etc.
func findBestQueueSub(sl []*subState) *subState {
	var (
		leastOutstanding = int(^uint(0) >> 1)
		rsub             *subState
	)
	for _, sub := range sl {

		sub.RLock()
		sOut := len(sub.acksPending)
		sStalled := sub.stalled
		sHasFailedHB := sub.hasFailedHB
		sub.RUnlock()

		// Favor non stalled subscribers and clients that do not have failed heartbeats
		if !sStalled && !sHasFailedHB {
			if sOut < leastOutstanding {
				leastOutstanding = sOut
				rsub = sub
			}
		}
	}

	len := len(sl)
	if rsub == nil && len > 0 {
		rsub = sl[0]
	}
	if len > 1 && rsub == sl[0] {
		copy(sl, sl[1:len])
		sl[len-1] = rsub
	}

	return rsub
}

// Send a message to the queue group
// Assumes qs lock held for write
func (s *StanServer) sendMsgToQueueGroup(qs *queueState, m *pb.MsgProto, force bool) (*subState, bool, bool) {
	sub := findBestQueueSub(qs.subs)
	if sub == nil {
		return nil, false, false
	}
	sub.Lock()
	wasStalled := sub.stalled
	didSend, sendMore := s.sendMsgToSub(sub, m, force)
	// If this is not a redelivery and the sub was not stalled, but now is,
	// bump the number of stalled members.
	if !force && !wasStalled && sub.stalled {
		qs.stalledSubCount++
	}
	if didSend && sub.LastSent > qs.lastSent {
		qs.lastSent = sub.LastSent
	}
	sub.Unlock()
	return sub, didSend, sendMore
}

// processMsg will process a message, and possibly send to clients, etc.
func (s *StanServer) processMsg(c *channel) {
	ss := c.ss

	// Since we iterate through them all.
	ss.RLock()
	// Walk the plain subscribers and deliver to each one
	for _, sub := range ss.psubs {
		s.sendAvailableMessages(c, sub)
	}

	// Check the queue subscribers
	for _, qs := range ss.qsubs {
		s.sendAvailableMessagesToQueue(c, qs)
	}
	ss.RUnlock()
}

// Used for sorting by sequence
type bySeq []uint64

func (a bySeq) Len() int           { return (len(a)) }
func (a bySeq) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySeq) Less(i, j int) bool { return a[i] < a[j] }

// Returns an array of message sequence numbers ordered by sequence.
func makeSortedSequences(sequences map[uint64]int64) []uint64 {
	results := make([]uint64, 0, len(sequences))
	for seq := range sequences {
		results = append(results, seq)
	}
	sort.Sort(bySeq(results))
	return results
}

// Used for sorting by expiration time
type byExpire []*pendingMsg

func (a byExpire) Len() int      { return (len(a)) }
func (a byExpire) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byExpire) Less(i, j int) bool {
	// If expire is 0, it means the server was restarted
	// and we don't have the expiration time, which will
	// be set later. Order by sequence instead.
	if a[i].expire == 0 || a[j].expire == 0 {
		return a[i].seq < a[j].seq
	}
	return a[i].expire < a[j].expire
}

// Returns an array of pendingMsg ordered by expiration date, unless
// the expiration date in the pendingMsgs map is not set (0), which
// happens after a server restart. In this case, the array is ordered
// by message sequence numbers.
func makeSortedPendingMsgs(pendingMsgs map[uint64]int64) []*pendingMsg {
	results := make([]*pendingMsg, 0, len(pendingMsgs))
	for seq, expire := range pendingMsgs {
		results = append(results, &pendingMsg{seq: seq, expire: expire})
	}
	sort.Sort(byExpire(results))
	return results
}

// Redeliver all outstanding messages to a durable subscriber, used on resubscribe.
func (s *StanServer) performDurableRedelivery(c *channel, sub *subState) {
	// Sort our messages outstanding from acksPending, grab some state and unlock.
	sub.RLock()
	sortedSeqs := makeSortedSequences(sub.acksPending)
	clientID := sub.ClientID
	newOnHold := sub.newOnHold
	subID := sub.ID
	sub.RUnlock()

	if s.debug && len(sortedSeqs) > 0 {
		sub.RLock()
		durName := sub.DurableName
		if durName == "" {
			durName = sub.QGroup
		}
		sub.RUnlock()
		s.log.Debugf("[Client:%s] Redelivering to subid=%d, durable=%s", clientID, subID, durName)
	}

	// If we don't find the client, we are done.
	client := s.clients.lookup(clientID)
	if client == nil {
		return
	}
	// Go through all messages
	for _, seq := range sortedSeqs {
		m := s.getMsgForRedelivery(c, sub, seq)
		if m == nil {
			continue
		}

		if s.trace {
			s.log.Tracef("[Client:%s] Redelivering to subid=%d, seq=%d", clientID, subID, m.Sequence)
		}

		// Flag as redelivered.
		m.Redelivered = true

		sub.Lock()
		// Force delivery
		s.sendMsgToSub(sub, m, forceDelivery)
		sub.Unlock()
	}
	// Release newOnHold if needed.
	if newOnHold {
		sub.Lock()
		sub.newOnHold = false
		sub.Unlock()
	}
}

// Redeliver all outstanding messages that have expired.
func (s *StanServer) performAckExpirationRedelivery(sub *subState, isStartup bool) {
	// Sort our messages outstanding from acksPending, grab some state and unlock.
	sub.Lock()
	sortedPendingMsgs := makeSortedPendingMsgs(sub.acksPending)
	if len(sortedPendingMsgs) == 0 {
		sub.clearAckTimer()
		sub.Unlock()
		return
	}
	expTime := int64(sub.ackWait)
	subject := sub.subject
	qs := sub.qstate
	clientID := sub.ClientID
	subID := sub.ID
	if sub.ackTimer == nil {
		s.setupAckTimer(sub, sub.ackWait)
	}
	if qs == nil {
		// If the client has some failed heartbeats, ignore this request.
		if sub.hasFailedHB {
			// Reset the timer
			sub.ackTimer.Reset(sub.ackWait)
			sub.Unlock()
			if s.debug {
				s.log.Debugf("[Client:%s] Skipping redelivery to subid=%d due to missed client heartbeat", clientID, subID)
			}
			return
		}
	}
	sub.Unlock()

	c := s.channels.get(subject)
	// Should not happen at this time since channels are not
	// removed...
	if c == nil {
		s.log.Errorf("[Client:%s] Aborting redelivery to subid=%d for non existing channel %s", clientID, subID, subject)
		sub.Lock()
		sub.clearAckTimer()
		sub.Unlock()
		return
	}

	now := time.Now().UnixNano()
	// limit is now plus a buffer of 15ms to avoid repeated timer callbacks.
	limit := now + int64(15*time.Millisecond)

	var pick *subState
	sent := false
	needToSetExpireTime := false
	tracePrinted := false
	nextExpirationTIme := int64(0)

	// We will move through acksPending(sorted) and see what needs redelivery.
	for _, pm := range sortedPendingMsgs {
		m := s.getMsgForRedelivery(c, sub, pm.seq)
		if m == nil {
			continue
		}
		expireTime := pm.expire
		if expireTime == 0 {
			needToSetExpireTime = true
			expireTime = m.Timestamp + expTime
		}

		// If this message has not yet expired, reset timer for next callback
		if expireTime > limit {
			if nextExpirationTIme == 0 {
				nextExpirationTIme = expireTime
			}
			if !tracePrinted && s.trace {
				tracePrinted = true
				s.log.Tracef("[Client:%s] Redelivery for subid=%d, skipping seq=%d", clientID, subID, m.Sequence)
			}
			if needToSetExpireTime {
				sub.Lock()
				// Is message still pending?
				if _, present := sub.acksPending[pm.seq]; present {
					// Update expireTime
					expireTime = time.Now().UnixNano() + expTime
					sub.acksPending[pm.seq] = expireTime
				}
				sub.Unlock()
				continue
			}
			break
		}

		// Flag as redelivered.
		m.Redelivered = true

		// Handle QueueSubscribers differently, since we will choose best subscriber
		// to redeliver to, not necessarily the same one.
		// However, on startup, resends only to member that had previously this message
		// otherwise this could cause a message to be redelivered to multiple members.
		if qs != nil && !isStartup {
			qs.Lock()
			pick, sent, _ = s.sendMsgToQueueGroup(qs, m, forceDelivery)
			qs.Unlock()
			if pick == nil {
				s.log.Errorf("[Client:%s] Unable to find queue subscriber for subid=%d", clientID, subID)
				break
			}
			// If the message is redelivered to a different queue subscriber,
			// we need to process an implicit ack for the original subscriber.
			// We do this only after confirmation that it was successfully added
			// as pending on the other queue subscriber.
			if pick != sub && sent {
				s.processAck(c, sub, m.Sequence)
			}
		} else {
			sub.Lock()
			s.sendMsgToSub(sub, m, forceDelivery)
			sub.Unlock()
		}
	}

	// Adjust the timer
	sub.adjustAckTimer(nextExpirationTIme)
}

// getMsgForRedelivery looks up the message from storage. If not found -
// because it has been removed due to limit - processes an ACK for this
// sub/sequence number and returns nil, otherwise return a copy of the
// message (since it is going to be modified: m.Redelivered = true)
func (s *StanServer) getMsgForRedelivery(c *channel, sub *subState, seq uint64) *pb.MsgProto {
	m, err := c.store.Msgs.Lookup(seq)
	if m == nil || err != nil {
		if err != nil {
			s.log.Errorf("Error getting message for redelivery subid=%d, seq=%d, err=%v",
				sub.ID, seq, err)
		}
		// Ack it so that it does not reincarnate on restart
		s.processAck(c, sub, seq)
		return nil
	}
	// The store implementation does not return a copy, we need one
	mcopy := *m
	return &mcopy
}

// Sends the message to the subscriber
// Unless `force` is true, in which case message is always sent, if the number
// of acksPending is greater or equal to the sub's MaxInFlight limit, messages
// are not sent and subscriber is marked as stalled.
// Sub lock should be held before calling.
func (s *StanServer) sendMsgToSub(sub *subState, m *pb.MsgProto, force bool) (bool, bool) {
	if sub == nil || m == nil || !sub.initialized || (sub.newOnHold && !m.Redelivered) {
		return false, false
	}

	// Don't send if we have too many outstanding already, unless forced to send.
	ap := int32(len(sub.acksPending))
	if !force && (ap >= sub.MaxInFlight) {
		sub.stalled = true
		return false, false
	}

	if s.trace {
		var action string
		if m.Redelivered {
			action = "Redelivering"
		} else {
			action = "Delivering"
		}
		s.log.Tracef("[Client:%s] %s msg to subid=%d, subject=%s, seq=%d",
			sub.ClientID, action, sub.ID, m.Subject, m.Sequence)
	}

	// Marshal of a pb.MsgProto cannot fail
	b, _ := m.Marshal()
	// but protect against a store implementation that may incorrectly
	// return an empty message.
	if len(b) == 0 {
		panic("store implementation returned an empty message")
	}
	if err := s.ncs.Publish(sub.Inbox, b); err != nil {
		s.log.Errorf("[Client:%s] Failed sending to subid=%d, subject=%s, seq=%d, err=%v",
			sub.ClientID, sub.ID, m.Subject, m.Sequence, err)
		return false, false
	}

	// Setup the ackTimer as needed now. I don't want to use defer in this
	// function, and want to make sure that if we exit before the end, the
	// timer is set. It will be adjusted/stopped as needed.
	if sub.ackTimer == nil {
		s.setupAckTimer(sub, sub.ackWait)
	}

	// If this message is already pending, do not add it again to the store.
	if expTime, present := sub.acksPending[m.Sequence]; present {
		// However, update the next expiration time.
		if expTime == 0 {
			// That can happen after a server restart, so need to use
			// the current time.
			expTime = time.Now().UnixNano()
		}
		// bump the next expiration time with the sub's ackWait.
		expTime += int64(sub.ackWait)
		sub.acksPending[m.Sequence] = expTime
		return true, true
	}
	// Store in storage
	if err := sub.store.AddSeqPending(sub.ID, m.Sequence); err != nil {
		s.log.Errorf("[Client:%s] Unable to add pending message to subid=%d, subject=%s, seq=%d, err=%v",
			sub.ClientID, sub.ID, m.Subject, m.Sequence, err)
		return false, false
	}

	// Update LastSent if applicable
	if m.Sequence > sub.LastSent {
		sub.LastSent = m.Sequence
	}

	// Store in ackPending.
	// Use current time to compute expiration time instead of m.Timestamp.
	// A message can be persisted in the log and send much later to a
	// new subscriber. Basing expiration time on m.Timestamp would
	// likely set the expiration time in the past!
	sub.acksPending[m.Sequence] = time.Now().UnixNano() + int64(sub.ackWait)

	// Now that we have added to acksPending, check again if we
	// have reached the max and tell the caller that it should not
	// be sending more at this time.
	if !force && (ap+1 == sub.MaxInFlight) {
		sub.stalled = true
		return true, false
	}

	return true, true
}

// Sets up the ackTimer to fire at the given duration.
// sub's lock held on entry.
func (s *StanServer) setupAckTimer(sub *subState, d time.Duration) {
	sub.ackTimer = time.AfterFunc(d, func() {
		s.performAckExpirationRedelivery(sub, false)
	})
}

func (s *StanServer) startIOLoop() {
	s.ioChannelWG.Add(1)
	s.ioChannel = make(chan *ioPendingMsg, ioChannelSize)
	// Use wait group to ensure that the loop is as ready as
	// possible before we setup the subscriptions and open the door
	// to incoming NATS messages.
	ready := &sync.WaitGroup{}
	ready.Add(1)
	go s.ioLoop(ready)
	ready.Wait()
}

func (s *StanServer) ioLoop(ready *sync.WaitGroup) {
	defer s.ioChannelWG.Done()

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
	storesToFlush := make(map[*channel]struct{}, 64)

	var _pendingMsgs [ioChannelSize]*ioPendingMsg
	var pendingMsgs = _pendingMsgs[:0]

	storeIOPendingMsg := func(iopm *ioPendingMsg) {
		cs, err := s.assignAndStore(&iopm.pm)
		if err != nil {
			s.log.Errorf("[Client:%s] Error processing message for subject %q: %v", iopm.pm.ClientID, iopm.m.Subject, err)
			s.sendPublishErr(iopm.m.Reply, iopm.pm.Guid, err)
		} else {
			pendingMsgs = append(pendingMsgs, iopm)
			storesToFlush[cs] = struct{}{}
		}
	}

	batchSize := s.opts.IOBatchSize
	sleepTime := s.opts.IOSleepTime
	sleepDur := time.Duration(sleepTime) * time.Microsecond
	max := 0

	ready.Done()
	for {
		select {
		case iopm := <-s.ioChannel:
			// store the one we just pulled
			storeIOPendingMsg(iopm)

			remaining := batchSize - 1
			// fill the pending messages slice with at most our batch size,
			// unless the channel is empty.
			for remaining > 0 {
				ioChanLen := len(s.ioChannel)

				// if we are empty, wait, check again, and break if nothing.
				// While this adds some latency, it optimizes batching.
				if ioChanLen == 0 {
					if sleepTime > 0 {
						time.Sleep(sleepDur)
						ioChanLen = len(s.ioChannel)
						if ioChanLen == 0 {
							break
						}
					} else {
						break
					}
				}

				// stick to our buffer size
				if ioChanLen > remaining {
					ioChanLen = remaining
				}

				for i := 0; i < ioChanLen; i++ {
					storeIOPendingMsg(<-s.ioChannel)
				}
				// Keep track of max number of messages in a batch
				if ioChanLen > max {
					max = ioChanLen
					atomic.StoreInt64(&(s.ioChannelStatsMaxBatchSize), int64(max))
				}
				remaining -= ioChanLen
			}

			// flush all the stores with messages written to them...
			for c := range storesToFlush {
				if err := c.store.Msgs.Flush(); err != nil {
					// TODO: Attempt recovery, notify publishers of error.
					panic(fmt.Errorf("Unable to flush msg store: %v", err))
				}
				// Call this here, so messages are sent to subscribers,
				// which means that msg seq is added to subscription file
				s.processMsg(c)
				if err := c.store.Subs.Flush(); err != nil {
					panic(fmt.Errorf("Unable to flush sub store: %v", err))
				}
				// Remove entry from map (this is safe in Go)
				delete(storesToFlush, c)
			}

			// Ack our messages back to the publisher
			for i := range pendingMsgs {
				iopm := pendingMsgs[i]
				s.ackPublisher(iopm)
				pendingMsgs[i] = nil
			}

			// clear out pending messages
			pendingMsgs = pendingMsgs[:0]

		case <-s.ioChannelQuit:
			return
		}
	}
}

// assignAndStore will assign a sequence ID and then store the message.
func (s *StanServer) assignAndStore(pm *pb.PubMsg) (*channel, error) {
	c, err := s.lookupOrCreateChannel(pm.Subject)
	if err != nil {
		return nil, err
	}
	if _, err := c.store.Msgs.Store(pm.Data); err != nil {
		return nil, err
	}
	return c, nil
}

// ackPublisher sends the ack for a message.
func (s *StanServer) ackPublisher(iopm *ioPendingMsg) {
	msgAck := &iopm.pa
	msgAck.Guid = iopm.pm.Guid
	needed := msgAck.Size()
	s.tmpBuf = util.EnsureBufBigEnough(s.tmpBuf, needed)
	n, _ := msgAck.MarshalTo(s.tmpBuf)
	if s.trace {
		pm := &iopm.pm
		s.log.Tracef("[Client:%s] Acking Publisher subj=%s guid=%s", pm.ClientID, pm.Subject, pm.Guid)
	}
	s.ncs.Publish(iopm.m.Reply, s.tmpBuf[:n])
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
func (s *StanServer) removeAllNonDurableSubscribers(client *client) {
	// client has been unregistered and no other routine can add/remove
	// subscriptions, so it is safe to use the original.
	client.RLock()
	subs := client.subs
	client.RUnlock()
	for _, sub := range subs {
		sub.RLock()
		subject := sub.subject
		sub.RUnlock()
		// Get the channel
		c := s.channels.get(subject)
		if c == nil {
			continue
		}
		// Don't remove durables
		c.ss.Remove(c, sub, false)
	}
}

// processUnsubscribeRequest will process a unsubscribe request.
func (s *StanServer) processUnsubscribeRequest(m *nats.Msg) {
	req := &pb.UnsubscribeRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		s.log.Errorf("Invalid unsub request from %s", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidUnsubReq)
		return
	}
	s.performSubUnsubOrClose(spb.CtrlMsg_SubUnsubscribe, scheduleRequest, m, req)
}

// processSubCloseRequest will process a subscription close request.
func (s *StanServer) processSubCloseRequest(m *nats.Msg) {
	req := &pb.UnsubscribeRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		s.log.Errorf("Invalid sub close request from %s", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidUnsubReq)
		return
	}
	s.performSubUnsubOrClose(spb.CtrlMsg_SubClose, scheduleRequest, m, req)
}

// performSubUnsubOrClose either schedules the request to the
// subscriber's AckInbox subscriber, or processes the request in place.
func (s *StanServer) performSubUnsubOrClose(reqType spb.CtrlMsg_Type, schedule bool, m *nats.Msg, req *pb.UnsubscribeRequest) {
	// With partitioning, first verify that this server is handling this
	// channel. If not, do not return an error, since another server will
	// handle it. If no other server is, the client will get a timeout.
	if s.partitions != nil {
		if r := s.partitions.sl.Match(req.Subject); len(r) == 0 {
			return
		}
	}

	action := "unsub"
	isSubClose := false
	if reqType == spb.CtrlMsg_SubClose {
		action = "sub close"
		isSubClose = true
	}
	c := s.channels.get(req.Subject)
	if c == nil {
		s.log.Errorf("[Client:%s] %s request missing subject %s",
			req.ClientID, action, req.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}

	// Get the subStore
	ss := c.ss

	sub := ss.LookupByAckInbox(req.Inbox)
	if sub == nil {
		s.log.Errorf("[Client:%s] %s request for missing inbox %s",
			req.ClientID, action, req.Inbox)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSub)
		return
	}

	// Lock for the remainder of the function
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if schedule {
		processInPlace := false
		sub.Lock()
		removed := sub.removed
		if !removed {
			// We send a single message to a single handler, no need for ref count
			_, ctrlNatsMsg := s.createCtrlMsg(reqType, false, m.Reply, m.Data)
			// Don't use sub.AckInbox directly since it may
			// need to be prefixed with s.acksSubsPrefix.
			ctrlNatsMsg.Subject = s.getAckSubject(sub)
			// In case of error, process the request in place.
			if s.ncs.PublishMsg(ctrlNatsMsg) != nil {
				processInPlace = true
			}
		}
		sub.Unlock()
		if !processInPlace || removed {
			return
		}
	}

	// Remove from Client
	if !s.clients.removeSub(req.ClientID, sub) {
		s.log.Errorf("[Client:%s] %s request for missing client", req.ClientID, action)
		s.sendSubscriptionResponseErr(m.Reply, ErrUnknownClient)
		return
	}

	// Remove the subscription
	unsubscribe := !isSubClose
	ss.Remove(c, sub, unsubscribe)
	s.monMu.Lock()
	s.numSubs--
	s.monMu.Unlock()

	// Create a non-error response
	resp := &pb.SubscriptionResponse{AckInbox: req.Inbox}
	b, _ := resp.Marshal()
	s.ncs.Publish(m.Reply, b)
}

func (s *StanServer) sendSubscriptionResponseErr(reply string, err error) {
	resp := &pb.SubscriptionResponse{Error: err.Error()}
	b, _ := resp.Marshal()
	s.ncs.Publish(reply, b)
}

// Clear the ackTimer.
// sub Lock held in entry.
func (sub *subState) clearAckTimer() {
	if sub.ackTimer != nil {
		sub.ackTimer.Stop()
		sub.ackTimer = nil
	}
}

// adjustAckTimer adjusts the timer based on a given next
// expiration time.
// The timer will be stopped if there is no more pending ack.
// If there are pending acks, the timer will be reset to the
// default sub.ackWait value if the given expiration time is
// 0 or in the past. Otherwise, it is set to the remaining time
// between the given expiration time and now.
func (sub *subState) adjustAckTimer(nextExpirationTime int64) {
	sub.Lock()
	defer sub.Unlock()

	// Possible that the subscriber has been destroyed, and timer cleared
	if sub.ackTimer == nil {
		return
	}

	// Check if there are still pending acks
	if len(sub.acksPending) > 0 {
		// Capture time
		now := time.Now().UnixNano()

		// If the next expiration time is 0 or less than now,
		// use the default ackWait
		if nextExpirationTime <= now {
			sub.ackTimer.Reset(sub.ackWait)
		} else {
			// Compute the time the ackTimer should fire, based
			// on the given next expiration time and now.
			fireIn := (nextExpirationTime - now)
			sub.ackTimer.Reset(time.Duration(fireIn))
		}
	} else {
		// No more pending acks, clear the timer.
		sub.clearAckTimer()
	}
}

// Used to generate durable key. This should not be called on non-durables.
func (sub *subState) durableKey() string {
	if sub.DurableName == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s", sub.ClientID, sub.subject, sub.DurableName)
}

// Returns true if this sub is a queue subscriber (durable or not)
func (sub *subState) isQueueSubscriber() bool {
	return sub.QGroup != ""
}

// Returns true if this sub is a durable queue subscriber
func (sub *subState) isQueueDurableSubscriber() bool {
	return sub.QGroup != "" && sub.IsDurable
}

// Returns true if this is a "shadow" durable queue subscriber
func (sub *subState) isShadowQueueDurable() bool {
	return sub.IsDurable && sub.QGroup != "" && sub.ClientID == ""
}

// Returns true if this sub is a durable subscriber (not a durable queue sub)
func (sub *subState) isDurableSubscriber() bool {
	return sub.DurableName != ""
}

// Returns true if this is an offline durable subscriber.
func (sub *subState) isOfflineDurableSubscriber() bool {
	return sub.DurableName != "" && sub.ClientID == ""
}

// Used to generate durable key. This should not be called on non-durables.
func durableKey(sr *pb.SubscriptionRequest) string {
	if sr.DurableName == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s", sr.ClientID, sr.Subject, sr.DurableName)
}

// addSubscription adds `sub` to the client and store.
func (s *StanServer) addSubscription(ss *subStore, sub *subState) error {
	// Store in client
	if !s.clients.addSub(sub.ClientID, sub) {
		return fmt.Errorf("can't find clientID: %v", sub.ClientID)
	}
	// Store this subscription in subStore
	if err := ss.Store(sub); err != nil {
		return err
	}
	return nil
}

// updateDurable adds back `sub` to the client and updates the store.
// No lock is needed for `sub` since it has just been created.
func (s *StanServer) updateDurable(ss *subStore, sub *subState) error {
	// Store in the client
	if !s.clients.addSub(sub.ClientID, sub) {
		return fmt.Errorf("can't find clientID: %v", sub.ClientID)
	}
	// Update this subscription in the store
	if err := sub.store.UpdateSub(&sub.SubState); err != nil {
		return err
	}
	ss.Lock()
	// Do this only for durable subscribers (not durable queue subscribers).
	if sub.isDurableSubscriber() {
		// Add back into plain subscribers
		ss.psubs = append(ss.psubs, sub)
	}
	// And in ackInbox lookup map.
	ss.acks[sub.AckInbox] = sub
	ss.Unlock()

	return nil
}

// processSubscriptionRequest will process a subscription request.
func (s *StanServer) processSubscriptionRequest(m *nats.Msg) {
	sr := &pb.SubscriptionRequest{}
	err := sr.Unmarshal(m.Data)
	if err != nil {
		s.log.Errorf("Invalid Subscription request from %s: %v", m.Subject, err)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSubReq)
		return
	}

	// ClientID must not be empty.
	if sr.ClientID == "" {
		s.log.Errorf("Missing ClientID in subscription request from %s", m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrMissingClient)
		return
	}

	// AckWait must be >= 1s (except in test mode where negative value means that
	// duration should be interpreted as Milliseconds)
	if !testAckWaitIsInMillisecond && sr.AckWaitInSecs <= 0 {
		s.log.Errorf("[Client:%s] Invalid AckWait (%v) in subscription request from %s",
			sr.ClientID, sr.AckWaitInSecs, m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidAckWait)
		return
	}

	// MaxInflight must be >= 1
	if sr.MaxInFlight <= 0 {
		s.log.Errorf("[Client:%s] Invalid MaxInflight (%v) in subscription request from %s",
			sr.ClientID, sr.MaxInFlight, m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidMaxInflight)
		return
	}

	// StartPosition between StartPosition_NewOnly and StartPosition_First
	if sr.StartPosition < pb.StartPosition_NewOnly || sr.StartPosition > pb.StartPosition_First {
		s.log.Errorf("[Client:%s] Invalid StartPosition (%v) in subscription request from %s",
			sr.ClientID, int(sr.StartPosition), m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidStart)
		return
	}

	// Make sure subject is valid
	if !util.IsChannelNameValid(sr.Subject, false) {
		s.log.Errorf("[Client:%s] Invalid channel %q in subscription request from %s",
			sr.ClientID, sr.Subject, m.Subject)
		s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSubject)
		return
	}

	// In partitioning mode, do not fail the subscription request
	// if this server does not have the channel. It could be that there
	// is another server out there that will accept the subscription.
	// If not, the client will get a subscription request timeout.
	if s.partitions != nil {
		if r := s.partitions.sl.Match(sr.Subject); len(r) == 0 {
			return
		}
	}

	// Grab channel state, create a new one if needed.
	c, err := s.lookupOrCreateChannel(sr.Subject)
	if err != nil {
		s.log.Errorf("Unable to create store for subject %s", sr.Subject)
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}
	// Get the subStore
	ss := c.ss

	var sub *subState

	ackInbox, ackSubject := s.createAckInboxAndSubject()

	// Will be true for durable queue subscribers and durable subscribers alike.
	isDurable := false
	// Will be set to false for en existing durable subscriber or existing
	// queue group (durable or not).
	setStartPos := true
	// Check for durable queue subscribers
	if sr.QGroup != "" {
		if sr.DurableName != "" {
			// For queue subscribers, we prevent DurableName to contain
			// the ':' character, since we use it for the compound name.
			if strings.Contains(sr.DurableName, ":") {
				s.log.Errorf("[Client:%s] Invalid DurableName (%q) for queue subscriber from %s",
					sr.ClientID, sr.DurableName, sr.Subject)
				s.sendSubscriptionResponseErr(m.Reply, ErrInvalidDurName)
				return
			}
			isDurable = true
			// Make the queue group a compound name between durable name and q group.
			sr.QGroup = fmt.Sprintf("%s:%s", sr.DurableName, sr.QGroup)
			// Clear DurableName from this subscriber.
			sr.DurableName = ""
		}
		// Lookup for an existing group. Only interested in situation where
		// the group exist, but is empty and had a shadow subscriber.
		ss.RLock()
		qs := ss.qsubs[sr.QGroup]
		if qs != nil {
			qs.Lock()
			if qs.shadow != nil {
				sub = qs.shadow
				qs.shadow = nil
				qs.subs = append(qs.subs, sub)
			}
			qs.Unlock()
			setStartPos = false
		}
		ss.RUnlock()
	} else if sr.DurableName != "" {
		// Check for DurableSubscriber status
		if sub = ss.LookupByDurable(durableKey(sr)); sub != nil {
			sub.RLock()
			clientID := sub.ClientID
			sub.RUnlock()
			if clientID != "" {
				s.log.Errorf("[Client:%s] Invalid ClientID in subscription request from %s",
					sr.ClientID, m.Subject)
				s.sendSubscriptionResponseErr(m.Reply, ErrDupDurable)
				return
			}
			setStartPos = false
		}
		isDurable = true
	}
	var (
		subStartTrace string
		subIsNew      bool
	)
	if sub != nil {
		// ok we have a remembered subscription
		sub.Lock()
		// Set ClientID and new AckInbox but leave LastSent to the
		// remembered value.
		sub.AckInbox = ackInbox
		sub.ClientID = sr.ClientID
		sub.Inbox = sr.Inbox
		sub.IsDurable = true
		// Use some of the new options, but ignore the ones regarding start position
		sub.MaxInFlight = sr.MaxInFlight
		sub.AckWaitInSecs = sr.AckWaitInSecs
		sub.ackWait = computeAckWait(sr.AckWaitInSecs)
		sub.stalled = false
		if len(sub.acksPending) > 0 {
			// We have a durable with pending messages, set newOnHold
			// until we have performed the initial redelivery.
			sub.newOnHold = true
			s.setupAckTimer(sub, sub.ackWait)
		}
		// Clear the removed and IsClosed flags that were set during a Close()
		sub.removed = false
		sub.IsClosed = false
		sub.Unlock()

		// Case of restarted durable subscriber, or first durable queue
		// subscriber re-joining a group that was left with pending messages.
		err = s.updateDurable(ss, sub)
	} else {
		subIsNew = true
		// Create sub here (can be plain, durable or queue subscriber)
		sub = &subState{
			SubState: spb.SubState{
				ClientID:      sr.ClientID,
				QGroup:        sr.QGroup,
				Inbox:         sr.Inbox,
				AckInbox:      ackInbox,
				MaxInFlight:   sr.MaxInFlight,
				AckWaitInSecs: sr.AckWaitInSecs,
				DurableName:   sr.DurableName,
				IsDurable:     isDurable,
			},
			subject:     sr.Subject,
			ackWait:     computeAckWait(sr.AckWaitInSecs),
			acksPending: make(map[uint64]int64),
			store:       c.store.Subs,
		}

		if setStartPos {
			// set the start sequence of the subscriber.
			subStartTrace, err = s.setSubStartSequence(c, sub, sr)
		}

		if err == nil {
			// add the subscription to stan
			err = s.addSubscription(ss, sub)
		}
	}
	if err != nil {
		// Try to undo what has been done.
		s.closeMu.Lock()
		ss.Remove(c, sub, false)
		s.closeMu.Unlock()
		s.log.Errorf("Unable to add subscription for %s: %v", sr.Subject, err)
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}
	if s.debug {
		traceCtx := subStateTraceCtx{clientID: sr.ClientID, isNew: subIsNew, startTrace: subStartTrace}
		traceSubState(s.log, sub, &traceCtx)
	}

	s.monMu.Lock()
	s.numSubs++
	s.monMu.Unlock()

	// In case this is a durable, sub already exists so we need to protect access
	sub.Lock()
	// Subscribe to acks.
	if s.acksSubsPoolSize == 0 {
		// We MUST use the same connection than all other internal subscribers
		// if we want to receive messages in order from NATS server.
		sub.ackSub, err = s.nc.Subscribe(ackInbox, s.processAckMsg)
		if err != nil {
			sub.Unlock()
			panic(fmt.Sprintf("Could not subscribe to ack subject, %v\n", err))
		}
		sub.ackSub.SetPendingLimits(-1, -1)
		// However, we need to flush to ensure that NATS server processes
		// this subscription request before we return OK and start sending
		// messages to the client.
		s.nc.Flush()
	}

	// Create a non-error response
	resp := &pb.SubscriptionResponse{AckInbox: ackSubject}
	b, _ := resp.Marshal()
	s.ncs.Publish(m.Reply, b)

	// Capture under lock here
	qs := sub.qstate
	// Now that we have sent the response, we set the subscription to initialized,
	// which allows messages to be sent to it - but not sooner (which could happen
	// without this since the subscription is added to the system earlier and
	// incoming messages to the channel would trigger delivery).
	sub.initialized = true
	sub.Unlock()

	s.subStartCh <- &subStartInfo{c: c, sub: sub, qs: qs, isDurable: isDurable}
}

type subStateTraceCtx struct {
	clientID      string
	isRemove      bool
	isNew         bool
	isUnsubscribe bool
	isGroupEmpty  bool
	startTrace    string
}

func traceSubState(log logger.Logger, sub *subState, ctx *subStateTraceCtx) {
	sub.RLock()
	defer sub.RUnlock()
	var (
		action   string
		specific string
		durable  string
		sending  string
		queue    string
		prefix   string
	)
	if sub.IsDurable {
		durable = "durable "
	}
	if sub.QGroup != "" {
		queue = "queue "
	}
	if ctx.isRemove {
		if (ctx.isUnsubscribe || !sub.IsDurable) && (sub.QGroup == "" || ctx.isGroupEmpty) {
			prefix = "Removed"
		} else if sub.QGroup != "" && !ctx.isGroupEmpty {
			prefix = "Removed member from"
		} else {
			prefix = "Suspended"
		}
	} else {
		if ctx.startTrace != "" {
			prefix = "Started new"
		} else if sub.QGroup != "" && ctx.isNew {
			prefix = "Added member to"
		} else if sub.IsDurable {
			prefix = "Resumed"
		}
	}
	action = fmt.Sprintf("%s %s%s", prefix, durable, queue)
	if sub.QGroup != "" {
		specific = fmt.Sprintf(" queue=%s,", sub.QGroup)
	} else if sub.IsDurable {
		specific = fmt.Sprintf(" durable=%s,", sub.DurableName)
	}
	if !ctx.isRemove && ctx.startTrace != "" {
		sending = ", sending " + ctx.startTrace
	}
	log.Debugf("[Client:%s] %ssubscription, subject=%s, inbox=%s,%s subid=%d%s",
		ctx.clientID, action, sub.subject, sub.Inbox, specific, sub.ID, sending)
}

// createAckInboxAndSubject returns an AckInbox.
// Based on the configuration, this is simply a nats.NewInbox()
// or a unique subject with a prefix that corresponds to one
// of the acksSub pooled subscription.
// NOTE: So far, this function is called from a single go-routine,
// so no locking for s.acksSubIndex is required.
func (s *StanServer) createAckInboxAndSubject() (string, string) {
	ackInbox := nats.NewInbox()
	ackInboxSubject := ackInbox
	if s.acksSubsPoolSize > 0 {
		// Convert _INBOX.abcdefghijk to abcdefghijk
		ackInbox = ackInbox[natsInboxPrefixLen:]
		// Then prefix with the index of one of the ackSub in the array.
		// Use round-robin for now.
		acksSubsIndex := s.acksSubsIndex
		s.acksSubsIndex++
		if s.acksSubsIndex >= s.acksSubsPoolSize {
			s.acksSubsIndex = 0
		}
		// This results in ackInbox being something like: 2.abcdefghijk
		ackInbox = fmt.Sprintf("%d.%s", acksSubsIndex, ackInbox)
		// The client will send the acks to a subject that looks like:
		// _STAN.subacks.2.abcdefghijk
		ackInboxSubject = s.acksSubsPrefix + ackInbox
	}
	return ackInbox, ackInboxSubject
}

// getAckSubject returns the proper ACK subject for the given subscription.
// Assumes sub's lock is held on entry and sub has not been removed.
func (s *StanServer) getAckSubject(sub *subState) string {
	if sub.ackSub != nil {
		return sub.ackSub.Subject
	}
	return s.acksSubsPrefix + sub.AckInbox
}

func (s *StanServer) processSubscriptionsStart() {
	defer s.wg.Done()
	for {
		select {
		case subStart := <-s.subStartCh:
			c := subStart.c
			sub := subStart.sub
			qs := subStart.qs
			isDurable := subStart.isDurable
			if isDurable {
				// Redeliver any outstanding.
				s.performDurableRedelivery(c, sub)
			}
			// publish messages to this subscriber
			if qs != nil {
				s.sendAvailableMessagesToQueue(c, qs)
			} else {
				s.sendAvailableMessages(c, sub)
			}
		case <-s.subStartQuit:
			return
		}
	}
}

// processAckMsg processes inbound acks from clients for delivered messages.
func (s *StanServer) processAckMsg(m *nats.Msg) {
	ack := &pb.Ack{}
	if ack.Unmarshal(m.Data) != nil {
		if s.processCtrlMsg(m) {
			return
		}
	}
	c := s.channels.get(ack.Subject)
	if c == nil {
		s.log.Errorf("Unable to process ack seq=%d, channel %s not found", ack.Sequence, ack.Subject)
		return
	}
	s.processAck(c, c.ss.LookupByAckInbox(m.Subject), ack.Sequence)
}

// processAck processes an ack and if needed sends more messages.
func (s *StanServer) processAck(c *channel, sub *subState, sequence uint64) {
	if sub == nil {
		return
	}

	var stalled bool

	// This is immutable, so can grab outside of sub's lock.
	// If we have a queue group, we want to grab queue's lock before
	// sub's lock.
	qs := sub.qstate
	if qs != nil {
		qs.Lock()
	}

	sub.Lock()

	if s.trace {
		s.log.Tracef("[Client:%s] Processing ack for subid=%d, subject=%s, seq=%d",
			sub.ClientID, sub.ID, sub.subject, sequence)
	}

	if err := sub.store.AckSeqPending(sub.ID, sequence); err != nil {
		s.log.Errorf("[Client:%s] Unable to persist ack for subid=%d, subject=%s, seq=%d, err=%v",
			sub.ClientID, sub.ID, sub.subject, sequence, err)
		sub.Unlock()
		if qs != nil {
			qs.Unlock()
		}
		return
	}

	delete(sub.acksPending, sequence)
	if sub.stalled && int32(len(sub.acksPending)) < sub.MaxInFlight {
		// For queue, we must not check the queue stalled count here. The queue
		// as a whole may not be stalled, yet, if this sub was stalled, it is
		// not now since the pending acks is below MaxInflight. The server should
		// try to send available messages.
		// It works also if the queue *was* stalled (all members were stalled),
		// then this member is no longer stalled, which release the queue.

		// Trigger send of available messages by setting this to true.
		stalled = true

		// Clear the stalled flag from this sub
		sub.stalled = false
		// .. and update the queue's stalled members count if this is a queue sub.
		if qs != nil && qs.stalledSubCount > 0 {
			qs.stalledSubCount--
		}
	}
	sub.Unlock()
	if qs != nil {
		qs.Unlock()
	}

	// Leave the reset/cancel of the ackTimer to the redelivery cb.

	if !stalled {
		return
	}

	if qs != nil {
		s.sendAvailableMessagesToQueue(c, qs)
	} else {
		s.sendAvailableMessages(c, sub)
	}
}

// Send any messages that are ready to be sent that have been queued to the group.
func (s *StanServer) sendAvailableMessagesToQueue(c *channel, qs *queueState) {
	if c == nil || qs == nil {
		return
	}

	qs.Lock()
	if qs.newOnHold {
		qs.Unlock()
		return
	}
	for nextSeq := qs.lastSent + 1; qs.stalledSubCount < len(qs.subs); nextSeq++ {
		nextMsg := s.getNextMsg(c, &nextSeq, &qs.lastSent)
		if nextMsg == nil {
			break
		}
		if _, sent, sendMore := s.sendMsgToQueueGroup(qs, nextMsg, honorMaxInFlight); !sent || !sendMore {
			break
		}
	}
	qs.Unlock()
}

// Send any messages that are ready to be sent that have been queued.
func (s *StanServer) sendAvailableMessages(c *channel, sub *subState) {
	sub.Lock()
	for nextSeq := sub.LastSent + 1; !sub.stalled; nextSeq++ {
		nextMsg := s.getNextMsg(c, &nextSeq, &sub.LastSent)
		if nextMsg == nil {
			break
		}
		if sent, sendMore := s.sendMsgToSub(sub, nextMsg, honorMaxInFlight); !sent || !sendMore {
			break
		}
	}
	sub.Unlock()
}

func (s *StanServer) getNextMsg(c *channel, nextSeq, lastSent *uint64) *pb.MsgProto {
	for {
		nextMsg, err := c.store.Msgs.Lookup(*nextSeq)
		if err != nil {
			s.log.Errorf("Error looking up message %v:%v (%v)", c.name, *nextSeq, err)
			// TODO: This will stop delivery. Will revisit later to see if we
			// should move to the next message (if avail) or not.
			return nil
		}
		if nextMsg != nil {
			return nextMsg
		}
		// Reason why we don't call FirstMsg here is that
		// FirstMsg could be costly (read from disk, etc)
		// to realize that the message is of lower sequence.
		// So check with cheaper FirstSequence() first.

		// TODO: Error is ignored here. Will revisit this whole function
		// later with handling of gaps in message sequence (either due
		// to errors or absence of messages - that may have been removed
		// after fixing a previous store corruption).
		firstAvail, _ := c.store.Msgs.FirstSequence()
		if firstAvail <= *nextSeq {
			return nil
		}
		// TODO: We may send dataloss advisories to the client
		// through the use of a subscription created optionally
		// by the sub and given to the server through the SubscriptionRequest.
		// For queue group, server would pick one of the member to send
		// the advisory to.

		// For now, just skip the missing ones.
		*nextSeq = firstAvail
		*lastSent = firstAvail - 1

		// Note that the next lookup could still fail because
		// the first avail message may have been dropped in the
		// meantime.
	}
}

// Setup the start position for the subscriber.
func (s *StanServer) setSubStartSequence(c *channel, sub *subState, sr *pb.SubscriptionRequest) (string, error) {
	sub.Lock()
	defer sub.Unlock()

	lastSent := uint64(0)
	debugTrace := ""

	// In all start position cases, if there is no message, ensure
	// lastSent stays at 0.

	switch sr.StartPosition {
	case pb.StartPosition_NewOnly:
		var err error
		lastSent, err = c.store.Msgs.LastSequence()
		if err != nil {
			return "", err
		}
		if s.debug {
			debugTrace = fmt.Sprintf("new-only, seq=%d", lastSent+1)
		}
	case pb.StartPosition_LastReceived:
		lastSeq, err := c.store.Msgs.LastSequence()
		if err != nil {
			return "", err
		}
		if lastSeq > 0 {
			lastSent = lastSeq - 1
		}
		if s.debug {
			debugTrace = fmt.Sprintf("last message, seq=%d", lastSent+1)
		}
	case pb.StartPosition_TimeDeltaStart:
		startTime := time.Now().UnixNano() - sr.StartTimeDelta
		// If there is no message, seq will be 0.
		seq, err := c.store.Msgs.GetSequenceFromTimestamp(startTime)
		if err != nil {
			return "", err
		}
		if seq > 0 {
			// If the time delta is in the future relative to the last
			// message in the log, 'seq' will be equal to last sequence + 1,
			// so this would translate to "new only" semantic.
			lastSent = seq - 1
		}
		if s.debug {
			debugTrace = fmt.Sprintf("from time time='%v' seq=%d", time.Unix(0, startTime), lastSent+1)
		}
	case pb.StartPosition_SequenceStart:
		// If there is no message, firstSeq and lastSeq will be equal to 0.
		firstSeq, lastSeq, err := c.store.Msgs.FirstAndLastSequence()
		if err != nil {
			return "", err
		}
		// StartSequence is an uint64, so can't be lower than 0.
		if sr.StartSequence < firstSeq {
			// That translates to sending the first message available.
			lastSent = firstSeq - 1
		} else if sr.StartSequence > lastSeq {
			// That translates to "new only"
			lastSent = lastSeq
		} else if sr.StartSequence > 0 {
			// That translates to sending the message with StartSequence
			// sequence number.
			lastSent = sr.StartSequence - 1
		}
		if s.debug {
			debugTrace = fmt.Sprintf("from sequence, asked_seq=%d actual_seq=%d", sr.StartSequence, lastSent+1)
		}
	case pb.StartPosition_First:
		firstSeq, err := c.store.Msgs.FirstSequence()
		if err != nil {
			return "", err
		}
		if firstSeq > 0 {
			lastSent = firstSeq - 1
		}
		if s.debug {
			debugTrace = fmt.Sprintf("from beginning, seq=%d", lastSent+1)
		}
	}
	sub.LastSent = lastSent
	return debugTrace, nil
}

// startGoRoutine starts the given function as a go routine if and only if
// the server was not shutdown at that time. This is required because
// we cannot increment the wait group after the shutdown process has started.
func (s *StanServer) startGoRoutine(f func()) {
	s.mu.Lock()
	if !s.shutdown {
		s.wg.Add(1)
		go f()
	}
	s.mu.Unlock()
}

// ClusterID returns the STAN Server's ID.
func (s *StanServer) ClusterID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.info.ClusterID
}

// State returns the state of this server.
func (s *StanServer) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// setLastError sets the last fatal error that occurred. This is
// used in case of an async error that cannot directly be reported
// to the user.
func (s *StanServer) setLastError(err error) {
	s.mu.Lock()
	s.lastError = err
	s.state = Failed
	s.mu.Unlock()
	s.log.Fatalf("%v", err)
}

// LastError returns the last fatal error the server experienced.
func (s *StanServer) LastError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastError
}

// Shutdown will close our NATS connection and shutdown any embedded NATS server.
func (s *StanServer) Shutdown() {
	s.log.Noticef("Shutting down.")

	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return
	}

	// Allows Shutdown() to be idempotent
	s.shutdown = true
	// Change the state too
	s.state = Shutdown

	// We need to make sure that the storeIOLoop returns before
	// closing the Store
	waitForIOStoreLoop := true

	// Capture under lock
	store := s.store
	ns := s.natsServer
	// Do not close and nil the connections here, they are used in many places
	// without locking. Once closed, s.nc.xxx() calls will simply fail, but
	// we won't panic.
	ncs := s.ncs
	nc := s.nc
	ftnc := s.ftnc

	// Stop processing subscriptions start requests
	s.subStartQuit <- struct{}{}

	if s.ioChannel != nil {
		// Notify the IO channel that we are shutting down
		s.ioChannelQuit <- struct{}{}
	} else {
		waitForIOStoreLoop = false
	}
	// In case we are running in FT mode.
	if s.ftQuit != nil {
		s.ftQuit <- struct{}{}
	}
	// In case we are running in Partitioning mode
	if s.partitions != nil {
		s.partitions.shutdown()
	}
	s.mu.Unlock()

	// Make sure the StoreIOLoop returns before closing the Store
	if waitForIOStoreLoop {
		s.ioChannelWG.Wait()
	}

	// Close/Shutdown resources. Note that unless one instantiates StanServer
	// directly (instead of calling RunServer() and the like), these should
	// not be nil.
	if store != nil {
		store.Close()
	}
	if ncs != nil {
		ncs.Close()
	}
	if nc != nil {
		nc.Close()
	}
	if ftnc != nil {
		ftnc.Close()
	}
	if ns != nil {
		ns.Shutdown()
	}

	// Wait for go-routines to return
	s.wg.Wait()
}
