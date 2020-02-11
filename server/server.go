// Copyright 2016-2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	natsdLogger "github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/nats-io/stan.go/pb"
)

// A single NATS Streaming Server

// Server defaults.
const (
	// VERSION is the current version for the NATS Streaming server.
	VERSION = "0.17.0"

	DefaultClusterID      = "test-cluster"
	DefaultDiscoverPrefix = "_STAN.discover"
	DefaultPubPrefix      = "_STAN.pub"
	DefaultSubPrefix      = "_STAN.sub"
	DefaultSubClosePrefix = "_STAN.subclose"
	DefaultUnSubPrefix    = "_STAN.unsub"
	DefaultClosePrefix    = "_STAN.close"
	defaultAcksPrefix     = "_STAN.ack"
	defaultSnapshotPrefix = "_STAN.snap"
	defaultRaftPrefix     = "_STAN.raft"
	DefaultStoreType      = stores.TypeMemory

	// Prefix of subject active server is sending HBs to
	ftHBPrefix = "_STAN.ft"

	// DefaultHeartBeatInterval is the interval at which server sends heartbeat to a client
	DefaultHeartBeatInterval = 30 * time.Second
	// DefaultClientHBTimeout is how long server waits for a heartbeat response
	DefaultClientHBTimeout = 10 * time.Second
	// DefaultMaxFailedHeartBeats is the number of failed heartbeats before server closes
	// the client connection (total= (heartbeat interval + heartbeat timeout) * (fail count + 1)
	DefaultMaxFailedHeartBeats = int((5 * time.Minute) / DefaultHeartBeatInterval)

	// Timeout used to ping the known client when processing a connection
	// request for a duplicate client ID.
	defaultCheckDupCIDTimeout = 500 * time.Millisecond

	// DefaultIOBatchSize is the maximum number of messages to accumulate before flushing a store.
	DefaultIOBatchSize = 1024

	// DefaultIOSleepTime is the duration (in micro-seconds) the server waits for more messages
	// before starting processing. Set to 0 (or negative) to disable the wait.
	DefaultIOSleepTime = int64(0)

	// DefaultLogCacheSize is the number of Raft log entries to cache in memory
	// to reduce disk IO.
	DefaultLogCacheSize = 512

	// DefaultLogSnapshots is the number of Raft log snapshots to retain.
	DefaultLogSnapshots = 2

	// DefaultTrailingLogs is the number of log entries to leave after a
	// snapshot and compaction.
	DefaultTrailingLogs = 10240

	// Length of the channel used to schedule subscriptions start requests.
	// Subscriptions requests are processed from the same NATS subscription.
	// When a subscriber starts and it has pending messages, the server
	// processes the new subscription request and sends avail messages out
	// (up to MaxInflight). When done in place, this can cause other
	// new subscriptions requests to timeout. Server uses a channel to schedule
	// start (that is sending avail messages) of new subscriptions. This is
	// the default length of that channel.
	defaultSubStartChanLen = 2048

	// Name of the file to store Raft log.
	raftLogFile = "raft.log"

	// In partitioning mode, when a client connects, the connect request
	// may reach several servers, but the first response the client gets
	// allows it to proceed with either publish or subscribe.
	// So it is possible for a server running in partitioning mode to
	// receives a connection request followed by a message or subscription.
	// Although the conn request would be first in the tcp connection, it
	// is then possible that the PubMsg or SubscriptionRequest be processed
	// first due to the use of different nats subscriptions.
	// To prevent that, when checking if a client exists, in this particular
	// mode we will possibly wait to be notified when the client has been
	// registered. This is the default duration for this wait.
	defaultClientCheckTimeout = time.Second

	// Interval at which server goes through list of subscriptions with
	// pending sent/ack operations that needs to be replicated.
	defaultLazyReplicationInterval = time.Second

	// Log statement printed when server is considered ready
	streamingReadyLog = "Streaming Server is ready"
)

// Constant to indicate that sendMsgToSub() should check number of acks pending
// against MaxInFlight to know if message should be sent out.
const (
	forceDelivery    = true
	honorMaxInFlight = false
)

// Constants to indicate if we are replicating a Sent or an Ack
const (
	replicateSent = true
	replicateAck  = false
)

const (
	// Client send connID in ConnectRequest and PubMsg, and server
	// listens and responds to client PINGs. The validity of the
	// connection (based on connID) is checked on incoming PINGs.
	protocolOne = int32(1)
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
	ErrClusteredRestart   = errors.New("stan: cannot restart server in clustered mode if it was not previously clustered")
	ErrChanDelInProgress  = errors.New("stan: channel is being deleted")
)

// Shared regular expression to check clientID validity.
// No lock required since from doc: https://golang.org/pkg/regexp/
// A Regexp is safe for concurrent use by multiple goroutines.
var clientIDRegEx *regexp.Regexp

var (
	testAckWaitIsInMillisecond bool
	clientCheckTimeout         = defaultClientCheckTimeout
	lazyReplicationInterval    = defaultLazyReplicationInterval
	testDeleteChannel          bool
	testSubSentAndAckSlowApply bool
)

var (
	// gitCommit injected at build
	gitCommit string
)

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
	c  *channel
	dc bool // if true, this is a request to delete this channel.

	// Use for synchronization between ioLoop and other routines
	sc  chan struct{}
	sdc chan struct{}
}

// Constant that defines the size of the channel that feeds the IO thread.
const ioChannelSize = 64 * 1024

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
	Clustered
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
	case Clustered:
		return "CLUSTERED"
	default:
		return "UNKNOW STATE"
	}
}

type channelStore struct {
	sync.RWMutex
	channels map[string]*channel
	store    stores.Store
	stan     *StanServer
}

func newChannelStore(srv *StanServer, s stores.Store) *channelStore {
	cs := &channelStore{
		channels: make(map[string]*channel),
		store:    s,
		stan:     srv,
	}
	return cs
}

func (cs *channelStore) get(name string) *channel {
	cs.RLock()
	c := cs.channels[name]
	cs.RUnlock()
	return c
}

func (cs *channelStore) getIfNotAboutToBeDeleted(name string) *channel {
	cs.RLock()
	c := cs.channels[name]
	if c != nil && c.activity != nil && c.activity.deleteInProgress {
		cs.RUnlock()
		return nil
	}
	cs.RUnlock()
	return c
}

func (cs *channelStore) createChannel(s *StanServer, name string) (*channel, error) {
	cs.Lock()
	c, err := cs.createChannelLocked(s, name, 0)
	cs.Unlock()
	return c, err
}

func (cs *channelStore) createChannelLocked(s *StanServer, name string, id uint64) (*channel, error) {
	// It is possible that there were 2 concurrent calls to lookupOrCreateChannel
	// which first uses `channelStore.get()` and if not found, calls this function.
	// So we need to check now that we have the write lock that the channel has
	// not already been created.
	c := cs.channels[name]
	if c != nil {
		if c.activity != nil && c.activity.deleteInProgress {
			return nil, ErrChanDelInProgress
		}
		return c, nil
	}
	if s.isClustered {
		if s.isLeader() && id == 0 {
			var err error
			if id, err = s.raft.store.LastIndex(); err != nil {
				return nil, err
			}
		}
		if err := s.raft.store.SetChannelID(name, id); err != nil {
			return nil, err
		}
	}
	sc, err := cs.store.CreateChannel(name)
	if err != nil {
		return nil, err
	}
	c, err = cs.create(s, name, sc)
	if err != nil {
		if id != 0 {
			cs.stan.raft.store.DeleteChannelID(name)
		}
		return nil, err
	}
	c.id = id
	if s.isStandaloneOrLeader() && c.activity != nil {
		c.startDeleteTimer()
	}
	cs.stan.log.Noticef("Channel %q has been created", name)
	return c, nil
}

// low-level creation and storage in memory of a *channel
// Lock is held on entry or not needed.
func (cs *channelStore) create(s *StanServer, name string, sc *stores.Channel) (*channel, error) {
	c := &channel{name: name, store: sc, ss: s.createSubStore(), stan: s, nextSubID: 1}
	lastSequence, err := c.store.Msgs.LastSequence()
	if err != nil {
		return nil, err
	}
	c.nextSequence = lastSequence + 1
	cs.channels[name] = c
	cl := cs.store.GetChannelLimits(name)
	if cl.MaxInactivity > 0 {
		c.activity = &channelActivity{maxInactivity: cl.MaxInactivity}
	}
	return c, nil
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

func (cs *channelStore) maybeStartChannelDeleteTimer(name string, c *channel) {
	cs.Lock()
	if c == nil {
		c = cs.channels[name]
	}
	if c != nil && c.activity != nil && !c.activity.deleteInProgress && !c.ss.hasActiveSubs() {
		c.startDeleteTimer()
	}
	cs.Unlock()
}

func (cs *channelStore) stopDeleteTimer(c *channel) {
	cs.Lock()
	c.stopDeleteTimer()
	if c.activity != nil {
		c.activity.deleteInProgress = false
	}
	cs.Unlock()
}

func (cs *channelStore) turnOffPreventDelete(c *channel) {
	cs.Lock()
	if c != nil && c.activity != nil {
		c.activity.preventDelete = false
	}
	cs.Unlock()
}

type channel struct {
	// This is used in clustering mode in a specific situation where
	// all messages may have been expired so the store reports 0,0
	// but we know that the firstSeq should be something else.
	// Used with atomic operation.
	firstSeq     uint64
	nextSequence uint64
	name         string
	id           uint64
	store        *stores.Channel
	ss           *subStore
	lTimestamp   int64
	stan         *StanServer
	activity     *channelActivity
	nextSubID    uint64

	// Used in cluster mode. This is to know if the message store
	// last sequence should be checked before storing a message in
	// Apply(). Protected by the raft's FSM lock.
	lSeqChecked bool
}

type channelActivity struct {
	last             time.Time
	maxInactivity    time.Duration
	timer            *time.Timer
	deleteInProgress bool
	preventDelete    bool
	timerSet         bool
}

// Starts the delete timer that when firing will post
// a channel delete request to the ioLoop.
// The channelStore's mutex must be held on entry.
func (c *channel) startDeleteTimer() {
	c.activity.last = time.Now()
	c.resetDeleteTimer(c.activity.maxInactivity)
}

// Stops the delete timer.
// The channelStore's mutex must be held on entry.
func (c *channel) stopDeleteTimer() {
	if c.activity.timer != nil {
		c.activity.timer.Stop()
		c.activity.timerSet = false
		if c.stan.debug {
			c.stan.log.Debugf("Channel %q delete timer stopped", c.name)
		}
	}
}

// Resets the delete timer to the given duration.
// If the timer was not created, this call will create it.
// The channelStore's mutex must be held on entry.
func (c *channel) resetDeleteTimer(newDuration time.Duration) {
	a := c.activity
	if a.timer == nil {
		a.timer = time.AfterFunc(newDuration, func() {
			c.stan.sendDeleteChannelRequest(c)
		})
	} else {
		a.timer.Reset(newDuration)
	}
	if c.stan.debug {
		c.stan.log.Debugf("Channel %q delete timer set to fire in %v", c.name, newDuration)
	}
	a.timerSet = true
}

// pubMsgToMsgProto converts a PubMsg to a MsgProto and assigns a timestamp
// which is monotonic with respect to the channel.
func (c *channel) pubMsgToMsgProto(pm *pb.PubMsg, seq uint64) *pb.MsgProto {
	m := &pb.MsgProto{
		Sequence:  seq,
		Subject:   pm.Subject,
		Reply:     pm.Reply,
		Data:      pm.Data,
		Timestamp: time.Now().UnixNano(),
	}
	if c.lTimestamp > 0 && m.Timestamp < c.lTimestamp {
		m.Timestamp = c.lTimestamp
	}
	c.lTimestamp = m.Timestamp
	return m
}

// Sets a subscription that will handle snapshot restore requests from followers.
func (s *StanServer) subToSnapshotRestoreRequests() error {
	var (
		msgBuf                []byte
		buf                   []byte
		snapshotRestorePrefix = fmt.Sprintf("%s.%s.", defaultSnapshotPrefix, s.info.ClusterID)
		prefixLen             = len(snapshotRestorePrefix)
	)
	sub, err := s.ncsr.Subscribe(snapshotRestorePrefix+">", func(m *nats.Msg) {
		if len(m.Data) != 16 {
			s.log.Errorf("Invalid snapshot request, data len=%v", len(m.Data))
			return
		}

		// Servers up to 0.14.1 (included) had a defect both on the leader and
		// follower side. That is, they would both break out of their loop when
		// getting the first "not found" message (expired or removed due to
		// limits).
		// Starting at 0.14.2, server needs to complete their loop to make sure
		// they have restored all messages in the start/end range.
		// The code below tries to handle pre and post 0.14.2 follower requests.

		// A follower at version 0.14.2+ will include a known suffix to its
		// reply subject. The leader here can then send the first available
		// message when getting a request for a message of a given sequence
		// that is not found.
		v2 := strings.HasSuffix(m.Reply, "."+restoreMsgsV2)

		// For the newer servers, include a "reply" subject to the response
		// to let the follower know that this is coming from a 0.14.2+ server.
		var reply string
		var replyFirstAvail string
		if v2 {
			reply = restoreMsgsV2
			replyFirstAvail = restoreMsgsV2 + restoreMsgsFirstAvailSuffix
		}

		cname := m.Subject[prefixLen:]
		c := s.channels.getIfNotAboutToBeDeleted(cname)
		if c == nil {
			s.ncsr.PublishRequest(m.Reply, reply, nil)
			return
		}
		start := util.ByteOrder.Uint64(m.Data[:8])
		end := util.ByteOrder.Uint64(m.Data[8:])

		for seq := start; seq <= end; seq++ {
			sendingTheFirstAvail := false
			msg, err := c.store.Msgs.Lookup(seq)
			if err != nil {
				s.log.Errorf("Snapshot restore request error for channel %q, error looking up message %v: %v", c.name, seq, err)
				return
			}
			// If the requestor is a server 0.14.2+, we will send the first
			// available message.
			if msg == nil && v2 {
				msg, err = c.store.Msgs.FirstMsg()
				if err != nil {
					s.log.Errorf("Snapshot restore request error for channel %q, error looking up first message: %v", c.name, err)
					return
				}
				sendingTheFirstAvail = msg != nil
				if sendingTheFirstAvail && msg.Sequence < seq {
					// It could be that a follower tries to restore from
					// a snapshot of previous version of the same channel name
					// that had more messages than current channel. So
					// bail out if the first avail is below the last seq
					// we dealt with.
					msg = nil
				}
			}
			if msg == nil {
				// We don't have this message because of channel limits.
				// Return nil to caller to signal this state.
				buf = nil
			} else {
				msgBuf = util.EnsureBufBigEnough(msgBuf, msg.Size())
				n, err := msg.MarshalTo(msgBuf)
				if err != nil {
					panic(err)
				}
				buf = msgBuf[:n]
			}
			// This will be empty string for old requestors, or restoreMsgsV2
			// for servers 0.14.2+
			respReply := reply
			if sendingTheFirstAvail {
				// Use the reply subject that contains information that this
				// is the first available message. Requestors 0.16.1+ will
				// make use of that.
				respReply = replyFirstAvail
			}
			if err := s.ncsr.PublishRequest(m.Reply, respReply, buf); err != nil {
				s.log.Errorf("Snapshot restore request error for channel %q, unable to send response for seq %v: %v", c.name, seq, err)
				return
			}
			// If we sent a message and seq was not the expected one,
			// reset seq to proper value for the next iteration.
			if msg != nil && msg.Sequence != seq {
				seq = msg.Sequence
			} else if buf == nil {
				// Regardless of the version of the requestor, if we are
				// here we can stop. If it is a pre 0.14.2, the follower
				// would have break out of its for-loop when getting the
				// nil message, so there is no point in continuing.
				// And if it is a 0.14.2+, we have sent the first avail
				// message and so if we are here it means that there
				// is none, so we really have nothing else to do.
				return
			}
			select {
			case <-s.shutdownCh:
				return
			default:
			}
		}
	})
	if err != nil {
		return err
	}
	sub.SetPendingLimits(-1, -1)
	s.snapReqSub = sub
	return nil
}

// StanServer structure represents the NATS Streaming Server
type StanServer struct {
	// Keep all members for which we use atomic at the beginning of the
	// struct and make sure they are all 64bits (or use padding if necessary).
	// atomic.* functions crash on 32bit machines if operand is not aligned
	// at 64bit. See https://github.com/golang/go/issues/599
	ioChannelStatsMaxBatchSize int64 // stats of the max number of messages than went into a single batch
	stats                      struct {
		inMsgs   int64
		inBytes  int64
		outMsgs  int64
		outBytes int64
	}

	mu         sync.RWMutex
	shutdown   bool
	shutdownCh chan struct{}
	serverID   string
	info       spb.ServerInfo // Contains cluster ID and subjects
	natsServer *server.Server
	opts       *Options
	natsOpts   *server.Options
	startTime  time.Time

	// For scalability, a dedicated connection is used to publish
	// messages to subscribers and for replication.
	nc   *nats.Conn // used for most protocol messages
	ncs  *nats.Conn // used for sending to subscribers and acking publishers
	nca  *nats.Conn // used to receive subscriptions acks
	ncr  *nats.Conn // used for raft messages
	ncsr *nats.Conn // used for raft snapshot replication

	wg sync.WaitGroup // Wait on go routines during shutdown

	// Used when processing connect requests for client ID already registered
	dupCIDTimeout time.Duration

	// Clients
	clients       *clientStore
	cliDupCIDsMu  sync.Mutex
	cliDipCIDsMap map[string]struct{}

	// channels
	channels *channelStore

	// Store
	store stores.Store

	// IO Channel
	ioChannel     chan *ioPendingMsg
	ioChannelQuit chan struct{}
	ioChannelWG   sync.WaitGroup

	// To protect some close related requests
	closeMu sync.Mutex

	tmpBuf []byte // Used to marshal protocols (right now, only PubAck)

	subStartCh   chan *subStartInfo
	subStartQuit chan struct{}

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

	// Specific to clustering
	raft        *raftNode
	raftLogging bool
	isClustered bool
	ssarepl     *subsSentAndAckReplication
	snapReqSub  *nats.Subscription

	// Our internal subscriptions
	connectSub  *nats.Subscription
	closeSub    *nats.Subscription
	pubSub      *nats.Subscription
	subSub      *nats.Subscription
	subCloseSub *nats.Subscription
	subUnsubSub *nats.Subscription
	cliPingSub  *nats.Subscription

	// For sending responses to client PINGS. Used to be global but would
	// cause races when running more than 1 server in a program or test.
	pingResponseOKBytes            []byte
	pingResponseInvalidClientBytes []byte
}

type subsSentAndAckReplication struct {
	ready    *sync.Map
	waiting  *sync.Map
	gates    *sync.Map
	notifyCh chan struct{}
}

func (s *StanServer) isLeader() bool {
	return atomic.LoadInt64(&s.raft.leader) == 1
}

func (s *StanServer) isStandaloneOrLeader() bool {
	return !s.isClustered || s.isLeader()
}

// subStore holds all known state for all subscriptions
type subStore struct {
	sync.RWMutex
	psubs    []*subState            // plain subscribers
	qsubs    map[string]*queueState // queue subscribers
	durables map[string]*subState   // durables lookup
	acks     map[string]*subState   // ack inbox lookup
	stan     *StanServer            // back link to the server
}

// Holds all queue subsribers for a subject/group and
// tracks lastSent for the group.
type queueState struct {
	sync.RWMutex
	lastSent        uint64
	subs            []*subState
	rdlvCount       map[uint64]uint32
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

	savedClientID string // Used only for closed durables in Clustering mode and monitoring endpoints.

	replicate *subSentAndAck // Used in Clustering mode
	norepl    bool           // When a sub is being closed, prevents collectSentOrAck to recreate `replicate`.

	rdlvCount map[uint64]uint32 // Used only when not a queue sub, otherwise queueState's rldvCount is used.

	// So far, compacting these booleans into a byte flag would not save space.
	// May change if we need to add more.
	initialized bool // false until the subscription response has been sent to prevent data to be sent too early.
	stalled     bool
	newOnHold   bool // Prevents delivery of new msgs until old are redelivered (on restart)
	hasFailedHB bool // This is set when server sends heartbeat to this subscriber's client.
}

type subSentAndAck struct {
	sent     []uint64
	ack      []uint64
	applying bool
}

// Returns the total number of subscriptions (including offline (queue) durables).
// This is used essentially for the monitoring endpoint /streaming/serverz.
func (s *StanServer) numSubs() int {
	total := 0
	s.channels.RLock()
	for _, c := range s.channels.channels {
		c.ss.RLock()
		total += len(c.ss.psubs)
		// Need to add offline durables
		for _, sub := range c.ss.durables {
			if sub.ClientID == "" {
				total++
			}
		}
		for _, qsub := range c.ss.qsubs {
			qsub.RLock()
			total += len(qsub.subs)
			// If this is a durable queue subscription and all members
			// are offline, qsub.shadow will be not nil. Report this one.
			if qsub.shadow != nil {
				total++
			}
			qsub.RUnlock()
		}
		c.ss.RUnlock()
	}
	s.channels.RUnlock()
	return total
}

// Looks up, or create a new channel if it does not exist
func (s *StanServer) lookupOrCreateChannel(name string) (*channel, error) {
	cs := s.channels
	cs.RLock()
	c := cs.channels[name]
	if c != nil {
		if c.activity != nil && c.activity.deleteInProgress {
			cs.RUnlock()
			return nil, ErrChanDelInProgress
		}
		cs.RUnlock()
		return c, nil
	}
	cs.RUnlock()
	return cs.createChannel(s, name)
}

func (s *StanServer) lookupOrCreateChannelPreventDelete(name string) (*channel, bool, error) {
	cs := s.channels
	cs.Lock()
	c := cs.channels[name]
	if c != nil {
		if c.activity != nil && c.activity.deleteInProgress {
			cs.Unlock()
			return nil, false, ErrChanDelInProgress
		}
	} else {
		var err error
		c, err = cs.createChannelLocked(s, name, 0)
		if err != nil {
			cs.Unlock()
			return nil, false, err
		}
	}
	preventDelete := false
	if c.activity != nil {
		c.activity.preventDelete = true
		c.stopDeleteTimer()
		preventDelete = true
	}
	cs.Unlock()
	return c, preventDelete, nil
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

	// Adds to storage.
	// Use sub lock to avoid race with waitForAcks in some tests
	sub.Lock()
	err := sub.store.CreateSub(&sub.SubState)
	sub.Unlock()
	if err == nil {
		err = sub.store.Flush()
	}
	if err != nil {
		ss.stan.log.Errorf("Unable to store subscription [%v:%v] on [%s]: %v", sub.ClientID, sub.Inbox, sub.subject, err)
		return err
	}

	ss.updateState(sub)

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
			if qs.shadow != nil {
				ss.stan.log.Warnf("Duplicate shadow durable queue consumer (subid=%v) for group %q", sub.ID, sub.QGroup)
			}
			if qs.shadow == nil || sub.LastSent > qs.lastSent {
				qs.shadow = sub
			}
		} else {
			// Store by ackInbox for ack direct lookup
			ss.acks[sub.AckInbox] = sub

			qs.subs = append(qs.subs, sub)

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
		}
		// Needed in the case of server restart, where
		// the queue group's last sent needs to be updated
		// based on the recovered subscriptions.
		if sub.LastSent > qs.lastSent {
			qs.lastSent = sub.LastSent
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

// returns an array of all subscriptions (plain, online durables and queue members).
func (ss *subStore) getAllSubs() []*subState {
	ss.RLock()
	subs := make([]*subState, 0, len(ss.psubs))
	subs = append(subs, ss.psubs...)
	for _, qs := range ss.qsubs {
		qs.RLock()
		subs = append(subs, qs.subs...)
		qs.RUnlock()
	}
	ss.RUnlock()
	return subs
}

// hasSubs returns true if there is any active subscription for this subStore.
// That is, offline durable subscriptions are ignored.
func (ss *subStore) hasActiveSubs() bool {
	ss.RLock()
	defer ss.RUnlock()
	if len(ss.psubs) > 0 {
		return true
	}
	for _, qsub := range ss.qsubs {
		// For a durable queue group, when the group is offline,
		// qsub.shadow is not nil, but the qsub.subs array should be
		// empty.
		if len(qsub.subs) > 0 {
			return true
		}
	}
	return false
}

// Remove a subscriber from the subscription store, leaving durable
// subscriptions unless `unsubscribe` is true.
func (ss *subStore) Remove(c *channel, sub *subState, unsubscribe bool) {
	if sub == nil {
		return
	}

	var (
		log               logger.Logger
		queueGroupIsEmpty bool
	)

	ss.Lock()
	s := ss.stan
	standaloneOrLeader := !s.isClustered || s.isLeader()
	if s.debug {
		log = s.log
	}

	sub.Lock()
	subject := sub.subject
	clientID := sub.ClientID
	durableKey := ""
	// Do this before clearing the sub.ClientID since this is part of the key!!!
	if sub.isDurableSubscriber() {
		durableKey = sub.durableKey()
	}
	// This is needed when doing a snapshot in clustering mode or for monitoring endpoints
	sub.savedClientID = sub.ClientID
	// Clear the subscriptions clientID
	sub.ClientID = ""
	ackInbox := sub.AckInbox
	qs := sub.qstate
	isDurable := sub.IsDurable
	subid := sub.ID
	store := sub.store
	sub.stopAckSub()
	sub.Unlock()

	reportError := func(err error) {
		s.log.Errorf("Error deleting subscription subid=%d, subject=%s, err=%v", subid, subject, err)
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

		sub.Lock()
		sub.clearAckTimer()
		qgroup := sub.QGroup
		sub.Unlock()

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
			if standaloneOrLeader {
				// Set expiration in the past to force redelivery
				expirationTime := time.Now().UnixNano() - int64(time.Second)
				// If there are pending messages in this sub, they need to be
				// transferred to remaining queue subscribers.
				numQSubs := len(qs.subs)
				idx := 0
				sub.RLock()
				// Need to update if this member was the one with the last
				// message of the group.
				storageUpdate = sub.LastSent == qs.lastSent
				sortedPendingMsgs := sub.makeSortedPendingMsgs()
				for _, pm := range sortedPendingMsgs {
					// Get one of the remaning queue subscribers.
					qsub := qs.subs[idx]
					qsub.Lock()
					// Store in storage
					if err := qsub.store.AddSeqPending(qsub.ID, pm.seq); err != nil {
						s.log.Errorf("[Client:%s] Unable to transfer message to subid=%d, subject=%s, seq=%d, err=%v",
							clientID, subid, subject, pm.seq, err)
						qsub.Unlock()
						continue
					}
					// We don't need to update if the sub's lastSent is transferred
					// to another queue subscriber.
					if storageUpdate && pm.seq == qs.lastSent {
						storageUpdate = false
					}
					// Update LastSent if applicable
					if pm.seq > qsub.LastSent {
						qsub.LastSent = pm.seq
					}
					// Store in ackPending.
					qsub.acksPending[pm.seq] = expirationTime
					// Keep track of this qsub
					if qsubs == nil {
						qsubs = make(map[uint64]*subState)
					}
					if _, tracked := qsubs[qsub.ID]; !tracked {
						qsubs[qsub.ID] = qsub
					}
					if s.isClustered {
						s.collectSentOrAck(qsub, true, pm.seq)
					}
					qsub.Unlock()
					// Move to the next queue subscriber, going back to first if needed.
					idx++
					if idx == numQSubs {
						idx = 0
					}
				}
				sub.RUnlock()
			}
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

		sub.Lock()
		sub.clearAckTimer()
		sub.Unlock()

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

	if standaloneOrLeader {
		// Go over the list of queue subs to which we have transferred
		// messages from the leaving member. We want to have those
		// messages redelivered quickly.
		for _, qsub := range qsubs {
			qsub.Lock()
			// Make the timer fire soon. performAckExpirationRedelivery
			// will then re-order messages, etc..
			fireIn := 100 * time.Millisecond
			if qsub.ackTimer == nil {
				s.setupAckTimer(qsub, fireIn)
			} else {
				qsub.ackTimer.Reset(fireIn)
			}
			qsub.Unlock()
		}
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
	ss.RLock()
	sub := ss.acks[ackInbox]
	ss.RUnlock()
	return sub
}

// Options for NATS Streaming Server
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
	Secure             bool          // Create a TLS enabled connection
	ClientCert         string        // Client Certificate for TLS
	ClientKey          string        // Client Key for TLS
	ClientCA           string        // Client CAs for TLS
	TLSSkipVerify      bool          // Skips the server's certificate chain and host name verification (Insecure!)
	TLSServerName      string        // Used to verify the hostname returned in the server certificate
	IOBatchSize        int           // Maximum number of messages collected from clients before starting their processing.
	IOSleepTime        int64         // Duration (in micro-seconds) the server waits for more message to fill up a batch.
	NATSServerURL      string        // URL for external NATS Server to connect to. If empty, NATS Server is embedded.
	NATSCredentials    string        // Credentials file for connecting to external NATS Server.
	ClientHBInterval   time.Duration // Interval at which server sends heartbeat to a client.
	ClientHBTimeout    time.Duration // How long server waits for a heartbeat response.
	ClientHBFailCount  int           // Number of failed heartbeats before server closes client connection.
	FTGroupName        string        // Name of the FT Group. A group can be 2 or more servers with a single active server and all sharing the same datastore.
	Partitioning       bool          // Specify if server only accepts messages/subscriptions on channels defined in StoreLimits.
	SyslogName         string        // Optional name for the syslog (usueful on Windows when running several servers as a service)
	Encrypt            bool          // Specify if server should encrypt messages payload when storing them
	EncryptionCipher   string        // Cipher used for encryption. Supported are "AES" and "CHACHA". If none is specified, defaults to AES on platforms with Intel processors, CHACHA otherwise.
	EncryptionKey      []byte        // Encryption key. The environment NATS_STREAMING_ENCRYPTION_KEY takes precedence and is the preferred way to provide the key.
	Clustering         ClusteringOptions
	NATSClientOpts     []nats.Option
}

// Clone returns a deep copy of the Options object.
func (o *Options) Clone() *Options {
	// A simple copy covers pretty much everything
	clone := *o
	// But we have the problem of the PerChannel map that needs
	// to be copied.
	clone.PerChannel = (&o.StoreLimits).ClonePerChannelMap()
	// Make a copy of the clustering peers
	if len(o.Clustering.Peers) > 0 {
		clone.Clustering.Peers = make([]string, 0, len(o.Clustering.Peers))
		clone.Clustering.Peers = append(clone.Clustering.Peers, o.Clustering.Peers...)
	}
	return &clone
}

// DefaultOptions are default options for the NATS Streaming Server
var defaultOptions = Options{
	ID:                DefaultClusterID,
	DiscoverPrefix:    DefaultDiscoverPrefix,
	StoreType:         DefaultStoreType,
	FileStoreOpts:     stores.DefaultFileStoreOptions,
	IOBatchSize:       DefaultIOBatchSize,
	IOSleepTime:       DefaultIOSleepTime,
	ClientHBInterval:  DefaultHeartBeatInterval,
	ClientHBTimeout:   DefaultClientHBTimeout,
	ClientHBFailCount: DefaultMaxFailedHeartBeats,
}

// GetDefaultOptions returns default options for the NATS Streaming Server
func GetDefaultOptions() (o *Options) {
	opts := defaultOptions
	opts.StoreLimits = stores.DefaultStoreLimits
	return &opts
}

// DefaultNatsServerOptions are default options for the NATS server
var DefaultNatsServerOptions = server.Options{
	Host:   "127.0.0.1",
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

func obfuscatePasswordInURL(url string) string {
	atPos := strings.Index(url, "@")
	if atPos == -1 {
		return url
	}
	start := strings.Index(url, "://")
	if start == -1 {
		return "invalid url"
	}
	newurl := url[:start+3]
	newurl += "[REDACTED]"
	newurl += url[atPos:]
	return newurl
}

func (s *StanServer) stanReconnectedHandler(nc *nats.Conn) {
	s.log.Noticef("connection %q reconnected to NATS Server at %q",
		nc.Opts.Name, obfuscatePasswordInURL(nc.ConnectedUrl()))
}

func (s *StanServer) stanClosedHandler(nc *nats.Conn) {
	s.log.Debugf("connection %q has been closed", nc.Opts.Name)
}

func (s *StanServer) stanErrorHandler(nc *nats.Conn, sub *nats.Subscription, err error) {
	if sub != nil {
		s.log.Errorf("Asynchronous error on connection %s, subject %s: %v",
			nc.Opts.Name, sub.Subject, err)
	} else {
		s.log.Errorf("Asynchronous error on connection %s: %v",
			nc.Opts.Name, err)
	}
}

func (s *StanServer) buildServerURLs() ([]string, error) {
	var hostport string
	natsURL := s.opts.NATSServerURL
	opts := s.natsOpts
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
		switch opts.Host {
		case "0.0.0.0":
			hostport = net.JoinHostPort("127.0.0.1", sport)
		case "::", "[::]":
			hostport = net.JoinHostPort("::1", sport)
		default:
			hostport = net.JoinHostPort(opts.Host, sport)
		}
	}
	return []string{fmt.Sprintf("nats://%s", hostport)}, nil
}

// createNatsClientConn creates a connection to the NATS server, using
// TLS if configured.  Pass in the NATS server options to derive a
// connection url, and for other future items (e.g. auth)
func (s *StanServer) createNatsClientConn(name string) (*nats.Conn, error) {
	var err error
	ncOpts := nats.DefaultOptions

	if s.opts.NATSCredentials != "" {
		nats.UserCredentials(s.opts.NATSCredentials)(&ncOpts)
	}

	for _, o := range s.opts.NATSClientOpts {
		o(&ncOpts)
	}
	// If a TLSConfig is passed as an option, reject if streaming TLSServerName
	// is set, but is different from the existing TLSConfig.ServerName.
	if ncOpts.TLSConfig != nil &&
		ncOpts.TLSConfig.ServerName != "" &&
		s.opts.TLSServerName != "" &&
		ncOpts.TLSConfig.ServerName != s.opts.TLSServerName {
		return nil, fmt.Errorf("conflict between TLS configuration's ServerName %q and Streaming's TLSServerName %q",
			ncOpts.TLSConfig.ServerName, s.opts.TLSServerName)
	}

	ncOpts.Servers, err = s.buildServerURLs()
	if err != nil {
		return nil, err
	}
	ncOpts.User = s.natsOpts.Username
	ncOpts.Password = s.natsOpts.Password
	ncOpts.Token = s.natsOpts.Authorization

	ncOpts.Name = fmt.Sprintf("_NSS-%s-%s", s.opts.ID, name)

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
	if s.opts.Secure {
		if err = nats.Secure()(&ncOpts); err != nil {
			return nil, err
		}
	}
	if s.opts.ClientCA != "" {
		if err = nats.RootCAs(s.opts.ClientCA)(&ncOpts); err != nil {
			return nil, err
		}
	}
	if s.opts.ClientCert != "" {
		if err = nats.ClientCert(s.opts.ClientCert, s.opts.ClientKey)(&ncOpts); err != nil {
			return nil, err
		}
	}
	// Check for TLSSkipVerify and TLSServerName
	if s.opts.TLSServerName != "" || s.opts.TLSSkipVerify {
		// Create TLSConfig if needed
		if ncOpts.TLSConfig == nil {
			if err = nats.Secure(&tls.Config{})(&ncOpts); err != nil {
				return nil, err
			}
		}
		// Do not override if false or empty (since TLSConfig may have been
		// passed as an option).
		if s.opts.TLSSkipVerify {
			ncOpts.TLSConfig.InsecureSkipVerify = s.opts.TLSSkipVerify
		}
		if s.opts.TLSServerName != "" {
			ncOpts.TLSConfig.ServerName = s.opts.TLSServerName
		}
	}
	// Shorten the time we wait to try to reconnect.
	// Don't make it too often because it may exhaust the number of FDs.
	ncOpts.ReconnectWait = 250 * time.Millisecond
	// Make it try to reconnect for ever.
	ncOpts.MaxReconnect = -1
	// To avoid possible duplicate redeliveries, etc.., set the reconnect
	// buffer to -1 to avoid any buffering in the nats library and flush
	// on reconnect.
	ncOpts.ReconnectBufSize = -1

	var nc *nats.Conn
	if nc, err = ncOpts.Connect(); err != nil {
		return nil, err
	}
	return nc, err
}

func (s *StanServer) createNatsConnections() error {
	var err error
	s.ncs, err = s.createNatsClientConn("send")
	if err == nil {
		s.nc, err = s.createNatsClientConn("general")
	}
	if err == nil {
		s.nca, err = s.createNatsClientConn("acks")
	}
	if err == nil && s.opts.FTGroupName != "" {
		s.ftnc, err = s.createNatsClientConn("ft")
	}
	if err == nil && s.isClustered {
		s.ncr, err = s.createNatsClientConn("raft")
		if err == nil {
			s.ncsr, err = s.createNatsClientConn("raft_snap")
		}
	}
	return err
}

// NewNATSOptions returns a new instance of (NATS) Options.
// This is needed if one wants to configure specific NATS options
// before starting a NATS Streaming Server (with RunServerWithOpts()).
func NewNATSOptions() *server.Options {
	opts := server.Options{}
	return &opts
}

// RunServer will startup an embedded NATS Streaming Server and a nats-server to support it.
func RunServer(ID string) (*StanServer, error) {
	sOpts := GetDefaultOptions()
	sOpts.ID = ID
	nOpts := DefaultNatsServerOptions
	return RunServerWithOpts(sOpts, &nOpts)
}

// RunServerWithOpts allows you to run a NATS Streaming Server with full control
// on the Streaming and NATS Server configuration.
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
	// For now, no support for partitioning and clustering at the same time
	if sOpts.Partitioning && sOpts.Clustering.Clustered {
		return nil, fmt.Errorf("stan: channels partitioning in clustering mode is not supported")
	}

	if sOpts.Clustering.Clustered {
		if sOpts.StoreType == stores.TypeMemory {
			return nil, fmt.Errorf("stan: clustering mode not supported with %s store type", stores.TypeMemory)
		}
		// Override store sync configuration with cluster sync.
		sOpts.FileStoreOpts.DoSync = sOpts.Clustering.Sync

		// Remove cluster's node ID (if present) from the list of peers.
		if len(sOpts.Clustering.Peers) > 0 && sOpts.Clustering.NodeID != "" {
			nodeID := sOpts.Clustering.NodeID
			peers := make([]string, 0, len(sOpts.Clustering.Peers))
			for _, p := range sOpts.Clustering.Peers {
				if p != nodeID {
					peers = append(peers, p)
				}
			}
			if len(peers) != len(sOpts.Clustering.Peers) {
				sOpts.Clustering.Peers = peers
			}
		}
	}

	s := StanServer{
		serverID:      nuid.Next(),
		opts:          sOpts,
		natsOpts:      nOpts,
		dupCIDTimeout: defaultCheckDupCIDTimeout,
		ioChannelQuit: make(chan struct{}),
		trace:         sOpts.Trace,
		debug:         sOpts.Debug,
		subStartCh:    make(chan *subStartInfo, defaultSubStartChanLen),
		subStartQuit:  make(chan struct{}, 1),
		startTime:     time.Now(),
		log:           logger.NewStanLogger(),
		shutdownCh:    make(chan struct{}),
		isClustered:   sOpts.Clustering.Clustered,
		raftLogging:   sOpts.Clustering.RaftLogging,
		cliDipCIDsMap: make(map[string]struct{}),
	}

	// If a custom logger is provided, use this one, otherwise, check
	// if we should configure the logger or not.
	if sOpts.CustomLogger != nil {
		s.log.SetLogger(sOpts.CustomLogger, nOpts.Logtime, sOpts.Debug, sOpts.Trace, "")
	} else if sOpts.EnableLogging {
		s.configureLogger()
	}

	s.log.Noticef("Starting nats-streaming-server[%s] version %s", sOpts.ID, VERSION)

	// ServerID is used to check that a brodcast protocol is not ours,
	// for instance with FT. Some err/warn messages may be printed
	// regarding other instance's ID, so print it on startup.
	s.log.Noticef("ServerID: %v", s.serverID)
	s.log.Noticef("Go version: %v", runtime.Version())
	gc := gitCommit
	if gc == "" {
		gc = "not set"
	}
	s.log.Noticef("Git commit: [%s]", gc)

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

	// Create the store.
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
	if s.isClustered {
		// Wrap our store with a RaftStore instance that avoids persisting
		// data that we don't need because they are handled by the actual
		// raft logs.
		store = stores.NewRaftStore(s.log, store, storeLimits)
	}
	if sOpts.Encrypt || len(sOpts.EncryptionKey) > 0 {
		// In clustering mode, RAFT is using its own logs (not the one above),
		// so we need to keep the key intact until we call newRaftLog().
		var key []byte
		if s.isClustered && len(sOpts.EncryptionKey) > 0 {
			key = append(key, sOpts.EncryptionKey...)
		} else {
			key = sOpts.EncryptionKey
		}
		store, err = stores.NewCryptoStore(store, sOpts.EncryptionCipher, key)
		if err != nil {
			return nil, err
		}
	}
	s.store = store

	// Start the IO Loop before creating the channel store since the
	// go routine watching for channel inactivity may schedule events
	// to the IO loop.
	s.startIOLoop()

	s.clients = newClientStore(s.store)
	s.channels = newChannelStore(&s, s.store)

	// If no NATS server url is provided, it means that we embed the NATS Server
	if sOpts.NATSServerURL == "" {
		if err := s.startNATSServer(); err != nil {
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
	if err := s.createNatsConnections(); err != nil {
		return nil, err
	}
	if sOpts.TLSSkipVerify {
		s.log.Warnf("TLS certificate chain and hostname will not be verified. DO NOT USE IN PRODUCTION!")
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
		state := Standalone
		if s.isClustered {
			state = Clustered
		}
		if err := s.start(state); err != nil {
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
func (s *StanServer) configureLogger() {
	var newLogger logger.Logger

	sOpts := s.opts
	nOpts := s.natsOpts

	enableDebug := nOpts.Debug || sOpts.Debug
	enableTrace := nOpts.Trace || sOpts.Trace

	syslog := nOpts.Syslog
	// Enable syslog if no log file is specified and we're running as a
	// Windows service so that logs are written to the Windows event log.
	if isWindowsService() && nOpts.LogFile == "" {
		syslog = true
	}
	// If we have a syslog name specified, make sure we will use this name.
	// This is for syslog and remote syslogs running on Windows.
	if sOpts.SyslogName != "" {
		natsdLogger.SetSyslogName(sOpts.SyslogName)
	}

	if nOpts.LogFile != "" {
		newLogger = natsdLogger.NewFileLogger(nOpts.LogFile, nOpts.Logtime, enableDebug, enableTrace, true)
		if nOpts.LogSizeLimit > 0 {
			if l, ok := newLogger.(*natsdLogger.Logger); ok {
				l.SetSizeLimit(nOpts.LogSizeLimit)
			}
		}
	} else if nOpts.RemoteSyslog != "" {
		newLogger = natsdLogger.NewRemoteSysLogger(nOpts.RemoteSyslog, enableDebug, enableTrace)
	} else if syslog {
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

	s.log.SetLogger(newLogger, nOpts.Logtime, sOpts.Debug, sOpts.Trace, nOpts.LogFile)
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

	// If using partitioning, send our list and start go routines handling
	// channels list requests.
	if s.opts.Partitioning {
		if err := s.initPartitions(); err != nil {
			return err
		}
	}

	s.state = runningState

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

		// If clustering was enabled but we are recovering a server that was
		// previously not clustered, return an error. This is not allowed
		// because there is preexisting state that is not represented in the
		// Raft log.
		if s.isClustered && s.info.NodeID == "" {
			return ErrClusteredRestart
		}
		// Use recovered clustering node ID.
		s.opts.Clustering.NodeID = s.info.NodeID

		// Restore clients state
		s.processRecoveredClients(recoveredState.Clients)

		// Default Raft log path to ./<cluster-id>/<node-id> if not set. This
		// must be done here before recovering channels since that will
		// initialize Raft groups if clustered.
		if s.opts.Clustering.RaftLogPath == "" {
			s.opts.Clustering.RaftLogPath = filepath.Join(s.opts.ID, s.opts.Clustering.NodeID)
		}

		// Process recovered channels (if any).
		recoveredSubs, err = s.processRecoveredChannels(recoveredState.Channels)
		if err != nil {
			return err
		}
	} else {
		s.info.ClusterID = s.opts.ID

		// Generate Subjects
		s.info.Discovery = fmt.Sprintf("%s.%s", s.opts.DiscoverPrefix, s.info.ClusterID)
		s.info.Publish = fmt.Sprintf("%s.%s", DefaultPubPrefix, subjID)
		s.info.Subscribe = fmt.Sprintf("%s.%s", DefaultSubPrefix, subjID)
		s.info.SubClose = fmt.Sprintf("%s.%s", DefaultSubClosePrefix, subjID)
		s.info.Unsubscribe = fmt.Sprintf("%s.%s", DefaultUnSubPrefix, subjID)
		s.info.Close = fmt.Sprintf("%s.%s", DefaultClosePrefix, subjID)
		s.info.AcksSubs = fmt.Sprintf("%s.%s", defaultAcksPrefix, subjID)

		if s.opts.Clustering.Clustered {
			// If clustered, assign a random cluster node ID if not provided.
			if s.opts.Clustering.NodeID == "" {
				s.opts.Clustering.NodeID = nuid.Next()
			}
			s.info.NodeID = s.opts.Clustering.NodeID
		}

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

	// If clustered, start Raft group.
	if s.isClustered {
		s.ssarepl = &subsSentAndAckReplication{
			ready:    &sync.Map{},
			waiting:  &sync.Map{},
			gates:    &sync.Map{},
			notifyCh: make(chan struct{}, 1),
		}
		s.wg.Add(1)
		go s.subsSentAndAckReplicator()
		// Default Raft log path to ./<cluster-id>/<node-id> if not set.
		if s.opts.Clustering.RaftLogPath == "" {
			s.opts.Clustering.RaftLogPath = filepath.Join(s.opts.ID, s.opts.Clustering.NodeID)
		}
		s.log.Noticef("Cluster Node ID : %s", s.info.NodeID)
		s.log.Noticef("Cluster Log Path: %s", s.opts.Clustering.RaftLogPath)
		if err := s.startRaftNode(recoveredState != nil); err != nil {
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
		// Do some post recovery processing setup some timers, etc...)
		s.postRecoveryProcessing(recoveredState.Clients, recoveredSubs)
	}

	// Flush to make sure all subscriptions are processed before
	// we return control to the user.
	if err := s.nc.Flush(); err != nil {
		return fmt.Errorf("could not flush the subscriptions, %v", err)
	}

	s.log.Noticef("Message store is %s", s.store.Name())
	if s.opts.FilestoreDir != "" {
		s.log.Noticef("Store location: %v", s.opts.FilestoreDir)
	}
	// The store has a copy of the limits and the inheritance
	// was not applied to our limits. To have them displayed correctly,
	// call Build() on them (we know that this is not going to fail,
	// otherwise we would not have been able to create the store).
	s.opts.StoreLimits.Build()
	storeLimitsLines := (&s.opts.StoreLimits).Print()
	for _, l := range storeLimitsLines {
		s.log.Noticef(l)
	}
	if !s.isClustered {
		s.log.Noticef(streamingReadyLog)
	}

	// Execute (in a go routine) redelivery of unacknowledged messages,
	// and release newOnHold. We only do this if not clustered. If
	// clustered, the leader will handle redelivery upon election.
	if !s.isClustered {
		s.wg.Add(1)
		go func() {
			s.performRedeliveryOnStartup(recoveredSubs)
			s.wg.Done()
		}()
	}
	return nil
}

// startRaftNode creates and starts the Raft group.
// This should only be called if the server is running in clustered mode.
func (s *StanServer) startRaftNode(hasStreamingState bool) error {
	if err := s.createServerRaftNode(hasStreamingState); err != nil {
		return err
	}
	node := s.raft

	leaderWait := make(chan struct{}, 1)
	leaderReady := func() {
		select {
		case leaderWait <- struct{}{}:
		default:
		}
	}
	if node.State() != raft.Leader {
		leaderReady()
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case isLeader := <-node.notifyCh:
				if isLeader {
					err := s.leadershipAcquired()
					leaderReady()
					if err != nil {
						s.log.Errorf("Error on leadership acquired: %v", err)
						switch {
						case err == raft.ErrRaftShutdown:
							// Node shutdown, just return.
							return
						case err == raft.ErrLeadershipLost:
						case err == raft.ErrNotLeader:
							// Node lost leadership, continue loop.
							continue
						default:
							// TODO: probably step down as leader?
							panic(err)
						}
					}
				} else {
					s.leadershipLost()
				}
			case <-s.shutdownCh:
				// Signal channel here to handle edge case where we might
				// otherwise block forever on the channel when shutdown.
				leaderReady()
				return
			}
		}
	}()

	<-leaderWait
	return nil
}

func (s *StanServer) sendSynchronziationRequest() (chan struct{}, chan struct{}) {
	sc := make(chan struct{}, 1)
	sdc := make(chan struct{})
	iopm := &ioPendingMsg{sc: sc, sdc: sdc}
	s.ioChannel <- iopm
	return sc, sdc
}

// leadershipAcquired should be called when this node is elected leader.
// This should only be called when the server is running in clustered mode.
func (s *StanServer) leadershipAcquired() error {
	s.log.Noticef("server became leader, performing leader promotion actions")
	defer func() {
		s.log.Noticef("finished leader promotion actions")
		s.log.Noticef(streamingReadyLog)
	}()

	// If we were not the leader, there should be nothing in the ioChannel
	// (processing of client publishes). However, since a node could go
	// from leader to follower to leader again, let's make sure that we
	// synchronize with the ioLoop before we touch the channels' nextSequence.
	sc, sdc := s.sendSynchronziationRequest()

	// Wait for the ioLoop to reach that special iopm and notifies us (or
	// give up if server is shutting down).
	select {
	case <-sc:
	case <-s.ioChannelQuit:
		close(sdc)
		return nil
	}
	// Then, we will notify it back to unlock it when were are done here.
	defer close(sdc)

	// Start listening to snapshot restore requests here...
	if err := s.subToSnapshotRestoreRequests(); err != nil {
		return err
	}

	// Use a barrier to ensure all preceding operations are applied to the FSM
	if err := s.raft.Barrier(0).Error(); err != nil {
		return err
	}

	channels := s.channels.getAll()
	for _, c := range channels {
		// Update next sequence to assign.
		_, lastSequence, err := s.getChannelFirstAndlLastSeq(c)
		if err != nil {
			return err
		}
		if c.nextSequence <= lastSequence {
			c.nextSequence = lastSequence + 1
		}
	}

	// Setup client heartbeats and subscribe to acks for each sub.
	for _, client := range s.clients.getClients() {
		client.RLock()
		cID := client.info.ID
		for _, sub := range client.subs {
			s.subChangesOnLeadershipAcquired(sub)
			if err := sub.startAckSub(s.nca, s.processAckMsg); err != nil {
				client.RUnlock()
				return err
			}
		}
		client.RUnlock()
		s.clients.setClientHB(cID, s.opts.ClientHBInterval, func() {
			s.checkClientHealth(cID)
		})
	}

	// Start the internal subscriptions so we receive protocols from clients.
	if err := s.initInternalSubs(true); err != nil {
		return err
	}

	var allSubs []*subState
	for _, c := range channels {
		subs := c.ss.getAllSubs()
		if len(subs) > 0 {
			allSubs = append(allSubs, subs...)
		}
		if c.activity != nil {
			s.channels.maybeStartChannelDeleteTimer(c.name, c)
		}
	}
	if len(allSubs) > 0 {
		s.startGoRoutine(func() {
			s.performRedeliveryOnStartup(allSubs)
			s.wg.Done()
		})
	}

	if err := s.nc.Flush(); err != nil {
		return err
	}
	if err := s.nca.Flush(); err != nil {
		return err
	}

	atomic.StoreInt64(&s.raft.leader, 1)
	return nil
}

// leadershipLost should be called when this node loses leadership.
// This should only be called when the server is running in clustered mode.
func (s *StanServer) leadershipLost() {
	s.log.Noticef("server lost leadership, performing leader stepdown actions")
	defer s.log.Noticef("finished leader stepdown actions")

	// Cancel outstanding client heartbeats. We aren't concerned about races
	// where new clients might be connecting because at this point, the server
	// will no longer accept new client connections, but even if it did, the
	// heartbeat would be automatically removed when it fires.
	for _, client := range s.clients.getClients() {
		s.clients.removeClientHB(client)
		// Ensure subs ackTimer is stopped
		subs := client.getSubsCopy()
		for _, sub := range subs {
			s.subChangesOnLeadershipLost(sub)
		}
	}

	// Unsubscribe to the snapshot request per channel since we are no longer
	// leader.
	for _, c := range s.channels.getAll() {
		if c.activity != nil {
			s.channels.stopDeleteTimer(c)
		}
	}

	// Only the leader will receive protocols from clients
	s.unsubscribeInternalSubs()

	atomic.StoreInt64(&s.raft.leader, 0)
}

// TODO:  Explore parameter passing in gnatsd.  Keep separate for now.
func (s *StanServer) configureClusterOpts() error {
	opts := s.natsOpts
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
func (s *StanServer) configureNATSServerTLS() error {
	opts := s.natsOpts
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

// startNATSServer starts the embedded NATS server, possibly updating
// the NATS Server's clustering and/or TLS options.
func (s *StanServer) startNATSServer() error {
	if err := s.configureClusterOpts(); err != nil {
		return err
	}
	if err := s.configureNATSServerTLS(); err != nil {
		return err
	}
	opts := s.natsOpts
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
	if !s.isClustered {
		s.clients.recoverClients(clients)
	}
}

// Reconstruct the subscription state on restart.
func (s *StanServer) processRecoveredChannels(channels map[string]*stores.RecoveredChannel) ([]*subState, error) {
	allSubs := make([]*subState, 0, 16)

	for channelName, recoveredChannel := range channels {
		channel, err := s.channels.create(s, channelName, recoveredChannel.Channel)
		if err != nil {
			return nil, err
		}
		if !s.isClustered {
			ss := channel.ss
			ss.Lock()
			// Get the recovered subscriptions for this channel.
			for _, recSub := range recoveredChannel.Subscriptions {
				sub := s.recoverOneSub(channel, recSub.Sub, recSub.Pending, nil)
				if sub != nil {
					// Subscribe to subscription ACKs
					if err := sub.startAckSub(s.nca, s.processAckMsg); err != nil {
						ss.Unlock()
						return nil, err
					}
					allSubs = append(allSubs, sub)
				}
			}
			ss.Unlock()
			// Now that we have recovered possible subscriptions for this channel,
			// check if we should start the delete timer.
			if channel.activity != nil {
				s.channels.maybeStartChannelDeleteTimer(channelName, channel)
			}
		}
	}
	return allSubs, nil
}

func (s *StanServer) recoverOneSub(c *channel, recSub *spb.SubState, pendingAcksAsMap map[uint64]struct{},
	pendingAcksAsArray []uint64) *subState {

	// map, but nowhere else.
	processOfflineSub := func(c *channel, sub *subState) {
		c.ss.durables[sub.durableKey()] = sub
		// Now that the key is computed, clear ClientID otherwise
		// durable would not be able to be restarted.
		sub.savedClientID = sub.ClientID
		sub.ClientID = ""
	}

	// Create a subState
	sub := &subState{
		SubState: *recSub,
		subject:  c.name,
		ackWait:  computeAckWait(recSub.AckWaitInSecs),
		store:    c.store.Subs,
	}
	// Depending from where this function is called, we are given
	// a map[uint64]struct{} or a []uint64.
	if len(pendingAcksAsMap) != 0 {
		sub.acksPending = make(map[uint64]int64, len(pendingAcksAsMap))
		for seq := range pendingAcksAsMap {
			sub.acksPending[seq] = 0
		}
	} else {
		sub.acksPending = make(map[uint64]int64, len(pendingAcksAsArray))
		for _, seq := range pendingAcksAsArray {
			sub.acksPending[seq] = 0
		}
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
			processOfflineSub(c, sub)
			return nil
		}
	}
	// Add the subscription to the corresponding client
	added := s.clients.addSub(sub.ClientID, sub)
	if added || sub.IsDurable {
		// Repair for issue https://github.com/nats-io/nats-streaming-server/issues/215
		// Do not recover a queue durable subscriber that still
		// has ClientID but for which connection was closed (=>!added)
		if !added && sub.isQueueDurableSubscriber() && !sub.isShadowQueueDurable() {
			s.log.Warnf("Not recovering ghost durable queue subscriber: [%s]:[%s] subject=%s inbox=%s", sub.ClientID, sub.QGroup, sub.subject, sub.Inbox)
			c.store.Subs.DeleteSub(sub.ID)
			return nil
		}
		// Fix for older offline durable subscribers. Newer offline durable
		// subscribers have IsClosed set to true and therefore are handled aboved.
		if durableSub && !added {
			processOfflineSub(c, sub)
		} else {
			// Add this subscription to subStore.
			c.ss.updateState(sub)
			// Add to the array, unless this is the shadow durable queue sub that
			// was left in the store in order to maintain the group's state.
			if !sub.isShadowQueueDurable() {
				return sub
			}
		}
	}
	return nil
}

// Do some final setup. Be minded of locking here since the server
// has started communication with NATS server/clients.
func (s *StanServer) postRecoveryProcessing(recoveredClients []*stores.Client, recoveredSubs []*subState) {
	for _, sub := range recoveredSubs {
		sub.Lock()
		// Consider this subscription initialized. Note that it may
		// still have newOnHold == true, which would prevent incoming
		// messages to be delivered before we attempt to redeliver
		// unacknowledged messages in performRedeliveryOnStartup.
		sub.initialized = true
		sub.Unlock()
	}
	// Go through the list of clients and ensure their Hb timer is set. Only do
	// this for standalone mode. If clustered, timers will be setup on leader
	// election.
	if !s.isClustered {
		for _, sc := range recoveredClients {
			// Because of the loop, we need to make copy for the closure
			cID := sc.ID
			s.clients.setClientHB(cID, s.opts.ClientHBInterval, func() {
				s.checkClientHealth(cID)
			})
		}
	}
}

// Redelivers unacknowledged messages, releases the hold for new messages delivery,
// and kicks delivery of available messages.
func (s *StanServer) performRedeliveryOnStartup(recoveredSubs []*subState) {
	queues := make(map[*queueState]*channel)

	for _, sub := range recoveredSubs {
		// Ignore subs that did not have any ack pendings on startup.
		sub.Lock()
		// Consider this subscription ready to receive messages
		sub.initialized = true
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
		// Reset redelivery count map (this is to be consistent that if
		// in cluster mode a node regains leadership the count restarts
		// at 1. Otherwise, you could get something like this:
		// node A is leader: 1, 2, 3 - then loses leadership (but does not exit)
		// node B is leader: 1, 2, 3, 4, 5, 6 - then loses leadership
		// node A is leader: 4, 5, 6, ...
		qs.rdlvCount = nil
		// This is required in cluster mode if a node was leader,
		// lost it and then becomes leader again, all that without
		// restoring from snapshot.
		qs.stalledSubCount = 0
		for _, sub := range qs.subs {
			sub.RLock()
			if sub.stalled {
				qs.stalledSubCount++
			}
			sub.RUnlock()
		}
		qs.Unlock()
		s.sendAvailableMessagesToQueue(c, qs)
	}
}

// initSubscriptions will setup initial subscriptions for discovery etc.
func (s *StanServer) initSubscriptions() error {

	// Do not create internal subscriptions in clustered mode,
	// the leader will when it gets elected.
	if !s.isClustered {
		createSubOnClientPublish := true

		if s.partitions != nil {
			// Receive published messages from clients, but only on the list
			// of static channels.
			if err := s.partitions.initSubscriptions(); err != nil {
				return err
			}
			// Since we create a subscription per channel, do not create
			// the internal subscription on the > wildcard
			createSubOnClientPublish = false
		}

		if err := s.initInternalSubs(createSubOnClientPublish); err != nil {
			return err
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

func (s *StanServer) initInternalSubs(createPub bool) error {
	var err error
	// Listen for connection requests.
	s.connectSub, err = s.createSub(s.info.Discovery, s.connectCB, "discover")
	if err != nil {
		return err
	}
	if createPub {
		// Receive published messages from clients.
		pubSubject := fmt.Sprintf("%s.>", s.info.Publish)
		s.pubSub, err = s.createSub(pubSubject, s.processClientPublish, "publish")
		if err != nil {
			return err
		}
		s.pubSub.SetPendingLimits(-1, -1)
	}
	// Receive subscription requests from clients.
	s.subSub, err = s.createSub(s.info.Subscribe, s.processSubscriptionRequest, "subscribe request")
	if err != nil {
		return err
	}
	// Receive unsubscribe requests from clients.
	s.subUnsubSub, err = s.createSub(s.info.Unsubscribe, s.processUnsubscribeRequest, "subscription unsubscribe")
	if err != nil {
		return err
	}
	// Receive subscription close requests from clients.
	s.subCloseSub, err = s.createSub(s.info.SubClose, s.processSubCloseRequest, "subscription close request")
	if err != nil {
		return err
	}
	// Receive close requests from clients.
	s.closeSub, err = s.createSub(s.info.Close, s.processCloseRequest, "close request")
	if err != nil {
		return err
	}
	// Receive PINGs from clients.
	s.cliPingSub, err = s.createSub(s.info.Discovery+".pings", s.processClientPings, "client pings")
	return err
}

func (s *StanServer) unsubscribeInternalSubs() {
	if s.connectSub != nil {
		s.connectSub.Unsubscribe()
		s.connectSub = nil
	}
	if s.closeSub != nil {
		s.closeSub.Unsubscribe()
		s.closeSub = nil
	}
	if s.subSub != nil {
		s.subSub.Unsubscribe()
		s.subSub = nil
	}
	if s.subCloseSub != nil {
		s.subCloseSub.Unsubscribe()
		s.subCloseSub = nil
	}
	if s.subUnsubSub != nil {
		s.subUnsubSub.Unsubscribe()
		s.subUnsubSub = nil
	}
	if s.pubSub != nil {
		s.pubSub.Unsubscribe()
		s.pubSub = nil
	}
	if s.cliPingSub != nil {
		s.cliPingSub.Unsubscribe()
		s.cliPingSub = nil
	}
	if s.snapReqSub != nil {
		s.snapReqSub.Unsubscribe()
		s.snapReqSub = nil
	}
}

func (s *StanServer) createSub(subj string, f nats.MsgHandler, errTxt string) (*nats.Subscription, error) {
	sub, err := s.nc.Subscribe(subj, f)
	if err != nil {
		return nil, fmt.Errorf("could not subscribe to %s subject: %v", errTxt, err)
	}
	return sub, nil
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

	// If the client ID is already registered, check to see if it's the case
	// that the client refreshed (e.g. it crashed and came back) or if the
	// connection is a duplicate. If it refreshed, we will close the old
	// client and open a new one.
	client := s.clients.lookup(req.ClientID)
	if client != nil {
		// When detecting a duplicate, the processing of the connect request
		// is going to be processed in a go-routine. We need however to keep
		// track and fail another request on the same client ID until the
		// current one has finished.
		s.cliDupCIDsMu.Lock()
		if _, exists := s.cliDipCIDsMap[req.ClientID]; exists {
			s.cliDupCIDsMu.Unlock()
			s.log.Debugf("[Client:%s] Connect failed; already connected", req.ClientID)
			s.sendConnectErr(m.Reply, ErrInvalidClient.Error())
			return
		}
		s.cliDipCIDsMap[req.ClientID] = struct{}{}
		s.cliDupCIDsMu.Unlock()

		s.startGoRoutine(func() {
			defer s.wg.Done()
			isDup := false
			if s.isDuplicateConnect(client) {
				s.log.Debugf("[Client:%s] Connect failed; already connected", req.ClientID)
				s.sendConnectErr(m.Reply, ErrInvalidClient.Error())
				isDup = true
			}
			s.cliDupCIDsMu.Lock()
			if !isDup {
				s.handleConnect(req, m, true)
			}
			delete(s.cliDipCIDsMap, req.ClientID)
			s.cliDupCIDsMu.Unlock()
		})
		return
	}
	s.cliDupCIDsMu.Lock()
	s.handleConnect(req, m, false)
	s.cliDupCIDsMu.Unlock()
}

func (s *StanServer) handleConnect(req *pb.ConnectRequest, m *nats.Msg, replaceOld bool) {
	var err error

	// If clustered, thread operations through Raft.
	if s.isClustered {
		err = s.replicateConnect(req, replaceOld)
	} else {
		err = s.processConnect(req, replaceOld)
	}

	if err != nil {
		// Error has already been logged.
		s.sendConnectErr(m.Reply, err.Error())
		return
	}

	// Send connect response to client and start heartbeat timer.
	s.finishConnectRequest(req, m.Reply)
}

// isDuplicateConnect determines if the given client ID is a duplicate
// connection by pinging the old client's heartbeat inbox and checking if it
// responds. If it does, it's a duplicate connection.
func (s *StanServer) isDuplicateConnect(client *client) bool {
	client.RLock()
	hbInbox := client.info.HbInbox
	client.RUnlock()

	// This is the HbInbox from the "old" client. See if it is up and
	// running by sending a ping to that inbox.
	_, err := s.nc.Request(hbInbox, nil, s.dupCIDTimeout)

	// If err is nil, the currently registered client responded, so this is a
	// duplicate.
	return err == nil
}

// When calling ApplyFuture.Error(), if we get an error it means
// that raft failed to commit to its log.
// But if we also want the result of FSM.Apply(), which in this
// case is StanServer.Apply(), we need to check the Response().
// So we first check error from future.Error(). If nil, then we
// check the Response.
func waitForReplicationErrResponse(f raft.ApplyFuture) error {
	err := f.Error()
	if err == nil {
		resp := f.Response()
		// We call this function when we know that FutureApply's
		// Response is an error object.
		if resp != nil {
			err = f.Response().(error)
		}
	}
	return err
}

// Leader invokes this to replicate the command to delete a channel.
func (s *StanServer) replicateDeleteChannel(c *channel) {
	op := &spb.RaftOperation{
		OpType:    spb.RaftOperation_DeleteChannel,
		Channel:   c.name,
		ChannelID: c.id,
	}
	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}
	// Wait on result of replication.
	if err = s.raft.Apply(data, 0).Error(); err != nil {
		// If we have lost leadership, clear the deleteInProgress flag.
		cs := s.channels
		cs.Lock()
		if c.activity != nil {
			c.activity.deleteInProgress = false
		}
		cs.Unlock()
	}
}

// Check if the channel can be deleted. If so, do it in place.
// This is called from the ioLoop by the leader or a standlone server.
func (s *StanServer) handleChannelDelete(c *channel) {
	delete := false
	cs := s.channels
	cs.Lock()
	a := c.activity
	if a.preventDelete || a.deleteInProgress || c.ss.hasActiveSubs() {
		if s.debug {
			s.log.Debugf("Channel %q cannot be deleted: preventDelete=%v inProgress=%v hasActiveSubs=%v",
				c.name, a.preventDelete, a.deleteInProgress, c.ss.hasActiveSubs())
		}
		c.stopDeleteTimer()
	} else {
		elapsed := time.Since(a.last)
		if elapsed >= a.maxInactivity {
			if s.debug {
				s.log.Debugf("Channel %q is being deleted", c.name)
			}
			c.stopDeleteTimer()
			// Leave in map for now, but mark as deleted. If we removed before
			// completion of the removal, a new lookup could re-create while
			// in the process of deleting it.
			a.deleteInProgress = true
			delete = true
		} else {
			var next time.Duration
			if elapsed < 0 {
				next = a.maxInactivity
			} else {
				// elapsed < a.maxInactivity
				next = a.maxInactivity - elapsed
			}
			if s.debug {
				s.log.Debugf("Channel %q cannot be deleted now, reset timer to fire in %v",
					c.name, next)
			}
			c.resetDeleteTimer(next)
		}
	}
	cs.Unlock()
	if delete {
		if testDeleteChannel {
			time.Sleep(time.Second)
		}
		if s.isClustered {
			s.replicateDeleteChannel(c)
		} else {
			s.processDeleteChannel(c.name)
		}
	}
}

// Actual deletetion of the channel.
func (s *StanServer) processDeleteChannel(channel string) {
	cs := s.channels
	cs.Lock()
	defer cs.Unlock()
	c := cs.channels[channel]
	if c == nil {
		s.log.Errorf("Error deleting channel %q: not found", channel)
		return
	}
	if c.activity != nil && c.activity.preventDelete {
		s.log.Errorf("The channel %q cannot be deleted at this time since a subscription has been created", channel)
		return
	}
	// Delete from store
	err := cs.store.DeleteChannel(channel)
	if err == nil && s.isClustered {
		// In cluster mode, delete the ID from the RAFT store
		err = s.raft.store.DeleteChannelID(channel)
	}
	if err != nil {
		s.log.Errorf("Error deleting channel %q: %v", channel, err)
		if s.isStandaloneOrLeader() && c.activity != nil {
			c.activity.deleteInProgress = false
			c.startDeleteTimer()
		}
		return
	}
	delete(s.channels.channels, channel)
	s.log.Noticef("Channel %q has been deleted", channel)
}

func (s *StanServer) replicateConnect(req *pb.ConnectRequest, refresh bool) error {
	op := &spb.RaftOperation{
		OpType: spb.RaftOperation_Connect,
		ClientConnect: &spb.AddClient{
			Request: req,
			Refresh: refresh,
		},
	}
	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}
	// Wait on result of replication.
	return waitForReplicationErrResponse(s.raft.Apply(data, 0))
}

func (s *StanServer) processConnect(req *pb.ConnectRequest, replaceOld bool) error {
	// If the client restarted, close the old one first.
	if replaceOld {
		s.closeClient(req.ClientID)

		// Because connections are processed under a common lock, it is not
		// possible for a connection request for the same client ID to come in at
		// the same time after unregistering the old client.
	}

	// Try to register
	info := &spb.ClientInfo{
		ID:           req.ClientID,
		HbInbox:      req.HeartbeatInbox,
		ConnID:       req.ConnID,
		Protocol:     req.Protocol,
		PingInterval: req.PingInterval,
		PingMaxOut:   req.PingMaxOut,
	}
	_, err := s.clients.register(info)
	if err != nil {
		s.log.Errorf("[Client:%s] Error registering client: %v", req.ClientID, err)
		return err
	}

	if replaceOld {
		s.log.Debugf("[Client:%s] Replaced old client (Inbox=%v)", req.ClientID, req.HeartbeatInbox)
	} else {
		s.log.Debugf("[Client:%s] Connected (Inbox=%v)", req.ClientID, req.HeartbeatInbox)
	}
	return nil
}

func (s *StanServer) finishConnectRequest(req *pb.ConnectRequest, replyInbox string) {
	clientID := req.ClientID
	// Heartbeat timer.
	s.clients.setClientHB(clientID, s.opts.ClientHBInterval, func() { s.checkClientHealth(clientID) })

	cr := &pb.ConnectResponse{
		PubPrefix:        s.info.Publish,
		SubRequests:      s.info.Subscribe,
		UnsubRequests:    s.info.Unsubscribe,
		SubCloseRequests: s.info.SubClose,
		CloseRequests:    s.info.Close,
		Protocol:         protocolOne,
	}
	// We could set those unconditionally since even with
	// older clients, the protobuf Unmarshal would simply not
	// decode them.
	if req.Protocol >= protocolOne {
		cr.PingRequests = s.info.Discovery + ".pings"
		// In the future, we may want to return different values
		// than the one the client sent in the connect request.
		// For now, return the values from the request.
		// Note: Server and clients have possibly different HBs values.
		cr.PingInterval = req.PingInterval
		cr.PingMaxOut = req.PingMaxOut
	}
	b, _ := cr.Marshal()
	s.nc.Publish(replyInbox, b)
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

	// If clustered and we lost leadership, we should stop
	// heartbeating as the new leader will take over.
	if s.isClustered && !s.isLeader() {
		// Do not remove client HB here. We do that in
		// leadershipLost. We could be here because the
		// callback fired while we are not yet finished
		// acquiring leadership.
		client.Lock()
		if client.hbt != nil {
			client.hbt.Reset(s.opts.ClientHBInterval)
		}
		client.Unlock()
		return
	}

	client.RLock()
	hbInbox := client.info.HbInbox
	client.RUnlock()

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
	hadFailed := client.fhb > 0
	// If we did not get the reply, increase the number of
	// failed heartbeats.
	if err != nil {
		client.fhb++
		// If we have reached the max number of failures
		if client.fhb > s.opts.ClientHBFailCount {
			s.log.Errorf("[Client:%s] Timed out on heartbeats", clientID)
			// close the client (connection). This locks the
			// client object internally so unlock here.
			client.Unlock()
			s.barrier(func() {
				// If clustered, thread operations through Raft.
				if s.isClustered {
					if err := s.replicateConnClose(&pb.CloseRequest{ClientID: clientID}); err != nil {
						s.log.Errorf("[Client:%s] Failed to replicate disconnect on heartbeat expiration: %v",
							clientID, err)
					}
				} else {
					s.closeClient(clientID)
				}
			})
			return
		}
	} else {
		// We got the reply, reset the number of failed heartbeats.
		client.fhb = 0
	}
	// Reset the timer to fire again.
	client.hbt.Reset(s.opts.ClientHBInterval)
	var (
		subs        []*subState
		hasFailedHB = client.fhb > 0
	)
	if (hadFailed && !hasFailedHB) || (!hadFailed && hasFailedHB) {
		// Get a copy of subscribers and client.fhb while under lock
		subs = client.getSubsCopy()
	}
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
func (s *StanServer) closeClient(clientID string) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	// Lookup client first, will unregister only after removing its subscriptions
	client := s.clients.lookup(clientID)
	if client == nil {
		s.log.Errorf("Unknown client %q in close request", clientID)
		return ErrUnknownClient
	}

	// Remove all non-durable subscribers.
	s.removeAllNonDurableSubscribers(client)

	// Remove from our clientStore.
	if _, err := s.clients.unregister(clientID); err != nil {
		s.log.Errorf("Error unregistering client %q: %v", clientID, err)
	}

	if s.debug {
		client.RLock()
		hbInbox := client.info.HbInbox
		client.RUnlock()
		s.log.Debugf("[Client:%s] Closed (Inbox=%v)", clientID, hbInbox)
	}
	return nil
}

// processCloseRequest will process connection close requests from clients.
func (s *StanServer) processCloseRequest(m *nats.Msg) {
	req := &pb.CloseRequest{}
	err := req.Unmarshal(m.Data)
	if err != nil {
		s.log.Errorf("Received invalid close request, subject=%s", m.Subject)
		s.sendCloseResponse(m.Reply, ErrInvalidCloseReq)
		return
	}

	s.barrier(func() {
		var err error
		// If clustered, thread operations through Raft.
		if s.isClustered {
			err = s.replicateConnClose(req)
		} else {
			err = s.closeClient(req.ClientID)
		}
		// If there was an error, it has been already logged.

		// Send response, if err is nil, will be a success response.
		s.sendCloseResponse(m.Reply, err)
	})
}

func (s *StanServer) replicateConnClose(req *pb.CloseRequest) error {
	// Go through the list of subscriptions and possibly
	// flush the pending replication of sent/ack.
	subs := s.clients.getSubs(req.ClientID)
	for _, sub := range subs {
		s.endSubSentAndAckReplication(sub, false)
	}

	op := &spb.RaftOperation{
		OpType:           spb.RaftOperation_Disconnect,
		ClientDisconnect: req,
	}
	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}
	// Wait on result of replication.
	return waitForReplicationErrResponse(s.raft.Apply(data, 0))
}

func (s *StanServer) sendCloseResponse(subj string, closeErr error) {
	resp := &pb.CloseResponse{}
	if closeErr != nil {
		resp.Error = closeErr.Error()
	}
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
	atomic.AddInt64(&s.stats.inMsgs, 1)
	atomic.AddInt64(&s.stats.inBytes, int64(len(m.Data)))

	// Make sure we have a guid and valid channel name.
	if pm.Guid == "" || !util.IsChannelNameValid(pm.Subject, false) {
		s.log.Errorf("Received invalid client publish message %v", pm)
		s.sendPublishErr(m.Reply, pm.Guid, ErrInvalidPubReq)
		return
	}

	if s.debug {
		s.log.Tracef("[Client:%s] Received message from publisher subj=%s guid=%s", pm.ClientID, pm.Subject, pm.Guid)
	}

	// Check if the client is valid. We do this after the clustered check so
	// that only the leader performs this check.
	valid := false
	if s.partitions != nil {
		// In partitioning mode it is possible that we get there
		// before the connect request is processed. If so, make sure we wait
		// for conn request	to be processed first. Check clientCheckTimeout
		// doc for details.
		valid = s.clients.isValidWithTimeout(pm.ClientID, pm.ConnID, clientCheckTimeout)
	} else {
		valid = s.clients.isValid(pm.ClientID, pm.ConnID)
	}
	if !valid {
		s.log.Errorf("Received invalid client publish message %v", pm)
		s.sendPublishErr(m.Reply, pm.Guid, ErrInvalidPubReq)
		return
	}

	s.ioChannel <- iopm
}

// processClientPings receives a PING from a client. The payload is the client's UID.
// If the client is present, a response with nil payload is sent back to indicate
// success, otherwise the payload contains an error message.
func (s *StanServer) processClientPings(m *nats.Msg) {
	if len(m.Data) == 0 {
		return
	}
	ping := &pb.Ping{}
	if err := ping.Unmarshal(m.Data); err != nil {
		return
	}
	var reply []byte
	client := s.clients.lookupByConnID(ping.ConnID)
	if client != nil {
		// If the client has failed heartbeats and since the
		// server just received a PING from the client, reset
		// the server-to-client HB timer so that a PING is
		// sent soon and the client's subscriptions failedHB
		// is cleared.
		client.RLock()
		hasFailedHBs := client.fhb > 0
		client.RUnlock()
		if hasFailedHBs {
			client.Lock()
			client.hbt.Reset(time.Millisecond)
			client.Unlock()
		}
		if s.pingResponseOKBytes == nil {
			s.pingResponseOKBytes, _ = (&pb.PingResponse{}).Marshal()
		}
		reply = s.pingResponseOKBytes
	} else {
		if s.pingResponseInvalidClientBytes == nil {
			pingError := &pb.PingResponse{
				Error: "client has been replaced or is no longer registered",
			}
			s.pingResponseInvalidClientBytes, _ = pingError.Marshal()
		}
		reply = s.pingResponseInvalidClientBytes
	}
	s.ncs.Publish(m.Reply, reply)
}

// CtrlMsg are no longer used to solve connection and subscription close/unsub
// ordering issues. However, a (newer) server may still receive those from
// older servers in the same NATS cluster.
// Since original behavior was to ignore control messages sent from a server
// other than itself, and since new server do not send those (in this context
// at least), this function simply make sure that if it is a properly formed
// CtrlMsg, we just ignore.
func (s *StanServer) processCtrlMsg(m *nats.Msg) bool {
	cm := &spb.CtrlMsg{}
	// Since we don't use CtrlMsg for connection/subscription close/unsub,
	// simply return true if CtrlMsg is valid so that this message is ignored.
	return cm.Unmarshal(m.Data) == nil
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
func (s *StanServer) sendMsgToQueueGroup(qs *queueState, m *pb.MsgProto, force bool) (*subState, bool) {
	sub := findBestQueueSub(qs.subs)
	if sub == nil {
		return nil, false
	}
	sub.Lock()
	wasStalled := sub.stalled
	didSend, _ := s.sendMsgToSub(sub, m, force)
	// If this is not a redelivery and the sub was not stalled, but now is,
	// bump the number of stalled members.
	if !force && !wasStalled && sub.stalled {
		qs.stalledSubCount++
	}
	if didSend && sub.LastSent > qs.lastSent {
		qs.lastSent = sub.LastSent
	}
	sub.Unlock()
	return sub, didSend
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
func (sub *subState) makeSortedSequences() []uint64 {
	results := make([]uint64, 0, len(sub.acksPending))
	for seq := range sub.acksPending {
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
	// be set later, or if expiration is identical, then
	// order by sequence instead.
	if a[i].expire == 0 || a[j].expire == 0 || a[i].expire == a[j].expire {
		return a[i].seq < a[j].seq
	}
	return a[i].expire < a[j].expire
}

// Returns an array of pendingMsg ordered by expiration date, unless
// the expiration date in the pendingMsgs map is not set (0), which
// happens after a server restart. In this case, the array is ordered
// by message sequence numbers.
func (sub *subState) makeSortedPendingMsgs() []*pendingMsg {
	results := make([]*pendingMsg, 0, len(sub.acksPending))
	for seq, expire := range sub.acksPending {
		results = append(results, &pendingMsg{seq: seq, expire: expire})
	}
	sort.Sort(byExpire(results))
	return results
}

func qsLock(qs *queueState) {
	if qs != nil {
		qs.Lock()
	}
}

func qsUnlock(qs *queueState) {
	if qs != nil {
		qs.Unlock()
	}
}

// Redeliver all outstanding messages to a durable subscriber, used on resubscribe.
func (s *StanServer) performDurableRedelivery(c *channel, sub *subState) {
	// Sort our messages outstanding from acksPending, grab some state and unlock.
	sub.RLock()
	sortedSeqs := sub.makeSortedSequences()
	clientID := sub.ClientID
	newOnHold := sub.newOnHold
	subID := sub.ID
	qs := sub.qstate
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
	if s.clients.lookup(clientID) != nil {
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

			qsLock(qs)
			sub.Lock()
			// Force delivery
			s.sendMsgToSub(sub, m, forceDelivery)
			sub.Unlock()
			qsUnlock(qs)
		}
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
	// If server has been shutdown, clear timer and be done (will happen only in tests)
	s.mu.RLock()
	shutdown := s.shutdown
	s.mu.RUnlock()
	if shutdown {
		sub.Lock()
		sub.clearAckTimer()
		sub.Unlock()
		return
	}
	// Sort our messages outstanding from acksPending, grab some state and unlock.
	sub.Lock()
	sortedPendingMsgs := sub.makeSortedPendingMsgs()
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
	if c == nil {
		s.log.Errorf("[Client:%s] Aborting redelivery to subid=%d for non existing channel %s", clientID, subID, subject)
		sub.Lock()
		sub.clearAckTimer()
		sub.Unlock()
		return
	}

	// In cluster mode we will always redeliver to the same queue member.
	// This is to avoid to have to replicated sent/ack when a message would
	// be redelivered (removed from one member to be sent to another member)
	isClustered := s.isClustered

	now := time.Now().UnixNano()
	// limit is now plus a buffer of 15ms to avoid repeated timer callbacks.
	limit := now + int64(15*time.Millisecond)

	var (
		pick               *subState
		sent               bool
		foundWithZero      bool
		nextExpirationTime int64
	)

	// We will move through acksPending(sorted) and see what needs redelivery.
	for _, pm := range sortedPendingMsgs {
		m := s.getMsgForRedelivery(c, sub, pm.seq)
		if m == nil {
			continue
		}
		// If we found any pm.expire with 0 in the array (due to a server restart),
		// ensure that all have now an expiration set, then reschedule right away.
		if foundWithZero || pm.expire == 0 {
			foundWithZero = true
			if pm.expire == 0 {
				sub.Lock()
				// Is message still pending?
				if _, present := sub.acksPending[pm.seq]; present {
					sub.acksPending[pm.seq] = m.Timestamp + expTime
				}
				sub.Unlock()
			}
			continue
		}

		// If this message has not yet expired, reset timer for next callback
		if pm.expire > limit {
			nextExpirationTime = pm.expire
			if s.trace {
				s.log.Tracef("[Client:%s] Redelivery for subid=%d, skipping seq=%d", clientID, subID, m.Sequence)
			}
			break
		}

		// Flag as redelivered.
		m.Redelivered = true

		// Handle QueueSubscribers differently, since we will choose best subscriber
		// to redeliver to, not necessarily the same one.
		// However, on startup, resends only to member that had previously this message
		// otherwise this could cause a message to be redelivered to multiple members.
		if !isClustered && qs != nil && !isStartup {
			qs.Lock()
			pick, sent = s.sendMsgToQueueGroup(qs, m, forceDelivery)
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
				s.processAck(c, sub, m.Sequence, false)
			}
		} else {
			qsLock(qs)
			sub.Lock()
			s.sendMsgToSub(sub, m, forceDelivery)
			sub.Unlock()
			qsUnlock(qs)
		}
	}
	if foundWithZero {
		// Restart expiration now that ackPending map's expire values are properly
		// set. Note that messages may have been added/removed in the meantime.
		s.performAckExpirationRedelivery(sub, isStartup)
		return
	}

	// Adjust the timer
	sub.adjustAckTimer(nextExpirationTime)
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
		s.processAck(c, sub, seq, false)
		return nil
	}
	// The store implementation does not return a copy, we need one
	mcopy := *m
	return &mcopy
}

// Little helper to signal a "chan struct{}" without risk of blocking.
func signalCh(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func (s *StanServer) subChangesOnLeadershipAcquired(sub *subState) {
	sub.Lock()
	sub.norepl = false
	sub.Unlock()
}

func (s *StanServer) subChangesOnLeadershipLost(sub *subState) {
	sub.Lock()
	sub.rdlvCount = nil
	sub.stopAckSub()
	sub.clearAckTimer()
	s.clearSentAndAck(sub)
	sub.Unlock()
}

// Keep track of sent or ack messages.
// If the number of operations reach a certain threshold,
// the sub is added to list of subs that should be flushed asap.
// This call does not do actual RAFT replication and should not block.
// Caller holds the sub's Lock.
func (s *StanServer) collectSentOrAck(sub *subState, sent bool, sequence uint64) {
	if sub.norepl {
		return
	}
	sr := s.ssarepl
	if sub.replicate == nil {
		sub.replicate = &subSentAndAck{
			sent: make([]uint64, 0, 100),
			ack:  make([]uint64, 0, 100),
		}
	}
	r := sub.replicate
	if sent {
		r.sent = append(r.sent, sequence)
	} else {
		r.ack = append(r.ack, sequence)
	}
	// This function is called with exactly one event at a time.
	// Use exact count to decide when to add to given map. This
	// avoid the need for booleans to not add more than once.
	l := len(r.sent) + len(r.ack)
	if l == 1 {
		sr.waiting.Store(sub, struct{}{})
	} else if l == 100 {
		sr.waiting.Delete(sub)
		sr.ready.Store(sub, struct{}{})
		signalCh(sr.notifyCh)
	}
}

// Replicates through RAFT
func (s *StanServer) replicateSubSentAndAck(sub *subState) {
	var data []byte

	sr := s.ssarepl
	sub.Lock()
	r := sub.replicate
	if r != nil && len(r.sent)+len(r.ack) > 0 {
		data = createSubSentAndAckProto(sub, r)
		r.sent = r.sent[:0]
		r.ack = r.ack[:0]
		r.applying = true
	}
	sub.Unlock()

	if data != nil {
		if testSubSentAndAckSlowApply {
			time.Sleep(100 * time.Millisecond)
		}
		s.raft.Apply(data, 0)

		sub.Lock()
		r = sub.replicate
		// If r is nil it means either that the leader lost leadrship,
		// in which case we don't do anything, or the sub/conn is being
		// closed and endSubSentAndAckReplication() is waiting on a
		// channel stored in "gates" map. If we find it, signal.
		if r == nil {
			if c, ok := sr.gates.Load(sub); ok {
				sr.gates.Delete(sub)
				signalCh(c.(chan struct{}))
			}
		} else {
			r.applying = false
		}
		sub.Unlock()
	}
}

// Little helper function to create a RaftOperation_SendAndAck protocol
// and serialize it.
func createSubSentAndAckProto(sub *subState, r *subSentAndAck) []byte {
	op := &spb.RaftOperation{
		OpType: spb.RaftOperation_SendAndAck,
		SubSentAck: &spb.SubSentAndAck{
			Channel:  sub.subject,
			AckInbox: sub.AckInbox,
			Sent:     r.sent,
			Ack:      r.ack,
		},
	}
	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

// This is called when a subscription is closed or unsubscribed, or
// a connection is closed, but prior to the RAFT replication of such
// event.
// Depending on the type of event, we want to make sure that we flush
// the possibly remaning sent/ack events.
func (s *StanServer) endSubSentAndAckReplication(sub *subState, unsub bool) {
	var ch chan struct{}
	var data []byte
	var subID uint64
	var inbox string

	sub.Lock()
	r := sub.replicate
	if r == nil {
		sub.Unlock()
		return
	}
	if !unsub && (sub.IsDurable || sub.qstate != nil) && len(r.sent)+len(r.ack) > 0 {
		data = createSubSentAndAckProto(sub, r)
	}
	// If the replicator is about to apply, or in middle of it, we
	// want to wait for it to finish regardless if we have to replicate
	// something or not. We are not expecting this situation to occur often.
	if r.applying {
		ch = make(chan struct{}, 1)
		s.ssarepl.gates.Store(sub, ch)
		subID = sub.ID
		inbox = sub.Inbox
	}
	s.clearSentAndAck(sub)
	sub.Unlock()

	if ch != nil {
		select {
		case <-ch:
		case <-s.shutdownCh:
			return
		case <-time.After(5 * time.Second):
			s.log.Errorf("Timeout waiting for subscription's sent/ack to be replicated - subid=%d inbox=%s", subID, inbox)
			return
		}
	}
	if data != nil {
		s.raft.Apply(data, 0)
	}
}

// Sub lock is held on entry
func (s *StanServer) clearSentAndAck(sub *subState) {
	sr := s.ssarepl
	sr.waiting.Delete(sub)
	sr.ready.Delete(sub)
	sub.replicate = nil
	sub.norepl = true
}

// long-lived go-routine that performs RAFT replication of subscriptions'
// sent/ack operations.
func (s *StanServer) subsSentAndAckReplicator() {
	defer s.wg.Done()

	s.mu.Lock()
	sr := s.ssarepl
	ready := sr.ready
	waiting := sr.waiting
	notifyCh := sr.notifyCh
	s.mu.Unlock()

	ticker := time.NewTicker(lazyReplicationInterval)

	flush := func() {
		ready.Range(func(k, _ interface{}) bool {
			ready.Delete(k)
			s.replicateSubSentAndAck(k.(*subState))
			return true
		})
	}

	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			addedToReady := false
			waiting.Range(func(k, _ interface{}) bool {
				waiting.Delete(k)
				// Move to ready map
				ready.Store(k, struct{}{})
				addedToReady = true
				return true
			})
			// If some were transferred and nobody has signaled
			// to flush the ready ones, do it here
			if addedToReady && len(notifyCh) == 0 {
				flush()
			}
		case <-notifyCh:
			flush()
		}
	}
}

// This is invoked from raft thread on a follower. It persists given
// sequence number to subscription of given AckInbox. It updates the
// sub (and queue state) LastSent value. It adds the sequence to the
// map of acksPending.
func (s *StanServer) processReplicatedSendAndAck(ssa *spb.SubSentAndAck) {
	c, err := s.lookupOrCreateChannel(ssa.Channel)
	if err != nil {
		return
	}
	sub := c.ss.LookupByAckInbox(ssa.AckInbox)
	if sub == nil {
		return
	}
	sub.Lock()
	defer sub.Unlock()

	// This is not optimized. The leader sent all accumulated sent and ack
	// sequences. For queue members, there is no much that can be done
	// because by nature seq will not be contiguous, but for non queue
	// subs, this could be optimized.
	for _, sequence := range ssa.Sent {
		// Update LastSent if applicable
		if sequence > sub.LastSent {
			sub.LastSent = sequence
		}
		// In case this is a queue member, update queue state's LastSent.
		if sub.qstate != nil && sequence > sub.qstate.lastSent {
			sub.qstate.lastSent = sequence
		}
		// Set 0 for expiration time. This will be computed
		// when the follower becomes leader and attempts to
		// redeliver messages.
		sub.acksPending[sequence] = 0
	}
	// Now remove the acks pending that we potentially just added ;-)
	for _, sequence := range ssa.Ack {
		delete(sub.acksPending, sequence)
	}
	sub.stalled = len(sub.acksPending) >= int(sub.MaxInFlight)
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

	if m.Redelivered {
		sub.updateRedeliveryCount(m)
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
	atomic.AddInt64(&s.stats.outMsgs, 1)
	atomic.AddInt64(&s.stats.outBytes, int64(len(b)))

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

	// If in cluster mode, schedule replication of the sent event.
	if s.isClustered {
		s.collectSentOrAck(sub, replicateSent, m.Sequence)
	}

	// Store in storage
	if err := sub.store.AddSeqPending(sub.ID, m.Sequence); err != nil {
		s.log.Errorf("[Client:%s] Unable to add pending message to subid=%d, subject=%s, seq=%d, err=%v",
			sub.ClientID, sub.ID, sub.subject, m.Sequence, err)
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

// Will set the redelivery count for this message that is ready to be redelivered.
// sub's lock is held on entry, and if it is a queue sub, qstate's lock is held too.
func (sub *subState) updateRedeliveryCount(m *pb.MsgProto) {
	var rdlvCountMap *map[uint64]uint32
	if sub.qstate != nil {
		rdlvCountMap = &sub.qstate.rdlvCount
	} else {
		rdlvCountMap = &sub.rdlvCount
	}
	if *rdlvCountMap == nil {
		*rdlvCountMap = make(map[uint64]uint32)
	}
	for {
		(*rdlvCountMap)[m.Sequence]++
		m.RedeliveryCount = (*rdlvCountMap)[m.Sequence]
		// Just in case we rolled over...
		if m.RedeliveryCount != 0 {
			break
		}
	}
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

	storesToFlush := make(map[*channel]struct{}, 64)

	var (
		_pendingMsgs [ioChannelSize]*ioPendingMsg
		pendingMsgs  = _pendingMsgs[:0]
	)

	storeIOPendingMsgs := func(iopms []*ioPendingMsg) {
		var (
			futuresMap map[*channel]raft.Future
			err        error
		)
		if s.isClustered {
			futuresMap, err = s.replicate(iopms)
			// If replicate() returns an error, it means that no future
			// was applied, so we can fail all published messages.
			if err != nil {
				for _, iopm := range iopms {
					s.logErrAndSendPublishErr(iopm, err)
				}
			} else {
				for c, f := range futuresMap {
					// Wait for the replication result.
					// We know that we panic in StanServer.Apply() if storing
					// of messages fail. So the only reason f.Error() would
					// return an error (we are not using timeout in Apply())
					// is if raft fails to store its log, but it would have
					// then switched follower state. On leadership acquisition
					// we do reset nextSequence based on lastSequence on store.
					// Regardless, do reset here in case of error.

					// Note that each future contains a batch of messages for
					// a given channel. All futures in the map are for different
					// channels.
					if err := f.Error(); err != nil {
						lastSeq, lerr := c.store.Msgs.LastSequence()
						if lerr != nil {
							panic(fmt.Errorf("error during message replication (%v), unable to get store last sequence: %v", err, lerr))
						}
						c.nextSequence = lastSeq + 1
					} else {
						storesToFlush[c] = struct{}{}
					}
				}
				// We have 1 future per channel. However, the array of iopms
				// may be from different channels. For each iopm we look
				// up its corresponding future and if there was an error
				// (same for all iopms of the same channel) we fail the
				// corresponding publishers.
				for _, iopm := range iopms {
					// We can call Error() again, this is not a problem.
					if err := futuresMap[iopm.c].Error(); err != nil {
						s.logErrAndSendPublishErr(iopm, err)
					} else {
						pendingMsgs = append(pendingMsgs, iopm)
					}
				}
			}
		} else {
			for _, iopm := range iopms {
				pm := &iopm.pm
				c, err := s.lookupOrCreateChannel(pm.Subject)
				if err == nil {
					msg := c.pubMsgToMsgProto(pm, c.nextSequence)
					_, err = c.store.Msgs.Store(msg)
				}
				if err != nil {
					s.logErrAndSendPublishErr(iopm, err)
				} else {
					c.nextSequence++
					pendingMsgs = append(pendingMsgs, iopm)
					storesToFlush[c] = struct{}{}
				}
			}
		}
	}

	var (
		batchSize = s.opts.IOBatchSize
		sleepTime = s.opts.IOSleepTime
		sleepDur  = time.Duration(sleepTime) * time.Microsecond
		max       = 0
		batch     = make([]*ioPendingMsg, 0, batchSize)
		dciopm    *ioPendingMsg
	)

	synchronizationRequest := func(iopm *ioPendingMsg) {
		iopm.sc <- struct{}{}
		<-iopm.sdc
	}

	ready.Done()
	for {
		batch = batch[:0]
		select {
		case iopm := <-s.ioChannel:
			// Is this a request to delete a channel?
			if iopm.dc {
				s.handleChannelDelete(iopm.c)
				continue
			} else if iopm.sc != nil {
				synchronizationRequest(iopm)
				continue
			}
			batch = append(batch, iopm)

			remaining := batchSize - 1
		FILL_BATCH_LOOP:
			// fill the message batch slice with at most our batch size,
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
					iopm = <-s.ioChannel
					if iopm.dc {
						dciopm = iopm
						break FILL_BATCH_LOOP
					} else if iopm.sc != nil {
						synchronizationRequest(iopm)
					} else {
						batch = append(batch, iopm)
					}
				}
				// Keep track of max number of messages in a batch
				if ioChanLen > max {
					max = ioChanLen
					atomic.StoreInt64(&(s.ioChannelStatsMaxBatchSize), int64(max))
				}
				remaining -= ioChanLen
			}

			// If clustered, wait on the result of replication.
			storeIOPendingMsgs(batch)

			// flush all the stores with messages written to them...
			for c := range storesToFlush {
				if err := c.store.Msgs.Flush(); err != nil {
					// TODO: Attempt recovery, notify publishers of error.
					panic(fmt.Errorf("unable to flush msg store: %v", err))
				}
				// Call this here, so messages are sent to subscribers,
				// which means that msg seq is added to subscription file
				s.processMsg(c)
				if err := c.store.Subs.Flush(); err != nil {
					panic(fmt.Errorf("unable to flush sub store: %v", err))
				}
				// Remove entry from map (this is safe in Go)
				delete(storesToFlush, c)
				// When relevant, update the last activity
				if c.activity != nil {
					c.activity.last = time.Unix(0, c.lTimestamp)
				}
			}

			// Ack our messages back to the publisher
			for i := range pendingMsgs {
				iopm := pendingMsgs[i]
				s.ackPublisher(iopm)
				pendingMsgs[i] = nil
			}

			// clear out pending messages
			pendingMsgs = pendingMsgs[:0]

			// If there was a request to delete a channel, try now
			if dciopm != nil {
				s.handleChannelDelete(dciopm.c)
				dciopm = nil
			}

		case <-s.ioChannelQuit:
			return
		}
	}
}

func (s *StanServer) logErrAndSendPublishErr(iopm *ioPendingMsg, err error) {
	s.log.Errorf("[Client:%s] Error processing message for subject %q: %v",
		iopm.pm.ClientID, iopm.m.Subject, err)
	s.sendPublishErr(iopm.m.Reply, iopm.pm.Guid, err)
}

// Sends a special ioPendingMsg to indicate that we should attempt
// to delete the given channel.
func (s *StanServer) sendDeleteChannelRequest(c *channel) {
	iopm := &ioPendingMsg{c: c, dc: true}
	s.ioChannel <- iopm
}

// replicate will replicate the batch of messages to followers and return
// futures (one for each channel messages were replicated for) which, when
// waited upon, will indicate if the replication was successful or not. This
// should only be called if running in clustered mode.
func (s *StanServer) replicate(iopms []*ioPendingMsg) (map[*channel]raft.Future, error) {
	var (
		futures = make(map[*channel]raft.Future)
		batches = make(map[*channel]*spb.Batch)
	)
	for _, iopm := range iopms {
		pm := &iopm.pm
		c, err := s.lookupOrCreateChannel(pm.Subject)
		if err != nil {
			return nil, err
		}
		msg := c.pubMsgToMsgProto(pm, c.nextSequence)
		batch := batches[c]
		if batch == nil {
			batch = &spb.Batch{}
			batches[c] = batch
		}
		batch.Messages = append(batch.Messages, msg)
		iopm.c = c
		c.nextSequence++
	}
	for c, batch := range batches {
		op := &spb.RaftOperation{
			OpType:       spb.RaftOperation_Publish,
			PublishBatch: batch,
			ChannelID:    c.id,
		}
		data, err := op.Marshal()
		if err != nil {
			panic(err)
		}
		futures[c] = s.raft.Apply(data, 0)
	}
	return futures, nil
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
	clientID := client.info.ID
	client.RUnlock()
	var (
		storesToFlush = map[string]stores.SubStore{}
		channels      = map[string]struct{}{}
	)
	for _, sub := range subs {
		sub.RLock()
		subject := sub.subject
		isDurable := sub.IsDurable
		subStore := sub.store
		sub.RUnlock()
		// Get the channel
		c := s.channels.get(subject)
		if c == nil {
			continue
		}
		// Don't remove durables
		c.ss.Remove(c, sub, false)
		// If the sub is a durable, there may have been an update to storage,
		// so we will want to flush the store. In clustering, during replay,
		// subStore may be nil.
		if isDurable && subStore != nil {
			storesToFlush[subject] = subStore
		}
		channels[subject] = struct{}{}
	}
	if len(storesToFlush) > 0 {
		for subject, subStore := range storesToFlush {
			if err := subStore.Flush(); err != nil {
				s.log.Errorf("[Client:%s] Error flushing store while removing subscriptions: subject=%s, err=%v", clientID, subject, err)
			}
		}
	}
	for channel := range channels {
		s.channels.maybeStartChannelDeleteTimer(channel, nil)
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
	s.performUnsubOrCloseSubscription(m, req, false)
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
	s.performUnsubOrCloseSubscription(m, req, true)
}

// Used when processing protocol messages to guarantee ordering.
// Since protocols handlers use different subscriptions, a client
// may send a message then close the connection, but those protocols
// are processed by different internal subscriptions in the server.
// Using nats's Conn.Barrier() we ensure that messages have been
// processed in their respective callbacks before invoking `f`.
// Since we also use a separate connection to handle acks, we
// also need to flush the connection used to process ack's and
// chained Barrier calls between s.nc and s.nca.
func (s *StanServer) barrier(f func()) {
	s.nc.Barrier(func() {
		// Ensure all pending acks are received by the connection
		s.nca.Flush()
		// Then ensure that all acks have been processed in processAckMsg callbacks
		// before executing the closing function.
		s.nca.Barrier(f)
	})
}

// performUnsubOrCloseSubscription processes the unsub or close subscription
// request.
func (s *StanServer) performUnsubOrCloseSubscription(m *nats.Msg, req *pb.UnsubscribeRequest, isSubClose bool) {
	// With partitioning, first verify that this server is handling this
	// channel. If not, do not return an error, since another server will
	// handle it. If no other server is, the client will get a timeout.
	if s.partitions != nil {
		if r := s.partitions.sl.Match(req.Subject); len(r) == 0 {
			return
		}
	}

	s.barrier(func() {
		var err error
		if s.isClustered {
			if isSubClose {
				err = s.replicateCloseSubscription(req)
			} else {
				err = s.replicateRemoveSubscription(req)
			}
		} else {
			s.closeMu.Lock()
			err = s.unsubscribe(req, isSubClose)
			s.closeMu.Unlock()
		}
		// If there was an error, it has been already logged.

		if err == nil {
			// This will check if the channel has MaxInactivity defined,
			// if so and there is no active subscription, it will start the
			// delete timer.
			s.channels.maybeStartChannelDeleteTimer(req.Subject, nil)
		}

		// If err is nil, it will be a non-error response
		s.sendSubscriptionResponseErr(m.Reply, err)
	})
}

func (s *StanServer) unsubscribe(req *pb.UnsubscribeRequest, isSubClose bool) error {
	action := "unsub"
	if isSubClose {
		action = "sub close"
	}
	c := s.channels.get(req.Subject)
	if c == nil {
		s.log.Errorf("[Client:%s] %s request missing subject %s",
			req.ClientID, action, req.Subject)
		return ErrInvalidSub
	}
	sub := c.ss.LookupByAckInbox(req.Inbox)
	if sub == nil {
		s.log.Errorf("[Client:%s] %s request for missing inbox %s",
			req.ClientID, action, req.Inbox)
		return ErrInvalidSub
	}
	return s.unsubscribeSub(c, req.ClientID, action, sub, isSubClose, true)
}

func (s *StanServer) unsubscribeSub(c *channel, clientID, action string, sub *subState, isSubClose, shouldFlush bool) error {
	// Remove from Client
	if !s.clients.removeSub(clientID, sub) {
		s.log.Errorf("[Client:%s] %s request for missing client", clientID, action)
		return ErrUnknownClient
	}
	// Remove the subscription
	unsubscribe := !isSubClose
	c.ss.Remove(c, sub, unsubscribe)
	var err error
	if shouldFlush {
		sub.RLock()
		ss := sub.store
		sub.RUnlock()
		err = ss.Flush()
	}
	return err
}

func (s *StanServer) replicateRemoveSubscription(req *pb.UnsubscribeRequest) error {
	return s.replicateUnsubscribe(req, spb.RaftOperation_RemoveSubscription)
}

func (s *StanServer) replicateCloseSubscription(req *pb.UnsubscribeRequest) error {
	return s.replicateUnsubscribe(req, spb.RaftOperation_CloseSubscription)
}

func (s *StanServer) replicateUnsubscribe(req *pb.UnsubscribeRequest, opType spb.RaftOperation_Type) error {
	// When closing a subscription, we need to possibly "flush" the
	// pending sent/ack that need to be replicated
	c := s.channels.get(req.Subject)
	if c != nil {
		sub := c.ss.LookupByAckInbox(req.Inbox)
		if sub != nil {
			unsub := opType == spb.RaftOperation_RemoveSubscription
			s.endSubSentAndAckReplication(sub, unsub)
		}
	}
	op := &spb.RaftOperation{
		OpType: opType,
		Unsub:  req,
	}
	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}
	// Wait on result of replication.
	return waitForReplicationErrResponse(s.raft.Apply(data, 0))
}

func (s *StanServer) sendSubscriptionResponseErr(reply string, err error) {
	if reply == "" {
		return
	}
	resp := &pb.SubscriptionResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
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

// Subscribes to the AckInbox subject in order to process subscription's acks
// if not already done.
// This function grabs and releases the sub's lock.
func (sub *subState) startAckSub(nc *nats.Conn, cb nats.MsgHandler) error {
	ackSub, err := nc.Subscribe(sub.AckInbox, cb)
	if err != nil {
		return err
	}
	sub.Lock()
	// Should not occur, but if it was already set,
	// unsubscribe old and replace.
	sub.stopAckSub()
	sub.ackSub = ackSub
	sub.ackSub.SetPendingLimits(-1, -1)
	sub.Unlock()
	return nil
}

// Stops subscribing to AckInbox.
// Lock assumed held on entry.
func (sub *subState) stopAckSub() {
	if sub.ackSub != nil {
		sub.ackSub.Unsubscribe()
		sub.ackSub = nil
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

// replicateSub replicates the SubscriptionRequest to nodes in the cluster via Raft.
func (s *StanServer) replicateSub(c *channel, sr *pb.SubscriptionRequest, ackInbox string, subID uint64) (*subState, error) {
	op := &spb.RaftOperation{
		OpType: spb.RaftOperation_Subscribe,
		Sub: &spb.AddSubscription{
			Request:  sr,
			AckInbox: ackInbox,
			ID:       subID,
		},
		ChannelID: c.id,
	}
	data, err := op.Marshal()
	if err != nil {
		panic(err)
	}
	// Replicate operation and wait on result.
	future := s.raft.Apply(data, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}
	rs := future.Response().(*replicatedSub)
	return rs.sub, rs.err
}

// addSubscription adds `sub` to the client and store.
func (s *StanServer) addSubscription(ss *subStore, sub *subState) error {
	// Store in client
	if !s.clients.addSub(sub.ClientID, sub) {
		return fmt.Errorf("can't find clientID: %v", sub.ClientID)
	}
	// Store this subscription in subStore
	if err := ss.Store(sub); err != nil {
		s.clients.removeSub(sub.ClientID, sub)
		return err
	}
	return nil
}

// updateDurable adds back `sub` to the client and updates the store.
func (s *StanServer) updateDurable(ss *subStore, sub *subState, clientID string) error {
	// Store in the client
	if !s.clients.addSub(clientID, sub) {
		return fmt.Errorf("can't find clientID: %v", clientID)
	}
	// Update this subscription in the store
	sub.Lock()
	err := sub.store.UpdateSub(&sub.SubState)
	sub.Unlock()
	if err != nil {
		return err
	}
	// Do this only for durable subscribers (not durable queue subscribers).
	if sub.isDurableSubscriber() {
		// Add back into plain subscribers
		ss.psubs = append(ss.psubs, sub)
	}
	// And in ackInbox lookup map.
	ss.acks[sub.AckInbox] = sub

	return nil
}

// processSub adds the subscription to the server.
func (s *StanServer) processSub(c *channel, sr *pb.SubscriptionRequest, ackInbox string, subID uint64) (*subState, error) {
	var (
		err error
		sub *subState
		ss  = c.ss
	)

	ss.Lock()

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
				ss.Unlock()
				return nil, ErrInvalidDurName
			}
			isDurable = true
			// Make the queue group a compound name between durable name and q group.
			sr.QGroup = fmt.Sprintf("%s:%s", sr.DurableName, sr.QGroup)
			// Clear DurableName from this subscriber.
			sr.DurableName = ""
		}
		// Lookup for an existing group. Only interested in situation where
		// the group exist, but is empty and had a shadow subscriber.
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
	} else if sr.DurableName != "" {
		// Check for DurableSubscriber status
		if sub = ss.durables[durableKey(sr)]; sub != nil {
			sub.RLock()
			clientID := sub.ClientID
			sub.RUnlock()
			if clientID != "" {
				s.log.Errorf("[Client:%s] Duplicate durable subscription registration", sr.ClientID)
				ss.Unlock()
				return nil, ErrDupDurable
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
			if !s.isClustered || s.isLeader() {
				s.setupAckTimer(sub, sub.ackWait)
			}
		}
		// Clear the IsClosed flags that were set during a Close()
		sub.IsClosed = false
		// In cluster mode, need to reset this flag.
		if s.isClustered {
			sub.norepl = false
		}
		// Reset the hasFailedHB boolean since it may have been set
		// if the client previously crashed and server set this
		// flag to its subs.
		sub.hasFailedHB = false
		// Reset initialized for restarting durable subscription.
		// Without this (and if there is no message to be redelivered,
		// which would set newOnHold), we could be sending avail
		// messages before the response has been sent to client library.
		sub.initialized = false
		sub.Unlock()

		// Case of restarted durable subscriber, or first durable queue
		// subscriber re-joining a group that was left with pending messages.
		err = s.updateDurable(ss, sub, sr.ClientID)
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
			var lastSent uint64
			subStartTrace, lastSent, err = s.setSubStartSequence(c, sr)
			if err == nil {
				sub.LastSent = lastSent
			}
		}

		if err == nil {
			// add the subscription to stan.
			// In cluster mode, the server decides of the subscription ID
			// (so that subscriptions have the same ID on replay). So
			// set it prior to this call.
			sub.ID = subID
			err = s.addSubscription(ss, sub)
			if err == nil && subID > 0 {
				if subID >= c.nextSubID {
					c.nextSubID = subID + 1
				}
			}
		}
	}
	ss.Unlock()
	if err == nil && (!s.isClustered || s.isLeader()) {
		err = sub.startAckSub(s.nca, s.processAckMsg)
		if err == nil {
			// Need tp make sure that this subscription is processed by
			// NATS Server before sending response (since we use different
			// connection to send the response)
			s.nca.Flush()
		}
	}
	if err != nil {
		// Try to undo what has been done.
		s.clients.removeSub(sr.ClientID, sub)
		s.closeMu.Lock()
		ss.Remove(c, sub, subIsNew)
		s.closeMu.Unlock()
		s.log.Errorf("Unable to add subscription for %s: %v", sr.Subject, err)
		return nil, err
	}
	if s.debug {
		traceCtx := subStateTraceCtx{clientID: sr.ClientID, isNew: subIsNew, startTrace: subStartTrace}
		traceSubState(s.log, sub, &traceCtx)
	}

	return sub, nil
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
		// Also check that the connection request has already
		// been processed. Check clientCheckTimeout doc for details.
		if !s.clients.isValidWithTimeout(sr.ClientID, nil, clientCheckTimeout) {
			s.log.Errorf("[Client:%s] Rejecting subscription on %q: connection not created yet",
				sr.ClientID, sr.Subject)
			s.sendSubscriptionResponseErr(m.Reply, ErrInvalidSubReq)
			return
		}
	}

	var (
		sub      *subState
		ackInbox = nats.NewInbox()
	)

	// Lookup/create the channel and prevent this channel to be deleted
	// until we are done with this subscription. This will also stop
	// the delete timer if one was set.
	c, preventDelete, err := s.lookupOrCreateChannelPreventDelete(sr.Subject)
	if err == nil {
		// If clustered, thread operations through Raft.
		if s.isClustered {
			// For start requests other than SequenceStart, we MUST convert the request
			// to a SequenceStart, otherwise, during the replay on server restart, the
			// subscription would be created with whatever is the seq at that time.
			// For instance, a request with new-only could originally be created with
			// the current max seq of 100, but when the cluster is restarted and sub
			// request is replayed, the channel's current max may be 200, which
			// would cause the subscription to be created at start 200, which could cause
			// subscription to miss all messages in between.
			if sr.StartPosition != pb.StartPosition_SequenceStart {
				// Figure out what the sequence should be based on orinal StartPosition
				// request.
				var seq uint64
				_, seq, err = s.setSubStartSequence(c, sr)
				if err == nil {
					// Convert to a SequenceStart start position with the proper sequence
					// number. Since setSubStartSequence() is returning what should be
					// the lastSent, we need to bump the count by 1.
					sr.StartPosition = pb.StartPosition_SequenceStart
					sr.StartSequence = seq + 1
				}
			}
			if err == nil {
				c.ss.Lock()
				subID := c.nextSubID
				c.ss.Unlock()
				sub, err = s.replicateSub(c, sr, ackInbox, subID)
			}
		} else {
			sub, err = s.processSub(c, sr, ackInbox, 0)
		}
	}
	if err != nil {
		s.channels.turnOffPreventDelete(c)
		s.channels.maybeStartChannelDeleteTimer(sr.Subject, c)
		s.sendSubscriptionResponseErr(m.Reply, err)
		return
	}
	if preventDelete {
		defer s.channels.turnOffPreventDelete(c)
	}

	// In case this is a durable, sub already exists so we need to protect access
	sub.Lock()

	// Create a non-error response
	resp := &pb.SubscriptionResponse{AckInbox: sub.AckInbox}
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

	s.subStartCh <- &subStartInfo{c: c, sub: sub, qs: qs, isDurable: sub.IsDurable}
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
	sub := c.ss.LookupByAckInbox(m.Subject)
	if sub == nil {
		return
	}
	s.processAck(c, sub, ack.Sequence, true)
}

// processAck processes an ack and if needed sends more messages.
func (s *StanServer) processAck(c *channel, sub *subState, sequence uint64, fromUser bool) {
	var stalled bool

	// This is immutable, so can grab outside of sub's lock.
	// If we have a queue group, we want to grab queue's lock before
	// sub's lock.
	qs := sub.qstate
	if qs != nil {
		qs.Lock()
	}

	sub.Lock()

	persistAck := func(aSub *subState) bool {
		if err := aSub.store.AckSeqPending(aSub.ID, sequence); err != nil {
			s.log.Errorf("[Client:%s] Unable to persist ack for subid=%d, subject=%s, seq=%d, err=%v",
				aSub.ClientID, aSub.ID, aSub.subject, sequence, err)
			return false
		}
		return true
	}

	if _, found := sub.acksPending[sequence]; found {
		// If in cluster mode, schedule replication of the ack.
		if s.isClustered {
			s.collectSentOrAck(sub, replicateAck, sequence)
		}
		if s.trace && fromUser {
			s.log.Tracef("[Client:%s] Processing ack for subid=%d, subject=%s, seq=%d",
				sub.ClientID, sub.ID, sub.subject, sequence)
		}
		if !persistAck(sub) {
			sub.Unlock()
			if qs != nil {
				qs.Unlock()
			}
			return
		}
		delete(sub.acksPending, sequence)
	} else if qs != nil && fromUser {
		// For queue members, if this is not an internally generated ACK
		// and we don't find the sequence in this sub's pending, we are
		// going to look for it in other members and process it if found.
		sub.Unlock()
		for _, qsub := range qs.subs {
			if qsub == sub {
				continue
			}
			qsub.Lock()
			if _, found := qsub.acksPending[sequence]; found {
				delete(qsub.acksPending, sequence)
				persistAck(qsub)
				qsub.Unlock()
				break
			}
			qsub.Unlock()
		}
		sub.Lock()
		// Proceed with original sub (regardless if member was found
		// or not) so that server sends more messages if needed.
	}
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

	if sub.qstate != nil {
		s.sendAvailableMessagesToQueue(c, sub.qstate)
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
	// Short circuit if no active members
	if len(qs.subs) == 0 {
		qs.Unlock()
		return
	}
	// If redelivery at startup in progress, don't attempt to deliver new messages
	if qs.newOnHold {
		qs.Unlock()
		return
	}
	for nextSeq := qs.lastSent + 1; qs.stalledSubCount < len(qs.subs); nextSeq++ {
		nextMsg := s.getNextMsg(c, &nextSeq, &qs.lastSent)
		if nextMsg == nil {
			break
		}
		if _, sent := s.sendMsgToQueueGroup(qs, nextMsg, honorMaxInFlight); !sent {
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
	for i := 0; ; i++ {
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
		// Message was not found, check the store first/last sequences.
		first, last, _ := c.store.Msgs.FirstAndLastSequence()
		if *nextSeq >= last {
			// This means that we are looking for a message that has not
			// been stored. This is perfectly normal when delivering messages
			// and reach the end of the channel.
			return nil
		} else if *nextSeq < first {
			// We were trying to lookup a message that has likely now
			// been removed (expired, or due to max msgs/bytes etc) since
			// the first available is greater than the message we were
			// looking for. Try to lookup the first available.
			*nextSeq = first
			*lastSent = first - 1
		} else if i > 0 {
			// The last condition is when `nextSeq` is between `first` and
			// `last`, which could mean 2 things:
			// - New messages have been stored between the lookup at the top
			//   of the loop and calling FirstAndLastSequence(), so we should
			//   try again.
			// - There is a gap - which should not happen but we have decided
			//   to support this situation - so we move by one at a time until
			//   we find a valid message.

			// So if i==0 (first iteration) we don't come here and simply try
			// again. Otherwise, move the requested sequence in search of the
			// first valid message.
			*nextSeq++
			*lastSent++
		}

		// Note that the next lookup could still fail because
		// the first avail message may have been dropped in the
		// meantime.
	}
}

// Setup the start position for the subscriber.
func (s *StanServer) setSubStartSequence(c *channel, sr *pb.SubscriptionRequest) (string, uint64, error) {
	lastSent := uint64(0)
	debugTrace := ""

	// In all start position cases, if there is no message, ensure
	// lastSent stays at 0.

	switch sr.StartPosition {
	case pb.StartPosition_NewOnly:
		var err error
		lastSent, err = c.store.Msgs.LastSequence()
		if err != nil {
			return "", 0, err
		}
		if s.debug {
			debugTrace = fmt.Sprintf("new-only, seq=%d", lastSent+1)
		}
	case pb.StartPosition_LastReceived:
		lastSeq, err := c.store.Msgs.LastSequence()
		if err != nil {
			return "", 0, err
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
			return "", 0, err
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
			return "", 0, err
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
			return "", 0, err
		}
		if firstSeq > 0 {
			lastSent = firstSeq - 1
		}
		if s.debug {
			debugTrace = fmt.Sprintf("from beginning, seq=%d", lastSent+1)
		}
	}
	return debugTrace, lastSent, nil
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

// ClusterID returns the NATS Streaming Server's ID.
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

	close(s.shutdownCh)

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
	ncr := s.ncr
	ncsr := s.ncsr
	nc := s.nc
	ftnc := s.ftnc
	nca := s.nca

	// Stop processing subscriptions start requests
	s.subStartQuit <- struct{}{}

	if s.ioChannel != nil {
		// Notify the IO channel that we are shutting down
		close(s.ioChannelQuit)
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
	var channels map[string]*channel
	if s.channels != nil {
		channels = s.channels.getAll()
	}
	s.mu.Unlock()

	// Stop all redelivery timers.
	for _, c := range channels {
		subs := c.ss.getAllSubs()
		for _, sub := range subs {
			sub.Lock()
			sub.clearAckTimer()
			sub.Unlock()
		}
	}

	// Make sure the StoreIOLoop returns before closing the Store
	if waitForIOStoreLoop {
		s.ioChannelWG.Wait()
	}

	// Close Raft group before closing store.
	if s.raft != nil {
		if err := s.raft.shutdown(); err != nil {
			s.log.Errorf("Failed to stop Raft node: %v", err)
		}
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
	if ncr != nil {
		ncr.Close()
	}
	if ncsr != nil {
		ncsr.Close()
	}
	if nc != nil {
		nc.Close()
	}
	if ftnc != nil {
		ftnc.Close()
	}
	if nca != nil {
		nca.Close()
	}
	if ns != nil {
		ns.Shutdown()
	}

	// Wait for go-routines to return
	s.wg.Wait()
}
