// Copyright 2016 Apcera Inc. All rights reserved.

// A Go client for the STAN/NATS messaging system (https://nats.io).
package stan

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats"
)

const (
	Version               = "0.0.1"
	DefaultNatsURL        = "nats://localhost:4222"
	DefaultConnectWait    = 2 * time.Second
	DefaultAckWait        = 2 * time.Second
	DefaultMaxInflight    = 1024
	DefaultDiscoverPrefix = "_STAN.discover"
	DefaultACKPrefix      = "_STAN.acks"
)

// Errors
var (
	ErrClusterUnreachable = errors.New("stan: cluster unreachable")
	ErrConnectionClosed   = errors.New("stan: connection closed")
	ErrTimeout            = errors.New("stan: publish ack timeout")
	ErrBadAck             = errors.New("stan: malformed ack")
	ErrBadSubscription    = errors.New("stan: invalid subscription")
)

// Conn represents a connection to the STAN subsystem. It can Publish and
// Subscribe to messages withing the STAN cluster.
type Conn interface {
	// Publish
	Publish(subject string, data []byte) error
	PublishAsync(subject string, data []byte, ah AckHandler) string
	// Publish with Reply
	PublishWithReply(subject, reply string, data []byte) error
	PublishAsyncWithReply(subject, reply string, data []byte, ah AckHandler) string

	// Subscribe
	Subscribe(subject string, cb MsgHandler, opts ...SubscriptionOption) (Subscription, error)
}

// Subscription represents a subscription within the STAN cluster. Subscriptions
// will be rate matched and follow at-least delivery semantics.
type Subscription interface {
	Unsubscribe() error
}

// SubscriptionOption is a function on the options for a subscription.
type SubscriptionOption func(*SubscriptionOptions) error

// MsgHandler is a callback function that processes messages delivered to
// asynchronous subscribers.
type MsgHandler func(msg *Msg)

// SubscriptionOptions are used to control the Subscription's behavior.
type SubscriptionOptions struct {
	// DurableName, if set will survive client restarts.
	DurableName string
	// Controls the number of messages the cluster will have inflight without an ACK.
	MaxInflight int
	// Controls the time the cluster will wait for an ACK for a given message.
	AckWait time.Duration
	// StartPosition enum from proto.
	StartAt StartPosition
	// Optional start sequence number.
	StartSequence uint64
	// Optional start time.
	StartTime time.Time
}

var DefaultSubscriptionOptions = SubscriptionOptions{
	MaxInflight: DefaultMaxInflight,
	AckWait:     DefaultAckWait,
}

// MaxInflight is an Option to set the maximum number of messages the cluster will send
// without an ACK.
func MaxInflight(m int) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.MaxInflight = m
		return nil
	}
}

// AckWait is an Option to set the timeout for waiting for an ACK from the cluster's
// point of view for delivered messages.
func AckWait(t time.Duration) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.AckWait = t
		return nil
	}
}

// StartPosition sets the desired start position for the message stream.
func StartAt(sp StartPosition) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = sp
		return nil
	}
}

// StartSequence sets the desired start sequence position and state.
func StartAtSequence(seq uint64) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = StartPosition_SequenceStart
		o.StartSequence = seq
		return nil
	}
}

// StartTime sets the desired start time position and state.
func StartAtTime(start time.Time) SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = StartPosition_TimeStart
		o.StartTime = start
		return nil
	}
}

// StartWithLastReceived is a helper function to set start position to last received.
func StartWithLastReceived() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = StartPosition_LastReceived
		return nil
	}
}

// DeliverAllAvailable will deliver all messages available.
func DeliverAllAvailable() SubscriptionOption {
	return func(o *SubscriptionOptions) error {
		o.StartAt = StartPosition_First
		return nil
	}
}

// A conn represents a bare connection to a stan cluster.
type conn struct {
	sync.Mutex
	clientID        string
	serverID        string
	pubPrefix       string // Publish prefix set by stan, append our subject.
	subRequests     string // Subject to send subscription requests.
	unsubRequests   string // Subject to send unsubscribe requests.
	ackSubject      string // publish acks
	ackSubscription *nats.Subscription
	subMap          map[string]*subscription
	ackMap          map[string]*ack
	opts            Options
	nc              *nats.Conn
}

// Closure for ack contexts.
type ack struct {
	t  *time.Timer
	ah AckHandler
}

// AckHandler is used for Async Publishing to provide status of the ack.
// The func will be passed teh GUID and any error state. No error means the
// message was sucessfully received by STAN.
type AckHandler func(string, error)

// Options can be used to a create a customized connection.
type Options struct {
	NatsURL        string
	NatsConn       *nats.Conn
	ConnectTimeout time.Duration
	AckTimeout     time.Duration
	DiscoverPrefix string
}

var DefaultOptions = Options{
	NatsURL:        DefaultNatsURL,
	ConnectTimeout: DefaultConnectWait,
	AckTimeout:     DefaultAckWait,
	DiscoverPrefix: DefaultDiscoverPrefix,
}

// Option is a function on the options for a connection.
type Option func(*Options) error

// ConnectWait is an Option to set the timeout for establishing a connection.
func ConnectWait(t time.Duration) Option {
	return func(o *Options) error {
		o.ConnectTimeout = t
		return nil
	}
}

// PubAckWait is an Option to set the timeout for waiting for an ACK for a
// published message.
func PubAckWait(t time.Duration) Option {
	return func(o *Options) error {
		o.AckTimeout = t
		return nil
	}
}

// Connect will form a connection to the STAN subsystem.
func Connect(stanClusterID, clientID string, options ...Option) (Conn, error) {
	// Process Options
	c := conn{opts: DefaultOptions}
	for _, opt := range options {
		if err := opt(&c.opts); err != nil {
			return nil, err
		}
	}
	// Create a connection if it doesn't exist.
	if c.nc == nil {
		if nc, err := nats.Connect(c.opts.NatsURL); err != nil {
			return nil, err
		} else {
			c.nc = nc
		}
	}
	// Send Request to discover the cluster
	discoverSubject := fmt.Sprintf("%s.%s", c.opts.DiscoverPrefix, stanClusterID)
	reply, err := c.nc.Request(discoverSubject, nil, c.opts.ConnectTimeout)
	if err != nil {
		if err == nats.ErrTimeout {
			return nil, ErrClusterUnreachable
		} else {
			return nil, err
		}
	}
	// Process the response, grab server pubPrefix
	cr := &ConnectResponse{}
	err = cr.Unmarshal(reply.Data)
	//	err = json.Unmarshal(reply.Data, &cr)
	if err != nil {
		return nil, err
	}

	// Capture cluster configuration endpoints to publish and subscribe/unsubscribe.
	c.pubPrefix = cr.PubPrefix
	c.subRequests = cr.SubRequests
	c.unsubRequests = cr.UnsubRequests

	// Setup the ACK subscription
	c.ackSubject = fmt.Sprintf("%s.%s", DefaultACKPrefix, newGUID())
	if c.ackSubscription, err = c.nc.Subscribe(c.ackSubject, c.processAck); err != nil {
		return nil, err
	}
	c.ackSubscription.SetPendingLimits(1024*1024, 32*1024*1024)
	c.ackMap = make(map[string]*ack)

	// Create Subscription map
	c.subMap = make(map[string]*subscription)

	return &c, nil
}

// Process an ack from the STAN cluster
func (sc *conn) processAck(m *nats.Msg) {
	pa := &PubAck{}
	err := pa.Unmarshal(m.Data)
	if err != nil {
		// FIXME, make closure to have context?
		fmt.Printf("Error processing unmarshal\n")
	}
	sc.Lock()
	a := sc.ackMap[pa.Id]
	sc.removeAck(pa.Id)
	sc.Unlock()

	// Perform the ackHandler callback
	if a != nil && a.ah != nil {
		a.ah(pa.Id, nil)
	}
}

// Publish will publish to the cluster and wait for an ACK.
func (sc *conn) Publish(subject string, data []byte) (e error) {
	return sc.PublishWithReply(subject, "", data)
}

// PublishAsync will publish to the cluster on pubPrefix+subject and asynchronously
// process the ACK or error state. It will return the GUID for the message being sent.
func (sc *conn) PublishAsync(subject string, data []byte, ah AckHandler) string {
	return sc.PublishAsyncWithReply(subject, "", data, ah)
}

// PublishWithReply will publish to the cluster and wait for an ACK.
func (sc *conn) PublishWithReply(subject, reply string, data []byte) (e error) {
	// FIXME(dlc) Pool?
	ch := make(chan bool)
	ah := func(guid string, err error) {
		e = err
		ch <- true
	}
	sc.PublishAsyncWithReply(subject, reply, data, ah)
	<-ch
	return e
}

// PublishAsyncWithReply will publish to the cluster and asynchronously
// process the ACK or error state. It will return the GUID for the message being sent.
func (sc *conn) PublishAsyncWithReply(subject, reply string, data []byte, ah AckHandler) string {
	subj := fmt.Sprintf("%s.%s", sc.pubPrefix, subject)
	pe := &PubMsg{Id: newGUID(), Subject: subject, Reply: reply, Data: data}
	b, _ := pe.Marshal()
	a := &ack{ah: ah}

	sc.Lock()
	sc.ackMap[pe.Id] = a
	err := sc.nc.PublishRequest(subj, sc.ackSubject, b)
	if err != nil {
		// Handle error by calling ah
		if ah != nil {
			go ah(pe.Id, err)
		}
	} else {
		// Setup the timer for expiration.
		a.t = time.AfterFunc(sc.opts.AckTimeout, func() {
			sc.Lock()
			sc.removeAck(pe.Id)
			sc.Unlock()
			a.ah(pe.Id, ErrTimeout)
		})
	}
	sc.Unlock()
	return pe.Id
}

// removeAck removes the ack from the ackMap and cancels any state, e.g. timers
// Assumes lock is held.
func (sc *conn) removeAck(guid string) {
	a := sc.ackMap[guid]
	if a != nil && a.t != nil {
		a.t.Stop()
	}
	delete(sc.ackMap, guid)
}

// A subscription represents a subscription to a stan cluster.
type subscription struct {
	sync.RWMutex
	sc       *conn
	subject  string
	inbox    string
	ackInbox string
	inboxSub *nats.Subscription
	opts     SubscriptionOptions
	cb       MsgHandler
}

// New style Inbox
// FIXME(dlc) remove once ported back to nats client.
func newInbox() string {
	return fmt.Sprintf("_INBOX.%s", newGUID())
}

// Helper function to produce time.Time from timestamp ns.
func (m *Msg) Time() time.Time {
	return time.Unix(0, m.Timestamp)
}

// Process an msg from the STAN cluster
func (sc *conn) processMsg(raw *nats.Msg) {
	msg := &Msg{}
	err := msg.Unmarshal(raw.Data)
	if err != nil {
		panic("Error processing unmarshal for msg")
	}
	// Lookup the subscription
	sc.Lock()
	sub := sc.subMap[raw.Subject]
	sc.Unlock()
	if sub == nil {
		return
	}
	sub.RLock()
	cb := sub.cb
	ackSubject := sub.ackInbox
	sub.RUnlock()

	// Perform the callback
	if cb != nil {
		cb(msg)
	}

	// Now auto-ack
	ack := &Ack{Seq: msg.Seq}
	b, _ := ack.Marshal()
	sc.nc.Publish(ackSubject, b)
}

// Subscribe will perform a subscription with the given options to the STAN cluster.
func (sc *conn) Subscribe(subject string, cb MsgHandler, options ...SubscriptionOption) (Subscription, error) {
	sub := &subscription{subject: subject, inbox: newInbox(), cb: cb, sc: sc, opts: DefaultSubscriptionOptions}
	for _, opt := range options {
		if err := opt(&sub.opts); err != nil {
			return nil, err
		}
	}
	sc.Lock()
	// Listen for actual messages.
	if nsub, err := sc.nc.Subscribe(sub.inbox, sc.processMsg); err != nil {
		sc.Unlock()
		return nil, err
	} else {
		sub.inboxSub = nsub
	}
	// Register subscription.
	sc.subMap[sub.inbox] = sub
	sc.Unlock()

	// Create a subscription request
	// FIXME(dlc) add others.
	sr := &SubscriptionRequest{
		Subject:       subject,
		Inbox:         sub.inbox,
		MaxInFlight:   int32(sub.opts.MaxInflight),
		AckWaitInSecs: int32(sub.opts.AckWait / time.Second),
		StartPosition: sub.opts.StartAt,
	}

	// Conditionals
	switch sr.StartPosition {
	case StartPosition_TimeStart:
		sr.StartTime = sub.opts.StartTime.UnixNano()
	case StartPosition_SequenceStart:
		sr.StartSequence = sub.opts.StartSequence
	}

	b, _ := sr.Marshal()
	reply, err := sc.nc.Request(sc.subRequests, b, 2*time.Second)
	if err != nil {
		// FIXME(dlc) unwind subscription from above.
		return nil, err
	}
	r := &SubscriptionResponse{}
	if err := r.Unmarshal(reply.Data); err != nil {
		// FIXME(dlc) unwind subscription from above.
		return nil, err
	}
	if r.Error != "" {
		// FIXME(dlc) unwind subscription from above.
		return nil, errors.New(r.Error)
	}
	sub.Lock()
	sub.ackInbox = r.AckInbox
	sub.Unlock()

	return sub, nil
}

// Unsubscribe removes interest in the subscription
func (sub *subscription) Unsubscribe() error {
	if sub == nil {
		return ErrBadSubscription
	}
	sub.Lock()
	sc := sub.sc
	if sc == nil {
		// Already closed.
		return ErrBadSubscription
	}
	sub.sc = nil
	sub.inboxSub.Unsubscribe()
	sub.inboxSub = nil
	inbox := sub.inbox
	sub.Unlock()

	if sc == nil {
		return nil
	}

	sc.Lock()
	delete(sc.subMap, inbox)
	reqSubject := sc.unsubRequests
	sc.Unlock()

	// Send Unsubscribe to server.

	// FIXME(dlc) - Add in durable
	usr := &UnsubscribeRequest{
		Subject: sub.subject,
		Inbox:   sub.ackInbox,
	}
	b, _ := usr.Marshal()
	// FIXME(dlc) - make timeout configurable.
	reply, err := sc.nc.Request(reqSubject, b, 2*time.Second)
	if err != nil {
		return err
	}
	r := &SubscriptionResponse{}
	if err := r.Unmarshal(reply.Data); err != nil {
		return err
	}
	if r.Error != "" {
		return errors.New(r.Error)
	}

	return nil
}
