package server

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats"
	"github.com/nats-io/nats-streaming-server/stores"

	"github.com/nats-io/gnatsd/auth"
	"io/ioutil"
	"sync"
	"sync/atomic"
)

const (
	clusterName = DefaultClusterID
	clientName  = "me"
)

// So that we can pass tests and benchmarks...
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

var defaultDataStore string

func init() {
	tmpDir, err := ioutil.TempDir(".", "data_server_")
	if err != nil {
		panic("Could not create tmp dir")
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp directory: %v", err))
	}
	defaultDataStore = tmpDir
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if ok == false {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

// Helper function to shutdown last, a server that is being restarted in a test.
func shutdownRestartedServerOnTestExit(s **StanServer) {
	srv := *s
	srv.Shutdown()
}

// Helper function that checks that the number returned by function `f`
// is equal to `expected`, otherwise fails.
func checkCount(t tLogger, expected int, f func() (string, int)) {
	if label, count := f(); count != expected {
		stackFatalf(t, "Incorrect number of %s, expected %v got %v", label, expected, count)
	}
}

// Helper function that waits that the number returned by function `f`
// is equal to `expected` for a certain period of time, otherwise fails.
func waitForCount(t tLogger, expected int, f func() (string, int)) {
	ok := false
	label := ""
	count := 0
	timeout := time.Now().Add(5 * time.Second)
	for !ok && time.Now().Before(timeout) {
		label, count = f()
		if count != expected {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ok = true
	}
	if !ok {
		stackFatalf(t, "Timeout waiting to get %v %s, got %v", expected, label, count)
	}
}

// Helper function that fails if number of clients is not as expected
func checkClients(t tLogger, s *StanServer, expected int) {
	checkCount(t, expected, func() (string, int) { return getClientsCountFunc(s) })
}

// Helper function that waits for a while to get the expected number of clients,
// otherwise fails.
func waitForNumClients(t tLogger, s *StanServer, expected int) {
	waitForCount(t, expected, func() (string, int) { return getClientsCountFunc(s) })
}

// Helper function that returns the number of clients
func getClientsCountFunc(s *StanServer) (string, int) {
	return "clients", s.store.GetClientsCount()
}

// Helper function that fails if number of subscriptions is not as expected
func checkSubs(t tLogger, s *StanServer, ID string, expected int) []*subState {
	// Since we need to return the array and we want the array to match
	// the expected value, use the "public" API here.
	subs := s.clients.GetSubs(ID)
	checkCount(t, expected, func() (string, int) { return "subscriptions", len(subs) })
	return subs
}

// Helper function that waits for a while to get the expected number of subscriptions,
// otherwise fails.
func waitForNumSubs(t tLogger, s *StanServer, ID string, expected int) {
	waitForCount(t, expected, func() (string, int) {
		// We avoid getting a copy of the subscriptions array here
		// by directly returning the length of the array.
		c := s.clients.Lookup(ID)
		c.RLock()
		defer c.RUnlock()
		return "subscriptions", len(c.subs)
	})
}

func waitForAcks(t tLogger, s *StanServer, ID string, subID uint64, expected int) {
	subs := s.clients.GetSubs(clientName)
	var sub *subState
	for _, s := range subs {
		s.RLock()
		sID := s.ID
		s.RUnlock()
		if sID == subID {
			sub = s
			break
		}
	}
	if sub == nil {
		stackFatalf(t, "Subscription %v not found", subID)
	}
	waitForCount(t, 0, func() (string, int) {
		sub.RLock()
		count := len(sub.acksPending)
		sub.RUnlock()
		return "ack pending", count
	})
}

func createConnectionWithNatsOpts(t tLogger, clientName string,
	natsOpts ...nats.Option) (stan.Conn, *nats.Conn) {
	opts := nats.DefaultOptions
	opts.Servers = []string{nats.DefaultURL}
	for _, opt := range natsOpts {
		if err := opt(&opts); err != nil {
			stackFatalf(t, "Unexpected error on setting options: %v", err)
		}
	}
	nc, err := opts.Connect()
	if err != nil {
		stackFatalf(t, "Unexpected error on connect: %v", err)
	}
	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		nc.Close()
		stackFatalf(t, "Unexpected error on connect: %v", err)
	}
	return sc, nc
}

func NewDefaultConnection(t tLogger) stan.Conn {
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	return sc
}

func cleanupDatastore(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		stackFatalf(t, "Error cleaning up datastore: %v", err)
	}
}

// Dumb wait program to sync on callbacks, etc... Will timeout
func Wait(ch chan bool) error {
	return WaitTime(ch, 5*time.Second)
}

func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

// RunServerWithDebugTrace is a helper to assist debugging
func RunServerWithDebugTrace(opts *Options, enableDebug, enableTrace bool) *StanServer {
	var sOpts *Options

	if opts == nil {
		sOpts = GetDefaultOptions()
	} else {
		sOpts = opts
	}

	nOpts := natsd.Options{}

	sOpts.Debug = enableDebug
	sOpts.Trace = enableTrace
	nOpts.NoLog = false

	ConfigureLogger(sOpts, &nOpts)

	return RunServerWithOpts(sOpts, nil)
}

func TestRunServer(t *testing.T) {
	// Test passing nil options
	s := RunServerWithOpts(nil, nil)
	s.Shutdown()

	// Test passing stan options, nil nats options
	opts := GetDefaultOptions()
	s = RunServerWithOpts(opts, nil)
	defer s.Shutdown()
	clusterID := s.ClusterID()

	if clusterID != clusterName {
		t.Fatalf("Expected cluster ID of %s, found %s\n", clusterName, clusterID)
	}
	s.Shutdown()

	// Test passing nil stan options, some nats options
	nOpts := &natsd.Options{}
	nOpts.NoLog = true
	s = RunServerWithOpts(nil, nOpts)
	defer s.Shutdown()
}

func TestDefaultOptions(t *testing.T) {

	opts := GetDefaultOptions()
	opts.Debug = !defaultOptions.Debug

	opts2 := GetDefaultOptions()
	if opts2.Debug == opts.Debug {
		t.Fatal("Modified original default options.")
	}
}

func TestDoubleShutdown(t *testing.T) {
	s := RunServer(clusterName)
	s.Shutdown()

	ch := make(chan bool)

	go func() {
		s.Shutdown()
		ch <- true
	}()

	if err := Wait(ch); err != nil {
		t.Fatal("Second shutdown blocked")
	}
}

type response interface {
	Unmarshal([]byte) error
}

func checkServerResponse(nc *nats.Conn, subj string, expectedError error, r response) error {
	resp, err := nc.Request(subj, []byte("dummy"), time.Second)
	if err != nil {
		return fmt.Errorf("Unexpected error on publishing request: %v", err)
	}
	if err := r.Unmarshal(resp.Data); err != nil {
		return fmt.Errorf("Unexpected response object: %v", err)
	}
	// All our protos have the Error field.
	v := reflect.Indirect(reflect.ValueOf(r))
	f := v.FieldByName("Error")
	if !f.IsValid() {
		return fmt.Errorf("Field Error not found in the response: %v", f)
	}
	connErr := f.String()
	if connErr != expectedError.Error() {
		return fmt.Errorf("Expected response to be %q, got %q", expectedError.Error(), connErr)
	}
	return nil
}

func TestInvalidRequests(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	// Use a bare NATS connection to send incorrect requests
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	// Send a dummy message on the STAN connect subject
	// Get the connect subject
	connSubj := fmt.Sprintf("%s.%s", s.opts.DiscoverPrefix, clusterName)
	if err := checkServerResponse(nc, connSubj, ErrInvalidConnReq,
		&pb.ConnectResponse{}); err != nil {
		t.Fatalf("%v", err)
	}

	// Send a dummy message on the STAN publish subject
	if err := checkServerResponse(nc, s.info.Publish+".foo", ErrInvalidPubReq,
		&pb.PubAck{}); err != nil {
		t.Fatalf("%v", err)
	}

	// Send a dummy message on the STAN subscription init subject
	if err := checkServerResponse(nc, s.info.Subscribe, ErrInvalidSubReq,
		&pb.SubscriptionResponse{}); err != nil {
		t.Fatalf("%v", err)
	}

	// Send a dummy message on the STAN subscription unsub subject
	if err := checkServerResponse(nc, s.info.Unsubscribe, ErrInvalidUnsubReq,
		&pb.SubscriptionResponse{}); err != nil {
		t.Fatalf("%v", err)
	}

	// Send a dummy message on the STAN close subject
	if err := checkServerResponse(nc, s.info.Close, ErrInvalidCloseReq,
		&pb.CloseResponse{}); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestClientIDIsValid(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	// Use a bare NATS connection to send incorrect requests
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	// Get the connect subject
	connSubj := fmt.Sprintf("%s.%s", s.opts.DiscoverPrefix, clusterName)

	invalidClientIDs := []string{"", "id with spaces", "id:with:columns",
		"id,with,commas", "id.with.dots", "id with spaces, commas and: columns and dots.",
		"idWithLotsOfNotAllowedCharacters!@#$%^&*()"}

	for _, cID := range invalidClientIDs {
		req := &pb.ConnectRequest{ClientID: cID, HeartbeatInbox: "hbInbox"}
		b, _ := req.Marshal()

		resp, err := nc.Request(connSubj, b, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error on publishing request: %v", err)
		}
		r := &pb.ConnectResponse{}
		err = r.Unmarshal(resp.Data)
		if err != nil {
			t.Fatalf("Unexpected response object: %v", err)
		}
		if r.Error == "" {
			t.Fatal("Expected error, got none")
		}
	}

	validClientIDs := []string{"id", "id_with_underscores", "id-with-hypens"}

	for _, cID := range validClientIDs {
		req := &pb.ConnectRequest{ClientID: cID, HeartbeatInbox: "hbInbox"}
		b, _ := req.Marshal()

		resp, err := nc.Request(connSubj, b, time.Second)
		if err != nil {
			t.Fatalf("Unexpected error on publishing request: %v", err)
		}
		r := &pb.ConnectResponse{}
		err = r.Unmarshal(resp.Data)
		if err != nil {
			t.Fatalf("Unexpected response object: %v", err)
		}
		if r.Error != "" {
			t.Fatalf("Unexpected response error: %v", r.Error)
		}
	}
}

func sendInvalidSubRequest(s *StanServer, nc *nats.Conn, req *pb.SubscriptionRequest) error {
	b, err := req.Marshal()
	if err != nil {
		return fmt.Errorf("Error during marshal: %v", err)
	}
	rep, err := nc.Request(s.info.Subscribe, b, time.Second)
	if err != nil {
		return fmt.Errorf("Unexpected error: %v", err)
	}
	// Check response
	subRep := &pb.SubscriptionResponse{}
	subRep.Unmarshal(rep.Data)

	// Expect error
	if subRep.Error == "" {
		return fmt.Errorf("Expected error, got none")
	}
	return nil
}

func TestInvalidSubRequest(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	// Use a bare NATS connection to send incorrect requests
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	// Create empty request
	req := &pb.SubscriptionRequest{}

	// Send this empty request
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// Add a valid ackWait
	req.AckWaitInSecs = 3

	// Set invalid subject
	req.Subject = "foo*.bar"
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}
	// Other kinds of invalid subject
	req.Subject = "foo.bar*"
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}
	req.Subject = "foo.>.*"
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// Set valid subject, still no client ID specified
	req.Subject = "foo"
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// Set ClientID, should complain that it does not know about clientName
	req.ClientID = clientName
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// TODO: This may change if we fix startSequenceValid
	// Set a start position that we don't have
	req.StartPosition = pb.StartPosition_SequenceStart
	req.StartSequence = 100
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// TODO: This may change if we fix startTimeValid
	// Set a start position that we don't have
	req.StartPosition = pb.StartPosition_TimeDeltaStart
	req.StartTimeDelta = int64(10 * time.Second)
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	req.StartPosition = pb.StartPosition_NewOnly
	// Set DurableName and QGroup
	req.DurableName = "mydur"
	req.QGroup = "mygroup"
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// A durable
	req.DurableName = "mydur"
	req.QGroup = ""
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// A queue subscriber
	req.DurableName = ""
	req.QGroup = "mygroup"
	if err := sendInvalidSubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// There should be no client created
	checkClients(t, s, 0)

	// But channel "foo" should have been created though
	if !s.store.HasChannel() {
		t.Fatal("Expected channel foo to have been created")
	}
}

func sendInvalidUnsubRequest(s *StanServer, nc *nats.Conn, req *pb.UnsubscribeRequest) error {
	b, err := req.Marshal()
	if err != nil {
		return fmt.Errorf("Error during marshal: %v", err)
	}
	rep, err := nc.Request(s.info.Unsubscribe, b, time.Second)
	if err != nil {
		return fmt.Errorf("Unexpected error: %v", err)
	}
	// Check response
	subRep := &pb.SubscriptionResponse{}
	subRep.Unmarshal(rep.Data)

	// Expect error
	if subRep.Error == "" {
		return fmt.Errorf("Expected error, got none")
	}
	return nil
}

func TestInvalidUnsubRequest(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	// Use a bare NATS connection to send incorrect requests
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}

	// Create a valid subscription first
	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Verify server state. Client should be created
	client := s.clients.Lookup(clientName)
	if client == nil {
		t.Fatal("A client should have been created")
	}
	subs := checkSubs(t, s, clientName, 1)

	// Create empty request
	req := &pb.UnsubscribeRequest{}

	// Send this empty request
	if err := sendInvalidUnsubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// Unsubscribe for a subject we did not subscribe to
	req.Subject = "bar"
	if err := sendInvalidUnsubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// Invalid ack inbox
	req.Subject = "foo"
	req.ClientID = clientName
	req.Inbox = "wrong"
	if err := sendInvalidUnsubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// Correct subject, inbox, but invalid ClientID
	req.Subject = "foo"
	req.Inbox = subs[0].AckInbox
	req.ClientID = "wrong"
	if err := sendInvalidUnsubRequest(s, nc, req); err != nil {
		t.Fatalf("%v", err)
	}

	// Valid unsubscribe
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v\n", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Check that sub's has been removed.
	subs = checkSubs(t, s, clientName, 0)
}

func TestDuplicateClientIDs(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	c1 := NewDefaultConnection(t)
	defer c1.Close()

	if c2, err := stan.Connect(clusterName, clientName); err == nil || err.Error() != ErrInvalidClient.Error() {
		if c2 != nil {
			c2.Close()
		}
		t.Fatalf("Expected to get error %q, got %q", ErrInvalidClient, err)
	}

	// Check that there only one client registered
	checkClients(t, s, 1)
}

func TestRedelivery(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer sc.Close()

	rch := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
			rch <- true
		}
	}

	// Create a plain sub
	if _, err := sc.Subscribe("foo", cb, stan.SetManualAckMode(),
		stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Send first message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Add a delay before the next message
	time.Sleep(500 * time.Millisecond)
	// Send second message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	subs := checkSubs(t, s, clientName, 1)
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if sub.acksPending == nil || len(sub.acksPending) != 2 {
			t.Fatalf("Expected to have two ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer == nil {
			t.Fatalf("Expected timer to be set")
		}
	}(subs[0])

	for i := 0; i < 2; i++ {
		if err := Wait(rch); err != nil {
			t.Fatalf("Messages not redelivered")
		}
	}

	// Wait for another ackWait to check if timer is cleared
	time.Sleep(1250 * time.Millisecond)

	// Check state
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if len(sub.acksPending) != 0 {
			t.Fatalf("Expected to have no ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer != nil {
			t.Fatalf("Expected timer to be nil")
		}
	}(subs[0])
}

func TestRedeliveryRace(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.AckWait(time.Second), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	time.Sleep(time.Second)
	sub.Unsubscribe()
}

func TestQueueRedelivery(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer sc.Close()

	rch := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
			rch <- true
		}
	}

	// Create a queue subscriber
	if _, err := sc.QueueSubscribe("foo", "group", cb, stan.SetManualAckMode(),
		stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Send first message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Add a delay before the next message
	time.Sleep(500 * time.Millisecond)
	// Send second message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	subs := checkSubs(t, s, clientName, 1)
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if sub.acksPending == nil || len(sub.acksPending) != 2 {
			t.Fatalf("Expected to have two ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer == nil {
			t.Fatalf("Expected timer to be set")
		}
	}(subs[0])

	for i := 0; i < 2; i++ {
		if err := Wait(rch); err != nil {
			t.Fatalf("Messages not redelivered")
		}
	}

	// Wait for another ackWait to check if timer is cleared
	time.Sleep(1250 * time.Millisecond)

	// Check state
	func(sub *subState) {
		sub.RLock()
		defer sub.RUnlock()
		if len(sub.acksPending) != 0 {
			t.Fatalf("Expected to have no ackPending, got %v", len(sub.acksPending))
		}
		if sub.ackTimer != nil {
			t.Fatalf("Expected timer to be nil")
		}
	}(subs[0])
}

func TestDurableRedelivery(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	ch := make(chan bool)
	rch := make(chan bool)
	errors := make(chan error, 5)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		switch count {
		case 1:
			ch <- true
		case 2:
			rch <- true
		default:
			errors <- fmt.Errorf("Unexpected message %v", m)
		}
	}

	sc := NewDefaultConnection(t)
	defer sc.Close()

	_, err := sc.Subscribe("foo", cb, stan.DurableName("dur"), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for first message to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Failed to receive first message")
	}

	// Report error if any
	if len(errors) > 0 {
		t.Fatalf("%v", <-errors)
	}

	// Close the client
	sc.Close()

	// Restart client
	sc2 := NewDefaultConnection(t)
	defer sc2.Close()

	sub2, err := sc2.Subscribe("foo", cb, stan.DurableName("dur"), stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// Wait for redelivered message
	if err := Wait(rch); err != nil {
		t.Fatal("Messages were not redelivered to durable")
	}

	// Report error if any
	if len(errors) > 0 {
		t.Fatalf("%v", <-errors)
	}
}

func TestDurableRestartWithMaxAckInFlight(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	maxAckPending := 100
	stopDurAt := 10
	total := stopDurAt + maxAckPending + 100

	// Send all messages
	sc := NewDefaultConnection(t)
	defer sc.Close()
	msg := []byte("hello")
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	// Have a cb that stop the connection after a certain amount of
	// messages received
	count := 0
	ch := make(chan bool)
	cb := func(_ *stan.Msg) {
		count++
		if count == stopDurAt || count == total {
			sc.Close()
			ch <- true
		}
	}
	// Start the durable
	_, err := sc.Subscribe("foo", cb, stan.DurableName("dur"),
		stan.MaxInflight(maxAckPending), stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for the connection to be closed after receiving
	// a certain amount of messages.
	if err := Wait(ch); err != nil {
		t.Fatal("Waited too long for the messages to be received")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Restart the durable
	_, err = sc.Subscribe("foo", cb, stan.DurableName("dur"),
		stan.MaxInflight(maxAckPending), stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for all messages to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Waited too long for the messages to be received")
	}
}

func testStalledDelivery(t *testing.T, typeSub string) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	lastMsgSentTime := int64(0)
	ackDelay := 700 * time.Millisecond
	toSend := int32(2)
	numSubs := 1

	ch := make(chan bool)
	errors := make(chan error)
	acked := int32(0)

	cb := func(m *stan.Msg) {
		// No message should be redelivered because of MaxInFlight==1
		if m.Redelivered {
			errors <- fmt.Errorf("Unexpected message redelivered")
			return
		}
		go func() {
			time.Sleep(ackDelay)
			m.Ack()
			if atomic.AddInt32(&acked, 1) == toSend {
				ch <- true
			}
		}()
		if int32(m.Sequence) == toSend {
			now := time.Now().UnixNano()
			sent := atomic.LoadInt64(&lastMsgSentTime)
			elapsed := time.Duration(now - sent)
			if elapsed < ackDelay-20*time.Millisecond {
				errors <- fmt.Errorf("Second message received too soon: %v", elapsed)
				return
			}
		}
	}
	if typeSub == "queue" {
		// Create 2 queue subs with manual ack mode and maxInFlight of 1.
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		numSubs = 2
		toSend = 3
	} else if typeSub == "durable" {
		// Create a durable with manual ack mode and maxInFlight of 1.
		if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur"),
			stan.SetManualAckMode(), stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	} else {
		// Create a sub with manual ack mode and maxInFlight of 1.
		if _, err := sc.Subscribe("foo", cb, stan.SetManualAckMode(),
			stan.MaxInflight(1)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}
	// Wait for subscriber to be registered before starting publish
	waitForNumSubs(t, s, clientName, numSubs)
	// Send our messages
	for i := int32(0); i < toSend; i++ {
		if i == toSend-1 {
			atomic.AddInt64(&lastMsgSentTime, time.Now().UnixNano())
		}
		msg := fmt.Sprintf("msg_%d", (i + 1))
		if err := sc.Publish("foo", []byte(msg)); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Wait for completion or error
	select {
	case <-ch:
		break
	case e := <-errors:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our messages")
	}
}

func TestStalledDelivery(t *testing.T) {
	testStalledDelivery(t, "sub")
}

func TestStalledQueueDelivery(t *testing.T) {
	testStalledDelivery(t, "queue")
}

func TestStalledDurableDelivery(t *testing.T) {
	testStalledDelivery(t, "durable")
}

func testStalledRedelivery(t *testing.T, typeSub string) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Override maxStalledRedelivery
	setMaxStalledRedeliveries(1)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	recv := int32(0)
	rdlv := int32(0)
	toSend := int32(1)
	numSubs := 1

	ch := make(chan bool)
	rch := make(chan bool, 2)
	errors := make(chan error)

	cb := func(m *stan.Msg) {
		if !m.Redelivered {
			r := atomic.AddInt32(&recv, 1)
			if r > toSend {
				errors <- fmt.Errorf("Should have received only 1 message, got %v", r)
				return
			} else if r == toSend {
				ch <- true
			}
		} else {
			m.Ack()
			// We have received our redelivered message(s), we're done
			if atomic.AddInt32(&rdlv, 1) == toSend {
				rch <- true
			}
		}
	}
	if typeSub == "queue" {
		// Create 2 queue subs with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		if _, err := sc.QueueSubscribe("foo", "group", cb,
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		numSubs = 2
		toSend = 2
	} else if typeSub == "durable" {
		// Create a durable with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur"),
			stan.SetManualAckMode(), stan.MaxInflight(1),
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	} else {
		// Create a sub with manual ack mode and maxInFlight of 1,
		// and redelivery delay of 1 sec.
		if _, err := sc.Subscribe("foo", cb, stan.SetManualAckMode(),
			stan.MaxInflight(1), stan.AckWait(time.Second),
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}
	// Wait for subscriber to be registered before starting publish
	waitForNumSubs(t, s, clientName, numSubs)
	// Send
	for i := int32(0); i < toSend; i++ {
		msg := fmt.Sprintf("msg_%d", (i + 1))
		if err := sc.Publish("foo", []byte(msg)); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Make sure the message is received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Wait for completion or error
	select {
	case <-rch:
		break
	case e := <-errors:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our redelivered message")
	}
	// Same test but with server restart.
	atomic.StoreInt32(&recv, 0)
	atomic.StoreInt32(&rdlv, 0)
	// Send
	for i := int32(0); i < toSend; i++ {
		msg := fmt.Sprintf("msg_%d", (i + 1))
		if err := sc.Publish("foo", []byte(msg)); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Make sure the message is received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart server
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)
	// Wait for completion or error
	select {
	case <-rch:
		break
	case e := <-errors:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our redelivered message")
	}
}

func TestStalledRedelivery(t *testing.T) {
	testStalledRedelivery(t, "sub")
}

func TestStalledQueueRedelivery(t *testing.T) {
	testStalledRedelivery(t, "queue")
}

func TestStalledDurableRedelivery(t *testing.T) {
	testStalledRedelivery(t, "durable")
}

func TestTooManyChannelsOnCreateSub(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxChannels = 1
	s := RunServerWithOpts(sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// That should create channel foo
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// This should fail because we reached the limit
	if _, err := sc.Subscribe("bar", func(_ *stan.Msg) {}); err == nil {
		t.Fatalf("Expected error due to too many channels, got none")
	}
}

func TestTooManyChannelsOnPublish(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxChannels = 1
	s := RunServerWithOpts(sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// That should create channel foo
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// This should fail since we reached the max channels limit
	if err := sc.Publish("bar", []byte("hello")); err == nil {
		t.Fatalf("Expected error due to too many channels, got none")
	}

	// Check that channel bar was not created
	if s.store.LookupChannel("bar") != nil {
		t.Fatal("Channel bar should not have been created")
	}
}

func TestTooManySubs(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxSubscriptions = 1
	s := RunServerWithOpts(sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// This should be ok
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// We should get an error here
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err == nil {
		t.Fatal("Expected error on subscribe, go none")
	}
	cs := s.store.LookupChannel("foo")
	if cs == nil || cs.UserData == nil {
		t.Fatal("Expected channel to exist")
	}
	ss := cs.UserData.(*subStore)
	func() {
		ss.RLock()
		defer ss.RUnlock()
		if ss.psubs == nil || len(ss.psubs) != 1 {
			t.Fatalf("Expected only one subscription, got %v", len(ss.psubs))
		}
	}()
}

func TestMaxMsgs(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxMsgs = 10
	s := RunServerWithOpts(sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	for i := 0; i < 2*sOpts.MaxMsgs; i++ {
		sc.Publish("foo", []byte("msg"))
	}

	// We should not have more than MaxMsgs
	cs := s.store.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	n, _, err := cs.Msgs.State()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	if n != sOpts.MaxMsgs {
		t.Fatalf("Expected msgs count to be %v, got %v", sOpts.MaxMsgs, n)
	}
}

func TestMaxBytes(t *testing.T) {
	payload := []byte("hello")
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxBytes = uint64(len(payload) * 10)
	s := RunServerWithOpts(sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	count := 2 * (int(sOpts.MaxBytes) / len(payload))
	for i := 0; i < count; i++ {
		sc.Publish("foo", payload)
	}

	// We should not have more than MaxMsgs
	cs := s.store.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	_, b, err := cs.Msgs.State()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	if b != sOpts.MaxBytes {
		t.Fatalf("Expected msgs size to be %v, got %v", sOpts.MaxBytes, b)
	}
}

func TestRunServerWithFileStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	// Create our own NATS connection to control reconnect wait
	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(50*time.Millisecond), nats.MaxReconnects(500))
	defer nc.Close()
	defer sc.Close()

	rch := make(chan bool)
	delivered := int32(0)
	redelivered := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			atomic.AddInt32(&redelivered, 1)
		} else {
			if atomic.AddInt32(&delivered, 1) == 3 {
				rch <- true
			}
		}
	}

	// 2 Queue subscribers on bar
	if _, err := sc.QueueSubscribe("bar", "group", cb); err != nil {
		t.Fatalf("Unexpected error on queue subscribe: %v", err)
	}
	if _, err := sc.QueueSubscribe("bar", "group", cb); err != nil {
		t.Fatalf("Unexpected error on queue subscribe: %v", err)
	}
	// 1 Durable on baz
	if _, err := sc.Subscribe("baz", cb, stan.DurableName("mydur")); err != nil {
		t.Fatalf("Unexpected error on durable subscribe: %v", err)
	}
	// 1 Plain subscriber on foo
	if _, err := sc.Subscribe("foo", cb); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Wait for all subscriptions to be processed by the server
	waitForNumSubs(t, s, clientName, 4)

	// Publish some messages.
	if err := sc.Publish("bar", []byte("Msg for bar")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("baz", []byte("Msg for baz")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("foo", []byte("Msg for foo")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for all 3 messages
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our messages")
	}
	// There should be no redelivered message
	if r := atomic.LoadInt32(&redelivered); r != 0 {
		t.Fatalf("There should be no redelivered message, got %v", r)
	}

	// Wait a bit for the acks to be processed
	time.Sleep(50 * time.Millisecond)

	// Shutdown server
	s.Shutdown()

	// Reset delivered count
	atomic.StoreInt32(&delivered, 0)

	// Recover
	s = RunServerWithOpts(opts, nil)

	// Check server recovered state
	// Should be 1 client
	checkClients(t, s, 1)

	// Should be 4 subscriptions
	checkSubs(t, s, clientName, 4)

	// helper to check that there is no ack pending
	checkNoAckPending := func(sub *subState) {
		sub.RLock()
		lap := len(sub.acksPending)
		sub.RUnlock()
		if lap != 0 {
			t.Fatalf("Server shutdown too soon? Unexpected un-ack'ed messages: %v", lap)
		}
	}

	// Check details now.
	// 2 Queue subscribers on bar
	cs := s.store.LookupChannel("bar")
	if cs == nil || cs.UserData == nil {
		t.Fatal("Expected channel bar to exist")
	}
	func() {
		ss := cs.UserData.(*subStore)
		ss.RLock()
		defer ss.RUnlock()
		if len(ss.durables) != 0 {
			t.Fatalf("Unexpected durables for bar: %v", len(ss.durables))
		}
		if len(ss.psubs) != 0 {
			t.Fatalf("Unexpected plain subscribers for bar: %v", len(ss.psubs))
		}
		if len(ss.qsubs) != 1 {
			t.Fatalf("Expected one queue group for bar, got: %v", len(ss.qsubs))
		}
		qs := ss.qsubs["group"]
		if qs == nil {
			t.Fatal("Expected to get a queue state")
		}
		qs.RLock()
		qsubs := qs.subs
		qs.RUnlock()
		if qsubs == nil || len(qsubs) != 2 {
			t.Fatalf("Unexpected number of queue subscribers of group 'group' for channel bar, got: %v", len(qsubs))
		}
		// Check for the two queue subscribers
		for _, sub := range qsubs {
			checkNoAckPending(sub)
		}
	}()

	// One durable on baz
	cs = s.store.LookupChannel("baz")
	if cs == nil || cs.UserData == nil {
		t.Fatal("Expected channel baz to exist")
	}
	func() {
		ss := cs.UserData.(*subStore)
		ss.RLock()
		defer ss.RUnlock()
		if len(ss.durables) != 1 {
			t.Fatalf("Expected one durable for baz: %v", len(ss.durables))
		}
		// Durables are both in plain subs and durables
		if len(ss.psubs) != 1 {
			t.Fatalf("Unexpected plain subscribers for baz: %v", len(ss.psubs))
		}
		if len(ss.qsubs) != 0 {
			t.Fatalf("Unexpected queue groups for baz, got: %v", len(ss.qsubs))
		}
		checkNoAckPending(ss.psubs[0])
	}()

	// One plain subscriber on foo
	cs = s.store.LookupChannel("foo")
	if cs == nil || cs.UserData == nil {
		t.Fatal("Expected channel foo to exist")
	}
	func() {
		ss := cs.UserData.(*subStore)
		ss.RLock()
		defer ss.RUnlock()
		if len(ss.durables) != 0 {
			t.Fatalf("Unexpected durables for foo: %v", len(ss.durables))
		}
		if len(ss.psubs) != 1 {
			t.Fatalf("Expected 1 plain subscriber for foo: %v", len(ss.psubs))
		}
		if len(ss.qsubs) != 0 {
			t.Fatalf("Unexpected queue subscribers for foo, got: %v", len(ss.qsubs))
		}
		checkNoAckPending(ss.psubs[0])
	}()

	// Since we use the same connection to send new messages,
	// we don't have to explicitly wait that the client has
	// reconnected (sends are buffered and flushed on reconnect)

	// Send new messages, should be received.
	if err := sc.Publish("bar", []byte("New Msg for bar")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("baz", []byte("New Msg for baz")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("foo", []byte("New Msg for foo")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for the new messages
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our messages")
	}
	// There should be no redelivered message
	if r := atomic.LoadInt32(&redelivered); r != 0 {
		t.Fatalf("There should be no redelivered message, got %v", r)
	}
}

func checkDurable(t *testing.T, s *StanServer, channel, durName, durKey string) {
	c := s.clients.Lookup(clientName)
	if c == nil {
		stackFatalf(t, "Expected client %v to be registered", clientName)
	}
	c.RLock()
	subs := c.subs
	c.RUnlock()
	if len(subs) != 1 {
		stackFatalf(t, "Expected 1 sub, got %v", len(subs))
	}
	sub := subs[0]
	if sub.DurableName != durName {
		stackFatalf(t, "Expected durable name %v, got %v", durName, sub.DurableName)
	}
	// Check that durable is also in subStore
	cs := s.store.LookupChannel(channel)
	if cs == nil {
		stackFatalf(t, "Expected channel %q to be created", channel)
	}
	ss := cs.UserData.(*subStore)
	ss.RLock()
	durInSS := ss.durables[durKey]
	ss.RUnlock()
	if durInSS == nil || durInSS.DurableName != durName {
		stackFatalf(t, "Expected durable to be in subStore")
	}
}

func TestDurableCanReconnect(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	cb := func(_ *stan.Msg) {}

	durName := "mydur"
	sr := &pb.SubscriptionRequest{
		ClientID:    clientName,
		Subject:     "foo",
		DurableName: durName,
	}
	durKey := durableKey(sr)

	// Create durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is created
	checkDurable(t, s, "foo", durName, durKey)

	// We should not be able to create a second durable on same subject
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err == nil {
		t.Fatal("Expected to fail to create a second durable with same name")
	}

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)
}

func TestDurableAckedMsgNotRedelivered(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Make a channel big enough so that we don't block
	msgs := make(chan *stan.Msg, 10)

	cb := func(m *stan.Msg) {
		msgs <- m
	}

	durName := "mydur"
	sr := &pb.SubscriptionRequest{
		ClientID:    clientName,
		Subject:     "foo",
		DurableName: durName,
	}
	durKey := durableKey(sr)

	// Create durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is created
	checkDurable(t, s, "foo", durName, durKey)

	// We verified that there is 1 sub, and this is our durable.
	subs := s.clients.GetSubs(clientName)
	durable := subs[0]
	durable.RLock()
	// Get the AckInbox.
	ackInbox := durable.AckInbox
	// Get the ack subscriber
	ackSub := durable.ackSub
	durable.RUnlock()

	// Send a message
	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify message is acked.
	checkDurableNoPendingAck(t, s, true, ackInbox, ackSub, 1)

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Send a second message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Verify that we have different AckInbox and ackSub and message is acked.
	checkDurableNoPendingAck(t, s, false, ackInbox, ackSub, 2)

	// Close stan connection
	sc.Close()

	// Connect again
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Start the durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName(durName)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is found
	checkDurable(t, s, "foo", durName, durKey)

	// Verify that we have different AckInbox and ackSub and message is acked.
	checkDurableNoPendingAck(t, s, false, ackInbox, ackSub, 2)

	numMsgs := len(msgs)
	if numMsgs > 2 {
		t.Fatalf("Expected only 2 messages to be delivered, got %v", numMsgs)
	}
	for i := 0; i < numMsgs; i++ {
		m := <-msgs
		if m.Redelivered {
			t.Fatal("Unexpected redelivered message")
		}
		if m.Sequence != uint64(i+1) {
			t.Fatalf("Expected message %v's sequence to be %v, got %v", (i + 1), (i + 1), m.Sequence)
		}
	}
}

func checkDurableNoPendingAck(t *testing.T, s *StanServer, isSame bool,
	ackInbox string, ackSub *nats.Subscription, expectedSeq uint64) {
	// When called, we know that there is 1 sub, and the sub is a durable.
	subs := s.clients.GetSubs(clientName)
	durable := subs[0]
	durable.RLock()
	durAckInbox := durable.AckInbox
	durAckSub := durable.ackSub
	durable.RUnlock()

	if isSame {
		if durAckInbox != ackInbox {
			stackFatalf(t, "Expected ackInbox %v, got %v", ackInbox, durAckInbox)
		}
		if durAckSub != ackSub {
			stackFatalf(t, "Expected subscriber on ack to be %p, got %p", ackSub, durAckSub)
		}
	} else {
		if durAckInbox == ackInbox {
			stackFatalf(t, "Expected different ackInbox'es")
		}
		if durAckSub == ackSub {
			stackFatalf(t, "Expected different ackSub")
		}
	}

	limit := time.Now().Add(5 * time.Second)
	for time.Now().Before(limit) {
		durable.RLock()
		lastSent := durable.LastSent
		acks := len(durable.acksPending)
		durable.RUnlock()

		if lastSent != expectedSeq || acks > 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// We are ok
		return
	}
	stackFatalf(t, "Message was not acknowledged")
}

func TestClientCrashAndReconnect(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// should get a duplicate clientID error
	if sc2, err := stan.Connect(clusterName, clientName); err == nil {
		sc2.Close()
		t.Fatal("Expected to be unable to connect")
	}

	// kill the NATS conn
	nc.Close()

	// Since the original client won't respond to a ping, we should
	// be able to connect, and it should not take too long.
	start := time.Now()

	// should succeed
	if sc2, err := stan.Connect(clusterName, clientName); err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	} else {
		defer sc2.Close()
	}

	duration := time.Now().Sub(start)
	if duration > 5*time.Second {
		t.Fatalf("Took too long to be able to connect: %v", duration)
	}
}

func TestStartPositionNewOnly(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(_ *stan.Msg) {
		rch <- true
	}

	// Start a subscriber with "NewOnly" as start position.
	// Since there was no message previously sent, it should
	// not receive anything yet.
	sub, err := sc.Subscribe("foo", cb, stan.StartAt(pb.StartPosition_NewOnly))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait a little bit and ensure no message was received
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}

	// Send a message now.
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}

	// Start another subscriber with "NewOnly" as start position.
	sub2, err := sc.Subscribe("foo", cb, stan.StartAt(pb.StartPosition_NewOnly))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// It should not receive anything
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}
}

func TestStartPositionLastReceived(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(_ *stan.Msg) {
		rch <- true
	}

	// Start a subscriber with "LastReceived" as start position.
	// Since there was no message previously sent, it should
	// not receive anything yet.
	sub, err := sc.Subscribe("foo", cb, stan.StartWithLastReceived())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait a little bit and ensure no message was received
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}

	// Send a message now.
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}

	rch = make(chan bool)

	cb = func(m *stan.Msg) {
		if string(m.Data) == "msg2" {
			rch <- true
		}
	}

	// Send two messages
	if err := sc.Publish("bar", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("bar", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Start a subscriber with "LastReceived" as start position.
	sub2, err := sc.Subscribe("bar", cb, stan.StartWithLastReceived())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// The second message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
}

func TestStartPositionFirstSequence(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(_ *stan.Msg) {
		rch <- true
	}

	// Start a subscriber with "FirstSequence" as start position.
	// Since there was no message previously sent, it should
	// not receive anything yet.
	sub, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait a little bit and ensure no message was received
	if err := WaitTime(rch, 500*time.Millisecond); err == nil {
		t.Fatal("No message should have been received")
	}

	// Send a message now.
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}

	mch := make(chan *stan.Msg, 2)

	cb = func(m *stan.Msg) {
		mch <- m
	}

	// Send two messages
	if err := sc.Publish("bar", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc.Publish("bar", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Start a subscriber with "FirstPosition" as start position.
	sub2, err := sc.Subscribe("bar", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	first := true
	select {
	case m := <-mch:
		if first {
			if string(m.Data) != "msg1" {
				t.Fatalf("Expected msg1 first, got %v", string(m.Data))
			}
			first = false
		} else {
			if string(m.Data) != "msg2" {
				t.Fatalf("Expected msg2 second, got %v", string(m.Data))
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our message")
	}
}

func TestStartPositionSequenceStart(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(_ *stan.Msg) {
		rch <- true
	}

	// Start a subscriber with "Sequence" as start position.
	// As of now, since there is no message, the call will fail.
	sub, err := sc.Subscribe("foo", cb, stan.StartAtSequence(0))
	if err == nil {
		sub.Unsubscribe()
		t.Fatal("Expected error on subscribe, got none")
	}

	// Send a message now.
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Create a new subscriber with "Sequence" 1
	sub2, err := sc.Subscribe("foo", cb, stan.StartAtSequence(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub2.Unsubscribe()

	// Message should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
}

func TestStartPositionTimeDelta(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

	cb := func(m *stan.Msg) {
		if string(m.Data) == "msg2" {
			rch <- true
		}
	}

	//FIXME(ik): As of now, start at a time delta when no message
	// has been stored would return an error. So test only with
	// messages present.
	// Send a message.
	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait 1.5 seconds.
	time.Sleep(1500 * time.Millisecond)

	// Sends a second message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Start a subscriber with "TimeDelta" as start position.
	sub, err := sc.Subscribe("foo", cb, stan.StartAtTimeDelta(1*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Message 2 should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
}

func TestIgnoreRecoveredSubForUnknownClientID(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// For delete the client
	s.clients.Unregister(clientName)

	// Shutdown the server
	s.Shutdown()

	// Restart the server
	s = RunServerWithOpts(opts, nil)

	// Check that client does not exist
	if s.clients.Lookup(clientName) != nil {
		t.Fatal("Client should not have been recovered")
	}
	// Channel would be recovered
	cs := s.store.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should have been recovered")
	}
	// But there should not be any subscription
	ss := cs.UserData.(*subStore)
	ss.RLock()
	numSubs := len(ss.psubs)
	ss.RUnlock()
	if numSubs > 0 {
		t.Fatalf("Should not have restored subscriptions, got %v", numSubs)
	}
}

func TestCheckClientHealth(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	// Override HB settings
	s.Lock()
	s.hbInterval = 50 * time.Millisecond
	s.hbTimeout = 10 * time.Millisecond
	s.maxFailedHB = 5
	s.Unlock()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Wait for client to be registered
	waitForNumClients(t, s, 1)

	// Check that client is not incorrectly purged
	time.Sleep(time.Duration(s.maxFailedHB)*(s.hbInterval+s.hbTimeout) + 100*time.Millisecond)
	// Client should still be there
	waitForNumClients(t, s, 1)

	// kill the NATS conn
	nc.Close()

	// Check that the server closes the connection
	waitForNumClients(t, s, 0)
}

func TestConnectsWithDupCID(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	// Not too small to avoid flapping tests.
	s.dupCIDTimeout = 1 * time.Second
	s.dupMaxCIDRoutines = 5
	total := int(s.dupMaxCIDRoutines)

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	dupCIDName := "dupCID"

	sc, err := stan.Connect(clusterName, dupCIDName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	defer sc.Close()

	// Close the nc connection
	nc.Close()

	var wg sync.WaitGroup

	// Channel large enough to hold all possible errors.
	errors := make(chan error, 3*total)

	dupTimeoutMin := time.Duration(float64(s.dupCIDTimeout) * 0.9)
	dupTimeoutMax := time.Duration(float64(s.dupCIDTimeout) * 1.1)

	wg.Add(1)

	connect := func(cid string, shouldFail bool) (stan.Conn, time.Duration, error) {
		start := time.Now()
		c, err := stan.Connect(clusterName, cid, stan.ConnectWait(3*s.dupCIDTimeout))
		duration := time.Now().Sub(start)
		if shouldFail {
			if c != nil {
				c.Close()
			}
			if err == nil || err == stan.ErrConnectReqTimeout {
				return nil, 0, fmt.Errorf("Connect should have failed")
			}
			return nil, duration, nil
		} else if err != nil {
			return nil, 0, err
		}
		return c, duration, nil
	}

	getErrors := func() string {
		errorsStr := ""
		numErrors := len(errors)
		for i := 0; i < numErrors; i++ {
			e := <-errors
			oneErr := fmt.Sprintf("%d: %s\n", (i + 1), e.Error())
			if i == 0 {
				errorsStr = "\n"
			}
			errorsStr = errorsStr + oneErr
		}
		return errorsStr
	}

	// Start this go routine that will try to connect 2*total-1
	// times. These all should fail (quickly) since the one
	// connecting below should be the one that connects.
	go func() {
		defer wg.Done()
		time.Sleep(s.dupCIDTimeout / 2)
		for i := 0; i < 2*total-1; i++ {
			_, duration, err := connect(dupCIDName, true)
			if err != nil {
				errors <- err
				continue
			}
			// These should fail "immediately", so consider it a failure if
			// it is close to the dupCIDTimeout
			if duration >= dupTimeoutMin {
				errors <- fmt.Errorf("Connect took too long to fail: %v", duration)
			}
		}
	}()

	// This connection on different client ID should not take long
	newConn, duration, err := connect("newCID", false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer newConn.Close()
	if duration >= dupTimeoutMin {
		t.Fatalf("Connect expected to be fast, took %v", duration)
	}

	// This one should connect, and it should take close to dupCIDTimeout
	replaceConn, duration, err := connect(dupCIDName, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer replaceConn.Close()
	if duration < dupTimeoutMin || duration > dupTimeoutMax {
		t.Fatalf("Connect expected in the range [%v-%v], took %v",
			dupTimeoutMin, dupTimeoutMax, duration)
	}

	// Wait for all other connects to complete
	wg.Wait()

	// Report possible errors
	if errs := getErrors(); errs != "" {
		t.Fatalf("Test failed: %v", errs)
	}

	// We don't need those anymore.
	newConn.Close()
	replaceConn.Close()

	// Now, let's create (total + 1) connections with different CIDs
	// and close their NATS connection. Then try to "reconnect".
	// The first (total) connections should each take about dupCIDTimeout to
	// complete.
	// The last (total + 1) connection should be delayed waiting for
	// a go routine to finish. So the expected duration - assuming that
	// they all start roughly at the same time - would be 2 * dupCIDTimeout.
	for i := 0; i < total+1; i++ {
		nc, err := nats.Connect(nats.DefaultURL)
		if err != nil {
			t.Fatalf("Unexpected error on connect: %v", err)
		}
		defer nc.Close()

		cid := fmt.Sprintf("%s_%d", dupCIDName, i)
		sc, err := stan.Connect(clusterName, cid, stan.NatsConn(nc))
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		defer sc.Close()

		// Close the nc connection
		nc.Close()
	}

	wg.Add(total + 1)

	// Need to close the connections only after the test is done
	conns := make([]stan.Conn, total+1)

	// Cleanup function
	cleanupConns := func() {
		wg.Wait()
		for _, c := range conns {
			c.Close()
		}
	}

	var delayedGuard sync.Mutex
	delayed := false

	// Connect 1 more than the max number of allowed go routines.
	for i := 0; i < total+1; i++ {
		go func(idx int) {
			defer wg.Done()
			cid := fmt.Sprintf("%s_%d", dupCIDName, idx)
			c, duration, err := connect(cid, false)
			if err != nil {
				errors <- err
				return
			}
			conns[idx] = c
			ok := false
			if duration >= dupTimeoutMin && duration <= dupTimeoutMax {
				ok = true
			}
			if !ok && duration >= 2*dupTimeoutMin && duration <= 2*dupTimeoutMax {
				delayedGuard.Lock()
				if delayed {
					delayedGuard.Unlock()
					errors <- fmt.Errorf("Failing %q, only one connection should take that long", cid)
					return
				}
				delayed = true
				delayedGuard.Unlock()
				return
			}
			if !ok {
				if duration < dupTimeoutMin || duration > dupTimeoutMax {
					errors <- fmt.Errorf("Connect with cid %q expected in the range [%v-%v], took %v",
						cid, dupTimeoutMin, dupTimeoutMax, duration)
				}
			}
		}(i)
	}

	// Wait for all routines to return
	wg.Wait()

	// Wait for other connects to complete, and close them.
	cleanupConns()

	// Report possible errors
	if errs := getErrors(); errs != "" {
		t.Fatalf("Test failed: %v", errs)
	}
}

func TestStoreTypeUnknown(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = "MyType"

	var failedServer *StanServer
	defer func() {
		if r := recover(); r == nil {
			if failedServer != nil {
				failedServer.Shutdown()
			}
			t.Fatal("Server should have failed with a panic because of unknown store type")
		}
	}()
	failedServer = RunServerWithOpts(opts, nil)
}

func TestFileStoreMissingDirectory(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = ""

	var failedServer *StanServer
	defer func() {
		if r := recover(); r == nil {
			if failedServer != nil {
				failedServer.Shutdown()
			}
			t.Fatal("Server should have failed with a panic because missing directory")
		}
	}()
	failedServer = RunServerWithOpts(opts, nil)
}

func TestFileStoreChangedClusterID(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	s.Shutdown()

	var failedServer *StanServer
	defer func() {
		if r := recover(); r == nil {
			if failedServer != nil {
				failedServer.Shutdown()
			}
			t.Fatal("Server should have failed with a panic because of different IDs")
		}
	}()
	// Change cluster ID, running the server should fail with a panic
	opts.ID = "differentID"
	failedServer = RunServerWithOpts(opts, nil)
}

func TestFileStoreRedeliveredPerSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Send one message on "foo"
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Restart server
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)

	// Message should not be marked as redelivered
	cs := s.store.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should have been recovered")
	}
	if m := cs.Msgs.FirstMsg(); m == nil || m.Redelivered {
		t.Fatal("Message should have been recovered as not redelivered")
	}

	first := make(chan bool)
	ch := make(chan bool)
	rch := make(chan bool)
	errors := make(chan error, 10)
	delivered := int32(0)
	redelivered := int32(0)

	var sub1 stan.Subscription

	cb := func(m *stan.Msg) {
		if m.Redelivered && m.Sub == sub1 {
			m.Ack()
			if atomic.AddInt32(&redelivered, 1) == 1 {
				rch <- true
			}
		} else if !m.Redelivered {
			d := atomic.AddInt32(&delivered, 1)
			switch d {
			case 1:
				first <- true
			case 2:
				ch <- true
			}
		} else {
			errors <- fmt.Errorf("Unexpected redelivered message to sub1")
		}
	}

	// Start a subscriber that consumes the message but does not ack it.
	var err error
	if sub1, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(), stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for 1st msg to be received
	if err := Wait(first); err != nil {
		t.Fatal("Did not get our first message")
	}
	// Restart server
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)

	// Client should have been recovered
	checkClients(t, s, 1)

	// There should be 1 subscription
	checkSubs(t, s, clientName, 1)

	// Now start a second subscriber that will receive the old message
	if _, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for that message to be received.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
	// Wait for the redelivered message.
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our redelivered message")
	}
	// Report error if any
	if len(errors) > 0 {
		t.Fatalf("%v", <-errors)
	}
	// There should be only 1 redelivered message
	if c := atomic.LoadInt32(&redelivered); c != 1 {
		t.Fatalf("Expected 1 redelivered message, got %v", c)
	}
}

func TestFileStoreDurableCanReceiveAfterRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		ch <- true
	}

	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Create our durable
	if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure this is registered
	waitForNumSubs(t, s, clientName, 1)
	// Close the connection
	sc.Close()

	// Restart durable
	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()
	if _, err := sc.Subscribe("foo", cb, stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure it is registered
	waitForNumSubs(t, s, clientName, 1)

	// Restart server
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)

	// Send 1 message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for message to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestFileStoreCheckClientHealthAfterRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	// Create 2 clients
	sc1, nc1 := createConnectionWithNatsOpts(t, "c1",
		nats.ReconnectWait(10*time.Second))
	defer nc1.Close()
	defer sc1.Close()

	sc2, nc2 := createConnectionWithNatsOpts(t, "c2",
		nats.ReconnectWait(10*time.Second))
	defer nc2.Close()
	defer sc2.Close()

	// Make sure they are registered
	waitForNumClients(t, s, 2)
	// Restart
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)
	// Check that there are 2 clients
	checkClients(t, s, 2)
	// Change server's hb settings
	s.hbInterval = 100 * time.Millisecond
	s.hbTimeout = 10 * time.Millisecond
	s.maxFailedHB = 2
	// Tweak their hbTimer interval to make the test short
	clients := s.store.GetClients()
	for cID, sc := range clients {
		c := sc.UserData.(*client)
		c.Lock()
		if c.hbt == nil {
			c.Unlock()
			t.Fatalf("HeartBeat Timer of client %q should have been set", cID)
		}
		c.hbt.Reset(s.hbInterval)
		c.Unlock()
	}
	// Both clients should quickly timed-out
	waitForNumClients(t, s, 0)
}

func TestFileStoreRedeliveryCbPerSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Send one message on "foo"
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	rch := make(chan bool)
	errors := make(chan error, 10)
	sub1Redel := int32(0)
	sub2Redel := int32(0)

	var sub1 stan.Subscription
	var sub2 stan.Subscription

	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
		}
		if m.Redelivered {
			if m.Sub == sub1 {
				if atomic.AddInt32(&sub1Redel, 1) > 1 {
					errors <- fmt.Errorf("More than one redeliverd msg for sub1")
					return
				}
			} else if m.Sub == sub2 {
				if atomic.AddInt32(&sub2Redel, 1) > 1 {
					errors <- fmt.Errorf("More than one redeliverd msg for sub1")
					return
				}
			} else {
				errors <- fmt.Errorf("Redelivered msg for unknown subscription")
			}
		}
		s1 := atomic.LoadInt32(&sub1Redel)
		s2 := atomic.LoadInt32(&sub2Redel)
		total := s1 + s2
		if total == 2 {
			rch <- true
		}
	}

	// Start 2 subscribers that consume the message but do not ack it.
	var err error
	if sub1, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(), stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if sub2, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.SetManualAckMode(), stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Restart server
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)

	// Client should have been recovered
	checkClients(t, s, 1)

	// There should be 2 subscriptions
	checkSubs(t, s, clientName, 2)

	// Wait for all redelivered messages.
	select {
	case e := <-errors:
		t.Fatalf("%v", e)
		break
	case <-rch:
		break
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get our redelivered messages")
	}
}

func TestFileStorePersistMsgRedeliveredToDifferentQSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	var err error
	var sub2 stan.Subscription

	errs := make(chan error, 10)
	sub2Recv := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			if m.Sub != sub2 {
				errs <- fmt.Errorf("Expected redelivered msg to be sent to sub2")
				return
			}
			sub2Recv <- true
		}
	}

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Create two queue subscribers with manual ackMode that will
	// not ack the message.
	if _, err := sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(time.Second),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create this subscriber that will receive and ack the message
	sub2, err = sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(time.Second),
		stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure these are registered.
	waitForNumSubs(t, s, clientName, 2)
	// Send a message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for sub2 to receive the message.
	select {
	case <-sub2Recv:
		break
	case e := <-errs:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get out message")
	}

	// Stop server
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)

	// Get subs
	subs := s.clients.GetSubs(clientName)
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subscriptions to be recovered, got %v", len(subs))
	}
	// Message should be in sub2's ackPending
	for _, s := range subs {
		s.RLock()
		na := len(s.acksPending)
		sID := s.ID
		s.RUnlock()
		if sID == 1 && na != 0 {
			t.Fatal("Unexpected un-acknowledged message for sub1")
		} else if sID == 2 && na != 1 {
			t.Fatal("Unacknowledged message should have been recovered for sub2")
		}
	}
}

func TestFileStoreAckMsgRedeliveredToDifferentQueueSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	var err error
	var sub2 stan.Subscription

	errs := make(chan error, 10)
	sub2Recv := make(chan bool)
	redelivered := int32(0)
	trackDelivered := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			if m.Sub != sub2 {
				errs <- fmt.Errorf("Expected redelivered msg to be sent to sub2")
				return
			}
			if atomic.AddInt32(&redelivered, 1) != 1 {
				errs <- fmt.Errorf("Message redelivered after restart")
				return
			}
			sub2Recv <- true
		} else {
			if atomic.LoadInt32(&trackDelivered) == 1 {
				errs <- fmt.Errorf("Unexpected non redelivered message: %v", m)
				return
			}
		}
	}

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	// Create a queue subscriber with manual ackMode that will
	// not ack the message.
	if _, err := sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(time.Second),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create this subscriber that will receive and ack the message
	sub2, err = sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure these are registered.
	waitForNumSubs(t, s, clientName, 2)
	// Send a message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for sub2 to receive the message.
	select {
	case <-sub2Recv:
		break
	case e := <-errs:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get out message")
	}
	// Wait for msg sent to sub2 to be ack'ed
	waitForAcks(t, s, clientName, 2, 0)

	// Stop server
	s.Shutdown()
	// Track unexpected delivery of non redelivered message
	atomic.StoreInt32(&trackDelivered, 1)
	// Restart server
	s = RunServerWithOpts(opts, nil)

	// Get subs
	subs := s.clients.GetSubs(clientName)
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subscriptions to be recovered, got %v", len(subs))
	}
	// No ackPending should be recovered
	for _, s := range subs {
		s.RLock()
		na := len(s.acksPending)
		sID := s.ID
		s.RUnlock()
		if na != 0 {
			t.Fatalf("Unexpected un-acknowledged message for sub: %v", sID)
		}
	}
	// Wait for possible redelivery
	select {
	case e := <-errs:
		t.Fatalf("%v", e)
	case <-time.After(1500 * time.Millisecond):
		break
	}
}

func TestFileStoreAutomaticDeliveryOnRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := RunServerWithOpts(opts, nil)
	defer s.Shutdown()

	toSend := int32(10)
	msg := []byte("msg")

	// Get our STAN connection
	sc := NewDefaultConnection(t)

	// Send messages
	for i := int32(0); i < toSend; i++ {
		if err := sc.Publish("foo", msg); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	blocked := make(chan bool)
	ready := make(chan bool)
	done := make(chan bool)
	received := int32(0)

	// Message callback
	cb := func(m *stan.Msg) {
		count := atomic.AddInt32(&received, 1)
		if count == 2 {
			// Notify we are ready
			ready <- true
			// Wait to be unblocked
			<-blocked
		} else if count == toSend {
			done <- true
		}
	}
	// Create a subscriber that will block after receiving 2 messages.
	if _, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable(),
		stan.MaxInflight(2)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for callback to block
	if err := Wait(ready); err != nil {
		t.Fatal("Did not get our messages")
	}

	// Restart server
	s.Shutdown()
	s = RunServerWithOpts(opts, nil)
	defer s.Shutdown()

	// Release 	the consumer
	close(blocked)
	// Wait for messages to be delivered
	if err := Wait(done); err != nil {
		t.Fatal("Messages were not automatically delivered")
	}
}

func TestSubscribeShrink(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	nsubs := 1000
	subs := make([]stan.Subscription, 0, nsubs)
	for i := 1; i <= nsubs; i++ {
		sub, err := sc.Subscribe("foo", nil)
		if err != nil {
			t.Fatalf("Got an error on subscribe: %v\n", err)
		}
		subs = append(subs, sub)
	}
	// Check number of subs
	waitForNumSubs(t, s, clientName, nsubs)
	// Now unsubsribe them all
	for _, sub := range subs {
		err := sub.Unsubscribe()
		if err != nil {
			t.Fatalf("Got an error on unsubscribe: %v\n", err)
		}
	}
	// Check number of subs
	waitForNumSubs(t, s, clientName, 0)
	// Make sure that array size reduced
	client := s.clients.Lookup(clientName)
	if client == nil {
		t.Fatal("Client should exist")
	}
	client.RLock()
	defer client.RUnlock()
	if cap(client.subs) >= nsubs {
		t.Fatalf("Expected array capacity to have gone down, got %v", len(client.subs))
	}
}

func TestGetSubStoreRace(t *testing.T) {
	numChans := 10000

	opts := GetDefaultOptions()
	opts.MaxChannels = numChans + 1
	s := RunServerWithOpts(opts, nil)
	defer s.Shutdown()

	errs := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	chanNames := make([]string, numChans)
	// Create the channel names in advance to increase concurrency
	// of critical code in the function below.
	for i := 0; i < numChans; i++ {
		chanNames[i] = fmt.Sprintf("channel_%v", i)
	}
	// Perform lookup of channel and access subStore
	f := func() {
		defer wg.Done()
		for i := 0; i < numChans; i++ {
			cs, err := s.lookupOrCreateChannel(chanNames[i])
			if err != nil {
				errs <- err
				return
			}
			if cs.UserData == nil {
				errs <- fmt.Errorf("subStore is nil")
				return
			}
		}
	}
	// Run two go routines to cause parallel execution
	go f()
	go f()
	// Wait for them to return
	wg.Wait()
	// Report possible errors
	if len(errs) > 0 {
		t.Fatalf("%v", <-errs)
	}
}

// TestEnsureStandAlone tests to ensure the server
// panics if it detects another instance running in
// a cluster.
func TestEnsureStandAlone(t *testing.T) {

	// Start a streaming server, and setup a route
	nOpts := DefaultNatsServerOptions
	nOpts.ClusterListenStr = "nats://127.0.0.1:5550"
	nOpts.RoutesStr = "nats://127.0.0.1:5551"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := RunServerWithOpts(sOpts, &nOpts)
	defer s.Shutdown()

	// Start a second streaming server and route to the first, while using the
	// same cluster ID.  It should panic.
	var failedServer *StanServer

	defer func() {
		if r := recover(); r == nil {
			if failedServer != nil {
				failedServer.Shutdown()
			}
			t.Fatal("Server did not detect a duplicate instance.")
		}
	}()

	nOpts2 := DefaultNatsServerOptions
	nOpts2.Port = 4333
	nOpts2.ClusterListenStr = "nats://127.0.0.1:5551"
	nOpts2.RoutesStr = "nats://127.0.0.1:5550"
	failedServer = RunServerWithOpts(sOpts, &nOpts2)
}

func TestAuthenticationUserPass(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Username = "colin"
	nOpts.Password = "alpine"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := RunServerWithOpts(sOpts, &nOpts)
	defer s.Shutdown()

	_, err := nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Server allowed a plain connection")
	}

	_, err = nats.Connect(fmt.Sprintf("nats://%s:badpass@%s:%d", nOpts.Username, nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Server allowed invalid credentials")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s@%s:%d", nOpts.Username, nOpts.Password, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestAuthenticationUserOnly(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Username = "colin"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := RunServerWithOpts(sOpts, &nOpts)
	defer s.Shutdown()

	_, err := nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Server allowed a plain connection")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:@%s:%d", nOpts.Username, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestAuthenticationToken(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Authorization = "0ffw1dth"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := RunServerWithOpts(sOpts, &nOpts)
	defer s.Shutdown()

	_, err := nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Authentication allowed a plain connection")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s@%s:%d", nOpts.Authorization, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestAuthenticationMultiUser(t *testing.T) {

	nOpts, err := natsd.ProcessConfigFile("../test/configs/multi_user.conf")
	if err != nil {
		t.Fatalf("Unable to parse configuration file: %v", err)
	}
	nOpts.Username = "alice"
	nOpts.Password = "foo"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := RunServerWithOpts(sOpts, nOpts)
	defer s.Shutdown()

	_, err = nats.Connect(fmt.Sprintf("nats://%s:%d", nOpts.Host, nOpts.Port))
	if err == nil {
		t.Fatalf("Authentication allowed a plain connection")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%s@%s:%d", nOpts.Username, nOpts.Password, nOpts.Host, nOpts.Port))
	if err != nil {
		t.Fatalf("Authentication did not succeed when expected to: %v", err)
	}
	nc.Close()
}

func TestTLSSuccess(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.ClientCert = "../test/certs/client-cert.pem"
	sOpts.ClientCA = "../test/certs/ca.pem"
	sOpts.ClientKey = "../test/certs/client-key.pem"

	s := RunServerWithOpts(sOpts, &nOpts)
	defer s.Shutdown()
}

func TestTLSSuccessSecure(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.Secure = true

	s := RunServerWithOpts(sOpts, &nOpts)
	defer s.Shutdown()
}

func TestTLSFailServerTLSClientPlain(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	var failedServer *StanServer
	defer func() {
		if r := recover(); r == nil {
			if failedServer != nil {
				failedServer.Shutdown()
			}
			t.Fatal("Server did not fail with invalid TLS configuration")
		}
	}()
	failedServer = RunServerWithOpts(sOpts, &nOpts)
}

func TestTLSFailClientTLSServerPlain(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.ClientCert = "../test/certs/client-cert.pem"
	sOpts.ClientCA = "../test/certs/ca.pem"
	sOpts.ClientKey = "../test/certs/client-key.pem"

	var failedServer *StanServer
	defer func() {
		if r := recover(); r == nil {
			if failedServer != nil {
				failedServer.Shutdown()
			}
			t.Fatal("Server did not fail with invalid TLS configuration")
		}
	}()
	failedServer = RunServerWithOpts(sOpts, &nOpts)
}

func TestIOChannel(t *testing.T) {
	// TODO: When running tests on my Windows VM, looks like we are getting
	// a slow consumer scenario (the NATS Streaming server being the slow
	// consumer). So skip for now.
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}

	run := func(opts *Options) {
		s := RunServerWithOpts(opts, nil)
		defer s.Shutdown()

		sc := NewDefaultConnection(t)
		defer sc.Close()

		ackCb := func(guid string, ackErr error) {
			if ackErr != nil {
				panic(fmt.Errorf("%v - Ack for %v: %v", time.Now(), guid, ackErr))
			}
		}

		total := 10000
		msg := []byte("Hello")
		var err error
		for i := 0; i < total; i++ {
			if i < total-1 {
				_, err = sc.PublishAsync("foo", msg, ackCb)
			} else {
				err = sc.Publish("foo", msg)
			}
			if err != nil {
				stackFatalf(t, "Unexpected error on publish: %v", err)
			}
		}

		// Make sure we have all our messages stored in the server
		checkCount(t, total, func() (string, int) {
			n, _, _ := s.store.MsgsState("foo")
			return "Messages", n
		})
		// Check that the server's ioChannel did not grow bigger than expected
		ioChannelSize := int(atomic.LoadInt64(&(s.ioChannelStatsMaxBatchSize)))
		if ioChannelSize > opts.IOBatchSize {
			stackFatalf(t, "Expected max channel size to be smaller than %v, got %v", opts.IOBatchSize, ioChannelSize)
		}
	}

	sOpts := GetDefaultOptions()
	run(sOpts)

	sOpts = GetDefaultOptions()
	sOpts.IOBatchSize = 50
	run(sOpts)

	sOpts = GetDefaultOptions()
	sOpts.IOBatchSize = 0
	run(sOpts)

	sOpts = GetDefaultOptions()
	sOpts.IOSleepTime = 500
	run(sOpts)
}

func TestDontEmbedNATSNotRunning(t *testing.T) {
	sOpts := GetDefaultOptions()
	// Make sure that with empty string (normally the default), we
	// can run the streaming server (will embed NATS)
	sOpts.NATSServerURL = ""
	s := RunServerWithOpts(sOpts, nil)
	s.Shutdown()

	// Point to a NATS Server that will not be running
	sOpts.NATSServerURL = "nats://localhost:5223"

	// Don't start a NATS Server, starting streaming server
	// should fail.

	var failedServer *StanServer
	defer func() {
		if r := recover(); r == nil {
			failedServer.Shutdown()
			t.Fatal("Expected streaming server to fail to start")
		}
	}()
	failedServer = RunServerWithOpts(sOpts, nil)
}

func TestDontEmbedNATSRunning(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.NATSServerURL = "nats://localhost:5223"

	nOpts := DefaultNatsServerOptions
	nOpts.Host = "localhost"
	nOpts.Port = 5223
	natsd := natsdTest.RunServer(&nOpts)
	defer natsd.Shutdown()

	s := RunServerWithOpts(sOpts, &nOpts)
	defer s.Shutdown()
}

func TestDontEmbedNATSMultipleURLs(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Host = "localhost"
	nOpts.Port = 5223
	nOpts.Username = "ivan"
	nOpts.Password = "pwd"
	natsServer := natsdTest.RunServer(&nOpts)
	defer natsServer.Shutdown()
	auth := &auth.Plain{Username: nOpts.Username, Password: nOpts.Password}
	natsServer.SetClientAuthMethod(auth)

	sOpts := GetDefaultOptions()

	workingURLs := []string{
		"nats://localhost:5223",
		"nats://ivan:pwd@localhost:5223",
		"nats://ivan:pwd@localhost:5223, nats://ivan:pwd@localhost:5224",
	}
	for _, url := range workingURLs {
		sOpts.NATSServerURL = url
		s := RunServerWithOpts(sOpts, &nOpts)
		s.Shutdown()
	}

	notWorkingURLs := []string{
		"nats://ivan:incorrect@localhost:5223",
		"nats://localhost:5223,nats://ivan:pwd@localhost:5224",
		"nats://localhost",
		"localhost:5223",
		"localhost",
		"nats://ivan:pwd@:5224",
		" ",
	}
	for _, url := range notWorkingURLs {
		func() {
			var s *StanServer
			defer func() {
				if r := recover(); r == nil {
					s.Shutdown()
					t.Fatalf("Expected streaming server to fail to start with url=%v", url)
				}
			}()
			sOpts.NATSServerURL = url
			s = RunServerWithOpts(sOpts, &nOpts)
		}()
	}
}

func TestQueueMaxInFlight(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	total := 100
	payload := []byte("hello")
	for i := 0; i < total; i++ {
		sc.Publish("foo", payload)
	}

	ch := make(chan bool)
	received := 0
	cb := func(m *stan.Msg) {
		if !m.Redelivered {
			received++
			if received == total {
				ch <- true
			}
		}
	}
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(5)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not get all our messages")
	}
}
