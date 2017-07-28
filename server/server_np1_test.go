// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/stores"
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

var (
	defaultDataStore string
	testLogger       logger.Logger
	errOnPurpose     = errors.New("On purpose")
)

func init() {
	tmpDir, err := ioutil.TempDir(".", "data_server_")
	if err != nil {
		panic("Could not create tmp dir")
	}
	if err := os.Remove(tmpDir); err != nil {
		panic(fmt.Errorf("Error removing temp directory: %v", err))
	}
	defaultDataStore = tmpDir
	// Set debug and trace for this file.
	defaultOptions.Trace = true
	defaultOptions.Debug = true
	// For FT tests, reduce the HB/Timeout intervals
	setFTTestsHBInterval()
	// Dummy/no-op Logger
	testLogger = logger.NewStanLogger()
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

func msgStoreFirstAndLastSequence(t tLogger, ms stores.MsgStore) (uint64, uint64) {
	f, l, err := ms.FirstAndLastSequence()
	if err != nil {
		stackFatalf(t, "Error getting first and last sequence: %v", err)
	}
	return f, l
}

func msgStoreFirstMsg(t tLogger, ms stores.MsgStore) *pb.MsgProto {
	m, err := ms.FirstMsg()
	if err != nil {
		stackFatalf(t, "Error getting sequence first message: %v", err)
	}
	return m
}

func msgStoreState(t tLogger, ms stores.MsgStore) (int, uint64) {
	n, b, err := ms.State()
	if err != nil {
		stackFatalf(t, "Error getting message state: %v", err)
	}
	return n, b
}

func channelsGet(t tLogger, cs *channelStore, name string) *channel {
	c := cs.get(name)
	if c == nil {
		stackFatalf(t, "Channel %q should exist", name)
	}
	return c
}

// Helper function to shutdown last, a server that is being restarted in a test.
func shutdownRestartedServerOnTestExit(s **StanServer) {
	srv := *s
	srv.Shutdown()
	srv = nil
}

// Helper function to shutdown last, a NATS server that is being restarted in a test.
func shutdownRestartedNATSServerOnTestExit(s **natsd.Server) {
	srv := *s
	srv.Shutdown()
	srv = nil
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
	return "clients", s.clients.count()
}

// Helper function that fails if number of subscriptions is not as expected
func checkSubs(t tLogger, s *StanServer, ID string, expected int) []*subState {
	// Since we need to return the array and we want the array to match
	// the expected value, use the "public" API here.
	subs := s.clients.getSubs(ID)
	checkCount(t, expected, func() (string, int) { return "subscriptions", len(subs) })
	return subs
}

// Helper function that waits for a while to get the expected number of subscriptions,
// otherwise fails.
func waitForNumSubs(t tLogger, s *StanServer, ID string, expected int) {
	waitForCount(t, expected, func() (string, int) {
		// We avoid getting a copy of the subscriptions array here
		// by directly returning the length of the array.
		c := s.clients.lookup(ID)
		c.RLock()
		defer c.RUnlock()
		return "subscriptions", len(c.subs)
	})
}

func waitForAcks(t tLogger, s *StanServer, ID string, subID uint64, expected int) {
	subs := s.clients.getSubs(ID)
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

func runServerWithOpts(t *testing.T, sOpts *Options, nOpts *natsd.Options) *StanServer {
	s, err := RunServerWithOpts(sOpts, nOpts)
	if err != nil {
		stackFatalf(t, err.Error())
	}
	return s
}

func runServer(t *testing.T, clusterName string) *StanServer {
	s, err := RunServer(clusterName)
	if err != nil {
		stackFatalf(t, err.Error())
	}
	return s
}

// RunServerWithDebugTrace is a helper to assist debugging
func RunServerWithDebugTrace(opts *Options, enableDebug, enableTrace bool) (*StanServer, error) {
	var sOpts *Options

	if opts == nil {
		sOpts = GetDefaultOptions()
	} else {
		sOpts = opts.Clone()
	}

	nOpts := natsd.Options{}

	sOpts.Debug = enableDebug
	sOpts.Trace = enableTrace
	nOpts.NoLog = false
	nOpts.NoSigs = true

	sOpts.EnableLogging = true
	return RunServerWithOpts(sOpts, &nOpts)
}

type testChannelStoreFailStore struct{ stores.Store }

func (s *testChannelStoreFailStore) CreateChannel(name string) (*stores.Channel, error) {
	return nil, errOnPurpose
}

func TestChannelStore(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	cs := newChannelStore(s.store)
	if cs.get("foo") != nil {
		t.Fatal("Nothing should be returned")
	}
	c, err := cs.createChannel(s, "foo")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	c2 := cs.get("foo")
	if c2 != c {
		t.Fatalf("Channels should be same, got %v vs %v", c2, c)
	}
	c3, err := cs.createChannel(s, "foo")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	if c3 != c {
		t.Fatalf("Channels should be same, got %v vs %v", c3, c)
	}
	c4, err := cs.createChannel(s, "bar")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	if cs.count() != 2 {
		t.Fatalf("Expected 2 channels, got %v", err)
	}
	channels := cs.getAll()
	for k, v := range channels {
		if k != "foo" && k != "bar" {
			t.Fatalf("Unexpected channel name: %v", k)
		}
		if k == "foo" && v != c {
			t.Fatalf("Unexpected channel for foo, expected %v, got %v", c, v)
		} else if k == "bar" && v != c4 {
			t.Fatalf("Unexpected channel for bar, expected %v, got %v", c4, v)
		}
	}
	if _, _, err := cs.msgsState("baz"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Channel baz does not exist, call should have failed, got %v", err)
	}
	c.store.Msgs.Store([]byte("foo"))
	c4.store.Msgs.Store([]byte("bar"))
	if n, _, err := cs.msgsState(""); n != 2 || err != nil {
		t.Fatalf("Expected 2 messages, got %v err=%v", n, err)
	}

	// Produce store failure
	cs.Lock()
	cs.store = &testChannelStoreFailStore{Store: cs.store}
	cs.Unlock()
	if c, err := cs.createChannel(s, "error"); c != nil || err == nil {
		t.Fatalf("Should have failed, got %v err=%v", c, err)
	}
}

func TestRunServer(t *testing.T) {
	// Test passing nil options
	s := runServerWithOpts(t, nil, nil)
	s.Shutdown()

	// Test passing stan options, nil nats options
	opts := GetDefaultOptions()
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	clusterID := s.ClusterID()

	if clusterID != clusterName {
		t.Fatalf("Expected cluster ID of %s, found %s\n", clusterName, clusterID)
	}
	s.Shutdown()

	// Test passing nil stan options, some nats options
	nOpts := &natsd.Options{}
	nOpts.NoLog = true
	nOpts.NoSigs = true
	s = runServerWithOpts(t, nil, nOpts)
	defer s.Shutdown()
}

type dummyLogger struct {
	sync.Mutex
	msg string
}

func (d *dummyLogger) log(format string, args ...interface{}) {
	d.Lock()
	d.msg = fmt.Sprintf(format, args...)
	d.Unlock()
}

func (d *dummyLogger) Noticef(format string, args ...interface{}) {
	d.log(format, args...)
}

func (d *dummyLogger) Debugf(format string, args ...interface{}) {
	d.log(format, args...)
}

func (d *dummyLogger) Tracef(format string, args ...interface{}) {
	d.log(format, args...)
}

func (d *dummyLogger) Errorf(format string, args ...interface{}) {
	d.log(format, args...)
}

func (d *dummyLogger) Fatalf(format string, args ...interface{}) {
	d.log(format, args...)
}

func TestRunServerFailureLogsCause(t *testing.T) {
	d := &dummyLogger{}

	sOpts := GetDefaultOptions()
	sOpts.NATSServerURL = "nats://localhost:4444"
	sOpts.CustomLogger = d

	// We expect the server to fail to start
	s, err := RunServerWithOpts(sOpts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected error, got none")
	}
	// We should get a trace in the log
	if !strings.Contains(d.msg, "available for connection") {
		t.Fatalf("Expected to get a cause as invalid connection, got: %v", d.msg)
	}
}

func TestServerLoggerDebugAndTrace(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.EnableLogging = true
	sOpts.Debug = true
	sOpts.Trace = true

	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	defer func() {
		os.Stderr = oldStderr
		r.Close()
	}()
	os.Stderr = w
	done := make(chan bool, 1)
	buf := make([]byte, 1024)
	out := make([]byte, 0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				n, _ := r.Read(buf)
				out = append(out, buf[:n]...)
			}
		}
	}()
	s, err := RunServerWithOpts(sOpts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	s.Shutdown()
	// Signal that we are done (the channel is buffered)
	done <- true
	// If the go routine is sitting on a read, make it break out
	// (calling r.Close() would produce races)
	w.Write([]byte("*"))

	wg.Wait()
	// This is a bit dependent on what we currently print with
	// trace and debug. May need to be adjusted.
	str := string(out)
	if !strings.Contains(str, "NATS conn opts") || !strings.Contains(str, "Publish subject") {
		t.Fatalf("Expected tracing to include debug and trace, got %v", out)
	}
}

func TestDefaultOptions(t *testing.T) {

	opts := GetDefaultOptions()
	opts.Debug = !defaultOptions.Debug

	opts2 := GetDefaultOptions()
	if opts2.Debug == opts.Debug {
		t.Fatal("Modified original default options")
	}
}

func TestOptionsClone(t *testing.T) {
	opts := GetDefaultOptions()
	opts.Trace = true
	opts.PerChannel = make(map[string]*stores.ChannelLimits)
	cl := &stores.ChannelLimits{}
	cl.MaxMsgs = 100
	opts.PerChannel["foo"] = cl

	clone := opts.Clone()
	if !reflect.DeepEqual(opts, clone) {
		t.Fatalf("Expected %#v, got %#v", opts, clone)
	}

	// Change a field
	opts.Trace = false
	// Expecting the clone to now be different
	if reflect.DeepEqual(opts, clone) {
		t.Fatal("Expected clone to be different after original changed, was not")
	}
	// Revert the field change
	opts.Trace = true
	// Should be same again
	if !reflect.DeepEqual(opts, clone) {
		t.Fatalf("Expected %#v, got %#v", opts, clone)
	}
	// Change a per channel's element
	cl.MaxMsgs = 50
	// Expecting the clone to now be different
	if reflect.DeepEqual(opts, clone) {
		t.Fatal("Expected clone to be different after original changed, was not")
	}
	// Add one channel to original
	cl2 := *cl
	opts.PerChannel["bar"] = &cl2
	// Verify it is not added to the cloned.
	if _, exist := clone.PerChannel["bar"]; exist {
		t.Fatal("The channel bar should not be in the cloned options")
	}
}

func TestDoubleShutdown(t *testing.T) {
	s := runServer(t, clusterName)
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

func TestServerStates(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()
	checkState(t, s, Standalone)
	s.Shutdown()
	checkState(t, s, Shutdown)
	// FT states are checked in ft_test.go
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
	s := runServer(t, clusterName)
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

	// Send a dummy message on the STAN subscription close subject
	if err := checkServerResponse(nc, s.info.SubClose, ErrInvalidUnsubReq,
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
	s := runServer(t, clusterName)
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
		if r.Error != ErrInvalidClientID.Error() {
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

func sendInvalidSubRequest(s *StanServer, nc *nats.Conn, req *pb.SubscriptionRequest, expectedErr error) error {
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
	if subRep.Error != expectedErr.Error() {
		return fmt.Errorf("Expected error %v, got %v", expectedErr.Error(), subRep.Error)
	}
	return nil
}

func TestInvalidSubRequest(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	// Use a bare NATS connection to send incorrect requests
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	// This test is very dependent on the validity tests performed
	// in StanServer.processSubscriptionRequest(). Any cahnge there
	// may require changes here.

	// Create empty request
	req := &pb.SubscriptionRequest{}

	// We have already tested corrupted SusbcriptionRequests
	// (as in Unmarshal errors) in TestInvalidRequests. Here, we check
	// validity of request's fields.

	// Send this empty request, clientID is missing
	if err := sendInvalidSubRequest(s, nc, req, ErrMissingClient); err != nil {
		t.Fatalf("%v", err)
	}

	// Set a clientID so we move on to next check
	req.ClientID = clientName

	// Test invalid AckWait values
	req.AckWaitInSecs = 0
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidAckWait); err != nil {
		t.Fatalf("%v", err)
	}
	req.AckWaitInSecs = -1
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidAckWait); err != nil {
		t.Fatalf("%v", err)
	}

	// Test invalid MaxInflight values
	req.AckWaitInSecs = 1
	req.MaxInFlight = 0
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidMaxInflight); err != nil {
		t.Fatalf("%v", err)
	}
	req.MaxInFlight = -1
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidMaxInflight); err != nil {
		t.Fatalf("%v", err)
	}

	// Test invalid StartPosition values
	req.MaxInFlight = 1
	req.StartPosition = pb.StartPosition_NewOnly - 1
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidStart); err != nil {
		t.Fatalf("%v", err)
	}
	req.StartPosition = pb.StartPosition_First + 1
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidStart); err != nil {
		t.Fatalf("%v", err)
	}

	// Test invalid subjects
	req.StartPosition = pb.StartPosition_First
	req.Subject = "foo*.bar"
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidSubject); err != nil {
		t.Fatalf("%v", err)
	}
	// Other kinds of invalid subject
	req.Subject = "foo.bar*"
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidSubject); err != nil {
		t.Fatalf("%v", err)
	}
	req.Subject = "foo.>.*"
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidSubject); err != nil {
		t.Fatalf("%v", err)
	}

	// Test Queue Group DurableName
	req.Subject = "foo"
	req.QGroup = "queue"
	req.DurableName = "dur:name"
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidDurName); err != nil {
		t.Fatalf("%v", err)
	}

	// Reset those
	req.QGroup = ""
	req.DurableName = ""

	// Now we should have an error that says that we can't find client ID
	// (that is, client was not registered).
	if err := sendInvalidSubRequest(s, nc, req, fmt.Errorf("can't find clientID: %v", clientName)); err != nil {
		t.Fatalf("%v", err)
	}

	// There should be no client created
	checkClients(t, s, 0)

	// But channel "foo" should have been created though
	if s.channels.count() == 0 {
		t.Fatal("Expected channel foo to have been created")
	}

	// Create a durable
	sc := NewDefaultConnection(t)
	defer sc.Close()
	dur, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Close durable
	if err := dur.Close(); err != nil {
		t.Fatalf("Error closing durable: %v", err)
	}
	// Close client
	sc.Close()
	// Ensure this is processed
	checkClients(t, s, 0)
	// Try to update the durable now that client does not exist.
	req.ClientID = clientName
	req.Subject = "foo"
	req.DurableName = "dur"
	if err := sendInvalidSubRequest(s, nc, req, fmt.Errorf("can't find clientID: %v", clientName)); err != nil {
		t.Fatalf("%v", err)
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
	s := runServer(t, clusterName)
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
	client := s.clients.lookup(clientName)
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
	checkSubs(t, s, clientName, 0)
}

func TestDuplicateClientIDs(t *testing.T) {
	s := runServer(t, clusterName)
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
	s := runServer(t, clusterName)
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

func TestMultipleRedeliveries(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	dlvTimes := make(map[uint64]int64)
	mu := &sync.Mutex{}
	sent := 5
	count := 0
	ch := make(chan bool)
	ackWait := int64(time.Second)
	lowBound := int64(float64(ackWait) * 0.9)
	highBound := int64(float64(ackWait) * 1.1)
	errCh := make(chan error)
	cb := func(m *stan.Msg) {
		now := time.Now().UnixNano()
		mu.Lock()
		lastDlv := dlvTimes[m.Sequence]
		if lastDlv != 0 && (now < lastDlv-lowBound || now > lastDlv+highBound) {
			if len(errCh) == 0 {
				errCh <- fmt.Errorf("Message %d redelivered %v instead of [%v,%v] after last (re)delivery",
					m.Sequence, time.Duration(now-lastDlv), time.Duration(lowBound), time.Duration(highBound))
				mu.Unlock()
				return
			}
		} else {
			dlvTimes[m.Sequence] = now
		}
		if m.Redelivered {
			count++
			if count == 2*sent*4 {
				// we want at least 4 redeliveries
				ch <- true
			}
		}
		mu.Unlock()
	}
	// Create regular subscriber
	if _, err := sc.Subscribe("foo", cb,
		stan.SetManualAckMode(),
		stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// And two queue subscribers from same group
	for i := 0; i < 2; i++ {
		if _, err := sc.QueueSubscribe("foo", "bar", cb,
			stan.SetManualAckMode(),
			stan.AckWait(time.Second)); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}

	for i := 0; i < 5; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Wait for all redeliveries or errors
	select {
	case e := <-errCh:
		t.Fatal(e)
	case <-ch: // all good
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get all our redeliveries")
	}
}

func TestRedeliveryRace(t *testing.T) {
	s := runServer(t, clusterName)
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
	s := runServer(t, clusterName)
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

// As of now, it is possible for members of the same group to have different
// AckWait values. This test checks that if a member with an higher AckWait
// than the other member leaves, the message with an higher expiration time
// is set to the remaining member's AckWait value.
// It also checks that on the opposite case, if a member leaves and the
// remaining member has a higher AckWait, the original expiration time is
// maintained.
func TestQueueSubsWithDifferentAckWait(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	var qsub1, qsub2, qsub3 stan.Subscription
	var err error

	dch := make(chan bool)
	rch2 := make(chan bool)
	rch3 := make(chan bool)

	cb := func(m *stan.Msg) {
		if !m.Redelivered {
			dch <- true
		} else {
			if m.Sub == qsub2 {
				rch2 <- true
			} else if m.Sub == qsub3 {
				rch3 <- true
				// stop further redeliveries, test is done.
				qsub3.Close()
			}
		}
	}
	// Create first queue member with high AckWait
	qsub1, err = sc.QueueSubscribe("foo", "bar", cb,
		stan.SetManualAckMode(),
		stan.AckWait(30*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Send the single message used in this test
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for message to be received
	if err := Wait(dch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Create the second member with low AckWait
	qsub2, err = sc.QueueSubscribe("foo", "bar", cb,
		stan.SetManualAckMode(),
		stan.AckWait(time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check we have the two members
	checkQueueGroupSize(t, s, "foo", "bar", true, 2)
	// Close the first, message should be redelivered within
	// qsub2's AckWait, which is 1 second.
	qsub1.Close()
	// Check we have only 1 member
	checkQueueGroupSize(t, s, "foo", "bar", true, 1)
	// Wait for redelivery
	select {
	case <-rch2:
	// ok
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("Message should have been redelivered")
	}
	// Create 3rd member with higher AckWait than the 2nd
	qsub3, err = sc.QueueSubscribe("foo", "bar", cb,
		stan.SetManualAckMode(),
		stan.AckWait(10*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Close qsub2
	qsub2.Close()
	// Check we have only 1 member
	checkQueueGroupSize(t, s, "foo", "bar", true, 1)
	// Wait for redelivery. It should happen after the remaining
	// of the first redelivery to qsub2 and its AckWait, which
	// should be less than a second.
	select {
	case <-rch3:
	// ok
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("Message should have been redelivered")
	}
}

func TestDurableRedelivery(t *testing.T) {
	s := runServer(t, clusterName)
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

func TestDurableRestartWithMaxInflight(t *testing.T) {
	s := runServer(t, clusterName)
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
	s := runServer(t, clusterName)
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

func TestTooManyChannelsOnCreateSub(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxChannels = 1
	s := runServerWithOpts(t, sOpts, nil)
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
	s := runServerWithOpts(t, sOpts, nil)
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
	if s.channels.get("bar") != nil {
		t.Fatal("Channel bar should not have been created")
	}
}

func TestTooManySubs(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxSubscriptions = 1
	s := runServerWithOpts(t, sOpts, nil)
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
	cs := channelsGet(t, s.channels, "foo")
	ss := cs.ss
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
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	for i := 0; i < 2*sOpts.MaxMsgs; i++ {
		sc.Publish("foo", []byte("msg"))
	}

	// We should not have more than MaxMsgs
	cs := channelsGet(t, s.channels, "foo")
	if n, _ := msgStoreState(t, cs.store.Msgs); n != sOpts.MaxMsgs {
		t.Fatalf("Expected msgs count to be %v, got %v", sOpts.MaxMsgs, n)
	}
}

func TestMaxBytes(t *testing.T) {
	payload := []byte("hello")
	m := pb.MsgProto{Data: payload, Subject: "foo", Sequence: 1, Timestamp: time.Now().UnixNano()}
	msgSize := m.Size()
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.MaxBytes = int64(msgSize * 10)
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	count := 2 * (int(sOpts.MaxBytes) / len(payload))
	for i := 0; i < count; i++ {
		sc.Publish("foo", payload)
	}

	// We should not have more than MaxMsgs
	cs := channelsGet(t, s.channels, "foo")
	if _, b := msgStoreState(t, cs.store.Msgs); b != uint64(sOpts.MaxBytes) {
		t.Fatalf("Expected msgs size to be %v, got %v", sOpts.MaxBytes, b)
	}
}

func checkDurable(t *testing.T, s *StanServer, channel, durName, durKey string) {
	c := s.clients.lookup(clientName)
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
	cs := channelsGet(t, s.channels, channel)
	ss := cs.ss
	ss.RLock()
	durInSS := ss.durables[durKey]
	ss.RUnlock()
	if durInSS == nil || durInSS.DurableName != durName {
		stackFatalf(t, "Expected durable to be in subStore")
	}
}

func TestDurableCanReconnect(t *testing.T) {
	s := runServer(t, clusterName)
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
	s := runServer(t, clusterName)
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
	subs := s.clients.getSubs(clientName)
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

func TestDurableRemovedOnUnsubscribe(t *testing.T) {
	s := runServer(t, clusterName)
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
	sub, err := sc.Subscribe("foo", cb, stan.DurableName(durName))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Check durable is created
	checkDurable(t, s, "foo", durName, durKey)

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Unexpected error on unsubscribe: %v", err)
	}

	// Check that durable is removed
	cs := channelsGet(t, s.channels, "foo")
	ss := cs.ss
	ss.RLock()
	durInSS := ss.durables[durKey]
	ss.RUnlock()
	if durInSS != nil {
		t.Fatal("Durable should have been removed")
	}
}

func checkDurableNoPendingAck(t *testing.T, s *StanServer, isSame bool,
	ackInbox string, ackSub *nats.Subscription, expectedSeq uint64) {
	// When called, we know that there is 1 sub, and the sub is a durable.
	subs := s.clients.getSubs(clientName)
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
	s := runServer(t, clusterName)
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

	duration := time.Since(start)
	if duration > 5*time.Second {
		t.Fatalf("Took too long to be able to connect: %v", duration)
	}
}

func TestStartPositionNewOnly(t *testing.T) {
	s := runServer(t, clusterName)
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
