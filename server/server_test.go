// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nuid"
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
	subs := s.clients.GetSubs(ID)
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
	msg string
}

func (d *dummyLogger) Noticef(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Debugf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Tracef(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Errorf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
}

func (d *dummyLogger) Fatalf(format string, args ...interface{}) {
	d.msg = fmt.Sprintf(format, args...)
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

func getTestDefaultOptsForFileStore() *Options {
	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	opts.FileStoreOpts.BufferSize = 1024
	return opts
}

func testStalledRedelivery(t *testing.T, typeSub string) {
	cleanupDatastore(t, defaultDataStore)
	// defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = runServerWithOpts(t, opts, nil)
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
	if s.store.LookupChannel("bar") != nil {
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
	s := runServerWithOpts(t, sOpts, nil)
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
	cs := s.store.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Channel foo should exist")
	}
	_, b, err := cs.Msgs.State()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	if b != uint64(sOpts.MaxBytes) {
		t.Fatalf("Expected msgs size to be %v, got %v", sOpts.MaxBytes, b)
	}
}

func TestRunServerWithFileStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = runServerWithOpts(t, opts, nil)

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

func TestRecoveredDurableCanReconnect(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

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

	// Close the connection
	sc.Close()

	// Restart the server
	s.Shutdown()

	// Recover
	s = runServerWithOpts(t, opts, nil)

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
	cs := s.store.LookupChannel("foo")
	if cs == nil {
		t.Fatal("Expected channel foo to be created")
	}
	ss := cs.UserData.(*subStore)
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

func TestStartPositionLastReceived(t *testing.T) {
	s := runServer(t, clusterName)
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
	s := runServer(t, clusterName)
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
	for {
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
				// We are done!
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Did not get our message")
		}
	}
}

func TestStartPositionSequenceStart(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	opts.MaxMsgs = 10
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Send more messages than max so that first is not 1
	for i := 0; i < opts.MaxMsgs+10; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Check first/last
	firstSeq, lastSeq := s.store.LookupChannel("foo").Msgs.FirstAndLastSequence()
	if firstSeq != uint64(opts.MaxMsgs+1) {
		t.Fatalf("Expected first sequence to be %v, got %v", uint64(opts.MaxMsgs+1), firstSeq)
	}
	if lastSeq != uint64(opts.MaxMsgs+10) {
		t.Fatalf("Expected last sequence to be %v, got %v", uint64(opts.MaxMsgs+10), lastSeq)
	}

	rch := make(chan bool)
	first := int32(1)
	// Create subscriber with sequence below firstSeq, it should not fail
	// and receive message with sequence == firstSeq
	sub, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == firstSeq {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", firstSeq, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtSequence(1), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure correct msg is received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our message")
	}
	sub.Unsubscribe()

	// Create subscriber with sequence higher than lastSeq, it should not
	// fail, but not receive until we send a new message.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == lastSeq+1 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", lastSeq+1, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtSequence(lastSeq+1), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Make sure correct msg is received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our message")
	}
	sub.Unsubscribe()

	// Create a subscriber with sequence somewhere in range.
	// It should receive that message.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == firstSeq+3 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", firstSeq+3, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtSequence(firstSeq+3), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Make sure correct msg is received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not get our message")
	}
	sub.Unsubscribe()
}

func TestStartPositionTimeDelta(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	rch := make(chan bool)

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

	// Create subscriber with TimeDelta in the past, should
	// not fail and get first message.
	first := int32(1)
	sub, err := sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == 1 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", 1, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtTimeDelta(10*time.Second), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Message 1 should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
	sub.Unsubscribe()

	// Start a subscriber with "TimeDelta" as start position.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == 2 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", 2, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtTimeDelta(1*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Message 2 should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
	sub.Unsubscribe()

	// Wait a bit
	time.Sleep(250 * time.Millisecond)
	// Create a subscriber with delta that would point to
	// after the end of the log.
	atomic.StoreInt32(&first, 1)
	sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
		if m.Sequence == 3 {
			rch <- true
		} else if atomic.LoadInt32(&first) == 1 {
			t.Fatalf("First message should be sequence %v, got %v", 3, m.Sequence)
		}
		atomic.StoreInt32(&first, 0)
	}, stan.StartAtTimeDelta(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("msg3")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Message 3 should be received
	if err := Wait(rch); err != nil {
		t.Fatal("Did not receive our message")
	}
	sub.Unsubscribe()
}

func TestStartPositionWithDurable(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	ch := make(chan bool)
	expected := uint64(1)
	cb := func(m *stan.Msg) {
		if m.Sequence == atomic.LoadUint64(&expected) {
			sc.Close()
			ch <- true
		}
	}
	// Start a durable at first index
	if _, err := sc.Subscribe("foo", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message to be received and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Restart the durable with different start position. It should
	// be ignored and resume from where it left of.
	// Since connection is closed in the callback last message is not
	// acked, but that's fine. We still wait for the next message.
	atomic.StoreUint64(&expected, 2)
	if _, err := sc.Subscribe("foo", cb,
		stan.StartAtSequence(uint64(total-5)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start sequence above total message
	atomic.StoreUint64(&expected, 3)
	if _, err := sc.Subscribe("foo", cb,
		stan.StartAtSequence(uint64(total+10)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start time in future, which should not fail if
	// start position is ignored for a resumed durable.
	atomic.StoreUint64(&expected, 4)
	if _, err := sc.Subscribe("foo", cb,
		stan.StartAtTime(time.Now().Add(10*time.Second)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestStartPositionWithDurableQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	ch := make(chan bool)
	expected := uint64(1)
	cb := func(m *stan.Msg) {
		if m.Sequence == atomic.LoadUint64(&expected) {
			sc.Close()
			ch <- true
		}
	}
	// Start a durable queue subscriber at first index
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message to be received and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Restart the queue durable with different start position. It should
	// be ignored and resume from where it left of.
	// Since connection is closed in the callback last message is not
	// acked, but that's fine. We still wait for the next message.
	atomic.StoreUint64(&expected, 2)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total-5)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start sequence above total message
	atomic.StoreUint64(&expected, 3)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total+10)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Restart a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Try with start time in future, which should not fail if
	// start position is ignored for a resumed durable.
	atomic.StoreUint64(&expected, 4)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtTime(time.Now().Add(10*time.Second)),
		stan.DurableName("dur"),
		stan.MaxInflight(1)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message and connection to be closed.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestStartPositionWithQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	ch := make(chan bool)
	expected := uint64(1)
	cb := func(m *stan.Msg) {
		if m.Sequence == atomic.LoadUint64(&expected) {
			ch <- true
		}
	}
	// Start a queue subsriber at first index
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	atomic.StoreUint64(&expected, 2)
	// Add a member to the group with start sequence
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total-5)),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Try with start sequence above total message
	atomic.StoreUint64(&expected, 3)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtSequence(uint64(total+10)),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Try with start time in future, which should not fail if
	// start position is ignored for queue group
	atomic.StoreUint64(&expected, 4)
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.StartAtTime(time.Now().Add(10*time.Second)),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestIgnoreRecoveredSubForUnknownClientID(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = runServerWithOpts(t, opts, nil)

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
	opts := GetDefaultOptions()
	opts.ID = clusterName
	// Override HB settings
	opts.ClientHBInterval = 50 * time.Millisecond
	opts.ClientHBTimeout = 10 * time.Millisecond
	opts.ClientHBFailCount = 5
	s := runServerWithOpts(t, opts, nil)
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

	// Wait for client to be registered
	waitForNumClients(t, s, 1)

	// Check that client is not incorrectly purged
	dur := (s.opts.ClientHBInterval + s.opts.ClientHBTimeout)
	dur *= time.Duration(s.opts.ClientHBFailCount + 1)
	dur += 100 * time.Millisecond
	time.Sleep(dur)
	// Client should still be there
	waitForNumClients(t, s, 1)

	// kill the NATS conn
	nc.Close()

	// Check that the server closes the connection
	waitForNumClients(t, s, 0)
}

func TestCheckClientHealthDontKeepClientLock(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	// Override HB settings
	opts.ClientHBInterval = 50 * time.Millisecond
	opts.ClientHBTimeout = 3 * time.Second
	opts.ClientHBFailCount = 1
	s := runServerWithOpts(t, opts, nil)
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

	// Wait for client to be registered
	waitForNumClients(t, s, 1)

	// Kill the NATS Connection
	nc.Close()

	// Check that when the server sends a HB request,
	// the client is not blocked for the duration of the
	// HB Timeout
	start := time.Now()

	// Since we can't reliably know when the server is performing
	// the HB request, we are going to wait for at least 2 HB intervals
	// before checking.
	time.Sleep(2 * opts.ClientHBInterval)

	c := s.clients.Lookup(clientName)
	c.RLock()
	// This is to avoid staticcheck "empty critical section (SA2001)" report
	_ = c.fhb
	c.RUnlock()
	dur := time.Since(start)
	// This should have taken less than HB Timeout
	if dur >= opts.ClientHBTimeout {
		t.Fatalf("Client may be locked for the duration of the HB request: %v", dur)
	}
}

func TestConnectsWithDupCID(t *testing.T) {
	s := runServer(t, clusterName)
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
		duration := time.Since(start)
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

	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestFileStoreMissingDirectory(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = ""

	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestFileStoreChangedClusterID(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = defaultDataStore
	s := runServerWithOpts(t, opts, nil)
	s.Shutdown()

	// Change cluster ID, running the server should fail with a panic
	opts.ID = "differentID"
	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestFileStoreRedeliveredPerSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = runServerWithOpts(t, opts, nil)

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
	s = runServerWithOpts(t, opts, nil)

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

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = runServerWithOpts(t, opts, nil)

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

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	// Change server's hb settings
	opts.ClientHBInterval = 100 * time.Millisecond
	opts.ClientHBTimeout = 10 * time.Millisecond
	opts.ClientHBFailCount = 2
	s = runServerWithOpts(t, opts, nil)
	// Check that there are 2 clients
	checkClients(t, s, 2)
	// Tweak their hbTimer interval to make the test short
	clients := s.store.GetClients()
	for cID, sc := range clients {
		c := sc.UserData.(*client)
		c.Lock()
		if c.hbt == nil {
			c.Unlock()
			t.Fatalf("HeartBeat Timer of client %q should have been set", cID)
		}
		c.Unlock()
	}
	// Both clients should quickly timed-out
	waitForNumClients(t, s, 0)
}

func TestFileStoreRedeliveryCbPerSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = runServerWithOpts(t, opts, nil)

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

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	sub2, err = sc.QueueSubscribe("foo", "g1", cb, stan.AckWait(10*time.Second),
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
		waitForAcks(t, s, clientName, 1, 0)
		break
	case e := <-errs:
		t.Fatalf("%v", e)
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get out message")
	}

	// Stop server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

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

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = runServerWithOpts(t, opts, nil)

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

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
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
	s = nil
	s2 := runServerWithOpts(t, opts, nil)
	// defer s.Shutdown()

	// Release 	the consumer
	close(blocked)
	// Wait for messages to be delivered
	if err := Wait(done); err != nil {
		t.Fatal("Messages were not automatically delivered")
	}
	sc.Close()
	s2.Shutdown()
	s2 = nil
}

func TestSubscribeShrink(t *testing.T) {
	s := runServer(t, clusterName)
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
	s := runServerWithOpts(t, opts, nil)
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
	nOpts.Cluster.ListenStr = "nats://127.0.0.1:5550"
	nOpts.RoutesStr = "nats://127.0.0.1:5551"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()

	// Start a second streaming server and route to the first, while using the
	// same cluster ID.  It should fail
	nOpts2 := DefaultNatsServerOptions
	nOpts2.Port = 4333
	nOpts2.Cluster.ListenStr = "nats://127.0.0.1:5551"
	nOpts2.RoutesStr = "nats://127.0.0.1:5550"
	s2, err := RunServerWithOpts(sOpts, &nOpts2)
	if s2 != nil || err == nil {
		s2.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestAuthenticationUserPass(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Username = "colin"
	nOpts.Password = "alpine"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s := runServerWithOpts(t, sOpts, &nOpts)
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

	s := runServerWithOpts(t, sOpts, &nOpts)
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

	s := runServerWithOpts(t, sOpts, &nOpts)
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

	s := runServerWithOpts(t, sOpts, nOpts)
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

	s := runServerWithOpts(t, sOpts, &nOpts)
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

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()
}

func TestTLSFailServerTLSClientPlain(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName

	s, err := RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestTLSFailClientTLSServerPlain(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.ClientCert = "../test/certs/client-cert.pem"
	sOpts.ClientCA = "../test/certs/ca.pem"
	sOpts.ClientKey = "../test/certs/client-key.pem"

	s, err := RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestIOChannel(t *testing.T) {
	// TODO: When running tests on my Windows VM, looks like we are getting
	// a slow consumer scenario (the NATS Streaming server being the slow
	// consumer). So skip for now.
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}

	run := func(opts *Options) {
		s := runServerWithOpts(t, opts, nil)
		defer s.Shutdown()

		sc := NewDefaultConnection(t)
		defer sc.Close()

		ackCb := func(guid string, ackErr error) {
			if ackErr != nil {
				panic(fmt.Errorf("%v - Ack for %v: %v", time.Now(), guid, ackErr))
			}
		}

		total := s.opts.IOBatchSize + 100
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
		// For IOBatchSize > 0, check that the actual limit was never crossed.
		if opts.IOBatchSize > 0 {
			// Check that the server's ioChannel did not grow bigger than expected
			ioChannelSize := int(atomic.LoadInt64(&(s.ioChannelStatsMaxBatchSize)))
			if ioChannelSize > opts.IOBatchSize {
				stackFatalf(t, "Expected max channel size to be smaller than %v, got %v", opts.IOBatchSize, ioChannelSize)
			}
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
	s := runServerWithOpts(t, sOpts, nil)
	s.Shutdown()

	// Point to a NATS Server that will not be running
	sOpts.NATSServerURL = "nats://localhost:5223"

	// Don't start a NATS Server, starting streaming server
	// should fail.
	s, err := RunServerWithOpts(sOpts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestDontEmbedNATSRunning(t *testing.T) {
	sOpts := GetDefaultOptions()
	sOpts.NATSServerURL = "nats://localhost:5223"

	nOpts := DefaultNatsServerOptions
	nOpts.Host = "localhost"
	nOpts.Port = 5223
	natsd := natsdTest.RunServer(&nOpts)
	defer natsd.Shutdown()

	s := runServerWithOpts(t, sOpts, &nOpts)
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

	sOpts := GetDefaultOptions()

	workingURLs := []string{
		"nats://localhost:5223",
		"nats://ivan:pwd@localhost:5223",
		"nats://ivan:pwd@localhost:5223, nats://ivan:pwd@localhost:5224",
	}
	for _, url := range workingURLs {
		sOpts.NATSServerURL = url
		s := runServerWithOpts(t, sOpts, &nOpts)
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
		sOpts.NATSServerURL = url
		s, err := RunServerWithOpts(sOpts, &nOpts)
		if s != nil || err == nil {
			s.Shutdown()
			t.Fatalf("Expected streaming server to fail to start with url=%v", url)
		}
	}
}

func TestQueueMaxInFlight(t *testing.T) {
	s := runServer(t, clusterName)
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

func TestAckTimerSetOnStalledSub(t *testing.T) {

	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	createDur := func() *subState {
		durName := "mydur"

		// Create a durable with MaxInFlight==1 and ack mode (don't ack the message)
		if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {},
			stan.DurableName(durName),
			stan.DeliverAllAvailable(),
			stan.SetManualAckMode(),
			stan.MaxInflight(1)); err != nil {
			stackFatalf(t, "Unexpected error on subscribe: %v", err)
		}
		// Make sure durable is created
		subs := checkSubs(t, s, clientName, 1)
		if len(subs) != 1 {
			stackFatalf(t, "Should be only 1 durable, got %v", len(subs))
		}
		return subs[0]
	}

	// Create durable
	createDur()

	// Wait for durable to be created
	waitForNumSubs(t, s, clientName, 1)

	// Close
	sc.Close()

	// Recreate connection
	sc = NewDefaultConnection(t)
	defer sc.Close()

	// Restart durable
	dur := createDur()

	// Wait for durable to be created
	waitForNumSubs(t, s, clientName, 1)

	// Now check that the timer is set
	dur.RLock()
	timerSet := dur.ackTimer != nil
	dur.RUnlock()
	if !timerSet {
		t.Fatal("Timer should have been set")
	}
}

func TestFileStoreDontSendToOfflineDurablesOnRestart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Run a standalone NATS Server
	gs := natsdTest.RunServer(nil)
	defer gs.Shutdown()

	opts := getTestDefaultOptsForFileStore()
	opts.NATSServerURL = nats.DefaultURL
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	durName := "mydur"
	// Create a durable with manual ack mode (don't ack the message)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {},
		stan.DurableName(durName),
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode()); err != nil {
		stackFatalf(t, "Unexpected error on subscribe: %v", err)
	}
	// Make sure durable is created
	subs := checkSubs(t, s, clientName, 1)
	if len(subs) != 1 {
		stackFatalf(t, "Should be only 1 durable, got %v", len(subs))
	}
	dur := subs[0]
	dur.RLock()
	inbox := dur.Inbox
	dur.RUnlock()

	// Close the client
	sc.Close()

	newSender := NewDefaultConnection(t)
	defer newSender.Close()
	if err := newSender.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	newSender.Close()

	// Create a raw NATS connection
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	failCh := make(chan bool, 10)
	// Setup a consumer on the durable inbox
	sub, err := nc.Subscribe(inbox, func(_ *nats.Msg) {
		failCh <- true
	})
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Stop the Streaming server
	s.Shutdown()
	// Restart the Streaming server
	s = runServerWithOpts(t, opts, nil)

	// We should not get any message, if we do, this is an error
	if err := WaitTime(failCh, time.Second); err == nil {
		t.Fatal("Consumer got a message")
	}
}

func TestFileStoreNoPanicOnShutdown(t *testing.T) {
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForFileStore()
	opts.NATSServerURL = nats.DefaultURL

	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	// Start a go routine that keeps sending messages
	sendQuit := make(chan bool)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		sc, nc := createConnectionWithNatsOpts(t, clientName, nats.NoReconnect())
		defer sc.Close()
		defer nc.Close()

		payload := []byte("hello")
		for {
			select {
			case <-sendQuit:
				return
			default:
				sc.PublishAsync("foo", payload, nil)
			}
		}
	}()
	// Wait for some messages to have been sent
	time.Sleep(100 * time.Millisecond)
	// Shutdown the server, it should not panic
	s.Shutdown()
	// Stop and wait for go routine to end
	sendQuit <- true
	wg.Wait()
}

func TestNonDurableRemovedFromStoreOnConnClose(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Create a subscription
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Close the client connection
	sc.Close()
	// Shutdown the server
	s.Shutdown()
	// Open the store directly and verify that the sub record is not even found.
	limits := stores.DefaultStoreLimits
	store, err := stores.NewFileStore(testLogger, defaultDataStore, &limits)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer store.Close()
	recoveredState, err := store.Recover()
	if err != nil {
		t.Fatalf("Error recovering state: %v", err)
	}
	if recoveredState == nil {
		t.Fatal("Expected to recover state, got none")
	}
	rsubsArray := recoveredState.Subs["foo"]
	if len(rsubsArray) > 0 {
		t.Fatalf("Expected no subscription to be recovered from store, got %v", len(rsubsArray))
	}
}

func TestQueueGroupRemovedOnLastMemberLeaving(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		if m.Sequence == 1 {
			ch <- true
		}
	}
	// Create a queue subscriber
	if _, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close the connection which will remove the queue subscriber
	sc.Close()

	// Create a new connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Send a new message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a queue subscriber. The group should have been destroyed
	// when the last member left, so even with a new name, this should
	// be a new group and start from msg seq 1
	qsub, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Test with unsubscribe
	if err := qsub.Unsubscribe(); err != nil {
		t.Fatalf("Error during Unsubscribe: %v", err)
	}
	// Recreate a queue subscriber, it should again receive from msg1
	if _, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestQueueSubscriberTransferPendingMsgsOnClose(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	var sub1 stan.Subscription
	var sub2 stan.Subscription
	var err error
	ch := make(chan bool)
	qsetup := make(chan bool)
	cb := func(m *stan.Msg) {
		<-qsetup
		if m.Sub == sub1 && m.Sequence == 1 && !m.Redelivered {
			ch <- true
		} else if m.Sub == sub2 && m.Sequence == 1 && m.Redelivered {
			ch <- true
		}
	}
	// Create a queue subscriber with MaxInflight == 1 and manual ACK
	// so that it does not ack it and see if it will be redelivered.
	sub1, err = sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	qsetup <- true
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Start 2nd queue subscriber on same group
	sub2, err = sc.QueueSubscribe("foo", "group", cb, stan.AckWait(time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Unsubscribe the first member
	sub1.Unsubscribe()
	qsetup <- true
	// The second queue subscriber should receive the first message.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestFileStoreQueueSubLeavingUpdateQGroupLastSent(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		ch <- true
	}
	// Create a queue subscriber with MaxInflight == 1 and manual ACK
	// so that it does not ack it and see if it will be redelivered.
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.MaxInflight(1),
		stan.SetManualAckMode()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// send second message
	if err := sc.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start 2nd queue subscriber on same group
	sub2, err := sc.QueueSubscribe("foo", "group", cb, stan.DeliverAllAvailable())
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// The second queue subscriber should receive the second message.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Unsubscribe the second member
	sub2.Unsubscribe()
	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Send a third message
	if err := sc.Publish("foo", []byte("msg3")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a third queue subscriber, it should receive message 3
	msgCh := make(chan *stan.Msg)
	lastMsgCb := func(m *stan.Msg) {
		msgCh <- m
	}
	if _, err := sc.QueueSubscribe("foo", "group", lastMsgCb, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for msg3 or an error to occur
	gotIt := false
	select {
	case m := <-msgCh:
		if m.Sequence != 3 {
			t.Fatalf("Unexpected message: %v", m)
		} else {
			gotIt = true
			break
		}
	case <-time.After(time.Second):
		// Wait for a bit to see if we receive extraneous messages
		break
	}
	if !gotIt {
		t.Fatal("Did not get message 3")
	}
}

func checkQueueGroupSize(t *testing.T, s *StanServer, channelName, groupName string, expectedExist bool, expectedSize int) {
	cs := s.store.LookupChannel(channelName)
	if cs == nil {
		stackFatalf(t, "Expected channel store %q to exist", channelName)
	}
	s.mu.RLock()
	groupSize := 0
	group, exist := cs.UserData.(*subStore).qsubs[groupName]
	if exist {
		groupSize = len(group.subs)
	}
	s.mu.RUnlock()
	if expectedExist && !exist {
		stackFatalf(t, "Expected group to still exist, does not")
	}
	if !expectedExist && exist {
		stackFatalf(t, "Expected group to not exist, it does")
	}
	if expectedExist {
		if groupSize != expectedSize {
			stackFatalf(t, "Expected group size to be %v, got %v", expectedSize, groupSize)
		}
	}
}

func TestBasicDurableQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()
	if err := sc1.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if count == 1 && m.Sequence == 1 {
			ch <- true
		} else if count == 2 && m.Sequence == 2 {
			ch <- true
		}
	}
	// Create a durable queue subscriber.
	sc2, err := stan.Connect(clusterName, "sc2cid")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group exists
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// For this test, make sure we wait for ack to be processed.
	waitForAcks(t, s, "sc2cid", 1, 0)
	// Close the durable queue sub's connection.
	// This should not close the queue group
	sc2.Close()
	// Check queue group still exist, but size 0
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 0)
	// Send another message
	if err := sc1.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Create a durable queue subscriber on the group.
	sub, err := sc1.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// It should receive message 2 only
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Now unsubscribe the sole member
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("Error during Unsubscribe: %v", err)
	}
	// Group should be gone.
	checkQueueGroupSize(t, s, "foo", "qsub:group", false, 0)
}

func TestDurableAndNonDurableQueueSub(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Expect failure if durable name contains ':'
	_, err := sc.QueueSubscribe("foo", "group", func(_ *stan.Msg) {},
		stan.DurableName("qsub:"))
	if err == nil {
		t.Fatal("Expected error on subscribe")
	}
	if err.Error() != ErrInvalidDurName.Error() {
		t.Fatalf("Expected error %v, got %v", ErrInvalidDurName, err)
	}

	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		ch <- true
	}
	// Create a durable queue subscriber.
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Create a regular one
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check groups exists
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	checkQueueGroupSize(t, s, "foo", "group", true, 1)
	// Wait to receive the messages
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Non durable group should be gone
	checkQueueGroupSize(t, s, "foo", "group", false, 0)
	// Other should exist, but empty
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 0)
}

func TestDurableQueueSubRedeliveryOnRejoin(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()
	total := 100
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("msg1")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	dlv := 0
	dch := make(chan bool)
	cb1 := func(m *stan.Msg) {
		if !m.Redelivered {
			dlv++
			if dlv == total {
				dch <- true
			}
		}
	}
	rdlv := 0
	rdch := make(chan bool)
	sigCh := make(chan bool)
	signaled := false
	cb2 := func(m *stan.Msg) {
		if m.Redelivered && int(m.Sequence) <= total {
			rdlv++
			if rdlv == total {
				rdch <- true
			}
		} else if !m.Redelivered && int(m.Sequence) > total {
			if !signaled {
				signaled = true
				sigCh <- true
			}
		}
	}
	// Create a durable queue subscriber with manual ack
	if _, err := sc.QueueSubscribe("foo", "group", cb1,
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.MaxInflight(total),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Wait for it to receive the message
	if err := Wait(dch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Create new one
	sc2, err := stan.Connect(clusterName, "sc2cid")
	if err != nil {
		t.Fatalf("Unexpected error during connect: %v", err)
	}
	defer sc2.Close()
	// Rejoin the group
	if _, err := sc2.QueueSubscribe("foo", "group", cb2,
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.AckWait(time.Second),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 2)
	// Send one more message, which should go to sub2
	if err := sc.Publish("foo", []byte("last")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for it to be received
	if err := Wait(sigCh); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Message should be redelivered
	if err := Wait(rdch); err != nil {
		t.Fatal("Did not get our redelivered message")
	}
}

func TestFileStoreDurableQueueSub(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()
	if err := sc1.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if count == 1 && m.Sequence == 1 {
			ch <- true
		} else if count == 2 && m.Sequence == 2 {
			ch <- true
		}
	}
	// Create a durable queue subscriber.
	sc2, err := stan.Connect(clusterName, "sc2cid")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// For this test, make sure ack is processed
	waitForAcks(t, s, "sc2cid", 1, 0)
	// Close the durable queue sub's connection.
	// This should not close the queue group
	sc2.Close()
	// Check queue group still exist, but size 0
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 0)
	// Send another message
	if err := sc1.Publish("foo", []byte("msg2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Stop and restart the server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Create another client connection
	sc2, err = stan.Connect(clusterName, "sc2cid")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	// Create a durable queue subscriber on the group.
	if _, err := sc2.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// It should receive message 2 only
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestFileStoreDurableQueueSubRedeliveryOnRejoin(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc := NewDefaultConnection(t)
	defer sc.Close()
	if err := sc.Publish("foo", []byte("msg1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	ch := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if (count == 1 && !m.Redelivered) || (count == 2 && m.Redelivered) {
			ch <- true
		}
	}
	// Create a durable queue subscriber with manual ack
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.SetManualAckMode(),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Wait for it to receive the message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Stop server
	s.Shutdown()
	// Restart it
	s = runServerWithOpts(t, opts, nil)
	// Connect
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Rejoin the group
	if _, err := sc.QueueSubscribe("foo", "group", cb,
		stan.DeliverAllAvailable(),
		stan.AckWait(time.Second),
		stan.DurableName("qsub")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Check group
	checkQueueGroupSize(t, s, "foo", "qsub:group", true, 1)
	// Message should be redelivered
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
}

func TestDroppedMessagesOnSendToSub(t *testing.T) {
	opts := GetDefaultOptions()
	opts.MaxMsgs = 3
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Produce 1 message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a durable, it should receive the message
	ch := make(chan bool)
	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {
		ch <- true
	}, stan.DurableName("dur"),
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Close connection
	sc.Close()
	// Recreate a connection
	sc = NewDefaultConnection(t)
	defer sc.Close()
	// Send messages 2, 3, 4 and 5. Messages 1 and 2 should be dropped.
	for i := 2; i <= 5; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Start the durable, it should receive messages 3, 4 and 5
	expectedSeq := uint64(3)
	good := 0
	cb := func(m *stan.Msg) {
		if m.Sequence == expectedSeq {
			good++
			if good == 3 {
				ch <- true
			}
		}
		expectedSeq++
	}
	if _, err := sc.Subscribe("foo", cb,
		stan.DurableName("dur"),
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for messages:
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
}

func TestDroppedMessagesOnSendToQueueSub(t *testing.T) {
	opts := GetDefaultOptions()
	opts.MaxMsgs = 3
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Produce 1 message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Start a queue subscriber, it should receive the message
	ch := make(chan bool)
	blocked := make(chan bool)
	if _, err := sc.QueueSubscribe("foo", "bar", func(m *stan.Msg) {
		ch <- true
		// Block
		<-blocked
		m.Ack()
	}, stan.MaxInflight(1), stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Send messages 2, 3, 4 and 5. Messages 1 and 2 should be dropped.
	for i := 2; i <= 5; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Start another member, it should receive messages 3, 4 and 5
	expectedSeq := uint64(3)
	good := 0
	cb := func(m *stan.Msg) {
		if m.Sequence == expectedSeq {
			good++
			if good == 3 {
				ch <- true
			}
		}
		expectedSeq++
	}
	if _, err := sc.QueueSubscribe("foo", "bar", cb,
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait for messages:
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
	// Unlock first member
	close(blocked)
}

func TestDroppedMessagesOnRedelivery(t *testing.T) {
	opts := GetDefaultOptions()
	opts.MaxMsgs = 3
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Produce 3 messages
	for i := 0; i < 3; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	// Start a subscriber with manual ack, don't ack the first
	// delivered messages.
	ch := make(chan bool)
	ready := make(chan bool)
	expectedSeq := uint64(2)
	good := 0
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
			if m.Sequence == expectedSeq {
				good++
				if good == 3 {
					ch <- true
				}
			}
			expectedSeq++
		} else if m.Sequence == 3 {
			ready <- true
		}
	}
	if _, err := sc.Subscribe("foo", cb,
		stan.SetManualAckMode(),
		stan.AckWait(time.Second),
		stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Wait to receive 3rd message, then send one more
	if err := Wait(ready); err != nil {
		t.Fatal("Did not get our message")
	}
	// Send one more, this should cause 1st message to be dropped
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Wait for redelivery of message 2, 3 and 4.
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
}

func TestPerChannelLimits(t *testing.T) {
	f := func(idx int) {
		opts := GetDefaultOptions()
		if idx == 1 {
			cleanupDatastore(t, defaultDataStore)
			defer cleanupDatastore(t, defaultDataStore)

			opts.FilestoreDir = defaultDataStore
			opts.StoreType = stores.TypeFile
		}
		opts.MaxMsgs = 10
		opts.MaxAge = time.Hour
		clfoo := stores.ChannelLimits{}
		clfoo.MaxMsgs = 2
		clbar := stores.ChannelLimits{}
		clbar.MaxBytes = 1000
		clbaz := stores.ChannelLimits{}
		clbaz.MaxSubscriptions = 1
		clbaz.MaxAge = time.Second
		sl := &opts.StoreLimits
		sl.AddPerChannel("foo", &clfoo)
		sl.AddPerChannel("bar", &clbar)
		sl.AddPerChannel("baz", &clbaz)

		s := runServerWithOpts(t, opts, nil)
		defer s.Shutdown()

		sc := NewDefaultConnection(t)
		defer sc.Close()

		// Sending on foo should be limited to 2 messages
		for i := 0; i < 10; i++ {
			if err := sc.Publish("foo", []byte("hello")); err != nil {
				t.Fatalf("Unexpected error on publish: %v", err)
			}
		}
		// Check messages count
		s.mu.RLock()
		n, _, err := s.store.MsgsState("foo")
		s.mu.RUnlock()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		if n != clfoo.MaxMsgs {
			t.Fatalf("Expected only %v messages, got %v", clfoo.MaxMsgs, n)
		}

		// Sending on bar should be limited by the size, or the count of global
		// setting
		for i := 0; i < 100; i++ {
			if err := sc.Publish("bar", []byte("hello")); err != nil {
				t.Fatalf("Unexpected error on publish: %v", err)
			}
		}
		// Check messages count
		s.mu.RLock()
		n, b, err := s.store.MsgsState("bar")
		s.mu.RUnlock()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		// There should be more than for foo, but no more than opts.MaxMsgs
		if n <= clfoo.MaxMsgs {
			t.Fatalf("Expected more messages than %v", n)
		}
		if n > opts.MaxMsgs {
			t.Fatalf("Should be limited by parent MaxMsgs of %v, got %v", opts.MaxMsgs, n)
		}
		// The size should be lower than clbar.MaxBytes
		if b > uint64(clbar.MaxBytes) {
			t.Fatalf("Expected less than %v bytes, got %v", clbar.MaxBytes, b)
		}

		// Number of subscriptions on baz should be limited to 1
		for i := 0; i < 2; i++ {
			if _, err := sc.Subscribe("baz", func(_ *stan.Msg) {}); i == 1 && err == nil {
				t.Fatal("Expected max subscriptions to be 1")
			}
		}
		// Max Age for baz should be 1sec.
		if err := sc.Publish("baz", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
		// Wait more than max age
		time.Sleep(1500 * time.Millisecond)
		// Check state
		s.mu.RLock()
		n, _, err = s.store.MsgsState("baz")
		s.mu.RUnlock()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		if n != 0 {
			t.Fatalf("Message should have expired")
		}
	}
	for i := 0; i < 2; i++ {
		f(i)
	}
}

func TestProtocolOrder(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	for i := 0; i < 10; i++ {
		if err := sc.Publish("bar", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}

	ch := make(chan bool)
	errCh := make(chan error)

	recv := int32(0)
	mode := 0
	var sc2 stan.Conn
	cb := func(m *stan.Msg) {
		count := atomic.AddInt32(&recv, 1)
		if err := m.Ack(); err != nil {
			errCh <- err
			return
		}
		if count == 10 {
			var err error
			switch mode {
			case 1:
				err = m.Sub.Unsubscribe()
			case 2:
				err = m.Sub.Close()
			case 3:
				err = sc2.Close()
			case 4:
				err = m.Sub.Unsubscribe()
				if err == nil {
					err = sc2.Close()
				}
			case 5:
				err = m.Sub.Close()
				if err == nil {
					err = sc2.Close()
				}
			}
			if err != nil {
				errCh <- err
			} else {
				ch <- true
			}
		}
	}

	total := 50
	// Unsubscribe should not be processed before last processed Ack
	mode = 1
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		if _, err := sc.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Subscription close should not be processed before last processed Ack
	mode = 2
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		if _, err := sc.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Connection close should not be processed before last processed Ack
	mode = 3
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		conn, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		sc2 = conn
		if _, err := sc2.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Connection close should not be processed before unsubscribe and last processed Ack
	mode = 4
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		conn, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		sc2 = conn
		if _, err := sc2.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}
	// Connection close should not be processed before sub close and last processed Ack
	mode = 5
	for i := 0; i < total; i++ {
		atomic.StoreInt32(&recv, 0)
		conn, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		sc2 = conn
		if _, err := sc2.Subscribe("bar", cb,
			stan.SetManualAckMode(), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		// Wait confirmation of received message
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatal(e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
	}

	// Mix pub and subscribe calls
	ch = make(chan bool)
	errCh = make(chan error)
	startSubAt := 50
	var sub stan.Subscription
	var err error
	for i := 1; i <= 100; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
		if i == startSubAt {
			sub, err = sc.Subscribe("foo", func(m *stan.Msg) {
				if m.Sequence == uint64(startSubAt)+1 {
					ch <- true
				} else if len(errCh) == 0 {
					errCh <- fmt.Errorf("Received message %v instead of %v", m.Sequence, startSubAt+1)
				}
			})
			if err != nil {
				t.Fatalf("Unexpected error on subscribe: %v", err)
			}
		}
	}
	// Wait confirmation of received message
	select {
	case <-ch:
	case e := <-errCh:
		t.Fatal(e)
	case <-time.After(5 * time.Second):
		t.Fatal("Timed-out waiting for messages")
	}
	sub.Unsubscribe()

	// Acks should be processed before Connection close
	for i := 0; i < total; i++ {
		rcv := int32(0)
		sc2, err := stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		// Create durable and close connection in cb when
		// receiving 10th message.
		if _, err := sc2.Subscribe("foo", func(m *stan.Msg) {
			if atomic.AddInt32(&rcv, 1) == 10 {
				sc2.Close()
				ch <- true
			}
		}, stan.DurableName("dur"), stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := Wait(ch); err != nil {
			t.Fatal("Did not get our messages")
		}
		// Connection is closed at this point. Recreate one
		sc2, err = stan.Connect(clusterName, "otherclient")
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		// Recreate durable and first message should be 10.
		first := true
		sub, err = sc2.Subscribe("foo", func(m *stan.Msg) {
			if first {
				if m.Redelivered && m.Sequence == 10 {
					ch <- true
				} else {
					errCh <- fmt.Errorf("Unexpected message: %v", m)
				}
				first = false
			}
		}, stan.DurableName("dur"), stan.DeliverAllAvailable(),
			stan.MaxInflight(1), stan.SetManualAckMode())
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		// Wait for ok or error
		select {
		case <-ch:
		case e := <-errCh:
			t.Fatalf("Error: %v", e)
		case <-time.After(5 * time.Second):
			t.Fatal("Timed-out waiting for messages")
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Fatalf("Unexpected error on unsubscribe: %v", err)
		}
		sc2.Close()
	}
}

func TestDurableClosedNotUnsubscribed(t *testing.T) {
	closeSubscriber(t, "sub")
	closeSubscriber(t, "queue")
}

func closeSubscriber(t *testing.T, subType string) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("msg")); err != nil {
		stackFatalf(t, "Unexpected error on publish: %v", err)
	}
	// Create a durable
	durName := "dur"
	groupName := "group"
	durKey := fmt.Sprintf("%s-%s-%s", clientName, "foo", durName)
	if subType == "queue" {
		durKey = fmt.Sprintf("%s:%s", durName, groupName)
	}
	ch := make(chan bool)
	errCh := make(chan bool)
	count := 0
	cb := func(m *stan.Msg) {
		count++
		if m.Sequence != uint64(count) {
			errCh <- true
			return
		}
		ch <- true
	}
	var sub stan.Subscription
	var err error
	if subType == "sub" {
		sub, err = sc.Subscribe("foo", cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	} else {
		sub, err = sc.QueueSubscribe("foo", groupName, cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	}
	if err != nil {
		stackFatalf(t, "Unexpected error on subscribe: %v", err)
	}
	wait := func() {
		select {
		case <-errCh:
			stackFatalf(t, "Unexpected message received")
		case <-ch:
		case <-time.After(5 * time.Second):
			stackFatalf(t, "Did not get our message")
		}
	}
	wait()

	s.mu.RLock()
	cs := s.store.LookupChannel("foo")
	ss := cs.UserData.(*subStore)
	var dur *subState
	if subType == "sub" {
		dur = ss.durables[durKey]
	} else {
		dur = ss.qsubs[durKey].subs[0]
	}
	s.mu.RUnlock()
	if dur == nil {
		stackFatalf(t, "Durable should have been found")
	}
	// Make sure ACKs are processed before closing to avoid redelivery
	waitForAcks(t, s, clientName, dur.ID, 0)
	// Close durable, don't unsubscribe it
	if err := sub.Close(); err != nil {
		stackFatalf(t, "Error on subscriber close: %v", err)
	}
	// Durable should still be present
	ss.RLock()
	var there bool
	if subType == "sub" {
		_, there = ss.durables[durKey]
	} else {
		_, there = ss.qsubs[durKey]
	}
	ss.RUnlock()
	if !there {
		stackFatalf(t, "Durable should still be present")
	}
	// Send second message
	if err := sc.Publish("foo", []byte("msg")); err != nil {
		stackFatalf(t, "Unexpected error on publish: %v", err)
	}
	// Restart the durable
	if subType == "sub" {
		sub, err = sc.Subscribe("foo", cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	} else {
		sub, err = sc.QueueSubscribe("foo", groupName, cb,
			stan.DeliverAllAvailable(),
			stan.DurableName(durName))
	}
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	wait()
	// Unsubscribe for good
	if err := sub.Unsubscribe(); err != nil {
		stackFatalf(t, "Unexpected error on unsubscribe")
	}
	// Wait for unsub to be fully processed
	waitForNumSubs(t, s, clientName, 0)
	// Should have been removed
	ss.RLock()
	if subType == "sub" {
		_, there = ss.durables[durKey]
	} else {
		_, there = ss.qsubs[durKey]
	}
	ss.RUnlock()
	if there {
		stackFatalf(t, "Durable should not be present")
	}
}

func TestProcessCommandLineArgs(t *testing.T) {
	var host string
	var port int
	cmd := flag.NewFlagSet("nats-streaming-server", flag.ExitOnError)
	cmd.StringVar(&host, "a", "0.0.0.0", "Host.")
	cmd.IntVar(&port, "p", 4222, "Port.")

	cmd.Parse([]string{"-a", "127.0.0.1", "-p", "9090"})
	showVersion, showHelp, err := natsd.ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion || showHelp {
		t.Errorf("Expected not having to handle subcommands")
	}

	cmd.Parse([]string{"version"})
	showVersion, showHelp, err = natsd.ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if !showVersion {
		t.Errorf("Expected having to handle version command")
	}
	if showHelp {
		t.Errorf("Expected not having to handle help command")
	}

	cmd.Parse([]string{"help"})
	showVersion, showHelp, err = natsd.ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion {
		t.Errorf("Expected not having to handle version command")
	}
	if !showHelp {
		t.Errorf("Expected having to handle help command")
	}

	cmd.Parse([]string{"foo", "-p", "9090"})
	_, _, err = natsd.ProcessCommandLineArgs(cmd)
	if err == nil {
		t.Errorf("Expected an error handling the command arguments")
	}
}

func TestFileStoreQMemberRemovedFromStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()

	ch := make(chan bool)
	// Create the group (adding the first member)
	if _, err := sc1.QueueSubscribe("foo", "group",
		func(_ *stan.Msg) {
			ch <- true
		},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	sc2, err := stan.Connect(clusterName, "othername")
	if err != nil {
		stackFatalf(t, "Expected to connect correctly, got err %v", err)
	}
	defer sc2.Close()
	// Add a second member to the group
	if _, err := sc2.QueueSubscribe("foo", "group", func(_ *stan.Msg) {},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// Remove it by closing the connection.
	sc2.Close()

	// Send a message and verify it is received
	if err := sc1.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Have the first leave the group too.
	sc1.Close()

	// Restart the server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	// Send a new message
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// Have a member rejoin the group
	if _, err := sc.QueueSubscribe("foo", "group",
		func(_ *stan.Msg) {
			ch <- true
		},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// It should receive the second message
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Check server state
	s.mu.RLock()
	cs, _ := s.lookupOrCreateChannel("foo")
	ss := cs.UserData.(*subStore)
	s.mu.RUnlock()
	ss.RLock()
	qs := ss.qsubs["dur:group"]
	ss.RUnlock()
	if len(qs.subs) != 1 {
		t.Fatalf("Expected only 1 member, got %v", len(qs.subs))
	}
}

func TestIgnoreFailedHBInAckRedeliveryForQGroup(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	opts.ClientHBInterval = 100 * time.Millisecond
	opts.ClientHBTimeout = time.Millisecond
	opts.ClientHBFailCount = 100000
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	count := 0
	ch := make(chan bool)
	cb := func(m *stan.Msg) {
		count++
		if count == 4 {
			ch <- true
		}
	}
	// Create first queue member. Use NatsConn so we can close the NATS
	// connection to produced failed HB
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	sc1, err := stan.Connect(clusterName, "client1", stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	if _, err := sc1.QueueSubscribe("foo", "group", cb, stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Create 2nd member.
	sc2 := NewDefaultConnection(t)
	defer sc2.Close()
	if _, err := sc2.QueueSubscribe("foo", "group", cb, stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Send 2 messages, expecting to go to sub1 then sub2
	for i := 0; i < 2; i++ {
		sc1.Publish("foo", []byte("hello"))
	}
	// Wait for those messages to be ack'ed
	waitForAcks(t, s, "client1", 1, 0)
	// Close connection of sub1
	nc.Close()
	// Send 2 more messages
	for i := 0; i < 2; i++ {
		sc2.Publish("foo", []byte("hello"))
	}
	// Wait for messages to be received
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
}

func TestAckPublisherBufSize(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	errCh := make(chan error)
	inbox := nats.NewInbox()
	iopm := &ioPendingMsg{m: &nats.Msg{Reply: inbox}}
	nc.Subscribe(inbox, func(m *nats.Msg) {
		pubAck := pb.PubAck{}
		if err := pubAck.Unmarshal(m.Data); err != nil {
			errCh <- err
			return
		}
		if !reflect.DeepEqual(pubAck, iopm.pa) {
			errCh <- fmt.Errorf("Expected PubAck: %v, got: %v", iopm.pa, pubAck)
			return
		}
		errCh <- nil
	})
	nc.Flush()

	checkErr := func() {
		select {
		case e := <-errCh:
			if e != nil {
				t.Fatalf("%v", e)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not get our message")
		}
	}

	iopm.pm.Guid = nuid.Next()
	s.ackPublisher(iopm)
	checkErr()

	iopm.pm.Guid = "this is a very very very very very very very very very very very very very very very very very very long guid"
	s.ackPublisher(iopm)
	checkErr()
}

func TestDontSendEmptyMsgProto(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.NoReconnect())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	// Since server is expected to crash, do not attempt to close sc
	// because it would delay test by 2 seconds.

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	waitForNumSubs(t, s, clientName, 1)

	subs := s.clients.GetSubs(clientName)
	sub := subs[0]

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Server should have panic'ed")
		}
	}()

	m := &pb.MsgProto{}
	sub.Lock()
	s.sendMsgToSub(sub, m, false)
	sub.Unlock()
}

func TestMsgsNotSentToSubBeforeSubReqResponse(t *testing.T) {
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
	connReq := &pb.ConnectRequest{
		ClientID:       clientName,
		HeartbeatInbox: nats.NewInbox(),
	}
	crb, _ := connReq.Marshal()
	respMsg, err := nc.Request(connSubj, crb, 5*time.Second)
	if err != nil {
		t.Fatalf("Request error: %v", err)
	}
	connResponse := &pb.ConnectResponse{}
	connResponse.Unmarshal(respMsg.Data)

	subSubj := connResponse.SubRequests
	unsubSubj := connResponse.UnsubRequests
	pubSubj := connResponse.PubPrefix + ".foo"

	pubMsg := &pb.PubMsg{
		ClientID: clientName,
		Subject:  "foo",
		Data:     []byte("hello"),
	}
	subReq := &pb.SubscriptionRequest{
		ClientID:      clientName,
		MaxInFlight:   1024,
		Subject:       "foo",
		StartPosition: pb.StartPosition_First,
		AckWaitInSecs: 30,
	}
	unsubReq := &pb.UnsubscribeRequest{
		ClientID: clientName,
		Subject:  "foo",
	}
	for i := 0; i < 100; i++ {
		// Use the same subscriber for subscription request response and data,
		// so we can reliably check if data comes before response.
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("Unable to create nats subscriber: %v", err)
		}
		subReq.Inbox = inbox
		bytes, _ := subReq.Marshal()
		// Send the request with inbox as the Reply
		if err := nc.PublishRequest(subSubj, inbox, bytes); err != nil {
			t.Fatalf("Error sending request: %v", err)
		}
		// Followed by a data message
		pubMsg.Guid = nuid.Next()
		bytes, _ = pubMsg.Marshal()
		if err := nc.Publish(pubSubj, bytes); err != nil {
			t.Fatalf("Error sending msg: %v", err)
		}
		nc.Flush()
		// Dequeue
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Did not get our message: %v", err)
		}
		// It should always be the subscription response!!!
		msgProto := &pb.MsgProto{}
		err = msgProto.Unmarshal(msg.Data)
		if err == nil && msgProto.Sequence != 0 {
			t.Fatalf("Iter=%v - Did not receive valid subscription response: %#v - %v", (i + 1), msgProto, err)
		}
		subReqResp := &pb.SubscriptionResponse{}
		subReqResp.Unmarshal(msg.Data)
		unsubReq.Inbox = subReqResp.AckInbox
		bytes, _ = unsubReq.Marshal()
		if err := nc.Publish(unsubSubj, bytes); err != nil {
			t.Fatalf("Unable to send unsub request: %v", err)
		}
		sub.Unsubscribe()
	}
}

func TestFileStoreAcksPool(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	opts.AckSubsPoolSize = 5
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	var allSubs []stan.Subscription

	// Check server's ackSub pool
	checkPoolSize := func() {
		s.mu.RLock()
		poolSize := len(s.acksSubs)
		s.mu.RUnlock()
		if poolSize != opts.AckSubsPoolSize {
			stackFatalf(t, "Expected acksSubs pool size to be %v, got %v", opts.AckSubsPoolSize, poolSize)
		}
	}
	checkPoolSize()

	// Check total subs and each sub's ackSub is pooled or not as expected.
	checkAckSubs := func(total int32, checkSubFunc func(sub *subState) error) {
		subs := s.clients.GetSubs(clientName)
		if len(subs) != int(total) {
			stackFatalf(t, "Expected %d subs, got %v", total, len(subs))
		}
		for _, sub := range subs {
			sub.RLock()
			err := checkSubFunc(sub)
			sub.RUnlock()
			if err != nil {
				stackFatalf(t, err.Error())
			}
		}
	}

	sc, nc := createConnectionWithNatsOpts(t, clientName,
		nats.ReconnectWait(100*time.Millisecond))
	defer nc.Close()
	defer sc.Close()

	totalSubs := int32(10)
	errCh := make(chan error, totalSubs)
	ch := make(chan bool)
	count := int32(0)
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			errCh <- fmt.Errorf("Unexpected redelivered message: %v", m)
		} else if atomic.AddInt32(&count, 1) == atomic.LoadInt32(&totalSubs) {
			ch <- true
		}
	}
	// Create 10 subs
	for i := 0; i < int(totalSubs); i++ {
		sub, err := sc.Subscribe("foo", cb, stan.AckWait(time.Second))
		if err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
		allSubs = append(allSubs, sub)
	}
	checkReceived := func(subs int32) {
		atomic.StoreInt32(&count, 0)
		atomic.StoreInt32(&totalSubs, subs)
		// Send 1 message
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			stackFatalf(t, "Unexpected error on publish: %v", err)
		}
		// Wait for all messages to be received
		if err := Wait(ch); err != nil {
			stackFatalf(t, "Did not get our messages")
		}
		// Wait for more than redelivery time
		time.Sleep(1500 * time.Millisecond)
		// Check that there was no error
		select {
		case e := <-errCh:
			stackFatalf(t, e.Error())
		default:
		}
	}
	checkReceived(totalSubs)
	// Check that server's subs have nil ackSub
	checkAckSubs(totalSubs, func(sub *subState) error {
		if sub.ackSub != nil {
			return fmt.Errorf("Expected ackSub for sub %v to be nil, was not", sub.ID)
		}
		return nil
	})
	// Stop server and restart with lower pool size
	s.Shutdown()
	opts.AckSubsPoolSize = 2
	s = runServerWithOpts(t, opts, nil)
	// Check server's ackSub pool
	checkPoolSize()
	// Check that AckInbox with ackSubIndex > AcksPoolSize-1 have
	// individual ackSub
	checkAckSubs(totalSubs, func(sub *subState) error {
		wantsNil := false
		ackSubIndexEnd := strings.Index(sub.AckInbox, ".")
		if n, _ := strconv.Atoi(sub.AckInbox[:ackSubIndexEnd]); n <= 1 {
			wantsNil = true
		}
		if wantsNil && sub.ackSub != nil {
			return fmt.Errorf("Expected ackSub for sub %v to be nil, was not", sub.ID)
		} else if !wantsNil && sub.ackSub == nil {
			return fmt.Errorf("Expected ackSub for sub %v to be not nil, was nil", sub.ID)
		}
		return nil
	})
	checkReceived(totalSubs)
	// Restart server with no acksSub pool
	s.Shutdown()
	opts.AckSubsPoolSize = 0
	s = runServerWithOpts(t, opts, nil)
	// Check server's ackSub pool
	checkPoolSize()
	// Check that all subs have an individual ackSub
	checkAckSubs(totalSubs, func(sub *subState) error {
		if sub.ackSub == nil {
			return fmt.Errorf("Expected ackSub for sub %v to be not nil, was nil", sub.ID)
		}
		return nil
	})
	checkReceived(totalSubs)
	// Add another subscriber
	sub, err := sc.Subscribe("foo", func(_ *stan.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	allSubs = append(allSubs, sub)
	// Restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)
	// Check that the new sub's AckInbox is _INBOX as usual.
	checkAckSubs(totalSubs+1, func(sub *subState) error {
		if int(sub.ID) > int(totalSubs) {
			if sub.AckInbox[:len(nats.InboxPrefix)] != nats.InboxPrefix {
				return fmt.Errorf("Unexpected AckInbox: %v", sub)
			}
		}
		if sub.ackSub == nil {
			return fmt.Errorf("Expected ackSub for sub %v to be not nil, was nil", sub.ID)
		}
		return nil
	})
	// Restart server with ackPool
	s.Shutdown()
	opts.AckSubsPoolSize = 2
	s = runServerWithOpts(t, opts, nil)
	// Check server's ackSub pool
	checkPoolSize()
	// Check that unsubscribe work ok
	for _, sub := range allSubs {
		if err := sub.Unsubscribe(); err != nil {
			t.Fatalf("Error on unsubscribe: %v", err)
		}
	}
	// Create a subscription without call unsubscribe and make
	// sure it does not prevent closing of connection.
	if _, err := sc.Subscribe("foo", cb, stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	checkReceived(1)
	// Close the client connection
	sc.Close()
	nc.Close()
	// Make sure connection close is correctly processed.
	waitForNumClients(t, s, 0)
}

func TestAckSubsSubjectsInPoolUseUniqueSubject(t *testing.T) {
	opts := GetDefaultOptions()
	opts.ID = clusterName
	opts.AckSubsPoolSize = 1
	s1 := runServerWithOpts(t, opts, nil)
	defer s1.Shutdown()

	opts.NATSServerURL = nats.DefaultURL
	opts.ID = "otherCluster"
	s2 := runServerWithOpts(t, opts, nil)
	defer s2.Shutdown()

	sc1 := NewDefaultConnection(t)
	defer sc1.Close()

	if _, err := sc1.Subscribe("foo", func(m *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	sc2, err := stan.Connect("otherCluster", "otherClient")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc2.Close()

	// Create a subscription with manual ack, and ack after message is
	// redelivered.
	cb := func(m *stan.Msg) {
		if m.Redelivered {
			m.Ack()
		}
	}
	if _, err := sc2.Subscribe("foo", cb,
		stan.SetManualAckMode(),
		stan.AckWait(time.Second)); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}

	// Produce 1 message for each connection
	if err := sc1.Publish("foo", []byte("hello s1")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	if err := sc2.Publish("foo", []byte("hello s2")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	// Wait for ack of sc2 to be processed by s2
	waitForAcks(t, s2, "otherClient", 1, 0)

	s1.mu.Lock()
	s1AcksReceived, _ := s1.acksSubs[0].Delivered()
	s1.mu.Unlock()
	if s1AcksReceived != 1 {
		t.Fatalf("Expected pooled ack sub to receive only 1 message, got %v", s1AcksReceived)
	}
	s2.mu.Lock()
	s2AcksReceived, _ := s2.acksSubs[0].Delivered()
	s2.mu.Unlock()
	if s2AcksReceived != 1 {
		t.Fatalf("Expected pooled ack sub to receive only 1 message, got %v", s2AcksReceived)
	}
}

func TestNewOnHoldSetOnDurableRestart(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	var (
		dur stan.Subscription
		err error
	)
	total := 50
	count := 0
	ch := make(chan bool)
	dur, err = sc.Subscribe("foo", func(_ *stan.Msg) {
		count++
		if count == total {
			dur.Close()
			ch <- true
		}
	}, stan.SetManualAckMode(), stan.DurableName("dur"))
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}

	// Start a go routine that will pump messages to the channel
	// and make sure that the very first message we receive is
	// the redelivered message.
	failed := false
	mu := sync.Mutex{}
	count = 0
	cb := func(m *stan.Msg) {
		count++
		if m.Sequence != uint64(count) && !m.Redelivered {
			failed = true
		}
		if count == total {
			mu.Lock()
			dur.Unsubscribe()
			mu.Unlock()
			ch <- true
		}
	}
	stop := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := sc.Publish("foo", []byte("hello")); err != nil {
					t.Fatalf("Unexpected error on publish: %v", err)
				}
			}
		}
	}()
	// Restart durable
	mu.Lock()
	dur, err = sc.Subscribe("foo", cb, stan.DeliverAllAvailable(), stan.DurableName("dur"))
	mu.Unlock()
	if err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Make publishers stop
	close(stop)
	// Wait for go routine to finish
	wg.Wait()
	// Check our success
	if failed {
		t.Fatal("Did not receive the redelivered messages first")
	}
}

func TestUnlimitedPerChannelLimits(t *testing.T) {
	opts := GetDefaultOptions()
	opts.StoreLimits.MaxChannels = 2
	// Set very small global limits
	opts.StoreLimits.MaxMsgs = 1
	opts.StoreLimits.MaxBytes = 1
	opts.StoreLimits.MaxAge = time.Millisecond
	opts.StoreLimits.MaxSubscriptions = 1
	// Add a channel that has all unlimited values
	cl := &stores.ChannelLimits{}
	cl.MaxMsgs = -1
	cl.MaxBytes = -1
	cl.MaxAge = -1
	cl.MaxSubscriptions = -1
	opts.StoreLimits.AddPerChannel("foo", cl)
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	sc := NewDefaultConnection(t)
	defer sc.Close()
	// Check that we can send more than 1 message of more than 1 byte
	total := 10
	for i := 0; i < total; i++ {
		if err := sc.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	count := int32(0)
	ch := make(chan bool)
	cb := func(_ *stan.Msg) {
		if c := atomic.AddInt32(&count, 1); c == int32(2*total) {
			ch <- true
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := sc.Subscribe("foo", cb, stan.DeliverAllAvailable()); err != nil {
			t.Fatalf("Unexpected error on subscribe: %v", err)
		}
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our messages")
	}
	// Wait for more than the global limit MaxAge and verify messages
	// are still there
	time.Sleep(15 * time.Millisecond)
	s.mu.RLock()
	n, _, _ := s.store.MsgsState("foo")
	s.mu.RUnlock()
	if n != total {
		t.Fatalf("Should be %v messages, store reports %v", total, n)
	}
	// Now use a channel not defined in PerChannel and we should be subject
	// to global limits.
	for i := 0; i < total; i++ {
		if err := sc.Publish("bar", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error on publish: %v", err)
		}
	}
	if _, err := sc.Subscribe("bar", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// This one should fail
	if _, err := sc.Subscribe("bar", func(_ *stan.Msg) {}); err == nil {
		t.Fatal("Expected to fail to subscribe, did not")
	}
	// Wait more than MaxAge
	time.Sleep(15 * time.Millisecond)
	// Messages should have all disappear
	s.mu.RLock()
	n, _, _ = s.store.MsgsState("bar")
	s.mu.RUnlock()
	if n != 0 {
		t.Fatalf("Expected 0 messages, store reports %v", n)
	}
}

func TestFileStoreMultipleShadowQSubs(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	opts := getTestDefaultOptsForFileStore()
	s := runServerWithOpts(t, opts, nil)
	s.Shutdown()

	fs, err := stores.NewFileStore(testLogger, defaultDataStore, &stores.DefaultStoreLimits)
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer fs.Close()
	cs, _, err := fs.CreateChannel("foo", nil)
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	sub := spb.SubState{
		ID:            1,
		AckInbox:      nats.NewInbox(),
		Inbox:         nats.NewInbox(),
		AckWaitInSecs: 30,
		MaxInFlight:   10,
		LastSent:      1,
		IsDurable:     true,
		QGroup:        "dur:queue",
	}
	cs.Subs.CreateSub(&sub)
	sub.ID = 2
	sub.LastSent = 2
	cs.Subs.CreateSub(&sub)
	fs.Close()

	// Should not panic
	s = runServerWithOpts(t, opts, nil)
	defer s.Shutdown()
	scs, err := s.lookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Error looking up channel: %v", err)
	}
	ss := scs.UserData.(*subStore)
	ss.RLock()
	qs := ss.qsubs["dur:queue"]
	ss.RUnlock()
	if qs == nil {
		t.Fatal("Should have recovered queue group")
	}
	qs.RLock()
	shadow := qs.shadow
	lastSent := qs.lastSent
	qs.RUnlock()
	if shadow == nil {
		t.Fatal("Should have recovered a shadow queue sub")
	}
	if shadow.ID != 2 || lastSent != 2 {
		t.Fatalf("Recovered shadow queue sub should be ID 2, lastSent 2, got %v, %v", shadow.ID, lastSent)
	}
}
