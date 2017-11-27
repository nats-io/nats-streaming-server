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
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/test"
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
	defaultDataStore    string
	testLogger          logger.Logger
	errOnPurpose        = errors.New("On purpose")
	benchStoreType      = stores.TypeMemory
	persistentStoreType = stores.TypeFile
)

// The SourceAdmin is used by the test setup to have access
// to the database server and create the test streaming database.
// The Source contains the URL that the Store needs to actually
// connect to the server and use the database.
const (
	testDefaultDatabaseName = "test_server_nats_streaming"

	testDefaultMySQLSource      = "nss:password@/" + testDefaultDatabaseName
	testDefaultMySQLSourceAdmin = "nss:password@/"

	testDefaultPostgresSource      = "dbname=" + testDefaultDatabaseName + " sslmode=disable"
	testDefaultPostgresSourceAdmin = "sslmode=disable"
)

var (
	testSQLDriver       = test.DriverMySQL
	testSQLSource       = testDefaultMySQLSource
	testSQLSourceAdmin  = testDefaultMySQLSourceAdmin
	testSQLDatabaseName = testDefaultDatabaseName
)

func TestMain(m *testing.M) {
	var (
		bst   string
		pst   string
		doSQL bool
	)
	flag.StringVar(&bst, "bench_store", "", "store type for bench tests (mem, file)")
	flag.StringVar(&pst, "persistent_store", "", "store type for server recovery related tests (file)")
	test.AddSQLFlags(flag.CommandLine, &testSQLDriver, &testSQLSource, &testSQLSourceAdmin, &testSQLDatabaseName)
	flag.Parse()
	bst = strings.ToUpper(bst)
	pst = strings.ToUpper(pst)
	switch bst {
	case "":
		// use default
	case stores.TypeMemory, stores.TypeFile, stores.TypeSQL:
		benchStoreType = bst
	default:
		fmt.Printf("Unknown store %q for bench tests\n", bst)
		os.Exit(2)
	}
	// Will add DB store and others when avail
	switch pst {
	case "":
	// use default
	case stores.TypeFile, stores.TypeSQL:
		persistentStoreType = pst
	default:
		fmt.Printf("Unknown or unsupported store %q for persistent store server tests\n", pst)
		os.Exit(2)
	}

	// If either (or both) bench or tests select an SQL store, we need to do
	// so initializing and cleaning at the end of the test.
	doSQL = benchStoreType == stores.TypeSQL || persistentStoreType == stores.TypeSQL

	if doSQL {
		defaultSources := make(map[string][]string)
		defaultSources[test.DriverMySQL] = []string{testDefaultMySQLSource, testDefaultMySQLSourceAdmin}
		defaultSources[test.DriverPostgres] = []string{testDefaultPostgresSource, testDefaultPostgresSourceAdmin}
		if err := test.ProcessSQLFlags(flag.CommandLine, defaultSources); err != nil {
			fmt.Println(err.Error())
			os.Exit(2)
		}
		// Create the SQL Database once, the cleanup is simply deleting
		// content from tables (so we don't have to recreate them).
		if err := test.CreateSQLDatabase(testSQLDriver, testSQLSourceAdmin,
			testSQLSource, testSQLDatabaseName); err != nil {
			fmt.Printf("Error initializing SQL Datastore: %v", err)
			os.Exit(2)
		}
	}
	ret := m.Run()
	if doSQL {
		// Now that the tests/benchs are done, delete the database
		test.DeleteSQLDatabase(testSQLDriver, testSQLSourceAdmin, testSQLDatabaseName)
	}
	os.Exit(ret)
}

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
	// Make the server interpret all our sub's AckWait() as milliseconds instead
	// of seconds.
	testAckWaitIsInMillisecond = true
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

func channelsLookupOrCreate(t tLogger, s *StanServer, name string) *channel {
	c, err := s.lookupOrCreateChannel(name)
	if err != nil {
		stackFatalf(t, "Error creating/looking up channel %q: %v", name, err)
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

func cleanupDatastore(t tLogger) {
	switch persistentStoreType {
	case stores.TypeFile:
		if err := os.RemoveAll(defaultDataStore); err != nil {
			stackFatalf(t, "Error cleaning up datastore: %v", err)
		}
	case stores.TypeSQL:
		test.CleanupSQLDatastore(t, testSQLDriver, testSQLSource)
	}
}

func getTestDefaultOptsForPersistentStore() *Options {
	opts := GetDefaultOptions()
	opts.StoreType = persistentStoreType
	switch persistentStoreType {
	case stores.TypeFile:
		opts.FilestoreDir = defaultDataStore
		opts.FileStoreOpts.BufferSize = 1024
	case stores.TypeSQL:
		opts.SQLStoreOpts.Driver = testSQLDriver
		opts.SQLStoreOpts.Source = testSQLSource
	default:
		panic(fmt.Sprintf("Need to specify configuration for store: %q", persistentStoreType))
	}
	return opts
}

func ackWaitInMs(val int) time.Duration {
	// When creating a subscription without AckWait(), the library
	// sends AckWaitInSecs==30 for 30 seconds. If we want to
	// use milliseconds we are going to send a negative value
	// corresponding to the number of milliseconds we want.
	// The server variable testAckWaitIsInMillisecond is set
	// to true at the beginning of the test suite. With that,
	// the server will use the absoulute value and interpret
	// is as milliseconds.
	return time.Duration(val*-1) * time.Second
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

type testChannelStoreFailStore struct{ stores.Store }

func (s *testChannelStoreFailStore) CreateChannel(name string) (*stores.Channel, error) {
	return nil, errOnPurpose
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

func (d *dummyLogger) Noticef(format string, args ...interface{}) { d.log(format, args...) }
func (d *dummyLogger) Debugf(format string, args ...interface{})  { d.log(format, args...) }
func (d *dummyLogger) Tracef(format string, args ...interface{})  { d.log(format, args...) }
func (d *dummyLogger) Errorf(format string, args ...interface{})  { d.log(format, args...) }
func (d *dummyLogger) Fatalf(format string, args ...interface{})  { d.log(format, args...) }

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
			if cs.ss == nil {
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
			n, _, _ := s.channels.msgsState("foo")
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

	subs := s.clients.getSubs(clientName)
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

func TestPersistentStoreAcksPool(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
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
		subs := s.clients.getSubs(clientName)
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
	ch := make(chan bool)
	count := int32(0)
	cb := func(m *stan.Msg) {
		if !m.Redelivered && atomic.AddInt32(&count, 1) == atomic.LoadInt32(&totalSubs) {
			ch <- true
		}
	}
	// Create 10 subs
	for i := 0; i < int(totalSubs); i++ {
		sub, err := sc.Subscribe("foo", cb, stan.AckWait(ackWaitInMs(50)))
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
		cs := channelsGet(t, s.channels, "foo")
		cs.ss.RLock()
		subsArray := cs.ss.psubs
		cs.ss.RUnlock()
		timeout := time.Now().Add(2 * time.Second)
		ap := false
		for time.Now().Before(timeout) {
			for _, s := range subsArray {
				s.RLock()
				ap = len(s.acksPending) > 0
				s.RUnlock()
				if ap {
					break
				}
			}
			if ap {
				time.Sleep(10 * time.Millisecond)
				continue
			} else {
				break
			}
		}
		if ap {
			stackFatalf(t, "Unexpected unacknowledged messages")
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
	if _, err := sc.Subscribe("foo", cb, stan.AckWait(ackWaitInMs(15))); err != nil {
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
		stan.AckWait(ackWaitInMs(15))); err != nil {
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

func TestAckForUnknownChannel(t *testing.T) {
	logger := &checkErrorLogger{checkErrorStr: "not found"}
	opts := GetDefaultOptions()
	opts.CustomLogger = logger
	s, err := RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if _, err := sc.Subscribe("foo", func(_ *stan.Msg) {}); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	waitForNumSubs(t, s, clientName, 1)
	c := channelsGet(t, s.channels, "foo")
	sub := c.ss.psubs[0]

	ack := pb.Ack{
		Subject:  "bar",
		Sequence: 1,
	}
	ackBytes, err := ack.Marshal()
	if err != nil {
		t.Fatalf("Error during marshaling: %v", err)
	}
	sc.NatsConn().Publish(sub.AckInbox, ackBytes)
	timeout := time.Now().Add(3 * time.Second)
	for time.Now().Before(timeout) {
		logger.Lock()
		gotIt := logger.gotError
		logger.Unlock()
		if gotIt {
			// We are done!
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Server did not log error about not finding channel")
}
