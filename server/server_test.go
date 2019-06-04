// Copyright 2016-2019 The NATS Authors
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
	"errors"
	"flag"
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

	natsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-streaming-server/logger"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	_ "github.com/lib/pq"              // postgres driver
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
	errOnPurpose        = errors.New("on purpose")
	benchStoreType      = stores.TypeMemory
	persistentStoreType = stores.TypeFile
	testUseEncryption   bool
	testEncryptionKey   = "testkey"
)

// The SourceAdmin is used by the test setup to have access
// to the database server and create the test streaming database.
// The Source contains the URL that the Store needs to actually
// connect to the server and use the database.
const (
	testDefaultDatabaseName = "test_server_nats_streaming"

	testDefaultMySQLSource      = "nss:password@/" + testDefaultDatabaseName
	testDefaultMySQLSourceAdmin = "nss:password@/"

	testDefaultPostgresSource      = "sslmode=disable dbname=" + testDefaultDatabaseName
	testDefaultPostgresSourceAdmin = "sslmode=disable"
)

var (
	testSQLDriver       = test.DriverMySQL
	testSQLSource       = testDefaultMySQLSource
	testSQLSourceAdmin  = testDefaultMySQLSourceAdmin
	testSQLDatabaseName = testDefaultDatabaseName
	testDBSuffixes      = []string{"", "_a", "_b", "_c"}
	doSQL               = false
)

func TestMain(m *testing.M) {
	var (
		bst         string
		pst         string
		sqlCreateDb bool
		sqlDeleteDb bool
	)
	flag.StringVar(&bst, "bench_store", "", "store type for bench tests (mem, file)")
	flag.StringVar(&pst, "persistent_store", "", "store type for server recovery related tests (file)")
	// This one is added here so that if we want to disable sql for stores tests
	// we can use the same param for all packages as in "go test -v ./... -sql=false"
	flag.Bool("sql", false, "Not used for server tests")
	// Those 2 sql related flags are handled here, not in AddSQLFlags
	flag.BoolVar(&sqlCreateDb, "sql_create_db", true, "create sql database on startup")
	flag.BoolVar(&sqlDeleteDb, "sql_delete_db", true, "delete sql database on exit")
	flag.BoolVar(&testUseEncryption, "encrypt", false, "use encryption")
	flag.StringVar(&testEncryptionKey, "encryption_key", string(testEncryptionKey), "encryption key")
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
		if sqlCreateDb {
			for _, n := range testDBSuffixes {
				// Create the SQL Database once, the cleanup is simply deleting
				// content from tables (so we don't have to recreate them).
				if err := test.CreateSQLDatabase(testSQLDriver, testSQLSourceAdmin,
					testSQLSource+n, testSQLDatabaseName+n); err != nil {
					fmt.Printf("Error initializing SQL Datastore: %v", err)
					os.Exit(2)
				}
			}
		}
	}
	ret := m.Run()
	if doSQL && sqlDeleteDb {
		// Now that the tests/benchs are done, delete the database
		for _, n := range testDBSuffixes {
			test.DeleteSQLDatabase(testSQLDriver, testSQLSourceAdmin, testSQLDatabaseName+n)
		}
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
	// For tests that use FileStore and do message expiration, etc..
	stores.FileStoreTestSetBackgroundTaskInterval(15 * time.Millisecond)
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...) + "\n" + stack()
	t.Fatalf(msg)
}

func stack() string {
	lines := make([]string, 0, 32)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	return strings.Join(lines, "\n")
}

func printAllStacks() {
	size := 1024 * 1024
	buf := make([]byte, size)
	n := 0
	for {
		n = runtime.Stack(buf, true)
		if n < size {
			break
		}
		size *= 2
		buf = make([]byte, size)
	}
	fmt.Printf("Go-routines:\n%s\n", string(buf[:n]))
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

// Helper function that waits up to totalWait for the function `f` to return
// without error. When f returns error, this function sleeps for waitInBetween.
// At the end of the totalWait, the last reported error from `f` causes the
// test to fail.
func waitFor(t tLogger, totalWait, waitInBetween time.Duration, f func() error) {
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(waitInBetween)
	}
	if err != nil {
		stackFatalf(t, err.Error())
	}
}

// Helper function that waits that the number returned by function `f`
// is equal to `expected` for a certain period of time, otherwise fails.
func waitForCount(t tLogger, expected int, f func() (string, int)) {
	waitFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		label, count := f()
		if count != expected {
			return fmt.Errorf("Timeout waiting to get %v %s, got %v", expected, label, count)
		}
		return nil
	})
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
		if c == nil {
			// Could happen in clustering mode when creation
			// of channel did not happen yet in a node and test
			// if checking that node. Just return something different
			// from expected to cause waitForCount to try again.
			return "subscriptions", -1
		}
		c.RLock()
		defer c.RUnlock()
		return "subscriptions", len(c.subs)
	})
}

func waitForAcks(t tLogger, s *StanServer, ID string, subID uint64, expected int) {
	var sub *subState
	waitFor(t, 5*time.Second, 15*time.Millisecond, func() error {
		subs := s.clients.getSubs(ID)
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
			return fmt.Errorf("Subscription %v not found", subID)
		}
		return nil
	})
	waitForCount(t, expected, func() (string, int) {
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
		for _, n := range testDBSuffixes {
			test.CleanupSQLDatastore(t, testSQLDriver, testSQLSource+n)
		}
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
	if testUseEncryption {
		if sOpts == nil {
			sOpts = GetDefaultOptions()
		}
		sOpts.Encrypt = true
		sOpts.EncryptionKey = []byte(testEncryptionKey)
	}
	s, err := RunServerWithOpts(sOpts, nOpts)
	if err != nil {
		stackFatalf(t, err.Error())
	}
	if testUseEncryption {
		// Since the key was cleared when creating the crypto store,
		// restore now for the rest of tests to work properly.
		sOpts.EncryptionKey = []byte(testEncryptionKey)
	}
	return s
}

func runServer(t *testing.T, clusterName string) *StanServer {
	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	nOpts := DefaultNatsServerOptions
	return runServerWithOpts(t, sOpts, &nOpts)
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
func (d *dummyLogger) Warnf(format string, args ...interface{})   { d.log(format, args...) }

func TestVersionMatchesTag(t *testing.T) {
	tag := os.Getenv("TRAVIS_TAG")
	if tag == "" {
		t.SkipNow()
	}
	// We expect a tag of the form vX.Y.Z. If that's not the case,
	// we need someone to have a look. So fail if first letter is not
	// a `v`
	if tag[0] != 'v' {
		t.Fatalf("Expect tag to start with `v`, tag is: %s", tag)
	}
	// Strip the `v` from the tag for the version comparison.
	if VERSION != tag[1:] {
		t.Fatalf("Version (%s) does not match tag (%s)", VERSION, tag[1:])
	}
}

func TestChannelStore(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	cs := newChannelStore(s, s.store)
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
	c.store.Msgs.Store(&pb.MsgProto{Sequence: 1, Data: []byte("foo")})
	c4.store.Msgs.Store(&pb.MsgProto{Sequence: 1, Data: []byte("bar")})
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

	opts = GetDefaultOptions()
	opts.Clustering.Peers = []string{"a", "b", "c"}
	clone = opts.Clone()
	if !reflect.DeepEqual(opts.Clustering.Peers, clone.Clustering.Peers) {
		t.Fatalf("Expected %#v, got %#v", opts.Clustering.Peers, clone.Clustering.Peers)
	}
	opts.Clustering.Peers[0] = "z"
	if reflect.DeepEqual(opts.Clustering.Peers, clone.Clustering.Peers) {
		t.Fatal("Expected clone to be different after original changed, was not")
	}
}

func TestConcurrentLookupOfChannels(t *testing.T) {
	// The original test (TestGetSubStoreRace) was a test to detect a race
	// that previously existed when creating a channel's subStore. However,
	// the internal code around that has drastically changed and does not
	// make that test really relevant. Will keep this to perform concurrent
	// lookup or create or channels, but lower the number of channels to
	// 100 (down from 8000) because otherwise with -race, this takes a lot
	// of memory, slowing down some other tests below...
	numChans := 100

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
	for i := 0; i < 2; i++ {
		func() {
			cleanupDatastore(t)
			defer cleanupDatastore(t)

			opts := getTestDefaultOptsForPersistentStore()
			if i == 1 {
				opts.Partitioning = true
				opts.AddPerChannel("foo", &stores.ChannelLimits{})
				opts.AddPerChannel("bar", &stores.ChannelLimits{})
				opts.AddPerChannel("baz", &stores.ChannelLimits{})
			}
			s := runServerWithOpts(t, opts, nil)
			defer s.Shutdown()

			pubSc := NewDefaultConnection(t)
			defer pubSc.Close()

			total := 1000
			for i := 0; i < total; i++ {
				if _, err := pubSc.PublishAsync("baz", []byte("hello"), nil); err != nil {
					t.Fatalf("Unexpected error on publish: %v", err)
				}
			}
			pubSc.Close()

			// The barrier for close just guarantees that all the clientPublish
			// callbacks have been invoked (where we check that the pub message
			// comes from a valid connection), not that messages have been stored.
			// We will check the channel's store msgs count but expect that we
			// may not get the correct count right away.
			timeout := time.Now().Add(5 * time.Second)
			count := 0
			for time.Now().Before(timeout) {
				c := s.channels.get("baz")
				if c != nil {
					count, _ = msgStoreState(t, c.store.Msgs)
					if count == total {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
			if count != total {
				t.Fatalf("There should have been %d messages, got %v", total, count)
			}

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

			total = 50
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
		}()
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

func TestAckProcessedBeforeClose(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	ch := make(chan bool, 1)
	for i := 0; i < 50; i++ {
		if _, err := sc.Subscribe("foo",
			func(m *stan.Msg) {
				m.Ack()
				m.Sub.Close()
				ch <- true
			},
			stan.DeliverAllAvailable(),
			stan.DurableName("dur"),
			stan.SetManualAckMode()); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if err := Wait(ch); err != nil {
			t.Fatal("Did not get our message")
		}
		pc := 0
		cs := s.channels.get("foo")
		cs.ss.RLock()
		for _, dur := range cs.ss.durables {
			pc = len(dur.acksPending)
		}
		cs.ss.RUnlock()
		if pc != 0 {
			t.Fatalf("Did not expect pending messages, got %v", pc)
		}
		dur, err := sc.Subscribe("foo", func(_ *stan.Msg) {}, stan.DurableName("dur"))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		dur.Unsubscribe()
	}
}
