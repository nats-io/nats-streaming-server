// Copyright 2016-2017 Apcera Inc. All rights reserved.
package server

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats-streaming-server/stores"
)

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
		t.Fatalf("Expected tracing to include debug and trace, got %v", str)
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

func TestStoreTypeUnknown(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := GetDefaultOptions()
	opts.StoreType = "MyType"

	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestFileStoreMissingDirectory(t *testing.T) {
	if persistentStoreType != stores.TypeFile {
		t.SkipNow()
	}
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := GetDefaultOptions()
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = ""

	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestPersistentStoreChangedClusterID(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
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

func TestPersistentStoreNoPanicOnShutdown(t *testing.T) {
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	opts := getTestDefaultOptsForPersistentStore()
	opts.NATSServerURL = nats.DefaultURL

	cleanupDatastore(t)
	defer cleanupDatastore(t)

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

func TestPersistentStoreRunServer(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
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
	cs := channelsGet(t, s.channels, "bar")
	func() {
		ss := cs.ss
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
	cs = channelsGet(t, s.channels, "baz")
	func() {
		ss := cs.ss
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
	cs = channelsGet(t, s.channels, "foo")
	func() {
		ss := cs.ss
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
