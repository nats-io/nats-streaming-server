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
	natsdTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
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
	d := &captureFatalLogger{}

	sOpts := GetDefaultOptions()
	sOpts.NATSServerURL = "nats://127.0.0.1:4444"
	sOpts.CustomLogger = d

	// We expect the server to fail to start
	s, err := RunServerWithOpts(sOpts, nil)
	if err == nil {
		s.Shutdown()
		t.Fatal("Expected error, got none")
	}

	// We should get a trace in the log
	if !strings.Contains(d.fatal, "Failed to start") {
		t.Fatalf("Expected to get a cause as invalid connection, got: %v", d.msg)
	}
}

func TestServerLoggerDebugAndTrace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
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
	buf := make([]byte, 10000)
	out := make([]byte, 0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			n, _ := r.Read(buf)
			out = append(out, buf[:n]...)
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	s, err := RunServerWithOpts(sOpts, nil)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	sc := NewDefaultConnection(t)
	sc.Publish("foo", []byte("hello"))
	sc.Close()
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
	if !strings.Contains(str, "Received message from publisher") || !strings.Contains(str, "Publish subject") {
		t.Fatalf("Expected tracing to include debug and trace, got %v", str)
	}
}

// TestEnsureStandAlone tests to ensure the server
// panics if it detects another instance running in
// a cluster.
func TestEnsureStandAlone(t *testing.T) {

	// Start a streaming server, and setup a route
	nOpts := DefaultNatsServerOptions
	nOpts.Cluster.Name = "abc"
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
	nOpts2.Cluster.Name = "abc"
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
	sOpts.ClientCA = "../test/certs/ca.pem"

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()
}

func TestTLSSwitchAutomatically(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.TLSCert = "../test/certs/server-cert.pem"
	nOpts.TLSKey = "../test/certs/server-key.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ID = clusterName
	sOpts.ClientCA = "../test/certs/ca.pem"

	s, err := RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		t.Fatalf("Should have connected, got %v", err)
	}
	s.Shutdown()
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

func TestTLSServerNameAndSkipVerify(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.Host = "0.0.0.0"
	nOpts.TLSCert = "../test/certs/server-noip.pem"
	nOpts.TLSKey = "../test/certs/server-key-noip.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.ClientCert = "../test/certs/client-cert.pem"
	sOpts.ClientKey = "../test/certs/client-key.pem"
	sOpts.ClientCA = "../test/certs/ca.pem"

	s, err := RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}

	sOpts.TLSServerName = "localhost"
	s, err = RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		t.Fatalf("Expected server to start ok, got %v", err)
	}
	s.Shutdown()
	s = nil

	sOpts.TLSServerName = ""
	s, err = RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}

	sOpts.TLSSkipVerify = true
	s, err = RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		t.Fatalf("Expected server to start ok, got %v", err)
	}
	s.Shutdown()

	// With insecure, all client cert/key/ca can be removed
	// and connections should still succeed
	sOpts.ClientCert, sOpts.ClientKey, sOpts.ClientCA = "", "", ""
	s, err = RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		t.Fatalf("Expected server to start ok, got %v", err)
	}
	s.Shutdown()

	// However, it should fail if NATS Server requires client cert verification
	nOpts.TLSVerify = true
	s, err = RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
}

func TestTLSServerNameAndSkipVerifyConflicts(t *testing.T) {
	nOpts := DefaultNatsServerOptions

	nOpts.Host = "0.0.0.0"
	nOpts.TLSCert = "../test/certs/server-noip.pem"
	nOpts.TLSKey = "../test/certs/server-key-noip.pem"
	nOpts.TLSCaCert = "../test/certs/ca.pem"

	sOpts := GetDefaultOptions()
	sOpts.NATSClientOpts = []nats.Option{nats.Secure(&tls.Config{ServerName: "localhost"})}
	sOpts.ClientCert = "../test/certs/client-cert.pem"
	sOpts.ClientKey = "../test/certs/client-key.pem"
	sOpts.ClientCA = "../test/certs/ca.pem"
	sOpts.TLSServerName = "confict"

	s, err := RunServerWithOpts(sOpts, &nOpts)
	if s != nil || err == nil {
		s.Shutdown()
		t.Fatal("Expected server to fail to start, it did not")
	}
	if !strings.Contains(err.Error(), "conflict between") {
		t.Fatalf("Error should indicate conflict, got %v", err)
	}
	sOpts.TLSServerName = ""
	s, err = RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		t.Fatalf("Unable to start server: %v", err)
	}
	s.Shutdown()

	// Pass skip verify as a NATS option
	sOpts.NATSClientOpts = []nats.Option{nats.Secure(&tls.Config{InsecureSkipVerify: true})}
	// Use the wrong name for streaming TLSServerName
	sOpts.TLSServerName = "wrong"
	// And make sure that NATS Option skip veriy is not overridden with that..
	sOpts.TLSSkipVerify = false
	s, err = RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		t.Fatalf("Unable to start server: %v", err)
	}
	s.Shutdown()
}

func TestDontEmbedNATSNotRunning(t *testing.T) {
	sOpts := GetDefaultOptions()
	// Make sure that with empty string (normally the default), we
	// can run the streaming server (will embed NATS)
	sOpts.NATSServerURL = ""
	s := runServerWithOpts(t, sOpts, nil)
	s.Shutdown()

	// Point to a NATS Server that will not be running
	sOpts.NATSServerURL = "nats://127.0.0.1:5223"

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
	sOpts.NATSServerURL = "nats://127.0.0.1:5223"

	nOpts := DefaultNatsServerOptions
	nOpts.Host = "127.0.0.1"
	nOpts.Port = 5223
	natsd := natsdTest.RunServer(&nOpts)
	defer natsd.Shutdown()

	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()
}

func TestDontEmbedNATSMultipleURLs(t *testing.T) {
	nOpts := DefaultNatsServerOptions
	nOpts.Host = "127.0.0.1"
	nOpts.Port = 5223
	nOpts.Username = "ivan"
	nOpts.Password = "pwd"
	natsServer := natsdTest.RunServer(&nOpts)
	defer natsServer.Shutdown()

	sOpts := GetDefaultOptions()

	workingURLs := []string{
		"nats://127.0.0.1:5223",
		"nats://ivan:pwd@127.0.0.1:5223",
		"nats://ivan:pwd@127.0.0.1:5223, nats://ivan:pwd@127.0.0.1:5224",
	}
	for _, url := range workingURLs {
		sOpts.NATSServerURL = url
		s := runServerWithOpts(t, sOpts, &nOpts)
		s.Shutdown()
	}

	notWorkingURLs := []string{
		"nats://ivan:incorrect@127.0.0.1:5223",
		"nats://127.0.0.1:5223,nats://ivan:pwd@127.0.0.1:5224",
		"nats://127.0.0.1",
		"127.0.0.1:5223",
		"127.0.0.1",
		"nats://ivan:pwd@:5224",
		" ",
	}
	for _, url := range notWorkingURLs {
		sOpts.NATSServerURL = url
		s, err := RunServerWithOpts(sOpts, nil)
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
	sendQuit := make(chan bool, 1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	nc, err := nats.Connect(nats.DefaultURL, nats.NoReconnect())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	sc, err := stan.Connect(clusterName, clientName,
		stan.NatsConn(nc),
		stan.PubAckWait(250*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()
	go func() {
		defer wg.Done()
		payload := []byte("hello")
		for {
			select {
			case <-sendQuit:
				// We know that the server is shutdown, so close
				// the NATS connection first so that STAN does not
				// try to send the close protocol (which will timeout
				// with a 2sec by default).
				nc.Close()
				sc.Close()
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

type captureNoticesLogger struct {
	dummyLogger
	notices []string
}

func (l *captureNoticesLogger) Noticef(format string, args ...interface{}) {
	l.Lock()
	n := fmt.Sprintf(format, args...)
	l.notices = append(l.notices, fmt.Sprintf("%s\n", n))
	l.Unlock()
}

type captureWarnLogger struct {
	dummyLogger
	warnings []string
}

func (l *captureWarnLogger) Warnf(format string, args ...interface{}) {
	l.Lock()
	n := fmt.Sprintf(format, args...)
	l.warnings = append(l.warnings, fmt.Sprintf("%s\n", n))
	l.Unlock()
}

type captureFatalLogger struct {
	dummyLogger
	fatal string
}

func (l *captureFatalLogger) Fatalf(format string, args ...interface{}) {
	l.Lock()
	// Normally the server would stop after first fatal error.
	// So capture only one.
	if l.fatal == "" {
		l.fatal = fmt.Sprintf(format, args...)
	}
	l.Unlock()
}

type storeNoClose struct {
	stores.Store
}

func (snc *storeNoClose) Close() error {
	return nil
}

func TestGhostDurableSubs(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	c, err := s.lookupOrCreateChannel("foo")
	if err != nil {
		t.Fatalf("Error creating channel: %v", err)
	}
	badDurableQueueSub := &spb.SubState{
		ClientID:      "me",
		IsDurable:     true,
		QGroup:        "myqueue",
		Inbox:         "inbox",
		AckInbox:      "myack",
		AckWaitInSecs: 30,
		MaxInFlight:   1,
	}
	if err := c.store.Subs.CreateSub(badDurableQueueSub); err != nil {
		t.Fatalf("Error creating bad sub: %v", err)
	}
	s.Shutdown()

	// Re-open
	l := &captureWarnLogger{}
	opts.EnableLogging = true
	opts.CustomLogger = l
	s = runServerWithOpts(t, opts, nil)

	// Check if the error message appeared or not
	check := func(expected bool) {
		present := false
		l.Lock()
		for _, n := range l.warnings {
			if strings.Contains(n, "ghost") {
				present = true
				break
			}
		}
		warnings := l.warnings
		// clear the logger notices
		l.warnings = nil
		l.Unlock()
		if expected && !present {
			stackFatalf(t, "No sign of ghost warning in the log:\n%v", warnings)
		} else if !expected && present {
			stackFatalf(t, "The warning should no longer be in the log:\n%v", warnings)
		}
	}
	s.Shutdown()
	check(true)

	// We cannot do the rest of this test on Windows because
	// to simulate crash we don't close store and restart the
	// server. This would not be allowed on Windows.
	if runtime.GOOS == "windows" {
		return
	}

	// Re-open, this time, there should not be the warning anymore.
	opts.ClientHBInterval = 250 * time.Millisecond
	opts.ClientHBTimeout = 100 * time.Millisecond
	opts.ClientHBFailCount = 1
	s = runServerWithOpts(t, opts, nil)
	check(false)

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()
	sc, err := stan.Connect(clusterName, clientName,
		stan.NatsConn(nc), stan.ConnectWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()

	ch := make(chan bool, 1)
	// Create queue sub
	if _, err := sc.QueueSubscribe("foo", "bar",
		func(_ *stan.Msg) {
			ch <- true
		},
		stan.DurableName("dur")); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}

	// Close NATS connection to cause server to close client connection due
	// to missed HBs.
	nc.Close()

	waitForNumClients(t, s, 0)

	// Change store to simulate no flush on simulated crash
	orgStore := s.store
	s.store = &storeNoClose{Store: s.store}
	s.Shutdown()

	// Re-open, there should be no warning.
	s = runServerWithOpts(t, opts, nil)
	check(false)

	sc.Close()
	orgStore.Close()
}

func TestGetNATSOptions(t *testing.T) {
	opts := GetDefaultOptions()
	nopts := NewNATSOptions()
	nopts.Host = "127.0.0.1"
	nopts.Port = 4567
	s, err := RunServerWithOpts(opts, nopts)
	if err != nil {
		t.Fatalf("Error running server: %v", err)
	}
	defer s.Shutdown()
	sc, err := stan.Connect(clusterName, clientName, stan.NatsURL("nats://127.0.0.1:4567"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	sc.Close()
}

func TestRunServerWithCrypto(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	opts.Encrypt = true

	s, err := RunServerWithOpts(opts, nil)
	if s != nil || err == nil {
		if s != nil {
			s.Shutdown()
		}
		t.Fatalf("Expected no server and error, got %v %v", s, err)
	}

	opts.EncryptionKey = []byte("testkey")
	s = runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	sc, nc := createConnectionWithNatsOpts(t, clientName, nats.ReconnectWait(50*time.Millisecond))
	defer sc.Close()
	defer nc.Close()

	msg := []byte("hello")
	if err := sc.Publish("foo", msg); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}

	msgCh := make(chan *stan.Msg, 1)
	if _, err := sc.Subscribe("foo", func(m *stan.Msg) {
		msgCh <- m
		m.Sub.Close()
	}, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	select {
	case m := <-msgCh:
		if !reflect.DeepEqual(m.Data, msg) {
			t.Fatalf("Unexpected message, got %s", m.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not receive message")
	}

	s.Shutdown()

	// Restart without encryption, should get encrypted content.
	// Do not use test's runServerWithOpts() because it would
	// set the encryption key by default.
	opts.Encrypt = false
	opts.EncryptionKey = nil
	s, err = RunServerWithOpts(opts, nil)
	if err != nil {
		t.Fatalf("Error starting server: %v", err)
	}

	if _, err := sc.Subscribe("foo", func(m *stan.Msg) {
		msgCh <- m
		m.Sub.Close()
	}, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	select {
	case m := <-msgCh:
		if reflect.DeepEqual(m.Data, msg) {
			t.Fatalf("Unexpected message, got %s", m.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not receive message")
	}
}

func TestDontExposeUserPassword(t *testing.T) {
	ns := natsdTest.RunDefaultServer()
	defer shutdownRestartedNATSServerOnTestExit(&ns)

	l := &captureNoticesLogger{}
	sOpts := GetDefaultOptions()
	sOpts.CustomLogger = l
	sOpts.NATSServerURL = "nats://127.0.0.1:4222"
	nOpts := natsdTest.DefaultTestOptions
	nOpts.Username = "ivan"
	nOpts.Password = "password"
	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()

	// Restart the NATS server that should cause streaming
	// to reconnect and log the connected url.
	ns.Shutdown()
	ns = natsdTest.RunDefaultServer()

	collectNotice := func(t *testing.T) string {
		t.Helper()
		var msg string
		waitFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			l.Lock()
			for _, n := range l.notices {
				if strings.Contains(n, "general\" reconnected to NATS Server at") {
					msg = n
					l.Unlock()
					return nil
				}
			}
			l.Unlock()
			return fmt.Errorf("did not get proper notices")
		})
		return msg
	}
	// Now make sure that this string does not contain our user/password
	msg := collectNotice(t)
	if strings.Contains(msg, "ivan:password@") {
		t.Fatalf("Password exposed in url: %v", msg)
	}

	// Now try again but with nats_server_url that contains user/pass
	s.Shutdown()
	l.Lock()
	l.notices = l.notices[:0]
	l.Unlock()
	sOpts.NATSServerURL = "nats://ivan:password@127.0.0.1:4222"
	s = runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()

	// Restart the NATS server that should cause streaming
	// to reconnect and log the connected url.
	ns.Shutdown()
	ns = natsdTest.RunDefaultServer()

	msg = collectNotice(t)
	if !strings.Contains(msg, "nats://[REDACTED]@127.0.0.1:") {
		t.Fatalf("Password exposed in url: %v", msg)
	}
}

func TestStreamingServerReadyLog(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)
	cleanupRaftLog(t)
	defer cleanupRaftLog(t)

	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	checkLog := func(t *testing.T, l *captureNoticesLogger, expected bool) {
		t.Helper()
		check := func() error {
			present := false
			l.Lock()
			for _, n := range l.notices {
				if strings.Contains(n, streamingReadyLog) {
					present = true
					break
				}
			}
			l.Unlock()
			if !expected && present {
				return fmt.Errorf("Did not expect the log statement at this time")
			} else if expected && !present {
				return fmt.Errorf("Log statement still not present")
			}
			return nil
		}
		// When not expected, wait a bit to make sure that the
		// statement does not show up.
		if !expected {
			time.Sleep(500 * time.Millisecond)
			if err := check(); err != nil {
				t.Fatal(err.Error())
			}
		} else {
			waitFor(t, time.Second, 15*time.Millisecond, func() error {
				return check()
			})
		}
	}

	// Standalone case
	sOpts := GetDefaultOptions()
	l := &captureNoticesLogger{}
	sOpts.CustomLogger = l
	sOpts.NATSServerURL = "nats://127.0.0.1:4222"
	s := runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()
	checkLog(t, l, true)
	s.Shutdown()

	// FT case..
	sOpts = getTestFTDefaultOptions()
	l = &captureNoticesLogger{}
	sOpts.CustomLogger = l
	sOpts.NATSServerURL = "nats://127.0.0.1:4222"
	s = runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()
	checkLog(t, l, true)

	sOpts = getTestFTDefaultOptions()
	l2 := &captureNoticesLogger{}
	sOpts.CustomLogger = l2
	sOpts.NATSServerURL = "nats://127.0.0.1:4222"
	s2 := runServerWithOpts(t, sOpts, nil)
	defer s2.Shutdown()
	// At first, we should not get the lock since server
	// is standby...
	checkLog(t, l2, false)
	// Now shutdown s and s2 should report it is ready
	s.Shutdown()
	checkLog(t, l2, true)
	s2.Shutdown()

	// Clustering case..
	sOpts = getTestDefaultOptsForClustering("a", false)
	sOpts.Clustering.Peers = []string{"b"}
	l = &captureNoticesLogger{}
	sOpts.CustomLogger = l
	s = runServerWithOpts(t, sOpts, nil)
	defer s.Shutdown()
	checkLog(t, l, false)

	sOpts = getTestDefaultOptsForClustering("b", false)
	sOpts.Clustering.Peers = []string{"a"}
	s2 = runServerWithOpts(t, sOpts, nil)
	defer s2.Shutdown()

	getLeader(t, 5*time.Second, s, s2)
	checkLog(t, l, true)
}

func TestReopenLogFileStopsNATSDebugTrace(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "nats-streaming-server")
	if err != nil {
		t.Fatal("Could not create tmp dir")
	}
	defer os.RemoveAll(tmpDir)
	file, err := ioutil.TempFile(tmpDir, "log_")
	if err != nil {
		t.Fatalf("Could not create the temp file: %v", err)
	}
	file.Close()

	nOpts := natsdTest.DefaultTestOptions
	nOpts.LogFile = file.Name()
	nOpts.Debug = true
	nOpts.Trace = true
	nOpts.Logtime = true
	nOpts.LogSizeLimit = 20 * 1024

	sOpts := GetDefaultOptions()
	// Ensure STAN debug and trace are set to false. This was the issue
	sOpts.Debug = false
	sOpts.Trace = false
	sOpts.EnableLogging = true
	s := runServerWithOpts(t, sOpts, &nOpts)
	defer s.Shutdown()

	check := func(str string, expected bool) {
		t.Helper()
		buf, err := ioutil.ReadFile(nOpts.LogFile)
		if err != nil {
			t.Fatalf("Error reading file: %v", err)
		}
		sbuf := string(buf)
		present := strings.Contains(sbuf, str)
		if expected && !present {
			t.Fatalf("Expected to find %q, did not: %s", str, sbuf)
		} else if !expected && present {
			t.Fatalf("Expected to not find %q, but it was: %s", str, sbuf)
		}
	}
	sc, err := stan.Connect(clusterName, "before_reopen")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()
	check("before_reopen", true)
	check("[DBG] STREAM: [Client:before_reopen]", false)

	s.log.ReopenLogFile()
	check("File log re-opened", true)

	sc.Close()
	sc, err = stan.Connect(clusterName, "after_reopen")
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()
	check("after_reopen", true)
	check("[DBG] STREAM: [Client:after_reopen]", false)

	payload := make([]byte, 1000)
	for i := 0; i < len(payload); i++ {
		payload[i] = 'A'
	}
	pstr := string(payload)
	for i := 0; i < 10; i++ {
		s.log.Noticef(pstr)
	}
	// Check that size limit has been applied.
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Error reading directory: %v", err)
	}
	if len(files) == 1 {
		t.Fatal("Expected log to have been rotated, was not")
	}
}
