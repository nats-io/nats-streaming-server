// Copyright 2017-2018 The NATS Authors
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
	"fmt"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nuid"
	"strings"
	"sync"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	natsdTest "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
)

func setPartitionsVarsForTest() {
	partitionsRequestTimeout = 250 * time.Millisecond
	partitionsNoPanic = true
}

func resetDefaultPartitionsVars() {
	partitionsRequestTimeout = partitionsDefaultRequestTimeout
	partitionsNoPanic = false
}

func TestPartitionsNoChannelConfiguredError(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	opts := GetDefaultOptions()
	opts.Partitioning = true
	s, err := RunServerWithOpts(opts, nil)
	if s != nil {
		s.Shutdown()
		t.Fatalf("No server should be returned")
	}
	if err != ErrNoChannel {
		t.Fatalf("Expected ErrNoStaticChannel error, got: %v", err)
	}
}

func TestPartitionsInvalidChannelName(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	opts := GetDefaultOptions()
	opts.Partitioning = true
	serverShouldFail := func(channel string) {
		opts.StoreLimits.PerChannel = nil
		opts.StoreLimits.AddPerChannel(channel, &stores.ChannelLimits{})
		s, err := RunServerWithOpts(opts, nil)
		if s != nil {
			s.Shutdown()
		}
		if err == nil || !strings.Contains(err.Error(), "channel name") {
			t.Fatalf("Expected error about invalid channel name, got %v", err)
		}
	}
	serverShouldFail(".")
	serverShouldFail(".foo")
	serverShouldFail("foo.")
	serverShouldFail("foo*")
	serverShouldFail("foo.*.")
	serverShouldFail("foo.>.bar")
	serverShouldFail("foo/bar")
}

func TestPartitionsInvalidRequest(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	opts := GetDefaultOptions()
	opts.Partitioning = true
	opts.StoreLimits.AddPerChannel("foo", &stores.ChannelLimits{})
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	// Use raw NATS to send invalid requests
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	subj := partitionsPrefix + "." + clusterName

	// Sending those invalid requests should not make the server crash
	// and we should not get a response back
	msgCh := make(chan *nats.Msg)
	inbox := nats.NewInbox()
	if _, err := nc.Subscribe(inbox, func(m *nats.Msg) {
		msgCh <- m
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	verifyNoResponse := func() {
		select {
		case <-msgCh:
			stackFatalf(t, "Should not have receive a response, got: %v")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Send message without Reply
	nc.Publish(subj, []byte("hello"))
	verifyNoResponse()

	// Send request with empty body.
	nc.PublishRequest(subj, inbox, nil)
	verifyNoResponse()

	// Send dummy request (Unmarshal should fail).
	nc.PublishRequest(subj, inbox, []byte("hello"))
	verifyNoResponse()

	// Send invalid data for the channels encoding
	req := &spb.CtrlMsg{
		ServerID: "otherserver",
		MsgType:  spb.CtrlMsg_Partitioning,
		Data:     []byte{1, 2, 3, 4, 5},
	}
	reqBytes, _ := req.Marshal()
	nc.PublishRequest(subj, inbox, reqBytes)
	verifyNoResponse()

	req.Data = []byte{1}
	reqBytes, _ = req.Marshal()
	nc.PublishRequest(subj, inbox, reqBytes)
	verifyNoResponse()
}

func TestPartitionsMaxPayload(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	// For this test, both server will connect to same NATS Server
	ncOpts := natsdTest.DefaultTestOptions
	// Change MaxPayload
	ncOpts.MaxPayload = 50
	ns := natsdTest.RunServer(&ncOpts)
	defer ns.Shutdown()

	opts1 := GetDefaultOptions()
	opts1.NATSServerURL = "nats://localhost:4222"
	opts1.Partitioning = true
	opts1.StoreLimits.AddPerChannel("foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoo", &stores.ChannelLimits{})
	failSrv, err := RunServerWithOpts(opts1, nil)
	if failSrv != nil {
		failSrv.Shutdown()
	}
	// It should fail since we would not be able to send a single channel
	// due to MaxPayload restrictions.
	if err == nil {
		t.Fatal("Should have failed")
	}
	ns.Shutdown()

	// Change MaxPayload
	ncOpts.MaxPayload = 100
	ns = natsdTest.RunServer(&ncOpts)
	defer ns.Shutdown()

	opts1 = GetDefaultOptions()
	opts1.NATSServerURL = "nats://localhost:4222"
	opts1.Partitioning = true
	opts1.StoreLimits.AddPerChannel("foo", &stores.ChannelLimits{})
	s1 := runServerWithOpts(t, opts1, nil)
	defer s1.Shutdown()

	// Create raw subscriber to check what is being sent by s2
	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", ncOpts.Host, ncOpts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	total := 100
	count := 0
	ch := make(chan bool)
	verifyChannels := make(map[string]struct{})
	cb := func(m *nats.Msg) {
		req := &spb.CtrlMsg{}
		req.Unmarshal(m.Data)
		channels, _ := util.DecodeChannels(req.Data)
		for _, c := range channels {
			verifyChannels[c] = struct{}{}
			count++
		}
		if count == total {
			ch <- true
		}
	}
	if _, err := nc.Subscribe(partitionsPrefix+"."+clusterName, cb); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	opts2 := GetDefaultOptions()
	opts2.NATSServerURL = "nats://localhost:4222"
	opts2.Partitioning = true
	for i := 0; i < total-1; i++ {
		channelName := fmt.Sprintf("channel.number.%d", (i + 1))
		opts2.StoreLimits.AddPerChannel(channelName, &stores.ChannelLimits{})
	}
	// Add "foo". There is no guarantee that foo is in the last message
	// sent to s1, but we want to make sure that all requests are processed
	// and s2 receives an error back.
	opts2.StoreLimits.AddPerChannel("foo", &stores.ChannelLimits{})
	// This is expected to fail
	s2, err := RunServerWithOpts(opts2, nil)
	if s2 != nil {
		s2.Shutdown()
	}
	if err == nil {
		t.Fatal("Expected server to fail to start, did not")
	}
	// Wait that we have received all channels
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our channels list")
	}
	// We have received the total amount, now verify that we have all unique ones
	if len(verifyChannels) != total {
		t.Fatalf("Expected to get %d distinct channels, got %v", total, len(verifyChannels))
	}
}

func TestPartitionsOnlyThoseWork(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	okChannel := "foo"
	notOkChannel := "bar"

	opts := GetDefaultOptions()
	opts.Partitioning = true
	opts.StoreLimits.AddPerChannel(okChannel, &stores.ChannelLimits{})
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc, err := stan.Connect(clusterName, clientName,
		stan.ConnectWait(250*time.Millisecond), // works also for subscription requests
		stan.PubAckWait(250*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()
	// Should not be a problem to send a message to "foo"
	if err := sc.Publish(okChannel, []byte("msg to foo")); err != nil {
		t.Fatalf("Unexpected error on publish: %v", err)
	}
	// However, sending to bar should timeout
	if err := sc.Publish(notOkChannel, []byte("should fail")); err != stan.ErrTimeout {
		t.Fatalf("Expected timeout, got: %v", err)
	}
	// Creating subscription to "foo" should work and we should get the message
	ch := make(chan bool)
	if _, err := sc.Subscribe(okChannel, func(_ *stan.Msg) {
		ch <- true
	}, stan.DeliverAllAvailable()); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not get our message")
	}
	// Creating subscription on "bar" should fail with timeout since at this
	// point there is a single server and it is not handling "bar".
	if _, err := sc.Subscribe(notOkChannel, func(_ *stan.Msg) {}); err != stan.ErrSubReqTimeout {
		t.Fatalf("Expected timeout on subscribe, got: %v", err)
	}
}

func TestPartitionsWithClusterOfServers(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	// For this test, create a single NATS server to which both servers connect to.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	fooSubj := "foo"
	barSubj := "bar"

	opts1 := GetDefaultOptions()
	opts1.NATSServerURL = "nats://localhost:4222"
	opts1.Partitioning = true
	opts1.StoreLimits.AddPerChannel(fooSubj, &stores.ChannelLimits{})
	s1 := runServerWithOpts(t, opts1, nil)
	defer s1.Shutdown()

	opts2 := GetDefaultOptions()
	opts2.NATSServerURL = "nats://localhost:4222"
	opts2.Partitioning = true
	opts2.StoreLimits.AddPerChannel(barSubj, &stores.ChannelLimits{})
	s2 := runServerWithOpts(t, opts2, nil)
	defer s2.Shutdown()

	sc, err := stan.Connect(clusterName, clientName,
		stan.ConnectWait(250*time.Millisecond), // works also for subscription requests
		stan.PubAckWait(250*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()
	send := func(subj string) {
		if err := sc.Publish(subj, []byte("msg")); err != nil {
			stackFatalf(t, "Unexpected error on publish: %v", err)
		}
	}
	// Wait for client to be registered on both servers
	waitForNumClients(t, s1, 1)
	waitForNumClients(t, s2, 1)
	// Should not be a problem to send a message to "foo"
	send(fooSubj)
	// Sending to bar should work too..
	send(barSubj)

	sub := func(subj string) {
		ch := make(chan bool)
		sub, err := sc.Subscribe(subj, func(_ *stan.Msg) {
			ch <- true
		}, stan.DeliverAllAvailable())
		if err != nil {
			stackFatalf(t, "Unexpected error on subscribe: %v", err)
		}
		if err := Wait(ch); err != nil {
			stackFatalf(t, "Did not get our message")
		}
		// Make sure we don't have any timeout error
		if err := sub.Unsubscribe(); err != nil {
			stackFatalf(t, "Error on unsubscribe: %v", err)
		}
	}
	// Creating subscription to "foo" should work and we should get the message
	sub(fooSubj)
	// Same for bar
	sub(barSubj)

	// Now verify that each server handled only the channel it should.
	checkChannel := func(s *StanServer, chanOk, chanNotOk string) {
		if s.channels.get(chanNotOk) != nil {
			stackFatalf(t, "Server should not have channel %v", chanNotOk)
		}
		if n, _ := msgStoreState(t, s.channels.get(chanOk).store.Msgs); n != 1 {
			stackFatalf(t, "Channel %q should have 1 message and no error, got %v - %v", chanOk, n)
		}
	}
	checkChannel(s1, fooSubj, barSubj)
	checkChannel(s2, barSubj, fooSubj)

	// Make sure we don't have timeout error on close
	if err := sc.Close(); err != nil {
		t.Fatalf("Error on close: %v", err)
	}
}

func TestPartitionsDuplicatedOnTwoServers(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	// For this test, create a single NATS server to which both servers connect to.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	fooSubj := "foo"
	barSubj := "bar"

	opts1 := GetDefaultOptions()
	opts1.NATSServerURL = "nats://localhost:4222"
	opts1.Partitioning = true
	opts1.StoreLimits.AddPerChannel(fooSubj, &stores.ChannelLimits{})
	opts1.StoreLimits.AddPerChannel(barSubj, &stores.ChannelLimits{})
	s1 := runServerWithOpts(t, opts1, nil)
	defer s1.Shutdown()

	opts2 := GetDefaultOptions()
	opts2.NATSServerURL = "nats://localhost:4222"
	opts2.Partitioning = true
	opts2.StoreLimits.AddPerChannel(barSubj, &stores.ChannelLimits{})
	// Expecting this to fail
	s2, err := RunServerWithOpts(opts2, nil)
	if s2 != nil || err == nil {
		if s2 != nil {
			s2.Shutdown()
		}
		t.Fatal("Expected server to fail because of same channel defined in 2 servers")
	}
}

func TestPartitionsConflictDueToWildcards(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	opts1 := GetDefaultOptions()
	opts1.Partitioning = true
	opts1.StoreLimits.AddPerChannel("foo.*", &stores.ChannelLimits{})
	s1 := runServerWithOpts(t, opts1, nil)
	defer s1.Shutdown()

	opts2 := GetDefaultOptions()
	opts2.NATSServerURL = "nats://localhost:4222"
	opts2.Partitioning = true
	opts2.StoreLimits.AddPerChannel("foo.bar", &stores.ChannelLimits{})
	// Expecting this to fail
	s2, err := RunServerWithOpts(opts2, nil)
	if s2 != nil || err == nil {
		if s2 != nil {
			s2.Shutdown()
		}
		t.Fatal("Expected server to fail because of channel overlap on 2 servers")
	}
	opts2.StoreLimits.PerChannel = nil
	opts2.StoreLimits.AddPerChannel(">", &stores.ChannelLimits{})
	// Expecting this to fail
	s2, err = RunServerWithOpts(opts2, nil)
	if s2 != nil || err == nil {
		if s2 != nil {
			s2.Shutdown()
		}
		t.Fatal("Expected server to fail because of channel overlap on 2 servers")
	}
}

func TestPartitionsSendListAfterRouteEstablished(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	ncOpts1 := natsdTest.DefaultTestOptions
	ncOpts1.Host = "127.0.0.1"
	ncOpts1.Cluster.Host = "127.0.0.1"
	ncOpts1.Cluster.Port = 6222
	ncOpts1.Routes = natsd.RoutesFromStr("nats://127.0.0.1:6223")
	ns1 := natsdTest.RunServer(&ncOpts1)
	defer ns1.Shutdown()

	var s1, s2 *StanServer
	var mu sync.Mutex

	fooFromS1 := 0
	fooFromS2 := 0
	cb := func(s **StanServer, count *int) func(m *nats.Msg) {
		return func(m *nats.Msg) {
			req := &spb.CtrlMsg{}
			req.Unmarshal(m.Data)
			channels, _ := util.DecodeChannels(req.Data)
			for _, c := range channels {
				mu.Lock()
				if c == "foo" && *s != nil && req.ServerID == (*s).serverID {
					(*count)++
				}
				mu.Unlock()
			}
		}
	}
	// Create raw subscriber to check the list of channels sent by s1
	nc1, err := nats.Connect(fmt.Sprintf("nats://%s:%d", ncOpts1.Host, ncOpts1.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()
	if _, err := nc1.Subscribe(partitionsPrefix+"."+clusterName, cb(&s1, &fooFromS1)); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc1.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	opts1 := GetDefaultOptions()
	opts1.NATSServerURL = "nats://127.0.0.1:4222"
	opts1.Partitioning = true
	opts1.AddPerChannel("foo", &stores.ChannelLimits{})
	// Do this under this lock since the list will be received in the callback
	// above before s1 is set.
	mu.Lock()
	s1 = runServerWithOpts(t, opts1, nil)
	defer s1.Shutdown()
	mu.Unlock()

	// Setup the 2nd server but do not explicitly create a route to
	// ns1, so we will wait for ns1 to connect to ns2, which since
	// ns1 has an explicit route to ns2, ns1 will try to connect
	// to ns2 every 2 seconds. If we are "lucky" (for this test),
	// it means that s2 will first start without being connected
	// to s1 and should not know that there is a duplicate channel.
	// Once the route is established, this should trigger a resend
	// of the list and then the two servers should fail.
	ncOpts2 := natsdTest.DefaultTestOptions
	ncOpts2.Host = "127.0.0.1"
	ncOpts2.Port = 4223
	ncOpts2.Cluster.Host = "127.0.0.1"
	ncOpts2.Cluster.Port = 6223
	ns2 := natsdTest.RunServer(&ncOpts2)
	defer ns2.Shutdown()

	// Create raw subscriber to check the list of channels sent by s2
	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", ncOpts2.Host, ncOpts2.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()
	if _, err := nc2.Subscribe(partitionsPrefix+"."+clusterName, cb(&s2, &fooFromS2)); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc2.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	opts2 := GetDefaultOptions()
	opts2.NATSServerURL = "nats://127.0.0.1:4223"
	opts2.MaxChannels = 1000
	opts2.Partitioning = true
	opts2.AddPerChannel("foo", &stores.ChannelLimits{})
	mu.Lock()
	s2, err = RunServerWithOpts(opts2, nil)
	mu.Unlock()
	if err != nil {
		// The purpose of this test was to verify that protocols are resent
		// after the route is established. If, due to timing, the route
		// happens to be established before s2 starts, s2 would correctly
		// fail to start. We don't want to fail the test for that.
		if strings.Contains(err.Error(), "foo") {
			return
		}
		t.Fatalf("Unexpected error on startup: %v", err)
	}
	defer s2.Shutdown()

	// Wait for ns1 and ns2 to report that the route is established
	timeout := time.Now().Add(4 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		if ns1.NumRoutes() == 1 && ns2.NumRoutes() == 1 {
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		t.Fatal("Route still not established")
	}
	// The two servers should fail but since we prevent panic
	// for the test, servers will still be running.
	// After the route is established, each server should resend
	// their list, so the total of times each server receives
	// the list should be 2.
	var (
		cs1, cs2     int
		s1Err, s2Err error
	)
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		cs1 = fooFromS1
		cs2 = fooFromS2
		mu.Unlock()
		s1Err = s1.LastError()
		s2Err = s2.LastError()
		if cs1 != 2 || cs2 != 2 || s1Err == nil || s2Err == nil {
			continue
		}
		// pre-conditions are ok
		break
	}
	if cs1 != 2 {
		t.Fatalf("Expected to receive foo from S1 only twice, got %v", cs1)
	}
	if cs2 != 2 {
		t.Fatalf("Expected to receive foo from S2 only twice, got %v", cs2)
	}
	if s1Err == nil || s2Err == nil {
		t.Fatal("Both servers should have stopped")
	}
	// One more check...
	if !strings.Contains(s1Err.Error(), "foo") {
		t.Fatalf("Expected error about channel foo already defined, got %v", s1Err)
	}
	if !strings.Contains(s2Err.Error(), "foo") {
		t.Fatalf("Expected error about channel foo already defined, got %v", s2Err)
	}
}

func TestPartitionsWildcards(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	opts := GetDefaultOptions()
	opts.Partitioning = true
	opts.AddPerChannel("foo.*", &stores.ChannelLimits{})
	opts.AddPerChannel("bar.>", &stores.ChannelLimits{})
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	sc, err := stan.Connect(clusterName, clientName, stan.ConnectWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc.Close()
	cb := func(_ *stan.Msg) {}
	// These should succeed
	if _, err := sc.Subscribe("foo.bar", cb); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if _, err := sc.Subscribe("bar.foo", cb); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	if _, err := sc.Subscribe("bar.foo.baz", cb); err != nil {
		t.Fatalf("Unexpected error on subscribe: %v", err)
	}
	// This one should fail
	if _, err := sc.Subscribe("foo.*", cb); err == nil {
		t.Fatal("Expected error on subscribe, got none")
	}
	// This one should timeout
	if _, err := sc.Subscribe("foo.bar.baz", cb); err == nil {
		t.Fatal("Expected error on subscribe, got none")
	}
}

func checkWaitOnRegisterMap(t tLogger, s *StanServer, size int) {
	var start time.Time
	for {
		s.clients.RLock()
		m := s.clients.waitOnRegister
		mlen := len(m)
		s.clients.RUnlock()
		if m != nil && mlen == size {
			return
		}
		if start.IsZero() {
			start = time.Now()
		} else if time.Since(start) > clientCheckTimeout+50*time.Millisecond {
			stackFatalf(t, "map should have been created and of size %d, got %v", size, mlen)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func TestPartitionsRaceOnPub(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	clientCheckTimeout = 150 * time.Millisecond
	defer func() { clientCheckTimeout = defaultClientCheckTimeout }()

	opts := GetDefaultOptions()
	opts.Partitioning = true
	opts.AddPerChannel("foo", &stores.ChannelLimits{})
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	// stan.Connect() call blocks until it receives the response, so it is
	// not possible to publish a message before the server has processed the
	// connection request. However, with partitioning, it is possible that
	// the Connect() call receives an OK from one of the server and immediately
	// publishes a message. That message, although behind the connection request
	// going to another server, may be dispatched before (due to use of different
	// internal subscriptions for connection handling and client publish).
	//
	// To simulate this here, we use a NATS connection and send a PubMsg manually,
	// followed by the regular stan.Connect(). Then we wait for the response on
	// the PubMsg and we should not get any error in PubAck.

	// Create a direct NATS connection
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unable to connect: %v", err)
	}
	defer nc.Close()

	pubSubj := fmt.Sprintf("%s.foo", s.info.Publish)
	pubReq := &pb.PubMsg{ClientID: clientName, Subject: "foo", Data: []byte("hello")}
	pubNuid := nuid.New()

	pubSub, err := nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		t.Fatalf("Error creating sub on pub response: %v", err)
	}

	// Repeat the test, because even with bug, it would be possible
	// that the connection request is still processed first, which
	// would make the test pass.
	for i := 0; i < 5; i++ {
		func() {
			pubReq.Guid = pubNuid.Next()
			pubBytes, _ := pubReq.Marshal()

			// First case is to make sure that we get the failure if
			// no connection is processed.
			resp, err := nc.Request(pubSubj, pubBytes, clientCheckTimeout+50*time.Millisecond)
			if err != nil {
				t.Fatalf("Error on request: %v", err)
			}
			pubResp := &pb.PubAck{}
			pubResp.Unmarshal(resp.Data)
			if pubResp.Error != ErrInvalidPubReq.Error() {
				t.Fatalf("Expected error %q, got %q", ErrInvalidPubReq, pubResp.Error)
			}
			// Ensure that the notification map has been created, but is empty.
			checkWaitOnRegisterMap(t, s, 0)

			// Now resend a message, but this time don't wait for the response here,
			// instead connect, which should cause the PubMsg to be processed correctly.
			if err := nc.PublishRequest(pubSubj, pubSub.Subject, pubBytes); err != nil {
				t.Fatalf("Error sending PubMsg: %v", err)
			}
			checkWaitOnRegisterMap(t, s, 1)
			sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			defer sc.Close()

			// Now we should get the OK for the PubMsg.
			resp, err = pubSub.NextMsg(clientCheckTimeout + 100*time.Millisecond)
			if err != nil {
				t.Fatalf("Error waiting for pub response: %v", err)
			}
			pubResp = &pb.PubAck{}
			pubResp.Unmarshal(resp.Data)
			if pubResp.Error != "" {
				t.Fatalf("Connection %d - Error on publish: %v", (i + 1), pubResp.Error)
			}
			checkWaitOnRegisterMap(t, s, 0)
		}()
	}
}

func TestPartitionsRaceOnSub(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	clientCheckTimeout = 150 * time.Millisecond
	defer func() { clientCheckTimeout = defaultClientCheckTimeout }()

	opts := GetDefaultOptions()
	opts.Partitioning = true
	opts.AddPerChannel("foo", &stores.ChannelLimits{})
	s := runServerWithOpts(t, opts, nil)
	defer s.Shutdown()

	// See description of the issue in TestPartitionsRaceOnPub.
	// This is the same except that we are dealing with subscription requests
	// here.

	// Create a direct NATS connection
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Unable to connect: %v", err)
	}
	defer nc.Close()

	subSubj := s.info.Subscribe
	subReq := &pb.SubscriptionRequest{ClientID: clientName, Subject: "foo", AckWaitInSecs: 30, MaxInFlight: 1}

	subSub, err := nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		t.Fatalf("Error creating sub on sub response: %v", err)
	}

	// Repeat the test, because even with bug, it would be possible
	// that the connection request is still processed first, which
	// would make the test pass.
	for i := 0; i < 5; i++ {
		func() {
			subReq.Inbox = nats.NewInbox()
			subBytes, _ := subReq.Marshal()

			// First case is to make sure that we get the failure if
			// no connection is processed.
			resp, err := nc.Request(subSubj, subBytes, clientCheckTimeout+50*time.Millisecond)
			if err != nil {
				t.Fatalf("Error on request: %v", err)
			}
			subResp := &pb.SubscriptionResponse{}
			subResp.Unmarshal(resp.Data)
			if subResp.Error != ErrInvalidSubReq.Error() {
				t.Fatalf("Expected error %q, got %q", ErrInvalidSubReq, subResp.Error)
			}
			// Ensure that the notification map has been created, but is empty.
			checkWaitOnRegisterMap(t, s, 0)

			// Now resend the subscription, but this time don't wait for the response here,
			// instead connect, which should cause the SubscriptionRequest to be processed correctly.
			if err := nc.PublishRequest(subSubj, subSub.Subject, subBytes); err != nil {
				t.Fatalf("Error sending PubMsg: %v", err)
			}
			checkWaitOnRegisterMap(t, s, 1)
			sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			defer sc.Close()

			// Now we should get the OK for the PubMsg.
			resp, err = subSub.NextMsg(clientCheckTimeout + 100*time.Millisecond)
			if err != nil {
				t.Fatalf("Error waiting for pub response: %v", err)
			}
			subResp = &pb.SubscriptionResponse{}
			subResp.Unmarshal(resp.Data)
			if subResp.Error != "" {
				t.Fatalf("Connection %d - Error on subscribe: %v", (i + 1), subResp.Error)
			}
			checkWaitOnRegisterMap(t, s, 0)
		}()
	}
}

func TestPartitionsAndFT(t *testing.T) {
	cleanupFTDatastore(t)
	defer cleanupFTDatastore(t)

	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	// For this test, both server will connect to same NATS Server
	ncOpts := natsdTest.DefaultTestOptions
	ns := natsdTest.RunServer(&ncOpts)
	defer ns.Shutdown()

	opts := getTestFTDefaultOptions()
	opts.Partitioning = true
	opts.AddPerChannel("foo", &stores.ChannelLimits{})
	opts.NATSServerURL = "nats://localhost:4222"

	ft1 := runServerWithOpts(t, opts, nil)
	defer ft1.Shutdown()

	// The standby should be able to start
	ft2 := runServerWithOpts(t, opts, nil)
	defer ft2.Shutdown()

	ft1.Shutdown()

	// Wait for ft2 to activate
	checkState(t, ft2, FTActive)

	sc := NewDefaultConnection(t)
	defer sc.Close()

	if err := sc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
}

func TestPartitionsClientPings(t *testing.T) {
	setPartitionsVarsForTest()
	defer resetDefaultPartitionsVars()

	clientCheckTimeout = 150 * time.Millisecond
	defer func() { clientCheckTimeout = defaultClientCheckTimeout }()

	// For this test, create a single NATS server to which both servers connect to.
	ns := natsdTest.RunDefaultServer()
	defer ns.Shutdown()

	fooSubj := "foo"
	barSubj := "bar"

	opts1 := GetDefaultOptions()
	opts1.NATSServerURL = "nats://localhost:4222"
	opts1.Partitioning = true
	opts1.StoreLimits.AddPerChannel(fooSubj, &stores.ChannelLimits{})
	s1 := runServerWithOpts(t, opts1, nil)
	defer s1.Shutdown()

	opts2 := GetDefaultOptions()
	opts2.NATSServerURL = "nats://localhost:4222"
	opts2.Partitioning = true
	opts2.StoreLimits.AddPerChannel(barSubj, &stores.ChannelLimits{})
	s2 := runServerWithOpts(t, opts2, nil)
	defer s2.Shutdown()

	testClientPings(t, s1)
}
