// Copyright 2016-2018 The NATS Authors
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
	"sync"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nuid"
)

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

func TestClientCrashAndReconnect(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	total := 10
	clientNames := []string{}
	for i := 0; i < total; i++ {
		clientNames = append(clientNames, fmt.Sprintf("client%d", i))
	}

	natsConns := []*nats.Conn{}
	scConnsMu := sync.Mutex{}
	scConns := []stan.Conn{}

	defer func() {
		for _, nc := range natsConns {
			if nc != nil {
				nc.Close()
			}
		}
		for _, sc := range scConns {
			sc.Close()
		}
	}()

	for _, cn := range clientNames {
		nc, err := nats.Connect(nats.DefaultURL)
		if err != nil {
			t.Fatalf("Unexpected error on connect: %v", err)
		}
		natsConns = append(natsConns, nc)

		sc, err := stan.Connect(clusterName, cn, stan.NatsConn(nc))
		if err != nil {
			t.Fatalf("Expected to connect correctly, got err %v", err)
		}
		scConns = append(scConns, sc)
	}

	// Get the connected clients' inboxes
	clients := s.clients.getClients()
	if cc := len(clients); cc != total {
		t.Fatalf("There should be %d clients, got %v", total, cc)
	}
	hbInboxes := []string{}
	for _, cn := range clientNames {
		cli := clients[cn]
		if cli == nil {
			t.Fatalf("Expected client %q to exist, did not", cn)
		}
		hbInboxes = append(hbInboxes, cli.info.HbInbox)
	}

	// should get a duplicate clientID error
	for _, cn := range clientNames {
		if sc, err := stan.Connect(clusterName, cn); err == nil {
			sc.Close()
			t.Fatal("Expected to be unable to connect")
		}
	}

	// kill the NATS conn
	for i, nc := range natsConns {
		natsConns[i] = nil
		nc.Close()
	}

	// Since the original client won't respond to a ping, we should
	// be able to connect, and it should not take too long.
	start := time.Now()

	// should succeed
	wg := sync.WaitGroup{}
	wg.Add(total)
	errCh := make(chan error, total)
	for _, cn := range clientNames {
		go func(cn string) {
			defer wg.Done()

			sc, err := stan.Connect(clusterName, cn)
			if err != nil {
				errCh <- fmt.Errorf("Unexpected error on connect: %v", err)
				return
			}
			scConnsMu.Lock()
			scConns = append(scConns, sc)
			scConnsMu.Unlock()
		}(cn)
	}
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatalf(e.Error())
	default:
	}

	duration := time.Since(start)
	if duration > 5*time.Second {
		t.Fatalf("Took too long to be able to connect: %v", duration)
	}

	clients = s.clients.getClients()
	if cc := len(clients); cc != total {
		t.Fatalf("There should be %v client, got %v", total, cc)
	}
	for i := 0; i < total; i++ {
		cli := clients[clientNames[i]]
		if cli == nil {
			t.Fatalf("Expected client %q to exist, did not", clientNames[i])
		}
		// Check we have registered the "new" client which should have
		// a different HbInbox
		if hbInboxes[i] == cli.info.HbInbox {
			t.Fatalf("Looks like restarted client was not properly registered")
		}
	}
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

	c := s.clients.lookup(clientName)
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

func TestClientsWithDupCID(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	// Not too small to avoid flapping tests.
	s.dupCIDTimeout = 1 * time.Second
	total := 5

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
}

func TestPersistentStoreCheckClientHealthAfterRestart(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	// Create 2 clients
	sc1, err := stan.Connect(clusterName, "c1", stan.ConnectWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sc1.Close()
	sc2, err := stan.Connect(clusterName, "c2", stan.ConnectWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
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
	// Check their hbTimer is set
	clients := s.clients.getClients()
	for cID, c := range clients {
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

func TestClientPings(t *testing.T) {
	s := runServer(t, clusterName)
	defer s.Shutdown()

	testClientPings(t, s)
}

func testClientPings(t *testing.T, s *StanServer) {
	s.mu.RLock()
	s.opts.ClientHBTimeout = 15 * time.Millisecond
	discoverySubj := s.info.Discovery
	pubSubj := s.info.Publish
	s.mu.RUnlock()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	hbInbox := nats.NewInbox()
	creq := &pb.ConnectRequest{
		ClientID:       "me",
		HeartbeatInbox: hbInbox,
		ConnID:         []byte(nuid.Next()),
		Protocol:       protocolOne,
		PingInterval:   1,
		PingMaxOut:     3,
	}
	firstConnID := creq.ConnID
	creqBytes, _ := creq.Marshal()
	crespMsg, err := nc.Request(discoverySubj, creqBytes, 2*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	cresp := &pb.ConnectResponse{}
	if err := cresp.Unmarshal(crespMsg.Data); err != nil {
		t.Fatalf("Error on connect response: %v", err)
	}
	if cresp.Error != "" {
		t.Fatalf("Error on connect: %v", cresp.Error)
	}
	if cresp.Protocol != protocolOne || cresp.PingRequests == "" || cresp.PingInterval != 1 || cresp.PingMaxOut != 3 {
		t.Fatalf("Unexpected response: %#v", cresp)
	}

	// Artificially add a sub and set timer to fire soon
	s.clients.addSub("me", &subState{})
	client := s.clients.lookup("me")
	client.Lock()
	client.hbt.Reset(15 * time.Millisecond)
	client.Unlock()

	// Wait a bit and check client failedHB
	time.Sleep(100 * time.Millisecond)
	client.RLock()
	cliHasFailedHB := client.fhb > 0
	sub := client.subs[0]
	sub.RLock()
	subHasFailedHB := sub.hasFailedHB
	sub.RUnlock()
	client.RUnlock()
	if !cliHasFailedHB || !subHasFailedHB {
		t.Fatalf("Expected client and sub to have failed heartbeats")
	}

	// Set a subscription to reply to server HBs only once
	nc.Subscribe(hbInbox, func(m *nats.Msg) {
		nc.Publish(m.Reply, nil)
		m.Sub.Unsubscribe()
	})

	// Send ping, expect success
	ping := pb.Ping{ConnID: creq.ConnID}
	pingBytes, _ := ping.Marshal()
	resp, err := nc.Request(cresp.PingRequests, pingBytes, time.Second)
	if err != nil {
		t.Fatalf("Error on ping: %v", err)
	}
	pingResp := &pb.PingResponse{}
	if err := pingResp.Unmarshal(resp.Data); err != nil {
		t.Fatalf("Error decoding ping response: %v", err)
	}
	if pingResp.Error != "" {
		t.Fatalf("Got ping response error: %v", pingResp.Error)
	}

	// This should have triggered the server to send HB, to
	// which we replied, which then should clear the client
	// and sub failed HB count.
	time.Sleep(100 * time.Millisecond)
	client.RLock()
	cliHasFailedHB = client.fhb > 0
	sub = client.subs[0]
	sub.RLock()
	subHasFailedHB = sub.hasFailedHB
	sub.RUnlock()
	client.RUnlock()
	if cliHasFailedHB || subHasFailedHB {
		t.Fatalf("Expected client and sub to not have failed heartbeats, got %v and %v", cliHasFailedHB, subHasFailedHB)
	}

	// Send with incorrect connID, expect error
	ping.ConnID = []byte("wrongconnID")
	pingBytes, _ = ping.Marshal()
	resp, err = nc.Request(cresp.PingRequests, pingBytes, time.Second)
	if err != nil {
		t.Fatalf("Error on ping: %v", err)
	}
	pingResp = &pb.PingResponse{}
	if err := pingResp.Unmarshal(resp.Data); err != nil {
		t.Fatalf("Error decoding ping response: %v", err)
	}
	if pingResp.Error == "" {
		t.Fatalf("Expected ping response error, got none")
	}

	// Register new client with same client ID. Since this is a fake
	// client that is not going to respond to server HB to detect if
	// it is still present, the old client will be closed, new client
	// will be accepted.
	creq = &pb.ConnectRequest{
		ClientID:       "me",
		HeartbeatInbox: nats.NewInbox(),
		ConnID:         []byte(nuid.Next()),
		Protocol:       protocolOne,
		PingInterval:   1,
		PingMaxOut:     3,
	}
	creqBytes, _ = creq.Marshal()
	crespMsg, err = nc.Request(discoverySubj, creqBytes, 2*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	cresp = &pb.ConnectResponse{}
	if err := cresp.Unmarshal(crespMsg.Data); err != nil {
		t.Fatalf("Error on connect response: %v", err)
	}
	if cresp.Error != "" {
		t.Fatalf("Error on connect: %v", cresp.Error)
	}

	// Using the "old" client, send a PING with original connID.
	// Since the old client has been replaced, we should get an error
	ping = pb.Ping{ConnID: firstConnID}
	pingBytes, _ = ping.Marshal()

	// In partitioning mode, one of the server may not have
	// yet replaced the old client and may send an OK back first.
	// So in case of no error, repeat once.
	for i := 0; i < 2; i++ {
		resp, err = nc.Request(cresp.PingRequests, pingBytes, time.Second)
		if err != nil {
			t.Fatalf("Error on ping: %v", err)
		}
		pingResp = &pb.PingResponse{}
		if err := pingResp.Unmarshal(resp.Data); err != nil {
			t.Fatalf("Error decoding ping response: %v", err)
		}
		if pingResp.Error != "" {
			// Expected error, ok.
			break
		}
		if i == 0 {
			time.Sleep(50 * time.Millisecond)
		} else {
			t.Fatalf("Expected ping response error, got none")
		}
	}

	// Also expect a publish to fail since the old client is no longer valid.
	msg := &pb.PubMsg{
		ClientID: "me",
		Subject:  "foo",
		ConnID:   firstConnID,
		Guid:     nuid.Next(),
		Data:     []byte("hello"),
	}
	msgBytes, _ := msg.Marshal()
	pubAckSubj := nats.NewInbox()
	ch := make(chan bool, 1)
	if _, err := nc.Subscribe(pubAckSubj, func(m *nats.Msg) {
		pubAck := &pb.PubAck{}
		pubAck.Unmarshal(m.Data)
		if pubAck.Error != "" {
			ch <- true
		}
	}); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.PublishRequest(pubSubj+".foo", pubAckSubj, msgBytes); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := Wait(ch); err != nil {
		t.Fatal("Did not our error back")
	}
}

func TestPersistentStoreRecoverClientInfo(t *testing.T) {
	cleanupDatastore(t)
	defer cleanupDatastore(t)

	opts := getTestDefaultOptsForPersistentStore()
	s := runServerWithOpts(t, opts, nil)
	defer shutdownRestartedServerOnTestExit(&s)

	s.mu.RLock()
	discoverySubj := s.info.Discovery
	s.mu.RUnlock()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	creq := &pb.ConnectRequest{
		ClientID:       "me",
		HeartbeatInbox: nats.NewInbox(),
		ConnID:         []byte(nuid.Next()),
		Protocol:       protocolOne,
		PingInterval:   1,
		PingMaxOut:     3,
	}
	creqBytes, _ := creq.Marshal()
	crespMsg, err := nc.Request(discoverySubj, creqBytes, 2*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	cresp := &pb.ConnectResponse{}
	if err := cresp.Unmarshal(crespMsg.Data); err != nil {
		t.Fatalf("Error on connect response: %v", err)
	}
	if cresp.Error != "" {
		t.Fatalf("Error on connect: %v", cresp.Error)
	}
	if cresp.Protocol != protocolOne || cresp.PingRequests == "" || cresp.PingInterval != 1 || cresp.PingMaxOut != 3 {
		t.Fatalf("Unexpected response: %#v", cresp)
	}

	// Shutdown and restart server
	s.Shutdown()
	s = runServerWithOpts(t, opts, nil)

	// Check client's info was properly persisted
	c := s.clients.lookup("me")
	if c == nil {
		t.Fatalf("Client was not recovered")
	}
	c.RLock()
	proto := c.info.Protocol
	cid := c.info.ConnID
	pi := c.info.PingInterval
	pmo := c.info.PingMaxOut
	c.RUnlock()

	if proto != 1 {
		t.Fatalf("Recovered proto should be 1, got %v", proto)
	}
	if string(cid) != string(creq.ConnID) {
		t.Fatalf("Recovered ConnID should be %s, got %s", creq.ConnID, cid)
	}
	if pi != 1 {
		t.Fatalf("Recovered ping interval should be 1, got %v", time.Duration(pi))
	}
	if pmo != 3 {
		t.Fatalf("Recovered ping max out should be 3, got %v", pmo)
	}
	nc.Close()
}
