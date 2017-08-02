// Copyright 2016-2017 Apcera Inc. All rights reserved.
package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
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
