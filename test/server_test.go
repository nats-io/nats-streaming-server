// Copyright 2016 Apcera Inc. All rights reserved.

package test

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats"
	"github.com/nats-io/stan"

	"fmt"
	natsd "github.com/nats-io/gnatsd/test"
)

const (
	defaultClusterName = "test-cluster"
)

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

func TestNoNats(t *testing.T) {
	if _, err := stan.Connect("someNonExistantServerID", "myTestClient"); err != nats.ErrNoServers {
		t.Fatalf("Expected NATS: No Servers err, got %v\n", err)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

func TestUnreachable(t *testing.T) {
	s := natsd.RunDefaultServer()
	defer s.Shutdown()

	// Non-Existant or Unreachable
	connectTime := 25 * time.Millisecond
	start := time.Now()
	if _, err := stan.Connect("someNonExistantServerID", "myTestClient", stan.ConnectWait(connectTime)); err != stan.ErrConnectReqTimeout {
		t.Fatalf("Expected Unreachable err, got %v\n", err)
	}
	if delta := time.Since(start); delta < connectTime {
		t.Fatalf("Expected to wait at least %v, but only waited %v\n", connectTime, delta)
	}
}

const (
	clusterName = "my_test_cluster"
	clientName  = "me"
)

// So that we can pass tests and benchmarks...
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func NewDefaultConnection(t tLogger) stan.Conn {
	sc, err := stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	return sc
}

func TestClientCrashAndReconnect(t *testing.T) {
	s := RunServer(clusterName)
	defer s.Shutdown()

	opts := natsd.DefaultTestOptions
	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))

	sc, err := stan.Connect(clusterName, clientName, stan.NatsConn(nc))
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}
	sc.Subscribe("foo", func(m *stan.Msg) {})

	// should get a client ID not found
	sc, err = stan.Connect(clusterName, clientName)
	if err == nil {
		t.Fatalf("Expected to be unable to connect, received no error.")
	}

	// kill the NATS conn
	nc.Close()

	// should get a client ID not found
	sc, err = stan.Connect(clusterName, clientName)
	if err == nil {
		t.Fatalf("Expected to be unable to connect, received no error.")
	}

	time.Sleep(time.Second * 4)

	// should connect
	sc, err = stan.Connect(clusterName, clientName)
	if err != nil {
		t.Fatalf("Expected to connect correctly, got err %v", err)
	}

}
