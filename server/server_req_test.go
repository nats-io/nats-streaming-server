// Copyright 2016-2021 The NATS Authors
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
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
)

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
	// in StanServer.processSubscriptionRequest(). Any change there
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
	// For these tests, we need to disable testAckWaitIsInMillisecond
	testAckWaitIsInMillisecond = false
	req.AckWaitInSecs = 0
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidAckWait); err != nil {
		t.Fatalf("%v", err)
	}
	req.AckWaitInSecs = -1
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidAckWait); err != nil {
		t.Fatalf("%v", err)
	}
	testAckWaitIsInMillisecond = true

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
	sc := NewDefaultConnection(t)
	defer sc.Close()
	req.Subject = "foo"
	req.QGroup = "queue"
	req.DurableName = "dur:name"
	if err := sendInvalidSubRequest(s, nc, req, ErrInvalidDurName); err != nil {
		t.Fatalf("%v", err)
	}
	sc.Close()

	// Reset those
	req.QGroup = ""
	req.DurableName = ""

	// Now we should have an error that says that we have an unknown client ID.
	if err := sendInvalidSubRequest(s, nc, req, ErrUnknownClient); err != nil {
		t.Fatalf("%v", err)
	}

	// There should be no client created
	checkClients(t, s, 0)

	// But channel "foo" should have been created though
	if s.channels.count() == 0 {
		t.Fatal("Expected channel foo to have been created")
	}

	// Create a durable
	sc = NewDefaultConnection(t)
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
	if err := sendInvalidSubRequest(s, nc, req, ErrUnknownClient); err != nil {
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
