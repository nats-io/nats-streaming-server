// Copyright 2017-2019 The NATS Authors
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
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
	"github.com/nats-io/nats.go"
)

// Constants related to partitioning
const (
	// Prefix of subject to send list of channels in this server's partition
	partitionsPrefix = "_STAN.part"
	// Default timeout for a server to wait for replies to its request
	partitionsDefaultRequestTimeout = time.Second
	// This is the value that is stored in the sublist for a given subject
	channelInterest = 1
	// Default wait before checking for channels when notified
	// that the NATS cluster topology has changed. This gives a chance
	// for the new server joining the cluster to send its subscriptions
	// list.
	partitionsDefaultWaitOnTopologyChange = 500 * time.Millisecond
)

// So that we can override in tests
var (
	partitionsRequestTimeout = partitionsDefaultRequestTimeout
	partitionsNoPanic        = false
	partitionsWaitOnChange   = partitionsDefaultWaitOnTopologyChange
)

type partitions struct {
	sync.Mutex
	s               *StanServer
	channels        []string
	sl              *util.Sublist
	nc              *nats.Conn
	sendListSubject string
	processChanSub  *nats.Subscription
	inboxSub        *nats.Subscription
	isShutdown      bool
}

// Initialize the channels partitions objects and issue the first
// request to check if other servers in the cluster incorrectly have
// any of the channel that this server is supposed to handle.
func (s *StanServer) initPartitions() error {
	// The option says that the server should only use the pre-defined channels,
	// but none was specified. Don't see the point in continuing...
	if len(s.opts.StoreLimits.PerChannel) == 0 {
		return ErrNoChannel
	}
	nc, err := s.createNatsClientConn("pc")
	if err != nil {
		return err
	}
	p := &partitions{
		s:  s,
		nc: nc,
	}
	// Now that the connection is created, we need to set s.partitioning to cp
	// so that server shutdown can properly close this connection.
	s.partitions = p
	p.createChannelsMapAndSublist(s.opts.StoreLimits.PerChannel)
	p.sendListSubject = partitionsPrefix + "." + s.opts.ID
	// Use the partitions' own connection for channels list requests
	p.processChanSub, err = p.nc.Subscribe(p.sendListSubject, p.processChannelsListRequests)
	if err != nil {
		return fmt.Errorf("unable to subscribe: %v", err)
	}
	p.processChanSub.SetPendingLimits(-1, -1)
	p.inboxSub, err = p.nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		return fmt.Errorf("unable to subscribe: %v", err)
	}
	p.Lock()
	// Set this before the first attempt so we don't miss any notification
	// of a change in topology. Since we hold the lock, and even if there
	// was a notification happening now, the callback will execute only
	// after we are done with the initial check.
	nc.SetDiscoveredServersHandler(p.topologyChanged)
	// Now send our list and check if any server is complaining
	// about having one channel in common.
	if err := p.checkChannelsUniqueInCluster(); err != nil {
		p.Unlock()
		return err
	}
	p.Unlock()
	return nil
}

// Creates the channels map based on the store's PerChannel map that was given.
func (p *partitions) createChannelsMapAndSublist(storeChannels map[string]*stores.ChannelLimits) {
	p.channels = make([]string, 0, len(storeChannels))
	p.sl = util.NewSublist()
	for c := range storeChannels {
		p.channels = append(p.channels, c)
		// When creating the store, we have already checked that channel names
		// were valid. So this call cannot fail.
		p.sl.Insert(c, channelInterest)
	}
}

// Topology changed. Sends the list of channels.
func (p *partitions) topologyChanged(_ *nats.Conn) {
	p.Lock()
	defer p.Unlock()
	if p.isShutdown {
		return
	}
	// Let's wait before checking (sending the list and waiting for a reply)
	// so that the new NATS Server has a chance to send its local
	// subscriptions to the rest of the cluster. That will reduce the risk
	// of missing the reply from the new server.
	time.Sleep(partitionsWaitOnChange)
	if err := p.checkChannelsUniqueInCluster(); err != nil {
		// If server is started from command line, the Fatalf
		// call will cause the process to exit. If the server
		// is run programmatically and no logger has been set
		// we need to exit with the panic.
		p.s.log.Fatalf("Partitioning error: %v", err)
		// For tests
		if partitionsNoPanic {
			p.s.setLastError(err)
			return
		}
		panic(err)
	}
}

// Create the internal subscriptions on the list of channels.
func (p *partitions) initSubscriptions() error {
	// NOTE: Use the server's nc connection here, not the partitions' one.
	for _, channelName := range p.channels {
		pubSubject := fmt.Sprintf("%s.%s", p.s.info.Publish, channelName)
		if _, err := p.s.nc.Subscribe(pubSubject, p.s.processClientPublish); err != nil {
			return fmt.Errorf("could not subscribe to publish subject %q, %v", channelName, err)
		}
	}
	return nil
}

// Sends a request to the rest of the cluster and wait a bit for
// responses (we don't know if or how many servers there may be).
// No server lock used since this is called inside RunServerWithOpts().
func (p *partitions) checkChannelsUniqueInCluster() error {
	// We use the subscription on an inbox to get the replies.
	// Send our list
	if err := util.SendChannelsList(p.channels, p.sendListSubject, p.inboxSub.Subject, p.nc, p.s.serverID); err != nil {
		return fmt.Errorf("unable to send channels list: %v", err)
	}
	// Since we don't know how many servers are out there, keep
	// calling NextMsg until we get a timeout
	for {
		reply, err := p.inboxSub.NextMsg(partitionsRequestTimeout)
		if err == nats.ErrTimeout {
			return nil
		}
		if err != nil {
			return fmt.Errorf("unable to get partitioning reply: %v", err)
		}
		resp := spb.CtrlMsg{}
		if err := resp.Unmarshal(reply.Data); err != nil {
			return fmt.Errorf("unable to decode partitioning response: %v", err)
		}
		if len(resp.Data) > 0 {
			return fmt.Errorf("channel %q causes conflict with channels on server %q",
				string(resp.Data), resp.ServerID)
		}
	}
}

// Decode the incoming partitioning protocol message.
// It can be an HB, in which case, if it is from a new server
// we send our list to the cluster, or it can be a request
// from another server. If so, we reply to the given inbox
// with either an empty Data field or the name of the first
// channel we have in common.
func (p *partitions) processChannelsListRequests(m *nats.Msg) {
	// Message cannot be empty, we are supposed to receive
	// a spb.CtrlMsg_Partitioning protocol. We should also
	// have a repy subject
	if len(m.Data) == 0 || m.Reply == "" {
		return
	}
	req := spb.CtrlMsg{}
	if err := req.Unmarshal(m.Data); err != nil {
		p.s.log.Errorf("Error processing partitioning request: %v", err)
		return
	}
	// If this is our own request, ignore
	if req.ServerID == p.s.serverID {
		return
	}
	channels, err := util.DecodeChannels(req.Data)
	if err != nil {
		p.s.log.Errorf("Error processing partitioning request: %v", err)
		return
	}
	// Check that we don't have any of these channels defined.
	// If we do, send a reply with simply the name of the offending
	// channel in reply.Data
	reply := spb.CtrlMsg{
		ServerID: p.s.serverID,
		MsgType:  spb.CtrlMsg_Partitioning,
	}
	gotError := false
	sl := util.NewSublist()
	for _, c := range channels {
		if r := p.sl.Match(c); len(r) > 0 {
			reply.Data = []byte(c)
			gotError = true
			break
		}
		sl.Insert(c, channelInterest)
	}
	if !gotError {
		// Go over our channels and check with the other server sublist
		for _, c := range p.channels {
			if r := sl.Match(c); len(r) > 0 {
				reply.Data = []byte(c)
				break
			}
		}
	}
	replyBytes, _ := reply.Marshal()
	// If there is no duplicate, reply.Data will be empty, which means
	// that there was no conflict.
	if err := p.nc.Publish(m.Reply, replyBytes); err != nil {
		p.s.log.Errorf("Error sending reply to partitioning request: %v", err)
	}
}

// Notifies all go-routines used by partitioning code that the
// server is shuting down and closes the internal NATS connection.
func (p *partitions) shutdown() {
	p.Lock()
	defer p.Unlock()
	if p.isShutdown {
		return
	}
	p.isShutdown = true
	if p.nc != nil {
		p.nc.Close()
	}
}
