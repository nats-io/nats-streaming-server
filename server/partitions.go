// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"sync"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
)

// Constants related to partitioning
const (
	// Prefix of subject to send list of channels in this server's partition
	partitionsPrefix = "_STAN.part"
	// Default timeout for a server to wait for replies to its request
	partitionsDefaultRequestTimeout = time.Second
	// This is the value that is stored in the sublist for a given subject
	channelInterest = 1
	// Messages channel size
	partitionsMsgChanSize = 65536
	// Number of bytes used to encode a channel name
	partitionsEncodedChannelLen = 2
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
	ctrlMsgSubject  string // send to this subject when processing close/unsub requests
	processChanSub  *nats.Subscription
	inboxSub        *nats.Subscription
	msgsCh          chan *nats.Msg
	isShutdown      bool
	quitCh          chan struct{}
}

// Creates the subscription for partitioning communication, sends
// the initial request (in case there are other servers listening) and
// starts the go routines handling HBs and cleanup of servers map.
func (s *StanServer) initPartitions(sOpts *Options, nOpts *natsd.Options, storeChannels map[string]*stores.ChannelLimits) error {
	// The option says that the server should only use the pre-defined channels,
	// but none was specified. Don't see the point in continuing...
	if len(storeChannels) == 0 {
		return ErrNoChannel
	}
	nc, err := s.createNatsClientConn("pc", sOpts, nOpts)
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
	p.createChannelsMapAndSublist(storeChannels)
	p.sendListSubject = partitionsPrefix + "." + sOpts.ID
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
	p.msgsCh = make(chan *nats.Msg, partitionsMsgChanSize)
	p.quitCh = make(chan struct{}, 1)
	s.wg.Add(1)
	go p.postClientPublishIncomingMsgs()
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

// We use a channel subscription in order to minimize the number
// of go routines for all the explicit pub subscriptions.
// This go routine simply pulls a message from the channel and
// invokes processClientPublish(). We may have slow consumer issues,
// if so, will have to figure out another way.
func (p *partitions) postClientPublishIncomingMsgs() {
	defer p.s.wg.Done()
	for {
		select {
		case <-p.quitCh:
			return
		case m := <-p.msgsCh:
			p.s.processClientPublish(m)
		}
	}
}

// Create the internal subscriptions on the list of channels.
func (p *partitions) initSubscriptions() error {
	// NOTE: Use the server's nc connection here, not the partitions' one.
	for _, channelName := range p.channels {
		pubSubject := fmt.Sprintf("%s.%s", p.s.info.Publish, channelName)
		if _, err := p.s.nc.ChanSubscribe(pubSubject, p.msgsCh); err != nil {
			return fmt.Errorf("could not subscribe to publish subject %q, %v", channelName, err)
		}
	}
	// Add a dedicated subject for when we try to schedule a control
	// message to be processed after all messages from a given client
	// have been processed.
	p.ctrlMsgSubject = fmt.Sprintf("%s.%s", p.s.info.Publish, nats.NewInbox())
	if _, err := p.s.nc.ChanSubscribe(p.ctrlMsgSubject, p.msgsCh); err != nil {
		return fmt.Errorf("could not subscribe to subject %q, %v", p.ctrlMsgSubject, err)
	}
	return nil
}

// Sends a request to the rest of the cluster and wait a bit for
// responses (we don't know if or how many servers there may be).
// No server lock used since this is called inside RunServerWithOpts().
func (p *partitions) checkChannelsUniqueInCluster() error {
	// We use the subscription on an inbox to get the replies.
	// Send our list
	if err := p.sendChannelsList(p.inboxSub.Subject); err != nil {
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

// Sends the list of channels to a known subject, possibly splitting the list
// in several requests if it cannot fit in a single message.
func (p *partitions) sendChannelsList(replyInbox string) error {
	// Since the NATS message payload is limited, we need to repeat
	// requests if all channels can't fit in a request.
	maxPayload := int(p.nc.MaxPayload())
	// Reuse this request object to send the (possibly many) protocol message(s).
	header := &spb.CtrlMsg{
		ServerID: p.s.serverID,
		MsgType:  spb.CtrlMsg_Partitioning,
	}
	// The Data field (a byte array) will require 1+len(array)+(encoded size of array).
	// To be conservative, let's just use a 8 bytes integer
	headerSize := header.Size() + 1 + 8
	var (
		bytes []byte // Reused buffer in which the request is to marshal info
		n     int    // Size of the serialized request in the above buffer
		count int    // Number of channels added to the request
	)
	for start := 0; start != len(p.channels); start += count {
		bytes, n, count = p.encodeRequest(header, bytes, headerSize, maxPayload, start)
		if count == 0 {
			return fmt.Errorf("message payload too small to send partitioning channels list")
		}
		if err := p.nc.PublishRequest(p.sendListSubject, replyInbox, bytes[:n]); err != nil {
			return err
		}
	}
	return p.nc.Flush()
}

// Adds as much channels as possible (based on the NATS max message payload) and
// returns a serialized request. The buffer `reqBytes` is passed (and returned) so
// that it can be reused if more than one request is needed. This call will
// expand the size as needed. The number of bytes used in this buffer is returned
// along with the number of encoded channels.
func (p *partitions) encodeRequest(request *spb.CtrlMsg, reqBytes []byte, headerSize, maxPayload, start int) ([]byte, int, int) {
	// Each string will be encoded in the form:
	// - length (2 bytes)
	// - string as a byte array.
	var _encodedSize = [partitionsEncodedChannelLen]byte{}
	encodedSize := _encodedSize[:]
	// We are going to encode the channels in this buffer
	chanBuf := make([]byte, 0, maxPayload)
	var (
		count         int          // Number of encoded channels
		estimatedSize = headerSize // This is not an overestimation of the total size
		numBytes      int          // This is what is returned by MarshalTo
	)
	for i := start; i < len(p.channels); i++ {
		c := []byte(p.channels[i])
		cl := len(c)
		needed := partitionsEncodedChannelLen + cl
		// Check if adding this channel to current buffer makes us go over
		if estimatedSize+needed > maxPayload {
			// Special case if we cannot even encode 1 channel
			if count == 0 {
				return reqBytes, 0, 0
			}
			break
		}
		// Encoding the channel here. First the size, then the channel name as byte array.
		util.ByteOrder.PutUint16(encodedSize, uint16(cl))
		chanBuf = append(chanBuf, encodedSize...)
		chanBuf = append(chanBuf, c...)
		count++
		estimatedSize += needed
	}
	if count > 0 {
		request.Data = chanBuf
		reqBytes = util.EnsureBufBigEnough(reqBytes, estimatedSize)
		numBytes, _ = request.MarshalTo(reqBytes)
		if numBytes > maxPayload {
			panic(fmt.Errorf("partitioning: request size is %v (max payload is %v)", numBytes, maxPayload))
		}
	}
	return reqBytes, numBytes, count
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
	channels, err := decodeChannels(req.Data)
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

// decodes from the given by array the list of channel names and return
// them as an array of strings.
func decodeChannels(data []byte) ([]string, error) {
	channels := []string{}
	pos := 0
	for pos < len(data) {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("partitioning: unable to decode size, pos=%v len=%v", pos, len(data))
		}
		cl := int(util.ByteOrder.Uint16(data[pos:]))
		pos += partitionsEncodedChannelLen
		end := pos + cl
		if end > len(data) {
			return nil, fmt.Errorf("partitioning: unable to decode channel, pos=%v len=%v max=%v (string=%v)",
				pos, cl, len(data), string(data[pos:]))
		}
		c := string(data[pos:end])
		channels = append(channels, c)
		pos = end
	}
	return channels, nil
}

// Notifies all go-routines used by partitioning code that the
// server is shuting down and closes the internal NATS connection.
func (p *partitions) shutdown() {
	p.Lock()
	if p.isShutdown {
		p.Unlock()
		return
	}
	p.isShutdown = true
	p.Unlock()
	if p.quitCh != nil {
		p.quitCh <- struct{}{}
	}
	if p.nc != nil {
		p.nc.Close()
	}
}
