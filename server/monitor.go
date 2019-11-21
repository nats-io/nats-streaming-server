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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	natsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/prometheus/procfs"
)

// Routes for the monitoring pages
const (
	RootPath     = "/streaming"
	ServerPath   = RootPath + "/serverz"
	StorePath    = RootPath + "/storez"
	ClientsPath  = RootPath + "/clientsz"
	ChannelsPath = RootPath + "/channelsz"

	defaultMonitorListLimit = 1024
)

// Serverz describes the NATS Streaming Server
type Serverz struct {
	ClusterID     string    `json:"cluster_id"`
	ServerID      string    `json:"server_id"`
	Version       string    `json:"version"`
	GoVersion     string    `json:"go"`
	State         string    `json:"state"`
	Role          string    `json:"role,omitempty"`
	Now           time.Time `json:"now"`
	Start         time.Time `json:"start_time"`
	Uptime        string    `json:"uptime"`
	Clients       int       `json:"clients"`
	Subscriptions int       `json:"subscriptions"`
	Channels      int       `json:"channels"`
	TotalMsgs     int       `json:"total_msgs"`
	TotalBytes    uint64    `json:"total_bytes"`
	InMsgs        int64     `json:"in_msgs"`
	InBytes       int64     `json:"in_bytes"`
	OutMsgs       int64     `json:"out_msgs"`
	OutBytes      int64     `json:"out_bytes"`
	OpenFDs       int       `json:"open_fds,omitempty"`
	MaxFDs        int       `json:"max_fds,omitempty"`
}

// Storez describes the NATS Streaming Store
type Storez struct {
	ClusterID  string             `json:"cluster_id"`
	ServerID   string             `json:"server_id"`
	Now        time.Time          `json:"now"`
	Type       string             `json:"type"`
	Limits     stores.StoreLimits `json:"limits"`
	TotalMsgs  int                `json:"total_msgs"`
	TotalBytes uint64             `json:"total_bytes"`
}

// Clientsz lists the client connections
type Clientsz struct {
	ClusterID string     `json:"cluster_id"`
	ServerID  string     `json:"server_id"`
	Now       time.Time  `json:"now"`
	Offset    int        `json:"offset"`
	Limit     int        `json:"limit"`
	Count     int        `json:"count"`
	Total     int        `json:"total"`
	Clients   []*Clientz `json:"clients"`
}

// Clientz describes a NATS Streaming Client connection
type Clientz struct {
	ID            string                      `json:"id"`
	HBInbox       string                      `json:"hb_inbox"`
	Subscriptions map[string][]*Subscriptionz `json:"subscriptions,omitempty"`
}

// Channelsz lists the name of all NATS Streaming Channelsz
type Channelsz struct {
	ClusterID string      `json:"cluster_id"`
	ServerID  string      `json:"server_id"`
	Now       time.Time   `json:"now"`
	Offset    int         `json:"offset"`
	Limit     int         `json:"limit"`
	Count     int         `json:"count"`
	Total     int         `json:"total"`
	Names     []string    `json:"names,omitempty"`
	Channels  []*Channelz `json:"channels,omitempty"`
}

// Channelz describes a NATS Streaming Channel
type Channelz struct {
	Name          string           `json:"name"`
	Msgs          int              `json:"msgs"`
	Bytes         uint64           `json:"bytes"`
	FirstSeq      uint64           `json:"first_seq"`
	LastSeq       uint64           `json:"last_seq"`
	Subscriptions []*Subscriptionz `json:"subscriptions,omitempty"`
}

// Subscriptionz describes a NATS Streaming Subscription
type Subscriptionz struct {
	ClientID     string `json:"client_id"`
	Inbox        string `json:"inbox"`
	AckInbox     string `json:"ack_inbox"`
	DurableName  string `json:"durable_name,omitempty"`
	QueueName    string `json:"queue_name,omitempty"`
	IsDurable    bool   `json:"is_durable"`
	IsOffline    bool   `json:"is_offline"`
	MaxInflight  int    `json:"max_inflight"`
	AckWait      int    `json:"ack_wait"`
	LastSent     uint64 `json:"last_sent"`
	PendingCount int    `json:"pending_count"`
	IsStalled    bool   `json:"is_stalled"`
}

func (s *StanServer) startMonitoring(nOpts *natsd.Options) error {
	var hh http.Handler
	// If we are connecting to remote NATS Server, we start our own
	// HTTP(s) server.
	if s.opts.NATSServerURL != "" {
		ns, err := natsd.NewServer(nOpts)
		if err != nil {
			return err
		}
		s.natsServer = ns
		if err := s.natsServer.StartMonitoring(); err != nil {
			return err
		}
		hh = s.natsServer.HTTPHandler()
	} else {
		hh = s.natsServer.HTTPHandler()
	}
	if hh == nil {
		return errors.New("unable to start monitoring server")
	}

	mux := hh.(*http.ServeMux)
	mux.HandleFunc(RootPath, s.handleRootz)
	mux.HandleFunc(ServerPath, s.handleServerz)
	mux.HandleFunc(StorePath, s.handleStorez)
	mux.HandleFunc(ClientsPath, s.handleClientsz)
	mux.HandleFunc(ChannelsPath, s.handleChannelsz)

	return nil
}

func (s *StanServer) handleRootz(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, `<html lang="en">
   <head>
    <link rel="shortcut icon" href="http://nats.io/img/favicon.ico">
    <style type="text/css">
      body { font-family: "Century Gothic", CenturyGothic, AppleGothic, sans-serif; font-size: 22; }
      a { margin-left: 32px; }
    </style>
  </head>
  <body>
    <img src="http://nats.io/img/logo.png" alt="NATS Streaming">
    <br/>
	<a href=%s>server</a><br/>
	<a href=%s>store</a><br/>
	<a href=%s>clients</a><br/>
	<a href=%s>channels</a><br/>
    <br/>
    <a href=http://nats.io/documentation/server/gnatsd-monitoring/>help</a>
  </body>
</html>`, ServerPath, StorePath, ClientsPath, ChannelsPath)
}

func (s *StanServer) handleServerz(w http.ResponseWriter, r *http.Request) {
	numChannels := s.channels.count()
	count, bytes, err := s.channels.msgsState("")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting information about channels state: %v", err), http.StatusInternalServerError)
		return
	}
	var role string
	s.mu.RLock()
	state := s.state
	if s.raft != nil {
		role = s.raft.State().String()
	}
	s.mu.RUnlock()

	numSubs := s.numSubs()
	now := time.Now()

	fds := 0
	maxFDs := 0
	if p, err := procfs.Self(); err == nil {
		fds, err = p.FileDescriptorsLen()
		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting file descriptors len: %v", err), http.StatusInternalServerError)
			return
		}
		limits, err := p.Limits()
		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting process limits: %v", err), http.StatusInternalServerError)
			return
		}
		maxFDs = int(limits.OpenFiles)
	}

	serverz := &Serverz{
		ClusterID:     s.info.ClusterID,
		ServerID:      s.serverID,
		Version:       VERSION,
		GoVersion:     runtime.Version(),
		State:         state.String(),
		Role:          role,
		Now:           now,
		Start:         s.startTime,
		Uptime:        myUptime(now.Sub(s.startTime)),
		Clients:       s.clients.count(),
		Channels:      numChannels,
		Subscriptions: numSubs,
		TotalMsgs:     count,
		TotalBytes:    bytes,
		InMsgs:        atomic.LoadInt64(&s.stats.inMsgs),
		InBytes:       atomic.LoadInt64(&s.stats.inBytes),
		OutMsgs:       atomic.LoadInt64(&s.stats.outMsgs),
		OutBytes:      atomic.LoadInt64(&s.stats.outBytes),
		OpenFDs:       fds,
		MaxFDs:        maxFDs,
	}
	s.sendResponse(w, r, serverz)
}

func myUptime(d time.Duration) string {
	// Just use total seconds for uptime, and display days / years
	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}
	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}
	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}
	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}
	return fmt.Sprintf("%ds", tsecs)
}

func (s *StanServer) handleStorez(w http.ResponseWriter, r *http.Request) {
	count, bytes, err := s.channels.msgsState("")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting information about channels state: %v", err), http.StatusInternalServerError)
		return
	}
	storez := &Storez{
		ClusterID:  s.info.ClusterID,
		ServerID:   s.serverID,
		Now:        time.Now(),
		Type:       s.store.Name(),
		Limits:     s.opts.StoreLimits,
		TotalMsgs:  count,
		TotalBytes: bytes,
	}
	s.sendResponse(w, r, storez)
}

type byClientID []*Clientz

func (c byClientID) Len() int           { return len(c) }
func (c byClientID) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c byClientID) Less(i, j int) bool { return c[i].ID < c[j].ID }

func (s *StanServer) handleClientsz(w http.ResponseWriter, r *http.Request) {
	singleClient := r.URL.Query().Get("client")
	subsOption, _ := strconv.Atoi(r.URL.Query().Get("subs"))
	if singleClient != "" {
		clientz := getMonitorClient(s, singleClient, subsOption)
		if clientz == nil {
			http.Error(w, fmt.Sprintf("Client %s not found", singleClient), http.StatusNotFound)
			return
		}
		s.sendResponse(w, r, clientz)
	} else {
		offset, limit := getOffsetAndLimit(r)
		clients := s.clients.getClients()
		totalClients := len(clients)
		carr := make([]*Clientz, 0, totalClients)
		for cID := range clients {
			cz := &Clientz{ID: cID}
			carr = append(carr, cz)
		}
		sort.Sort(byClientID(carr))

		minoff, maxoff := getMinMaxOffset(offset, limit, totalClients)
		carr = carr[minoff:maxoff]

		// Since clients may be unregistered between the time we get the client IDs
		// and the time we build carr array, lets count the number of elements
		// actually intserted.
		carrSize := 0
		for _, c := range carr {
			client := s.clients.lookup(c.ID)
			if client != nil {
				client.RLock()
				c.HBInbox = client.info.HbInbox
				if subsOption == 1 {
					c.Subscriptions = getMonitorClientSubs(client)
				}
				client.RUnlock()
				carrSize++
			}
		}
		carr = carr[0:carrSize]
		clientsz := &Clientsz{
			ClusterID: s.info.ClusterID,
			ServerID:  s.serverID,
			Now:       time.Now(),
			Offset:    offset,
			Limit:     limit,
			Total:     totalClients,
			Count:     len(carr),
			Clients:   carr,
		}
		s.sendResponse(w, r, clientsz)
	}
}

func getMonitorClient(s *StanServer, clientID string, subsOption int) *Clientz {
	cli := s.clients.lookup(clientID)
	if cli == nil {
		return nil
	}
	cli.RLock()
	defer cli.RUnlock()
	cz := &Clientz{
		HBInbox: cli.info.HbInbox,
		ID:      cli.info.ID,
	}
	if subsOption == 1 {
		cz.Subscriptions = getMonitorClientSubs(cli)
	}
	return cz
}

func getMonitorClientSubs(client *client) map[string][]*Subscriptionz {
	subs := client.subs
	var subsz map[string][]*Subscriptionz
	for _, sub := range subs {
		if subsz == nil {
			subsz = make(map[string][]*Subscriptionz)
		}
		array := subsz[sub.subject]
		newArray := append(array, createSubscriptionz(sub))
		if &newArray != &array {
			subsz[sub.subject] = newArray
		}
	}
	return subsz
}

func getMonitorChannelSubs(ss *subStore) []*Subscriptionz {
	ss.RLock()
	defer ss.RUnlock()
	subsz := make([]*Subscriptionz, 0)
	for _, sub := range ss.psubs {
		subsz = append(subsz, createSubscriptionz(sub))
	}
	// Get only offline durables (the online also appear in ss.psubs)
	for _, sub := range ss.durables {
		if sub.ClientID == "" {
			subsz = append(subsz, createSubscriptionz(sub))
		}
	}
	for _, qsub := range ss.qsubs {
		qsub.RLock()
		for _, sub := range qsub.subs {
			subsz = append(subsz, createSubscriptionz(sub))
		}
		// If this is a durable queue subscription and all members
		// are offline, qsub.shadow will be not nil. Report this one.
		if qsub.shadow != nil {
			subsz = append(subsz, createSubscriptionz(qsub.shadow))
		}
		qsub.RUnlock()
	}
	return subsz
}

func createSubscriptionz(sub *subState) *Subscriptionz {
	sub.RLock()
	subz := &Subscriptionz{
		ClientID:     sub.ClientID,
		Inbox:        sub.Inbox,
		AckInbox:     sub.AckInbox,
		DurableName:  sub.DurableName,
		QueueName:    sub.QGroup,
		IsDurable:    sub.IsDurable,
		IsOffline:    (sub.ClientID == ""),
		MaxInflight:  int(sub.MaxInFlight),
		AckWait:      int(sub.AckWaitInSecs),
		LastSent:     sub.LastSent,
		PendingCount: len(sub.acksPending),
		IsStalled:    sub.stalled,
	}
	// Case of offline durable (queue) subscriptions
	if sub.ClientID == "" {
		subz.ClientID = sub.savedClientID
	}
	sub.RUnlock()
	return subz
}

// When we support only Go 1.8+, replace sort with sort.Slice
type byName []string

func (a byName) Len() int           { return (len(a)) }
func (a byName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byName) Less(i, j int) bool { return a[i] < a[j] }

type byChannelName []*Channelz

func (a byChannelName) Len() int           { return (len(a)) }
func (a byChannelName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byChannelName) Less(i, j int) bool { return a[i].Name < a[j].Name }

func (s *StanServer) handleChannelsz(w http.ResponseWriter, r *http.Request) {
	channelName := r.URL.Query().Get("channel")
	subsOption, _ := strconv.Atoi(r.URL.Query().Get("subs"))
	if channelName != "" {
		s.handleOneChannel(w, r, channelName, subsOption)
	} else {
		offset, limit := getOffsetAndLimit(r)
		channels := s.channels.getAll()
		totalChannels := len(channels)
		minoff, maxoff := getMinMaxOffset(offset, limit, totalChannels)
		channelsz := &Channelsz{
			ClusterID: s.info.ClusterID,
			ServerID:  s.serverID,
			Now:       time.Now(),
			Offset:    offset,
			Limit:     limit,
			Total:     totalChannels,
		}
		if subsOption == 1 {
			carr := make([]*Channelz, 0, totalChannels)
			for cn := range channels {
				cz := &Channelz{Name: cn}
				carr = append(carr, cz)
			}
			sort.Sort(byChannelName(carr))
			carr = carr[minoff:maxoff]
			for _, cz := range carr {
				cs := channels[cz.Name]
				if err := s.updateChannelz(cz, cs, subsOption); err != nil {
					http.Error(w, fmt.Sprintf("Error getting information about channel %q: %v", channelName, err), http.StatusInternalServerError)
					return
				}
			}
			channelsz.Count = len(carr)
			channelsz.Channels = carr
		} else {
			carr := make([]string, 0, totalChannels)
			for cn := range channels {
				carr = append(carr, cn)
			}
			sort.Sort(byName(carr))
			carr = carr[minoff:maxoff]
			channelsz.Count = len(carr)
			channelsz.Names = carr
		}
		s.sendResponse(w, r, channelsz)
	}
}

func (s *StanServer) handleOneChannel(w http.ResponseWriter, r *http.Request, name string, subsOption int) {
	cs := s.channels.get(name)
	if cs == nil {
		http.Error(w, fmt.Sprintf("Channel %s not found", name), http.StatusNotFound)
		return
	}
	channelz := &Channelz{Name: name}
	if err := s.updateChannelz(channelz, cs, subsOption); err != nil {
		http.Error(w, fmt.Sprintf("Error getting information about channel %q: %v", name, err), http.StatusInternalServerError)
		return
	}
	s.sendResponse(w, r, channelz)
}

func (s *StanServer) updateChannelz(cz *Channelz, c *channel, subsOption int) error {
	msgs, bytes, err := c.store.Msgs.State()
	if err != nil {
		return fmt.Errorf("unable to get message state: %v", err)
	}
	fseq, lseq, err := s.getChannelFirstAndlLastSeq(c)
	if err != nil {
		return fmt.Errorf("unable to get first and last sequence: %v", err)
	}
	cz.Msgs = msgs
	cz.Bytes = bytes
	cz.FirstSeq = fseq
	cz.LastSeq = lseq
	if subsOption == 1 {
		cz.Subscriptions = getMonitorChannelSubs(c.ss)
	}
	return nil
}

func (s *StanServer) sendResponse(w http.ResponseWriter, r *http.Request, content interface{}) {
	b, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		s.log.Errorf("Error marshaling response to %q request: %v", r.URL, err)
	}
	natsd.ResponseHandler(w, r, b)
}

func getOffsetAndLimit(r *http.Request) (int, int) {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = defaultMonitorListLimit
	}
	return offset, limit
}

func getMinMaxOffset(offset, limit, total int) (int, int) {
	minoff := offset
	if minoff > total {
		minoff = total
	}
	maxoff := offset + limit
	if maxoff > total {
		maxoff = total
	}
	return minoff, maxoff
}
