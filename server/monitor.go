// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"time"

	gnatsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats-streaming-server/stores"
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
	Now           time.Time `json:"now"`
	Start         time.Time `json:"start_time"`
	Uptime        string    `json:"uptime"`
	Clients       int       `json:"clients"`
	Subscriptions int       `json:"subscriptions"`
	Channels      int       `json:"channels"`
	TotalMsgs     int       `json:"total_msgs"`
	TotalBytes    uint64    `json:"total_bytes"`
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
	Inbox        string `json:"inbox"`
	AckInbox     string `json:"ack_inbox"`
	DurableName  string `json:"durable_name,omitempty"`
	QueueName    string `json:"queue_name,omitempty"`
	IsDurable    bool   `json:"is_durable"`
	MaxInflight  int    `json:"max_inflight"`
	AckWait      int    `json:"ack_wait"`
	LastSent     uint64 `json:"last_sent"`
	PendingCount int    `json:"pending_count"`
	IsStalled    bool   `json:"is_stalled"`
}

func (s *StanServer) startMonitoring(nOpts *gnatsd.Options) error {
	var hh http.Handler
	// If we are connecting to remote NATS Server, we start our own
	// HTTP(s) server.
	if s.opts.NATSServerURL != "" {
		s.natsServer = gnatsd.New(nOpts)
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
	numChannels := s.store.GetChannelsCount()
	count, bytes, _ := s.store.MsgsState(stores.AllChannels)
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	s.monMu.RLock()
	numSubs := s.numSubs
	s.monMu.RUnlock()
	now := time.Now()
	serverz := &Serverz{
		ClusterID:     s.info.ClusterID,
		ServerID:      s.serverID,
		Version:       VERSION,
		GoVersion:     runtime.Version(),
		State:         state.String(),
		Now:           now,
		Start:         s.startTime,
		Uptime:        myUptime(now.Sub(s.startTime)),
		Clients:       s.store.GetClientsCount(),
		Channels:      numChannels,
		Subscriptions: numSubs,
		TotalMsgs:     count,
		TotalBytes:    bytes,
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
	count, bytes, _ := s.store.MsgsState(stores.AllChannels)
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
		var clientz *Clientz
		client := s.store.GetClient(singleClient)
		if client != nil {
			clientz = getMonitorClient(client, subsOption)
		}
		if clientz == nil {
			http.Error(w, fmt.Sprintf("Client %s not found", singleClient), http.StatusNotFound)
			return
		}
		s.sendResponse(w, r, clientz)
	} else {
		offset, limit := getOffsetAndLimit(r)
		clients := s.store.GetClients()
		totalClients := len(clients)
		carr := make([]*Clientz, 0, totalClients)
		for _, c := range clients {
			cz := &Clientz{ID: c.ID}
			carr = append(carr, cz)
		}
		sort.Sort(byClientID(carr))

		minoff, maxoff := getMinMaxOffset(offset, limit, totalClients)
		carr = carr[minoff:maxoff]

		for _, c := range carr {
			cli := clients[c.ID]
			c.HBInbox = cli.HbInbox
			if subsOption == 1 {
				srvCli := cli.UserData.(*client)
				c.Subscriptions = getMonitorClientSubs(srvCli, true)
			}
		}
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

func getMonitorClient(c *stores.Client, subsOption int) *Clientz {
	cli := c.UserData.(*client)
	cli.RLock()
	defer cli.RUnlock()
	if cli.unregistered {
		return nil
	}
	cz := &Clientz{
		HBInbox: c.HbInbox,
		ID:      c.ID,
	}
	if subsOption == 1 {
		cz.Subscriptions = getMonitorClientSubs(cli, false)
	}
	return cz
}

func getMonitorClientSubs(client *client, needsLock bool) map[string][]*Subscriptionz {
	if needsLock {
		client.RLock()
		defer client.RUnlock()
		if client.unregistered {
			return nil
		}
	}
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
	for _, sub := range ss.durables {
		subsz = append(subsz, createSubscriptionz(sub))
	}
	for _, qsub := range ss.qsubs {
		qsub.RLock()
		for _, sub := range qsub.subs {
			subsz = append(subsz, createSubscriptionz(sub))
		}
		qsub.RUnlock()
	}
	return subsz
}

func createSubscriptionz(sub *subState) *Subscriptionz {
	sub.RLock()
	subz := &Subscriptionz{
		Inbox:        sub.Inbox,
		AckInbox:     sub.AckInbox,
		DurableName:  sub.DurableName,
		QueueName:    sub.QGroup,
		IsDurable:    sub.IsDurable,
		MaxInflight:  int(sub.MaxInFlight),
		AckWait:      int(sub.AckWaitInSecs),
		LastSent:     sub.LastSent,
		PendingCount: len(sub.acksPending),
		IsStalled:    sub.stalled,
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
		channels := s.store.GetChannels()
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
				updateChannelz(cz, cs, subsOption)
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
	cs := s.store.LookupChannel(name)
	if cs == nil {
		http.Error(w, fmt.Sprintf("Channel %s not found", name), http.StatusNotFound)
		return
	}
	channelz := &Channelz{Name: name}
	updateChannelz(channelz, cs, subsOption)
	s.sendResponse(w, r, channelz)
}

func updateChannelz(cz *Channelz, cs *stores.ChannelStore, subsOption int) {
	msgs, bytes, _ := cs.Msgs.State()
	fseq, lseq := cs.Msgs.FirstAndLastSequence()
	cz.Msgs = msgs
	cz.Bytes = bytes
	cz.FirstSeq = fseq
	cz.LastSeq = lseq
	if subsOption == 1 {
		ss := cs.UserData.(*subStore)
		cz.Subscriptions = getMonitorChannelSubs(ss)
	}
}

func (s *StanServer) sendResponse(w http.ResponseWriter, r *http.Request, content interface{}) {
	b, err := json.MarshalIndent(content, "", "  ")
	if err != nil {
		s.log.Errorf("Error marshaling response to %q request: %v", r.URL, err)
	}
	gnatsd.ResponseHandler(w, r, b)
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
