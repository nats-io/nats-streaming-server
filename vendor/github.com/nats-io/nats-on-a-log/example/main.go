// Copyright 2017 Apcera Inc. All rights reserved.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nats-on-a-log"
)

func main() {
	var (
		port         = flag.String("port", "8080", "http port")
		id           = flag.String("id", "", "node id")
		kvPath       = flag.String("kv-path", "", "KV store path")
		logPath      = flag.String("log-path", "", "Raft log path")
		snapshotPath = flag.String("sn-path", "", "Log snapshot path")
		peersPath    = flag.String("peers-path", "", "Peers JSON path")
		peers        = flag.String("peers", "", "List of peers")
	)
	flag.Parse()

	if *kvPath == "" || *logPath == "" || *snapshotPath == "" || *peersPath == "" {
		panic("path not provided")
	}

	if *peers == "" {
		panic("peers not provided")
	}

	p := strings.Split(*peers, ",")

	var (
		config   = raft.DefaultConfig()
		fsm, err = NewFSM(*kvPath)
		natsOpts = nats.DefaultOptions
	)
	if err != nil {
		panic(err)
	}

	store, err := raftboltdb.NewBoltStore(*logPath)
	if err != nil {
		panic(err)
	}

	cacheStore, err := raft.NewLogCache(512, store)
	if err != nil {
		panic(err)
	}

	snapshots, err := raft.NewFileSnapshotStore(*snapshotPath, 2, os.Stdout)
	if err != nil {
		panic(err)
	}

	conn, err := natsOpts.Connect()
	if err != nil {
		panic(err)
	}

	trans, err := natslog.NewNATSTransport(*id, conn, 2*time.Second, os.Stdout)
	if err != nil {
		panic(err)
	}

	peersStore := raft.NewJSONPeers(*peersPath, trans)
	if err := peersStore.SetPeers(p); err != nil {
		panic(err)
	}

	node, err := raft.NewRaft(config, fsm, cacheStore, store, snapshots, peersStore, trans)
	if err != nil {
		panic(err)
	}

	go func() {
		for isLeader := range node.LeaderCh() {
			if isLeader {
				fmt.Println("*** LEADERSHIP ACQUIRED ***")
				bar := node.Barrier(0)
				if err := bar.Error(); err != nil {
					fmt.Printf("Failed applying barrier when becoming leader: %s\n", err)
				}
			} else {
				fmt.Println("*** LEADERSHIP LOST ***")
			}
		}
	}()

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Must use POST request", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var buf bytes.Buffer
		_, err := buf.ReadFrom(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var op operation
		if err := json.Unmarshal(buf.Bytes(), &op); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if string(op.Key) == "" {
			http.Error(w, "Key not set", http.StatusBadRequest)
			return
		}
		future := node.Apply(buf.Bytes(), 5*time.Second)
		if err := future.Error(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte(strconv.FormatInt(int64(future.Index()), 10)))
	})

	http.HandleFunc("/get/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/get/")
		val, err := fsm.Get([]byte(key))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if val == nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		w.Write(val)
	})

	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		panic(err)
	}
}
