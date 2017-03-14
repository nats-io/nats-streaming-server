// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"time"
)

const (
	VERSION = "0.4"

	// Election timeout MIN and MAX per RAFT spec suggestion.
	MIN_ELECTION_TIMEOUT = 500 * time.Millisecond
	MAX_ELECTION_TIMEOUT = 2 * MIN_ELECTION_TIMEOUT

	// Heartbeat tick for LEADERS.
	// Should be << MIN_ELECTION_TIMEOUT per RAFT spec.
	HEARTBEAT_INTERVAL = 100 * time.Millisecond

	NO_LEADER = ""
	NO_VOTE   = ""
)
