// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"fmt"
)

type State int8

// Allowable states for a Graft node.
const (
	FOLLOWER State = iota
	LEADER
	CANDIDATE
	CLOSED
)

// Convenience for printing, etc.
func (s State) String() string {
	switch s {
	case FOLLOWER:
		return "Follower"
	case LEADER:
		return "Leader"
	case CANDIDATE:
		return "Candidate"
	case CLOSED:
		return "Closed"
	default:
		return fmt.Sprintf("Unknown[%d]", s)
	}
}
