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
	"math/rand"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/nats-streaming-server/util"
	"github.com/nats-io/nats.go"
)

// FT constants
const (
	ftDefaultHBInterval       = time.Second
	ftDefaultHBMissedInterval = 1250 * time.Millisecond
)

var (
	// Some go-routine will panic, which we can't recover in test.
	// So the tests will set this to true to be able to test the
	// correct behavior.
	ftNoPanic bool
	// For tests purposes, we may want to pause for the first
	// attempt at getting the store lock so that test can
	// switch store with a mocked one.
	ftPauseBeforeFirstAttempt bool
	ftPauseCh                 = make(chan struct{})
	// This can be changed for tests purposes.
	ftHBInterval       = ftDefaultHBInterval
	ftHBMissedInterval = ftDefaultHBMissedInterval
)

func ftReleasePause() {
	ftPauseCh <- struct{}{}
}

// ftStart will return only when this server has become active
// and was able to get the store's exclusive lock.
// This is running in a separate go-routine so if server state
// changes, take care of using the server's lock.
func (s *StanServer) ftStart() (retErr error) {
	s.log.Noticef("Starting in standby mode")
	// For tests purposes
	if ftPauseBeforeFirstAttempt {
		<-ftPauseCh
	}
	print, _ := util.NewBackoffTimeCheck(time.Second, 2, time.Minute)
	for {
		select {
		case <-s.ftQuit:
			// we are done
			return nil
		case <-s.ftHBCh:
			// go back to the beginning of the for loop
			continue
		case <-time.After(s.ftHBMissedInterval):
			// try to lock the store
		}
		locked, err := s.ftGetStoreLock()
		if err != nil {
			// Log the error, but go back and wait for the next interval and
			// try again. It is possible that the error resolves (for instance
			// the connection to the database is restored - for SQL stores).
			s.log.Errorf("ft: error attempting to get the store lock: %v", err)
			continue
		} else if locked {
			break
		}
		// Here, we did not get the lock, print and go back to standby.
		// Use some backoff for the printing to not fill up the log
		if print.Ok() {
			s.log.Noticef("ft: unable to get store lock at this time, going back to standby")
		}
	}
	// Capture the time this server activated. It will be used in case several
	// servers claim to be active. Not bulletproof since there could be clock
	// differences, etc... but when more than one server has acquired the store
	// lock it means we are already in trouble, so just trying to minimize the
	// possible store corruption...
	activationTime := time.Now()
	s.log.Noticef("Server is active")
	s.startGoRoutine(func() {
		s.ftSendHBLoop(activationTime)
	})
	// Start the recovery process, etc..
	return s.start(FTActive)
}

// ftGetStoreLock returns true if the server was able to get the
// exclusive store lock, false othewise, or if there was a fatal error doing so.
func (s *StanServer) ftGetStoreLock() (bool, error) {
	// Normally, the store would be set early and is immutable, but some
	// FT tests do set a mock store after the server is created, so use
	// locking here to avoid race reports.
	s.mu.Lock()
	store := s.store
	s.mu.Unlock()
	if ok, err := store.GetExclusiveLock(); !ok || err != nil {
		// We got an error not related to locking (could be not supported,
		// permissions error, file not reachable, etc..)
		if err != nil {
			return false, fmt.Errorf("ft: fatal error getting the store lock: %v", err)
		}
		// If ok is false, it means that we did not get the lock.
		return false, nil
	}
	return true, nil
}

// ftSendHBLoop is used by an active server to send HB to the FT subject.
// Standby servers receiving those HBs do not attempt to lock the store.
// When they miss HBs, they will.
func (s *StanServer) ftSendHBLoop(activationTime time.Time) {
	// Release the wait group on exit
	defer s.wg.Done()

	timeAsBytes, _ := activationTime.MarshalBinary()
	ftHB := &spb.CtrlMsg{
		MsgType:  spb.CtrlMsg_FTHeartbeat,
		ServerID: s.serverID,
		Data:     timeAsBytes,
	}
	ftHBBytes, _ := ftHB.Marshal()
	print, _ := util.NewBackoffTimeCheck(time.Second, 2, time.Minute)
	for {
		if err := s.ftnc.Publish(s.ftSubject, ftHBBytes); err != nil {
			if print.Ok() {
				s.log.Errorf("Unable to send FT heartbeat: %v", err)
			}
		}
	startSelect:
		select {
		case m := <-s.ftHBCh:
			hb := spb.CtrlMsg{}
			if err := hb.Unmarshal(m.Data); err != nil {
				goto startSelect
			}
			// Ignore our own message
			if hb.MsgType != spb.CtrlMsg_FTHeartbeat || hb.ServerID == s.serverID {
				goto startSelect
			}
			// Another server claims to be active
			peerActivationTime := time.Time{}
			if err := peerActivationTime.UnmarshalBinary(hb.Data); err != nil {
				s.log.Errorf("Error decoding activation time: %v", err)
			} else {
				// Step down if the peer's activation time is earlier than ours.
				err := fmt.Errorf("ft: serverID %q claims to be active", hb.ServerID)
				if peerActivationTime.Before(activationTime) {
					err = fmt.Errorf("%s, aborting", err)
					if ftNoPanic {
						s.setLastError(err)
						return
					}
					panic(err)
				} else {
					s.log.Errorf(err.Error())
				}
			}
		case <-time.After(s.ftHBInterval):
		// We'll send the ping at the top of the for loop
		case <-s.ftQuit:
			return
		}
	}
}

// ftSetup checks that all required FT parameters have been specified and
// create the channel required for shutdown.
// Note that FTGroupName has to be set before server invokes this function,
// so this parameter is not checked here.
func (s *StanServer) ftSetup() error {
	// Check that store type is ok. So far only support for FileStore
	if s.opts.StoreType != stores.TypeFile && s.opts.StoreType != stores.TypeSQL {
		return fmt.Errorf("ft: only %v or %v stores supported in FT mode", stores.TypeFile, stores.TypeSQL)
	}
	// So far, those are not exposed to users, just used in tests.
	// Still make sure that the missed HB interval is > than the HB
	// interval.
	if ftHBMissedInterval < time.Duration(float64(ftHBInterval)*1.1) {
		return fmt.Errorf("ft: the missed heartbeat interval needs to be"+
			" at least 10%% of the heartbeat interval (hb=%v missed hb=%v",
			ftHBInterval, ftHBMissedInterval)
	}
	// Set the HB and MissedHB intervals, using a bit of randomness
	rand.Seed(time.Now().UnixNano())
	s.ftHBInterval = ftGetRandomInterval(ftHBInterval)
	s.ftHBMissedInterval = ftGetRandomInterval(ftHBMissedInterval)
	// Subscribe to FT subject
	s.ftSubject = fmt.Sprintf("%s.%s.%s", ftHBPrefix, s.opts.ID, s.opts.FTGroupName)
	s.ftHBCh = make(chan *nats.Msg)
	sub, err := s.ftnc.Subscribe(s.ftSubject, func(m *nats.Msg) {
		// Dropping incoming FT HBs is not crucial, we will then check for
		// store lock.
		select {
		case s.ftHBCh <- m:
		default:
		}
	})
	if err != nil {
		return fmt.Errorf("ft: unable to subscribe on ft subject: %v", err)
	}
	// We don't want to cause possible slow consumer error
	sub.SetPendingLimits(-1, -1)
	// Create channel to notify FT go routine to quit.
	s.ftQuit = make(chan struct{}, 1)
	// Set the state as standby initially
	s.state = FTStandby
	return nil
}

// ftGetRandomInterval returns a random interval with at most +/- 10%
// of the given interval.
func ftGetRandomInterval(interval time.Duration) time.Duration {
	tenPercent := int(float64(interval) * 0.10)
	random := time.Duration(rand.Intn(tenPercent))
	if rand.Intn(2) == 1 {
		return interval + random
	}
	return interval - random
}
