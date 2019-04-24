// Copyright 2016-2018 The NATS Authors
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

package stores

import (
	"reflect"
	"testing"

	"github.com/nats-io/nats-streaming-server/spb"
)

func TestCSMaxSubs(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			tests := []struct {
				name  string
				limit int
			}{
				{"foo", 2},
				{"bar", 0},
			}

			for _, tm := range tests {
				maxSubs := tm.limit
				limits := testDefaultStoreLimits
				limits.MaxSubscriptions = maxSubs
				if err := s.SetLimits(&limits); err != nil {
					t.Fatalf("Error setting limits: %v", err)
				}

				total := maxSubs + 1
				if maxSubs == 0 {
					total = 10
				}
				cs := storeCreateChannel(t, s, tm.name)
				sub := &spb.SubState{}
				numSubs := 0
				var err error
				lastSubID := uint64(0)
				for i := 0; i < total; i++ {
					sub.ID = 0
					err = cs.Subs.CreateSub(sub)
					if err != nil {
						break
					}
					lastSubID = sub.ID
					numSubs++
				}
				if maxSubs == 0 && err != nil {
					t.Fatalf("Should not have failed, got %v", err)
				} else if maxSubs > 0 {
					if err == nil || err != ErrTooManySubs {
						t.Fatalf("Error should have been ErrTooManySubs, got %v", err)
					}
					if numSubs != maxSubs {
						t.Fatalf("Wrong number of subs: %v vs %v", numSubs, maxSubs)
					}
					// Remove the last subscription, and do it twice (second
					// time should have no effect).
					for i := 0; i < 2; i++ {
						if err := cs.Subs.DeleteSub(lastSubID); err != nil {
							t.Fatalf("Error on delete: %v", err)
						}
					}
					// Now try to add back 2 subscriptions...
					// First should be fine
					sub.ID = 0
					if err := cs.Subs.CreateSub(sub); err != nil {
						t.Fatalf("Error on create: %v", err)
					}
					// This one should fail:
					sub.ID = 0
					if err := cs.Subs.CreateSub(sub); err == nil || err != ErrTooManySubs {
						t.Fatalf("Error should have been ErrTooManySubs, got %v", err)
					}
				}
			}
		})
	}
}

func TestCSBasicSubStore(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")
			ss := cs.Subs
			sub := &spb.SubState{}
			sub.AckInbox = "AckInbox"

			err := ss.CreateSub(sub)
			if err != nil {
				t.Fatalf("Unexpected error on create sub: %v", err)
			}
			subID := sub.ID
			if sub.ID == 0 {
				t.Fatalf("Expected a positive subID, got: %v", sub.ID)
			}
			if err := ss.AddSeqPending(sub.ID, 1); err != nil {
				t.Fatalf("Unexpected error on AddSeqPending: %v", err)
			}
			// Make sure call is idempotent
			if err := ss.AddSeqPending(sub.ID, 1); err != nil {
				t.Fatalf("Unexpected error on AddSeqPending: %v", err)
			}
			if err := ss.AckSeqPending(sub.ID, 1); err != nil {
				t.Fatalf("Unexpected error on AckSeqPending: %v", err)
			}
			if err := ss.AckSeqPending(sub.ID, 1); err != nil {
				t.Fatalf("Unexpected error on AckSeqPending: %v", err)
			}
			// Update the subscription
			sub.AckInbox = "newAckInbox"
			if err := ss.UpdateSub(sub); err != nil {
				t.Fatalf("Unexpected error on update sub: %v", err)
			}
			subStoreDeleteSub(t, ss, sub.ID)

			// Chekck that there is no error if we add updates for deleted sub.
			if err := ss.AddSeqPending(sub.ID, 2); err != nil {
				t.Fatalf("Unexpected error on AddSeqPending: %v", err)
			}
			// Check that ack update for non existent sub is OK
			if err := ss.AckSeqPending(sub.ID+1, 10); err != nil {
				t.Fatalf("Unexpected error on AddSeqPending: %v", err)
			}

			// Create a new subscription, make sure subID is not reused
			sub.ID = 0
			err = ss.CreateSub(sub)
			if err != nil {
				t.Fatalf("Error on create sub: %v", err)
			}
			if sub.ID <= subID {
				t.Fatalf("Unexpected subID: %v", sub.ID)
			}
			subToDelete := sub.ID
			subID = sub.ID
			// Create another
			sub.ID = 0
			err = ss.CreateSub(sub)
			if err != nil {
				t.Fatalf("Error on create sub: %v", err)
			}
			if sub.ID <= subID {
				t.Fatalf("Unexpected subID: %v", sub.ID)
			}
			subID = sub.ID
			// Remove the sub created before the last
			if err := ss.DeleteSub(subToDelete); err != nil {
				t.Fatalf("Error on delete sub: %v", err)
			}
			// Create a last one and make sure it does not collide with the
			// second sub we created.
			sub.ID = 0
			err = ss.CreateSub(sub)
			if err != nil {
				t.Fatalf("Error on create sub: %v", err)
			}
			if sub.ID <= subID {
				t.Fatalf("Unexpected subID: %v", sub.ID)
			}
		})
	}
}

func TestCSRecoverSubUpdateForDeleteSubOK(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			if !st.recoverable {
				t.SkipNow()
			}
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")
			// Store one sub for which we are going to store updates
			// and then delete
			sub1 := storeSub(t, cs, "foo")
			// This one will stay and should be recovered
			sub2 := storeSub(t, cs, "foo")

			// Add several pending seq for sub1
			storeSubPending(t, cs, "foo", sub1, 1, 2, 3)

			// Delete sub
			storeSubDelete(t, cs, "foo", sub1)

			// Add more updates
			storeSubPending(t, cs, "foo", sub1, 4, 5)
			storeSubAck(t, cs, "foo", sub1, 1)

			// Delete unexisting subs
			storeSubDelete(t, cs, "foo", sub2+1, sub2+2, sub2+3)

			// Close the store
			s.Close()

			// Recovers now, should not have any error
			limits := testDefaultStoreLimits
			limits.MaxSubscriptions = 1
			s, state := testReOpenStore(t, st, &limits)
			defer s.Close()

			cs = getRecoveredChannel(t, state, "foo")
			getRecoveredSubs(t, state, "foo", 1)
			// Make sure the subs count was not messed-up by the fact
			// that the store recovered delete requests for un-recovered
			// subscriptions.
			// Since we have set the limit of subs to 1, and we have
			// recovered one, we should fail creating a new one.
			sub := &spb.SubState{
				ClientID:      "me",
				Inbox:         "inbox",
				AckInbox:      "ackinbox",
				AckWaitInSecs: 10,
			}
			if err := cs.Subs.CreateSub(sub); err == nil || err != ErrTooManySubs {
				t.Fatalf("Should have failed creating a sub, got %v", err)
			}
		})
	}
}

func TestCSNoSubIDCollisionAfterRecovery(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			if !st.recoverable {
				t.SkipNow()
			}
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")
			// Store a subscription.
			sub1 := storeSub(t, cs, "foo")

			// Close the store
			s.Close()

			// Recovers now
			s, state := testReOpenStore(t, st, nil)
			defer s.Close()
			cs = getRecoveredChannel(t, state, "foo")
			getRecoveredSubs(t, state, "foo", 1)
			// Store new subscription
			sub2 := storeSub(t, cs, "foo")

			if sub2 <= sub1 {
				t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", sub1+1, sub2)
			}

			// Delete sub1, and create a new one. sub1 ID should not be reused
			storeSubDelete(t, cs, "foo", sub1)
			sub3 := storeSub(t, cs, "foo")
			if sub3 <= sub2 {
				t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", sub2+1, sub3)
			}

			// Delete the last sub and make sure after recovery we get a higher id
			storeSubDelete(t, cs, "foo", sub3)

			// Close the store
			s.Close()

			// Recovers now
			s, state = testReOpenStore(t, st, nil)
			defer s.Close()
			cs = getRecoveredChannel(t, state, "foo")
			// sub2 should be recovered
			getRecoveredSubs(t, state, "foo", 1)

			// Store new subscription
			sub4 := storeSub(t, cs, "foo")

			if sub4 <= sub3 {
				t.Fatalf("Invalid subscription id after recovery, should be at leat %v, got %v", sub3+1, sub4)
			}
		})
	}
}

func TestCSSubLastSentCorrectOnRecovery(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			if !st.recoverable {
				t.SkipNow()
			}
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")
			// Store a subscription.
			subID := storeSub(t, cs, "foo")

			// A message
			msg := []byte("hello")

			// Store msg seq 1 and 2
			m1 := storeMsg(t, cs, "foo", 1, msg)
			m2 := storeMsg(t, cs, "foo", 2, msg)

			// Store m1 and m2 for this subscription, then m1 again.
			storeSubPending(t, cs, "foo", subID, m1.Sequence, m2.Sequence, m1.Sequence)

			// Restart server
			s.Close()
			s, state := testReOpenStore(t, st, nil)
			defer s.Close()
			cs = getRecoveredChannel(t, state, "foo")
			subs := getRecoveredSubs(t, state, "foo", 1)
			sub := subs[0]
			// Check that sub's last seq is m2.Sequence
			if sub.Sub.LastSent != m2.Sequence {
				t.Fatalf("Expected LastSent to be %v, got %v", m2.Sequence, sub.Sub.LastSent)
			}

			// Ack m1 and m2
			storeSubAck(t, cs, "foo", subID, m1.Sequence, m2.Sequence)

			// Restart server
			s.Close()
			s, state = testReOpenStore(t, st, nil)
			defer s.Close()
			getRecoveredChannel(t, state, "foo")
			subs = getRecoveredSubs(t, state, "foo", 1)
			sub = subs[0]
			// Check that sub's last seq is m2.Sequence
			if sub.Sub.LastSent != m2.Sequence {
				t.Fatalf("Expected LastSent to be %v, got %v", m2.Sequence, sub.Sub.LastSent)
			}
		})
	}
}

func TestCSUpdatedSub(t *testing.T) {
	for _, st := range testStores {
		st := st
		t.Run(st.name, func(t *testing.T) {
			if !st.recoverable {
				t.SkipNow()
			}
			t.Parallel()
			defer endTest(t, st)
			s := startTest(t, st)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")
			// Create a subscription.
			subID := storeSub(t, cs, "foo")

			// A message
			msg := []byte("hello")

			// Store msg seq 1 and 2
			m1 := storeMsg(t, cs, "foo", 1, msg)
			m2 := storeMsg(t, cs, "foo", 2, msg)
			m3 := storeMsg(t, cs, "foo", 3, msg)

			// Store m1 and m2 for this subscription
			storeSubPending(t, cs, "foo", subID, m1.Sequence, m2.Sequence)

			// Update the subscription
			ss := cs.Subs
			updatedSub := &spb.SubState{
				ID:            subID,
				ClientID:      "me",
				Inbox:         "inbox",
				AckInbox:      "newAckInbox",
				AckWaitInSecs: 10,
			}
			if err := ss.UpdateSub(updatedSub); err != nil {
				t.Fatalf("Error updating subscription: %v", err)
			}
			// Store m3 for this subscription
			storeSubPending(t, cs, "foo", subID, m3.Sequence)

			// Store a subscription with update only, should be recovered
			subWithoutNew := &spb.SubState{
				ID:            subID + 1,
				ClientID:      "me",
				Inbox:         "inbox",
				AckInbox:      "ackinbox",
				AckWaitInSecs: 10,
			}
			if err := ss.UpdateSub(subWithoutNew); err != nil {
				t.Fatalf("Error updating subscription: %v", err)
			}

			// Restart server
			s.Close()
			s, state := testReOpenStore(t, st, nil)
			defer s.Close()
			subs := getRecoveredSubs(t, state, "foo", 2)
			// Subscriptions are recovered from a map, and then returned as an array.
			// There is no guarantee that we get them in the order they were persisted.
			for _, s := range subs {
				if s.Sub.ID == subID {
					// Check that sub's last seq is m3.Sequence
					if s.Sub.LastSent != m3.Sequence {
						t.Fatalf("Expected LastSent to be %v, got %v", m3.Sequence, s.Sub.LastSent)
					}
					// Update lastSent since we know it is correct.
					updatedSub.LastSent = m3.Sequence
					// Now compare that what we recovered is same that we used to update.
					if !reflect.DeepEqual(*s.Sub, *updatedSub) {
						t.Fatalf("Expected subscription to be %v, got %v", updatedSub, s.Sub)
					}
				} else if s.Sub.ID == subID+1 {
					// Compare that what we recovered is same that we used to update.
					if !reflect.DeepEqual(*s.Sub, *subWithoutNew) {
						t.Fatalf("Expected subscription to be %v, got %v", subWithoutNew, s.Sub)
					}
				} else {
					t.Fatalf("Unexpected subscription ID: %v", s.Sub.ID)
				}
			}
		})
	}
}
