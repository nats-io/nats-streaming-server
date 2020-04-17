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
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

func TestFSBadSubFile(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// Create a valid store file first
	fs := createDefaultFileStore(t)

	cs := storeCreateChannel(t, fs, "foo")
	// Store a subscription
	storeSub(t, cs, "foo")

	// Close it
	fs.Close()

	// First delete the file...
	fileName := filepath.Join(testFSDefaultDatastore, "foo", subsFileName)
	if err := os.Remove(fileName); err != nil {
		t.Fatalf("Unable to delete the subscriptions file %q: %v", fileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating client file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(fileName); err != nil {
			t.Fatalf("Unexpected error removing file: %v", err)
		}
		// Create the file with proper file version
		file, err := openFile(fileName)
		if err != nil {
			t.Fatalf("Error creating file: %v", err)
		}
		return file
	}

	// Restore a valid file
	file := resetToValidFile()
	// Write size that causes read of content to EOF
	if err := util.WriteInt(file, 100); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Test with various types
	types := []recordType{subRecNew, subRecUpdate, subRecDel, subRecMsg, subRecAck, 99}
	content := []byte("abc")
	crc := crc32.ChecksumIEEE(content)
	for _, oneType := range types {
		// Restore a valid file
		file = resetToValidFile()
		// Write a type that does not exist
		if err := util.WriteInt(file, int(oneType)<<24|len(content)); err != nil {
			t.Fatalf("Error writing header: %v", err)
		}
		// Write CRC
		if err := util.WriteInt(file, int(crc)); err != nil {
			t.Fatalf("Error writing crc: %v", err)
		}
		// Write dummy content
		if _, err := file.Write(content); err != nil {
			t.Fatalf("Error writing info: %v", err)
		}
		// Close the file
		if err := file.Close(); err != nil {
			t.Fatalf("Unexpected error closing file: %v", err)
		}
		// We should fail to create the filestore
		expectedErrorOpeningDefaultFileStore(t)
	}
}

func checkSubStoreRecCounts(t *testing.T, s *FileSubStore, expectedSubs, expectedRecs, expectedDelRecs int) {
	s.RLock()
	numSubs := len(s.subs)
	numRecs := s.numRecs
	numDel := s.delRecs
	s.RUnlock()
	if numSubs != expectedSubs {
		stackFatalf(t, "Expected %v subs, got %v", expectedSubs, numSubs)
	}
	if numRecs != expectedRecs {
		stackFatalf(t, "Expected %v recs, got %v", expectedRecs, numRecs)
	}
	if numDel != expectedDelRecs {
		stackFatalf(t, "Expected %v free recs, got %v", expectedDelRecs, numDel)
	}
}

func TestFSCompactSubsFileOnDelete(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	total := 6
	threshold := 3

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs := storeCreateChannel(t, fs, "foo")
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Create an empty sub, we don't care about the content
	sub := &spb.SubState{}
	subIDs := make([]uint64, 0, total)
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	// Delete not enough records to cause compaction
	for i := 0; i < threshold-1; i++ {
		subID := subIDs[i]
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, total-threshold+1, total, threshold-1)

	// Recover
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = getRecoveredChannel(t, state, "foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Make sure our numbers are correct on recovery
	checkSubStoreRecCounts(t, ss, total-threshold+1, total, threshold-1)

	// Delete more to cause compaction
	ss.DeleteSub(subIDs[threshold-1])

	// Since we compact, we now have the same number of recs and subs,
	// and no delete records.
	checkSubStoreRecCounts(t, ss, total-threshold, total-threshold, 0)

	// Make sure we don't compact too often
	count := total - threshold - 1
	for i := 0; i < count; i++ {
		subID := subIDs[threshold+i]
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 1, total-threshold, count)

	// Wait for longer than compact interval
	time.Sleep(1500 * time.Millisecond)
	// Cause a compact by adding and then removing a subscription
	ss.DeleteSub(subIDs[total-1])
	// Check stats
	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	// Check that compacted file is as expected
	fs.Close()
	fs, state = openDefaultFileStore(t)
	defer fs.Close()
	cs = getRecoveredChannel(t, state, "foo")
	ss = cs.Subs.(*FileSubStore)

	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	fs.Close()
	// Wipe-out everything
	cleanupFSDatastore(t)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.Unlock()

	cs = storeCreateChannel(t, fs, "foo")
	ss = cs.Subs.(*FileSubStore)

	// Make sure we can't compact
	subIDs = subIDs[:0]
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	for _, subID := range subIDs {
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 0, total, total)

	fs.Close()
	// Wipe-out everything
	cleanupFSDatastore(t)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()

	cs = storeCreateChannel(t, fs, "foo")
	ss = cs.Subs.(*FileSubStore)

	// Make sure we can't compact
	subIDs = subIDs[:0]
	for i := 0; i < total; i++ {
		if err := ss.CreateSub(sub); err != nil {
			t.Fatalf("Unexpected error creating subscription: %v", err)
		}
		subIDs = append(subIDs, sub.ID)
	}
	checkSubStoreRecCounts(t, ss, total, total, 0)
	for _, subID := range subIDs {
		ss.DeleteSub(subID)
	}
	checkSubStoreRecCounts(t, ss, 0, total, total)
}

func TestFSCompactSubsFileOnAck(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs := storeCreateChannel(t, fs, "foo")
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	totalSeqs := 10
	threshold := 1 + 5 // 1 for the sub, 5 for acks

	// Create an empty sub, we don't care about the content
	sub := &spb.SubState{}
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}

	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, 0)
	// Delete not enough records to cause compaction
	for i := 0; i < threshold-1; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding ack: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, threshold-1)

	// Recover
	fs.Close()
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = getRecoveredChannel(t, state, "foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	// Make sure our numbers are correct on recovery
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, threshold-1)

	// Add 1 more ack to cause compaction
	if err := ss.AckSeqPending(sub.ID, uint64(threshold)); err != nil {
		t.Fatalf("Unexpected error adding ack: %v", err)
	}
	// Now the number of acks should be 0.
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs-threshold, 0)
	startCount := 1 + totalSeqs - threshold

	// Make sure we don't compact too often
	start := 10000
	// Add some
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 0)
	// Then remove them all. Total gain/loss is 0.
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding ack: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 2*totalSeqs)

	// Wait for longer than compact interval
	time.Sleep(1500 * time.Millisecond)
	// Cause a compact
	willCompactID := uint64(20000)
	if err := ss.AddSeqPending(sub.ID, willCompactID); err != nil {
		t.Fatalf("Unexpected error adding seq: %v", err)
	}
	if err := ss.AckSeqPending(sub.ID, willCompactID); err != nil {
		t.Fatalf("Unexpected error adding ack: %v", err)
	}
	// Check stats
	checkSubStoreRecCounts(t, ss, 1, startCount, 0)

	// Check that compacted file is as expected
	fs.Close()
	fs, state = openDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	cs = getRecoveredChannel(t, state, "foo")
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.compactItvl = time.Second
	ss.Unlock()

	checkSubStoreRecCounts(t, ss, 1, startCount, 0)

	// Add more sequences
	start = 30000
	// Add some
	for i := 0; i < 2*totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(start+i)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, startCount+2*totalSeqs, 0)
	// Remove the subscription, this should cause a compact
	ss.DeleteSub(sub.ID)
	checkSubStoreRecCounts(t, ss, 0, 0, 0)

	fs.Close()
	// Wipe-out everything
	cleanupFSDatastore(t)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.Unlock()

	cs = storeCreateChannel(t, fs, "foo")
	ss = cs.Subs.(*FileSubStore)

	// Create an empty sub, we don't care about the content
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}
	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	// Remove all
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, totalSeqs)

	fs.Close()
	// Wipe-out everything
	cleanupFSDatastore(t)

	fs = createDefaultFileStore(t)
	defer fs.Close()

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = 50
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()

	cs = storeCreateChannel(t, fs, "foo")
	ss = cs.Subs.(*FileSubStore)

	// Create an empty sub, we don't care about the content
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Unexpected error creating subscription: %v", err)
	}
	// Add sequences
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AddSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	// Remove all
	for i := 0; i < totalSeqs; i++ {
		if err := ss.AckSeqPending(sub.ID, uint64(i+1)); err != nil {
			t.Fatalf("Unexpected error adding seq: %v", err)
		}
	}
	checkSubStoreRecCounts(t, ss, 1, 1+totalSeqs, totalSeqs)
}

func TestFSNoReferenceToCallerSubState(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	ss := cs.Subs
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         "inbox",
		AckInbox:      "ackinbox",
		AckWaitInSecs: 10,
	}
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Error creating subscription")
	}
	// Get the fileStore sub object
	fss := ss.(*FileSubStore)
	fss.RLock()
	storeSub := fss.subs[sub.ID].(*subscription).sub
	fss.RUnlock()
	// Content of filestore's subscription must match sub
	if !reflect.DeepEqual(*sub, *storeSub) {
		t.Fatalf("Expected sub to be %v, got %v", sub, storeSub)
	}
	// However, these should not be the same objects (no sharing)
	if sub == storeSub {
		t.Fatalf("SubState should not be shared between server and store")
	}
}

func TestFSCompactSubsUpdateLastSent(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	total := 10
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), []byte("hello"))
	}
	expectedLastSent := 5
	subID := storeSub(t, cs, "foo")
	for i := 0; i < expectedLastSent; i++ {
		storeSubPending(t, cs, "foo", subID, uint64(i+1))
	}
	// Consume the 2 last
	storeSubAck(t, cs, "foo", subID, 4, 5)
	// Force a compact
	ss := cs.Subs.(*FileSubStore)
	ss.compact(ss.file.name)
	// Close and re-open store
	s.Close()
	s, rs := openDefaultFileStore(t)
	defer s.Close()
	// Get sub from recovered state
	rsubs := getRecoveredSubs(t, rs, "foo", 1)
	rsub := rsubs[0]
	if rsub.Sub.LastSent != uint64(expectedLastSent) {
		t.Fatalf("Expected recovered subscription LastSent to be %v, got %v", expectedLastSent, rsub.Sub.LastSent)
	}
}

func TestFSSubStoreVariousBufferSizes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	seq := uint64(1)
	sizes := []int{0, subBufMinShrinkSize - subBufMinShrinkSize/10, subBufMinShrinkSize, 3*subBufMinShrinkSize + subBufMinShrinkSize/2}
	for _, size := range sizes {

		// Create a store with buffer writer of the given size
		fs := createDefaultFileStore(t, BufferSize(size))
		defer fs.Close()

		cs := storeCreateChannel(t, fs, "foo")
		m := storeMsg(t, cs, "foo", seq, []byte("hello"))
		seq++

		// Perform some activities on subscriptions file
		subID := storeSub(t, cs, "foo")

		// Get FileSubStore
		ss := cs.Subs.(*FileSubStore)

		// Cause a flush to empty the buffer
		ss.Flush()

		// Check that bw is not nil and writer points to the buffer writer
		ss.RLock()
		bw := ss.bw
		writer := ss.writer
		file := ss.file.handle
		bufSize := 0
		if ss.bw != nil {
			bufSize = ss.bw.buf.Available()
		}
		ss.RUnlock()
		if size == 0 {
			if bw != nil {
				t.Fatal("FileSubStore's buffer writer should be nil")
			}
		} else if bw == nil {
			t.Fatal("FileSubStore's buffer writer should not be nil")
		}
		if size == 0 {
			if writer != file {
				t.Fatal("FileSubStore's writer should be set to file")
			}
		} else if writer != bw.buf {
			t.Fatal("FileSubStore's writer should be set to the buffer writer")
		}
		initialSize := size
		if size > subBufMinShrinkSize {
			initialSize = subBufMinShrinkSize
		}
		if bufSize != initialSize {
			t.Fatalf("Incorrect initial size, should be %v, got %v", initialSize, bufSize)
		}

		// Fill up the buffer (meaningfull only when buffer is used)
		fillBuffer := func() {
			total := 0
			for i := 0; i < 1000; i++ {
				ss.RLock()
				before := ss.bw.buf.Buffered()
				ss.RUnlock()
				storeSubPending(t, cs, "foo", subID, m.Sequence)
				ss.RLock()
				if ss.bw.buf.Buffered() > before {
					total += ss.bw.buf.Buffered() - before
				} else {
					total += ss.bw.buf.Buffered()
				}
				ss.RUnlock()
				// Stop when we have persisted at least 2 times the max buffer size
				if total >= 2*size {
					// We should have caused buffer to be flushed by now
					break
				}
			}
			if total < 2*size {
				t.Fatalf("Did not reach target total (%v, got %v) after limit iterations", 2*size, total)
			}
		}
		if size > 0 {
			fillBuffer()
		} else {
			// Just write a bunch of stuff
			for i := 0; i < 50; i++ {
				storeSubPending(t, cs, "foo", subID, m.Sequence)
			}
		}

		ss.RLock()
		bufSize = 0
		if size > 0 {
			bufSize = ss.bw.bufSize
		}
		ss.RUnlock()
		if size == 0 {
			if bufSize != 0 {
				t.Fatalf("BufferSize is 0, so ss.bufSize should be 0, got %v", bufSize)
			}
		} else if size <= subBufMinShrinkSize {
			// If size is smaller than min shrink size, the buffer should not have
			// increased in size
			if bufSize > subBufMinShrinkSize {
				t.Fatalf("BufferSize=%v - ss.bw size should at or below %v, got %v", size, subBufMinShrinkSize, bufSize)
			}
		} else {
			// We should have started at min size, and now size should have been increased.
			if bufSize <= subBufMinShrinkSize || bufSize > size {
				t.Fatalf("BufferSize=%v - ss.bw size should have increased but no more than %v, got %v", size, size, bufSize)
			}
		}

		// Delete subscription
		storeSubDelete(t, cs, "foo", subID)
		// Compact
		ss.Lock()
		err := ss.compact(ss.file.name)
		ss.Unlock()
		if err != nil {
			t.Fatalf("Error during compact: %v", err)
		}
		ss.RLock()
		bw = ss.bw
		writer = ss.writer
		file = ss.file.handle
		shrinkTimerOn := ss.shrinkTimer != nil
		ss.RUnlock()
		if size == 0 {
			if bw != nil {
				t.Fatal("FileSubStore's buffer writer should be nil")
			}
		} else if bw == nil {
			t.Fatal("FileSubStore's buffer writer should not be nil")
		}
		if size == 0 {
			if writer != file {
				t.Fatal("FileSubStore's writer should be set to file")
			}
		} else if writer != bw.buf {
			t.Fatal("FileSubStore's writer should be set to the buffer writer")
		}
		// When buffer size is greater than min size, see if it shrinks
		if size > subBufMinShrinkSize {
			if !shrinkTimerOn {
				t.Fatal("Timer should have been created to try shrink the buffer")
			}
			// Invoke the timer callback manually (so we don't have to wait)
			// Call many times and make sure size never goes down too low.
			for i := 0; i < 14; i++ {
				ss.shrinkBuffer(false)
			}
			// Now check
			ss.RLock()
			bufSizeNow := ss.bw.bufSize
			ss.RUnlock()
			if bufSizeNow >= bufSize {
				t.Fatalf("BufferSize=%v - Buffer size expected to decrease, got: %v", size, bufSizeNow)
			}
			if bufSizeNow < subBufMinShrinkSize {
				t.Fatalf("BufferSize=%v - Buffer should not go below %v, got %v", size, subBufMinShrinkSize, bufSizeNow)
			}

			// Check that the request to shrink is canceled if more data arrive
			// First make buffer expand.
			fillBuffer()
			// Flush to empty it
			ss.Flush()
			// Invoke shrink
			ss.shrinkBuffer(false)
			// Check that request is set
			ss.RLock()
			shrinkReq := ss.bw.shrinkReq
			ss.RUnlock()
			if !shrinkReq {
				t.Fatal("Shrink request should be true")
			}
			// Cause buffer to expand again
			fillBuffer()
			// Check that request should have been canceled.
			ss.RLock()
			shrinkReq = ss.bw.shrinkReq
			ss.RUnlock()
			if shrinkReq {
				t.Fatal("Shrink request should be false")
			}
		}
		fs.Close()
		cleanupFSDatastore(t)
	}
}

func TestFSSubAPIsOnFileErrors(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// No buffer for this test
	fs := createDefaultFileStore(t, BufferSize(0))
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.handle.Close()
	ss.Unlock()

	expectToFail := func(f func() error) {
		if err := f(); err == nil {
			stackFatalf(t, "Expected to get error about file being closed, got none")
		}
	}
	expectToFail(func() error { return cs.Subs.CreateSub(&spb.SubState{}) })
	expectToFail(func() error { return cs.Subs.UpdateSub(&spb.SubState{}) })
	expectToFail(func() error { return cs.Subs.AddSeqPending(1, 1) })
	expectToFail(func() error { return cs.Subs.AckSeqPending(1, 1) })
	expectToFail(func() error { return cs.Subs.DeleteSub(1) })
}

func TestFSCreateSubNotCountedOnError(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// No buffer for this test
	fs := createDefaultFileStore(t, BufferSize(0))
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.handle.Close()
	ss.Unlock()

	sub := &spb.SubState{}
	if err := ss.CreateSub(sub); err == nil {
		t.Fatal("Expected error on sub create, got none")
	}

	// Verify that the subscriptions' map is empty
	ss.RLock()
	lm := len(ss.subs)
	ss.RUnlock()
	if lm != 0 {
		t.Fatalf("Expected subs map to be empty, got %v", lm)
	}
}

func TestFSSubscriptionsFileWithExtraZeros(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ss := c.Subs
	sub1 := &spb.SubState{
		ClientID:      "me",
		Inbox:         "inbox",
		AckInbox:      "ackInbox",
		AckWaitInSecs: 10,
	}
	if err := ss.CreateSub(sub1); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	ss.(*FileSubStore).RLock()
	fname := ss.(*FileSubStore).file.name
	ss.(*FileSubStore).RUnlock()

	s.Close()

	f, err := openFileWithFlags(fname, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	b := make([]byte, recordHeaderSize)
	if _, err := f.Write(b); err != nil {
		t.Fatalf("Error adding zeros: %v", err)
	}
	f.Close()

	// Reopen file store
	s, rs := openDefaultFileStore(t)
	defer s.Close()
	c = getRecoveredChannel(t, rs, "foo")
	ss = c.Subs
	subs := getRecoveredSubs(t, rs, "foo", 1)
	sub := subs[0]
	if !reflect.DeepEqual(sub.Sub, sub1) {
		t.Fatalf("Expected subscription %v, got %v", sub1, sub.Sub)
	}
	// Add one more sub
	sub2 := &spb.SubState{
		ClientID:      "me2",
		Inbox:         "inbox2",
		AckInbox:      "ackInbox2",
		AckWaitInSecs: 12,
	}
	if err := ss.CreateSub(sub2); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	s.Close()

	// Reopen file store
	s, rs = openDefaultFileStore(t)
	defer s.Close()
	subs = getRecoveredSubs(t, rs, "foo", 2)
	for _, sub := range subs {
		// subs is an array but created from a map, so order is not guaranteed.
		var osub *spb.SubState
		if sub.Sub.ID == sub1.ID {
			osub = sub1
		} else {
			osub = sub2
		}
		if !reflect.DeepEqual(sub.Sub, osub) {
			t.Fatalf("Expected subscription %v, got %v", osub, sub.Sub)
		}
	}
}

func TestFSSubscriptionsFileVersionError(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ss := c.Subs
	sub1 := &spb.SubState{
		ClientID:      "me",
		Inbox:         "inbox",
		AckInbox:      "ackInbox",
		AckWaitInSecs: 10,
	}
	if err := ss.CreateSub(sub1); err != nil {
		t.Fatalf("Error creating sub: %v", err)
	}
	ss.(*FileSubStore).RLock()
	fname := ss.(*FileSubStore).file.name
	ss.(*FileSubStore).RUnlock()

	s.Close()

	os.Remove(fname)
	if err := ioutil.WriteFile(fname, []byte(""), 0666); err != nil {
		t.Fatalf("Error writing file: %v", err)
	}

	s, err := NewFileStore(testLogger, testFSDefaultDatastore, nil)
	if err != nil {
		t.Fatalf("Error creating filestore: %v", err)
	}
	defer s.Close()
	if _, err := s.Recover(); err == nil || !strings.Contains(err.Error(), "recover subscription store for [foo]") {
		t.Fatalf("Unexpected error: %v", err)
	}
}
