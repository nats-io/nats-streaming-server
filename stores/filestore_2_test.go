// Copyright 2016-2017 Apcera Inc. All rights reserved.

package stores

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

func TestFSBadSubFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Create a valid store file first
	fs := createDefaultFileStore(t)

	cs := storeCreateChannel(t, fs, "foo")
	// Store a subscription
	storeSub(t, cs, "foo")

	// Close it
	fs.Close()

	// First delete the file...
	fileName := filepath.Join(defaultDataStore, "foo", subsFileName)
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

func TestFSAddClientError(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	// Test failure of AddClient (generic tested in common_test.go)
	// Close the client file to cause error
	fs.clientsFile.handle.Close()
	// Should fail
	if c, err := fs.AddClient("c1", "hbInbox"); err == nil {
		t.Fatal("Expected error, got none")
	} else if c != nil {
		t.Fatalf("Should not have gotten a client back, got %v", c)
	}
}

func TestFSCompactClientFile(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	total := 10
	threshold := total / 2

	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()

	check := func(fs *FileStore, expectedClients, expectedDelRecs int) {
		fs.RLock()
		numClients := len(fs.clients)
		delRecs := fs.cliDeleteRecs
		fs.RUnlock()
		if numClients != expectedClients {
			stackFatalf(t, "Expected %v clients, got %v", expectedClients, numClients)
		}
		if delRecs != expectedDelRecs {
			stackFatalf(t, "Expected %v delete records, got %v", expectedDelRecs, delRecs)
		}
	}

	// Create clients below threshold
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete half.
	for i := 0; i < threshold-1; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeDeleteClient(t, fs, cid)
	}
	check(fs, threshold+1, threshold-1)

	// Recover
	fs.Close()
	fs, _ = openDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	// since we set things manually, we need to compute this here
	fs.compactItvl = time.Second
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()
	// Verify our numbers are same after recovery
	check(fs, threshold+1, threshold-1)

	// Delete one more, this should trigger compaction
	cid := fmt.Sprintf("cid_%d", threshold)
	storeDeleteClient(t, fs, cid)
	// One client less, 0 del records after a compaction
	check(fs, threshold, 0)

	// Make sure we don't compact too often
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Delete almost all of them
	for i := 0; i < total-1; i++ {
		cid := fmt.Sprintf("cid_%d", total+i+1)
		storeDeleteClient(t, fs, cid)
	}
	// The number of clients should be same than before + 1,
	// and lots of delete
	check(fs, threshold+1, total-1)
	// Now wait for the interval and a bit more
	time.Sleep(1500 * time.Millisecond)
	// Delete one more, compaction should occur
	cid = fmt.Sprintf("cid_%d", 2*total)
	storeDeleteClient(t, fs, cid)
	// One less client, 0 delete records after compaction
	check(fs, threshold, 0)

	fs.Close()
	// Wipe out
	cleanupDatastore(t, defaultDataStore)
	fs = createDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes: disable compaction
	fs.Lock()
	fs.opts.CompactEnabled = false
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = -1
	fs.Unlock()
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeDeleteClient(t, fs, cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)

	fs.Close()
	// Wipe out
	cleanupDatastore(t, defaultDataStore)
	fs = createDefaultFileStore(t)
	defer fs.Close()
	// Override options for test purposes: have a big min file size
	fs.Lock()
	fs.opts.CompactEnabled = true
	fs.opts.CompactFragmentation = threshold * 100 / total
	fs.opts.CompactMinFileSize = 10 * 1024 * 1024
	fs.Unlock()
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeAddClient(t, fs, cid, "hbInbox")
	}
	// Should be `total` clients, and 0 delete records
	check(fs, total, 0)
	// Delete all
	for i := 0; i < total; i++ {
		cid := fmt.Sprintf("cid_%d", (i + 1))
		storeDeleteClient(t, fs, cid)
	}
	// No client, but no reduction in number of delete records since no compaction
	check(fs, 0, total)
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
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
	cleanupDatastore(t, defaultDataStore)

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
	cleanupDatastore(t, defaultDataStore)

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
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

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
	cleanupDatastore(t, defaultDataStore)

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
	cleanupDatastore(t, defaultDataStore)

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

func TestFSFlush(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, DoSync(true))
	defer fs.Close()

	cs := testFlush(t, fs)

	// Now specific tests to File store
	msg := storeMsg(t, cs, "foo", []byte("new msg"))
	subID := storeSub(t, cs, "foo")
	storeSubPending(t, cs, "foo", subID, msg.Sequence)
	// Close the underlying file
	ms := cs.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.writeSlice.file.handle.Close()
	ms.Unlock()
	// Expect Flush to fail
	if err := cs.Msgs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
	// Close the underlying file
	ss := cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.handle.Close()
	ss.Unlock()
	// Expect Flush to fail
	if err := cs.Subs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}

	// Close and re-open
	fs.Close()
	fs, state := openDefaultFileStore(t, DoSync(true))
	defer fs.Close()

	// Check that Flush() is still doing Sync even if
	// buf writer is empty
	cs = getRecoveredChannel(t, state, "foo")
	// Close the underlying file
	ms = cs.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.writeSlice.file.handle.Close()
	ms.Unlock()
	// Expect Flush to fail
	if err := cs.Msgs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
	// Close the underlying file
	ss = cs.Subs.(*FileSubStore)
	ss.Lock()
	ss.file.handle.Close()
	// Simulate that there was activity (alternatively,
	// we would need a buffer size smaller than a sub record
	// being written so that buffer writer is by-passed).
	ss.activity = true
	ss.Unlock()
	// Expect Flush to fail
	if err := cs.Subs.Flush(); err == nil {
		t.Fatal("Expected Flush to fail, did not")
	}
}

func TestFSDoSync(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	total := 100
	dur := [2]time.Duration{}

	for i := 0; i < 2; i++ {
		sOpts := DefaultFileStoreOptions
		sOpts.DoSync = true
		if i == 1 {
			sOpts.DoSync = false
		}
		fs, _, err := newFileStore(t, defaultDataStore, &testDefaultStoreLimits, AllOptions(&sOpts))
		if err != nil {
			stackFatalf(t, "Unable to create a FileStore instance: %v", err)
		}
		defer fs.Close()

		cs := storeCreateChannel(t, fs, "foo")
		subID := storeSub(t, cs, "foo")

		msg := make([]byte, 1024)
		start := time.Now()
		// Send more message when fsync is disabled. It should still be faster,
		// and would catch if bug in code where we always do fsync, regardless
		// of option.
		for j := 0; j < total+(i*total/10); j++ {
			m := storeMsg(t, cs, "foo", msg)
			cs.Msgs.Flush()
			storeSubPending(t, cs, "foo", subID, m.Sequence)
			cs.Subs.Flush()
		}
		dur[i] = time.Since(start)

		fs.Close()
		cleanupDatastore(t, defaultDataStore)
	}
	if dur[0] < dur[1] {
		t.Fatalf("Expected File sync enabled to be slower than disabled: %v vs %v", dur[0], dur[1])
	}
}

type testReader struct {
	content     []byte
	start       int
	errToReturn error
}

func (t *testReader) setContent(content []byte) {
	t.content = content
	t.start = 0
}

func (t *testReader) setErrToReturn(err error) {
	t.errToReturn = err
}

func (t *testReader) Read(p []byte) (n int, err error) {
	if t.errToReturn != nil {
		return 0, t.errToReturn
	}
	if len(t.content)-t.start < len(p) {
		return 0, io.EOF
	}
	copy(p, t.content[t.start:t.start+len(p)])
	t.start += len(p)
	return len(t.content), nil
}

func TestFSReadRecord(t *testing.T) {
	r := &testReader{}

	var err error

	buf := make([]byte, 5)
	var retBuf []byte
	recType := recNoType
	recSize := 0

	// Reader returns an error
	errReturned := fmt.Errorf("Fake error")
	r.setErrToReturn(errReturned)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err != errReturned {
		t.Fatalf("Expected error %v, got: %v", errReturned, err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Record not containing CRC
	_header := [4]byte{}
	header := _header[:]
	util.ByteOrder.PutUint32(header, 0)
	r.setErrToReturn(nil)
	r.setContent(header)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Wrong CRC
	b := make([]byte, recordHeaderSize+5)
	util.ByteOrder.PutUint32(b, 5)
	copy(b[recordHeaderSize:], []byte("hello"))
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if recSize != 0 {
		t.Fatalf("Expected recSize to be 0, got %v", recSize)
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}
	// Not asking for CRC should return ok
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if string(retBuf[:recSize]) != "hello" {
		t.Fatalf("Expected body to be \"hello\", got %q", string(retBuf[:recSize]))
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Check that returned buffer has expanded as required
	b = make([]byte, recordHeaderSize+10)
	payload := []byte("hellohello")
	util.ByteOrder.PutUint32(b, uint32(len(payload)))
	util.ByteOrder.PutUint32(b[4:recordHeaderSize], crc32.ChecksumIEEE(payload))
	copy(b[recordHeaderSize:], payload)
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, false, crc32.IEEETable, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be different than the one provided")
	}
	if string(retBuf[:recSize]) != string(payload) {
		t.Fatalf("Expected body to be %q got %v", string(payload), string(retBuf[:recSize]))
	}
	if recType != recNoType {
		t.Fatalf("Expected recType to be recNoType, got %v", recType)
	}

	// Check rec type returned properly
	util.ByteOrder.PutUint32(b, 1<<24|10) // reuse previous buf
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, recSize, recType, err = readRecord(r, buf, true, crc32.IEEETable, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be different than the one provided")
	}
	if string(retBuf[:recSize]) != string(payload) {
		t.Fatalf("Expected body to be %q got %v", string(payload), string(retBuf[:recSize]))
	}
	if recType != recordType(1) {
		t.Fatalf("Expected recType to be 1, got %v", recType)
	}
}

type testWriter struct {
	buf         []byte
	errToReturn error
}

func (t *testWriter) setErrToReturn(err error) {
	t.errToReturn = err
}

func (t *testWriter) reset() {
	t.buf = t.buf[:0]
}

func (t *testWriter) Write(p []byte) (n int, err error) {
	if t.errToReturn != nil {
		return 0, t.errToReturn
	}
	t.buf = append(t.buf, p...)
	return len(p), nil
}

type recordProduceErrorOnMarshal struct {
	errToReturn error
}

func (r *recordProduceErrorOnMarshal) Size() int {
	return 1
}

func (r *recordProduceErrorOnMarshal) MarshalTo(b []byte) (int, error) {
	return 0, r.errToReturn
}

func TestFSWriteRecord(t *testing.T) {
	w := &testWriter{}

	var err error

	buf := make([]byte, 100)
	var retBuf []byte
	size := 0

	cli := &spb.ClientInfo{ID: "me", HbInbox: "inbox"}
	cliBuf, _ := cli.Marshal()

	retBuf, size, err = writeRecord(w, buf, addClient, cli, cli.Size(), crc32.IEEETable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if size != recordHeaderSize+len(cliBuf) {
		t.Fatalf("Expected size to be %v, got %v", recordHeaderSize+len(cliBuf), size)
	}
	header := util.ByteOrder.Uint32(w.buf[:4])
	recType := recordType(header >> 24 & 0xFF)
	if recType != addClient {
		t.Fatalf("Expected type %v, got %v", addClient, recType)
	}
	crc := util.ByteOrder.Uint32(w.buf[4:recordHeaderSize])
	if crc != crc32.ChecksumIEEE(cliBuf) {
		t.Fatal("Wrong crc")
	}
	if !reflect.DeepEqual(retBuf[recordHeaderSize:size], cliBuf) {
		t.Fatalf("Unexpected content: %v", retBuf[recordHeaderSize:])
	}

	// Check with no type
	w.reset()
	retBuf, size, err = writeRecord(w, buf, recNoType, cli, cli.Size(), crc32.IEEETable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if size != recordHeaderSize+len(cliBuf) {
		t.Fatalf("Expected size to be %v, got %v", recordHeaderSize+len(cliBuf), size)
	}
	header = util.ByteOrder.Uint32(w.buf[:4])
	recType = recordType(header >> 24 & 0xFF)
	if recType != recNoType {
		t.Fatalf("Expected type %v, got %v", recNoType, recType)
	}
	crc = util.ByteOrder.Uint32(w.buf[4:recordHeaderSize])
	if crc != crc32.ChecksumIEEE(cliBuf) {
		t.Fatal("Wrong crc")
	}
	if !reflect.DeepEqual(retBuf[recordHeaderSize:size], cliBuf) {
		t.Fatalf("Unexpected content: %v", retBuf[recordHeaderSize:])
	}

	// Check for marshaling error
	w.reset()
	errReturned := fmt.Errorf("Fake error")
	corruptRec := &recordProduceErrorOnMarshal{errToReturn: errReturned}
	retBuf, size, err = writeRecord(w, buf, recNoType, corruptRec, corruptRec.Size(), crc32.IEEETable)
	if err != errReturned {
		t.Fatalf("Expected error %v, got %v", errReturned, err)
	}
	if retBuf == nil {
		t.Fatal("Returned slice should not be nil")
	}
	if size != 0 {
		t.Fatalf("Size should be 0, got %v", size)
	}
}

func TestFSNoPartialWriteDueToBuffering(t *testing.T) {
	// We could use any record type here, using MsgProto
	m1 := &pb.MsgProto{Sequence: 1, Subject: "foo", Data: []byte("msg1")}
	m2 := &pb.MsgProto{Sequence: 2, Subject: "foo", Data: []byte("msg2")}

	w := &testWriter{}
	size := m1.Size() + recordHeaderSize + 2
	bw := bufio.NewWriterSize(w, size)

	// Write first record
	m1buf, _, _ := writeRecord(bw, nil, recNoType, m1, m1.Size(), crc32.IEEETable)
	// Now add m2
	writeRecord(bw, nil, recNoType, m2, m2.Size(), crc32.IEEETable)
	// Check that w's buf only contains m2 (no partial)
	if bw.Buffered() != m2.Size()+recordHeaderSize {
		t.Fatalf("Expected buffer to contain %v bytes, got %v", m2.Size()+recordHeaderSize, bw.Buffered())
	}
	if len(w.buf) != m1.Size()+recordHeaderSize {
		t.Fatalf("Expected backend buffer to contain %v bytes, got %v", m1.Size()+recordHeaderSize, len(w.buf))
	}
	if !reflect.DeepEqual(w.buf, m1buf) {
		t.Fatal("Backend buffer does not contain what we expected")
	}

	w.reset()
	errThrown := fmt.Errorf("On purpose")
	w.setErrToReturn(errThrown)
	// Write first record
	writeRecord(bw, nil, recNoType, m1, m1.Size(), crc32.IEEETable)
	// Now add m2
	_, _, err := writeRecord(bw, nil, recNoType, m2, m2.Size(), crc32.IEEETable)
	if err != errThrown {
		t.Fatalf("Expected error %v, got %v", errThrown, err)
	}
}

func TestFSNoReferenceToCallerSubState(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	s := createDefaultFileStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	ss := cs.Subs
	sub := &spb.SubState{
		ClientID:      "me",
		Inbox:         nuidGen.Next(),
		AckInbox:      nuidGen.Next(),
		AckWaitInSecs: 10,
	}
	if err := ss.CreateSub(sub); err != nil {
		t.Fatalf("Error creating subscription")
	}
	// Get the fileStore sub object
	fss := ss.(*FileSubStore)
	fss.RLock()
	storeSub := fss.subs[sub.ID].sub
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
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	s := createDefaultFileStore(t)
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	total := 10
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", []byte("hello"))
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

func TestFSFileSlicesClosed(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Reduce some filestore values for this test
	bkgTasksSleepDuration = 100 * time.Millisecond
	cacheTTL = int64(200 * time.Millisecond)
	defer func() {
		bkgTasksSleepDuration = defaultBkgTasksSleepDuration
		cacheTTL = int64(defaultCacheTTL)
	}()

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 50
	fs, err := NewFileStore(testLogger, defaultDataStore, &limits,
		SliceConfig(10, 0, 0, ""))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer fs.Close()
	payload := []byte("hello")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < limits.MaxMsgs; i++ {
		storeMsg(t, cs, "foo", payload)
	}
	ms := cs.Msgs.(*FileMsgStore)
	ms.Flush()
	// Wait for cache to be empty
	timeout := time.Now().Add(time.Duration(3 * cacheTTL))
	empty := false
	for time.Now().Before(timeout) {
		ms.RLock()
		empty = len(ms.cache.seqMaps) == 0
		ms.RUnlock()
		if empty {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !empty {
		t.Fatal("Cache should be empty")
	}
	for i := 0; i < limits.MaxMsgs; i++ {
		ms.Lookup(uint64(i + 1))
	}
	time.Sleep(1500 * time.Millisecond)
	ms.RLock()
	for i, s := range ms.files {
		if s == ms.writeSlice {
			continue
		}
		if s.file.handle != nil {
			ms.RUnlock()
			t.Fatalf("File slice %v should be closed (data file)", i)
		}
		if s.idxFile.handle != nil {
			ms.RUnlock()
			t.Fatalf("File slice %v should be closed (index file)", i)
		}
	}
	ms.RUnlock()
}

func TestFSRecoverWithoutIndexFiles(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 8
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	total := limits.MaxMsgs + 1
	payload := []byte("hello")
	msgs := make([]*pb.MsgProto, 0, total)
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		msgs = append(msgs, storeMsg(t, cs, "foo", payload))
	}
	msgStore := cs.Msgs.(*FileMsgStore)
	// Get the index file names
	fs.RLock()
	idxFileNames := make([]string, 0, len(msgStore.files))
	for _, sl := range msgStore.files {
		idxFileNames = append(idxFileNames, sl.idxFile.name)
	}
	fs.RUnlock()
	// Close store
	fs.Close()

	// Remove the index files
	for _, fn := range idxFileNames {
		if err := os.Remove(fn); err != nil {
			t.Fatalf("Error removing file %q: %v", fn, err)
		}
	}
	// Restart store
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	cs = getRecoveredChannel(t, state, "foo")
	for i := 0; i < total; i++ {
		m := msgStoreLookup(t, cs.Msgs, uint64(i+1))
		if !reflect.DeepEqual(m, msgs[i]) {
			t.Fatalf("Expected to get message %v, got %v", msgs[i], m)
		}
	}
}

func TestFSEmptySlice(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	m := storeMsg(t, cs, "foo", []byte("hello"))

	fs.Close()

	// Add an empty slice
	file, err := openFile(filepath.Join(defaultDataStore, "foo", msgFilesPrefix+"2"+datSuffix))
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	file.Close()

	// Recover
	fs, state := openDefaultFileStore(t)
	defer fs.Close()

	cs = getRecoveredChannel(t, state, "foo")
	lm := msgStoreLookup(t, cs.Msgs, 1)
	if err != nil {
		t.Fatalf("Error getting message 1: %v", err)
	}
	if !reflect.DeepEqual(m, lm) {
		t.Fatalf("Expected recovered message to be %v, got %v", m, lm)
	}
}

func TestFSStoreMsgCausesFlush(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, BufferSize(50))
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	m1 := storeMsg(t, cs, "foo", []byte("hello"))
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	buffered := ms.bw.buf.Buffered()
	bufferedMsgs := len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != m1.Size()+recordHeaderSize {
		t.Fatalf("Expected buffered to be %v, got %v", m1.Size()+recordHeaderSize, buffered)
	}
	if bufferedMsgs != 1 {
		t.Fatalf("Expected 1 buffered message, got %v", bufferedMsgs)
	}

	m2 := storeMsg(t, cs, "foo", []byte("hello again!"))
	ms.RLock()
	buffered = ms.bw.buf.Buffered()
	bufferedMsgs = len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != m2.Size()+recordHeaderSize {
		t.Fatalf("Expected buffered to be %v, got %v", m2.Size()+recordHeaderSize, buffered)
	}
	if bufferedMsgs != 1 {
		t.Fatalf("Expected 1 buffered message, got %v", bufferedMsgs)
	}

	// Now store a message that is bigger than the buffer, it should be
	// directly written to file
	payload := make([]byte, 200)
	storeMsg(t, cs, "foo", payload)
	ms.RLock()
	buffered = ms.bw.buf.Buffered()
	bufferedMsgs = len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != 0 {
		t.Fatalf("Expected buffered to be 0, got %v", buffered)
	}
	if bufferedMsgs != 0 {
		t.Fatalf("Expected 0 buffered message, got %v", bufferedMsgs)
	}
}

func TestFSRemoveFileSlices(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// Set config such that each slice store only 1 message
	fs := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	limits := DefaultStoreLimits
	// Ensure that slices will be removed.
	limits.MaxMsgs = 3
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	expectedFirst := uint64(5)
	total := 7
	payload := []byte("hello")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", payload)
	}
	// Check first and last indexes
	ms := cs.Msgs.(*FileMsgStore)
	if m := msgStoreFirstMsg(t, ms); m.Sequence != expectedFirst {
		t.Fatalf("Expected message sequence to be %v, got %v", expectedFirst, m.Sequence)
	}
	if m := msgStoreLastMsg(t, ms); m.Sequence != uint64(total) {
		t.Fatalf("Expected message sequence to be %v, got %v", total, m.Sequence)
	}
	// Close store
	fs.Close()

	// Reopen
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	cs = getRecoveredChannel(t, state, "foo")
	ms = cs.Msgs.(*FileMsgStore)
	if m := msgStoreFirstMsg(t, ms); m.Sequence != expectedFirst {
		t.Fatalf("Expected message sequence to be %v, got %v", expectedFirst, m.Sequence)
	}
	if m := msgStoreLastMsg(t, ms); m.Sequence != uint64(total) {
		t.Fatalf("Expected message sequence to be %v, got %v", total, m.Sequence)
	}
}

func TestFSFirstEmptySliceRemovedOnCreateNewSlice(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, time.Second, ""))
	defer fs.Close()

	limits := DefaultStoreLimits
	limits.MaxAge = time.Second
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	cs := storeCreateChannel(t, fs, "foo")
	// Store a message
	storeMsg(t, cs, "foo", []byte("test"))

	// Wait for message to expire
	timeout := time.Now().Add(5 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		if n, _ := msgStoreState(t, cs.Msgs); n == 0 {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Message should have expired")
	}

	// First slice should still exist although empty
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	numFiles := len(ms.files)
	firstFileSeq := ms.firstFSlSeq
	empty := false
	if ms.writeSlice != nil && ms.writeSlice.msgsCount == ms.writeSlice.rmCount {
		empty = true
	}
	firstWrite := ms.writeSlice.firstWrite
	ms.RUnlock()
	if !empty || numFiles != 1 || firstFileSeq != 1 {
		t.Fatalf("Expected slice to be empty, numFiles and firstFileSeq to be 1, got %v, %v and %v",
			empty, numFiles, firstFileSeq)
	}

	// Since slice time check uses ms.timeTick, ensure that we wait long enough.
	timeout = time.Now().Add(5 * time.Second)
	ok = false
	for time.Now().Before(timeout) {
		timeTick := atomic.LoadInt64(&ms.timeTick)

		if timeTick-firstWrite > int64(time.Second) {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Waited too long for timeTick to update")
	}

	// Send another message...
	storeMsg(t, cs, "foo", []byte("test"))

	timeout = time.Now().Add(5 * time.Second)
	ok = false
	for time.Now().Before(timeout) {
		if n, _ := msgStoreState(t, cs.Msgs); n == 1 {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Should have gotten a message")
	}

	// A new slice should have been created and the first one deleted.
	ms.RLock()
	numFiles = len(ms.files)
	firstFileSeq = ms.firstFSlSeq
	updatedwriteSlice := ms.writeSlice == ms.files[2]
	ms.RUnlock()
	if !updatedwriteSlice || numFiles != 1 || firstFileSeq != 2 {
		t.Fatalf("Expected current slice to be updated to second slice, numFiles to be 1, firstFileSeq to be 2, got %v, %v and %v",
			updatedwriteSlice, numFiles, firstFileSeq)
	}
}

func TestFSSubStoreVariousBufferSizes(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	sizes := []int{0, subBufMinShrinkSize - subBufMinShrinkSize/10, subBufMinShrinkSize, 3*subBufMinShrinkSize + subBufMinShrinkSize/2}
	for _, size := range sizes {

		// Create a store with buffer writer of the given size
		fs := createDefaultFileStore(t, BufferSize(size))
		defer fs.Close()

		cs := storeCreateChannel(t, fs, "foo")
		m := storeMsg(t, cs, "foo", []byte("hello"))

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
				ss.shrinkBuffer()
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
			ss.shrinkBuffer()
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
		cleanupDatastore(t, defaultDataStore)
	}
}

func TestFSMsgStoreVariousBufferSizes(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	sizes := []int{0, msgBufMinShrinkSize - msgBufMinShrinkSize/10, msgBufMinShrinkSize, 3*msgBufMinShrinkSize + msgBufMinShrinkSize/2}
	for _, size := range sizes {

		// Create a store with buffer writer of the given size
		fs := createDefaultFileStore(t, BufferSize(size))
		defer fs.Close()

		cs := storeCreateChannel(t, fs, "foo")
		storeMsg(t, cs, "foo", []byte("hello"))

		// Get FileMsgStore
		ms := cs.Msgs.(*FileMsgStore)

		// Cause a flush to empty the buffer
		ms.Flush()

		// Check that bw is not nil and writer points to the buffer writer
		ms.RLock()
		bw := ms.bw
		writer := ms.writer
		file := ms.writeSlice.file.handle
		bufSize := 0
		if ms.bw != nil {
			bufSize = ms.bw.buf.Available()
		}
		ms.RUnlock()
		if size == 0 {
			if bw != nil {
				t.Fatal("FileMsgStore's buffer writer should be nil")
			}
		} else if bw == nil {
			t.Fatal("FileMsgStore's buffer writer should not be nil")
		}
		if size == 0 {
			if writer != file {
				t.Fatal("FileMsgStore's writer should be set to file")
			}
		} else if writer != bw.buf {
			t.Fatal("FileMsgStore's writer should be set to the buffer writer")
		}
		initialSize := size
		if size > msgBufMinShrinkSize {
			initialSize = msgBufMinShrinkSize
		}
		if bufSize != initialSize {
			t.Fatalf("Incorrect initial size, should be %v, got %v", initialSize, bufSize)
		}

		// Fill up the buffer (meaningfull only when buffer is used)
		fillBuffer := func() {
			total := 0
			for i := 0; i < 1000; i++ {
				ms.RLock()
				before := ms.bw.buf.Buffered()
				ms.RUnlock()
				storeMsg(t, cs, "foo", []byte("hello"))
				ms.RLock()
				if ms.bw.buf.Buffered() > before {
					total += ms.bw.buf.Buffered() - before
				} else {
					total += ms.bw.buf.Buffered()
				}
				ms.RUnlock()
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
				storeMsg(t, cs, "foo", []byte("hello"))
			}
		}

		ms.RLock()
		bufSize = 0
		if size > 0 {
			bufSize = ms.bw.bufSize
		}
		ms.RUnlock()
		if size == 0 {
			if bufSize != 0 {
				t.Fatalf("BufferSize is 0, so ss.bufSize should be 0, got %v", bufSize)
			}
		} else if size < msgBufMinShrinkSize {
			// If size is smaller than min shrink size, the buffer should not have
			// increased in size
			if bufSize > msgBufMinShrinkSize {
				t.Fatalf("BufferSize=%v - ss.bw size should at or below %v, got %v", size, msgBufMinShrinkSize, bufSize)
			}
		} else {
			// We should have started at min size, and now size should have been increased.
			if bufSize < msgBufMinShrinkSize || bufSize > size {
				t.Fatalf("BufferSize=%v - ss.bw size should have increased but no more than %v, got %v", size, size, bufSize)
			}
		}

		// When buffer size is greater than min size, see if it shrinks
		if size > msgBufMinShrinkSize {
			// Invoke the timer callback manually (so we don't have to wait)
			// Call many times and make sure size never goes down too low.
			for i := 0; i < 14; i++ {
				ms.Lock()
				ms.bw.tryShrinkBuffer(ms.writeSlice.file.handle)
				ms.writer = ms.bw.buf
				ms.Unlock()
			}
			// Now check
			ms.RLock()
			bufSizeNow := ms.bw.bufSize
			ms.RUnlock()
			if bufSizeNow >= bufSize {
				t.Fatalf("Buffer size expected to decrease, got: %v", bufSizeNow)
			}
			if bufSizeNow < msgBufMinShrinkSize {
				t.Fatalf("Buffer should not go below %v, got %v", msgBufMinShrinkSize, bufSizeNow)
			}

			// Check that the request to shrink is canceled if more data arrive
			// First make buffer expand.
			fillBuffer()
			// Flush to empty it
			ms.Flush()
			// Invoke shrink
			ms.Lock()
			ms.bw.tryShrinkBuffer(ms.writeSlice.file.handle)
			ms.Unlock()
			// Check that request is set
			ms.RLock()
			shrinkReq := ms.bw.shrinkReq
			ms.RUnlock()
			if !shrinkReq {
				t.Fatal("Shrink request should be true")
			}
			// Cause buffer to expand again
			fillBuffer()
			// Check that request should have been canceled.
			ms.RLock()
			shrinkReq = ms.bw.shrinkReq
			ms.RUnlock()
			if shrinkReq {
				t.Fatal("Shrink request should be false")
			}
		}
		fs.Close()
		cleanupDatastore(t, defaultDataStore)
	}
}

func TestFSPerChannelLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testPerChannelLimits(t, fs)
}

func TestFSArchiveScript(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	tmpDir, err := ioutil.TempDir(".", "")
	if err != nil {
		t.Fatalf("Unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Unable to get current directory: %v", err)
	}
	scriptFile := ""
	content := ""
	if runtime.GOOS == "windows" {
		scriptFile = fmt.Sprintf("%s\\script_%v.bat", pwd, time.Now().UnixNano())
		content = fmt.Sprintf("mkdir %s\\%s\\%%1\nmove %%2 %s\\%s\\%%1\nmove %%3 %s\\%s\\%%1", pwd, tmpDir, pwd, tmpDir, pwd, tmpDir)
	} else {
		scriptFile = fmt.Sprintf("%s/script_%v.sh", pwd, time.Now().UnixNano())
		content = fmt.Sprintf("#!/bin/bash\nmkdir -p %s/%s/$1\nmv $2 $3 %s/%s/$1\n", pwd, tmpDir, pwd, tmpDir)
	}
	if err := ioutil.WriteFile(scriptFile, []byte(content), 0777); err != nil {
		t.Fatalf("Error creating script: %v", err)
	}
	defer os.Remove(scriptFile)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, scriptFile))
	defer fs.Close()
	limits := DefaultStoreLimits
	limits.MaxMsgs = 1
	fs.SetLimits(&limits)

	cs := storeCreateChannel(t, fs, "foo")
	// Store one message
	storeMsg(t, cs, "foo", []byte("msg1"))

	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	fileName := ms.files[1].file.name
	ms.RUnlock()

	// Store one more message. Should move to next slice and invoke script
	// for first slice.
	storeMsg(t, cs, "foo", []byte("msg2"))

	// Original file should not be present
	ok := false
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(fileName); s == nil || serr != nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}

	// File should have been moved to tmpDir by script
	ok = false
	bakFile := fmt.Sprintf("%s/foo/%s1%s%s", tmpDir, msgFilesPrefix, datSuffix, bakSuffix)
	timeout = time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(bakFile); s != nil && serr == nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File should have been moved to tmp dir")
	}
	// Close store
	fs.Close()
	// Cleanup datastore
	cleanupDatastore(t, defaultDataStore)

	// Create a script that will error out
	os.Remove(scriptFile)
	content = "xxx"
	if err := ioutil.WriteFile(scriptFile, []byte(content), 0777); err != nil {
		t.Fatalf("Error creating script: %v", err)
	}
	defer os.Remove(scriptFile)

	fs = createDefaultFileStore(t, SliceConfig(0, 0, 0, scriptFile))
	defer fs.Close()
	fs.SetLimits(&limits)

	cs = storeCreateChannel(t, fs, "foo")
	// Store one message
	storeMsg(t, cs, "foo", []byte("msg1"))

	// Store one more message. Should move to next slice and invoke script
	// for first slice.
	storeMsg(t, cs, "foo", []byte("msg2"))

	// Original file should not be present
	ok = false
	timeout = time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(fileName); s == nil || serr != nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}

	// Since script should fail, .bak file should still be in channel's directory
	ok = true
	timeout = time.Now().Add(time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(bakFile); s == nil || serr != nil {
			ok = false
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}
}

func TestFSNoSliceLimitAndNoChannelLimits(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	// No slice limit
	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, ""))
	defer fs.Close()

	// And no channel limit
	limits := StoreLimits{}
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting file limits: %v", err)
	}

	total := 1000
	msg := []byte("msg")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", msg)
	}

	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	numFiles := len(ms.files)
	firstFileSeq := ms.firstFSlSeq
	lastFileSeq := ms.lastFSlSeq
	ms.RUnlock()

	if numFiles != 1 || firstFileSeq != 1 || lastFileSeq != 1 {
		t.Fatalf("Expected numFiles, firstFileSeq and lastFileSeq to be all 1, got %v, %v and %v",
			numFiles, firstFileSeq, lastFileSeq)
	}
}

func TestFSMsgRemovedWhileBuffered(t *testing.T) {
	// Test is irrelevant if no buffering used
	if disableBufferWriters {
		t.SkipNow()
	}
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limits := DefaultStoreLimits
	limits.MaxMsgs = 10
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting limits: %v", &limits)
	}

	total := 1000
	msg := []byte("msg")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", msg)
	}

	fs.Close()

	fs, state, err := newFileStore(t, defaultDataStore, &limits)
	if err != nil {
		t.Fatalf("Unexpected error opening store: %v", err)
	}
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected to recover a state")
	}
}
