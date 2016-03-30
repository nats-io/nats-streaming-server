package stores

import (
	"os"
	"testing"
)

const (
	defaultDataStore = "../data"
)

func cleanupDatastore(dir string) {
	os.RemoveAll(dir)
}

func TestFSBasicCreate(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	fs, err := NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	testBasicCreate(t, fs, "FILESTORE")
}

func TestFSNothingRecoveredOnFreshStart(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	fs, err := NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	testNothingRecoveredOnFreshStart(t, fs)
}

func TestFSNewChannel(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	fs, err := NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	testNewChannel(t, fs)
}

func TestFSCloseIdempotent(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	fs, err := NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	testCloseIdempotent(t, fs)
}

func TestFSBasicMsgStore(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	fs, err := NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	testBasicMsgStore(t, fs)
}

func TestFSBasicRecovery(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	fooRecovered := false
	barRecovered := false

	fs, err := NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	if fs.LookupChannel("foo") != nil {
		fooRecovered = true
	}
	if fs.LookupChannel("bar") != nil {
		barRecovered = true
	}

	// Nothing should be recovered
	if fooRecovered || barRecovered {
		t.Fatalf("Unexpected recovery: foo=%v bar=%v", fooRecovered, barRecovered)
	}

	storeMsg(t, fs, "foo", []byte("foomsg"))
	storeMsg(t, fs, "bar", []byte("barmsg"))

	fs.Close()

	fs, err = NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatalf("Expected channel foo to exist")
	}
	m := cs.Msgs.FirstMsg()
	if m == nil || string(m.Data) != "foomsg" {
		t.Fatalf("Unexpected message for foo channel: %v", m)
	}

	cs = fs.LookupChannel("bar")
	if cs == nil {
		t.Fatalf("Expected channel bar to exist")
	}
	m = cs.Msgs.FirstMsg()
	if m == nil || string(m.Data) != "barmsg" {
		t.Fatalf("Unexpected message for bar channel: %v", m)
	}

	cs = fs.LookupChannel("baz")
	if cs != nil {
		t.Fatal("Expected to get nil channel for baz, got something instead")
	}
}

func TestFSMsgsState(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	fs, err := NewFileStore(defaultDataStore, DefaultChannelLimits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	testMsgsState(t, fs)
}

func TestFSLimits(t *testing.T) {
	cleanupDatastore(defaultDataStore)
	defer cleanupDatastore(defaultDataStore)

	limitCount := 100

	limits := DefaultChannelLimits
	limits.MaxNumMsgs = limitCount

	fs, err := NewFileStore(defaultDataStore, limits)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	testLimits(t, fs, limitCount)
}
