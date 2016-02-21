package stan

import (
	"testing"

	"github.com/nats-io/nats"
)

func TestGUIDInit(t *testing.T) {
	if globalGUID == nil {
		t.Fatalf("Expected g to be non-nil\n")
	}
	var zb [preLen]byte
	if globalGUID.pre == zb {
		t.Fatalf("Expected pre to be initialized\n")
	}
	if globalGUID.seq == 0 {
		t.Fatalf("Expected seq to be non-zero\n")
	}
}

func TestGUIDRollover(t *testing.T) {
	globalGUID.Lock()
	globalGUID.seq = maxSeq
	oldPre := globalGUID.pre
	globalGUID.Unlock()
	newGUID()
	globalGUID.Lock()
	defer globalGUID.Unlock()
	if globalGUID.pre == oldPre {
		t.Fatalf("Expected new pre, got the old one\n")
	}
}

func TestGUIDLen(t *testing.T) {
	guid := newGUID()
	if len(guid) != totalLen {
		t.Fatalf("Expected len of %d, got %d\n", totalLen, len(guid))
	}
}

func TestBasicUniqueness(t *testing.T) {
	n := 10000
	m := make(map[string]bool)
	for i := 0; i < n; i++ {
		g := newGUID()
		if m[g] {
			t.Fatalf("Duplicate GUID found: %v\n", g)
		}
	}
}

func BenchmarkGUIDSpeed(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		newGUID()
	}
}

// FIXME(dlc) Replace with GUID stuff from here.
func BenchmarkInboxSpeed(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		nats.NewInbox()
	}
}
