// Copyright 2016 Apcera Inc. All rights reserved.

package util

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// So that we can pass tests and benchmarks...
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers:
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if ok == false {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}

	t.Fatalf("%s", strings.Join(lines, "\n"))
}

func TestEnsureBufBigEnough(t *testing.T) {
	buf := make([]byte, 3)
	newBuf := EnsureBufBigEnough(buf, 2)
	if len(newBuf) != len(buf) {
		t.Fatal("EnsureBufBigEnough should not have allocated a new buffer")
	}
	newBuf = EnsureBufBigEnough(buf, 10)
	if len(newBuf) <= 10 {
		t.Fatalf("Buffer should be at least 10, it is: %v", len(newBuf))
	}
}

func TestWriteInt(t *testing.T) {
	fileName := "test.dat"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer os.Remove(fileName)
	defer file.Close()
	if err := WriteInt(file, 123); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestReadInt(t *testing.T) {
	fileName := "test.dat"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer os.Remove(fileName)
	defer file.Close()

	if _, err := ReadInt(file); err == nil {
		t.Fatal("Expected an error")
	}
	if err := WriteInt(file, 123); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if v, err := ReadInt(file); err != nil || v != 123 {
		t.Fatalf("Expected to read 123, got: %v (err=%v)", v, err)
	}
}

func benchCRCWithPoly(b *testing.B, arraySize int, poly uint32) {
	b.StopTimer()
	array := make([]byte, arraySize)
	for i := 0; i < arraySize; i++ {
		array[i] = byte(rand.Intn(255))
	}
	table := crc32.MakeTable(poly)
	crc := crc32.Checksum(array, table)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if c := crc32.Checksum(array, table); c != crc {
			stackFatalf(b, "Expected checksum %v, got %v", crc, c)
		}
	}
	b.StopTimer()
}

func BenchmarkCRCIEEE_16(b *testing.B) {
	benchCRCWithPoly(b, 16, crc32.IEEE)
}

func BenchmarkCRCIEEE_32(b *testing.B) {
	benchCRCWithPoly(b, 32, crc32.IEEE)
}

func BenchmarkCRCIEEE_64(b *testing.B) {
	benchCRCWithPoly(b, 64, crc32.IEEE)
}

func BenchmarkCRCIEEE_128(b *testing.B) {
	benchCRCWithPoly(b, 128, crc32.IEEE)
}

func BenchmarkCRCIEEE_256(b *testing.B) {
	benchCRCWithPoly(b, 256, crc32.IEEE)
}

func BenchmarkCRCIEEE_512(b *testing.B) {
	benchCRCWithPoly(b, 512, crc32.IEEE)
}

func BenchmarkCRCIEEE_1024(b *testing.B) {
	benchCRCWithPoly(b, 1024, crc32.IEEE)
}

func BenchmarkCRCIEEE_2048(b *testing.B) {
	benchCRCWithPoly(b, 2048, crc32.IEEE)
}

func BenchmarkCRCIEEE_4096(b *testing.B) {
	benchCRCWithPoly(b, 4096, crc32.IEEE)
}

func BenchmarkCRCIEEE_8192(b *testing.B) {
	benchCRCWithPoly(b, 8192, crc32.IEEE)
}

func BenchmarkCRCIEEE_100K(b *testing.B) {
	benchCRCWithPoly(b, 100*1024, crc32.IEEE)
}

func BenchmarkCRCIEEE_1M(b *testing.B) {
	benchCRCWithPoly(b, 1024*1024, crc32.IEEE)
}

func BenchmarkCRCCastagnoli_16(b *testing.B) {
	benchCRCWithPoly(b, 16, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_32(b *testing.B) {
	benchCRCWithPoly(b, 32, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_64(b *testing.B) {
	benchCRCWithPoly(b, 64, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_128(b *testing.B) {
	benchCRCWithPoly(b, 128, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_256(b *testing.B) {
	benchCRCWithPoly(b, 256, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_512(b *testing.B) {
	benchCRCWithPoly(b, 512, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_1024(b *testing.B) {
	benchCRCWithPoly(b, 1024, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_2048(b *testing.B) {
	benchCRCWithPoly(b, 2048, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_4096(b *testing.B) {
	benchCRCWithPoly(b, 4096, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_8192(b *testing.B) {
	benchCRCWithPoly(b, 8192, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_100K(b *testing.B) {
	benchCRCWithPoly(b, 100*1024, crc32.Castagnoli)
}

func BenchmarkCRCCastagnoli_1M(b *testing.B) {
	benchCRCWithPoly(b, 1024*1024, crc32.Castagnoli)
}

/*
 * These are comparatively too slow...
func BenchmarkCRCKoopman_16(b *testing.B) {
	benchCRCWithPoly(b, 16, crc32.Koopman)
}
(...)
func BenchmarkCRCKoopman_4096(b *testing.B) {
	benchCRCWithPoly(b, 4096, crc32.Koopman)
}
*/

func TestVerifyCRC(t *testing.T) {
	array := []byte("this is a test")
	crc := crc32.Checksum(array, crc32.IEEETable)
	if err := VerifyCRC(array, crc32.IEEETable, crc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	array[2] = 'x'
	if err := VerifyCRC(array, crc32.IEEETable, crc); err == nil {
		t.Fatal("Expected verification to fail")
	}
	otherTable := crc32.MakeTable(crc32.Castagnoli)
	crc = crc32.Checksum(array, otherTable)
	if err := VerifyCRC(array, otherTable, crc); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	array[2] = 'y'
	if err := VerifyCRC(array, otherTable, crc); err == nil {
		t.Fatal("Expected verification to fail")
	}
	// Use wrong table should fail
	if err := VerifyCRC(array, crc32.IEEETable, crc); err == nil {
		t.Fatal("Expected verification to fail")
	}
}

func TestGetRecord(t *testing.T) {
	record := []byte("this is a test")
	crc := crc32.Checksum(record, crc32.IEEETable)
	array := make([]byte, CrcSize+len(record))
	ByteOrder.PutUint32(array, crc)
	copy(array[CrcSize:len(array)], record)
	// Get the record out of the array, doing CRC validation
	r, err := GetRecord(array, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(record, r) {
		t.Fatalf("Expected %q, got %q", record, r)
	}
	// Modify something in the array, crc should fail
	array[CrcSize+4] = 'x'
	r, err = GetRecord(array, true)
	if err == nil {
		t.Fatal("Expected crc verification to fail")
	}
	// Ask not to do the crc validation
	r, err = GetRecord(array, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// The returned record should be corrupted
	if !reflect.DeepEqual(array[CrcSize:len(array)], r) {
		t.Fatalf("Expected %q, got %q", string(record), string(r))
	}
}

type MemWriter struct {
	buf  []byte
	used int
}

func (mw *MemWriter) Write(b []byte) (int, error) {
	if mw.used+len(b) > cap(mw.buf) {
		return 0, fmt.Errorf("insufficient buffer, cap=%v buffer size=%v", cap(mw.buf), len(b))
	}
	copy(mw.buf[mw.used:mw.used+len(b)], b)
	mw.used += len(b)
	return len(mw.buf), nil
}

func TestWriteRecord(t *testing.T) {
	rec := []byte("this is a test")
	// Try with too small buffer for various tokens
	w := &MemWriter{buf: make([]byte, 3)}
	if err := WriteRecord(w, rec); err == nil {
		t.Fatal("Expected an error")
	}
	w = &MemWriter{buf: make([]byte, 4+CrcSize-1)}
	if err := WriteRecord(w, rec); err == nil {
		t.Fatal("Expected an error")
	}
	w = &MemWriter{buf: make([]byte, 4+CrcSize+len(rec)-1)}
	if err := WriteRecord(w, rec); err == nil {
		t.Fatal("Expected an error")
	}
	// No error now
	w = &MemWriter{buf: make([]byte, 1024)}
	if err := WriteRecord(w, rec); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	readBackRecord, err := GetRecord(w.buf[4:w.used], true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(readBackRecord, rec) {
		t.Fatalf("Expected %q, got %q", string(readBackRecord), string(rec))
	}
}
