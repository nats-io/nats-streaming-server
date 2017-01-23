// Copyright 2016-2017 Apcera Inc. All rights reserved.

package util

import (
	"fmt"
	"os"
	"testing"
)

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
	newBuf = EnsureBufBigEnough(nil, 5)
	if len(newBuf) != 5 {
		t.Fatalf("Buffer should be exactly 5, it is: %v", len(newBuf))
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

func TestCloseFile(t *testing.T) {
	fileName := "test.dat"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer os.Remove(fileName)

	err = nil
	cferr := CloseFile(err, file)
	if cferr != nil {
		t.Fatalf("Unexpected error: %v", cferr)
	}

	err = fmt.Errorf("Previous error")
	cferr = CloseFile(err, file)
	if cferr != err {
		t.Fatalf("Expected original error to be untouched")
	}

	err = nil
	cferr = CloseFile(err, file)
	if cferr == err {
		t.Fatalf("Expected returned error to be different")
	}
}
