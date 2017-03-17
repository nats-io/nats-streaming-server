// Copyright 2016 Apcera Inc. All rights reserved.

package server

import (
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-streaming-server/stores"
)

const (
	mapStructErr = "map/struct"
	wrongTypeErr = "value is expected to be"
	wrongTimeErr = "time: "
)

func TestParseConfig(t *testing.T) {
	opts := Options{}
	if err := ProcessConfigFile("../test/configs/test_parse.conf", &opts); err != nil {
		t.Fatalf("Unexpected error on config file parsing: %v", err)
	}
	// This test depends on the expected values in the config file.
	// Any modification there should be reflected here.
	if opts.ID != "me" {
		t.Fatalf("Expected ID to be %q, got %q", "me", opts.ID)
	}
	if opts.DiscoverPrefix != "discover" {
		t.Fatalf("Expected DiscoverPrefix to be %q, got %q", "discover", opts.DiscoverPrefix)
	}
	if opts.StoreType != stores.TypeFile {
		t.Fatalf("Expected StoreType to be %q, got %q", stores.TypeFile, opts.StoreType)
	}
	if opts.FilestoreDir != "/path/to/datastore" {
		t.Fatalf("Expected FilestoreDir to be %q, got %q", "file", opts.FilestoreDir)
	}
	if !opts.Debug {
		t.Fatalf("Expected Debug to be true, got false")
	}
	if !opts.Trace {
		t.Fatalf("Expected Trace to be true, got false")
	}
	if !opts.Secure {
		t.Fatalf("Expected Secure to be true, got false")
	}
	if opts.NATSServerURL != "nats://localhost:4222" {
		t.Fatalf("Expected NATSServerURL to be %q, got %q", "nats://localhost:4222", opts.NATSServerURL)
	}
	if opts.ClientCert != "/path/to/client/cert_file" {
		t.Fatalf("Expected ClientCert to be %q, got %q", "/path/to/client/cert_file", opts.ClientCert)
	}
	if opts.ClientKey != "/path/to/client/key_file" {
		t.Fatalf("Expected ClientKey to be %q, got %q", "/path/to/client/key_file", opts.ClientKey)
	}
	if opts.ClientCA != "/path/to/client/ca_file" {
		t.Fatalf("Expected ClientCA to be %q, got %q", "/path/to/client/ca_file", opts.ClientCA)
	}
	if !opts.FileStoreOpts.CompactEnabled {
		t.Fatalf("Expected CompactEnabled to be true, got false")
	}
	if !opts.FileStoreOpts.DoCRC {
		t.Fatalf("Expected DoCRC to be true, got false")
	}
	if !opts.FileStoreOpts.DoSync {
		t.Fatalf("Expected DoSync to be true, got false")
	}
	if opts.FileStoreOpts.CompactFragmentation != 1 {
		t.Fatalf("Expected CompactFragmentation to be 1, got %v", opts.FileStoreOpts.CompactFragmentation)
	}
	if opts.FileStoreOpts.CompactInterval != 2 {
		t.Fatalf("Expected CompactInterval to be 1, got %v", opts.FileStoreOpts.CompactInterval)
	}
	if opts.FileStoreOpts.CompactMinFileSize != 3 {
		t.Fatalf("Expected CompactMinFileSize to be 3, got %v", opts.FileStoreOpts.CompactMinFileSize)
	}
	if opts.FileStoreOpts.BufferSize != 4 {
		t.Fatalf("Expected BufferSize to be 4, got %v", opts.FileStoreOpts.BufferSize)
	}
	if opts.FileStoreOpts.CRCPolynomial != 5 {
		t.Fatalf("Expected CRCPolynomial to be 5, got %v", opts.FileStoreOpts.CRCPolynomial)
	}
	if opts.FileStoreOpts.SliceMaxMsgs != 6 {
		t.Fatalf("Expected SliceMaxMsgs to be 6, got %v", opts.FileStoreOpts.SliceMaxMsgs)
	}
	if opts.FileStoreOpts.SliceMaxBytes != 7 {
		t.Fatalf("Expected SliceMaxBytes to be 7, got %v", opts.FileStoreOpts.SliceMaxBytes)
	}
	if opts.FileStoreOpts.SliceMaxAge != 8*time.Second {
		t.Fatalf("Expected SliceMaxMsgs to be 8s, got %v", opts.FileStoreOpts.SliceMaxAge)
	}
	if opts.FileStoreOpts.SliceArchiveScript != "myArchiveScript" {
		t.Fatalf("Expected SliceArchiveScript to be myArchiveScript, got %v", opts.FileStoreOpts.SliceArchiveScript)
	}
	if opts.FileStoreOpts.FileDescriptorsLimit != 8 {
		t.Fatalf("Expected FileDescriptorsLimit to be 8, got %v", opts.FileStoreOpts.FileDescriptorsLimit)
	}
	if opts.MaxChannels != 11 {
		t.Fatalf("Expected MaxChannels to be 11, got %v", opts.MaxChannels)
	}
	if opts.MaxMsgs != 12 {
		t.Fatalf("Expected MaxMsgs to be 12, got %v", opts.MaxMsgs)
	}
	if opts.MaxBytes != 13 {
		t.Fatalf("Expected MaxBytes to be 13, got %v", opts.MaxBytes)
	}
	if opts.MaxAge != 14*time.Second {
		t.Fatalf("Expected MaxAge to be 14, got %v", opts.MaxAge)
	}
	if opts.MaxSubscriptions != 15 {
		t.Fatalf("Expected MaxSubscriptions to be 15, got %v", opts.MaxSubscriptions)
	}
	if len(opts.PerChannel) != 2 {
		t.Fatalf("Expected PerChannel map to have 2 elements, got %v", len(opts.PerChannel))
	}
	cl, ok := opts.PerChannel["foo"]
	if !ok {
		t.Fatal("Expected channel foo to be found")
	}
	if cl.MaxMsgs != 1 {
		t.Fatalf("Expected MaxMsgs to be 1, got %v", cl.MaxMsgs)
	}
	if cl.MaxBytes != 2 {
		t.Fatalf("Expected MaxBytes to be 2, got %v", cl.MaxBytes)
	}
	if cl.MaxAge != 3*time.Second {
		t.Fatalf("Expected MaxAge to be 3, got %v", cl.MaxAge)
	}
	if cl.MaxSubscriptions != 4 {
		t.Fatalf("Expected MaxSubscriptions to be 4, got %v", cl.MaxSubscriptions)
	}
	cl, ok = opts.PerChannel["bar"]
	if !ok {
		t.Fatal("Expected channel bar to be found")
	}
	if cl.MaxMsgs != 5 {
		t.Fatalf("Expected MaxMsgs to be 5, got %v", cl.MaxMsgs)
	}
	if cl.MaxBytes != 6 {
		t.Fatalf("Expected MaxBytes to be 6, got %v", cl.MaxBytes)
	}
	if cl.MaxAge != 7*time.Second {
		t.Fatalf("Expected MaxAge to be 7, got %v", cl.MaxAge)
	}
	if cl.MaxSubscriptions != 8 {
		t.Fatalf("Expected MaxSubscriptions to be 8, got %v", cl.MaxSubscriptions)
	}
	if opts.ClientHBInterval != 10*time.Second {
		t.Fatalf("Expected ClientHBInterval to be 10s, got %v", opts.ClientHBInterval)
	}
	if opts.ClientHBTimeout != time.Second {
		t.Fatalf("Expected ClientHBTimeout to be 1s, got %v", opts.ClientHBTimeout)
	}
	if opts.ClientHBFailCount != 2 {
		t.Fatalf("Expected ClientHBFailCount to be 2, got %v", opts.ClientHBFailCount)
	}
	if opts.AckSubsPoolSize != 3 {
		t.Fatalf("Expected AckSubscriptions to be 3, got %v", opts.AckSubsPoolSize)
	}
	if opts.FTGroupName != "ft" {
		t.Fatalf("Expected FTGroupName to be %q, got %q", "ft", opts.FTGroupName)
	}
}

func TestParsePermError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}
	tmpDir, err := ioutil.TempDir("", "streaming")
	if err != nil {
		t.Fatalf("Could not create tmp dir: %v", err)
	}
	file, err := ioutil.TempFile(tmpDir, "config.conf")
	if err != nil {
		t.Fatalf("Could not create tmp file: %v", err)
	}
	os.Chmod(tmpDir, 0400)

	defer file.Close()
	defer os.RemoveAll(tmpDir)
	defer os.Chmod(tmpDir, 0770)

	if _, err := file.Write([]byte("id=me")); err != nil {
		t.Fatalf("Error writing to file: %v", err)
	}
	opts := Options{}
	if err := ProcessConfigFile(file.Name(), &opts); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

func TestParseParserError(t *testing.T) {
	confFile := "wrong_config.conf"
	if err := ioutil.WriteFile(confFile, []byte("x=."), 0660); err != nil {
		t.Fatalf("Unexpected error creating conf file: %v", err)
	}
	defer os.Remove(confFile)
	opts := Options{}
	if err := ProcessConfigFile(confFile, &opts); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

func TestParseStoreType(t *testing.T) {
	confFile := "wrong_config.conf"
	if err := ioutil.WriteFile(confFile, []byte("store=memory"), 0660); err != nil {
		t.Fatalf("Unexpected error creating conf file: %v", err)
	}
	defer os.Remove(confFile)
	opts := Options{}
	if err := ProcessConfigFile(confFile, &opts); err != nil {
		t.Fatalf("Unexpected failure: %v", err)
	}
	if opts.StoreType != stores.TypeMemory {
		t.Fatalf("Expected store type to be %v, got %v", stores.TypeMemory, opts.StoreType)
	}
	os.Remove(confFile)

	if err := ioutil.WriteFile(confFile, []byte("store=xyz"), 0660); err != nil {
		t.Fatalf("Unexpected error creating conf file: %v", err)
	}
	defer os.Remove(confFile)
	opts = Options{}
	if err := ProcessConfigFile(confFile, &opts); err == nil {
		t.Fatal("Expected failure due to unkown store type, got none")
	}
}

func TestParseMapStruct(t *testing.T) {
	expectFailureFor(t, "store_limits: xxx", mapStructErr)
	expectFailureFor(t, "store_limits: {\nchannels: xxx\n}", mapStructErr)
	expectFailureFor(t, "store_limits: {\nchannels: {\n\"foo\": xxx\n}\n}", mapStructErr)
	expectFailureFor(t, "tls: xxx", mapStructErr)
	expectFailureFor(t, "file: xxx", mapStructErr)
}

func TestParseWrongTypes(t *testing.T) {
	expectFailureFor(t, "id: 123", wrongTypeErr)
	expectFailureFor(t, "discover_prefix: 123", wrongTypeErr)
	expectFailureFor(t, "store: 123", wrongTypeErr)
	expectFailureFor(t, "dir: 123", wrongTypeErr)
	expectFailureFor(t, "sd: 123", wrongTypeErr)
	expectFailureFor(t, "sv: 123", wrongTypeErr)
	expectFailureFor(t, "ns: 123", wrongTypeErr)
	expectFailureFor(t, "secure: 123", wrongTypeErr)
	expectFailureFor(t, "hb_interval: 123", wrongTypeErr)
	expectFailureFor(t, "hb_interval: \"foo\"", wrongTimeErr)
	expectFailureFor(t, "hb_timeout: 123", wrongTypeErr)
	expectFailureFor(t, "hb_timeout: \"foo\"", wrongTimeErr)
	expectFailureFor(t, "hb_fail_count: false", wrongTypeErr)
	expectFailureFor(t, "ack_subs_pool_size: false", wrongTypeErr)
	expectFailureFor(t, "ft_group: 123", wrongTypeErr)
	expectFailureFor(t, "store_limits:{max_channels:false}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{max_msgs:false}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{max_bytes:false}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{max_age:false}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{max_age:\"foo\"}", wrongTimeErr)
	expectFailureFor(t, "store_limits:{max_subs:false}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo\":{max_msgs:false}}}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo\":{max_bytes:false}}}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo\":{max_age:\"1h:0m\"}}}", wrongTimeErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo\":{max_age:false}}}", wrongTypeErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo\":{max_subs:false}}}", wrongTypeErr)
	expectFailureFor(t, "tls:{client_cert:123}", wrongTypeErr)
	expectFailureFor(t, "tls:{client_key:123}", wrongTypeErr)
	expectFailureFor(t, "tls:{client_ca:123}", wrongTypeErr)
	expectFailureFor(t, "file:{compact:123}", wrongTypeErr)
	expectFailureFor(t, "file:{compact_frag:false}", wrongTypeErr)
	expectFailureFor(t, "file:{compact_interval:false}", wrongTypeErr)
	expectFailureFor(t, "file:{compact_min_size:false}", wrongTypeErr)
	expectFailureFor(t, "file:{buffer_size:false}", wrongTypeErr)
	expectFailureFor(t, "file:{crc:123}", wrongTypeErr)
	expectFailureFor(t, "file:{crc_poly:false}", wrongTypeErr)
	expectFailureFor(t, "file:{sync:123}", wrongTypeErr)
	expectFailureFor(t, "file:{slice_max_msgs:true}", wrongTypeErr)
	expectFailureFor(t, "file:{slice_max_bytes:false}", wrongTypeErr)
	expectFailureFor(t, "file:{slice_max_age:123}", wrongTypeErr)
	expectFailureFor(t, "file:{slice_max_age:\"1h:0m\"}", wrongTimeErr)
	expectFailureFor(t, "file:{slice_archive_script:123}", wrongTypeErr)
	expectFailureFor(t, "file:{fds_limit:false}", wrongTypeErr)
}

func expectFailureFor(t *testing.T, content, errorMatch string) {
	confFile := "wrong_config.conf"
	if err := ioutil.WriteFile(confFile, []byte(content), 0660); err != nil {
		t.Fatalf("Unexpected error creating conf file: %v", err)
	}
	defer os.Remove(confFile)
	opts := Options{}
	if err := ProcessConfigFile(confFile, &opts); err == nil {
		t.Fatalf("For content: %q, expected failure, did not get one", content)
	} else if !strings.Contains(err.Error(), errorMatch) {
		t.Fatalf("Possible unexpected error: %v", err)
	}
}
