// Copyright 2016-2017 Apcera Inc. All rights reserved.

package server

import (
	"bytes"
	"flag"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	natsd "github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats-streaming-server/stores"
)

const (
	mapStructErr = "map/struct"
	wrongTypeErr = "value is expected to be"
	wrongTimeErr = "time: "
	wrongChanErr = "invalid channel name"
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
	if opts.FileStoreOpts.ParallelRecovery != 9 {
		t.Fatalf("Expected ParallelRecovery to be 9, got %v", opts.FileStoreOpts.ParallelRecovery)
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
	if !opts.Partitioning {
		t.Fatalf("Expected Partitioning to be true, got false")
	}
	if opts.SQLStoreOpts.Driver != "mysql" {
		t.Fatalf("Expected SQL Driver to be %q, got %q", "mysql", opts.SQLStoreOpts.Driver)
	}
	if opts.SQLStoreOpts.Source != "ivan:pwd@/nss_db" {
		t.Fatalf("Expected SQL Source to be %q, got %q", "ivan:pwd@/nss_db", opts.SQLStoreOpts.Source)
	}
	if !opts.SQLStoreOpts.NoCaching {
		t.Fatal("Expected SQL NoCaching to be true, got false")
	}
	if opts.SQLStoreOpts.MaxOpenConns != 5 {
		t.Fatalf("Expected SQL MaxOpenConns to be 5, got %v", opts.SQLStoreOpts.MaxOpenConns)
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
		t.Fatal("Expected failure due to unknown store type, got none")
	}
	os.Remove(confFile)

	goodStores := []string{
		stores.TypeMemory,
		stores.TypeFile,
		stores.TypeSQL,
	}
	for _, gs := range goodStores {
		if err := ioutil.WriteFile(confFile, []byte("store="+gs), 0660); err != nil {
			t.Fatalf("Unexpected error creating conf file: %v", err)
		}
		defer os.Remove(confFile)
		opts = Options{}
		if err := ProcessConfigFile(confFile, &opts); err != nil {
			t.Fatalf("Error processing config file: %v", err)
		}
		os.Remove(confFile)
		if opts.StoreType != gs {
			t.Fatalf("Expected store type to be %q, got %q", gs, opts.StoreType)
		}
	}
}

func TestParsePerChannelLimitsSetToZero(t *testing.T) {
	confFile := "config.conf"
	defer os.Remove(confFile)
	if err := ioutil.WriteFile(confFile,
		[]byte("store_limits: {channels: {foo: {max_msgs: 0, max_bytes: 0, max_age: \"0\", max_subs: 0}}}"), 0660); err != nil {
		t.Fatalf("Unexpected error creating conf file: %v", err)
	}
	opts := Options{}
	if err := ProcessConfigFile(confFile, &opts); err != nil {
		t.Fatalf("Unexpected failure: %v", err)
	}
	cl := opts.StoreLimits.PerChannel["foo"]
	if cl == nil {
		t.Fatal("PerChannel foo should exist")
	}
	// The config should set all the limits to -1 since they are
	// set to 0 (unlimited) in the config file.
	expected := stores.ChannelLimits{}
	expected.MaxMsgs = -1
	expected.MaxBytes = -1
	expected.MaxAge = -1
	expected.MaxSubscriptions = -1
	if !reflect.DeepEqual(*cl, expected) {
		t.Fatalf("Expected channel limits for foo to be %v, got %v", expected, *cl)
	}
}

func TestParseMapStruct(t *testing.T) {
	expectFailureFor(t, "streaming: xxx", mapStructErr)
	expectFailureFor(t, "store_limits: xxx", mapStructErr)
	expectFailureFor(t, "store_limits: {\nchannels: xxx\n}", mapStructErr)
	expectFailureFor(t, "store_limits: {\nchannels: {\n\"foo\": xxx\n}\n}", mapStructErr)
	expectFailureFor(t, "tls: xxx", mapStructErr)
	expectFailureFor(t, "file: xxx", mapStructErr)
	expectFailureFor(t, "sql: xxx", mapStructErr)
}

func TestParseWrongTypes(t *testing.T) {
	expectFailureFor(t, "streaming:{id:123}", wrongTypeErr)
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
	expectFailureFor(t, "partitioning: 123", wrongTypeErr)
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
	expectFailureFor(t, "store_limits:{channels:{\"foo.*bar\":{}}}", wrongChanErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo.>.>\":{}}}", wrongChanErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo..bar\":{}}}", wrongChanErr)
	expectFailureFor(t, "store_limits:{channels:{\"foo/bar\":{}}}", wrongChanErr)
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
	expectFailureFor(t, "file:{parallel_recovery:false}", wrongTypeErr)
	expectFailureFor(t, "sql:{driver:false}", wrongTypeErr)
	expectFailureFor(t, "sql:{source:false}", wrongTypeErr)
	expectFailureFor(t, "sql:{no_caching:123}", wrongTypeErr)
	expectFailureFor(t, "sql:{max_open_conns:false}", wrongTypeErr)
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

func TestParseConfigureOptions(t *testing.T) {
	// We are not testing some of the flags that are handled directly by NATS.
	// Provide a no-op print version/help/help tls function.
	noPrint := func() {}
	// Helper function that expect parsing with given args to not produce an error.
	mustNotFail := func(args []string) (*Options, *natsd.Options) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		sopts, nopts, err := ConfigureOptions(fs, args, noPrint, noPrint, noPrint)
		if err != nil {
			stackFatalf(t, "Error on configure: %v", err)
		}
		return sopts, nopts
	}

	// Helper function that expect configuration to fail.
	expectToFail := func(args []string, errContent ...string) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		// Silence the flagSet so that on failure nothing is printed.
		// (flag.FlagSet internally would print error message about unknown flags, etc..)
		silenceOutput := &bytes.Buffer{}
		fs.SetOutput(silenceOutput)
		sopts, nopts, err := ConfigureOptions(fs, args, noPrint, noPrint, noPrint)
		if sopts != nil || nopts != nil || err == nil {
			stackFatalf(t, "Expected no option and an error, got sopts=%v and nopts=%v and err=%v", sopts, nopts, err)
		}
		for _, testErr := range errContent {
			if strings.Contains(err.Error(), testErr) {
				// We got the error we wanted.
				return
			}
		}
		stackFatalf(t, "Expected errors containing any of those %v, got %v", errContent, err)
	}

	// Basic test with cluster id
	sopts, _ := mustNotFail([]string{"-cid", "me"})
	if sopts.ID != "me" {
		t.Fatalf("Expected cid to be me, got %v", sopts.ID)
	}

	// Should fail because flag is not defined
	expectToFail([]string{"-xxx", "foo"}, "flag")

	// Should fail because of config files missing
	expectToFail([]string{"-sc", "xxx.conf", "-c", "../test/configs/test_parse.conf"}, "file")
	expectToFail([]string{"-sc", "../test/configs/test_parse.conf", "-c", "xxx.conf"}, "file")
	expectToFail([]string{"-sc", "xxx.conf"}, "file")
	expectToFail([]string{"-c", "xxx.conf"}, "file")

	// The config set both debug and trace to true
	sopts, _ = mustNotFail([]string{"-sc", "../test/configs/test_parse.conf"})
	if !sopts.Debug || !sopts.Trace {
		t.Fatal("Debug and Trace should have been set to true")
	}
	// The config set both debug and trace to true, override with -SDV=false
	sopts, _ = mustNotFail([]string{"-sc", "../test/configs/test_parse.conf", "-SDV=false"})
	if sopts.Debug || sopts.Trace {
		t.Fatal("Debug and Trace should have been set to false")
	}

	// Test bytes values
	sopts, _ = mustNotFail([]string{"-max_bytes", "100KB", "-mb", "100KB", "-file_compact_min_size", "200KB", "-file_buffer_size", "300KB"})
	if sopts.MaxBytes != 100*1024 {
		t.Fatalf("Expected max_bytes to be 100KB, got %v", sopts.MaxBytes)
	}
	if sopts.FileStoreOpts.CompactMinFileSize != 200*1024 {
		t.Fatalf("Expected file_compact_min_size to be 200KB, got %v", sopts.FileStoreOpts.CompactMinFileSize)
	}
	if sopts.FileStoreOpts.BufferSize != 300*1024 {
		t.Fatalf("Expected file_buffer_size to be 300KB, got %v", sopts.FileStoreOpts.BufferSize)
	}

	// Failures with bytes
	expectToFail([]string{"-max_bytes", "12abc"}, "error")
	expectToFail([]string{"-max_bytes", "x1x"}, "size")
	expectToFail([]string{"-max_bytes", "100a", "-mb", "100a", "-file_compact_min_size", "200a", "-file_buffer_size", "300a"}, "error")

	sconf := "s.conf"
	nconf := "n.conf"
	defer os.Remove(sconf)
	defer os.Remove(nconf)

	// This test will first use both streaming and nats configuration
	// files, each having configuration elements for the other module
	// that should be ignored since they will be processed individually.
	scontent := []byte(`
		port: 4223
		streaming: {
			cluster_id: my_cluster
		}`)
	if err := ioutil.WriteFile(sconf, scontent, 0660); err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	ncontent := []byte(`
		port: 5223
		streaming: {
			cluster_id: my_cluster_2
		}`)
	if err := ioutil.WriteFile(nconf, ncontent, 0660); err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	sopts, nopts := mustNotFail([]string{"-sc", sconf, "-c", nconf})
	// Check that streaming and NATS options have been correctly loaded
	if sopts.ID != "my_cluster" {
		t.Fatalf("Unexpected cluster id: %v", sopts.ID)
	}
	if nopts.Port != 5223 {
		t.Fatalf("Unexpected listen port: %v", nopts.Port)
	}
	// Since logtime is not defined, it should default to `true`
	if !nopts.Logtime {
		t.Fatalf("Unexpected logtime value: %v", nopts.Logtime)
	}

	// Now pass only one file, and verify that the single file is used
	// for both streaming and nats.
	sopts, nopts = mustNotFail([]string{"-sc", sconf})
	if sopts.ID != "my_cluster" {
		t.Fatalf("Unexpected cluster id: %v", sopts.ID)
	}
	// This should be the port defined in scontent
	if nopts.Port != 4223 {
		t.Fatalf("Unexpected listen port: %v", nopts.Port)
	}
	// Since logtime is not defined, it should default to `true`
	if !nopts.Logtime {
		t.Fatalf("Unexpected logtime value: %v", nopts.Logtime)
	}
	// Same with other conf file
	sopts, nopts = mustNotFail([]string{"-c", nconf})
	if sopts.ID != "my_cluster_2" {
		t.Fatalf("Unexpected cluster id: %v", sopts.ID)
	}
	// This should be the port defined in scontent
	if nopts.Port != 5223 {
		t.Fatalf("Unexpected listen port: %v", nopts.Port)
	}
	// Since logtime is not defined, it should default to `true`
	if !nopts.Logtime {
		t.Fatalf("Unexpected logtime value: %v", nopts.Logtime)
	}
	// Ensure that if logtime is present in the config file, its value is used.
	// This test belongs more in NATS, but this is an issue that surfaced
	// in previous attempts to solve flags override. So keeping it here so
	// that we catch such issue if we were to change the flag override code
	// and break it.
	for i := 0; i < 2; i++ {
		os.Remove(nconf)
		if i == 0 {
			ncontent = []byte(`logtime: false`)
		} else {
			ncontent = []byte(`logtime: true`)
		}
		if err := ioutil.WriteFile(nconf, ncontent, 0660); err != nil {
			t.Fatalf("Error creating conf file: %v", err)
		}
		_, nopts = mustNotFail([]string{"-c", nconf})
		// Logtime is specified in the log, so it should be the value that is in
		// the file.
		if i == 0 && nopts.Logtime || i == 1 && !nopts.Logtime {
			t.Fatalf("Unexpected logtime value: %v", nopts.Logtime)
		}
	}
}
