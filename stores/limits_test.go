// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAddPerChannel(t *testing.T) {
	sl := testDefaultStoreLimits
	cl := &ChannelLimits{
		MsgStoreLimits{
			MaxMsgs:  10,
			MaxBytes: 100,
			MaxAge:   1000,
		},
		SubStoreLimits{
			MaxSubscriptions: 10,
		},
	}
	sl.AddPerChannel("foo", cl)
	if len(sl.PerChannel) != 1 {
		t.Fatalf("Expected 1 channel, got %v", len(sl.PerChannel))
	}
	addedCL := sl.PerChannel["foo"]
	if addedCL == nil {
		t.Fatal("ChannelLimits not found")
	}
	if !reflect.DeepEqual(*cl, *addedCL) {
		t.Fatalf("Expected channel limits to be %v, got %v", *cl, *addedCL)
	}
}

func TestBuildErrors(t *testing.T) {
	sl := testDefaultStoreLimits

	// This function calls Build(), expects and error and check
	// that the error it gets starts with the given error text.
	expectError := func(errTxt string) {
		err := sl.Build()
		if err == nil {
			stackFatalf(t, "Expected error on build, did not get one")
		}
		expectedErrTxt := strings.ToLower(errTxt)
		gotErrorTxt := strings.ToLower(err.Error())
		if !strings.HasPrefix(gotErrorTxt, expectedErrTxt) {
			stackFatalf(t, "Expected error to be about %q, got %v", expectedErrTxt, gotErrorTxt)
		}
	}
	// Check that we get an error for negative values.
	sl.MaxChannels = -1
	expectError("Max channels")

	sl.MaxChannels = 1
	sl.MaxSubscriptions = -1
	expectError("Max subscriptions")

	sl.MaxChannels = 1
	sl.MaxSubscriptions = 1
	sl.MaxMsgs = -1
	expectError("Max messages")

	sl.MaxChannels = 1
	sl.MaxSubscriptions = 1
	sl.MaxMsgs = 1
	sl.MaxBytes = -1
	expectError("Max bytes")

	sl.MaxChannels = 1
	sl.MaxSubscriptions = 1
	sl.MaxMsgs = 1
	sl.MaxBytes = 1
	sl.MaxAge = -1
	expectError("Max age")

	// Reset sl
	sl.MaxChannels = 1
	sl.MaxSubscriptions = 1
	sl.MaxMsgs = 1
	sl.MaxBytes = 1
	sl.MaxAge = 1

	// Adding a second channel should cause build failures, AddPerChannel itself
	// does not fail.
	cl := &ChannelLimits{}
	cl.MaxMsgs = 10
	cl.MaxAge = 2 * time.Hour
	sl.AddPerChannel("foo", cl)
	sl.AddPerChannel("bar", cl)
	expectError("Too many channels")

	// Reset sl
	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo.*", cl)
	expectError("invalid channel name")

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo.>", cl)
	expectError("invalid channel name")

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo.", cl)
	expectError("invalid channel name")
}

func TestAppliedInheritance(t *testing.T) {
	sl := testDefaultStoreLimits
	sl.MaxMsgs = 11
	sl.MaxBytes = 12
	sl.MaxAge = 13
	sl.MaxSubscriptions = 14

	checkPerChannel := func(maxMsgs int, maxBytes, maxAge int64, maxSubs int) {
		cl := &ChannelLimits{}
		cl.MaxMsgs = maxMsgs
		cl.MaxBytes = maxBytes
		cl.MaxAge = time.Duration(maxAge)
		cl.MaxSubscriptions = maxSubs
		sl.AddPerChannel("foo", cl)
		if err := sl.Build(); err != nil {
			stackFatalf(t, "Unexpected error on build: %v", err)
		}
		builtCl := sl.PerChannel["foo"]
		// Make sure that values from global are used for limits that were specified as 0.
		if maxMsgs == 0 && builtCl.MaxMsgs != sl.MaxMsgs {
			t.Fatalf("Max messages from global limit not inherited: %v", builtCl)
		}
		if maxBytes == 0 && builtCl.MaxBytes != sl.MaxBytes {
			t.Fatalf("Max bytes from global limit not inherited: %v", builtCl)
		}
		if maxAge == 0 && builtCl.MaxAge != sl.MaxAge {
			t.Fatalf("Max age from global limit not inherited: %v", builtCl)
		}
		if maxSubs == 0 && builtCl.MaxSubscriptions != sl.MaxSubscriptions {
			t.Fatalf("Max messages from global limit not inherited: %v", builtCl)
		}
	}
	checkPerChannel(10, 0, 0, 0)
	checkPerChannel(0, 10, 0, 0)
	checkPerChannel(0, 0, 10, 0)
	checkPerChannel(0, 0, 0, 10)
}

func TestLimitsUnlimited(t *testing.T) {
	sl := &StoreLimits{}
	// All global are unlimited. We should be able to set any other
	// value for a Per-Channel limit
	cl := &ChannelLimits{}
	cl.MaxMsgs = 1000000
	cl.MaxBytes = int64(cl.MaxMsgs * 1024)
	cl.MaxSubscriptions = 1000000
	cl.MaxAge = time.Duration(1000000) * time.Hour
	// Add 10 channels
	for i := 0; i < 10; i++ {
		sl.AddPerChannel(fmt.Sprintf("foo.%d", i), cl)
	}
	if err := sl.Build(); err != nil {
		t.Fatalf("Unexpected error on build: %v", err)
	}
	// Use non unlimited global limits, check that we can have
	// unlimited values for per-channel
	sl = &StoreLimits{}
	sl.MaxChannels = 1000
	sl.MaxMsgs = 1
	sl.MaxBytes = 1
	sl.MaxAge = 1
	sl.MaxSubscriptions = 1
	// Set all limit for this channel to -1
	cl = &ChannelLimits{}
	cl.MaxMsgs = -1
	cl.MaxBytes = -1
	cl.MaxAge = -1
	cl.MaxSubscriptions = -1
	sl.AddPerChannel("foo", cl)
	if err := sl.Build(); err != nil {
		t.Fatalf("Unexpected error on build: %v", err)
	}
	builtCl := sl.PerChannel["foo"]
	// the result should be that all values are set to 0.
	// So compare with this empty structure
	expected := ChannelLimits{}
	if !reflect.DeepEqual(*builtCl, expected) {
		t.Fatalf("Expected limits to be %v, got %v", expected, *builtCl)
	}
}
