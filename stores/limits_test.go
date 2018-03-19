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
	"strings"
	"testing"
	"time"
)

func TestLimitsAddPerChannel(t *testing.T) {
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
		2000,
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

func TestLimitsBuildErrors(t *testing.T) {
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

	sl.MaxChannels = 1
	sl.MaxSubscriptions = 1
	sl.MaxMsgs = 1
	sl.MaxBytes = 1
	sl.MaxAge = 1
	sl.MaxInactivity = -1
	expectError("Max inactivity")

	// Reset sl
	sl.MaxChannels = 1
	sl.MaxSubscriptions = 1
	sl.MaxMsgs = 1
	sl.MaxBytes = 1
	sl.MaxAge = 1
	sl.MaxInactivity = 1

	// Adding a second channel should cause build failures, AddPerChannel itself
	// does not fail.
	cl := &ChannelLimits{}
	cl.MaxMsgs = 10
	cl.MaxAge = 2 * time.Hour
	sl.AddPerChannel("foo", cl)
	sl.AddPerChannel("bar", cl)
	expectError("Too many channels")

	// Reset sl, check invalid channel names
	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo.*bar", cl)
	expectError("invalid channel name")

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo.**.bar", cl)
	expectError("invalid channel name")

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo.>.bar", cl)
	expectError("invalid channel name")

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo.", cl)
	expectError("invalid channel name")

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel(".foo", cl)
	expectError("invalid channel name")

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	sl.AddPerChannel("foo/bar", cl)
	expectError("invalid channel name")
}

func TestLimitsPerChannelOverride(t *testing.T) {
	sl := testDefaultStoreLimits

	// This function calls Build(), expects no error.
	expectNoError := func(channel string, expectedLimits *ChannelLimits) {
		if err := sl.Build(); err != nil {
			stackFatalf(t, "Unexpected error on Build: %v", err)
		}
		cl := sl.PerChannel[channel]
		if !reflect.DeepEqual(*cl, *expectedLimits) {
			stackFatalf(t, "Expected limits for %q to be %v, got %v",
				channel, *expectedLimits, *cl)
		}
	}

	// Check per-channel values are below global limits.
	sl.MaxChannels = 2
	sl.MaxMsgs = 10
	sl.MaxBytes = 20
	sl.MaxSubscriptions = 30
	sl.MaxAge = time.Second
	sl.MaxInactivity = time.Minute

	cl := &ChannelLimits{}
	cl.MaxMsgs = 5
	sl.AddPerChannel("foo", cl)
	cl2 := *cl
	cl2.MaxMsgs = 12
	sl.AddPerChannel("bar", &cl2)
	expectNoError("bar", &cl2)

	cl = &ChannelLimits{}
	cl.MaxBytes = 25
	cl2 = *cl
	cl2.MaxBytes = 18
	sl.AddPerChannel("foo", cl)
	sl.AddPerChannel("bar", &cl2)
	expectNoError("foo", cl)

	cl = &ChannelLimits{}
	cl.MaxAge = time.Second
	cl2 = *cl
	cl2.MaxAge = time.Hour
	sl.AddPerChannel("foo", cl)
	sl.AddPerChannel("bar", &cl2)
	expectNoError("bar", &cl2)

	cl = &ChannelLimits{}
	cl.MaxInactivity = time.Minute
	cl2 = *cl
	cl2.MaxInactivity = time.Hour
	sl.AddPerChannel("foo", cl)
	sl.AddPerChannel("bar", &cl2)
	expectNoError("bar", &cl2)

	cl = &ChannelLimits{}
	cl.MaxSubscriptions = 35
	cl2 = *cl
	cl2.MaxSubscriptions = 10
	sl.AddPerChannel("foo", cl)
	sl.AddPerChannel("bar", &cl2)
	expectNoError("foo", cl)

	sl = testDefaultStoreLimits
	cl = &ChannelLimits{}
	cl.MaxMsgs = 10
	cl2 = *cl
	cl2.MaxMsgs = 20
	sl.AddPerChannel("foo.>", cl)
	sl.AddPerChannel("foo.*", &cl2)
	expectNoError("foo.*", &cl2)

	sl = testDefaultStoreLimits
	// All other limits are set by default
	sl.MaxAge = time.Hour
	cl = &ChannelLimits{}
	// We override MaxSubscriptions by setting to -1
	cl.MaxSubscriptions = -1
	sl.AddPerChannel("foo.*", cl)
	// Check that we get what we expect
	// After Build, we should get foo.*'s cl be equal
	// to this (in value). The MaxSubscriptions should have
	// been set to 0 after inheritance is applied.
	cl2 = sl.ChannelLimits
	cl2.MaxSubscriptions = 0
	expectNoError("foo.*", &cl2)

	// Repeat tests with each limit
	cl = &ChannelLimits{}
	cl.MaxMsgs = -1
	sl.AddPerChannel("foo.*", cl)
	// Check that we get what we expect
	// After Build, we should get foo.*'s cl be equal
	// to this (in value). The MaxSubscriptions should have
	// been set to 0 after inheritance is applied.
	cl2 = sl.ChannelLimits
	cl2.MaxMsgs = 0
	expectNoError("foo.*", &cl2)

	cl = &ChannelLimits{}
	cl.MaxBytes = -1
	sl.AddPerChannel("foo.*", cl)
	// Check that we get what we expect
	// After Build, we should get foo.*'s cl be equal
	// to this (in value). The MaxSubscriptions should have
	// been set to 0 after inheritance is applied.
	cl2 = sl.ChannelLimits
	cl2.MaxBytes = 0
	expectNoError("foo.*", &cl2)

	cl = &ChannelLimits{}
	cl.MaxAge = -1
	sl.AddPerChannel("foo.*", cl)
	// Check that we get what we expect
	// After Build, we should get foo.*'s cl be equal
	// to this (in value). The MaxSubscriptions should have
	// been set to 0 after inheritance is applied.
	cl2 = sl.ChannelLimits
	cl2.MaxAge = 0
	expectNoError("foo.*", &cl2)

	cl = &ChannelLimits{}
	cl.MaxInactivity = -1
	sl.AddPerChannel("foo.*", cl)
	// Check that we get what we expect
	// After Build, we should get foo.*'s cl be equal
	// to this (in value). The MaxSubscriptions should have
	// been set to 0 after inheritance is applied.
	cl2 = sl.ChannelLimits
	cl2.MaxInactivity = 0
	expectNoError("foo.*", &cl2)
}

func TestLimitsInheritance(t *testing.T) {
	sl := testDefaultStoreLimits
	sl.MaxMsgs = 11
	sl.MaxBytes = 12
	sl.MaxAge = 13
	sl.MaxSubscriptions = 14
	sl.MaxInactivity = 15

	addPerChannel := func(channel string, maxMsgs int, maxBytes, maxAge int64, maxSubs int, MaxInactivity int64) {
		cl := &ChannelLimits{}
		cl.MaxMsgs = maxMsgs
		cl.MaxBytes = maxBytes
		cl.MaxAge = time.Duration(maxAge)
		cl.MaxSubscriptions = maxSubs
		cl.MaxInactivity = time.Duration(MaxInactivity)
		sl.AddPerChannel(channel, cl)
	}
	checkPerChannel := func(channel string, maxMsgs int, maxBytes, maxAge int64, maxSubs int, MaxInactivity int64) {
		sl.PerChannel = nil
		addPerChannel(channel, maxMsgs, maxBytes, maxAge, maxSubs, MaxInactivity)
		if err := sl.Build(); err != nil {
			stackFatalf(t, "Unexpected error on build: %v", err)
		}
		builtCl := sl.PerChannel[channel]
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
		if MaxInactivity == 0 && builtCl.MaxInactivity != sl.MaxInactivity {
			t.Fatalf("MaxInactivity from global limit not inherited: %v", builtCl)
		}
		if maxSubs == 0 && builtCl.MaxSubscriptions != sl.MaxSubscriptions {
			t.Fatalf("Max messages from global limit not inherited: %v", builtCl)
		}
	}
	checkPerChannel("foo", 10, 0, 0, 0, 0)
	checkPerChannel("foo", 0, 10, 0, 0, 0)
	checkPerChannel("foo", 0, 0, 10, 0, 0)
	checkPerChannel("foo", 0, 0, 0, 10, 0)
	checkPerChannel("foo", 0, 0, 0, 0, 10)
	// Should be the same even if the channel has wildcards
	checkPerChannel("foo.*", 10, 0, 0, 0, 0)
	checkPerChannel("foo.*", 0, 10, 0, 0, 0)
	checkPerChannel("foo.*", 0, 0, 10, 0, 0)
	checkPerChannel("foo.*", 0, 0, 0, 10, 0)
	checkPerChannel("foo.*", 0, 0, 0, 0, 10)
	// Check inheritance between wildcard channels.
	// We start with global limits as:
	//             Msgs Bytes Age Subs MaxInactivity
	// Global        11    12  13   14      15
	//
	// Say we have the following channels
	//
	//             Msgs Bytes Age Subs MaxInactivity
	// *.*                  2
	// *.>                          5
	// foo.>         10
	// foo.*.>             20
	// foo.*.*.>               30
	// foo.a.b.c                    40
	// foo.d.e.f     50        60
	// foo.baz             70       80
	// bar.*.>       90
	// bar.baz.>          100              110
	// bar.baz.boz                         120
	//
	// After build we should have:
	//
	//             Msgs Bytes Age Subs MaxInactivity
	// *.>           11    12  13    5      15
	// *.*           11     2  13    5      15
	// foo.>         10     2  13    5      15
	// foo.*.>       10    20  13    5      15
	// foo.*.*.>     10    20  30    5      15
	// foo.a.b.c     10    20  30   40      15
	// foo.d.e.f     50    20  60    5      15
	// foo.baz       10    70  13   80      15
	// bar.*.>       90    12  13    5      15
	// bar.baz.>     90   100  13    5     110
	// bar.baz.boz   90   100  13    5     120
	// bar.baz.bat   90   100  13    5     110
	sl.PerChannel = nil
	addPerChannel("*.*", 0, 2, 0, 0, 0)
	addPerChannel("*.>", 0, 0, 0, 5, 0)
	addPerChannel("foo.>", 10, 0, 0, 0, 0)
	addPerChannel("foo.*.>", 0, 20, 0, 0, 0)
	addPerChannel("foo.*.*.>", 0, 0, 30, 0, 0)
	addPerChannel("foo.a.b.c", 0, 0, 0, 40, 0)
	addPerChannel("foo.d.e.f", 50, 0, 60, 0, 0)
	addPerChannel("foo.baz", 0, 70, 0, 80, 0)
	addPerChannel("bar.*.>", 90, 0, 0, 0, 0)
	addPerChannel("bar.baz.>", 0, 100, 0, 0, 110)
	addPerChannel("bar.baz.boz", 0, 0, 0, 0, 120)
	addPerChannel("bar.baz.bat", 0, 0, 0, 0, 110)
	if err := sl.Build(); err != nil {
		t.Fatalf("Unexpected error on build: %v", err)
	}
	checkChannel := func(channel string, maxMsgs int, maxBytes, maxAge int64, maxSubs int, MaxInactivity int64) {
		expectedLimit := &ChannelLimits{}
		expectedLimit.MaxMsgs = maxMsgs
		expectedLimit.MaxBytes = maxBytes
		expectedLimit.MaxAge = time.Duration(maxAge)
		expectedLimit.MaxSubscriptions = maxSubs
		expectedLimit.MaxInactivity = time.Duration(MaxInactivity)
		builtCl := sl.PerChannel[channel]
		if !reflect.DeepEqual(*builtCl, *expectedLimit) {
			stackFatalf(t, "Expected limits for %q to be %v, got %v",
				channel, *expectedLimit, *builtCl)
		}
	}
	checkChannel("*.>", 11, 12, 13, 5, 15)
	checkChannel("*.*", 11, 2, 13, 5, 15)
	checkChannel("foo.>", 10, 2, 13, 5, 15)
	checkChannel("foo.*.>", 10, 20, 13, 5, 15)
	checkChannel("foo.*.*.>", 10, 20, 30, 5, 15)
	checkChannel("foo.a.b.c", 10, 20, 30, 40, 15)
	checkChannel("foo.d.e.f", 50, 20, 60, 5, 15)
	checkChannel("foo.baz", 10, 70, 13, 80, 15)
	checkChannel("bar.*.>", 90, 12, 13, 5, 15)
	checkChannel("bar.baz.>", 90, 100, 13, 5, 110)
	checkChannel("bar.baz.boz", 90, 100, 13, 5, 120)
	checkChannel("bar.baz.bat", 90, 100, 13, 5, 110)
}

func TestLimitsWildcardsDontCountForMaxChannels(t *testing.T) {
	sl := testDefaultStoreLimits
	sl.MaxChannels = 2

	sl.AddPerChannel("foo.>", &ChannelLimits{})
	sl.AddPerChannel("foo.*", &ChannelLimits{})
	sl.AddPerChannel("foo.bar.*", &ChannelLimits{})
	if err := sl.Build(); err != nil {
		t.Fatalf("Should be ok to get more wildcard channels than MaxChannels: %v", err)
	}

	sl = testDefaultStoreLimits
	sl.MaxChannels = 2
	sl.AddPerChannel("foo.>", &ChannelLimits{})
	sl.AddPerChannel("foo.*", &ChannelLimits{})
	sl.AddPerChannel("foo.bar.*", &ChannelLimits{})
	sl.AddPerChannel("foo.bar", &ChannelLimits{})
	sl.AddPerChannel("foo.baz", &ChannelLimits{})
	if err := sl.Build(); err != nil {
		t.Fatalf("Should be ok to get more wildcard channels than MaxChannels: %v", err)
	}

	sl = testDefaultStoreLimits
	sl.MaxChannels = 2
	sl.AddPerChannel("foo.>", &ChannelLimits{})
	sl.AddPerChannel("foo.*", &ChannelLimits{})
	sl.AddPerChannel("foo.bar.*", &ChannelLimits{})
	sl.AddPerChannel("foo.bar", &ChannelLimits{})
	sl.AddPerChannel("foo.baz", &ChannelLimits{})
	sl.AddPerChannel("foo", &ChannelLimits{})
	if err := sl.Build(); err == nil {
		t.Fatal("Should have failed due to too many channels")
	}
}

func TestLimitsPrint(t *testing.T) {
	sl := testDefaultStoreLimits
	sl.AddPerChannel(">", &ChannelLimits{MsgStoreLimits: MsgStoreLimits{MaxMsgs: 10}})
	sl.AddPerChannel("foo.>", &ChannelLimits{MsgStoreLimits: MsgStoreLimits{MaxBytes: 1024}, MaxInactivity: 5 * time.Second})
	sl.AddPerChannel("foo.bar.>", &ChannelLimits{MsgStoreLimits: MsgStoreLimits{MaxAge: time.Second}})
	sl.AddPerChannel("foo.bar.baz.>", &ChannelLimits{SubStoreLimits: SubStoreLimits{MaxSubscriptions: 20}})
	sl.AddPerChannel("bar", &ChannelLimits{SubStoreLimits: SubStoreLimits{MaxSubscriptions: 30}})
	if err := sl.Build(); err != nil {
		t.Fatalf("Error on build: %v", err)
	}
	lines := sl.Print()
	ok := 0
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		if l == ">" {
			if lines[i+1] != " |-> Messages                     10" {
				t.Fatalf("Unexpected content for %v", l)
			}
			i++
			ok++
		} else if l == " bar" {
			if lines[i+1] != "  |-> Subscriptions               30" {
				t.Fatalf("Unexpected content for %v", l)
			}
			i++
			ok++
		} else if l == " foo.>" {
			if lines[i+1] != "  |-> Bytes                  1.00 KB" ||
				lines[i+2] != "  |-> Inactivity                  5s" ||
				lines[i+3] != "  foo.bar.>" ||
				lines[i+4] != "   |-> Age                        1s" ||
				lines[i+5] != "   foo.bar.baz.>" ||
				lines[i+6] != "    |-> Subscriptions             20" {
				t.Fatalf("Unexpected content for %v", l)
			}
			i += 6
			ok++
		}
	}
	if ok != 3 {
		t.Fatalf("Output not as expected")
	}
}

func TestLimitsClone(t *testing.T) {
	slo := testDefaultStoreLimits
	sl := &slo
	cl := &ChannelLimits{}
	cl.MaxMsgs = 100
	cl.MaxSubscriptions = 20
	cl.MaxAge = time.Second
	cl.MaxInactivity = time.Second
	sl.AddPerChannel("foo", cl)

	clone := sl.Clone()
	if !reflect.DeepEqual(*clone, *sl) {
		t.Fatalf("Expected %v, got %v", sl, *clone)
	}
	// Change the original
	cl.MaxMsgs = 200
	// They should now be different
	if reflect.DeepEqual(*clone, *sl) {
		t.Fatal("Expected clone and original to now be different")
	}
	sl.AddPerChannel("foo", cl)
	if reflect.DeepEqual(*clone, *sl) {
		t.Fatal("Expected clone and original to now be different")
	}
	// Get a clone again
	clone = sl.Clone()
	if !reflect.DeepEqual(*clone, *sl) {
		t.Fatalf("Expected %v, got %v", sl, *clone)
	}
	// Change one of the global properties
	sl.MaxBytes = 100
	// They should now be different
	if reflect.DeepEqual(*clone, *sl) {
		t.Fatal("Expected clone and original to now be different")
	}
}
