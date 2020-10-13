// Copyright 2017-2018 The NATS Authors
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

package util

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// This is taken from NATS Server's sublist tests and adapted
// to the generic implementation.

func verifyCount(s *Sublist, count uint32, t *testing.T) {
	if s.Count() != count {
		stackFatalf(t, "Count is %d, should be %d", s.Count(), count)
	}
}

func verifyLen(r []interface{}, l int, t *testing.T) {
	if len(r) != l {
		stackFatalf(t, "Results len is %d, should be %d", len(r), l)
	}
}

func verifyMember(r []interface{}, val interface{}, t *testing.T) {
	for _, v := range r {
		if v == nil {
			continue
		}
		if v == val {
			return
		}
	}
	stackFatalf(t, "Value '%+v' not found in results", val)
}

func verifyNumLevels(s *Sublist, expected int, t *testing.T) {
	dl := s.NumLevels()
	if dl != expected {
		stackFatalf(t, "NumLevels is %d, should be %d", dl, expected)
	}
}

func checkOrder(t *testing.T, r []interface{}, expectedVals ...interface{}) {
	if len(expectedVals) != len(r) {
		stackFatalf(t, "Expected %v values, got %v", len(expectedVals), len(r))
	}
	for i := 0; i < len(r); i++ {
		if !reflect.DeepEqual(r[i], expectedVals[i]) {
			stackFatalf(t, "Expected %v, got %v", expectedVals, r)
		}
	}
}

func TestSublistInit(t *testing.T) {
	s := NewSublist()
	verifyCount(s, 0, t)
}

func TestSublistInsertCount(t *testing.T) {
	s := NewSublist()
	s.Insert("foo", "fooval")
	s.Insert("bar", "barval")
	s.Insert("foo.bar", "foobarval")
	verifyCount(s, 3, t)
}

func TestSublistSimple(t *testing.T) {
	s := NewSublist()
	subject := "foo"
	val := "foo value"
	s.Insert(subject, val)
	verifyCount(s, 1, t)
	r := s.Match(subject)
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
}

func TestSublistSimpleMultiTokens(t *testing.T) {
	s := NewSublist()
	subject := "foo.bar.baz"
	val := "value"
	s.Insert(subject, val)
	r := s.Match(subject)
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
}

func TestSublistPartialWildcard(t *testing.T) {
	s := NewSublist()
	literalSubj := "a.b.c"
	val1 := "val1"
	wildcardSubj := "a.*.c"
	val2 := "val2"
	s.Insert(literalSubj, val1)
	s.Insert(wildcardSubj, val2)
	r := s.Match(literalSubj)
	checkOrder(t, r, val2, val1)
}

func TestSublistPartialWildcardAtEnd(t *testing.T) {
	s := NewSublist()
	literalSubj := "a.b.c"
	val1 := "val1"
	wildcardSubj := "a.b.*"
	val2 := "val2"
	s.Insert(literalSubj, val1)
	s.Insert(wildcardSubj, val2)
	r := s.Match("a.b.c")
	checkOrder(t, r, val2, val1)
}

func TestSublistFullWildcard(t *testing.T) {
	s := NewSublist()
	literalSubj := "a.b.c"
	val1 := "val1"
	wildcardSubj := "a.>"
	val2 := "val2"
	s.Insert(literalSubj, val1)
	s.Insert(wildcardSubj, val2)
	r := s.Match("a.b.c")
	checkOrder(t, r, val2, val1)
}

func TestSublistRemove(t *testing.T) {
	s := NewSublist()
	subject := "a.b.c.d"
	val1 := "val1"
	s.Insert(subject, val1)
	verifyCount(s, 1, t)
	r := s.Match(subject)
	verifyLen(r, 1, t)
	val2 := "val2"
	s.Remove(subject, val2)
	verifyCount(s, 1, t)
	s.Remove(subject, val1)
	verifyCount(s, 0, t)
	r = s.Match(subject)
	verifyLen(r, 0, t)
}

func TestSublistRemoveWildcard(t *testing.T) {
	s := NewSublist()
	subj1 := "a.b.c.d"
	val1 := "val1"
	subj2 := "a.b.*.d"
	val2 := "val2"
	subj3 := "a.b.>"
	val3 := "val3"
	s.Insert(subj1, val1)
	s.Insert(subj2, val2)
	s.Insert(subj3, val3)
	verifyCount(s, 3, t)
	r := s.Match(subj1)
	verifyLen(r, 3, t)
	s.Remove(subj1, val1)
	verifyCount(s, 2, t)
	s.Remove(subj3, val3)
	verifyCount(s, 1, t)
	s.Remove(subj2, val2)
	verifyCount(s, 0, t)
	r = s.Match(subj1)
	verifyLen(r, 0, t)
}

func TestSublistRemoveCleanup(t *testing.T) {
	s := NewSublist()
	literal := "a.b.c.d.e.f"
	depth := len(strings.Split(literal, tsep))
	val := "val"
	verifyNumLevels(s, 0, t)
	s.Insert(literal, val)
	verifyNumLevels(s, depth, t)
	s.Remove(literal, val)
	verifyNumLevels(s, 0, t)
}

func TestSublistRemoveCleanupWildcards(t *testing.T) {
	s := NewSublist()
	subject := "a.b.*.d.e.>"
	depth := len(strings.Split(subject, tsep))
	val := "val"
	verifyNumLevels(s, 0, t)
	s.Insert(subject, val)
	verifyNumLevels(s, depth, t)
	s.Remove(subject, val)
	verifyNumLevels(s, 0, t)
}

func TestSublistInvalidSubjectsInsert(t *testing.T) {
	s := NewSublist()

	// Insert can have wildcards, but not empty tokens,
	// and can not have a FWC that is not the terminal token.

	val := "val"
	// beginning empty token
	if err := s.Insert(".foo", val); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}

	// trailing empty token
	if err := s.Insert("foo.", val); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// empty middle token
	if err := s.Insert("foo..bar", val); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// empty middle token #2
	if err := s.Insert("foo.bar..baz", val); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// fwc not terminal
	if err := s.Insert("foo.>.bar", val); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
}

func TestSublistCache(t *testing.T) {
	s := NewSublist()

	// Test add a remove logistics
	lsub := "a.b.c.d"
	lsubVal := "val1"
	psub := "a.b.*.d"
	psubVal := "val2"
	fsub := "a.b.>"
	fsubVal := "val3"
	s.Insert(lsub, lsubVal)
	r := s.Match(lsub)
	verifyLen(r, 1, t)
	s.Insert(psub, psubVal)
	s.Insert(fsub, fsubVal)
	verifyCount(s, 3, t)
	r = s.Match(lsub)
	verifyLen(r, 3, t)
	s.Remove(lsub, lsubVal)
	verifyCount(s, 2, t)
	s.Remove(fsub, fsubVal)
	verifyCount(s, 1, t)
	s.Remove(psub, psubVal)
	verifyCount(s, 0, t)

	// Check that cache is now empty
	if cc := s.CacheCount(); cc != 0 {
		t.Fatalf("Cache should be zero, got %d\n", cc)
	}

	r = s.Match(lsub)
	verifyLen(r, 0, t)

	for i := 0; i < 2*slCacheMax; i++ {
		s.Match(fmt.Sprintf("foo-%d\n", i))
	}

	if cc := s.CacheCount(); cc > slCacheMax {
		t.Fatalf("Cache should be constrained by cacheMax, got %d for current count\n", cc)
	}
}

func checkBool(b, expected bool, t *testing.T) {
	if b != expected {
		stackFatalf(t, "Expected %v, but got %v\n", expected, b)
	}
}

func TestSublistMatchLiterals(t *testing.T) {
	checkBool(matchLiteral("foo", "foo"), true, t)
	checkBool(matchLiteral("foo", "bar"), false, t)
	checkBool(matchLiteral("foo", "*"), true, t)
	checkBool(matchLiteral("foo", ">"), true, t)
	checkBool(matchLiteral("foo.bar", ">"), true, t)
	checkBool(matchLiteral("foo.bar", "foo.>"), true, t)
	checkBool(matchLiteral("foo.bar", "bar.>"), false, t)
	checkBool(matchLiteral("stats.test.22", "stats.>"), true, t)
	checkBool(matchLiteral("stats.test.22", "stats.*.*"), true, t)
	checkBool(matchLiteral("foo.bar", "foo"), false, t)
	checkBool(matchLiteral("stats.test.foos", "stats.test.foos"), true, t)
	checkBool(matchLiteral("stats.test.foos", "stats.test.foo"), false, t)
}

func TestSublistBadSubjectOnRemove(t *testing.T) {
	bad := "a.b..d"
	val := "bad"

	s := NewSublist()
	if err := s.Insert(bad, val); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}

	if err := s.Remove(bad, val); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}

	badfwc := "a.>.b"
	if err := s.Remove(badfwc, val); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}
}

func TestSublistTwoTokenDontMatchSingleToken(t *testing.T) {
	s := NewSublist()
	val := "val"
	s.Insert("foo", val)
	r := s.Match("foo")
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
	r = s.Match("foo.bar")
	verifyLen(r, 0, t)
}

func TestSublistStoreSameValue(t *testing.T) {
	s := NewSublist()
	val := "val"
	lsub1 := "foo"
	lsub2 := "bar"
	s.Insert(lsub1, val)
	s.Insert(lsub2, val)
	verifyCount(s, 2, t)
	r := s.Match(lsub1)
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
	r = s.Match(lsub2)
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
	s.Remove(lsub2, val)
	verifyCount(s, 1, t)
	r = s.Match(lsub1)
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
	r = s.Match(lsub2)
	verifyLen(r, 0, t)
	s.Insert(lsub1, val)
	verifyCount(s, 2, t)
	r = s.Match(lsub1)
	verifyLen(r, 2, t)
	for _, v := range r {
		if v != val {
			t.Fatalf("Unexpected value: %v", v)
		}
	}
}

func TestSublistShrink(t *testing.T) {
	s := NewSublist()
	for i := 0; i < 10; i++ {
		s.Insert("foo", 1)
	}
	verifyCount(s, 10, t)
	s.RLock()
	capacity := cap(s.root.nodes["foo"].elements)
	s.RUnlock()
	if capacity < 10 {
		t.Fatalf("Capacity should be at least 10, got %v", capacity)
	}
	for i := 0; i < 9; i++ {
		s.Remove("foo", 1)
	}
	verifyCount(s, 1, t)
	s.RLock()
	newCapacity := cap(s.root.nodes["foo"].elements)
	s.RUnlock()
	if newCapacity >= capacity {
		t.Fatalf("Capacity should have decreased, got %v", newCapacity)
	}
}

func TestSublistSubjects(t *testing.T) {
	subjects := []string{"*.*", "foo.>", "*.>", ">", "foo.*.>", "bar.>", "foo.bar.>", "bar.baz"}
	pOne := []string{">", "*.>", "*.*", "foo.>", "foo.*.>", "foo.bar.>", "bar.>", "bar.baz"}
	pTwo := []string{">", "*.>", "*.*", "bar.>", "bar.baz", "foo.>", "foo.*.>", "foo.bar.>"}

	for i := 0; i < 1000; i++ {
		randomized := rand.Perm(len(subjects))
		randSubjects := make([]string, len(subjects))
		for j := 0; j < len(subjects); j++ {
			randSubjects[j] = subjects[randomized[j]]
		}
		s := NewSublist()
		for _, subj := range randSubjects {
			if err := s.Insert(subj, subj); err != nil {
				t.Fatalf("Error inserting in sublist: %v", err)
			}
		}
		r := s.Subjects()
		if len(r) != len(subjects) {
			t.Fatalf("Expected Subjects to return %v subjects, got %v", len(subjects), len(r))
		}
		if !reflect.DeepEqual(r, pOne) && !reflect.DeepEqual(r, pTwo) {
			t.Fatalf("Result expected to be either %v or %v, got %v", pOne, pTwo, r)
		}
	}

	subjects = []string{"bar", "bar.*", "bar.baz", "bar.>", "bar.*.bat", "bar.baz.*", "bar.baz.biz.box", "bar.baz.>"}
	expected := []string{"bar", "bar.>", "bar.*", "bar.*.bat", "bar.baz", "bar.baz.>", "bar.baz.*", "bar.baz.biz.box"}
	s := NewSublist()
	for _, subj := range subjects {
		s.Insert(subj, subj)
	}
	r := s.Match("bar")
	if len(r) == 0 {
		t.Fatalf("bar should be in the sublist")
	}
	if r[0].(string) != "bar" {
		t.Fatalf("invalid value for bar: %q", r[0].(string))
	}
	subjs := s.Subjects()
	if !reflect.DeepEqual(subjs, expected) {
		t.Fatalf("Expected subject:\n%q\n got\n%q", expected, subjs)
	}

	subjects = []string{"bar", "bar.*.*.box", "bar.baz.bat.*", ">", "*", "bar.baz.bat"}
	expected = []string{">", "*", "bar", "bar.*.*.box", "bar.baz.bat", "bar.baz.bat.*"}
	s = NewSublist()
	for _, subj := range subjects {
		s.Insert(subj, subj)
	}
	subjs = s.Subjects()
	if !reflect.DeepEqual(subjs, expected) {
		t.Fatalf("Expected subject:\n%q\n got\n%q", expected, subjs)
	}
}
