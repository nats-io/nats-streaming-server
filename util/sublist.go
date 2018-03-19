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
	"errors"
	"sync"
)

// This is taken from NATS Server's sublist and modified to use
// an interface{} instead of a *subscription which is relevant
// only in NATS Server.

// Common byte variables for wildcards and token separator.
const (
	pwc   = '*'
	spwc  = "*"
	fwc   = '>'
	sfwc  = ">"
	tsep  = "."
	btsep = '.'
)

// cacheMax is used to bound limit the frontend cache
const slCacheMax = 1024

// Sublist related errors
var (
	ErrInvalidSubject = errors.New("sublist: invalid subject")
	ErrNotFound       = errors.New("sublist: no match found")
)

// A Sublist stores and efficiently retrieves subscriptions.
type Sublist struct {
	sync.RWMutex
	root  *level
	cache map[string][]interface{}
	count uint32
}

// A node contains subscriptions and a pointer to the next level.
type node struct {
	next     *level
	elements []interface{}
}

// A level represents a group of nodes and special pointers to
// wildcard nodes.
type level struct {
	nodes    map[string]*node
	pwc, fwc *node
}

// Create a new default node.
func newNode() *node {
	return &node{elements: make([]interface{}, 0, 4)}
}

// Create a new default level.
func newLevel() *level {
	return &level{nodes: make(map[string]*node)}
}

// NewSublist creates a default sublist
func NewSublist() *Sublist {
	return &Sublist{root: newLevel(), cache: make(map[string][]interface{})}
}

// Insert adds a subscription into the sublist
func (s *Sublist) Insert(subject string, element interface{}) error {
	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])

	s.Lock()

	sfwc := false
	l := s.root
	var n *node

	for _, t := range tokens {
		if len(t) == 0 || sfwc {
			s.Unlock()
			return ErrInvalidSubject
		}

		switch t[0] {
		case pwc:
			n = l.pwc
		case fwc:
			n = l.fwc
			sfwc = true
		default:
			n = l.nodes[t]
		}
		if n == nil {
			n = newNode()
			switch t[0] {
			case pwc:
				l.pwc = n
			case fwc:
				l.fwc = n
			default:
				l.nodes[t] = n
			}
		}
		if n.next == nil {
			n.next = newLevel()
		}
		l = n.next
	}
	n.elements = append(n.elements, element)
	s.addToCache(subject, element)
	s.count++
	s.Unlock()
	return nil
}

// addToCache will add the new entry to existing cache
// entries if needed. Assumes write lock is held.
func (s *Sublist) addToCache(subject string, element interface{}) {
	for k, r := range s.cache {
		if matchLiteral(k, subject) {
			// Copy since others may have a reference.
			nr := append([]interface{}(nil), r...)
			nr = append(nr, element)
			s.cache[k] = nr
		}
	}
}

// removeFromCache will remove any active cache entries on that subject.
// Assumes write lock is held.
func (s *Sublist) removeFromCache(subject string) {
	for k := range s.cache {
		if !matchLiteral(k, subject) {
			continue
		}
		// Since someone else may be referencing, can't modify the list
		// safely, just let it re-populate.
		delete(s.cache, k)
	}
}

// Match will match all entries to the literal subject.
// It will return a set of results.
func (s *Sublist) Match(subject string) []interface{} {
	s.RLock()
	rc, ok := s.cache[subject]
	s.RUnlock()
	if ok {
		return rc
	}

	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])
	result := make([]interface{}, 0, 4)

	s.Lock()
	matchLevel(s.root, tokens, &result)

	// Add to our cache
	s.cache[subject] = result
	// Bound the number of entries to sublistMaxCache
	if len(s.cache) > slCacheMax {
		for k := range s.cache {
			delete(s.cache, k)
			break
		}
	}
	s.Unlock()

	return result
}

// matchLevel is used to recursively descend into the trie.
func matchLevel(l *level, toks []string, results *[]interface{}) {
	var pwc, n *node
	for i, t := range toks {
		if l == nil {
			return
		}
		if l.fwc != nil {
			*results = append(*results, l.fwc.elements...)
		}
		if pwc = l.pwc; pwc != nil {
			matchLevel(pwc.next, toks[i+1:], results)
		}
		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if pwc != nil {
		*results = append(*results, pwc.elements...)
	}
	if n != nil {
		*results = append(*results, n.elements...)
	}
}

// lnt is used to track descent into levels for a removal for pruning.
type lnt struct {
	l *level
	n *node
	t string
}

// Remove will remove an element from the sublist.
func (s *Sublist) Remove(subject string, element interface{}) error {
	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])

	s.Lock()
	defer s.Unlock()

	sfwc := false
	l := s.root
	var n *node

	// Track levels for pruning
	var lnts [32]lnt
	levels := lnts[:0]

	for _, t := range tokens {
		if len(t) == 0 || sfwc {
			return ErrInvalidSubject
		}
		if l == nil {
			return ErrNotFound
		}
		switch t[0] {
		case pwc:
			n = l.pwc
		case fwc:
			n = l.fwc
			sfwc = true
		default:
			n = l.nodes[t]
		}
		if n != nil {
			levels = append(levels, lnt{l, n, t})
			l = n.next
		} else {
			l = nil
		}
	}
	if !s.removeFromNode(n, element) {
		return ErrNotFound
	}
	s.count--
	for i := len(levels) - 1; i >= 0; i-- {
		l, n, t := levels[i].l, levels[i].n, levels[i].t
		if n.isEmpty() {
			l.pruneNode(n, t)
		}
	}
	s.removeFromCache(subject)
	return nil
}

// pruneNode is used to prune an empty node from the tree.
func (l *level) pruneNode(n *node, t string) {
	if n == nil {
		return
	}
	if n == l.fwc {
		l.fwc = nil
	} else if n == l.pwc {
		l.pwc = nil
	} else {
		delete(l.nodes, t)
	}
}

// isEmpty will test if the node has any entries. Used
// in pruning.
func (n *node) isEmpty() bool {
	if len(n.elements) == 0 {
		if n.next == nil || n.next.numNodes() == 0 {
			return true
		}
	}
	return false
}

// Return the number of nodes for the given level.
func (l *level) numNodes() int {
	num := len(l.nodes)
	if l.pwc != nil {
		num++
	}
	if l.fwc != nil {
		num++
	}
	return num
}

// Removes an element from a list.
func removeFromList(element interface{}, l []interface{}) ([]interface{}, bool) {
	for i := 0; i < len(l); i++ {
		if l[i] == element {
			last := len(l) - 1
			l[i] = l[last]
			l[last] = nil
			l = l[:last]
			return shrinkAsNeeded(l), true
		}
	}
	return l, false
}

// Remove the sub for the given node.
func (s *Sublist) removeFromNode(n *node, element interface{}) (found bool) {
	if n == nil {
		return false
	}
	n.elements, found = removeFromList(element, n.elements)
	return found
}

// Checks if we need to do a resize. This is for very large growth then
// subsequent return to a more normal size from unsubscribe.
func shrinkAsNeeded(l []interface{}) []interface{} {
	ll := len(l)
	cl := cap(l)
	// Don't bother if list not too big
	if cl <= 8 {
		return l
	}
	pFree := float32(cl-ll) / float32(cl)
	if pFree > 0.50 {
		return append([]interface{}(nil), l...)
	}
	return l
}

// Count returns the number of subscriptions.
func (s *Sublist) Count() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.count
}

// CacheCount returns the number of result sets in the cache.
func (s *Sublist) CacheCount() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.cache)
}

// matchLiteral is used to test literal subjects, those that do not have any
// wildcards, with a target subject. This is used in the cache layer.
func matchLiteral(literal, subject string) bool {
	li := 0
	ll := len(literal)
	for i := 0; i < len(subject); i++ {
		if li >= ll {
			return false
		}
		b := subject[i]
		switch b {
		case pwc:
			// Skip token in literal
			ll := len(literal)
			for {
				if li >= ll || literal[li] == btsep {
					li--
					break
				}
				li++
			}
		case fwc:
			return true
		default:
			if b != literal[li] {
				return false
			}
		}
		li++
	}
	// Make sure we have processed all of the literal's chars..
	return li >= ll
}

// NumLevels returns the maximum number of levels in the sublist.
func (s *Sublist) NumLevels() int {
	return visitLevel(s.root, 0)
}

// visitLevel is used to descend the Sublist tree structure
// recursively.
func visitLevel(l *level, depth int) int {
	if l == nil || l.numNodes() == 0 {
		return depth
	}

	depth++
	maxDepth := depth

	for _, n := range l.nodes {
		if n == nil {
			continue
		}
		newDepth := visitLevel(n.next, depth)
		if newDepth > maxDepth {
			maxDepth = newDepth
		}
	}
	if l.pwc != nil {
		pwcDepth := visitLevel(l.pwc.next, depth)
		if pwcDepth > maxDepth {
			maxDepth = pwcDepth
		}
	}
	if l.fwc != nil {
		fwcDepth := visitLevel(l.fwc.next, depth)
		if fwcDepth > maxDepth {
			maxDepth = fwcDepth
		}
	}
	return maxDepth
}

// Subjects returns an array of all subjects in this sublist
// ordered from the widest to the narrowest of subjects.
// Order between non wildcard tokens in a given level is
// random though.
//
// For instance, if the sublist contains (in any inserted order):
//
// *.*, foo.>, *.>, foo.*.>, >, bar.>, foo.bar.>, bar.baz
//
// the returned array will be one of the two possibilities:
//
// >, *.>, *.*, foo.>, foo.*.>, foo.bar.>, bar.>, bar.baz
//
// or
//
// >, *.>, *.*, bar.>, bar.baz, foo.>, foo.*.>, foo.bar.>
//
// For a given level, the order will still always be from
// wider to narrower, that is, foo.> comes before foo.*.>
// which comes before foo.bar.>, and bar.> always comes
// before bar.baz, but all the "bar" subjects may be
// before or after all the "foo" subjects.
func (s *Sublist) Subjects() []string {
	s.RLock()
	defer s.RUnlock()
	subjects := make([]string, 0, s.count)
	getSubjects(s.root, "", &subjects)
	return subjects
}

func getSubjects(l *level, subject string, res *[]string) {
	if l == nil || l.numNodes() == 0 {
		*res = append(*res, subject)
		return
	}
	var fs string
	if l.fwc != nil {
		if subject != "" {
			fs = subject + tsep + sfwc
		} else {
			fs = sfwc
		}
		getSubjects(l.fwc.next, fs, res)
	}
	if l.pwc != nil {
		if subject != "" {
			fs = subject + tsep + spwc
		} else {
			fs = spwc
		}
		getSubjects(l.pwc.next, fs, res)
	}
	for s, n := range l.nodes {
		if subject != "" {
			fs = subject + tsep + s
		} else {
			fs = s
		}
		getSubjects(n.next, fs, res)
	}
}
