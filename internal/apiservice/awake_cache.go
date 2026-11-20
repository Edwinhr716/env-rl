// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apiservice

import (
	"container/list"
	"sync"
	"time"
)

const (
	defaultAwakeCapacity = 65536
	defaultAwakeTTL      = 10 * time.Minute
)

type awakeEntry struct {
	key       string
	expiresAt time.Time
	elem      *list.Element
	awake     bool
	gen       uint64
	caps      map[string]bool
}

// boundedAwakeCache maintains a thread-safe, bounded TTL cache of environments
// known to be awake and their observed guest capabilities. Remove advances a
// per-key generation so an in-flight wake started before a suspend or delete
// cannot mark the environment awake again.
type boundedAwakeCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	nextGen  uint64
	entries  map[string]*awakeEntry
	order    *list.List
}

func newBoundedAwakeCache(capacity int, ttl time.Duration) *boundedAwakeCache {
	if capacity <= 0 {
		capacity = defaultAwakeCapacity
	}
	if ttl <= 0 {
		ttl = defaultAwakeTTL
	}
	return &boundedAwakeCache{
		capacity: capacity,
		ttl:      ttl,
		entries:  make(map[string]*awakeEntry),
		order:    list.New(),
	}
}

func (c *boundedAwakeCache) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if entry, ok := c.entries[key]; ok {
		entry.awake = true
		entry.expiresAt = now.Add(c.ttl)
		c.order.MoveToFront(entry.elem)
		return
	}

	c.pruneLocked(now)

	entry := &awakeEntry{
		key:       key,
		expiresAt: now.Add(c.ttl),
		awake:     true,
	}
	entry.elem = c.order.PushFront(entry)
	c.entries[key] = entry
}

// Generation returns the current generation of key (0 when absent or expired)
// without mutating the cache, for pairing with AddIfGen across ensureAwake.
func (c *boundedAwakeCache) Generation(key string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok || time.Now().After(entry.expiresAt) {
		return 0
	}
	return entry.gen
}

// AddIfGen marks key awake with a fresh TTL only when its generation still
// matches gen, returning whether the entry was marked awake. The generation is
// kept on the cache entry and is lost if capacity eviction pushes the entry out.
func (c *boundedAwakeCache) AddIfGen(key string, gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	entry, ok := c.entries[key]
	if ok && now.After(entry.expiresAt) {
		c.removeEntryLocked(entry)
		ok = false
	}
	var currentGen uint64
	if ok {
		currentGen = entry.gen
	}
	if currentGen != gen {
		return false
	}
	if ok {
		entry.awake = true
		entry.expiresAt = now.Add(c.ttl)
		c.order.MoveToFront(entry.elem)
		return true
	}

	c.pruneLocked(now)

	entry = &awakeEntry{
		key:       key,
		expiresAt: now.Add(c.ttl),
		awake:     true,
		gen:       gen,
	}
	entry.elem = c.order.PushFront(entry)
	c.entries[key] = entry
	return true
}

func (c *boundedAwakeCache) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return false
	}
	if time.Now().After(entry.expiresAt) {
		c.removeEntryLocked(entry)
		return false
	}
	return entry.awake
}

// RecordCap records an advertised guest capability without marking the
// environment awake or extending the TTL of an existing entry.
func (c *boundedAwakeCache) RecordCap(key, cap string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	entry, ok := c.entries[key]
	if ok && now.After(entry.expiresAt) {
		c.removeEntryLocked(entry)
		ok = false
	}
	if !ok {
		c.pruneLocked(now)
		entry = &awakeEntry{
			key:       key,
			expiresAt: now.Add(c.ttl),
			awake:     false,
			caps:      make(map[string]bool),
		}
		entry.elem = c.order.PushFront(entry)
		c.entries[key] = entry
	} else if entry.caps == nil {
		entry.caps = make(map[string]bool)
	}
	entry.caps[cap] = true
}

func (c *boundedAwakeCache) HasCap(key, cap string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return false
	}
	if time.Now().After(entry.expiresAt) {
		c.removeEntryLocked(entry)
		return false
	}
	return entry.caps != nil && entry.caps[cap]
}

// Remove clears the awake state and cached capabilities of key and advances its
// generation so an in-flight wake started before Remove cannot re-add it. When
// key is absent, Remove inserts a generation-only tombstone entry that occupies
// one of the cache's capacity slots (65,536 by default) for the 10-minute TTL
// (so CreateEnvironment, SuspendEnvironment, and DeleteEnvironment under
// -reactive-wake each use a slot and can push out the oldest awake entry when
// full, costing one ensureAwake probe on its next call).
func (c *boundedAwakeCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	c.nextGen++
	if entry, ok := c.entries[key]; ok {
		entry.awake = false
		entry.caps = nil
		entry.gen = c.nextGen
		entry.expiresAt = now.Add(c.ttl)
		c.order.MoveToFront(entry.elem)
		return
	}

	c.pruneLocked(now)

	entry := &awakeEntry{
		key:       key,
		expiresAt: now.Add(c.ttl),
		awake:     false,
		gen:       c.nextGen,
	}
	entry.elem = c.order.PushFront(entry)
	c.entries[key] = entry
}

func (c *boundedAwakeCache) pruneLocked(now time.Time) {
	for elem := c.order.Back(); elem != nil; {
		prev := elem.Prev()
		entry := elem.Value.(*awakeEntry)
		if now.After(entry.expiresAt) {
			c.removeEntryLocked(entry)
		} else {
			break
		}
		elem = prev
	}

	for len(c.entries) >= c.capacity {
		back := c.order.Back()
		if back == nil {
			break
		}
		c.removeEntryLocked(back.Value.(*awakeEntry))
	}
}

func (c *boundedAwakeCache) removeEntryLocked(entry *awakeEntry) {
	if entry.elem != nil {
		c.order.Remove(entry.elem)
		entry.elem = nil
	}
	delete(c.entries, entry.key)
}
