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

package process

import (
	"container/list"
	"crypto/sha256"
	"sync"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultIdempotencyTTL      = 15 * time.Minute
	defaultIdempotencyCapacity = 16384
	maxRequestIDLength         = 128
)

type idempotencyEntry struct {
	key         string
	fingerprint [32]byte
	inFlight    bool
	done        chan struct{}
	processID   string
	err         error
	expiresAt   time.Time
	elem        *list.Element
}

// idempotencyTable tracks in-flight and recently completed StartProcess calls by request_id.
// It is bounded by capacity and retention TTL to prevent unbounded memory growth.
// Entries live in ate-env-guest memory and measure TTL on that process's monotonic time.Now()
// clock, so snapshot suspend and resume preserve the table with remaining TTL governed by the
// guest's monotonic clock, whereas restarting ate-env-guest empties the table.
type idempotencyTable struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	entries  map[string]*idempotencyEntry
	order    *list.List
}

func newIdempotencyTable(capacity int, ttl time.Duration) *idempotencyTable {
	if capacity <= 0 {
		capacity = defaultIdempotencyCapacity
	}
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	return &idempotencyTable{
		capacity: capacity,
		ttl:      ttl,
		entries:  make(map[string]*idempotencyEntry),
		order:    list.New(),
	}
}

func validateRequestID(key string) error {
	if len(key) > maxRequestIDLength {
		return status.Errorf(codes.InvalidArgument, "request_id exceeds maximum length of %d bytes", maxRequestIDLength)
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 32 || key[i] > 126 {
			return status.Error(codes.InvalidArgument, "request_id must contain printable ASCII characters only")
		}
	}
	return nil
}

func computeRequestFingerprint(req *ateenvv1alpha.StartProcessRequest) [32]byte {
	clone := proto.Clone(req).(*ateenvv1alpha.StartProcessRequest)
	clone.RequestId = ""
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
	if err != nil {
		return sha256.Sum256([]byte(req.String()))
	}
	return sha256.Sum256(data)
}

// getOrCreate checks if key exists and is still live. If so, it returns the
// entry and alreadyExists=true. Otherwise, a new in-flight entry is stored and
// returned with alreadyExists=false.
func (t *idempotencyTable) getOrCreate(key string, req *ateenvv1alpha.StartProcessRequest) (*idempotencyEntry, bool, error) {
	if err := validateRequestID(key); err != nil {
		return nil, false, err
	}
	fp := computeRequestFingerprint(req)

	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if entry, ok := t.entries[key]; ok {
		if !entry.inFlight && !now.Before(entry.expiresAt) {
			t.removeEntryLocked(entry)
		} else {
			if entry.fingerprint != fp {
				return nil, false, status.Error(codes.InvalidArgument, "request_id reused with different request parameters")
			}
			return entry, true, nil
		}
	}

	t.pruneLocked(now)

	entry := &idempotencyEntry{
		key:         key,
		fingerprint: fp,
		inFlight:    true,
		done:        make(chan struct{}),
		expiresAt:   now.Add(t.ttl),
	}
	entry.elem = t.order.PushFront(entry)
	t.entries[key] = entry
	return entry, false, nil
}

// complete records the outcome of an in-flight StartProcess call and broadcasts to any waiters.
func (t *idempotencyTable) complete(entry *idempotencyEntry, processID string, err error) {
	t.mu.Lock()
	entry.inFlight = false
	if err != nil || processID == "" {
		t.removeEntryLocked(entry)
		if err != nil {
			entry.err = err
		} else {
			entry.err = status.Error(codes.Internal, "process initialization failed")
		}
		close(entry.done)
		t.mu.Unlock()
		return
	}
	entry.processID = processID
	entry.expiresAt = time.Now().Add(t.ttl)
	if entry.elem != nil {
		t.order.MoveToFront(entry.elem)
	}
	close(entry.done)
	t.mu.Unlock()
}

func (t *idempotencyTable) pruneLocked(now time.Time) {
	for elem := t.order.Back(); elem != nil; {
		prev := elem.Prev()
		entry := elem.Value.(*idempotencyEntry)
		if entry.inFlight {
			elem = prev
			continue
		}
		if !now.Before(entry.expiresAt) {
			t.removeEntryLocked(entry)
		} else {
			break
		}
		elem = prev
	}

	for len(t.entries) >= t.capacity {
		var evicted *idempotencyEntry
		for elem := t.order.Back(); elem != nil; elem = elem.Prev() {
			entry := elem.Value.(*idempotencyEntry)
			if !entry.inFlight {
				evicted = entry
				break
			}
		}
		if evicted == nil {
			break
		}
		t.removeEntryLocked(evicted)
	}
}

func (t *idempotencyTable) removeEntryLocked(entry *idempotencyEntry) {
	if entry.elem != nil {
		t.order.Remove(entry.elem)
		entry.elem = nil
	}
	delete(t.entries, entry.key)
}
