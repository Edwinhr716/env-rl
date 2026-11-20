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
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStartProcessIdempotency checks WF-13 (StartProcessRequest.request_id
// deduplication, deterministic fingerprinting, parameter mismatch rejection,
// post-TTL reuse with different parameters, waiter context status mapping, key
// validation, and error eviction).
func TestStartProcessIdempotency(t *testing.T) {
	tracker, err := NewTracker(TrackerConfig{
		Workspace: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewTracker: %v", err)
	}
	defer tracker.Close()

	svc := NewService(tracker)
	ctx := context.Background()

	// 1. Concurrent StartProcess calls with the same request_id must execute once.
	const concurrentCount = 5
	var wg sync.WaitGroup
	procs := make([]*ateenvv1alpha.Process, concurrentCount)
	errs := make([]error, concurrentCount)

	for i := 0; i < concurrentCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p, err := svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
				Command:   []string{"sh", "-c", "echo hello"},
				RequestId: "req-dedupe-1",
			})
			procs[idx] = p
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
	}

	firstID := procs[0].GetProcessId()
	for i := 1; i < concurrentCount; i++ {
		if procs[i].GetProcessId() != firstID {
			t.Errorf("call %d returned ProcessId %s, want %s", i, procs[i].GetProcessId(), firstID)
		}
	}

	// 2. Subsequent call with same request_id returns the same Process without re-launching.
	p2, err := svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo hello"},
		RequestId: "req-dedupe-1",
	})
	if err != nil {
		t.Fatalf("subsequent call failed: %v", err)
	}
	if p2.GetProcessId() != firstID {
		t.Errorf("subsequent call returned ProcessId %s, want %s", p2.GetProcessId(), firstID)
	}

	// 3. Different request_id starts a new distinct process.
	p3, err := svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo hello"},
		RequestId: "req-dedupe-2",
	})
	if err != nil {
		t.Fatalf("different request_id call failed: %v", err)
	}
	if p3.GetProcessId() == firstID {
		t.Errorf("different request_id returned same ProcessId %s, want distinct", p3.GetProcessId())
	}

	// 4. Same request_id with different command/payload while live must fail with InvalidArgument.
	_, err = svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo different-payload"},
		RequestId: "req-dedupe-1",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reused live request_id with different payload = %v, want codes.InvalidArgument", err)
	}

	// 5. Invalid request_id length (> 128 bytes) must fail with InvalidArgument.
	longID := strings.Repeat("a", 129)
	_, err = svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo hello"},
		RequestId: longID,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("request_id > 128 bytes = %v, want codes.InvalidArgument", err)
	}

	// 6. Non-printable ASCII must fail with InvalidArgument.
	_, err = svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo hello"},
		RequestId: "req\x00bad",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("non-printable ASCII request_id = %v, want codes.InvalidArgument", err)
	}

	// 7. Deterministic hashing over multiple env vars across many iterations.
	multiEnvReq := &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", "env"},
		Env: map[string]string{
			"VAR1": "alpha",
			"VAR2": "beta",
			"VAR3": "gamma",
			"VAR4": "delta",
			"VAR5": "epsilon",
			"VAR6": "zeta",
		},
		RequestId: "req-multienv",
	}
	pMulti, err := svc.StartProcess(ctx, multiEnvReq)
	if err != nil {
		t.Fatalf("first multi-env call failed: %v", err)
	}
	for i := 0; i < 50; i++ {
		pRetry, err := svc.StartProcess(ctx, multiEnvReq)
		if err != nil {
			t.Fatalf("iteration %d: unexpected failure on deterministic multi-env retry: %v", i, err)
		}
		if pRetry.GetProcessId() != pMulti.GetProcessId() {
			t.Fatalf("iteration %d: got process %s, want %s", i, pRetry.GetProcessId(), pMulti.GetProcessId())
		}
	}

	// 8. Error eviction: a failed StartProcess evicts the entry, so the same request_id can be reused to start a valid process.
	_, err = svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"/nonexistent/binary/that/fails"},
		RequestId: "req-failed-evict",
	})
	if err == nil {
		t.Fatal("expected failure on nonexistent binary, got nil")
	}
	pRecovered, err := svc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo recovered"},
		RequestId: "req-failed-evict",
	})
	if err != nil {
		t.Fatalf("StartProcess with same request_id after failure failed: %v", err)
	}
	if pRecovered.GetProcessId() == "" {
		t.Fatal("StartProcess returned empty ProcessId after error eviction")
	}

	// 9. Expired entry reuse: once an entry's TTL elapses, reusing its request_id
	// with different parameters must remove the expired entry and run the new
	// command instead of failing with InvalidArgument.
	shortSvc := &Service{
		tracker:     tracker,
		idempotency: newIdempotencyTable(16, 20*time.Millisecond),
	}
	pBeforeTTL, err := shortSvc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo before-ttl"},
		RequestId: "req-ttl-reuse",
	})
	if err != nil {
		t.Fatalf("StartProcess before TTL expiry: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	pAfterTTL, err := shortSvc.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo after-ttl"},
		RequestId: "req-ttl-reuse",
	})
	if err != nil {
		t.Fatalf("StartProcess with different command after TTL expiry failed: %v", err)
	}
	if pAfterTTL.GetProcessId() == pBeforeTTL.GetProcessId() {
		t.Fatalf("StartProcess after TTL expiry returned old ProcessId %s, want new process", pAfterTTL.GetProcessId())
	}

	// 10. Cancelled or deadline-exceeded caller waiting on an in-flight entry
	// must receive codes.Canceled / codes.DeadlineExceeded rather than raw ctx.Err().
	inflightReq := &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"sh", "-c", "echo inflight"},
		RequestId: "req-inflight-ctx",
	}
	inflightEntry, exists, err := svc.idempotency.getOrCreate(inflightReq.GetRequestId(), inflightReq)
	if err != nil || exists {
		t.Fatalf("getOrCreate(inflight): exists=%v err=%v", exists, err)
	}
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := svc.StartProcess(canceledCtx, inflightReq); status.Code(err) != codes.Canceled {
		t.Fatalf("StartProcess with cancelled waiter ctx = %v (code %v), want codes.Canceled", err, status.Code(err))
	}
	expiredCtx, cancelDeadline := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancelDeadline()
	if _, err := svc.StartProcess(expiredCtx, inflightReq); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("StartProcess with expired waiter ctx = %v (code %v), want codes.DeadlineExceeded", err, status.Code(err))
	}
	svc.idempotency.complete(inflightEntry, "proc-inflight-done", nil)
}

// TestIdempotencyTable_HitPreservesExpiryOrder checks WF-13 (hits do not move
// entries ahead of newer entries, complete orders by updated expiresAt, and
// expired entries are removed before fingerprint comparison).
func TestIdempotencyTable_HitPreservesExpiryOrder(t *testing.T) {
	reqFor := func(id, cmd string) *ateenvv1alpha.StartProcessRequest {
		return &ateenvv1alpha.StartProcessRequest{
			Command:   []string{"sh", "-c", cmd},
			RequestId: id,
		}
	}

	// 1. Retrying k1 must not move it ahead of k2: once k1 expires while k2 is
	// live, inserting k3 at capacity prunes expired k1 from the tail and keeps k2.
	synctest.Test(t, func(t *testing.T) {
		tbl := newIdempotencyTable(2, 10*time.Second)

		e1, exists, err := tbl.getOrCreate("k1", reqFor("k1", "echo 1"))
		if err != nil || exists {
			t.Fatalf("getOrCreate(k1): exists=%v err=%v", exists, err)
		}
		tbl.complete(e1, "proc-1", nil) // expires at t=10s

		time.Sleep(5 * time.Second) // t=5s
		e2, exists, err := tbl.getOrCreate("k2", reqFor("k2", "echo 2"))
		if err != nil || exists {
			t.Fatalf("getOrCreate(k2): exists=%v err=%v", exists, err)
		}
		tbl.complete(e2, "proc-2", nil) // expires at t=15s

		time.Sleep(time.Second) // t=6s: hit k1 without extending its TTL
		if got, hit, err := tbl.getOrCreate("k1", reqFor("k1", "echo 1")); err != nil || !hit || got.processID != "proc-1" {
			t.Fatalf("retry k1 at t=6s: hit=%v id=%q err=%v", hit, got.processID, err)
		}

		time.Sleep(5 * time.Second) // t=11s: k1 expired (10s), k2 still live (15s)
		e3, exists, err := tbl.getOrCreate("k3", reqFor("k3", "echo 3"))
		if err != nil || exists {
			t.Fatalf("getOrCreate(k3): exists=%v err=%v", exists, err)
		}
		tbl.complete(e3, "proc-3", nil)

		if got, hit, err := tbl.getOrCreate("k2", reqFor("k2", "echo 2")); err != nil || !hit || got.processID != "proc-2" {
			t.Fatalf("retry k2 at t=11s: hit=%v err=%v, want cached proc-2 (expired k1 must be pruned instead of evicting live k2)", hit, err)
		}
	})

	// 2. When both k1 and k2 are still live at capacity, hitting k1 still evicts
	// older k1 first rather than newer k2.
	synctest.Test(t, func(t *testing.T) {
		tbl := newIdempotencyTable(2, 10*time.Second)

		e1, _, _ := tbl.getOrCreate("k1", reqFor("k1", "echo 1"))
		tbl.complete(e1, "proc-1", nil) // expires at t=10s

		time.Sleep(3 * time.Second) // t=3s
		e2, _, _ := tbl.getOrCreate("k2", reqFor("k2", "echo 2"))
		tbl.complete(e2, "proc-2", nil) // expires at t=13s

		time.Sleep(time.Second) // t=4s: hit k1
		if _, hit, err := tbl.getOrCreate("k1", reqFor("k1", "echo 1")); err != nil || !hit {
			t.Fatalf("hit k1: hit=%v err=%v", hit, err)
		}

		// Insert k3 at t=4s (capacity 2): k1 has the earlier expiry and must be evicted.
		e3, _, _ := tbl.getOrCreate("k3", reqFor("k3", "echo 3"))
		tbl.complete(e3, "proc-3", nil)

		if _, hit, err := tbl.getOrCreate("k2", reqFor("k2", "echo 2")); err != nil || !hit {
			t.Fatalf("k2 should remain cached after capacity eviction: hit=%v err=%v", hit, err)
		}
		if e1New, hit, err := tbl.getOrCreate("k1", reqFor("k1", "echo 1-new")); err != nil || hit {
			t.Fatalf("evicted k1 should accept a fresh entry: hit=%v err=%v", hit, err)
		} else {
			tbl.complete(e1New, "proc-1-new", nil)
		}
	})

	// 3. Out-of-order completion: k1 starts before k2 but finishes later, so
	// complete moves k1 to the front alongside its newer expiresAt, and exact
	// TTL boundary expiry allows reusing k2 with different parameters.
	synctest.Test(t, func(t *testing.T) {
		tbl := newIdempotencyTable(2, 10*time.Second)

		e1, _, _ := tbl.getOrCreate("k1", reqFor("k1", "echo slow")) // started at t=0
		time.Sleep(time.Second)                                      // t=1s
		e2, _, _ := tbl.getOrCreate("k2", reqFor("k2", "echo fast"))
		tbl.complete(e2, "proc-2", nil) // expires at t=11s

		time.Sleep(3 * time.Second)     // t=4s
		tbl.complete(e1, "proc-1", nil) // expires at t=14s; moved ahead of k2

		time.Sleep(7 * time.Second) // t=11s: exact boundary now == k2.expiresAt
		e2Reuse, hit, err := tbl.getOrCreate("k2", reqFor("k2", "echo different-after-expiry"))
		if err != nil || hit {
			t.Fatalf("reuse k2 at exact expiry with different command: hit=%v err=%v, want fresh entry", hit, err)
		}
		tbl.complete(e2Reuse, "proc-2b", nil)

		if got, hit, err := tbl.getOrCreate("k1", reqFor("k1", "echo slow")); err != nil || !hit || got.processID != "proc-1" {
			t.Fatalf("k1 with later completion expiry must remain cached: hit=%v err=%v", hit, err)
		}
	})
}

// TestIdempotencyTable_CompleteMoveToFrontProtectsLiveEntryFromPrune checks WF-13:
// when k1 starts before k2 and completes after k2, complete must move k1 ahead
// of k2 so that once k2 expires, inserting k3 at capacity prunes expired k2
// rather than evicting live k1.
func TestIdempotencyTable_CompleteMoveToFrontProtectsLiveEntryFromPrune(t *testing.T) {
	reqFor := func(id, cmd string) *ateenvv1alpha.StartProcessRequest {
		return &ateenvv1alpha.StartProcessRequest{
			Command:   []string{"sh", "-c", cmd},
			RequestId: id,
		}
	}

	synctest.Test(t, func(t *testing.T) {
		tbl := newIdempotencyTable(2, 10*time.Second)

		e1, exists, err := tbl.getOrCreate("k1", reqFor("k1", "echo slow")) // started at t=0
		if err != nil || exists {
			t.Fatalf("getOrCreate(k1): exists=%v err=%v", exists, err)
		}

		time.Sleep(time.Second) // t=1s
		e2, exists, err := tbl.getOrCreate("k2", reqFor("k2", "echo fast"))
		if err != nil || exists {
			t.Fatalf("getOrCreate(k2): exists=%v err=%v", exists, err)
		}
		tbl.complete(e2, "proc-2", nil) // expires at t=11s

		time.Sleep(3 * time.Second)     // t=4s
		tbl.complete(e1, "proc-1", nil) // expires at t=14s; must move ahead of k2

		time.Sleep(8 * time.Second) // t=12s: k2 expired (11s), k1 still live (14s)
		e3, exists, err := tbl.getOrCreate("k3", reqFor("k3", "echo third"))
		if err != nil || exists {
			t.Fatalf("getOrCreate(k3): exists=%v err=%v", exists, err)
		}
		tbl.complete(e3, "proc-3", nil)

		if got, hit, err := tbl.getOrCreate("k1", reqFor("k1", "echo slow")); err != nil || !hit || got.processID != "proc-1" {
			t.Fatalf("retry k1 at t=12s: hit=%v id=%q err=%v, want cached proc-1 (expired k2 must be pruned instead of evicting live k1)", hit, func() string {
				if got == nil {
					return ""
				}
				return got.processID
			}(), err)
		}
	})
}

// TestIdempotencyTable_CapacityLoopPreservesInFlightEntry checks WF-13: when
// the table is at capacity and every stored entry is still inFlight, inserting
// another key must not evict an inFlight entry, preserving deduplication while
// the first call is in progress.
func TestIdempotencyTable_CapacityLoopPreservesInFlightEntry(t *testing.T) {
	reqFor := func(id, cmd string) *ateenvv1alpha.StartProcessRequest {
		return &ateenvv1alpha.StartProcessRequest{
			Command:   []string{"sh", "-c", cmd},
			RequestId: id,
		}
	}

	tbl := newIdempotencyTable(1, 10*time.Second)

	e1, hit, err := tbl.getOrCreate("k1", reqFor("k1", "echo 1"))
	if err != nil || hit {
		t.Fatalf("getOrCreate(k1): hit=%v err=%v", hit, err)
	}

	// k1 is still inFlight when k2 arrives on a capacity-1 table.
	e2, hit, err := tbl.getOrCreate("k2", reqFor("k2", "echo 2"))
	if err != nil || hit {
		t.Fatalf("getOrCreate(k2): hit=%v err=%v", hit, err)
	}

	// A duplicate of k1 while k1 is in progress must still hit e1 rather than starting a second run.
	dup1, hit, err := tbl.getOrCreate("k1", reqFor("k1", "echo 1"))
	if err != nil || !hit || dup1 != e1 {
		t.Fatalf("duplicate getOrCreate(k1) while inFlight: hit=%v sameEntry=%v err=%v, want hit=true on e1", hit, dup1 == e1, err)
	}

	tbl.complete(e1, "proc-1", nil)
	tbl.complete(e2, "proc-2", nil)
	if dup1.processID != "proc-1" {
		t.Fatalf("dup1.processID = %q, want proc-1", dup1.processID)
	}
}
