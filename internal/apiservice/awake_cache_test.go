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
	"testing"
	"testing/synctest"
	"time"
)

// TestBoundedAwakeCache_RecordCapOnAbsentKeyDoesNotMarkAwake checks WF-10.
func TestBoundedAwakeCache_RecordCapOnAbsentKeyDoesNotMarkAwake(t *testing.T) {
	c := newBoundedAwakeCache(10, time.Minute)
	c.RecordCap("env-absent", "start-request-id")

	if c.Contains("env-absent") {
		t.Fatal("Contains returned true after RecordCap on absent key, want false")
	}
	if !c.HasCap("env-absent", "start-request-id") {
		t.Fatal("HasCap returned false, want true")
	}
}

// TestBoundedAwakeCache_ContainsPreservesExpiryOrder checks WF-10.
func TestBoundedAwakeCache_ContainsPreservesExpiryOrder(t *testing.T) {
	// 1. Capacity eviction evicts the oldest entry (env-1) even if Contains("env-1") was called.
	c := newBoundedAwakeCache(2, time.Minute)
	c.Add("env-1")
	c.Add("env-2")

	if !c.Contains("env-1") {
		t.Fatal("Contains(env-1) = false, want true")
	}
	c.Add("env-3")
	if c.Contains("env-1") {
		t.Fatal("Contains(env-1) = true after capacity eviction, want false")
	}
	if !c.Contains("env-2") {
		t.Fatal("Contains(env-2) = false, want true")
	}
	if !c.Contains("env-3") {
		t.Fatal("Contains(env-3) = false, want true")
	}

	// 2. pruneLocked prunes expired entries from the back even after Contains touched them.
	synctest.Test(t, func(t *testing.T) {
		tc := newBoundedAwakeCache(10, 10*time.Second)
		tc.Add("env-1") // expires at t=10s
		time.Sleep(5 * time.Second)
		tc.Add("env-2") // expires at t=15s
		if !tc.Contains("env-1") {
			t.Fatal("Contains(env-1) at t=5s = false, want true")
		}
		time.Sleep(6 * time.Second) // t=11s: env-1 expired, env-2 still valid
		tc.Add("env-3")             // pruneLocked must prune env-1 without stopping at env-2
		if got := len(tc.entries); got != 2 {
			t.Fatalf("len(entries) after pruneLocked = %d, want 2 (expired env-1 pruned)", got)
		}
	})
}

// TestBoundedAwakeCache_RecordCapDoesNotExtendAwakeTTL checks WF-10.
func TestBoundedAwakeCache_RecordCapDoesNotExtendAwakeTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tc := newBoundedAwakeCache(10, 10*time.Second)
		tc.Add("env-1") // expires at t=10s

		time.Sleep(6 * time.Second) // t=6s
		tc.RecordCap("env-1", "start-request-id")
		if !tc.Contains("env-1") {
			t.Fatal("Contains(env-1) at t=6s = false, want true")
		}
		if !tc.HasCap("env-1", "start-request-id") {
			t.Fatal("HasCap(env-1) at t=6s = false, want true")
		}

		time.Sleep(5 * time.Second) // t=11s: past original 10s TTL
		if tc.Contains("env-1") {
			t.Fatal("Contains(env-1) at t=11s = true, want false (RecordCap must not extend awake TTL)")
		}
	})
}

// TestBoundedAwakeCache_RemoveInvalidatesInFlightGeneration checks WF-12 and WF-13.
func TestBoundedAwakeCache_RemoveInvalidatesInFlightGeneration(t *testing.T) {
	c := newBoundedAwakeCache(10, time.Minute)

	// 1. Key absent when Generation is read, then Remove runs before AddIfGen.
	genCold := c.Generation("env-cold")
	c.Remove("env-cold")
	if c.AddIfGen("env-cold", genCold) {
		t.Fatal("AddIfGen succeeded after Remove on cold key, want false")
	}
	if c.Contains("env-cold") {
		t.Fatal("Contains(env-cold) = true after invalidated AddIfGen, want false")
	}

	// 2. Key present with recorded capability when Generation is read, then Remove
	// clears both awake state and cached capabilities and invalidates AddIfGen.
	c.Add("env-warm")
	c.RecordCap("env-warm", "start-request-id")
	if !c.HasCap("env-warm", "start-request-id") {
		t.Fatal("HasCap(env-warm) = false before Remove, want true")
	}
	genWarm := c.Generation("env-warm")
	c.Remove("env-warm")
	if c.HasCap("env-warm", "start-request-id") {
		t.Fatal("HasCap(env-warm) = true after Remove, want false (Remove must clear capabilities)")
	}
	if c.AddIfGen("env-warm", genWarm) {
		t.Fatal("AddIfGen succeeded after Remove on warm key, want false")
	}
	if c.Contains("env-warm") {
		t.Fatal("Contains(env-warm) = true after invalidated AddIfGen, want false")
	}

	// 3. A subsequent wake that starts after Remove observes the new generation and succeeds.
	genNext := c.Generation("env-warm")
	if !c.AddIfGen("env-warm", genNext) {
		t.Fatal("AddIfGen failed for wake started after Remove, want true")
	}
	if !c.Contains("env-warm") {
		t.Fatal("Contains(env-warm) = false after fresh AddIfGen, want true")
	}
}
