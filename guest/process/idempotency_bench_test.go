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
	"strconv"
	"testing"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
)

func BenchmarkStartProcessIdempotencyTable(b *testing.B) {
	req := &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", "pytest -q tests/test_core.py"},
		Cwd:     "/workspace",
		Env: map[string]string{
			"VAR1": "alpha",
			"VAR2": "beta",
			"VAR3": "gamma",
			"VAR4": "delta",
			"VAR5": "epsilon",
			"VAR6": "zeta",
		},
		RequestId: "550e8400-e29b-41d4-a716-446655440000",
	}

	b.Run("FreshKey", func(b *testing.B) {
		tbl := newIdempotencyTable(defaultIdempotencyCapacity, defaultIdempotencyTTL)
		b.ReportAllocs()
		var seq uint64
		for b.Loop() {
			seq++
			key := "req-" + strconv.FormatUint(seq, 10)
			entry, exists, err := tbl.getOrCreate(key, req)
			if err != nil || exists {
				b.Fatalf("getOrCreate(%q): exists=%v err=%v", key, exists, err)
			}
			tbl.complete(entry, "proc-1", nil)
		}
	})

	b.Run("DuplicateHit", func(b *testing.B) {
		tbl := newIdempotencyTable(defaultIdempotencyCapacity, defaultIdempotencyTTL)
		entry, _, err := tbl.getOrCreate(req.GetRequestId(), req)
		if err != nil {
			b.Fatal(err)
		}
		tbl.complete(entry, "proc-1", nil)

		b.ReportAllocs()
		for b.Loop() {
			got, exists, err := tbl.getOrCreate(req.GetRequestId(), req)
			if err != nil || !exists || got.processID != "proc-1" {
				b.Fatalf("unexpected hit result: exists=%v err=%v", exists, err)
			}
		}
	})
}
