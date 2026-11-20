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

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

// snapshotWith writes one measurements_snapshots sample holding the given
// cumulative value for every bucket of one ttfi histogram plus its _count.
func snapshotWith(t *testing.T, dir string, ts time.Time, le map[string]float64) {
	t.Helper()
	var series []schema.PrometheusSeries
	var total float64
	for bound, v := range le {
		if bound == "+Inf" {
			total = v
		}
		val := v
		series = append(series, schema.PrometheusSeries{
			Name:   "ate_env_client_ttfi_seconds_bucket",
			Labels: map[string]string{"le": bound, "status": "ok", "source": "cold_create"},
			Status: "ok",
			Value:  &val,
		})
	}
	cnt := total
	series = append(series, schema.PrometheusSeries{
		Name:   "ate_env_client_ttfi_seconds_count",
		Labels: map[string]string{"status": "ok", "source": "cold_create"},
		Status: "ok",
		Value:  &cnt,
	})
	active := 4.0
	m := schema.Measurements{
		SchemaVersion: schema.SchemaVersion,
		RunID:         "snap-run",
		CollectedAt:   ts,
		PromMetrics:   series,
		ServerSummary: map[string]schema.MetricSummary{
			"active_sandboxes": {Count: 1, Sum: active, Min: active, Max: active, P50: active, P90: active, P95: active, P99: active, Unit: "sandboxes"},
		},
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "measurements_snapshots"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "measurements_snapshots", ts.Format("20060102T150405.000Z")+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReportRollsUpSnapshotBucketSeriesByLabels(t *testing.T) {
	dir := t.TempDir()
	// A trace-derived ttfi of 500 ms that the metrics must replace.
	events := `{"run_id":"snap-run","event":"ttfi","env":"e1","dur_ms":500,"ok":true}` + "\n"
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events", "trace.jsonl"), []byte(events), 0o644); err != nil {
		t.Fatal(err)
	}
	meas := schema.Measurements{SchemaVersion: schema.SchemaVersion, RunID: "snap-run", CollectedAt: time.Date(2026, 11, 5, 16, 0, 0, 0, time.UTC)}
	raw, _ := json.MarshalIndent(meas, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "measurements.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	// Two samples: all four le series grow, so the window holds 10 observations
	// with p50 at the 0.5 bucket boundary... 2 in <=0.25, 5 in <=0.5, 10 in <=1.
	t0 := time.Date(2026, 11, 5, 16, 0, 10, 0, time.UTC)
	snapshotWith(t, dir, t0, map[string]float64{"0.25": 0, "0.5": 0, "1": 0, "+Inf": 0})
	snapshotWith(t, dir, t0.Add(30*time.Second), map[string]float64{"0.25": 2, "0.5": 5, "1": 10, "+Inf": 10})

	var out, errBuf bytes.Buffer
	if code := execute([]string{"report", "-dest", dir}, &out, &errBuf); code != 0 {
		t.Fatalf("bench report failed (code=%d): %s", code, errBuf.String())
	}
	rawMeas, err := os.ReadFile(filepath.Join(dir, "measurements.json"))
	if err != nil {
		t.Fatal(err)
	}
	rolled, err := schema.DecodeJSONStrict[schema.Measurements](bytes.NewReader(rawMeas))
	if err != nil {
		t.Fatal(err)
	}
	buckets := 0
	for _, s := range rolled.PromMetrics {
		if s.Name == "ate_env_client_ttfi_seconds_bucket" {
			buckets++
			if len(s.Values) != 2 {
				t.Fatalf("bucket %s must carry both samples, got %v", s.Labels["le"], s.Values)
			}
		}
	}
	if buckets != 4 {
		t.Fatalf("rolled-up measurements hold %d ttfi bucket series, want 4 (one per le)", buckets)
	}
	rep, err := swebench.LoadOrSummarizeReport(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	st, ok := rep.StatFor(swebench.EventTTFI, "")
	if !ok || st.Count != 10 {
		t.Fatalf("ttfi stat = %+v ok=%v, want the 10 metric observations, not the single trace event", st, ok)
	}
	// p50 = rank 5 of 10 lands exactly on the 0.5 bucket boundary.
	if st.P50MS != 500 {
		t.Fatalf("ttfi p50 = %v, want 500 from the bucket quantile", st.P50MS)
	}
	if st.P95MS <= 500 || st.P95MS > 1000 {
		t.Fatalf("ttfi p95 = %v, want within the (0.5, 1] bucket", st.P95MS)
	}
}

func TestReportRollsUpNewInWindowMaxAndMinGauges(t *testing.T) {
	type snapSample struct {
		present bool
		count   float64
		maxSec  float64
		minSec  float64
	}
	cases := []struct {
		name    string
		samples [3]snapSample
		wantMax float64
		wantMin float64
	}{
		{
			name: "new_in_sample_2_flat_extremes",
			samples: [3]snapSample{
				{present: false},
				{present: true, count: 4, maxSec: 2.0, minSec: 0.5},
				{present: true, count: 4, maxSec: 2.0, minSec: 0.5},
			},
			wantMax: 2000,
			wantMin: 500,
		},
		{
			name: "new_in_sample_3_only",
			samples: [3]snapSample{
				{present: false},
				{present: false},
				{present: true, count: 4, maxSec: 2.0, minSec: 0.5},
			},
			wantMax: 2000,
			wantMin: 500,
		},
		{
			name: "new_in_sample_2_decreasing_min",
			samples: [3]snapSample{
				{present: false},
				{present: true, count: 2, maxSec: 2.0, minSec: 0.5},
				{present: true, count: 4, maxSec: 2.0, minSec: 0.3},
			},
			wantMax: 2000,
			wantMin: 300,
		},
		{
			name: "long_lived_flat_extremes_stay_zero",
			samples: [3]snapSample{
				{present: true, count: 10, maxSec: 40.0, minSec: 0.2},
				{present: true, count: 12, maxSec: 40.0, minSec: 0.2},
				{present: true, count: 14, maxSec: 40.0, minSec: 0.2},
			},
			wantMax: 0,
			wantMin: 0,
		},
		{
			name: "long_lived_moving_extremes_accepted",
			samples: [3]snapSample{
				{present: true, count: 10, maxSec: 40.0, minSec: 0.2},
				{present: true, count: 12, maxSec: 45.0, minSec: 0.1},
				{present: true, count: 14, maxSec: 45.0, minSec: 0.1},
			},
			wantMax: 45000,
			wantMin: 100,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			events := `{"run_id":"snap-run","event":"task","env":"e1","dur_ms":100,"ok":true}` + "\n"
			if err := os.MkdirAll(filepath.Join(dir, "events"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "events", "trace.jsonl"), []byte(events), 0o644); err != nil {
				t.Fatal(err)
			}
			t0 := time.Date(2026, 11, 5, 16, 0, 0, 0, time.UTC)
			baseMeas := schema.Measurements{SchemaVersion: schema.SchemaVersion, RunID: "snap-run", CollectedAt: t0}
			rawBase, err := json.MarshalIndent(baseMeas, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "measurements.json"), rawBase, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(dir, "measurements_snapshots"), 0o755); err != nil {
				t.Fatal(err)
			}

			lbls := map[string]string{"status": "ok", "source": "warm_pool"}
			for i, sp := range tc.samples {
				ts := t0.Add(time.Duration(i+1) * 10 * time.Second)
				var series []schema.PrometheusSeries
				active := 1.0
				series = append(series, schema.PrometheusSeries{
					Name:   "ate_env_fleet_warm_replicas",
					Labels: map[string]string{"backend": "substrate"},
					Status: "ok",
					Value:  &active,
				})
				if sp.present {
					cnt := sp.count
					mx := sp.maxSec
					mn := sp.minSec
					series = append(series,
						schema.PrometheusSeries{
							Name:   "ate_env_client_ready_to_first_command_seconds_bucket",
							Labels: map[string]string{"le": "1.0", "status": "ok", "source": "warm_pool"},
							Status: "ok",
							Value:  &cnt,
						},
						schema.PrometheusSeries{
							Name:   "ate_env_client_ready_to_first_command_seconds_bucket",
							Labels: map[string]string{"le": "+Inf", "status": "ok", "source": "warm_pool"},
							Status: "ok",
							Value:  &cnt,
						},
						schema.PrometheusSeries{
							Name:   "ate_env_client_ready_to_first_command_seconds_count",
							Labels: lbls,
							Status: "ok",
							Value:  &cnt,
						},
						schema.PrometheusSeries{
							Name:   "ate_env_client_ready_to_first_command_max_seconds",
							Labels: lbls,
							Status: "ok",
							Value:  &mx,
						},
						schema.PrometheusSeries{
							Name:   "ate_env_client_ready_to_first_command_min_seconds",
							Labels: lbls,
							Status: "ok",
							Value:  &mn,
						},
					)
				}
				snap := schema.Measurements{
					SchemaVersion: schema.SchemaVersion,
					RunID:         "snap-run",
					CollectedAt:   ts,
					PromMetrics:   series,
				}
				rawSnap, err := json.MarshalIndent(snap, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				snapPath := filepath.Join(dir, "measurements_snapshots", ts.Format("20060102T150405.000Z")+".json")
				if err := os.WriteFile(snapPath, rawSnap, 0o644); err != nil {
					t.Fatal(err)
				}
			}

			var out, errBuf bytes.Buffer
			if code := execute([]string{"report", "-dest", dir}, &out, &errBuf); code != 0 {
				t.Fatalf("bench report failed (code=%d): %s", code, errBuf.String())
			}
			rawMeas, err := os.ReadFile(filepath.Join(dir, "measurements.json"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := schema.DecodeJSONStrict[schema.Measurements](bytes.NewReader(rawMeas)); err != nil {
				t.Fatalf("rolled measurements.json failed strict decode: %v", err)
			}
			rep, err := swebench.LoadOrSummarizeReport(filepath.Join(dir, "report.json"))
			if err != nil {
				t.Fatal(err)
			}
			got, ok := rep.CustomMetrics["ready_to_first_cmd_ms"]
			if !ok {
				t.Fatalf("ready_to_first_cmd_ms missing from CustomMetrics: %+v", rep.CustomMetrics)
			}
			if got.Max != tc.wantMax || got.Min != tc.wantMin {
				t.Fatalf("ready_to_first_cmd_ms Max=%v Min=%v, want Max=%v Min=%v", got.Max, got.Min, tc.wantMax, tc.wantMin)
			}
		})
	}
}
