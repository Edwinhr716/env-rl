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

package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

func TestEmbeddedSchemasResolve(t *testing.T) {
	schemas := []string{
		"scenario.schema.json",
		"schedule.schema.json",
		"placement.schema.json",
		"event.schema.json",
		"run_window.schema.json",
		"measurements.schema.json",
		"node_cache.schema.json",
		"restore_sample.schema.json",
		"report.schema.json",
		"stats_row.schema.json",
		"system_description.schema.json",
		"trace_event.schema.json",
		"image_placement.schema.json",
		"preflight_coverage.schema.json",
	}
	for _, name := range schemas {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadJSONSchema(name); err != nil {
				t.Fatalf("LoadJSONSchema(%q) failed: %v", name, err)
			}
		})
	}
}

func TestFixturesValidateAndRoundTrip(t *testing.T) {
	t.Run("scenario.yaml", func(t *testing.T) {
		rawYAML := mustRead(t, "testdata/scenario.yaml")
		jsonBytes, err := yaml.YAMLToJSON(rawYAML)
		if err != nil {
			t.Fatalf("yaml.YAMLToJSON: %v", err)
		}
		if err := ValidateAgainstJSONSchema("scenario.schema.json", jsonBytes); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeYAMLStrict[Scenario](rawYAML)
		if err != nil {
			t.Fatalf("DecodeYAMLStrict: %v", err)
		}
		if v.SUT.PoolSizePerImg != 4 || v.SUT.PoolEnabled == nil || !*v.SUT.PoolEnabled || v.SUT.MaxActivePods != 64 {
			t.Errorf("sut pool fields = size %d, enabled %v, max_active_pods %d; want 4, true, 64", v.SUT.PoolSizePerImg, v.SUT.PoolEnabled, v.SUT.MaxActivePods)
		}
		ov, ok := v.Overlays["pool-8"]
		if !ok || ov.WarmPoolSize != 8 || ov.SUT.PoolSizePerImg != 8 || ov.SUT.PoolEnabled == nil || !*ov.SUT.PoolEnabled {
			t.Errorf("overlays[pool-8] = %+v (present %v); want warm_pool_size 8 and sut pool fields 8, true", ov, ok)
		}
		if v.Footing != "warm" || v.Seed != 7 || v.Window.CooldownSteps != 1 || v.Window.WarmupSec != 30 {
			t.Errorf("footing %q, seed %d, window %+v; want warm, 7 and cooldown_steps 1, warmup_sec 30", v.Footing, v.Seed, v.Window)
		}
		// The decoded scenario marshals back to a document the schema accepts:
		// the overlays: entry without an inner name must not come out as
		// name: "".
		roundTripJSON(t, "scenario.schema.json", v)
		marshaledYAML, err := yaml.Marshal(v)
		if err != nil {
			t.Fatalf("yaml.Marshal: %v", err)
		}
		rt, err := DecodeYAMLStrict[Scenario](marshaledYAML)
		if err != nil {
			t.Fatalf("round-trip DecodeYAMLStrict: %v", err)
		}
		if !reflect.DeepEqual(v, rt) {
			t.Fatalf("Scenario round-trip mismatch:\ngot:  %+v\nwant: %+v", rt, v)
		}
	})

	t.Run("schedule.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/schedule.json")
		if err := ValidateAgainstJSONSchema("schedule.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[Schedule](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "schedule.schema.json", v)
	})

	t.Run("scenario with preflight", func(t *testing.T) {
		// The fixture plus a preflight block validates and decodes.
		var scen map[string]any
		if err := yaml.Unmarshal(mustRead(t, "testdata/scenario.yaml"), &scen); err != nil {
			t.Fatal(err)
		}
		scen["preflight"] = map[string]any{"plan": "image-placement.json", "coverage": "runs/pf/preflight-coverage.json"}
		raw, err := json.Marshal(scen)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateAgainstJSONSchema("scenario.schema.json", raw); err != nil {
			t.Fatalf("schema validate with preflight: %v", err)
		}
		v, err := DecodeJSONStrict[Scenario](bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if v.Preflight == nil || v.Preflight.Plan != "image-placement.json" || v.Preflight.Coverage != "runs/pf/preflight-coverage.json" {
			t.Errorf("Preflight = %+v", v.Preflight)
		}
		scen["preflight"] = map[string]any{"plan": "x", "other": 1}
		raw, _ = json.Marshal(scen)
		if err := ValidateAgainstJSONSchema("scenario.schema.json", raw); err == nil {
			t.Error("an unknown preflight key validated")
		}
		// The schema refuses what Scenario.Validate refuses (a preflight
		// with neither path) and an empty path, and takes the footing the
		// preflight block goes with.
		for name, pf := range map[string]map[string]any{
			"empty":          {},
			"empty plan":     {"plan": ""},
			"empty coverage": {"plan": "p.json", "coverage": ""},
		} {
			scen["preflight"] = pf
			raw, _ = json.Marshal(scen)
			if err := ValidateAgainstJSONSchema("scenario.schema.json", raw); err == nil {
				t.Errorf("preflight %s validated", name)
			}
		}
		// Go and the schema agree on footing: warm, cold or absent.
		scen["preflight"] = map[string]any{"coverage": "cov.json"}
		for footing, want := range map[string]bool{"warm": true, "cold": true, "hot": false, "Warm": false, " warm": false} {
			scen["footing"] = footing
			raw, _ = json.Marshal(scen)
			if err := ValidateAgainstJSONSchema("scenario.schema.json", raw); (err == nil) != want {
				t.Errorf("footing %q: schema err = %v, want valid = %v", footing, err, want)
			}
			_, err := DecodeJSONStrict[Scenario](bytes.NewReader(raw))
			if (err == nil) != want {
				t.Errorf("footing %q: Go err = %v, want valid = %v", footing, err, want)
			}
			if !want && (err == nil || !strings.Contains(err.Error(), "footing")) {
				t.Errorf("footing %q: Go err = %v, want it to name footing", footing, err)
			}
		}
		delete(scen, "footing")
		raw, _ = json.Marshal(scen)
		if err := ValidateAgainstJSONSchema("scenario.schema.json", raw); err != nil {
			t.Errorf("no footing: schema err = %v", err)
		}
		if _, err := DecodeJSONStrict[Scenario](bytes.NewReader(raw)); err != nil {
			t.Errorf("no footing: Go err = %v", err)
		}
	})

	t.Run("preflight_coverage.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/preflight_coverage.json")
		if err := ValidateAgainstJSONSchema("preflight_coverage.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[PreflightCoverage](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		roundTripJSON(t, "preflight_coverage.schema.json", v)
	})

	t.Run("preflight_coverage validate bounds", func(t *testing.T) {
		load := func() PreflightCoverage {
			v, err := DecodeJSONStrict[PreflightCoverage](bytes.NewReader(mustRead(t, "testdata/preflight_coverage.json")))
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
		good := load()
		good.Domains[0].Images[0].UndeletedProbes = []string{"pf-x-0-0-1-0"}
		good.Domains[0].Images[0].WaitedMS = 5
		if err := good.Validate(); err != nil {
			t.Errorf("undeleted_probes and waited_ms: %v", err)
		}
		bad := load()
		bad.Domains[0].Images[0].UndeletedProbes = []string{""}
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "empty undeleted probe id") {
			t.Errorf("empty undeleted probe id: err = %v", err)
		}
		for _, w := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			v := load()
			v.Domains[0].Images[0].WaitedMS = w
			if err := v.Validate(); err == nil || !strings.Contains(err.Error(), "waited_ms must be a finite number >= 0") {
				t.Errorf("waited_ms %v: err = %v", w, err)
			}
		}
	})

	t.Run("image_placement.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/image_placement.json")
		if err := ValidateAgainstJSONSchema("image_placement.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[ImagePlacement](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		roundTripJSON(t, "image_placement.schema.json", v)
	})

	t.Run("placement.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/placement.json")
		if err := ValidateAgainstJSONSchema("placement.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[PlacementPlan](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "placement.schema.json", v)
	})

	t.Run("event.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/event.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("event.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[Event](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("len(items) = %d, want 2", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "event.schema.json", item)
		}
	})

	t.Run("run_window.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/run_window.json")
		if err := ValidateAgainstJSONSchema("run_window.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[RunWindow](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "run_window.schema.json", v)
	})

	t.Run("measurements.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/measurements.json")
		if err := ValidateAgainstJSONSchema("measurements.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[Measurements](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "measurements.schema.json", v)
	})

	t.Run("cache_before.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/cache_before.json")
		if err := ValidateAgainstJSONSchema("node_cache.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[NodeCacheArtifact](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "node_cache.schema.json", v)
	})

	t.Run("restore_timings.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/restore_timings.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("restore_sample.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[RestoreSample](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("len(items) = %d, want 2", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "restore_sample.schema.json", item)
		}
	})

	t.Run("report.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/report.json")
		if err := ValidateAgainstJSONSchema("report.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[Report](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "report.schema.json", v)
	})

	t.Run("stats.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/stats.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("stats_row.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[StatsRow](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("len(items) = %d, want 2", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "stats_row.schema.json", item)
		}
	})

	t.Run("system_description.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/system_description.json")
		if err := ValidateAgainstJSONSchema("system_description.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[SystemDescription](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "system_description.schema.json", v)
	})

	t.Run("trace_event.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/trace_event.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("trace_event.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[TraceEvent](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 4 {
			t.Fatalf("len(items) = %d, want 4", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "trace_event.schema.json", item)
		}
	})
}

func TestBaselineEventStreamsDecode(t *testing.T) {
	for _, tc := range []struct {
		file      string
		wantCount int
	}{
		{file: "testdata/baseline-eval.jsonl", wantCount: 29},
		{file: "testdata/baseline-steps.jsonl", wantCount: 58},
	} {
		t.Run(tc.file, func(t *testing.T) {
			raw := mustRead(t, tc.file)
			if err := ValidateJSONLAgainstJSONSchema("event.schema.json", bytes.NewReader(raw)); err != nil {
				t.Fatalf("ValidateJSONLAgainstJSONSchema(%s): %v", tc.file, err)
			}
			events, err := DecodeJSONLStrict[Event](bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("DecodeJSONLStrict(%s): %v", tc.file, err)
			}
			if len(events) != tc.wantCount {
				t.Fatalf("len(events) = %d, want %d", len(events), tc.wantCount)
			}
			for i, ev := range events {
				data, err := json.Marshal(ev)
				if err != nil {
					t.Fatalf("event %d marshal: %v", i, err)
				}
				if err := ValidateAgainstJSONSchema("event.schema.json", data); err != nil {
					t.Fatalf("event %d re-validate: %v", i, err)
				}
			}
		})
	}
}

// An overlays: entry takes its name from the map key. An inner name must
// match the key, and an empty key or a key that is not a directory name is
// an error. The JSON schema accepts an entry without a name and requires one
// on a variant.
func TestScenarioOverlaysTakeTheirNameFromTheKey(t *testing.T) {
	const head = `schema_version: v1
name: overlays-by-key
sut:
  backend: substrate
jobs:
  - name: sim
    kind: simulated
    loader_mode: steps
    steps: 1
    dataset: d.jsonl
    trajectory: t.yaml
`
	for _, tc := range []struct {
		name    string
		tail    string
		wantErr string
	}{
		{"no inner name", "overlays:\n  pool-8:\n    warm_pool_size: 8\n", ""},
		{"matching inner name", "overlays:\n  pool-8:\n    name: pool-8\n    warm_pool_size: 8\n", ""},
		{"inner name differs", "overlays:\n  pool-8:\n    name: pool-16\n", "does not match the key"},
		{"empty key", "overlays:\n  \"\":\n    warm_pool_size: 8\n", "empty key"},
		{"key with a path separator", "overlays:\n  ../escaped:\n    warm_pool_size: 8\n", "../escaped"},
		{"variant without a name", "variants:\n  - warm_pool_size: 8\n", "name is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(head + tc.tail)
			v, err := DecodeYAMLStrict[Scenario](raw)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("DecodeYAMLStrict: %v", err)
				}
				if ov, ok := v.Overlays["pool-8"]; !ok || ov.WarmPoolSize != 8 {
					t.Errorf("overlays[pool-8] = %+v (present %v), want warm_pool_size 8", ov, ok)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("DecodeYAMLStrict err = %v, want one that holds %q", err, tc.wantErr)
			}
			jsonBytes, err := yaml.YAMLToJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			schemaErr := ValidateAgainstJSONSchema("scenario.schema.json", jsonBytes)
			switch tc.name {
			case "empty key", "variant without a name":
				if schemaErr == nil {
					t.Errorf("scenario.schema.json accepted the %s", tc.name)
				}
			case "no inner name", "matching inner name":
				if schemaErr != nil {
					t.Errorf("scenario.schema.json rejected the %s: %v", tc.name, schemaErr)
				}
			}
		})
	}
}

// FeatureOverlay.Validate and scenario.schema.json reject the same overlay
// values: an unknown prewarm_policy or snapshot_mode, a negative
// warm_pool_size or lookahead_steps, an invalid sut, and an empty env_vars or
// flags name. Before, Validate let prewarm_policy: "off" and
// warm_pool_size: -4 through and bench run -overlay dropped them.
func TestFeatureOverlayInvalidValuesFailGoAndSchemaAlike(t *testing.T) {
	const head = `schema_version: v1
name: overlay-values
footing: warm
sut:
  backend: substrate
jobs:
  - name: sim
    kind: simulated
    loader_mode: steps
    steps: 1
    dataset: d.jsonl
    trajectory: t.yaml
overlays:
  probe:
`
	for _, tc := range []struct {
		name    string
		tail    string
		wantErr string
	}{
		{"prewarm_policy off", "    prewarm_policy: \"off\"\n", "prewarm_policy"},
		{"snapshot_mode sometimes", "    snapshot_mode: sometimes\n", "snapshot_mode"},
		{"negative warm_pool_size", "    warm_pool_size: -4\n", "warm_pool_size"},
		{"negative lookahead_steps", "    lookahead_steps: -1\n", "lookahead_steps"},
		{"sut backend", "    sut:\n      backend: bogus\n", "backend"},
		{"sut replicas", "    sut:\n      replicas: -1\n", "replicas"},
		{"sut pool_size_per_image", "    sut:\n      pool_size_per_image: -4\n", "pool_size_per_image"},
		{"sut max_active_pods", "    sut:\n      max_active_pods: -1\n", "max_active_pods"},
		{"empty env_vars name", "    env_vars:\n      \"\": x\n", "env_vars"},
		{"empty flags name", "    flags:\n      \"\": x\n", "flags"},
		{"unknown park", "    park: bogus\n", "park policy"},
		{"unknown park_policy", "    park_policy: bogus\n", "park policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(head + tc.tail)
			if _, err := DecodeYAMLStrict[Scenario](raw); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("DecodeYAMLStrict err = %v, want one that holds %q", err, tc.wantErr)
			}
			jsonBytes, err := yaml.YAMLToJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateAgainstJSONSchema("scenario.schema.json", jsonBytes); err == nil {
				t.Errorf("scenario.schema.json accepted the %s", tc.name)
			}
		})
	}
	t.Run("the documented values pass both", func(t *testing.T) {
		tails := []string{
			"    prewarm_policy: warm\n",
			"    prewarm_policy: cold\n",
			"    prewarm_policy: none\n",
			"    snapshot_mode: sync\n",
			"    snapshot_mode: async\n",
			"    snapshot_mode: none\n",
			"    snapshot_mode: \"off\"\n",
			"    snapshot_mode: disabled\n",
			"    warm_pool_size: 0\n    lookahead_steps: 0\n",
			"    sut:\n      backend: agent-sandbox\n      replicas: 1\n",
			"    sut:\n      pool_size_per_image: 0\n      max_active_pods: 0\n",
		}
		// park and park_policy take each value NormalizeParkPolicy takes.
		for _, park := range []string{"suspend-async", "suspend-sync", "fork", "delete", "none", "suspend", "hold", "snapshot", "release"} {
			if _, err := NormalizeParkPolicy(ParkPolicy(park)); err != nil {
				t.Fatalf("NormalizeParkPolicy(%q): %v", park, err)
			}
			tails = append(tails, "    park: "+park+"\n", "    park_policy: "+park+"\n")
		}
		for _, tail := range tails {
			raw := []byte(head + tail)
			if _, err := DecodeYAMLStrict[Scenario](raw); err != nil {
				t.Errorf("DecodeYAMLStrict(%q): %v", strings.TrimSpace(tail), err)
			}
			jsonBytes, err := yaml.YAMLToJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateAgainstJSONSchema("scenario.schema.json", jsonBytes); err != nil {
				t.Errorf("scenario.schema.json rejected %q: %v", strings.TrimSpace(tail), err)
			}
		}
	})
	// The base sut follows the same rules as an overlay's sut.
	t.Run("negative counts on the base sut", func(t *testing.T) {
		for _, field := range []string{"replicas", "pool_size_per_image", "max_active_pods"} {
			raw := []byte(strings.Replace(head, "  backend: substrate\n", "  backend: substrate\n  "+field+": -4\n", 1) + "    warm_pool_size: 1\n")
			if _, err := DecodeYAMLStrict[Scenario](raw); err == nil || !strings.Contains(err.Error(), field) {
				t.Errorf("DecodeYAMLStrict with sut.%s: -4 err = %v, want one that holds %q", field, err, field)
			}
			jsonBytes, err := yaml.YAMLToJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateAgainstJSONSchema("scenario.schema.json", jsonBytes); err == nil {
				t.Errorf("scenario.schema.json accepted sut.%s: -4", field)
			}
		}
	})
	t.Run("Validate on the struct", func(t *testing.T) {
		for _, tc := range []struct {
			ov      FeatureOverlay
			wantErr string
		}{
			{FeatureOverlay{Name: "p", PrewarmPolicy: "off"}, "prewarm_policy"},
			{FeatureOverlay{Name: "p", SnapshotMode: "sometimes"}, "snapshot_mode"},
			{FeatureOverlay{Name: "p", WarmPoolSize: -4}, "warm_pool_size"},
			{FeatureOverlay{Name: "p", Lookahead: -1}, "lookahead_steps"},
			{FeatureOverlay{Name: "p", SUT: SUTSpec{Access: "bogus"}}, "sut"},
			{FeatureOverlay{Name: "p", SUT: SUTSpec{PoolSizePerImg: -4}}, "pool_size_per_image"},
			{FeatureOverlay{Name: "p", SUT: SUTSpec{MaxActivePods: -1}}, "max_active_pods"},
		} {
			if err := tc.ov.Validate(); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("FeatureOverlay%+v.Validate() = %v, want an error that holds %q", tc.ov, err, tc.wantErr)
			}
		}
	})
}

func TestStrictDecodeRejectsUnknownField(t *testing.T) {
	t.Run("JSON unknown field", func(t *testing.T) {
		payload := `{"schema_version":"v1","run_id":"r1","start_ts":"2026-10-22T01:20:00Z","end_ts":"2026-10-22T01:25:00Z","bogus":123}`
		if _, err := DecodeJSONStrict[RunWindow](strings.NewReader(payload)); err == nil {
			t.Fatal("expected error for unknown JSON field, got nil")
		}
		if err := ValidateAgainstJSONSchema("run_window.schema.json", []byte(payload)); err == nil {
			t.Fatal("expected JSON Schema error for additional property, got nil")
		}
	})

	t.Run("YAML unknown field", func(t *testing.T) {
		payload := []byte(`
schema_version: v1
scenario_name: test
unknown_top_level: true
jobs: []
`)
		if _, err := DecodeYAMLStrict[Schedule](payload); err == nil {
			t.Fatal("expected error for unknown YAML field, got nil")
		}
	})
}

func TestStrictDecodeRejectsTrailingPayload(t *testing.T) {
	payload := `{"schema_version":"v1","run_id":"r1","start_ts":"2026-10-22T01:20:00Z","end_ts":"2026-10-22T01:25:00Z"} {"extra":true}`
	if _, err := DecodeJSONStrict[RunWindow](strings.NewReader(payload)); err == nil {
		t.Fatal("expected error for trailing JSON token, got nil")
	}
	if err := ValidateAgainstJSONSchema("run_window.schema.json", []byte(payload)); err == nil {
		t.Fatal("expected ValidateAgainstJSONSchema error for trailing JSON token, got nil")
	}
}

func TestJSONSchemaRejectsMissingRequiredAndUnknownField(t *testing.T) {
	cases := []struct {
		name       string
		schemaName string
		payload    string
	}{
		{
			name:       "schedule missing jobs",
			schemaName: "schedule.schema.json",
			payload:    `{"schema_version":"v1","scenario_name":"s"}`,
		},
		{
			name:       "placement missing env_id",
			schemaName: "placement.schema.json",
			payload:    `{"schema_version":"v1","scenario_name":"s","jobs":[{"job":"j","assignments":[{"step":1,"instance_id":"i"}]}]}`,
		},
		{
			name:       "event missing dur_ms",
			schemaName: "event.schema.json",
			payload:    `{"ts":"2026-10-22T01:23:23Z","run":"r","event":"create","ok":true}`,
		},
		{
			name:       "event invalid source enum",
			schemaName: "event.schema.json",
			payload:    `{"ts":"2026-10-22T01:23:23Z","run":"r","event":"create","dur_ms":1,"ok":true,"source":"invalid_source"}`,
		},
		{
			name:       "run_window missing end_ts",
			schemaName: "run_window.schema.json",
			payload:    `{"schema_version":"v1","run_id":"r","start_ts":"2026-10-22T01:20:00Z"}`,
		},
		{
			name:       "report missing validity",
			schemaName: "report.schema.json",
			payload:    `{"schema_version":"v1","run_id":"r","scenario_name":"s","generated_at":"2026-10-22T01:20:00Z"}`,
		},
		{
			name:       "stats_row empty measurements",
			schemaName: "stats_row.schema.json",
			payload:    `{"schema_version":"v1","timestamp":"2026-10-22T01:20:00Z","test_name":"t","metric":"m","measurements":{}}`,
		},
		{
			name:       "system_description missing cluster",
			schemaName: "system_description.schema.json",
			payload:    `{"schema_version":"v1"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateAgainstJSONSchema(tc.schemaName, []byte(tc.payload)); err == nil {
				t.Fatalf("expected JSON Schema error for %s, got nil", tc.name)
			}
		})
	}
}

func TestJSONLRejectsMalformedLineWithLineNumber(t *testing.T) {
	input := strings.Join([]string{
		`{"ts":"2026-10-22T01:23:23Z","run":"r","event":"create","dur_ms":10,"ok":true}`,
		`{"ts":"not-a-timestamp","run":"r","event":"create","dur_ms":10,"ok":true}`,
	}, "\n")
	_, err := DecodeJSONLStrict[Event](strings.NewReader(input))
	if err == nil {
		t.Fatal("expected error on line 2, got nil")
	}
	if !strings.Contains(err.Error(), "line 2:") {
		t.Fatalf("expected error to mention 'line 2:', got %v", err)
	}
}

func TestParkPolicyNormalization(t *testing.T) {
	for _, tc := range []struct {
		in      ParkPolicy
		want    ParkPolicy
		wantErr bool
	}{
		{in: "", want: ParkSuspendSync},
		{in: ParkSuspend, want: ParkSuspendSync},
		{in: ParkSuspendSync, want: ParkSuspendSync},
		{in: ParkSuspendAsync, want: ParkSuspendAsync},
		{in: ParkFork, want: ParkFork},
		{in: ParkDelete, want: ParkDelete},
		{in: ParkNone, want: ParkNone},
		{in: "bogus", wantErr: true},
	} {
		got, err := NormalizeParkPolicy(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("NormalizeParkPolicy(%q) succeeded, want error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("NormalizeParkPolicy(%q) failed: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("NormalizeParkPolicy(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestValidationRules(t *testing.T) {
	t.Run("RunWindow end before start", func(t *testing.T) {
		rw := RunWindow{
			SchemaVersion: SchemaVersion,
			RunID:         "r1",
			StartTS:       "2026-10-22T01:25:00Z",
			EndTS:         "2026-10-22T01:20:00Z",
		}
		if err := rw.Validate(); err == nil {
			t.Fatal("expected error when end_ts < start_ts")
		}
	})

	t.Run("Report invalid without reasons", func(t *testing.T) {
		rep := Report{
			SchemaVersion: SchemaVersion,
			RunID:         "r1",
			ScenarioName:  "s1",
			GeneratedAt:   "2026-10-22T01:25:00Z",
			Validity:      ReportValidity{Valid: false},
		}
		if err := rep.Validate(); err == nil {
			t.Fatal("expected error when validity.valid=false and reasons is empty")
		}
	})

	t.Run("StatsRow NaN measurement", func(t *testing.T) {
		row := StatsRow{
			SchemaVersion: SchemaVersion,
			Timestamp:     "2026-10-22T01:25:00Z",
			TestName:      "t",
			Metric:        "m",
			Measurements:  map[string]float64{"x": math.NaN()},
		}
		if err := row.Validate(); err == nil {
			t.Fatal("expected error for NaN measurement")
		}
	})

	t.Run("TraceEvent invalid op", func(t *testing.T) {
		ev := TraceEvent{
			SchemaVersion: SchemaVersion,
			TS:            time.Now().UTC(),
			Env:           "sb-1",
			Op:            "bogus_op",
			DurMS:         10,
			OK:            true,
		}
		if err := ev.Validate(); err == nil {
			t.Fatal("expected error for invalid trace op")
		}
	})

	t.Run("JobSpec and PrometheusSeries invariants", func(t *testing.T) {
		simSteps := JobSpec{
			Name:       "sim-steps",
			Kind:       "simulated",
			LoaderMode: "steps",
			Dataset:    "d.jsonl",
			Trajectory: "t.yaml",
			Steps:      0,
		}
		if err := simSteps.Validate(); err == nil {
			t.Fatal("expected simulated steps job with steps=0 to fail validation")
		}
		if _, err := DecodeJSONStrict[PrometheusSeries](strings.NewReader(`{"name":"m1","status":"ok"}`)); err == nil {
			t.Fatal("expected PrometheusSeries with status=ok and no value to fail")
		}
		if _, err := DecodeJSONStrict[PrometheusSeries](strings.NewReader(`{"name":"m1","status":"error","err":"connection refused"}`)); err != nil {
			t.Fatalf("expected PrometheusSeries with status=error and err message to pass, got: %v", err)
		}
	})
}

func TestPythonSchemaValidatorCLI(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found on PATH")
	}

	t.Run("valid trace_event.jsonl", func(t *testing.T) {
		cmd := exec.Command(py, "validate_schema.py", "--schema", "trace_event", "--jsonl", "testdata/trace_event.jsonl")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("validate_schema.py failed on valid trace_event.jsonl: %v\n%s", err, string(out))
		}
	})

	t.Run("valid schedule.json", func(t *testing.T) {
		cmd := exec.Command(py, "validate_schema.py", "--schema", "schedule", "testdata/schedule.json")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("validate_schema.py failed on valid schedule.json: %v\n%s", err, string(out))
		}
	})

	// stdlib runs validate_schema.py with the jsonschema package hidden, so
	// the subtests below exercise the fallback wherever they run.
	stdlib := func(args ...string) *exec.Cmd {
		const hide = `import runpy, sys; sys.modules["jsonschema"] = None; sys.argv[0] = "validate_schema.py"; runpy.run_path("validate_schema.py", run_name="__main__")`
		return exec.Command(py, append([]string{"-c", hide}, args...)...)
	}
	scenarioWithJob := func(t *testing.T, job string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "scenario.json")
		doc := `{"schema_version":"v1","name":"s","sut":{},"jobs":[` + job + `]}`
		if err := os.WriteFile(p, []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// A job needs kind or mode (anyOf in $defs/JobSpec). The fallback and
	// the Go validator must give the same verdict.
	for _, tc := range []struct {
		name, job string
		valid     bool
	}{
		{"kind only", `{"name":"j1","kind":"real","real_mode":"measure"}`, true},
		{"mode only", `{"name":"j1","mode":"closed-loop"}`, true},
		{"kind and mode", `{"name":"j1","kind":"simulated","mode":"closed-loop"}`, true},
		{"no kind or mode", `{"name":"j1"}`, false},
	} {
		t.Run("stdlib fallback scenario job: "+tc.name, func(t *testing.T) {
			p := scenarioWithJob(t, tc.job)
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if goErr := ValidateAgainstJSONSchema("scenario.schema.json", raw); (goErr == nil) != tc.valid {
				t.Fatalf("Go validator: valid=%v, want %v (%v)", goErr == nil, tc.valid, goErr)
			}
			out, err := stdlib("--schema", "scenario", p).CombinedOutput()
			if (err == nil) != tc.valid {
				t.Fatalf("stdlib fallback: valid=%v, want %v\n%s", err == nil, tc.valid, out)
			}
			if !tc.valid && !strings.Contains(string(out), "anyOf") {
				t.Errorf("the error does not name anyOf:\n%s", out)
			}
		})
	}

	t.Run("SCHEMA_MAP names each v1 schema", func(t *testing.T) {
		out, err := exec.Command(py, "-B", "-c", `import validate_schema as v; print("\n".join(sorted(v.SCHEMA_MAP.values())))`).Output()
		if err != nil {
			t.Fatalf("list SCHEMA_MAP: %v", err)
		}
		mapped := map[string]bool{}
		for _, f := range strings.Fields(string(out)) {
			mapped[f] = true
		}
		files, err := filepath.Glob(filepath.Join("v1", "*.schema.json"))
		if err != nil || len(files) == 0 {
			t.Fatalf("glob v1 schemas: %v (%d files)", err, len(files))
		}
		for _, f := range files {
			if !mapped[filepath.Base(f)] {
				t.Errorf("SCHEMA_MAP has no name for %s", filepath.Base(f))
			}
		}
	})

	t.Run("rejects unknown field", func(t *testing.T) {
		tmp := t.TempDir()
		badPath := filepath.Join(tmp, "bad.jsonl")
		if err := os.WriteFile(badPath, []byte(`{"schema_version":"v1","ts":"2026-10-22T01:20:01Z","env":"sb-1","op":"create","dur_ms":10,"ok":true,"bogus":1}`+"\n"), 0o644); err != nil {
			t.Fatalf("write bad.jsonl: %v", err)
		}
		cmd := exec.Command(py, "validate_schema.py", "--schema", "trace_event", "--jsonl", badPath)
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("expected validate_schema.py to fail on unknown field, got success:\n%s", string(out))
		}
	})

	// propertyNames reaches the stdlib fallback too, so an empty overlay
	// env_vars or flags name fails the CLI as it fails Validate.
	t.Run("rejects an empty overlay env_vars name", func(t *testing.T) {
		const scen = `{"schema_version":"v1","name":"s","sut":{"backend":"substrate"},` +
			`"jobs":[{"name":"sim","kind":"simulated","loader_mode":"steps","steps":1,"dataset":"d.jsonl","trajectory":"t.yaml"}],` +
			`"overlays":{"probe":{"env_vars":{%q:"x"}}}}`
		tmp := t.TempDir()
		for _, tc := range []struct {
			key     string
			wantErr bool
		}{{"", true}, {"ATE_PREFETCH", false}} {
			path := filepath.Join(tmp, "scenario.json")
			if err := os.WriteFile(path, []byte(fmt.Sprintf(scen, tc.key)), 0o644); err != nil {
				t.Fatal(err)
			}
			// The jsonschema package and the stdlib fallback word the
			// failure differently; both name the env_vars location.
			for _, cmd := range []*exec.Cmd{
				exec.Command(py, "validate_schema.py", "--schema", "scenario.schema.json", path),
				stdlibValidatorCmd(py, "--schema", "scenario.schema.json", path),
			} {
				out, err := cmd.CombinedOutput()
				if tc.wantErr && (err == nil || !strings.Contains(string(out), "overlays.probe.env_vars")) {
					t.Errorf("env_vars name %q: validate_schema.py err = %v, want a failure at overlays.probe.env_vars:\n%s", tc.key, err, out)
				}
				if !tc.wantErr && err != nil {
					t.Errorf("env_vars name %q: validate_schema.py failed: %v\n%s", tc.key, err, out)
				}
			}
		}
	})

	// A variants entry is checked through allOf (FeatureOverlay plus a
	// required name). The CLI gives the same verdict with the jsonschema
	// package and with the stdlib fallback, which is forced here because
	// the package can be present on the machine.
	t.Run("checks variants entries through allOf", func(t *testing.T) {
		const scen = `{"schema_version":"v1","name":"s","sut":{"backend":"substrate"},` +
			`"jobs":[{"name":"sim","kind":"simulated","loader_mode":"steps","steps":1,"dataset":"d.jsonl","trajectory":"t.yaml"}],` +
			`"variants":[%s]}`
		tmp := t.TempDir()
		for _, tc := range []struct {
			name    string
			variant string
			wantErr string
		}{
			{"no name", `{"prewarm_policy":"warm"}`, "name"},
			{"invalid prewarm_policy", `{"name":"v","prewarm_policy":"off"}`, "off"},
			{"negative warm_pool_size", `{"name":"v","warm_pool_size":-4}`, "-4"},
			{"negative sut pool_size_per_image", `{"name":"v","sut":{"pool_size_per_image":-4}}`, "-4"},
			{"unknown field", `{"name":"v","bogus":1}`, "bogus"},
			{"not an object", `123`, "object"},
			{"valid", `{"name":"v","prewarm_policy":"warm","warm_pool_size":4}`, ""},
		} {
			path := filepath.Join(tmp, "scenario.json")
			if err := os.WriteFile(path, []byte(fmt.Sprintf(scen, tc.variant)), 0o644); err != nil {
				t.Fatal(err)
			}
			for _, mode := range []struct {
				name string
				cmd  *exec.Cmd
			}{
				{"default", exec.Command(py, "validate_schema.py", "--schema", "scenario.schema.json", path)},
				{"stdlib fallback", stdlibValidatorCmd(py, "--schema", "scenario.schema.json", path)},
			} {
				out, err := mode.cmd.CombinedOutput()
				if tc.wantErr != "" && (err == nil || !strings.Contains(string(out), tc.wantErr)) {
					t.Errorf("%s, %s: validate_schema.py err = %v, want a failure that holds %q:\n%s", tc.name, mode.name, err, tc.wantErr, out)
				}
				if tc.wantErr == "" && err != nil {
					t.Errorf("%s, %s: validate_schema.py failed: %v\n%s", tc.name, mode.name, err, out)
				}
			}
		}
	})

	// The stdlib fallback enforces each keyword the v1 schemas use, and a
	// schema with a keyword outside that set is an error, not a skipped rule.
	t.Run("the stdlib fallback covers each v1 schema keyword", func(t *testing.T) {
		const prog = `import glob, json, sys
sys.modules["jsonschema"] = None
import validate_schema as v
bad = {p: v._unsupported_keywords(json.load(open(p))) for p in sorted(glob.glob("v1/*.json"))}
bad = {p: k for p, k in bad.items() if k}
sys.exit(f"keywords without a stdlib rule: {bad}" if bad else 0)`
		if out, err := exec.Command(py, "-c", prog).CombinedOutput(); err != nil {
			t.Errorf("v1 schemas: %v\n%s", err, out)
		}

		tmp := t.TempDir()
		write := func(name, body string) string {
			path := filepath.Join(tmp, name)
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			return path
		}
		unknown := write("unknown.schema.json", `{"type":"object","properties":{"a":{"type":"string","pattern":"^x"}}}`)
		out, err := stdlibValidatorCmd(py, "--schema", unknown, write("a.json", `{"a":"y"}`)).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "#/properties/a/pattern") {
			t.Errorf("schema with pattern: err = %v, want a failure that names #/properties/a/pattern:\n%s", err, out)
		}
		// The walk follows anyOf branches as it follows allOf.
		inAnyOf := write("anyof.schema.json", `{"anyOf":[{"type":"string","pattern":"^x"},{"type":"integer"}]}`)
		out, err = stdlibValidatorCmd(py, "--schema", inAnyOf, write("b.json", `1`)).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "#/anyOf/0/pattern") {
			t.Errorf("schema with pattern under anyOf: err = %v, want a failure that names #/anyOf/0/pattern:\n%s", err, out)
		}

		// minProperties: stats_row.measurements and image_placement selector.
		row := `{"schema_version":"v1","timestamp":"2026-11-11T00:00:00Z","test_name":"t","metric":"m","measurements":%s}`
		out, err = stdlibValidatorCmd(py, "--schema", "stats_row", write("empty.json", fmt.Sprintf(row, `{}`))).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "minProperties") {
			t.Errorf("stats_row with empty measurements: err = %v, want a minProperties failure:\n%s", err, out)
		}
		if out, err := stdlibValidatorCmd(py, "--schema", "stats_row", write("one.json", fmt.Sprintf(row, `{"p50":1}`))).CombinedOutput(); err != nil {
			t.Errorf("stats_row with one measurement: %v\n%s", err, out)
		}
	})
}

// stdlibValidatorCmd runs validate_schema.py with the jsonschema import
// blocked, thus the stdlib fallback validates also where the package exists.
func stdlibValidatorCmd(py string, args ...string) *exec.Cmd {
	const prog = `import runpy, sys
sys.modules["jsonschema"] = None
sys.argv = ["validate_schema.py", *sys.argv[1:]]
runpy.run_path("validate_schema.py", run_name="__main__")`
	return exec.Command(py, append([]string{"-c", prog}, args...)...)
}

func mustRead(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return b
}

func roundTripJSON[T any](t *testing.T, schemaName string, val T) {
	t.Helper()
	encoded, err := json.Marshal(val)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if err := ValidateAgainstJSONSchema(schemaName, encoded); err != nil {
		t.Fatalf("round-trip ValidateAgainstJSONSchema(%s): %v\npayload: %s", schemaName, err, string(encoded))
	}
	if _, err := DecodeJSONStrict[T](bytes.NewReader(encoded)); err != nil {
		t.Fatalf("round-trip DecodeJSONStrict: %v", err)
	}
}

func TestPreflightCoverageValidateEnforcesStatesAndConsistency(t *testing.T) {
	raw := mustRead(t, "testdata/preflight_coverage.json")
	var base PreflightCoverage
	if err := json.Unmarshal(raw, &base); err != nil {
		t.Fatal(err)
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	cases := map[string]func(c *PreflightCoverage){
		"unknown golden state": func(c *PreflightCoverage) { c.Goldens[0].State = "skipped" },
		"domain complete with an incomplete image": func(c *PreflightCoverage) {
			c.Domains[1].Complete = true
		},
		"coverage complete with a golden not ready": func(c *PreflightCoverage) {
			c.Domains[1].Images[0].Nodes = []string{"a", "b"}
			c.Domains[1].Images[0].Complete = true
			c.Domains[1].Complete = true
			c.Complete = true // goldens[1] is failed
		},
		"empty node": func(c *PreflightCoverage) { c.Domains[0].Images[0].Nodes[0] = "" },
	}
	for name, mutate := range cases {
		var c PreflightCoverage
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
	}

	// Normalize turns nil slices into empty arrays, so the JSON matches the schema.
	c := PreflightCoverage{SchemaVersion: SchemaVersion, RunID: "pf", StartedAt: base.StartedAt, FinishedAt: base.FinishedAt,
		Domains: []DomainCoverage{{Name: "d0", Nodes: 1, Images: []ImageCoverage{{Image: "img", Wanted: 1}}}}}
	c.Normalize()
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAgainstJSONSchema("preflight_coverage.schema.json", out); err != nil {
		t.Errorf("normalized empty coverage does not validate: %v\n%s", err, out)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("normalized empty coverage: %v", err)
	}
}

func TestFaultClassificationSchemaAndValidation(t *testing.T) {
	raw := mustRead(t, "testdata/report.json")
	rep, err := DecodeJSONStrict[Report](bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("DecodeJSONStrict: %v", err)
	}
	rep.FaultClassification = &FaultClassification{
		AllRollouts:       100,
		InfraDropped:      1,
		Tier1Dropped:      1,
		Tier2Policy:       2,
		Tier3Dropped:      0,
		RecoveredRetries:  3,
		PostRolloutErrors: 1,
		InfraFaultRate:    0.01,
		TargetRate:        DefaultInfraFaultRateTarget,
		MeetsTarget:       true,
		Categories: []FaultCategorySummary{
			{
				Tier:       FaultTierBeforeEpisode,
				Category:   FaultCatNoCapacity,
				Dropped:    1,
				Recovered:  0,
				Events:     1,
				ExampleErr: "no worker has room",
			},
			{
				Tier:       FaultTierPolicy,
				Category:   FaultCatNonZeroExit,
				Dropped:    2,
				Recovered:  0,
				Events:     2,
				ExampleErr: "exit 1, expected 0",
			},
			{
				Tier:       FaultTierDuringEpisode,
				Category:   FaultCatWakeResumeFailure,
				Dropped:    0,
				Recovered:  3,
				Events:     0,
				ExampleErr: "wake succeeded after 2 attempts",
			},
		},
	}
	if err := rep.Validate(); err != nil {
		t.Fatalf("Validate valid FaultClassification: %v", err)
	}
	roundTripJSON(t, "report.schema.json", rep)

	badSum := *rep.FaultClassification
	badSum.InfraDropped = 5 // != Tier1Dropped + Tier3Dropped
	if err := badSum.Validate(); err == nil {
		t.Fatal("expected Validate error when infra_dropped != tier1_dropped + tier3_dropped")
	}

	badDrops := *rep.FaultClassification
	badDrops.Tier2Policy = badDrops.AllRollouts // 1 + 100 + 0 > 100
	if err := badDrops.Validate(); err == nil {
		t.Fatal("expected Validate error when the tier drops exceed all_rollouts")
	}

	badTier := *rep.FaultClassification
	badTier.Categories = []FaultCategorySummary{{Tier: "tier4_unknown", Category: "x"}}
	if err := badTier.Validate(); err == nil {
		t.Fatal("expected Validate error for invalid fault tier")
	}

	// The category list is closed: "other" is a member, a made-up name is not.
	withOther := *rep.FaultClassification
	withOther.Categories = append([]FaultCategorySummary(nil), rep.FaultClassification.Categories...)
	withOther.Categories = append(withOther.Categories, FaultCategorySummary{
		Tier: FaultTierDuringEpisode, Category: FaultCatOther, Dropped: 0, Events: 1, ExampleErr: "no space left on device",
	})
	if err := withOther.Validate(); err != nil {
		t.Fatalf("Validate with an other category: %v", err)
	}
	rep.FaultClassification = &withOther
	roundTripJSON(t, "report.schema.json", rep)

	badCat := *rep.FaultClassification
	badCat.Categories = []FaultCategorySummary{{Tier: FaultTierBeforeEpisode, Category: "disk_full"}}
	if err := badCat.Validate(); err == nil {
		t.Fatal("expected Validate error for a category outside the closed list")
	}
}
