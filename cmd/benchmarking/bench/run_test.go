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
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/env/internal/benchmarking/job"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

func TestExecuteScenarioRunPropagatesAllOverlayFields(t *testing.T) {
	ctx := context.Background()
	prefetchFalse := false
	prefetchTrue := true
	poolEnabled := true

	baseScen := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "overlay-propagation-test",
		Footing:       "warm",
		Seed:          7,
		SUT: schema.SUTSpec{
			Backend:      "substrate",
			Access:       "ate-env-api",
			Endpoint:     "localhost:7777",
			Substrate:    &schema.SubstrateSUTSpec{Atespace: "ate-env", BaseTemplate: "default-template"},
			NodePool:     "base-pool",
			RuntimeClass: "runc",
			Replicas:     2,
			ExtraArgs: map[string]string{
				"log_level": "info",
			},
			Labels: map[string]string{
				"tier": "base",
			},
		},
		Jobs: []schema.JobSpec{
			{
				Name:           "sim-closed",
				Mode:           schema.JobModeClosedLoop,
				Steps:          3,
				BatchSize:      1,
				Concurrency:    1,
				StepPauseMS:    1,
				ParkPolicy:     schema.ParkPolicyHold,
				Prefetch:       true,
				LookaheadSteps: 1,
				SnapshotLoop:   false,
				Instances:      3,
			},
			{
				Name: "real-trainer",
				Mode: schema.JobModeReal,
				TrainerCommand: []string{
					"sh",
					"-c",
					`printf 'step=1 timing_s/step=1.0 env_marker=%s base_keep=%s flag_count=%d %s\n' "$OVERLAY_MARKER" "$BASE_KEEP" "$#" "$*"`,
					"trainer",
				},
				TraceMode: "measure",
				EnvVars: map[string]string{
					"BASE_KEEP":      "1",
					"OVERLAY_MARKER": "0",
				},
			},
		},
		Overlays: map[string]schema.FeatureOverlay{
			"full-override": {
				Name: "full-override",
				// park wins over park_policy when both are set.
				Park:          schema.ParkSuspendAsync,
				ParkPolicy:    "fork",
				Prefetch:      &prefetchTrue,
				Lookahead:     2,
				WarmPoolSize:  24,
				SnapshotMode:  "async",
				PrewarmPolicy: "warm",
				SUT: schema.SUTSpec{
					Backend:        "agent-sandbox",
					Access:         "sdk",
					Endpoint:       "overlay-api:9090",
					Substrate:      &schema.SubstrateSUTSpec{Atespace: "overlay-ns", BaseTemplate: "overlay-tmpl"},
					NodePool:       "overlay-pool",
					RuntimeClass:   "gvisor",
					Replicas:       5,
					MaxActivePods:  64,
					PoolEnabled:    &poolEnabled,
					Namespace:      "overlay-k8s-ns",
					Template:       "overlay-template",
					ServerAddr:     "overlay-server:7443",
					StorageBackend: "gcs",
					ValuesOverlay:  map[string]any{"replicaCount": 3},
					ExtraArgs: map[string]string{
						"api_image":   "gcr.io/ov/api:v2",
						"guest_image": "gcr.io/ov/guest:v2",
						"wake-budget": "9m",
					},
					Labels: map[string]string{
						"overlay": "enabled",
					},
				},
				EnvVars: map[string]string{
					"OVERLAY_MARKER": "9",
				},
				Flags: map[string]string{
					"--enable-fast-restore": "1",
					"custom-limit":          "42",
				},
			},
			"no-prefetch-no-snap": {
				Name:          "no-prefetch-no-snap",
				ParkPolicy:    "delete",
				Prefetch:      &prefetchFalse,
				WarmPoolSize:  0,
				SnapshotMode:  "none",
				PrewarmPolicy: "cold",
			},
		},
	}

	// Build a deterministic 3-step schedule with distinct images per step so Lookahead=2
	// vs Prefetch=false produces a clear difference in prefetch_hint events.
	sched := &schema.Schedule{
		SchemaVersion: schema.SchemaVersion,
		ScenarioName:  baseScen.Name,
		Seed:          baseScen.Seed,
		Jobs: []schema.ScheduledJob{
			{
				Job:        "sim-closed",
				ParkPolicy: schema.ParkNone,
				Steps: []schema.ScheduledStep{
					{
						Step:           1,
						StepPauseMS:    1,
						Tasks:          []schema.ScheduledTask{{InstanceID: "inst-1", Image: "img-1"}},
						PrefetchImages: []string{"img-2"},
					},
					{
						Step:           2,
						StepPauseMS:    1,
						Tasks:          []schema.ScheduledTask{{InstanceID: "inst-2", Image: "img-2"}},
						PrefetchImages: []string{"img-3"},
					},
					{
						Step:        3,
						StepPauseMS: 1,
						Tasks:       []schema.ScheduledTask{{InstanceID: "inst-3", Image: "img-3"}},
					},
				},
			},
		},
	}

	// 1. Run with "full-override" overlay.
	dirFull := filepath.Join(t.TempDir(), "full")
	if _, err := executeScenarioRun(ctx, &baseScen, sched, "full-override", "run-ov-full", "test-cluster", dirFull); err != nil {
		t.Fatalf("executeScenarioRun(full-override): %v", err)
	}

	// Verify baseScen and sched were not mutated in place.
	if baseScen.SUT.Replicas != 2 || baseScen.SUT.Substrate.Atespace != "ate-env" || baseScen.Jobs[1].EnvVars["OVERLAY_MARKER"] != "0" {
		t.Fatalf("baseScen was mutated in place: %+v", baseScen)
	}
	if sched.Jobs[0].ParkPolicy != schema.ParkNone || len(sched.Jobs[0].Steps[0].PrefetchImages) != 1 {
		t.Fatalf("caller sched was mutated in place: %+v", sched.Jobs[0])
	}

	// Verify SUT propagated into sut-manifest.yaml and Flags stayed out of it.
	manifestBytes, err := os.ReadFile(filepath.Join(dirFull, "sut-manifest.yaml"))
	if err != nil {
		t.Fatalf("read sut-manifest.yaml: %v", err)
	}
	manifest := string(manifestBytes)
	for _, want := range []string{
		"name: overlay-ns",
		"replicas: 5",
		"runtimeClassName: gvisor",
		"cloud.google.com/gke-nodepool: overlay-pool",
		"image: gcr.io/ov/api:v2",
		"-log_level=info",
		"-wake-budget=9m",
	} {
		if !strings.Contains(manifest, want) {
			t.Errorf("sut-manifest.yaml missing %q:\n%s", want, manifest)
		}
	}
	for _, flag := range []string{"enable-fast-restore", "custom-limit"} {
		if strings.Contains(manifest, flag) {
			t.Errorf("sut-manifest.yaml carries trainer flag %q; flags must reach the trainer command only:\n%s", flag, manifest)
		}
	}

	// Verify the SUT fields the manifest does not render, the merged Labels,
	// and park over park_policy, on the scenario executeScenarioRun ran.
	ovFull, err := resolveScenarioOverlay("full-override", &baseScen)
	if err != nil {
		t.Fatalf("resolveScenarioOverlay: %v", err)
	}
	applied := cloneScenario(&baseScen)
	if err := applyFeatureOverlay(&applied, ovFull); err != nil {
		t.Fatalf("applyFeatureOverlay: %v", err)
	}
	if s := applied.SUT; s.Namespace != "overlay-k8s-ns" ||
		s.Template != "overlay-template" ||
		s.ServerAddr != "overlay-server:7443" ||
		s.StorageBackend != "gcs" ||
		!reflect.DeepEqual(s.ValuesOverlay, map[string]any{"replicaCount": 3}) {
		t.Errorf("SUT namespace/template/server_addr/storage_backend/values_overlay not applied: %+v", s)
	}
	if want := map[string]string{"tier": "base", "overlay": "enabled"}; !maps.Equal(applied.SUT.Labels, want) {
		t.Errorf("SUT.Labels = %v, want %v", applied.SUT.Labels, want)
	}
	for _, j := range applied.Jobs {
		if j.Park != schema.ParkSuspendAsync || j.ParkPolicy != schema.ParkSuspendAsync {
			t.Errorf("job %s park = %q, park_policy = %q; want suspend-async (park over park_policy fork)", j.Name, j.Park, j.ParkPolicy)
		}
	}

	// Verify SUT + WarmPoolSize propagated into system_description.json.
	rawSys, err := os.ReadFile(filepath.Join(dirFull, "system_description.json"))
	if err != nil {
		t.Fatalf("read system_description.json: %v", err)
	}
	sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
	if err != nil {
		t.Fatalf("decode system_description.json: %v", err)
	}
	if sysDesc.Backend != "agent-sandbox" ||
		sysDesc.Access != "sdk" ||
		sysDesc.RuntimeClass != "gvisor" ||
		sysDesc.MaxActivePods != 64 ||
		sysDesc.PoolSizePerImage != 24 ||
		sysDesc.APIImage != "gcr.io/ov/api:v2" ||
		sysDesc.GuestImage != "gcr.io/ov/guest:v2" {
		t.Fatalf("system_description.json did not reflect SUT/WarmPoolSize overrides: %+v", sysDesc)
	}
	if len(sysDesc.Overlays) != 1 {
		t.Fatalf("system_description overlays = %+v, want one", sysDesc.Overlays)
	}
	if ref := sysDesc.Overlays[0]; ref.ParkPolicy != schema.ParkSuspendAsync ||
		ref.SnapshotLoop == nil || !*ref.SnapshotLoop ||
		ref.Prefetch == nil || !*ref.Prefetch ||
		ref.LookaheadSteps != 2 {
		t.Errorf("overlay ref did not record the resolved job settings: %+v", ref)
	}

	// Verify SnapshotMode, Park/ParkPolicy, Lookahead, EnvVars, and Flags in events.jsonl.
	evsFull, err := readEventsFile(t, filepath.Join(dirFull, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}
	var (
		step1Prefetch  []string
		snapshotCount  int
		simParkTags    []string
		realStepMetric map[string]float64
	)
	for _, ev := range evsFull {
		if ev.Job == "sim-closed" && ev.Event == swebench.EventPhase && ev.Name == "prefetch_hint" && ev.Step == 1 {
			step1Prefetch = append(step1Prefetch, ev.Image)
		}
		if ev.Job == "sim-closed" && ev.Event == swebench.EventPhase && ev.Name == "snapshot" {
			snapshotCount++
		}
		if ev.Job == "sim-closed" && ev.Event == swebench.EventStep {
			simParkTags = append(simParkTags, ev.Tags["park_policy"])
		}
		if ev.Job == "real-trainer" && ev.Event == swebench.EventStep && ev.Step == 1 {
			realStepMetric = ev.Metrics
		}
	}
	if !slices.Equal(step1Prefetch, []string{"img-2", "img-3"}) {
		t.Errorf("step 1 prefetch_hint images = %v, want [img-2 img-3] from Lookahead=2", step1Prefetch)
	}
	if snapshotCount != 3 {
		t.Errorf("snapshot phase count = %d, want 3 from SnapshotMode=async", snapshotCount)
	}
	for _, tag := range simParkTags {
		if tag != string(schema.ParkSuspendAsync) {
			t.Errorf("sim-closed step park_policy tag = %q, want %q", tag, schema.ParkSuspendAsync)
		}
	}
	if realStepMetric == nil ||
		realStepMetric["env_marker"] != 9 ||
		realStepMetric["base_keep"] != 1 ||
		realStepMetric["flag_count"] != 2 ||
		realStepMetric["--enable-fast-restore"] != 1 ||
		realStepMetric["-custom-limit"] != 42 {
		t.Errorf("real-trainer step metrics did not reflect EnvVars and Flags: %+v", realStepMetric)
	}

	// 2. Run with "no-prefetch-no-snap" overlay (Prefetch=false, SnapshotMode=none, ParkPolicy=delete).
	dirOff := filepath.Join(t.TempDir(), "off")
	if _, err := executeScenarioRun(ctx, &baseScen, sched, "no-prefetch-no-snap", "run-ov-off", "test-cluster", dirOff); err != nil {
		t.Fatalf("executeScenarioRun(no-prefetch-no-snap): %v", err)
	}
	evsOff, err := readEventsFile(t, filepath.Join(dirOff, "events.jsonl"))
	if err != nil {
		t.Fatalf("read off events.jsonl: %v", err)
	}
	for _, ev := range evsOff {
		if ev.Job == "sim-closed" && ev.Event == swebench.EventPhase && ev.Name == "prefetch_hint" {
			t.Errorf("unexpected prefetch_hint event when Prefetch=false: %+v", ev)
		}
		if ev.Job == "sim-closed" && ev.Event == swebench.EventPhase && ev.Name == "snapshot" {
			t.Errorf("unexpected snapshot event when SnapshotMode=none and ParkPolicy=delete: %+v", ev)
		}
		if ev.Job == "sim-closed" && ev.Event == swebench.EventStep && ev.Tags["park_policy"] != string(schema.ParkDelete) {
			t.Errorf("sim-closed step park_policy = %q, want %q", ev.Tags["park_policy"], schema.ParkDelete)
		}
	}

	// Verify on-disk schedule.json stays identical across both overlay runs for A/B comparison.
	schedFullBytes, err := os.ReadFile(filepath.Join(dirFull, "schedule.json"))
	if err != nil {
		t.Fatal(err)
	}
	schedOffBytes, err := os.ReadFile(filepath.Join(dirOff, "schedule.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(schedFullBytes, schedOffBytes) {
		t.Fatalf("schedule.json differed between overlays:\n--- full ---\n%s\n--- off ---\n%s", schedFullBytes, schedOffBytes)
	}

	// 3. Verify overlays declared in Scenario.Variants resolve and preserve warm footing when PrewarmPolicy is unset.
	variantScen := baseScen
	variantScen.Overlays = nil
	variantScen.Variants = []schema.FeatureOverlay{
		{
			Name:     "variant-only",
			Park:     schema.ParkFork,
			Prefetch: &prefetchFalse,
		},
	}
	dirVar := filepath.Join(t.TempDir(), "variant")
	repVar, err := executeScenarioRun(ctx, &variantScen, sched, "variant-only", "run-ov-var", "test-cluster", dirVar)
	if err != nil {
		t.Fatalf("executeScenarioRun(variant-only): %v", err)
	}
	if repVar.Validity == nil || !repVar.Validity.Valid {
		t.Fatalf("variant-only report validity = %+v, want valid=true", repVar.Validity)
	}
	if repVar.AcquisitionSources[string(schema.SourceWarmPool)] == 0 {
		t.Fatalf("expected warm_pool acquisition source to be preserved for warm scenario variant, got %+v", repVar.AcquisitionSources)
	}
}

// Each FeatureOverlay field set alone must change applyFeatureOverlay's
// output, and each SUTSpec field set alone must change mergeSUTSpec's output,
// against an empty overlay. A new field without a probe here, or without
// propagation, fails.
func TestOverlayFieldsEachTakeEffect(t *testing.T) {
	prefetchTrue := true
	poolOff := false
	poolOn := true
	base := schema.Scenario{
		Name:    "field-probe",
		Footing: "warm",
		SUT: schema.SUTSpec{
			Backend:        "substrate",
			Access:         "ate-env-api",
			Endpoint:       "base:1",
			Substrate:      &schema.SubstrateSUTSpec{Atespace: "base-ns", BaseTemplate: "base-tmpl"},
			NodePool:       "base-pool",
			RuntimeClass:   "runc",
			Replicas:       2,
			ExtraArgs:      map[string]string{"log_level": "info"},
			Namespace:      "base-k8s",
			Template:       "base-template",
			ServerAddr:     "base:2",
			StorageBackend: "local",
			PoolEnabled:    &poolOff,
			PoolSizePerImg: 4,
			MaxActivePods:  8,
			ValuesOverlay:  map[string]any{"a": "1"},
			Labels:         map[string]string{"tier": "base"},
		},
		Jobs: []schema.JobSpec{
			{Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 2, BatchSize: 1},
			{Name: "trainer", Mode: schema.JobModeReal, TrainerCommand: []string{"python", "train.py"}},
		},
	}
	metadata := map[string]bool{"SchemaVersion": true, "Name": true, "Description": true}

	t.Run("FeatureOverlay", func(t *testing.T) {
		want := cloneScenario(&base)
		if err := applyFeatureOverlay(&want, &schema.FeatureOverlay{Name: "empty"}); err != nil {
			t.Fatalf("applyFeatureOverlay(empty): %v", err)
		}
		if !reflect.DeepEqual(want, cloneScenario(&base)) {
			t.Fatalf("an empty overlay changed the scenario:\n%+v\nbase:\n%+v", want, base)
		}
		probes := map[string]any{
			"Park":          schema.ParkFork,
			"ParkPolicy":    "delete",
			"Prefetch":      &prefetchTrue,
			"Lookahead":     2,
			"WarmPoolSize":  24,
			"SnapshotMode":  "sync",
			"PrewarmPolicy": "cold",
			"SUT":           schema.SUTSpec{Backend: "agent-sandbox"},
			"EnvVars":       map[string]string{"OVERLAY_ONLY": "1"},
			"Flags":         map[string]string{"steps": "10"},
		}
		n := forEachFieldProbe(t, reflect.TypeOf(schema.FeatureOverlay{}), metadata, probes, func(t *testing.T, name string, v reflect.Value) {
			ov := v.Interface().(schema.FeatureOverlay)
			ov.Name = "probe-" + name
			got := cloneScenario(&base)
			if err := applyFeatureOverlay(&got, &ov); err != nil {
				t.Fatalf("applyFeatureOverlay(%s): %v", name, err)
			}
			if reflect.DeepEqual(got, want) {
				t.Errorf("FeatureOverlay.%s set alone did not change applyFeatureOverlay's output", name)
			}
		})
		if n != len(probes) {
			t.Errorf("probed %d FeatureOverlay fields, want %d", n, len(probes))
		}
	})

	t.Run("SUTSpec", func(t *testing.T) {
		want := cloneSUTSpec(base.SUT)
		mergeSUTSpec(&want, schema.SUTSpec{})
		if !reflect.DeepEqual(want, base.SUT) {
			t.Fatalf("an empty SUTSpec changed the merge: %+v", want)
		}
		probes := map[string]any{
			"Backend":        "agent-sandbox",
			"Access":         "sdk",
			"Endpoint":       "ov:1",
			"Substrate":      &schema.SubstrateSUTSpec{Atespace: "ov-ns"},
			"NodePool":       "ov-pool",
			"RuntimeClass":   "gvisor",
			"Replicas":       5,
			"ExtraArgs":      map[string]string{"wake-budget": "9m"},
			"Namespace":      "ov-k8s",
			"Template":       "ov-template",
			"ServerAddr":     "ov:2",
			"StorageBackend": "gcs",
			"PoolEnabled":    &poolOn,
			"PoolSizePerImg": 9,
			"MaxActivePods":  16,
			"ValuesOverlay":  map[string]any{"b": "2"},
			"Labels":         map[string]string{"overlay": "on"},
		}
		n := forEachFieldProbe(t, reflect.TypeOf(schema.SUTSpec{}), nil, probes, func(t *testing.T, name string, v reflect.Value) {
			got := cloneSUTSpec(base.SUT)
			mergeSUTSpec(&got, v.Interface().(schema.SUTSpec))
			if reflect.DeepEqual(got, want) {
				t.Errorf("SUTSpec.%s set alone did not change mergeSUTSpec's output", name)
			}
		})
		if n != len(probes) {
			t.Errorf("probed %d SUTSpec fields, want %d", n, len(probes))
		}
	})
}

// forEachFieldProbe sets each field of typ that skip does not name to its
// probe value on a zero value of typ and calls check with it. A field without
// a probe, or a probe without a field, fails the test. It returns the number
// of fields probed.
func forEachFieldProbe(t *testing.T, typ reflect.Type, skip map[string]bool, probes map[string]any, check func(t *testing.T, name string, v reflect.Value)) int {
	t.Helper()
	seen := map[string]bool{}
	n := 0
	for _, f := range reflect.VisibleFields(typ) {
		if f.Anonymous || skip[f.Name] {
			continue
		}
		seen[f.Name] = true
		p, ok := probes[f.Name]
		if !ok {
			t.Errorf("%s.%s has no probe value: add one and make the field take effect", typ.Name(), f.Name)
			continue
		}
		v := reflect.New(typ).Elem()
		v.FieldByIndex(f.Index).Set(reflect.ValueOf(p))
		check(t, f.Name, v)
		n++
	}
	for name := range probes {
		if !seen[name] {
			t.Errorf("%s has no field %s: drop the probe", typ.Name(), name)
		}
	}
	return n
}

func TestApplyFeatureOverlayColdPrewarmKeepsPoolSize(t *testing.T) {
	base := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "cold-keeps-pool",
		Footing:       "warm",
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api", PoolSizePerImg: 16},
		Jobs: []schema.JobSpec{{
			Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 2, BatchSize: 1, Concurrency: 1, Instances: 2,
		}},
	}
	runScen := cloneScenario(&base)
	if err := applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "cold", PrewarmPolicy: "cold"}); err != nil {
		t.Fatal(err)
	}
	if runScen.Footing != "cold" || runScen.SUT.PoolSizePerImg != 16 {
		t.Errorf("prewarm_policy cold: footing = %q, pool_size_per_image = %d; want cold and the base 16", runScen.Footing, runScen.SUT.PoolSizePerImg)
	}
	runScen = cloneScenario(&base)
	if err := applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "cold-4", PrewarmPolicy: "cold", WarmPoolSize: 4}); err != nil {
		t.Fatal(err)
	}
	if runScen.SUT.PoolSizePerImg != 4 {
		t.Errorf("warm_pool_size 4 with prewarm_policy cold: pool_size_per_image = %d, want 4", runScen.SUT.PoolSizePerImg)
	}

	// cold-only on a base with a pool: the recorded pool size is the base's,
	// the run acquires cold, and the report stays valid on cold footing.
	sched, err := job.GenerateSchedule(&base, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	rep, err := executeScenarioRun(context.Background(), &base, sched, "cold-only", "run-cold-pool", "test-cluster", dir)
	if err != nil {
		t.Fatalf("executeScenarioRun(cold-only): %v", err)
	}
	rawSys, err := os.ReadFile(filepath.Join(dir, "system_description.json"))
	if err != nil {
		t.Fatal(err)
	}
	sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
	if err != nil {
		t.Fatal(err)
	}
	if sysDesc.PoolSizePerImage != 16 {
		t.Errorf("system_description pool_size_per_image = %d, want the base 16", sysDesc.PoolSizePerImage)
	}
	if rep.Validity == nil || !rep.Validity.Valid {
		t.Errorf("report validity = %+v, want valid", rep.Validity)
	}
	if rep.AcquisitionSources[string(schema.SourceWarmPool)] != 0 || rep.AcquisitionSources[string(schema.SourceColdStart)] == 0 {
		t.Errorf("acquisition sources = %v, want cold_start only", rep.AcquisitionSources)
	}
}

func TestApplyFeatureOverlayFlagsReachTrainerOnly(t *testing.T) {
	base := schema.Scenario{
		Name: "flags-target",
		SUT: schema.SUTSpec{
			Backend:   "substrate",
			Access:    "ate-env-api",
			ExtraArgs: map[string]string{"log_level": "info"},
		},
		Jobs: []schema.JobSpec{
			{Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 1, BatchSize: 1},
			{Name: "trainer", Mode: schema.JobModeReal, TrainerCommand: []string{"python", "train.py"}},
		},
	}
	runScen := cloneScenario(&base)
	ov := &schema.FeatureOverlay{
		Name:  "trainer-flags",
		Flags: map[string]string{"steps": "10", "--enable-fast-restore": "1"},
	}
	if err := applyFeatureOverlay(&runScen, ov); err != nil {
		t.Fatalf("applyFeatureOverlay: %v", err)
	}
	if !maps.Equal(runScen.SUT.ExtraArgs, base.SUT.ExtraArgs) {
		t.Errorf("SUT.ExtraArgs = %v, want the base %v: flags must not reach the API args", runScen.SUT.ExtraArgs, base.SUT.ExtraArgs)
	}
	want := []string{"python", "train.py", "--enable-fast-restore=1", "-steps=10"}
	if got := runScen.Jobs[1].TrainerCommand; !slices.Equal(got, want) {
		t.Errorf("real job TrainerCommand = %q, want %q", got, want)
	}
	if got := runScen.Jobs[0].TrainerCommand; len(got) != 0 {
		t.Errorf("simulated job TrainerCommand = %q, want none", got)
	}
}

func TestApplyFeatureOverlaySnapshotModeKeepsPark(t *testing.T) {
	base := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "snapshot-mode-keeps-park",
		Footing:       "warm",
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api"},
		Jobs: []schema.JobSpec{{
			Name:        "sim",
			Mode:        schema.JobModeClosedLoop,
			Steps:       2,
			BatchSize:   1,
			Concurrency: 1,
			Instances:   2,
			Park:        schema.ParkDelete,
		}},
	}

	for _, tc := range []struct {
		mode string
		loop bool
	}{{"sync", true}, {"async", true}, {"none", false}} {
		runScen := cloneScenario(&base)
		if err := applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "snap", SnapshotMode: tc.mode}); err != nil {
			t.Fatalf("applyFeatureOverlay(snapshot_mode=%s): %v", tc.mode, err)
		}
		j := runScen.Jobs[0]
		if j.Park != schema.ParkDelete || j.ParkPolicy != "" {
			t.Errorf("snapshot_mode=%s: park = %q, park_policy = %q; want the base delete and empty", tc.mode, j.Park, j.ParkPolicy)
		}
		if j.SnapshotLoop != tc.loop {
			t.Errorf("snapshot_mode=%s: snapshot_loop = %v, want %v", tc.mode, j.SnapshotLoop, tc.loop)
		}
	}

	// The cold-only preset sets snapshot_mode: none and no park. The recorded
	// step park policy must stay the job's delete.
	sched, err := job.GenerateSchedule(&base, nil)
	if err != nil {
		t.Fatalf("GenerateSchedule: %v", err)
	}
	dir := t.TempDir()
	if _, err := executeScenarioRun(context.Background(), &base, sched, "cold-only", "run-keep-park", "test-cluster", dir); err != nil {
		t.Fatalf("executeScenarioRun(cold-only): %v", err)
	}
	evs, err := readEventsFile(t, filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	steps := 0
	for _, ev := range evs {
		if ev.Job != "sim" {
			continue
		}
		if ev.Event == swebench.EventStep {
			steps++
			if got := ev.Tags["park_policy"]; got != string(schema.ParkDelete) {
				t.Errorf("step %d park_policy = %q, want %q", ev.Step, got, schema.ParkDelete)
			}
		}
		if ev.Event == swebench.EventPhase && ev.Name == "snapshot" {
			t.Errorf("snapshot phase recorded with snapshot_mode none: %+v", ev)
		}
	}
	if steps != 2 {
		t.Errorf("recorded %d step events, want 2", steps)
	}
	rawSys, err := os.ReadFile(filepath.Join(dir, "system_description.json"))
	if err != nil {
		t.Fatal(err)
	}
	sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
	if err != nil {
		t.Fatal(err)
	}
	if len(sysDesc.Overlays) != 1 || sysDesc.Overlays[0].ParkPolicy != schema.ParkDelete ||
		sysDesc.Overlays[0].SnapshotLoop == nil || *sysDesc.Overlays[0].SnapshotLoop {
		t.Errorf("overlay ref = %+v, want park_policy delete and snapshot_loop false", sysDesc.Overlays)
	}
}

// A lookahead_steps: 2 job gets the same prefetch hints from the generator
// and from an overlay that sets prefetch: true, which changes no prefetch
// setting of the job.
func TestOverlayPrefetchMatchesGeneratorLookahead(t *testing.T) {
	prefetchTrue := true
	base := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "lookahead-parity",
		Footing:       "cold",
		Seed:          5,
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api"},
		Jobs: []schema.JobSpec{{
			Name:           "sim",
			Mode:           schema.JobModeClosedLoop,
			Steps:          5,
			BatchSize:      1,
			Concurrency:    1,
			Prefetch:       true,
			LookaheadSteps: 2,
		}},
		Overlays: map[string]schema.FeatureOverlay{
			"prefetch-on": {Name: "prefetch-on", Prefetch: &prefetchTrue},
		},
	}
	insts := make([]swebench.Instance, 6)
	for i := range insts {
		insts[i] = swebench.Instance{InstanceID: "inst-" + string(rune('a'+i)), Image: "img-" + string(rune('a'+i))}
	}
	sched, err := job.GenerateSchedule(&base, insts)
	if err != nil {
		t.Fatalf("GenerateSchedule: %v", err)
	}
	hints := func(overlayName string) map[int][]string {
		t.Helper()
		dir := t.TempDir()
		if _, err := executeScenarioRun(context.Background(), &base, sched, overlayName, "run-"+overlayName, "test-cluster", dir); err != nil {
			t.Fatalf("executeScenarioRun(%q): %v", overlayName, err)
		}
		evs, err := readEventsFile(t, filepath.Join(dir, "events.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		out := map[int][]string{}
		for _, ev := range evs {
			if ev.Job == "sim" && ev.Event == swebench.EventPhase && ev.Name == "prefetch_hint" {
				out[ev.Step] = append(out[ev.Step], ev.Image)
			}
		}
		return out
	}
	without := hints("")
	with := hints("prefetch-on")
	if !maps.EqualFunc(without, with, slices.Equal) {
		t.Fatalf("prefetch hints differ:\nwithout overlay: %v\nwith prefetch-on: %v", without, with)
	}
	if len(without) == 0 {
		t.Fatalf("no prefetch_hint events; the sampled schedule needs a seed whose steps differ: %+v", sched.Jobs[0].Steps)
	}
}

// A pool size from an overlay turns the pool on. On a base with the pool
// off, pool-on acquires from the pool it sized and pool-off acquires cold,
// and the recorded pool size and mode agree with the acquisition source.
func TestOverlayPoolSizeTurnsPoolOn(t *testing.T) {
	poolOff := false
	base := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "pool-off-base",
		Footing:       "warm",
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api", PoolEnabled: &poolOff},
		Jobs: []schema.JobSpec{{
			Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 2, BatchSize: 1, Concurrency: 1, Instances: 2,
		}},
	}
	sched, err := job.GenerateSchedule(&base, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		overlay  string
		source   schema.AcquisitionSource
		other    schema.AcquisitionSource
		poolSize int
		mode     string
	}{
		{"pool-on", schema.SourceWarmPool, schema.SourceColdStart, 32, "prewarmed"},
		{"pool-off", schema.SourceColdStart, schema.SourceWarmPool, 0, "ephemeral"},
	} {
		dir := t.TempDir()
		rep, err := executeScenarioRun(context.Background(), &base, sched, tc.overlay, "run-"+tc.overlay, "test-cluster", dir)
		if err != nil {
			t.Fatalf("executeScenarioRun(%s): %v", tc.overlay, err)
		}
		if rep.AcquisitionSources[string(tc.source)] == 0 || rep.AcquisitionSources[string(tc.other)] != 0 {
			t.Errorf("%s: acquisition sources = %v, want %s and no %s", tc.overlay, rep.AcquisitionSources, tc.source, tc.other)
		}
		rawSys, err := os.ReadFile(filepath.Join(dir, "system_description.json"))
		if err != nil {
			t.Fatal(err)
		}
		sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
		if err != nil {
			t.Fatal(err)
		}
		if sysDesc.PoolSizePerImage != tc.poolSize {
			t.Errorf("%s: system_description pool_size_per_image = %d, want %d", tc.overlay, sysDesc.PoolSizePerImage, tc.poolSize)
		}
		if sysDesc.LifecyclePolicy == nil || sysDesc.LifecyclePolicy.Mode != tc.mode {
			t.Errorf("%s: system_description lifecycle_policy = %+v, want mode %s", tc.overlay, sysDesc.LifecyclePolicy, tc.mode)
		}
	}

	// sut.pool_size_per_image turns the pool on too; an overlay
	// sut.pool_enabled: false wins over its own pool size.
	runScen := cloneScenario(&base)
	if err := applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "sized", SUT: schema.SUTSpec{PoolSizePerImg: 8}}); err != nil {
		t.Fatal(err)
	}
	if runScen.SUT.PoolEnabled == nil || !*runScen.SUT.PoolEnabled {
		t.Error("sut.pool_size_per_image 8: pool_enabled is not true")
	}
	runScen = cloneScenario(&base)
	if err := applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "sized-off", WarmPoolSize: 8, SUT: schema.SUTSpec{PoolEnabled: &poolOff}}); err != nil {
		t.Fatal(err)
	}
	if runScen.SUT.PoolEnabled == nil || *runScen.SUT.PoolEnabled {
		t.Error("warm_pool_size 8 with sut.pool_enabled false: pool_enabled is not false")
	}
}

// Without -overlay, the simulated acquisition source follows the scenario's
// footing and pool fields, as it does with one.
func TestAcquisitionSourceWithoutOverlayFollowsScenario(t *testing.T) {
	poolOff, poolOn := false, true
	for _, tc := range []struct {
		name    string
		footing string
		sut     schema.SUTSpec
		want    schema.AcquisitionSource
	}{
		{"warm footing", "warm", schema.SUTSpec{}, schema.SourceWarmPool},
		{"warm footing with a pool", "warm", schema.SUTSpec{PoolSizePerImg: 16}, schema.SourceWarmPool},
		{"warm footing with the pool on", "warm", schema.SUTSpec{PoolEnabled: &poolOn}, schema.SourceWarmPool},
		{"warm footing with the pool off", "warm", schema.SUTSpec{PoolEnabled: &poolOff}, schema.SourceColdStart},
		{"cold footing", "cold", schema.SUTSpec{}, schema.SourceColdStart},
		{"cold footing with a base pool", "cold", schema.SUTSpec{PoolSizePerImg: 16}, schema.SourceColdStart},
		{"no footing and no pool", "", schema.SUTSpec{}, schema.SourceColdStart},
		{"no footing with a pool", "", schema.SUTSpec{PoolSizePerImg: 16}, schema.SourceWarmPool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sut := tc.sut
			sut.Backend, sut.Access = "substrate", "ate-env-api"
			scen := schema.Scenario{
				SchemaVersion: schema.SchemaVersion,
				Name:          "no-overlay-source",
				Footing:       tc.footing,
				SUT:           sut,
				Jobs: []schema.JobSpec{{
					Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 2, BatchSize: 1, Concurrency: 1, Instances: 2,
				}},
			}
			if got := acquisitionSource(&scen, nil); got != tc.want {
				t.Errorf("acquisitionSource = %s, want %s", got, tc.want)
			}
			sched, err := job.GenerateSchedule(&scen, nil)
			if err != nil {
				t.Fatal(err)
			}
			rep, err := executeScenarioRun(context.Background(), &scen, sched, "", "run-no-overlay", "test-cluster", t.TempDir())
			if err != nil {
				t.Fatalf("executeScenarioRun: %v", err)
			}
			other := schema.SourceColdStart
			if tc.want == schema.SourceColdStart {
				other = schema.SourceWarmPool
			}
			if rep.AcquisitionSources[string(tc.want)] == 0 || rep.AcquisitionSources[string(other)] != 0 {
				t.Errorf("acquisition sources = %v, want %s and no %s", rep.AcquisitionSources, tc.want, other)
			}
		})
	}
}

// The overlay's sut.extra_args, flags and env_vars reach
// system_description.json in three fields, so an equal key in two of them
// survives, and an env_vars value never reaches the file.
func TestOverlayRefKeepsInputsApartAndEnvValuesOut(t *testing.T) {
	const secret = "hf_secret_value"
	base := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "overlay-inputs",
		Footing:       "warm",
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api"},
		Jobs: []schema.JobSpec{
			{Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 1, BatchSize: 1, Concurrency: 1, Instances: 1},
			{Name: "trainer", Mode: schema.JobModeReal, TraceMode: "measure", TrainerCommand: []string{"sh", "-c", "echo step=1 timing_s/step=1.0"}},
		},
		Overlays: map[string]schema.FeatureOverlay{
			"inputs": {
				Name:    "inputs",
				SUT:     schema.SUTSpec{ExtraArgs: map[string]string{"log_level": "debug"}},
				Flags:   map[string]string{"log_level": "info", "steps": "10"},
				EnvVars: map[string]string{"LOG_LEVEL": "warn", "HF_TOKEN": secret, "log_level": "trace"},
			},
		},
	}
	sched, err := job.GenerateSchedule(&base, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if _, err := executeScenarioRun(context.Background(), &base, sched, "inputs", "run-inputs", "test-cluster", dir); err != nil {
		t.Fatalf("executeScenarioRun(inputs): %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "system_description.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(secret)) {
		t.Errorf("system_description.json holds an env_vars value:\n%s", raw)
	}
	if err := schema.ValidateAgainstJSONSchema("system_description.schema.json", raw); err != nil {
		t.Fatalf("system_description.json does not match the schema: %v", err)
	}
	sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(sysDesc.Overlays) != 1 {
		t.Fatalf("overlays = %+v, want one", sysDesc.Overlays)
	}
	ref := sysDesc.Overlays[0]
	if want := map[string]string{"log_level": "debug"}; !maps.Equal(ref.SUTOverrides, want) {
		t.Errorf("sut_overrides = %v, want the overlay's sut.extra_args %v", ref.SUTOverrides, want)
	}
	if want := map[string]string{"log_level": "info", "steps": "10"}; !maps.Equal(ref.Flags, want) {
		t.Errorf("flags = %v, want %v", ref.Flags, want)
	}
	if want := []string{"HF_TOKEN", "LOG_LEVEL", "log_level"}; !slices.Equal(ref.EnvVarNames, want) {
		t.Errorf("env_var_names = %v, want the sorted names %v", ref.EnvVarNames, want)
	}

	// An overlay without these inputs records none of the three fields.
	empty := schema.OverlayRef{Name: "none"}
	recordOverlayInputs(&empty, &schema.FeatureOverlay{Name: "none"})
	if empty.SUTOverrides != nil || empty.Flags != nil || empty.EnvVarNames != nil {
		t.Errorf("empty overlay recorded inputs: %+v", empty)
	}
}

// The footing a run executes with follows the acquisition source, so the
// prewarm event, lifecycle_policy and Validity Rule 1 agree with the task
// acquisitions report.json records, on a cold base, a warm base and an
// omitted footing, with and without an overlay.
func TestRunFootingFollowsAcquisitionSource(t *testing.T) {
	poolOff, poolOn := false, true
	for _, tc := range []struct {
		name    string
		footing string
		sut     schema.SUTSpec
		overlay string
		want    schema.AcquisitionSource
	}{
		{"cold base with a base pool", "cold", schema.SUTSpec{PoolSizePerImg: 8}, "", schema.SourceColdStart},
		{"cold base, overlay sizes the pool", "cold", schema.SUTSpec{}, "pool-8", schema.SourceWarmPool},
		{"warm base", "warm", schema.SUTSpec{}, "", schema.SourceWarmPool},
		{"warm base with the pool off", "warm", schema.SUTSpec{PoolEnabled: &poolOff}, "", schema.SourceColdStart},
		{"warm base, overlay turns the pool off", "warm", schema.SUTSpec{PoolSizePerImg: 8}, "pool-off-sut", schema.SourceColdStart},
		{"omitted footing with the pool on", "", schema.SUTSpec{PoolEnabled: &poolOn, PoolSizePerImg: 4}, "", schema.SourceWarmPool},
		{"omitted footing with a pool size", "", schema.SUTSpec{PoolSizePerImg: 4}, "", schema.SourceWarmPool},
		{"omitted footing without a pool", "", schema.SUTSpec{}, "", schema.SourceColdStart},
		{"omitted footing, pool-on", "", schema.SUTSpec{}, "pool-on", schema.SourceWarmPool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sut := tc.sut
			sut.Backend, sut.Access = "substrate", "ate-env-api"
			scen := schema.Scenario{
				SchemaVersion: schema.SchemaVersion,
				Name:          "footing-source",
				Footing:       tc.footing,
				SUT:           sut,
				Jobs: []schema.JobSpec{{
					Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 2, BatchSize: 1, Concurrency: 1, Instances: 2,
				}},
				Overlays: map[string]schema.FeatureOverlay{
					"pool-8":       {Name: "pool-8", WarmPoolSize: 8},
					"pool-off-sut": {Name: "pool-off-sut", SUT: schema.SUTSpec{PoolEnabled: &poolOff}},
				},
			}
			sched, err := job.GenerateSchedule(&scen, nil)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			rep, err := executeScenarioRun(context.Background(), &scen, sched, tc.overlay, "run-footing", "test-cluster", dir)
			if err != nil {
				t.Fatalf("executeScenarioRun: %v", err)
			}
			other := schema.SourceColdStart
			wantMode, wantPrewarm := "ephemeral", 0
			if tc.want == schema.SourceWarmPool {
				other = schema.SourceColdStart
				wantMode, wantPrewarm = "prewarmed", 1
			} else {
				other = schema.SourceWarmPool
			}
			if rep.AcquisitionSources[string(tc.want)] == 0 || rep.AcquisitionSources[string(other)] != 0 {
				t.Errorf("acquisition sources = %v, want %s and no %s", rep.AcquisitionSources, tc.want, other)
			}
			if rep.PrewarmCount != wantPrewarm {
				t.Errorf("report prewarm_count = %d, want %d beside %s acquisitions", rep.PrewarmCount, wantPrewarm, tc.want)
			}
			if rep.Validity == nil || !rep.Validity.Valid {
				t.Errorf("report validity = %+v, want valid", rep.Validity)
			}
			rawSys, err := os.ReadFile(filepath.Join(dir, "system_description.json"))
			if err != nil {
				t.Fatal(err)
			}
			sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
			if err != nil {
				t.Fatal(err)
			}
			pol := sysDesc.LifecyclePolicy
			if pol == nil || pol.Mode != wantMode || pol.PrewarmAtStart != (wantPrewarm > 0) || pol.PrewarmCount != wantPrewarm {
				t.Errorf("lifecycle_policy = %+v, want mode %s, prewarm_at_start %v and prewarm_count %d beside %s acquisitions",
					pol, wantMode, wantPrewarm > 0, wantPrewarm, tc.want)
			}
		})
	}
}

// A prefetch: false job emits no prefetch_hint events, with no overlay, with
// pool-on (which sets neither prefetch nor lookahead_steps) and with an
// explicit prefetch: false overlay, although the generator fills the base
// schedule's hints for every job; schedule.json on disk stays that base.
func TestPrefetchFalseJobEmitsNoHints(t *testing.T) {
	prefetchFalse := false
	base := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "prefetch-false",
		Footing:       "warm",
		Seed:          5,
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api"},
		Jobs: []schema.JobSpec{{
			Name:           "sim",
			Mode:           schema.JobModeClosedLoop,
			Steps:          5,
			BatchSize:      1,
			Concurrency:    1,
			Prefetch:       false,
			LookaheadSteps: 1,
		}},
		Overlays: map[string]schema.FeatureOverlay{
			"prefetch-off": {Name: "prefetch-off", Prefetch: &prefetchFalse},
		},
	}
	insts := make([]swebench.Instance, 6)
	for i := range insts {
		insts[i] = swebench.Instance{InstanceID: "inst-" + string(rune('a'+i)), Image: "img-" + string(rune('a'+i))}
	}
	sched, err := job.GenerateSchedule(&base, insts)
	if err != nil {
		t.Fatalf("GenerateSchedule: %v", err)
	}
	baseHints := 0
	for _, st := range sched.Jobs[0].Steps {
		baseHints += len(st.PrefetchImages)
	}
	if baseHints == 0 {
		t.Fatalf("the generator filled no hints; the sampled schedule needs steps that differ: %+v", sched.Jobs[0].Steps)
	}
	baseJSON, err := json.Marshal(sched)
	if err != nil {
		t.Fatal(err)
	}

	hints := func(scen *schema.Scenario, overlayName string) int {
		t.Helper()
		dir := t.TempDir()
		if _, err := executeScenarioRun(context.Background(), scen, sched, overlayName, "run-"+overlayName, "test-cluster", dir); err != nil {
			t.Fatalf("executeScenarioRun(%q): %v", overlayName, err)
		}
		rawSched, err := os.ReadFile(filepath.Join(dir, "schedule.json"))
		if err != nil {
			t.Fatal(err)
		}
		onDisk, err := schema.DecodeJSONStrict[schema.Schedule](bytes.NewReader(rawSched))
		if err != nil {
			t.Fatal(err)
		}
		if diskJSON, _ := json.Marshal(&onDisk); !bytes.Equal(diskJSON, baseJSON) {
			t.Errorf("overlay %q: schedule.json on disk is not the base schedule:\n%s", overlayName, rawSched)
		}
		evs, err := readEventsFile(t, filepath.Join(dir, "events.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, ev := range evs {
			if ev.Job == "sim" && ev.Event == swebench.EventPhase && ev.Name == "prefetch_hint" {
				n++
			}
		}
		return n
	}
	for _, overlayName := range []string{"", "pool-on", "prefetch-off"} {
		if n := hints(&base, overlayName); n != 0 {
			t.Errorf("overlay %q: %d prefetch_hint events for a prefetch: false job, want 0", overlayName, n)
		}
	}
	// The same job with prefetch: true gets the base schedule's hints.
	on := cloneScenario(&base)
	on.Jobs[0].Prefetch = true
	if n := hints(&on, ""); n != baseHints {
		t.Errorf("prefetch: true job: %d prefetch_hint events, want the base schedule's %d", n, baseHints)
	}
}

// An overlays: entry in scenario.yaml takes its name from the map key, so a
// bench run -overlay with it needs no inner name.
func TestRunOverlayFromMapKeyWithoutInnerName(t *testing.T) {
	dir := t.TempDir()
	scenPath := filepath.Join(dir, "scenario.yaml")
	scenYAML := `schema_version: v1
name: overlay-from-key
footing: cold
sut:
  backend: substrate
  access: ate-env-api
jobs:
  - name: sim
    mode: closed-loop
    steps: 2
    batch_size: 1
    concurrency: 1
    instances: 2
overlays:
  pool-8:
    warm_pool_size: 8
`
	if err := os.WriteFile(scenPath, []byte(scenYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	var stdout, stderr bytes.Buffer
	if err := runScenarioCmd([]string{"-scenario", scenPath, "-overlay", "pool-8", "-dest", out}, &stdout, &stderr); err != nil {
		t.Fatalf("bench run -overlay pool-8: %v", err)
	}
	rawSys, err := os.ReadFile(filepath.Join(out, "system_description.json"))
	if err != nil {
		t.Fatal(err)
	}
	sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
	if err != nil {
		t.Fatal(err)
	}
	if len(sysDesc.Overlays) != 1 || sysDesc.Overlays[0].Name != "pool-8" || sysDesc.PoolSizePerImage != 8 {
		t.Errorf("overlays = %+v, pool_size_per_image = %d; want one overlay pool-8 and a pool of 8", sysDesc.Overlays, sysDesc.PoolSizePerImage)
	}
}

// An overlay value outside its range is an error before the run, not a field
// that is dropped. Before, prewarm_policy: "off" and warm_pool_size: -4 on a
// warm footing exited 0 and ran warm.
func TestRunOverlayInvalidValueIsAnError(t *testing.T) {
	const head = `schema_version: v1
name: overlay-values
footing: warm
sut:
  backend: substrate
  access: ate-env-api
jobs:
  - name: sim
    mode: closed-loop
    steps: 2
    batch_size: 1
    concurrency: 1
    instances: 2
overlays:
  probe:
`
	for _, tc := range []struct {
		name    string
		tail    string
		wantErr string
	}{
		{"prewarm_policy off", "    prewarm_policy: \"off\"\n", "prewarm_policy"},
		{"negative warm_pool_size", "    warm_pool_size: -4\n", "warm_pool_size"},
		{"negative sut pool_size_per_image", "    sut:\n      pool_size_per_image: -4\n", "pool_size_per_image"},
		{"negative sut max_active_pods", "    sut:\n      max_active_pods: -4\n", "max_active_pods"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			scenPath := filepath.Join(dir, "scenario.yaml")
			if err := os.WriteFile(scenPath, []byte(head+tc.tail), 0o644); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, "out")
			var stdout, stderr bytes.Buffer
			err := runScenarioCmd([]string{"-scenario", scenPath, "-overlay", "probe", "-dest", out}, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("bench run -overlay probe err = %v, want one that holds %q", err, tc.wantErr)
			}
			if _, err := os.Stat(filepath.Join(out, "system_description.json")); err == nil {
				t.Errorf("bench run wrote system_description.json although the overlay is invalid")
			}
		})
	}

	// A negative count on the base sut is an error with and without -overlay;
	// before, the run wrote a system_description.json its schema rejects.
	for _, field := range []string{"pool_size_per_image", "max_active_pods"} {
		for _, overlayArgs := range [][]string{nil, {"-overlay", "probe"}} {
			t.Run("base sut "+field+" "+strings.Join(overlayArgs, ""), func(t *testing.T) {
				dir := t.TempDir()
				scenPath := filepath.Join(dir, "scenario.yaml")
				body := strings.Replace(head, "  access: ate-env-api\n", "  access: ate-env-api\n  "+field+": -4\n", 1) + "    warm_pool_size: 1\n"
				if err := os.WriteFile(scenPath, []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
				out := filepath.Join(dir, "out")
				var stdout, stderr bytes.Buffer
				args := append([]string{"-scenario", scenPath, "-dest", out}, overlayArgs...)
				err := runScenarioCmd(args, &stdout, &stderr)
				if err == nil || !strings.Contains(err.Error(), field) {
					t.Fatalf("bench run err = %v, want one that holds %q", err, field)
				}
				if _, err := os.Stat(out); err == nil {
					t.Errorf("bench run created %s although the base sut is invalid", out)
				}
			})
		}
	}

	// applyFeatureOverlay rejects the same values on its own and leaves the
	// scenario as it was.
	base := schema.Scenario{
		Name:    "warm",
		Footing: "warm",
		SUT:     schema.SUTSpec{Backend: "substrate", PoolSizePerImg: 4},
		Jobs:    []schema.JobSpec{{Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 2, BatchSize: 1}},
	}
	for _, tc := range []struct {
		ov      schema.FeatureOverlay
		wantErr string
	}{
		{schema.FeatureOverlay{Name: "off", PrewarmPolicy: "off"}, "prewarm_policy"},
		{schema.FeatureOverlay{Name: "neg", WarmPoolSize: -4}, "warm_pool_size"},
		{schema.FeatureOverlay{Name: "snap", SnapshotMode: "sometimes"}, "snapshot_mode"},
		{schema.FeatureOverlay{Name: "look", Lookahead: -1}, "lookahead_steps"},
		{schema.FeatureOverlay{Name: "sut", SUT: schema.SUTSpec{Backend: "bogus"}}, "backend"},
		{schema.FeatureOverlay{Name: "sut-pool", SUT: schema.SUTSpec{PoolSizePerImg: -4}}, "pool_size_per_image"},
		{schema.FeatureOverlay{Name: "sut-pods", SUT: schema.SUTSpec{MaxActivePods: -4}}, "max_active_pods"},
	} {
		got := cloneScenario(&base)
		if err := applyFeatureOverlay(&got, &tc.ov); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("applyFeatureOverlay(%s) = %v, want an error that holds %q", tc.ov.Name, err, tc.wantErr)
		}
		if !reflect.DeepEqual(got, cloneScenario(&base)) {
			t.Errorf("applyFeatureOverlay(%s) changed the scenario on error:\n%+v", tc.ov.Name, got)
		}
	}
}

// An overlay name is a directory name under -dest, so a name that is "." or
// ".." or holds a path separator is an error before anything is written, and
// a pair of equal names is an error, because arm B would overwrite arm A.
func TestOverlayNamesAreSafeDirectoryNames(t *testing.T) {
	for _, name := range []string{"", ".", "..", "../escaped-arm", "a/b", `a\b`} {
		if err := (schema.FeatureOverlay{Name: name}).Validate(); err == nil {
			t.Errorf("FeatureOverlay{Name: %q}.Validate() = nil, want an error", name)
		}
	}
	scen := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "names",
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api"},
		Jobs:          []schema.JobSpec{{Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 1, BatchSize: 1, Instances: 1}},
		Overlays: map[string]schema.FeatureOverlay{
			"../escaped-arm": {WarmPoolSize: 8},
		},
	}
	if _, err := resolveScenarioOverlay("../escaped-arm", &scen); err == nil {
		t.Error("resolveScenarioOverlay(../escaped-arm) = nil, want an error although the scenario defines it")
	}
	if err := scen.Validate(); err == nil {
		t.Error("Scenario.Validate accepted the overlay key ../escaped-arm")
	}

	dir := t.TempDir()
	scenPath := filepath.Join(dir, "scenario.yaml")
	scenYAML := `schema_version: v1
name: names
footing: warm
sut:
  backend: substrate
  access: ate-env-api
jobs:
  - name: sim
    mode: closed-loop
    steps: 1
    batch_size: 1
    instances: 1
`
	if err := os.WriteFile(scenPath, []byte(scenYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "dest", "pair")
	var stdout, stderr bytes.Buffer
	err := runScenarioCmd([]string{"-scenario", scenPath, "-overlay", "../escaped-arm,pool-off", "-dest", dest}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "../escaped-arm") {
		t.Errorf("-overlay ../escaped-arm,pool-off: err = %v, want an error that names it", err)
	}
	for _, p := range []string{filepath.Join(dir, "dest", "escaped-arm"), dest} {
		if _, statErr := os.Stat(p); statErr == nil {
			t.Errorf("%s exists after a rejected -overlay name", p)
		}
	}
	err = runScenarioCmd([]string{"-scenario", scenPath, "-overlay", "pool-on,pool-on", "-dest", filepath.Join(dir, "same")}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "two different") {
		t.Errorf("-overlay pool-on,pool-on: err = %v, want a two-different-overlays error", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "same")); statErr == nil {
		t.Error("the pair directory exists after a rejected equal pair")
	}
}

// flags reach a real job's trainer_command only. An overlay with flags fails
// on a scenario whose real job runs a trainer_manifest, or that has no real
// job, so system_description.json never records flags no trainer saw; a
// simulated job keeps its command. An empty env_vars or flags name is an
// error.
func TestOverlayFlagsNeedARealTrainerCommandJob(t *testing.T) {
	flags := map[string]string{"steps": "10"}
	sut := schema.SUTSpec{Backend: "substrate", Access: "ate-env-api"}
	sim := schema.JobSpec{Name: "sim", Mode: schema.JobModeClosedLoop, Steps: 1, BatchSize: 1, Instances: 1}

	manifest := schema.Scenario{Name: "manifest", SUT: sut, Jobs: []schema.JobSpec{
		sim,
		{Name: "trainer", Mode: schema.JobModeReal, TrainerManifest: "examples/trainer/job.yaml", TraceMode: "trace"},
	}}
	runScen := cloneScenario(&manifest)
	err := applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "flags", Flags: flags})
	if err == nil || !strings.Contains(err.Error(), "trainer_manifest") {
		t.Errorf("flags on a trainer_manifest job: err = %v, want a trainer_manifest error", err)
	}

	simOnly := schema.Scenario{SchemaVersion: schema.SchemaVersion, Name: "sim-only", SUT: sut, Jobs: []schema.JobSpec{sim}}
	runScen = cloneScenario(&simOnly)
	err = applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "flags", Flags: flags})
	if err == nil || !strings.Contains(err.Error(), "has none") {
		t.Errorf("flags on a simulated-only scenario: err = %v, want a no-real-job error", err)
	}
	simOnly.Overlays = map[string]schema.FeatureOverlay{"flags": {Name: "flags", Flags: flags}}
	dir := filepath.Join(t.TempDir(), "out")
	if _, err := executeScenarioRun(context.Background(), &simOnly, nil, "flags", "run-flags", "test-cluster", dir); err == nil {
		t.Error("executeScenarioRun with flags on a simulated-only scenario = nil, want an error")
	}
	if _, statErr := os.Stat(dir); statErr == nil {
		t.Error("the run directory exists after a rejected overlay")
	}

	// A simulated job with a trainer_command of its own keeps it; only the
	// real job's command grows.
	simWithCmd := sim
	simWithCmd.TrainerCommand = []string{"python", "sim.py"}
	mixed := schema.Scenario{Name: "mixed", SUT: sut, Jobs: []schema.JobSpec{
		simWithCmd,
		{Name: "trainer", Mode: schema.JobModeReal, TrainerCommand: []string{"python", "train.py"}},
	}}
	runScen = cloneScenario(&mixed)
	if err := applyFeatureOverlay(&runScen, &schema.FeatureOverlay{Name: "flags", Flags: flags}); err != nil {
		t.Fatalf("applyFeatureOverlay: %v", err)
	}
	if got := runScen.Jobs[0].TrainerCommand; !slices.Equal(got, []string{"python", "sim.py"}) {
		t.Errorf("simulated job TrainerCommand = %q, want it unchanged", got)
	}
	if got, want := runScen.Jobs[1].TrainerCommand, []string{"python", "train.py", "-steps=10"}; !slices.Equal(got, want) {
		t.Errorf("real job TrainerCommand = %q, want %q", got, want)
	}

	for _, tc := range []struct {
		name string
		ov   schema.FeatureOverlay
	}{
		{"empty env_vars name", schema.FeatureOverlay{Name: "e", EnvVars: map[string]string{"": "x"}}},
		{"empty flags name", schema.FeatureOverlay{Name: "f", Flags: map[string]string{"": "1"}}},
	} {
		if err := tc.ov.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want an error", tc.name)
		}
	}
	if err := (schema.FeatureOverlay{Name: "ok", EnvVars: map[string]string{"A": "1"}, Flags: map[string]string{"b": "2"}}).Validate(); err != nil {
		t.Errorf("Validate() with named env_vars and flags: %v", err)
	}
}

// overlays[0] records park_policy, prefetch and lookahead_steps from the
// closed-loop jobs, which run the step schedule, and snapshot_loop from all
// simulated jobs; an open-loop job with other step settings does not unset
// them.
func TestOverlayRefSettingsFromClosedLoopJobsOnly(t *testing.T) {
	closed := schema.JobSpec{
		Name: "trainer", Mode: schema.JobModeClosedLoop, Steps: 2, BatchSize: 1, Concurrency: 1, Instances: 2,
		ParkPolicy: schema.ParkPolicySnapshot, Prefetch: true, LookaheadSteps: 1, SnapshotLoop: true,
	}
	open := schema.JobSpec{
		Name: "eval", Mode: schema.JobModeOpenLoop, Concurrency: 2, ArrivalRate: 40, DurationSec: 0.05, Instances: 2,
		ParkPolicy: schema.ParkPolicyRelease,
	}
	base := schema.Scenario{
		SchemaVersion: schema.SchemaVersion,
		Name:          "mixed-loops",
		Footing:       "warm",
		SUT:           schema.SUTSpec{Backend: "substrate", Access: "ate-env-api"},
		Jobs:          []schema.JobSpec{closed, open},
	}
	sched, err := job.GenerateSchedule(&base, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if _, err := executeScenarioRun(context.Background(), &base, sched, "pool-on", "run-mixed", "test-cluster", dir); err != nil {
		t.Fatalf("executeScenarioRun(pool-on): %v", err)
	}
	rawSys, err := os.ReadFile(filepath.Join(dir, "system_description.json"))
	if err != nil {
		t.Fatal(err)
	}
	sysDesc, err := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
	if err != nil {
		t.Fatal(err)
	}
	if len(sysDesc.Overlays) != 1 {
		t.Fatalf("overlays = %+v, want one", sysDesc.Overlays)
	}
	ref := sysDesc.Overlays[0]
	if ref.ParkPolicy != schema.ParkSuspendSync || ref.Prefetch == nil || !*ref.Prefetch || ref.LookaheadSteps != 1 ||
		ref.SnapshotLoop == nil || !*ref.SnapshotLoop {
		t.Errorf("overlays[0] = %+v, want the closed-loop job's park_policy suspend-sync, prefetch true, lookahead_steps 1 and snapshot_loop true", ref)
	}

	// snapshot_loop spans both simulated modes: a disagreement unsets it,
	// while the step settings still come from the closed-loop job.
	openOff := open
	openOff.SnapshotLoop = false
	var unit schema.OverlayRef
	recordResolvedJobSettings(&unit, []schema.JobSpec{closed, openOff, {Name: "real", Mode: schema.JobModeReal}})
	if unit.SnapshotLoop != nil {
		t.Errorf("snapshot_loop = %v, want unset when the open-loop job disagrees", *unit.SnapshotLoop)
	}
	if unit.ParkPolicy != schema.ParkSuspendSync || unit.Prefetch == nil || !*unit.Prefetch || unit.LookaheadSteps != 1 {
		t.Errorf("ref = %+v, want park_policy suspend-sync, prefetch true and lookahead_steps 1 from the closed-loop job", unit)
	}
}

func readEventsFile(t *testing.T, path string) ([]swebench.Event, error) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return swebench.ReadEvents(f)
}
