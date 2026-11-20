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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/job"
	"github.com/agent-substrate/env/internal/benchmarking/measure"
	"github.com/agent-substrate/env/internal/benchmarking/overlay"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/sut"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

func runScenarioCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	scenarioPath := fs.String("scenario", "", "Path to Scenario YAML file. Required.")
	destDir := fs.String("dest", "", "Destination artifact directory ($ARTIFACT_DIR).")
	outDir := fs.String("out-dir", "", "Alias for -dest: destination artifact directory.")
	overlayFlag := fs.String("overlay", "", "Optional overlay preset name or comma-separated pair <A>,<B>.")
	datasetPath := fs.String("dataset", "", "Optional dataset JSONL path.")
	instancesPath := fs.String("instances", "", "Alias for -dataset: optional dataset JSONL path.")
	runID := fs.String("run-id", "bench-run", "Run identifier.")
	cluster := fs.String("cluster", "local-cluster", "Cluster name recorded in system_description.json.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	targetDir := *destDir
	if targetDir == "" {
		targetDir = *outDir
	}
	if *scenarioPath == "" || targetDir == "" {
		return fmt.Errorf("-scenario and -dest (or -out-dir) are required")
	}

	raw, err := os.ReadFile(*scenarioPath)
	if err != nil {
		return err
	}
	scen, err := schema.DecodeYAMLStrict[schema.Scenario](raw)
	if err != nil {
		return err
	}
	// A relative preflight.coverage is next to the scenario file, as a
	// reader of the file expects, not relative to the working directory.
	if scen.Preflight != nil && scen.Preflight.Coverage != "" && !filepath.IsAbs(scen.Preflight.Coverage) {
		scen.Preflight.Coverage = filepath.Join(filepath.Dir(*scenarioPath), scen.Preflight.Coverage)
	}

	dsSource := *datasetPath
	if dsSource == "" {
		dsSource = *instancesPath
	}
	var insts []swebench.Instance
	if dsSource != "" {
		insts, err = swebench.LoadDataset(dsSource)
		if err != nil {
			return err
		}
	} else {
		insts = swebench.SyntheticInstances(16, "")
	}

	sched, err := job.GenerateSchedule(&scen, insts)
	if err != nil {
		return err
	}

	overlays := parseOverlayFlag(*overlayFlag)
	for _, name := range overlays {
		if err := schema.ValidateOverlayName(name); err != nil {
			return fmt.Errorf("-overlay: %w", err)
		}
	}
	ctx := context.Background()
	if len(overlays) <= 1 {
		ovName := ""
		if len(overlays) == 1 {
			ovName = overlays[0]
		}
		if _, err := executeScenarioRun(ctx, &scen, sched, ovName, *runID, *cluster, targetDir); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "completed benchmark run in %s\n", targetDir)
		return nil
	}
	if len(overlays) != 2 {
		return fmt.Errorf("-overlay supports at most 2 comma-separated overlays, got %d", len(overlays))
	}

	nameA, nameB := overlays[0], overlays[1]
	if nameA == nameB {
		return fmt.Errorf("-overlay %s,%s: a pair needs two different overlays, or arm B overwrites arm A", nameA, nameB)
	}
	dirA := filepath.Join(targetDir, nameA)
	dirB := filepath.Join(targetDir, nameB)
	repA, err := executeScenarioRun(ctx, &scen, sched, nameA, *runID+"-a", *cluster, dirA)
	if err != nil {
		return fmt.Errorf("run overlay %s: %w", nameA, err)
	}
	repB, err := executeScenarioRun(ctx, &scen, sched, nameB, *runID+"-b", *cluster, dirB)
	if err != nil {
		return fmt.Errorf("run overlay %s: %w", nameB, err)
	}

	cmp, err := overlay.CompareOverlayPair(nameA, repA, sched, nameB, repB, sched)
	if err != nil {
		return err
	}
	cmpJSON, err := json.MarshalIndent(cmp, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(targetDir, "comparison.json"), append(cmpJSON, '\n'), 0o644); err != nil {
		return err
	}
	md := overlay.RenderComparisonMarkdown(cmp)
	if err := os.WriteFile(filepath.Join(targetDir, "comparison.md"), []byte(md), 0o644); err != nil {
		return err
	}
	fmt.Fprint(stdout, md)
	return nil
}

func parseOverlayFlag(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func executeScenarioRun(
	ctx context.Context,
	scen *schema.Scenario,
	sched *schema.Schedule,
	overlayName string,
	runID string,
	cluster string,
	outDir string,
) (*swebench.Report, error) {
	// Resolve the overlay before anything touches outDir, so a bad name
	// creates no directory.
	runScen := cloneScenario(scen)
	var resolvedOv *schema.FeatureOverlay
	if overlayName != "" {
		ov, err := resolveScenarioOverlay(overlayName, scen)
		if err != nil {
			return nil, err
		}
		if err := applyFeatureOverlay(&runScen, ov); err != nil {
			return nil, err
		}
		resolvedOv = ov
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	defaultSource := string(acquisitionSource(&runScen, resolvedOv))
	// The footing the run executes with. The acquisition source decides it,
	// not runScen.Footing alone: sut.pool_enabled: false acquires cold on a
	// warm footing, and a pool acquires warm on an omitted footing. The
	// prewarm event, lifecycle_policy and Validity Rule 1 follow it, so they
	// agree with the acquisitions report.json records.
	runFooting := "cold"
	if defaultSource == string(schema.SourceWarmPool) {
		runFooting = "warm"
	}

	// 1. Render SUT manifest and system_description.json.
	prov := sut.NewProvisioner(nil)
	if _, err := prov.Provision(ctx, sut.ProvisionOptions{
		Spec:        runScen.SUT,
		ArtifactDir: outDir,
		RenderOnly:  true,
	}); err != nil {
		return nil, err
	}
	sysDesc := sut.BuildSystemDescription(runScen.SUT, cluster, "v1.31", "HEAD", "verified", nil)
	if sysDesc.Backend == "" {
		sysDesc.Backend = runScen.SUT.Backend
	}
	if sysDesc.Access == "" {
		sysDesc.Access = runScen.SUT.Access
	}
	if overlayName != "" {
		scenBytes, _ := json.Marshal(scen)
		scenSum := sha256.Sum256(scenBytes)
		ovSum := sha256.Sum256([]byte(overlayName))
		basePath := scen.Name
		if basePath == "" {
			basePath = "scenario.yaml"
		}
		ref := schema.OverlayRef{
			Name:           overlayName,
			Hash:           "sha256:" + hex.EncodeToString(ovSum[:8]),
			BaseRecipePath: basePath,
			BaseRecipeHash: "sha256:" + hex.EncodeToString(scenSum[:8]),
		}
		recordOverlayInputs(&ref, resolvedOv)
		recordResolvedJobSettings(&ref, runScen.Jobs)
		sysDesc.Overlays = []schema.OverlayRef{ref}
	}
	if err := sut.WriteSystemDescription(filepath.Join(outDir, "system_description.json"), sysDesc); err != nil {
		return nil, err
	}

	// 2. Write measurements.json.
	meas, err := measure.CollectMeasurements(ctx, measure.CollectOptions{
		RunID: runID,
	})
	if err != nil {
		return nil, err
	}
	if err := measure.WriteMeasurements(filepath.Join(outDir, "measurements.json"), meas); err != nil {
		return nil, err
	}

	// 3. Run ScenarioRunner across all jobs (writing schedule.json and events.jsonl).
	execSched := executionSchedule(sched, &runScen, resolvedOv)
	sr := &job.ScenarioRunner{
		RunID:    runID,
		Scenario: &runScen,
		Schedule: execSched,
		OutDir:   outDir,
		ClientFunc: func(_ schema.JobSpec) swebench.EnvClient {
			return &localScenarioEnvClient{source: defaultSource}
		},
		SleepFunc: func(_ context.Context, _ time.Duration) error { return nil },
	}
	if err := sr.Run(ctx); err != nil {
		return nil, err
	}
	// The runner wrote the execution schedule. Replace it with the base
	// schedule, so both sides of an A/B pair hold identical schedule.json
	// files and the comparison's schedule match holds. The scenario's jobs,
	// and OverlayRef in system_description.json with an overlay, record the
	// settings the run executed with. A crash between the two writes leaves
	// the execution schedule on disk.
	if sched != nil {
		if err := swebench.WriteSchedule(filepath.Join(outDir, "schedule.json"), sched); err != nil {
			return nil, err
		}
	}

	// 4. A scenario whose footing rests on a pre-flight names its coverage
	// file: for a warm arm it goes into the bundle as preflight-coverage.json,
	// the evidence Validity Rule 1 reads (a cold arm of an overlay run gets
	// none, as a pre-flight contradicts cold footing). Without one, a warm
	// footing gets the synthetic prewarm event this local run stands in
	// with. The footing is the one the run executes with (runFooting, from
	// the acquisition source), so a run that acquires cold on a scenario
	// with footing: warm gets neither.
	eventsPath := filepath.Join(outDir, "events.jsonl")
	var coverage *schema.PreflightCoverage
	coverageName := ""
	if runScen.Preflight != nil && runFooting == "warm" {
		if runScen.Preflight.Coverage == "" {
			// A plan alone is not evidence: the warm footing would fall back
			// to the synthetic prewarm event and pass for the wrong reason.
			return nil, fmt.Errorf("scenario preflight names a plan (%s) but no coverage; a warm footing that rests on a pre-flight needs its preflight-coverage.json", runScen.Preflight.Plan)
		}
		cov, err := swebench.ReadPreflightCoverage(runScen.Preflight.Coverage)
		if err != nil {
			return nil, fmt.Errorf("scenario preflight.coverage: %w", err)
		}
		if runScen.Preflight.Plan != "" && (cov.Plan == "" || filepath.Base(cov.Plan) != filepath.Base(runScen.Preflight.Plan)) {
			return nil, fmt.Errorf("scenario preflight.plan %q is not the plan the coverage %s was written for (%q)", runScen.Preflight.Plan, runScen.Preflight.Coverage, cov.Plan)
		}
		coverageName = "preflight-coverage.json"
		if err := swebench.WritePreflightCoverage(filepath.Join(outDir, coverageName), cov); err != nil {
			return nil, err
		}
		coverage = cov
	}
	if runFooting == "warm" && coverage == nil {
		f, err := os.OpenFile(eventsPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			rec := swebench.NewRecorder(f)
			rec.Emit(swebench.Event{
				RunID: runID,
				Event: swebench.EventPrewarm,
				Image: "prewarmed-image",
				DurMS: 10,
				OK:    true,
			})
			_ = rec.Close()
			_ = f.Close()
		}
	}

	// 5. Summarize and validate report.json, enrich system_description.json, and write report.md + run_window.json.
	rep, err := swebench.BuildReport(eventsPath)
	if err != nil {
		return nil, err
	}
	enrichSystemDescriptionFromArtifactDir(outDir, &sysDesc, rep, meas)
	if sysDesc.MaxConcurrentSandboxesCap <= 0 {
		if sysDesc.MaxActivePods > 0 {
			sysDesc.MaxConcurrentSandboxesCap = sysDesc.MaxActivePods
		} else if sysDesc.Load != nil && sysDesc.Load.MaxConcurrent > 0 {
			sysDesc.MaxConcurrentSandboxesCap = sysDesc.Load.MaxConcurrent
		} else {
			sysDesc.MaxConcurrentSandboxesCap = 16
		}
	}
	if sysDesc.LifecyclePolicy == nil {
		mode := "ephemeral"
		parkMode := "ephemeral"
		prewarm := false
		if runFooting == "warm" {
			mode = "prewarmed"
			parkMode = "suspend"
			prewarm = true
		}
		sysDesc.LifecyclePolicy = &schema.SandboxLifecyclePolicy{
			Mode:           mode,
			ParkMode:       parkMode,
			PrewarmAtStart: prewarm,
			// The prewarm events the run wrote, so the file on disk agrees
			// with report.json without the later AttachClusterTelemetry.
			PrewarmCount: rep.PrewarmCount,
		}
	}
	if err := sut.WriteSystemDescription(filepath.Join(outDir, "system_description.json"), sysDesc); err != nil {
		return nil, err
	}
	rep.AttachClusterTelemetry(&sysDesc, meas)
	swebench.Validate(rep, swebench.ValidateOptions{
		Footing:        runFooting,
		Schedule:       sched,
		TraceCaptured:  slices.ContainsFunc(runScen.Jobs, func(j schema.JobSpec) bool { return j.TraceMode == "trace" }),
		TraceFileCount: len(swebench.FindTraceFiles(outDir)),
		// The coverage is given as read, and named by its place in the
		// bundle, so the evidence holds after the bundle moves.
		PreflightCoverage:     coverage,
		PreflightCoveragePath: coverageName,
	})

	var mdBuf bytes.Buffer
	rep.WriteMarkdown(&mdBuf)
	if err := os.WriteFile(filepath.Join(outDir, "report.md"), mdBuf.Bytes(), 0o644); err != nil {
		return nil, err
	}
	if err := ensureRunWindowFile(outDir, runID, cluster, time.Time{}, time.Time{}, rep.TraceMetadata()); err != nil {
		return nil, err
	}

	repBytes, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(outDir, "report.json"), append(repBytes, '\n'), 0o644); err != nil {
		return nil, err
	}
	return rep, nil
}

func cloneScenario(scen *schema.Scenario) schema.Scenario {
	runScen := *scen
	runScen.SUT = cloneSUTSpec(scen.SUT)
	if len(scen.Jobs) > 0 {
		runScen.Jobs = slices.Clone(scen.Jobs)
		for i := range runScen.Jobs {
			runScen.Jobs[i].TrainerCommand = slices.Clone(scen.Jobs[i].TrainerCommand)
			runScen.Jobs[i].EnvVars = maps.Clone(scen.Jobs[i].EnvVars)
		}
	}
	return runScen
}

func cloneSUTSpec(s schema.SUTSpec) schema.SUTSpec {
	out := s
	if s.Substrate != nil {
		sub := *s.Substrate
		out.Substrate = &sub
	}
	if s.PoolEnabled != nil {
		pe := *s.PoolEnabled
		out.PoolEnabled = &pe
	}
	out.ExtraArgs = maps.Clone(s.ExtraArgs)
	out.ValuesOverlay = maps.Clone(s.ValuesOverlay)
	out.Labels = maps.Clone(s.Labels)
	return out
}

// acquisitionSource is the source the local scenario client reports for a
// run of runScen, after ov (nil without -overlay) was applied. The pool
// fields and the footing decide, with or without an overlay: a pool that is
// off acquires cold; a cold footing acquires cold, because the pool was not
// filled before the run window, unless the overlay itself sized or enabled
// the pool; a pool that is on or a warm footing acquires from the pool.
func acquisitionSource(runScen *schema.Scenario, ov *schema.FeatureOverlay) schema.AcquisitionSource {
	s := runScen.SUT
	poolOn := s.PoolSizePerImg > 0 || (s.PoolEnabled != nil && *s.PoolEnabled)
	overlayPool := ov != nil && (ov.WarmPoolSize > 0 ||
		ov.SUT.PoolSizePerImg > 0 ||
		(ov.SUT.PoolEnabled != nil && *ov.SUT.PoolEnabled))
	switch {
	case s.PoolEnabled != nil && !*s.PoolEnabled:
		return schema.SourceColdStart
	case runScen.Footing == "cold" && !overlayPool:
		// A base pool size stays in the manifest, but the run acquires cold.
		return schema.SourceColdStart
	case poolOn, runScen.Footing == "warm":
		return schema.SourceWarmPool
	default:
		return schema.SourceColdStart
	}
}

func resolveScenarioOverlay(name string, scen *schema.Scenario) (*schema.FeatureOverlay, error) {
	if err := schema.ValidateOverlayName(name); err != nil {
		return nil, err
	}
	var merged map[string]schema.FeatureOverlay
	if len(scen.Variants) > 0 || len(scen.Overlays) > 0 {
		merged = make(map[string]schema.FeatureOverlay, len(scen.Variants)+len(scen.Overlays))
		for _, v := range scen.Variants {
			if v.Name != "" {
				merged[v.Name] = v
			}
		}
		for k, v := range scen.Overlays {
			merged[k] = v
		}
	}
	ov, err := overlay.ResolveOverlay(name, merged)
	if err != nil {
		return nil, err
	}
	if err := ov.Validate(); err != nil {
		return nil, err
	}
	return ov, nil
}

// applyFeatureOverlay applies ov to runScen in place. An overlay that fails
// FeatureOverlay.Validate is an error before anything changes, so a value
// outside its range (prewarm_policy: "off", warm_pool_size: -4) never runs as
// the base scenario.
func applyFeatureOverlay(runScen *schema.Scenario, ov *schema.FeatureOverlay) error {
	if ov == nil {
		return nil
	}
	if err := ov.Validate(); err != nil {
		return err
	}
	if ov.PrewarmPolicy != "" {
		footing, err := schema.FootingForPrewarmPolicy(ov.PrewarmPolicy)
		if err != nil {
			return fmt.Errorf("overlay %q: %w", ov.Name, err)
		}
		runScen.Footing = footing
	}
	// warm_pool_size and prewarm_policy are separate knobs: a cold prewarm
	// policy leaves the base pool size alone.
	if ov.WarmPoolSize > 0 {
		runScen.SUT.PoolSizePerImg = ov.WarmPoolSize
	}
	// A pool size from the overlay turns the pool on, so a base with
	// sut.pool_enabled: false acquires from the pool the overlay sized. An
	// overlay sut.pool_enabled wins; the merge below applies it.
	if ov.SUT.PoolEnabled == nil && (ov.WarmPoolSize > 0 || ov.SUT.PoolSizePerImg > 0) {
		on := true
		runScen.SUT.PoolEnabled = &on
	}

	mergeSUTSpec(&runScen.SUT, ov.SUT)
	if err := runScen.SUT.Validate(); err != nil {
		return fmt.Errorf("overlay %q sut: %w", ov.Name, err)
	}
	// flags reach the trainer command of each real job only. The ate-env-api
	// arguments come from sut.extra_args, so a trainer flag never lands in
	// the manifest and an API flag never lands in the trainer argv. A
	// simulated job runs no trainer, and a trainer_manifest job takes no
	// argv, so flags on a scenario without a real trainer_command job are an
	// error: system_description.json would record them as active.
	if args := trainerFlagArgs(ov.Flags); len(args) > 0 {
		applied := 0
		for i := range runScen.Jobs {
			j := &runScen.Jobs[i]
			if !isRealJob(*j) {
				continue
			}
			if len(j.TrainerCommand) == 0 {
				return fmt.Errorf("overlay %q: flags reach a trainer_command only; real job %q runs a trainer_manifest", ov.Name, j.Name)
			}
			j.TrainerCommand = append(j.TrainerCommand, args...)
			applied++
		}
		if applied == 0 {
			return fmt.Errorf("overlay %q: flags need a real job with a trainer_command; the scenario has none", ov.Name)
		}
	}

	// snapshot_mode drives the runner's snapshot loop only. The park policy
	// is a separate knob and changes only when the overlay sets park or
	// park_policy.
	if ov.SnapshotMode != "" {
		loop, err := schema.SnapshotLoopForMode(ov.SnapshotMode)
		if err != nil {
			return fmt.Errorf("overlay %q: %w", ov.Name, err)
		}
		for i := range runScen.Jobs {
			runScen.Jobs[i].SnapshotLoop = loop
		}
	}

	rawPark := ov.Park
	if rawPark == "" && ov.ParkPolicy != "" {
		rawPark = schema.ParkPolicy(ov.ParkPolicy)
	}
	if rawPark != "" {
		normPark, err := schema.NormalizeParkPolicy(rawPark)
		if err != nil {
			return fmt.Errorf("overlay %q: %w", ov.Name, err)
		}
		for i := range runScen.Jobs {
			runScen.Jobs[i].Park = normPark
			runScen.Jobs[i].ParkPolicy = normPark
		}
	}

	for i := range runScen.Jobs {
		if ov.Prefetch != nil {
			runScen.Jobs[i].Prefetch = *ov.Prefetch
			if !*ov.Prefetch && ov.Lookahead == 0 {
				runScen.Jobs[i].LookaheadSteps = 0
			}
		}
		if ov.Lookahead > 0 {
			runScen.Jobs[i].LookaheadSteps = ov.Lookahead
			if ov.Prefetch == nil {
				runScen.Jobs[i].Prefetch = true
			}
		}
		if len(ov.EnvVars) > 0 {
			if runScen.Jobs[i].EnvVars == nil {
				runScen.Jobs[i].EnvVars = make(map[string]string, len(ov.EnvVars))
			}
			maps.Copy(runScen.Jobs[i].EnvVars, ov.EnvVars)
		}
	}
	return nil
}

// trainerFlagArgs renders overlay flags as sorted -name=value arguments. A key
// that already starts with "-" keeps its dashes.
func trainerFlagArgs(flags map[string]string) []string {
	if len(flags) == 0 {
		return nil
	}
	keys := slices.Sorted(maps.Keys(flags))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		name := k
		if !strings.HasPrefix(name, "-") {
			name = "-" + name
		}
		out = append(out, name+"="+flags[k])
	}
	return out
}

func mergeSUTSpec(dst *schema.SUTSpec, src schema.SUTSpec) {
	if src.Backend != "" {
		dst.Backend = src.Backend
	}
	if src.Access != "" {
		dst.Access = src.Access
	}
	if src.Endpoint != "" {
		dst.Endpoint = src.Endpoint
	}
	if src.Substrate != nil {
		if dst.Substrate == nil {
			dst.Substrate = &schema.SubstrateSUTSpec{}
		}
		if src.Substrate.Atespace != "" {
			dst.Substrate.Atespace = src.Substrate.Atespace
		}
		if src.Substrate.BaseTemplate != "" {
			dst.Substrate.BaseTemplate = src.Substrate.BaseTemplate
		}
	}
	if src.NodePool != "" {
		dst.NodePool = src.NodePool
	}
	if src.RuntimeClass != "" {
		dst.RuntimeClass = src.RuntimeClass
	}
	if src.Replicas > 0 {
		dst.Replicas = src.Replicas
	}
	if src.Namespace != "" {
		dst.Namespace = src.Namespace
	}
	if src.Template != "" {
		dst.Template = src.Template
	}
	if src.ServerAddr != "" {
		dst.ServerAddr = src.ServerAddr
	}
	if src.StorageBackend != "" {
		dst.StorageBackend = src.StorageBackend
	}
	if src.PoolEnabled != nil {
		pe := *src.PoolEnabled
		dst.PoolEnabled = &pe
	}
	if src.PoolSizePerImg > 0 {
		dst.PoolSizePerImg = src.PoolSizePerImg
	}
	if src.MaxActivePods > 0 {
		dst.MaxActivePods = src.MaxActivePods
	}
	if len(src.ExtraArgs) > 0 {
		if dst.ExtraArgs == nil {
			dst.ExtraArgs = make(map[string]string, len(src.ExtraArgs))
		}
		maps.Copy(dst.ExtraArgs, src.ExtraArgs)
	}
	if len(src.ValuesOverlay) > 0 {
		if dst.ValuesOverlay == nil {
			dst.ValuesOverlay = make(map[string]any, len(src.ValuesOverlay))
		}
		maps.Copy(dst.ValuesOverlay, src.ValuesOverlay)
	}
	if len(src.Labels) > 0 {
		if dst.Labels == nil {
			dst.Labels = make(map[string]string, len(src.Labels))
		}
		maps.Copy(dst.Labels, src.Labels)
	}
}

// recordResolvedJobSettings records on ref the snapshot loop the simulated
// jobs share after the overlay, and the park policy, prefetch and lookahead
// the closed-loop jobs share, because schedule.json on disk is the base
// schedule. An open-loop job runs no step schedule and ignores those three.
// A setting the jobs disagree on stays unset.
func recordResolvedJobSettings(ref *schema.OverlayRef, jobs []schema.JobSpec) {
	var (
		parks      []schema.ParkPolicy
		loops      []bool
		prefetches []bool
		lookaheads []int
	)
	for _, j := range jobs {
		if isRealJob(j) {
			continue
		}
		loops = append(loops, j.SnapshotLoop)
		if j.Mode == schema.JobModeOpenLoop {
			continue
		}
		park := j.Park
		if park == "" {
			park = j.ParkPolicy
		}
		if park != "" {
			if norm, err := schema.NormalizeParkPolicy(park); err == nil {
				park = norm
			}
		}
		parks = append(parks, park)
		prefetches = append(prefetches, j.Prefetch)
		lookaheads = append(lookaheads, j.LookaheadSteps)
	}
	if p, ok := uniform(parks); ok {
		ref.ParkPolicy = p
	}
	if l, ok := uniform(loops); ok {
		ref.SnapshotLoop = &l
	}
	if p, ok := uniform(prefetches); ok {
		ref.Prefetch = &p
	}
	if n, ok := uniform(lookaheads); ok {
		ref.LookaheadSteps = n
	}
}

// isRealJob reports whether j launches a real trainer, by mode or by kind.
func isRealJob(j schema.JobSpec) bool {
	return j.Mode == schema.JobModeReal || j.Kind == "real"
}

// uniform returns the one value all of vals hold, and false when vals is
// empty or holds more than one value.
func uniform[T comparable](vals []T) (T, bool) {
	var zero T
	if len(vals) == 0 {
		return zero, false
	}
	for _, v := range vals[1:] {
		if v != vals[0] {
			return zero, false
		}
	}
	return vals[0], true
}

// recordOverlayInputs copies into ref the overlay inputs that neither the
// manifest nor the base schedule shows: sut.extra_args as sut_overrides,
// flags as flags, and the names of env_vars as env_var_names. The three
// stay in separate fields, so an equal key in two of them survives. An
// env_vars value never reaches system_description.json, which bench upload
// publishes; the scenario the base_recipe_hash names holds it.
func recordOverlayInputs(ref *schema.OverlayRef, ov *schema.FeatureOverlay) {
	if ov == nil {
		return
	}
	if len(ov.SUT.ExtraArgs) > 0 {
		ref.SUTOverrides = maps.Clone(ov.SUT.ExtraArgs)
	}
	if len(ov.Flags) > 0 {
		ref.Flags = maps.Clone(ov.Flags)
	}
	if len(ov.EnvVars) > 0 {
		ref.EnvVarNames = slices.Sorted(maps.Keys(ov.EnvVars))
	}
}

// executionSchedule is the schedule the runner executes: a copy of sched
// whose prefetch hints follow each job's prefetch and lookahead_steps, after
// ov (nil without -overlay) was applied, and whose park_policy follows the
// jobs when ov sets park or park_policy. The generator fills every job's
// hints, so a prefetch: false job would emit prefetch_hint events from the
// base schedule. sched stays the base schedule, which schedule.json on disk
// holds.
func executionSchedule(sched *schema.Schedule, runScen *schema.Scenario, ov *schema.FeatureOverlay) *schema.Schedule {
	if sched == nil {
		return nil
	}
	jobByName := make(map[string]schema.JobSpec, len(runScen.Jobs))
	for _, j := range runScen.Jobs {
		jobByName[j.Name] = j
	}
	cp := *sched
	cp.Jobs = make([]schema.ScheduledJob, len(sched.Jobs))
	for i, sj := range sched.Jobs {
		sjCopy := sj
		sjCopy.Steps = make([]schema.ScheduledStep, len(sj.Steps))
		for s, st := range sj.Steps {
			stCopy := st
			stCopy.Tasks = slices.Clone(st.Tasks)
			stCopy.PrefetchImages = slices.Clone(st.PrefetchImages)
			sjCopy.Steps[s] = stCopy
		}
		if spec, ok := jobByName[sj.Job]; ok {
			if ov != nil && (ov.Park != "" || ov.ParkPolicy != "") {
				sjCopy.ParkPolicy = spec.ParkPolicy
			}
			if spec.Prefetch {
				// The generator's function and default, so a run with and
				// without an overlay gets the same hints for the same job
				// settings.
				swebench.PopulatePrefetchImages(sjCopy.Steps, spec.LookaheadSteps)
			} else {
				for s := range sjCopy.Steps {
					sjCopy.Steps[s].PrefetchImages = nil
				}
			}
		}
		cp.Jobs[i] = sjCopy
	}
	return &cp
}

func validateCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	destDir := fs.String("dest", "", "Path to benchmark artifact directory ($ARTIFACT_DIR) for full bundle validation.")
	folderName := fs.String("folder", "", "Optional GCS folder name to validate (<YYYY-MM-DD>_<model>_<dataset>_<nickname>[_traced]; _traced iff the bundle captured a trace).")
	reportPath := fs.String("report", "", "Path to report.json (single-file mode when -dest is omitted).")
	eventsPath := fs.String("events", "", "Optional path to events.jsonl (single-file mode).")
	schedulePath := fs.String("schedule", "", "Optional path to schedule.json (single-file mode).")
	footing := fs.String("footing", "", "Declared footing (warm or cold).")
	minTasks := fs.Int("min-tasks", 0, "Minimum required tasks.")
	minRewardRate := fs.Float64("min-reward-rate", 0, "Minimum required reward rate.")
	coveragePath := fs.String("preflight-coverage", "", "Optional preflight-coverage.json for Validity Rule 1 (single-file mode; a bundle's own is found without this).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts := swebench.ValidateOptions{
		Footing:               *footing,
		MinTasks:              *minTasks,
		MinRewardRate:         *minRewardRate,
		CheckRewardRate:       *minRewardRate > 0,
		PreflightCoveragePath: *coveragePath,
	}
	if strings.TrimSpace(*destDir) != "" {
		bRes, err := ValidateArtifactBundle(*destDir, opts, *folderName)
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(bRes, "", "  ")
		fmt.Fprintln(stdout, string(data))
		if !bRes.Valid {
			return fmt.Errorf("bundle validation failed: %s", strings.Join(bRes.Reasons, "; "))
		}
		return nil
	}
	if *reportPath == "" && *eventsPath == "" {
		return fmt.Errorf("either -dest, -report, or -events is required")
	}
	_, v, err := swebench.ValidateReportOrEvents(*reportPath, *eventsPath, *schedulePath, opts)
	if err != nil {
		return err
	}
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Fprintln(stdout, string(data))
	if !v.Valid {
		return fmt.Errorf("report validation failed: %s", strings.Join(v.Reasons, "; "))
	}
	return nil
}

func compareCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	fs.SetOutput(stderr)
	basePath := fs.String("baseline", "", "Path to baseline report.json.")
	candPath := fs.String("candidate", "", "Path to candidate report.json.")
	thresholdsPath := fs.String("thresholds", "", "Optional path to thresholds YAML/JSON.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *basePath == "" || *candPath == "" {
		return fmt.Errorf("-baseline and -candidate are required")
	}
	res, err := swebench.CompareReportFiles(*basePath, *candPath, *thresholdsPath)
	if err != nil {
		return err
	}
	fmt.Fprint(stdout, swebench.FormatComparisonMarkdown(res))
	if !res.Passed {
		return fmt.Errorf("comparison failed thresholds: %s", strings.Join(res.Regressions, "; "))
	}
	return nil
}

type localScenarioEnvClient struct {
	seq    atomic.Int64
	source string
}

func (c *localScenarioEnvClient) Create(_ context.Context, _, _ string) (swebench.EnvHandle, error) {
	id := c.seq.Add(1)
	src := c.source
	if src == "" {
		src = string(schema.SourceWarmPool)
	}
	return swebench.EnvHandle{
		Name:   fmt.Sprintf("env-%d", id),
		Source: src,
	}, nil
}

func (c *localScenarioEnvClient) Exec(_ context.Context, _, _ string, _ time.Duration) (swebench.ExecResult, error) {
	return swebench.ExecResult{ExitCode: 0, Stdout: "ok"}, nil
}

func (c *localScenarioEnvClient) Delete(_ context.Context, _ string) error {
	return nil
}

func (c *localScenarioEnvClient) Snapshot(_ context.Context, envName string) (string, error) {
	return "snap-" + envName, nil
}

func (c *localScenarioEnvClient) Resume(_ context.Context, snapshot, _, _ string) (swebench.EnvHandle, error) {
	return swebench.EnvHandle{
		Name:   "resumed-" + snapshot,
		Source: string(schema.SourceSnapshotResume),
	}, nil
}
