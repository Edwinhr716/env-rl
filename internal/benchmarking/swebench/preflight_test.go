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

package swebench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

const (
	pfImgA = "registry.example/swebench/a@sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pfImgB = "registry.example/swebench/b@sha256:" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pfImgC = "registry.example/swebench/c@sha256:" + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

// twoDomainPlan places a in d0 and b in both domains, two nodes each.
func twoDomainPlan() *schema.ImagePlacement {
	return &schema.ImagePlacement{
		SchemaVersion: schema.SchemaVersion,
		UnpackFactor:  1,
		Replication:   1,
		Domains: []schema.PlacementDomain{
			{Name: "d0", Selector: map[string]string{"ate-env/domain": "d0"}, Nodes: 2, NodeCacheBytes: 100, AssignedBytes: 2, ImageCount: 2},
			{Name: "d1", Selector: map[string]string{"ate-env/domain": "d1"}, Nodes: 2, NodeCacheBytes: 100, AssignedBytes: 1, ImageCount: 1},
		},
		Images: []schema.ImagePlacementEntry{
			{Image: pfImgA, Domains: []string{"d0"}, BytesCompressed: 1, BytesUnpacked: 1},
			{Image: pfImgB, Domains: []string{"d0", "d1"}, BytesCompressed: 1, BytesUnpacked: 1},
		},
	}
}

// probeID is the name ate-env-api gives a probe of the pass's first attempt:
// the pass's run id, the plan indexes of the domain and the image, the
// attempt (1), the round and the slot.
func probeID(runID string, domain, image, round, slot int) string {
	return fmt.Sprintf("%s-%d-%d-1-%d-%d", runID, domain, image, round, slot)
}

func newPreflightFake() *fakeAPI {
	return &fakeAPI{
		domainNodes:      map[string][]string{"d0": {"node-d0-1", "node-d0-2"}, "d1": {"node-d1-1", "node-d1-2"}},
		goldenReadyAfter: 2,
	}
}

func runPreflight(t *testing.T, api *fakeAPI, mutate func(*PreflightOptions)) (*schema.PreflightCoverage, error, []Event) {
	t.Helper()
	b := newFakeBackend(t, api)
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	opts := PreflightOptions{
		Plan:          twoDomainPlan(),
		PlanPath:      "image-placement.json",
		Backend:       b,
		Recorder:      rec,
		RunID:         "pf-test",
		ProbesPerNode: 1,
		MaxRounds:     3,
		GoldenTimeout: time.Second,
		GoldenPoll:    time.Millisecond,
		ProbeTimeout:  5 * time.Second,
	}
	if mutate != nil {
		mutate(&opts)
	}
	w, err := NewPreflightWarmer(opts)
	if err != nil {
		t.Fatal(err)
	}
	report, runErr := w.Run(context.Background())
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	var events []Event
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		if err := schema.ValidateAgainstJSONSchema("event.schema.json", []byte(line)); err != nil {
			t.Fatalf("event schema %q: %v", line, err)
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("event %q: %v", line, err)
		}
		events = append(events, e)
	}
	if err := report.Validate(); err != nil {
		t.Fatalf("report does not validate: %v", err)
	}
	rawReport, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateAgainstJSONSchema("preflight_coverage.schema.json", rawReport); err != nil {
		t.Fatalf("report JSON schema: %v", err)
	}
	return report, runErr, events
}

func countEvents(events []Event, kind string) int {
	n := 0
	for _, e := range events {
		if e.Event == kind {
			n++
		}
	}
	return n
}

func TestPreflightCoversEveryNodeOfEveryDomain(t *testing.T) {
	api := newPreflightFake()
	// One image at a time per domain: the fake spreads a domain's creates
	// round-robin over its nodes, so a round of two probes covers both nodes
	// only when no other image's probes interleave.
	report, err, events := runPreflight(t, api, func(o *PreflightOptions) { o.ImageConcurrency = 1 })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Complete {
		t.Errorf("report incomplete: %+v", report)
	}
	// Goldens: ensured once per image (in parallel, so in any order), polled
	// until ready, tag recorded, in plan order.
	ensured := append([]string(nil), api.ensured...)
	sort.Strings(ensured)
	if !reflect.DeepEqual(ensured, []string{pfImgA, pfImgB}) {
		t.Errorf("ensured = %v", api.ensured)
	}
	if len(report.Goldens) != 2 || report.Goldens[0].Image != pfImgA || report.Goldens[1].Image != pfImgB {
		t.Errorf("goldens = %+v, want a then b", report.Goldens)
	}
	for _, g := range report.Goldens {
		if g.State != "ready" || !strings.HasPrefix(g.Tag, "golden-") || g.Template == "" {
			t.Errorf("golden %+v, want ready with a tag and template", g)
		}
	}
	// Coverage: a on both nodes of d0; b on both nodes of d0 and d1.
	want := map[string]map[string][]string{
		"d0": {pfImgA: {"node-d0-1", "node-d0-2"}, pfImgB: {"node-d0-1", "node-d0-2"}},
		"d1": {pfImgB: {"node-d1-1", "node-d1-2"}},
	}
	for _, d := range report.Domains {
		if !d.Complete {
			t.Errorf("domain %s incomplete: %+v", d.Name, d)
		}
		if len(d.Images) != len(want[d.Name]) {
			t.Errorf("domain %s covers %d images, want %d", d.Name, len(d.Images), len(want[d.Name]))
		}
		for _, ic := range d.Images {
			if got := want[d.Name][ic.Image]; !reflect.DeepEqual(ic.Nodes, got) || !ic.Complete || ic.Rounds != 1 || ic.Probes != 2 || ic.Failed != 0 {
				t.Errorf("%s/%s = %+v, want nodes %v in one round of two probes", d.Name, ic.Image, ic, got)
			}
		}
	}
	// Every probe carried its domain's selector and was deleted.
	for id, sel := range api.selectors {
		if sel["ate-env/domain"] == "" {
			t.Errorf("probe %s has no domain selector: %v", id, sel)
		}
	}
	if len(api.deleted) != len(api.created) || len(api.suspended) != 0 {
		t.Errorf("created %d, deleted %d, suspended %d; want all deleted", len(api.created), len(api.deleted), len(api.suspended))
	}
	// Events: a template and a golden per image, a probe per environment
	// with its node; no run event (prewarm) the report would count.
	if n := countEvents(events, EventPreflightTemplate); n != 2 {
		t.Errorf("template events = %d, want 2", n)
	}
	if n := countEvents(events, EventPreflightGolden); n != 2 {
		t.Errorf("golden events = %d, want 2", n)
	}
	if n := countEvents(events, EventPrewarm); n != 0 {
		t.Errorf("prewarm events = %d, want none", n)
	}
	probes := 0
	for _, e := range events {
		if e.Event != EventPreflightProbe {
			continue
		}
		probes++
		if e.Node == "" || e.Labels["domain"] == "" || !e.OK || e.Env == "" || e.Image == "" {
			t.Errorf("probe event lacks node, domain, env or image: %+v", e)
		}
	}
	if probes != len(api.created) {
		t.Errorf("probe events = %d, created = %d", probes, len(api.created))
	}
}

// With a concurrency above one a domain warms several images at once, so
// more than nodes x ProbesPerNode probes are alive in it; with one, never.
func TestPreflightImageConcurrency(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	// Slow probes: a round holds its probes a while, so the two images of
	// d0 overlap when the warmer lets them.
	api.warmHold = 20 * time.Millisecond
	if _, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.ImageConcurrency = 2 }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if api.maxInflight["d0"] <= 2 {
		t.Errorf("d0 max in flight = %d with concurrency 2, want above nodes x probes (2)", api.maxInflight["d0"])
	}

	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmHold = 20 * time.Millisecond
	if _, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.ImageConcurrency = 1 }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if api.maxInflight["d0"] > 2 {
		t.Errorf("d0 max in flight = %d with concurrency 1, want at most 2", api.maxInflight["d0"])
	}

	// node_slots caps the concurrency and refuses too many probes per node.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmHold = 20 * time.Millisecond
	if _, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.ImageConcurrency = 4
		o.Plan.Domains[0].NodeSlots = 1
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if api.maxInflight["d0"] > 2 {
		t.Errorf("d0 max in flight = %d with one slot per node, want at most 2", api.maxInflight["d0"])
	}
	plan := twoDomainPlan()
	plan.Domains[0].NodeSlots = 1
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: newFakeBackend(t, newPreflightFake()), ProbesPerNode: 2}); err == nil || !strings.Contains(err.Error(), "node slots") {
		t.Errorf("2 probes per node on 1 slot: err = %v, want a node slots error", err)
	}
}

// A domain does not wait for a slow golden before warming the images behind
// it: a, first in plan order, never gets its golden; b, behind it in d0, is
// covered while a's golden is still pending, with one image at a time.
func TestPreflightStreamsReadyGoldensIntoCoverage(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.goldenPendingForever = map[string]bool{pfImgA: true}
	report, err, events := runPreflight(t, api, func(o *PreflightOptions) {
		o.ImageConcurrency = 1
		o.GoldenTimeout = 200 * time.Millisecond
	})
	if err == nil || !strings.Contains(err.Error(), "1 goldens not ready") {
		t.Fatalf("Run error = %v, want a's golden not ready", err)
	}
	// The recorder keeps the order: every probe of b in d0 is recorded
	// before a's golden gives up, and a's probes after it.
	aGolden, bProbesBefore, aProbesAfter := -1, 0, 0
	for i, e := range events {
		switch {
		case e.Event == EventPreflightGolden && e.Image == pfImgA:
			aGolden = i
		case e.Event == EventPreflightProbe && e.Image == pfImgB && e.Labels["domain"] == "d0" && aGolden < 0:
			bProbesBefore++
		case e.Event == EventPreflightProbe && e.Image == pfImgA && aGolden >= 0:
			aProbesAfter++
		}
	}
	if aGolden < 0 || bProbesBefore != 2 || aProbesAfter != 2 {
		t.Errorf("b probes in d0 before a's golden settled = %d, a probes after = %d; want 2 and 2", bProbesBefore, aProbesAfter)
	}
	for _, d := range report.Domains {
		if !d.Complete {
			t.Errorf("domain %s incomplete: a pending golden still boots and fills the layers: %+v", d.Name, d)
		}
	}
}

func TestPreflightReportsShortCoverageAfterItsRounds(t *testing.T) {
	api := newPreflightFake()
	api.sameNode = true
	report, err, _ := runPreflight(t, api, nil)
	if err == nil || !strings.Contains(err.Error(), "short of their nodes") {
		t.Fatalf("Run error = %v, want a shortfall", err)
	}
	if report.Complete {
		t.Error("report complete with one node per domain")
	}
	for _, d := range report.Domains {
		if d.Complete || len(d.NodesSeen) != 1 {
			t.Errorf("domain %s: complete=%v nodes seen %v", d.Name, d.Complete, d.NodesSeen)
		}
		for _, ic := range d.Images {
			if ic.Complete || ic.Rounds != 3 || len(ic.Nodes) != 1 || ic.Probes != 6 {
				t.Errorf("%s/%s = %+v, want 3 rounds of 2 probes on one node", d.Name, ic.Image, ic)
			}
		}
	}
	if len(api.deleted) != len(api.created) {
		t.Errorf("created %d, deleted %d", len(api.created), len(api.deleted))
	}
}

func TestPreflightGoldenFailure(t *testing.T) {
	api := newPreflightFake()
	api.goldenFail = map[string]string{pfImgB: "GoldenActorCrashed: boom"}
	report, err, events := runPreflight(t, api, nil)
	if err == nil || !strings.Contains(err.Error(), "1 goldens not ready") {
		t.Fatalf("Run error = %v, want one golden not ready", err)
	}
	var b schema.GoldenRecord
	for _, g := range report.Goldens {
		if g.Image == pfImgB {
			b = g
		}
	}
	if b.State != "failed" || !strings.Contains(b.Error, "boom") {
		t.Errorf("b golden = %+v, want failed with the reason", b)
	}
	// The coverage pass still ran: a failed golden means boots, and boots
	// fill the layers too.
	for _, d := range report.Domains {
		if !d.Complete {
			t.Errorf("domain %s incomplete with a failed golden: %+v", d.Name, d)
		}
	}
	failedGoldenEvents := 0
	for _, e := range events {
		if e.Event == EventPreflightGolden && !e.OK {
			failedGoldenEvents++
		}
	}
	if failedGoldenEvents != 1 {
		t.Errorf("failed golden events = %d, want 1", failedGoldenEvents)
	}
}

// Goldens disabled on the base template: the templates exist with no
// golden, the pass does not wait, and completes on coverage alone.
func TestPreflightGoldensDisabled(t *testing.T) {
	api := newPreflightFake()
	api.goldenDisabled = true
	report, err, _ := runPreflight(t, api, nil)
	if err != nil || !report.Complete {
		t.Fatalf("goldens disabled: err=%v complete=%v", err, report.Complete)
	}
	for _, g := range report.Goldens {
		if g.State != "none" || g.Error != "" || g.Template == "" {
			t.Errorf("golden = %+v, want none with the template", g)
		}
	}
	if n := api.statusCalls[pfImgA]; n != 1 {
		t.Errorf("status calls = %d, want the one from ensure", n)
	}
}

func TestPreflightGoldenTimeout(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 1 << 20
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.GoldenTimeout = 5 * time.Millisecond })
	if err == nil || !strings.Contains(err.Error(), "2 goldens not ready") {
		t.Fatalf("Run error = %v, want both goldens pending", err)
	}
	for _, g := range report.Goldens {
		if g.State != "pending" || !strings.Contains(g.Error, "still pending") || g.Template == "" {
			t.Errorf("golden = %+v, want pending with a timeout error and the template it was ensured as", g)
		}
	}
}

// A GoldenTimeout below GoldenPoll ends the wait at the timeout, not at the
// first poll.
func TestPreflightGoldenTimeoutBelowThePollInterval(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 1 << 20
	started := time.Now()
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.GoldenTimeout = 100 * time.Millisecond
		o.GoldenPoll = 10 * time.Second
	})
	if elapsed := time.Since(started); elapsed < 100*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("Run took %s, want the golden timeout (100ms), not the poll interval (10s)", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "2 goldens not ready") {
		t.Fatalf("Run error = %v, want both goldens pending", err)
	}
	for _, g := range report.Goldens {
		if g.State != "pending" || !strings.Contains(g.Error, "still pending") {
			t.Errorf("golden = %+v, want pending with a timeout error", g)
		}
	}
}

// A round in which every probe fails ends the pair, also after a round that
// placed some: round 1 lands both probes on one node, round 2's creates fail,
// and no round 3 follows.
func TestPreflightStopsWhenARoundFailsEntirely(t *testing.T) {
	// Creates the control plane cut off (Unavailable) may have landed: they
	// are deleted by id, next to the probes that were created.
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.sameNode = true
	api.createFailAfter = 2
	api.createFailCode = codes.Unavailable
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.MaxRounds = 3
	})
	if err == nil || !strings.Contains(err.Error(), "short of their nodes") {
		t.Fatalf("Run error = %v, want a shortfall", err)
	}
	ic := report.Domains[0].Images[0]
	if ic.Rounds != 2 || ic.Probes != 4 || ic.Failed != 2 || len(ic.Nodes) != 1 || !strings.Contains(ic.Error, "cut the create off") {
		t.Errorf("coverage = %+v, want two rounds, the second failed entirely", ic)
	}
	// Every created probe was deleted; a create the server refused made
	// nothing to delete.
	deleted := map[string]bool{}
	for _, id := range api.deleted {
		deleted[id] = true
	}
	for _, id := range api.created {
		if !deleted[id] {
			t.Errorf("probe %s was created and not deleted", id)
		}
	}
	if len(api.deleted) != 2 {
		t.Errorf("deletes = %d, want 2: the two probes of the first round", len(api.deleted))
	}
}

// node_slots bounds only the domains of the pass.
func TestPreflightNodeSlotsApplyToSelectedDomains(t *testing.T) {
	plan := twoDomainPlan()
	plan.Domains[1].NodeSlots = 1
	backend := newFakeBackend(t, newPreflightFake())
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend, ProbesPerNode: 2, Domains: []string{"d0"}}); err != nil {
		t.Errorf("d0 alone with d1 at one slot: %v", err)
	}
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend, ProbesPerNode: 2}); err == nil || !strings.Contains(err.Error(), "node slots") {
		t.Errorf("both domains: err = %v, want the node slots error", err)
	}
}

func TestPreflightSubsetsAndValidation(t *testing.T) {
	api := newPreflightFake()
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.Images = []string{pfImgA}; o.Domains = []string{"d0"} })
	if err != nil || !report.Complete {
		t.Fatalf("subset run: err=%v complete=%v", err, report.Complete)
	}
	if len(report.Goldens) != 1 || len(report.Domains) != 1 || len(report.Domains[0].Images) != 1 {
		t.Errorf("subset report = %+v", report)
	}
	// A domain subset drops the images no selected domain holds, so the
	// pass does not wait for goldens it does not use. Its probe names still
	// carry the domain's index in the plan, so parallel -domains passes
	// sharing the default -run-id do not collide on pf-0-....
	apiD0 := newPreflightFake()
	if _, err, _ := runPreflight(t, apiD0, func(o *PreflightOptions) { o.RunID = ""; o.Domains = []string{"d0"} }); err != nil {
		t.Fatalf("d0 only: %v", err)
	}
	api = newPreflightFake()
	report, err, _ = runPreflight(t, api, func(o *PreflightOptions) { o.RunID = ""; o.Domains = []string{"d1"} })
	if err != nil || !report.Complete || len(report.Goldens) != 1 || report.Goldens[0].Image != pfImgB {
		t.Errorf("d1 only: err=%v goldens=%+v, want b alone", err, report.Goldens)
	}
	d0Names := map[string]bool{}
	for _, id := range apiD0.created {
		d0Names[id] = true
	}
	for _, id := range api.created {
		// The default run id is pf-<6 random characters>; the plan's domain
		// index follows it.
		if !strings.HasPrefix(id, "pf-") || !strings.Contains(id, "-1-") || !strings.HasPrefix(id[len("pf-")+6:], "-1-") {
			t.Errorf("d1 probe id = %q, want plan domain index 1 (pf-<rand>-1-...)", id)
		}
		if d0Names[id] {
			t.Errorf("d1 probe id %q collides with d0 pass", id)
		}
	}
	for _, bad := range []func(*PreflightOptions){
		func(o *PreflightOptions) { o.Images = []string{pfImgC} },
		func(o *PreflightOptions) { o.Domains = []string{"d9"} },
		func(o *PreflightOptions) { o.Images = []string{pfImgA}; o.Domains = []string{"d1"} },
		func(o *PreflightOptions) { o.RunID = "Not Valid" },
		func(o *PreflightOptions) { o.Plan = nil },
		func(o *PreflightOptions) { o.Backend = nil },
		// ate-env-api's WarmImage refuses these: a refused call would only
		// show as a short pair.
		func(o *PreflightOptions) { o.ProbesPerNode = 9 },
		func(o *PreflightOptions) { o.MaxRounds = 11 },
	} {
		opts := PreflightOptions{Plan: twoDomainPlan(), Backend: newFakeBackend(t, newPreflightFake())}
		bad(&opts)
		if _, err := NewPreflightWarmer(opts); err == nil {
			t.Error("NewPreflightWarmer accepted an invalid option")
		}
	}
}

func TestPreflightRepeatsACutOffWarmCall(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmUnavailableFirst = 3
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.wakeBackoff = time.Millisecond
	})
	if err != nil || !report.Complete {
		t.Fatalf("transient WarmImage Unavailable: err=%v complete=%v", err, report.Complete)
	}
	if ic := report.Domains[0].Images[0]; ic.Rounds != 1 || ic.Failed != 0 {
		t.Errorf("coverage after the repeats = %+v, want 1 round and 0 failed", ic)
	}
	if api.warmCalls != 4 {
		t.Errorf("warm calls = %d, want 3 cut off and 1 that ran", api.warmCalls)
	}

	// A call cut off after a round that covered one node of two (an api
	// server restart): the old probes stay, the repeat sends its own run id,
	// so no name collides.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.domainNodes["d0"] = []string{"node-d0-1", "node-d0-1", "node-d0-2", "node-d0-2"}
	api.warmCutAfterCreates = 1
	report, err, _ = runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.wakeBackoff = time.Millisecond
	})
	if len(api.warmRequests) != 2 || api.warmRequests[0].GetRunId() == api.warmRequests[1].GetRunId() || !strings.HasSuffix(api.warmRequests[1].GetRunId(), "-2") {
		t.Fatalf("warm requests = %d, want two with one run id per attempt", len(api.warmRequests))
	}
	if left := len(api.created) - len(api.deleted); left != 2 {
		t.Errorf("probes left behind by the cut-off call = %d, want the round's two", left)
	}
	// The coverage holds; the cut-off call's probes fail the pass by name
	// (TestPreflightListsTheProbesOfACutOffCall).
	if !report.Complete || err == nil || !strings.Contains(err.Error(), "2 probes left undeleted") {
		t.Errorf("cut off after the creates: complete=%v err=%v, want complete with the two leftovers named", report.Complete, err)
	}

	// The repeats stop after ProbeTimeout when the control plane stays down.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmFail = status.Error(codes.Unavailable, "down")
	start := time.Now()
	report, err, _ = runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.wakeBackoff = time.Millisecond
		o.ProbeTimeout = 60 * time.Millisecond
	})
	if err == nil || report.Complete || time.Since(start) > 5*time.Second {
		t.Fatalf("control plane down: err=%v complete=%v after %s, want a bounded failure", err, report.Complete, time.Since(start))
	}
	if api.warmCalls < 2 || api.warmCalls > maxWarmAttempts {
		t.Errorf("warm calls = %d, want repeats within %d attempts and the probe timeout", api.warmCalls, maxWarmAttempts)
	}

	// A call the server refuses is not repeated: the pair is short with
	// the server's reason.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmFail = status.Error(codes.InvalidArgument, "probes_per_node 4 exceeds the 2 probes per node this server runs on one worker selector at once")
	report, err, _ = runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.wakeBackoff = time.Millisecond
	})
	if err == nil || report.Complete {
		t.Fatalf("refused WarmImage: err=%v complete=%v, want failure", err, report.Complete)
	}
	if ic := report.Domains[0].Images[0]; !strings.Contains(ic.Error, "probes_per_node 4 exceeds") || len(ic.Nodes) != 0 {
		t.Errorf("coverage = %+v, want the server's reason and no nodes", ic)
	}
	if api.warmCalls != 1 {
		t.Errorf("warm calls = %d, want 1: no repeat of a refusal", api.warmCalls)
	}
}

// The repeats of a cut-off call last for ProbeTimeout from the first
// failure: a backoff longer than what is left of it is cut to that, so a
// short ProbeTimeout still gets its repeat.
func TestPreflightRepeatBackoffIsCutToTheBudget(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmUnavailableFirst = 1
	start := time.Now()
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.wakeBackoff = 30 * time.Second
		o.ProbeTimeout = 50 * time.Millisecond
	})
	if api.warmCalls != 2 {
		t.Fatalf("warm calls = %d, want 2: one repeat inside the 50ms budget", api.warmCalls)
	}
	if err != nil || !report.Complete {
		t.Errorf("err=%v complete=%v, want the repeat to complete the pass", err, report.Complete)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the pass took %s, want the wait cut to the 50ms budget", took)
	}
}

// A WarmImage call that breaks after it streamed probes never says which of
// them the server deleted (the coverage comes last, and an api server that
// restarted deleted none). The pass lists every probe such a call streamed
// under undeleted_probes and fails naming them; it does not repeat a call
// whose stream showed every node.
func TestPreflightListsTheProbesOfACutOffCall(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmCutAfterCreates = 1
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.wakeBackoff = time.Millisecond
	})
	left := []string{probeID("pf-test", 0, 0, 1, 0), probeID("pf-test", 0, 0, 1, 1)}
	if created, deleted := len(api.created), len(api.deleted); created-deleted != 2 {
		t.Fatalf("created %d, deleted %d: want the cut-off round's two probes left", created, deleted)
	}
	if err == nil || !strings.Contains(err.Error(), "2 probes left undeleted") || !strings.Contains(err.Error(), left[0]) {
		t.Errorf("Run error = %v, want the two probes the cut-off call left named", err)
	}
	ic := report.Domains[0].Images[0]
	if !ic.Complete || !reflect.DeepEqual(ic.Nodes, []string{"node-d0-1", "node-d0-2"}) || !reflect.DeepEqual(ic.UndeletedProbes, left) {
		t.Errorf("coverage = %+v, want complete from the stream with %v undeleted", ic, left)
	}
	if ic.Probes != 2 || ic.Rounds != 1 || !strings.Contains(ic.Error, "api server restarted") {
		t.Errorf("coverage = %+v, want the stream's two probes, one round and the call's error", ic)
	}
	if api.warmCalls != 1 {
		t.Errorf("warm calls = %d, want 1: the stream showed every node, no repeat", api.warmCalls)
	}
}

// The probes of a call that an api server restart cut off are named after
// the call's run id and nothing else records them (the server holds a
// call's probes in memory), so every log line about a cut-off call carries
// that run id: the repeat, the give-up, and the call that is not repeated.
func TestPreflightLogsTheRunIDOfACutOffCall(t *testing.T) {
	run := func(api *fakeAPI, mutate func(*PreflightOptions)) string {
		var mu sync.Mutex
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(lockedWriter{&mu, &logs}, nil))
		runPreflight(t, api, func(o *PreflightOptions) {
			o.Images = []string{pfImgA}
			o.Domains = []string{"d0"}
			o.wakeBackoff = time.Millisecond
			o.Logger = logger
			if mutate != nil {
				mutate(o)
			}
		})
		mu.Lock()
		defer mu.Unlock()
		return logs.String()
	}
	has := func(logs, msg, runID string) bool {
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, msg) && strings.Contains(line, "run_id="+runID+" ") {
				return true
			}
		}
		return false
	}

	// One repeat, then a clean call.
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmUnavailableFirst = 1
	if logs := run(api, nil); !has(logs, "repeating", "pf-test-0-0-1") {
		t.Errorf("the repeat's log line has no run_id=pf-test-0-0-1:\n%s", logs)
	}

	// The control plane stays down: the give-up names the last call.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmFail = status.Error(codes.Unavailable, "down")
	logs := run(api, func(o *PreflightOptions) { o.ProbeTimeout = 20 * time.Millisecond })
	last := warmRunID("pf-test", 0, 0, api.warmCalls)
	if !has(logs, "giving up", last) {
		t.Errorf("the give-up's log line has no run_id=%s:\n%s", last, logs)
	}

	// Cut off after the stream showed every node: no repeat, one line.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.warmCutAfterCreates = 1
	if logs := run(api, nil); !has(logs, "not repeated", "pf-test-0-0-1") {
		t.Errorf("the cut-off call's log line has no run_id=pf-test-0-0-1:\n%s", logs)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// The coverage of a pair is the sum of its attempts: a node the cut-off call
// showed stays covered when the repeat lands elsewhere, and the counters add
// up.
func TestPreflightSumsTheAttemptsOfARepeatedCall(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	// Four workers on two nodes: the first two probes of the image land on
	// node-d0-1 (the cut-off call), the next two on node-d0-2 (the repeat).
	api.domainNodes["d0"] = []string{"node-d0-1", "node-d0-1", "node-d0-2", "node-d0-2"}
	api.warmCutAfterCreates = 1
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.MaxRounds = 1
		o.wakeBackoff = time.Millisecond
	})
	if api.warmCalls != 2 {
		t.Fatalf("warm calls = %d, want the cut-off call and one repeat", api.warmCalls)
	}
	ic := report.Domains[0].Images[0]
	if !ic.Complete || !reflect.DeepEqual(ic.Nodes, []string{"node-d0-1", "node-d0-2"}) {
		t.Errorf("coverage = %+v, want both nodes: one from each attempt", ic)
	}
	if ic.Probes != 4 || ic.Rounds != 2 || ic.Failed != 0 {
		t.Errorf("coverage = %+v, want 4 probes and 2 rounds over the two attempts", ic)
	}
	left := []string{probeID("pf-test", 0, 0, 1, 0), probeID("pf-test", 0, 0, 1, 1)}
	if !reflect.DeepEqual(ic.UndeletedProbes, left) {
		t.Errorf("undeleted = %v, want the cut-off call's %v", ic.UndeletedProbes, left)
	}
	if !report.Complete || err == nil || !strings.Contains(err.Error(), "2 probes left undeleted") {
		t.Errorf("Run: complete=%v err=%v, want complete and the leftovers named", report.Complete, err)
	}
}

func TestPreflightCancellationSkipsUnstartedProbes(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.domainNodes["d0"] = []string{"n0", "n1", "n2", "n3"}
	// Four probes per node: above the server's default cap of two, so the
	// fake's server runs with -warm-max-probes-per-node=4.
	api.warmMaxProbesPerNode = 4

	plan := twoDomainPlan()
	plan.Domains[0].Nodes = 4
	w, err := NewPreflightWarmer(PreflightOptions{
		Plan:          plan,
		Backend:       newFakeBackend(t, api),
		RunID:         "pf-cancel",
		ProbesPerNode: 4,
		Images:        []string{pfImgA},
		Domains:       []string{"d0"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// A cancel during the call: the server stops at the next probe, deletes
	// what it created, and the call ends in Canceled; the coverage carries
	// what the stream showed, with no empty node.
	inner, innerCancel := context.WithCancel(context.Background())
	api.mu.Lock()
	api.onCreate = func(string) { innerCancel() }
	api.mu.Unlock()
	ic := w.coverImage(inner, 0, 0, plan.Domains[0], pfImgA)
	for _, n := range ic.Nodes {
		if n == "" {
			t.Fatalf("empty node recorded from a cancelled call: %+v", ic)
		}
	}
	if ic.Complete || ic.Probes >= 16 || ic.Error == "" {
		t.Fatalf("cancelled call: coverage=%+v, want incomplete, fewer than 16 probes and an error", ic)
	}
	// The server deletes the broken call's probes once the stream is gone,
	// on its own clock: shortly after the cancel returns.
	for deadline := time.Now().Add(5 * time.Second); ; {
		api.mu.Lock()
		created, deleted := len(api.created), len(api.deleted)
		api.mu.Unlock()
		if created == deleted || time.Now().After(deadline) {
			if created != deleted {
				t.Fatalf("created %d, deleted %d after the cancel; want all deleted", created, deleted)
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Cancel mid-round during Run via CreateEnvironment callback.
	ctx, cancel := context.WithCancel(context.Background())
	api.mu.Lock()
	api.onCreate = func(string) { cancel() }
	api.mu.Unlock()
	report, runErr := w.Run(ctx)
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", runErr)
	}
	if report.Complete {
		t.Fatalf("cancelled report marked complete: %+v", report)
	}
	if err := report.Validate(); err != nil {
		t.Fatalf("cancelled report.Validate: %v", err)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateAgainstJSONSchema("preflight_coverage.schema.json", raw); err != nil {
		t.Fatalf("cancelled report schema: %v\n%s", err, string(raw))
	}

	// Pre-cancelled context before Run starts: domain coverage still
	// validates and can be written by WritePreflightCoverage.
	api.mu.Lock()
	api.onCreate = nil
	api.mu.Unlock()
	preCtx, preCancel := context.WithCancel(context.Background())
	preCancel()
	report2, runErr2 := w.Run(preCtx)
	if !errors.Is(runErr2, context.Canceled) {
		t.Fatalf("pre-cancelled Run err = %v, want context.Canceled", runErr2)
	}
	if err := report2.Validate(); err != nil {
		t.Fatalf("pre-cancelled report.Validate: %v", err)
	}
	raw2, err := json.Marshal(report2)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateAgainstJSONSchema("preflight_coverage.schema.json", raw2); err != nil {
		t.Fatalf("pre-cancelled report schema: %v\n%s", err, string(raw2))
	}
}

// A probe name another pass or a user holds is not ours: the create fails
// with AlreadyExists, the probe counts as failed, and nothing deletes the
// environment behind the name (a cut-off create is deleted by id:
// TestPreflightStopsWhenARoundFailsEntirely).
func TestPreflightLeavesACollidingNameAlone(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	taken := probeID("pf-test", 0, 0, 1, 0)
	api.existing = map[string]bool{taken: true}
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.MaxRounds = 1
	})
	if err == nil || !strings.Contains(err.Error(), "short of their nodes") {
		t.Fatalf("Run error = %v, want a shortfall", err)
	}
	ic := report.Domains[0].Images[0]
	if ic.Probes != 2 || ic.Failed != 1 || !strings.Contains(ic.Error, "already exists") {
		t.Errorf("coverage = %+v, want one refused probe", ic)
	}
	for _, id := range api.deleted {
		if id == taken {
			t.Errorf("the environment %s, which is not ours, was deleted", taken)
		}
	}
	if len(api.deleted) != 1 {
		t.Errorf("deletes = %v, want only our own probe", api.deleted)
	}
}

// The fake API refuses the WarmImage requests ate-env-api refuses, with the
// server's code, so a pre-flight test cannot pass on a request a real server
// turns down. The same table drives the Python fake
// (test_the_fake_refuses_what_the_server_refuses).
func TestFakeWarmImageRefusesWhatTheServerRefuses(t *testing.T) {
	api := newPreflightFake() // d0 and d1, two nodes each
	api.domainNodes["wide"] = make([]string, 129)
	for i := range api.domainNodes["wide"] {
		api.domainNodes["wide"][i] = fmt.Sprintf("n%d", i)
	}
	b := newFakeBackend(t, api)
	d0 := map[string]string{"ate-env/domain": "d0"}
	ok := WarmOptions{WorkerSelector: d0, Nodes: 2, ProbesPerNode: 2, MaxRounds: 3, ProbeTimeout: time.Second, RunID: "pf-ok-0-0-1"}
	if _, err := b.WarmImage(context.Background(), pfImgA, ok, nil); err != nil {
		t.Fatalf("a request within the server's bounds: %v", err)
	}
	for name, tc := range map[string]struct {
		mutate func(*WarmOptions)
		cap    int
		want   string
	}{
		"probes per node above the wire bound":  {func(o *WarmOptions) { o.ProbesPerNode = 9 }, 8, "probes_per_node must be between 1 and 8"},
		"probes per node above the server cap":  {func(o *WarmOptions) { o.ProbesPerNode = 3 }, 0, "exceeds the 2 probes per node"},
		"probes per node above a raised cap":    {func(o *WarmOptions) { o.ProbesPerNode = 5 }, 4, "exceeds the 4 probes per node"},
		"rounds above the wire bound":           {func(o *WarmOptions) { o.MaxRounds = 11 }, 0, "max_rounds must be between 1 and 10"},
		"negative rounds":                       {func(o *WarmOptions) { o.MaxRounds = -1 }, 0, "max_rounds must be between 1 and 10"},
		"a run id of 21 characters":             {func(o *WarmOptions) { o.RunID = strings.Repeat("a", 21) }, 0, "run_id must match"},
		"a run id outside [a-z0-9-]":            {func(o *WarmOptions) { o.RunID = "PF_1" }, 0, "run_id must match"},
		"a run id that ends in a dash":          {func(o *WarmOptions) { o.RunID = "pf-" }, 0, "run_id must match"},
		"more nodes than the selector's":        {func(o *WarmOptions) { o.Nodes = 3 }, 0, "nodes 3 exceeds the 2 nodes"},
		"negative nodes":                        {func(o *WarmOptions) { o.Nodes = -1 }, 0, "nodes must be >= 0"},
		"a round of more than 256 probes":       {func(o *WarmOptions) { o.WorkerSelector = map[string]string{"ate-env/domain": "wide"}; o.Nodes = 129 }, 0, "exceed the 256 probes a round may start"},
		"a round of more than 256, nodes unset": {func(o *WarmOptions) { o.WorkerSelector = map[string]string{"ate-env/domain": "wide"}; o.Nodes = 0 }, 0, "exceed the 256 probes a round may start"},
	} {
		opts := ok
		tc.mutate(&opts)
		api.mu.Lock()
		api.warmMaxProbesPerNode = tc.cap
		created := len(api.created)
		api.mu.Unlock()
		_, err := b.WarmImage(context.Background(), pfImgA, opts, nil)
		// The Go client names the InvalidArgument status so.
		if err == nil || !strings.Contains(err.Error(), "invalid argument") || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want InvalidArgument with %q", name, err, tc.want)
			continue
		}
		api.mu.Lock()
		if len(api.created) != created {
			t.Errorf("%s: the refused call created %d probes", name, len(api.created)-created)
		}
		api.mu.Unlock()
	}
	// A raised cap takes what it allows.
	api.mu.Lock()
	api.warmMaxProbesPerNode = 4
	api.mu.Unlock()
	opts := ok
	opts.ProbesPerNode, opts.RunID = 4, "pf-ok-0-0-2"
	if _, err := b.WarmImage(context.Background(), pfImgA, opts, nil); err != nil {
		t.Errorf("4 probes per node under a cap of 4: %v", err)
	}
}

// largePlan has the given number of two-node domains and of images; image i
// is placed in domain i modulo the domains.
func largePlan(domains, images int) *schema.ImagePlacement {
	p := &schema.ImagePlacement{SchemaVersion: schema.SchemaVersion, UnpackFactor: 1, Replication: 1}
	for i := range domains {
		name := fmt.Sprintf("d%d", i)
		p.Domains = append(p.Domains, schema.PlacementDomain{Name: name, Selector: map[string]string{"ate-env/domain": name}, Nodes: 2, NodeCacheBytes: 100})
	}
	for i := range images {
		p.Images = append(p.Images, schema.ImagePlacementEntry{
			Image:   fmt.Sprintf("registry.example/swebench/i%d@sha256:%064x", i, i),
			Domains: []string{fmt.Sprintf("d%d", i%domains)}, BytesCompressed: 1, BytesUnpacked: 1,
		})
	}
	return p
}

// The run id of a call is <run>-<domain>-<image>-<attempt> with the indexes
// in base 36, and it has to fit ate-env-api's 20 characters for the pairs
// the pass warms, not for the whole plan.
func TestPreflightWarmRunIDFitsTheServerLimit(t *testing.T) {
	for _, tc := range []struct {
		domain, image, attempt int
		want                   string
	}{
		{0, 0, 1, "pf-abcdef-0-0-1"},
		{9, 10, 9, "pf-abcdef-9-a-9"},
		{35, 36, 2, "pf-abcdef-z-10-2"},
		{350, 36840, 9, "pf-abcdef-9q-sfc-9"},
		{46655, 1679615, 9, "pf-abcdef-zzz-zzzz-9"},
	} {
		got := warmRunID("pf-abcdef", tc.domain, tc.image, tc.attempt)
		if got != tc.want || len(got) > warmRunIDLimit {
			t.Errorf("warmRunID(pf-abcdef, %d, %d, %d) = %q (%d characters), want %q within %d", tc.domain, tc.image, tc.attempt, got, len(got), tc.want, warmRunIDLimit)
		}
	}
	backend := newFakeBackend(t, newPreflightFake())

	// The boundary: the two-domain, two-image plan appends at most -1-1-9.
	for n, ok := range map[int]bool{warmRunIDLimit - 6: true, warmRunIDLimit - 5: false} {
		_, err := NewPreflightWarmer(PreflightOptions{Plan: twoDomainPlan(), Backend: backend, RunID: strings.Repeat("a", n)})
		if ok != (err == nil) || (err != nil && !strings.Contains(err.Error(), "indexes")) {
			t.Errorf("run id of %d characters: err = %v, want accepted=%v", n, err, ok)
		}
	}

	// 351 domains and 36,841 images, the default run id: the last pair's
	// call was pf-<6>-350-36840-9 in decimal, 21 characters.
	plan := largePlan(351, 36841)
	w, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend})
	if err != nil {
		t.Fatalf("351 domains x 36,841 images with the default run id: %v", err)
	}
	if id := warmRunID(w.opts.RunID, 350, 36840, maxWarmAttempts); len(id) > warmRunIDLimit {
		t.Errorf("the last pair's run id %q has %d characters, want at most %d", id, len(id), warmRunIDLimit)
	}

	// A subset is measured by its own pairs: with 37 domains a 14-character
	// run id fits domain 0 (-0-0-9) and not domain 36 (-10-0-9).
	plan = largePlan(37, 37)
	long := strings.Repeat("a", warmRunIDLimit-6)
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend, RunID: long, Domains: []string{"d0"}}); err != nil {
		t.Errorf("domain 0 of 37 with a %d-character run id: %v", len(long), err)
	}
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend, RunID: long}); err == nil || !strings.Contains(err.Error(), "indexes") {
		t.Errorf("all 37 domains with a %d-character run id: err = %v, want the indexes error", len(long), err)
	}
}

func TestPreflightDefaultRunIDIsUniquePerPass(t *testing.T) {
	plan := twoDomainPlan()
	backend := newFakeBackend(t, newPreflightFake())
	a, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{a.opts.RunID, b.opts.RunID} {
		if !strings.HasPrefix(id, DefaultPreflightRunIDPrefix+"-") || len(id) != len(DefaultPreflightRunIDPrefix)+7 || !runIDPattern.MatchString(id) {
			t.Errorf("default run id %q, want pf-<6 characters> matching %s", id, runIDPattern)
		}
	}
	if a.opts.RunID == b.opts.RunID {
		t.Errorf("two passes got the same default run id %q", a.opts.RunID)
	}
}

// A probe whose delete fails is listed under the image's undeleted_probes,
// the coverage still says what was covered, and the pass fails naming it.
func TestPreflightReportsUndeletedProbes(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	stuck := probeID("pf-test", 0, 0, 1, 1)
	api.deleteFail = map[string]bool{stuck: true}
	// The server's delete retries are its own (TestWarmImageRetriesProbeDeletes
	// in apiservice); the fake answers once and lists the probe.
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.Images = []string{pfImgA}; o.Domains = []string{"d0"} })
	if err == nil || !strings.Contains(err.Error(), "1 probes left undeleted") || !strings.Contains(err.Error(), stuck) {
		t.Fatalf("Run error = %v, want the undeleted probe named", err)
	}
	ic := report.Domains[0].Images[0]
	if !ic.Complete || !reflect.DeepEqual(ic.UndeletedProbes, []string{stuck}) {
		t.Errorf("coverage = %+v, want complete with %s undeleted", ic, stuck)
	}
	if !report.Complete {
		t.Error("report.Complete = false; the coverage held, the leftover is reported separately")
	}
	if got := UndeletedProbes(report); !reflect.DeepEqual(got, []string{stuck}) {
		t.Errorf("UndeletedProbes = %v", got)
	}
}

// A probe whose delete answers NotFound is gone already: it is not listed as
// undeleted and the pass is clean.
func TestPreflightTreatsNotFoundOnDeleteAsGone(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	gone := probeID("pf-test", 0, 0, 1, 1)
	api.deleteNotFound = map[string]bool{gone: true}
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.Images = []string{pfImgA}; o.Domains = []string{"d0"} })
	if err != nil || !report.Complete {
		t.Fatalf("Run: err=%v complete=%v, want a clean pass", err, report.Complete)
	}
	if got := UndeletedProbes(report); len(got) != 0 {
		t.Errorf("undeleted = %v, want none for a NotFound delete", got)
	}
}
