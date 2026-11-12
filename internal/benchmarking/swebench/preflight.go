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
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

// Pre-flight event names. They go to the pre-flight's own events file, not
// to the run's shards, so the run report does not count them.
const (
	EventPreflightTemplate = "preflight_template" // one image's derived template ensured
	EventPreflightGolden   = "preflight_golden"   // one image's golden settled: ready, none, failed, or given up
	EventPreflightProbe    = "preflight_probe"    // one probe environment: created, ready, node read, deleted
)

// PreflightOptions configure a PreflightWarmer.
type PreflightOptions struct {
	// Plan is the image placement to fill; PlanPath names it in the report.
	Plan     *schema.ImagePlacement
	PlanPath string
	Backend  ImageWarmer
	// Recorder receives the events; nil records nothing.
	Recorder *Recorder
	Logger   *slog.Logger
	// RunID prefixes the probe environment names: the pass appends the
	// domain and image indexes in the plan (base 36) and the attempt, ate-env-api the round and
	// the slot, and the whole run id of a call is at most 20 characters.
	// Empty gives pf-<6 random characters>, unique per pass.
	RunID string
	// ProbesPerNode is how many probes a round starts per node of a domain;
	// a round holds its probes alive until all are placed, so the scheduler
	// spreads them. Default 2, at most 8; ate-env-api also refuses more
	// than its -warm-max-probes-per-node (2 unless raised).
	ProbesPerNode int
	// ImageConcurrency is how many images a domain warms at once, each its
	// own WarmImage call with its own rounds. A domain whose plan entry has
	// node_slots caps it at node_slots / ProbesPerNode. Default 4. Across
	// calls ate-env-api caps the probes in flight per node of one worker
	// selector at -warm-max-probes-per-node, so calls beyond that cap wait
	// for slots on the server (the coverage's waited_ms sums the wait).
	ImageConcurrency int
	// MaxRounds caps the rounds per (image, domain); default 3, at most 10.
	MaxRounds int
	// GoldenTimeout gives up on the goldens still baking once none became
	// ready or failed for that long; default 30 min. GoldenPoll is how often
	// the baking goldens are polled; default 10 s.
	GoldenTimeout time.Duration
	GoldenPoll    time.Duration
	// ProbeTimeout bounds one probe on the server: create, resume and node
	// read; default 10 min. The delete has the server's own bound, 60 s.
	ProbeTimeout time.Duration
	// Images and Domains, when set, restrict the pass to those.
	Images  []string
	Domains []string
	// EnsureWidth is how many templates are ensured, and later polled, at
	// once; default 8.
	EnsureWidth int

	now         func() time.Time
	wakeBackoff time.Duration
}

// PreflightWarmer fills the node caches an image placement plan describes.
// It ensures each image's derived template, which starts the golden bake,
// then per domain warms images as their goldens become ready: one
// ate-env-api WarmImage call per (image, domain), in which the server
// creates probe environments with the domain's worker selector, round after
// round, until every node of the domain has run the image once, reading
// the node each probe landed on, and deletes them.
type PreflightWarmer struct {
	opts PreflightOptions
}

// NewPreflightWarmer validates opts and applies the defaults.
func NewPreflightWarmer(opts PreflightOptions) (*PreflightWarmer, error) {
	if opts.Plan == nil {
		return nil, errors.New("preflight: a plan is required")
	}
	if err := opts.Plan.Validate(); err != nil {
		return nil, fmt.Errorf("preflight: %w", err)
	}
	if opts.Backend == nil {
		return nil, errors.New("preflight: a backend is required")
	}
	if opts.RunID == "" {
		// A random suffix, as ate-env-api's WarmImage uses: two passes in one
		// atespace must not share probe names (see createRefused).
		opts.RunID = newPreflightRunID()
	}
	if len(opts.RunID) > maxRunIDLen || !runIDPattern.MatchString(opts.RunID) {
		return nil, fmt.Errorf("preflight: run id %q must match %s and be at most %d characters", opts.RunID, runIDPattern, maxRunIDLen)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.ProbesPerNode <= 0 {
		opts.ProbesPerNode = 2
	}
	if opts.ImageConcurrency <= 0 {
		opts.ImageConcurrency = 4
	}
	if opts.MaxRounds <= 0 {
		opts.MaxRounds = 3
	}
	if opts.GoldenTimeout <= 0 {
		opts.GoldenTimeout = 30 * time.Minute
	}
	if opts.GoldenPoll <= 0 {
		opts.GoldenPoll = 10 * time.Second
	}
	if opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 10 * time.Minute
	}
	if opts.EnsureWidth <= 0 {
		opts.EnsureWidth = 8
	}
	// What ate-env-api's WarmImage refuses is refused here: a refused call
	// only shows as a short pair, after the goldens were waited for.
	if opts.ProbesPerNode > apiservice.MaxWarmProbesPerNode {
		return nil, fmt.Errorf("preflight: %d probes per node exceed the %d ate-env-api takes", opts.ProbesPerNode, apiservice.MaxWarmProbesPerNode)
	}
	if opts.MaxRounds > apiservice.MaxWarmRounds {
		return nil, fmt.Errorf("preflight: %d rounds exceed the %d ate-env-api takes", opts.MaxRounds, apiservice.MaxWarmRounds)
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	if opts.wakeBackoff <= 0 {
		opts.wakeBackoff = DefaultWakeBackoff
	}
	for _, name := range opts.Domains {
		if opts.Plan.Domain(name) == nil {
			return nil, fmt.Errorf("preflight: the plan has no domain %q", name)
		}
	}
	for _, img := range opts.Images {
		if opts.Plan.Entry(img) == nil {
			return nil, fmt.Errorf("preflight: the plan does not place %q", img)
		}
	}
	w := &PreflightWarmer{opts: opts}
	// Only the domains of the pass: an unselected pool's slots do not bound it.
	for _, d := range w.domains() {
		if d.NodeSlots > 0 && opts.ProbesPerNode > d.NodeSlots {
			return nil, fmt.Errorf("preflight: %d probes per node exceed the %d node slots of domain %s", opts.ProbesPerNode, d.NodeSlots, d.Name)
		}
	}
	if len(w.images()) == 0 {
		return nil, errors.New("preflight: no image of the pass is placed in its domains")
	}
	// The longest run id a call of this pass sends: the highest plan indexes
	// among the pass's own domains and images, not the plan's, so a subset
	// of a large plan is not refused for pairs it never warms.
	di, ii := w.highestIndexes()
	if longest := warmRunID(opts.RunID, di, ii, maxWarmAttempts); len(longest) > warmRunIDLimit {
		return nil, fmt.Errorf("preflight: run id %q with the domain, image and attempt indexes the pass appends (%s) exceeds the %d characters ate-env-api takes; use a shorter one", opts.RunID, longest, warmRunIDLimit)
	}
	return w, nil
}

// highestIndexes is the highest plan index of a domain and of an image of
// the pass.
func (w *PreflightWarmer) highestIndexes() (domain, image int) {
	for _, d := range w.domains() {
		domain = max(domain, w.domainIndex(d.Name))
	}
	inPass := map[string]bool{}
	for _, img := range w.images() {
		inPass[img] = true
	}
	for i, e := range w.opts.Plan.Images {
		if inPass[e.Image] {
			image = i
		}
	}
	return domain, image
}

// Run executes the pre-flight and returns the coverage report. The error is
// non-nil when the pass is incomplete (a golden not ready, a pair short of
// its nodes), when the context ended, or when the pass is complete but left
// probes it could not delete (the report's undeleted_probes; Complete stays
// true then, and the error names them). The report is returned either way,
// so the caller can write it.
func (w *PreflightWarmer) Run(ctx context.Context) (*schema.PreflightCoverage, error) {
	o := w.opts
	started := o.now()
	images := w.images()
	domains := w.domains()

	report := &schema.PreflightCoverage{
		SchemaVersion: schema.SchemaVersion,
		RunID:         o.RunID,
		StartedAt:     started,
		Plan:          o.PlanPath,
	}

	goldens := newGoldenTracker(images, started)
	w.ensureTemplates(ctx, images, goldens)
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		w.pollGoldens(ctx, goldens)
	}()
	coverage, coverageOK := w.cover(ctx, domains, goldens)
	<-polled

	report.Goldens, report.Domains = goldens.records(images), coverage
	report.FinishedAt = o.now()
	report.Complete = goldens.allOK() && coverageOK && ctx.Err() == nil
	undeleted := UndeletedProbes(report)

	if o.Recorder != nil {
		_ = o.Recorder.Flush()
	}
	if ctx.Err() != nil {
		return report, ctx.Err()
	}
	if !report.Complete {
		return report, fmt.Errorf("pre-flight incomplete: %s", w.shortfall(report))
	}
	if len(undeleted) > 0 {
		// The coverage holds; the leftovers hold slots until swept, so the
		// pass still fails and names them.
		return report, fmt.Errorf("pre-flight complete, %d probes left undeleted (first: %s); sweep them with ate-env delete", len(undeleted), undeleted[0])
	}
	return report, nil
}

// UndeletedProbes lists every probe the pass could not delete, sorted.
func UndeletedProbes(r *schema.PreflightCoverage) []string {
	var out []string
	for _, d := range r.Domains {
		for _, ic := range d.Images {
			out = append(out, ic.UndeletedProbes...)
		}
	}
	sort.Strings(out)
	return out
}

// images is the pass's images in plan order: those of Images, or all, that
// at least one domain of the pass holds.
func (w *PreflightWarmer) images() []string {
	wanted := map[string]bool{}
	for _, img := range w.opts.Images {
		wanted[img] = true
	}
	domains := map[string]bool{}
	for _, d := range w.domains() {
		domains[d.Name] = true
	}
	var out []string
	for _, e := range w.opts.Plan.Images {
		if len(wanted) > 0 && !wanted[e.Image] {
			continue
		}
		for _, d := range e.Domains {
			if domains[d] {
				out = append(out, e.Image)
				break
			}
		}
	}
	return out
}

func (w *PreflightWarmer) domains() []schema.PlacementDomain {
	if len(w.opts.Domains) == 0 {
		return w.opts.Plan.Domains
	}
	want := make(map[string]bool, len(w.opts.Domains))
	for _, n := range w.opts.Domains {
		want[n] = true
	}
	var out []schema.PlacementDomain
	for _, d := range w.opts.Plan.Domains {
		if want[d.Name] {
			out = append(out, d)
		}
	}
	return out
}

func (w *PreflightWarmer) domainIndex(name string) int {
	for i, d := range w.opts.Plan.Domains {
		if d.Name == name {
			return i
		}
	}
	return 0
}

// imageWidth is how many images the domain warms at once.
func (w *PreflightWarmer) imageWidth(d schema.PlacementDomain) int {
	k := w.opts.ImageConcurrency
	if d.NodeSlots > 0 {
		if c := d.NodeSlots / w.opts.ProbesPerNode; c < k {
			k = c
		}
	}
	return max(k, 1)
}

// goldenTracker holds each image's golden state while the pass runs. The
// coverage work of an image waits on its done channel; changed is closed and
// replaced whenever an image settles, so a waiter can pick the next settled
// image instead of a fixed one.
type goldenTracker struct {
	mu       sync.Mutex
	waits    map[string]*goldenWait
	changed  chan struct{}
	progress time.Time
}

type goldenWait struct {
	task    int
	done    chan struct{}
	ensured time.Time
	// template is the derived template's name once ensured, so a record
	// settled by a timeout or a cancel still names it.
	template string
	settled  bool
	ok       bool
	rec      schema.GoldenRecord
}

func newGoldenTracker(images []string, now time.Time) *goldenTracker {
	t := &goldenTracker{waits: make(map[string]*goldenWait, len(images)), changed: make(chan struct{}), progress: now}
	for i, img := range images {
		t.waits[img] = &goldenWait{task: i, done: make(chan struct{}), ensured: now, rec: schema.GoldenRecord{Image: img}}
	}
	return t
}

// settle records the final state of one image's golden and releases the
// work waiting on it; it returns false when the image was settled already.
func (t *goldenTracker) settle(img string, rec schema.GoldenRecord, ok bool, now time.Time) (*goldenWait, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	gw := t.waits[img]
	if gw.settled {
		return gw, false
	}
	gw.settled, gw.ok, gw.rec = true, ok, rec
	gw.rec.WaitMS = DurMS(now.Sub(gw.ensured))
	t.progress = now
	close(gw.done)
	close(t.changed)
	t.changed = make(chan struct{})
	return gw, true
}

// template is the derived template name of an ensured image, or "".
func (t *goldenTracker) template(img string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.waits[img].template
}

func (t *goldenTracker) pending() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for img, gw := range t.waits {
		if !gw.settled {
			out = append(out, img)
		}
	}
	sort.Strings(out)
	return out
}

func (t *goldenTracker) lastProgress() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.progress
}

func (t *goldenTracker) records(images []string) []schema.GoldenRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]schema.GoldenRecord, 0, len(images))
	for _, img := range images {
		out = append(out, t.waits[img].rec)
	}
	return out
}

func (t *goldenTracker) allOK() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, gw := range t.waits {
		if !gw.ok {
			return false
		}
	}
	return true
}

// ensureTemplates derives every image's template, which starts the bakes,
// and settles the images whose golden needs no wait: ready, failed, or none
// when goldens are disabled on the base template (the probes then boot cold
// and still fill the layer caches).
func (w *PreflightWarmer) ensureTemplates(ctx context.Context, images []string, t *goldenTracker) {
	o := w.opts
	forEach(ctx, len(images), o.EnsureWidth, func(i int) {
		img := images[i]
		t0 := o.now()
		st, err := o.Backend.EnsureImageTemplate(ctx, img)
		w.record(Event{Event: EventPreflightTemplate, Task: i, Image: img, Env: st.Template, DurMS: DurMS(o.now().Sub(t0)), OK: err == nil, Err: errString(err)})
		t.mu.Lock()
		t.waits[img].ensured = o.now()
		t.waits[img].template = st.Template
		t.mu.Unlock()
		if err != nil {
			w.settleGolden(t, img, schema.GoldenRecord{Image: img, State: "failed", Error: err.Error()}, false)
			return
		}
		w.settleFromState(t, img, st, true)
	})
}

// settleFromState settles the image when st is final; a pending golden stays
// open. When goldens are disabled the template exists with no golden, which
// ensured tells apart from a template that is not there.
func (w *PreflightWarmer) settleFromState(t *goldenTracker, img string, st ImageState, ensured bool) {
	rec := schema.GoldenRecord{Image: img, Template: st.Template, State: string(st.Golden), Tag: st.Tag, Error: st.Error}
	switch st.Golden {
	case GoldenReady:
		w.settleGolden(t, img, rec, true)
	case GoldenFailed:
		rec.Error = "golden bake failed: " + st.Error
		w.settleGolden(t, img, rec, false)
	case GoldenNone:
		if st.Exists {
			w.settleGolden(t, img, rec, true)
		} else if ensured {
			rec.State, rec.Error = "failed", "the template was not created"
			w.settleGolden(t, img, rec, false)
		}
	}
}

// settleGolden settles the image and records its golden event once.
func (w *PreflightWarmer) settleGolden(t *goldenTracker, img string, rec schema.GoldenRecord, ok bool) {
	gw, first := t.settle(img, rec, ok, w.opts.now())
	if !first {
		return
	}
	w.record(Event{Event: EventPreflightGolden, Task: gw.task, Image: img, Env: gw.rec.Template, DurMS: gw.rec.WaitMS, OK: ok, Err: gw.rec.Error})
	if !ok {
		w.opts.Logger.Warn("preflight: golden not ready", slog.String("image", img), slog.String("state", gw.rec.State), slog.String("err", gw.rec.Error))
	}
}

// pollGoldens polls the baking goldens until all settle, none has settled
// for GoldenTimeout, or the context ends. A status call that fails is
// retried at the next poll; the timeout bounds that too.
func (w *PreflightWarmer) pollGoldens(ctx context.Context, t *goldenTracker) {
	o := w.opts
	t.mu.Lock()
	t.progress = o.now()
	t.mu.Unlock()
	for {
		pending := t.pending()
		if len(pending) == 0 {
			return
		}
		// No sleep beyond the deadline: with GoldenTimeout below GoldenPoll
		// the wait ends at the timeout, after one last poll.
		sleep := min(o.GoldenPoll, max(0, o.GoldenTimeout-o.now().Sub(t.lastProgress())))
		select {
		case <-ctx.Done():
			for _, img := range pending {
				w.settleGolden(t, img, schema.GoldenRecord{Image: img, Template: t.template(img), State: "pending", Error: ctx.Err().Error()}, false)
			}
			return
		case <-time.After(sleep):
		}
		forEach(ctx, len(pending), o.EnsureWidth, func(i int) {
			img := pending[i]
			st, err := o.Backend.ImageStatus(ctx, img)
			if err != nil {
				o.Logger.Warn("preflight: image status failed", slog.String("image", img), slog.Any("err", err))
				return
			}
			w.settleFromState(t, img, st, false)
		})
		if stalled := o.now().Sub(t.lastProgress()); stalled >= o.GoldenTimeout {
			for _, img := range t.pending() {
				w.settleGolden(t, img, schema.GoldenRecord{Image: img, Template: t.template(img), State: "pending", Error: fmt.Sprintf("golden still pending after %s without progress", o.GoldenTimeout)}, false)
			}
			return
		}
	}
}

// cover runs the coverage pass, every domain in parallel. Inside a domain
// up to imageWidth images run at once, each taken as soon as its golden has
// settled (earliest in plan order first), so a slow bake does not hold up
// the images behind it and a domain never has more than
// imageWidth x nodes x ProbesPerNode probes in flight.
func (w *PreflightWarmer) cover(ctx context.Context, domains []schema.PlacementDomain, t *goldenTracker) ([]schema.DomainCoverage, bool) {
	results := make([]schema.DomainCoverage, len(domains))
	for di, d := range domains {
		imgs := []schema.ImageCoverage{}
		for _, e := range w.opts.Plan.Images {
			if _, ok := t.waits[e.Image]; ok && contains(e.Domains, d.Name) {
				imgs = append(imgs, schema.ImageCoverage{Image: e.Image, Nodes: []string{}, Wanted: d.Nodes, Error: "not started"})
			}
		}
		results[di] = schema.DomainCoverage{Name: d.Name, Nodes: d.Nodes, NodesSeen: []string{}, Images: imgs, Complete: len(imgs) == 0}
	}
	forEach(ctx, len(domains), len(domains), func(di int) {
		d := domains[di]
		planDI := w.domainIndex(d.Name)
		type item struct {
			index int
			image string
		}
		var items []item
		for ii, e := range w.opts.Plan.Images {
			if _, ok := t.waits[e.Image]; ok && contains(e.Domains, d.Name) {
				items = append(items, item{ii, e.Image})
			}
		}
		covs := make([]schema.ImageCoverage, len(items))
		ran := make([]bool, len(items))
		started := make([]bool, len(items))
		sem := make(chan struct{}, w.imageWidth(d))
		var wg sync.WaitGroup
		for n := 0; n < len(items); {
			// The first item, in plan order, whose golden has settled.
			t.mu.Lock()
			changed := t.changed
			pick := -1
			for k, it := range items {
				if !started[k] && t.waits[it.image].settled {
					pick = k
					break
				}
			}
			t.mu.Unlock()
			if pick < 0 {
				select {
				case <-ctx.Done():
					n = len(items)
					continue
				case <-changed:
					continue
				}
			}
			select {
			case <-ctx.Done():
				n = len(items)
				continue
			case sem <- struct{}{}:
			}
			started[pick] = true
			n++
			wg.Add(1)
			go func(k int, it item) {
				defer wg.Done()
				defer func() { <-sem }()
				covs[k] = w.coverImage(ctx, planDI, it.index, d, it.image)
				ran[k] = true
			}(pick, items[pick])
		}
		wg.Wait()

		dc := schema.DomainCoverage{Name: d.Name, Nodes: d.Nodes, NodesSeen: []string{}, Images: []schema.ImageCoverage{}, Complete: true}
		seen := map[string]bool{}
		for k, it := range items {
			if !ran[k] {
				dc.Complete = false
				dc.Images = append(dc.Images, schema.ImageCoverage{Image: it.image, Nodes: []string{}, Wanted: d.Nodes, Error: "not started"})
				continue
			}
			for _, node := range covs[k].Nodes {
				seen[node] = true
			}
			if !covs[k].Complete {
				dc.Complete = false
			}
			dc.Images = append(dc.Images, covs[k])
		}
		dc.NodesSeen = sortedKeys(seen)
		if len(dc.NodesSeen) > d.Nodes {
			w.opts.Logger.Warn("preflight: domain has more nodes than the plan says", slog.String("domain", d.Name), slog.Int("plan", d.Nodes), slog.Int("seen", len(dc.NodesSeen)))
		}
		results[di] = dc
	})
	allOK := ctx.Err() == nil
	for _, dc := range results {
		if !dc.Complete {
			allOK = false
		}
	}
	return results, allOK
}

// coverImage runs ate-env-api's WarmImage for one image in one domain: the
// server runs the rounds of probes until every node of the domain has run
// the image or the rounds run out, and reports the coverage. Each probe the
// stream reports becomes a preflight_probe event. A call the control plane
// cuts off (Unavailable, Unknown, DeadlineExceeded, Internal, Aborted) is
// repeated with the wake backoff, for up to ProbeTimeout and
// maxWarmAttempts attempts inside ctx, unless its stream showed every node
// already; each attempt sends its own run id, so the probes of a broken
// call that an api server restart left behind never share a name with the
// repeat's. The pair's coverage sums its attempts. The probes a broken call
// streamed are listed in UndeletedProbes: the server deletes them when it
// can, and the call that would have said so never ended.
func (w *PreflightWarmer) coverImage(ctx context.Context, di, ii int, d schema.PlacementDomain, img string) schema.ImageCoverage {
	o := w.opts
	ic := schema.ImageCoverage{Image: img, Nodes: []string{}, Wanted: d.Nodes}
	opts := WarmOptions{
		WorkerSelector: d.Selector,
		Nodes:          d.Nodes,
		ProbesPerNode:  o.ProbesPerNode,
		MaxRounds:      o.MaxRounds,
		ProbeTimeout:   o.ProbeTimeout,
	}
	// The pair's coverage is the sum of its attempts: the nodes are the
	// union, the counters add up. An attempt that ended in an error counts
	// with what its stream showed (the server sends its coverage only at the
	// end); one that ended clean counts with the server's coverage.
	seen := map[string]bool{}
	var probes, failed, rounds int
	var waited time.Duration
	undeleted := []string{}
	// The attempt in flight, as its stream shows it.
	var mu sync.Mutex
	var att struct {
		ids                    []string
		probes, failed, rounds int
		err                    string
	}
	onProbe := func(p WarmProbe) {
		w.record(Event{Event: EventPreflightProbe, Image: img, Env: p.ID, Node: p.Node, Attempts: p.Round, DurMS: DurMS(p.Duration), OK: p.Error == "", Err: p.Error, Labels: map[string]string{"domain": d.Name}})
		mu.Lock()
		defer mu.Unlock()
		att.ids = append(att.ids, p.ID)
		att.probes++
		att.rounds = max(att.rounds, p.Round)
		if p.Error != "" {
			att.failed++
			att.err = p.Error
		} else if p.Node != "" {
			seen[p.Node] = true
		}
	}
	// fill writes the sums into ic; errText is the pair's error.
	fill := func(errText string) {
		ic.Nodes = sortedKeys(seen)
		ic.Probes, ic.Failed, ic.Rounds = probes, failed, rounds
		ic.Complete = len(ic.Nodes) >= d.Nodes
		ic.Error = errText
		ic.WaitedMS = DurMS(waited)
		ic.UndeletedProbes = nil
		if len(undeleted) > 0 {
			ic.UndeletedProbes = append([]string{}, undeleted...)
			sort.Strings(ic.UndeletedProbes)
		}
	}
	wait := o.wakeBackoff
	var giveUp time.Time
	for attempt := 1; ; attempt++ {
		opts.RunID = warmRunID(o.RunID, di, ii, attempt)
		mu.Lock()
		att.ids, att.probes, att.failed, att.rounds, att.err = nil, 0, 0, 0, ""
		mu.Unlock()
		cov, err := o.Backend.WarmImage(ctx, img, opts, onProbe)
		if err == nil {
			for _, node := range cov.Nodes {
				seen[node] = true
			}
			probes, failed, rounds = probes+cov.Probes, failed+cov.Failed, rounds+cov.Rounds
			waited += cov.Waited
			undeleted = append(undeleted, cov.UndeletedProbes...)
			fill(cov.Error)
			// A clean call alone is complete when the server says so.
			if attempt == 1 {
				ic.Complete = cov.Complete && ic.Complete
			}
			break
		}
		mu.Lock()
		probes, failed, rounds = probes+att.probes, failed+att.failed, rounds+att.rounds
		if ctx.Err() == nil {
			// The server deletes a round's probes after it streamed them, and
			// reports the ones it could not delete in the coverage this call
			// never sent: an api server that restarted left them all. The
			// pass cannot tell, so every probe the call streamed is listed.
			// After a cancel the server is there and deletes them.
			undeleted = append(undeleted, att.ids...)
		}
		fill(err.Error())
		mu.Unlock()
		// The stream can have shown every node before the call broke: the
		// pair is covered, and a repeat would only boot the image again.
		// Every line about a cut-off call names the call's run id: its
		// probes are <run id>-<round>-<slot>, and when the api server
		// restarted nothing else records them.
		call := []any{slog.String("image", img), slog.String("domain", d.Name), slog.String("run_id", opts.RunID), slog.Int("attempt", attempt), slog.Any("err", err)}
		if ctx.Err() != nil {
			break
		}
		if ic.Complete {
			o.Logger.Warn("preflight: warm call cut off after its stream showed every node, not repeated; its probes may be left", call...)
			break
		}
		if !isTransient(err) {
			break
		}
		if giveUp.IsZero() {
			giveUp = time.Now().Add(o.ProbeTimeout)
		}
		// The repeats last until giveUp, not until a whole backoff no longer
		// fits before it: the wait is cut to what is left.
		left := time.Until(giveUp)
		if attempt >= maxWarmAttempts || left <= 0 {
			o.Logger.Warn("preflight: warm call cut off, giving up; the probes of the cut-off calls may be left", call...)
			break
		}
		sleep := min(wait, left)
		if dl, ok := ctx.Deadline(); ok && time.Now().Add(sleep).After(dl) {
			break
		}
		o.Logger.Warn("preflight: warm call cut off, repeating", call...)
		select {
		case <-ctx.Done():
			return ic
		case <-time.After(sleep):
		}
		wait = min(wait*2, 5*o.wakeBackoff)
	}
	if o.Recorder != nil {
		_ = o.Recorder.Flush()
	}
	return ic
}

// warmRunIDLimit is ate-env-api's limit on WarmImage's run_id; the pass's
// run id plus the domain, image and attempt indexes it appends must fit in
// it. maxWarmAttempts bounds the repeats of a cut-off call (and the attempt
// index to one digit).
const (
	warmRunIDLimit  = 20
	maxWarmAttempts = 9
)

// warmRunID is the run id of one WarmImage call: the pass's run id with the
// plan indexes of the domain and the image and the attempt, so the calls of
// one pass, and the repeats of one call, never name a probe alike; the
// server appends -<round>-<slot>. The indexes are in base 36 (0-9, a-z):
// with the default pf-<6> run id that leaves seven characters for the two,
// enough for 46,656 domains with 1,679,616 images (decimal ran out at 351
// domains with 36,841 images). PreflightWarmer._warm_run_id in
// clients/python writes the same names.
func warmRunID(runID string, domain, image, attempt int) string {
	return runID + "-" + strconv.FormatInt(int64(domain), 36) + "-" + strconv.FormatInt(int64(image), 36) + "-" + strconv.Itoa(attempt)
}

// DefaultPreflightRunIDPrefix starts the run id of a pass that gives none;
// newPreflightRunID appends six random characters.
const DefaultPreflightRunIDPrefix = "pf"

func newPreflightRunID() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	rand.Read(b) // never fails since Go 1.24
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return DefaultPreflightRunIDPrefix + "-" + string(b)
}

func (w *PreflightWarmer) record(e Event) {
	if w.opts.Recorder == nil {
		return
	}
	if e.RunID == "" {
		e.RunID = w.opts.RunID
	}
	w.opts.Recorder.Record(e)
}

// GoldenSettled is schema.GoldenSettled: ready, or none when goldens are
// disabled on the base template.
func GoldenSettled(g schema.GoldenRecord) bool { return schema.GoldenSettled(g) }

// shortfall summarises what kept the pass from completing.
func (w *PreflightWarmer) shortfall(r *schema.PreflightCoverage) string {
	return preflightShortfall(r)
}

// preflightShortfall names what a pre-flight report is missing: goldens not
// ready, (image, domain) pairs short of their nodes, or an interruption.
func preflightShortfall(r *schema.PreflightCoverage) string {
	var parts []string
	notReady := 0
	for _, g := range r.Goldens {
		if !GoldenSettled(g) {
			notReady++
		}
	}
	if notReady > 0 {
		parts = append(parts, fmt.Sprintf("%d goldens not ready", notReady))
	}
	short := 0
	for _, d := range r.Domains {
		for _, ic := range d.Images {
			if !ic.Complete {
				short++
			}
		}
	}
	if short > 0 {
		parts = append(parts, fmt.Sprintf("%d (image, domain) pairs short of their nodes", short))
	}
	if undeleted := UndeletedProbes(r); len(undeleted) > 0 {
		parts = append(parts, fmt.Sprintf("%d probes left undeleted (first: %s; listed per image under undeleted_probes)", len(undeleted), undeleted[0]))
	}
	if len(parts) == 0 {
		parts = append(parts, "interrupted")
	}
	return strings.Join(parts, "; ")
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// WritePreflightCoverage validates c and writes it as indented JSON.
func WritePreflightCoverage(path string, c *schema.PreflightCoverage) error {
	c.Normalize()
	if err := c.Validate(); err != nil {
		return err
	}
	return writeJSONFile(path, c)
}

// ReadPreflightCoverage reads and validates a preflight-coverage.json.
func ReadPreflightCoverage(path string) (*schema.PreflightCoverage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	c, err := schema.DecodeJSONStrict[schema.PreflightCoverage](f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}
