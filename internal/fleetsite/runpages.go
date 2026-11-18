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

package fleetsite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// maxShownJSON bounds a bundle file shown on its run page.
const maxShownJSON = 256 << 10

// bundleFiles are the files of a benchmark bundle the run page shows, of
// the six `bench validate` requires (cmd/benchmarking/bench/bundle.go).
var bundleFiles = []string{"run_window.json", "report.json", "system_description.json"}

// RunReport.write names its JSON <strategy>_<n>tasks_<YYYYMMDD-HHMMSS>[-<i>].json;
// the run page reads at most maxRunReports of them, each up to
// maxRunReportBytes.
var runReportName = regexp.MustCompile(`^[A-Za-z0-9._-]+_[0-9]+tasks_[0-9]{8}-[0-9]{6}(-[0-9]+)?\.json$`)

const (
	maxRunReports     = 20
	maxRunReportBytes = 1 << 20
)

// runDetailBuilds bounds the run details built at once: each can parse a
// 64 MiB span file.
const runDetailBuilds = 2

// maxRunDetails bounds the cached run details.
const maxRunDetails = 64

// RunReportPhase is one phase of a RunReport (ate_env_rl.observability).
type RunReportPhase struct {
	Count  int64   `json:"count"`
	TotalS float64 `json:"total_s"`
	MaxS   float64 `json:"max_s"`
}

// RunReport is the JSON of RunReport.write, the fields the run page shows.
type RunReport struct {
	File      string                    `json:"file"`
	Strategy  string                    `json:"strategy"`
	TotalS    float64                   `json:"total_s"`
	Phases    map[string]RunReportPhase `json:"phases"`
	Claims    int64                     `json:"claims"`
	TasksOK   int64                     `json:"tasks_ok"`
	TasksErr  int64                     `json:"tasks_err"`
	WarmTotal int64                     `json:"warm_replicas_total"`
	WarmPeak  int64                     `json:"warm_replicas_peak"`
}

// phaseRow is one phase of the report table.
type phaseRow struct {
	Name string
	RunReportPhase
}

// runReportOrder is RunReport.ORDER (clients/python/src/ate_env_rl/observability.py).
var runReportOrder = []string{
	"preflight", "plan", "preflight_images", "ensure_template", "preflight_golden", "preflight_probe",
	"warm_pool", "acquire", "process", "recycle", "release", "reap", "teardown",
}

// phaseRows orders the phases as RunReport.ORDER, then the others by name
// (to_dict keeps them in insertion order, which a JSON object loses).
func (r RunReport) phaseRows() []phaseRow {
	order := map[string]int{}
	for i, p := range runReportOrder {
		order[p] = i
	}
	var out []phaseRow
	for n, p := range r.Phases {
		out = append(out, phaseRow{Name: n, RunReportPhase: p})
	}
	sort.Slice(out, func(i, j int) bool {
		oi, iok := order[out[i].Name]
		oj, jok := order[out[j].Name]
		switch {
		case iok && jok:
			return oi < oj
		case iok != jok:
			return iok
		default:
			return out[i].Name < out[j].Name
		}
	})
	return out
}

// BundleFile is one bundle file as the run page shows it: indented JSON, or
// why it is not shown.
type BundleFile struct {
	Name string `json:"name"`
	JSON string `json:"json,omitempty"`
	Note string `json:"note,omitempty"`
}

// RunDetail is one run: its files and what the page reads from them.
type RunDetail struct {
	RunEntry
	Files []RunFile `json:"files"`
	// Trainer runs.
	Reports  []RunReport `json:"reports,omitempty"`
	Trace    *RunTrace   `json:"trace,omitempty"`
	TraceURL string      `json:"trace_url,omitempty"`
	HasHTML  bool        `json:"has_report_html,omitempty"`
	Notes    []string    `json:"notes,omitempty"`
	// Benchmark bundles.
	Bundle []BundleFile `json:"bundle,omitempty"`
}

// runDetails caches built run details by run and file listing, so a page
// view reads and parses a run's files once per change, and bounds the builds
// in flight.
type runDetails struct {
	mu       sync.Mutex
	entries  map[string]runDetailEntry
	inflight map[string]*runDetailCall
	sem      chan struct{}
	// timeout bounds one build from the time it gets its slot; zero means
	// requestTimeout.
	timeout time.Duration
}

type runDetailEntry struct {
	at     time.Time
	detail RunDetail
}

type runDetailCall struct {
	done   chan struct{}
	detail RunDetail
	err    error
}

// detailKey names a run's files as listed: a changed, added or removed file
// is a new key.
func detailKey(e RunEntry, files []RunFile) string {
	var b strings.Builder
	b.WriteString(e.Path)
	for _, f := range files {
		fmt.Fprintf(&b, "\x00%s\x00%d\x00%d", f.Name, f.Size, f.Updated.UnixNano())
	}
	return b.String()
}

func (s *Server) runsUnavailable(w http.ResponseWriter, r *http.Request, api bool) bool {
	if s.cfg.Runs != nil {
		return false
	}
	if api {
		s.apiError(w, r, http.StatusServiceUnavailable, "no run source is configured", nil)
	} else {
		s.pageError(w, r, http.StatusServiceUnavailable, "No run source is configured.", nil)
	}
	return true
}

func (s *Server) apiRuns(w http.ResponseWriter, r *http.Request) {
	if s.runsUnavailable(w, r, true) {
		return
	}
	idx, err := s.cfg.Runs.Index(r.Context(), force(r))
	if err != nil {
		// The request's own deadline (requestTimeout) ended the wait, or
		// the client left: answer, so a client that is still there does
		// not get an empty 200.
		s.apiError(w, r, http.StatusBadGateway, "the runs could not be listed", err)
		return
	}
	writeJSON(w, idx)
}

func (s *Server) runsPage(w http.ResponseWriter, r *http.Request) {
	if s.runsUnavailable(w, r, false) {
		return
	}
	idx, err := s.cfg.Runs.Index(r.Context(), force(r))
	if err != nil {
		s.pageError(w, r, http.StatusBadGateway, "The runs could not be listed.", err)
		return
	}
	var trainer, bench []RunEntry
	for _, e := range idx.Runs {
		if e.Kind == RunTrainer {
			trainer = append(trainer, e)
		} else {
			bench = append(bench, e)
		}
	}
	s.render(w, r, http.StatusOK, "runs.html", map[string]any{
		"Title": "Runs", "Trainer": trainer, "Bench": bench, "Truncated": idx.Truncated, "Errors": idx.Errors,
	})
}

// findRun resolves the run of the path, /…/trainer/{id} or
// /…/bench/{bucket}/{id}; false when the request was answered.
func (s *Server) findRun(w http.ResponseWriter, r *http.Request, api bool) (RunEntry, []RunFile, bool) {
	if s.runsUnavailable(w, r, api) {
		return RunEntry{}, nil, false
	}
	fail := func(code int, msg string, err error) {
		if api {
			s.apiError(w, r, code, msg, err)
		} else {
			s.pageError(w, r, code, msg, err)
		}
	}
	kind, bucket, id := RunTrainer, r.PathValue("bucket"), r.PathValue("id")
	if bucket != "" {
		kind = RunBenchmark
	}
	if !runID.MatchString(id) {
		fail(http.StatusBadRequest, "That is not a run id or a bundle folder name.", nil)
		return RunEntry{}, nil, false
	}
	e, files, ok, err := s.cfg.Runs.Find(r.Context(), kind, bucket, id)
	if err != nil {
		fail(http.StatusBadGateway, "The run could not be read.", err)
		return RunEntry{}, nil, false
	}
	if !ok {
		fail(http.StatusNotFound, "No run or bundle has that name.", nil)
		return RunEntry{}, nil, false
	}
	return e, files, true
}

// runDetail returns what the run page shows, from the cache when the run's
// listing, files, did not change.
func (s *Server) runDetail(ctx context.Context, e RunEntry, files []RunFile) (RunDetail, error) {
	key := detailKey(e, files)
	now := s.cfg.Now()
	d := &s.runDetails
	d.mu.Lock()
	if d.entries == nil {
		d.entries, d.inflight, d.sem = map[string]runDetailEntry{}, map[string]*runDetailCall{}, make(chan struct{}, runDetailBuilds)
	}
	if c, ok := d.entries[key]; ok && now.Sub(c.at) < RunsCacheTTL {
		d.mu.Unlock()
		return c.detail, nil
	}
	call, ok := d.inflight[key]
	if !ok {
		call = &runDetailCall{done: make(chan struct{})}
		d.inflight[key] = call
		go func() {
			// The deadline starts with the build, not with the wait for a
			// slot, so a queued build gets its whole time.
			d.sem <- struct{}{}
			timeout := d.timeout
			if timeout == 0 {
				timeout = requestTimeout
			}
			bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
			call.detail, call.err = s.buildRunDetail(bctx, e, files)
			expired := bctx.Err() != nil
			cancel()
			<-d.sem
			d.mu.Lock()
			defer d.mu.Unlock()
			delete(d.inflight, key)
			// A build that ran out of time may hold notes for reads that
			// failed for that reason alone: it serves its waiters and is
			// not cached.
			if call.err == nil && !expired {
				// The TTL counts from the end of the build, as the
				// metrics proxy counts it from the end of its query.
				done := s.cfg.Now()
				for k, c := range d.entries {
					if done.Sub(c.at) >= RunsCacheTTL || len(d.entries) >= maxRunDetails {
						delete(d.entries, k)
					}
				}
				d.entries[key] = runDetailEntry{at: done, detail: call.detail}
			}
			close(call.done)
		}()
	}
	d.mu.Unlock()
	select {
	case <-call.done:
		return call.detail, call.err
	case <-ctx.Done():
		return RunDetail{}, ctx.Err()
	}
}

// logRunRead logs a run file that the store could not read; the page shows
// a note without the detail.
func logRunRead(e RunEntry, name string, err error) {
	log.Printf("fleetsite: reading %s of run %s: %v", name, e.Path, err)
}

// buildRunDetail reads what the run page shows. A file that cannot be read
// or parsed is a note on the page, not an error: the rest still shows.
func (s *Server) buildRunDetail(ctx context.Context, e RunEntry, files []RunFile) (RunDetail, error) {
	d := RunDetail{RunEntry: e, Files: files}
	has := map[string]RunFile{}
	for _, f := range files {
		has[f.Name] = f
	}
	if e.Kind == RunBenchmark {
		for _, name := range bundleFiles {
			bf := BundleFile{Name: name}
			if _, ok := has[name]; !ok {
				bf.Note = "not in the bundle"
			} else if data, err := s.cfg.Runs.ReadFile(ctx, e, name, maxShownJSON); err != nil {
				logRunRead(e, name, err)
				bf.Note = "not shown: larger than 256 KiB or unreadable; download it below"
			} else {
				var buf bytes.Buffer
				if json.Indent(&buf, data, "", "  ") != nil {
					bf.Note = "not valid JSON"
				} else {
					bf.JSON = buf.String()
				}
			}
			d.Bundle = append(d.Bundle, bf)
		}
		return d, nil
	}

	_, d.HasHTML = has["report.html"]
	for _, f := range files {
		if !runReportName.MatchString(f.Name) {
			continue
		}
		if len(d.Reports) == maxRunReports {
			d.Notes = append(d.Notes, fmt.Sprintf("Only the first %d RunReport files are shown.", maxRunReports))
			break
		}
		if f.Size > maxRunReportBytes {
			d.Notes = append(d.Notes, f.Name+" is larger than 1 MiB and is not shown.")
			continue
		}
		data, err := s.cfg.Runs.ReadFile(ctx, e, f.Name, maxRunReportBytes)
		if err != nil {
			logRunRead(e, f.Name, err)
			d.Notes = append(d.Notes, f.Name+" could not be read.")
			continue
		}
		var rep RunReport
		if json.Unmarshal(data, &rep) != nil || rep.Strategy == "" {
			d.Notes = append(d.Notes, f.Name+" is not a RunReport the page can read (a non-finite number, as Python writes NaN or Infinity, is not JSON).")
			continue
		}
		rep.File = f.Name
		d.Reports = append(d.Reports, rep)
	}
	if _, ok := has["spans.jsonl"]; ok {
		data, err := s.cfg.Runs.ReadFile(ctx, e, "spans.jsonl", maxRunFileBytes)
		switch {
		case err != nil:
			logRunRead(e, "spans.jsonl", err)
			d.Notes = append(d.Notes, "spans.jsonl could not be read; it may be larger than 64 MiB.")
		default:
			tr, ok, err := ParseRunTrace(data)
			switch {
			case err != nil:
				d.Notes = append(d.Notes, "spans.jsonl is not OTLP/JSON: "+err.Error())
			case !ok:
				d.Notes = append(d.Notes, "spans.jsonl has no "+RunSpanName+" span.")
			default:
				d.Trace = &tr
				project := tr.Project
				if project == "" {
					project = s.cfg.TraceProject
				}
				d.TraceURL = TraceURL(project, tr.TraceID)
			}
		}
	}
	return d, nil
}

func (s *Server) apiRun(w http.ResponseWriter, r *http.Request) {
	e, files, ok := s.findRun(w, r, true)
	if !ok {
		return
	}
	d, err := s.runDetail(r.Context(), e, files)
	if err != nil {
		s.apiError(w, r, http.StatusBadGateway, "the run could not be read", err)
		return
	}
	writeJSON(w, d)
}

// waterfallBar is one bar of the SVG, in percent of the run span.
type waterfallBar struct {
	WaterfallSpan
	X, Width, Y float64
	Indent      int
}

// barOf places a span on the run span's 0..100: both ends are clamped, so a
// span outside the run shows as a sliver at its edge.
func barOf(sp WaterfallSpan, runMS float64, i int) waterfallBar {
	start := min(max(100*sp.OffsetMS/runMS, 0), 100)
	end := min(max(100*(sp.OffsetMS+sp.DurMS)/runMS, 0), 100)
	x, w := start, end-start
	if w < 0.1 {
		x, w = min(x, 99.9), 0.1
	}
	return waterfallBar{WaterfallSpan: sp, X: x, Width: w, Y: float64(i * 14), Indent: min(sp.Depth, 12) * 8}
}

func (s *Server) runPage(w http.ResponseWriter, r *http.Request) {
	e, files, ok := s.findRun(w, r, false)
	if !ok {
		return
	}
	d, err := s.runDetail(r.Context(), e, files)
	if err != nil {
		s.pageError(w, r, http.StatusBadGateway, "The run could not be read.", err)
		return
	}
	data := map[string]any{"Title": "Run " + e.ID, "Run": d}
	var phases [][]phaseRow
	for _, rep := range d.Reports {
		phases = append(phases, rep.phaseRows())
	}
	data["Phases"] = phases
	if d.Trace != nil && d.Trace.DurationMS > 0 {
		var bars []waterfallBar
		for i, sp := range d.Trace.Spans {
			bars = append(bars, barOf(sp, d.Trace.DurationMS, i))
		}
		data["Bars"] = bars
		data["Height"] = len(bars)*14 + 2
	}
	s.render(w, r, http.StatusOK, "run.html", data)
}

// runFile serves one file of a run as a download, report.html too: a
// report's script never runs on the site's origin or in a tab it opened.
func (s *Server) runFile(w http.ResponseWriter, r *http.Request) {
	e, files, ok := s.findRun(w, r, false)
	if !ok {
		return
	}
	name := r.PathValue("name")
	listed := false
	for _, f := range files {
		listed = listed || f.Name == name
	}
	if !listed {
		s.pageError(w, r, http.StatusNotFound, "The run has no such file.", nil)
		return
	}
	data, err := s.cfg.Runs.ReadFile(r.Context(), e, name, maxRunFileBytes)
	if err != nil {
		s.pageError(w, r, http.StatusBadGateway, "The file could not be read; it may be larger than 64 MiB.", err)
		return
	}
	ct := "application/octet-stream"
	switch strings.ToLower(path.Ext(name)) {
	case ".json":
		ct = "application/json"
	case ".jsonl", ".txt", ".md", ".log":
		ct = "text/plain; charset=utf-8"
	}
	h := w.Header()
	h.Set("Content-Type", ct)
	// The name matches runFileName, so it needs no quoting.
	h.Set("Content-Disposition", `attachment; filename="`+name+`"`)
	_, _ = w.Write(data)
}
