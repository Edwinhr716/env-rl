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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RunSpanName is the span of one fleet run, the root of its trace
// (docs/observability.md, "What nests under what").
const RunSpanName = "ate_env_fleet.run"

// maxWaterfallSpans bounds the bars of one waterfall; the rest are counted.
const maxWaterfallSpans = 300

// maxSpanLine bounds one line of a span file, one OTLP export request.
const maxSpanLine = 16 << 20

// RunTrace is the run span's trace as a span file holds it.
type RunTrace struct {
	TraceID string `json:"trace_id"`
	// Project is the gcp.project_id (else cloud.account.id) resource
	// attribute of the run span, the project whose Cloud Trace holds the
	// trace; empty when the file does not say.
	Project string `json:"project,omitempty"`
	// Spans are the trace's spans by start time, at most maxWaterfallSpans.
	Spans []WaterfallSpan `json:"spans"`
	// Omitted counts the trace's spans past maxWaterfallSpans.
	Omitted int `json:"omitted,omitempty"`
	// DurationMS is the run span's duration.
	DurationMS float64 `json:"duration_ms"`
}

// WaterfallSpan is one bar: its offset from the run span's start, its
// duration and its depth under the run span.
type WaterfallSpan struct {
	Name     string  `json:"name"`
	Service  string  `json:"service,omitempty"`
	Depth    int     `json:"depth"`
	OffsetMS float64 `json:"offset_ms"`
	DurMS    float64 `json:"duration_ms"`
	Error    bool    `json:"error,omitempty"`
}

type otlpAttr struct {
	Key   string `json:"key"`
	Value struct {
		StringValue string `json:"stringValue"`
	} `json:"value"`
}

type otlpSpan struct {
	TraceID      string `json:"traceId"`
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId"`
	Name         string `json:"name"`
	Start        string `json:"startTimeUnixNano"`
	End          string `json:"endTimeUnixNano"`
	Status       struct {
		Code int `json:"code"`
	} `json:"status"`
}

type otlpRequest struct {
	ResourceSpans []struct {
		Resource struct {
			Attributes []otlpAttr `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []struct {
			Spans []otlpSpan `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"resourceSpans"`
}

// span keeps what the waterfall needs of one span.
type span struct {
	name, id, parent, service string
	start, end                int64
	errored                   bool
}

func attr(attrs []otlpAttr, key string) string {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value.StringValue
		}
	}
	return ""
}

// eachSpan calls fn for every well-formed span of an OTLP/JSON span file,
// one export request per line, with its resource's service and project.
func eachSpan(data []byte, fn func(s otlpSpan, start, end int64, service, project string)) error {
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64<<10), maxSpanLine)
	line := 0
	for sc.Scan() {
		line++
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		var req otlpRequest
		if err := json.Unmarshal(b, &req); err != nil {
			return fmt.Errorf("line %d is not an OTLP/JSON export request: %w", line, err)
		}
		for _, rs := range req.ResourceSpans {
			service := attr(rs.Resource.Attributes, "service.name")
			project := attr(rs.Resource.Attributes, "gcp.project_id")
			if project == "" {
				project = attr(rs.Resource.Attributes, "cloud.account.id")
			}
			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					start, err1 := strconv.ParseInt(s.Start, 10, 64)
					end, err2 := strconv.ParseInt(s.End, 10, 64)
					if err1 != nil || err2 != nil || end < start {
						continue
					}
					fn(s, start, end, service, project)
				}
			}
		}
	}
	return sc.Err()
}

// ValidProject reports whether s is a Google Cloud project id, also a
// domain-scoped one (example.com:my-project), by the rule the metrics proxy
// and a span file's project use.
func ValidProject(s string) bool { return gcpProject.MatchString(s) }

// ParseRunTrace reads an OTLP/JSON span file, one export request per line,
// finds the latest run span and returns its trace. ok is false when the
// file has no run span; a line that is not an export request is an error.
// The first pass finds the run span; the second keeps only its trace's
// spans, so the memory is that trace's, not the file's.
func ParseRunTrace(data []byte) (RunTrace, bool, error) {
	var run *span
	var runProject string
	err := eachSpan(data, func(s otlpSpan, start, end int64, service, project string) {
		if s.Name == RunSpanName && traceID.MatchString(s.TraceID) && (run == nil || start > run.start) {
			run = &span{name: s.Name, id: s.SpanID + "/" + s.TraceID, start: start, end: end}
			runProject = project
		}
	})
	if err != nil {
		return RunTrace{}, false, err
	}
	if run == nil {
		return RunTrace{}, false, nil
	}
	runSpanID, runTraceID, _ := strings.Cut(run.id, "/")
	out := RunTrace{TraceID: runTraceID, DurationMS: float64(run.end-run.start) / 1e6}
	if gcpProject.MatchString(runProject) {
		out.Project = runProject
	}

	var trace []span
	parent := map[string]string{}
	_ = eachSpan(data, func(s otlpSpan, start, end int64, service, _ string) {
		if s.TraceID != runTraceID {
			return
		}
		trace = append(trace, span{name: s.Name, id: s.SpanID, parent: s.ParentSpanID, service: service, start: start, end: end, errored: s.Status.Code == 2})
		parent[s.SpanID] = s.ParentSpanID
	})
	sort.SliceStable(trace, func(i, j int) bool { return trace[i].start < trace[j].start })

	depth := newDepths(parent, runSpanID)
	for i, s := range trace {
		if i == maxWaterfallSpans {
			out.Omitted = len(trace) - maxWaterfallSpans
			break
		}
		out.Spans = append(out.Spans, WaterfallSpan{
			Name:     s.name,
			Service:  s.service,
			Depth:    depth.of(s.id),
			OffsetMS: float64(s.start-run.start) / 1e6,
			DurMS:    float64(s.end-s.start) / 1e6,
			Error:    s.errored,
		})
	}
	return out, true, nil
}

// depths counts the hops from a span up to the root span, memoized and
// without recursion, so a deep chain costs one walk; a missing parent or a
// cycle ends a chain at 0. steps counts the parent lookups, which the
// memoization keeps to one per span.
type depths struct {
	parent map[string]string
	memo   map[string]int
	steps  int
}

func newDepths(parent map[string]string, root string) *depths {
	return &depths{parent: parent, memo: map[string]int{root: 0}}
}

func (d *depths) of(id string) int {
	var chain []string
	onChain := map[string]bool{}
	for cur := id; ; {
		if n, ok := d.memo[cur]; ok {
			for i := len(chain) - 1; i >= 0; i-- {
				n++
				d.memo[chain[i]] = n
			}
			return d.memo[id]
		}
		d.steps++
		p, ok := d.parent[cur]
		if !ok || p == "" || onChain[cur] {
			d.memo[cur] = 0
			continue
		}
		onChain[cur] = true
		chain = append(chain, cur)
		cur = p
	}
}
