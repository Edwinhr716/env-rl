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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

const testTrace = "0af7651916cd43dd8448eb211c80319c"

// FS-2: the Live page shows the newest transitions of the last hour, links
// each to its actor and trace, reloads itself without a script, and makes one
// query per LiveCacheTTL for every viewer.
func TestLivePage(t *testing.T) {
	ts := newTestSite(t)
	ts.events.recent = []Event{
		{Time: "2026-11-16T11:59:59Z", Project: "proj-a", Cluster: "c-1", Atespace: "ate-env", ActorName: "env-001", ActorUID: "8f2a0001", Operation: "ResumeActor", State: "running", TraceID: testTrace},
		{Time: "2026-11-16T11:59:58Z", Project: "proj-a", Cluster: "c-1", Atespace: "ate-env", ActorName: "<b>x</b>", ActorUID: "8f2a0002", Operation: "SuspendActor", State: "suspended"},
		// From a log line that lacks the values: no link to a page that
		// cannot exist.
		{Time: "2026-11-16T11:59:57Z", ActorName: "orphan", Operation: "DeleteActor", State: "deleted"},
	}
	resp, body := ts.get(t, "/live")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%d: %s", resp.StatusCode, body)
	}
	for _, want := range []string{
		`<meta http-equiv="refresh" content="10">`,
		`href="/actors/8f2a0001"`,
		`href="https://console.cloud.google.com/traces/list?project=proj-a&amp;tid=` + testTrace + `"`,
		`&lt;b&gt;x&lt;/b&gt;`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("Live page lacks %q", want)
		}
	}
	if strings.Contains(body, `href="/actors/"`) || strings.Contains(body, `href="/clusters//"`) {
		t.Error("an event without a uid or a cluster links to a page")
	}
	if !strings.Contains(body, "<td>orphan</td>") {
		t.Error("the event without links is not shown")
	}
	if ts.events.limit != LiveEvents || !ts.events.since.Equal(t0.Add(-time.Hour)) {
		t.Errorf("query limit %d since %s, want %d since an hour ago", ts.events.limit, ts.events.since, LiveEvents)
	}
	if csp := resp.Header.Get("Content-Security-Policy"); strings.Contains(csp, "script-src") {
		t.Errorf("CSP %q allows a script", csp)
	}

	// Another viewer within the TTL gets the same query.
	ts.do(t, http.MethodGet, "/live", "other@example.com")
	_, body = ts.get(t, "/api/v1/live")
	if ts.events.calls != 1 {
		t.Errorf("%d queries for three views within the TTL, want 1", ts.events.calls)
	}
	var live struct {
		Events []Event `json:"events"`
	}
	if err := json.Unmarshal([]byte(body), &live); err != nil || len(live.Events) != 3 {
		t.Errorf("/api/v1/live = %s, %v", body, err)
	}
	ts.clock.add(LiveCacheTTL)
	ts.get(t, "/live")
	if ts.events.calls != 2 {
		t.Errorf("%d queries after the TTL, want 2", ts.events.calls)
	}

	// While BigQuery fails, the failure is cached for the TTL too, and the
	// page keeps reloading itself.
	ts.events.err = errors.New("bigquery: detail")
	ts.clock.add(LiveCacheTTL)
	before := ts.events.calls
	if resp, body := ts.get(t, "/api/v1/live"); resp.StatusCode != http.StatusBadGateway || strings.Contains(body, "detail") {
		t.Errorf("failed query: %d %s", resp.StatusCode, body)
	}
	resp, body = ts.get(t, "/live")
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, `<meta http-equiv="refresh"`) || strings.Contains(body, "detail") {
		t.Errorf("failed Live page: %d, want 502 with the reload", resp.StatusCode)
	}
	ts.do(t, http.MethodGet, "/live", "other@example.com")
	if n := ts.events.calls - before; n != 1 {
		t.Errorf("%d queries for three failing views within the TTL, want 1", n)
	}
	ts.srv.cfg.Events = nil
	if resp, body := ts.get(t, "/live"); resp.StatusCode != http.StatusOK || !strings.Contains(body, "No events table") || strings.Contains(body, "http-equiv") {
		t.Errorf("no events table: %d, want 200 without a reload", resp.StatusCode)
	}
	if resp, _ := ts.get(t, "/api/v1/live"); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("no events table, API: %d", resp.StatusCode)
	}
}

// FS-2: a Live query that takes longer than the TTL, or times out, is
// cached for the whole TTL after it ends, so the self-reloading page does
// not start a slow query on every reload; its window still ends when it was
// asked for.
func TestLiveCacheCountsFromTheEndOfTheQuery(t *testing.T) {
	ts := newTestSite(t)
	ts.events.err = errors.New("bigquery: timeout")
	ts.events.onRecent = func() { ts.clock.add(LiveCacheTTL + 5*time.Second) }
	if resp, _ := ts.get(t, "/live"); resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("slow failing query: %d, want 502", resp.StatusCode)
	}
	if !ts.events.since.Equal(t0.Add(-liveWindow)) {
		t.Errorf("the window starts at %s, want an hour before the query was asked for", ts.events.since)
	}
	ts.events.onRecent = nil
	ts.get(t, "/live")
	ts.clock.add(LiveCacheTTL - time.Second)
	ts.get(t, "/api/v1/live")
	if ts.events.calls != 1 {
		t.Errorf("%d queries within the TTL of the end of a slow failing query, want 1", ts.events.calls)
	}
	ts.clock.add(time.Second)
	ts.get(t, "/live")
	if ts.events.calls != 2 {
		t.Errorf("%d queries a TTL after the end, want 2", ts.events.calls)
	}
}

// FS-1, FS-2: the Actor page joins the actor's snapshot entry with its
// transitions, and ?nocache=1 lists the bucket at once.
func TestActorPage(t *testing.T) {
	ts := newTestSite(t)
	writeSnapshot(t, ts.dir, golden(t, "proj-a", "c-1", t0))
	ts.events.out = []Event{
		{Time: "2026-11-16T11:00:00Z", Project: "proj-a", Cluster: "c-1", ActorUID: "8f2a0002", Operation: "ResumeActor", State: "running", TraceID: testTrace},
		{Time: "2026-11-16T11:00:05Z", Project: "proj-a", Cluster: "c-1", ActorUID: "8f2a0002", Operation: "ResumeActor", State: "crashed", TraceID: "not-a-trace"},
	}
	resp, body := ts.get(t, "/actors/8f2a0002?since=2026-11-16T10:00:00Z")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%d: %s", resp.StatusCode, body)
	}
	for _, want := range []string{
		"env-002", `href="/clusters/proj-a/c-1"`, "snapshot download timed out", "ResumeActor", "tid=" + testTrace, "not-a-trace",
		// The JSON link keeps the page's window, and each transition links
		// its cluster.
		`href="/api/v1/actors/8f2a0002/events?since=2026-11-16T10%3A00%3A00Z"`,
		`<td><a href="/clusters/proj-a/c-1">proj-a / c-1</a></td><td>ResumeActor</td>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("Actor page lacks %q", want)
		}
	}
	if strings.Contains(body, "tid=not-a-trace") {
		t.Error("a malformed trace id became a link")
	}
	if ts.events.uid != "8f2a0002" || !ts.events.since.Equal(t0.Add(-2*time.Hour)) {
		t.Errorf("events of %q since %s", ts.events.uid, ts.events.since)
	}
	if _, body := ts.get(t, "/actors/8f2a0002"); !strings.Contains(body, `href="/api/v1/actors/8f2a0002/events"`) {
		t.Error("without ?since= the JSON link is not the default window")
	}

	// Deleted: not in a snapshot, but with events.
	if resp, body := ts.get(t, "/actors/gone-01"); resp.StatusCode != http.StatusOK || !strings.Contains(body, "No cluster's latest snapshot lists this actor") {
		t.Errorf("deleted actor with events: %d", resp.StatusCode)
	}
	ts.events.out = nil
	if resp, _ := ts.get(t, "/actors/gone-02"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown actor without events: %d, want 404", resp.StatusCode)
	}
	// FS-1: an actor in a snapshot written after the last listing shows up
	// with ?nocache=1 at once, and not before without it.
	newer := golden(t, "proj-b", "c-2", t0)
	newer.Actors[0].UID = "new-actor-01"
	writeSnapshot(t, ts.dir, newer)
	if resp, _ := ts.get(t, "/actors/new-actor-01"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("actor of a new snapshot within the listing's TTL: %d, want 404 from the cached listing", resp.StatusCode)
	}
	if resp, body := ts.get(t, "/actors/new-actor-01?nocache=1"); resp.StatusCode != http.StatusOK || !strings.Contains(body, `href="/clusters/proj-b/c-2"`) {
		t.Errorf("?nocache=1 does not list the bucket at once: %d", resp.StatusCode)
	}
	for _, p := range []string{"/actors/bad%20uid", "/actors/u?since=yesterday", "/actors/u?since=2026-01-01T00:00:00Z"} {
		if resp, _ := ts.get(t, p); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", p, resp.StatusCode)
		}
	}
	// The page shares the events route's limit.
	ts.events.out = []Event{{ActorUID: "8f2a0001"}}
	limited := false
	for range eventsBurst + 1 {
		if _, body := ts.get(t, "/actors/8f2a0001"); strings.Contains(body, "Too many events requests") {
			limited = true
		}
	}
	if !limited {
		t.Error("the Actor page is not limited with the events route")
	}
	// The Cluster page links each actor to its page.
	if _, body := ts.get(t, "/clusters/proj-a/c-1"); !strings.Contains(body, `href="/actors/8f2a0001"`) {
		t.Error("the Cluster page does not link the actor page")
	}
}

func TestTraceURL(t *testing.T) {
	if got := TraceURL("proj-a", testTrace); got != "https://console.cloud.google.com/traces/list?project=proj-a&tid="+testTrace {
		t.Errorf("TraceURL = %q", got)
	}
	// A domain-scoped project keeps its trace links.
	if got := TraceURL("example.com:proj-a", testTrace); got != "https://console.cloud.google.com/traces/list?project=example.com%3Aproj-a&tid="+testTrace {
		t.Errorf("TraceURL of a domain-scoped project = %q", got)
	}
	for _, tc := range [][2]string{{"proj-a", ""}, {"proj-a", strings.ToUpper(testTrace)}, {"Proj A", testTrace}, {"", testTrace}, {"proj-a", testTrace + "&x=1"}, {":proj-a", testTrace}} {
		if got := TraceURL(tc[0], tc[1]); got != "" {
			t.Errorf("TraceURL(%q, %q) = %q, want none", tc[0], tc[1], got)
		}
	}
}

type fakeMetrics struct {
	req  MetricsRequest
	data json.RawMessage
	err  error
}

func (f *fakeMetrics) Range(_ context.Context, req MetricsRequest) (json.RawMessage, error) {
	f.req = req
	return f.data, f.err
}

// FS-6: the metrics API runs a named query only.
func TestAPIMetrics(t *testing.T) {
	ts := newTestSite(t)
	if resp, _ := ts.get(t, "/api/v1/metrics/requests_rate"); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("no metrics project: %d, want 503", resp.StatusCode)
	}
	m := &fakeMetrics{data: json.RawMessage(`{"resultType":"matrix","result":[]}`)}
	ts.srv.cfg.Metrics = m
	resp, body := ts.get(t, "/api/v1/metrics/environments?cluster=bench-1&namespace=ate-env&window=6h")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `"resultType":"matrix"`) {
		t.Fatalf("%d: %s", resp.StatusCode, body)
	}
	if m.req != (MetricsRequest{Name: "environments", Cluster: "bench-1", Namespace: "ate-env", Window: "6h"}) {
		t.Errorf("request = %+v", m.req)
	}
	ts.get(t, "/api/v1/metrics/environments?project=proj-b-123&cluster=ate-dev")
	if m.req.Project != "proj-b-123" {
		t.Errorf("request = %+v", m.req)
	}
	for _, p := range []string{
		"/api/v1/metrics/up",
		"/api/v1/metrics/requests_rate?window=7d",
		`/api/v1/metrics/requests_rate?cluster=a%22%7Dor+vector(1)`,
		"/api/v1/metrics/requests_rate?project=Bad%20Project",
	} {
		if resp, _ := ts.get(t, p); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", p, resp.StatusCode)
		}
	}
	// The route has its own limit, because each new request is a Cloud
	// Monitoring query.
	limited := 0
	for range metricsBurst + 2 {
		if resp, _ := ts.do(t, http.MethodGet, "/api/v1/metrics/workers", "busy@example.com"); resp.StatusCode == http.StatusTooManyRequests {
			limited++
		}
	}
	if limited != 2 {
		t.Errorf("%d metrics requests limited, want 2 (burst %d)", limited, metricsBurst)
	}
	m.err = errors.New("monitoring: detail")
	if resp, body := ts.get(t, "/api/v1/metrics/workers"); resp.StatusCode != http.StatusBadGateway || strings.Contains(body, "detail") {
		t.Errorf("failed query: %d %s", resp.StatusCode, body)
	}
}
