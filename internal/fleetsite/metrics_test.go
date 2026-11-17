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
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/option"
)

// FS-6: the named queries, with the filters as label matchers.
func TestMetricsRequestPromQL(t *testing.T) {
	for _, tc := range []struct {
		req  MetricsRequest
		want string
	}{
		// A 5m rate: the 6h and 24h windows step 3m and 5m, and a shorter
		// range would skip the samples between two steps.
		{MetricsRequest{Name: "requests_rate"}, "sum by (project_id, location, cluster, namespace, method) (rate(ate_env_api_requests_total[5m]))"},
		{MetricsRequest{Name: "environments", Cluster: "bench-1", Namespace: "ate-env"}, `max by (project_id, location, cluster, namespace, atespace, state) (ate_env_api_environments{cluster="bench-1",namespace="ate-env"})`},
		// Two clusters of one name in two projects: the project picks one.
		{MetricsRequest{Name: "environments", Project: "proj-b-123", Cluster: "ate-dev"}, `ate_env_api_environments{project_id="proj-b-123",cluster="ate-dev"}`},
		// A domain-scoped project is a project too.
		{MetricsRequest{Name: "environments", Project: "example.com:proj-b"}, `ate_env_api_environments{project_id="example.com:proj-b"}`},
		// Substrate's series come over OTLP: Cloud Monitoring keeps the
		// dotted names, without _total, and the dotted label keys, which a
		// query copies before it groups by them.
		{MetricsRequest{Name: "crashes_rate", Namespace: "ate-env"}, `rate({"ate.actor.crashes","ate.workerpool.namespace"="ate-env"}[5m])`},
		{MetricsRequest{Name: "crashes_rate", Cluster: "c"}, `sum by (project_id, location, cluster, ate_template_name) (label_replace(rate({"ate.actor.crashes",cluster="c"}[5m]), "ate_template_name", "$1", "ate.template.name", "(.*)"))`},
		{MetricsRequest{Name: "workers", Namespace: "ate-env"}, `{"ate.workerpool.workers","ate.worker.state"!="unschedulable","ate.sandbox.class"!="unknown","ate.workerpool.namespace"="ate-env"}`},
		// Averaged across the ateapi replicas, keeping state and class,
		// then added: a max counts a worker that changed state twice.
		{MetricsRequest{Name: "workers"}, `sum by (project_id, location, cluster, ate_workerpool_namespace, ate_workerpool_name) (avg by (project_id, location, cluster, ate_workerpool_namespace, ate_workerpool_name, ate_worker_state, ate_sandbox_class) (label_replace(`},
		{MetricsRequest{Name: "workers"}, `"ate_sandbox_class", "$1", "ate.sandbox.class", "(.*)"))`},
	} {
		if err := tc.req.Validate(); err != nil {
			t.Fatalf("%+v: %v", tc.req, err)
		}
		if got := tc.req.PromQL(); !strings.Contains(got, tc.want) {
			t.Errorf("%+v: %s, want it to contain %s", tc.req, got, tc.want)
		}
	}
	// Every name of the plan's table is in the allow-list, and nothing else.
	for _, n := range []string{"requests_rate", "create_to_ready_p50", "wake_wait_p95", "environments", "workers", "crashes_rate"} {
		if _, ok := NamedQueries[n]; !ok {
			t.Errorf("%s is not a named query", n)
		}
	}
	if len(NamedQueries) != 6 {
		t.Errorf("%d named queries, want the plan's 6", len(NamedQueries))
	}
	longest := time.Duration(0)
	for _, step := range MetricsWindows {
		longest = max(longest, step)
	}
	rangeVector := regexp.MustCompile(`\[(\d+m)\]`)
	for n := range NamedQueries {
		q := (MetricsRequest{Name: n}).PromQL()
		// Every query keeps clusters of one name in two projects apart.
		if strings.Count(q, "by (project_id, location, cluster") < 1 {
			t.Errorf("%s does not group by project_id, location and cluster: %s", n, q)
		}
		// Managed Service for Prometheus rejects a dotted label in by (...).
		for _, by := range regexp.MustCompile(`by \(([^)]*)\)`).FindAllStringSubmatch(q, -1) {
			if strings.ContainsAny(by[1], `."`) {
				t.Errorf("%s groups by a dotted label: %s", n, by[0])
			}
		}
		// No rate range is shorter than the longest step.
		for _, r := range rangeVector.FindAllStringSubmatch(q, -1) {
			if d, _ := time.ParseDuration(r[1]); d < longest {
				t.Errorf("%s: a %s range under a %s step skips samples", n, r[1], longest)
			}
		}
	}
	if q := (MetricsRequest{Name: "up"}).PromQL(); q != "" {
		t.Errorf("PromQL of an unknown name = %q, want none", q)
	}
}

func TestMetricsRequestRejects(t *testing.T) {
	for _, req := range []MetricsRequest{
		{Name: "up"},
		{Name: "requests_rate", Window: "2h"},
		{Name: "requests_rate", Cluster: `a"}or vector(1)`},
		{Name: "requests_rate", Cluster: "UPPER"},
		{Name: "requests_rate", Namespace: `a\"`},
		{Name: "requests_rate", Project: "Bad Project"},
		{Name: "requests_rate", Project: `p"}`},
		{Name: "requests_rate", Project: `example.com:p"}`},
		{Name: "requests_rate", Project: ":proj-b-123"},
	} {
		if err := req.Validate(); err == nil {
			t.Errorf("%+v accepted", req)
		}
	}
}

// fakeMonitoring answers query_range like the Cloud Monitoring PromQL API.
type fakeMonitoring struct {
	mu      sync.Mutex
	queries []*http.Request
	status  int
	body    string
	// onQuery runs before the answer, as a slow query's time passes.
	onQuery func()
}

func (f *fakeMonitoring) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.queries = append(f.queries, r)
	f.mu.Unlock()
	if f.onQuery != nil {
		f.onQuery()
	}
	w.WriteHeader(f.status)
	_, _ = w.Write([]byte(f.body))
}

func newTestPromQL(t *testing.T, f *fakeMonitoring, clock *fakeClock) *PromQLMetrics {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	p, err := NewPromQLMetrics(context.Background(), "central-proj", srv.URL, option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	p.now = clock.now
	return p
}

// slowMonitoring answers after release, counting the calls.
type slowMonitoring struct {
	fakeMonitoring
	release chan struct{}
}

func (f *slowMonitoring) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	<-f.release
	f.fakeMonitoring.ServeHTTP(w, r)
}

// Concurrent requests for one query share one upstream call.
func TestPromQLMetricsSharesACall(t *testing.T) {
	f := &slowMonitoring{fakeMonitoring: fakeMonitoring{status: http.StatusOK, body: `{"status":"success","data":{}}`}, release: make(chan struct{})}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	p, err := NewPromQLMetrics(context.Background(), "central-proj", srv.URL, option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	p.now = (&fakeClock{t: t0}).now
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Range(context.Background(), MetricsRequest{Name: "workers", Window: "1h"}); err != nil {
				t.Error(err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(f.release)
	wg.Wait()
	if n := len(f.queries); n != 1 {
		t.Errorf("%d upstream calls for 20 concurrent requests, want 1", n)
	}
}

func TestPromQLMetricsRange(t *testing.T) {
	f := &fakeMonitoring{status: http.StatusOK, body: `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"cluster":"c"},"values":[[1,"2"]]}]}}`}
	clock := &fakeClock{t: t0}
	p := newTestPromQL(t, f, clock)
	req := MetricsRequest{Name: "requests_rate", Cluster: "c", Window: "1h"}
	data, err := p.Range(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"resultType":"matrix"`) {
		t.Errorf("data = %s", data)
	}
	r := f.queries[0]
	q := r.URL.Query()
	if r.URL.Path != "/v1/projects/central-proj/location/global/prometheus/api/v1/query_range" ||
		q.Get("query") != req.PromQL() || q.Get("step") != "30" ||
		q.Get("end") != "1794830400" || q.Get("start") != "1794826800" {
		t.Errorf("request = %s ?%s", r.URL.Path, r.URL.RawQuery)
	}

	// Cached for MetricsCacheTTL.
	if _, err := p.Range(context.Background(), req); err != nil || len(f.queries) != 1 {
		t.Errorf("%d queries within the TTL, %v", len(f.queries), err)
	}
	clock.add(MetricsCacheTTL)
	if _, err := p.Range(context.Background(), req); err != nil || len(f.queries) != 2 {
		t.Errorf("%d queries after the TTL, %v", len(f.queries), err)
	}
}

// A query that takes longer than the TTL, or times out, is still cached for
// the whole TTL after it ends, while its window ends when it was asked for.
func TestPromQLMetricsTTLCountsFromTheEndOfTheCall(t *testing.T) {
	clock := &fakeClock{t: t0}
	f := &fakeMonitoring{status: http.StatusGatewayTimeout, body: `{"status":"error","error":"deadline exceeded"}`}
	f.onQuery = func() { clock.add(MetricsCacheTTL + 5*time.Second) }
	p := newTestPromQL(t, f, clock)
	req := MetricsRequest{Name: "workers", Window: "1h"}
	if _, err := p.Range(context.Background(), req); err == nil {
		t.Fatal("no error")
	}
	if end := f.queries[0].URL.Query().Get("end"); end != strconv.FormatInt(t0.Unix(), 10) {
		t.Errorf("the window ends at %s, want when the query was asked for (%d)", end, t0.Unix())
	}
	f.onQuery = nil
	clock.add(MetricsCacheTTL - time.Second)
	if _, err := p.Range(context.Background(), req); err == nil || len(f.queries) != 1 {
		t.Errorf("%d upstream calls within the TTL of the end of a slow failing call, want 1 (err %v)", len(f.queries), err)
	}
	clock.add(time.Second)
	if _, err := p.Range(context.Background(), req); err == nil || len(f.queries) != 2 {
		t.Errorf("%d upstream calls a TTL after the end, want 2", len(f.queries))
	}
}

// A domain-scoped metrics project is accepted and escaped in the path.
func TestPromQLMetricsDomainScopedProject(t *testing.T) {
	f := &fakeMonitoring{status: http.StatusOK, body: `{"status":"success","data":{}}`}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	p, err := NewPromQLMetrics(context.Background(), "example.com:central-proj", srv.URL, option.WithoutAuthentication(), option.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	p.now = (&fakeClock{t: t0}).now
	if _, err := p.Range(context.Background(), MetricsRequest{Name: "workers", Window: "1h"}); err != nil {
		t.Fatal(err)
	}
	if got, want := f.queries[0].URL.Path, "/v1/projects/example.com:central-proj/location/global/prometheus/api/v1/query_range"; got != want {
		t.Errorf("path %s, want %s", got, want)
	}
}

func TestPromQLMetricsErrors(t *testing.T) {
	for _, tc := range []struct {
		f    *fakeMonitoring
		want string
	}{
		{&fakeMonitoring{status: http.StatusBadRequest, body: `{"status":"error","error":"parse error"}`}, "parse error"},
		{&fakeMonitoring{status: http.StatusOK, body: `{"status":"error","error":"x"}`}, "x"},
		// A Google API error keeps its reason, which an operator needs.
		{&fakeMonitoring{status: http.StatusForbidden, body: `{"error":{"code":403,"message":"Permission monitoring.timeSeries.list denied","status":"PERMISSION_DENIED"}}`}, "monitoring.timeSeries.list denied"},
		{&fakeMonitoring{status: http.StatusForbidden, body: `<html>denied</html>`}, "<html>denied</html>"},
		{&fakeMonitoring{status: http.StatusOK, body: strings.Repeat(" ", maxMetricsBytes+1)}, "larger than"},
	} {
		_, err := newTestPromQL(t, tc.f, &fakeClock{t: t0}).Range(context.Background(), MetricsRequest{Name: "workers", Window: "1h"})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("status %d %.30q: err %v, want %q", tc.f.status, tc.f.body, err, tc.want)
		}
	}
	for _, req := range []MetricsRequest{{Name: "up"}, {Name: "workers", Window: "7d"}, {Name: "workers", Cluster: `a"}`}} {
		f := &fakeMonitoring{status: http.StatusOK}
		if _, err := newTestPromQL(t, f, &fakeClock{t: t0}).Range(context.Background(), req); err == nil || len(f.queries) != 0 {
			t.Errorf("Range ran %+v: err %v, %d upstream calls", req, err, len(f.queries))
		}
	}

	// A failing query is cached for the TTL like a result.
	f := &fakeMonitoring{status: http.StatusInternalServerError, body: `{"status":"error","error":"down"}`}
	p := newTestPromQL(t, f, &fakeClock{t: t0})
	for range 5 {
		if _, err := p.Range(context.Background(), MetricsRequest{Name: "workers", Window: "1h"}); err == nil {
			t.Fatal("no error")
		}
	}
	if len(f.queries) != 1 {
		t.Errorf("%d upstream calls for 5 retries of a failing query within the TTL, want 1", len(f.queries))
	}
	if _, err := NewPromQLMetrics(context.Background(), "Bad Project", MonitoringEndpoint, option.WithoutAuthentication()); err == nil {
		t.Error("a bad project accepted")
	}
}
