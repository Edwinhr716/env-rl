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
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

// MetricsCacheTTL is how long one named query's result, or its error,
// serves requests.
const MetricsCacheTTL = 10 * time.Second

// maxMetricsBytes bounds one PromQL response.
const maxMetricsBytes = 8 << 20

// monitoringReadScope is the OAuth scope of the Cloud Monitoring PromQL API.
const monitoringReadScope = "https://www.googleapis.com/auth/monitoring.read"

// MonitoringEndpoint is the Cloud Monitoring PromQL API.
const MonitoringEndpoint = "https://monitoring.googleapis.com/"

// metricLabelValue matches a cluster or namespace filter: a Kubernetes name
// or a GKE cluster name. It rules out quotes and backslashes, so a value
// cannot leave its label matcher.
var metricLabelValue = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// namedQuery is one entry of the allow-list. render returns the PromQL for
// the label matchers it is given (each `label="value"`, possibly none);
// namespaceLabel is the label a namespace filter matches: the ate-env-api
// deployment's namespace for its own series, the WorkerPool's for
// Substrate's.
type namedQuery struct {
	render         func(matchers []string) string
	namespaceLabel string
}

// promSelector selects a series that Managed Service for Prometheus scraped
// (ate-env-api's PodMonitoring): a Prometheus metric name.
func promSelector(metric string, matchers []string) string {
	if len(matchers) == 0 {
		return metric
	}
	return metric + "{" + strings.Join(matchers, ",") + "}"
}

// otelSelector selects a series that Substrate pushed over OTLP through GKE
// Managed OpenTelemetry. Cloud Monitoring keeps the dotted instrument name,
// without a _total suffix, and the dotted attribute keys, so the name is a
// quoted matcher (go.ate.dev/substrate docs/dev/best-practices/metrics.md).
func otelSelector(metric string, matchers []string) string {
	return "{" + strings.Join(append([]string{strconv.Quote(metric)}, matchers...), ",") + "}"
}

// copyLabels copies each dotted label key to its underscore form with
// label_replace. Managed Service for Prometheus rejects a dotted label in
// by (...), so a query copies a label before it groups by it, as Substrate's
// own rules do (docs/examples/gmp-alerting-rules.yaml).
func copyLabels(expr string, dotted ...string) string {
	for _, l := range dotted {
		expr = fmt.Sprintf(`label_replace(%s, %q, "$1", %q, "(.*)")`, expr, strings.ReplaceAll(l, ".", "_"), l)
	}
	return expr
}

// NamedQueries is the allow-list of the metrics API
// (docs/plans/sandbox-fleet-site/interfaces.md, "Named PromQL queries"). The
// API runs only these, never a query from the request. Every query groups by
// project_id and location next to cluster, the labels Managed Service for
// Prometheus puts on each series, so two clusters of one name in two
// projects stay two series. Each rate range is 5m, no shorter than the
// longest step of MetricsWindows, so a range query sees every sample.
var NamedQueries = map[string]namedQuery{
	"requests_rate": {func(m []string) string {
		return "sum by (project_id, location, cluster, namespace, method) (rate(" + promSelector("ate_env_api_requests_total", m) + "[5m]))"
	}, "namespace"},
	"create_to_ready_p50": {func(m []string) string {
		return "histogram_quantile(0.5, sum by (project_id, location, cluster, namespace, le) (rate(" + promSelector("ate_env_api_create_to_ready_seconds_bucket", m) + "[5m])))"
	}, "namespace"},
	"wake_wait_p95": {func(m []string) string {
		return "histogram_quantile(0.95, sum by (project_id, location, cluster, namespace, le) (rate(" + promSelector("ate_env_api_wake_wait_seconds_bucket", m) + "[5m])))"
	}, "namespace"},
	"environments": {func(m []string) string {
		return "max by (project_id, location, cluster, namespace, atespace, state) (" + promSelector("ate_env_api_environments", m) + ")"
	}, "namespace"},
	// The workers that can take actors, per pool. Every ateapi replica
	// reports the whole fleet from its own cache: average across the
	// replicas first, keeping state and sandbox class, then add, as
	// Substrate's metrics registry says (a max would count a worker that
	// changed state under both states). Unschedulable workers and workers
	// of the unknown class are not capacity.
	"workers": {func(m []string) string {
		sel := otelSelector("ate.workerpool.workers", append([]string{`"ate.worker.state"!="unschedulable"`, `"ate.sandbox.class"!="unknown"`}, m...))
		return "sum by (project_id, location, cluster, ate_workerpool_namespace, ate_workerpool_name) (avg by (project_id, location, cluster, ate_workerpool_namespace, ate_workerpool_name, ate_worker_state, ate_sandbox_class) (" +
			copyLabels(sel, "ate.workerpool.namespace", "ate.workerpool.name", "ate.worker.state", "ate.sandbox.class") + "))"
	}, `"ate.workerpool.namespace"`},
	"crashes_rate": {func(m []string) string {
		return "sum by (project_id, location, cluster, ate_template_name) (" + copyLabels("rate("+otelSelector("ate.actor.crashes", m)+"[5m])", "ate.template.name") + ")"
	}, `"ate.workerpool.namespace"`},
}

// MetricsWindows are the windows the metrics API takes, with the step of
// each, so a range has 60 to 300 points and a 5m rate covers each step.
var MetricsWindows = map[string]time.Duration{
	"15m": 15 * time.Second,
	"1h":  30 * time.Second,
	"6h":  3 * time.Minute,
	"24h": 5 * time.Minute,
}

// DefaultMetricsWindow is the window without ?window=.
const DefaultMetricsWindow = "1h"

// MetricsRequest is one call of the metrics API, checked by Validate.
type MetricsRequest struct {
	Name, Project, Cluster, Namespace, Window string
}

// ErrBadMetricsRequest is the error of a request the allow-list refuses.
var ErrBadMetricsRequest = errors.New("bad metrics request")

// Validate checks the request against the allow-list and sets the default
// window.
func (m *MetricsRequest) Validate() error {
	_, ok := NamedQueries[m.Name]
	if !ok {
		names := make([]string, 0, len(NamedQueries))
		for n := range NamedQueries {
			names = append(names, n)
		}
		sort.Strings(names)
		return fmt.Errorf("%w: %q is not a named query (%s)", ErrBadMetricsRequest, m.Name, strings.Join(names, ", "))
	}
	if m.Window == "" {
		m.Window = DefaultMetricsWindow
	}
	if _, ok := MetricsWindows[m.Window]; !ok {
		return fmt.Errorf("%w: window %q is not one of 15m, 1h, 6h, 24h", ErrBadMetricsRequest, m.Window)
	}
	if m.Project != "" && !gcpProject.MatchString(m.Project) {
		return fmt.Errorf("%w: project %q is not a Google Cloud project id", ErrBadMetricsRequest, m.Project)
	}
	if m.Cluster != "" && !metricLabelValue.MatchString(m.Cluster) {
		return fmt.Errorf("%w: cluster %q is not a cluster name", ErrBadMetricsRequest, m.Cluster)
	}
	if m.Namespace != "" {
		if !metricLabelValue.MatchString(m.Namespace) {
			return fmt.Errorf("%w: namespace %q is not a namespace name", ErrBadMetricsRequest, m.Namespace)
		}
	}
	return nil
}

// PromQL returns the query a valid request runs, or "" for a name that is
// not in the allow-list.
func (m MetricsRequest) PromQL() string {
	q, ok := NamedQueries[m.Name]
	if !ok {
		return ""
	}
	var matchers []string
	if m.Project != "" {
		matchers = append(matchers, `project_id="`+m.Project+`"`)
	}
	if m.Cluster != "" {
		matchers = append(matchers, `cluster="`+m.Cluster+`"`)
	}
	if m.Namespace != "" {
		matchers = append(matchers, q.namespaceLabel+`="`+m.Namespace+`"`)
	}
	return q.render(matchers)
}

// MetricsSource runs a named query over a window.
type MetricsSource interface {
	Range(ctx context.Context, req MetricsRequest) (json.RawMessage, error)
}

// PromQLMetrics queries the Cloud Monitoring PromQL API of one project, the
// metrics scope that holds the clusters' projects, as the site's service
// account (application default credentials). Results are cached
// MetricsCacheTTL per instance, and concurrent requests for one query share
// one upstream call.
type PromQLMetrics struct {
	client *http.Client
	base   string // .../v1/projects/<project>/location/global/prometheus/api/v1/
	now    func() time.Time

	mu       sync.Mutex
	cache    map[MetricsRequest]metricsEntry
	inflight map[MetricsRequest]*metricsCall
}

// metricsCall is one upstream call that concurrent requests share.
type metricsCall struct {
	done chan struct{}
	data json.RawMessage
	err  error
}

// metricsEntry is a cached result or error. at is when its upstream call
// ended, so a call that took longer than MetricsCacheTTL is still cached
// for the whole TTL.
type metricsEntry struct {
	at   time.Time
	data json.RawMessage
	err  error
}

// NewPromQLMetrics returns a client for project at endpoint
// (MonitoringEndpoint in production). opts configure the HTTP client.
func NewPromQLMetrics(ctx context.Context, project, endpoint string, opts ...option.ClientOption) (*PromQLMetrics, error) {
	if !gcpProject.MatchString(project) {
		return nil, fmt.Errorf("metrics project %q is not a Google Cloud project id", project)
	}
	client, _, err := htransport.NewClient(ctx, append([]option.ClientOption{option.WithScopes(monitoringReadScope)}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("creating the Cloud Monitoring client: %w", err)
	}
	return &PromQLMetrics{
		client:   client,
		base:     strings.TrimSuffix(endpoint, "/") + "/v1/projects/" + url.PathEscape(project) + "/location/global/prometheus/api/v1/",
		now:      time.Now,
		cache:    map[MetricsRequest]metricsEntry{},
		inflight: map[MetricsRequest]*metricsCall{},
	}, nil
}

// Range runs req, which Validate accepted, as a range query ending now and
// returns the data of the Prometheus response.
func (p *PromQLMetrics) Range(ctx context.Context, req MetricsRequest) (json.RawMessage, error) {
	// The route validates first; a caller of the package may not have.
	if err := req.Validate(); err != nil {
		return nil, err
	}
	now := p.now()
	p.mu.Lock()
	if e, ok := p.cache[req]; ok && now.Sub(e.at) < MetricsCacheTTL {
		p.mu.Unlock()
		return e.data, e.err
	}
	call, ok := p.inflight[req]
	if !ok {
		call = &metricsCall{done: make(chan struct{})}
		p.inflight[req] = call
		// The call runs apart from the request that started it, so a closed
		// tab does not fail the requests that share it.
		go func() {
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), requestTimeout)
			defer cancel()
			// now ends the query's window; the TTL counts from the end of
			// the call, which can take up to requestTimeout.
			call.data, call.err = p.fetch(cctx, req, now)
			done := p.now()
			p.mu.Lock()
			defer p.mu.Unlock()
			delete(p.inflight, req)
			// Drop the entries that went old, so the cache holds at most the
			// requests of one TTL. An error is kept like a result, so retries
			// of a failing query make one upstream call per TTL.
			for k, e := range p.cache {
				if done.Sub(e.at) >= MetricsCacheTTL {
					delete(p.cache, k)
				}
			}
			p.cache[req] = metricsEntry{at: done, data: call.data, err: call.err}
			close(call.done)
		}()
	}
	p.mu.Unlock()
	select {
	case <-call.done:
		return call.data, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fetch makes the upstream call of one range query.
func (p *PromQLMetrics) fetch(ctx context.Context, req MetricsRequest, now time.Time) (json.RawMessage, error) {
	step := MetricsWindows[req.Window]
	window, _ := time.ParseDuration(req.Window)
	v := url.Values{
		"query": {req.PromQL()},
		"start": {strconv.FormatInt(now.Add(-window).Unix(), 10)},
		"end":   {strconv.FormatInt(now.Unix(), 10)},
		"step":  {strconv.Itoa(int(step.Seconds()))},
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"query_range?"+v.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(hreq)
	if err != nil {
		return nil, fmt.Errorf("querying Cloud Monitoring: %w", err)
	}
	defer resp.Body.Close()
	body, err := readLimited(resp.Body, "the PromQL response", maxMetricsBytes)
	if err != nil {
		return nil, err
	}
	var out struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil || (resp.StatusCode == http.StatusOK && out.Status != "success") || resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the Cloud Monitoring API answered %d: %s", resp.StatusCode, upstreamError(out.Error, body))
	}
	return out.Data, nil
}

// upstreamError returns the reason of a failed PromQL response: the
// Prometheus error string, the message of a Google API error
// ({"error": {"code", "message"}}), else the start of the body.
func upstreamError(field json.RawMessage, body []byte) string {
	var s string
	if json.Unmarshal(field, &s) == nil && s != "" {
		return s
	}
	var g struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(field, &g) == nil && g.Message != "" {
		return g.Message
	}
	if len(body) > 512 {
		body = body[:512]
	}
	return string(body)
}

// gcpProject matches a Google Cloud project id, also a domain-scoped one
// (example.com:my-project), as `ate-env manifest collector` takes it
// (cmd/ate-env/collector.go). It has no quote or backslash, so it cannot
// leave a label matcher.
var gcpProject = regexp.MustCompile(`^([a-z0-9][a-z0-9.-]*[a-z0-9]:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
