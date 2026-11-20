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
	"errors"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/agent-substrate/env/internal/inventory"
	"golang.org/x/time/rate"
)

// LiveEvents is how many transitions the Live page and API show.
const LiveEvents = 200

// liveWindow bounds the Live query to the last hour, so it scans one or two
// day partitions.
const liveWindow = time.Hour

// LiveCacheTTL is how long one Live query serves every user of an instance;
// the Live page reloads itself every LiveRefresh.
const (
	LiveCacheTTL = 10 * time.Second
	LiveRefresh  = 10 * time.Second
)

// traceID matches a W3C trace id, as the events carry it.
var traceID = regexp.MustCompile(`^[0-9a-f]{32}$`)

// liveCache holds the last Live query, its result or its error, for every
// user of the instance, so a page that reloads itself makes one BigQuery job
// per LiveCacheTTL, not one per viewer, also while BigQuery fails. One query
// runs at a time, apart from the requests that wait for it. at is when the
// query was asked for (the end of its window, shown as queried_at); done is
// when it ended, from which the TTL counts, so a query that took longer than
// the TTL, or timed out, is still cached for the whole TTL.
type liveCache struct {
	mu      sync.Mutex
	at      time.Time
	done    time.Time
	events  []Event
	err     error
	running chan struct{}
}

// recent returns the newest LiveEvents transitions of the last liveWindow,
// and the time they were queried.
func (s *Server) recent(ctx context.Context) ([]Event, time.Time, error) {
	now := s.cfg.Now()
	s.live.mu.Lock()
	if !s.live.done.IsZero() && now.Sub(s.live.done) < LiveCacheTTL {
		defer s.live.mu.Unlock()
		return s.live.events, s.live.at, s.live.err
	}
	if s.live.running == nil {
		done := make(chan struct{})
		s.live.running = done
		go func() {
			qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), requestTimeout)
			defer cancel()
			events, err := s.cfg.Events.RecentEvents(qctx, now.Add(-liveWindow), LiveEvents)
			end := s.cfg.Now()
			s.live.mu.Lock()
			s.live.at, s.live.done, s.live.events, s.live.err, s.live.running = now, end, events, err, nil
			s.live.mu.Unlock()
			close(done)
		}()
	}
	running := s.live.running
	s.live.mu.Unlock()
	select {
	case <-running:
	case <-ctx.Done():
		return nil, time.Time{}, ctx.Err()
	}
	s.live.mu.Lock()
	defer s.live.mu.Unlock()
	return s.live.events, s.live.at, s.live.err
}

// parseSince reads ?since=: RFC 3339 within maxEventsWindow, default
// defaultEventsWindow ago.
func (s *Server) parseSince(r *http.Request) (time.Time, error) {
	now := s.cfg.Now()
	v := r.URL.Query().Get("since")
	if v == "" {
		return now.Add(-defaultEventsWindow), nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, errors.New("since is not an RFC 3339 time")
	}
	if t.After(now) || now.Sub(t) > maxEventsWindow {
		return time.Time{}, errors.New("since must be in the last 30 days")
	}
	return t, nil
}

// TraceURL returns the Cloud Trace page of a trace in project, or "" when
// either is not well formed.
func TraceURL(project, trace string) string {
	if !gcpProject.MatchString(project) || !traceID.MatchString(trace) {
		return ""
	}
	return "https://console.cloud.google.com/traces/list?" + url.Values{"project": {project}, "tid": {trace}}.Encode()
}

// eventRow is one transition as the pages show it. A link is empty when
// the event's values cannot name a page: the event comes from a log line.
type eventRow struct {
	Event
	TraceURL, ActorURL, ClusterURL string
}

func eventRows(events []Event) []eventRow {
	out := make([]eventRow, len(events))
	for i, e := range events {
		row := eventRow{Event: e, TraceURL: TraceURL(e.Project, e.TraceID)}
		if actorUID.MatchString(e.ActorUID) {
			row.ActorURL = "/actors/" + url.PathEscape(e.ActorUID)
		}
		if e.Project != "" && e.Cluster != "" && inventory.CheckPathSegment("project", e.Project) == nil && inventory.CheckPathSegment("cluster", e.Cluster) == nil {
			row.ClusterURL = "/clusters/" + url.PathEscape(e.Project) + "/" + url.PathEscape(e.Cluster)
		}
		out[i] = row
	}
	return out
}

// actorIn is an actor as one cluster's snapshot lists it.
type actorIn struct {
	Project, Cluster string
	Actor            inventory.Actor
}

func (s *Server) apiLive(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Events == nil {
		s.apiError(w, r, http.StatusServiceUnavailable, "no events table is configured", nil)
		return
	}
	events, at, err := s.recent(r.Context())
	if err != nil {
		s.apiError(w, r, http.StatusBadGateway, "the events could not be read", err)
		return
	}
	writeJSON(w, map[string]any{"queried_at": at.UTC(), "events": events})
}

func (s *Server) livePage(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{"Title": "Live", "Window": liveWindow.String()}
	if s.cfg.Events == nil {
		// Nothing to reload for.
		data["Unavailable"] = "No events table is configured."
		s.render(w, r, http.StatusOK, "live.html", data)
		return
	}
	// The page reloads itself also after an error, so it recovers when
	// BigQuery does.
	data["Refresh"] = int(LiveRefresh.Seconds())
	events, at, err := s.recent(r.Context())
	if err != nil {
		log.Printf("fleetsite: %s %q: the events could not be read: %v", r.Method, r.URL.Path, err)
		data["Unavailable"] = "The events could not be read; the page tries again in a few seconds."
		s.render(w, r, http.StatusBadGateway, "live.html", data)
		return
	}
	data["Events"] = eventRows(events)
	data["QueriedAt"] = at.UTC()
	s.render(w, r, http.StatusOK, "live.html", data)
}

func (s *Server) actorPage(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if !actorUID.MatchString(uid) {
		s.pageError(w, r, http.StatusBadRequest, "That is not an actor uid.", nil)
		return
	}
	since, err := s.parseSince(r)
	if err != nil {
		s.pageError(w, r, http.StatusBadRequest, "The since parameter must be an RFC 3339 time in the last 30 days.", nil)
		return
	}
	clusters, err := s.cfg.Snapshots.Clusters(r.Context(), force(r))
	if err != nil {
		s.pageError(w, r, http.StatusBadGateway, "The inventory bucket could not be listed.", err)
		return
	}
	// A uid names one actor; it is listed in at most one snapshot, unless
	// a cluster was moved between projects.
	var found []actorIn
	for _, c := range clusters {
		if c.Snapshot == nil {
			continue
		}
		for _, a := range c.Snapshot.Actors {
			if a.UID == uid {
				found = append(found, actorIn{Project: c.Project, Cluster: c.Cluster, Actor: a})
			}
		}
	}
	data := map[string]any{"Title": "Actor " + uid, "UID": uid, "Found": found, "Since": since.UTC()}
	// The JSON link reads the same window as the page.
	eventsJSON := "/api/v1/actors/" + url.PathEscape(uid) + "/events"
	if r.URL.Query().Get("since") != "" {
		eventsJSON += "?" + url.Values{"since": {since.UTC().Format(time.RFC3339)}}.Encode()
	}
	data["EventsJSON"] = eventsJSON
	switch {
	case s.cfg.Events == nil:
		data["EventsUnavailable"] = "No events table is configured."
	case !s.allowEvents(user(r)):
		data["EventsUnavailable"] = "Too many events requests; reload in a few seconds."
	default:
		events, err := s.cfg.Events.ActorEvents(r.Context(), uid, since)
		if err != nil {
			s.pageError(w, r, http.StatusBadGateway, "The events could not be read.", err)
			return
		}
		data["Events"] = eventRows(events)
		data["Truncated"] = len(events) >= MaxEvents
	}
	if rows, _ := data["Events"].([]eventRow); len(found) == 0 && len(rows) == 0 && data["EventsUnavailable"] == nil {
		s.pageError(w, r, http.StatusNotFound, "No snapshot lists this actor and it has no events in the window.", nil)
		return
	}
	s.render(w, r, http.StatusOK, "actor.html", data)
}

func (s *Server) apiMetrics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	req := MetricsRequest{Name: r.PathValue("name"), Project: q.Get("project"), Cluster: q.Get("cluster"), Namespace: q.Get("namespace"), Window: q.Get("window")}
	if err := req.Validate(); err != nil {
		s.apiError(w, r, http.StatusBadRequest, err.Error(), nil)
		return
	}
	if s.cfg.Metrics == nil {
		s.apiError(w, r, http.StatusServiceUnavailable, "no metrics project is configured", nil)
		return
	}
	if !s.allowLimited(user(r), func(l *userLimiter) *rate.Limiter { return l.metrics }) {
		w.Header().Set("Retry-After", "1")
		s.apiError(w, r, http.StatusTooManyRequests, "too many metrics requests", nil)
		return
	}
	data, err := s.cfg.Metrics.Range(r.Context(), req)
	if err != nil {
		s.apiError(w, r, http.StatusBadGateway, "the metrics could not be read", err)
		return
	}
	writeJSON(w, map[string]any{"name": req.Name, "project": req.Project, "cluster": req.Cluster, "namespace": req.Namespace, "window": req.Window, "query": req.PromQL(), "data": data})
}
