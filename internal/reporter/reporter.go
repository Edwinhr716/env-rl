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

// Package reporter writes one cluster's fleet inventory snapshot
// (internal/inventory) on an interval: the atespaces, templates, tags, workers
// and actors the Substrate control plane lists, with the WorkerPools and the
// ate-env-api Deployments the Kubernetes API lists. It only reads the
// cluster; its one write is the snapshot object.
package reporter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/inventory"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// catalogEvery is how many ticks one read of the atespaces, templates and
// tags serves. They change far less often than actors do, and an actor whose
// atespace or template the last read lacks forces a read at once.
const catalogEvery = 10

// DefaultInterval and DefaultSnapshotInterval are the defaults of
// ate-env-reporter's -interval and -snapshot-interval.
const (
	DefaultInterval         = 5 * time.Second
	DefaultSnapshotInterval = time.Hour
)

// maxBackoff caps the wait between ticks while they fail.
const maxBackoff = time.Minute

// maxDroppedLogged bounds the label keys remembered as logged: selector keys
// come from the callers that create actors.
const maxDroppedLogged = 100

// tickSpanName names the span of one tick when Config.Tracer is set.
const tickSpanName = "ate-env-reporter.tick"

// tickTimeout bounds one tick, so a control plane that hangs fails the tick
// and readiness instead of stopping the loop.
const tickTimeout = time.Minute

// Cluster is the Kubernetes side of a snapshot.
type Cluster interface {
	WorkerPools(ctx context.Context) ([]inventory.WorkerPool, error)
	Deployments(ctx context.Context) ([]inventory.Deployment, error)
}

// Store writes one object of the bucket layout. A write replaces the object
// as a whole: a reader sees the old content or the new, never a part.
type Store interface {
	Write(ctx context.Context, name string, data []byte) error
}

// Config identifies the cluster and the writer, and sets the pace.
type Config struct {
	// Project and Cluster name the cluster; both are required. Location is
	// optional.
	Project, Cluster, Location string
	// Version and Commit identify the reporter build; Version is required.
	Version, Commit string
	// Interval is the time from one tick's start to the next.
	Interval time.Duration
	// SnapshotInterval is how often a tick also keeps its snapshot under
	// v1/snapshots/.
	SnapshotInterval time.Duration
	// Tracer, when set, starts a tickSpanName span for each tick that Run
	// starts; the control-plane calls of the tick are its children when the
	// ate.Client traces.
	Tracer trace.Tracer
	// OnTick, when set, is called after each tick that Run starts with the
	// tick's duration and error. A tick that ends because Run's context is
	// done is not reported.
	OnTick func(d time.Duration, err error)
}

// Reporter builds and writes snapshots. Tick and Run are for one goroutine;
// Ready is safe from any.
type Reporter struct {
	cfg     Config
	control *ate.Client
	cluster Cluster
	store   Store

	ticks     int
	atespaces []*ateapipb.Atespace
	templates []*ateapipb.ActorTemplate
	tags      []*ateapipb.Tag
	// lastKept is the generated_at of the last snapshot kept under
	// v1/snapshots/.
	lastKept time.Time
	// unresolved are the atespaces and templates that actors named and the
	// catalog read that followed still lacked: a template deleted under its
	// actors. They do not force another read.
	unresolved map[[2]string]bool
	// dropped are the label keys already logged as left out, at most
	// maxDroppedLogged of them.
	dropped map[string]bool
	ready   atomic.Bool
}

// New returns a Reporter that reads control and cluster and writes to store.
func New(cfg Config, control *ate.Client, cluster Cluster, store Store) (*Reporter, error) {
	switch {
	case cfg.Project == "" || cfg.Cluster == "":
		return nil, errors.New("reporter: project and cluster are required")
	case cfg.Version == "":
		return nil, errors.New("reporter: version is required")
	case cfg.Interval < time.Second:
		// interval_s is whole seconds and must be positive.
		return nil, fmt.Errorf("reporter: interval %s is under one second", cfg.Interval)
	case cfg.SnapshotInterval < cfg.Interval:
		return nil, fmt.Errorf("reporter: snapshot interval %s is under the interval %s", cfg.SnapshotInterval, cfg.Interval)
	}
	// Project and Cluster are one segment each of the object names; a
	// separator or a dot segment would write to another cluster's place.
	if err := inventory.CheckPathSegment("project", cfg.Project); err != nil {
		return nil, fmt.Errorf("reporter: %w", err)
	}
	if err := inventory.CheckPathSegment("cluster", cfg.Cluster); err != nil {
		return nil, fmt.Errorf("reporter: %w", err)
	}
	if cfg.Tracer == nil {
		cfg.Tracer = noop.NewTracerProvider().Tracer("")
	}
	return &Reporter{cfg: cfg, control: control, cluster: cluster, store: store, dropped: map[string]bool{}}, nil
}

// LatestName is the object a cluster's reporter rewrites on every tick.
func LatestName(project, cluster string) string {
	return path.Join("v1/inventory", project, cluster, "latest.json")
}

// KeptName is the object that keeps the snapshot generated at t.
func KeptName(project, cluster string, t time.Time) string {
	return path.Join("v1/snapshots", project, cluster, t.UTC().Format("20060102T150405Z")+".json")
}

// Ready reports whether the last tick wrote its snapshot.
func (r *Reporter) Ready() bool { return r.ready.Load() }

// Run ticks until ctx is done. A failed tick is logged and retried: the wait
// after it doubles with each consecutive failure, up to maxBackoff, and
// returns to the interval with the first success.
func (r *Reporter) Run(ctx context.Context) {
	failures := 0
	for {
		start := time.Now()
		err := r.tracedTick(ctx)
		if ctx.Err() != nil {
			return
		}
		if r.cfg.OnTick != nil {
			r.cfg.OnTick(time.Since(start), err)
		}
		r.ready.Store(err == nil)
		var wait time.Duration
		if err != nil {
			// The whole backoff from the tick's end: a tick that failed at
			// tickTimeout has outlasted any wait measured from its start,
			// and the next would follow it at once.
			failures++
			wait = backoff(r.cfg.Interval, failures)
			log.Printf("tick failed (%d in a row, next in %s): %v", failures, wait, err)
		} else {
			if failures > 0 {
				log.Printf("tick succeeded after %d failures", failures)
			}
			failures = 0
			// The interval is from one tick's start to the next.
			wait = max(0, r.cfg.Interval-time.Since(start))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// tracedTick runs one tick within tickTimeout, in a span of its own.
func (r *Reporter) tracedTick(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, tickTimeout)
	defer cancel()
	ctx, span := r.cfg.Tracer.Start(ctx, tickSpanName)
	defer span.End()
	err := r.Tick(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// backoff is the wait after failures consecutive failed ticks.
func backoff(interval time.Duration, failures int) time.Duration {
	limit := max(interval, maxBackoff)
	wait := interval
	for range failures {
		if wait >= limit {
			break
		}
		wait *= 2
	}
	return min(wait, limit)
}

// Tick reads the cluster, builds one snapshot and writes it as latest.json,
// even when nothing but generated_at changed, so the object's generation
// moves with it. Every SnapshotInterval the same bytes are also kept under
// v1/snapshots/; when that second write fails the tick fails with
// latest.json already written, and the next tick tries to keep its own.
func (r *Reporter) Tick(ctx context.Context) error {
	snap, err := r.snapshot(ctx)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := snap.Encode(&buf); err != nil {
		return fmt.Errorf("reporter: encoding the snapshot: %w", err)
	}
	if err := r.store.Write(ctx, LatestName(r.cfg.Project, r.cfg.Cluster), buf.Bytes()); err != nil {
		return fmt.Errorf("reporter: writing latest.json: %w", err)
	}
	// Half an interval of slack, so a tick that starts a little early still
	// keeps its snapshot when the two intervals are equal.
	if r.lastKept.IsZero() || snap.GeneratedAt.Sub(r.lastKept) >= r.cfg.SnapshotInterval-r.cfg.Interval/2 {
		name := KeptName(r.cfg.Project, r.cfg.Cluster, snap.GeneratedAt)
		if err := r.store.Write(ctx, name, buf.Bytes()); err != nil {
			return fmt.Errorf("reporter: writing %s: %w", name, err)
		}
		r.lastKept = snap.GeneratedAt
	}
	return nil
}

// snapshot reads the control plane and the Kubernetes API and returns a
// sorted, valid snapshot.
func (r *Reporter) snapshot(ctx context.Context) (*inventory.Snapshot, error) {
	actors, err := r.control.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("reporter: %w", err)
	}
	workers, err := r.control.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("reporter: %w", err)
	}
	if r.ticks%catalogEvery == 0 || len(r.catalogMisses(actors, r.unresolved)) > 0 {
		if err := r.readCatalog(ctx); err != nil {
			return nil, err
		}
		r.unresolved = r.catalogMisses(actors, nil)
	}
	r.ticks++
	pools, err := r.cluster.WorkerPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("reporter: reading WorkerPools: %w", err)
	}
	deployments, err := r.cluster.Deployments(ctx)
	if err != nil {
		return nil, fmt.Errorf("reporter: reading Deployments: %w", err)
	}

	snap := &inventory.Snapshot{
		SchemaVersion: inventory.SchemaVersion,
		// Whole seconds: the kept object's name carries the same instant.
		GeneratedAt: time.Now().UTC().Truncate(time.Second),
		Project:     r.cfg.Project,
		Cluster:     r.cfg.Cluster,
		Location:    r.cfg.Location,
		Reporter: inventory.Reporter{
			Version:         r.cfg.Version,
			Commit:          r.cfg.Commit,
			IntervalSeconds: int(r.cfg.Interval / time.Second),
		},
		Atespaces:   make([]inventory.Atespace, 0, len(r.atespaces)),
		Templates:   make([]inventory.Template, 0, len(r.templates)),
		WorkerPools: append([]inventory.WorkerPool{}, pools...),
		Workers:     make([]inventory.Worker, 0, len(workers)),
		Actors:      make([]inventory.Actor, 0, len(actors)),
		Deployments: append([]inventory.Deployment{}, deployments...),
	}
	for _, a := range r.atespaces {
		snap.Atespaces = append(snap.Atespaces, atespaceOf(a))
	}
	for _, t := range r.templates {
		snap.Templates = append(snap.Templates, templateOf(t))
	}
	for _, t := range r.tags {
		snap.Tags = append(snap.Tags, tagOf(t))
	}
	for _, w := range workers {
		snap.Workers = append(snap.Workers, workerOf(w, r.keep))
	}
	for _, a := range actors {
		snap.Actors = append(snap.Actors, actorOf(a, r.keep))
	}
	snap.Counts = inventory.ComputeCounts(snap.Actors)
	snap.Sort()
	if err := snap.Validate(); err != nil {
		return nil, fmt.Errorf("reporter: %w", err)
	}
	return snap, nil
}

// readCatalog reads the atespaces, templates and tags. The three replace the
// last read together, so a failed read leaves a consistent set.
func (r *Reporter) readCatalog(ctx context.Context) error {
	atespaces, err := r.control.ListAtespaces(ctx)
	if err != nil {
		return fmt.Errorf("reporter: %w", err)
	}
	templates, err := r.control.ListAllActorTemplates(ctx)
	if err != nil {
		return fmt.Errorf("reporter: %w", err)
	}
	tags, err := r.control.ListAllTags(ctx)
	if err != nil {
		return fmt.Errorf("reporter: %w", err)
	}
	r.atespaces, r.templates, r.tags = atespaces, templates, tags
	return nil
}

// catalogMisses returns the atespaces (as {name, ""}) and the templates (as
// {atespace, name}) that actors name and the last catalog read lacks, less
// those in known. A miss is the sign of one created since the read.
func (r *Reporter) catalogMisses(actors []*ateapipb.Actor, known map[[2]string]bool) map[[2]string]bool {
	atespaces := make(map[string]bool, len(r.atespaces))
	for _, a := range r.atespaces {
		atespaces[a.GetMetadata().GetName()] = true
	}
	templates := make(map[[2]string]bool, len(r.templates))
	for _, t := range r.templates {
		templates[[2]string{t.GetMetadata().GetAtespace(), t.GetMetadata().GetName()}] = true
	}
	misses := map[[2]string]bool{}
	for _, a := range actors {
		if k := [2]string{a.GetMetadata().GetAtespace(), ""}; !atespaces[k[0]] && !known[k] {
			misses[k] = true
		}
		if ref := a.GetActorTemplate(); ref != nil {
			if k := [2]string{ref.GetAtespace(), ref.GetName()}; !templates[k] && !known[k] {
				misses[k] = true
			}
		}
	}
	return misses
}

// keep reports whether a label or selector key goes into the snapshot. A key
// that names a credential stays out, as the schema requires, and is logged
// once, for the first maxDroppedLogged such keys.
func (r *Reporter) keep(key string) bool {
	if !inventory.CredentialKey(key) {
		return true
	}
	if !r.dropped[key] && len(r.dropped) < maxDroppedLogged {
		r.dropped[key] = true
		log.Printf("label key %q names a credential; snapshots leave it out", key)
	}
	return false
}
