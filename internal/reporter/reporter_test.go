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

package reporter

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/inventory"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
)

const (
	guestImage  = "example.com/ate-env-guest@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	rootfsImage = "example.com/python@sha256:1a2b3c4d5e6f0000000000000000000000000000000000000000000000000000"
	poolName    = "default-template-workerpool"
)

// staticCluster is a Cluster with fixed content.
type staticCluster struct {
	pools       []inventory.WorkerPool
	deployments []inventory.Deployment
	err         error
}

func (c staticCluster) WorkerPools(context.Context) ([]inventory.WorkerPool, error) {
	return c.pools, c.err
}

func (c staticCluster) Deployments(context.Context) ([]inventory.Deployment, error) {
	return c.deployments, c.err
}

// memStore keeps every write, and fails the next fail writes.
type memStore struct {
	mu     sync.Mutex
	fail   int
	names  []string
	times  []time.Time
	latest map[string][]byte
}

func (s *memStore) Write(_ context.Context, name string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail > 0 {
		s.fail--
		return errors.New("bucket unavailable")
	}
	if s.latest == nil {
		s.latest = map[string][]byte{}
	}
	s.names = append(s.names, name)
	s.times = append(s.times, time.Now())
	s.latest[name] = bytes.Clone(data)
	return nil
}

// kept returns the names written under v1/snapshots/.
func (s *memStore) kept() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, n := range s.names {
		if strings.HasPrefix(n, "v1/snapshots/") {
			out = append(out, n)
		}
	}
	return out
}

// latestTimes returns when each latest.json write landed.
func (s *memStore) latestTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []time.Time
	for i, n := range s.names {
		if strings.HasSuffix(n, "/latest.json") {
			out = append(out, s.times[i])
		}
	}
	return out
}

func baseTemplate(name string) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: name},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{workerPoolLabel: poolName}},
		Containers:     []*ateapipb.Container{{Name: "guest", Image: guestImage}},
	}
}

func createActor(t *testing.T, ctrl *fakecontrol.Server, name, template string, selector map[string]string) {
	t.Helper()
	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ate-env", Name: template},
	}
	if selector != nil {
		actor.WorkerSelector = &ateapipb.Selector{MatchLabels: selector}
	}
	if _, err := ctrl.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: actor}); err != nil {
		t.Fatal(err)
	}
}

// newControl returns a fake control plane with the ate-env atespace, the
// default template and one worker.
func newControl(t *testing.T) *fakecontrol.Server {
	t.Helper()
	ctrl := fakecontrol.New()
	if _, err := ctrl.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ate-env"}},
	}); err != nil {
		t.Fatal(err)
	}
	ctrl.AddTemplate(baseTemplate("default-template"))
	ctrl.AddWorker(fakecontrol.Worker("ate-env", poolName, "node-1", map[string]string{workerPoolLabel: poolName}, 8))
	return ctrl
}

func newReporter(t *testing.T, ctrl *fakecontrol.Server, cluster Cluster, store Store, interval, snapshotInterval time.Duration) *Reporter {
	t.Helper()
	r, err := New(Config{
		Project: "p", Cluster: "c", Location: "us-central1", Version: "v0.1.0", Commit: "abc1234",
		Interval: interval, SnapshotInterval: snapshotInterval,
	}, ate.NewControlClient(ctrl.Client()), cluster, store)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func decode(t *testing.T, data []byte) *inventory.Snapshot {
	t.Helper()
	snap, err := inventory.DecodeSnapshot(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeSnapshot: %v\n%s", err, data)
	}
	return snap
}

func tick(t *testing.T, r *Reporter, store *memStore) *inventory.Snapshot {
	t.Helper()
	if err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	return decode(t, store.latest[LatestName("p", "c")])
}

// TestTickWritesTheClusterAsAValidSnapshot covers every object kind once: the
// snapshot a tick writes into the bucket layout decodes with DecodeSnapshot
// and carries what the control plane and the cluster hold.
func TestTickWritesTheClusterAsAValidSnapshot(t *testing.T) {
	ctrl := newControl(t)
	derived := baseTemplate("default-template-1a2b3c4d5e6f")
	derived.Containers[0].Image = rootfsImage
	derived.Volumes = []*ateapipb.Volume{{Name: apiservice.GuestVolumeName, Image: &ateapipb.ImageVolumeSource{Reference: guestImage}}}
	derived.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
		GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-env", Name: "golden-1"},
	}}
	ctrl.AddTemplate(derived)
	ctrl.AddTag(&ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "golden-1"},
		SourceActor: &ateapipb.ObjectRef{Atespace: "ate-env", Name: "golden-7f3e"},
	})
	selector := map[string]string{workerPoolLabel: poolName}
	createActor(t, ctrl, "env-1", "default-template-1a2b3c4d5e6f", selector)
	createActor(t, ctrl, "env-2", "default-template", nil)
	ctrl.Crash("env-2", "restore failed")

	cluster := staticCluster{
		pools:       []inventory.WorkerPool{{Namespace: "ate-env", Name: poolName, Replicas: 1, ReadyReplicas: 1, SandboxClass: "gvisor"}},
		deployments: []inventory.Deployment{{Namespace: "ate-env", Name: "ate-env-api", Image: "example.com/ate-env-api:v1", Ready: 1}},
	}
	dir := t.TempDir()
	r := newReporter(t, ctrl, cluster, DirStore{Dir: dir}, 5*time.Second, time.Hour)
	if err := r.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	latest, err := os.ReadFile(filepath.Join(dir, "v1", "inventory", "p", "c", "latest.json"))
	if err != nil {
		t.Fatal(err)
	}
	snap := decode(t, latest)
	if snap.Project != "p" || snap.Cluster != "c" || snap.Location != "us-central1" {
		t.Errorf("identity = %s/%s in %s", snap.Project, snap.Cluster, snap.Location)
	}
	if want := (inventory.Reporter{Version: "v0.1.0", Commit: "abc1234", IntervalSeconds: 5}); snap.Reporter != want {
		t.Errorf("reporter = %+v, want %+v", snap.Reporter, want)
	}
	if len(snap.Atespaces) != 1 || snap.Atespaces[0].Name != "ate-env" {
		t.Errorf("atespaces = %+v", snap.Atespaces)
	}
	if len(snap.Templates) != 2 {
		t.Fatalf("templates = %+v", snap.Templates)
	}
	base, der := snap.Templates[0], snap.Templates[1]
	if base.Name != "default-template" || base.GuestImage != guestImage || base.Base != "" || base.RootfsImage != "" || base.WorkerPool != poolName || base.GoldenState != inventory.GoldenPending {
		t.Errorf("base template = %+v", base)
	}
	if der.Base != "default-template" || der.RootfsImage != rootfsImage || der.GuestImage != guestImage || der.GoldenState != inventory.GoldenReady {
		t.Errorf("derived template = %+v", der)
	}
	if len(snap.Tags) != 1 || snap.Tags[0].Name != "golden-1" || snap.Tags[0].SourceActor != "golden-7f3e" {
		t.Errorf("tags = %+v", snap.Tags)
	}
	if len(snap.Workers) != 1 {
		t.Fatalf("workers = %+v", snap.Workers)
	}
	w := snap.Workers[0]
	if w.Pool != poolName || w.Node != "node-1" || w.State != inventory.WorkerStateActive || !reflect.DeepEqual(w.Labels, selector) {
		t.Errorf("worker = %+v", w)
	}
	if len(snap.Actors) != 2 {
		t.Fatalf("actors = %+v", snap.Actors)
	}
	actors := map[string]inventory.Actor{}
	for _, a := range snap.Actors {
		actors[a.Name] = a
	}
	if a := actors["env-1"]; a.State != inventory.ActorStateRunning || a.Template != "default-template-1a2b3c4d5e6f" || a.AssignedNode != "node-1" || a.Worker != w.Name || !reflect.DeepEqual(a.WorkerSelector, selector) || a.Crash != nil {
		t.Errorf("env-1 = %+v", a)
	}
	if a := actors["env-2"]; a.State != inventory.ActorStateCrashed || a.WorkerSelector != nil || a.Crash == nil || a.Crash.Message != "restore failed" {
		t.Errorf("env-2 = %+v", a)
	}
	if !reflect.DeepEqual(snap.WorkerPools, cluster.pools) || !reflect.DeepEqual(snap.Deployments, cluster.deployments) {
		t.Errorf("workerpools = %+v, deployments = %+v", snap.WorkerPools, snap.Deployments)
	}
	if got := snap.Counts.ActorsByState; got[inventory.ActorStateRunning] != 1 || got[inventory.ActorStateCrashed] != 1 {
		t.Errorf("counts = %+v", snap.Counts)
	}

	// The first tick also keeps its snapshot, the same bytes, named by
	// generated_at.
	kept, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(KeptName("p", "c", snap.GeneratedAt))))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(kept, latest) {
		t.Error("the kept snapshot differs from latest.json")
	}
	entries, err := os.ReadDir(filepath.Join(dir, "v1", "inventory", "p", "c"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("v1/inventory/p/c holds %d entries, want latest.json alone", len(entries))
	}
}

// TestEmptyClusterIsAValidSnapshot: no pools, no deployments (nil from the
// Cluster) and no actors still encode as lists the schema accepts.
func TestEmptyClusterIsAValidSnapshot(t *testing.T) {
	store := &memStore{}
	r := newReporter(t, fakecontrol.New(), staticCluster{}, store, 5*time.Second, time.Hour)
	snap := tick(t, r, store)
	if len(snap.Actors)+len(snap.Workers)+len(snap.WorkerPools)+len(snap.Deployments)+len(snap.Atespaces) != 0 {
		t.Errorf("snapshot of an empty cluster = %+v", snap)
	}
}

// TestCatalogIsReadOnAMissAndEveryTenthTick: a template registered between
// ticks appears when an actor names it, or with the tenth tick's read.
func TestCatalogIsReadOnAMissAndEveryTenthTick(t *testing.T) {
	has := func(snap *inventory.Snapshot, name string) bool {
		for _, tmpl := range snap.Templates {
			if tmpl.Name == name {
				return true
			}
		}
		return false
	}
	ctrl := newControl(t)
	store := &memStore{}
	r := newReporter(t, ctrl, staticCluster{}, store, 5*time.Second, time.Hour)
	tick(t, r, store) // tick 1 reads the catalog

	ctrl.AddTemplate(baseTemplate("second"))
	if snap := tick(t, r, store); has(snap, "second") {
		t.Fatal("tick 2 read the catalog with no miss")
	}
	createActor(t, ctrl, "env-1", "second", nil)
	if snap := tick(t, r, store); !has(snap, "second") {
		t.Fatal("an actor of a template the catalog lacks did not force a read")
	}

	ctrl.AddTemplate(baseTemplate("third"))
	for n := 4; n <= catalogEvery; n++ {
		if snap := tick(t, r, store); has(snap, "third") {
			t.Fatalf("tick %d read the catalog with no miss", n)
		}
	}
	if snap := tick(t, r, store); !has(snap, "third") {
		t.Fatalf("tick %d did not read the catalog", catalogEvery+1)
	}
}

// TestADeletedTemplateDoesNotForceACatalogReadOnEveryTick: an actor can
// outlive its template. The read that follows the miss does not resolve it,
// so the next ticks go back to the tenth-tick cadence.
func TestADeletedTemplateDoesNotForceACatalogReadOnEveryTick(t *testing.T) {
	ctrl := newControl(t)
	createActor(t, ctrl, "orphan", "deleted-template", nil)
	store := &memStore{}
	r := newReporter(t, ctrl, staticCluster{}, store, 5*time.Second, time.Hour)
	tick(t, r, store)

	ctrl.AddTemplate(baseTemplate("second"))
	snap := tick(t, r, store)
	for _, tmpl := range snap.Templates {
		if tmpl.Name == "second" {
			t.Fatal("the orphan actor forced a catalog read on tick 2")
		}
	}
	// A new miss still forces one.
	createActor(t, ctrl, "env-1", "second", nil)
	snap = tick(t, r, store)
	if len(snap.Templates) != 2 {
		t.Fatalf("templates after a new miss = %+v", snap.Templates)
	}
}

// TestCredentialKeysStayOut: a worker label or a selector key that names a
// credential would fail Validate and with it the whole snapshot, so the
// reporter leaves the key out and keeps the rest.
func TestCredentialKeysStayOut(t *testing.T) {
	ctrl := newControl(t)
	ctrl.AddWorker(fakecontrol.Worker("ate-env", "other", "node-2", map[string]string{"example.com/api-token": "x", "zone": "a"}, 8))
	createActor(t, ctrl, "env-1", "default-template", map[string]string{"example.com/password": "x"})
	store := &memStore{}
	r := newReporter(t, ctrl, staticCluster{}, store, 5*time.Second, time.Hour)
	snap := tick(t, r, store)
	for _, w := range snap.Workers {
		if w.Pool == "other" && !reflect.DeepEqual(w.Labels, map[string]string{"zone": "a"}) {
			t.Errorf("worker labels = %v, want the zone alone", w.Labels)
		}
	}
	if got := snap.Actors[0].WorkerSelector; got != nil {
		t.Errorf("worker_selector = %v, want none", got)
	}
}

func TestTickFailsWithoutWritingWhenAReadFails(t *testing.T) {
	ctrl := newControl(t)
	ctrl.DenyListWorkers = true
	store := &memStore{}
	r := newReporter(t, ctrl, staticCluster{}, store, 5*time.Second, time.Hour)
	if err := r.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), codes.PermissionDenied.String()) {
		t.Fatalf("Tick = %v, want the PermissionDenied of ListWorkers", err)
	}
	r = newReporter(t, newControl(t), staticCluster{err: errors.New("cache gone")}, store, 5*time.Second, time.Hour)
	if err := r.Tick(context.Background()); err == nil || !strings.Contains(err.Error(), "cache gone") {
		t.Fatalf("Tick = %v, want the Cluster's error", err)
	}
	if len(store.names) != 0 {
		t.Errorf("a failed tick wrote %v", store.names)
	}
}

// TestRunKeepsASnapshotEverySnapshotInterval: latest.json on every tick, a
// kept snapshot on the first tick and then every SnapshotInterval.
func TestRunKeepsASnapshotEverySnapshotInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &memStore{}
		r := newReporter(t, newControl(t), staticCluster{}, store, 5*time.Second, 15*time.Second)
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now()
		go r.Run(ctx)
		time.Sleep(31 * time.Second)
		cancel()
		synctest.Wait()

		if got := len(store.latestTimes()); got != 7 {
			t.Errorf("%d latest.json writes in 31 s at 5 s, want 7", got)
		}
		var want []string
		for _, s := range []time.Duration{0, 15 * time.Second, 30 * time.Second} {
			want = append(want, KeptName("p", "c", start.Add(s)))
		}
		if got := store.kept(); !reflect.DeepEqual(got, want) {
			t.Errorf("kept snapshots = %v, want %v", got, want)
		}
	})
}

// TestRunKeepsEveryTickWhenTheIntervalsAreEqual: -interval 5s
// -snapshot-interval 5s keeps one file per tick, the configuration FS-14 of
// the fleet site plan asks for.
func TestRunKeepsEveryTickWhenTheIntervalsAreEqual(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &memStore{}
		r := newReporter(t, newControl(t), staticCluster{}, store, 5*time.Second, 5*time.Second)
		ctx, cancel := context.WithCancel(t.Context())
		go r.Run(ctx)
		time.Sleep(11 * time.Second)
		cancel()
		synctest.Wait()
		if got := len(store.kept()); got != 3 {
			t.Errorf("%d kept snapshots after 3 ticks, want 3: %v", got, store.kept())
		}
	})
}

// TestRunBacksOffWhileTicksFailAndRecovers: the wait doubles with each
// failure, readiness follows the last tick, and the first success returns
// to the interval.
func TestRunBacksOffWhileTicksFailAndRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &memStore{fail: 3}
		r := newReporter(t, newControl(t), staticCluster{}, store, 5*time.Second, time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now()
		go r.Run(ctx)

		// Ticks at 0, 10 and 30 s fail (waits of 10, 20 and 40 s).
		time.Sleep(31 * time.Second)
		if r.Ready() {
			t.Error("ready while every tick failed")
		}
		// The tick at 70 s succeeds; the next come 5 s apart.
		time.Sleep(50 * time.Second)
		if !r.Ready() {
			t.Error("not ready after a tick wrote its snapshot")
		}
		cancel()
		synctest.Wait()

		var got []time.Duration
		for _, at := range store.latestTimes() {
			got = append(got, at.Sub(start))
		}
		if want := []time.Duration{70 * time.Second, 75 * time.Second, 80 * time.Second}; !reflect.DeepEqual(got, want) {
			t.Errorf("latest.json written at %v, want %v", got, want)
		}
	})
}

// hangingCluster's WorkerPools blocks until its context is done, as a
// Kubernetes API that does not answer; starts holds when each call began.
type hangingCluster struct {
	mu     sync.Mutex
	starts []time.Time
}

func (c *hangingCluster) WorkerPools(ctx context.Context) ([]inventory.WorkerPool, error) {
	c.mu.Lock()
	c.starts = append(c.starts, time.Now())
	c.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *hangingCluster) Deployments(context.Context) ([]inventory.Deployment, error) {
	return []inventory.Deployment{}, nil
}

// TestRunWaitsTheBackoffAfterATickThatTimedOut: a tick that fails at the
// tick timeout is followed by the whole backoff wait. Measured from the
// tick's start the wait would be over already, and the ticks would run back
// to back.
func TestRunWaitsTheBackoffAfterATickThatTimedOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cluster := &hangingCluster{}
		r := newReporter(t, newControl(t), cluster, &memStore{}, 5*time.Second, time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now()
		go r.Run(ctx)

		time.Sleep(4 * time.Minute)
		cancel()
		synctest.Wait()

		var got []time.Duration
		cluster.mu.Lock()
		for _, at := range cluster.starts {
			got = append(got, at.Sub(start))
		}
		cluster.mu.Unlock()
		// Each tick hangs for tickTimeout (60 s); the waits after are 10, 20
		// and 40 s.
		want := []time.Duration{0, 70 * time.Second, 150 * time.Second}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("ticks started at %v, want %v", got, want)
		}
		if r.Ready() {
			t.Error("ready while every tick timed out")
		}
	})
}

func TestBackoffDoublesUpToTheCap(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		failures int
		want     time.Duration
	}{
		{5 * time.Second, 1, 10 * time.Second},
		{5 * time.Second, 3, 40 * time.Second},
		{5 * time.Second, 4, time.Minute},
		{5 * time.Second, 50, time.Minute},
		{2 * time.Minute, 1, 2 * time.Minute},
	} {
		if got := backoff(tc.interval, tc.failures); got != tc.want {
			t.Errorf("backoff(%s, %d) = %s, want %s", tc.interval, tc.failures, got, tc.want)
		}
	}
}

func TestNewRejects(t *testing.T) {
	ok := Config{Project: "p", Cluster: "c", Version: "v1", Interval: 5 * time.Second, SnapshotInterval: time.Hour}
	for name, edit := range map[string]func(*Config){
		"project":           func(c *Config) { c.Project = "" },
		"cluster":           func(c *Config) { c.Cluster = "" },
		"version":           func(c *Config) { c.Version = "" },
		"interval":          func(c *Config) { c.Interval = 500 * time.Millisecond },
		"snapshot interval": func(c *Config) { c.SnapshotInterval = time.Second },
		// Project and cluster are one path segment each of the object name.
		"project with a slash":     func(c *Config) { c.Project = "projects/my-proj" },
		"project of dots":          func(c *Config) { c.Project = ".." },
		"project that climbs":      func(c *Config) { c.Project = "../../../.." },
		"project with a backslash": func(c *Config) { c.Project = `a\b` },
		"cluster with a slash":     func(c *Config) { c.Cluster = "us-central1/c1" },
		"cluster of one dot":       func(c *Config) { c.Cluster = "." },
		"cluster of dots":          func(c *Config) { c.Cluster = ".." },
	} {
		cfg := ok
		edit(&cfg)
		if _, err := New(cfg, nil, nil, nil); err == nil {
			t.Errorf("%s: New accepted %+v", name, cfg)
		}
	}
}

func TestDirStoreReplacesTheObjectAndLeavesNoTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	store := DirStore{Dir: dir}
	for _, content := range []string{"one", "two"} {
		if err := store.Write(context.Background(), "v1/inventory/p/c/latest.json", []byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(dir, "v1", "inventory", "p", "c"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "latest.json" {
		t.Fatalf("directory holds %v", entries)
	}
	got, err := os.ReadFile(filepath.Join(dir, "v1", "inventory", "p", "c", "latest.json"))
	if err != nil || string(got) != "two" {
		t.Fatalf("latest.json = %q, %v", got, err)
	}
}

// TestDirStoreRefusesANameOutsideTheDirectory: a name that is absolute or
// climbs out of Dir writes nothing, there or anywhere else.
func TestDirStoreRefusesANameOutsideTheDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "out")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"../escaped.json",
		"v1/inventory/../../../escaped.json",
		LatestName("../../../..", "escaped"),
		"/escaped.json",
		"",
		".",
	} {
		if err := (DirStore{Dir: dir}).Write(context.Background(), name, []byte("x")); err == nil {
			t.Errorf("Write(%q) = nil, want an error", name)
		}
	}
	for _, d := range []string{root, dir} {
		entries, err := os.ReadDir(d)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.Name() != "out" {
				t.Errorf("%s holds %s after refused writes", d, e.Name())
			}
		}
	}
}

func TestGuestVolumeNameMatchesAPIService(t *testing.T) {
	if guestVolumeName != apiservice.GuestVolumeName {
		t.Fatalf("guestVolumeName = %q, apiservice.GuestVolumeName = %q", guestVolumeName, apiservice.GuestVolumeName)
	}
}
