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
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"google.golang.org/api/iterator"
)

// runsPrefix holds the trainers' run folders in the inventory bucket:
// v1/runs/<run_id>/ (docs/plans/sandbox-fleet-site/interfaces.md).
const runsPrefix = "v1/runs/"

// RunsCacheTTL is how long one listing of the run folders serves requests.
const RunsCacheTTL = time.Minute

// maxRunsListed bounds the folders listed per source.
const maxRunsListed = 500

// maxRunFiles bounds the files of one run folder.
const maxRunFiles = 200

// maxRunFileBytes bounds one file the site reads or serves from a run.
const maxRunFileBytes = 64 << 20

// runID matches a trainer run id: one path segment.
var runID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// runFileName matches a file name in a run folder: one path segment.
var runFileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$`)

// RunFile is one object of a run folder.
type RunFile struct {
	Name    string    `json:"name"`
	Size    int64     `json:"size"`
	Updated time.Time `json:"updated"`
}

// RunStore lists and reads run folders. Bucket "" is the inventory bucket;
// another is a benchmark bucket of FLEET_BENCH_BUCKETS. It only reads.
type RunStore interface {
	// Folders returns the folder names directly under prefix.
	Folders(ctx context.Context, bucket, prefix string) ([]string, error)
	// Files returns the objects directly in folder, a full prefix without
	// its trailing slash.
	Files(ctx context.Context, bucket, folder string) ([]RunFile, error)
	// Read returns an object, refusing one larger than limit bytes.
	Read(ctx context.Context, bucket, name string, limit int) ([]byte, error)
}

// BucketRuns reads run folders from Cloud Storage with the site's
// credentials.
type BucketRuns struct {
	Client    *storage.Client
	Inventory string // the inventory bucket, for bucket ""
}

func (s BucketRuns) bucket(b string) *storage.BucketHandle {
	if b == "" {
		b = s.Inventory
	}
	return s.Client.Bucket(b)
}

func (s BucketRuns) Folders(ctx context.Context, bucket, prefix string) ([]string, error) {
	q := &storage.Query{Prefix: prefix, Delimiter: "/"}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, err
	}
	var out []string
	it := s.bucket(bucket).Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("listing %s in %q: %w", prefix, bucket, err)
		}
		if attrs.Prefix != "" {
			out = append(out, strings.TrimSuffix(strings.TrimPrefix(attrs.Prefix, prefix), "/"))
		}
	}
}

func (s BucketRuns) Files(ctx context.Context, bucket, folder string) ([]RunFile, error) {
	q := &storage.Query{Prefix: folder + "/", Delimiter: "/"}
	if err := q.SetAttrSelection([]string{"Name", "Size", "Updated"}); err != nil {
		return nil, err
	}
	var out []RunFile
	it := s.bucket(bucket).Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("listing %s in %q: %w", folder, bucket, err)
		}
		// A name ending in "/" is a folder placeholder, not a file.
		if attrs.Name != "" && !strings.HasSuffix(attrs.Name, "/") {
			out = append(out, RunFile{Name: path.Base(attrs.Name), Size: attrs.Size, Updated: attrs.Updated})
		}
	}
}

func (s BucketRuns) Read(ctx context.Context, bucket, name string, limit int) ([]byte, error) {
	r, err := s.bucket(bucket).Object(name).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}
	defer r.Close()
	return readLimited(r, name, limit)
}

// errOutside is the error of a name that a symbolic link takes out of the
// local directory (-dir); a listing treats it as no folder.
var errOutside = errors.New("leaves the local directory")

// DirRuns reads run folders under a local directory: the inventory bucket's
// layout under Dir, and a benchmark bucket b under Dir/b, for a local run.
// Nothing outside Dir is listed or read, also through a symbolic link: a
// benchmark bucket's directory that is a link out of Dir lists as empty.
type DirRuns struct{ Dir string }

// local maps an object name of a bucket to a path under Dir, refusing a
// bucket that is not one path element, a name that is not local, and a path
// that a symbolic link takes out of Dir. name "" is the bucket's root. A path
// that does not exist is returned as is, for the caller's fs.ErrNotExist.
func (s DirRuns) local(bucket, name string) (string, error) {
	if bucket != "" && (!filepath.IsLocal(bucket) || filepath.Base(bucket) != bucket) {
		return "", fmt.Errorf("%q is not a bucket directory name", bucket)
	}
	if name != "" && !filepath.IsLocal(filepath.FromSlash(name)) {
		return "", fmt.Errorf("%q is not a name under the bucket", name)
	}
	p := filepath.Join(s.Dir, bucket, filepath.FromSlash(name))
	resolved, err := filepath.EvalSymlinks(p)
	if errors.Is(err, fs.ErrNotExist) {
		return p, nil
	}
	if err != nil {
		return "", err
	}
	root, err := filepath.EvalSymlinks(s.Dir)
	if err != nil {
		return "", err
	}
	if inner, err := filepath.Rel(root, resolved); err != nil || (inner != "." && !filepath.IsLocal(inner)) {
		return "", fmt.Errorf("%q in %q: %w", name, bucket, errOutside)
	}
	return resolved, nil
}

func (s DirRuns) Folders(_ context.Context, bucket, prefix string) ([]string, error) {
	dir, err := s.local(bucket, strings.TrimSuffix(prefix, "/"))
	if errors.Is(err, errOutside) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func (s DirRuns) Files(_ context.Context, bucket, folder string) ([]RunFile, error) {
	dir, err := s.local(bucket, folder)
	if errors.Is(err, errOutside) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []RunFile
	for _, e := range entries {
		// Regular files only: a symbolic link could point out of the dir.
		if !e.Type().IsRegular() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		out = append(out, RunFile{Name: e.Name(), Size: info.Size(), Updated: info.ModTime()})
	}
	return out, nil
}

func (s DirRuns) Read(_ context.Context, bucket, name string, limit int) ([]byte, error) {
	p, err := s.local(bucket, name)
	if err != nil {
		return nil, err
	}
	if info, err := os.Lstat(p); err != nil {
		return nil, err
	} else if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%q is not a regular file", name)
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readLimited(f, name, limit)
}

// RunEntry is one run of the index: a trainer run folder in the inventory
// bucket, or a benchmark bundle in a benchmark bucket.
type RunEntry struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`             // "trainer" or "benchmark"
	Bucket string `json:"bucket,omitempty"` // the benchmark bucket
	Traced bool   `json:"traced,omitempty"` // a bundle folder with the _traced suffix
	// Path is the run's place in the site's URLs: trainer/<id> or
	// bench/<bucket>/<id>, each segment escaped.
	Path string `json:"path"`
}

// Run kinds.
const (
	RunTrainer   = "trainer"
	RunBenchmark = "benchmark"
)

func trainerRun(id string) RunEntry {
	return RunEntry{ID: id, Kind: RunTrainer, Path: "trainer/" + url.PathEscape(id)}
}

func benchRun(bucket, id string, traced bool) RunEntry {
	return RunEntry{ID: id, Kind: RunBenchmark, Bucket: bucket, Traced: traced, Path: "bench/" + url.PathEscape(bucket) + "/" + url.PathEscape(id)}
}

// RunIndex is the listing of every source.
type RunIndex struct {
	Runs []RunEntry `json:"runs"`
	// Truncated names the sources that had more than maxRunsListed folders.
	Truncated []string `json:"truncated,omitempty"`
	// Errors names the sources that could not be listed; the others still
	// show.
	Errors []string `json:"errors,omitempty"`
}

// Runs lists and finds the run folders of the inventory bucket and the
// benchmark buckets. The index is cached RunsCacheTTL per instance.
type Runs struct {
	store   RunStore
	buckets []string
	now     func() time.Time
	// timeout bounds one listing: refreshTimeout, shorter than the
	// request's requestTimeout, so a listing that runs out still answers
	// the request that waits for it, with the sources it could not list.
	timeout time.Duration

	mu    sync.Mutex
	at    time.Time // when the cached index's listing ended
	index RunIndex  // the cached index
	// last is the result of the latest listing, cached or not: a listing
	// whose own timeout expired is handed to the requests that waited for
	// it, and not cached.
	last RunIndex
	// started and completed count listings; running is closed when the
	// listing in flight ends, nil when none runs.
	started, completed int
	running            chan struct{}
}

// NewRuns returns the run listing over store and the benchmark buckets.
// now nil means time.Now.
func NewRuns(store RunStore, benchBuckets []string, now func() time.Time) *Runs {
	if now == nil {
		now = time.Now
	}
	return &Runs{store: store, buckets: benchBuckets, now: now, timeout: refreshTimeout}
}

// Index returns every run: trainer runs, then bundles per bucket, each
// sorted by name, descending, at most maxRunsListed per source. Bundle names
// start with their date, so they list newest first; a trainer run id need
// not sort by time. A source that cannot be listed is an entry of Errors,
// cached like the rest.
//
// The listing runs apart from the request that started it, under
// refreshTimeout (r.timeout), so a closed tab neither fails it for the other requests
// nor caches its cancellation. Concurrent requests share one listing; a
// forced one waits for a listing that starts after it. A listing whose own
// timeout expired is returned but not cached. The error is the caller's
// ctx.Err() when it stops waiting.
func (r *Runs) Index(ctx context.Context, force bool) (RunIndex, error) {
	r.mu.Lock()
	if !force && !r.at.IsZero() && r.now().Sub(r.at) < RunsCacheTTL {
		defer r.mu.Unlock()
		return r.index, nil
	}
	// The listing this request needs: one that starts after it when
	// forced, else the next one to end.
	target := r.completed + 1
	if force {
		target = r.started + 1
	}
	for r.completed < target {
		if r.running == nil {
			r.started++
			r.running = make(chan struct{})
			go r.refresh(context.WithoutCancel(ctx), r.running)
		}
		running := r.running
		r.mu.Unlock()
		select {
		case <-running:
		case <-ctx.Done():
			return RunIndex{}, ctx.Err()
		}
		r.mu.Lock()
	}
	defer r.mu.Unlock()
	return r.last, nil
}

// refresh lists every source and closes done. A listing whose context
// expired is not cached: the next request lists again.
func (r *Runs) refresh(base context.Context, done chan struct{}) {
	ctx, cancel := context.WithTimeout(base, r.timeout)
	defer cancel()
	idx := r.list(ctx)
	expired := ctx.Err() != nil
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last = idx
	if !expired {
		// The TTL counts from the end of the listing.
		r.at, r.index = r.now(), idx
	}
	r.completed++
	r.running = nil
	close(done)
}

// list reads every source once.
func (r *Runs) list(ctx context.Context) RunIndex {
	var idx RunIndex
	source := func(bucket, prefix, label string, entry func(string) (RunEntry, bool)) {
		names, err := r.store.Folders(ctx, bucket, prefix)
		if err != nil {
			log.Printf("fleetsite: listing %s: %v", label, err)
			idx.Errors = append(idx.Errors, label)
			return
		}
		var runs []RunEntry
		for _, n := range names {
			if e, ok := entry(n); ok {
				runs = append(runs, e)
			}
		}
		sort.Slice(runs, func(i, j int) bool { return runs[i].ID > runs[j].ID })
		if len(runs) > maxRunsListed {
			runs = runs[:maxRunsListed]
			idx.Truncated = append(idx.Truncated, label)
		}
		idx.Runs = append(idx.Runs, runs...)
	}
	source("", runsPrefix, "gs://<inventory>/"+runsPrefix, func(n string) (RunEntry, bool) {
		return trainerRun(n), runID.MatchString(n)
	})
	for _, b := range r.buckets {
		source(b, "", "gs://"+b+"/", func(n string) (RunEntry, bool) {
			traced, err := schema.CheckFolderName(n)
			return benchRun(b, n, traced), err == nil && runID.MatchString(n)
		})
	}
	return idx
}

// Find returns a run by its source and id, read from the store, not from the
// capped index, with its files as Files lists them: kind RunTrainer, or
// RunBenchmark with a configured bucket. ok is false when the id is not a
// run's or its folder has no file.
func (r *Runs) Find(ctx context.Context, kind, bucket, id string) (RunEntry, []RunFile, bool, error) {
	if !runID.MatchString(id) {
		return RunEntry{}, nil, false, nil
	}
	var e RunEntry
	switch kind {
	case RunTrainer:
		e = trainerRun(id)
	case RunBenchmark:
		known := false
		for _, b := range r.buckets {
			known = known || b == bucket
		}
		traced, err := schema.CheckFolderName(id)
		if !known || err != nil {
			return RunEntry{}, nil, false, nil
		}
		e = benchRun(bucket, id, traced)
	default:
		return RunEntry{}, nil, false, nil
	}
	files, err := r.Files(ctx, e)
	if err != nil {
		return RunEntry{}, nil, false, err
	}
	return e, files, len(files) > 0, nil
}

// folder returns the object prefix of a run, without its trailing slash.
func (e RunEntry) folder() string {
	if e.Kind == RunTrainer {
		return runsPrefix + e.ID
	}
	return e.ID
}

// Files lists a run's files, at most maxRunFiles, by name.
func (r *Runs) Files(ctx context.Context, e RunEntry) ([]RunFile, error) {
	files, err := r.store.Files(ctx, e.Bucket, e.folder())
	if err != nil {
		return nil, err
	}
	var out []RunFile
	for _, f := range files {
		if runFileName.MatchString(f.Name) {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) > maxRunFiles {
		out = out[:maxRunFiles]
	}
	return out, nil
}

// ReadFile reads one file of a run; name must be a file name of its listing.
func (r *Runs) ReadFile(ctx context.Context, e RunEntry, name string, limit int) ([]byte, error) {
	if !runFileName.MatchString(name) {
		return nil, fmt.Errorf("%q is not a run file name", name)
	}
	return r.store.Read(ctx, e.Bucket, e.folder()+"/"+name, limit)
}
