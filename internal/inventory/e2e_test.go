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

package inventory

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestE2ESnapshotDir accepts the output of a reporter that ran with -out-dir
// against a real cluster (FS-14 of the fleet site plan). hack/ci/e2e.py runs
// it after the reporter stops, with the directory, the identity and the
// counts kubectl-ate printed. It is in this package, not next to the
// reporter, because the job compiles it for this one run: this package's
// test binary has about 340 packages, the reporter's about 1,140 (the
// Kubernetes and Cloud Storage clients and their fakes). The object names
// are written out here for that reason; internal/reporter's LatestName and
// KeptName build them, and interfaces.md of the fleet site plan fixes them.
//
//	ATE_ENV_REPORTER_E2E_DIR      the -out-dir
//	ATE_ENV_REPORTER_E2E_PROJECT  the -project
//	ATE_ENV_REPORTER_E2E_CLUSTER  the -cluster
//	ATE_ENV_REPORTER_E2E_ACTORS   actors of all atespaces
//	ATE_ENV_REPORTER_E2E_WORKERS  workers
func TestE2ESnapshotDir(t *testing.T) {
	dir := os.Getenv("ATE_ENV_REPORTER_E2E_DIR")
	if dir == "" {
		t.Skip("ATE_ENV_REPORTER_E2E_DIR is not set")
	}
	project, cluster := os.Getenv("ATE_ENV_REPORTER_E2E_PROJECT"), os.Getenv("ATE_ENV_REPORTER_E2E_CLUSTER")
	count := func(name string) int {
		n, err := strconv.Atoi(os.Getenv(name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return n
	}
	wantActors, wantWorkers := count("ATE_ENV_REPORTER_E2E_ACTORS"), count("ATE_ENV_REPORTER_E2E_WORKERS")

	decode := func(path string) *Snapshot {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		snap, err := DecodeSnapshot(f)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if snap.Project != project || snap.Cluster != cluster {
			t.Fatalf("%s is of %s/%s, want %s/%s", path, snap.Project, snap.Cluster, project, cluster)
		}
		return snap
	}

	latest := decode(filepath.Join(dir, "v1", "inventory", project, cluster, "latest.json"))
	if len(latest.Actors) != wantActors || len(latest.Workers) != wantWorkers {
		t.Errorf("latest.json holds %d actors and %d workers, kubectl-ate lists %d and %d",
			len(latest.Actors), len(latest.Workers), wantActors, wantWorkers)
	}
	if len(latest.Atespaces) == 0 || len(latest.Templates) == 0 || len(latest.WorkerPools) == 0 || len(latest.Deployments) == 0 {
		t.Errorf("latest.json holds %d atespaces, %d templates, %d WorkerPools and %d ate-env-api Deployments; want at least one of each",
			len(latest.Atespaces), len(latest.Templates), len(latest.WorkerPools), len(latest.Deployments))
	}

	// The names sort by time, and generated_at advances from one to the next.
	kept, err := filepath.Glob(filepath.Join(dir, "v1", "snapshots", project, cluster, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) < 3 {
		t.Fatalf("%d kept snapshots, want at least 3: %v", len(kept), kept)
	}
	for i := 1; i < len(kept); i++ {
		if prev, next := decode(kept[i-1]), decode(kept[i]); !next.GeneratedAt.After(prev.GeneratedAt) {
			t.Errorf("%s (generated_at %s) does not follow %s (%s)", kept[i], next.GeneratedAt, kept[i-1], prev.GeneratedAt)
		}
	}
}
