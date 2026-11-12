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

package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/inventory"
	"github.com/agent-substrate/env/internal/reporter"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	atefake "go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// TestRunWritesSnapshotsOverGRPC runs the command's wiring: the control plane
// over gRPC and TLS, the Kubernetes lists, the directory store and the identity from
// the flags. It stops when the context is done.
func TestRunWritesSnapshotsOverGRPC(t *testing.T) {
	ctrl := fakecontrol.New()
	if _, err := ctrl.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "ate-env"}},
	}); err != nil {
		t.Fatal(err)
	}
	ctrl.AddWorker(fakecontrol.Worker("ate-env", "pool", "node-1", map[string]string{"ate-env/workerpool": "pool"}, 8))
	ctrl.AddTemplate(&ateapipb.ActorTemplate{Metadata: &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "default-template"}})
	if _, err := ctrl.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "env-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ate-env", Name: "default-template"},
	}}); err != nil {
		t.Fatal(err)
	}
	addr, stopControl, err := ctrl.Serve()
	if err != nil {
		t.Fatal(err)
	}
	defer stopControl()

	kube := kubefake.NewClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-env", Name: "ate-env-api"},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ate-env-api", Image: "example.com/ate-env-api:v1"}},
		}}},
	})
	substrate := atefake.NewSimpleClientset(&atev1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-env", Name: "pool"},
		Spec:       atev1.WorkerPoolSpec{Replicas: 1},
	})

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, kube, substrate, config{
			outDir: dir, project: "kind", cluster: "kind", location: "local",
			interval: time.Second, snapshotInterval: time.Second,
			ateapi: addr, skipVerify: true, healthAddr: "127.0.0.1:0",
		})
	}()

	latest := filepath.Join(dir, filepath.FromSlash(reporter.LatestName("kind", "kind")))
	var data []byte
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if data, err = os.ReadFile(latest); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("run returned before a snapshot: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s after 30 s", latest)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run = %v, want nil when the context is done", err)
	}

	snap, err := inventory.DecodeSnapshot(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if snap.Project != "kind" || snap.Cluster != "kind" || snap.Reporter.IntervalSeconds != 1 {
		t.Errorf("identity = %s/%s, interval %d s", snap.Project, snap.Cluster, snap.Reporter.IntervalSeconds)
	}
	if len(snap.Actors) != 1 || len(snap.Workers) != 1 || len(snap.WorkerPools) != 1 || len(snap.Deployments) != 1 {
		t.Errorf("snapshot holds %d actors, %d workers, %d pools, %d deployments; want one of each",
			len(snap.Actors), len(snap.Workers), len(snap.WorkerPools), len(snap.Deployments))
	}
}

// TestReportAndServeStopsTheReportWhenTheServerFails: when the health server
// fails, reportAndServe returns only after the report loop has returned, so
// the caller closes the clients with no tick in flight and nothing leaks
// while the caller's context stays open.
func TestReportAndServeStopsTheReportWhenTheServerFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Serve on a closed listener fails at once.
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	var returned atomic.Bool
	report := func(ctx context.Context) {
		<-ctx.Done()
		// A tick that is cut takes a moment to return.
		time.Sleep(50 * time.Millisecond)
		returned.Store(true)
	}

	// The caller's context stays open: only the server's failure ends it.
	err = reportAndServe(t.Context(), report, &http.Server{Handler: healthHandler(func() bool { return true })}, ln)
	if err == nil || !strings.Contains(err.Error(), "health server") {
		t.Fatalf("reportAndServe = %v, want the health server's error", err)
	}
	if !returned.Load() {
		t.Error("reportAndServe returned while the report loop still ran")
	}
}

func TestIdentity(t *testing.T) {
	onGKE := func() bool { return true }
	offGKE := func() bool { return false }
	projectID := func(context.Context) (string, error) { return "meta-project", nil }
	attribute := func(_ context.Context, key string) (string, error) {
		switch key {
		case "cluster-name":
			return "meta-cluster", nil
		case "cluster-location":
			return "us-central1", nil
		}
		return "", errors.New("no attribute " + key)
	}
	unreachable := func(context.Context, string) (string, error) { return "", errors.New("metadata server unreachable") }

	for _, tc := range []struct {
		name    string
		in      config
		onGCE   func() bool
		attr    func(context.Context, string) (string, error)
		want    config
		wantErr string
	}{
		{name: "GKE fills what the flags left empty", onGCE: onGKE, attr: attribute,
			want: config{project: "meta-project", cluster: "meta-cluster", location: "us-central1"}},
		{name: "flags win on GKE", in: config{project: "p", cluster: "c"}, onGCE: onGKE, attr: attribute,
			want: config{project: "p", cluster: "c", location: "us-central1"}},
		{name: "all flags set, no metadata read", in: config{project: "p", cluster: "c", location: "l"}, onGCE: onGKE, attr: unreachable,
			want: config{project: "p", cluster: "c", location: "l"}},
		{name: "off GKE the location stays empty", in: config{project: "kind", cluster: "kind"}, onGCE: offGKE, attr: unreachable,
			want: config{project: "kind", cluster: "kind"}},
		{name: "off GKE without a cluster", in: config{project: "kind"}, onGCE: offGKE, attr: unreachable, wantErr: "-project and -cluster"},
		{name: "a VM that is not a GKE node has no location", in: config{project: "p", cluster: "c"}, onGCE: onGKE,
			attr: func(_ context.Context, key string) (string, error) { return "", metadata.NotDefinedError(key) },
			want: config{project: "p", cluster: "c"}},
		{name: "a VM that is not a GKE node needs -cluster", in: config{project: "p"}, onGCE: onGKE,
			attr:    func(_ context.Context, key string) (string, error) { return "", metadata.NotDefinedError(key) },
			wantErr: "cluster-name"},
		{name: "GKE metadata read fails", onGCE: onGKE, attr: unreachable, wantErr: "cluster-name"},
	} {
		cfg := tc.in
		err := identity(context.Background(), &cfg, tc.onGCE, projectID, tc.attr)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%s: identity = %v, want an error containing %q", tc.name, err, tc.wantErr)
			}
			continue
		}
		if err != nil || cfg != tc.want {
			t.Errorf("%s: identity = %+v, %v; want %+v", tc.name, cfg, err, tc.want)
		}
	}
}

func TestHealthHandler(t *testing.T) {
	ready := false
	h := healthHandler(func() bool { return ready })
	get := func(path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	if got := get("/healthz"); got != http.StatusOK {
		t.Errorf("/healthz = %d", got)
	}
	if got := get("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz before a snapshot = %d, want 503", got)
	}
	ready = true
	if got := get("/readyz"); got != http.StatusOK {
		t.Errorf("/readyz after a snapshot = %d, want 200", got)
	}
}
