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
	"errors"
	"reflect"
	"testing"

	"github.com/agent-substrate/env/internal/inventory"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	atefake "go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func apiDeployment(args ...string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-env", Name: "ate-env-api"},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: apiContainerName, Image: "example.com/ate-env-api:v1", Args: args}},
		}}},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 2},
	}
}

func TestDeploymentOf(t *testing.T) {
	plain := inventory.Deployment{Namespace: "ate-env", Name: "ate-env-api", Image: "example.com/ate-env-api:v1", Ready: 2}

	if got, ok := deploymentOf(apiDeployment("-listen", "0.0.0.0:7777")); !ok || got != plain {
		t.Errorf("plain = %+v, %v", got, ok)
	}

	for _, arg := range []string{"-enable-tracing", "--enable-tracing", "-enable-tracing=true", "-enable-tracing=1"} {
		if got, _ := deploymentOf(apiDeployment(arg)); !got.Tracing {
			t.Errorf("%s: tracing not detected", arg)
		}
	}
	if got, _ := deploymentOf(apiDeployment("-enable-tracing=false")); got.Tracing {
		t.Error("-enable-tracing=false read as tracing")
	}
	// What Go's flag package does not read as the flag: an argument after
	// the "--" terminator, and more than two dashes.
	for _, args := range [][]string{
		{"--", "-enable-tracing"},
		{"-listen", "0.0.0.0:7777", "--", "--enable-tracing=true"},
		{"---enable-tracing"},
		{"enable-tracing"},
	} {
		if got, _ := deploymentOf(apiDeployment(args...)); got.Tracing {
			t.Errorf("%q read as tracing", args)
		}
	}
	if got, _ := deploymentOf(apiDeployment("-enable-tracing", "--", "-enable-tracing=false")); !got.Tracing {
		t.Error("the flag before -- was not read")
	}

	full := apiDeployment("-metrics-listen-addr=:9090")
	full.Labels = map[string]string{versionLabel: "v0.1.0"}
	full.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 7777}, {Name: metricsPortName, ContainerPort: 9090}}
	want := plain
	want.Version, want.MetricsPort = "v0.1.0", 9090
	if got, _ := deploymentOf(full); got != want {
		t.Errorf("full = %+v, want %+v", got, want)
	}

	other := apiDeployment()
	other.Spec.Template.Spec.Containers[0].Name = "nginx"
	if _, ok := deploymentOf(other); ok {
		t.Error("a Deployment without the ate-env-api container was taken")
	}
}

func TestWorkerPoolOf(t *testing.T) {
	pool := &atev1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-env", Name: "pool"},
		Spec: atev1.WorkerPoolSpec{
			Replicas:    4,
			WorkerImage: "example.com/ateworker-gvisor:v1",
			SandboxClasses: []atev1.WorkerPoolSandboxClass{{
				Name:      atev1.SandboxClassGvisor,
				ConfigRef: &atev1.SandboxConfigReference{Name: "gvisor-default"},
			}},
		},
		Status: atev1.WorkerPoolStatus{Replicas: 4, ReadyReplicas: 3},
	}
	want := inventory.WorkerPool{
		Namespace: "ate-env", Name: "pool", Replicas: 4, ReadyReplicas: 3,
		SandboxClass: string(atev1.SandboxClassGvisor), SandboxConfig: "gvisor-default", WorkerImage: "example.com/ateworker-gvisor:v1",
	}
	if got := workerPoolOf(pool); got != want {
		t.Errorf("workerPoolOf = %+v, want %+v", got, want)
	}
	pool.Spec.SandboxClasses = nil
	if got := workerPoolOf(pool); got.SandboxClass != "" || got.SandboxConfig != "" {
		t.Errorf("a pool without a sandbox class = %+v", got)
	}
}

func TestTemplateOf(t *testing.T) {
	guestVolume := []*ateapipb.Volume{{Name: guestVolumeName, Image: &ateapipb.ImageVolumeSource{Reference: guestImage}}}
	for _, tc := range []struct {
		name   string
		tmpl   *ateapipb.ActorTemplate
		base   string
		rootfs string
		guest  string
		pool   string
		golden string
	}{
		{
			name: "guest as the container image",
			tmpl: &ateapipb.ActorTemplate{
				Metadata:       &ateapipb.ResourceMetadata{Atespace: "a", Name: "default-template"},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{workerPoolLabel: "pool"}},
				Containers:     []*ateapipb.Container{{Image: guestImage}},
			},
			guest: guestImage, pool: "pool", golden: inventory.GoldenPending,
		},
		{
			name: "derived for a rootfs image",
			tmpl: &ateapipb.ActorTemplate{
				Metadata:   &ateapipb.ResourceMetadata{Atespace: "a", Name: "default-template-1a2b3c4d5e6f"},
				Containers: []*ateapipb.Container{{Image: rootfsImage}},
				Volumes:    guestVolume,
			},
			base: "default-template", rootfs: rootfsImage, guest: guestImage, golden: inventory.GoldenPending,
		},
		{
			name: "guest volume, a name of its own",
			tmpl: &ateapipb.ActorTemplate{
				Metadata:   &ateapipb.ResourceMetadata{Atespace: "a", Name: "python"},
				Containers: []*ateapipb.Container{{Image: rootfsImage}},
				Volumes:    guestVolume,
			},
			rootfs: rootfsImage, guest: guestImage, golden: inventory.GoldenPending,
		},
		{
			name: "golden snapshots disabled",
			tmpl: &ateapipb.ActorTemplate{
				Metadata:   &ateapipb.ResourceMetadata{Atespace: "a", Name: "t"},
				Containers: []*ateapipb.Container{{Image: guestImage}},
				SnapshotConfig: &ateapipb.SnapshotConfig{GoldenSnapshotConfig: &ateapipb.GoldenSnapshotConfig{
					Mode: ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED,
				}},
			},
			guest: guestImage, golden: inventory.GoldenNone,
		},
		{
			name: "golden snapshot failed",
			tmpl: &ateapipb.ActorTemplate{
				Metadata:   &ateapipb.ResourceMetadata{Atespace: "a", Name: "t"},
				Containers: []*ateapipb.Container{{Image: guestImage}},
				Status:     &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{ErrorMessage: "ProbeFailed: timeout"}},
			},
			guest: guestImage, golden: inventory.GoldenFailed,
		},
	} {
		got := templateOf(tc.tmpl)
		if got.Base != tc.base || got.RootfsImage != tc.rootfs || got.GuestImage != tc.guest || got.WorkerPool != tc.pool || got.GoldenState != tc.golden {
			t.Errorf("%s: templateOf = %+v", tc.name, got)
		}
	}
}

// TestActorOfCrashWithoutATime: a crash record without crash_time takes the
// actor's update time, so the record survives Validate.
func TestActorOfCrashWithoutATime(t *testing.T) {
	updated := timestamppb.Now()
	got := actorOf(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "n", Uid: "u", CreateTime: updated, UpdateTime: updated},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: "boom"},
		},
	}, func(string) bool { return true })
	if got.Crash == nil || !got.Crash.CrashTime.Equal(updated.AsTime()) {
		t.Errorf("crash = %+v, want the update time %s", got.Crash, updated.AsTime())
	}
}

// TestKubeClusterListsPoolsAndAPIDeploymentsOfEveryNamespace reads the fake
// clientsets, and a list the API refuses is the caller's error.
func TestKubeClusterListsPoolsAndAPIDeploymentsOfEveryNamespace(t *testing.T) {
	second := apiDeployment()
	second.Namespace = "team-b"
	other := apiDeployment()
	other.Name = "web"
	other.Spec.Template.Spec.Containers[0].Name = "nginx"
	kube := kubefake.NewClientset(apiDeployment(), second, other)
	substrate := atefake.NewSimpleClientset(
		&atev1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "ate-env", Name: "pool"}, Spec: atev1.WorkerPoolSpec{Replicas: 2}},
		&atev1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "pool"}, Spec: atev1.WorkerPoolSpec{Replicas: 3}},
	)
	c := KubeCluster{Kube: kube, Substrate: substrate}

	pools, err := c.WorkerPools(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	replicas := map[string]int32{}
	for _, p := range pools {
		replicas[p.Namespace+"/"+p.Name] = p.Replicas
	}
	if want := map[string]int32{"ate-env/pool": 2, "team-b/pool": 3}; !reflect.DeepEqual(replicas, want) {
		t.Errorf("pools = %v, want %v", replicas, want)
	}

	deployments, err := c.Deployments(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	namespaces := map[string]bool{}
	for _, d := range deployments {
		namespaces[d.Namespace] = true
		if d.Name != "ate-env-api" {
			t.Errorf("took Deployment %s/%s", d.Namespace, d.Name)
		}
	}
	if want := map[string]bool{"ate-env": true, "team-b": true}; !reflect.DeepEqual(namespaces, want) {
		t.Errorf("deployments in %v, want %v", namespaces, want)
	}

	forbidden := func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	}
	kube.PrependReactor("list", "deployments", forbidden)
	substrate.PrependReactor("list", "workerpools", forbidden)
	if _, err := c.Deployments(t.Context()); err == nil {
		t.Error("Deployments returned no error for a refused list")
	}
	if _, err := c.WorkerPools(t.Context()); err == nil {
		t.Error("WorkerPools returned no error for a refused list")
	}
}
