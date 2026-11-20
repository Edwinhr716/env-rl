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
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/env/internal/inventory"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
)

// Values this package shares with the code that writes them. Each is a copy:
// guestVolumeName of apiservice.GuestVolumeName (a test keeps the two equal),
// and the other three of constants in cmd/ate-env's manifest.go.
const (
	// guestVolumeName is the image volume that carries the guest in a
	// template derived for a rootfs image.
	guestVolumeName = "guest"
	// workerPoolLabel is the label `ate-env manifest` puts on a WorkerPool,
	// with the pool's name, and that its templates select.
	workerPoolLabel = "ate-env/workerpool"
	// apiContainerName is ate-env-api's container in its Deployment.
	apiContainerName = "ate-env-api"
	// metricsPortName is the container port ate-env-api serves metrics on.
	metricsPortName = "metrics"
)

// versionLabel is the Kubernetes recommended label for a workload's version.
// `ate-env manifest` does not set it, so a Deployment's version is empty
// unless the operator labels the Deployment.
const versionLabel = "app.kubernetes.io/version"

// derivedDigestLen is how many hex digits of the rootfs image digest end a
// derived template's name (apiservice.ImageTemplateName).
const derivedDigestLen = 12

func asTime(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

func atespaceOf(a *ateapipb.Atespace) inventory.Atespace {
	return inventory.Atespace{Name: a.GetMetadata().GetName(), CreateTime: asTime(a.GetMetadata().GetCreateTime())}
}

// templateOf describes a template. One that mounts the guest as an image
// volume was derived for a rootfs image: its container image is the rootfs
// image, and its base is its own name without the digest suffix, when the
// name ends in the image's digest. Any other template runs the guest as its
// container image.
func templateOf(t *ateapipb.ActorTemplate) inventory.Template {
	out := inventory.Template{
		Atespace:   t.GetMetadata().GetAtespace(),
		Name:       t.GetMetadata().GetName(),
		WorkerPool: t.GetWorkerSelector().GetMatchLabels()[workerPoolLabel],
		GoldenState: inventory.GoldenState(t.GetStatus().GetGoldenSnapshotStatus(),
			t.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() != ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED),
		CreateTime: asTime(t.GetMetadata().GetCreateTime()),
	}
	var image string
	if c := t.GetContainers(); len(c) > 0 {
		image = c[0].GetImage()
	}
	for _, v := range t.GetVolumes() {
		if v.GetName() == guestVolumeName && v.GetImage() != nil {
			out.GuestImage = v.GetImage().GetReference()
			out.RootfsImage = image
			if _, digest, ok := strings.Cut(image, "@sha256:"); ok && len(digest) >= derivedDigestLen {
				if base, found := strings.CutSuffix(out.Name, "-"+digest[:derivedDigestLen]); found {
					out.Base = base
				}
			}
			return out
		}
	}
	out.GuestImage = image
	return out
}

func tagOf(t *ateapipb.Tag) inventory.Tag {
	return inventory.Tag{
		Atespace:    t.GetMetadata().GetAtespace(),
		Name:        t.GetMetadata().GetName(),
		SourceActor: t.GetSourceActor().GetName(),
		CreateTime:  asTime(t.GetMetadata().GetCreateTime()),
	}
}

func resourcesOf(r *ateapipb.WorkerResources) inventory.Resources {
	return inventory.Resources{CPUMillicores: r.GetCpuMillicores(), MemoryBytes: r.GetMemoryBytes()}
}

// kept returns the entries of m whose key keep accepts, or nil when none is
// left, so an empty map is omitted from the snapshot.
func kept(m map[string]string, keep func(string) bool) map[string]string {
	var out map[string]string
	for k, v := range m {
		if !keep(k) {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = v
	}
	return out
}

func workerOf(w *ateapipb.Worker, keep func(string) bool) inventory.Worker {
	return inventory.Worker{
		Name:      w.GetMetadata().GetName(),
		Namespace: w.GetWorkerNamespace(),
		Pool:      w.GetWorkerPool(),
		Pod:       w.GetWorkerPod(),
		Node:      w.GetNodeName(),
		State:     inventory.WorkerStateValue(w.GetStatus().GetState()),
		Capacity:  resourcesOf(w.GetStatus().GetCapacity()),
		Allocated: resourcesOf(w.GetStatus().GetAllocated()),
		Labels:    kept(w.GetLabels(), keep),
	}
}

// actorOf describes an actor. Substrate's actors carry no labels, so Labels
// stays empty. A crash record without a time takes the actor's update time,
// the transition that wrote it.
func actorOf(a *ateapipb.Actor, keep func(string) bool) inventory.Actor {
	md, st := a.GetMetadata(), a.GetStatus()
	out := inventory.Actor{
		UID:            md.GetUid(),
		Atespace:       md.GetAtespace(),
		Name:           md.GetName(),
		Template:       a.GetActorTemplate().GetName(),
		SourceTag:      a.GetSourceTag().GetName(),
		State:          inventory.ActorStateValue(st.GetState()),
		AssignedNode:   st.GetAssignedNode(),
		Worker:         st.GetWorkerAssignment().GetWorker().GetName(),
		WorkerSelector: kept(a.GetWorkerSelector().GetMatchLabels(), keep),
		CreateTime:     asTime(md.GetCreateTime()),
		UpdateTime:     asTime(md.GetUpdateTime()),
	}
	if c := st.GetCrash(); c != nil {
		out.Crash = &inventory.Crash{Message: c.GetMessage(), CrashTime: asTime(c.GetCrashTime())}
		if out.Crash.CrashTime.IsZero() {
			out.Crash.CrashTime = out.UpdateTime
		}
	}
	return out
}

// workerPoolOf describes a WorkerPool object. A pool has one sandbox class.
func workerPoolOf(p *atev1.WorkerPool) inventory.WorkerPool {
	out := inventory.WorkerPool{
		Namespace:     p.Namespace,
		Name:          p.Name,
		Replicas:      p.Spec.Replicas,
		ReadyReplicas: p.Status.ReadyReplicas,
		WorkerImage:   p.Spec.WorkerImage,
	}
	if c := p.Spec.SandboxClasses; len(c) > 0 {
		out.SandboxClass = string(c[0].Name)
		if c[0].ConfigRef != nil {
			out.SandboxConfig = c[0].ConfigRef.Name
		}
	}
	return out
}

// deploymentOf describes d when it runs ate-env-api, which its container's
// name tells; ok is false for any other Deployment.
func deploymentOf(d *appsv1.Deployment) (out inventory.Deployment, ok bool) {
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name != apiContainerName {
			continue
		}
		out = inventory.Deployment{
			Namespace: d.Namespace,
			Name:      d.Name,
			Image:     c.Image,
			Version:   d.Labels[versionLabel],
			Ready:     d.Status.ReadyReplicas,
		}
		for _, arg := range c.Args {
			// A boolean flag as Go's flag package reads it: before the
			// "--" terminator, after one dash or two, and bare or with a
			// value ParseBool accepts.
			if arg == "--" {
				break
			}
			flagArg, dashed := strings.CutPrefix(arg, "--")
			if !dashed {
				flagArg, dashed = strings.CutPrefix(arg, "-")
			}
			if !dashed {
				continue
			}
			if name, value, hasValue := strings.Cut(flagArg, "="); name == "enable-tracing" {
				on, err := strconv.ParseBool(value)
				out.Tracing = !hasValue || (err == nil && on)
			}
		}
		for _, p := range c.Ports {
			if p.Name == metricsPortName {
				out.MetricsPort = p.ContainerPort
			}
		}
		return out, true
	}
	return out, false
}
