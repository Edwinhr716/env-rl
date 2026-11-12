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
	"context"

	"github.com/agent-substrate/env/internal/inventory"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// KubeCluster lists the WorkerPools and the Deployments of every namespace
// from the Kubernetes API on each call. A list that fails fails the tick, so
// a snapshot never carries Kubernetes state older than itself. The lists
// accept the API server's cache (resourceVersion 0), the read an informer
// starts with.
type KubeCluster struct {
	Kube      kubernetes.Interface
	Substrate versioned.Interface
}

var fromCache = metav1.ListOptions{ResourceVersion: "0"}

func (c KubeCluster) WorkerPools(ctx context.Context) ([]inventory.WorkerPool, error) {
	pools, err := c.Substrate.AteV1().WorkerPools(metav1.NamespaceAll).List(ctx, fromCache)
	if err != nil {
		return nil, err
	}
	out := make([]inventory.WorkerPool, 0, len(pools.Items))
	for i := range pools.Items {
		out = append(out, workerPoolOf(&pools.Items[i]))
	}
	return out, nil
}

// Deployments returns the Deployments that run ate-env-api.
func (c KubeCluster) Deployments(ctx context.Context) ([]inventory.Deployment, error) {
	deployments, err := c.Kube.AppsV1().Deployments(metav1.NamespaceAll).List(ctx, fromCache)
	if err != nil {
		return nil, err
	}
	out := []inventory.Deployment{}
	for i := range deployments.Items {
		if dep, ok := deploymentOf(&deployments.Items[i]); ok {
			out = append(out, dep)
		}
	}
	return out, nil
}
