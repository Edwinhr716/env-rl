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
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/reporter"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func runManifestReporter(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newManifestCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(append([]string{"reporter"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// reporterDocs splits the output into its documents by kind.
func reporterDocs(t *testing.T, out string) map[string]string {
	t.Helper()
	docs := map[string]string{}
	for doc := range strings.SplitSeq(out, "\n---\n") {
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatalf("%q: %v", doc, err)
		}
		docs[meta.Kind] = doc
	}
	return docs
}

func TestManifestReporterForGKE(t *testing.T) {
	out, err := runManifestReporter(t, "--image", "example.com/reporter@sha256:abc", "--bucket", "fleet-bucket",
		"--gcp-service-account", "ate-env-reporter@p.iam.gserviceaccount.com")
	if err != nil {
		t.Fatal(err)
	}
	docs := reporterDocs(t, out)
	if got := len(docs); got != 5 {
		t.Fatalf("%d documents, want Namespace, ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment:\n%s", got, out)
	}

	var ns corev1.Namespace
	if err := yaml.Unmarshal([]byte(docs["Namespace"]), &ns); err != nil {
		t.Fatal(err)
	}
	if ns.Name != reporterName || ns.Labels[apiAccessLabel] != "true" {
		t.Errorf("namespace = %s %v, want %s labeled %s", ns.Name, ns.Labels, reporterName, apiAccessLabel)
	}

	var sa corev1.ServiceAccount
	if err := yaml.Unmarshal([]byte(docs["ServiceAccount"]), &sa); err != nil {
		t.Fatal(err)
	}
	if sa.Annotations[workloadIdentityAnnotation] != "ate-env-reporter@p.iam.gserviceaccount.com" {
		t.Errorf("ServiceAccount annotations = %v", sa.Annotations)
	}

	// The role is what internal/reporter reads: a list of two kinds.
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal([]byte(docs["ClusterRole"]), &role); err != nil {
		t.Fatal(err)
	}
	want := []rbacv1.PolicyRule{
		{APIGroups: []string{"ate.dev"}, Resources: []string{"workerpools"}, Verbs: []string{"list"}},
		{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"list"}},
	}
	if !reflect.DeepEqual(role.Rules, want) {
		t.Errorf("ClusterRole rules = %+v, want %+v", role.Rules, want)
	}

	var binding rbacv1.ClusterRoleBinding
	if err := yaml.Unmarshal([]byte(docs["ClusterRoleBinding"]), &binding); err != nil {
		t.Fatal(err)
	}
	if binding.RoleRef.Name != role.Name || len(binding.Subjects) != 1 || binding.Subjects[0].Name != sa.Name || binding.Subjects[0].Namespace != sa.Namespace {
		t.Errorf("binding = %+v", binding)
	}

	var dep appsv1.Deployment
	if err := yaml.Unmarshal([]byte(docs["Deployment"]), &dep); err != nil {
		t.Fatal(err)
	}
	if *dep.Spec.Replicas != 1 || dep.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Errorf("replicas %d, strategy %s: want one writer, replaced in place", *dep.Spec.Replicas, dep.Spec.Strategy.Type)
	}
	pod := dep.Spec.Template.Spec
	c := pod.Containers[0]
	if pod.ServiceAccountName != sa.Name || c.Image != "example.com/reporter@sha256:abc" {
		t.Errorf("pod runs %s as %s", c.Image, pod.ServiceAccountName)
	}
	// On GKE the identity comes from the metadata server: no identity flag.
	if want := []string{"-ateapi-token-file=/var/run/secrets/ateapi/token", "-bucket=fleet-bucket"}; !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
	if c.ReadinessProbe.HTTPGet.Path != "/readyz" || c.LivenessProbe.HTTPGet.Path != "/healthz" || c.ReadinessProbe.HTTPGet.Port.IntValue() != reporterHealthPort {
		t.Errorf("probes = %+v, %+v", c.ReadinessProbe.HTTPGet, c.LivenessProbe.HTTPGet)
	}
	token := pod.Volumes[0].Projected.Sources[0].ServiceAccountToken
	if len(pod.Volumes) != 1 || token.Audience != "api.ate-system.svc" {
		t.Errorf("volumes = %+v", pod.Volumes)
	}
}

func TestManifestReporterOutDir(t *testing.T) {
	out, err := runManifestReporter(t, "--image", "img", "--out-dir", "/snap", "--namespace", "fleet",
		"--project", "kind", "--cluster", "kind", "--location", "local", "--interval", "5s", "--snapshot-interval", "5s")
	if err != nil {
		t.Fatal(err)
	}
	docs := reporterDocs(t, out)
	var dep appsv1.Deployment
	if err := yaml.Unmarshal([]byte(docs["Deployment"]), &dep); err != nil {
		t.Fatal(err)
	}
	pod := dep.Spec.Template.Spec
	c := pod.Containers[0]
	for _, arg := range []string{"-out-dir=/snap", "-project=kind", "-cluster=kind", "-location=local", "-interval=5s", "-snapshot-interval=5s"} {
		if !slices.Contains(c.Args, arg) {
			t.Errorf("args %v lack %s", c.Args, arg)
		}
	}
	if dep.Namespace != "fleet" {
		t.Errorf("namespace = %s", dep.Namespace)
	}
	// The directory is an emptyDir, written as one in the YAML: an empty
	// volume source would be pruned from the output.
	if len(pod.Volumes) != 2 || pod.Volumes[1].EmptyDir == nil || c.VolumeMounts[1].MountPath != "/snap" || c.VolumeMounts[1].Name != pod.Volumes[1].Name {
		t.Errorf("volumes = %+v, mounts = %+v", pod.Volumes, c.VolumeMounts)
	}
	var sa corev1.ServiceAccount
	if err := yaml.Unmarshal([]byte(docs["ServiceAccount"]), &sa); err != nil {
		t.Fatal(err)
	}
	if len(sa.Annotations) != 0 {
		t.Errorf("ServiceAccount annotations = %v, want none", sa.Annotations)
	}
}

func TestManifestReporterRejects(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"no image":             {[]string{"--bucket", "b"}, "--image is required"},
		"no destination":       {[]string{"--image", "i"}, "one of --bucket and --out-dir"},
		"two destinations":     {[]string{"--image", "i", "--bucket", "b", "--out-dir", "/snap"}, "one of --bucket and --out-dir"},
		"relative out-dir":     {[]string{"--image", "i", "--out-dir", "snap"}, "absolute"},
		"empty namespace":      {[]string{"--image", "i", "--bucket", "b", "--namespace", " "}, "--namespace is empty"},
		"policy, no namespace": {[]string{"--access-policy", "--namespace", ""}, "--namespace is empty"},
		// Values the pod or kubectl apply would refuse.
		"blank image":               {[]string{"--image", " ", "--bucket", "b"}, "--image is required"},
		"blank bucket":              {[]string{"--image", "i", "--bucket", " "}, "one of --bucket and --out-dir"},
		"blank out-dir":             {[]string{"--image", "i", "--out-dir", " "}, "one of --bucket and --out-dir"},
		"bucket as a gs:// URL":     {[]string{"--image", "i", "--bucket", "gs://my-bucket"}, "not a gs:// URL"},
		"bucket with a path":        {[]string{"--image", "i", "--bucket", "my-bucket/fleet"}, "--bucket"},
		"out-dir at the root":       {[]string{"--image", "i", "--out-dir", "/"}, "--out-dir"},
		"out-dir of slashes":        {[]string{"--image", "i", "--out-dir", "//"}, "--out-dir"},
		"out-dir with a colon":      {[]string{"--image", "i", "--out-dir", "/snap:ro"}, "--out-dir"},
		"out-dir over the binary":   {[]string{"--image", "i", "--out-dir", "/ko-app"}, "--out-dir"},
		"out-dir under the binary":  {[]string{"--image", "i", "--out-dir", "/ko-app/snap"}, "--out-dir"},
		"out-dir that cleans to it": {[]string{"--image", "i", "--out-dir", "/snap/../ko-app/"}, "--out-dir"},
		"out-dir over the token":    {[]string{"--image", "i", "--out-dir", "/var/run/secrets/ateapi"}, "--out-dir"},
		"out-dir over the secrets":  {[]string{"--image", "i", "--out-dir", "/var/run/secrets"}, "--out-dir"},
		"out-dir over /run/secrets": {[]string{"--image", "i", "--out-dir", "/run/secrets/ateapi"}, "--out-dir"},
		"project with a slash":      {[]string{"--image", "i", "--bucket", "b", "--project", "projects/p"}, "path segment"},
		"cluster of dots":           {[]string{"--image", "i", "--bucket", "b", "--cluster", ".."}, "path segment"},
		// What ate-env-reporter refuses at start (reporter.New), with its
		// defaults of 5s and 1h for a flag left unset.
		"interval 0s":                      {[]string{"--image", "i", "--bucket", "b", "--interval", "0s"}, "--interval"},
		"interval under 1s":                {[]string{"--image", "i", "--bucket", "b", "--interval", "500ms"}, "--interval"},
		"negative interval":                {[]string{"--image", "i", "--bucket", "b", "--interval", "-5s"}, "--interval"},
		"snapshot interval 0s":             {[]string{"--image", "i", "--bucket", "b", "--snapshot-interval", "0s"}, "--snapshot-interval"},
		"snapshot interval under default":  {[]string{"--image", "i", "--bucket", "b", "--snapshot-interval", "2s"}, "--snapshot-interval"},
		"interval over default snapshot":   {[]string{"--image", "i", "--bucket", "b", "--interval", "2h"}, "--snapshot-interval"},
		"snapshot interval under interval": {[]string{"--image", "i", "--bucket", "b", "--interval", "10s", "--snapshot-interval", "9s"}, "--snapshot-interval"},
	} {
		if _, err := runManifestReporter(t, tc.args...); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want one containing %q", name, err, tc.want)
		}
	}
}

// TestManifestReporterTrimsAndCleans: surrounding space is dropped and the
// directory is cleaned, in the arguments and in the mount.
func TestManifestReporterTrimsAndCleans(t *testing.T) {
	out, err := runManifestReporter(t, "--image", " img ", "--out-dir", " /snap//kept/ ", "--project", " kind ", "--cluster", "kind")
	if err != nil {
		t.Fatal(err)
	}
	var d appsv1.Deployment
	if err := yaml.Unmarshal([]byte(reporterDocs(t, out)["Deployment"]), &d); err != nil {
		t.Fatal(err)
	}
	c := d.Spec.Template.Spec.Containers[0]
	if c.Image != "img" {
		t.Errorf("image = %q", c.Image)
	}
	for _, want := range []string{"-out-dir=/snap/kept", "-project=kind"} {
		if !slices.Contains(c.Args, want) {
			t.Errorf("args %q lack %q", c.Args, want)
		}
	}
	if !slices.ContainsFunc(c.VolumeMounts, func(m corev1.VolumeMount) bool { return m.MountPath == "/snap/kept" }) {
		t.Errorf("mounts %+v lack /snap/kept", c.VolumeMounts)
	}

	out, err = runManifestReporter(t, "--image", "img", "--bucket", " fleet-bucket ")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "- -bucket=fleet-bucket\n") {
		t.Errorf("the bucket is not trimmed:\n%s", out)
	}
}

// TestManifestReporterIntervalsAgreeWithTheReporter: the command accepts a
// pair of intervals when ate-env-reporter does, with the same defaults, so a
// manifest it prints does not start a pod that exits on its flags.
func TestManifestReporterIntervalsAgreeWithTheReporter(t *testing.T) {
	if reporterDefaultInterval != reporter.DefaultInterval || reporterDefaultSnapshotInterval != reporter.DefaultSnapshotInterval {
		t.Fatalf("defaults %s and %s, the reporter's are %s and %s",
			reporterDefaultInterval, reporterDefaultSnapshotInterval, reporter.DefaultInterval, reporter.DefaultSnapshotInterval)
	}
	durations := []string{"", "500ms", "1s", "5s", "30m", "1h", "2h"}
	for _, interval := range durations {
		for _, snapshot := range durations {
			args := []string{"--image", "i", "--bucket", "b"}
			cfg := reporter.Config{Project: "p", Cluster: "c", Version: "v", Interval: reporter.DefaultInterval, SnapshotInterval: reporter.DefaultSnapshotInterval}
			if interval != "" {
				args = append(args, "--interval", interval)
				cfg.Interval, _ = time.ParseDuration(interval)
			}
			if snapshot != "" {
				args = append(args, "--snapshot-interval", snapshot)
				cfg.SnapshotInterval, _ = time.ParseDuration(snapshot)
			}
			_, reporterErr := reporter.New(cfg, nil, nil, nil)
			_, err := runManifestReporter(t, args...)
			if (err == nil) != (reporterErr == nil) {
				t.Errorf("--interval %q --snapshot-interval %q: manifest err = %v, reporter.New err = %v", interval, snapshot, err, reporterErr)
			}
		}
	}
}

// TestManifestReporterAccessPolicy: the viewer binding names the
// ServiceAccount the manifests create, and needs none of their flags.
func TestManifestReporterAccessPolicy(t *testing.T) {
	out, err := runManifestReporter(t, "--access-policy", "--namespace", "fleet")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Bindings []struct {
			Role    string   `json:"role"`
			Members []string `json:"members"`
		} `json:"bindings"`
	}
	if err := yaml.Unmarshal([]byte(out), &policy); err != nil {
		t.Fatalf("%q: %v", out, err)
	}
	if len(policy.Bindings) != 1 || policy.Bindings[0].Role != "viewer" ||
		!reflect.DeepEqual(policy.Bindings[0].Members, []string{"user:system:serviceaccount:fleet:ate-env-reporter"}) {
		t.Errorf("policy = %+v", policy)
	}
}
