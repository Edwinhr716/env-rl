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
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/agent-substrate/env/internal/inventory"
	"github.com/spf13/cobra"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// reporterName names the fleet inventory reporter's namespace, ServiceAccount,
// ClusterRole, ClusterRoleBinding and Deployment.
const reporterName = "ate-env-reporter"

// reporterHealthPort is the port of ate-env-reporter's -health-listen-addr
// default.
const reporterHealthPort = 9091

// reporterDefaultInterval and reporterDefaultSnapshotInterval are
// ate-env-reporter's defaults for a flag the manifest leaves out
// (reporter.DefaultInterval and DefaultSnapshotInterval; a test holds them
// equal, because this command does not link internal/reporter).
const (
	reporterDefaultInterval         = 5 * time.Second
	reporterDefaultSnapshotInterval = time.Hour
)

// workloadIdentityAnnotation binds a Kubernetes ServiceAccount to a Google
// service account on GKE.
const workloadIdentityAnnotation = "iam.gke.io/gcp-service-account"

type reporterConfig struct {
	namespace string
	image     string
	bucket    string
	outDir    string
	project   string
	cluster   string
	location  string
	// interval and snapshotInterval go to the reporter's flags when their
	// Set field is true; otherwise the reporter runs with its defaults.
	interval            time.Duration
	intervalSet         bool
	snapshotInterval    time.Duration
	snapshotIntervalSet bool
	gcpServiceAccount   string

	// Telemetry, as for ate-env-api in manifestConfig: enableTracing renders
	// -enable-tracing, otlpEndpoint and otlpInsecure set the exporter's
	// environment, metricsPort renders -metrics-listen-addr=:<port> and
	// exposes the port as "metrics" (zero leaves metrics off), and
	// podMonitoring adds a PodMonitoring that scrapes it.
	enableTracing bool
	otlpEndpoint  string
	otlpInsecure  bool
	metricsPort   int32
	podMonitoring bool
}

// reporterReservedDirs are the directories --out-dir must stay out of: an
// emptyDir there hides the ko image's binary, or the mounted tokens
// (/var/run is /run in most images; a second mount at the ateapi token's
// path is also refused by the API server).
var reporterReservedDirs = []string{"/ko-app", "/var/run/secrets", "/run/secrets"}

func (c *reporterConfig) validate() error {
	for _, v := range []*string{&c.namespace, &c.image, &c.bucket, &c.outDir, &c.project, &c.cluster, &c.location, &c.gcpServiceAccount, &c.otlpEndpoint} {
		*v = strings.TrimSpace(*v)
	}
	switch {
	case c.namespace == "":
		return errors.New("--namespace is empty")
	case c.image == "":
		return errors.New("--image is required: no ate-env-reporter image is published yet, build one with ko from ./cmd/ate-env-reporter")
	case (c.bucket == "") == (c.outDir == ""):
		return errors.New("set one of --bucket and --out-dir")
	case strings.HasPrefix(c.bucket, "gs://"):
		return fmt.Errorf("--bucket %q: give the bucket's name, not a gs:// URL", c.bucket)
	case strings.Contains(c.bucket, "/"):
		return fmt.Errorf("--bucket %q: give the bucket's name alone, without a path", c.bucket)
	}
	if c.outDir != "" {
		if !strings.HasPrefix(c.outDir, "/") {
			return fmt.Errorf("--out-dir %q is not an absolute path", c.outDir)
		}
		c.outDir = path.Clean(c.outDir)
		switch {
		case c.outDir == "/":
			return errors.New("--out-dir is the root directory: name a directory for the emptyDir volume")
		case strings.Contains(c.outDir, ":"):
			return fmt.Errorf("--out-dir %q holds a colon, which a volume mount path cannot", c.outDir)
		}
		for _, reserved := range reporterReservedDirs {
			if c.outDir == reserved || strings.HasPrefix(c.outDir, reserved+"/") {
				return fmt.Errorf("--out-dir %q is in %s, which the image or a token mount uses", c.outDir, reserved)
			}
		}
	}
	// One segment each of the object name, as reporter.New requires.
	if c.project != "" {
		if err := inventory.CheckPathSegment("--project", c.project); err != nil {
			return err
		}
	}
	if c.cluster != "" {
		if err := inventory.CheckPathSegment("--cluster", c.cluster); err != nil {
			return err
		}
	}
	// The rules of reporter.New, on the values the reporter would run with:
	// a pod that refuses its flags exits at start, and the command would
	// have printed its manifest without a word.
	interval, snapshotInterval := reporterDefaultInterval, reporterDefaultSnapshotInterval
	if c.intervalSet {
		interval = c.interval
	}
	if c.snapshotIntervalSet {
		snapshotInterval = c.snapshotInterval
	}
	switch {
	case interval < time.Second:
		return fmt.Errorf("--interval %s is under one second", interval)
	case snapshotInterval < interval:
		return fmt.Errorf("--snapshot-interval %s is under the interval %s", snapshotInterval, interval)
	}
	switch {
	case c.metricsPort < 0 || c.metricsPort > 65535:
		return fmt.Errorf("--metrics-port %d is not a port number", c.metricsPort)
	case c.metricsPort == reporterHealthPort:
		return fmt.Errorf("--metrics-port %d is the health port; metrics are served on a listener of their own", c.metricsPort)
	case c.podMonitoring && c.metricsPort == 0:
		return errors.New("--pod-monitoring needs --metrics-port: there is no metrics port to scrape")
	case (c.otlpEndpoint != "" || c.otlpInsecure) && !c.enableTracing:
		return errors.New("--otlp-endpoint and --otlp-insecure configure the trace exporter and need --enable-tracing")
	}
	return nil
}

func newManifestReporterCommand() *cobra.Command {
	cfg := reporterConfig{}
	var accessPolicy bool
	cmd := &cobra.Command{
		Use:   "reporter",
		Short: "Generate the manifests of ate-env-reporter, the fleet inventory reporter",
		Long: `Reporter prints the Kubernetes manifests of ate-env-reporter: its namespace,
ServiceAccount, a ClusterRole and binding that let it list WorkerPools and
Deployments, and its Deployment. One reporter runs per cluster. On an interval
it writes the cluster's inventory snapshot to the bucket --bucket names, as
v1/inventory/<project>/<cluster>/latest.json, and reads the control plane and
the Kubernetes API without writing to either.

  ate-env manifest reporter --image <image> --bucket <bucket> | kubectl apply -f -

On GKE the project, cluster and location come from the metadata server, and
--gcp-service-account binds the ServiceAccount through Workload Identity to the
Google service account that may write the bucket. --out-dir writes under a
directory of an emptyDir volume instead, for a cluster without a bucket.

--enable-tracing exports a span per tick to --otlp-endpoint. --metrics-port
serves ate_env_reporter_poll_duration_seconds on a listener of its own, and
--pod-monitoring has Managed Service for Prometheus scrape it every 5s.

The reporter lists the actors and workers of every atespace, which takes the
global viewer role. With --access-policy the command prints that binding
instead, in the form kubectl-ate reads. The global policy is one object for
the whole control plane and it holds the owners, so read it first with
"kubectl ate get access-policy --global -o yaml". When that prints a policy,
add the member to its viewer binding and apply the file with "kubectl ate
update access-policy --global -f <file>"; do not create over it. When it
prints nothing, as on a fresh Substrate install, there is no policy to update:
create it once with

  ate-env manifest reporter --access-policy | kubectl ate create access-policy --global -f -`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if accessPolicy {
				if cfg.namespace = strings.TrimSpace(cfg.namespace); cfg.namespace == "" {
					return errors.New("--namespace is empty")
				}
				return writeProtoYAML(cmd.OutOrStdout(), buildReporterAccessPolicy(cfg.namespace), "global access policy")
			}
			cfg.intervalSet = cmd.Flags().Changed("interval")
			cfg.snapshotIntervalSet = cmd.Flags().Changed("snapshot-interval")
			if err := cfg.validate(); err != nil {
				return err
			}
			return writeManifests(cmd.OutOrStdout(), buildReporterManifests(cfg))
		},
	}
	cmd.Flags().StringVar(&cfg.namespace, "namespace", reporterName, "Kubernetes namespace the reporter runs in")
	cmd.Flags().StringVar(&cfg.image, "image", "", "digest-pinned ate-env-reporter image (required)")
	cmd.Flags().StringVar(&cfg.bucket, "bucket", "", "name of the Cloud Storage bucket the snapshots go to (no gs:// prefix, no path)")
	cmd.Flags().StringVar(&cfg.outDir, "out-dir", "", "write the snapshots under this absolute path, the mount of an emptyDir volume, instead of a bucket; not /, /ko-app or a secrets directory")
	cmd.Flags().StringVar(&cfg.project, "project", "", "project of the cluster (ate-env-reporter -project); required off GKE")
	cmd.Flags().StringVar(&cfg.cluster, "cluster", "", "name of the cluster (ate-env-reporter -cluster); required off GKE")
	cmd.Flags().StringVar(&cfg.location, "location", "", "location of the cluster (ate-env-reporter -location)")
	cmd.Flags().DurationVar(&cfg.interval, "interval", 0, "time from one snapshot to the next, at least 1s (ate-env-reporter -interval, default 5s)")
	cmd.Flags().DurationVar(&cfg.snapshotInterval, "snapshot-interval", 0, "how often a snapshot is also kept under v1/snapshots/, at least the interval (ate-env-reporter -snapshot-interval, default 1h)")
	cmd.Flags().StringVar(&cfg.gcpServiceAccount, "gcp-service-account", "", "Google service account the ServiceAccount acts as through Workload Identity, set as its "+workloadIdentityAnnotation+" annotation")
	cmd.Flags().BoolVar(&cfg.enableTracing, "enable-tracing", false, "run ate-env-reporter with -enable-tracing, exporting a span per tick over OTLP/gRPC")
	cmd.Flags().StringVar(&cfg.otlpEndpoint, "otlp-endpoint", "", "OTLP/gRPC collector endpoint for ate-env-reporter's traces, set as OTEL_EXPORTER_OTLP_ENDPOINT, e.g. http://ate-env-collector.ate-env-collector:4317; needs --enable-tracing")
	cmd.Flags().BoolVar(&cfg.otlpInsecure, "otlp-insecure", false, "set OTEL_EXPORTER_OTLP_INSECURE=true: talk to the collector in plaintext; needs --enable-tracing")
	cmd.Flags().Int32Var(&cfg.metricsPort, "metrics-port", 0, "port ate-env-reporter serves Prometheus metrics on (-metrics-listen-addr), exposed on the container as \"metrics\" with prometheus.io scrape annotations; 0 leaves metrics off. The metrics listener has no authentication and no Host check")
	cmd.Flags().BoolVar(&cfg.podMonitoring, "pod-monitoring", false, "add a PodMonitoring (monitoring.googleapis.com/v1) that has Google Cloud Managed Service for Prometheus scrape the metrics port every "+podMonitoringInterval+"; needs --metrics-port and Managed Prometheus on the cluster. The pod keeps its prometheus.io annotations, so a cluster that also runs an annotation-based scraper scrapes the port twice")
	cmd.Flags().BoolVar(&accessPolicy, "access-policy", false, "print the global access policy binding (viewer) the reporter needs, instead of the Kubernetes manifests")
	return cmd
}

// buildReporterAccessPolicy returns the global viewer binding for the
// reporter's ServiceAccount, to merge into the control plane's one global
// policy.
func buildReporterAccessPolicy(namespace string) *ateapipb.AccessPolicy {
	return &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{
			Role:    "viewer",
			Members: []string{"user:system:serviceaccount:" + namespace + ":" + reporterName},
		}},
	}
}

func buildReporterManifests(cfg reporterConfig) []any {
	labels := map[string]string{"app": reporterName}

	sa := &corev1.ServiceAccount{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{Name: reporterName, Namespace: cfg.namespace, Labels: labels},
	}
	if cfg.gcpServiceAccount != "" {
		sa.Annotations = map[string]string{workloadIdentityAnnotation: cfg.gcpServiceAccount}
	}

	// The two kinds internal/reporter lists, in every namespace.
	role := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: reporterName, Labels: labels},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{atev1.GroupVersion.Group}, Resources: []string{"workerpools"}, Verbs: []string{"list"}},
			{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"list"}},
		},
	}
	binding := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: reporterName, Labels: labels},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: reporterName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: reporterName, Namespace: cfg.namespace}},
	}

	args := []string{"-ateapi-token-file=/var/run/secrets/ateapi/token"}
	tokenExpiration := int64(7200)
	mounts := []corev1.VolumeMount{{Name: "ateapi-token", MountPath: "/var/run/secrets/ateapi", ReadOnly: true}}
	volumes := []corev1.Volume{{
		Name: "ateapi-token",
		VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
				Audience:          "api.ate-system.svc",
				ExpirationSeconds: &tokenExpiration,
				Path:              "token",
			}}},
		}},
	}}
	if cfg.bucket != "" {
		args = append(args, "-bucket="+cfg.bucket)
	} else {
		args = append(args, "-out-dir="+cfg.outDir)
		mounts = append(mounts, corev1.VolumeMount{Name: "snapshots", MountPath: cfg.outDir})
		// The limit bounds the kept snapshots, which nothing deletes.
		limit := resource.MustParse("1Gi")
		volumes = append(volumes, corev1.Volume{Name: "snapshots", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &limit}}})
	}
	for _, f := range []struct{ name, value string }{
		{"project", cfg.project}, {"cluster", cfg.cluster}, {"location", cfg.location},
	} {
		if f.value != "" {
			args = append(args, "-"+f.name+"="+f.value)
		}
	}
	if cfg.intervalSet {
		args = append(args, "-interval="+cfg.interval.String())
	}
	if cfg.snapshotIntervalSet {
		args = append(args, "-snapshot-interval="+cfg.snapshotInterval.String())
	}
	ports := []corev1.ContainerPort{{Name: "health", ContainerPort: reporterHealthPort}}
	var podAnnotations map[string]string
	if cfg.enableTracing {
		args = append(args, "-enable-tracing")
	}
	if cfg.metricsPort != 0 {
		args = append(args, fmt.Sprintf("-metrics-listen-addr=:%d", cfg.metricsPort))
		ports = append(ports, corev1.ContainerPort{Name: metricsPortName, ContainerPort: cfg.metricsPort})
		podAnnotations = map[string]string{
			"prometheus.io/scrape": "true",
			"prometheus.io/port":   fmt.Sprint(cfg.metricsPort),
			"prometheus.io/path":   "/metrics",
		}
	}
	var env []corev1.EnvVar
	if cfg.otlpEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: cfg.otlpEndpoint})
	}
	if cfg.otlpInsecure {
		env = append(env, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_INSECURE", Value: "true"})
	}

	replicas := int32(1)
	probe := func(path string) *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(reporterHealthPort)},
		}}
	}
	deployment := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: reporterName, Namespace: cfg.namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			// One writer per cluster: the old pod stops before the new one
			// starts, so two never rewrite latest.json in turn.
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: podAnnotations},
				Spec: corev1.PodSpec{
					// The ServiceAccount is the principal of the ClusterRole
					// binding, of the control plane's viewer binding
					// (buildReporterAccessPolicy) and of Workload Identity.
					ServiceAccountName: reporterName,
					Containers: []corev1.Container{{
						Name:           reporterName,
						Image:          cfg.image,
						Args:           args,
						Ports:          ports,
						Env:            env,
						VolumeMounts:   mounts,
						LivenessProbe:  probe("/healthz"),
						ReadinessProbe: probe("/readyz"),
					}},
					Volumes: volumes,
				},
			},
		},
	}

	objs := []any{
		// The label admits the namespace's pods to ateapi (apiAccessLabel).
		&corev1.Namespace{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: metav1.ObjectMeta{Name: cfg.namespace, Labels: map[string]string{apiAccessLabel: "true"}},
		},
		sa, role, binding, deployment,
	}
	if cfg.podMonitoring {
		objs = append(objs, buildPodMonitoring(reporterName, cfg.namespace))
	}
	return objs
}
