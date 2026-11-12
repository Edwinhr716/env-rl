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

// Command ate-env-reporter writes one cluster's fleet inventory snapshot to a
// Cloud Storage bucket, or a local directory, on an interval. It reads the
// Substrate control plane and the Kubernetes API and writes nothing to the
// cluster. See internal/reporter and internal/inventory.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/storage"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/reporter"
	"github.com/agent-substrate/env/internal/telemetry"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// startTimeout bounds the reads of the metadata server before the first tick.
const startTimeout = time.Minute

// shutdownTimeout bounds the wait for open health requests at exit.
const shutdownTimeout = 5 * time.Second

func main() {
	var (
		bucket           = flag.String("bucket", "", "Cloud Storage bucket the snapshots go to, written with the application default credentials; required unless -out-dir is set")
		outDir           = flag.String("out-dir", "", "write the bucket layout (v1/inventory/, v1/snapshots/) under this local directory instead of a bucket")
		project          = flag.String("project", "", "project of the cluster, part of the snapshot's identity and one segment of its object name (no / or \\); empty reads it from the GKE metadata server")
		cluster          = flag.String("cluster", "", "name of the cluster, part of the snapshot's identity and one segment of its object name (no / or \\); empty reads it from the GKE metadata server")
		location         = flag.String("location", "", "location of the cluster; empty reads it from the GKE metadata server, and stays empty off GKE")
		interval         = flag.Duration("interval", reporter.DefaultInterval, "time from one snapshot to the next; while snapshots fail the wait doubles, up to 60 s")
		snapshotInterval = flag.Duration("snapshot-interval", reporter.DefaultSnapshotInterval, "how often a snapshot is also kept under v1/snapshots/")
		ateapi           = flag.String("ateapi", ate.DefaultControlAddr, "address of the ateapi gRPC control plane")
		tokenFile        = flag.String("ateapi-token-file", "", "file with a bearer token for the control plane, e.g. a projected ServiceAccount token with the ateapi audience")
		skipVerify       = flag.Bool("skip-verify", true, "skip TLS certificate verification on the control plane connection")
		healthAddr       = flag.String("health-listen-addr", ":9091", "address to serve /healthz and /readyz on; /readyz answers 200 while the last snapshot was written")
	)
	flag.Parse()
	if flag.NArg() > 0 {
		log.Fatalf("unexpected argument %q", flag.Arg(0))
	}
	if (*bucket == "") == (*outDir == "") {
		log.Fatal("set one of -bucket and -out-dir")
	}

	// The kubeconfig rules of kubectl: KUBECONFIG or ~/.kube/config, and the
	// pod's ServiceAccount when neither exists.
	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	if err != nil {
		log.Fatalf("loading the Kubernetes client configuration: %v", err)
	}
	kube, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("creating the Kubernetes client: %v", err)
	}
	substrate, err := versioned.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("creating the WorkerPool client: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, kube, substrate, config{
		bucket: *bucket, outDir: *outDir,
		project: *project, cluster: *cluster, location: *location,
		interval: *interval, snapshotInterval: *snapshotInterval,
		ateapi: *ateapi, tokenFile: *tokenFile, skipVerify: *skipVerify,
		healthAddr: *healthAddr,
	}); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	bucket, outDir             string
	project, cluster, location string
	interval, snapshotInterval time.Duration
	ateapi, tokenFile          string
	skipVerify                 bool
	healthAddr                 string
}

// run reports until ctx is done, which is not an error.
func run(ctx context.Context, kube kubernetes.Interface, substrate versioned.Interface, cfg config) error {
	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	if err := identity(startCtx, &cfg, metadata.OnGCE, metadata.ProjectIDWithContext, metadata.InstanceAttributeValueWithContext); err != nil {
		return err
	}
	version, commit := telemetry.BuildVersion()

	var store reporter.Store = reporter.DirStore{Dir: cfg.outDir}
	if cfg.bucket != "" {
		gcs, err := storage.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("creating the Cloud Storage client: %w", err)
		}
		defer gcs.Close()
		store = reporter.BucketStore{Bucket: gcs.Bucket(cfg.bucket)}
	}

	control, err := ate.New(ate.Options{ControlAddr: cfg.ateapi, SkipVerify: cfg.skipVerify, BearerTokenFile: cfg.tokenFile})
	if err != nil {
		return err
	}
	defer control.Close()

	rep, err := reporter.New(reporter.Config{
		Project: cfg.project, Cluster: cfg.cluster, Location: cfg.location,
		Version: version, Commit: commit,
		Interval: cfg.interval, SnapshotInterval: cfg.snapshotInterval,
	}, control, reporter.KubeCluster{Kube: kube, Substrate: substrate}, store)
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", cfg.healthAddr)
	if err != nil {
		return fmt.Errorf("health listener %s: %w", cfg.healthAddr, err)
	}
	dest := "gs://" + cfg.bucket
	if cfg.bucket == "" {
		dest = cfg.outDir
	}
	log.Printf("ate-env-reporter %s: %s/%s every %s to %s (ateapi %s, health on %s)", version, cfg.project, cfg.cluster, cfg.interval, dest, cfg.ateapi, ln.Addr())

	srv := &http.Server{Handler: healthHandler(rep.Ready), ReadHeaderTimeout: 5 * time.Second}
	return reportAndServe(ctx, rep.Run, srv, ln)
}

// reportAndServe runs report and serves srv on ln until ctx is done, which
// is not an error, or until the server fails. It returns after report has
// returned, so the caller can close the clients that report uses.
func reportAndServe(ctx context.Context, report func(context.Context), srv *http.Server, ln net.Listener) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	done := make(chan struct{})
	go func() {
		defer close(done)
		report(runCtx)
	}()
	select {
	case <-done:
		// ctx is done: a signal.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-serveErr:
		stopRun()
		<-done
		return fmt.Errorf("health server: %w", err)
	}
}

// identity fills the project, cluster and location that the flags left empty
// from the metadata server. Without one the project and the cluster must come
// from the flags. The cluster attributes exist on GKE only: on another
// Compute Engine VM the cluster must come from the flag, and the location
// stays empty.
func identity(ctx context.Context, cfg *config, onGCE func() bool, projectID func(context.Context) (string, error), attribute func(context.Context, string) (string, error)) error {
	if cfg.project != "" && cfg.cluster != "" && cfg.location != "" {
		return nil
	}
	if !onGCE() {
		if cfg.project == "" || cfg.cluster == "" {
			return errors.New("no GKE metadata server: set -project and -cluster")
		}
		return nil
	}
	var err error
	if cfg.project == "" {
		if cfg.project, err = projectID(ctx); err != nil {
			return fmt.Errorf("reading the project from the metadata server: %w", err)
		}
	}
	if cfg.cluster == "" {
		if cfg.cluster, err = attribute(ctx, "cluster-name"); err != nil {
			return fmt.Errorf("reading cluster-name from the metadata server: %w", err)
		}
	}
	if cfg.location == "" {
		cfg.location, err = attribute(ctx, "cluster-location")
		if _, undefined := errors.AsType[metadata.NotDefinedError](err); undefined {
			cfg.location, err = "", nil
		}
		if err != nil {
			return fmt.Errorf("reading cluster-location from the metadata server: %w", err)
		}
	}
	return nil
}

// healthHandler serves /healthz, 200 while the process runs, and /readyz,
// 200 while ready reports true.
func healthHandler(ready func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready() {
			http.Error(w, "the last snapshot was not written", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok\n")
	})
	return mux
}
