# Interface contracts: sandbox fleet site

Status: accepted (#223); last checked 2026-11-10 against `3dd8bab2`. The snapshot schema in the next
section is code since pull request 1 (#226, `internal/inventory`): its object kinds, field names and
rules are fixed, and a change to them is a schema change. The names in the other sections (bucket
layout, reporter flags and identity, event stream and tables, collector pipelines, site routes and
environment variables, named queries, Terraform) are proposals until their pull request lands; the
lead confirms them and records changes in [decisions.md](./decisions.md).

## Bucket layout

```
gs://<bucket>/v1/inventory/<project>/<cluster>/latest.json          rewritten every interval by that cluster's reporter, its only writer
gs://<bucket>/v1/snapshots/<project>/<cluster>/<ts>.json            hourly, lifecycle 30 days (its own prefix, so no rule can touch latest.json)
gs://<bucket>/v1/events/<dt=YYYY-MM-DD>/...                          Cloud Storage subscription output, lifecycle 400 days
gs://<bucket>/v1/traces/<year>/<month>/<day>/...                     google_cloud_storage exporter output, gzip
gs://<bucket>/v1/runs/<run_id>/                                      RunReport .txt and .json, spans.jsonl, run.json and report.html (FS-11 lists the three steps)
gs://<bench bucket>/<YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>[_traced]/   benchmark bundles from bench upload (#270)
```

The site discovers clusters by listing `v1/inventory/*/*/latest.json`. There is no shared index
object, so no two writers touch one object. `<project>` and `<cluster>` are one path segment each:
the reporter, the snapshot schema and the decoder refuse a value that holds `/` or `\` or is `.` or
`..`, so the name always has this depth and stays under the cluster's own prefix.

## Snapshot schema, `internal/inventory`

```json
{
  "schema_version": "v1",
  "generated_at": "2026-11-03T12:00:05Z",
  "project": "example-project",
  "cluster": "bench-1",
  "location": "us-central1",
  "reporter": {
    "version": "v0.1.0",
    "commit": "abc1234",
    "interval_s": 5
  },
  "atespaces": [
    {
      "name": "ate-env",
      "create_time": "2026-11-03T12:00:00Z"
    }
  ],
  "templates": [
    {
      "atespace": "ate-env",
      "name": "default-template",
      "guest_image": "example.com/ate-env-guest@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "workerpool": "default-template-workerpool",
      "golden_state": "none",
      "create_time": "2026-11-03T12:00:00Z"
    },
    {
      "atespace": "ate-env",
      "name": "default-template-1a2b3c4d5e6f",
      "base": "default-template",
      "rootfs_image": "example.com/python@sha256:1a2b3c4d5e6f0000000000000000000000000000000000000000000000000000",
      "guest_image": "example.com/ate-env-guest@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "workerpool": "default-template-workerpool",
      "golden_state": "ready",
      "create_time": "2026-11-03T12:00:01Z"
    }
  ],
  "tags": [
    {
      "atespace": "ate-env",
      "name": "golden-default-template-1a2b3c4d5e6f",
      "source_actor": "golden-7f3e",
      "create_time": "2026-11-03T12:00:02Z"
    }
  ],
  "workerpools": [
    {
      "namespace": "ate-env",
      "name": "default-template-workerpool",
      "replicas": 4,
      "ready_replicas": 4,
      "sandbox_class": "gvisor",
      "sandbox_config": "gvisor-default",
      "worker_image": "example.com/ateworker-gvisor@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  ],
  "workers": [
    {
      "name": "w-0a1b2c",
      "namespace": "ate-env",
      "pool": "default-template-workerpool",
      "pod": "default-template-workerpool-7c9d8-x2k4q",
      "node": "gke-bench-1-pool-1-abcd",
      "state": "active",
      "capacity": {
        "cpu_m": 8000,
        "memory_bytes": 34359738368
      },
      "allocated": {
        "cpu_m": 2000,
        "memory_bytes": 4294967296
      },
      "labels": {
        "ate-env/workerpool": "default-template-workerpool"
      }
    }
  ],
  "actors": [
    {
      "uid": "8f2a0001",
      "atespace": "ate-env",
      "name": "env-001",
      "template": "default-template-1a2b3c4d5e6f",
      "state": "running",
      "assigned_node": "gke-bench-1-pool-1-abcd",
      "worker": "w-0a1b2c",
      "worker_selector": {
        "ate-env/workerpool": "default-template-workerpool"
      },
      "create_time": "2026-11-03T12:00:03Z",
      "update_time": "2026-11-03T12:00:04Z",
      "labels": {
        "example.com/run-id": "r-1"
      }
    },
    {
      "uid": "8f2a0002",
      "atespace": "ate-env",
      "name": "env-002",
      "template": "default-template-1a2b3c4d5e6f",
      "source_tag": "golden-default-template-1a2b3c4d5e6f",
      "state": "crashed",
      "create_time": "2026-11-03T12:00:03Z",
      "update_time": "2026-11-03T12:00:05Z",
      "crash": {
        "message": "atelet: restore failed: snapshot download timed out",
        "crash_time": "2026-11-03T12:00:05Z"
      }
    }
  ],
  "deployments": [
    {
      "namespace": "ate-env",
      "name": "ate-env-api",
      "image": "example.com/ate-env-api@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "version": "v0.1.0",
      "ready": 1,
      "tracing": true,
      "metrics_port": 9090
    }
  ],
  "counts": {
    "actors_by_state": {
      "crashed": 1,
      "running": 1
    },
    "actors_by_atespace": {
      "ate-env": 2
    }
  }
}
```

Added in pull request 2, additive to v1: `workers[].labels`, the labels `ListWorkers` returns,
because a placement domain is the distinct label set of a pool's workers, as `ListPlacementDomains`
computes it; and `actors[].worker_selector`, the per-environment selector from #207. Both are
`$defs/Labels` in the schema, as `actors[].labels` is.

Rules: timestamps RFC 3339 UTC; `state` is the `ateapi.ActorState` value lowercased, the same
vocabulary as the event stream; `worker` and `name` of workers are opaque control-plane names, never
parsed; a label or selector key that names a credential (`token`, `password`, `authorization`,
`secret`, `credential`, in any letter case) is refused by both validators, and the reporter leaves
such a key out. Substrate's actors carry no labels at the version `go.mod` requires, so
`actors[].labels` stays empty until they do.

## Reporter, `cmd/ate-env-reporter`

| Flag                                | Default                                 | Meaning                                                                                                                                           |
| ----------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-bucket`                           | required unless `-out-dir` is set       | destination bucket; exactly one of `-bucket` and `-out-dir` is set                                                                                |
| `-project`, `-cluster`, `-location` | from the GKE metadata server when empty | snapshot identity; off GKE, `-project` and `-cluster` are required (`DecodeSnapshot` rejects a snapshot without them) and `-location` stays empty |
| `-interval`                         | `5s`                                    | time from one tick's start to the next, at least 1 s; the wait doubles with each failed tick, up to 60 s                                          |
| `-snapshot-interval`                | `1h`                                    | how often `latest.json` is also kept under `snapshots/`, as `<YYYYMMDD>T<HHMMSS>Z.json`; the first tick keeps one                                 |
| `-out-dir`                          | empty                                   | write the bucket layout (`v1/inventory/`, `v1/snapshots/`) under a local directory (unit tests, the Kind end-to-end case)                         |
| `-ateapi`, `-ateapi-token-file`     | as `ate-env-api`                        | Substrate control plane                                                                                                                           |
| `-skip-verify`                      | `true`, as `ate-env-api`                | skip TLS verification of the control plane                                                                                                        |
| `-health-listen-addr`               | `:9091`                                 | `/healthz`, and `/readyz`, which answers 200 while the last tick wrote its snapshot                                                               |

Not in pull request 2: `-enable-tracing` and `-metrics-listen-addr` with
`ate_env_reporter_poll_duration_seconds{status}`. They come with the telemetry manifests (pull
request 3), where a scrape configuration exists; see [decisions.md](./decisions.md).

Identity (pull request 2b, `ate-env manifest reporter`): Kubernetes ServiceAccount
`ate-env-reporter` in namespace `ate-env-reporter` labeled `ate.dev/api-access=true`; projected
token with audience `api.ate-system.svc`; Workload Identity to
`ate-env-reporter@<project>.iam.gserviceaccount.com` with `roles/storage.objectUser` on the bucket,
restricted by an IAM condition to `v1/inventory/<project>/<cluster>/` and
`v1/snapshots/<project>/<cluster>/` (an overwrite needs `storage.objects.delete` next to `create`);
the principal `user:system:serviceaccount:ate-env-reporter:ate-env-reporter` merged into the global
AccessPolicy's `viewer` binding (one object for the whole control plane:
`ate-env manifest reporter --access-policy` prints the binding and the operator merges it with
`kubectl ate get access-policy --global -o yaml` and
`kubectl ate update access-policy --global -f <file>`, as the README describes for `ate-env-api`'s
`ListPlacementDomains`, or creates the policy from that output with
`kubectl ate create access-policy --global -f -` when the control plane has none); ClusterRole with
`list` on `workerpools.ate.dev` and `deployments.apps`, the two kinds the reporter reads.

Behavior: one `ListActors` across atespaces and one `ListWorkers` per tick, paged at 1000;
`ListAtespaces`, `ListActorTemplates` and `ListTags` every tenth tick, or at once when an actor
names an atespace or a template the last read lacks (one that the read does not resolve, a template
deleted under its actors, does not force another); one list of WorkerPools and one of Deployments
per tick, from the API server's cache. A Deployment is taken when it has a container named
`ate-env-api`. Each tick rewrites `latest.json` with one atomic object write, even when nothing but
`generated_at` changed, so the object generation and the JSON `generated_at` move together and the
site revalidates by generation. A tick that cannot read a source writes nothing and is retried. A
tick whose kept copy fails has written `latest.json` and still counts as failed. The reporter never
calls `ate-env-api`.

Tests: unit tests against `internal/internaltest/fakecontrol`, which has `ListAtespaces`,
`ListActorTemplates`, `ListTags` and `AddTag` since pull request 2, and against the fake Kubernetes
clientsets; pull request 2b adds one end-to-end case in `hack/ci/e2e.py` that runs the reporter with
`-out-dir`, `-project` and `-cluster` against the Kind Substrate and accepts the output with
`DecodeSnapshot` (FS-14).

## Event stream

Substrate's records as documented in its `docs/observability.md`:

| Record                | `event.name`              | Attributes                                                                                                                                                            |
| --------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Actor state changed` | `ate.actor.state_changed` | `ate.atespace`, `ate.actor.name`, `ate.actor.uid`, `ate.template.atespace`, `ate.template.name`, `ate.actor.operation.name`, `ate.actor.state`; `trace_id`, `span_id` |
| `Actor crashed`       | `ate.actor.crashed`       | the same                                                                                                                                                              |
| `Actor usage sampled` | `ate.actor.usage_sampled` | identity keys, `ate.workerpool.*`, `ate.sandbox.class`, `ate.stats.*`, `ate.actor.epoch`                                                                              |

Phase 1 sink filter, one sink per cluster project, destination the central topic:

```
resource.type="k8s_container"
resource.labels.namespace_name="ate-system"
resource.labels.container_name="ate-api-server"
(jsonPayload.msg="Actor state changed" OR jsonPayload.msg="Actor crashed")
```

BigQuery table `fleet.actor_events` from the Pub/Sub BigQuery subscription without a topic schema:
`subscription_name`, `message_id`, `publish_time`, `data` (JSON, the raw payload: the Cloud Logging
`LogEntry` in phase 1, the OTLP log record in phase 2) and `attributes` (JSON). A topic schema maps
top-level fields only and cannot flatten either payload. The typed columns `project`, `cluster`,
`time`, `atespace`, `actor_name`, `actor_uid`, `template_atespace`, `template_name`, `operation`,
`state`, `trace_id` and `span_id` are the view `fleet.actor_events_v`, projected from `data` with
`JSON_VALUE`; project and cluster come from the log entry resource labels. The site queries the
view. Phase 2 adds a second branch to the view for the OTLP record shape.

Phase 2: the collector receives the OTLP log events and routes `event.name` in
`{ate.actor.state_changed, ate.actor.crashed}` to `googlecloudpubsub` with no sampling and no
filtering processor on that pipeline, and `ate.actor.usage_sampled` to GCS only.

## Collector pipelines, `manifests/telemetry/collector.yaml`

| Pipeline | Receivers                                                                  | Exporters                                                                                                                                |
| -------- | -------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| traces   | `otlp` (4317, 4318)                                                        | `otlphttp` to `https://telemetry.googleapis.com`, `google_cloud_storage` (prefix `v1/traces`, gzip, partition `year=%Y/month=%m/day=%d`) |
| metrics  | `prometheus` scraping `ate-env-api` only when PodMonitoring is unavailable | `googlemanagedprometheus` or none                                                                                                        |
| logs     | `otlp`                                                                     | `googlecloudpubsub` for the two lifecycle events; `google_cloud_storage` for usage samples                                               |

The collector's service account holds `roles/cloudtrace.agent`, `roles/storage.objectCreator` on the
bucket and `roles/pubsub.publisher` on the topic.

## Site, `cmd/ate-env-fleet-site`

Go, standard library HTTP, server-rendered HTML with inline SVG and small inline scripts, no
external assets, the same zero-dependency style as the explorer plan and `build_report.py`.

Runs page sources: `v1/runs/` in the inventory bucket for trainer run reports, and the benchmark
buckets in `FLEET_BENCH_BUCKETS` (comma-separated), where a run is a folder whose name passes
`bench`'s `ValidateGCSFolderName` and whose `run_window.json`, `report.json` and
`system_description.json` the page reads.

| Route                                                    | Returns                                                                                    |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `GET /`                                                  | Fleet page                                                                                 |
| `GET /clusters/{project}/{cluster}`                      | Cluster page                                                                               |
| `GET /actors/{uid}`                                      | Actor page                                                                                 |
| `GET /runs`, `GET /runs/{id}`                            | the run index; one run, where `{id}` is a trainer `run_id` or a bundle folder name (FS-11) |
| `GET /live`                                              | last 200 transitions                                                                       |
| `GET /api/v1/clusters`                                   | the index built from `latest.json` objects                                                 |
| `GET /api/v1/clusters/{project}/{cluster}`               | that snapshot                                                                              |
| `GET /api/v1/actors/{uid}/events?since=`                 | rows from `actor_events`                                                                   |
| `GET /api/v1/metrics/{name}?cluster=&namespace=&window=` | a named PromQL query from an allow-list, never free-form                                   |
| `GET /healthz`                                           | 200                                                                                        |

Auth: IAP in front; the service verifies `x-goog-iap-jwt-assertion` against the IAP audience on
every request and reads the user email from it. Service account roles: `roles/storage.objectViewer`
on the bucket, `roles/bigquery.dataViewer` and `roles/bigquery.jobUser` on the events dataset,
`roles/monitoring.viewer` on the metrics scope. No other role.

Caching: snapshots cached in memory per instance for 30 s and revalidated by object generation;
PromQL results cached 10 s; event queries bounded to 24 h by default.

## Named PromQL queries

| Name                  | Query                                                                                                                                                                                             |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `requests_rate`       | `sum by (cluster, namespace, method) (rate(ate_env_api_requests_total[1m]))`                                                                                                                      |
| `create_to_ready_p50` | `histogram_quantile(0.5, sum by (cluster, namespace, le) (rate(ate_env_api_create_to_ready_seconds_bucket[5m])))`                                                                                 |
| `wake_wait_p95`       | `histogram_quantile(0.95, sum by (cluster, namespace, le) (rate(ate_env_api_wake_wait_seconds_bucket[5m])))`                                                                                      |
| `environments`        | `max by (cluster, namespace, atespace, state) (ate_env_api_environments)` (each replica reports the whole count)                                                                                  |
| `workers`             | `sum by (cluster, ate_workerpool_namespace, ate_workerpool_name) (max by (cluster, ate_workerpool_namespace, ate_workerpool_name, ate_worker_state, ate_sandbox_class) (ate_workerpool_workers))` |
| `crashes_rate`        | `sum by (cluster, ate_template_name) (rate(ate_actor_crashes_total[5m]))`                                                                                                                         |

Metric names on the Cloud Monitoring side follow the OTLP-to-Prometheus mapping; the lead confirms
each name against the metrics explorer before the allow-list ships.

## Terraform, `deploy/fleet-site/`

Modules: `central` (bucket, lifecycle, topic, subscriptions, dataset, Cloud Run, IAP, service
accounts) applied once; `cluster` (sink, reporter and collector service accounts, Workload Identity
bindings) applied per cluster project. Inputs: `central_project`, `bucket`, `iap_group`,
`clusters = [{project, name, location}]`.
