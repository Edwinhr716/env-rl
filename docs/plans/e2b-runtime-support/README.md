# Plan: E2B runtime support

Status: proposal, 2026-11-09; last checked 2026-11-13 against `main` at `915894b6` and pull request
2 (#365) at `2e8efa07`. Nothing in this plan is merged. Pull requests 0, 1 and 2 are open (#363,
#364, #365), and section 5 has the results of its checks on Kind (2026-11-11). Checked against `e2b`
2.63.0 and `e2b-code-interpreter` 2.11.9 (PyPI), envd 0.10.2 (`e2b-dev/runtime`, `packages/envd`),
AgentENV `main` at `034ea2c`, and `kubernetes-sigs/agent-sandbox` `main` at `580cb3e6`. The criteria
are in [acceptance-criteria.md](./acceptance-criteria.md).

## 1. Goal

A program written for the E2B Python SDK runs its sandboxes as ate-env environments. The only change
to the program is the import of the sandbox class:

```python
# before
from e2b import Sandbox

# after
from ate_env_e2b import Sandbox
```

`sandbox.commands`, `sandbox.files` and `sandbox.pty` keep E2B's signatures, return types and
exceptions. The endpoint, the token and the atespace come from the environment variables the other
integrations already use (`SUBSTRATE_ENV_API`, `SUBSTRATE_ENV_AUTH_TOKEN`,
`SUBSTRATE_ENV_ATESPACE`).

Non-goals:

- The E2B JavaScript SDK and the `e2b` CLI. They need a server that speaks E2B's REST API (option B
  in section 3).
- E2B's template build API (`Template.build`), forks, named snapshots, volumes and the MCP gateway.
- E2B's cloud as a backend for `SandboxFleet`.

## 2. Research

### 2.1 E2B

- The SDK has two planes. The control plane is REST (`POST /v2/sandboxes`, `DELETE /sandboxes/{id}`,
  `POST /sandboxes/{id}/pause`, `POST /v2/sandboxes/{id}/connect`, `GET /v2/sandboxes`) with an
  `X-API-KEY` header. The data plane is `envd`, a daemon inside each sandbox on port 49983.
- `envd` serves two Connect-RPC services (`process.Process`, `filesystem.Filesystem`) and HTTP
  routes (`/files`, `/health`, `/init`, `/metrics`). The SDK's `Commands`, `Pty` and `Filesystem`
  classes are built on these and have no transport seam: replacing envd means rewriting them.
- `envd` is one static Go binary (`CGO_ENABLED=0`), Apache-2.0. With `-isnotfc` it runs as a plain
  container process without Firecracker; `-no-cgroups` removes the cgroup dependency.
- `/init` sets the access token, the environment variables, the default user and the default working
  directory. E2B's orchestrator calls it; the SDK never does.
- The SDK reaches envd at `https://49983-<sandbox_id>.<domain>`, or, when
  `ConnectionConfig.sandbox_url` is set, at that one URL with the routing in headers. The config
  also takes `extra_sandbox_headers`, sent on each envd call.
- `Sandbox.__init__(sandbox_id, envd_version, envd_access_token, connection_config, ...)` builds a
  working object without any REST call. The lifecycle lives in classmethods (`_create`,
  `_cls_connect_sandbox`, `_cls_kill`, `_cls_pause`, `_cls_get_info`, `_cls_set_timeout`, `list`).
  Instance methods call `SandboxApi._cls_*` by class name, thus a subclass must override the
  instance methods too.
- `e2b_code_interpreter.Sandbox` subclasses `e2b.Sandbox`. `run_code` posts to a FastAPI server on
  port 49999 that drives a Jupyter server; it does not use envd.
- The SDK is MIT, needs Python 3.11 or newer, and marks `sandbox_url` and `api_url` as deprecated
  for removal in the next major version.

### 2.2 AgentENV

AgentENV (`kvcache-ai/AgentENV`) supports the unmodified E2B SDK from the server side:

- Its gateway serves E2B's REST API (a superset of E2B's OpenAPI spec) and generates `e2b_`-prefixed
  keys so the SDK accepts them.
- Each microVM runs unmodified upstream envd, built from a pinned `e2b-dev/runtime` ref. It sits on
  a separate tools drive, thus the user image is not changed.
- Users set `E2B_API_URL` and `E2B_SANDBOX_URL` to the gateway. The gateway forwards any request
  that carries `E2b-Sandbox-Id` and `E2b-Sandbox-Port` to that port of that sandbox. Wildcard
  hostnames are optional and off by default.
- An E2B template ID resolves to an AgentENV template, which is one snapshot of an OCI image.
- Its code-interpreter suite is disabled, and its template build API rejects `COPY`.

Lesson: the REST layer is thin. The data plane is the expensive part, and running real envd removes
it.

### 2.3 agent-sandbox

- No E2B-compatible API or SDK adapter exists, merged or in an open pull request. Issues #235 and
  #1154 ask for one; both are stale.
- `examples/envd-sandbox` runs upstream envd with `--isnotfc --no-cgroups` on port 49983 in a plain
  pod, and drives its HTTP routes. It proves envd runs in a container without Firecracker.
- Each framework integration there (OpenHands, DeepAgents, OpenAI Agents SDK, NeMo Gym) subclasses
  the framework's own class. `AgentSandboxWorkspace` is the model `AteEnvWorkspace` already mirrors.

### 2.4 What sandboxenv-rl has

| E2B needs                          | sandboxenv-rl on `main`                                                                  |
| ---------------------------------- | ---------------------------------------------------------------------------------------- |
| envd inside any image              | Runtime layers: an image volume plus a guest sidecar with a readiness URL                |
| One URL, header routing to a port  | The port proxy's header form (`X-Env-Id`, `X-Env-Atespace`, `X-Env-Port`), streaming     |
| A credential the daemon never sees | `X-Env-Authorization`; the proxy checks and removes it, and forwards `Authorization`     |
| create, kill, pause, resume        | `CreateEnvironment`, `DeleteEnvironment`, `SuspendEnvironment`, wake on the next request |
| Template ID to image               | A registered template by name, or `rootfs_image` on a base template (derived template)   |
| List, metadata                     | Missing: no list RPC, no labels on an environment                                        |
| Timeout (kill after N seconds)     | Missing: no time to live (#76 covers auto-suspend only)                                  |
| `get_host(port)` hostname          | Missing: the port proxy has header and path forms, no hostname form                      |
| Egress rules                       | Missing: no per-environment network policy                                               |

The guest's own file API has only read and write (#78). This plan does not need more, because envd
serves the file calls.

## 3. Options

| Option                                                              | Server change                                                          | Covers                               | Cost                                                                                        |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------- |
| A. Subclass of `e2b.Sandbox`, envd as a runtime layer (recommended) | none for the core; D-8 can add an opt-in answer form to the port proxy | Python SDK, sync and async           | One small package; lifecycle overrides track the SDK's private classmethods                 |
| B. E2B REST API on ate-env-api, envd as a runtime layer             | a new REST surface                                                     | Python, JS, CLI, with no code change | A second public API to keep compatible; `sandbox_url` removal forces wildcard DNS and TLS   |
| C. Duck-typed class over `ate_env_rl`, no envd                      | file RPCs (#78), user                                                  | Python, only the methods we rewrite  | Rewrites `commands`, `files`, `pty` and their streaming semantics; drifts with each release |

Option A is the request ("only swapping the python class") and the pattern of
`integrations/openhands`: subclass the framework's class, inject its daemon as a layer, reach it
through the port proxy. Option A's layer is also the data plane of option B, thus B stays possible
later. Option C is rejected: it is the most code and the least compatible.

## 4. Design

### 4.1 The envd runtime layer

`integrations/e2b/layer` builds an image that holds the envd binary, built from a pinned
`e2b-dev/runtime` commit with its Apache-2.0 licence. Upstream tags no envd release after
`envd-v0.6.13` and has no notice file, thus the pin is the commit that released the version. A base
template mounts it:

```bash
ate-env manifest template --template e2b-base \
  --rootfs-image   <default image>@sha256:<digest> \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> \
  --layer          envd=<registry>/ate-env-e2b-envd@sha256:<digest>=/opt/e2b \
  --sidecar        '/opt/e2b/envd -isnotfc -no-cgroups -port 49983' \
  --sidecar-readyz http://127.0.0.1:49983/health \
  --add-capability SETUID --add-capability SETGID --add-capability CHOWN \
  --add-capability DAC_OVERRIDE --add-capability FOWNER
```

envd sets the user, the group and the groups of each process it starts, also for root. An actor's
container has only `AUDIT_WRITE`, `KILL` and `NET_BIND_SERVICE`, thus without `SETUID` and `SETGID`
no process starts. `CHOWN` is for a file written as another user; `DAC_OVERRIDE` and `FOWNER` let
root read and re-mode the files of the other users. `--add-capability` is pull request 0 (#363).

The rootfs image needs `/bin/sh`, which envd starts each process through, and `/bin/bash`, which the
SDK runs each command and each PTY in. envd itself is static and runs on a musl image.

Template derivation keeps the layer, thus an environment on any `rootfs_image` starts envd. envd
needs an image whose `USER` is root or not set (section 5, item 6). The golden snapshot of a derived
template has envd already listening.

### 4.2 The class

Package `ate-env-e2b` in `integrations/e2b`, import `ate_env_e2b`, depending on `ate-env-rl` and
`e2b>=2.63,<3`. It exports `Sandbox(e2b.Sandbox)` and `AsyncSandbox(e2b.AsyncSandbox)`, and
re-exports E2B's exceptions and result types so one import line changes.

`create`:

1. Resolve the template (section 4.4) and call `CreateEnvironment` with a generated id.
2. Wait for envd's `/health` through the port proxy. Delete the environment if the wait fails.
3. `POST /init` with the access token and `envs`. The class sends no `defaultUser` and no
   `defaultWorkdir`: commands run as `root` in `/root`, which is envd's default, and `user=` and
   `cwd=` on a call change them (D-4). `e2b.Sandbox.create` has no `user` or `cwd` argument.
4. Build the object with
   `ConnectionConfig(sandbox_url=<SUBSTRATE_ENV_API>, extra_sandbox_headers=...)`. The headers are
   `X-Env-Id`, `X-Env-Atespace`, `X-Env-Port: 49983`, `X-Env-Authorization` and `X-Access-Token`.

From there the SDK's own `commands`, `files` and `pty` run unchanged against real envd. The sync
class reaches `ate_env_rl` through `LoopRunner`, never `asyncio.run()`. For an `https://` endpoint
the class passes `credentials=grpc.ssl_channel_credentials()` to `ate_env_rl.Client`, which then
opens the secure channel itself and keeps its auth and trace interceptors (#360). Pull request 2
(#365) predates that argument: it opens a `grpc.aio.secure_channel` and passes it as `channel`, as
the NeMo Gym provider does, and changes to `credentials` when it takes `main`.

The SDK expects E2B's proxy to answer `502` with `was not found` for a sandbox that is gone, and
maps that answer to `SandboxNotRunningException` on the HTTP path and on the Connect path
(`envd/api.py`, `envd/rpc.py`). The port proxy answers `404` for a deleted or deleting environment,
`410` for a crashed one, and `503` with `Retry-After: 1` when a suspended one wakes longer than the
router's timeout (`internal/portproxy/portproxy.go`); it does not send the request again. On `404`
the stock `is_running` raises `NotFoundException`, `files.exists` returns `False` and `files.read`
raises `FileNotFoundException`. The SDK retries no status on the data plane: its envd clients use
`ConnectionRetryTransport`, which retries connection errors only (the `429`, `502`, `503` and `504`
retry of `e2b/retry.py` is on the REST client), thus neither a `502` nor a `503` during a wake is
tried again. A `/health` probe wakes a suspended environment.

The class therefore reads `GetEnvironment` before envd in `is_running`, `get_info` and `pause`; pull
request 2 (#365) has this. The data plane needs two more behaviours, and #365 has neither:

1. A call on a deleted or crashed environment raises `SandboxNotRunningException` (E2B-16, E2B-19).
2. A call during a wake waits and then succeeds (E2B-16, E2B-17, E2B-20).

The SDK builds its HTTP client and its Connect clients in module-level factories (`get_envd_api`,
`create_rpc_client`) on a shared transport pool, thus no per-sandbox transport exists to replace.
Decision D-8 picks the mechanism, and it must cover both behaviours:

- In the class: swap the private clients of each object for ones on a translating transport. `404`
  and `410` raise `SandboxNotRunningException` with the proxy's message, and `503` is retried after
  `Retry-After` until `request_timeout`. No server change.
- In the port proxy: for a request that asks for it, answer `502` with `was not found` for a
  deleted, deleting or crashed environment, and hold a request to a waking environment until the
  environment serves or the wake budget ends. A `502` or a `503` for a waking environment is not
  enough, because the SDK retries neither.

Until that pull request, a data-plane call on a gone, crashed or waking environment gets the port
proxy's answer through the SDK's stock mapping.

The envd access token is an HMAC, keyed with the API token, of `f"{atespace}/{env_id}"`; `/` is in
neither label, thus two pairs never share an input. `Sandbox.connect(sandbox_id)` in another process
derives it again, thus no store is needed. Without an API token there is no envd token, the same
trust level as the port proxy without auth.

### 4.3 Method mapping

| E2B                                                                                                       | ate-env                                                                                                                                                                                                                                                                               | Phase |
| --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| `create`, `with Sandbox.create()`                                                                         | `CreateEnvironment`, health wait, `/init`                                                                                                                                                                                                                                             | 1     |
| `kill` (returns `False` when gone)                                                                        | `DeleteEnvironment`                                                                                                                                                                                                                                                                   | 1     |
| `pause`                                                                                                   | `GetEnvironment`, then `SuspendEnvironment` durable; `False` when already suspended; `mode="filesystem"`, `keep_memory=False` and `strict=True` raise                                                                                                                                 | 1     |
| `beta_pause`                                                                                              | `pause` (the SDK's own alias)                                                                                                                                                                                                                                                         | 1     |
| `connect` (resumes a paused sandbox)                                                                      | `GetEnvironment`, then the health wait, which wakes it; `on_resume="reboot"` raises                                                                                                                                                                                                   | 1     |
| `get_info`, `is_running`                                                                                  | `GetEnvironment` and status mapping first; envd `/health` only on a running environment                                                                                                                                                                                               | 1     |
| `commands.*`, `pty.*`, `files.*`, `git.*`                                                                 | inherited: envd through the port proxy (`sandbox.git` runs its commands through `commands`; the SDK deprecates it)                                                                                                                                                                    | 1     |
| `envs`, `user`, `cwd`                                                                                     | `envs` of `create` through envd `/init`; `envs`, `user=` and `cwd=` of a call through envd's own per-call handling; the default is `root` in `/root`                                                                                                                                  | 1     |
| `AsyncSandbox`                                                                                            | the same overrides on the async class                                                                                                                                                                                                                                                 | 2     |
| `e2b_code_interpreter.Sandbox.run_code`, `*_code_context`                                                 | a second layer (the code-interpreter server, port 49999); `_jupyter_url` overridden to the path form `/v1alpha/atespaces/<atespace>/envs/<id>/ports/49999` and `_client` to send `X-Env-Authorization`, because the stock client sends `E2b-Sandbox-Port` without the sandbox headers | 3     |
| `timeout`, `set_timeout`                                                                                  | needs a time to live on the environment                                                                                                                                                                                                                                               | 4     |
| `metadata`, `Sandbox.list`                                                                                | needs labels and a list RPC                                                                                                                                                                                                                                                           | 4     |
| `get_host`, `download_url`, `upload_url`                                                                  | needs a hostname form on the port proxy                                                                                                                                                                                                                                               | 4     |
| `allow_internet_access=False`, `network`, `update_network`                                                | needs per-environment egress policy in Substrate                                                                                                                                                                                                                                      | none  |
| `fork`, `create_snapshot`, `list_snapshots`, `delete_snapshot`, `get_metrics`                             | no equivalent                                                                                                                                                                                                                                                                         | none  |
| `volume_mounts`, `mcp`, `get_mcp_url`, `get_mcp_token`, `iam`, `lifecycle`, `create(idempotency_key=...)` | no equivalent                                                                                                                                                                                                                                                                         | none  |

A method or argument without an implementation raises E2B's `SandboxException` and names
`integrations/e2b/README.md`, the user's document of the package. It never does nothing. This
matters most for the egress arguments: a program that asks for no internet access must not get an
open network.

Until phase 4, `timeout` is the one exception: the default E2B program passes none and relies on
`kill` or the context manager. Each `create` or `connect` call with a `timeout` emits a
`TimeoutNotEnforcedWarning` (a `UserWarning`, through `warnings.warn`, not `logging`), because no
server enforces the timeout, and `set_timeout` raises.

### 4.4 Templates

E2B's `template` is an opaque string that the server resolves. Here:

1. A name in the `templates` mapping (`SUBSTRATE_ENV_E2B_TEMPLATES`, `name=template` pairs) uses
   that registered template.
2. A digest-pinned image reference (`repo@sha256:...`) runs as `rootfs_image` on the base template
   (`SUBSTRATE_ENV_TEMPLATE`, default `e2b-base`).
3. No template, or E2B's default `base`, uses the base template itself.
4. Anything else raises `TemplateException`.

In each case the image must run as root: its `USER` is root or not set (section 5, item 6).

`e2b.Dockerfile` templates are built and pushed with Docker, then used by digest. E2B's start
command has no equivalent yet; a start command is a second `--sidecar` on a registered template.

## 5. Risks to check first

Each is a criterion in group V. Phase 1 starts with them, on Kind, before the class is written.
Results are from 2026-11-11: Kind, gVisor, Substrate `274605f1`, envd 0.10.2, `e2b` 2.63.0. Items 1
to 3 are the Kind test of pull request 1 (#364); item 4 was checked by hand.

1. envd under gVisor: starts with `-isnotfc -no-cgroups`, survives suspend and restore, and its
   process table and watchers are correct after a restore from the golden snapshot. **Result:**
   holds, with the five capabilities of section 4.1. Without `SETUID` and `SETGID` envd answers
   `/health` and each process start fails with `operation not permitted`. A process and a watcher
   survive a suspend and the wake. envd runs in a glibc image and in Alpine.
2. Users: envd looks up its default user in the image. `/init` must set a user that exists in an
   arbitrary task image (the image's user, else root). **Result:** envd's own default is `root`.
   `/init` with `defaultUser` changes it, and one call can name another user. envd starts a command
   in the home of its user and refuses the command when the home does not exist (`nobody` on
   Debian). Pull request 2 (#365) sends neither field: the default stays `root` in `/root`. A
   default user other than root needs `defaultWorkdir` with it.
3. `/init` repeat: whether a second `/init` after a resume or a `connect` is accepted, and what
   protects an envd that has no token yet. **Result:** the first `/init` with a token sets it. After
   that envd answers `401` to a call without the token and to an `/init` with another token or none,
   and accepts an `/init` with the same token: the derived token of section 4.2 can be sent again
   after each resume and each `connect`. Before the first `/init`, the port proxy's caller token is
   the only guard, and a process inside the environment can reach envd on localhost and set a token
   first.
4. Transport: the port proxy speaks HTTP/1.1 to the daemon. Check that the SDK's Connect client
   (default `E2B_HTTP_VERSION=2`) streams output and input through ate-env-api's listener. The SDK's
   pyqwest transport negotiates HTTP/2 through TLS ALPN only and sends HTTP/1.1 on plaintext
   `http://`, thus the HTTP/2 check needs TLS in front of `ate-env-api`. **Result:** holds on both.
   The stock SDK, built with `sandbox_url` and `extra_sandbox_headers`, runs commands, gets stdout
   as it arrives, sends stdin, drives a PTY, watches a directory and moves a 100 MiB file each way
   with a matching sha256, on plaintext HTTP/1.1 and on HTTP/2 through a TLS front. The first
   command after a suspend wakes the environment, and a background process is still listed.
5. `sandbox_url` removal in `e2b` 3: the fallback is a `ConnectionConfig` subclass that overrides
   `get_sandbox_url` and `get_sandbox_direct_url`: `SandboxBase` builds `envd_api_url` and
   `envd_direct_url` from them, `_file_url` reads the direct one, and the code interpreter calls
   `get_sandbox_url` with port 49999. The drift check in section 7 watches for it. **Result:** not
   checked; no `e2b` 3 exists.

The checks added three risks:

6. The image's `USER`. The Substrate pin (`274605f1`) contains `5d226a50`, thus a container runs as
   the image's `USER`. Substrate sets no ambient capabilities (`internal/ocispec/ocispec.go`), thus
   a non-root process has no effective capability and `--add-capability` does not help it;
   Substrate's own test wants `CapEff` 0 for a non-root `USER` (`internal/e2e/suites/imagefs`). The
   checks of items 1 to 4 used images that run as root. Not run on Kind (E2B-34): an image with a
   non-root `USER`. The expected result is that envd answers `/health`, thus `create` succeeds, and
   that each process start fails with `operation not permitted`, as for root without `SETUID` and
   `SETGID` (item 1). Phase 1 therefore requires an image whose `USER` is root or not set, and
   `integrations/e2b/README.md` must name this limit. A flag on `ate-env manifest template` cannot
   run the container as root: the actor template of the pin has no field for the user, and its
   `SecurityContext` holds capabilities only. Decision D-9 has the options for such an image and
   their order.
7. `/bin/bash`. The SDK runs each command and each PTY in `/bin/bash -l -c`. An image without it
   (Alpine) serves the file calls and fails each command. The class must say so in the error, not
   pass envd's `nice: can't execute '/bin/bash'` through.
8. TLS trust. The SDK verifies an `https://` endpoint against the system certificate store only: it
   has no option for a CA file, and on macOS it ignores `SSL_CERT_FILE`. An ate-env-api behind a
   private CA needs that CA in the system store of the client.

## 6. Phases and pull requests

| Phase | Pull request                                                                                                                                                                                                                                             | Depends on |
| ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| 1     | 0. `ate-env manifest template --add-capability` (#363)                                                                                                                                                                                                   |            |
| 1     | 1. The envd layer, its template script, and the group V checks as a Kind test (#364); the Kind check of an image with a non-root `USER` (E2B-34) goes into the same test                                                                                 | 0          |
| 1     | 2. `ate_env_e2b.Sandbox`: lifecycle, refusals, unit tests against a fake ate-env-api, CI job and Nix shell, README and integrations table (#365)                                                                                                         | 1          |
| 1     | 3. The data-plane error mapping and the wake handling that D-8 picks (E2B-16, E2B-17, E2B-19, E2B-20), the refusal of an image with a non-root `USER` in `create` (E2B-35), live E2E test with the class on Kind, the conformance script, `examples/e2b` | 2          |
| 2     | 4. `AsyncSandbox`                                                                                                                                                                                                                                        | 2          |
| 3     | 5. Code-interpreter layer and `ate_env_e2b.code_interpreter.Sandbox`                                                                                                                                                                                     | 2          |
| 4     | 6. Environment time to live: API field, enforcement, `timeout` and `set_timeout`                                                                                                                                                                         | 2          |
| 4     | 7. Environment labels and `ListEnvironments`: `metadata`, `Sandbox.list`                                                                                                                                                                                 | 2          |
| 4     | 8. Hostname form on the port proxy: `get_host`, signed file URLs                                                                                                                                                                                         | 2          |
| 5     | 9. An image with a non-root `USER` runs as its own user: the layer's envd makes no credential change for that user, and the class refuses `user=` for another one (D-9 option 3, E2B-36)                                                                 | 1, 3       |

Phase 4 items change `proto/ateenv/v1alpha` and are useful beyond E2B (the limits in the READMEs of
`integrations/openhands` and `integrations/nemo-gym` name the missing time to live, and those of
`integrations/openhands` and `integrations/harbor` the missing labels). Each gets its own issue
before work starts. Option B, the REST surface, is a later decision and reuses the layer and
phase 4.

## 7. Drift check for a new `e2b` release

1. Download the wheel into a directory of its own. Read it; do not import it.
2. Diff `sandbox/main.py`, `sandbox_sync/main.py`, `sandbox_async/main.py`,
   `sandbox_sync/sandbox_api.py` and `sandbox_async/sandbox_api.py` against the last checked
   version: new or renamed `_cls_*` classmethods, new `create` arguments, changes to `SandboxOpts`
   and `SandboxBase`.
3. Diff `connection_config.py` (`sandbox_url`, `extra_sandbox_headers`, `get_sandbox_url`,
   `get_sandbox_direct_url`) and `envd/api.py`, `envd/rpc.py` and `envd/client_shared.py` (the
   status-code mapping of section 4.2).
4. Diff `envd/versions.py` against the pinned envd version, and bump the layer when the SDK gates a
   feature on a newer envd.
5. Run the conformance test (E2B-25) with the new version, then move the upper bound.
6. When the layer carries a change to envd (D-9 option 3, E2B-36): check that the change still
   applies to the pinned envd version after a bump of step 4, and run its Kind check again.

## 8. Open decisions

| ID  | Decision                                                                                                                                                                                                                                                                                                                                                                                                   | Recommendation                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D-1 | Option A, B or C                                                                                                                                                                                                                                                                                                                                                                                           | A now; B later if the JS SDK or the CLI is needed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| D-2 | `timeout` before phase 4: warn, or refuse a non-default value                                                                                                                                                                                                                                                                                                                                              | Warn, with a `TimeoutNotEnforcedWarning` on each call as in pull request 2 (#365); refuse `set_timeout`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| D-3 | envd token: derived HMAC, or random and stored                                                                                                                                                                                                                                                                                                                                                             | Derived, until environments carry labels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| D-4 | Default user: root, the image's user, or require `user`                                                                                                                                                                                                                                                                                                                                                    | Root in `/root`, envd's default, as in pull request 2 (#365). The image's user is a later change: it needs `defaultWorkdir`, and at the pin envd does not work in an image with a non-root `USER` (section 5, item 6)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| D-5 | `pause` mode: durable, or `SUSPEND_MODE_LOCAL`                                                                                                                                                                                                                                                                                                                                                             | Durable; E2B programs expect a paused sandbox to survive                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| D-6 | Code interpreter in this series, or after a user asks                                                                                                                                                                                                                                                                                                                                                      | In the series, after phase 2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| D-7 | Python floor 3.11 (the SDK's) in a repository whose client is 3.10                                                                                                                                                                                                                                                                                                                                         | Accept 3.11 for this package only                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| D-8 | How a call on a gone, crashed or waking environment behaves as on E2B (section 4.2): the class swaps the private clients of each object for ones on a translating transport that also retries `503`, or the port proxy, for a request that asks for it, answers a gone or crashed environment in E2B's dialect (`502`, `was not found`) and holds a request to a waking environment until it serves        | The port proxy, with both parts: no coupling to SDK internals, and it also serves a later REST surface (option B). The `502` answer alone leaves a long wake as an error, because the SDK retries no status on the data plane                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| D-9 | An image whose `USER` is not root (section 5, item 6): (1) phase 1 takes root images only and `create` refuses another image; (2) Substrate gets a run-as-user field on the actor template, and `ate-env manifest template` a `--user` flag; (3) the envd of the layer starts a process without a credential change when the target user is the user it runs as; (4) ambient or file capabilities for envd | 1 now, with the refusal in `create` and the Kind check (E2B-34, E2B-35). 3 next, as a pull request of its own after the Kind check shows what envd does on such an image (E2B-36): the image then works as its own user only, `user=` for another user is refused, and the drift check gets the patch to follow. The change must also make that user envd's default user and its home the default working directory (in `/init`, or when envd does not run as root): envd's built-in default user is `root`, and a file call without a user resolves to the default user before it changes the owner of the file. 2 is the full answer, with `user="root"` and `user="user"` as on E2B; it is requested in agent-substrate/substrate#3245 and needs a pin bump. Not 4: Substrate sets no ambient capabilities (agent-substrate/substrate#1187 asks for them), file capabilities need extended attributes that an image layer can lose, and ambient capabilities give each process of the image more than it needs |
