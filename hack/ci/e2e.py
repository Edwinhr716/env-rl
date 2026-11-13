#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end integration test pipeline for sandboxenv-rl.

What it does:
  1. Validates Docker daemon access and locates/clones agent-substrate/substrate.
  2. Builds kubectl-ate and ate-env CLI binaries and places them in bin/.
  3. Provisions target cluster (Kind by default with local registry, or reuses existing).
  4. Deploys Agent Substrate control plane components (if provisioning fresh cluster).
  5. Compiles container images (ateworker-gvisor, ate-env-api, ate-env-guest, ate-env-reporter) using ko to target registry.
  6. Applies Kubernetes manifests (atespace, access policy, caller auth Secret, WorkerPool, and ate-env-api with metrics).
  7. Registers the ActorTemplate (snapshots under the cluster snapshot location) with an environment marker, and waits for its golden snapshot.
  8. Builds the envd runtime layer (integrations/e2b/layer) and registers the E2B base template with it on the rootfs image.
  9. Forwards a free local port and runs portable smoke test, Python SDK full-stack tests, Go SDK full-stack tests, NeMo Gym provider live test, envd runtime layer live test, swebench-loader Go SDK smoke test, and Harbor provider live trial test.
  10. Grants ate-env-reporter the global viewer role, deploys it and waits for its first snapshot, then runs it on the host with -out-dir for three ticks and accepts the files with TestE2ESnapshotDir.

When it runs:
  - CI: .github/workflows/ci.yaml in job `e2e-kind` on PR and push to main.
  - Wrapped by `make ci-e2e`.
  - Nix app: `nix run .#ci-e2e` (or `nix develop -c ci-e2e`).

How to run:
  - `make ci-e2e` (preferred; auto-selects Nix or Docker)
  - `nix run .#ci-e2e` / `nix develop -c ci-e2e`
  - Direct: `python3 hack/ci/e2e.py`
  Examples:
    make ci-e2e
    KEEP_CLUSTER=1 make ci-e2e
    REUSE_CLUSTER=sbx-e2e-abc123 make ci-e2e
    LOCAL_PORT=18888 REUSE_CLUSTER=sbx-e2e-abc123 make ci-e2e

Prerequisites:
  - Docker daemon running and accessible (socket mounted if inside container).
  - Tools on PATH: go, ko, kind, kubectl, pytest (all provided in nix develop or Dockerfile.ci).
  - Harbor runner: nix (for .#python-env-harbor), HARBOR_PYTEST_BIN, or harbor on PATH.

Environment variables:
  REUSE_CLUSTER                   (unset)             Existing cluster name to run against; never created or deleted
  KEEP_CLUSTER                    0                   Set to 1 to preserve cluster after run completes
  NAMESPACE                       ate-env             Kubernetes namespace for ate-env deployment
  LOCAL_PORT                      (free port)         Local port on host for ate-env-api port-forward (free port by default)
  TEMPLATE_NAME                   default-template    Name of ActorTemplate to register and test
  ATE_ENV_ROOTFS_IMAGE            (target default)    Rootfs image for create-from-image E2E, the envd layer E2E and swebench-loader; empty skips them
  E2B_E2E_MUSL_IMAGE              (target default)    Digest-pinned musl image for the envd layer E2E
  ATE_INGRESS_ALLOWED_SPIFFE_IDS  (auto)              Allowed SPIFFE IDs for router TLS ingress
  PYTEST_BIN                      (auto)              Path to pytest binary or python test runner
  HARBOR_PYTEST_BIN               (auto)              Path to pytest binary with harbor installed (defaults to .#python-env-harbor or host PATH)

Outputs / exit codes:
  - Provisions test cluster and Docker containers; publishes images to target registry.
  - Builds binaries into bin/ and generates golden snapshot.
  - Exits with 0 on pipeline success; non-zero if any provisioning, build, or test step fails.
  - Creates a Kind cluster named sbx-e2e-<8 hex> (random, not configurable; KIND_CLUSTER_NAME is
    ignored) and deletes it on completion or error unless KEEP_CLUSTER=1.

Troubleshooting:
  - See docs/chores/troubleshooting.md for common failure modes (inotify limits,
    GoldenActorCrashed, busy ports, and Substrate module resolution).

See also:
  hack/ci/targets.py, hack/ci/test_smoke.py, hack/ci/ensure-substrate.sh,
  clients/python/tests/e2e/test_full_stack.py, clients/go/e2e_test.go,
  integrations/nemo-gym/tests/test_e2e.py, integrations/harbor/tests/test_e2e.py,
  integrations/e2b/tests/test_envd_e2e.py, integrations/e2b/hack/base-template.sh,
  cmd/benchmarking/swebench-loader, Makefile, nix/ci.nix.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib
from targets import KindTarget

TEMPLATE_MARKER = "injected"
E2B_TEMPLATE = "e2b-base"


def locate_substrate(root: Path) -> Path:
    cmd = ["ensure-substrate"] if shutil.which("ensure-substrate") else [str(root / "hack/ci/ensure-substrate.sh")]
    return Path(lib.run(*cmd).stdout.strip()).resolve()


def build_cli_tools(root: Path, sub_dir: Path) -> tuple[Path, Path]:
    lib.step("Building CLI tools (kubectl-ate, ate-env)...")
    ate_ctl = root / "bin/kubectl-ate"
    ate_env = root / "bin/ate-env"
    for d, out, pkg in [(sub_dir, ate_ctl, "./cmd/kubectl-ate"), (root, ate_env, "./cmd/ate-env")]:
        out.parent.mkdir(parents=True, exist_ok=True)
        lib.run("go", "-C", str(d), "build", "-o", str(out), pkg, capture=False)

    os.environ["PATH"] = f"{root / 'bin'}:{sub_dir / 'bin'}:{os.environ.get('PATH', '')}"
    return ate_ctl, ate_env


def build_images(root: Path, sub_dir: Path, target: KindTarget) -> dict[str, str]:
    lib.step("Building container images using ko...")
    imgs: dict[str, str] = {}
    for key, c_dir, pkg in [
        ("worker", sub_dir, "./cmd/ateworker-gvisor"),
        ("api", root, "./cmd/ate-env-api"),
        ("guest", root, "./cmd/ate-env-guest"),
        ("reporter", root, "./cmd/ate-env-reporter"),
    ]:
        ko_env = dict(
            os.environ,
            KO_DOCKER_REPO=f"{target.registry}/{pkg.split('/')[-1]}",
            KO_DEFAULTPLATFORMS=target.platform,
        )
        out = lib.run("ko", "build", "--bare", pkg, cwd=str(c_dir), env=ko_env).stdout.strip().splitlines()
        imgs[key] = out[-1].strip()
        lib.step(f"  {key}-image: {imgs[key]}")
    return imgs


def merge_viewer_binding(policy: dict, member: str) -> dict | None:
    """The global access policy with member in its viewer binding, or None
    when it is there already. A policy read back with "bindings": null is a
    policy with no binding."""
    bindings = policy.get("bindings") or []
    policy["bindings"] = bindings
    viewer = next((b for b in bindings if b.get("role") == "viewer"), None)
    if viewer is None:
        bindings.append({"role": "viewer", "members": [member]})
        return policy
    members = viewer.get("members") or []
    viewer["members"] = members
    if member in members:
        return None
    members.append(member)
    return policy


def read_global_policy(target: KindTarget, ctl_str: str) -> dict | None:
    """The control plane's global access policy, or None when it has none.
    Any other failure of the read is an error, not a missing policy."""
    got = lib.run(
        ctl_str, f"--context={target.context}", "get", "access-policy", "--global", "-o", "json",
        check=False, capture=True,
    )
    stderr = got.stderr or ""
    if got.returncode != 0:
        if "no access policy" in stderr or "not found" in stderr.lower():
            return None
        raise RuntimeError(f"reading the global access-policy failed: {stderr}")
    if not got.stdout.strip():
        return None
    try:
        return json.loads(got.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"the global access-policy is not JSON: {e}: {got.stdout[:200]}") from e


def ensure_global_viewer_binding(target: KindTarget, ctl_str: str, env_str: str, ns: str) -> None:
    """Merge ate-env-api's global viewer binding (ListPlacementDomains, the
    pre-flight's pool listing) into the control plane's global access policy,
    as the README says: read it, add the member, update it; create it from
    the manifest only when there is none, and when that create says the
    policy exists after all, read and update it."""
    lib.step("Ensuring the global viewer binding for ate-env-api in Substrate...")
    manifest = lib.run(env_str, "manifest", "access-policy", "--global", "--namespace", ns, capture=True).stdout
    found = re.search(r"-\s*(user:\S+)", manifest)
    if not found:
        raise RuntimeError(f"no member in the global access-policy manifest:\n{manifest}")
    member = found.group(1)
    policy = read_global_policy(target, ctl_str)
    if policy is None:
        res = lib.run(
            ctl_str, f"--context={target.context}", "create", "access-policy", "--global", "-f", "-",
            input=manifest, check=False, capture=True,
        )
        if res.returncode == 0:
            return
        if "AlreadyExists" not in (res.stderr or ""):
            raise RuntimeError(f"Failed to create the global access-policy: {res.stderr}")
        policy = read_global_policy(target, ctl_str)
        if policy is None:
            raise RuntimeError("the global access-policy exists but cannot be read")
    merged = merge_viewer_binding(policy, member)
    if merged is None:
        return
    lib.run(
        ctl_str, f"--context={target.context}", "update", "access-policy", "--global", "-f", "-",
        input=json.dumps(merged), capture=True,
    )


def deploy_manifests(
    target: KindTarget,
    ate_ctl: Path,
    ate_env: Path,
    ns: str,
    tmpl: str,
    imgs: dict[str, str],
) -> str:
    ctl_str, env_str = str(ate_ctl), str(ate_env)
    lib.step("Ensuring atespace and access-policy in Substrate...")
    lib.run(ctl_str, f"--context={target.context}", "create", "atespace", ns, check=False)
    pipe_res = lib.pipe(
        [env_str, "manifest", "access-policy", "--namespace", ns],
        [ctl_str, f"--context={target.context}", "apply", "access-policy", "-a", ns, "-f", "-"],
        check=False,
        capture=True,
    )
    if pipe_res.returncode != 0:
        res = lib.pipe(
            [env_str, "manifest", "access-policy", "--namespace", ns],
            [ctl_str, f"--context={target.context}", "create", "access-policy", "-a", ns, "-f", "-"],
            check=False,
            capture=True,
        )
        if res.returncode != 0 and "AlreadyExists" not in (res.stderr or ""):
            raise RuntimeError(f"Failed to create access-policy: {res.stderr}")

    ensure_global_viewer_binding(target, ctl_str, env_str, ns)

    lib.step(f"Ensuring ate-env-api-token secret in namespace {ns}...")
    lib.pipe(
        ["kubectl", "create", "namespace", ns, "--dry-run=client", "-o", "yaml"],
        ["kubectl", f"--context={target.context}", "apply", "-f", "-"],
        check=True,
    )
    tok_res = lib.run(
        "kubectl", f"--context={target.context}", "-n", ns, "get", "secret",
        "ate-env-api-token", "-o", "jsonpath={.data.token}", check=False, capture=True,
    )
    raw_tok = tok_res.stdout.strip() if tok_res.returncode == 0 else ""
    if not raw_tok:
        lib.pipe(
            [env_str, "manifest", "auth-secret", "--namespace", ns],
            ["kubectl", f"--context={target.context}", "apply", "-f", "-"],
            check=True,
        )
        tok_res = lib.run(
            "kubectl", f"--context={target.context}", "-n", ns, "get", "secret",
            "ate-env-api-token", "-o", "jsonpath={.data.token}", check=True, capture=True,
        )
        raw_tok = tok_res.stdout.strip()
    if not raw_tok:
        raise RuntimeError(f"Secret ate-env-api-token in namespace {ns} has no token")
    auth_token = base64.b64decode(raw_tok).decode().strip()
    if not auth_token:
        raise RuntimeError(f"Decoded token from secret ate-env-api-token in namespace {ns} is empty")

    lib.step(f"Applying Kubernetes manifests (namespace/{ns}, WorkerPool, Deployment/ate-env-api)...")
    lib.pipe(
        [
            env_str,
            "manifest",
            "--namespace",
            ns,
            "--template",
            tmpl,
            "--api-image",
            imgs["api"],
            "--worker-image",
            imgs["worker"],
            "--replicas",
            "2",
            "--metrics-port",
            "9090",
            "--port-proxy-atespaces",
            ns,
        ],
        ["kubectl", f"--context={target.context}", "apply", "-f", "-"],
        check=True,
    )
    for target_deploy in ["deploy/ate-env-api", f"deploy/{tmpl}-workerpool"]:
        lib.run(
            "kubectl",
            f"--context={target.context}",
            "-n",
            ns,
            "rollout",
            "status",
            target_deploy,
            "--timeout=180s",
            capture=False,
        )

    lib.step(f"Cleaning stale templates and registering {tmpl} ActorTemplate in Substrate...")
    delete_templates(target, ate_ctl, ns, tmpl)

    lib.pipe(
        [
            env_str,
            "manifest",
            "template",
            "--atespace",
            ns,
            "--template",
            tmpl,
            "--workerpool",
            f"{tmpl}-workerpool",
            "--guest-image",
            imgs["guest"],
            "--env",
            f"E2E_TEMPLATE_MARKER={TEMPLATE_MARKER}",
        ],
        [ctl_str, f"--context={target.context}", "create", "actor-template", "-f", "-"],
        check=True,
    )

    wait_golden(target, ate_ctl, ns, tmpl)
    return auth_token


def delete_templates(target: KindTarget, ate_ctl: Path, ns: str, tmpl: str) -> None:
    """Deletes the template and the templates derived from it, when they exist."""
    ctl_str = str(ate_ctl)
    list_res = lib.run(
        ctl_str, f"--context={target.context}", "get", "actor-template", "-a", ns, "-o", "json",
        check=True, capture=True,
    )
    templates = json.loads(list_res.stdout or "{}").get("actorTemplates", [])
    derived_re = re.compile(rf"{re.escape(tmpl)}-[0-9a-f]{{12}}")
    for item in templates:
        name = item.get("metadata", {}).get("name")
        if name and (name == tmpl or derived_re.fullmatch(name)):
            lib.run(
                ctl_str, f"--context={target.context}", "delete", "actor-template",
                "-a", ns, name, check=True,
            )


def wait_golden(target: KindTarget, ate_ctl: Path, ns: str, tmpl: str) -> None:
    lib.step(f"Waiting for {tmpl} golden snapshot...")

    def check_golden():
        r = lib.run(
            str(ate_ctl),
            f"--context={target.context}",
            "get",
            "actor-template",
            "-a",
            ns,
            tmpl,
            "-o",
            "json",
            check=False,
        )
        if r.returncode == 0 and r.stdout.strip():
            st = json.loads(r.stdout).get("status", {}).get("goldenSnapshotStatus", {})
            if err := st.get("errorMessage"):
                raise RuntimeError(f"Golden snapshot failed: {err}")
            return st.get("goldenTag", {}).get("name")

    tag = lib.wait_until(check_golden, timeout=180.0, interval=2.0, what=f"{tmpl} golden snapshot")
    lib.step(f"Golden snapshot ready: {tag}")


def register_e2b_template(
    root: Path, target: KindTarget, ate_ctl: Path, ate_env: Path, ns: str, tmpl: str, guest_image: str
) -> None:
    """Builds the envd runtime layer and registers the E2B base template with it."""
    lib.step("Building the envd runtime layer (integrations/e2b/layer)...")
    repo = f"{target.registry}/ate-env-e2b-envd"
    with tempfile.TemporaryDirectory() as tmp_dir:
        metadata = Path(tmp_dir) / "metadata.json"
        lib.run(
            "docker", "buildx", "build", "--platform", target.platform, "--push",
            "--provenance=false", "--metadata-file", str(metadata), "-t", repo,
            str(root / "integrations/e2b/layer"),
            capture=False,
        )
        layer_image = f"{repo}@{json.loads(metadata.read_text())['containerimage.digest']}"
    lib.step(f"  envd-layer-image: {layer_image}")

    lib.step(f"Registering {E2B_TEMPLATE} ActorTemplate in Substrate...")
    delete_templates(target, ate_ctl, ns, E2B_TEMPLATE)
    lib.pipe(
        [
            str(root / "integrations/e2b/hack/base-template.sh"),
            layer_image,
            "--atespace",
            ns,
            "--workerpool",
            f"{tmpl}-workerpool",
            "--guest-image",
            guest_image,
            "--rootfs-image",
            target.rootfs_image,
        ],
        [str(ate_ctl), f"--context={target.context}", "create", "actor-template", "-f", "-"],
        env=dict(os.environ, ATE_ENV=str(ate_env)),
        check=True,
    )
    wait_golden(target, ate_ctl, ns, E2B_TEMPLATE)


REPORTER_NS = "ate-env-reporter"


def grant_reporter_viewer(target: KindTarget, ate_ctl: Path, ate_env: Path) -> None:
    """Merges the reporter's member into the global access policy's viewer binding.

    The global policy is one object: a policy that exists is read, extended and
    updated under its uid and version; only a control plane without one gets the
    binding as a new policy.
    """
    ctl = [str(ate_ctl), f"--context={target.context}"]
    wanted = lib.run(str(ate_env), "manifest", "reporter", "--access-policy", "--namespace", REPORTER_NS).stdout
    member = f"user:system:serviceaccount:{REPORTER_NS}:ate-env-reporter"
    # Without a policy kubectl-ate prints nothing to stdout and exits 0.
    got = lib.run(*ctl, "get", "access-policy", "--global", "-o", "json")
    if not got.stdout.strip():
        lib.run(*ctl, "create", "access-policy", "--global", "-f", "-", input=wanted)
        return
    policy = json.loads(got.stdout)
    bindings = policy.setdefault("bindings", [])
    viewer = next((b for b in bindings if b.get("role") == "viewer"), None)
    if viewer is None:
        viewer = {"role": "viewer", "members": []}
        bindings.append(viewer)
    if member in viewer.setdefault("members", []):
        return
    viewer["members"].append(member)
    lib.run(*ctl, "update", "access-policy", "--global", "-f", "-", input=json.dumps(policy))


def run_reporter(root: Path, target: KindTarget, ate_ctl: Path, ate_env: Path, image: str) -> None:
    """FS-14 of the fleet site plan: the reporter against the Kind Substrate.

    In the cluster, the Deployment of `ate-env manifest reporter` turns ready,
    which /readyz allows only after a snapshot was written: the ServiceAccount's
    token, the viewer binding and the ClusterRole all hold. On the host, the
    binary runs with -out-dir under the same ServiceAccount's token, and
    TestE2ESnapshotDir accepts the files.
    """
    lib.step("Granting ate-env-reporter the global viewer role...")
    grant_reporter_viewer(target, ate_ctl, ate_env)

    lib.step("Deploying ate-env-reporter and waiting for its first snapshot...")
    lib.pipe(
        [
            str(ate_env), "manifest", "reporter", "--namespace", REPORTER_NS, "--image", image,
            "--out-dir", "/snap", "--project", "kind", "--cluster", target.cluster_name,
        ],
        ["kubectl", f"--context={target.context}", "apply", "-f", "-"],
        check=True,
    )
    rollout = lib.run(
        "kubectl", f"--context={target.context}", "-n", REPORTER_NS, "rollout", "status",
        "deploy/ate-env-reporter", "--timeout=120s", check=False, capture=False,
    )
    if rollout.returncode != 0:
        lib.run("kubectl", f"--context={target.context}", "-n", REPORTER_NS, "logs", "deploy/ate-env-reporter", "--tail=50", check=False, capture=False)
        raise RuntimeError("ate-env-reporter did not turn ready")

    lib.step("Running ate-env-reporter with -out-dir for three ticks...")
    ctl = [str(ate_ctl), f"--context={target.context}"]
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        binary, token_file, kubeconfig, out_dir = tmp / "ate-env-reporter", tmp / "token", tmp / "kubeconfig", tmp / "snap"
        # ko built the reporter's image with CGO_ENABLED=0 and -trimpath. The
        # Go build cache is keyed by both, so the same two here make this
        # build a link of the packages ko compiled; without them it compiles
        # all of the reporter's dependencies a second time.
        ko_like = dict(os.environ, CGO_ENABLED="0")
        lib.run(
            "go", "-C", str(root), "build", "-trimpath", "-o", str(binary), "./cmd/ate-env-reporter",
            env=ko_like, capture=False,
        )
        token_file.write_text(
            lib.run(
                "kubectl", f"--context={target.context}", "-n", REPORTER_NS, "create", "token", "ate-env-reporter",
                "--audience", "api.ate-system.svc",
            ).stdout.strip()
        )
        kubeconfig.write_text(
            lib.run("kubectl", "config", "view", "--minify", "--flatten", f"--context={target.context}").stdout
        )
        port = pick_port()
        with lib.port_forward(target.context, "ate-system", "svc/api", port, target_port=443):
            proc = subprocess.Popen(
                [
                    str(binary), "-out-dir", str(out_dir), "-project", "kind", "-cluster", target.cluster_name,
                    "-interval", "5s", "-snapshot-interval", "5s",
                    "-ateapi", f"127.0.0.1:{port}", "-ateapi-token-file", str(token_file),
                    "-health-listen-addr", "127.0.0.1:0",
                ],
                env=dict(os.environ, KUBECONFIG=str(kubeconfig)),
            )
            try:
                kept = out_dir / "v1" / "snapshots" / "kind" / target.cluster_name

                def three_kept():
                    if proc.poll() is not None:
                        raise RuntimeError(f"ate-env-reporter exited early ({proc.returncode})")
                    return kept.is_dir() and len(list(kept.glob("*.json"))) >= 3

                lib.wait_until(three_kept, timeout=60.0, interval=1.0, what="three kept snapshots")
            finally:
                lib.stop_process(proc)
            # The counts are read after the reporter stopped: nothing creates
            # or deletes actors at this point of the pipeline.
            actors = json.loads(lib.run(*ctl, "get", "actor", "-A", "-o", "json").stdout or "{}").get("actors", [])
            workers = json.loads(lib.run(*ctl, "get", "worker", "-o", "json").stdout or "{}").get("workers", [])
        lib.run(
            "go", "-C", str(root), "test", "-trimpath", "-v", "-count=1", "./internal/inventory",
            "-run", "^TestE2ESnapshotDir$",
            env=dict(
                ko_like,
                ATE_ENV_REPORTER_E2E_DIR=str(out_dir),
                ATE_ENV_REPORTER_E2E_PROJECT="kind",
                ATE_ENV_REPORTER_E2E_CLUSTER=target.cluster_name,
                ATE_ENV_REPORTER_E2E_ACTORS=str(len(actors)),
                ATE_ENV_REPORTER_E2E_WORKERS=str(len(workers)),
            ),
            capture=False,
        )


def pick_port() -> int:
    if raw := os.environ.get("LOCAL_PORT"):
        port = int(raw)
        if lib.port_open("127.0.0.1", port):
            raise RuntimeError(
                f"LOCAL_PORT={port} is already in use on 127.0.0.1. "
                "Stop the existing listener or unset LOCAL_PORT to let e2e.py pick a free port."
            )
        return port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _candidate_bin_dirs(binary: Path) -> list[Path]:
    return list(dict.fromkeys((binary.absolute().parent, binary.resolve().parent)))


def resolve_harbor_runner(root: Path) -> tuple[list[str], str]:
    if hb_pytest := os.environ.get("HARBOR_PYTEST_BIN"):
        cand = Path(hb_pytest).expanduser()
        resolved = cand if cand.is_file() else Path(shutil.which(hb_pytest) or "")
        if not resolved.is_file():
            raise RuntimeError(f"HARBOR_PYTEST_BIN={hb_pytest} was not found")
        candidate_dirs = _candidate_bin_dirs(resolved)
        for parent_dir in candidate_dirs:
            if (parent_dir / "harbor").is_file():
                return [str(resolved.absolute())], str(parent_dir)
        searched = ", ".join(str(d) for d in candidate_dirs)
        raise RuntimeError(f"harbor binary not found alongside HARBOR_PYTEST_BIN in {searched}")
    if shutil.which("nix"):
        out_paths = (
            lib.run(
                "nix", "build", "--no-link", "--print-out-paths", ".#python-env-harbor",
                cwd=str(root),
            ).stdout.strip().splitlines()
        )
        harbor_env = Path(out_paths[-1])
        return [str(harbor_env / "bin/pytest")], str(harbor_env / "bin")
    if harbor_bin := shutil.which("harbor"):
        candidate_dirs = _candidate_bin_dirs(Path(harbor_bin))
        for harbor_dir in candidate_dirs:
            if (harbor_dir / "pytest").is_file():
                return [str(harbor_dir / "pytest")], str(harbor_dir)
        for harbor_dir in candidate_dirs:
            if (harbor_dir / "python").is_file():
                return [str(harbor_dir / "python"), "-m", "pytest"], str(harbor_dir)
        searched = ", ".join(str(d) for d in candidate_dirs)
        raise RuntimeError(f"Neither pytest nor python found alongside harbor in {searched}")
    raise RuntimeError(
        "Harbor live E2E requires Nix (.#python-env-harbor), HARBOR_PYTEST_BIN, or harbor on PATH"
    )


def run_tests(
    root: Path,
    target: KindTarget,
    ate_env: Path,
    ns: str,
    tmpl: str,
    port: int,
    auth_token: str,
    harbor_py: list[str],
    harbor_bin_dir: str,
) -> None:
    lib.step(f"Running test suites under port-forward on 127.0.0.1:{port}...")
    with lib.port_forward(target.context, ns, "svc/ate-env-api", port):
        test_env = dict(
            os.environ,
            KUBECTL_CONTEXT=target.context,
            NAMESPACE=ns,
            LOCAL_PORT=str(port),
            ATE_ENV_API_TARGET=f"127.0.0.1:{port}",
            SUBSTRATE_ENV_API=f"127.0.0.1:{port}",
            ATE_ENV_TEMPLATE=tmpl,
            ATE_ENV_BIN=str(ate_env),
            ATE_ENV_ROOTFS_IMAGE=target.rootfs_image or "",
            SUBSTRATE_ENV_AUTH_TOKEN=auth_token,
            ATE_ENV_EXPECT_METRICS="1",
            ATE_ENV_EXPECTED_MARKER=TEMPLATE_MARKER,
            ATE_ENV_EXPECT_PORT_PROXY_ALLOWLIST="1",
            # Editable .pth files in nix/python.nix:65 resolve workspace packages from
            # $REPO_ROOT at run time, and only pureEnvSetup exports it.
            REPO_ROOT=str(root),
        )
        py = [
            os.environ.get("PYTEST_BIN") or shutil.which("pytest") or sys.executable,
            *([] if os.environ.get("PYTEST_BIN") or shutil.which("pytest") else ["-m", "pytest"]),
        ]
        lib.step("Running portable smoke test (9 subtests)...")
        lib.run(*py, str(root / "hack/ci/test_smoke.py"), "-v", env=test_env, capture=False)
        lib.step("Running live Python SDK integration tests...")
        lib.run(*py, str(root / "clients/python/tests/e2e/test_full_stack.py"), "-v", env=test_env, capture=False)
        lib.step("Running live Go SDK integration tests...")
        lib.run(
            "go",
            "-C",
            str(root),
            "test",
            "-v",
            "-race",
            "-count=1",
            "-timeout",
            "15m",
            "./clients/go",
            "-run",
            "^TestE2EFullStack$",
            env={**test_env, "ATE_ENV_GUEST_TARGET": ""},
            capture=False,
        )

        lib.step("Running NeMo Gym provider live E2E tests...")
        lib.run(
            *py,
            str(root / "integrations/nemo-gym/tests/test_e2e.py"),
            "-k",
            "test_full_lifecycle or test_pause_policy_parks_and_the_next_command_wakes"
            " or test_a_background_service_outlives_its_command",
            "-v",
            env={
                **test_env,
                "SUBSTRATE_E2E_API_URL": f"127.0.0.1:{port}",
                "SUBSTRATE_E2E_TEMPLATE": tmpl,
                "SUBSTRATE_E2E_ATESPACE": ns,
            },
            capture=False,
        )

        if target.rootfs_image:
            lib.step("Running envd runtime layer live E2E tests...")
            lib.run(
                *py,
                str(root / "integrations/e2b/tests/test_envd_e2e.py"),
                "-v",
                env={
                    **test_env,
                    "SUBSTRATE_E2E_API_URL": f"127.0.0.1:{port}",
                    "E2B_E2E_TEMPLATE": E2B_TEMPLATE,
                    "E2B_E2E_ATESPACE": ns,
                    "E2B_E2E_MUSL_IMAGE": target.musl_image,
                    "E2B_E2E_NONROOT_IMAGE": target.nonroot_image,
                },
                capture=False,
            )

            lib.step("Running swebench-loader Go SDK live smoke test...")
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                manifest = tmp_path / "manifest.jsonl"
                manifest.write_text(
                    json.dumps(
                        {
                            "instance_id": "smoke-cmd-1",
                            "kind": "command",
                            "command": "echo swebench-live-ok",
                            "expected_stdout": "swebench-live-ok",
                            "image": target.rootfs_image,
                        }
                    )
                    + "\n"
                )
                events_jsonl = tmp_path / "events.jsonl"
                summary_json = tmp_path / "summary.json"
                loader_bin = tmp_path / "swebench-loader"
                lib.run(
                    "go", "-C", str(root), "build", "-o", str(loader_bin),
                    "./cmd/benchmarking/swebench-loader",
                    capture=False,
                )
                lib.run(
                    str(loader_bin),
                    "run",
                    "-dataset",
                    str(manifest),
                    "-endpoint",
                    f"127.0.0.1:{port}",
                    "-atespace",
                    ns,
                    "-base-template",
                    tmpl,
                    "-mode",
                    "eval",
                    "-images",
                    "1",
                    "-tasks-per-image",
                    "1",
                    "-width",
                    "1",
                    "-connections",
                    "1",
                    "-out",
                    str(events_jsonl),
                    env=test_env,
                    capture=False,
                )
                lib.run(
                    str(loader_bin),
                    "report",
                    str(events_jsonl),
                    "-json",
                    str(summary_json),
                    env=test_env,
                    capture=False,
                )
                summary = json.loads(summary_json.read_text())
                if (
                    summary.get("tasks") != 1
                    or summary.get("failed") != 0
                    or summary.get("rewarded") != 1
                ):
                    raise RuntimeError(f"swebench-loader run failed: {summary}")

        lib.step("Running Harbor provider live trial E2E test...")
        harbor_path = f"{harbor_bin_dir}:{test_env.get('PATH', '')}"
        lib.run(
            *harbor_py,
            str(root / "integrations/harbor/tests/test_e2e.py"),
            "-v",
            env={
                **test_env,
                "SUBSTRATE_E2E_API_URL": f"127.0.0.1:{port}",
                "HB_E2E_TEMPLATE": tmpl,
                "HB_E2E_ATESPACE": ns,
                "PATH": harbor_path,
            },
            capture=False,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end integration test pipeline for sandboxenv-rl on Kind.")
    parser.parse_args()

    if lib.run("docker", "info", check=False).returncode != 0:
        sys.exit("ERROR: Docker daemon is not running or accessible.")

    root = Path(__file__).resolve().parent.parent.parent
    sub_dir = locate_substrate(root)

    ns = os.environ.get("NAMESPACE", "ate-env")
    port = pick_port()
    tmpl = os.environ.get("TEMPLATE_NAME", "default-template")

    harbor_future: concurrent.futures.Future[tuple[list[str], str]] | None = None
    executor: concurrent.futures.ThreadPoolExecutor | None = None
    harbor_py: list[str] = []
    harbor_bin_dir = ""

    if not os.environ.get("HARBOR_PYTEST_BIN") and shutil.which("nix"):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        harbor_future = executor.submit(resolve_harbor_runner, root)
    else:
        harbor_py, harbor_bin_dir = resolve_harbor_runner(root)

    target = KindTarget(sub_dir=sub_dir, namespace=ns)

    try:
        target.provision()
        ate_ctl, ate_env = build_cli_tools(root, sub_dir)
        imgs = build_images(root, sub_dir, target)
        auth_token = deploy_manifests(target, ate_ctl, ate_env, ns, tmpl, imgs)
        if target.rootfs_image:
            register_e2b_template(root, target, ate_ctl, ate_env, ns, tmpl, imgs["guest"])
        if harbor_future is not None:
            harbor_py, harbor_bin_dir = harbor_future.result()
        run_tests(root, target, ate_env, ns, tmpl, port, auth_token, harbor_py, harbor_bin_dir)
        run_reporter(root, target, ate_ctl, ate_env, imgs["reporter"])
        lib.step("PASS: E2E Kind pipeline succeeded!")
    finally:
        try:
            if executor is not None:
                executor.shutdown(wait=True)
        finally:
            target.teardown()


if __name__ == "__main__":
    main()
