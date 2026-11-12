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

"""Unit tests for the global viewer binding of hack/ci/e2e.py, without a
cluster: the pure merge, and the read and the ensure over a scripted
lib.run."""

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e2e
from e2e import ensure_global_viewer_binding, merge_viewer_binding, read_global_policy

MEMBER = "user:system:serviceaccount:ate-env:ate-env-api"


def test_null_bindings_get_a_viewer_binding():
    policy = {"metadata": {"name": "default"}, "bindings": None}
    merged = merge_viewer_binding(policy, MEMBER)
    assert merged is not None
    assert merged["bindings"] == [{"role": "viewer", "members": [MEMBER]}]


def test_an_owner_only_policy_gains_the_viewer_binding():
    policy = {"bindings": [{"role": "owner", "members": ["user:alice"]}]}
    merged = merge_viewer_binding(policy, MEMBER)
    assert merged is not None
    assert merged["bindings"][1] == {"role": "viewer", "members": [MEMBER]}
    assert merged["bindings"][0] == {"role": "owner", "members": ["user:alice"]}


def test_a_member_already_bound_needs_no_update():
    policy = {"bindings": [{"role": "viewer", "members": ["user:bob", MEMBER]}]}
    assert merge_viewer_binding(policy, MEMBER) is None


def test_a_viewer_binding_without_members_gets_the_member():
    policy = {"bindings": [{"role": "viewer", "members": None}]}
    merged = merge_viewer_binding(policy, MEMBER)
    assert merged is not None and merged["bindings"][0]["members"] == [MEMBER]


TARGET = SimpleNamespace(context="kind-test")
MANIFEST = f"apiVersion: v1\nbindings:\n- role: viewer\n  members:\n  - {MEMBER}\n"


class _Script:
    """Stands in for lib.run: answers each kubectl-ate or ate-env call from a
    table keyed by the verb, and records the calls."""

    def __init__(self, answers: dict[str, list[tuple[int, str, str]]]):
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls: list[tuple[str, str | None]] = []

    def __call__(self, *argv, input=None, check=True, capture=True, **_kw):
        verb = next(a for a in argv if a in ("manifest", "get", "create", "update"))
        self.calls.append((verb, input))
        code, out, err = self.answers[verb].pop(0)
        if check and code != 0:
            raise RuntimeError(f"Command failed ({code}): {err}")
        return subprocess.CompletedProcess(list(argv), code, stdout=out, stderr=err)

    def verbs(self) -> list[str]:
        return [v for v, _ in self.calls]


@pytest.fixture
def script(monkeypatch):
    def install(answers):
        s = _Script(answers)
        monkeypatch.setattr(e2e.lib, "run", s)
        monkeypatch.setattr(e2e.lib, "step", lambda *_a, **_k: None)
        return s

    return install


def test_read_takes_a_missing_policy_for_none_and_any_other_failure_for_an_error(script):
    script({"get": [(1, "", "Error: no access policy is set")]})
    assert read_global_policy(TARGET, "kubectl-ate") is None
    script({"get": [(1, "", "rpc error: code = NotFound desc = policy not found")]})
    assert read_global_policy(TARGET, "kubectl-ate") is None
    script({"get": [(0, "  \n", "")]})
    assert read_global_policy(TARGET, "kubectl-ate") is None
    # A transport failure is not "no policy": creating over it would replace
    # the cluster's bindings.
    script({"get": [(1, "", "rpc error: code = Unavailable desc = connection refused")]})
    with pytest.raises(RuntimeError, match="reading the global access-policy failed"):
        read_global_policy(TARGET, "kubectl-ate")
    script({"get": [(0, "<html>", "")]})
    with pytest.raises(RuntimeError, match="is not JSON"):
        read_global_policy(TARGET, "kubectl-ate")


def test_ensure_creates_the_policy_from_the_manifest_when_there_is_none(script):
    s = script({
        "manifest": [(0, MANIFEST, "")],
        "get": [(1, "", "no access policy")],
        "create": [(0, "", "")],
    })
    ensure_global_viewer_binding(TARGET, "kubectl-ate", "ate-env", "ate-env")
    assert s.verbs() == ["manifest", "get", "create"]
    assert s.calls[2][1] == MANIFEST


def test_ensure_reads_and_updates_when_the_create_finds_the_policy_exists(script):
    existing = {"bindings": [{"role": "owner", "members": ["user:alice"]}]}
    s = script({
        "manifest": [(0, MANIFEST, "")],
        "get": [(1, "", "no access policy"), (0, json.dumps(existing), "")],
        "create": [(1, "", "rpc error: code = AlreadyExists desc = policy exists")],
        "update": [(0, "", "")],
    })
    ensure_global_viewer_binding(TARGET, "kubectl-ate", "ate-env", "ate-env")
    assert s.verbs() == ["manifest", "get", "create", "get", "update"]
    sent = json.loads(s.calls[4][1])
    assert sent["bindings"] == [
        {"role": "owner", "members": ["user:alice"]},
        {"role": "viewer", "members": [MEMBER]},
    ]


def test_ensure_updates_an_existing_policy_and_leaves_a_bound_member_alone(script):
    s = script({
        "manifest": [(0, MANIFEST, "")],
        "get": [(0, json.dumps({"bindings": [{"role": "viewer", "members": ["user:bob"]}]}), "")],
        "update": [(0, "", "")],
    })
    ensure_global_viewer_binding(TARGET, "kubectl-ate", "ate-env", "ate-env")
    assert s.verbs() == ["manifest", "get", "update"]
    assert json.loads(s.calls[2][1])["bindings"][0]["members"] == ["user:bob", MEMBER]
    s = script({
        "manifest": [(0, MANIFEST, "")],
        "get": [(0, json.dumps({"bindings": [{"role": "viewer", "members": [MEMBER]}]}), "")],
    })
    ensure_global_viewer_binding(TARGET, "kubectl-ate", "ate-env", "ate-env")
    assert s.verbs() == ["manifest", "get"], "a bound member needs no update"


def test_ensure_fails_on_a_read_error_a_create_error_and_a_manifest_without_a_member(script):
    s = script({
        "manifest": [(0, MANIFEST, "")],
        "get": [(1, "", "rpc error: code = Unavailable desc = connection refused")],
    })
    with pytest.raises(RuntimeError, match="reading the global access-policy failed"):
        ensure_global_viewer_binding(TARGET, "kubectl-ate", "ate-env", "ate-env")
    assert "create" not in s.verbs(), "a failed read must not lead to a create"
    script({
        "manifest": [(0, MANIFEST, "")],
        "get": [(1, "", "no access policy")],
        "create": [(1, "", "rpc error: code = PermissionDenied desc = no")],
    })
    with pytest.raises(RuntimeError, match="Failed to create the global access-policy"):
        ensure_global_viewer_binding(TARGET, "kubectl-ate", "ate-env", "ate-env")
    script({"manifest": [(0, "bindings: []\n", "")]})
    with pytest.raises(RuntimeError, match="no member in the global access-policy manifest"):
        ensure_global_viewer_binding(TARGET, "kubectl-ate", "ate-env", "ate-env")
