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

"""Tests of hack/verify/layers.py: each rule passes a tree that follows it and
names a violation in one that does not. Standard library only, so the Go CI
job runs it without the Python test environment:

    python3 hack/verify/test_layers.py
"""

from __future__ import annotations

import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import layers

M = layers.GO_MODULE

PYPROJECT = """
[project]
name = "ate-env-rl"
dependencies = ["grpcio>=1.85.1", "protobuf>=7.35"]
[project.optional-dependencies]
dev = ["pytest>=9", "ruff~=0.17.2"]
tracing = ["opentelemetry-api>=1.43"]
metrics = ["prometheus-client>=0.20"]
"""

EXAMPLES_README = "| [`mcp/`](./mcp/README.md) | text |\n| [`grpo/`](./grpo/README.md) | text |\n"
ROOT_PYPROJECT = '[tool.uv.workspace]\nmembers = ["clients/python", "integrations/harbor", "integrations/openhands"]\n'
INTEGRATIONS_README = "| [`harbor/`](./harbor/README.md) | x |\n| [`openhands/`](./openhands/README.md) | x |\n"


class PathRules(unittest.TestCase):
    def test_top_level(self):
        ok = ["cmd/a/main.go", "README.md", ".github/workflows/ci.yaml", "examples/mcp/main.go"]
        self.assertEqual(layers.check_top_level(ok), [])
        (v,) = layers.check_top_level(ok + ["plugins/x/y.py"])
        self.assertIn("top-level: plugins/", v)

    def test_examples_only(self):
        ok = [
            "examples/mcp/main.go",
            "clients/go/example_test.go",  # a file, next to its package
            "docs/examples.md",
        ]
        self.assertEqual(layers.check_examples_only(ok), [])
        got = layers.check_examples_only(
            ok + ["clients/python/examples/quickstart.py", "internal/examples/env/main.go", "integrations/x/example/a.py"]
        )
        self.assertEqual(len(got), 3)
        self.assertIn("examples-only: clients/python/examples/", got[0])
        self.assertIn("integrations/x/example/", got[1])
        self.assertIn("internal/examples/", got[2])

    def test_example_shape(self):
        ok = ["examples/README.md", "examples/mcp/README.md", "examples/mcp/main.go", "examples/grpo/README.md"]
        self.assertEqual(layers.check_example_shape(ok, EXAMPLES_README), [])
        got = layers.check_example_shape(
            ok + ["examples/new/run.py", "examples/grpo/pyproject.toml", "examples/loose.py"], EXAMPLES_README
        )
        self.assertEqual(
            [g.split(";")[0] for g in got],
            [
                "example-shape: examples/loose.py is not in an example's directory",
                "example-shape: examples/new/ has no README.md",
                "example-shape: examples/new/ has no row in examples/README.md",
                "example-shape: examples/grpo/pyproject.toml makes an example a distribution",
            ],
        )

    def test_integration_shape(self):
        ok = [
            "integrations/README.md",
            "integrations/harbor/pyproject.toml",
            "integrations/harbor/README.md",
            "integrations/harbor/src/ate_env_harbor/__init__.py",
            "integrations/harbor/tests/test_env.py",
            "integrations/openhands/pyproject.toml",
            "integrations/openhands/README.md",
            "integrations/openhands/src/ate_env_openhands/a.py",
            "integrations/openhands/tests/test_a.py",
        ]
        self.assertEqual(layers.check_integration_shape(ok, INTEGRATIONS_README, ROOT_PYPROJECT), [])
        got = layers.check_integration_shape(ok + ["integrations/newfw/adapter.py"], INTEGRATIONS_README, ROOT_PYPROJECT)
        self.assertEqual(
            got,
            [
                "integration-shape: integrations/newfw/ has no pyproject.toml",
                "integration-shape: integrations/newfw/ has no README.md",
                "integration-shape: integrations/newfw/ has no Python package under src/",
                "integration-shape: integrations/newfw/ has no tests/",
                "integration-shape: integrations/newfw is not a member of the uv workspace in the root pyproject.toml",
                "integration-shape: integrations/newfw/ has no row in integrations/README.md",
            ],
        )
        # A file directly under integrations/ is in no distribution, and the
        # import rules for integrations/<name>/ do not see it.
        (got,) = layers.check_integration_shape(ok + ["integrations/loose.py"], INTEGRATIONS_README, ROOT_PYPROJECT)
        self.assertIn("integration-shape: integrations/loose.py is not in an integration's directory", got)


class PythonImports(unittest.TestCase):
    def test_eager_and_lazy_imports(self):
        src = (
            "from __future__ import annotations\n"
            "import os, grpc\n"
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n"
            "    import torch\n"
            "try:\n"
            "    import prometheus_client\n"
            "except ImportError:\n"
            "    prometheus_client = None\n"
            "from . import sibling\n"
            "class C:\n"
            "    import json\n"
            "    def f(self):\n"
            "        from openhands.sdk import Agent\n"
        )
        got = {(m, eager) for m, _, eager in layers.python_imports(src)}
        self.assertEqual(
            got,
            {
                ("__future__.annotations", True), ("os", True), ("grpc", True), ("typing.TYPE_CHECKING", True),
                ("json", True), ("torch", False), ("prometheus_client", False), ("openhands.sdk.Agent", False),
            },
        )

    def test_only_the_guarded_branch_is_not_eager(self):
        src = (
            "import typing\n"
            "if typing.TYPE_CHECKING:\n"
            "    import tc_body\n"
            "else:\n"
            "    import tc_else\n"
            "if not TYPE_CHECKING:\n"
            "    import not_tc\n"
            "try:\n"
            "    import guarded\n"
            "except (AttributeError, ImportError):\n"
            "    import in_handler\n"
            "else:\n"
            "    import try_else\n"
            "finally:\n"
            "    import try_finally\n"
            "try:\n"
            "    import wrong_handler\n"
            "except ValueError:\n"
            "    pass\n"
            "try:\n"
            "    import no_handler\n"
            "finally:\n"
            "    pass\n"
            "try:\n"
            "    import bare\n"
            "except:\n"
            "    pass\n"
            "try:\n"
            "    import broad\n"
            "except Exception:\n"
            "    pass\n"
            "def f():\n"
            "    try:\n"
            "        pass\n"
            "    finally:\n"
            "        import in_function\n"
        )
        got = {m: eager for m, _, eager in layers.python_imports(src)}
        self.assertEqual(
            got,
            {
                "typing": True, "tc_body": False, "tc_else": True, "not_tc": True,
                "guarded": False, "in_handler": True, "try_else": True, "try_finally": True,
                "wrong_handler": True, "no_handler": True, "bare": False, "broad": False,
                "in_function": False,
            },
        )

    def test_from_import_names_the_submodule(self):
        got = [m for m, _, _ in layers.python_imports("from google import protobuf, cloud\nfrom os.path import *\n")]
        self.assertEqual(got, ["google.protobuf", "google.cloud", "os.path"])
        path = "clients/python/src/ate_env_rl/a.py"
        self.assertEqual(layers.check_core_imports({path: "from google import protobuf\n"}, PYPROJECT), [])
        (v,) = layers.check_core_imports({path: "from google import cloud\n"}, PYPROJECT)
        self.assertIn(f"{path}:1 imports google.cloud, which is not a dependency", v)

    def test_dynamic_imports_with_a_constant_name(self):
        src = (
            "import importlib\n"
            "from importlib import import_module\n"
            'a = importlib.import_module("dyn_a.sub")\n'
            'b = import_module("dyn_b")\n'
            'c = __import__("dyn_c")\n'
            "def f(name):\n"
            '    importlib.import_module("dyn_lazy")\n'
            "    importlib.import_module(name)\n"  # not a constant: not seen
            '    importlib.import_module(".rel", __package__)\n'  # relative: the package itself
        )
        got = {(m, eager) for m, _, eager in layers.python_imports(src)}
        self.assertEqual(
            got,
            {
                ("importlib", True), ("importlib.import_module", True),
                ("dyn_a.sub", True), ("dyn_b", True), ("dyn_c", True), ("dyn_lazy", False),
            },
        )
        path = "clients/python/src/ate_env_rl/a.py"
        got = layers.check_core_imports(
            {path: 'import importlib\nx = importlib.import_module("prometheus_client")\ndef f():\n    __import__("harbor")\n'},
            PYPROJECT,
        )
        self.assertEqual(len(got), 2)
        self.assertIn(f"{path}:2 imports prometheus_client at module level", got[0])
        self.assertIn(f"{path}:4 imports harbor, which is not a dependency", got[1])

    def test_core_allows_its_dependencies(self):
        src = (
            "import asyncio\nimport grpc\nfrom google.protobuf import empty_pb2\n"
            "from ate_env_rl import errors\n"
            "try:\n    from opentelemetry import trace\nexcept ImportError:\n    trace = None\n"
            "def f():\n    import prometheus_client\n"
        )
        self.assertEqual(layers.check_core_imports({"clients/python/src/ate_env_rl/a.py": src}, PYPROJECT), [])

    def test_core_refuses_a_framework_and_an_eager_optional(self):
        path = "clients/python/src/ate_env_rl/a.py"
        got = layers.check_core_imports(
            {
                path: (
                    "import openhands.sdk\n"  # a framework, eager
                    "from opentelemetry import trace\n"  # optional, but eager
                    "import pytest\n"  # a dev tool is not an import of the package
                    "from google.cloud import bigquery\n"  # google.protobuf does not cover google.cloud
                    "def f():\n    import harbor\n"  # lazy, and not declared
                )
            },
            PYPROJECT,
        )
        self.assertEqual(len(got), 5)
        self.assertIn(f"{path}:1 imports openhands.sdk, which is not a dependency", got[0])
        self.assertIn(f"{path}:2 imports opentelemetry.trace at module level", got[1])
        self.assertIn(f"{path}:3 imports pytest, which is not a dependency", got[2])
        self.assertIn(f"{path}:4 imports google.cloud.bigquery, which is not a dependency", got[3])
        self.assertIn(f"{path}:6 imports harbor, which is not a dependency", got[4])

    def test_core_lazy_optional_is_lazy_only(self):
        # R2E-Gym is not on PyPI: no extra declares it, CORE_LAZY_OPTIONAL does.
        self.assertLessEqual({"r2egym", "datasets", "google.cloud.storage"}, set(layers.CORE_LAZY_OPTIONAL))
        src = 'import importlib\ndef f():\n    importlib.import_module("google.cloud.storage")\n    importlib.import_module("datasets")\n'
        self.assertEqual(layers.check_core_imports({"clients/python/src/ate_env_rl/sources.py": src}, PYPROJECT), [])
        path = "clients/python/src/ate_env_rl/adapters/r2egym.py"
        lazy = "def f():\n    from r2egym.agenthub.runtime import docker\n"
        self.assertEqual(layers.check_core_imports({path: lazy}, PYPROJECT), [])
        (got,) = layers.check_core_imports({path: "import r2egym.agenthub\n"}, PYPROJECT)
        self.assertIn(f"{path}:1 imports r2egym.agenthub at module level", got)
        for name, reason in layers.CORE_LAZY_OPTIONAL.items():
            self.assertTrue(reason.strip(), name)

    def test_integration_isolation(self):
        packages = {"harbor": {"ate_env_harbor"}, "openhands": {"ate_env_openhands"}}
        ok = {
            "integrations/harbor/src/ate_env_harbor/env.py": "import harbor\nfrom ate_env_harbor import x\nimport ate_env_rl\n",
            "integrations/harbor/tests/test_env.py": "import ate_env_harbor\n",
            "examples/openhands/run.py": "import ate_env_openhands\nimport ate_env_harbor\n",
            "clients/python/src/ate_env_rl/a.py": "import grpc\n",
        }
        self.assertEqual(layers.check_integration_isolation(ok, packages), [])
        got = layers.check_integration_isolation(
            {
                "integrations/harbor/src/ate_env_harbor/env.py": "def f():\n    from ate_env_openhands.fleet import x\n",
                "clients/python/src/ate_env_rl/a.py": "try:\n    import ate_env_harbor\nexcept ImportError:\n    pass\n",
                "benchmarking/swebench/run.py": "import ate_env_openhands\n",
                "internal/benchmarking/swebench/trajectories/grade.py": "import ate_env_harbor\n",
            },
            packages,
        )
        self.assertEqual(len(got), 4)
        self.assertIn("internal/benchmarking/swebench/trajectories/grade.py:1 imports ate_env_harbor", got[3])
        self.assertIn("internal/ must not depend on it", got[3])
        self.assertIn("benchmarking/swebench/run.py:1 imports ate_env_openhands of integrations/openhands/", got[0])
        self.assertIn("clients/python/src/ate_env_rl/a.py:2 imports ate_env_harbor", got[1])
        self.assertIn("integrations/harbor/ must not depend on it", got[2])


    def test_framework_imports(self):
        ok = {
            "integrations/openhands/src/ate_env_openhands/ws.py": "import openhands.sdk\ndef f():\n    import r2egym\n",
            "integrations/harbor/tests/test_env.py": "from harbor.environments import base\ndef f():\n    import ray\n",
            "clients/python/src/ate_env_rl/adapters/x.py": "def f():\n    import r2egym\n",  # core-imports decides
            "benchmarking/swebench/run.py": "import datasets\nimport swebench\n",
            "docs/plans/x/sketch.py": "import verl\n",
        }
        self.assertEqual(layers.check_framework_imports(ok), [])
        got = layers.check_framework_imports(
            {
                "integrations/harbor/src/ate_env_harbor/env.py": "import openhands.sdk\n",
                "clients/python/tests/test_x.py": "def test():\n    import r2egym\n",
                "benchmarking/swebench/run.py": "import ray\n",
                "hack/ci/e2e.py": "from verl import trainer\n",
                "internal/benchmarking/swebench/trajectories/swebench-grade.py": "import ray\n",
            }
        )
        self.assertEqual(len(got), 5)
        self.assertIn("internal/benchmarking/swebench/trajectories/swebench-grade.py:1 imports the framework module ray", got[4])
        self.assertIn("benchmarking/swebench/run.py:1 imports the framework module ray", got[0])
        self.assertIn("a new integrations/<name>/", got[0])
        self.assertIn("clients/python/tests/test_x.py:2 imports the framework module r2egym", got[1])
        self.assertIn("goes to integrations/openhands/", got[1])
        self.assertIn("hack/ci/e2e.py:1", got[2])
        self.assertIn("integrations/harbor/src/ate_env_harbor/env.py:1 imports the framework module openhands.sdk", got[3])

    def test_example_imports(self):
        ok = {
            "examples/verl_swebench/train.py": "from examples.verl_swebench import cfg\n",
            "clients/python/src/ate_env_rl/a.py": "import grpc\nfrom . import examples_table\n",
        }
        self.assertEqual(layers.check_example_imports(ok), [])
        got = layers.check_example_imports(
            {
                "clients/python/tests/test_x.py": "def test():\n    from examples.verl_swebench import cfg\n",
                "integrations/harbor/src/ate_env_harbor/env.py": "import examples\n",
                "hack/ci/e2e.py": "import examples.grpo.run\n",
            }
        )
        self.assertEqual(len(got), 3)
        self.assertIn("example-imports: clients/python/tests/test_x.py:2 imports examples.verl_swebench", got[0])
        self.assertIn("hack/ci/e2e.py:1", got[1])
        self.assertIn("integrations/harbor/src/ate_env_harbor/env.py:1", got[2])


class GoLayers(unittest.TestCase):
    def test_go_placement(self):
        ok = ["cmd/a/main.go", "clients/go/client.go", "examples/mcp/main.go", "hack/metrics-docs/main.go", "proto/x.pb.go"]
        self.assertEqual(layers.check_go_placement(ok + ["benchmarking/run.py"]), [])
        got = layers.check_go_placement(ok + ["benchmarking/swebench/tool.go", "docs/x/main.go", "clients/python/x.go"])
        self.assertEqual([g.split(" ")[1] for g in got], ["benchmarking/swebench/tool.go", "clients/python/x.go", "docs/x/main.go"])

    def test_nested_go_module(self):
        self.assertEqual(layers.check_go_placement(["go.mod", "go.sum", "docs/go.mod.md"]), [])
        (got,) = layers.check_go_placement(["go.mod", "examples/mcp/go.mod", "examples/mcp/main.go"])
        self.assertIn("go-placement: examples/mcp/go.mod starts a nested Go module", got)

    def test_go_imports_of_each_platform(self):
        # A file with `//go:build !linux` is not in the Linux listing, and one
        # with `//go:build linux` is not in the macOS one: the rules read both.
        listings = {
            "linux": f"{M}/guest/process\tos {M}/internal/unknownfields\n{M}/cmd/ate-env-guest\t{M}/guest\n",
            "darwin": f"{M}/guest/process\tos {M}/internal/ate\n",
        }
        seen = []

        def run(cmd, cwd, env=None):
            seen.append(env["GOOS"])
            self.assertEqual(cmd[:2], ["go", "list"])
            return listings[env["GOOS"]]

        with mock.patch.object(layers, "_run", run):
            got = layers.go_imports(Path("."))
            deps = layers.go_guest_deps(Path("."))
        self.assertEqual(seen, ["linux", "darwin", "linux", "darwin"])
        self.assertEqual(
            got,
            {
                f"{M}/guest/process": [f"{M}/internal/ate", f"{M}/internal/unknownfields", "os"],
                f"{M}/cmd/ate-env-guest": [f"{M}/guest"],
            },
        )
        self.assertEqual(len(deps), len(set(deps)))
        (v,) = layers.check_go_layers(got)
        self.assertIn("guest/process imports internal/ate", v)

    def test_guest_transitive_deps(self):
        ok = [f"{M}/guest", f"{M}/internal/unknownfields", f"{M}/internal/telemetry", "google.golang.org/grpc"]
        self.assertEqual(layers.check_go_guest_deps(ok), [])
        got = layers.check_go_guest_deps(ok + [f"{M}/internal/ate", "go.ate.dev/substrate/pkg/x"])
        self.assertEqual(len(got), 2)
        self.assertIn("depend on internal/ate", got[0])
        self.assertIn("depend on go.ate.dev/substrate/pkg/x", got[1])

    def test_allowed_imports(self):
        ok = {
            f"{M}/guest/process": [f"{M}/proto/ateenv/v1alpha", f"{M}/internal/unknownfields", "os"],
            f"{M}/internal/apiservice": [f"{M}/internal/ate", "go.ate.dev/substrate/pkg/x"],
            f"{M}/cmd/ate-env": [f"{M}/clients/go", "go.ate.dev/substrate/pkg/x"],
            f"{M}/cmd/ate-env-reporter": [f"{M}/internal/reporter", "go.ate.dev/substrate/pkg/x"],
            f"{M}/internal/reporter": [f"{M}/internal/ate", f"{M}/internal/inventory", "go.ate.dev/substrate/api/x"],
            f"{M}/cmd/ate-env-guest": [f"{M}/guest", f"{M}/internal/telemetry"],
            f"{M}/examples/mcp": [f"{M}/clients/go", f"{M}/proto/ateenv/v1alpha"],
            f"{M}/examples/guest-daemon": [f"{M}/guest"],
            f"{M}/hack/metrics-docs": [f"{M}/internal/telemetry"],
            f"{M}/internal/benchmarking/swebench": [f"{M}/clients/go"],
            "example.com/other": [f"{M}/examples/mcp"],
        }
        self.assertEqual(layers.check_go_layers(ok), [])

    def test_denied_imports(self):
        cases = {
            ("guest/process", f"{M}/internal/ate"): "guest/process imports internal/ate",
            ("guest", "go.ate.dev/substrate/pkg/x"): "guest imports go.ate.dev/substrate/pkg/x",
            ("internal/apiservice", f"{M}/guest/process"): "internal/apiservice imports guest/process",
            ("internal/mcp", f"{M}/cmd/ate-env"): "internal/mcp imports cmd/ate-env",
            ("internal/mcp", "go.ate.dev/substrate/pkg/x"): "only cmd/ate-env",
            ("cmd/ate-env-guest", f"{M}/internal/ate"): "cmd/ate-env-guest imports internal/ate",
            ("cmd/ate-env-guest", "go.ate.dev/substrate/pkg/x"): "cmd/ate-env-guest imports go.ate.dev/",
            ("cmd/ate-env-api", f"{M}/examples/mcp"): "cmd/ate-env-api imports examples/mcp",
            ("cmd/ate-env-api", f"{M}/guest/process"): "cmd/ate-env-api imports guest/process",
            ("cmd/benchmarking/bench", f"{M}/guest"): "cmd/benchmarking/bench imports guest",
            ("clients/go", f"{M}/guest"): "clients/go imports guest",
            ("examples/mcp", f"{M}/internal/ate"): "examples/mcp imports internal/ate",
            ("internal/telemetry", f"{M}/hack/metrics-docs"): "internal/telemetry imports hack/metrics-docs",
        }
        for (src, imp), want in cases.items():
            with self.subTest(src=src, imp=imp):
                (got,) = layers.check_go_layers({f"{M}/{src}": [imp]})
                self.assertTrue(got.startswith("go-layers: "), got)
                self.assertIn(want, got)

    def test_prefixes_match_whole_path_elements(self):
        # cmd/ate-env is a Substrate area; cmd/ate-env-guest is not part of it.
        self.assertTrue(layers._under("cmd/ate-env/manifest", "cmd/ate-env"))
        self.assertFalse(layers._under("cmd/ate-env-guest", "cmd/ate-env"))
        self.assertFalse(layers._module_under("google.cloud.storage", "google.protobuf"))


class Inputs(unittest.TestCase):
    """A broken input gives one line that names the file or the tool, not a traceback."""

    def _repo(self, extra: dict[str, bytes]) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        base = {
            "examples/README.md": b"", "integrations/README.md": b"", "pyproject.toml": b"",
            layers.CORE_PYPROJECT: b'[project]\nname = "ate-env-rl"\n',
        }
        for name, data in {**base, **extra}.items():
            (root / name).parent.mkdir(parents=True, exist_ok=True)
            (root / name).write_bytes(data)
        return root

    def test_a_python_file_that_does_not_parse_is_named(self):
        with self.assertRaises(layers.InputError) as e:
            layers.python_imports("def (:\n", "hack/x.py")
        self.assertIn("hack/x.py:1: cannot parse", str(e.exception))
        self.assertEqual(layers.check_repository(self._repo({}), with_go=False), [])
        # Untracked, as a scratch file is: the file list includes it.
        with self.assertRaises(layers.InputError) as e:
            layers.check_repository(self._repo({"hack/ci/scratch.py": b"import os\nclass :\n"}), with_go=False)
        self.assertIn("hack/ci/scratch.py:2: cannot parse", str(e.exception))

    def test_a_python_file_that_is_not_utf8_is_named(self):
        with self.assertRaises(layers.InputError) as e:
            layers.check_repository(self._repo({"benchmarking/latin1.py": b"x = '\xe9'\n"}), with_go=False)
        self.assertIn("benchmarking/latin1.py: cannot read the file as UTF-8", str(e.exception))

    def test_a_missing_go_tool_names_no_go(self):
        real = subprocess.run

        def run(cmd, **kwargs):
            if cmd[0] == "go":
                raise FileNotFoundError(2, "No such file or directory", "go")
            return real(cmd, **kwargs)

        err = io.StringIO()
        with mock.patch.object(layers.subprocess, "run", run), contextlib.redirect_stderr(err):
            code = layers.main([])
        self.assertEqual(code, 2)
        self.assertIn("layers: cannot run `go`", err.getvalue())
        self.assertIn("--no-go", err.getvalue())
        self.assertNotIn("Traceback", err.getvalue())

    def test_a_failed_command_is_an_input_error(self):
        with self.assertRaises(layers.InputError) as e:
            layers._run([sys.executable, "-c", "import sys; sys.exit('boom')"], Path.cwd())
        self.assertIn("failed (1)", str(e.exception))
        self.assertIn("boom", str(e.exception))


class Repository(unittest.TestCase):
    def test_this_checkout_follows_the_rules(self):
        # The rule tables and the tree move together: a merge that brings a new
        # package, example or import fails here, not only in `layers.py` itself.
        # go-layers needs the go tool, so `make verify-layers` runs it apart.
        root = Path(__file__).resolve().parents[2]
        self.assertEqual(layers.check_repository(root, with_go=False), [])


if __name__ == "__main__":
    unittest.main()
