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

"""Verify that code is in the layer it belongs to (docs/dev/code-layout.md).

The rules, each named in the message of a violation:

  top-level       The top-level directories are the ones in TOP_LEVEL_DIRS. A
                  new one needs an entry there and in docs/dev/code-layout.md.
  examples-only   Usage examples are in the top-level examples/ directory. No
                  other directory is named "example" or "examples". (A Go
                  example_test.go next to its package is documentation of that
                  package and stays there.)
  example-shape   Each examples/<name>/ has a README.md and a row in
                  examples/README.md. An example is not a distribution: no
                  pyproject.toml, setup.py or setup.cfg under examples/. The
                  only file directly under examples/ is its README.md.
  integration-shape
                  Each integrations/<name>/ is a Python distribution of its
                  own: pyproject.toml, README.md, src/<package>/, tests/, a
                  member of the uv workspace in the root pyproject.toml, and a
                  row in integrations/README.md. The only file directly under
                  integrations/ is its README.md.
  core-imports    The Python client (clients/python/src/ate_env_rl) imports, at
                  module level, only the standard library, itself and the
                  dependencies of its pyproject.toml. An import that is lazy
                  (in a function) or guarded (the body of a `try` that handles
                  ImportError, or of `if TYPE_CHECKING:`) can also be
                  an optional dependency of the pyproject.toml or a name in
                  CORE_LAZY_OPTIONAL. Code that needs a third-party framework
                  at import time is an integration and goes to integrations/.
  integration-isolation
                  An integration's package is imported only by that
                  integration and by examples/: not by the Python client,
                  benchmarking/, hack/, internal/, tests/ or another
                  integration.
  framework-imports
                  A framework's modules (FRAMEWORKS) are imported only under
                  integrations/ and examples/, and one that an integration
                  owns only under that integration: not under clients/,
                  benchmarking/, hack/, internal/ or tests/. The
                  Python client's package answers to core-imports instead.
  example-imports No Python file outside examples/ imports the `examples`
                  package: an example is not a library.
  go-placement    Go files are under cmd/, internal/, guest/, proto/,
                  clients/go/, examples/ and hack/; Go benchmark code is in
                  cmd/benchmarking and internal/benchmarking. The root go.mod
                  is the only one: `go list ./...` does not enter a nested
                  module, so the Go rules would not read it.
  go-layers       Go packages import across areas only as GO_RULES permits:
                  guest/ is self-contained, internal/ and clients/ do not
                  import cmd/ or guest/, cmd/ imports guest/ only in
                  cmd/ate-env-guest, the guest daemon does not import the
                  control plane, nothing imports examples/ or hack/, examples/ use
                  only what an external module can (not internal/), and only
                  the packages in GO_SUBSTRATE_AREAS import go.ate.dev/.
                  guest/ and cmd/ate-env-guest also do not depend on
                  internal/ate or go.ate.dev/ through another package.
                  clients/go has a stricter check of its own, transitive:
                  hack/verify/go-client-deps.sh.

Run from anywhere in the repository: `hack/verify/layers.py` (or
`make verify-layers`). `--no-go` skips go-layers, which needs the `go` tool.
The file list is `git ls-files` plus the untracked files that .gitignore does
not exclude; the Go imports come from `go list` (tests excluded) for each GOOS
in GO_PLATFORMS. Tests: hack/verify/test_layers.py.
"""

from __future__ import annotations

import argparse
import ast
import os
import subprocess
import sys
import tomllib
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

# Top-level directories. Dot-directories and files are not checked.
TOP_LEVEL_DIRS = {
    "benchmarking",  # scenarios, SWE-bench scripts, report tools
    "clients",  # the Go and Python clients
    "cmd",  # binaries
    "docs",
    "examples",  # usage examples, not supported
    "guest",  # in-actor services
    "hack",  # repository scripts
    "integrations",  # adapters to third-party frameworks, one distribution each
    "internal",  # packages of this repository's binaries
    "nix",
    "proto",
    "tests",
}

EXAMPLE_DIR_NAMES = {"example", "examples"}
DISTRIBUTION_FILES = {"pyproject.toml", "setup.py", "setup.cfg"}

CORE_PYPROJECT = "clients/python/pyproject.toml"
CORE_PACKAGE_DIR = "clients/python/src/ate_env_rl"
CORE_PACKAGE = "ate_env_rl"
# Generated stubs import what the generator emits; protos.sh checks them.
CORE_GENERATED_DIR = CORE_PACKAGE_DIR + "/_gen"

# Distribution name -> the module prefix it provides, where the two differ.
DIST_MODULES = {
    "grpcio": "grpc",
    "protobuf": "google.protobuf",
    "opentelemetry-api": "opentelemetry",
    "opentelemetry-sdk": "opentelemetry",
    "opentelemetry-exporter-otlp-proto-grpc": "opentelemetry",
    "prometheus-client": "prometheus_client",
}

# Optional extras of the client that are tools, not imports of the package.
CORE_TOOL_EXTRAS = {"dev"}

# Modules the client imports lazily without declaring them: each needs a
# reason here. A framework the client cannot work without is not a candidate;
# that code is an integration.
CORE_LAZY_OPTIONAL: dict[str, str] = {
    # ate_env_rl.adapters.r2egym (#314) binds R2E-Gym's RepoEnv to a fleet
    # handle with the handle's methods and the standard library alone. R2E-Gym
    # is not on PyPI, so no extra of the pyproject.toml can declare it; the
    # module imports it on first use, and the package imports without it.
    "r2egym": "ate_env_rl.adapters.r2egym; R2E-Gym is not on PyPI, so no extra can declare it",
    # ate_env_rl.sources reads task lists from places that each need a package
    # most callers do not have. It imports the package by name on first use:
    # a missing `datasets` gives an ImportError that names the pip command, and
    # a missing `google.cloud.storage` falls back to the gcloud CLI.
    "datasets": "ate_env_rl.sources.SweBenchSource; loaded on first use, the error names the package",
    "google.cloud.storage": "ate_env_rl.sources gs:// reads; falls back to the gcloud CLI without it",
}

# Third-party framework module -> the integration that owns it. None: no
# integration owns it, so each integration and example can import it.
FRAMEWORKS: dict[str, str | None] = {
    "harbor": "harbor",
    "nemo_gym": "nemo-gym",
    "openhands": "openhands",
    "r2egym": "openhands",
    "uni_agent": "uni-agent",
    "ray": None,
    "trl": None,
    "tunix": None,
    "verl": None,
}

GO_DIRS = ("cmd", "internal", "guest", "proto", "clients/go", "examples", "hack")
GO_GUEST_ROOTS = ("guest", "cmd/ate-env-guest")
GO_GUEST_FORBIDDEN = ("internal/ate",)
# `go list` reads only the files of one GOOS. The binaries run on Linux and the
# developers build on macOS, so the Go rules read both file sets.
GO_PLATFORMS = ("linux", "darwin")

# Python trees that must not import an integration's package.
NO_INTEGRATION_IMPORTS = ("clients/", "benchmarking/", "hack/", "internal/", "tests/")

GO_MODULE = "github.com/agent-substrate/env"
GO_SUBSTRATE_PREFIX = "go.ate.dev/"

# Areas (path prefixes in the module) that import Substrate's modules.
GO_SUBSTRATE_AREAS = (
    "cmd/ate-env",
    "cmd/ate-env-reporter",  # builds the clientset that internal/reporter lists with
    "internal/apiservice",
    "internal/ate",
    "internal/benchmarking",
    "internal/internaltest",
    "internal/inventory",
    "internal/portproxy",
    "internal/reporter",  # reads the control plane and the Substrate CRDs for the snapshot
)

# (importer prefix, what it must not import, why). The first matching "allow"
# entry of GO_ALLOW exempts an import from the deny rules below it.
GO_DENY = (
    ("guest", ("cmd", "clients", "internal", "examples", "hack", "benchmarking"),
     "guest/ runs in the actor and imports only guest/, proto/ and internal/unknownfields"),
    ("internal", ("cmd", "guest", "examples", "hack"),
     "internal/ packages do not import binaries, in-actor services, examples or scripts"),
    ("cmd/ate-env-guest", ("internal/ate", "internal/apiservice"),
     "the guest daemon runs in task images without control-plane dependencies"),
    ("cmd", ("examples", "hack", "guest"),
     "binaries do not import examples, scripts or (the guest daemon aside) in-actor services"),
    ("clients", ("examples", "hack", "guest", "cmd"),
     "clients call the generated stubs; they do not import services, binaries, examples or scripts"),
    ("examples", ("internal", "cmd", "hack"),
     "an example uses what an external module can: clients/go, guest/, proto/"),
    ("hack", ("examples", "cmd"), "scripts do not import examples or binaries"),
)
GO_ALLOW = (
    ("guest", "internal/unknownfields"),
    ("cmd/ate-env-guest", "guest"),
)


class InputError(Exception):
    """The script cannot read its input: a tool is missing or fails, or a file does not parse."""


def _under(path: str, prefix: str) -> bool:
    """True when path is prefix or is inside the directory prefix."""
    prefix = prefix.rstrip("/")
    return path == prefix or path.startswith(prefix + "/")


def _module_under(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(prefix + ".")


# --- paths --------------------------------------------------------------------


def check_top_level(files: Iterable[str]) -> list[str]:
    seen = {PurePosixPath(f).parts[0] for f in files if "/" in f}
    extra = sorted(d for d in seen if not d.startswith(".") and d not in TOP_LEVEL_DIRS)
    return [
        f"top-level: {d}/ is not a known top-level directory; put the code in an existing "
        f"layer, or add {d!r} to TOP_LEVEL_DIRS and to docs/dev/code-layout.md"
        for d in extra
    ]


def check_examples_only(files: Iterable[str]) -> list[str]:
    bad: set[str] = set()
    for f in files:
        parts = PurePosixPath(f).parts[:-1]
        for i, part in enumerate(parts):
            if part in EXAMPLE_DIR_NAMES and i > 0:
                bad.add("/".join(parts[: i + 1]))
                break
    return [
        f"examples-only: {d}/ holds examples outside the top-level examples/; move it to examples/<name>/"
        for d in sorted(bad)
    ]


def _subdirs(files: Iterable[str], top: str) -> dict[str, list[str]]:
    """Immediate subdirectories of top -> their files, relative to the subdirectory."""
    out: dict[str, list[str]] = {}
    for f in files:
        parts = PurePosixPath(f).parts
        if len(parts) >= 3 and parts[0] == top:
            out.setdefault(parts[1], []).append("/".join(parts[2:]))
    return out


def _loose_files(files: Iterable[str], top: str) -> list[str]:
    """Files directly under top, its README.md aside."""
    return sorted(
        f for f in files if PurePosixPath(f).parts[:-1] == (top,) and PurePosixPath(f).name != "README.md"
    )


def _readme_names(readme_text: str) -> set[str]:
    """Names a README's table links as [`name/`](./name/...)."""
    names = set()
    for chunk in readme_text.split("](./")[1:]:
        names.add(chunk.split(")")[0].split("/")[0])
    return names


def check_example_shape(files: Iterable[str], readme_text: str) -> list[str]:
    files = list(files)
    out = []
    listed = _readme_names(readme_text)
    for f in _loose_files(files, "examples"):
        out.append(f"example-shape: {f} is not in an example's directory; move it to examples/<name>/")
    for name, inner in sorted(_subdirs(files, "examples").items()):
        if "README.md" not in inner:
            out.append(f"example-shape: examples/{name}/ has no README.md")
        if name not in listed:
            out.append(f"example-shape: examples/{name}/ has no row in examples/README.md")
    for f in sorted(files):
        if _under(f, "examples") and PurePosixPath(f).name in DISTRIBUTION_FILES:
            out.append(
                f"example-shape: {f} makes an example a distribution; code that others install "
                "goes to clients/ or integrations/"
            )
    return out


def integration_packages(files: Iterable[str]) -> dict[str, set[str]]:
    """Integration name -> the import packages under its src/."""
    out: dict[str, set[str]] = {}
    for name, inner in _subdirs(files, "integrations").items():
        pkgs = out.setdefault(name, set())
        for f in inner:
            parts = PurePosixPath(f).parts
            if len(parts) >= 3 and parts[0] == "src" and f.endswith(".py"):
                pkgs.add(parts[1])
    return out


def check_integration_shape(files: Iterable[str], readme_text: str, root_pyproject_text: str) -> list[str]:
    files = list(files)
    out = []
    listed = _readme_names(readme_text)
    packages = integration_packages(files)
    uv = tomllib.loads(root_pyproject_text).get("tool", {}).get("uv", {})
    members = set(uv.get("workspace", {}).get("members", []))
    for f in _loose_files(files, "integrations"):
        out.append(f"integration-shape: {f} is not in an integration's directory; move it to integrations/<name>/")
    for name, inner in sorted(_subdirs(files, "integrations").items()):
        for required in ("pyproject.toml", "README.md"):
            if required not in inner:
                out.append(f"integration-shape: integrations/{name}/ has no {required}")
        if not packages.get(name):
            out.append(f"integration-shape: integrations/{name}/ has no Python package under src/")
        if not any(f.startswith("tests/") for f in inner):
            out.append(f"integration-shape: integrations/{name}/ has no tests/")
        if f"integrations/{name}" not in members:
            out.append(
                f"integration-shape: integrations/{name} is not a member of the uv workspace in the root pyproject.toml"
            )
        if name not in listed:
            out.append(f"integration-shape: integrations/{name}/ has no row in integrations/README.md")
    return out


# --- Python imports -----------------------------------------------------------


# Exceptions whose handler makes a failed import in the `try` body survivable.
IMPORT_FAILURE_HANDLERS = {"ImportError", "ModuleNotFoundError", "Exception", "BaseException"}


def _is_type_checking(test: ast.expr) -> bool:
    """True for `TYPE_CHECKING` and `typing.TYPE_CHECKING`, not for `not TYPE_CHECKING`."""
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _handles_import_failure(node: ast.Try) -> bool:
    for handler in node.handlers:
        if handler.type is None:
            return True
        types = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
        for t in types:
            name = t.id if isinstance(t, ast.Name) else t.attr if isinstance(t, ast.Attribute) else None
            if name in IMPORT_FAILURE_HANDLERS:
                return True
    return False


def _dynamic_import(node: ast.Call) -> str | None:
    """The module of `import_module("m")`, `importlib.import_module("m")` or `__import__("m")`."""
    func = node.func
    name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
    if name not in ("import_module", "__import__") or not node.args:
        return None
    arg = node.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value and not arg.value.startswith("."):
        return arg.value
    return None


def python_imports(source: str, filename: str = "<source>") -> list[tuple[str, int, bool]]:
    """(module, line, eager) for each absolute import of a Python source.

    `from a import b` gives `a.b`: b can be a submodule, and the rules match
    prefixes, so `from google import protobuf` is `google.protobuf`. A call of
    `import_module` or `__import__` with a constant name is an import too.

    eager is True for an import that runs when the module is imported and
    whose failure is not handled: at module or class level. Not eager: an
    import in a function, in the body of `if TYPE_CHECKING:` (its `else` is
    eager), and in the body of a `try` that has a handler for ImportError or
    a base of it (the handlers, `else` and `finally` are eager).

    Raises InputError, with filename in the message, for a source that does
    not parse.
    """
    out: list[tuple[str, int, bool]] = []

    def visit_all(nodes: Iterable[ast.AST], eager: bool) -> None:
        for node in nodes:
            visit(node, eager)

    def visit(node: ast.AST, eager: bool) -> None:
        if isinstance(node, ast.Import):
            out.extend((alias.name, node.lineno, eager) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                for alias in node.names:
                    name = node.module if alias.name == "*" else f"{node.module}.{alias.name}"
                    out.append((name, node.lineno, eager))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            visit_all(ast.iter_child_nodes(node), False)
        elif isinstance(node, ast.If) and _is_type_checking(node.test):
            visit_all(node.body, False)
            visit_all(node.orelse, eager)
        elif isinstance(node, (ast.Try, ast.TryStar)):
            visit_all(node.body, eager and not _handles_import_failure(node))
            visit_all(node.handlers, eager)
            visit_all(node.orelse, eager)
            visit_all(node.finalbody, eager)
        else:
            if isinstance(node, ast.Call):
                module = _dynamic_import(node)
                if module:
                    out.append((module, node.lineno, eager))
            visit_all(ast.iter_child_nodes(node), eager)

    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as e:
        raise InputError(f"{filename}:{e.lineno}: cannot parse the file as Python: {e.msg}") from e
    except ValueError as e:  # a null byte, before Python 3.12
        raise InputError(f"{filename}: cannot parse the file as Python: {e}") from e
    visit(tree, True)
    return out


def _dist_name(requirement: str) -> str:
    name = requirement
    for sep in ("[", "<", ">", "=", "!", "~", ";", " "):
        name = name.split(sep)[0]
    return name.strip().lower().replace("_", "-")


def _dist_module(requirement: str) -> str:
    name = _dist_name(requirement)
    return DIST_MODULES.get(name, name.replace("-", "_"))


def core_allowed_modules(pyproject_text: str) -> tuple[set[str], set[str]]:
    """(modules allowed eagerly, modules allowed lazily or guarded only)."""
    project = tomllib.loads(pyproject_text).get("project", {})
    eager = {CORE_PACKAGE} | {_dist_module(r) for r in project.get("dependencies", [])}
    lazy = set(CORE_LAZY_OPTIONAL)
    for extra, requirements in project.get("optional-dependencies", {}).items():
        if extra not in CORE_TOOL_EXTRAS:
            lazy |= {_dist_module(r) for r in requirements}
    return eager, lazy


def check_core_imports(sources: dict[str, str], pyproject_text: str) -> list[str]:
    """sources: path -> text of each .py file of the Python client's package."""
    eager_ok, lazy_ok = core_allowed_modules(pyproject_text)
    out = []
    for path in sorted(sources):
        for module, line, eager in python_imports(sources[path], path):
            if module.split(".")[0] in sys.stdlib_module_names:
                continue
            if any(_module_under(module, m) for m in eager_ok):
                continue
            if any(_module_under(module, m) for m in lazy_ok):
                if eager:
                    out.append(
                        f"core-imports: {path}:{line} imports {module} at module level; it is an "
                        "optional dependency, so import it in a function or under try"
                    )
                continue
            out.append(
                f"core-imports: {path}:{line} imports {module}, which is not a dependency in "
                f"{CORE_PYPROJECT}; an adapter to a third-party framework goes to integrations/ "
                "(a lazy optional import needs an entry in CORE_LAZY_OPTIONAL of this script)"
            )
    return out


def check_integration_isolation(sources: dict[str, str], packages: dict[str, set[str]]) -> list[str]:
    """sources: path -> text of the .py files to check; packages: integration -> its packages."""
    owner = {pkg: name for name, pkgs in packages.items() for pkg in pkgs}
    out = []
    for path in sorted(sources):
        if _under(path, "integrations"):
            me = PurePosixPath(path).parts[1]
        elif path.startswith(NO_INTEGRATION_IMPORTS):
            me = None
        else:
            continue
        for module, line, _ in python_imports(sources[path], path):
            name = owner.get(module.split(".")[0])
            if name is None or name == me:
                continue
            where = f"integrations/{me}/" if me else PurePosixPath(path).parts[0] + "/"
            out.append(
                f"integration-isolation: {path}:{line} imports {module} of integrations/{name}/; "
                f"{where} must not depend on it (shared code goes to the Python client)"
            )
    return out


def check_framework_imports(sources: dict[str, str]) -> list[str]:
    """sources: path -> text of the .py files outside examples/."""
    out = []
    for path in sorted(sources):
        if _under(path, CORE_PACKAGE_DIR):
            continue  # core-imports decides, with CORE_LAZY_OPTIONAL
        if _under(path, "integrations"):
            me = PurePosixPath(path).parts[1]
        elif path.startswith(NO_INTEGRATION_IMPORTS):
            me = None
        else:
            continue
        for module, line, _ in python_imports(sources[path], path):
            root = module.split(".")[0]
            if root not in FRAMEWORKS or (me is not None and FRAMEWORKS[root] in (me, None)):
                continue
            home = f"integrations/{FRAMEWORKS[root]}/" if FRAMEWORKS[root] else "a new integrations/<name>/"
            out.append(
                f"framework-imports: {path}:{line} imports the framework module {module}; "
                f"code that needs it goes to {home} (or to examples/)"
            )
    return out


def check_example_imports(sources: dict[str, str]) -> list[str]:
    """sources: path -> text of the .py files to check."""
    out = []
    for path in sorted(sources):
        if _under(path, "examples"):
            continue
        for module, line, _ in python_imports(sources[path], path):
            if module.split(".")[0] == "examples":
                out.append(
                    f"example-imports: {path}:{line} imports {module}; an example is not a library "
                    "(shared code goes to the Python client or to an integration)"
                )
    return out


# --- Go -----------------------------------------------------------------------


def check_go_placement(files: Iterable[str]) -> list[str]:
    files = sorted(files)
    out = [
        f"go-placement: {f} is outside the Go layers ({', '.join(d + '/' for d in GO_DIRS)}); "
        "Go benchmark code goes to cmd/benchmarking or internal/benchmarking"
        for f in files
        if f.endswith(".go") and not any(_under(f, d) for d in GO_DIRS)
    ]
    out += [
        f"go-placement: {f} starts a nested Go module, which `go list ./...` and the Go rules do not "
        "read; the repository is one module (the root go.mod)"
        for f in files
        if PurePosixPath(f).name == "go.mod" and f != "go.mod"
    ]
    return out


def check_go_guest_deps(deps: Iterable[str]) -> list[str]:
    """deps: `go list -deps` of the guest packages (transitive, tests excluded)."""
    out = []
    for dep in sorted(set(deps)):
        rel = _go_rel(dep)
        if dep.startswith(GO_SUBSTRATE_PREFIX) or (rel is not None and any(_under(rel, f) for f in GO_GUEST_FORBIDDEN)):
            out.append(
                f"go-layers: {' and '.join(r + '/' for r in GO_GUEST_ROOTS)} depend on {rel or dep}; "
                "the guest runs in task images without control-plane dependencies"
            )
    return out


def _go_rel(import_path: str) -> str | None:
    """Path in the module of an import path of this repository, else None."""
    if import_path == GO_MODULE:
        return ""
    if import_path.startswith(GO_MODULE + "/"):
        return import_path[len(GO_MODULE) + 1 :]
    return None


def check_go_layers(imports: dict[str, list[str]]) -> list[str]:
    """imports: package import path -> its imports (`go list`, tests excluded)."""
    out = []
    for pkg in sorted(imports):
        src = _go_rel(pkg)
        if src is None:
            continue
        for imp in sorted(imports[pkg]):
            if imp.startswith(GO_SUBSTRATE_PREFIX):
                if not any(_under(src, area) for area in GO_SUBSTRATE_AREAS):
                    out.append(
                        f"go-layers: {src} imports {imp}; only {', '.join(GO_SUBSTRATE_AREAS)} talk "
                        "to Substrate (add the package to GO_SUBSTRATE_AREAS if it must)"
                    )
                continue
            dst = _go_rel(imp)
            if dst is None:
                continue
            if any(_under(src, a) and _under(dst, b) for a, b in GO_ALLOW):
                continue
            for importer, denied, why in GO_DENY:
                if _under(src, importer) and any(_under(dst, d) for d in denied):
                    out.append(f"go-layers: {src} imports {dst}; {why}")
                    break
    return out


# --- inputs -------------------------------------------------------------------


def _run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> str:
    hint = "; --no-go skips the rules that need the go tool" if cmd[0] == "go" else ""
    try:
        done = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False, env=env)
    except OSError as e:
        raise InputError(f"cannot run `{cmd[0]}` ({e}){hint}") from e
    if done.returncode != 0:
        raise InputError(f"`{' '.join(cmd)}` failed ({done.returncode}){hint}:\n{done.stderr.rstrip()}")
    return done.stdout


def _go_list(args: list[str], root: Path) -> list[str]:
    """Output lines of `go list <args>` for each GOOS of GO_PLATFORMS, in order."""
    lines: list[str] = []
    for goos in GO_PLATFORMS:
        lines += _run(["go", "list", *args], root, env={**os.environ, "GOOS": goos}).splitlines()
    return lines


def go_imports(root: Path) -> dict[str, list[str]]:
    out: dict[str, set[str]] = {}
    for line in _go_list(["-f", '{{.ImportPath}}\t{{join .Imports " "}}', "./..."], root):
        pkg, _, imports = line.partition("\t")
        out.setdefault(pkg, set()).update(imports.split())
    return {pkg: sorted(imports) for pkg, imports in out.items()}


def go_guest_deps(root: Path) -> list[str]:
    return sorted(set(_go_list(["-deps", *(f"./{r}/..." for r in GO_GUEST_ROOTS)], root)))


def check_repository(root: Path, with_go: bool = True) -> list[str]:
    files = [f for f in _run(["git", "ls-files", "-z", "-co", "--exclude-standard"], root).split("\0") if f and (root / f).exists()]

    def read(path: str) -> str:
        try:
            return (root / path).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as e:
            raise InputError(f"{path}: cannot read the file as UTF-8 text: {e}") from e

    def sources(keep) -> dict[str, str]:
        return {f: read(f) for f in files if f.endswith(".py") and keep(f)}

    out = check_top_level(files)
    out += check_examples_only(files)
    out += check_example_shape(files, read("examples/README.md"))
    out += check_integration_shape(files, read("integrations/README.md"), read("pyproject.toml"))
    out += check_core_imports(
        sources(lambda f: _under(f, CORE_PACKAGE_DIR) and not _under(f, CORE_GENERATED_DIR)),
        read(CORE_PYPROJECT),
    )
    out += check_integration_isolation(
        sources(lambda f: _under(f, "integrations") or f.startswith(NO_INTEGRATION_IMPORTS)),
        integration_packages(files),
    )
    out += check_framework_imports(sources(lambda f: not _under(f, "examples")))
    out += check_example_imports(sources(lambda f: not _under(f, "examples")))
    out += check_go_placement(files)
    if with_go:
        out += check_go_layers(go_imports(root))
        out += check_go_guest_deps(go_guest_deps(root))
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--no-go", action="store_true", help="skip go-layers (needs the go tool)")
    args = parser.parse_args(argv)
    try:
        root = Path(_run(["git", "rev-parse", "--show-toplevel"], Path(__file__).resolve().parent).strip())
        violations = check_repository(root, with_go=not args.no_go)
    except InputError as e:
        print(f"layers: {e}", file=sys.stderr)
        return 2
    if violations:
        print("Code is outside its layer (rules: hack/verify/layers.py, docs/dev/code-layout.md):", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        return 1
    print("layers: top-level directories, examples, integrations, framework, Python and Go imports are in their layers")
    return 0


if __name__ == "__main__":
    sys.exit(main())
