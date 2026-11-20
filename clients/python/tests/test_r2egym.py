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

"""The R2E-Gym adapter against a stand-in for R2E-Gym (installed into
sys.modules, as agent-sandbox-rl's adapter tests do) and a scripted fleet
handle. R2E-Gym itself is not on PyPI, so nothing here imports it."""

from __future__ import annotations

import io
import os
import subprocess
import sys
import tarfile
import threading
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ate_env_rl.adapters
import ate_env_rl.adapters.r2egym as adapter
from ate_env_rl import CommandExecutionError, CommandTimeoutError


class _ExecResult:
    def __init__(self, exit_code, stdout="", stderr="", timed_out=False):
        self.exit_code, self.stdout, self.stderr, self.timed_out = exit_code, stdout, stderr, timed_out


class _Handle:
    """A fleet handle: exec is scripted by command substring, write_file records."""

    def __init__(self, sandbox_id="sb-1", with_ds=True, image="example.com/task@sha256:" + "c" * 64):
        meta = {"repo": "django/django"}
        if with_ds:
            meta["ds"] = {"instance_id": "x", "docker_image": image, "repo": "django/django"}
        self.sandbox_id = sandbox_id
        self.task = SimpleNamespace(id="x", image=image, metadata=meta)
        self.execs: list[tuple[str, str, float | None]] = []
        self.envs: list[dict | None] = []
        self.files: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []
        self.replies: dict[str, _ExecResult] = {}

    def exec(self, command, cwd="", env=None, timeout_s=None, check=True):
        # Mirrors SandboxHandle.exec: with check=True a timeout or a non-zero
        # exit raises instead of returning the result.
        self.calls.append(("exec", command))
        self.execs.append((command, cwd, timeout_s))
        self.envs.append(env)
        res = _ExecResult(0, stdout=f"ran: {command}\n")
        for needle, reply in self.replies.items():
            if needle in command:
                res = reply
                break
        if check and res.timed_out:
            raise CommandTimeoutError(f"Command {command!r} timed out", result=res)
        if check and res.exit_code != 0:
            raise CommandExecutionError(
                f"Command {command!r} failed with exit code {res.exit_code}:\nSTDOUT: {res.stdout}", result=res
            )
        return res

    def write_file(self, path, content):
        self.calls.append(("write_file", path))
        self.files[path] = content if isinstance(content, bytes) else content.encode()


class _FakeBaseRuntime:
    """What R2E-Gym's DockerRuntime does around the four hooks, and nothing else.
    config and client stand for the kubernetes imports the real base loads."""

    def __init__(self, ds, docker_image=None, command="/bin/bash", logger=None, backend="docker", **kw):
        assert ds
        docker_mod.config.load_kube_config()  # the real base loads a kubeconfig here
        self.client = docker_mod.client.CoreV1Api()
        self.ds, self.docker_image, self.command = ds, docker_image, command
        self.logger = logger or MagicMock()
        self.backend, self.repo_name, self.docker_kwargs = backend, "repo", kw
        self.container = None
        self.container_name = None
        self.start_container(docker_image, command, None)
        self.setup_env()

    def start_container(self, image, command, name, **kw):
        if self.backend == "kubernetes-sandbox":
            try:
                self._start_kubernetes_sandbox()
            except Exception:  # the real base swallows and stops
                self.stop_container()

    def stop_container(self):
        if self.backend == "kubernetes-sandbox":
            self._stop_kubernetes_sandbox()

    def close(self):
        self.stop_container()

    def setup_env(self):
        self.run("echo setup")

    def run(self, code, timeout=90, args="", workdir=None):
        return self._run_kubernetes(code, timeout, args, workdir=workdir or "/testbed")

    def copy_to_container(self, src, dest):
        return self._copy_to_container_kubernetes(src, dest)

    def get_task_instruction(self):
        return "instruction"


class _FakeCommandParser:
    def parse_command_file(self, path):
        with open(path) as f:
            f.read()
        return [SimpleNamespace(name=os.path.splitext(os.path.basename(path))[0])]


class _FakeBaseRepoEnv:
    """Mirrors R2E-Gym's RepoEnv.add_commands: parse each file, strip .py,
    copy into /usr/local/bin, and chmod +x (or source .sh / non-shebang files)."""

    def _is_shebang_script(self, cmd_file):
        with open(cmd_file) as f:
            return f.readline().strip().startswith("#!")

    def add_commands(self, cmd_files):
        cmds = []
        for cmd_file in cmd_files:
            cmds.extend(self.cmd_parser.parse_command_file(cmd_file))
            _, ext = os.path.splitext(cmd_file)
            cmd_name = os.path.basename(cmd_file)
            if ext == ".py" or self._is_shebang_script(cmd_file):
                container_cmd_name = cmd_name[:-3] if ext == ".py" else cmd_name
                container_path = f"/usr/local/bin/{container_cmd_name}"
                self.runtime.copy_to_container(cmd_file, container_path)
                self.runtime.run(f"chmod +x {container_path}")
            elif ext == ".sh":
                container_path = f"/usr/local/bin/{cmd_name}"
                self.runtime.copy_to_container(cmd_file, container_path)
                self.runtime.run(f"bash -c 'source {container_path}'")
            else:
                container_path = f"/usr/local/bin/{cmd_name}"
                self.runtime.copy_to_container(cmd_file, container_path)
                self.runtime.run(f"chmod +x {container_path}")
                self.runtime.run(f"bash -c 'source {container_path}'")
        self.commands = cmds

    def close(self):
        self.runtime.close()


class _Boom:
    """kubeconfig loading and CoreV1Api must not be reached through ate-env."""

    def load_kube_config(self):
        raise AssertionError("load_kube_config was called")

    def load_incluster_config(self):
        raise AssertionError("load_incluster_config was called")

    def CoreV1Api(self, *a, **k):
        raise AssertionError("CoreV1Api was built")


docker_mod = None


@pytest.fixture
def fake_r2egym(monkeypatch):
    global docker_mod
    adapter._CLASSES = None

    def mod(name):
        m = types.ModuleType(name)
        monkeypatch.setitem(sys.modules, name, m)
        return m

    mod("r2egym")
    mod("r2egym.agenthub")
    mod("r2egym.agenthub.environment")
    env_mod = mod("r2egym.agenthub.environment.env")
    env_mod.EnvArgs = lambda ds=None, repo_path=None, docker_image=None: SimpleNamespace(ds=ds)
    env_mod.RepoEnv = _FakeBaseRepoEnv
    mod("r2egym.agenthub.runtime")
    docker_mod = mod("r2egym.agenthub.runtime.docker")
    docker_mod.DockerRuntime = _FakeBaseRuntime
    docker_mod.CMD_TIMEOUT = 900
    docker_mod.config = _Boom()
    docker_mod.client = _Boom()
    mod("r2egym.agenthub.utils")
    mod("r2egym.agenthub.utils.log").get_logger = lambda *a, **k: MagicMock()
    mod("r2egym.agenthub.agent")
    mod("r2egym.agenthub.agent.commands").ParseCommandBash = _FakeCommandParser
    yield docker_mod
    adapter._CLASSES = None


def test_adapters_exposes_r2egym_without_openhands(monkeypatch):
    monkeypatch.setitem(sys.modules, "openhands", None)
    from ate_env_rl.adapters import r2egym

    assert r2egym is adapter
    assert "r2egym" in ate_env_rl.adapters.__all__
    assert adapter.logger.name == "ate_env_rl.adapters.r2egym"
    assert adapter.__all__ == [
        "fresh_eval_env",
        "make_fleet_repo_env",
        "r2egym_command_files",
    ]


def test_guard_without_r2egym(monkeypatch):
    adapter._CLASSES = None
    monkeypatch.setitem(sys.modules, "r2egym", None)
    with pytest.raises(RuntimeError, match=r"^ate_env_rl\.adapters\.r2egym .*not on PyPI"):
        adapter.make_fleet_repo_env(_Handle())
    with pytest.raises(RuntimeError, match="not on PyPI"):
        adapter.r2egym_command_files()


def test_star_import_works_without_r2egym(monkeypatch):
    adapter._CLASSES = None
    monkeypatch.setitem(sys.modules, "r2egym", None)
    ns: dict[str, object] = {}
    exec("from ate_env_rl.adapters.r2egym import *", ns)
    assert {"fresh_eval_env", "make_fleet_repo_env", "r2egym_command_files"} <= set(ns)
    # The lazy classes are not in __all__: a star import must not need R2E-Gym.
    assert "AteEnvRuntime" not in ns and "AteEnvRepoEnv" not in ns
    assert "AteEnvRuntime" not in adapter.__all__ and "AteEnvRepoEnv" not in adapter.__all__
    with pytest.raises(RuntimeError, match="not on PyPI"):
        _ = adapter.AteEnvRuntime
    with pytest.raises(RuntimeError, match="not on PyPI"):
        _ = adapter.AteEnvRepoEnv


def test_bind_without_kubernetes_and_no_op_teardown(fake_r2egym):
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    rt = env.runtime
    assert rt.container is handle and rt.container_name == "sb-1"
    assert rt.client is None  # built from the stub, never from kubernetes
    assert handle.execs[0][0] == "echo setup"  # setup_env ran through the handle
    env.close()  # no release, no delete: the fleet owns the environment
    assert rt.container is handle
    # Outside a runtime's construction the real kubernetes names answer again.
    with pytest.raises(AssertionError, match="load_kube_config"):
        fake_r2egym.config.load_kube_config()
    with pytest.raises(AssertionError, match="CoreV1Api"):
        fake_r2egym.client.CoreV1Api()


def test_kubernetes_bypass_is_thread_local_and_holds_no_lock(fake_r2egym):
    """While one thread builds a runtime, another thread still reaches the real
    kubernetes names, and the build lock is free: rollouts set up in parallel."""
    seen: dict[str, object] = {}
    real_init = fake_r2egym.DockerRuntime.__init__

    def probing_init(self, *args, **kwargs):
        def probe():
            try:
                fake_r2egym.config.load_kube_config()
                seen["other_thread"] = "bypassed"
            except AssertionError:
                seen["other_thread"] = "real"
            seen["lock_free"] = adapter._BUILD_LOCK.acquire(blocking=False)
            if seen["lock_free"]:
                adapter._BUILD_LOCK.release()

        t = threading.Thread(target=probe)
        t.start()
        t.join()
        return real_init(self, *args, **kwargs)

    fake_r2egym.DockerRuntime.__init__ = probing_init
    adapter.make_fleet_repo_env(_Handle())
    assert seen == {"other_thread": "real", "lock_free": True}


def test_run_maps_exit_codes_timeouts_and_errors(fake_r2egym):
    handle = _Handle()
    handle.replies["ok"] = _ExecResult(0, stdout="\x1b[32mgreen\x1b[0m\r\n")
    handle.replies["fail"] = _ExecResult(2, stdout="out", stderr="err")
    handle.replies["slow"] = _ExecResult(None, timed_out=True)
    handle.replies["t124"] = _ExecResult(124)
    env = adapter.make_fleet_repo_env(handle)
    rt = env.runtime
    assert rt.run("ok") == ("green\n", "0")
    assert rt.run("fail") == ("outerr", "Error: Exit code 2")
    assert rt.run("slow", timeout=7) == ("The command took too long to execute (>7s)", "-1")
    assert rt.run("t124") == ("The command took too long to execute (>90s)", "-1")
    command, cwd, timeout_s = handle.execs[-1]
    assert command == "t124" and cwd == "/testbed" and timeout_s == 90

    def broken(*a, **k):
        raise ConnectionError("gone")

    handle.exec = broken
    assert rt.run("x") == ("Error executing command in environment: ConnectionError('gone')", "-1")


def test_copy_stages_a_tar_and_extracts_into_the_destination(fake_r2egym, tmp_path):
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    handle.calls.clear()
    src = tmp_path / "tool.py"
    src.write_text("print(1)\n")
    env.runtime.copy_to_container(str(src), "/usr/local/bin/renamed.py")
    # One copy is two guest calls: write_file of the tar, then unpack-and-remove.
    assert [kind for kind, _ in handle.calls] == ["write_file", "exec"]
    (staged, data), = handle.files.items()
    assert staged.startswith("/tmp/ate-env-copy-") and staged.endswith(".tar")
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        assert tar.getnames() == ["renamed.py"]
    command = handle.execs[-1][0]
    assert f"tar --no-same-owner -xmf {staged} -C /usr/local/bin" in command and f"rm -f {staged}" in command
    handle.calls.clear()
    handle.replies["-xmf"] = _ExecResult(1, stderr="tar: no space")
    with pytest.raises(RuntimeError, match="no space"):
        env.runtime.copy_to_container(str(src), "/usr/local/bin/again.py")
    assert [kind for kind, _ in handle.calls] == ["write_file", "exec"]


def test_copy_archives_members_as_root(fake_r2egym, tmp_path):
    # F-AC4-1: a sandbox root cannot chown to the host uid, thus tar must
    # neither carry the host owner nor try to restore it.
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    tree = tmp_path / "cmds"
    tree.mkdir()
    (tree / "a.sh").write_text("echo a\n")
    env.runtime.copy_to_container(str(tree), "/usr/local/bin/cmds")
    (_, data), = handle.files.items()
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        members = tar.getmembers()
    assert {m.name for m in members} == {"cmds", "cmds/a.sh"}
    assert all((m.uid, m.gid, m.uname, m.gname) == (0, 0, "root", "root") for m in members)
    assert "--no-same-owner" in handle.execs[-1][0]


def test_failed_copy_surfaces_the_tar_error(fake_r2egym, tmp_path):
    # The tar error must reach the caller, not a strict-mode exception that
    # carries stdout only.
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    src = tmp_path / "x.patch"
    src.write_text("diff\n")
    handle.replies["-xmf"] = _ExecResult(
        2, stdout="", stderr="tar: x.patch: Cannot change ownership to uid 804416, gid 0: Operation not permitted"
    )
    with pytest.raises(RuntimeError, match=r"exit 2\).*Cannot change ownership"):
        env.runtime.copy_to_container(str(src), "/tmp/x.patch")


def test_copy_timeout_says_so_and_removes_the_staged_tar(fake_r2egym, tmp_path):
    # A timeout kills the shell before its own rm -f runs. The error must name
    # the timeout, not "exit None", and a separate exec must remove the tar.
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    src = tmp_path / "x.patch"
    src.write_text("diff\n")
    handle.replies["-xmf"] = _ExecResult(None, timed_out=True)
    with pytest.raises(RuntimeError, match=r"-> /tmp/x\.patch timed out after 120 s"):
        env.runtime.copy_to_container(str(src), "/tmp/x.patch")
    (staged, _), = handle.files.items()
    assert handle.execs[-1][0] == f"rm -f {staged}"
    # A failed cleanup must not hide the timeout.
    scripted_exec = handle.exec

    def exec_with_failing_cleanup(command, **kwargs):
        if command.startswith("rm -f "):
            raise ConnectionError("guest gone")
        return scripted_exec(command, **kwargs)

    handle.exec = exec_with_failing_cleanup
    with pytest.raises(RuntimeError, match="timed out after 120 s"):
        env.runtime.copy_to_container(str(src), "/tmp/x.patch")


def test_bind_failure_raises(fake_r2egym):
    handle = _Handle(sandbox_id="")  # an empty id cannot be what the hook bound
    handle.sandbox_id = None
    with pytest.raises(RuntimeError, match="failed to bind"):
        adapter.make_fleet_repo_env(handle)


def test_requires_the_dataset_row(fake_r2egym):
    with pytest.raises(
        RuntimeError,
        match=(
            r"metadata\['ds'\].*ate_env_rl\.SweBenchSource\(keep_row=True\).*"
            r"under metadata\['ds'\].*Task\(id=\.\.\., image=\.\.\., metadata=\{'ds': row\}\)"
        ),
    ):
        adapter.make_fleet_repo_env(_Handle(with_ds=False))


def _make_four_tool_files(root):
    tools_dir = root / "tools"
    (tools_dir / "r2egym").mkdir(parents=True)
    rels = ("r2egym/file_editor.py", "search.py", "r2egym/execute_bash.py", "finish.py")
    paths = []
    for rel in rels:
        p = tools_dir / rel
        p.write_text(f"#!/usr/bin/env python3\n# {rel}\n")
        p.chmod(0o644)
        paths.append(str(p))
    return paths


def test_command_files_are_copied_in_one_batch(fake_r2egym, tmp_path):
    files = _make_four_tool_files(tmp_path)
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    handle.calls.clear()
    env.add_commands(files)
    # Four tools take two guest calls in total: one archive write and one unpack+chmod+rm.
    assert [kind for kind, _ in handle.calls] == ["write_file", "exec"]
    (staged, data), = handle.files.items()
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        members = tar.getmembers()
    assert [m.name for m in members] == ["file_editor", "search", "execute_bash", "finish"]
    assert all((m.uid, m.gid, m.uname, m.gname) == (0, 0, "root", "root") for m in members)
    command = handle.execs[-1][0]
    assert f"tar --no-same-owner -xmf {staged} -C /usr/local/bin" in command
    assert (
        "chmod +x /usr/local/bin/file_editor /usr/local/bin/search "
        "/usr/local/bin/execute_bash /usr/local/bin/finish"
    ) in command
    assert f"rm -f {staged}" in command
    assert [c.name for c in env.commands] == ["file_editor", "search", "execute_bash", "finish"]


def test_batch_flushes_before_sourcing_shell_command_files(fake_r2egym, tmp_path):
    py1 = tmp_path / "file_editor.py"
    py1.write_text("#!/usr/bin/env python3\n")
    sh = tmp_path / "helper.sh"
    sh.write_text("helper() {\n  echo hi\n}\n")
    py2 = tmp_path / "finish.py"
    py2.write_text("#!/usr/bin/env python3\n")
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    handle.calls.clear()
    env.add_commands([str(py1), str(sh), str(py2)])
    # First batch (file_editor + helper.sh) unpacks before `source /usr/local/bin/helper.sh`,
    # then finish unpacks after.
    assert [kind for kind, _ in handle.calls] == [
        "write_file",
        "exec",
        "exec",
        "write_file",
        "exec",
    ]
    assert "chmod +x /usr/local/bin/file_editor" in handle.calls[1][1]
    assert "chmod +x /usr/local/bin/helper.sh" not in handle.calls[1][1]
    assert "source /usr/local/bin/helper.sh" in handle.calls[2][1]

    # A missing file midway raises before flushing any queued tar to the guest.
    handle.calls.clear()
    prev = list(env.commands)
    with pytest.raises(FileNotFoundError):
        env.add_commands([str(py1), str(tmp_path / "missing.py")])
    assert handle.calls == []
    assert env.commands == prev


def test_fresh_eval_env_acquires_a_second_environment_and_releases_it(fake_r2egym):
    first = _Handle(sandbox_id="sb-agent")
    second = _Handle(sandbox_id="sb-eval")
    fleet = SimpleNamespace(acquire=MagicMock(return_value=second), release=MagicMock())
    with adapter.fresh_eval_env(fleet, first) as (eval_handle, env):
        assert eval_handle is second and env.runtime.container_name == "sb-eval"
        fleet.acquire.assert_called_once_with(first.task)
        fleet.release.assert_not_called()
    fleet.release.assert_called_once_with(second)


def test_classes_are_built_once_under_concurrency(fake_r2egym):
    results = []
    threads = [threading.Thread(target=lambda: results.append(adapter._build_classes())) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len({id(r[0]) for r in results}) == 1
    assert adapter.AteEnvRuntime is results[0][0] and adapter.AteEnvRepoEnv is results[0][1]


def test_run_uses_the_r2e_path_and_docker_environment(fake_r2egym):
    # F-AC4-2: commands run with R2E-Gym's PATH (the testbed venv first) plus
    # docker_kwargs["environment"], which overrides PATH, as R2E-Gym's
    # kubernetes backends do.
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    env.runtime.run("python --version")
    assert handle.envs[-1] == {"PATH": adapter._R2E_DOCKER_PATH}
    assert handle.envs[-1]["PATH"].startswith("/root/.venv/bin:")
    fake_r2egym.DOCKER_PATH = "/custom/bin:/usr/bin"
    adapter._CLASSES = None
    rt = adapter._build_classes()[0](handle, environment={"PIP_NO_INDEX": 1})
    rt.run("pip list")
    assert handle.envs[-1] == {"PATH": "/custom/bin:/usr/bin", "PIP_NO_INDEX": "1"}


def test_docker_environment_can_be_a_list_of_key_value_strings(fake_r2egym):
    # The Docker SDK takes environment as a dict or as ["KEY=VALUE", ...].
    handle = _Handle()
    runtime_cls = adapter._build_classes()[0]
    rt = runtime_cls(handle, environment=["PIP_NO_INDEX=1", "PATH=/opt/bin:/usr/bin", "OPTS=a=b"])
    rt.run("pip list")
    assert handle.envs[-1] == {"PATH": "/opt/bin:/usr/bin", "PIP_NO_INDEX": "1", "OPTS": "a=b"}
    rt = runtime_cls(handle, environment={"PATH": "/opt/bin"})
    rt.run("pip list")
    assert handle.envs[-1] == {"PATH": "/opt/bin"}
    with pytest.raises(TypeError, match="KEY=VALUE"):
        runtime_cls(handle, environment=["NO_EQUALS_SIGN"])
    with pytest.raises(TypeError, match="dict or a list"):
        runtime_cls(handle, environment="A=1")


class _ShellHandle(_Handle):
    """A handle whose exec runs the command in a real shell (cwd: ``root``) and
    whose write_file writes the real path."""

    def __init__(self, root, *, remap_usr_local_bin: bool = False):
        super().__init__()
        self.root = str(root)
        self._remap_usr_local_bin = remap_usr_local_bin

    def exec(self, command, cwd="", env=None, timeout_s=None, check=True):
        self.calls.append(("exec", command))
        self.execs.append((command, cwd, timeout_s))
        if command == "echo setup":
            return _ExecResult(0)
        shell_cmd = (
            command.replace("/usr/local/bin", f"{self.root}/usr/local/bin")
            if self._remap_usr_local_bin
            else command
        )
        done = subprocess.run(["/bin/sh", "-c", shell_cmd], cwd=self.root, capture_output=True, text=True)
        return _ExecResult(done.returncode, stdout=done.stdout, stderr=done.stderr)

    def write_file(self, path, content):
        super().write_file(path, content)
        with open(path, "wb") as f:
            f.write(self.files[path])


def test_copy_extracts_with_real_tar_and_mkdir(fake_r2egym, tmp_path):
    # The command runs in a real shell: a destination that ends with "/" is the
    # directory to copy into, and a directory whose name starts with "-" is
    # not read as an option of mkdir.
    root = tmp_path / "root"
    root.mkdir()
    handle = _ShellHandle(root)
    env = adapter.make_fleet_repo_env(handle)
    src = tmp_path / "tool.py"
    src.write_text("print(1)\n")
    tree = tmp_path / "cmds"
    tree.mkdir()
    (tree / "a.sh").write_text("echo a\n")

    handle.calls.clear()
    env.runtime.copy_to_container(str(src), f"{root}/opt/tools/")
    assert [kind for kind, _ in handle.calls] == ["write_file", "exec"]
    assert (root / "opt/tools/tool.py").read_text() == "print(1)\n"
    env.runtime.copy_to_container(str(tree) + "/", f"{root}/opt/")
    assert (root / "opt/cmds/a.sh").read_text() == "echo a\n"
    env.runtime.copy_to_container(str(src), "-dir/f.py")
    assert (root / "-dir/f.py").read_text() == "print(1)\n"
    env.runtime.copy_to_container(str(src), f"{root}/opt/tools/renamed.py")
    assert (root / "opt/tools/renamed.py").read_text() == "print(1)\n"
    for staged in handle.files:  # each staged tar is removed after the extract
        assert not os.path.exists(staged)


def test_failed_unpack_in_real_shell_removes_staged_tar_and_preserves_exit(fake_r2egym, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    handle = _ShellHandle(root)
    env = adapter.make_fleet_repo_env(handle)
    src = tmp_path / "x.patch"
    src.write_text("diff\n")

    # Non-empty directory at the destination file path: mkdir -p succeeds, tar exits 2,
    # rm -f removes the staged archive, and tar's exit code and stderr surface.
    blocker = root / "sub" / "x.patch"
    blocker.mkdir(parents=True)
    (blocker / "keep").write_text("1\n")
    handle.calls.clear()
    with pytest.raises(RuntimeError, match=r"failed \(exit 2\):.*tar:"):
        env.runtime.copy_to_container(str(src), str(blocker))
    assert [kind for kind, _ in handle.calls] == ["write_file", "exec"]
    staged = handle.calls[0][1]
    assert not os.path.exists(staged)

    # Regular file at the parent path: mkdir -p fails before tar runs, rm -f still removes
    # the staged archive, and mkdir's non-zero exit surfaces.
    file_parent = root / "not_a_dir"
    file_parent.write_text("x\n")
    handle.calls.clear()
    with pytest.raises(RuntimeError, match=r"failed \(exit 1\):"):
        env.runtime.copy_to_container(str(src), f"{file_parent}/x.patch")
    assert [kind for kind, _ in handle.calls] == ["write_file", "exec"]
    assert not os.path.exists(handle.calls[0][1])


def test_four_tool_batch_in_real_shell_sets_modes_and_cleans_up_on_success_and_failure(fake_r2egym, tmp_path):
    files = _make_four_tool_files(tmp_path)
    root = tmp_path / "ok"
    root.mkdir()
    handle = _ShellHandle(root, remap_usr_local_bin=True)
    env = adapter.make_fleet_repo_env(handle, command_files=files)
    # 1 setup exec + 2 calls (write_file + exec) for all 4 tools.
    assert [kind for kind, _ in handle.calls] == ["exec", "write_file", "exec"]
    for name, rel in (
        ("file_editor", "r2egym/file_editor.py"),
        ("search", "search.py"),
        ("execute_bash", "r2egym/execute_bash.py"),
        ("finish", "finish.py"),
    ):
        dest = root / "usr/local/bin" / name
        assert dest.read_text() == f"#!/usr/bin/env python3\n# {rel}\n"
        assert os.access(dest, os.X_OK)
    for staged in handle.files:
        assert not os.path.exists(staged)
    assert len(env.commands) == 4

    # Failed unpack of the batch removes the staged tar, surfaces tar's error, and leaves
    # env.commands untouched.
    bad_root = tmp_path / "bad"
    blocker = bad_root / "usr/local/bin" / "search"
    blocker.mkdir(parents=True)
    (blocker / "keep").write_text("1\n")
    bad_handle = _ShellHandle(bad_root, remap_usr_local_bin=True)
    bad_env = adapter.make_fleet_repo_env(bad_handle)
    bad_handle.calls.clear()
    with pytest.raises(RuntimeError, match=r"failed \(exit 2\):.*tar:"):
        bad_env.add_commands(files)
    assert [kind for kind, _ in bad_handle.calls] == ["write_file", "exec"]
    assert not os.path.exists(bad_handle.calls[0][1])
    assert bad_env.commands == []

    # Timeout on the batched unpack runs the fallback rm -f and leaves env.commands untouched.
    to_handle = _Handle()
    to_env = adapter.make_fleet_repo_env(to_handle)
    to_handle.calls.clear()
    to_handle.replies["-xmf"] = _ExecResult(None, timed_out=True)
    with pytest.raises(RuntimeError, match="timed out after 120 s"):
        to_env.add_commands(files)
    (staged_to, _), = to_handle.files.items()
    assert [kind for kind, _ in to_handle.calls] == ["write_file", "exec", "exec"]
    assert to_handle.calls[-1] == ("exec", f"rm -f {staged_to}")
    assert to_env.commands == []


def test_reset_rebinds_and_detects_a_failed_rebind(fake_r2egym):
    handle = _Handle()
    env = adapter.make_fleet_repo_env(handle)
    env.done = True
    assert env.reset() == "Environment reset"
    assert env.done is False and env.state is None
    assert env.runtime.container is handle and env.runtime.container_name == "sb-1"
    # The handle loses its sandbox: the base swallows the hook's error, and the
    # old binding must not pass for a new one.
    handle.sandbox_id = None
    with pytest.raises(RuntimeError, match="rebinding the environment failed"):
        env.reset()
    assert env.runtime.container is None
    # A handle that now names another sandbox binds that one.
    handle.sandbox_id = "sb-2"
    assert env.reset() == "Environment reset"
    assert env.runtime.container_name == "sb-2"


def test_fresh_eval_env_rejects_an_async_fleet(fake_r2egym):
    from ate_env_rl import AsyncSandboxFleet

    fleet = AsyncSandboxFleet.__new__(AsyncSandboxFleet)  # no client: nothing may be called
    with pytest.raises(TypeError, match=r"needs a SandboxFleet, got AsyncSandboxFleet"):
        with adapter.fresh_eval_env(fleet, _Handle()):
            pytest.fail("the body must not run")


def test_fresh_eval_env_validates_before_it_acquires(fake_r2egym):
    fleet = SimpleNamespace(acquire=MagicMock(), release=MagicMock())
    with pytest.raises(RuntimeError, match=r"metadata\['ds'\]"):
        with adapter.fresh_eval_env(fleet, _Handle(with_ds=False)):
            pytest.fail("the body must not run")
    fleet.acquire.assert_not_called()
    fleet.release.assert_not_called()


def test_fresh_eval_env_without_r2egym_acquires_nothing(monkeypatch):
    adapter._CLASSES = None
    monkeypatch.setitem(sys.modules, "r2egym", None)
    fleet = SimpleNamespace(acquire=MagicMock(), release=MagicMock())
    with pytest.raises(RuntimeError, match="not on PyPI"):
        with adapter.fresh_eval_env(fleet, _Handle()):
            pytest.fail("the body must not run")
    fleet.acquire.assert_not_called()
    fleet.release.assert_not_called()


def test_invalid_docker_environment_runs_no_command(fake_r2egym):
    handle = _Handle()
    runtime_cls = adapter._build_classes()[0]
    with pytest.raises(TypeError, match="dict or a list"):
        runtime_cls(handle, environment="BAD")
    assert handle.execs == []  # not even the base's setup commands
    # A value that turns invalid after construction raises from run(); it is
    # not reported as a failed command.
    rt = runtime_cls(handle, environment={"A": "1"})
    rt.docker_kwargs["environment"] = "BAD"
    sent = len(handle.execs)
    with pytest.raises(TypeError, match="dict or a list"):
        rt.run("true")
    assert len(handle.execs) == sent
    rt.logger.error.assert_not_called()


def test_command_files_come_from_the_package_directory(fake_r2egym, monkeypatch):
    pkg = sys.modules["r2egym"]
    tools = ["r2egym/file_editor.py", "search.py", "r2egym/execute_bash.py", "finish.py"]
    monkeypatch.setattr(pkg, "__file__", "/src/R2E-Gym/src/r2egym/__init__.py", raising=False)
    assert adapter.r2egym_command_files() == [
        os.path.join("/src/R2E-Gym/src/r2egym/agenthub/tools", t) for t in tools
    ]
    # A namespace package: __file__ is None, __path__ names the directory.
    monkeypatch.setattr(pkg, "__file__", None, raising=False)
    monkeypatch.setattr(pkg, "__path__", ["/ns/r2egym"], raising=False)
    assert adapter.r2egym_command_files() == [os.path.join("/ns/r2egym/agenthub/tools", t) for t in tools]
    monkeypatch.setattr(pkg, "__path__", [], raising=False)
    with pytest.raises(RuntimeError, match="no __file__ and no __path__"):
        adapter.r2egym_command_files()
