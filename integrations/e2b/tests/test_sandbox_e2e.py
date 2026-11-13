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

"""The classes on a live cluster, as an E2B program uses them.

Skipped unless SUBSTRATE_E2E_API_URL points at a reachable ate-env-api and
E2B_E2E_TEMPLATE names a base template with the envd layer
(integrations/e2b/hack/base-template.sh). SUBSTRATE_ENV_AUTH_TOKEN is the
caller token when the API requires one. E2B_E2E_ATESPACE names the atespace
when it is not the default. E2B_E2E_MUSL_IMAGE is a digest-pinned musl image
and E2B_E2E_NONROOT_IMAGE one whose USER is not root; without one, its test
is skipped. E2B_E2E_BIG_FILE_MIB is the size of the large file, 100 by default.

Each test goes through the public surface of the E2B SDK only: the calls are
the ones a program written for ``from e2b import Sandbox`` makes. The
commands, files and watchers are the SDK's own code on the real envd, under
gVisor, through the port proxy.
"""

from __future__ import annotations

import hashlib
import os
import re
import time

import pytest
from e2b import (
    CommandExitException,
    FilesystemEventType,
    NotFoundException,
    PtySize,
    SandboxException,
    SandboxNotFoundException,
    SandboxState,
)

from ate_env_e2b import AsyncSandbox, Sandbox

API_URL = os.environ.get("SUBSTRATE_E2E_API_URL")
TEMPLATE = os.environ.get("E2B_E2E_TEMPLATE")
ATESPACE = os.environ.get("E2B_E2E_ATESPACE", "ate-env")
MUSL_IMAGE = os.environ.get("E2B_E2E_MUSL_IMAGE")
NONROOT_IMAGE = os.environ.get("E2B_E2E_NONROOT_IMAGE")
BIG_FILE_MIB = int(os.environ.get("E2B_E2E_BIG_FILE_MIB", "100"))

pytestmark = pytest.mark.skipif(
    not (API_URL and TEMPLATE),
    reason="SUBSTRATE_E2E_API_URL and E2B_E2E_TEMPLATE not set; needs a live ate-env-api "
    "and a base template with the envd layer",
)


@pytest.fixture(autouse=True)
def settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """The variables that the class reads; the program passes no argument for them."""
    monkeypatch.setenv("SUBSTRATE_ENV_API", API_URL or "")
    monkeypatch.setenv("SUBSTRATE_ENV_ATESPACE", ATESPACE)
    monkeypatch.setenv("SUBSTRATE_ENV_E2B_TEMPLATES", f"base={TEMPLATE}")
    # A key for E2B's cloud in the environment changes nothing.
    monkeypatch.setenv("E2B_API_KEY", "e2b_" + "0" * 40)


def test_a_program_runs_commands_and_reads_and_writes_files() -> None:
    with Sandbox.create(envs={"GREETING": "hello"}) as sandbox:
        # Commands run as root in /root, with the variables of the create.
        result = sandbox.commands.run("echo $GREETING; id -un; pwd; echo oops >&2")
        assert result.exit_code == 0
        assert result.stdout.split() == ["hello", "root", "/root"]
        assert result.stderr.strip() == "oops"

        with pytest.raises(CommandExitException) as failed:
            sandbox.commands.run("echo before; exit 7")
        assert failed.value.exit_code == 7 and failed.value.stdout.strip() == "before"

        # user=, cwd= and envs= of one call. envd starts a command in the home
        # of its user: a user whose home is missing needs a working directory.
        with pytest.raises(SandboxException, match="does not exist"):
            sandbox.commands.run("id -un", user="nobody")
        other = sandbox.commands.run("id -un; pwd; echo $A", user="nobody", cwd="/tmp", envs={"A": "1"})
        assert other.stdout.split() == ["nobody", "/tmp", "1"]

        streamed: list[str] = []
        sandbox.commands.run("echo one; echo two", on_stdout=streamed.append)
        assert "".join(streamed).split() == ["one", "two"]

        # A background command: its output, its end, and a kill.
        handle = sandbox.commands.run("echo started; sleep 1; echo done", background=True)
        assert handle.pid in [process.pid for process in sandbox.commands.list()]
        finished = handle.wait()
        assert (finished.exit_code, finished.stdout.split()) == (0, ["started", "done"])
        sleeper = sandbox.commands.run("sleep 600", background=True)
        assert sleeper.kill() is True
        with pytest.raises(CommandExitException):
            sleeper.wait()
        assert sleeper.pid not in [process.pid for process in sandbox.commands.list()]

        # A PTY: input, output, the end of its shell, and a kill.
        terminal = sandbox.pty.create(PtySize(rows=24, cols=80))
        sandbox.pty.send_stdin(terminal.pid, b"echo pty-$((6*7)); exit\n")
        typed = bytearray()
        assert terminal.wait(on_pty=typed.extend).exit_code == 0
        assert b"pty-42" in typed
        other_terminal = sandbox.pty.create(PtySize(rows=24, cols=80))
        assert sandbox.pty.kill(other_terminal.pid) is True
        assert other_terminal.pid not in [process.pid for process in sandbox.commands.list()]

        # Files, through envd's file routes.
        info = sandbox.files.write("/tmp/dir/a.txt", "content\n")
        assert info.path == "/tmp/dir/a.txt"
        assert sandbox.files.read("/tmp/dir/a.txt") == "content\n"
        assert sandbox.files.exists("/tmp/dir/a.txt") and not sandbox.files.exists("/tmp/dir/no")
        assert [entry.name for entry in sandbox.files.list("/tmp/dir")] == ["a.txt"]
        assert sandbox.commands.run("cat /tmp/dir/a.txt").stdout == "content\n"
        # A file written for another user belongs to that user.
        sandbox.files.write("/tmp/theirs.txt", "x", user="nobody")
        assert sandbox.commands.run("stat -c %U /tmp/theirs.txt").stdout.strip() == "nobody"

        # A directory watcher.
        watcher = sandbox.files.watch_dir("/tmp/dir")
        sandbox.files.write("/tmp/dir/b.txt", "new")
        seen: list[tuple[str, FilesystemEventType]] = []
        deadline = time.monotonic() + 30
        while ("b.txt", FilesystemEventType.CREATE) not in seen and time.monotonic() < deadline:
            seen += [(event.name, event.type) for event in watcher.get_new_events()]
            time.sleep(0.5)
        watcher.stop()
        assert ("b.txt", FilesystemEventType.CREATE) in seen

        assert sandbox.is_running() is True
        info = sandbox.get_info()
        assert (info.sandbox_id, info.template_id, info.state) == (
            sandbox.sandbox_id,
            TEMPLATE,
            SandboxState.RUNNING,
        )
    assert sandbox.is_running() is False


def test_stdin_and_a_large_file_pass_through_the_port_proxy() -> None:
    with Sandbox.create() as sandbox:
        # Streamed stdin, and output while the command runs.
        seen: list[str] = []
        reader = sandbox.commands.run("cat", background=True, stdin=True)
        sandbox.commands.send_stdin(reader.pid, "first\n")
        sandbox.commands.send_stdin(reader.pid, "second\n")
        sandbox.commands.close_stdin(reader.pid)
        assert reader.wait(on_stdout=seen.append).exit_code == 0
        assert "".join(seen).split() == ["first", "second"]

        # A large file, up and down, on plaintext HTTP/1.1.
        size = BIG_FILE_MIB * 1024 * 1024
        data = os.urandom(1024 * 1024) * BIG_FILE_MIB
        sandbox.files.write("/tmp/big.bin", data, request_timeout=600)
        stat = sandbox.commands.run("stat -c %s /tmp/big.bin; sha256sum /tmp/big.bin").stdout.split()
        assert stat[:2] == [str(size), hashlib.sha256(data).hexdigest()]
        back = sandbox.files.read("/tmp/big.bin", format="bytes", request_timeout=600)
        assert len(back) == size and hashlib.sha256(back).digest() == hashlib.sha256(data).digest()


def test_pause_connect_and_kill() -> None:
    sandbox = Sandbox.create()
    try:
        sandbox.files.write("/root/kept.txt", "kept")
        sleeper = sandbox.commands.run("sleep 600", background=True)
        sandbox.files.make_dir("/root/watched")
        watcher = sandbox.files.watch_dir("/root/watched")

        assert sandbox.pause() is True
        assert sandbox.pause() is False
        assert sandbox.get_info().state is SandboxState.PAUSED
        assert sandbox.is_running() is False

        # As another process does it: by the id only.
        again = Sandbox.connect(sandbox.sandbox_id)
        assert again is not sandbox and again.sandbox_id == sandbox.sandbox_id
        assert again.is_running() is True
        assert again.commands.run("cat /root/kept.txt").stdout == "kept"
        # The memory came back too: the process of before the pause runs.
        assert sleeper.pid in [process.pid for process in again.commands.list()]
        assert again.commands.kill(sleeper.pid) is True
        # A watcher of before the pause delivers the events of after it.
        again.files.write("/root/watched/after.txt", "x")
        seen: list[tuple[str, FilesystemEventType]] = []
        deadline = time.monotonic() + 30
        while ("after.txt", FilesystemEventType.CREATE) not in seen and time.monotonic() < deadline:
            seen += [(event.name, event.type) for event in watcher.get_new_events()]
            time.sleep(0.5)
        assert ("after.txt", FilesystemEventType.CREATE) in seen
    finally:
        assert sandbox.kill() is True
    assert sandbox.is_running() is False
    assert sandbox.kill() is False
    assert Sandbox.kill(sandbox.sandbox_id) is False
    with pytest.raises(SandboxNotFoundException):
        sandbox.get_info()
    # What integrations/e2b/README.md says a data-plane call gets then.
    with pytest.raises(NotFoundException):
        sandbox.commands.run("true")


@pytest.mark.skipif(not MUSL_IMAGE, reason="E2B_E2E_MUSL_IMAGE not set")
def test_an_image_on_the_base_template_serves_the_file_calls_also_on_musl() -> None:
    # The image has no /bin/bash, which the SDK's commands need; envd's file
    # routes and the root check of create need none.
    with Sandbox.create(MUSL_IMAGE) as sandbox:
        sandbox.files.write("/tmp/dir/a.txt", "musl\n")
        assert sandbox.files.read("/tmp/dir/a.txt") == "musl\n"
        assert [entry.name for entry in sandbox.files.list("/tmp/dir")] == ["a.txt"]
        assert sandbox.get_info().state is SandboxState.RUNNING


@pytest.mark.skipif(not NONROOT_IMAGE, reason="E2B_E2E_NONROOT_IMAGE not set")
def test_create_refuses_an_image_that_does_not_run_as_root() -> None:
    with pytest.raises(SandboxException) as refused:
        Sandbox.create(NONROOT_IMAGE)
    message = str(refused.value)
    assert re.search(r"runs as user \d+.*, not as root", message), message
    assert NONROOT_IMAGE in message and "integrations/e2b/README.md" in message
    # The environment of the refused create is gone, or on its way out.
    env_id = re.search(r"\(environment (e2b-[0-9a-f]+)\)", message).group(1)
    deadline = time.monotonic() + 60
    while True:
        try:
            Sandbox.get_info(env_id)
        except SandboxNotFoundException:
            break
        assert time.monotonic() < deadline, f"{env_id} remains"
        time.sleep(1)


@pytest.mark.asyncio
async def test_the_async_class_runs_a_command_and_a_file_call() -> None:
    async with await AsyncSandbox.create() as sandbox:
        result = await sandbox.commands.run("id -un; pwd")
        assert result.stdout.split() == ["root", "/root"]
        await sandbox.files.write("/tmp/a.txt", "async")
        assert await sandbox.files.read("/tmp/a.txt") == "async"
        assert await sandbox.is_running() is True
    assert await sandbox.is_running() is False
