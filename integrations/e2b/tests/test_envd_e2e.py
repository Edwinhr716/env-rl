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

"""The envd runtime layer in live ate-env environments.

Skipped unless SUBSTRATE_E2E_API_URL points at a reachable ate-env-api and
E2B_E2E_TEMPLATE names a base template with the envd layer
(integrations/e2b/hack/base-template.sh). SUBSTRATE_ENV_AUTH_TOKEN is the
caller token when the API requires one. E2B_E2E_ATESPACE names the atespace
when it is not the default. E2B_E2E_MUSL_IMAGE and E2B_E2E_NONROOT_IMAGE are
digest-pinned images for the derived-template test (a musl image; an image
with a non-root USER); without one, its case is skipped.

The tests check the layer and its template, not envd's protocol: envd from the
layer serves in a glibc and a musl image behind the guest's readiness gate, a
derived template keeps the layer, envd and its processes survive a suspend,
the template's capabilities let envd give a file to another user, and the port
proxy asks for the API token. One case records what envd does in an image
with a non-root USER, which the integration does not support. The requests
are envd's own (Connect with JSON bodies, and its HTTP routes) through the
port proxy's header form, with the standard library only.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import pytest_asyncio
from ate_env_rl import Client, Env, EnvError, EnvironmentStatus

API_URL = os.environ.get("SUBSTRATE_E2E_API_URL")
TEMPLATE = os.environ.get("E2B_E2E_TEMPLATE")
ATESPACE = os.environ.get("E2B_E2E_ATESPACE", "ate-env")
MUSL_IMAGE = os.environ.get("E2B_E2E_MUSL_IMAGE")
NONROOT_IMAGE = os.environ.get("E2B_E2E_NONROOT_IMAGE")
AUTH_TOKEN = os.environ.get("SUBSTRATE_ENV_AUTH_TOKEN")

ENVD_PORT = 49983
READY_TIMEOUT = 180.0

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not (API_URL and TEMPLATE),
        reason="SUBSTRATE_E2E_API_URL and E2B_E2E_TEMPLATE not set; needs a live ate-env-api "
        "and a base template with the envd layer",
    ),
]


class StartError(Exception):
    """envd did not start the process; the message is envd's answer."""


class Envd:
    """envd in one environment, reached through the port proxy's header form."""

    def __init__(self, env_id: str, *, api_auth: bool = True):
        self.env_id = env_id
        self.api_auth = api_auth

    def _open(self, method: str, path: str, body: bytes | None = None, content_type: str | None = None):
        headers = {
            "X-Env-Id": self.env_id,
            "X-Env-Atespace": ATESPACE,
            "X-Env-Port": str(ENVD_PORT),
            "Connect-Protocol-Version": "1",
        }
        if AUTH_TOKEN and self.api_auth:
            headers["X-Env-Authorization"] = f"Bearer {AUTH_TOKEN}"
        if content_type:
            headers["Content-Type"] = content_type
        base = API_URL if "://" in (API_URL or "") else f"http://{API_URL}"
        req = urllib.request.Request(base + path, data=body, headers=headers, method=method)
        return urllib.request.urlopen(req, timeout=READY_TIMEOUT)

    def health(self) -> int:
        try:
            with self._open("GET", "/health") as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    def init(self) -> int:
        """POST /init with no fields, which sets nothing; returns the status."""
        try:
            with self._open("POST", "/init", b"{}", "application/json") as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    def upload(self, path: str, data: bytes, user: str) -> int:
        """Writes a file through envd's /files route, as user."""
        query = urllib.parse.urlencode({"path": path, "username": user})
        try:
            with self._open("POST", f"/files?{query}", data, "application/octet-stream") as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    def _start(self, script: str) -> Iterator[dict[str, Any]]:
        """Starts `sh -c script`: yields each event as envd sends it."""
        payload = json.dumps({"process": {"cmd": "/bin/sh", "args": ["-c", script]}}).encode()
        body = struct.pack(">BI", 0, len(payload)) + payload
        try:
            resp = self._open("POST", "/process.Process/Start", body, "application/connect+json")
        except urllib.error.HTTPError as e:
            raise StartError(f"HTTP {e.code}: {e.read().decode(errors='replace')}") from e
        with resp:
            while header := resp.read(5):
                flags, size = struct.unpack(">BI", header)
                frame = json.loads(resp.read(size) or b"{}")
                if flags & 0x02:  # end of stream
                    if "error" in frame:
                        raise StartError(json.dumps(frame["error"]))
                    return
                yield frame["event"]

    def run(self, script: str) -> tuple[int, str]:
        """Runs `sh -c script` to its end; returns the exit code and stdout."""
        out, code = b"", -1
        for event in self._start(script):
            if "data" in event:
                out += base64.b64decode(event["data"].get("stdout", ""))
            elif "end" in event:
                code = int(event["end"].get("exitCode", 0))
        return code, out.decode()

    def start_background(self, script: str) -> int:
        """Starts `sh -c script` and returns its pid without waiting for the end."""
        for event in self._start(script):
            if "start" in event:
                return int(event["start"]["pid"])
        raise StartError("envd sent no start event")

    def pids(self) -> set[int]:
        with self._open("POST", "/process.Process/List", b"{}", "application/json") as resp:
            return {int(p["pid"]) for p in json.loads(resp.read()).get("processes", [])}


@pytest_asyncio.fixture
async def client() -> AsyncIterator[Client]:
    async with Client(API_URL, auth_token=AUTH_TOKEN) as c:
        yield c


async def _create(client: Client, **kwargs: Any) -> Env:
    """Creates an environment and waits for its guest.

    The guest answers only after the sidecar's readiness URL has, thus envd
    needs no wait of its own after this returns.
    """
    env = await client.create(
        f"e2b-envd-{uuid.uuid4().hex[:8]}", template_name=TEMPLATE, atespace=ATESPACE, **kwargs
    )
    deadline = time.monotonic() + READY_TIMEOUT
    try:
        while True:
            try:
                await env.shell("true")
                return env
            except EnvError:
                if time.monotonic() > deadline:
                    raise
            await asyncio.sleep(1)
    except BaseException:
        await env.delete()
        raise


async def _wait_status(env: Env, want: EnvironmentStatus) -> None:
    deadline = time.monotonic() + READY_TIMEOUT
    while (await env.info()).status != want:
        assert time.monotonic() < deadline, f"{env.id} did not reach {want}"
        await asyncio.sleep(0.5)


async def test_envd_serves_in_the_base_image_once_the_guest_is_ready(client: Client) -> None:
    env = await _create(client)
    try:
        envd = Envd(env.id)
        assert await asyncio.to_thread(envd.health) == 204
        code, out = await asyncio.to_thread(envd.run, "echo from-envd; ls /opt/e2b")
        assert code == 0
        assert out.split() == ["from-envd", "LICENSE", "envd"]
    finally:
        await env.delete()


@pytest.mark.parametrize(
    ("image", "runs_as_root"),
    [
        pytest.param(
            MUSL_IMAGE, True, id="musl", marks=pytest.mark.skipif(not MUSL_IMAGE, reason="E2B_E2E_MUSL_IMAGE not set")
        ),
        pytest.param(
            NONROOT_IMAGE,
            False,
            id="non-root-user",
            marks=pytest.mark.skipif(not NONROOT_IMAGE, reason="E2B_E2E_NONROOT_IMAGE not set"),
        ),
    ],
)
async def test_a_derived_template_keeps_the_layer(
    client: Client, capsys: pytest.CaptureFixture[str], image: str, runs_as_root: bool
) -> None:
    """envd from the layer serves in another image; it starts processes only as root.

    The non-root case records the limit (E2B-34): Substrate runs the container
    as the image's USER with no effective capability, thus the template's
    capabilities do not reach envd. The integration supports only an image
    whose USER is root or not set.
    """
    env = await client.create(
        f"e2b-envd-{uuid.uuid4().hex[:8]}", template_name=TEMPLATE, atespace=ATESPACE, rootfs_image=image
    )
    try:
        envd = Envd(env.id)
        # Not _create: this test does not depend on what the guest's shell does as a non-root user.
        deadline = time.monotonic() + READY_TIMEOUT
        while (health := await asyncio.to_thread(envd.health)) != 204:
            assert time.monotonic() < deadline, f"envd /health answers {health}"
            await asyncio.sleep(1)

        init = await asyncio.to_thread(envd.init)
        try:
            started: Any = await asyncio.to_thread(envd.run, "id -u; ls /lib/ld-musl-* >/dev/null && echo musl")
        except StartError as e:
            started = e
        with capsys.disabled():
            print(f"\n{image}: /health -> {health}, /init -> {init}, process start -> {started!r}")

        assert init == 204
        if runs_as_root:
            assert started == (0, "0\nmusl\n")
        else:
            # Seen on Kind on 2026-11-13: code invalid_argument,
            # "fork/exec /bin/sh: operation not permitted".
            assert isinstance(started, StartError), f"envd started a process: {started!r}"
            assert "operation not permitted" in str(started)
    finally:
        await env.delete()


async def test_envd_and_its_process_survive_a_suspend(client: Client) -> None:
    env = await _create(client)
    try:
        envd = Envd(env.id)
        pid = await asyncio.to_thread(envd.start_background, "sleep 600")

        await env.suspend()
        await _wait_status(env, EnvironmentStatus.SUSPENDED)

        # The next request wakes the environment; envd is the restored process.
        assert pid in await asyncio.to_thread(envd.pids)
        code, out = await asyncio.to_thread(envd.run, "echo started")
        assert (code, out.strip()) == (0, "started")
        assert (await env.info()).status == EnvironmentStatus.RUNNING
    finally:
        await env.delete()


async def test_a_file_written_for_another_user_belongs_to_that_user(client: Client) -> None:
    env = await _create(client)
    try:
        envd = Envd(env.id)
        # envd gives the file to the user of the call (CHOWN).
        assert await asyncio.to_thread(envd.upload, "/tmp/theirs", b"secret", "nobody") == 200
        # Root re-modes a file it does not own (FOWNER) and reads it when
        # the mode gives it nothing (DAC_OVERRIDE).
        script = "chmod 600 /tmp/theirs && cat /tmp/theirs && stat -c ' %U %a' /tmp/theirs"
        code, out = await asyncio.to_thread(envd.run, script)
        assert (code, out.split()) == (0, ["secret", "nobody", "600"])
    finally:
        await env.delete()


@pytest.mark.skipif(not AUTH_TOKEN, reason="needs SUBSTRATE_ENV_AUTH_TOKEN")
async def test_envd_is_not_reachable_without_the_api_token(client: Client) -> None:
    env = await _create(client)
    try:
        # envd itself asks a caller for nothing: the port proxy is the guard.
        assert await asyncio.to_thread(Envd(env.id, api_auth=False).health) == 401
    finally:
        await env.delete()
