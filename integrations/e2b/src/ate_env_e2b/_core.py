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

"""The control plane of both classes, written once as coroutines.

Each coroutine takes a ``Settings`` and plain values, calls ate-env-api's
EnvironmentService through ``ate_env_rl.Client`` and, for envd's ``/health``,
``/init`` and the root check, the port proxy through its own ``httpx`` client.
It returns plain data and raises E2B's public exceptions. Nothing here knows
the SDK's ``Sandbox`` classes: ``AsyncSandbox`` awaits these coroutines, and
``Sandbox`` runs them on a background loop (``_runner``).

Each call has a deadline (``limit``). A gRPC channel belongs to the loop
that made it, thus the clients are kept per running loop.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import struct
import threading
import uuid
import weakref
from collections.abc import AsyncIterator, Mapping

import grpc
import httpx
from ate_env_rl import (
    Client,
    EnvError,
    EnvironmentInfo,
    EnvironmentStatus,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
    RpcError,
    SuspendMode,
)
from e2b import (
    AuthenticationException,
    InvalidArgumentException,
    SandboxException,
    SandboxNotFoundException,
    TimeoutException,
)

from ._settings import Settings

DOC = "integrations/e2b/README.md"

# The deadline of a call without a request_timeout. create, connect and pause
# can pull an image, restore a snapshot or upload one; the others are one RPC,
# and get the SDK's own default.
LONG_TIMEOUT_S = 300.0
SHORT_TIMEOUT_S = 60.0
# How long the cleanup of a failed create has, after the deadline of the create.
CLEANUP_TIMEOUT_S = 30.0
# How long one request of the wait for envd's /health has, before the wait
# tries again. /init and the root check have no limit of their own: the
# deadline of the call ends them, as a TimeoutException.
HEALTH_TIMEOUT_S = 30.0
# How long the lookup of the image's user has, when the root check fails.
USER_LOOKUP_TIMEOUT_S = 10.0

PAUSED = frozenset(
    {
        EnvironmentStatus.SUSPENDING,
        EnvironmentStatus.SUSPENDED,
        EnvironmentStatus.PAUSING,
        EnvironmentStatus.PAUSED,
    }
)

# A create that the server refused with one of these made no environment.
_REFUSED_CODES = frozenset({grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.ALREADY_EXISTS})

# Prints the user of the process: "<uid> <name>".
_WHOAMI = 'echo "$(id -u) $(id -un 2>/dev/null)"'

_lock = threading.Lock()
_clients: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[tuple, Client]] = (
    weakref.WeakKeyDictionary()
)
# Whether this process made a client or a background loop, and whether it is
# a child of os.fork() of a process that had.
_used = False
_forked = False


def _after_fork_in_child() -> None:
    global _lock, _forked
    _lock, _forked = threading.Lock(), _used


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


def use() -> None:
    """Marks the process as a user of gRPC's asyncio API; raises in a forked child.

    gRPC's asyncio channels and the background loop do not survive os.fork():
    a call in the child would wait with no end. It fails at once instead.
    """
    global _used
    if _forked:
        raise SandboxException(
            "this process is a child of os.fork(), and its parent had used ate_env_e2b before "
            "the fork: the gRPC channels do not work here. Make the first call after the fork, "
            f"or start the workers with the spawn or forkserver method; see {DOC}"
        )
    _used = True


def limit(request_timeout: float | None, default: float) -> float:
    """The deadline of one call, in seconds.

    ``None`` is the default of the call. E2B's ``0`` means no timeout; here
    each call has a deadline, thus ``0`` is the longest one, 300 s.
    """
    if request_timeout is None:
        return default
    if request_timeout < 0:
        raise InvalidArgumentException(f"request_timeout={request_timeout!r}: want 0 or more")
    return float(request_timeout) or LONG_TIMEOUT_S


def _client(settings: Settings) -> Client:
    """The client of the running loop for one endpoint and token."""
    use()
    loop = asyncio.get_running_loop()
    key = (settings.target, settings.tls, settings.token)
    with _lock:
        clients = _clients.setdefault(loop, {})
        if key not in clients:
            # With credentials the client opens the secure channel itself, and
            # the channel keeps the client's auth and trace interceptors.
            # Without them the client dials plaintext, also for https://.
            credentials = grpc.ssl_channel_credentials() if settings.tls else None
            clients[key] = Client(
                settings.target, auth_token=settings.token, credentials=credentials
            )
        return clients[key]


@contextlib.contextmanager
def _mapped():
    """Maps the client's errors to E2B's."""
    try:
        yield
    except NotFoundError as e:
        raise SandboxNotFoundException(str(e)) from e
    except InvalidArgumentError as e:
        raise InvalidArgumentException(str(e)) from e
    except PermissionDeniedError as e:
        raise AuthenticationException(str(e)) from e
    except RpcError as e:
        if e.code is grpc.StatusCode.UNAUTHENTICATED:
            raise AuthenticationException(str(e)) from e
        raise SandboxException(str(e)) from e
    except EnvError as e:
        raise SandboxException(str(e)) from e


@contextlib.asynccontextmanager
async def _deadline(seconds: float, what: str) -> AsyncIterator[list[str]]:
    """The deadline of one call. Yields a list: its last entry, when there is
    one, says in the error how far the call came."""
    progress: list[str] = []
    try:
        async with asyncio.timeout(seconds):
            with _mapped():
                yield progress
    except TimeoutError as e:
        raise TimeoutException(
            f"{what} did not end within {seconds:g}s"
            + (f": {progress[-1]}" if progress else "")
            + f" (request_timeout; see {DOC})"
        ) from e


def _http(settings: Settings, env_id: str) -> httpx.AsyncClient:
    """envd of one environment, through the port proxy.

    TLS trust is httpx's default (the certifi bundle, or SSL_CERT_FILE and
    SSL_CERT_DIR): the SDK has no public name for the trust of its own
    transport.
    """
    return httpx.AsyncClient(
        base_url=settings.sandbox_url,
        headers=settings.proxy_headers(env_id),
        timeout=HEALTH_TIMEOUT_S,
    )


async def _get(settings: Settings, env_id: str) -> EnvironmentInfo:
    return await _client(settings).get(env_id, atespace=settings.atespace)


async def _serving(settings: Settings, env_id: str) -> EnvironmentInfo:
    """An environment that can still serve; raises for the rest."""
    info = await _get(settings, env_id)
    if info.status is EnvironmentStatus.DELETING:
        raise SandboxNotFoundException(f"environment {env_id} is being deleted")
    if info.status is EnvironmentStatus.CRASHED:
        raise SandboxException(f"environment {env_id} has crashed; kill it and create a new one")
    return info


async def _wait_ready(
    http: httpx.AsyncClient, settings: Settings, env_id: str, progress: list[str]
) -> None:
    """Waits for envd's /health through the port proxy, until the deadline of the caller.

    The request also wakes a suspended environment. The proxy answers 503
    while the environment wakes (a proxy that knows the wake-wait header holds
    the request instead), and passes the router's 5xx through when the router
    has no answer yet; those and connection errors are tried again. A 404 is
    tried again only while the control plane still has the environment. The
    caller token refused (401, 403) and a crashed environment (410) end the
    wait at once.
    """
    while True:
        try:
            response = await http.get("/health")
            if response.is_success:
                return
            last = f"{response.status_code} {response.text.strip()}"
            if response.status_code in (401, 403):
                raise AuthenticationException(f"envd of {env_id}: {last}")
            if response.status_code == 410:
                raise SandboxException(f"envd of {env_id}: {last}")
            if response.status_code == 404:
                await _serving(settings, env_id)  # raises when the environment is gone
        except httpx.TransportError as e:
            last = f"{type(e).__name__}: {e}"
        progress.append(f"envd's /health answered last: {last}")
        await asyncio.sleep(0.5)


async def _init(http: httpx.AsyncClient, env_id: str, envs: Mapping[str, str]) -> None:
    """Gives envd the environment variables of the sandbox."""
    try:
        response = await http.post("/init", json={"envVars": dict(envs)}, timeout=None)
    except httpx.TransportError as e:
        raise SandboxException(
            f"envd of {env_id} did not answer /init: {type(e).__name__}: {e}"
        ) from e
    if not response.is_success:
        raise SandboxException(
            f"envd of {env_id} refused /init: {response.status_code} {response.text.strip()}"
        )


async def _envd_whoami(http: httpx.AsyncClient) -> tuple[str | None, str]:
    """Starts one process through envd; returns its output, or None and the error."""
    payload = json.dumps({"process": {"cmd": "/bin/sh", "args": ["-c", _WHOAMI]}}).encode()
    try:
        response = await http.post(
            "/process.Process/Start",
            content=struct.pack(">BI", 0, len(payload)) + payload,
            headers={"Content-Type": "application/connect+json", "Connect-Protocol-Version": "1"},
            timeout=None,
        )
    except httpx.TransportError as e:
        return None, f"{type(e).__name__}: {e}"
    body = response.content
    if not response.is_success:
        return None, f"{response.status_code} {response.text.strip()}"
    out, pos = b"", 0
    try:
        while pos + 5 <= len(body):
            flags, size = struct.unpack(">BI", body[pos : pos + 5])
            frame = json.loads(body[pos + 5 : pos + 5 + size] or b"{}")
            pos += 5 + size
            if flags & 0x02:  # the end of the stream
                if "error" in frame:
                    return None, str(frame["error"].get("message") or frame["error"])
                break
            data = frame.get("event", {}).get("data", {})
            out += base64.b64decode(data.get("stdout", ""))
    except (ValueError, AttributeError, struct.error) as e:
        return None, f"an answer that is not envd's: {type(e).__name__}: {e}"
    return out.decode(errors="replace").strip(), ""


def _user(whoami: str) -> tuple[str, str] | None:
    """("1000", "1000 (node)") from "1000 node"; None when it is no such line."""
    uid, _, name = whoami.partition(" ")
    if not uid.isdigit():
        return None
    return uid, f"{uid} ({name.strip()})" if name.strip() else uid


async def _require_root(
    http: httpx.AsyncClient, settings: Settings, env_id: str, image: str
) -> None:
    """Refuses an image that does not run as root: one process start through envd.

    envd sets the user of each process it starts. In an image whose user is
    not root it has no capability to do that, and in a template without
    SETUID and SETGID root has none either: the start fails, and so would each
    command and PTY of the program. Only then a second call asks the guest
    for the user of the image, to name it.
    """
    whoami, error = await _envd_whoami(http)
    user = _user(whoami) if whoami is not None else None
    if user is not None and user[0] == "0":
        return
    if user is None:
        error = error or f"the process printed {whoami!r}"
        with contextlib.suppress(Exception):
            async with asyncio.timeout(USER_LOOKUP_TIMEOUT_S):
                env = _client(settings).env(env_id, atespace=settings.atespace)
                user = _user((await env.shell(_WHOAMI)).stdout.strip())
    if user is not None and user[0] != "0":
        raise SandboxException(
            f"{image} runs as user {user[1]}, not as root: an E2B sandbox on ate-env needs an "
            f"image that runs as root, because envd cannot start a command in an image whose "
            f"USER is not root at this Substrate version"
            + (f" (envd: {error})" if error else "")
            + f"; see {DOC} (environment {env_id})"
        )
    raise SandboxException(
        f"envd of {env_id} ({image}) cannot start a process: {error}. The image must run as "
        f"root, on a template with the capabilities of the base template; see {DOC}"
    )


def _made_nothing(rpc: asyncio.Future) -> bool:
    """Whether the create ended with an answer that says no environment was made."""
    if not rpc.done() or rpc.cancelled() or rpc.exception() is None:
        return False
    e = rpc.exception()
    if isinstance(e, InvalidArgumentError | PermissionDeniedError | NotFoundError):
        return True
    return isinstance(e, RpcError) and e.code in _REFUSED_CODES


async def discard(
    settings: Settings, env_id: str, error: BaseException, rpc: asyncio.Future | None = None
) -> None:
    """Deletes the environment of a failed create; says so on the error when it cannot.

    A create that is still in flight (rpc) is awaited first: a delete that
    arrives before it would find nothing, and the environment would remain.
    """
    try:
        async with asyncio.timeout(CLEANUP_TIMEOUT_S):
            if rpc is not None and not rpc.done():
                await asyncio.wait([rpc])
            if rpc is not None and _made_nothing(rpc):
                return
            with contextlib.suppress(NotFoundError):
                await _client(settings).delete(env_id, atespace=settings.atespace)
    except BaseException as e:
        if rpc is not None:
            rpc.cancel()
        note = (
            f"environment {env_id} in atespace {settings.atespace} may remain: "
            f"its cleanup failed: {type(e).__name__}: {e}"
        )
        # In the message for the errors of this package, and as a note for
        # the rest (an interrupt, a cancellation).
        if isinstance(error, Exception) and error.args and isinstance(error.args[0], str):
            error.args = (f"{error.args[0]}. The {note}", *error.args[1:])
        else:
            error.add_note(f"The {note}")


async def create(
    settings: Settings,
    template: str | None,
    envs: Mapping[str, str] | None,
    request_timeout: float | None,
) -> str:
    """Creates an environment that envd serves as root; returns its id.

    A failure after the create request left, also a cancellation or the
    deadline, deletes the environment.
    """
    template_name, rootfs_image = settings.resolve_template(template)
    seconds = limit(request_timeout, LONG_TIMEOUT_S)
    env_id = f"e2b-{uuid.uuid4().hex[:12]}"
    rpc = asyncio.ensure_future(
        _client(settings).create(
            env_id,
            atespace=settings.atespace,
            template_name=template_name,
            rootfs_image=rootfs_image,
        )
    )
    try:
        async with _deadline(seconds, f"the create of environment {env_id}") as progress:
            # Shielded: the deadline or a cancellation must not hide a create
            # that the server still completes.
            await asyncio.shield(rpc)
            async with _http(settings, env_id) as http:
                await _wait_ready(http, settings, env_id, progress)
                if envs:
                    await _init(http, env_id, envs)
                image = f"image {rootfs_image}" if rootfs_image else f"template {template_name}"
                await _require_root(http, settings, env_id, image)
    except BaseException as e:
        await discard(settings, env_id, e, rpc)
        raise
    return env_id


async def resume(settings: Settings, env_id: str, request_timeout: float | None) -> None:
    """Wakes a suspended environment and waits for envd.

    A missing or crashed environment is an error at once, not a long wait.
    """
    seconds = limit(request_timeout, LONG_TIMEOUT_S)
    async with _deadline(seconds, f"the connect to environment {env_id}") as progress:
        await _serving(settings, env_id)
        async with _http(settings, env_id) as http:
            await _wait_ready(http, settings, env_id, progress)


async def info(settings: Settings, env_id: str, request_timeout: float | None) -> EnvironmentInfo:
    """The environment; raises for one that is gone or crashed."""
    async with _deadline(limit(request_timeout, SHORT_TIMEOUT_S), f"the read of {env_id}"):
        return await _serving(settings, env_id)


async def is_running(settings: Settings, env_id: str, request_timeout: float | None) -> bool:
    """Whether the environment runs and envd answers.

    Reads the status first, thus it never wakes a suspended environment.
    """
    async with _deadline(limit(request_timeout, SHORT_TIMEOUT_S), f"the check of {env_id}"):
        try:
            if (await _get(settings, env_id)).status is not EnvironmentStatus.RUNNING:
                return False
        except NotFoundError:
            return False
        try:
            async with _http(settings, env_id) as http:
                response = await http.get("/health")
        except httpx.TransportError:
            return False
        if response.status_code in (401, 403):
            # A refused caller token is a wrong setup, not a stopped sandbox.
            raise AuthenticationException(
                f"envd of {env_id}: {response.status_code} {response.text.strip()}"
            )
        return response.is_success


async def pause(settings: Settings, env_id: str, request_timeout: float | None) -> bool:
    """Suspends the environment durably; False when it is already suspended."""
    async with _deadline(limit(request_timeout, LONG_TIMEOUT_S), f"the pause of {env_id}"):
        if (await _serving(settings, env_id)).status in PAUSED:
            return False
        await _client(settings).suspend(
            env_id, atespace=settings.atespace, mode=SuspendMode.DURABLE
        )
        return True


async def kill(settings: Settings, env_id: str, request_timeout: float | None) -> bool:
    """Deletes the environment; False when it was already gone."""
    async with _deadline(limit(request_timeout, SHORT_TIMEOUT_S), f"the kill of {env_id}"):
        try:
            await _client(settings).delete(env_id, atespace=settings.atespace)
        except NotFoundError:
            return False
        return True
