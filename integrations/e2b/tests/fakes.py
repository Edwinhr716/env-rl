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

"""A fake ate-env-api for the class's tests: no cluster, no envd.

Two listeners share one state. The gRPC one is EnvironmentService, and the
guest's ProcessService for one command. The HTTP one stands in for the port
proxy with envd behind it, for the three routes the class itself calls:
``/health``, ``/init`` and one process start. It answers as the port proxy
does for an environment that is gone, crashed or waking, and wakes a
suspended environment on a request.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import struct
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
import grpc.aio
from ate_env_rl import DEFAULT_ATESPACE, EnvironmentStatus
from ate_env_rl._gen.ateenv.v1alpha import (
    env_pb2,
    env_pb2_grpc,
    guest_pb2,
    guest_pb2_grpc,
)


@dataclass
class FakeAteEnv:
    """The environments, what the listeners saw, and the faults to play."""

    # The caller token both listeners require; None for an open server.
    require_token: str | None = None
    envs: dict[str, dict] = field(default_factory=dict)
    # CreateEnvironment requests: (id, atespace, template, rootfs_image).
    creates: list[tuple[str, str, str, str]] = field(default_factory=list)
    suspends: list[tuple[str, int]] = field(default_factory=list)
    # The process starts that envd got.
    starts: list[dict] = field(default_factory=list)
    # The commands that the guest ran.
    guest_commands: list[list[str]] = field(default_factory=list)
    # DeleteEnvironment requests, by id.
    deletes: list[str] = field(default_factory=list)
    # HTTP requests the proxy forwarded or answered: (method, path, env id).
    requests: list[tuple[str, str, str]] = field(default_factory=list)
    # Every header value of every HTTP request and every metadata value of
    # every gRPC call, to prove what never leaves.
    header_values: list[str] = field(default_factory=list)
    # The lower-case name of every header of every HTTP request.
    header_names: set[str] = field(default_factory=set)
    # The JSON bodies of POST /init, by environment.
    inits: dict[str, list[dict]] = field(default_factory=dict)
    # The user of the image: what a process prints for `id -u` and `id -un`.
    image_user: tuple[int, str] = (0, "root")
    # When set, envd fails each process start with it; as a default, envd
    # fails the start in an image whose user is not root.
    start_error: str | None = None
    # When set, /init answers this status.
    init_status: int | None = None
    # Seconds that an RPC waits before it answers, by RPC name; and seconds
    # that envd waits before it answers a path ("/init", the process start).
    delays: dict[str, float] = field(default_factory=dict)
    # How many requests of a waking environment get 503 before it answers.
    wake_503s: int = 0
    # When set, /health answers this status for a running environment.
    health_status: int | None = None
    # When set, CreateEnvironment makes the environment and then fails with it.
    create_code_after: grpc.StatusCode | None = None
    # When set, DeleteEnvironment refuses with it.
    delete_code: grpc.StatusCode | None = None

    def set_status(self, env_id: str, status: EnvironmentStatus) -> None:
        self.envs[env_id]["status"] = int(status)

    def status(self, env_id: str) -> EnvironmentStatus:
        return EnvironmentStatus(self.envs[env_id]["status"])


class _Environments(env_pb2_grpc.EnvironmentServiceServicer):
    def __init__(self, state: FakeAteEnv) -> None:
        self.state = state

    def _pb(self, env_id: str) -> env_pb2.Environment:
        e = self.state.envs[env_id]
        return env_pb2.Environment(
            id=env_id,
            atespace=e["atespace"],
            template=env_pb2.Template(name=e["template"], atespace=e["atespace"]),
            status=e["status"],
        )

    async def CreateEnvironment(self, request, context):
        s = self.state
        await asyncio.sleep(s.delays.get("CreateEnvironment", 0))
        s.creates.append(
            (request.id, request.atespace, request.template.name, request.rootfs_image)
        )
        if request.id in s.envs:
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, "exists")
        s.envs[request.id] = {
            "atespace": request.atespace or DEFAULT_ATESPACE,
            "template": request.template.name,
            "status": int(EnvironmentStatus.RUNNING),
        }
        if s.create_code_after is not None:
            await context.abort(s.create_code_after, "the answer was lost")
        return env_pb2.CreateEnvironmentResponse(environment=self._pb(request.id))

    async def GetEnvironment(self, request, context):
        await asyncio.sleep(self.state.delays.get("GetEnvironment", 0))
        if request.id not in self.state.envs:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"environment {request.id} not found")
        return env_pb2.GetEnvironmentResponse(environment=self._pb(request.id))

    async def SuspendEnvironment(self, request, context):
        s = self.state
        if request.id not in s.envs:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"environment {request.id} not found")
        s.suspends.append((request.id, int(request.mode)))
        # Like ate-env-api: no change for an environment that is not running.
        if s.status(request.id) is EnvironmentStatus.RUNNING:
            s.set_status(request.id, EnvironmentStatus.SUSPENDED)
        return env_pb2.SuspendEnvironmentResponse()

    async def DeleteEnvironment(self, request, context):
        await asyncio.sleep(self.state.delays.get("DeleteEnvironment", 0))
        self.state.deletes.append(request.id)
        if self.state.delete_code is not None:
            await context.abort(self.state.delete_code, "delete refused")
        if self.state.envs.pop(request.id, None) is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"environment {request.id} not found")
        return env_pb2.DeleteEnvironmentResponse()


class _Guest(guest_pb2_grpc.ProcessServiceServicer):
    """The guest, for one command: it prints the user of the image."""

    def __init__(self, state: FakeAteEnv) -> None:
        self.state = state

    async def StartProcess(self, request, context):
        self.state.guest_commands.append(list(request.command))
        return guest_pb2.Process(process_id="proc-1", state=guest_pb2.PROCESS_STATE_RUNNING)

    async def StreamProcessOutput(self, request, context):
        uid, name = self.state.image_user
        yield guest_pb2.ProcessOutput(stdout=f"{uid} {name}\n".encode())
        yield guest_pb2.ProcessOutput(
            exit=guest_pb2.Process(process_id="proc-1", state=guest_pb2.PROCESS_STATE_EXITED)
        )


class _BearerCheck(grpc.aio.ServerInterceptor):
    def __init__(self, state: FakeAteEnv) -> None:
        self.state = state

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        want = self.state.require_token
        metadata = dict(handler_call_details.invocation_metadata or ())
        self.state.header_values.extend(str(value) for value in metadata.values())
        if want is None or handler is None or metadata.get("authorization") == f"Bearer {want}":
            return handler

        async def deny(_request, context):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or invalid bearer token")

        return grpc.unary_unary_rpc_method_handler(
            deny,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


def _proxy_handler(state: FakeAteEnv) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:  # quiet
            pass

        def _answer(self, code: int, text: str = "", headers: dict[str, str] | None = None) -> None:
            body = text.encode()
            self.send_response(code)
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            env_id = self.headers.get("X-Env-Id", "")
            state.requests.append((self.command, self.path, env_id))
            state.header_values.extend(self.headers.values())
            state.header_names.update(name.lower() for name in self.headers)

            # The port proxy: the caller token, then the environment's state.
            if state.require_token is not None and (
                self.headers.get("X-Env-Authorization") != f"Bearer {state.require_token}"
            ):
                return self._answer(401, "the port proxy needs the caller token")
            if self.headers.get("X-Env-Port") != "49983":
                return self._answer(400, "no X-Env-Port")
            env = state.envs.get(env_id)
            if env is None:
                return self._answer(404, f"environment {env_id} was not found")
            status = EnvironmentStatus(env["status"])
            if status is EnvironmentStatus.CRASHED:
                return self._answer(410, f"environment {env_id} has crashed", {"X-Env-State": "CRASHED"})
            if status is EnvironmentStatus.DELETING:
                # As the port proxy: a request never wakes it.
                return self._answer(
                    404,
                    f"environment {env_id} is being deleted (actor state DELETING)",
                    {"X-Env-State": "DELETING"},
                )
            if status is not EnvironmentStatus.RUNNING:
                if state.wake_503s > 0:
                    state.wake_503s -= 1
                    return self._answer(
                        503, f"environment {env_id} is still waking; retry", {"Retry-After": "1"}
                    )
                env["status"] = int(EnvironmentStatus.RUNNING)

            # envd.
            time.sleep(state.delays.get(self.path, 0))
            if self.path == "/health":
                if state.health_status == 404:  # as the proxy answers for no actor
                    return self._answer(404, f"environment {env_id} was not found")
                return self._answer(state.health_status or 204)
            if self.command == "POST" and self.path == "/init":
                if state.init_status:
                    return self._answer(state.init_status, "init refused")
                state.inits.setdefault(env_id, []).append(json.loads(body or b"{}"))
                return self._answer(204)
            if self.path == "/process.Process/Start" and body[:1] == b"\x00":
                return self._start(json.loads(body[5:]))
            return self._answer(404, "the fake envd has only /health, /init and one process start")

        def _start(self, request: dict) -> None:
            """envd's server stream: the output of the process, then the end."""
            uid, name = state.image_user
            error = state.start_error
            if error is None and uid != 0:
                # envd without SETUID: it cannot give the process its user.
                error = "error starting process: fork/exec /bin/sh: operation not permitted"
            if error is not None:
                frames = [(2, {"error": {"code": "internal", "message": error}})]
            else:
                out = base64.b64encode(f"{uid} {name}\n".encode()).decode()
                frames = [
                    (0, {"event": {"start": {"pid": 7}}}),
                    (0, {"event": {"data": {"stdout": out}}}),
                    (0, {"event": {"end": {"exited": True, "status": "exit status 0"}}}),
                    (2, {}),
                ]
            state.starts.append(request)
            stream = b""
            for flags, frame in frames:
                data = json.dumps(frame).encode()
                stream += struct.pack(">BI", flags, len(data)) + data
            self.send_response(200)
            self.send_header("Content-Type", "application/connect+json")
            self.send_header("Content-Length", str(len(stream)))
            self.end_headers()
            self.wfile.write(stream)

        do_GET = do_POST = _serve

    return Handler


@contextlib.contextmanager
def serve(state: FakeAteEnv | None = None) -> Iterator[tuple[str, str, FakeAteEnv]]:
    """Runs both listeners; yields the gRPC target, the proxy's URL and the state."""
    state = state or FakeAteEnv()
    proxy = ThreadingHTTPServer(("127.0.0.1", 0), _proxy_handler(state))
    threading.Thread(target=proxy.serve_forever, daemon=True).start()

    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()

    async def start() -> tuple[grpc.aio.Server, int]:
        server = grpc.aio.server(interceptors=[_BearerCheck(state)])
        env_pb2_grpc.add_EnvironmentServiceServicer_to_server(_Environments(state), server)
        guest_pb2_grpc.add_ProcessServiceServicer_to_server(_Guest(state), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        return server, port

    server, port = asyncio.run_coroutine_threadsafe(start(), loop).result(10)
    try:
        yield f"127.0.0.1:{port}", f"http://127.0.0.1:{proxy.server_address[1]}", state
    finally:
        asyncio.run_coroutine_threadsafe(server.stop(None), loop).result(10)
        loop.call_soon_threadsafe(loop.stop)
        proxy.shutdown()
        proxy.server_close()
