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

from __future__ import annotations

from dataclasses import dataclass

import grpc.aio
import pytest

from ate_env_rl import Client
from ate_env_rl._gen.ateenv.v1alpha import env_pb2_grpc, guest_pb2_grpc

from .fakes import FakeEnvironmentService, FakeFileSystemService, FakeProcessService


@dataclass
class Fakes:
    environments: FakeEnvironmentService
    processes: FakeProcessService
    filesystem: FakeFileSystemService


@pytest.fixture
async def fake_stack():
    """An in-process grpc.aio server with the three fake services, plus a Client."""
    server = grpc.aio.server()
    fakes = Fakes(FakeEnvironmentService(), FakeProcessService(), FakeFileSystemService())
    env_pb2_grpc.add_EnvironmentServiceServicer_to_server(fakes.environments, server)
    guest_pb2_grpc.add_ProcessServiceServicer_to_server(fakes.processes, server)
    guest_pb2_grpc.add_FileSystemServiceServicer_to_server(fakes.filesystem, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    client = Client(f"127.0.0.1:{port}")
    try:
        yield client, fakes
    finally:
        await client.close()
        await server.stop(None)


# The OpenTelemetry SDK accepts one global tracer provider per process, so the
# in-memory exporter that span tests read lives here, shared by all modules.
_PROVIDER = None


@pytest.fixture
def spans():
    """An in-memory exporter on the process-wide SDK provider; returns the exporter."""
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    global _PROVIDER
    if _PROVIDER is None:
        provider = TracerProvider(resource=Resource(attributes={"service.name": "ate-env-client"}))
        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _PROVIDER = (provider, exporter)
    provider, exporter = _PROVIDER
    exporter.clear()
    yield exporter
    exporter.clear()
