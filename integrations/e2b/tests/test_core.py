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

"""How the core builds its ``ate_env_rl.Client``: plaintext or TLS, one per loop.

No connection is made: a gRPC channel dials on its first call.
"""

from __future__ import annotations

import asyncio
from typing import Any

import grpc
import pytest
from ate_env_rl import Client

from ate_env_e2b import _core
from ate_env_e2b._settings import Settings


class _Recorder(Client):
    """The real client, with the arguments it was built from."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args, self.kwargs = args, kwargs
        super().__init__(*args, **kwargs)


@pytest.fixture
def made(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_core, "Client", _Recorder)
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", "caller-token")


def _client(endpoint: str, monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    monkeypatch.setenv("SUBSTRATE_ENV_API", endpoint)

    async def get() -> tuple[Client, Client]:
        return _core._client(Settings.from_env()), _core._client(Settings.from_env())

    first, again = asyncio.run(get())
    assert again is first  # one client for an endpoint and a token on one loop
    return first


def test_an_https_endpoint_gives_the_client_tls_credentials_and_no_channel(made, monkeypatch):
    client = _client("https://ate-env.example.com", monkeypatch)
    assert client.args == ("ate-env.example.com:443",)
    assert isinstance(client.kwargs["credentials"], grpc.ChannelCredentials)
    assert client.kwargs["auth_token"] == "caller-token"
    # The client opens the secure channel itself, thus the channel has the
    # client's auth and trace interceptors.
    assert "channel" not in client.kwargs


def test_a_plaintext_endpoint_gives_the_client_no_credentials(made, monkeypatch):
    client = _client("http://127.0.0.1:7777", monkeypatch)
    assert client.args == ("127.0.0.1:7777",)
    assert client.kwargs.get("credentials") is None and "channel" not in client.kwargs


def test_each_loop_and_each_endpoint_has_its_own_client(made, monkeypatch):
    first = _client("https://ate-env.example.com", monkeypatch)
    assert _client("https://ate-env.example.com", monkeypatch) is not first  # another loop
    assert _client("ate-env.example.com:443", monkeypatch).kwargs.get("credentials") is None
