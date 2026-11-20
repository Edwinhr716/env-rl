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

from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from .fakes import FakeAteEnv, serve

TOKEN = "caller-token"


@dataclass
class Ate:
    state: FakeAteEnv
    # E2B's per-call sandbox_url: the fake's HTTP listener is not on the gRPC port.
    url: str


@pytest.fixture
def ate(monkeypatch: pytest.MonkeyPatch) -> Iterator[Ate]:
    """A fake ate-env-api with a caller token, named by the environment."""
    for name in (
        "SUBSTRATE_ENV_ATESPACE",
        "SUBSTRATE_ENV_E2B_TEMPLATES",
        "SUBSTRATE_ENV_E2B_WAKE_WAIT",
    ):
        monkeypatch.delenv(name, raising=False)
    with serve(FakeAteEnv(require_token=TOKEN)) as (target, url, state):
        monkeypatch.setenv("SUBSTRATE_ENV_API", target)
        monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", TOKEN)
        yield Ate(state, url)
