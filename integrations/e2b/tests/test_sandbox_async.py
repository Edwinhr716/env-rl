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

"""The async class against the fake ate-env-api.

``AsyncSandbox`` awaits the same coroutines that ``Sandbox`` runs on its
background loop (test_sandbox.py covers them); these tests cover what differs:
the methods are awaited in both forms, on the loop of the caller, and a
cancellation cleans up.
"""

from __future__ import annotations

import asyncio
import warnings

import pytest
from ate_env_rl import EnvironmentStatus
from e2b import (
    InvalidArgumentException,
    NotFoundException,
    SandboxException,
    SandboxNotFoundException,
    SandboxState,
    TimeoutException,
)

from ate_env_e2b import AsyncSandbox, TimeoutNotEnforcedWarning, _core

from .conftest import Ate

REFUSED = r"is not supported on ate-env; see integrations/e2b/README\.md"


@pytest.mark.asyncio
async def test_the_lifecycle_in_both_forms(ate: Ate):
    sandbox = await AsyncSandbox.create(sandbox_url=ate.url, envs={"A": "1"})
    env_id = sandbox.sandbox_id
    assert ate.state.creates == [(env_id, "ate-env", "e2b-base", "")]
    assert ate.state.inits[env_id] == [{"envVars": {"A": "1"}}]
    assert await sandbox.is_running() is True
    assert (await sandbox.get_info()).state is SandboxState.RUNNING

    assert await sandbox.pause() is True
    assert await sandbox.beta_pause() is False
    assert await AsyncSandbox.pause(env_id, sandbox_url=ate.url) is False
    assert await sandbox.is_running() is False
    assert (await AsyncSandbox.get_info(env_id, sandbox_url=ate.url)).state is SandboxState.PAUSED

    other = await AsyncSandbox.connect(env_id, sandbox_url=ate.url)
    assert other is not sandbox and isinstance(other, AsyncSandbox)
    assert ate.state.status(env_id) is EnvironmentStatus.RUNNING
    assert await sandbox.connect() is sandbox

    assert await AsyncSandbox.kill(env_id, sandbox_url=ate.url) is True
    assert await sandbox.kill() is False
    with pytest.raises(SandboxNotFoundException):
        await sandbox.get_info()
    # The SDK's own async data plane, through the same proxy headers.
    with pytest.raises(NotFoundException, match="was not found"):
        await sandbox.commands.run("true")


@pytest.mark.asyncio
async def test_the_async_with_block_deletes_the_environment(ate: Ate):
    async with await AsyncSandbox.create(sandbox_url=ate.url) as sandbox:
        assert sandbox.sandbox_id in ate.state.envs
    assert ate.state.envs == {}


@pytest.mark.asyncio
async def test_refusals_and_the_timeout_warning(ate: Ate):
    with pytest.raises(SandboxException, match=rf"create\(metadata=...\) {REFUSED}"):
        await AsyncSandbox.create(sandbox_url=ate.url, metadata={})
    with pytest.raises(SandboxException, match=rf"Sandbox.set_timeout {REFUSED}"):
        await AsyncSandbox.set_timeout("e2b-0123456789ab", 60)
    assert ate.state.creates == []
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        sandbox = await AsyncSandbox.create(sandbox_url=ate.url, timeout=60)
        await sandbox.connect(timeout=60)
    assert [w.category for w in caught] == [TimeoutNotEnforcedWarning] * 2
    assert {w.filename for w in caught} == {__file__}
    with pytest.raises(SandboxException, match=rf"connect\(on_resume='reboot'\) {REFUSED}"):
        await sandbox.connect(on_resume="reboot")


@pytest.mark.asyncio
async def test_a_failed_create_and_a_cancelled_create_delete_the_environment(ate: Ate):
    ate.state.image_user = (1000, "node")
    with pytest.raises(SandboxException, match=r"runs as user 1000 \(node\), not as root"):
        await AsyncSandbox.create(sandbox_url=ate.url)
    assert len(ate.state.creates) == 1 and ate.state.envs == {}

    ate.state.image_user = (0, "root")
    ate.state.delays["CreateEnvironment"] = 1.0
    task = asyncio.ensure_future(AsyncSandbox.create(sandbox_url=ate.url))
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # The task ended only after the create had ended and the delete was sent.
    assert len(ate.state.creates) == 2 and ate.state.envs == {}
    assert ate.state.deletes[-1] == ate.state.creates[-1][0]


@pytest.mark.asyncio
async def test_an_option_the_sdk_refuses_raises_in_each_form(ate: Ate):
    sandbox = await AsyncSandbox.create(sandbox_url=ate.url)
    opts = {"http_version": "3"}
    for call in (
        lambda: AsyncSandbox.create(sandbox_url=ate.url, **opts),
        lambda: sandbox.get_info(**opts),
        lambda: sandbox.connect(**opts),
        lambda: sandbox.pause(**opts),
        lambda: sandbox.kill(**opts),
        lambda: AsyncSandbox.kill(sandbox.sandbox_id, sandbox_url=ate.url, **opts),
    ):
        with pytest.raises(InvalidArgumentException, match="http_version"):
            await call()
    assert len(ate.state.creates) == 1 and ate.state.deletes == []
    assert await sandbox.kill() is True


@pytest.mark.asyncio
async def test_a_slow_init_ends_at_the_request_timeout_as_a_timeout(ate: Ate, monkeypatch):
    monkeypatch.setattr(_core, "HEALTH_TIMEOUT_S", 0.2)
    ate.state.delays["/init"] = 3
    loop = asyncio.get_running_loop()
    start = loop.time()
    with pytest.raises(TimeoutException, match=r"the create of .* within 1s \(request_timeout"):
        await AsyncSandbox.create(sandbox_url=ate.url, envs={"A": "1"}, request_timeout=1)
    assert 0.9 < loop.time() - start < 2.5
    assert ate.state.envs == {}


@pytest.mark.asyncio
async def test_request_timeout_bounds_a_call(ate: Ate):
    sandbox = await AsyncSandbox.create(sandbox_url=ate.url)
    ate.state.delays["GetEnvironment"] = 3
    loop = asyncio.get_running_loop()
    start = loop.time()
    with pytest.raises(TimeoutException, match=r"within 0.3s \(request_timeout"):
        await sandbox.is_running(request_timeout=0.3)
    assert loop.time() - start < 1.5


def test_each_event_loop_gets_its_own_client(ate: Ate):
    async def lifecycle() -> bool:
        sandbox = await AsyncSandbox.create(sandbox_url=ate.url)
        return await sandbox.is_running() and await sandbox.kill()

    # A gRPC channel belongs to the loop that made it: a second asyncio.run
    # would fail on the channel of the first.
    assert asyncio.run(lifecycle()) is True
    assert asyncio.run(lifecycle()) is True
    assert ate.state.envs == {}
