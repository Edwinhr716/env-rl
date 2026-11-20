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

"""wait_pools_ready and warm_create_budget: FL-14 to FL-17 of docs/plans/fleet-sdk."""

from __future__ import annotations

import asyncio
import time

import pytest

from ate_env_rl import (
    AsyncSandboxFleet,
    Client,
    EnvironmentStatus,
    FleetConfig,
    PoolReadiness,
    SandboxFleet,
)
from ate_env_rl.backend.mock import MockBackendDriver
from ate_env_rl.backend.substrate import SubstrateBackendDriver
from ate_env_rl.config import ValidationError

IMG_A = "docker.io/library/python@sha256:" + "a" * 64
IMG_B = "docker.io/library/python@sha256:" + "b" * 64
IMG_C = "docker.io/library/python@sha256:" + "c" * 64


class _Stuck(MockBackendDriver):
    """A mock whose pools never fill past one replica."""

    def pool_size(self, template_id, rootfs_image=None):
        return min(1, super().pool_size(template_id, rootfs_image))


class _Blind(MockBackendDriver):
    """A mock that cannot count its pools."""

    def pool_size(self, template_id, rootfs_image=None):
        return None


class _Filling(MockBackendDriver):
    """A mock whose pools show empty until delay_s after the warm call."""

    def __init__(self, delay_s: float):
        super().__init__()
        self.delay_s = delay_s
        self.warm_calls = 0
        self._warmed_at = 0.0

    def warm_pool(self, *args, **kwargs):
        self.warm_calls += 1
        self._warmed_at = time.monotonic()
        super().warm_pool(*args, **kwargs)

    def pool_size(self, template_id, rootfs_image=None):
        if time.monotonic() - self._warmed_at < self.delay_s:
            return 0
        return super().pool_size(template_id, rootfs_image)


class _Calls(MockBackendDriver):
    def __init__(self):
        super().__init__()
        self.warm_calls = 0

    def warm_pool(self, *args, **kwargs):
        self.warm_calls += 1
        super().warm_pool(*args, **kwargs)


def _count_creates(monkeypatch, delay_s: float = 0.01) -> dict[str, int]:
    """Patch Client.create to count the creates in flight; returns the counters."""
    seen = {"now": 0, "max": 0, "total": 0}
    original = Client.create

    async def create(self, *args, **kwargs):
        seen["now"] += 1
        seen["total"] += 1
        seen["max"] = max(seen["max"], seen["now"])
        try:
            await asyncio.sleep(delay_s)
            return await original(self, *args, **kwargs)
        finally:
            seen["now"] -= 1

    monkeypatch.setattr(Client, "create", create)
    return seen


@pytest.mark.parametrize("fleet_kind", ["sync", "async"])
async def test_fl14_wait_pools_ready_returns_when_ready(fleet_kind):
    # FL-14: the wait returns once each pool holds its target, before the
    # timeout, and starts no warm-up of its own.
    driver = _Filling(delay_s=0.3)
    if fleet_kind == "sync":
        fleet = SandboxFleet(driver=driver)
        fleet.warm_images([IMG_A, IMG_B], replicas=3, wait=False)
        assert fleet.pool_size(IMG_A) == 0
        start = time.monotonic()
        readiness = fleet.wait_pools_ready(timeout_s=10, poll_s=0.01)
    else:
        fleet = AsyncSandboxFleet(driver=driver)
        await fleet.warm_images([IMG_A, IMG_B], replicas=3, wait=False)
        assert fleet.pool_size(IMG_A) == 0
        start = time.monotonic()
        readiness = await fleet.wait_pools_ready(timeout_s=10, poll_s=0.01)
    elapsed = time.monotonic() - start
    assert 0.2 <= elapsed < 5
    assert readiness == {IMG_A: PoolReadiness(ready=3, target=3), IMG_B: PoolReadiness(ready=3, target=3)}
    assert all(r.met for r in readiness.values())
    assert driver.warm_calls == 2


def test_fl14_sync_wait_pools_ready_starts_no_warm_up():
    driver = _Calls()
    fleet = SandboxFleet(driver=driver)
    assert fleet.wait_pools_ready(timeout_s=1) == {}
    fleet.warm_images([IMG_A, IMG_B], replicas=2)
    fleet.set_pool_replicas(IMG_B, 4)
    calls = driver.warm_calls
    start = time.monotonic()
    readiness = fleet.wait_pools_ready(timeout_s=5)
    assert time.monotonic() - start < 1
    assert readiness == {IMG_A: PoolReadiness(2, 2), IMG_B: PoolReadiness(4, 4)}
    assert driver.warm_calls == calls


def test_fl14_targets_follow_unwarm_and_teardown():
    fleet = SandboxFleet(driver=MockBackendDriver())
    fleet.warm_images([IMG_A, IMG_B, IMG_C], replicas=1)
    fleet.unwarm_image(IMG_A)
    fleet.set_pool_replicas(IMG_B, 0)
    assert fleet.wait_pools_ready(timeout_s=0) == {IMG_C: PoolReadiness(1, 1)}
    fleet.teardown()
    assert fleet.wait_pools_ready(timeout_s=0) == {}


async def test_fl14_async_targets_follow_unwarm_and_teardown():
    fleet = AsyncSandboxFleet(driver=MockBackendDriver())
    await fleet.warm_images([IMG_A, IMG_B, IMG_C], replicas=1)
    await fleet.unwarm_image(IMG_A)
    await fleet.set_pool_replicas(IMG_B, 0)
    assert await fleet.wait_pools_ready(timeout_s=0) == {IMG_C: PoolReadiness(1, 1)}
    await fleet.teardown()
    assert await fleet.wait_pools_ready(timeout_s=0) == {}


_BAD_WAIT_ARGS = [
    ({"poll_s": 0}, "poll_s"),
    ({"poll_s": -1}, "poll_s"),
    ({"poll_s": float("nan")}, "poll_s"),
    ({"poll_s": "1"}, "poll_s"),
    ({"timeout_s": "3"}, "timeout_s"),
    ({"timeout_s": None}, "timeout_s"),
    ({"timeout_s": True}, "timeout_s"),
    ({"timeout_s": float("nan")}, "timeout_s"),
]


@pytest.mark.parametrize("kwargs,name", _BAD_WAIT_ARGS)
async def test_fl15_wait_pools_ready_refuses_bad_arguments_on_both_fleets(kwargs, name):
    # Both fleets refuse the same arguments with the same error, before they
    # read a pool: a pool that never fills would otherwise hide the bad value.
    args = {"timeout_s": 0.2, **kwargs}
    sync_fleet = SandboxFleet(driver=_Stuck())
    sync_fleet.warm_images([IMG_A], replicas=3)
    with pytest.raises(ValidationError, match=name):
        sync_fleet.wait_pools_ready(**args)
    async_fleet = AsyncSandboxFleet(driver=_Stuck())
    await async_fleet.warm_images([IMG_A], replicas=3)
    with pytest.raises(ValidationError, match=name):
        await async_fleet.wait_pools_ready(**args)


def test_fl16_budget_on_a_driver_without_one_logs_a_warning(caplog):
    with caplog.at_level("WARNING", logger="ate_env_rl.fleet"):
        SandboxFleet(FleetConfig(warm_create_budget=2), driver=MockBackendDriver())
    assert "warm_create_budget=2 is ignored: MockBackendDriver" in caplog.text
    caplog.clear()
    with caplog.at_level("WARNING", logger="ate_env_rl.fleet"):
        SandboxFleet(driver=MockBackendDriver())
    assert "warm_create_budget" not in caplog.text


def test_fl16_unset_budget_keeps_the_drivers_own(fake_stack):
    client, _fakes = fake_stack
    driver = SubstrateBackendDriver(client=client, atespace="ate-env")
    driver.warm_create_budget = 5
    SandboxFleet(driver=driver)
    assert driver.warm_create_budget == 5
    SandboxFleet(FleetConfig(warm_create_budget=2), driver=driver)
    assert driver.warm_create_budget == 2


@pytest.mark.parametrize("fleet_kind", ["sync", "async"])
async def test_fl15_wait_pools_ready_returns_counts_at_timeout(fleet_kind):
    # FL-15: a pool short of its target holds the wait until the timeout,
    # which returns the counts and raises nothing.
    if fleet_kind == "sync":
        fleet = SandboxFleet(driver=_Stuck())
        fleet.warm_images([IMG_A, IMG_B], replicas=3)
        fleet.set_pool_replicas(IMG_B, 1)
        start = time.monotonic()
        readiness = fleet.wait_pools_ready(timeout_s=0.3, poll_s=0.05)
    else:
        fleet = AsyncSandboxFleet(driver=_Stuck())
        await fleet.warm_images([IMG_A, IMG_B], replicas=3)
        await fleet.set_pool_replicas(IMG_B, 1)
        start = time.monotonic()
        readiness = await fleet.wait_pools_ready(timeout_s=0.3, poll_s=0.05)
    elapsed = time.monotonic() - start
    assert 0.3 <= elapsed < 2
    assert readiness == {IMG_A: PoolReadiness(ready=1, target=3), IMG_B: PoolReadiness(ready=1, target=1)}
    assert not readiness[IMG_A].met and readiness[IMG_B].met


@pytest.mark.parametrize("fleet_kind", ["sync", "async"])
async def test_fl15_uncounted_pool_does_not_hold_the_wait(fleet_kind):
    if fleet_kind == "sync":
        fleet = SandboxFleet(driver=_Blind())
        fleet.warm_images([IMG_A], replicas=2)
        start = time.monotonic()
        readiness = fleet.wait_pools_ready(timeout_s=5)
    else:
        fleet = AsyncSandboxFleet(driver=_Blind())
        await fleet.warm_images([IMG_A], replicas=2)
        start = time.monotonic()
        readiness = await fleet.wait_pools_ready(timeout_s=5)
    assert time.monotonic() - start < 1
    assert readiness == {IMG_A: PoolReadiness(ready=None, target=2)}
    assert not readiness[IMG_A].met


async def test_fl16_warm_create_budget_caps_inflight_creates(fake_stack, monkeypatch):
    # FL-16: the budget caps the creates in flight across every image the
    # sync fleet warms at once; without it they are not capped.
    client, _fakes = fake_stack
    seen = _count_creates(monkeypatch)

    def warm(budget: int | None) -> None:
        cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test",
                          warm_create_budget=budget)
        fleet = SandboxFleet(cfg)
        try:
            fleet.warm_images([IMG_A, IMG_B, IMG_C], replicas=4)
            assert fleet.wait_pools_ready(timeout_s=10) == {
                img: PoolReadiness(4, 4) for img in (IMG_A, IMG_B, IMG_C)
            }
            fleet.teardown()
        finally:
            fleet.close()

    await asyncio.to_thread(warm, 2)
    assert seen["total"] == 12
    assert seen["max"] == 2

    seen.update(max=0, total=0)
    await asyncio.to_thread(warm, None)
    assert seen["total"] == 12
    assert seen["max"] > 2


async def test_fl17_async_warm_create_budget(fake_stack, monkeypatch):
    # FL-17: AsyncSandboxFleet.warm_images obeys the same budget.
    client, _fakes = fake_stack
    seen = _count_creates(monkeypatch)
    driver = SubstrateBackendDriver(client=client, atespace="ate-env")
    fleet = AsyncSandboxFleet(FleetConfig(warm_create_budget=3), driver)
    await fleet.warm_images([IMG_A, IMG_B, IMG_C], replicas=4)
    assert seen["total"] == 12
    assert seen["max"] == 3
    assert await fleet.wait_pools_ready(timeout_s=10) == {
        img: PoolReadiness(4, 4) for img in (IMG_A, IMG_B, IMG_C)
    }
    await fleet.teardown()

    seen.update(max=0, total=0)
    unbudgeted = AsyncSandboxFleet(driver=SubstrateBackendDriver(client=client, atespace="ate-env"))
    await unbudgeted.warm_images([IMG_A, IMG_B, IMG_C], replicas=4)
    assert seen["max"] > 3
    await unbudgeted.teardown()


async def test_fl17_creates_waiting_for_a_slot_do_not_start_after_a_teardown(fake_stack, monkeypatch):
    # With a budget of 1, most creates wait for a slot. A teardown during the
    # warm stops them: no create starts once the reap began.
    client, _fakes = fake_stack
    starts: list[float] = []
    original = Client.create

    async def create(self, *args, **kwargs):
        starts.append(time.monotonic())
        await asyncio.sleep(0.05)
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(Client, "create", create)
    driver = SubstrateBackendDriver(client=client, atespace="ate-env")
    fleet = AsyncSandboxFleet(FleetConfig(warm_create_budget=1), driver)
    warm = asyncio.create_task(fleet.warm_images([IMG_A, IMG_B], replicas=5))
    await asyncio.sleep(0.12)
    teardown_started = time.monotonic()
    await fleet.teardown()
    await warm
    assert 1 <= len(starts) < 10
    assert [t for t in starts if t >= teardown_started] == []
    assert fleet.pool_size(IMG_A) in (0, None)


async def test_fl17_a_changed_budget_applies_to_later_warms(fake_stack, monkeypatch):
    client, _fakes = fake_stack
    seen = _count_creates(monkeypatch)
    driver = SubstrateBackendDriver(client=client, atespace="ate-env")
    fleet = AsyncSandboxFleet(FleetConfig(warm_create_budget=2), driver)
    await fleet.warm_images([IMG_A, IMG_B], replicas=4)
    assert seen["max"] == 2
    await fleet.teardown()
    seen.update(max=0, total=0)
    driver.warm_create_budget = 4
    await fleet.warm_images([IMG_A, IMG_B], replicas=4)
    assert seen["max"] == 4
    await fleet.teardown()


@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "2"])
def test_fl16_warm_create_budget_validation(bad):
    with pytest.raises(ValidationError, match="warm_create_budget"):
        FleetConfig(warm_create_budget=bad)


async def test_fl14_substrate_wait_pools_ready_waits_for_background_suspend(fake_stack, monkeypatch):
    client, fakes = fake_stack
    orig_suspend = Client.suspend
    suspend_gate = asyncio.Event()
    suspend_started = 0

    async def gated_suspend(self, *args, **kwargs):
        nonlocal suspend_started
        suspend_started += 1
        await suspend_gate.wait()
        return await orig_suspend(self, *args, **kwargs)

    monkeypatch.setattr(Client, "suspend", gated_suspend)

    driver = SubstrateBackendDriver(client=client, atespace="ate-env")
    fleet = AsyncSandboxFleet(FleetConfig(warm_create_budget=2), driver)
    try:
        await fleet.warm_images([IMG_A], replicas=2, wait=False)
        assert fleet.pool_size(IMG_A) == 0

        timed_out = await fleet.wait_pools_ready(timeout_s=0.05, poll_s=0.01)
        assert timed_out == {IMG_A: PoolReadiness(ready=0, target=2)}
        assert not timed_out[IMG_A].met
        assert suspend_started == 2

        suspend_gate.set()
        readiness = await fleet.wait_pools_ready(timeout_s=5.0, poll_s=0.01)
        assert readiness == {IMG_A: PoolReadiness(ready=2, target=2)}
        assert readiness[IMG_A].met
        for aid in next(iter(driver._warm_paused_pool.values())):
            assert EnvironmentStatus(
                fakes.environments.environments[("ate-env", aid)].status
            ) in (EnvironmentStatus.SUSPENDED, EnvironmentStatus.PAUSED)
    finally:
        suspend_gate.set()
        await fleet.teardown()


async def test_fl14_concurrent_warm_images_and_zero_replicas(monkeypatch):
    # Targets are recorded before template resolution, so a concurrent
    # wait_pools_ready does not return {} early, and replicas=0 drops a target.
    gate = asyncio.Event()
    afleet = AsyncSandboxFleet(driver=MockBackendDriver())
    orig_ensure = afleet._ensure_template_for_image_async

    async def slow_ensure(image: str):
        await gate.wait()
        return await orig_ensure(image)

    monkeypatch.setattr(afleet, "_ensure_template_for_image_async", slow_ensure)

    warm_task = asyncio.create_task(afleet.warm_images([IMG_A], replicas=2, wait=True))
    wait_task = asyncio.create_task(afleet.wait_pools_ready(timeout_s=5.0, poll_s=0.01))
    await asyncio.sleep(0.03)
    assert not wait_task.done()

    gate.set()
    readiness = await wait_task
    await warm_task
    assert readiness == {IMG_A: PoolReadiness(ready=2, target=2)}

    await afleet.warm_images([IMG_A], replicas=0)
    assert await afleet.wait_pools_ready(timeout_s=0) == {}
    await afleet.teardown()

    sync_fleet = SandboxFleet(driver=MockBackendDriver())
    sync_fleet.warm_images([IMG_A], replicas=2)
    sync_fleet.warm_images([IMG_A], replicas=0)
    assert sync_fleet.wait_pools_ready(timeout_s=0) == {}
    sync_fleet.teardown()


@pytest.mark.parametrize("action", ["unwarm", "shrink"])
async def test_fl17_creates_waiting_for_a_slot_do_not_start_after_unwarm_or_shrink(
    fake_stack, monkeypatch, action
):
    # Generation check: unwarm_image() or a shrinking set_pool_replicas() during
    # a budgeted warm stops queued creates from starting.
    client, _fakes = fake_stack
    starts = 0
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    original = Client.create

    async def create(self, *args, **kwargs):
        nonlocal starts
        starts += 1
        if starts == 1:
            first_started.set()
            await release_first.wait()
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(Client, "create", create)
    driver = SubstrateBackendDriver(client=client, atespace="ate-env")
    fleet = AsyncSandboxFleet(FleetConfig(warm_create_budget=1), driver)
    try:
        warm = asyncio.create_task(fleet.warm_images([IMG_A], replicas=5))
        await first_started.wait()
        if action == "unwarm":
            await fleet.unwarm_image(IMG_A)
            release_first.set()
            await warm
            assert starts == 1
            assert fleet.pool_size(IMG_A) in (0, None)
        else:
            # set_pool_replicas(1) sees pool_size=0 and creates 1 of its own,
            # while the 4 queued creates from the cancelled warm do not start.
            resize = asyncio.create_task(fleet.set_pool_replicas(IMG_A, 1))
            await asyncio.sleep(0)
            release_first.set()
            await asyncio.gather(warm, resize)
            assert starts == 2
            assert fleet.pool_size(IMG_A) == 1
    finally:
        release_first.set()
        report = await fleet.teardown(verify=True)
        assert report is not None and report.surviving == ()


async def test_fl14_substrate_wait_false_failed_or_torn_down_suspend_cleans_up(
    fake_stack, monkeypatch
):
    client, fakes = fake_stack
    orig_suspend = Client.suspend
    calls = 0

    async def fail_first_suspend(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("suspend failed")
        return await orig_suspend(self, *args, **kwargs)

    monkeypatch.setattr(Client, "suspend", fail_first_suspend)
    driver = SubstrateBackendDriver(client=client, atespace="ate-env")
    fleet = AsyncSandboxFleet(FleetConfig(warm_create_budget=2), driver)
    try:
        await fleet.warm_images([IMG_A], replicas=2, wait=False)
        await asyncio.gather(*list(driver._background_tasks), return_exceptions=True)
        assert fleet.pool_size(IMG_A) == 1
        assert driver._warm_inflight == {}
        assert len(fakes.environments.environments) == 1
    finally:
        await fleet.teardown()

    # Teardown during an in-flight wait=False suspend does not race delete()
    # against suspend(), and the actor is deleted once suspend finishes.
    suspend_gate = asyncio.Event()
    suspend_started = asyncio.Event()
    delete_during_suspend = False
    orig_delete = Client.delete

    async def gated_suspend(self, *args, **kwargs):
        suspend_started.set()
        await suspend_gate.wait()
        return await orig_suspend(self, *args, **kwargs)

    async def tracked_delete(self, *args, **kwargs):
        nonlocal delete_during_suspend
        if not suspend_gate.is_set():
            delete_during_suspend = True
        return await orig_delete(self, *args, **kwargs)

    monkeypatch.setattr(Client, "suspend", gated_suspend)
    monkeypatch.setattr(Client, "delete", tracked_delete)
    driver2 = SubstrateBackendDriver(client=client, atespace="ate-env")
    fleet2 = AsyncSandboxFleet(FleetConfig(warm_create_budget=2), driver2)
    try:
        await fleet2.warm_images([IMG_A], replicas=1, wait=False)
        await suspend_started.wait()
        await fleet2.teardown()
        assert not delete_during_suspend
        suspend_gate.set()
        await asyncio.gather(*list(driver2._background_tasks), return_exceptions=True)
        assert fleet2.pool_size(IMG_A) == 0
        assert driver2._warm_inflight == {}
        assert fakes.environments.environments == {}
    finally:
        suspend_gate.set()
        await fleet2.teardown()
