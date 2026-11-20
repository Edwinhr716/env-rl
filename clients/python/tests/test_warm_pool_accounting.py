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

"""Warm-pool accounting under concurrency.

For any sequence of acquire, recycle, warm_pool and teardown operations,
interleaved at their awaits, by two runs on one driver: the warm-replica gauge
equals the warm pool, no warm actor is lost before the teardown, the teardown
of a run leaves no environment alive but what the other run holds, and
warm_pool never provisions more than it was asked for. The fake client pauses ``suspend()`` per actor, ``create()``
per warm_pool call, an acquire's ``get()`` and ``create()`` on its own gate,
``delete()`` during a teardown and, when told to, fails ``get()``, ``create()``
or ``delete()``, so the test chooses where the coroutines interleave.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import dataclasses
import itertools
import logging
import sys
import threading
from collections.abc import Callable, Coroutine
from typing import Any

import grpc.aio
import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    initialize,
    invariant,
    multiple,
    precondition,
    rule,
)

from ate_env_rl._gen.ateenv.v1alpha import env_pb2_grpc
from ate_env_rl.backend.substrate import SubstrateBackendDriver
from ate_env_rl.config import WARM_POOL_RUN_ID, ObservabilityConfig
from ate_env_rl.errors import (
    InfrastructureError,
    NotFoundError,
    SandboxStartError,
    SandboxUnavailableError,
)
from ate_env_rl.observability import Observer
from ate_env_rl.telemetry import metrics

from .fakes import FakeEnvironmentService

R = 3  # replicas asked for by every warm_pool call


@dataclasses.dataclass
class AcquireGate:
    """Pauses one acquire's ``get()`` at ``get_event`` and its ``create()`` at
    ``create_event``: a warm claim let through can still pause in the cold
    create it falls back to."""

    get_event: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    create_event: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    # The event the acquire waits on now; None before its first gated call.
    waiting: asyncio.Event | None = None
    # The create() reaches the server before it waits: the actor is live,
    # and only the reply is pending.
    lands_first: bool = False
    # If set, get() raises NotFoundError when the actor was deleted while gated.
    check_live: bool = False
    # The warm actor the acquire popped and now checks in get(); None outside it.
    claiming: str | None = None


# Set in the task that runs an acquire; the tasks it starts inherit it.
acquire_gate: contextvars.ContextVar[AcquireGate | None] = contextvars.ContextVar(
    "acquire_gate", default=None
)


class GatedClient:
    """A Substrate client whose calls wait on, or fail by, test-held settings."""

    def __init__(self) -> None:
        self.live: set[str] = set()
        self.suspend_gates: dict[str, asyncio.Event] = {}
        self.create_gate: asyncio.Event | None = None
        self.create_lands_first = False
        self.create_errors: dict[int, Exception] = {}  # by call number, from 0
        self.creates = 0
        self.delete_gate: asyncio.Event | None = None
        self.delete_gates: dict[str, asyncio.Event] = {}
        self.delete_error: Exception | None = None
        self.delete_errors: dict[str, Exception] = {}  # per actor, until removed
        self.delete_failures = 0  # fail this many delete() calls, then succeed
        self.get_gate: asyncio.Event | None = None
        self.get_error: Exception | None = None
        self.suspend_error: Exception | None = None

    async def create(self, actor_id: str, **kw: object) -> None:
        n, self.creates = self.creates, self.creates + 1
        gate = acquire_gate.get()
        if gate is not None:
            if gate.lands_first and n not in self.create_errors:
                self.live.add(actor_id)
            gate.waiting = gate.create_event
            await gate.create_event.wait()
        elif self.create_gate is not None:
            if self.create_lands_first and n not in self.create_errors:
                self.live.add(actor_id)
            await self.create_gate.wait()
        if n in self.create_errors:
            raise self.create_errors[n]
        self.live.add(actor_id)

    async def suspend(self, actor_id: str, **kw: object) -> None:
        gate = self.suspend_gates.get(actor_id)
        if gate is not None:
            await gate.wait()
        if self.suspend_error is not None:
            raise self.suspend_error

    async def delete(self, actor_id: str, **kw: object) -> None:
        actor_gate = self.delete_gates.get(actor_id)
        if actor_gate is not None:
            await actor_gate.wait()
        elif self.delete_gate is not None:
            await self.delete_gate.wait()
        actor_error = self.delete_errors.get(actor_id)
        if actor_error is not None:
            raise actor_error
        if self.delete_error is not None:
            raise self.delete_error
        if self.delete_failures > 0:
            self.delete_failures -= 1
            raise RuntimeError("unavailable")
        self.live.discard(actor_id)

    async def get(self, actor_id: str, **kw: object) -> None:
        gate = acquire_gate.get()
        if gate is not None:
            gate.waiting = gate.get_event
            gate.claiming = actor_id
            try:
                await gate.get_event.wait()
            finally:
                gate.claiming = None
            if gate.check_live and actor_id not in self.live:
                raise NotFoundError(actor_id)
        if self.get_gate is not None:
            await self.get_gate.wait()
        if self.get_error is not None:
            raise self.get_error


def _driver(client: GatedClient) -> tuple[SubstrateBackendDriver, Observer]:
    driver = SubstrateBackendDriver(client=client)
    obs = Observer(ObservabilityConfig(enable_metrics=False))
    driver.observer = obs
    return driver, obs


def _pool(driver: SubstrateBackendDriver) -> list[str]:
    return [a for v in driver._warm_paused_pool.values() for a in v]


async def _spin() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


async def test_recycle_paused_across_teardown_deletes_the_environment() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.suspend_gates["sb-1"] = asyncio.Event()

    recycle = asyncio.create_task(driver.release_async("sb-1", recycle=True))
    await _spin()  # paused in suspend()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.suspend_gates["sb-1"].set()
    await recycle

    assert client.live == set()
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_overlapping_warm_pool_calls_provision_once() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    first = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    second = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(first, second)

    assert len(_pool(driver)) == R
    assert len(client.live) == R
    assert sum(obs._warm.values()) == R


async def test_warm_pool_paused_across_teardown_deletes_its_actors() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.create_gate.set()
    await warm

    assert client.live == set()
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_after_teardown_ignores_the_old_reservation() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    old = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    new = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-2"))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(old, new)

    assert len(_pool(driver)) == R
    assert client.live == set(_pool(driver))
    assert sum(obs._warm.values()) == R


async def test_failed_warm_claim_after_teardown_deletes_the_environment() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    # A recycled actor from another run, waiting in the pool.
    client.live.add("sb-x")
    driver._owned_actors["sb-x"] = driver._make_instance("sb-x", "tmpl", run_id="run-0")
    driver._warm_paused_pool.setdefault(("tmpl", None), []).append("sb-x")
    obs.warm_add("substrate", 1)
    client.get_gate = asyncio.Event()
    client.get_error = RuntimeError("unavailable")

    claim = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # paused in get()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.get_gate.set()
    with pytest.raises(SandboxStartError):
        await claim

    assert client.live == set()
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_finishing_during_a_reap_deletes_its_actors() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.delete_gate = asyncio.Event()

    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # paused in the reap's delete()
    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # created during the reap
    assert _pool(driver) == []
    client.delete_gate.set()
    await reap
    obs.warm_reset()
    await warm

    assert client.live == set()
    assert _pool(driver) == []
    assert driver._owned_actors == {}
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_pools_what_it_created_before_raising() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_errors[1] = NotFoundError("no such template")

    with pytest.raises(SandboxStartError):
        await driver.warm_pool_async("tmpl", R)

    assert len(_pool(driver)) == R - 1
    assert set(_pool(driver)) == client.live
    assert sum(obs._warm.values()) == R - 1


async def test_failed_delete_after_teardown_keeps_the_actors_for_the_next_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    failures: list[str] = []
    obs.release_failed = failures.append  # type: ignore[method-assign]
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_error = RuntimeError("unavailable")
    client.create_gate.set()
    await warm

    assert failures == ["substrate"] * R
    assert len(client.live) == R
    assert set(driver._owned_actors) == client.live
    assert _pool(driver) == []

    client.delete_error = None
    await driver.reap_async("run-1")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_cancelled_delete_after_teardown_keeps_the_actor_owned() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-x")
    driver._owned_actors["sb-x"] = driver._make_instance("sb-x", "tmpl", run_id="run-0")
    driver._warm_paused_pool.setdefault(("tmpl", None), []).append("sb-x")
    obs.warm_add("substrate", 1)
    client.get_gate = asyncio.Event()
    client.get_error = RuntimeError("unavailable")

    claim = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # paused in get()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_gate = asyncio.Event()
    client.get_gate.set()
    await _spin()  # paused in the delete after teardown
    claim.cancel()
    with pytest.raises(asyncio.CancelledError):
        await claim

    assert "sb-x" in driver._owned_actors
    client.delete_gate.set()
    await driver.reap_async("run-0")
    assert client.live == set()


async def test_recycle_cancelled_in_suspend_keeps_the_actor_owned() -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.suspend_gates["sb-1"] = asyncio.Event()

    recycle = asyncio.create_task(driver.release_async("sb-1", recycle=True))
    await _spin()  # paused in suspend()
    recycle.cancel()
    with pytest.raises(asyncio.CancelledError):
        await recycle

    assert "sb-1" in driver._owned_actors
    await driver.reap_async("run-1")
    assert client.live == set()


async def test_failed_delete_after_teardown_leaves_warm_actors_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_failures = R
    client.create_gate.set()
    await warm
    assert len(client.live) == R

    # The next run's reap, not run-1's, finds them.
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_failed_recycle_delete_after_teardown_leaves_the_actor_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-1")
    handle = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    driver._owned_actors["sb-1"] = handle
    client.suspend_gates["sb-1"] = asyncio.Event()

    recycle = asyncio.create_task(driver.release_async("sb-1", recycle=True))
    await _spin()  # paused in suspend()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_error = RuntimeError("unavailable")
    client.suspend_gates["sb-1"].set()
    await recycle
    await _spin()  # the background delete that retries it failed too
    assert client.live == {"sb-1"}
    assert driver._owned_actors["sb-1"].run_id == WARM_POOL_RUN_ID
    assert handle.run_id == "run-1"  # the caller's handle is not changed
    assert not driver._background_deletes

    client.delete_error = None
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


# AC-15.9: a cancelled cleanup delete leaves the actor where a later run's reap
# finds it.
async def test_cancelled_delete_after_reap_leaves_warm_actors_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_gate = asyncio.Event()
    client.create_gate.set()
    await _spin()  # paused in the deletes after the teardown
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm
    assert len(client.live) == R

    client.delete_gate = None
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


# A release cancelled, or whose delete fails, with a teardown since it popped
# its actor: the reap could not see the actor, and a fleet reaps its run once.
# The release deletes it in a background task, which a reap still running
# joins, as a cancelled acquire does (_own_abandoned_create).
@pytest.mark.parametrize("when", ["during_reap", "after_reap"])
@pytest.mark.parametrize("lost", ["cancelled_in_suspend", "cancelled_in_delete", "failed"])
async def test_release_lost_across_a_reap_deletes_the_actor(lost: str, when: str) -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.update({"sb-1", "sb-old"})
    handle = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    driver._owned_actors["sb-1"] = handle
    driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
    recycle = lost == "cancelled_in_suspend"
    if recycle:
        client.suspend_gates["sb-1"] = asyncio.Event()
    client.delete_gates["sb-1"] = asyncio.Event()
    client.delete_gates["sb-old"] = asyncio.Event()  # holds the reap's own delete

    release = asyncio.create_task(driver.release_async("sb-1", recycle=recycle))
    await _spin()  # paused in suspend() or delete(); the actor is not owned now
    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # cannot see sb-1; waits in its delete of sb-old
    if when == "after_reap":
        client.delete_gates.pop("sb-old").set()
        assert await reap == 1
        obs.warm_reset()

    if lost == "failed":
        client.delete_failures = 1  # the release's delete fails; the next one lands
        client.delete_gates.pop("sb-1").set()
        await release  # release does not raise on a failed delete
    else:
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release
        await _spin()  # the background delete waits at the gate
        assert client.live >= {"sb-1"}
        assert [aid for t, aid in driver._background_deletes.items() if not t.done()] == ["sb-1"]
    assert handle.run_id == "run-1"  # the caller's handle is not changed

    if when == "during_reap":
        client.delete_gates.pop("sb-old").set()
        await _spin()  # the reap's own delete landed; it now joins the background one
        if lost != "failed":
            assert not reap.done(), "the reap returned without joining the background delete"
            client.delete_gates.pop("sb-1").set()
        assert await reap == 1
    else:
        if lost != "failed":
            client.delete_gates.pop("sb-1").set()
        await _spin()

    assert client.live == set()
    assert driver._owned_actors == {}
    assert not driver._background_deletes
    assert _pool(driver) == [] and sum(obs._warm.values()) == 0


@pytest.mark.parametrize("lost", ["cancelled", "failed"])
async def test_release_lost_across_a_reap_whose_background_delete_fails_leaves_the_actor_for_any_later_reap(
    lost: str,
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-1")
    handle = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    driver._owned_actors["sb-1"] = handle
    client.delete_gate = asyncio.Event()

    release = asyncio.create_task(driver.release_async("sb-1"))
    await _spin()  # paused in delete(); the actor is not owned now
    await driver.reap_async("run-1")  # cannot see it
    client.delete_error = RuntimeError("unavailable")
    if lost == "cancelled":
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release
    client.delete_gate.set()
    await _spin()  # the background delete failed too
    assert release.done()
    assert client.live == {"sb-1"}
    assert driver._owned_actors["sb-1"].run_id == WARM_POOL_RUN_ID
    assert handle.run_id == "run-1"  # the caller's handle is not changed
    assert not driver._background_deletes

    client.delete_error = None
    assert await driver.reap_async("run-2") == 1
    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("recycle", [True, False])
async def test_release_cancelled_without_a_teardown_keeps_the_run_id_and_starts_no_delete(
    recycle: bool,
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.suspend_gates["sb-1"] = asyncio.Event()
    client.delete_gate = asyncio.Event()

    release = asyncio.create_task(driver.release_async("sb-1", recycle=recycle))
    await _spin()  # paused in suspend() or delete()
    release.cancel()
    with pytest.raises(asyncio.CancelledError):
        await release
    await _spin()

    assert client.live == {"sb-1"}
    assert driver._owned_actors["sb-1"].run_id == "run-1"  # the run's own reap deletes it
    assert not driver._background_deletes
    client.delete_gate.set()
    assert await driver.reap_async("run-1") == 1
    assert client.live == set()


async def test_warm_pool_cancelled_before_its_deletes_start_leaves_actors_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    async def never_runs(actor_id: str, why: str) -> None:
        # Stands for a cancel that lands before any _delete_unpooled starts.
        await asyncio.Event().wait()

    driver._delete_unpooled = never_runs  # type: ignore[method-assign]
    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.create_gate.set()
    await _spin()  # owned the actors, awaiting the deletes
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm
    assert len(client.live) == R
    assert {i.run_id for i in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}

    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_release_delete_failure_without_a_reap_keeps_the_run_id() -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.delete_failures = 1

    await driver.release_async("sb-1")
    assert driver._owned_actors["sb-1"].run_id == "run-1"  # the run's own reap retries it
    await driver.reap_async("run-1")
    assert client.live == set()


# AC-15.7: a warm claim whose get() returns after an overlapping reap deleted
# the actor does not hand that actor out.
@pytest.mark.parametrize("reap_done", [True, False])
@pytest.mark.parametrize("owner", ["run-0", "run-1"])  # run-1: the reap skips it
async def test_warm_claim_overlapping_a_reap_returns_a_live_environment(
    reap_done: bool, owner: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    subphases: list[tuple[str, str, str]] = []

    def record(phase: str, seconds: float, status: str = "ok", *, source: str = "direct") -> None:
        subphases.append((phase, status, source))

    monkeypatch.setattr(metrics, "observe_subphase", record)
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-x")
    driver._owned_actors["sb-x"] = driver._make_instance("sb-x", "tmpl", run_id=owner)
    driver._warm_paused_pool.setdefault(("tmpl", None), []).append("sb-x")
    obs.warm_add("substrate", 1)
    client.get_gate = asyncio.Event()

    claim = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # popped sb-x, paused in get()
    if not reap_done:
        # run-0: pauses the reap's delete of sb-x. run-1: the reap skips sb-x
        # and deletes nothing; this pauses the claim's own delete of it.
        client.delete_gate = asyncio.Event()
    reap = asyncio.create_task(driver.reap_async("run-0"))
    await _spin()  # run-0: the reap took sb-x (still owned); run-1: it skips sb-x
    client.get_gate.set()  # get() still succeeds
    await _spin()
    if not reap_done:
        assert client.delete_gate is not None
        client.delete_gate.set()
    instance = await claim
    await reap

    assert instance.instance_id != "sb-x"
    assert instance.instance_id in client.live
    assert instance.metadata["claim_source"] == "cold_create"
    assert "sb-x" not in client.live
    assert set(driver._owned_actors) == {instance.instance_id}
    # One sample for the abandoned warm claim, then the cold create's own.
    assert subphases == [
        ("wire_claim", "error", "warm_pool"),
        ("wire_claim", "ok", "cold_create"),
    ]


class SlowCreatesClient(GatedClient):
    """The first ``fast`` create() calls return at once; the rest wait on create_gate."""

    def __init__(self, fast: int) -> None:
        super().__init__()
        self.fast = fast

    async def create(self, actor_id: str, **kw: object) -> None:
        if self.creates < self.fast:
            self.creates += 1
            self.live.add(actor_id)
            return
        await super().create(actor_id, **kw)

    async def delete(self, actor_id: str, **kw: object) -> None:
        if actor_id not in self.live:
            raise NotFoundError(actor_id)
        await super().delete(actor_id, **kw)


class SlowSuspendsClient(SlowCreatesClient):
    """Every create() lands; the first ``fast`` suspend() calls return, the rest wait."""

    def __init__(self, fast: int) -> None:
        super().__init__(fast=1_000)
        self.fast_suspends = fast
        self.suspends = 0
        self.suspend_gate = asyncio.Event()

    async def suspend(self, actor_id: str, **kw: object) -> None:
        n, self.suspends = self.suspends, self.suspends + 1
        if n >= self.fast_suspends:
            await self.suspend_gate.wait()


# AC-15.8: a warm_pool call cancelled during its create() calls leaves no live
# actor untracked; any run's reap deletes what it made.
async def test_cancelled_warm_pool_leaves_its_actors_for_any_reap() -> None:
    client = SlowCreatesClient(fast=2)
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # two actors created, the third create() paused
    assert len(client.live) == 2
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm

    assert client.live <= set(driver._owned_actors)  # nothing alive is untracked
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0
    assert driver._warm_inflight == {}
    # The third id's create never landed: NotFound for the reap, not counted.
    assert await driver.reap_async("run-2") == 2
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_warm_pool_cancelled_in_suspend_leaves_its_actors_for_any_reap() -> None:
    client = SlowSuspendsClient(fast=2)
    driver, obs = _driver(client)

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))  # wait=True
    await _spin()  # three actors created, the third suspend() paused
    assert len(client.live) == R
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm

    assert client.live <= set(driver._owned_actors)
    assert _pool(driver) == []
    assert await driver.reap_async("run-2") == R
    assert client.live == set()


async def test_warm_pool_cancelled_just_before_the_runs_reap_deletes_its_actors() -> None:
    # The reap snapshots what it deletes before the cancel handler runs, and
    # the run is not reaped again: the handler deletes the actors itself.
    client = SlowCreatesClient(fast=2)
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # two actors created, the third create() paused
    warm.cancel()
    await driver.reap_async("run-1")  # no await in between: the handler has not run
    with pytest.raises(asyncio.CancelledError):
        await warm
    await asyncio.gather(*list(driver._background_tasks))

    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("delete_fails", [False, True])
async def test_warm_actor_whose_suspend_fails_is_deleted_or_left_for_any_reap(
    delete_fails: bool,
) -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.suspend_error = RuntimeError("suspend failed")
    if delete_fails:
        client.delete_failures = R

    await driver.warm_pool_async("tmpl", R, run_id="run-1")  # wait=True

    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0
    assert client.live <= set(driver._owned_actors)  # nothing alive is untracked
    if delete_fails:
        assert len(client.live) == R
        assert {i.run_id for i in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}
        await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


# AC-15.10: a cold acquire whose create overlaps its own run's reap leaves the
# environment where a later reap finds it.
@pytest.mark.parametrize("reap_running", [False, True])
async def test_cold_acquire_overlapping_its_runs_reap_leaves_the_actor_for_any_later_reap(
    reap_running: bool,
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    if reap_running:
        # Another actor of run-1 keeps the reap in its delete() until the gate opens.
        client.live.add("sb-old")
        driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
        client.delete_gate = asyncio.Event()
    client.create_gate = asyncio.Event()

    acquire = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # cold create paused
    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # the reap's snapshot does not hold the new actor
    client.create_gate.set()
    instance = await acquire
    if reap_running:
        assert client.delete_gate is not None
        client.delete_gate.set()
    await reap

    assert instance.run_id == "run-1"  # the caller's handle is not changed
    assert driver._owned_actors[instance.instance_id].run_id == WARM_POOL_RUN_ID
    assert client.live == {instance.instance_id}
    client.delete_gate = None
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("reaped", ["another-run-during", "own-run-before"])
async def test_cold_acquire_keeps_its_run_id_without_an_overlapping_reap_of_its_run(
    reaped: str,
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    if reaped == "own-run-before":
        await driver.reap_async("run-1")  # a fleet tears down between runs and runs again
    client.create_gate = asyncio.Event()

    acquire = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()
    if reaped == "another-run-during":
        await driver.reap_async("run-0")
    client.create_gate.set()
    instance = await acquire

    assert driver._owned_actors[instance.instance_id].run_id == "run-1"
    await driver.reap_async("run-1")
    assert client.live == set()


@pytest.mark.parametrize("get_outcome", ["not_found", "ok"])
async def test_warm_claim_paused_across_own_runs_reap_tags_cold_fallback_for_any_reap(
    get_outcome: str,
) -> None:
    # AC-15.10: an acquire that pops a warm actor before its own run's reap,
    # pauses in get() across the whole reap, and then falls back to a cold
    # create owns the new actor under WARM_POOL_RUN_ID so a later reap deletes it.
    client = GatedClient()
    driver, obs = _driver(client)
    await driver.warm_pool_async("tmpl", 1, run_id="run-1")
    warm_id = _pool(driver)[0]
    client.get_gate = asyncio.Event()
    if get_outcome == "not_found":
        client.get_error = NotFoundError(warm_id)

    acquire = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # popped warm_id, paused in get()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.get_gate.set()
    instance = await acquire

    assert instance.instance_id != warm_id
    assert instance.run_id == "run-1"  # the caller's handle keeps its run_id
    assert driver._owned_actors[instance.instance_id].run_id == WARM_POOL_RUN_ID
    assert client.live == {instance.instance_id}
    assert await driver.reap_async("run-2") == 1
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_warm_claim_paused_across_another_runs_reap_keeps_own_run_id_on_cold_fallback() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    await driver.warm_pool_async("tmpl", 1, run_id="run-0")
    warm_id = _pool(driver)[0]
    client.get_gate = asyncio.Event()
    client.get_error = NotFoundError(warm_id)

    acquire = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # popped warm_id, paused in get()
    await driver.reap_async("run-0")
    obs.warm_reset()
    client.get_gate.set()
    instance = await acquire

    assert driver._owned_actors[instance.instance_id].run_id == "run-1"
    await driver.reap_async("run-2")  # another run's reap must spare run-1's live actor
    assert client.live == {instance.instance_id}
    await driver.reap_async("run-1")
    assert client.live == set()


async def test_warm_claim_retried_after_own_runs_reap_owns_the_refilled_actor_for_any_reap() -> None:
    # AC-15.10: a warm claim whose get() is NotFound across its own run's reap
    # retries. If another run refilled the pool in between, the retry pops that
    # actor. The run's reap is over, so the actor is owned under
    # WARM_POOL_RUN_ID, where a later reap of any run finds it.
    client = GatedClient()
    driver, obs = _driver(client)
    await driver.warm_pool_async("tmpl", 1, run_id="run-1")
    warm_id = _pool(driver)[0]
    gate = AcquireGate(check_live=True)  # get() is NotFound once the reap deleted warm_id
    token = acquire_gate.set(gate)
    acquire = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    acquire_gate.reset(token)
    await _spin()  # popped warm_id, paused in get()
    await driver.reap_async("run-1")
    obs.warm_reset()
    await driver.warm_pool_async("tmpl", 1, run_id="run-2")  # another run refills the pool
    (refill_id,) = _pool(driver)
    assert refill_id != warm_id
    gate.get_event.set()
    instance = await acquire

    assert instance.instance_id == refill_id
    assert instance.metadata["claim_source"] == "warm_pool"
    assert instance.run_id == "run-1"  # the caller's handle keeps its run_id
    assert driver._owned_actors[refill_id].run_id == WARM_POOL_RUN_ID
    assert _pool(driver) == []
    assert client.live == {refill_id}
    assert await driver.reap_async("run-3") == 1
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_warm_claim_paused_across_own_runs_reap_then_cancelled_in_cold_create_deletes_actor() -> None:
    # A warm claim paused across its own run's reap that falls back to cold
    # create and is then cancelled starts a background delete immediately,
    # since a reap ran after the acquire started.
    client = GatedClient()
    driver, obs = _driver(client)
    await driver.warm_pool_async("tmpl", 1, run_id="run-1")
    warm_id = _pool(driver)[0]
    client.get_gate = asyncio.Event()
    client.get_error = NotFoundError(warm_id)
    client.create_gate = asyncio.Event()
    client.create_lands_first = True

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # popped warm_id, paused in get()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.get_gate.set()
    await _spin()  # in cold create(), actor landed on server
    assert len(client.live) == 1
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    await _spin()

    assert client.live == set()
    assert driver._owned_actors == {}


# AC-15.2: a cold acquire cancelled while its create() is in flight, by its own
# timeout_s or by its caller, leaves the actor, if the create landed, for any
# run's reap.
@pytest.mark.parametrize("cancel", ["timeout", "caller"])
async def test_cold_acquire_cancelled_in_create_leaves_the_actor_for_any_reap(
    cancel: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    subphases: list[tuple[str, str, str]] = []

    def record(phase: str, seconds: float, status: str = "ok", *, source: str = "direct") -> None:
        subphases.append((phase, status, source))

    monkeypatch.setattr(metrics, "observe_subphase", record)
    client = GatedClient()
    driver, _ = _driver(client)
    acquire_gate.set(AcquireGate(lands_first=True))  # the reply never arrives

    if cancel == "timeout":
        with pytest.raises(SandboxStartError):
            await driver.acquire_async("tmpl", "run-1", timeout_s=0.05)
    else:
        acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
        await _spin()  # paused in create()
        acquire.cancel()
        with pytest.raises(asyncio.CancelledError):
            await acquire
    assert client.creates == 1 and len(client.live) == 1
    assert {i.run_id for i in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}
    # One wire_claim sample for the abandoned create, as for a failed one.
    wire = [s for s in subphases if s[0] == "wire_claim"]
    assert wire == [("wire_claim", "error", "cold_create")]

    await driver.reap_async("run-2")  # any later reap
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_cold_acquire_cancelled_in_create_after_its_runs_reap_deletes_the_actor() -> None:
    # The run's reap ran during the create and could not see the actor, and a
    # fleet reaps its run once: the cancel deletes it.
    client = GatedClient()
    driver, _ = _driver(client)
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # paused in create(), the actor live
    await driver.reap_async("run-1")
    assert len(client.live) == 1
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    await asyncio.gather(*list(driver._background_tasks))

    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("when", ["before_reap_no_await", "during_reap"])
async def test_reap_waits_for_background_delete_from_cancelled_cold_acquire(when: str) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # paused in create(), actor live
    assert len(client.live) == 1

    if when == "before_reap_no_await":
        reap_done = False

        async def unblock() -> None:
            await _spin()
            assert not reap_done
            assert client.delete_gate is not None
            client.delete_gate.set()

        unblocker = asyncio.create_task(unblock())
        client.delete_gate = asyncio.Event()
        acquire.cancel()
        await driver.reap_async("run-1")  # enters before acquire's cancel handler runs
        reap_done = True
        await unblocker
        with pytest.raises(asyncio.CancelledError):
            await acquire
    else:
        client.live.add("sb-old")
        driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
        client.delete_gates["sb-old"] = asyncio.Event()
        reap = asyncio.create_task(driver.reap_async("run-1"))
        await _spin()  # reap paused deleting sb-old
        client.delete_gate = asyncio.Event()
        acquire.cancel()
        with pytest.raises(asyncio.CancelledError):
            await acquire
        # Finish reap's own delete of sb-old; the background delete is still gated.
        client.delete_gates.pop("sb-old").set()
        await _spin()
        assert not reap.done()
        client.delete_gate.set()
        await reap

    # Immediately upon reap returning (with no extra spin), nothing is alive.
    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("when", ["before_reap_no_await", "during_reap"])
async def test_reap_waits_for_background_deletes_from_cancelled_warm_pool(when: str) -> None:
    client = SlowCreatesClient(fast=2)
    driver, _ = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # two actors created, third create() paused
    assert len(client.live) == 2

    if when == "before_reap_no_await":
        reap_done = False

        async def unblock() -> None:
            await _spin()
            assert not reap_done
            assert client.delete_gate is not None
            client.delete_gate.set()

        unblocker = asyncio.create_task(unblock())
        client.delete_gate = asyncio.Event()
        warm.cancel()
        await driver.reap_async("run-1")  # enters before warm's cancel handler runs
        reap_done = True
        await unblocker
        with pytest.raises(asyncio.CancelledError):
            await warm
    else:
        client.live.add("sb-old")
        driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
        client.delete_gates["sb-old"] = asyncio.Event()
        reap = asyncio.create_task(driver.reap_async("run-1"))
        await _spin()  # reap paused deleting sb-old
        client.delete_gate = asyncio.Event()
        warm.cancel()
        with pytest.raises(asyncio.CancelledError):
            await warm
        client.delete_gates.pop("sb-old").set()
        await _spin()
        assert not reap.done()
        client.delete_gate.set()
        await reap

    assert client.live == set()
    assert driver._owned_actors == {}


async def _start_gated_acquire(driver: SubstrateBackendDriver) -> asyncio.Task[object]:
    """On the driver's loop: an acquire paused in create(), its actor live."""
    acquire_gate.set(AcquireGate(lands_first=True))
    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()
    return acquire


# The sync facade: reap() and close() run on the driver's LoopRunner, and
# close() stops it. A background delete that a cancelled acquire starts must
# land before the loop stops, whether the cancel comes during the reap, between
# the reap and close(), or from close() stopping the loop.


def test_sync_reap_and_close_completes_background_delete_before_stopping_loop() -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-old")
    driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
    runner = driver._get_loop_runner()

    async def start() -> asyncio.Task[object]:
        client.delete_gates["sb-old"] = asyncio.Event()  # holds the reap's own delete
        client.delete_gate = asyncio.Event()  # holds the background delete
        return await _start_gated_acquire(driver)

    acquire = runner.run(start())
    assert len(client.live) == 2

    async def cancel_during_reap() -> None:
        while not driver._reaping:
            await asyncio.sleep(0)
        await _spin()  # the reap waits in its delete of sb-old
        acquire.cancel()
        await _spin()  # the cancel handler started the background delete, gated
        assert len(client.live) == 2
        client.delete_gates.pop("sb-old").set()
        await _spin()  # the reap's own delete landed; it now joins the background one
        assert driver._reaping == 1, "the reap returned without joining the background delete"
        client.delete_gate.set()

    helper = runner.submit(cancel_during_reap())
    assert driver.reap("run-1") == 1
    helper.result(timeout=1.0)
    driver.close()

    assert client.live == set()
    assert driver._owned_actors == {}


def test_sync_close_joins_background_delete_from_acquire_cancelled_after_reap() -> None:
    # The reap's join saw no delete: the acquire was still in create(). The
    # cancel comes between the reap and close(), so its delete starts then;
    # close() must wait for it before stopping the loop.
    client = GatedClient()
    driver, _ = _driver(client)
    runner = driver._get_loop_runner()
    joining = threading.Event()

    acquire = runner.run(_start_gated_acquire(driver))
    assert len(client.live) == 1
    assert driver.reap("run-1") == 0  # the snapshot has no actor of the acquire

    async def cancel_acquire() -> None:
        client.delete_gate = asyncio.Event()
        acquire.cancel()
        await _spin()  # the cancel handler started the delete; it waits at the gate
        assert len(client.live) == 1
        assert [aid for t, aid in driver._background_deletes.items() if not t.done()]
        join = driver._wait_background_deletes

        async def signalled_join() -> tuple[list[str], list[str]]:
            joining.set()
            return await join()

        driver._wait_background_deletes = signalled_join  # type: ignore[method-assign]

    def open_gate() -> None:
        # Open the gate only once close() joins the delete, so the test cannot
        # pass on a close() that does not wait. From its own thread: close()
        # cancels the other tasks on the driver's loop.
        assert joining.wait(timeout=5.0)
        assert client.delete_gate is not None
        runner.loop.call_soon_threadsafe(client.delete_gate.set)

    runner.run(cancel_acquire())
    opener = threading.Thread(target=open_gate)
    opener.start()
    driver.close()
    opener.join(timeout=5.0)

    assert not opener.is_alive()
    assert client.live == set()
    assert driver._owned_actors == {}


def test_sync_close_cancels_an_acquire_in_create_after_reap_and_deletes_its_actor() -> None:
    # close() with the acquire still in create() after the run's reap: close()
    # cancels it on the driver's loop, the cancel handler starts the delete,
    # and close() joins that delete before it closes the clients and stops the
    # loop, however long the delete takes after the handler's step.
    class DelayedDeleteClient(GatedClient):
        async def delete(self, actor_id: str, **kw: object) -> None:
            await asyncio.sleep(0.02)
            await super().delete(actor_id, **kw)

    client = DelayedDeleteClient()
    driver, _ = _driver(client)
    runner = driver._get_loop_runner()

    acquire = runner.run(_start_gated_acquire(driver))
    assert len(client.live) == 1
    assert driver.reap("run-1") == 0
    driver.close()

    assert acquire.cancelled()
    assert client.live == set()
    assert driver._owned_actors == {}


class _GatedEnvironments(FakeEnvironmentService):
    """CreateEnvironment registers the environment, then holds its reply until
    the call is cancelled (the first ``pass_creates`` creates reply at once);
    DeleteEnvironment records the id it removes. ``hold_gets`` and
    ``hold_suspends`` hold every GetEnvironment and SuspendEnvironment, and
    ``hold_deletes`` the next that many DeleteEnvironment calls, before they
    do anything, until the call is cancelled; ``held`` is set when one waits.
    ``delete_delay`` gives the seconds a DeleteEnvironment that is not held
    takes, by environment id; ``delete_calls`` counts every DeleteEnvironment
    that reached the server."""

    def __init__(self) -> None:
        super().__init__()
        self.pass_creates = 0
        self.creates = 0
        self.create_started = asyncio.Event()  # a held create reached the server
        self.create_gate = asyncio.Event()
        self.deleted: list[str] = []
        self.delete_calls = 0  # DeleteEnvironment calls that reached the server
        self.hold_gets = False
        self.hold_suspends = False
        self.hold_deletes = 0
        self.delete_delay: Callable[[str], float] | None = None
        self.held = asyncio.Event()  # a held get, suspend or delete reached the server
        self._never = asyncio.Event()

    async def _hold(self) -> None:
        self.held.set()
        await self._never.wait()

    async def CreateEnvironment(self, request, context):  # type: ignore[override]
        response = await super().CreateEnvironment(request, context)
        n, self.creates = self.creates, self.creates + 1
        if n < self.pass_creates:
            return response
        self.create_started.set()
        await self.create_gate.wait()
        return response

    async def GetEnvironment(self, request, context):  # type: ignore[override]
        if self.hold_gets:
            await self._hold()
        return await super().GetEnvironment(request, context)

    async def SuspendEnvironment(self, request, context):  # type: ignore[override]
        if self.hold_suspends:
            await self._hold()
        return await super().SuspendEnvironment(request, context)

    async def DeleteEnvironment(self, request, context):  # type: ignore[override]
        self.delete_calls += 1
        if self.hold_deletes > 0:
            self.hold_deletes -= 1
            await self._hold()
        if self.delete_delay is not None:
            await asyncio.sleep(self.delete_delay(request.id))
        self.deleted.append(request.id)
        return await super().DeleteEnvironment(request, context)


@pytest.fixture
async def gated_server():
    server = grpc.aio.server()
    envs = _GatedEnvironments()
    env_pb2_grpc.add_EnvironmentServiceServicer_to_server(envs, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        yield f"127.0.0.1:{port}", envs
    finally:
        await server.stop(None)


# The driver owns its channel (no client passed), as a fleet's does. Closing
# the channel cancels a create() still in flight, and the cancel handler's
# delete needs an open channel: close() and close_async() must let that delete
# land before the channels close and the loop stops.


async def test_sync_close_deletes_the_actor_of_an_acquire_in_create_on_an_owned_channel(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    # The sync facade: the acquire runs on the driver's own LoopRunner, which
    # close() quiesces before it closes the channel. The server runs on this
    # test's loop, so reap() and close() run in a thread.
    endpoint, envs = gated_server
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()

    acquire = runner.submit(driver.acquire_async("tmpl", "run-1"))
    await asyncio.wait_for(envs.create_started.wait(), timeout=5.0)
    (actor_id,) = [key[1] for key in envs.environments]
    assert await asyncio.to_thread(driver.reap, "run-1") == 0  # the snapshot has no actor of it

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        acquire.result(timeout=1.0)
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


async def test_close_async_deletes_the_actor_of_an_acquire_in_create_on_an_owned_channel(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    # On the caller's loop the acquire is the caller's task, which close_async
    # does not cancel; closing the channel cancels its create(). The cancel
    # handler's delete must still land, and no channel may stay open.
    endpoint, envs = gated_server
    driver = SubstrateBackendDriver(api_endpoint=endpoint)

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await asyncio.wait_for(envs.create_started.wait(), timeout=5.0)
    (actor_id,) = [key[1] for key in envs.environments]
    assert await driver.reap_async("run-1") == 0

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    assert acquire.cancelled()
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []


# close() and close_async() with no reap since the acquire started: a worker
# thread's acquire() after reap() returned and before close() starts, or a
# fleet whose setup failed before any reap. No reap follows, so close itself
# must count as the teardown the cancel handler looks for before it starts
# the delete (_may_pool).


@pytest.mark.parametrize("reap", ["before_acquire", "none"])
async def test_sync_close_deletes_the_actor_of_an_acquire_in_create_with_no_reap_since_it_started(
    reap: str, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()
    if reap == "before_acquire":
        assert await asyncio.to_thread(driver.reap, "run-1") == 0

    acquire = runner.submit(driver.acquire_async("tmpl", "run-1"))
    await asyncio.wait_for(envs.create_started.wait(), timeout=5.0)
    (actor_id,) = [key[1] for key in envs.environments]

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        acquire.result(timeout=1.0)
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


@pytest.mark.parametrize("reap", ["before_acquire", "none"])
async def test_close_async_deletes_the_actor_of_an_acquire_in_create_with_no_reap_since_it_started(
    reap: str, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    if reap == "before_acquire":
        assert await driver.reap_async("run-1") == 0

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await asyncio.wait_for(envs.create_started.wait(), timeout=5.0)
    (actor_id,) = [key[1] for key in envs.environments]

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    assert acquire.cancelled()
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []


# A fleet's exit (reap, then close) while a warm_pool call has one actor made
# and one CreateEnvironment in flight. Closing the channel cancels that create
# inside the call, not the call itself: the call must still own both actors
# and delete them before the channels close.


# With 18 replicas, 16 creates are held and one call waits on the call's
# semaphore with no create sent: it must not send one once close has started.
@pytest.mark.parametrize("replicas", [2, 18])
async def test_sync_close_during_a_warm_pool_on_an_owned_channel_deletes_its_actors(
    replicas: int, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    envs.pass_creates = 1  # the first create replies; the others are held
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()

    warm = runner.submit(driver.warm_pool_async("tmpl", replicas, run_id="run-1"))
    await _wait_until(lambda: envs.creates == min(replicas, 17))
    actor_ids = sorted(key[1] for key in envs.environments)
    assert len(actor_ids) == min(replicas, 17)
    assert await asyncio.to_thread(driver.reap, "run-1") == 0  # neither is owned yet

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        warm.result(timeout=1.0)
    assert sorted(envs.deleted) == actor_ids
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


@pytest.mark.parametrize("replicas", [2, 18])
async def test_close_async_during_a_warm_pool_on_an_owned_channel_deletes_its_actors(
    replicas: int, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    # On the caller's loop the warm_pool call is the caller's task, which
    # close_async does not cancel; closing the channel cancels the creates
    # held inside it. With 18 replicas that frees the semaphore for the call
    # that waited on it, which is not cancelled: it must not dial a new
    # channel and create an actor there.
    endpoint, envs = gated_server
    envs.pass_creates = 1
    driver = SubstrateBackendDriver(api_endpoint=endpoint)

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", replicas, run_id="run-1"))
    await _wait_until(lambda: envs.creates == min(replicas, 17))
    actor_ids = sorted(key[1] for key in envs.environments)
    assert len(actor_ids) == min(replicas, 17)
    assert await driver.reap_async("run-1") == 0

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    # All of it when close_async returns, with nothing left to wait for.
    assert warm.cancelled()
    assert envs.creates == min(replicas, 17)
    assert sorted(envs.deleted) == actor_ids
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._background_deletes == {}
    assert driver._client is None and driver._extra_clients == []


# A release, or the delete of a warm_pool call that finished after the reap,
# still in flight when the fleet exits (reap, then close). The release popped
# its actor, so the reap could not see it; close cancels the call, and nothing
# else deletes the actor: the cancel handler must start the delete, and close
# must join it before the channels close.


@pytest.mark.parametrize("recycle", [True, False])
async def test_sync_close_during_a_release_on_an_owned_channel_deletes_its_actor(
    recycle: bool, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    envs.pass_creates = 1
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()
    inst = await asyncio.to_thread(driver.acquire, "tmpl", "run-1")
    envs.hold_suspends, envs.hold_deletes = recycle, 0 if recycle else 1

    release = runner.submit(driver.release_async(inst.instance_id, recycle=recycle))
    await asyncio.wait_for(envs.held.wait(), timeout=5.0)
    assert await asyncio.to_thread(driver.reap, "run-1") == 0  # the release popped it

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        release.result(timeout=1.0)
    assert envs.deleted == [inst.instance_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


@pytest.mark.parametrize("recycle", [True, False])
async def test_close_async_during_a_release_on_an_owned_channel_deletes_its_actor(
    recycle: bool, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    envs.pass_creates = 1
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    inst = await driver.acquire_async("tmpl", "run-1")
    envs.hold_suspends, envs.hold_deletes = recycle, 0 if recycle else 1

    release = asyncio.create_task(driver.release_async(inst.instance_id, recycle=recycle))
    await asyncio.wait_for(envs.held.wait(), timeout=5.0)
    assert await driver.reap_async("run-1") == 0  # the release popped it

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    assert release.cancelled()
    assert envs.deleted == [inst.instance_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []


async def test_sync_close_during_a_warm_pool_delete_after_teardown_on_an_owned_channel_deletes_its_actor(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    endpoint, envs = gated_server
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()

    warm = runner.submit(driver.warm_pool_async("tmpl", 1, run_id="run-1"))
    await asyncio.wait_for(envs.create_started.wait(), timeout=5.0)
    (actor_id,) = [key[1] for key in envs.environments]
    assert await asyncio.to_thread(driver.reap, "run-1") == 0  # not owned yet
    envs.hold_deletes = 1
    envs.create_gate.set()  # the call finishes after the reap and deletes its actor
    await asyncio.wait_for(envs.held.wait(), timeout=5.0)

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        warm.result(timeout=1.0)
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


async def test_close_async_during_a_warm_pool_delete_after_teardown_on_an_owned_channel_deletes_its_actor(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    endpoint, envs = gated_server
    driver = SubstrateBackendDriver(api_endpoint=endpoint)

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", 1, run_id="run-1"))
    await asyncio.wait_for(envs.create_started.wait(), timeout=5.0)
    (actor_id,) = [key[1] for key in envs.environments]
    assert await driver.reap_async("run-1") == 0  # not owned yet
    envs.hold_deletes = 1
    envs.create_gate.set()  # the call finishes after the reap and deletes its actor
    await asyncio.wait_for(envs.held.wait(), timeout=5.0)

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    assert warm.cancelled()
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []


# A reap still in flight when close starts: a teardown() on another thread
# while this one closes the driver. close cancels the reap on its own runner,
# or closes the channel under its deletes; the actors are still owned, and no
# reap follows: the cancelled reap must start their deletes, and close must
# join them before the channels close. With 18 actors, 2 wait on the reap's
# semaphore and never sent a delete.


async def _wait_until(done: Callable[[], bool]) -> None:
    async def poll() -> None:
        while not done():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(poll(), timeout=5.0)


@pytest.mark.parametrize("actors", [1, 18])
async def test_sync_close_during_a_reap_on_an_owned_channel_deletes_its_actors(
    actors: int, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    envs.pass_creates = actors
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()
    actor_ids = sorted(
        [
            (await asyncio.to_thread(driver.acquire, "tmpl", "run-1")).instance_id
            for _ in range(actors)
        ]
    )
    envs.hold_deletes = min(actors, 16)  # every delete the reap sends is held

    reap = runner.submit(driver.reap_async("run-1"))
    await _wait_until(lambda: envs.hold_deletes == 0)

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        reap.result(timeout=1.0)
    assert sorted(envs.deleted) == actor_ids
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


@pytest.mark.parametrize("actors", [1, 18])
async def test_close_async_during_a_reap_on_an_owned_channel_deletes_its_actors(
    actors: int, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    # On the caller's loop the reap is the caller's task, which close_async
    # does not cancel; closing the channel cancels the deletes held inside it.
    endpoint, envs = gated_server
    envs.pass_creates = actors
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    actor_ids = sorted(
        [(await driver.acquire_async("tmpl", "run-1")).instance_id for _ in range(actors)]
    )
    envs.hold_deletes = min(actors, 16)

    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _wait_until(lambda: envs.hold_deletes == 0)

    await asyncio.wait_for(driver.close_async(), timeout=5.0)
    await asyncio.wait({reap}, timeout=5.0)

    assert reap.done()
    assert sorted(envs.deleted) == actor_ids
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []


async def test_close_async_during_a_reap_joins_the_deletes_of_the_actors_on_its_semaphore(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    # 16 of the reap's deletes are held on the server and 2 actors wait on its
    # semaphore. Closing the channel cancels the 16 and frees the semaphore
    # for the 2, which are not cancelled. Their deletes are the slow ones
    # (30 ms, against 2 ms for the others): close_async must still wait for
    # them, and must not leave them to a channel it opens and never closes.
    endpoint, envs = gated_server
    envs.pass_creates = 18
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    actor_ids = [(await driver.acquire_async("tmpl", "run-1")).instance_id for _ in range(18)]
    queued = set(actor_ids[16:])
    envs.hold_deletes = 16
    envs.delete_delay = lambda aid: 0.03 if aid in queued else 0.002

    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _wait_until(lambda: envs.hold_deletes == 0)

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    # When close_async returns, before the reap is awaited.
    assert envs.environments == {}
    assert sorted(envs.deleted) == sorted(actor_ids)
    # The 16 held deletes and one background delete per actor: the 2 that
    # waited sent no delete of their own on a channel dialed for them.
    assert envs.delete_calls == 16 + 18
    assert driver._owned_actors == {}
    assert driver._background_deletes == {}
    assert driver._client is None and driver._extra_clients == []
    await asyncio.wait({reap}, timeout=5.0)
    assert reap.done()
    assert driver._client is None and driver._extra_clients == []


async def test_close_async_joins_the_delete_of_a_call_cut_off_on_a_channel_it_redialed(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    # A caller's task that close_async does not cancel: its first release is
    # cut off by the channel close, and it goes on to a second release, which
    # dials a new channel. close_async closes that channel too, under the
    # second release's delete (30 ms; the background delete it joined first
    # takes 2 ms): the delete that release then starts must be joined as
    # well, and the channel it dials closed.
    endpoint, envs = gated_server
    envs.pass_creates = 2
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    first = (await driver.acquire_async("tmpl", "run-1")).instance_id
    second = (await driver.acquire_async("tmpl", "run-1")).instance_id
    envs.hold_deletes = 1
    envs.delete_delay = lambda aid: 0.03 if aid == second else 0.002

    async def caller() -> None:
        try:
            await driver.release_async(first)
        except asyncio.CancelledError:
            pass
        await driver.release_async(second)

    task = asyncio.create_task(caller())
    await asyncio.wait_for(envs.held.wait(), timeout=5.0)

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    assert envs.environments == {}
    assert sorted(envs.deleted) == sorted([first, second])
    assert driver._owned_actors == {}
    assert driver._background_deletes == {}
    assert driver._client is None and driver._extra_clients == []
    await asyncio.wait({task}, timeout=5.0)
    assert task.done()


def test_close_cancelling_a_reap_before_its_deletes_start_deletes_its_actors() -> None:
    # close()'s shutdown runs in the loop iteration after the reap took its
    # snapshot: it cancels the reap's delete tasks before their first step,
    # so none of them runs a cancel handler. The reap's own handler must
    # start the deletes.
    client = GatedClient()
    driver, _ = _driver(client)
    for aid in ("sb-1", "sb-2"):
        client.live.add(aid)
        driver._owned_actors[aid] = driver._make_instance(aid, "tmpl", run_id="run-1")
    runner = driver._get_loop_runner()

    async def reap_then_shutdown() -> tuple[asyncio.Task[int], asyncio.Task[None]]:
        # Both start in this order on the next iteration, with no await between.
        reap = asyncio.create_task(driver.reap_async("run-1"))
        shutdown = asyncio.create_task(driver._shutdown_async(cancel_calls=True))
        return reap, shutdown

    reap, shutdown = runner.run(reap_then_shutdown())

    async def join() -> None:
        await asyncio.wait({reap, shutdown}, timeout=5.0)

    runner.run(join())
    assert shutdown.done() and reap.cancelled()
    driver.close()

    assert client.live == set()
    assert driver._owned_actors == {}


# close during an unwarm_pool or a shrinking resize_pool of 18 pooled actors:
# 16 deletes are held on the server and 2 actors wait on the drain's
# semaphore. The actors left the pool and no reap follows a close: close()
# cancels the drain, close_async() closes the channel under its deletes, and
# each of the 18 must get a delete that the close joins.


@pytest.mark.parametrize("shrink", ["unwarm", "resize"])
async def test_sync_close_during_a_drain_on_an_owned_channel_deletes_its_actors(
    shrink: str, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    envs.pass_creates = 18
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()
    await asyncio.to_thread(driver.warm_pool, "tmpl", 18, True, "run-1")
    actor_ids = sorted(key[1] for key in envs.environments)
    assert len(actor_ids) == 18
    envs.hold_deletes = 16

    if shrink == "unwarm":
        drain = runner.submit(driver.unwarm_pool_async("tmpl"))
    else:
        drain = runner.submit(driver.resize_pool_async("tmpl", 0))
    await _wait_until(lambda: envs.hold_deletes == 0)

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        drain.result(timeout=1.0)
    assert sorted(envs.deleted) == actor_ids
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


@pytest.mark.parametrize("shrink", ["unwarm", "resize"])
async def test_close_async_during_a_drain_on_an_owned_channel_deletes_its_actors(
    shrink: str, gated_server: tuple[str, _GatedEnvironments]
) -> None:
    endpoint, envs = gated_server
    envs.pass_creates = 18
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    await driver.warm_pool_async("tmpl", 18, run_id="run-1")
    actor_ids = sorted(key[1] for key in envs.environments)
    assert len(actor_ids) == 18
    envs.hold_deletes = 16
    queued = set(_pool(driver)[16:])
    envs.delete_delay = lambda aid: 0.03 if aid in queued else 0.002

    if shrink == "unwarm":
        drain = asyncio.create_task(driver.unwarm_pool_async("tmpl"))
    else:
        drain = asyncio.create_task(driver.resize_pool_async("tmpl", 0))
    await _wait_until(lambda: envs.hold_deletes == 0)

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    # When close_async returns, before the drain is awaited.
    assert sorted(envs.deleted) == actor_ids
    assert envs.delete_calls == 16 + 18  # the 2 that waited sent none of their own
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._background_deletes == {}
    assert driver._client is None and driver._extra_clients == []
    await asyncio.wait({drain}, timeout=5.0)
    assert drain.done()
    assert driver._client is None and driver._extra_clients == []


def _owned_runner_shutdown_after(
    driver: SubstrateBackendDriver, call: Callable[[], Coroutine[Any, Any, object]], yields: int
) -> None:
    """Start ``call`` on the driver's own runner, and close()'s shutdown
    ``yields`` loop iterations later; wait for both."""
    runner = driver._get_loop_runner()

    async def start() -> tuple[asyncio.Task[object], asyncio.Task[None]]:
        task = asyncio.create_task(call())
        for _ in range(yields):
            await asyncio.sleep(0)
        shutdown = asyncio.create_task(driver._shutdown_async(cancel_calls=True))
        return task, shutdown

    task, shutdown = runner.run(start())

    async def join() -> None:
        await asyncio.wait({task, shutdown}, timeout=5.0)

    runner.run(join())
    assert shutdown.done() and task.done()
    driver.close()


# close()'s shutdown landing at each loop iteration of a call that deletes
# unpooled actors through a gather: at one of them the delete tasks exist and
# none has taken a step, so none runs a cancel handler, and the call's own
# handler must start the deletes.


# The window is 11 iterations in (the reap inside create() takes most of
# them); the range leaves room for it to move.
@pytest.mark.parametrize("yields", range(8, 16))
def test_close_cancelling_a_warm_pool_before_its_deletes_after_teardown_start_deletes_its_actors(
    yields: int,
) -> None:
    # warm_pool made its actors across a reap, so it deletes them instead of
    # pooling them.
    client = GatedClient()
    driver, _ = _driver(client)
    driver.reap("run-1")
    reaped = False

    async def reap_once(actor_id: str, **kw: object) -> None:
        # The run's reap, inside the first create(): the call sees a teardown.
        nonlocal reaped
        if not reaped:
            reaped = True
            await driver.reap_async("run-1")
        client.live.add(actor_id)

    client.create = reap_once  # type: ignore[method-assign]

    _owned_runner_shutdown_after(
        driver, lambda: driver.warm_pool_async("tmpl", 2, run_id="run-1"), yields
    )

    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("yields", range(4))  # the window is the first
@pytest.mark.parametrize("shrink", ["unwarm", "resize"])
def test_close_cancelling_a_drain_before_its_deletes_start_deletes_its_actors(
    shrink: str, yields: int
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    driver.warm_pool("tmpl", 2, run_id="run-1")
    assert len(client.live) == 2

    if shrink == "unwarm":
        _owned_runner_shutdown_after(driver, lambda: driver.unwarm_pool_async("tmpl"), yields)
    else:
        _owned_runner_shutdown_after(driver, lambda: driver.resize_pool_async("tmpl", 0), yields)

    assert client.live == set()
    assert driver._owned_actors == {}


# A warm claim cancelled in get(): the acquire popped the actor from the pool
# and no caller will hold it. With no teardown since the pop it goes back to
# the pool; after one it is deleted, because it may carry the run id of an
# earlier run (a recycled actor), which no later reap matches (AC-15.7).


@pytest.mark.parametrize("cancel", ["timeout", "caller"])
async def test_warm_claim_cancelled_in_get_returns_the_actor_to_the_pool(cancel: str) -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    await driver.warm_pool_async("tmpl", 1)
    (aid,) = _pool(driver)
    client.get_gate = asyncio.Event()

    timeout_s = 0.05 if cancel == "timeout" else 180.0
    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1", timeout_s=timeout_s))
    await _spin()  # paused in get()
    assert _pool(driver) == [] and sum(obs._warm.values()) == 0
    if cancel == "timeout":
        with pytest.raises(SandboxStartError):
            await acquire
    else:
        acquire.cancel()
        with pytest.raises(asyncio.CancelledError):
            await acquire

    assert _pool(driver) == [aid]
    assert sum(obs._warm.values()) == 1
    assert client.live == {aid}
    assert driver._owned_actors[aid].run_id == WARM_POOL_RUN_ID
    assert not driver._background_deletes
    client.get_gate = None
    assert (await driver.acquire_async("tmpl", "run-1")).instance_id == aid


@pytest.mark.parametrize("when", ["during_reap", "after_reap"])
async def test_warm_claim_of_an_earlier_runs_actor_cancelled_in_get_across_a_reap_deletes_it(
    when: str,
) -> None:
    # run-0 recycled sb-0 into the pool; run-1's acquire claims it and pauses
    # in get(). run-1's reap does not match it (run-0, and not pooled), and
    # neither does any later run's: the cancelled claim must delete it.
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.update({"sb-0", "sb-1"})
    driver._owned_actors["sb-0"] = driver._make_instance("sb-0", "tmpl", run_id="run-0")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    await driver.release_async("sb-0", recycle=True)
    client.get_gate = asyncio.Event()
    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # paused in get()
    assert _pool(driver) == []

    if when == "during_reap":
        client.delete_gates["sb-1"] = asyncio.Event()  # holds the reap's own delete
        client.delete_gates["sb-0"] = asyncio.Event()  # holds the background delete
        reap = asyncio.create_task(driver.reap_async("run-1"))
        await _spin()
        acquire.cancel()
        await _spin()  # the cancel handler started the background delete, gated
        client.delete_gates.pop("sb-1").set()
        await _spin()
        assert not reap.done(), "the reap returned without joining the background delete"
        client.delete_gates.pop("sb-0").set()
        assert await reap == 1
    else:
        assert await driver.reap_async("run-1") == 1
        assert client.live == {"sb-0"}
        acquire.cancel()
        await _spin()
    with pytest.raises(asyncio.CancelledError):
        await acquire

    assert client.live == set()
    assert driver._owned_actors == {}
    assert _pool(driver) == [] and sum(obs._warm.values()) == 0
    assert await driver.reap_async("run-2") == 0


async def test_sync_close_deletes_the_warm_actor_of_an_acquire_in_get_on_an_owned_channel(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    # The pooled actor carries run-0 (warm_pool(run_id=...) or a recycle), so
    # run-1's reap does not match it once the acquire popped it from the pool.
    endpoint, envs = gated_server
    envs.pass_creates = 1
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    runner = driver._get_loop_runner()
    await asyncio.to_thread(driver.warm_pool, "tmpl", 1, True, "run-0")
    (actor_id,) = [key[1] for key in envs.environments]
    envs.hold_gets = True

    acquire = runner.submit(driver.acquire_async("tmpl", "run-1"))
    await asyncio.wait_for(envs.held.wait(), timeout=5.0)
    assert await asyncio.to_thread(driver.reap, "run-1") == 0

    await asyncio.wait_for(asyncio.to_thread(driver.close), timeout=5.0)

    with pytest.raises(concurrent.futures.CancelledError):
        acquire.result(timeout=1.0)
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []
    assert driver._loop_runner is None


async def test_close_async_deletes_the_warm_actor_of_an_acquire_in_get_on_an_owned_channel(
    gated_server: tuple[str, _GatedEnvironments],
) -> None:
    endpoint, envs = gated_server
    envs.pass_creates = 1
    driver = SubstrateBackendDriver(api_endpoint=endpoint)
    await driver.warm_pool_async("tmpl", 1, run_id="run-0")
    (actor_id,) = [key[1] for key in envs.environments]
    envs.hold_gets = True

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await asyncio.wait_for(envs.held.wait(), timeout=5.0)
    assert await driver.reap_async("run-1") == 0

    await asyncio.wait_for(driver.close_async(), timeout=5.0)

    assert acquire.cancelled()
    assert envs.deleted == [actor_id]
    assert envs.environments == {}
    assert driver._owned_actors == {}
    assert driver._client is None and driver._extra_clients == []


async def test_reap_bounds_wait_on_hung_background_delete_without_cancelling_it() -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    driver.background_delete_timeout_s = 0.05
    client.live.add("sb-old")
    driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
    client.delete_gates["sb-old"] = asyncio.Event()
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()
    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # reap snapshotted only sb-old
    client.delete_gate = asyncio.Event()
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    client.delete_gates.pop("sb-old").set()
    await _spin()
    assert not reap.done()  # waiting on the hung background delete up to background_delete_timeout_s

    # At the deadline the reap names the actor still deleting, as it names an
    # actor whose own delete failed, so the caller can sweep it. The background
    # delete is not cancelled, and the actor stays tracked under
    # WARM_POOL_RUN_ID until the delete finishes.
    (late_id,) = {aid for aid in client.live if aid != "sb-old"}
    with pytest.raises(InfrastructureError, match="Failed to delete 1 environments") as err:
        await asyncio.wait_for(reap, timeout=1.0)
    assert late_id in str(err.value)
    assert "1 still deleting in the background" in str(err.value)
    pending = [t for t in driver._background_tasks if not t.done()]
    assert len(pending) == 1 and not pending[0].cancelled()
    assert {i.run_id for i in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}

    client.delete_gate.set()
    await _spin()
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_reap_names_the_actor_of_a_background_delete_that_fails_during_it() -> None:
    # The reap's snapshot holds only sb-old. The acquire cancelled during the
    # reap starts a background delete that fails fast (a 503 from Substrate),
    # so its task is done before the join: the reap must count it as a failed
    # delete, not return as if the actor were gone.
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-old")
    driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
    client.delete_gates["sb-old"] = asyncio.Event()
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # paused in create(), the actor live
    (new_id,) = {aid for aid in client.live if aid != "sb-old"}
    client.delete_errors[new_id] = SandboxUnavailableError("unavailable", status="HTTP 503")
    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # reap paused deleting sb-old; its snapshot has no new_id
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    await _spin()  # the background delete of new_id failed; nothing is pending
    assert not [t for t in driver._background_deletes if not t.done()]
    assert not reap.done()
    client.delete_gates.pop("sb-old").set()

    with pytest.raises(InfrastructureError, match="Failed to delete 1 environments") as err:
        await asyncio.wait_for(reap, timeout=1.0)
    assert new_id in str(err.value)
    assert "1 background delete(s) failed" in str(err.value)
    assert client.live == {new_id}
    assert driver._owned_actors[new_id].run_id == WARM_POOL_RUN_ID

    # Reported once: the next reap of any run deletes it, and a reap after
    # that has nothing to report.
    del client.delete_errors[new_id]
    assert await driver.reap_async("run-2") == 1
    assert client.live == set()
    assert driver._owned_actors == {}
    assert await driver.reap_async("run-3") == 0


async def test_reap_does_not_wait_for_the_background_delete_of_an_actor_it_deleted() -> None:
    # The acquire cancelled after run-1's reap starts a background delete that
    # hangs (a wedged stream). Its actor is owned under WARM_POOL_RUN_ID, so
    # run-2's reap deletes it itself, and its join must neither wait for the
    # hung delete of an actor that is gone nor name it.
    client = GatedClient()
    driver, _ = _driver(client)
    driver.background_delete_timeout_s = 0.2
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # paused in create(), the actor live
    (new_id,) = set(client.live)
    assert await driver.reap_async("run-1") == 0  # the snapshot has no actor of the acquire
    hung = client.delete_gates[new_id] = asyncio.Event()
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    await _spin()  # the background delete waits at the gate
    (task,) = [t for t in driver._background_deletes if not t.done()]
    assert driver._owned_actors[new_id].run_id == WARM_POOL_RUN_ID
    del client.delete_gates[new_id]  # the reap's own delete passes; the hung one still waits

    assert await asyncio.wait_for(driver.reap_async("run-2"), timeout=1.0) == 1
    assert client.live == set()
    assert driver._owned_actors == {}
    assert not task.done()  # not cancelled: it ends when its stream does

    hung.set()
    await _spin()
    assert task.done() and not task.cancelled()
    assert await driver.reap_async("run-3") == 0


async def test_reap_names_the_actor_of_a_background_delete_cancelled_before_its_first_step() -> None:
    # The acquire is cancelled during the reap, and the delete its cancel
    # handler starts is cancelled in the same step, before the task runs (a
    # task group's cleanup cancels every child at once). The coroutine never
    # enters _background_delete, so only the done callback can record the
    # actor: the reap must name it, as it names a delete that failed.
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-old")
    driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
    client.delete_gates["sb-old"] = asyncio.Event()
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # paused in create(), the actor live
    (new_id,) = {aid for aid in client.live if aid != "sb-old"}
    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # reap paused deleting sb-old; its snapshot has no new_id

    spawn = driver._spawn_background_delete
    cancelled: list[asyncio.Task[object]] = []

    def spawn_then_cancel(actor_id: str, why: str) -> None:
        spawn(actor_id, why)
        (task,) = [t for t, aid in driver._background_deletes.items() if aid == actor_id]
        task.cancel()  # before its first step: _background_delete never runs
        cancelled.append(task)

    driver._spawn_background_delete = spawn_then_cancel  # type: ignore[method-assign]
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    await _spin()
    assert [t.cancelled() for t in cancelled] == [True]
    assert not reap.done()
    client.delete_gates.pop("sb-old").set()

    with pytest.raises(InfrastructureError, match="Failed to delete 1 environments") as err:
        await asyncio.wait_for(reap, timeout=1.0)
    assert new_id in str(err.value)
    assert "1 background delete(s) failed" in str(err.value)
    assert client.live == {new_id}
    assert driver._owned_actors[new_id].run_id == WARM_POOL_RUN_ID

    driver._spawn_background_delete = spawn  # type: ignore[method-assign]
    assert await driver.reap_async("run-2") == 1
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_close_logs_the_actor_of_a_background_delete_that_fails_after_the_reap(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The reap's join saw no delete: the acquire was still in create(). Its
    # cancel after the reap starts a background delete that fails fast.
    # close_async() joins it, logs the actor and does not raise; the actor
    # stays owned under WARM_POOL_RUN_ID.
    client = GatedClient()
    driver, _ = _driver(client)
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()
    (new_id,) = set(client.live)
    assert await driver.reap_async("run-1") == 0
    client.delete_errors[new_id] = SandboxUnavailableError("unavailable", status="HTTP 503")
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    await _spin()  # the background delete failed

    caplog.set_level(logging.WARNING, logger="ate_env_rl.backend.substrate")
    caplog.clear()
    await driver.close_async()

    assert any(
        "background environment delete(s) failed" in r.getMessage() and new_id in r.getMessage()
        for r in caplog.records
    )
    assert client.live == {new_id}
    assert driver._owned_actors[new_id].run_id == WARM_POOL_RUN_ID


async def test_background_delete_timeout_zero_names_the_pending_delete_without_waiting() -> None:
    # 0 means no wait: the reap still lets the cancel handlers start their
    # deletes (the settle yields), then names every delete still pending, as it
    # names one still running at a later deadline. The delete is not cancelled.
    client = GatedClient()
    driver = SubstrateBackendDriver(client=client, background_delete_timeout_s=0)
    driver.observer = Observer(ObservabilityConfig(enable_metrics=False))
    client.live.add("sb-old")
    driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
    client.delete_gates["sb-old"] = asyncio.Event()
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()
    (new_id,) = {aid for aid in client.live if aid != "sb-old"}
    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # reap paused deleting sb-old; its snapshot has no new_id
    client.delete_gate = asyncio.Event()  # holds the background delete
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    client.delete_gates.pop("sb-old").set()
    await _spin()  # no wall-clock wait: the reap ends within these yields

    assert reap.done()
    with pytest.raises(InfrastructureError, match="Failed to delete 1 environments") as err:
        reap.result()
    assert new_id in str(err.value)
    assert "1 still deleting in the background" in str(err.value)
    pending = [t for t in driver._background_tasks if not t.done()]
    assert len(pending) == 1 and not pending[0].cancelled()
    assert driver._owned_actors[new_id].run_id == WARM_POOL_RUN_ID

    client.delete_gate.set()
    await _spin()
    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize(
    "value", [True, float("nan"), float("inf"), -float("inf"), -1, "10"], ids=repr
)
def test_background_delete_timeout_must_be_a_finite_number_of_seconds(value: object) -> None:
    # A bool is not a timeout; NaN never satisfies the join's deadline check,
    # so the join would never time out; a negative wait has no meaning.
    with pytest.raises(ValueError, match="background_delete_timeout_s"):
        SubstrateBackendDriver(client=GatedClient(), background_delete_timeout_s=value)  # type: ignore[arg-type]


async def test_direct_reap_moves_the_gauge_with_the_pool() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    await driver.warm_pool_async("tmpl", R)
    assert sum(obs._warm.values()) == R

    await driver.reap_async("run-1")  # no fleet, so no warm_reset()

    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_unwarm_cancels_a_warm_pool_in_flight() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.unwarm_pool_async("tmpl")
    client.create_gate.set()
    await warm

    assert _pool(driver) == []
    assert client.live == set()
    assert driver._owned_actors == {}
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_after_unwarm_ignores_the_cancelled_call() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    old = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.unwarm_pool_async("tmpl", rootfs_image=None)
    new = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(old, new)

    assert len(_pool(driver)) == R
    assert client.live == set(_pool(driver))
    assert sum(obs._warm.values()) == R


async def test_shrinking_resize_cancels_a_warm_pool_in_flight() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    resize = asyncio.create_task(driver.resize_pool_async("tmpl", 1))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(warm, resize)

    assert len(_pool(driver)) == 1
    assert client.live == set(_pool(driver))
    assert set(driver._owned_actors) == client.live
    assert sum(obs._warm.values()) == 1


@pytest.mark.parametrize("shrink", ["unwarm", "resize"])
async def test_failed_delete_of_drained_actors_keeps_them_for_the_next_reap(shrink: str) -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    failures: list[str] = []
    obs.release_failed = failures.append  # type: ignore[method-assign]
    await driver.warm_pool_async("tmpl", R, run_id="run-1")
    client.delete_failures = R

    if shrink == "unwarm":
        await driver.unwarm_pool_async("tmpl")
    else:
        await driver.resize_pool_async("tmpl", 0)

    assert failures == ["substrate"] * R
    assert len(client.live) == R
    assert set(driver._owned_actors) == client.live
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


# An unwarm_pool or shrinking resize_pool cancelled in its deletes. The actors
# left the pool, where any run's reap deleted them, and carry the run id
# warm_pool gave them: every one must still go with the next reap of any run,
# also the ones that waited on the drain's semaphore (18 actors: 16 in
# delete(), 2 waiting) or whose delete task never took a step.


@pytest.mark.parametrize("cancel", ["on_the_semaphore", "before_the_first_step"])
@pytest.mark.parametrize("shrink", ["unwarm", "resize"])
async def test_cancelled_drain_leaves_every_actor_for_the_next_reap_of_any_run(
    shrink: str, cancel: str
) -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    actors = 18 if cancel == "on_the_semaphore" else 2
    await driver.warm_pool_async("tmpl", actors, run_id="run-1")
    client.delete_gate = asyncio.Event()

    if shrink == "unwarm":
        drain = asyncio.create_task(driver.unwarm_pool_async("tmpl"))
    else:
        drain = asyncio.create_task(driver.resize_pool_async("tmpl", 0))
    if cancel == "on_the_semaphore":
        await _spin()  # 16 paused in delete(), 2 on the semaphore
    else:
        await asyncio.sleep(0)  # the drain made its delete tasks; none has run
    drain.cancel()
    await asyncio.gather(drain, return_exceptions=True)
    assert drain.cancelled()

    assert len(client.live) == actors
    assert {inst.run_id for inst in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}
    client.delete_gate = None
    assert await driver.reap_async("run-2") == actors
    assert client.live == set()
    assert driver._owned_actors == {}
    assert sum(obs._warm.values()) == 0


RUN_IDS = st.sampled_from(("run-0", "run-1"))


class WarmPoolMachine(RuleBasedStateMachine):
    owned = Bundle("owned")
    recycling = Bundle("recycling")

    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.client = GatedClient()
        self.driver, self.obs = _driver(self.client)
        self.ids = itertools.count()
        self.inflight: dict[str, asyncio.Task[None]] = {}
        self.warm_tasks: list[asyncio.Task[None]] = []
        self.reap: asyncio.Task[None] | None = None
        self.reap_gate: asyncio.Event | None = None
        self.claimed: set[str] = set()
        self.torn_down = False
        self.teardowns = 0  # start_teardown calls
        # Acquires in flight: task, gate, self.teardowns when each started,
        # and the run that acquires.
        self.acquires: dict[int, tuple[asyncio.Task[object], AcquireGate, int, str]] = {}
        self.acquired: list[str] = []  # returned, not yet in the owned bundle
        # The actors a caller holds, with the caller's run: handed out by an
        # acquire and not released since. The teardown is run-1's; run-0 is
        # another run on the same driver, whose held actors outlive it.
        self.holder: dict[str, str] = {}
        # Who held each actor now in a recycle, for a recycle that is cancelled.
        self.recycled_by: dict[str, str | None] = {}
        # Returned by an acquire that overlapped a teardown: the caller holds
        # it, and a later reap deletes it (AC-15.10), so it may outlive this one.
        self.late: set[str] = set()

    def teardown(self) -> None:
        try:
            # Not after a failed step (or Hypothesis ending the example early):
            # the step's own error must be the one reported.
            if sys.exc_info()[0] is None:
                self._end_run()
        finally:
            self._cancel_all()

    def _end_run(self) -> None:
        # The end of a run: callers give up on the acquires still in flight,
        # paused calls resume, and the fleet tears down if it has not. Then
        # nothing is left but what acquires handed out across the teardown.
        for task, _, _, _ in self.acquires.values():
            task.cancel()
        if self.client.create_gate is not None:
            self.client.create_gate.set()
        for gate in self.client.suspend_gates.values():
            gate.set()
        self._spin()
        if self._quiet():
            self.start_teardown()
        if self.reap is not None:
            self.finish_teardown()
        self._spin()
        assert not (self.inflight or self.warm_tasks or self.acquires), "work still paused"
        assert not self.driver._background_tasks, "deletes still running"
        self.nothing_survives_teardown()
        # AC-15.10: any environment handed out across run-1's teardown must be
        # deleted by a later run's reap even if the caller never releases it.
        # What run-0 still holds goes with run-0's own reap.
        if self.client.live:
            self.loop.run_until_complete(self.driver.reap_async("run-2"))
            left = self.client.live - self._held_by("run-0")
            assert not left, f"alive after next run's reap: {sorted(left)}"
            self.loop.run_until_complete(self.driver.reap_async("run-0"))
            assert not self.client.live, f"alive after run-0's reap: {sorted(self.client.live)}"
            assert not self.driver._owned_actors

    def _cancel_all(self) -> None:
        tasks = [*self.inflight.values(), *self.warm_tasks]
        tasks += [t for t, _, _, _ in self.acquires.values()]
        if self.reap is not None:
            tasks.append(self.reap)
        while tasks:
            for t in tasks:
                t.cancel()
            self.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            # Deletes a cancel started, which may start more when cancelled.
            tasks = list(self.driver._background_tasks)
        self.loop.close()

    def _spin(self) -> None:
        self.loop.run_until_complete(_spin())
        # Collect finished work; a task still waiting on a gate stays.
        for aid, task in list(self.inflight.items()):
            if task.done():
                task.result()
                del self.inflight[aid]
        for task in [t for t in self.warm_tasks if t.done()]:
            task.result()
            self.warm_tasks.remove(task)
        for n, (task, _, started_td, run_id) in list(self.acquires.items()):
            if not task.done():
                continue
            del self.acquires[n]
            if task.cancelled():
                continue
            inst = task.result()
            aid = inst.instance_id  # type: ignore[attr-defined]
            if inst.metadata["claim_source"] == "warm_pool":  # type: ignore[attr-defined]
                self.claimed.add(aid)
            self.holder[aid] = run_id
            # run-0 was not torn down: its actor is held, not late.
            if self.teardowns != started_td and run_id == "run-1":
                self.late.add(aid)
            self.acquired.append(aid)

    def _quiet(self) -> bool:
        return not self.torn_down and self.reap is None

    def _held_by(self, run_id: str) -> set[str]:
        return {aid for aid, run in self.holder.items() if run == run_id}

    async def _fleet_teardown(self) -> None:
        # What SandboxFleet.close and AsyncSandboxFleet.teardown do: reset the
        # gauge as soon as the reap returns, with no await in between.
        await self.driver.reap_async("run-1")
        # The reap joined every background delete whose actor is still owned.
        # One whose actor a later release popped (the caller released again
        # after a cancelled release) is that release's to finish.
        assert not [
            aid
            for t, aid in self.driver._background_deletes.items()
            if not t.done() and aid in self.driver._owned_actors
        ]
        self.obs.warm_reset()
        self.torn_down = True

    # One actor up front, so a teardown always waits on a delete and other work
    # can run inside it.
    @initialize(target=owned)
    def first_actor(self) -> str:
        return self.acquire_new(run_id="run-1")

    # Sometimes one pooled actor up front, so acquires also claim warm ones.
    @initialize(warm=st.booleans())
    def pool_first(self, warm: bool) -> None:
        if warm:
            self.loop.run_until_complete(self.driver.warm_pool_async("tmpl", 1))

    @precondition(lambda self: self._quiet())
    @rule(target=owned, run_id=RUN_IDS)
    def acquire_new(self, run_id: str) -> str:
        aid = f"sb-{next(self.ids)}"
        self.client.live.add(aid)
        self.driver._owned_actors[aid] = self.driver._make_instance(aid, "tmpl", run_id=run_id)
        self.holder[aid] = run_id
        return aid

    @precondition(lambda self: self._quiet() and _pool(self.driver))
    @rule(target=owned, run_id=RUN_IDS)
    def acquire_warm(self, run_id: str) -> str:
        inst = self.loop.run_until_complete(self.driver._acquire_internal("tmpl", run_id))
        self.claimed.add(inst.instance_id)
        self.holder[inst.instance_id] = run_id
        return inst.instance_id

    # An acquire through acquire_async, paused at its own gate: in get() for a
    # warm claim, in create() for a cold one, whose create may already have
    # landed on the server. It may finish or be cancelled at any later step,
    # including while a teardown runs; a warm claim let through pauses again
    # in the cold create it falls back to.
    @precondition(lambda self: self._quiet())
    @rule(
        lands_first=st.booleans(),
        check_live=st.booleans(),
        run_id=RUN_IDS,
        warm_first=st.booleans(),
    )
    def start_acquire(
        self, lands_first: bool, check_live: bool, run_id: str, warm_first: bool
    ) -> None:
        create_gate = self.client.create_gate
        if warm_first and not _pool(self.driver) and (create_gate is None or create_gate.is_set()):
            # One pooled actor first (unless creates are paused), so about
            # half the acquires are warm claims whatever the steps before
            # left in the pool.
            self.loop.run_until_complete(self.driver.warm_pool_async("tmpl", 1))
        gate = AcquireGate(lands_first=lands_first, check_live=check_live)

        async def acquire() -> object:
            acquire_gate.set(gate)
            return await self.driver.acquire_async("tmpl", run_id)

        task = self.loop.create_task(acquire())
        self.acquires[next(self.ids)] = (task, gate, self.teardowns, run_id)
        self._spin()  # paused in get() or create()

    # Lets the call the acquire waits in through: get(), or the create() a
    # warm claim fell back to, where the next step finishes or cancels it.
    @precondition(lambda self: self.acquires)
    @rule(target=owned, data=st.data())
    def finish_acquire(self, data: st.DataObject) -> object:
        n = data.draw(st.sampled_from(sorted(self.acquires)))
        gate = self.acquires[n][1]
        assert gate.waiting is not None, "the acquire is not in a gated call"
        gate.waiting.set()
        self._spin()
        done, self.acquired = self.acquired, []
        return multiple(*done)

    @precondition(lambda self: self.acquires)
    @rule(data=st.data())
    def cancel_acquire(self, data: st.DataObject) -> None:
        n = data.draw(st.sampled_from(sorted(self.acquires)))
        task, gate, _, _ = self.acquires[n]
        if gate.claiming is not None:
            # Cancelled in get(): the actor goes back to the pool, which a
            # warm_pool call may have filled meanwhile. As a recycled actor,
            # it is not one that warm_pool over-provisioned (P-11).
            self.claimed.add(gate.claiming)
        task.cancel()
        self._spin()

    # Recycles and warm_pool calls may start and finish while a teardown runs.
    @precondition(lambda self: not self.torn_down)
    @rule(target=recycling, aid=consumes(owned))
    def start_recycle(self, aid: str) -> str:
        if (
            aid not in self.driver._owned_actors
            or aid in self.inflight
            or aid in _pool(self.driver)
        ):
            return aid  # already reaped, released, recycling or back in the pool
        self.client.suspend_gates[aid] = asyncio.Event()
        self.recycled_by[aid] = self.holder.pop(aid, None)
        self.inflight[aid] = self.loop.create_task(self.driver.release_async(aid, recycle=True))
        self._spin()  # paused in suspend()
        return aid

    # A recycle cancelled in suspend(). With no teardown since it started, its
    # caller holds the actor again, under its run. During the teardown or
    # after it, the reap could not see the actor: the release deletes it in a
    # background task, which a reap still running joins (AC-15.9).
    @precondition(lambda self: self.inflight)
    @rule(data=st.data())
    def cancel_recycle(self, data: st.DataObject) -> None:
        aid = data.draw(st.sampled_from(sorted(self.inflight)))
        task = self.inflight.pop(aid)
        task.cancel()
        self.loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
        assert task.cancelled()
        run_id = self.recycled_by.pop(aid, None)
        if self._quiet() and run_id is not None:
            self.holder[aid] = run_id
        self._spin()

    @rule(target=owned, aid=consumes(recycling))
    def finish_recycle(self, aid: str) -> str:
        if aid in self.client.suspend_gates:
            self.client.suspend_gates[aid].set()
        self._spin()
        return aid

    @precondition(lambda self: not self.torn_down)
    @rule()
    def start_warm_pool(self) -> None:
        if self.client.create_gate is None or self.client.create_gate.is_set():
            self.client.create_gate = asyncio.Event()
        self.warm_tasks.append(self.loop.create_task(self.driver.warm_pool_async("tmpl", R)))
        self._spin()  # paused in create()

    @precondition(lambda self: self.warm_tasks)
    @rule()
    def finish_warm_pools(self) -> None:
        assert self.client.create_gate is not None
        self.client.create_gate.set()
        self._spin()

    # The teardown in two steps, so other work can run while the reap waits on
    # its deletes. The reap's own deletes (of the actors owned when it starts)
    # wait on reap_gate; a delete that other work starts during the teardown,
    # a cancelled acquire's background delete among them, waits on
    # delete_gate, which opens later, so the reap reaches its join with those
    # deletes still pending.
    @precondition(lambda self: self._quiet())
    @rule()
    def start_teardown(self) -> None:
        self.reap_gate = asyncio.Event()
        for aid in list(self.driver._owned_actors):
            self.client.delete_gates[aid] = self.reap_gate
        self.client.delete_gate = asyncio.Event()
        self.teardowns += 1
        self.reap = self.loop.create_task(self._fleet_teardown())
        self._spin()  # paused in the reap's delete() calls

    @precondition(lambda self: self.reap is not None)
    @rule()
    def finish_teardown(self) -> None:
        assert self.client.delete_gate is not None and self.reap is not None
        assert self.reap_gate is not None
        self.reap_gate.set()
        self._spin()  # the reap's own deletes landed; it joins the other deletes
        self.client.delete_gate.set()
        self.client.delete_gate = None
        self.loop.run_until_complete(self.reap)
        self.reap = None
        for aid in [a for a, g in self.client.delete_gates.items() if g is self.reap_gate]:
            del self.client.delete_gates[aid]
        self.reap_gate = None
        self._spin()

    # unwarm_pool while warm_pool calls may be paused in create().
    @precondition(lambda self: self._quiet())
    @rule()
    def unwarm(self) -> None:
        self.loop.run_until_complete(self.driver.unwarm_pool_async("tmpl"))
        self._spin()

    # Only with work in flight, or after the teardown, where no other rule
    # may be left: before it, with nothing in flight, the step changes nothing
    # and takes the place of one that could start, finish or cancel an acquire.
    @precondition(
        lambda self: (
            self.torn_down
            or self.acquires
            or self.inflight
            or self.warm_tasks
            or self.reap is not None
        )
    )
    @rule()
    def idle(self) -> None:
        self._spin()

    @invariant()
    def gauge_equals_pool(self) -> None:  # P-9
        if self.reap is not None and not self.reap.done():
            return  # the fleet resets the gauge after the reap returns
        gauge = sum(self.obs._warm.values())
        assert gauge == len(_pool(self.driver)), f"gauge={gauge} pool={len(_pool(self.driver))}"

    @invariant()
    def nothing_survives_teardown(self) -> None:  # P-10
        if (
            self.torn_down
            and not self.inflight
            and not self.warm_tasks
            and not self.acquires
            and not self.driver._background_tasks
        ):
            left = self.client.live - self.late - self._held_by("run-0")
            assert not left, f"alive after teardown: {sorted(left)}"
            orphans = [
                aid
                for aid in self.late & self.client.live
                if self.driver._owned_actors.get(aid) is None
                or self.driver._owned_actors[aid].run_id != WARM_POOL_RUN_ID
            ]
            assert not orphans, f"late actors not reapable by next run: {sorted(orphans)}"

    @invariant()
    def no_warm_actor_is_lost_before_teardown(self) -> None:  # AC-15.7
        # Before the teardown starts, a live actor that warm_pool made is in
        # the pool, held by a caller, in a recycle, or in the get() of an
        # acquire that claims it. One that is none of these (a cancelled
        # claim that did not put it back, say) is alive and of no use until
        # a reap.
        if not self._quiet():
            return
        in_get = {gate.claiming for _, gate, _, _ in self.acquires.values()}
        accounted = set(_pool(self.driver)) | set(self.holder) | set(self.inflight) | in_get
        lost = sorted(a for a in self.client.live - accounted if a.startswith("warm-"))
        assert not lost, f"live warm actors neither pooled nor held: {lost}"

    @invariant()
    def warm_pool_never_over_provisions(self) -> None:  # P-11
        # Actors warm_pool created and nobody claimed yet. Recycled actors
        # (cold-created, or claimed from the pool) may take the pool above R.
        fresh = [
            a for a in _pool(self.driver) if a.startswith("warm-") and a not in self.claimed
        ]
        assert len(fresh) <= R, f"{len(fresh)} warm_pool actors pooled, asked for {R}"


WarmPoolMachine.TestCase.settings = settings(
    max_examples=500, stateful_step_count=16, deadline=None
)
test_warm_pool_accounting = WarmPoolMachine.TestCase
