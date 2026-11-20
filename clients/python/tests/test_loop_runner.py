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

"""LoopRunner.stop(): the work a cancel handler starts finishes before the loop stops."""

from __future__ import annotations

import asyncio
import time

from ate_env_rl.loop_runner import _STOP_ROUNDS, LoopRunner


async def _spin() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


def test_stop_waits_for_the_tasks_a_cancel_handler_starts() -> None:
    # What the Substrate driver does when stop() cancels an acquire whose
    # create() is in flight after a reap: the cancel handler starts a delete
    # in a new task. The shutdown must wait for that task, not stop the loop
    # with it pending.
    runner = LoopRunner(name="test-stop")
    events: list[str] = []
    cleanups: list[asyncio.Task[None]] = []

    async def cleanup() -> None:
        await asyncio.sleep(0.05)
        events.append("cleanup done")

    async def worker() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanups.append(asyncio.create_task(cleanup()))
            events.append("worker cancelled")
            raise

    future = runner.submit(worker())
    runner.run(_spin())  # the worker waits on its event

    runner.stop()

    assert events == ["worker cancelled", "cleanup done"]
    assert future.cancelled()
    assert cleanups[0].done() and not cleanups[0].cancelled()
    assert runner.loop.is_closed()


def test_stop_ends_at_its_round_bound_when_cancel_handlers_keep_starting_tasks() -> None:
    # A chain of tasks that each start another: the shutdown waits for one
    # link per round and ends at its bound with the last link still to run.
    # The chain is one link longer than the rounds, so the bound is reached,
    # and the last link starts nothing, so nothing is left for loop.stop() to
    # abandon: it runs before the stop lands (it was queued first), and the
    # test leaves no coroutine un-awaited.
    runner = LoopRunner(name="test-stop-bound")
    links: list[asyncio.Task[None]] = []

    async def link(n: int) -> None:
        await asyncio.sleep(0)
        if n < _STOP_ROUNDS - 1:
            links.append(asyncio.create_task(link(n + 1)))

    async def worker() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            links.append(asyncio.create_task(link(0)))
            raise

    runner.submit(worker())
    runner.run(_spin())

    started = time.monotonic()
    runner.stop()

    assert time.monotonic() - started < 1.0
    assert runner.loop.is_closed()
    assert len(links) == _STOP_ROUNDS
    assert all(t.done() and not t.cancelled() for t in links)
