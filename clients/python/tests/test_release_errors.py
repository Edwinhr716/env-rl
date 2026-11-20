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

"""A failed delete on release is recorded, not swallowed.

``release`` keeps not raising (cleanup must not crash a trainer), but the
failure reaches the RunReport, the Prometheus series, the release span and
the run's reap, which retries the delete and raises if it still fails.
"""

from __future__ import annotations

import pytest

from ate_env_rl import AsyncSandboxFleet, FleetConfig, Task
from ate_env_rl.backend.substrate import SubstrateBackendDriver
from ate_env_rl.config import ObservabilityConfig
from ate_env_rl.errors import (
    FailedPreconditionError,
    InfrastructureError,
    NotFoundError,
)
from ate_env_rl.observability import Observer, RunReport

IMG = "docker.io/library/python@sha256:" + "c" * 64


def _failing_delete(client, exc):
    real = client.delete
    calls: list[str] = []

    async def delete(id, *, atespace="ate-env"):
        calls.append(id)
        if exc is not None:
            raise exc
        return await real(id, atespace=atespace)

    client.delete = delete
    return calls, real


async def _driver_with_actor(client):
    driver = SubstrateBackendDriver(client=client, atespace="ate-env", worker_family="c2")
    obs = Observer(ObservabilityConfig(enable_metrics=False))
    driver.observer = obs
    inst = await driver.acquire_async("tmpl-x", run_id="run-1")
    return driver, obs, inst


async def test_failed_delete_does_not_raise_and_is_counted(fake_stack):
    client, fakes = fake_stack
    driver, obs, inst = await _driver_with_actor(client)
    _failing_delete(client, FailedPreconditionError("ReleaseExternalSnapshots: 403"))

    with obs.run("t") as report:
        await driver.release_async(inst.instance_id)  # must not raise

    assert report.release_errors == 1
    assert report.to_dict()["release_errors"] == 1
    assert "release_errors=1" in report.summary()
    # Still owned, so the run's reap tries again.
    assert inst.instance_id in driver._owned_actors


async def test_reap_retries_a_failed_release_and_raises_if_it_still_fails(fake_stack):
    client, fakes = fake_stack
    driver, _, inst = await _driver_with_actor(client)
    calls, _ = _failing_delete(client, FailedPreconditionError("403"))

    await driver.release_async(inst.instance_id)
    with pytest.raises(InfrastructureError, match="Failed to delete 1 environments"):
        await driver.reap_async("run-1")
    assert calls == [inst.instance_id, inst.instance_id]


async def test_reap_cleans_up_after_a_transient_release_failure(fake_stack):
    client, fakes = fake_stack
    driver, _, inst = await _driver_with_actor(client)
    _, real = _failing_delete(client, FailedPreconditionError("transient"))
    await driver.release_async(inst.instance_id)

    client.delete = real  # the cause is fixed before teardown
    assert await driver.reap_async("run-1") == 1
    assert ("ate-env", inst.instance_id) not in fakes.environments.environments
    assert inst.instance_id not in driver._owned_actors


async def test_not_found_on_delete_counts_as_released(fake_stack):
    client, fakes = fake_stack
    driver, obs, inst = await _driver_with_actor(client)
    _failing_delete(client, NotFoundError("environment already gone"))

    with obs.run("t") as report:
        await driver.release_async(inst.instance_id)
    assert report.release_errors == 0
    assert inst.instance_id not in driver._owned_actors
    assert await driver.reap_async("run-1") == 0


async def test_successful_release_records_no_error(fake_stack):
    client, fakes = fake_stack
    driver, obs, inst = await _driver_with_actor(client)
    with obs.run("t") as report:
        await driver.release_async(inst.instance_id)
    assert report.release_errors == 0
    assert "release_errors" not in report.summary()
    assert ("ate-env", inst.instance_id) not in fakes.environments.environments


async def test_fleet_run_report_and_metric_show_failed_releases(fake_stack):
    prom = pytest.importorskip("prometheus_client")
    client, fakes = fake_stack
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test",
                      observability=ObservabilityConfig(enable_metrics=True))
    fleet = AsyncSandboxFleet(cfg)
    fleet.load_tasks([Task(id=f"t{i}", image=IMG) for i in range(3)])
    before = prom.REGISTRY.get_sample_value(
        "ate_env_fleet_release_failures_total", {"backend": "substrate"}) or 0.0
    _failing_delete(fleet.backend.client, FailedPreconditionError("403"))
    try:
        await fleet.run(lambda task, handle: None)
        assert fleet.report.release_errors == 3
        assert fleet.report.tasks_ok == 3  # the work itself succeeded
        after = prom.REGISTRY.get_sample_value(
            "ate_env_fleet_release_failures_total", {"backend": "substrate"})
        assert after - before == 3
        with pytest.raises(InfrastructureError):
            await fleet.teardown()
    finally:
        fleet.close()


async def test_release_span_is_marked_error(fake_stack):
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    # A local provider: never install the global one, which other tests own.
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    client, _ = fake_stack
    driver = SubstrateBackendDriver(client=client, atespace="ate-env", worker_family="c2")
    driver.tracer = provider.get_tracer("test")
    inst = await driver.acquire_async("tmpl-x", run_id="run-1")
    _failing_delete(client, FailedPreconditionError("gs://bucket/secret-path 403"))
    exporter.clear()
    await driver.release_async(inst.instance_id)

    release = [s for s in exporter.get_finished_spans() if s.name == "ate_env_client.release"]
    assert len(release) == 1
    assert release[0].status.status_code == trace.StatusCode.ERROR
    assert release[0].attributes["error.type"] == "FailedPreconditionError"
    assert "secret-path" not in (release[0].status.description or "")


def test_run_report_defaults_have_no_release_errors():
    r = RunReport("s")
    assert r.to_dict()["release_errors"] == 0


async def test_cancelled_release_propagates_and_keeps_the_environment_owned(fake_stack):
    import asyncio

    client, _fakes = fake_stack
    driver, obs, inst = await _driver_with_actor(client)
    _failing_delete(client, asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        with obs.run("t") as report:
            await driver.release_async(inst.instance_id)
    # Not recorded as a release failure, and still owned so reap retries it.
    assert report.release_errors == 0
    assert inst.instance_id in driver._owned_actors



class _FailingReap(Exception):
    pass


def _async_fleet_with_failing_reap(monkeypatch):
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock", strategy="naive"))
    closed = []

    def reap(run_id):
        raise InfrastructureError("could not delete 1 environment(s)")

    async def close_async():
        closed.append(True)

    monkeypatch.setattr(fleet.backend, "reap", reap)
    monkeypatch.setattr(fleet, "close_async", close_async)  # what the exit awaits
    return fleet, closed


async def test_async_fleet_exit_keeps_the_body_exception(monkeypatch, caplog):
    fleet, closed = _async_fleet_with_failing_reap(monkeypatch)
    with pytest.raises(_FailingReap):
        async with fleet:
            raise _FailingReap("body failed")
    assert closed == [True]
    assert "Fleet teardown failed" in caplog.text


async def test_async_fleet_exit_raises_teardown_error_when_body_succeeds(monkeypatch):
    fleet, closed = _async_fleet_with_failing_reap(monkeypatch)
    with pytest.raises(InfrastructureError, match="could not delete"):
        async with fleet:
            pass
    assert closed == [True]


_STRATEGIES = ["none", "naive", "sliding", "pipelined"]


def _fleet_with_failing_reap(monkeypatch, strategy):
    from ate_env_rl import SandboxFleet

    fleet = SandboxFleet(FleetConfig(backend="mock", strategy=strategy, window_size=1))
    fleet.add_tasks([Task(id=f"t-{i}", image=f"img-{i}") for i in range(2)])

    def reap(run_id):
        raise InfrastructureError("could not delete 1 environment(s)")

    monkeypatch.setattr(fleet.backend, "reap", reap)
    return fleet


@pytest.mark.parametrize("strategy", _STRATEGIES)
def test_fleet_run_keeps_an_interrupt_when_the_teardown_fails(monkeypatch, caplog, strategy):
    fleet = _fleet_with_failing_reap(monkeypatch, strategy)

    def interrupted(task, handle):
        raise KeyboardInterrupt

    # The teardown error in the interrupt's place would turn a Ctrl-C into a
    # failed run with the wrong cause.
    with pytest.raises(KeyboardInterrupt):
        fleet.run(interrupted)
    assert "Fleet teardown failed after a failed run" in caplog.text
    assert "could not delete 1 environment(s)" in caplog.text


@pytest.mark.parametrize("strategy", ["naive", "sliding", "pipelined"])
def test_fleet_run_keeps_a_warm_error_when_the_teardown_fails(monkeypatch, caplog, strategy):
    fleet = _fleet_with_failing_reap(monkeypatch, strategy)

    def no_warm(*args, **kwargs):
        raise _FailingReap("warm failed")

    # naive warms in setup(), the window strategies in warm_images().
    monkeypatch.setattr(fleet, "setup" if strategy == "naive" else "warm_images", no_warm)
    with pytest.raises(_FailingReap, match="warm failed"):
        fleet.run(lambda task, handle: None)
    assert "Fleet teardown failed after a failed run" in caplog.text


@pytest.mark.parametrize("strategy", _STRATEGIES)
def test_fleet_run_raises_the_teardown_error_when_the_run_succeeds(monkeypatch, strategy):
    fleet = _fleet_with_failing_reap(monkeypatch, strategy)
    with pytest.raises(InfrastructureError, match="could not delete"):
        fleet.run(lambda task, handle: None)


def test_fleet_exit_keeps_the_body_exception_and_logs_the_teardown_error(monkeypatch, caplog):
    fleet = _fleet_with_failing_reap(monkeypatch, "naive")
    with pytest.raises(_FailingReap, match="body failed"):
        with fleet:
            raise _FailingReap("body failed")
    # The teardown error is not raised, so the log is the only place it shows.
    assert "Fleet teardown failed after an error in the body" in caplog.text
    assert "could not delete 1 environment(s)" in caplog.text


def test_fleet_exit_raises_the_teardown_error_when_the_body_succeeds(monkeypatch, caplog):
    fleet = _fleet_with_failing_reap(monkeypatch, "naive")
    with pytest.raises(InfrastructureError, match="could not delete"):
        with fleet:
            pass
    # Raised, so not also logged as a teardown that lost to a body error.
    assert "Fleet teardown failed" not in caplog.text


def test_pipelined_keeps_a_prefetch_error_when_the_teardown_fails(monkeypatch, caplog):
    fleet = _fleet_with_failing_reap(monkeypatch, "pipelined")
    real_warm = fleet.warm_images
    warmed: list[list[str]] = []
    processed: list[str] = []

    def warm_images(images, **kwargs):
        warmed.append(list(images))
        if len(warmed) > 1:  # window 1, warmed by the prefetch thread
            raise _FailingReap("prefetch failed")
        return real_warm(images, **kwargs)

    monkeypatch.setattr(fleet, "warm_images", warm_images)
    with pytest.raises(_FailingReap, match="prefetch failed"):
        fleet.run(lambda task, handle: processed.append(task.id))
    # Window 0 warmed and ran; the error is the one of the prefetch of window 1.
    assert warmed == [["img-0"], ["img-1"]]
    assert processed == ["t-0"]
    assert "Fleet teardown failed after a failed run" in caplog.text
    assert "could not delete 1 environment(s)" in caplog.text


@pytest.mark.parametrize("strategy", _STRATEGIES)
def test_strategy_without_teardown_does_not_tear_down_after_a_failed_body(
    monkeypatch, caplog, strategy
):
    from ate_env_rl.strategies import STRATEGIES

    fleet = _fleet_with_failing_reap(monkeypatch, strategy)
    teardowns: list[bool] = []
    monkeypatch.setattr(fleet, "teardown", lambda: teardowns.append(True))

    def interrupted(task, handle):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        STRATEGIES[strategy](fleet, interrupted, 1, teardown=False)
    assert teardowns == []
    assert "Fleet teardown failed" not in caplog.text


@pytest.mark.parametrize("strategy", _STRATEGIES)
def test_strategy_without_teardown_does_not_tear_down_after_a_run_that_succeeds(
    monkeypatch, strategy
):
    from ate_env_rl.strategies import STRATEGIES

    fleet = _fleet_with_failing_reap(monkeypatch, strategy)
    teardowns: list[bool] = []
    monkeypatch.setattr(fleet, "teardown", lambda: teardowns.append(True))
    assert STRATEGIES[strategy](fleet, lambda task, handle: task.id, 1, teardown=False) == [
        "t-0",
        "t-1",
    ]
    assert teardowns == []


def test_fleet_close_logs_the_backend_close_error_that_the_teardown_error_replaces(
    monkeypatch, caplog
):
    fleet = _fleet_with_failing_reap(monkeypatch, "naive")

    def close():
        raise RuntimeError("channel close failed")

    monkeypatch.setattr(fleet.backend, "close", close, raising=False)
    # close() raises one error, the teardown's; the backend's is logged.
    with pytest.raises(InfrastructureError, match="could not delete"):
        fleet.close()
    assert "Backend close failed after a failed teardown" in caplog.text
    assert "channel close failed" in caplog.text


def test_fleet_close_raises_the_backend_close_error_when_the_teardown_succeeds(
    monkeypatch, caplog
):
    from ate_env_rl import SandboxFleet

    fleet = SandboxFleet(FleetConfig(backend="mock"))

    def close():
        raise RuntimeError("channel close failed")

    monkeypatch.setattr(fleet.backend, "close", close, raising=False)
    with pytest.raises(RuntimeError, match="channel close failed"):
        fleet.close()
    assert "Backend close failed" not in caplog.text
