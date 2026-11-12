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

"""FleetConfig(preflight=True): the image pre-flight runs from run(), before
the strategy, over the loaded tasks' images and the pools their creates can
land on."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ate_env_rl import (
    AsyncSandboxFleet,
    FleetConfig,
    PreflightError,
    SandboxFleet,
    Task,
)
from ate_env_rl._gen.ateenv.v1alpha import env_pb2

IMG_A = "docker.io/library/python@sha256:" + "a" * 64
IMG_B = "docker.io/library/python@sha256:" + "b" * 64
IMG_C = "docker.io/library/python@sha256:" + "c" * 64
NODES = {"d0": ["node-d0-1", "node-d0-2"], "d1": ["node-d1-1"]}


def _ready_goldens(fakes, base: str, *images: str) -> None:
    for image in images:
        digest = image.split("@sha256:", 1)[1]
        fakes.environments.image_templates[("ate-env", f"{base}-{digest[:12]}")] = env_pb2.ImageStatus(
            golden=env_pb2.GOLDEN_SNAPSHOT_STATE_READY, golden_tag="golden-1"
        )


def _pools(fakes, base: str) -> None:
    fakes.environments.domain_nodes = dict(NODES)
    fakes.environments.template_selectors[("ate-env", base)] = {}


def _tasks() -> list[Task]:
    return [Task(id=f"t{i}", image=IMG_A if i % 2 == 0 else IMG_B) for i in range(4)]


def _process(task: Task, handle) -> str:
    return task.id


async def test_preflight_flag_warms_the_pools_before_the_run(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test",
        strategy="sliding", batch_size=2, max_warmpool_replicas=1,
        preflight=True, placement={IMG_A: {"ate-env/domain": "d0"}},
    )

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            results = fleet.run(_process, concurrency=2)
            return results, fleet.preflight_coverage, fleet.report
        finally:
            fleet.close()

    results, coverage, report = await asyncio.to_thread(run)
    assert results == ["t0", "t1", "t2", "t3"]
    assert coverage is not None and coverage.complete, coverage and coverage.shortfall()
    # a is placed in d0 and warmed there only; b, unplaced, in every pool of
    # the template. The calls carry the fleet's atespace and base template.
    calls = sorted(
        (r.rootfs_image[-4:], r.worker_selector["ate-env/domain"], r.nodes)
        for r in fakes.environments.warm_requests
    )
    assert calls == [("aaaa", "d0", 2), ("bbbb", "d0", 2), ("bbbb", "d1", 1)]
    assert all(r.template.name == "tmpl-test" and r.atespace == "ate-env" for r in fakes.environments.warm_requests)
    # The pre-flight ran inside the run: its phases and its summary are in the report.
    assert report.phases["preflight_images"][0] == 1
    assert report.phases["preflight_probe"][0] == 10  # 2 probes per node: 4 + 4 + 2
    # The phase sits where it ran in the summary: before the templates.
    order = [k for k in report.to_dict()["phases"]]
    assert order.index("preflight_images") < order.index("ensure_template")
    assert report.environment["substrate"]["preflight"] == {
        "run_id": coverage.run_id, "complete": True, "clean": True, "shortfall": "complete",
        "images": 3, "domains": 2, "undeleted_probes": [], "waited_ms": 0.0, "goldens_not_ready": 0,
    }
    # The summary line formats the nested block instead of a dict repr.
    assert "preflight={run_id:" in report.summary() and "{'run_id'" not in report.summary()
    assert coverage.run_id.startswith("pf-")
    # The probes are gone; the run's own environments were released.
    assert fakes.environments.environments == {}


async def test_preflight_flag_on_the_async_fleet(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d1"},
    )
    async with AsyncSandboxFleet(cfg) as fleet:
        fleet.load_tasks(_tasks())
        results = await fleet.run(lambda task, handle: task.id, concurrency=2)
        coverage = fleet.preflight_coverage
    assert results == ["t0", "t1", "t2", "t3"]
    assert coverage is not None and coverage.complete
    # node_selector narrows every image to d1.
    assert {(r.rootfs_image[-4:], r.worker_selector["ate-env/domain"]) for r in fakes.environments.warm_requests} == {
        ("aaaa", "d1"), ("bbbb", "d1"),
    }
    assert fleet.report.environment["substrate"]["preflight"]["complete"] is True


async def test_preflight_flag_skips_images_without_a_rootfs(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A)
    fakes.environments.template_selectors[("ate-env", "fixed-tmpl")] = {}
    # b is routed to a fixed template and c is not pinned: neither has a
    # rootfs image to warm; a is warmed.
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        image_templates={IMG_B: "fixed-tmpl"},
    )
    # d names its template: its create sends no image, so IMG_C is not warmed.
    tasks = [
        Task(id="a", image=IMG_A),
        Task(id="b", image=IMG_B),
        Task(id="c", image="reg/x:latest"),
        Task(id="d", image=IMG_C, template="tmpl-test"),
    ]

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(tasks)
            fleet.run(_process, concurrency=3)
            return fleet.preflight_coverage
        finally:
            fleet.close()

    coverage = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete
    assert {r.rootfs_image for r in fakes.environments.warm_requests} == {IMG_A}

    # Nothing to warm at all: no coverage, no call, the run still goes.
    fakes.environments.warm_requests.clear()
    cfg2 = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                       image_templates={IMG_B: "fixed-tmpl"})

    def run2():
        fleet = SandboxFleet(cfg2)
        try:
            fleet.load_tasks([Task(id="b", image=IMG_B)])
            fleet.run(_process)
            return fleet.preflight_coverage, fleet.report
        finally:
            fleet.close()

    coverage, report = await asyncio.to_thread(run2)
    assert coverage is None and fakes.environments.warm_requests == []
    assert "preflight" not in report.environment["substrate"]


async def test_preflight_flag_without_the_global_role_fails_the_run_up_front(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    fakes.environments.deny_list_workers = True
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True)

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            with pytest.raises(PreflightError, match="access-policy --global"):
                fleet.run(_process)
            return fleet.preflight_coverage
        finally:
            fleet.close()

    assert await asyncio.to_thread(run) is None
    assert fakes.environments.environments == {}


async def test_preflight_flag_reports_a_short_coverage_and_runs_anyway(fake_stack, caplog):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    # Every probe lands on the first node: d0 is never fully covered.
    fakes.environments.warm_same_node = True
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      node_selector={"ate-env/domain": "d0"})

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            results = fleet.run(_process, concurrency=2)
            return results, fleet.preflight_coverage, fleet.report
        finally:
            fleet.close()

    with caplog.at_level("WARNING", logger="ate_env_rl.fleet"):
        results, coverage, report = await asyncio.to_thread(run)
    assert results == ["t0", "t1", "t2", "t3"]
    assert coverage is not None and not coverage.complete
    assert report.environment["substrate"]["preflight"]["shortfall"] == "2 (image, domain) pairs short of their nodes"
    assert any("image pre-flight incomplete" in r.message for r in caplog.records)


def test_preflight_flag_is_a_bool_and_the_mock_has_nothing_to_warm():
    with pytest.raises(Exception, match="preflight must be a bool"):
        FleetConfig(backend="mock", preflight="yes")  # type: ignore[arg-type]
    cfg = FleetConfig(backend="mock", preflight=True, strategy="naive")
    fleet = SandboxFleet(cfg)
    fleet.load_tasks([Task(id="t1", image="image-0"), Task(id="t2", image="image-1")])
    try:
        results = fleet.run(_process, concurrency=2)
    finally:
        fleet.close()
    assert results == ["t1", "t2"] and fleet.preflight_coverage is None
    assert "preflight" not in fleet.report.environment["mock"]
    # naive calls setup() inside run(): one pass, not one from each.
    assert fleet.report.phases["preflight_images"][0] == 1
    assert "preflight_images" in fleet.report.phases


async def test_preflight_flag_bounds_the_probes_by_the_pools_node_slots(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    # d1's nodes run one environment at once: the pre-flight sends one probe
    # per node instead of refusing to start.
    fakes.environments.node_slots = {"d1": 1}
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True)

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            return fleet.preflight_coverage
        finally:
            fleet.close()

    coverage = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete
    # d1 bounds its own probes; d0, with no node_slots, keeps the default.
    by_domain = {r.worker_selector["ate-env/domain"]: r.probes_per_node for r in fakes.environments.warm_requests}
    assert by_domain == {"d0": 2, "d1": 1}


async def test_preflight_flag_repeats_a_transient_pool_listing(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    fakes.environments.list_unavailable_first = 1
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True)

    def run():
        fleet = SandboxFleet(cfg)
        fleet.backend.preflight_list_backoff = 0.001
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            return fleet.preflight_coverage
        finally:
            fleet.close()

    coverage = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete


async def test_two_images_warm_one_pool_at_once(fake_stack):
    """Two concurrent WarmImage streams on a two-node pool, one probe per node:
    each round's probes are created before any event goes out, so neither
    stream is starved of a node by the other (the CI failure Sunny saw)."""
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    fakes.environments.warm_hold = 0.01
    fakes.environments.node_slots = {"d0": 1, "d1": 1}
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      node_selector={"ate-env/domain": "d0"})

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            return fleet.preflight_coverage
        finally:
            fleet.close()

    coverage = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete, coverage and coverage.shortfall()
    assert all(r.probes_per_node == 1 for r in fakes.environments.warm_requests)


async def test_probes_are_bounded_per_domain_not_by_the_smallest_pool(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    fakes.environments.node_slots = {"d0": 8, "d1": 1}
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        placement={IMG_A: {"ate-env/domain": "d0"}, IMG_B: {"ate-env/domain": "d1"}},
    )

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            return fleet.preflight_coverage
        finally:
            fleet.close()

    coverage = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete
    by_domain = {r.worker_selector["ate-env/domain"]: r.probes_per_node for r in fakes.environments.warm_requests}
    assert by_domain == {"d0": 2, "d1": 1}


async def test_a_leftover_probe_is_a_warning_and_in_the_report(fake_stack, caplog):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    fakes.environments.warm_undeleted = ["pf-x-0-0-1-1-0"]
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True)

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            return fleet.preflight_coverage, fleet.report
        finally:
            fleet.close()

    with caplog.at_level("WARNING", logger="ate_env_rl.fleet"):
        coverage, report = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete and not coverage.clean
    summary = report.environment["substrate"]["preflight"]
    assert summary["clean"] is False and "pf-x-0-0-1-1-0" in summary["undeleted_probes"]
    assert any("left undeleted" in r.message for r in caplog.records)


async def test_a_failed_second_run_leaves_no_stale_coverage(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True)

    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            first = fleet.preflight_coverage
            # A new image makes the next run() pass again; the listing is
            # refused now, so the run fails before any task and keeps no
            # stale coverage.
            fleet.add_tasks([Task(id="t9", image=IMG_C)])
            fakes.environments.deny_list_workers = True
            with pytest.raises(PreflightError):
                fleet.run(_process, concurrency=2)
            return first, fleet.preflight_coverage
        finally:
            fleet.close()

    first, second = await asyncio.to_thread(run)
    assert first is not None and first.complete and second is None


async def test_setup_runs_the_preflight_before_the_pools_and_run_does_not_repeat_it(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      max_warmpool_replicas=2)
    fleet = AsyncSandboxFleet(cfg)
    fleet.load_tasks(_tasks())
    async with fleet:
        log = list(fakes.environments.call_log)
        assert "warm" in log and "create" in log
        assert log.index("create") > max(i for i, c in enumerate(log) if c == "warm"), log
        warms = fakes.environments.warm_calls
        results = await fleet.run(lambda task, handle: task.id, concurrency=2)
    assert results == ["t0", "t1", "t2", "t3"]
    assert fakes.environments.warm_calls == warms, "run() repeated the pass setup() made"
    assert fleet.report.environment["substrate"]["preflight"]["complete"] is True


def _run_fleet(cfg: FleetConfig):
    fleet = SandboxFleet(cfg)
    try:
        fleet.load_tasks(_tasks())
        results = fleet.run(_process, concurrency=2)
        return results, fleet.preflight_coverage, fleet.report
    finally:
        fleet.close()


async def test_preflight_knobs_reach_the_warm_calls_and_the_report_sums_the_wait(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    # Each call reports half a second spent waiting for probe slots.
    fakes.environments.warm_waited = 0.5
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d0"},
        preflight_probes_per_node=1, preflight_rounds=5, preflight_probe_timeout_s=90,
    )
    _, coverage, report = await asyncio.to_thread(_run_fleet, cfg)
    assert coverage is not None and coverage.complete
    reqs = fakes.environments.warm_requests
    assert len(reqs) == 2
    assert {r.probes_per_node for r in reqs} == {1}
    assert {r.max_rounds for r in reqs} == {5}
    assert {r.probe_timeout.ToSeconds() for r in reqs} == {90}
    summary = report.environment["substrate"]["preflight"]
    assert summary["waited_ms"] == pytest.approx(1000.0)
    assert summary["goldens_not_ready"] == 0


async def test_preflight_golden_skip_probes_at_once_and_reports_the_goldens_pending(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    # IMG_A's golden is ready; IMG_B's template is new and its golden bakes
    # for good in the fake. With "wait" the pass would wait for the golden
    # timeout; "skip" probes both at once.
    _ready_goldens(fakes, "tmpl-test", IMG_A)
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d0"}, preflight_golden="skip",
        preflight_golden_timeout_s=3600,
    )
    results, coverage, report = await asyncio.wait_for(asyncio.to_thread(_run_fleet, cfg), 30)
    assert results == ["t0", "t1", "t2", "t3"]
    assert coverage is not None and not coverage.complete
    by_image = {g.image: g for g in coverage.goldens}
    assert by_image[IMG_A].state == "ready"
    assert by_image[IMG_B].state == "pending" and by_image[IMG_B].error == "golden not waited for"
    # A golden that is not waited for still names the template the ensure derived.
    assert by_image[IMG_B].template == "tmpl-test-" + "b" * 12
    # Both images were probed on both nodes of d0.
    assert {r.rootfs_image for r in fakes.environments.warm_requests} == {IMG_A, IMG_B}
    assert all(ic.complete for d in coverage.domains for ic in d.images)
    summary = report.environment["substrate"]["preflight"]
    assert summary["goldens_not_ready"] == 1 and summary["shortfall"] == "1 goldens not ready"


async def test_preflight_golden_fail_refuses_the_run_and_does_not_probe_the_unready_image(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A)
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d0"}, preflight_golden="fail",
        preflight_golden_timeout_s=0.2,
    )
    started = time.monotonic()
    with pytest.raises(PreflightError, match=r"1 of 2 golden snapshots are not ready.*" + IMG_B) as exc:
        await asyncio.wait_for(asyncio.to_thread(_run_fleet, cfg), 60)
    # The golden wait ends at preflight_golden_timeout_s (0.2 s), not at the
    # first poll (10 s).
    elapsed = time.monotonic() - started
    assert elapsed < 2.0, elapsed
    # The error carries the pass's coverage: the unready image's pair is
    # short with the reason, the ready image's is complete.
    pairs = {ic.image: ic for d in exc.value.coverage.domains for ic in d.images}
    assert pairs[IMG_A].complete and not pairs[IMG_B].complete
    assert pairs[IMG_B].error == "golden not ready" and pairs[IMG_B].probes == 0
    # The golden the wait gave up on names its template too.
    goldens = {g.image: g for g in exc.value.coverage.goldens}
    assert goldens[IMG_B].state == "pending" and goldens[IMG_B].template == "tmpl-test-" + "b" * 12
    # The ready image went to its nodes; the unready one was not probed.
    assert {r.rootfs_image for r in fakes.environments.warm_requests} == {IMG_A}
    # No task ran: the only environments are the pre-flight's own probes.
    assert "create" not in fakes.environments.call_log


def test_preflight_error_has_a_coverage_attribute_that_survives_pickle():
    import pickle

    from ate_env_rl.preflight import (
        DomainCoverage,
        GoldenRecord,
        ImageCoverage,
        PreflightCoverage,
    )

    # Every PreflightError has the attribute: a caller that catches the
    # error of a missing viewer binding reads None, not an AttributeError.
    plain = PreflightError("no viewer binding")
    assert plain.coverage is None
    again = pickle.loads(pickle.dumps(plain))
    assert type(again) is PreflightError and again.coverage is None and str(again) == "no viewer binding"
    assert again.retryable is False
    coverage = PreflightCoverage(run_id="pf-abc123", started_at=datetime.now(timezone.utc))
    coverage.goldens = [GoldenRecord(image=IMG_B, state="pending", error="golden not waited for")]
    coverage.domains = [
        DomainCoverage(
            name="pool-d0", nodes=2,
            images=[ImageCoverage(image=IMG_A, wanted=2, undeleted_probes=["pf-abc123-0-0-1-1-0"])],
        )
    ]
    err = pickle.loads(pickle.dumps(PreflightError("goldens not ready", status="x", coverage=coverage)))
    assert str(err) == "goldens not ready" and err.status == "x"
    assert err.coverage.run_id == "pf-abc123"
    assert err.coverage.goldens[0].image == IMG_B
    assert err.coverage.undeleted_probes == ["pf-abc123-0-0-1-1-0"]


async def test_preflight_golden_fail_records_the_coverage_and_the_leftover_probes(fake_stack, caplog):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A)
    # The ready image's pass leaves a probe undeleted; the run then fails on
    # IMG_B's golden. The fleet must still say so: in preflight_coverage, in
    # the run report and as a warning.
    fakes.environments.warm_undeleted = ["pf-x-0-0-1-1-0"]

    def run(make):
        fleet = make()
        try:
            fleet.load_tasks(_tasks())
            with pytest.raises(PreflightError) as exc:
                fleet.run(_process, concurrency=2)
            return exc.value, fleet.preflight_coverage, fleet.report
        finally:
            fleet.close()

    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d0"}, preflight_golden="fail",
        preflight_golden_timeout_s=0.2,
    )
    with caplog.at_level("WARNING", logger="ate_env_rl.fleet"):
        err, coverage, report = await asyncio.wait_for(asyncio.to_thread(run, lambda: SandboxFleet(cfg)), 60)
    assert coverage is err.coverage and coverage is not None
    assert coverage.undeleted_probes == ["pf-x-0-0-1-1-0"]
    summary = report.environment["substrate"]["preflight"]
    assert summary["undeleted_probes"] == ["pf-x-0-0-1-1-0"] and summary["goldens_not_ready"] == 1
    assert summary["complete"] is False and summary["clean"] is False
    assert any("pf-x-0-0-1-1-0" in r.getMessage() for r in caplog.records)

    # The async fleet records it the same way.
    async def arun():
        fleet = AsyncSandboxFleet(cfg)
        fleet.load_tasks(_tasks())
        try:
            with pytest.raises(PreflightError) as exc:
                await fleet.run(lambda task, handle: task.id, concurrency=2)
            return exc.value, fleet.preflight_coverage, fleet.report
        finally:
            await fleet.teardown()
            fleet.close()

    caplog.clear()
    with caplog.at_level("WARNING", logger="ate_env_rl.fleet"):
        err, coverage, report = await asyncio.wait_for(arun(), 60)
    assert coverage is err.coverage and coverage.undeleted_probes == ["pf-x-0-0-1-1-0"]
    assert report.environment["substrate"]["preflight"]["undeleted_probes"] == ["pf-x-0-0-1-1-0"]
    assert any("pf-x-0-0-1-1-0" in r.getMessage() for r in caplog.records)


async def test_preflight_knobs_on_the_async_fleet(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A)
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d1"},
        preflight_probes_per_node=1, preflight_rounds=4, preflight_golden="skip",
        preflight_golden_timeout_s=3600,
    )

    async def run():
        async with AsyncSandboxFleet(cfg) as fleet:
            fleet.load_tasks(_tasks())
            await fleet.run(lambda task, handle: task.id, concurrency=2)
            return fleet.preflight_coverage

    coverage = await asyncio.wait_for(run(), 30)
    assert coverage is not None and not coverage.complete
    reqs = fakes.environments.warm_requests
    assert {r.rootfs_image for r in reqs} == {IMG_A, IMG_B}
    assert {(r.probes_per_node, r.max_rounds) for r in reqs} == {(1, 4)}
    assert {g.image: g.state for g in coverage.goldens} == {IMG_A: "ready", IMG_B: "pending"}


def test_preflight_knobs_are_validated():
    for kw, match in (
        ({"preflight_probes_per_node": 0}, "preflight_probes_per_node must be an int between 1 and 8"),
        ({"preflight_rounds": True}, "preflight_rounds must be an int between 1 and 10"),
        # ate-env-api's wire bounds: it refuses more, and a refused call
        # only shows as a short coverage.
        ({"preflight_probes_per_node": 9}, "preflight_probes_per_node must be an int between 1 and 8"),
        ({"preflight_rounds": 11}, "preflight_rounds must be an int between 1 and 10"),
        ({"preflight_probe_timeout_s": 0}, "preflight_probe_timeout_s must be a number between 1e-06 and"),
        # Below a microsecond the wire value is zero, which the server refuses.
        ({"preflight_probe_timeout_s": 4e-7}, "preflight_probe_timeout_s must be a number between 1e-06 and"),
        ({"preflight_probe_timeout_s": 1e15}, "preflight_probe_timeout_s must be a number between 1e-06 and"),
        ({"preflight_probe_timeout_s": float("nan")}, "preflight_probe_timeout_s must be a number between 1e-06 and"),
        ({"preflight_golden_timeout_s": float("inf")}, "preflight_golden_timeout_s must be a finite number > 0"),
        ({"preflight_golden": "never"}, "preflight_golden must be one of wait, skip, fail"),
    ):
        with pytest.raises(Exception, match=match):
            FleetConfig(backend="mock", **kw)  # type: ignore[arg-type]
    # The knobs do nothing without the flag, and are accepted without it.
    FleetConfig(backend="mock", preflight_golden="fail", preflight_rounds=9)
    FleetConfig(backend="mock", preflight_probes_per_node=8, preflight_rounds=10, preflight_probe_timeout_s=1e-6)


async def test_probes_per_node_above_the_servers_cap_is_refused_and_reported_short(fake_stack, caplog):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    # Within the wire bound (8), above this server's -warm-max-probes-per-node
    # (2 unless raised): the client cannot know the flag, the server refuses
    # each call, and the pairs are short with the server's reason.
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d0"}, preflight_probes_per_node=4,
    )
    _, coverage, _ = await asyncio.to_thread(_run_fleet, cfg)
    assert coverage is not None and not coverage.complete
    errors = {ic.error for d in coverage.domains for ic in d.images}
    assert len(errors) == 1 and "-warm-max-probes-per-node" in next(iter(errors))
    # A server with the flag raised takes the same request.
    fakes.environments.warm_max_probes_per_node = 4
    _, coverage, _ = await asyncio.to_thread(_run_fleet, cfg)
    assert coverage is not None and coverage.complete
    assert {r.probes_per_node for r in fakes.environments.warm_requests[-2:]} == {4}


def test_preflight_batches_fit_the_run_id_budget():
    from ate_env_rl.backend.substrate import (
        merge_preflight_coverages,
        preflight_batch_size,
        preflight_batches,
    )
    from ate_env_rl.preflight import (
        DomainCoverage,
        GoldenRecord,
        ImageCoverage,
        ImagePlacement,
        PlacementEntry,
        PreflightCoverage,
        PreflightWarmer,
    )
    from ate_env_rl.types import PlacementDomain

    # A caller's run id sizes the batches: 14 characters and 10 domains
    # leave one digit for the image index, and no room for a batch index, so
    # 15 images are refused with a reason instead of a late ValueError.
    assert preflight_batch_size(10, 14) == 10
    assert preflight_batches(10, 10, "pf-custom-run1") == (10, ["pf-custom-run1"])
    with pytest.raises(ValueError, match="too long for 15 images in 10 domains"):
        preflight_batches(10, 15, "pf-custom-run1")
    # One batch keeps the caller's id; more batches get the batch index, and
    # the size shrinks to what the longer id leaves.
    assert preflight_batches(2, 5, "pf-custom") == (10**6, ["pf-custom"])
    assert preflight_batches(2, 2, "pf-custom", 1) == (1, ["pf-custom-0", "pf-custom-1"])
    # 10 characters, 10 domains, 25,000 images: one batch takes 10^5.
    assert preflight_batches(10, 25_000, "pf-custom1") == (10**5, ["pf-custom1"])
    # 10 characters, 10 domains, 250,000 images: with "-<b>" a batch takes
    # 10^3, 250 batches need three digits, which leave 10 per batch and too
    # many batches again: refused.
    with pytest.raises(ValueError, match="too long"):
        preflight_batches(10, 250_000, "pf-custom1")
    # 25 batches of one image: a two-digit batch index.
    size, ids = preflight_batches(10, 25, "pf-cust", 1)
    assert size == 1 and len(ids) == 25 and ids[0] == "pf-cust-0" and ids[-1] == "pf-cust-24"
    # Without a run id every batch gets the warmer's own random id.
    assert preflight_batches(101, 10_001) == (10_000, ["", ""])
    assert preflight_batch_size(101) == 10_000
    assert preflight_batch_size(351) == 10_000
    assert preflight_batch_size(1001) == 1_000
    # 101 domains and 10,001 images: two batches, and a warmer over one
    # batch constructs without a ValueError on the run id.
    domains = tuple(
        PlacementDomain(name=f"d{i}", selector={"ate-env/domain": f"d{i}"}, nodes=1) for i in range(101)
    )
    images = [f"registry.example/i@sha256:{i:064x}" for i in range(10_001)]
    batch = preflight_batch_size(len(domains))
    batches = [images[i : i + batch] for i in range(0, len(images), batch)]
    assert len(batches) == 2 and sum(len(b) for b in batches) == 10_001
    plan = ImagePlacement(domains, tuple(PlacementEntry(img, ("d0",)) for img in batches[0]))
    PreflightWarmer(object(), plan)  # type: ignore[arg-type]
    # Merging keeps every golden and image and joins a domain's nodes.
    c1 = PreflightCoverage(run_id="pf-1", started_at=datetime.now(timezone.utc), complete=True)
    c2 = PreflightCoverage(run_id="pf-2", started_at=datetime.now(timezone.utc), complete=False)
    c1.goldens = [GoldenRecord(image="a", template="t", state="ready")]
    c2.goldens = [GoldenRecord(image="b", template="t", state="ready")]
    c1.domains = [DomainCoverage(name="d0", nodes=2, nodes_seen=["n1"], images=[ImageCoverage(image="a", wanted=2)], complete=True)]
    c2.domains = [DomainCoverage(name="d0", nodes=2, nodes_seen=["n2"], images=[ImageCoverage(image="b", wanted=2)], complete=False)]
    m = merge_preflight_coverages([c1, c2])
    assert m.run_id == "pf-1" and [g.image for g in m.goldens] == ["a", "b"]
    assert len(m.domains) == 1 and m.domains[0].nodes_seen == ["n1", "n2"]
    assert [ic.image for ic in m.domains[0].images] == ["a", "b"] and not m.domains[0].complete
    assert not m.complete


async def test_a_large_image_list_is_warmed_in_batches(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True)

    def run():
        fleet = SandboxFleet(cfg)
        fleet.backend.preflight_batch_images = 1  # two images: two passes
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            return fleet.preflight_coverage
        finally:
            fleet.close()

    coverage = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete
    assert len({r.run_id.split("-")[1] for r in fakes.environments.warm_requests}) == 2, "two passes"
    assert {g.image for g in coverage.goldens} == {IMG_A, IMG_B}


async def test_golden_fail_stops_at_the_first_batch_with_an_unready_golden(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    # No golden is ready and none gets ready. Each batch has its own golden
    # wait, so two batches would wait twice for an error the first one
    # already decides.
    cfg = FleetConfig(
        backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
        node_selector={"ate-env/domain": "d0"}, preflight_golden="fail",
        preflight_golden_timeout_s=0.2,
    )

    def run():
        fleet = SandboxFleet(cfg)
        fleet.backend.preflight_batch_images = 1  # two images: two batches
        try:
            fleet.load_tasks(_tasks())
            start = time.monotonic()
            with pytest.raises(PreflightError) as exc:
                fleet.run(_process, concurrency=2)
            return exc.value, time.monotonic() - start
        finally:
            fleet.close()

    err, _ = await asyncio.wait_for(asyncio.to_thread(run), 90)
    # Only the first batch ran: one golden in the coverage, one template
    # ensured, and the error says what was left out.
    assert [g.image for g in err.coverage.goldens] == [IMG_A]
    assert ("ate-env", "tmpl-test-" + "b" * 12) not in fakes.environments.image_templates
    assert "1 of 1 golden snapshots are not ready" in str(err)
    assert "1 more images were not warmed" in str(err)


async def test_batches_of_a_callers_run_id_do_not_share_probe_names(fake_stack):
    from ate_env_rl.backend.substrate import SubstrateBackendDriver

    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test",
                      node_selector={"ate-env/domain": "d1"})
    # Batch 0's first probe cannot be deleted. With one run id for both
    # batches, batch 1 would create the same name and fail on it.
    fakes.environments.delete_fail = {"pf-custom-0-0-0-1-1-0"}

    def run():
        fleet = SandboxFleet(cfg)
        try:
            driver = fleet.backend
            assert isinstance(driver, SubstrateBackendDriver)
            driver.preflight_batch_images = 1
            return driver.preflight_images([IMG_A, IMG_B], run_id="pf-custom")
        finally:
            fleet.close()

    coverage = await asyncio.to_thread(run)
    assert coverage is not None and coverage.complete and coverage.run_id == "pf-custom"
    assert sorted(r.run_id for r in fakes.environments.warm_requests) == ["pf-custom-0-0-0-1", "pf-custom-1-0-0-1"]
    assert coverage.undeleted_probes == ["pf-custom-0-0-0-1-1-0"]
    assert all(ic.complete and not ic.error for d in coverage.domains for ic in d.images)
    # A run id too long for the indexes is a PreflightError that says so.
    with pytest.raises(PreflightError, match="too long for 2 images in 1 domains"):
        def too_long():
            fleet = SandboxFleet(cfg)
            try:
                fleet.backend.preflight_batch_images = 1
                return fleet.backend.preflight_images([IMG_A, IMG_B], run_id="pf-custom-run-12")
            finally:
                fleet.close()
        await asyncio.to_thread(too_long)


async def test_a_pass_with_nothing_to_warm_is_not_repeated_by_run(fake_stack):
    client, fakes = fake_stack
    # A base template with no active pool: the listing answers, nothing
    # matches, and the pass returns no coverage. setup() and run() list the
    # pools once between them.
    fakes.environments.template_selectors[("ate-env", "tmpl-test")] = {}
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      strategy="none")
    fleet = AsyncSandboxFleet(cfg)
    fleet.load_tasks(_tasks())
    async with fleet:
        await fleet.run(lambda task, handle: task.id, concurrency=2)
        assert fleet.preflight_coverage is None
        assert "preflight_images" not in fleet.report.phases, "run() repeated setup()'s pass"
        # The cluster can have changed by the next run(): it looks again.
        await fleet.run(lambda task, handle: task.id, concurrency=2)
        assert fleet.report.phases["preflight_images"][0] == 1


async def test_a_short_pass_is_made_again_by_the_next_run(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    fakes.environments.warm_same_node = True  # d0 is never fully covered
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      node_selector={"ate-env/domain": "d0"}, strategy="none")

    def runs():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            first, calls = fleet.preflight_coverage, fakes.environments.warm_calls
            # The cluster recovers: the second run() warms again and is complete.
            fakes.environments.warm_same_node = False
            fleet.run(_process, concurrency=2)
            return first, calls, fleet.preflight_coverage, fakes.environments.warm_calls
        finally:
            fleet.close()

    first, calls, second, calls_after = await asyncio.to_thread(runs)
    assert first is not None and not first.complete
    assert calls_after > calls, "the second run() reused the short coverage"
    assert second is not None and second.complete


async def test_a_short_pass_is_made_again_by_the_next_run_of_one_async_with(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    fakes.environments.warm_same_node = True  # d0 is never fully covered
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      node_selector={"ate-env/domain": "d0"}, strategy="none")
    # No teardown between the two runs: only run() itself drops the short
    # pass, so this is the case that needs _preflight_spent().
    fleet = AsyncSandboxFleet(cfg)
    fleet.load_tasks(_tasks())
    async with fleet:
        calls = fakes.environments.warm_calls
        await fleet.run(lambda task, handle: task.id, concurrency=2)
        first = fleet.preflight_coverage
        assert first is not None and not first.complete
        assert fakes.environments.warm_calls == calls, "run() repeated setup()'s short pass"
        fakes.environments.warm_same_node = False
        await fleet.run(lambda task, handle: task.id, concurrency=2)
        assert fakes.environments.warm_calls > calls, "the second run() reused the short coverage"
        assert fleet.preflight_coverage.complete
    fleet.close()


async def test_naive_run_makes_one_pass_when_the_coverage_is_short(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    fakes.environments.warm_same_node = True  # d0 is never fully covered
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      node_selector={"ate-env/domain": "d0"}, strategy="naive", max_warmpool_replicas=1)

    # naive calls setup() inside run(): setup() must reuse run()'s pass, also
    # a short one, and the report and the fleet must name the same pass.
    def run():
        fleet = SandboxFleet(cfg)
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
            return fleet.preflight_coverage, fleet.report, fakes.environments.warm_calls
        finally:
            fleet.close()

    coverage, report, calls = await asyncio.to_thread(run)
    assert coverage is not None and not coverage.complete
    assert report.phases["preflight_images"][0] == 1
    assert calls == 2, "one WarmImage call per image, one pass"
    assert report.environment["substrate"]["preflight"]["run_id"] == coverage.run_id


async def test_teardown_ends_the_reuse_of_a_complete_pass(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True,
                      node_selector={"ate-env/domain": "d1"}, strategy="none")
    # The async fleet tears down outside run(): a complete pass serves the
    # runs of one `async with`, and the teardown ends that.
    fleet = AsyncSandboxFleet(cfg)
    fleet.load_tasks(_tasks())
    async with fleet:
        one = fakes.environments.warm_calls
        await fleet.run(lambda task, handle: task.id, concurrency=2)
        await fleet.run(lambda task, handle: task.id, concurrency=2)
        assert fakes.environments.warm_calls == one, "a complete pass was repeated"
    await fleet.run(lambda task, handle: task.id, concurrency=2)
    assert fakes.environments.warm_calls > one, "the pass was reused across a teardown"
    fleet.close()

    # The sync strategies tear down at the end of run(): each run() warms.
    def runs():
        sync = SandboxFleet(cfg)
        try:
            sync.load_tasks(_tasks())
            sync.run(_process, concurrency=2)
            first = fakes.environments.warm_calls
            sync.run(_process, concurrency=2)
            return first, fakes.environments.warm_calls
        finally:
            sync.close()

    first, second = await asyncio.to_thread(runs)
    assert second > first


async def test_preflight_phases_carry_the_backend_label(fake_stack):
    client, fakes = fake_stack
    _pools(fakes, "tmpl-test")
    _ready_goldens(fakes, "tmpl-test", IMG_A, IMG_B)
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, template="tmpl-test", preflight=True)

    def run():
        fleet = SandboxFleet(cfg)
        seen: list[tuple[str, str]] = []
        obs = fleet.observer
        orig_phase, orig_record = obs.phase, obs.record_phase

        def phase(name, *, backend="-", family="-"):
            seen.append((name, backend))
            return orig_phase(name, backend=backend, family=family)

        def record_phase(name, dur, *, backend="-", family="-", status="ok"):
            seen.append((name, backend))
            return orig_record(name, dur, backend=backend, family=family, status=status)

        obs.phase = phase  # type: ignore[method-assign]
        obs.record_phase = record_phase  # type: ignore[method-assign]
        try:
            fleet.load_tasks(_tasks())
            fleet.run(_process, concurrency=2)
        finally:
            fleet.close()
        return seen

    seen = await asyncio.to_thread(run)
    names = {n for n, _ in seen}
    assert {"preflight_images", "ensure_template", "preflight_golden", "preflight_probe"} <= names
    off = sorted({(n, b) for n, b in seen if b != "substrate"})
    assert off == [], f"phases without backend=substrate: {off}"


def test_every_report_phase_is_in_the_observability_doc():
    from ate_env_rl.observability import RunReport

    doc = Path(__file__).resolve().parents[3] / "docs" / "observability.md"
    if not doc.exists():
        pytest.skip("docs/observability.md is not next to this checkout")
    text = doc.read_text(encoding="utf-8")
    missing = [p for p in RunReport.ORDER if f"`{p}`" not in text]
    assert missing == [], missing
