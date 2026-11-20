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

"""PreflightWarmer against the in-process fakes."""

from __future__ import annotations

import asyncio
import json
import time

import grpc
import pytest

from ate_env_rl import ImagePlacement, Observer, PreflightWarmer
from ate_env_rl._gen.ateenv.v1alpha import env_pb2
from ate_env_rl.errors import (
    FailedPreconditionError,
    InvalidArgumentError,
    PermissionDeniedError,
    SandboxUnavailableError,
)
from ate_env_rl.preflight import _transient, _warm_run_id

IMG_A = "registry.example/swebench/a@sha256:" + "a" * 64
IMG_B = "registry.example/swebench/b@sha256:" + "b" * 64
IMG_C = "registry.example/swebench/c@sha256:" + "c" * 64

PLAN = {
    "schema_version": "v1",
    "unpack_factor": 1,
    "replication": 1,
    "domains": [
        {"name": "d0", "selector": {"ate-env/domain": "d0"}, "nodes": 2, "node_cache_bytes": 0,
         "assigned_bytes": 2, "image_count": 2},
        {"name": "d1", "selector": {"ate-env/domain": "d1"}, "nodes": 2, "node_cache_bytes": 0,
         "assigned_bytes": 1, "image_count": 1},
    ],
    "images": [
        {"image": IMG_A, "domains": ["d0"], "bytes_compressed": 1, "bytes_unpacked": 1},
        {"image": IMG_B, "domains": ["d0", "d1"], "bytes_compressed": 1, "bytes_unpacked": 1},
    ],
}

NODES = {"d0": ["node-d0-1", "node-d0-2"], "d1": ["node-d1-1", "node-d1-2"]}


def _key(image: str) -> tuple[str, str]:
    digest = image.split("@sha256:", 1)[1]
    return ("ate-env", f"default-template-{digest[:12]}")


def _set_golden(fakes, image: str, state, *, tag: str = "", error: str = "") -> None:
    fakes.environments.image_templates[_key(image)] = env_pb2.ImageStatus(
        golden=state, golden_tag=tag, golden_error=error
    )


def _warmer(client, **kw) -> PreflightWarmer:
    opts = dict(run_id="pf-test", probes_per_node=1, image_concurrency=1, golden_poll=0.005,
                golden_timeout=1.0, probe_timeout=5.0)
    opts.update(kw)
    return PreflightWarmer(client, PLAN, **opts)


async def test_covers_every_node_of_every_domain(fake_stack, tmp_path):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_READY, tag="golden-7")
    obs = Observer()
    with obs.run("preflight") as report:
        coverage = await _warmer(client, observer=obs, plan_path="image-placement.json").run()

    assert coverage.complete, coverage.shortfall()
    assert [g.image for g in coverage.goldens] == [IMG_A, IMG_B]
    assert all(g.state == "ready" and g.tag == "golden-7" and g.template for g in coverage.goldens)
    want = {
        "d0": {IMG_A: ["node-d0-1", "node-d0-2"], IMG_B: ["node-d0-1", "node-d0-2"]},
        "d1": {IMG_B: ["node-d1-1", "node-d1-2"]},
    }
    for d in coverage.domains:
        assert d.complete and d.nodes_seen == NODES[d.name]
        assert {ic.image: ic.nodes for ic in d.images} == want[d.name]
        for ic in d.images:
            assert ic.complete and ic.rounds == 1 and ic.probes == 2 and ic.failed == 0
    # Every probe was deleted; each call carried its domain's selector, the
    # pass's knobs and its own run id.
    assert fakes.environments.environments == {}
    reqs = fakes.environments.warm_requests
    assert len(reqs) == 3 and all(r.worker_selector["ate-env/domain"] in NODES for r in reqs)
    assert {r.run_id for r in reqs} == {"pf-test-0-0-1", "pf-test-0-1-1", "pf-test-1-1-1"}
    assert all(r.nodes == 2 and r.probes_per_node == 1 and r.max_rounds == 3 for r in reqs)
    assert all(r.probe_timeout.ToTimedelta().total_seconds() == 5.0 for r in reqs)
    # The observer saw the phases.
    assert report.phases["ensure_template"][0] == 2
    assert report.phases["preflight_golden"][0] == 2
    assert report.phases["preflight_probe"][0] == 6

    # The report writes in the loader's schema.
    out = coverage.write(tmp_path / "preflight-coverage.json")
    data = json.loads(out.read_text())
    assert data["schema_version"] == "v1" and data["run_id"] == "pf-test"
    assert data["plan"] == "image-placement.json" and data["complete"] is True
    # The keys of the Go loader's preflight-coverage.json (schema v1), fixed
    # here so the check does not depend on the Go fixture being on the branch.
    assert set(data) == {"schema_version", "run_id", "started_at", "finished_at", "plan",
                         "goldens", "domains", "complete"}
    assert set(data["goldens"][0]) == {"image", "template", "state", "tag", "wait_ms"}
    assert set(data["domains"][0]) == {"name", "nodes", "nodes_seen", "images", "complete"}
    assert set(data["domains"][0]["images"][0]) == {"image", "nodes", "wanted", "probes",
                                                   "failed", "rounds", "complete"}
    assert data["started_at"].endswith("Z") and data["finished_at"] >= data["started_at"]
    for g in data["goldens"]:
        assert g["state"] in {"ready", "none", "pending", "failed"} and g["wait_ms"] >= 0
    for d in data["domains"]:
        assert isinstance(d["nodes"], int) and isinstance(d["complete"], bool)
        for ic in d["images"]:
            assert all(isinstance(ic[k], int) for k in ("wanted", "probes", "failed", "rounds"))
            assert isinstance(ic["complete"], bool) and isinstance(ic["nodes"], list)


async def test_streams_ready_goldens_and_gives_up_on_a_stalled_one(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    # a (first in plan order) never gets its golden; b is ready at once.
    _set_golden(fakes, IMG_B, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    coverage = await _warmer(client, golden_timeout=0.1).run()
    created = [r.rootfs_image for r in fakes.environments.warm_requests]

    assert not coverage.complete
    assert coverage.shortfall() == "1 goldens not ready"
    a = next(g for g in coverage.goldens if g.image == IMG_A)
    assert a.state == "pending" and "without progress" in a.error
    # b was covered in d0 before a's golden gave up; a was still probed after,
    # because a boot fills the layers too.
    first_a = created.index(IMG_A)
    assert created[:first_a].count(IMG_B) >= 2
    assert all(d.complete for d in coverage.domains)


async def test_golden_timeout_shorter_than_the_poll_interval_ends_the_wait_on_time(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_B, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # a's golden never comes. The poll interval (10 s, the default) is far
    # above the timeout: the wait ends at the timeout, not at the first poll.
    started = time.monotonic()
    coverage = await _warmer(client, golden_poll=10.0, golden_timeout=0.2).run()
    elapsed = time.monotonic() - started
    assert 0.2 <= elapsed < 2.0, elapsed
    a = next(g for g in coverage.goldens if g.image == IMG_A)
    assert a.state == "pending" and "without progress" in a.error
    # The last poll is at the deadline: a golden that became ready during the
    # sleep is seen, not given up on.
    fakes.environments.environments.clear()
    fakes.environments.image_templates.pop(_key(IMG_A), None)
    run = asyncio.create_task(_warmer(client, golden_poll=10.0, golden_timeout=0.3).run())
    await asyncio.sleep(0.1)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    coverage = await asyncio.wait_for(run, 5)
    assert coverage.complete


async def test_golden_failure_and_disabled_goldens(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # b's bake fails after the warmer ensured it.
    run = asyncio.create_task(_warmer(client, golden_timeout=2.0).run())
    await asyncio.sleep(0.05)
    _set_golden(fakes, IMG_B, env_pb2.GOLDEN_SNAPSHOT_STATE_FAILED, error="GoldenActorCrashed: boom")
    coverage = await run
    b = next(g for g in coverage.goldens if g.image == IMG_B)
    assert b.state == "failed" and "boom" in b.error
    assert not coverage.complete and all(d.complete for d in coverage.domains)

    # Goldens disabled on the base template: none, no wait, complete.
    fakes.environments.environments.clear()
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_NONE)
    coverage = await _warmer(client).run()
    assert coverage.complete
    assert all(g.state == "none" and not g.error and g.template for g in coverage.goldens)


async def test_short_coverage_and_probe_failures(fake_stack):
    client, fakes = fake_stack
    # Every probe of a domain lands on its first node. (The domain has both
    # nodes: the server refuses a request for more nodes than it lists.)
    fakes.environments.domain_nodes = dict(NODES)
    fakes.environments.warm_same_node = True
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    coverage = await _warmer(client, max_rounds=2).run()
    fakes.environments.warm_same_node = False
    assert not coverage.complete
    assert coverage.shortfall() == "3 (image, domain) pairs short of their nodes"
    for d in coverage.domains:
        for ic in d.images:
            assert not ic.complete and ic.rounds == 2 and ic.probes == 4 and len(ic.nodes) == 1
    assert fakes.environments.environments == {}

    # A domain with no nodes behind it: the probes report no node and fail.
    fakes.environments.domain_nodes = {"d1": NODES["d1"]}
    coverage = await _warmer(client, domains=["d0"]).run()
    d0 = coverage.domains[0]
    assert not d0.complete and d0.images[0].failed == 2 and "no node" in d0.images[0].error
    # The first round failed entirely, so the warmer stopped there.
    assert d0.images[0].rounds == 1


async def test_subsets_concurrency_cap_and_validation(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # Only d1 wanted: a, which no selected domain holds, is not waited for.
    w = _warmer(client, domains=["d1"])
    assert w.images == [IMG_B] and [d.name for d in w.domains] == ["d1"]
    coverage = await w.run()
    assert coverage.complete and [g.image for g in coverage.goldens] == [IMG_B]

    # node_slots caps the concurrency and bounds the probes per node of its
    # own domain only.
    plan = json.loads(json.dumps(PLAN))
    plan["domains"][0]["node_slots"] = 3
    w = PreflightWarmer(client, plan, probes_per_node=2, image_concurrency=8)
    assert w._image_width(w.domains[0]) == 1 and w._image_width(w.domains[1]) == 8
    w4 = PreflightWarmer(client, plan, probes_per_node=4)
    assert w4._probes_for(w4.domains[0]) == 3 and w4._probes_for(w4.domains[1]) == 4

    for bad in (
        dict(images=[IMG_C]),
        dict(domains=["d9"]),
        dict(images=[IMG_A], domains=["d1"]),
        dict(run_id="Not Valid"),
        dict(probes_per_node=0),
        dict(golden_timeout=0),
        # ate-env-api's wire bounds, and non-finite timeouts.
        dict(probes_per_node=9),
        dict(max_rounds=11),
        dict(probe_timeout=4e-7),
        dict(probe_timeout=1e15),
        dict(probe_timeout=float("nan")),
        dict(golden_timeout=float("inf")),
        dict(golden_poll=float("nan")),
        # A bool is not a number of seconds, and a string is not a number
        # (a TypeError until the check) nor a bool ("false" is truthy).
        dict(golden_timeout=True),
        dict(golden_poll=True),
        dict(golden_timeout="5"),
        dict(golden_poll=None),
        dict(golden_wait="false"),
        dict(golden_wait=0),
        dict(probe_without_golden="false"),
        dict(probe_without_golden=None),
    ):
        with pytest.raises(ValueError):
            PreflightWarmer(client, PLAN, **bad)
    # An int is a number of seconds.
    assert PreflightWarmer(client, PLAN, golden_timeout=5, golden_poll=1).golden_timeout == 5
    with pytest.raises(ValueError, match="schema_version"):
        ImagePlacement.from_dict({"schema_version": "v0"})
    with pytest.raises(ValueError, match="pinned by digest"):
        ImagePlacement.from_dict({**PLAN, "images": [{"image": "reg/x:latest", "domains": ["d0"]}]})


async def test_cancel_mid_round_deletes_the_probes_and_returns_at_once(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # b's golden never comes, so a cancel also has to stop the golden poller
    # instead of waiting for golden_timeout. The server holds a's first round
    # of probes, so the cancel lands mid-round.
    fakes.environments.warm_hold = 30.0
    warmer = _warmer(client, golden_timeout=30.0, probe_timeout=30.0)
    run = asyncio.create_task(warmer.run())
    deadline = time.monotonic() + 5
    while not fakes.environments.environments and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    assert fakes.environments.environments, "the probes of a's first round exist"
    started = time.monotonic()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert time.monotonic() - started < 5, "the cancel did not wait for golden_timeout"
    # The server deletes the broken call's probes once the stream is gone,
    # on its own clock: shortly after the cancel returns.
    deadline = time.monotonic() + 5
    while fakes.environments.environments and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    assert fakes.environments.environments == {}, "every probe was deleted"
    # What the run covered until then is kept, with the golden it was still
    # waiting for recorded as cancelled.
    cov = warmer.coverage
    assert cov is not None and cov.finished_at is not None
    assert {g.image: (g.state, g.error) for g in cov.goldens}[IMG_B] == ("pending", "cancelled")
    assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
                and "preflight" in repr(t.get_coro())], "no warmer task is left pending"


async def test_cancel_during_the_golden_wait_returns_at_once(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    # Neither golden comes: the run sits in the golden wait until cancelled.
    run = asyncio.create_task(_warmer(client, golden_timeout=30.0).run())
    await asyncio.sleep(0.05)
    started = time.monotonic()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert time.monotonic() - started < 5
    assert fakes.environments.environments == {}


async def test_second_round_covers_the_node_the_first_missed(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    # The first two creates of a domain land on its first node, as a
    # scheduler that favours a warm node does; the next round spreads.
    fakes.environments.first_node_creates = 2
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"]).run()
    assert coverage.complete
    ic = coverage.domains[0].images[0]
    assert ic.rounds == 2 and ic.probes == 4 and ic.nodes == ["node-d0-1", "node-d0-2"]


async def test_a_cut_off_warm_call_is_repeated_and_a_refused_one_is_not(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # The api server restarts twice under the call: the third call runs.
    fakes.environments.warm_unavailable_first = 2
    warmer = _warmer(client, images=[IMG_A], domains=["d0"])
    warmer._retry_backoff = 0.001
    coverage = await warmer.run()
    assert coverage.complete and fakes.environments.warm_calls == 3
    ic = coverage.domains[0].images[0]
    assert ic.rounds == 1 and ic.failed == 0 and ic.nodes == NODES["d0"]

    # A call cut off after a round that covered one node of two (an api
    # server restart): the old probes stay, the repeat sends its own run id,
    # so no name collides.
    fakes.environments.warm_calls = 0
    fakes.environments.warm_requests.clear()
    fakes.environments.domain_nodes = {**NODES, "d0": ["node-d0-1", "node-d0-1", "node-d0-2", "node-d0-2"]}
    fakes.environments.warm_cut_after_create = 1
    warmer = _warmer(client, images=[IMG_A], domains=["d0"])
    warmer._retry_backoff = 0.001
    coverage = await warmer.run()
    assert fakes.environments.warm_calls == 2
    assert [r.run_id for r in fakes.environments.warm_requests] == ["pf-test-0-0-1", "pf-test-0-0-2"]
    # The coverage holds; the cut-off call's probes keep the pass from clean
    # (test_a_cut_off_call_lists_the_probes_it_streamed).
    assert coverage.complete and not coverage.clean
    assert len(fakes.environments.environments) == 2, "the cut-off call's probes stay"
    fakes.environments.environments.clear()
    fakes.environments.domain_nodes = dict(NODES)

    # A refusal is final: the pair is short with the server's reason.
    fakes.environments.warm_calls = 0
    fakes.environments.warm_fail = (
        grpc.StatusCode.INVALID_ARGUMENT,
        "probes_per_node 4 exceeds the 2 probes per node this server runs on one worker selector at once",
    )
    warmer = _warmer(client, images=[IMG_A], domains=["d0"])
    warmer._retry_backoff = 0.001
    coverage = await warmer.run()
    assert not coverage.complete and fakes.environments.warm_calls == 1
    ic = coverage.domains[0].images[0]
    assert "probes_per_node 4 exceeds" in ic.error and ic.nodes == [] and ic.probes == 0


async def test_the_repeat_backoff_is_cut_to_the_budget(fake_stack):
    """The repeats last for probe_timeout from the first failure: a backoff
    longer than what is left of it is cut to that, so a short probe_timeout
    still gets its repeat."""
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    fakes.environments.warm_unavailable_first = 1
    warmer = _warmer(client, images=[IMG_A], domains=["d0"], probe_timeout=0.05)
    warmer._retry_backoff = 30.0
    started = time.monotonic()
    coverage = await asyncio.wait_for(warmer.run(), 10)
    assert fakes.environments.warm_calls == 2, "one repeat inside the 50 ms budget"
    assert coverage.clean
    assert time.monotonic() - started < 5


async def test_a_cut_off_call_lists_the_probes_it_streamed(fake_stack):
    """A warm call that breaks after it streamed probes never says which of
    them the server deleted (the coverage comes last, and an api server that
    restarted deleted none): the pass lists them and is not clean."""
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    fakes.environments.warm_cut_after_create = 1
    warmer = _warmer(client, images=[IMG_A], domains=["d0"])
    warmer._retry_backoff = 0.001
    coverage = await warmer.run()
    left = ["pf-test-0-0-1-1-0", "pf-test-0-0-1-1-1"]
    assert sorted(i for _, i in fakes.environments.environments) == left, "the server left the round"
    ic = coverage.domains[0].images[0]
    # The stream showed both nodes: complete, and no repeat.
    assert ic.complete and ic.nodes == NODES["d0"] and ic.probes == 2 and ic.rounds == 1
    assert fakes.environments.warm_calls == 1
    assert "api server restarted" in ic.error
    assert ic.undeleted_probes == left and coverage.undeleted_probes == left
    assert coverage.complete and not coverage.clean
    assert "2 probes left undeleted" in coverage.shortfall()


async def test_the_fake_refuses_what_the_server_refuses(fake_stack):
    """The fake ate-env-api refuses the WarmImage requests the server refuses,
    so a pre-flight test cannot pass on a request a real server turns down.
    The Go fake has the same table
    (TestFakeWarmImageRefusesWhatTheServerRefuses)."""
    client, fakes = fake_stack
    fakes.environments.domain_nodes = {**NODES, "wide": [f"n{i}" for i in range(129)]}
    d0, wide = {"ate-env/domain": "d0"}, {"ate-env/domain": "wide"}
    ok = dict(worker_selector=d0, nodes=2, probes_per_node=2, max_rounds=3, probe_timeout=1.0,
              run_id="pf-ok-0-0-1")
    assert (await client.warm_image(IMG_A, **ok)).complete
    for change, cap, want in [
        (dict(probes_per_node=9), 8, "probes_per_node must be between 1 and 8"),
        (dict(probes_per_node=3), 2, "exceeds the 2 probes per node"),
        (dict(probes_per_node=5), 4, "exceeds the 4 probes per node"),
        (dict(max_rounds=11), 2, "max_rounds must be between 1 and 10"),
        (dict(run_id="a" * 21), 2, "run_id must match"),
        (dict(run_id="PF_1"), 2, "run_id must match"),
        (dict(run_id="pf-"), 2, "run_id must match"),
        (dict(nodes=3), 2, "nodes 3 exceeds the 2 nodes"),
        (dict(worker_selector=wide, nodes=129), 2, "exceed the 256 probes a round may start"),
        (dict(worker_selector=wide, nodes=0), 2, "exceed the 256 probes a round may start"),
    ]:
        fakes.environments.warm_max_probes_per_node = cap
        with pytest.raises(InvalidArgumentError, match=want):
            await client.warm_image(IMG_A, **{**ok, **change})
        assert fakes.environments.environments == {}, "a refused call creates no probe"
    # A raised cap takes what it allows.
    fakes.environments.warm_max_probes_per_node = 4
    assert (await client.warm_image(IMG_A, **{**ok, "probes_per_node": 4, "run_id": "pf-ok-0-0-2"})).complete


async def test_a_cut_off_call_is_logged_with_its_run_id(fake_stack, caplog):
    """The probes of a call an api server restart cut off are named after the
    call's run id and nothing else records them, so every log line about a
    cut-off call carries it: the repeat, the give-up, the call not repeated."""
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)

    async def logs(**kw) -> list[str]:
        caplog.clear()
        warmer = _warmer(client, images=[IMG_A], domains=["d0"], **kw)
        warmer._retry_backoff = 0.001
        with caplog.at_level("WARNING", logger="ate_env_rl.preflight"):
            await warmer.run()
        return [r.getMessage() for r in caplog.records]

    fakes.environments.warm_unavailable_first = 1
    assert any("repeating" in m and "run id pf-test-0-0-1," in m for m in await logs())

    fakes.environments.warm_calls = 0
    fakes.environments.warm_fail = (grpc.StatusCode.UNAVAILABLE, "down")
    lines = await logs(probe_timeout=0.02)
    last = f"run id pf-test-0-0-{fakes.environments.warm_calls},"
    assert any("giving up" in m and last in m for m in lines), lines
    fakes.environments.warm_fail = None

    fakes.environments.warm_cut_after_create = 1
    assert any("not repeated" in m and "run id pf-test-0-0-1," in m for m in await logs())


async def test_the_attempts_of_a_repeated_call_add_up(fake_stack):
    """A node the cut-off call showed stays covered when the repeat lands
    elsewhere, and the counters are the sum of the attempts."""
    client, fakes = fake_stack
    # Four workers on two nodes: the first two probes land on node-d0-1 (the
    # cut-off call), the next two on node-d0-2 (the repeat).
    fakes.environments.domain_nodes = {"d0": ["node-d0-1", "node-d0-1", "node-d0-2", "node-d0-2"]}
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    fakes.environments.warm_cut_after_create = 1
    warmer = _warmer(client, images=[IMG_A], domains=["d0"], max_rounds=1)
    warmer._retry_backoff = 0.001
    coverage = await warmer.run()
    assert fakes.environments.warm_calls == 2
    ic = coverage.domains[0].images[0]
    assert ic.complete and ic.nodes == ["node-d0-1", "node-d0-2"]
    assert ic.probes == 4 and ic.rounds == 2 and ic.failed == 0
    assert ic.undeleted_probes == ["pf-test-0-0-1-1-0", "pf-test-0-0-1-1-1"]
    assert coverage.complete and not coverage.clean


def test_run_id_leaves_room_for_the_indexes(fake_stack):
    client, _ = fake_stack
    # PLAN has two domains and two images: "-1-1-9" is appended at most.
    assert PreflightWarmer(client, PLAN, run_id="a" * 14).run_id == "a" * 14
    with pytest.raises(ValueError, match="indexes"):
        PreflightWarmer(client, PLAN, run_id="a" * 15)

    # The indexes are in base 36; the names are the Go loader's
    # (TestPreflightWarmRunIDFitsTheServerLimit has the same table).
    for domain, image, attempt, want in [
        (0, 0, 1, "pf-abcdef-0-0-1"),
        (9, 10, 9, "pf-abcdef-9-a-9"),
        (35, 36, 2, "pf-abcdef-z-10-2"),
        (350, 36840, 9, "pf-abcdef-9q-sfc-9"),
        (46655, 1679615, 9, "pf-abcdef-zzz-zzzz-9"),
    ]:
        got = _warm_run_id("pf-abcdef", domain, image, attempt)
        assert got == want and len(got) <= 20

    def large(domains: int, images: int) -> dict:
        return {
            "schema_version": "v1",
            "domains": [
                {"name": f"d{i}", "selector": {"ate-env/domain": f"d{i}"}, "nodes": 2}
                for i in range(domains)
            ],
            "images": [
                {"image": f"registry.example/i{i}@sha256:{i:064x}", "domains": [f"d{i % domains}"]}
                for i in range(images)
            ],
        }

    # 351 domains and 36,841 images, the default run id: the last pair's call
    # was pf-<6>-350-36840-9 in decimal, 21 characters.
    w = PreflightWarmer(client, large(351, 36841))
    assert len(_warm_run_id(w.run_id, 350, 36840, 9)) <= 20
    # A subset is measured by its own pairs: with 37 domains a 14-character
    # run id fits domain 0 (-0-0-9) and not domain 36 (-10-0-9).
    plan = large(37, 37)
    assert PreflightWarmer(client, plan, run_id="a" * 14, domains=["d0"]).run_id == "a" * 14
    with pytest.raises(ValueError, match="indexes"):
        PreflightWarmer(client, plan, run_id="a" * 14)
    # A plan with no image is refused for that, not for the "-1" index its
    # length formatted (which a 14-character run id made one too long).
    with pytest.raises(ValueError, match="no image of the pass"):
        PreflightWarmer(client, {**PLAN, "images": []}, run_id="a" * 14)


def test_plan_domains_are_the_api_placement_domain() -> None:
    """The plan's domains and list_placement_domains() share one type."""
    from ate_env_rl import PlacementDomain, preflight

    selector = {"ate-env/domain": "a"}
    domain = {"name": "a", "selector": selector, "nodes": 2, "node_slots": 4}
    image = {"image": "img@sha256:" + "0" * 64, "domains": ["a"]}
    plan = ImagePlacement.from_dict(
        {"schema_version": "v1", "domains": [domain], "images": [image]}
    )
    want = PlacementDomain(name="a", selector=selector, nodes=2, node_slots=4)
    assert preflight.PlacementDomain is PlacementDomain
    assert [type(d) for d in plan.domains] == [PlacementDomain]
    assert plan.domains[0] == want
    assert plan.domains[0].worker_namespace == "" and plan.domains[0].workers == 0


async def test_a_colliding_name_is_left_alone(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # Someone else holds the name of our first probe.
    taken = "pf-test-0-0-1-1-0"
    await client.create(taken, rootfs_image=IMG_A)
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"], max_rounds=1).run()
    ic = coverage.domains[0].images[0]
    assert ic.probes == 2 and ic.failed == 1 and "already exists" in ic.error
    assert ("ate-env", taken) in fakes.environments.environments, "the environment that is not ours survived"
    assert len(fakes.environments.environments) == 1, "our own probe was deleted"


def test_default_run_id_is_unique_per_pass(fake_stack):
    client, _ = fake_stack
    a = PreflightWarmer(client, PLAN)
    b = PreflightWarmer(client, PLAN)
    for w in (a, b):
        assert w.run_id.startswith("pf-") and len(w.run_id) == 9
    assert a.run_id != b.run_id
    with pytest.raises(ValueError):
        PreflightWarmer(client, PLAN, run_id="Not Valid")


async def test_undeleted_probes_are_reported(fake_stack, tmp_path):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    stuck = "pf-test-0-0-1-1-1"
    fakes.environments.delete_fail = {stuck}
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"]).run()
    ic = coverage.domains[0].images[0]
    assert ic.complete and ic.undeleted_probes == [stuck]
    assert coverage.complete and not coverage.clean  # the coverage held; clean says a probe is left
    assert coverage.undeleted_probes == [stuck]
    assert "1 probes left undeleted" in coverage.shortfall()
    assert ("ate-env", stuck) in fakes.environments.environments
    # The module's example gates on clean, the flag that sees this probe.
    import ate_env_rl.preflight as preflight_module

    assert "if not coverage.clean:" in (preflight_module.__doc__ or "")
    assert "if not coverage.complete:" not in (preflight_module.__doc__ or "")
    data = json.loads(coverage.write(tmp_path / "c.json").read_text())
    assert data["domains"][0]["images"][0]["undeleted_probes"] == [stuck]
    assert "waited_ms" not in data["domains"][0]["images"][0]

def test_transient_classifies_the_errors():
    class _Raw(grpc.RpcError):
        def __init__(self, code):
            self._code = code

        def code(self):
            return self._code

    class _Mapped(Exception):
        def __init__(self, code):
            self.code = code

    repeated = [
        SandboxUnavailableError("down"),
        _Raw(grpc.StatusCode.DEADLINE_EXCEEDED),
        _Mapped(grpc.StatusCode.UNKNOWN),
        _Mapped(grpc.StatusCode.INTERNAL),
        _Raw(grpc.StatusCode.ABORTED),
    ]
    final = [
        InvalidArgumentError("bad"),
        PermissionDeniedError("no"),
        FailedPreconditionError("no template"),
        _Raw(grpc.StatusCode.ALREADY_EXISTS),
        _Mapped(grpc.StatusCode.RESOURCE_EXHAUSTED),
        RuntimeError("plain"),
    ]
    assert all(_transient(e) for e in repeated)
    assert not any(_transient(e) for e in final)


def test_plan_keeps_workers_and_namespace_from_the_api() -> None:
    plan = json.loads(json.dumps(PLAN))
    plan["domains"][0].update({"workers": 8, "worker_namespace": "ate-system", "node_slots": 6})
    loaded = ImagePlacement.from_dict(plan)
    d0 = loaded.domain("d0")
    assert d0 is not None and d0.workers == 8 and d0.worker_namespace == "ate-system" and d0.node_slots == 6
    d1 = loaded.domain("d1")
    assert d1 is not None and d1.workers == 0 and d1.worker_namespace == ""
    plan["domains"][0]["workers"] = -1
    with pytest.raises(ValueError, match="workers"):
        ImagePlacement.from_dict(plan)
    # A value of the wrong type is refused, a falsy one too: it is not read
    # as "absent". The schema allows an integer and a string, nothing else.
    for field, bad in [
        *(("workers", v) for v in (False, True, "", "8", [], {}, 0.0, 8.0, None)),
        *(("worker_namespace", v) for v in (123, 0, True, False, ["ate-system"], {}, None)),
    ]:
        plan = json.loads(json.dumps(PLAN))
        plan["domains"][0][field] = bad
        with pytest.raises(ValueError, match=f"{field} must be"):
            ImagePlacement.from_dict(plan)


def test_plan_refuses_domain_fields_of_the_wrong_type() -> None:
    # The same rule for the fields the plan always had: no falsy value of the
    # wrong type is read as "absent", and no wrong type reaches the domain.
    for field, bad, match in [
        *(("node_slots", v, "node_slots must be") for v in (False, True, "", [], {}, 0.0, None)),
        *(("name", v, "a unique name is required") for v in (123, True, ["d0"], None)),
        *(("selector", v, "a selector is required") for v in ({"k": 1}, {"k": None}, {1: "v"}, [["k", "v"]], "ab", 7)),
    ]:
        plan = json.loads(json.dumps(PLAN))
        plan["domains"][0][field] = bad
        with pytest.raises(ValueError, match=match):
            ImagePlacement.from_dict(plan)
    # An absent node_slots is still 0, "unknown".
    d0 = ImagePlacement.from_dict(PLAN).domain("d0")
    assert d0 is not None and d0.node_slots == 0


async def test_cancelled_run_retains_partial_coverage(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    _set_golden(fakes, IMG_B, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # Mid-call: a's first round is held on the server after its two probes
    # went out, one per node. The cancel keeps the domain with what the
    # stream had shown: both nodes, so the image is complete and carries no
    # error (not "cancelled").
    fakes.environments.warm_hold = 30.0
    # The cancel waits for the client to have read both probe events: the
    # probes exist on the server before their events reach the stream.
    shown = 0
    both_shown = asyncio.Event()
    original_warm = client.warm_image

    async def warm_image(image, *, on_probe=None, **kw):
        def counted(p):
            nonlocal shown
            if on_probe is not None:
                on_probe(p)
            shown += 1
            if shown == 2:
                both_shown.set()

        return await original_warm(image, on_probe=counted, **kw)

    client.warm_image = warm_image  # type: ignore[method-assign]
    warmer = _warmer(client, images=[IMG_A], domains=["d0"], probe_timeout=30.0)
    run = asyncio.create_task(warmer.run())
    await asyncio.wait_for(both_shown.wait(), 5)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    client.warm_image = original_warm  # type: ignore[method-assign]
    cov = warmer.coverage
    assert cov is not None and [d.name for d in cov.domains] == ["d0"]
    ic = cov.domains[0].images[0]
    # The stream had shown both nodes before the cancel: the pair counts as
    # complete from what the stream showed, with its probes.
    assert ic.image == IMG_A and ic.rounds == 1 and ic.probes == 2 and ic.complete
    assert ic.error == "" and ic.nodes == NODES["d0"]
    # A cancelled call reports no coverage: the server sweeps its probes and
    # nothing is listed as undeleted here; the run itself is not clean.
    assert cov.undeleted_probes == [] and not cov.clean and not cov.complete
    deadline = time.monotonic() + 5
    while fakes.environments.environments and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    assert fakes.environments.environments == {}

    # One of two nodes: both probes of a's round land on the first node and
    # the round is held. The cancel keeps that node; the image is short of
    # the second and marked cancelled.
    fakes.environments.warm_same_node = True
    shown = 0
    both_shown.clear()
    client.warm_image = warm_image  # type: ignore[method-assign]
    warmer = _warmer(client, images=[IMG_A], domains=["d0"], probe_timeout=30.0)
    run = asyncio.create_task(warmer.run())
    await asyncio.wait_for(both_shown.wait(), 5)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    client.warm_image = original_warm  # type: ignore[method-assign]
    fakes.environments.warm_same_node = False
    cov = warmer.coverage
    assert cov is not None and [d.name for d in cov.domains] == ["d0"]
    ic = cov.domains[0].images[0]
    assert ic.nodes == ["node-d0-1"] and not ic.complete and ic.error == "cancelled"
    assert ic.rounds == 1 and ic.probes == 2 and ic.failed == 0
    assert cov.domains[0].nodes_seen == ["node-d0-1"] and not cov.domains[0].complete
    assert cov.undeleted_probes == [] and not cov.clean
    deadline = time.monotonic() + 5
    while fakes.environments.environments and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    assert fakes.environments.environments == {}

    # Second image: a completes, then the cancel lands in b's call before
    # its round is created; a's complete coverage stays next to b's cut one.
    # Only b is held, and before its round: a runs through.
    fakes.environments.warm_hold_images = {IMG_B}
    fakes.environments.warm_hold_before_images = {IMG_B}
    fakes.environments.warm_requests.clear()
    warmer = _warmer(client, domains=["d0"], probe_timeout=30.0)
    run = asyncio.create_task(warmer.run())
    deadline = time.monotonic() + 5
    while not any(r.rootfs_image == IMG_B for r in fakes.environments.warm_requests) and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    await asyncio.sleep(0.02)  # b's call is held before it creates its round: no probe exists
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    cov = warmer.coverage
    assert cov is not None and [d.name for d in cov.domains] == ["d0"]
    by_image = {ic.image: ic for ic in cov.domains[0].images}
    assert by_image[IMG_A].complete and by_image[IMG_A].nodes == NODES["d0"]
    assert not by_image[IMG_B].complete and by_image[IMG_B].error == "cancelled"
    assert by_image[IMG_B].probes == 0 and by_image[IMG_B].nodes == []


async def test_not_found_on_delete_is_gone(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    fakes.environments.delete_not_found = {"pf-test-0-0-1-1-1"}
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"]).run()
    assert coverage.complete and coverage.clean and coverage.undeleted_probes == []
    assert fakes.environments.environments == {}
