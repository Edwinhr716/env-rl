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

"""`PreflightWarmer`: fill the node caches an image placement plan describes.

A placement plan (``image-placement.json`` from ``swebench-loader
plan-placement``) assigns each task image to placement domains: worker pools a
per-environment worker selector addresses. Before a run, the warmer puts every
image's layers and golden snapshot on every node of its domains, so the run's
first boot of an image on a node is a restore, not a pull::

    plan = ImagePlacement.from_file("image-placement.json")
    warmer = PreflightWarmer(client, plan, run_id="pf-7")
    coverage = await warmer.run()
    coverage.write("preflight-coverage.json")
    if not coverage.clean:
        print(coverage.shortfall())

It ensures each image's derived template, which starts the golden bake, then
warms every domain in parallel: as soon as an image's golden has settled it
runs one ate-env-api ``WarmImage`` call for the image in the domain
(``Client.warm_image``), in which the server creates probe environments with
the domain's worker selector, round after round, until every node of the
domain has run the image once; a probe is held until it runs, so the layers
and the golden snapshot are on its node, its node is read and it is deleted.
The coverage report has the same shape as the Go loader's
``preflight-coverage.json`` (``schema_version: v1``), so ``swebench-loader
report -preflight-coverage`` reads it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import pathlib
import re
import secrets
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import grpc

from .client import DEFAULT_ATESPACE, Client
from .config import warm_knob_error
from .errors import SandboxUnavailableError
from .observability import Observer
from .types import GoldenSnapshotState, ImageStatus, PlacementDomain, WarmProbe

logger = logging.getLogger("ate_env_rl.preflight")

SCHEMA_VERSION = "v1"

_RUN_ID = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
# ate-env-api's limit on a WarmImage run id: the pass's run id plus the
# -<domain>-<image>-<attempt> indexes it appends must fit. _MAX_WARM_ATTEMPTS
# bounds the repeats of a cut-off call (and the attempt index to one digit).
_MAX_RUN_ID = 20
_MAX_WARM_ATTEMPTS = 9
_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
# The gRPC codes a control plane under load hands out for a while; a warm
# call that ends in one is repeated.
_TRANSIENT = frozenset(
    (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.ABORTED,
    )
)


# ---------------------------------------------------------------------------
# The plan


# A plan's domains are the API's PlacementDomain (types.py), as
# list_placement_domains() returns them; the file carries name, selector,
# nodes and node_slots.


@dataclass(frozen=True)
class PlacementEntry:
    """One image and the domains the plan assigns it to."""

    image: str
    domains: tuple[str, ...]


@dataclass(frozen=True)
class ImagePlacement:
    """An image placement plan: ``image-placement.json`` (``schema_version: v1``).

    Only the parts the warmer uses are kept; the planner's budgets travel in
    the file untouched.
    """

    domains: tuple[PlacementDomain, ...]
    images: tuple[PlacementEntry, ...]

    @classmethod
    def from_dict(cls, plan: Mapping[str, Any]) -> ImagePlacement:
        version = plan.get("schema_version")
        if version != SCHEMA_VERSION:
            raise ValueError(f"image placement: schema_version {version!r} is not {SCHEMA_VERSION!r}")
        domains: list[PlacementDomain] = []
        names: set[str] = set()
        for i, d in enumerate(plan.get("domains") or []):
            name = d.get("name")
            labels = d.get("selector")
            nodes = d.get("nodes", 0)
            slots = d.get("node_slots", 0)
            if not isinstance(name, str) or not name or name in names:
                raise ValueError(f"image placement: domains[{i}]: a unique name is required")
            if (
                not isinstance(labels, Mapping)
                or not labels
                or not all(isinstance(k, str) and isinstance(v, str) and k and v for k, v in labels.items())
            ):
                raise ValueError(f"image placement: domain {name!r}: a selector is required")
            selector = dict(labels)
            if not isinstance(nodes, int) or isinstance(nodes, bool) or nodes < 1:
                raise ValueError(f"image placement: domain {name!r}: nodes must be an int >= 1")
            if not isinstance(slots, int) or isinstance(slots, bool) or slots < 0:
                raise ValueError(f"image placement: domain {name!r}: node_slots must be an int >= 0")
            # Read as they are: an "or" default would turn a falsy value of
            # the wrong type (false, "", [], 0.0) into "absent".
            workers = d.get("workers", 0)
            namespace = d.get("worker_namespace", "")
            if not isinstance(workers, int) or isinstance(workers, bool) or workers < 0:
                raise ValueError(f"image placement: domain {name!r}: workers must be an int >= 0")
            if not isinstance(namespace, str):
                raise ValueError(f"image placement: domain {name!r}: worker_namespace must be a string")
            names.add(name)
            domains.append(
                PlacementDomain(
                    name=name,
                    worker_namespace=namespace,
                    selector=selector,
                    nodes=nodes,
                    workers=workers,
                    node_slots=slots,
                )
            )
        if not domains:
            raise ValueError("image placement: at least one domain is required")
        images: list[PlacementEntry] = []
        seen: set[str] = set()
        for i, e in enumerate(plan.get("images") or []):
            image = e.get("image") or ""
            assigned = tuple(e.get("domains") or ())
            if not _DIGEST.search(image):
                raise ValueError(f"image placement: images[{i}]: {image!r} is not pinned by digest")
            if image in seen:
                raise ValueError(f"image placement: images[{i}]: {image!r} is listed twice")
            if not assigned or any(d not in names for d in assigned):
                raise ValueError(f"image placement: {image!r}: every domain must be in the plan")
            seen.add(image)
            images.append(PlacementEntry(image, assigned))
        return cls(tuple(domains), tuple(images))

    @classmethod
    def from_file(cls, path: str | pathlib.Path) -> ImagePlacement:
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    def domain(self, name: str) -> PlacementDomain | None:
        return next((d for d in self.domains if d.name == name), None)

    def entry(self, image: str) -> PlacementEntry | None:
        return next((e for e in self.images if e.image == image), None)


# ---------------------------------------------------------------------------
# The coverage report


@dataclass
class GoldenRecord:
    """One image's derived template and golden snapshot after the pre-flight.

    ``state`` is ``ready``, ``none`` (goldens are disabled on the base
    template), ``failed``, or ``pending``: the golden was still baking when the
    warmer gave up waiting, or the warmer did not wait for it (``golden_wait=False``;
    ``error`` is then ``golden not waited for``).
    """

    image: str
    template: str = ""
    state: str = ""
    tag: str = ""
    error: str = ""
    wait_ms: float = 0.0

    @property
    def settled(self) -> bool:
        """True when the run needs no more waiting: ready, or none by design."""
        return self.state == "ready" or (self.state == "none" and not self.error)


@dataclass
class ImageCoverage:
    """One image's coverage inside one domain."""

    image: str
    nodes: list[str] = field(default_factory=list)
    wanted: int = 0
    probes: int = 0
    failed: int = 0
    rounds: int = 0
    complete: bool = False
    error: str = ""
    # Probe environments whose delete failed, and the probes a warm call
    # streamed before it broke off (their delete is the server's and was
    # never confirmed); they hold a slot until swept.
    undeleted_probes: list[str] = field(default_factory=list)
    # Time the image's rounds waited for probe slots held by other passes,
    # when the warmer reports it (ate-env-api's WarmImage does).
    waited_ms: float = 0.0


@dataclass
class DomainCoverage:
    """One domain's result: the nodes its probes landed on and each image's coverage."""

    name: str
    nodes: int
    nodes_seen: list[str] = field(default_factory=list)
    images: list[ImageCoverage] = field(default_factory=list)
    complete: bool = True


@dataclass
class PreflightCoverage:
    """What a pre-flight did for a plan: ``preflight-coverage.json``.

    ``complete`` is True when every golden is ready (or none by design) and
    every (image, domain) pair reached its node count. A golden that the warmer
    did not wait for (``golden_wait=False``) and that is still baking makes it False.
    """

    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    plan: str = ""
    goldens: list[GoldenRecord] = field(default_factory=list)
    domains: list[DomainCoverage] = field(default_factory=list)
    complete: bool = False

    def shortfall(self) -> str:
        """What kept the pass from completing, in a sentence."""
        parts = []
        not_ready = sum(1 for g in self.goldens if not g.settled)
        if not_ready:
            parts.append(f"{not_ready} goldens not ready")
        short = sum(1 for d in self.domains for ic in d.images if not ic.complete)
        if short:
            parts.append(f"{short} (image, domain) pairs short of their nodes")
        undeleted = self.undeleted_probes
        if undeleted:
            parts.append(f"{len(undeleted)} probes left undeleted (first: {undeleted[0]})")
        return "; ".join(parts) or ("complete" if self.complete else "interrupted")

    @property
    def undeleted_probes(self) -> list[str]:
        """Every probe the pass could not delete, sorted; sweep them."""
        return sorted(p for d in self.domains for ic in d.images for p in ic.undeleted_probes)

    @property
    def clean(self) -> bool:
        """``complete`` and no probe left behind: the one flag to gate a run
        on. ``complete`` alone says the nodes are warm; a leftover probe
        holds a slot until swept (``undeleted_probes``)."""
        return self.complete and not self.undeleted_probes

    def to_dict(self) -> dict[str, Any]:
        """The report as JSON data, in the Go loader's schema."""
        out: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "started_at": _rfc3339(self.started_at),
            "finished_at": _rfc3339(self.finished_at or self.started_at),
        }
        if self.plan:
            out["plan"] = self.plan
        out["goldens"] = [_drop_empty(asdict(g), ("template", "tag", "error")) for g in self.goldens]
        out["domains"] = [
            {
                "name": d.name,
                "nodes": d.nodes,
                "nodes_seen": list(d.nodes_seen),
                "images": [
                    _drop_empty(asdict(ic), ("error", "undeleted_probes", "waited_ms"))
                    for ic in d.images
                ],
                "complete": d.complete,
            }
            for d in self.domains
        ]
        out["complete"] = self.complete
        return out

    def write(self, path: str | pathlib.Path) -> pathlib.Path:
        """Write the report as indented JSON and return its path."""
        p = pathlib.Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict(), indent=2, allow_nan=False) + "\n", encoding="utf-8")
        return p


def _rfc3339(t: datetime) -> str:
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _drop_empty(d: dict[str, Any], keys: Iterable[str]) -> dict[str, Any]:
    for k in keys:
        if not d.get(k):
            d.pop(k, None)
    return d


# ---------------------------------------------------------------------------
# The warmer


class _Golden:
    """One image's golden while the pass runs; the coverage work waits on it."""

    __slots__ = ("event", "ensured", "settled", "ok", "record")

    def __init__(self, image: str, now: float):
        self.event = asyncio.Event()
        self.ensured = now
        self.settled = False
        self.ok = False
        self.record = GoldenRecord(image=image)


class PreflightWarmer:
    """Fills the node caches an image placement plan describes.

    Args:
        client: the ``Client`` to drive: the warmer calls its
            ``image_status`` and ``warm_image``; ate-env-api creates, runs
            and deletes the probes.
        plan: the ``ImagePlacement`` (or its ``image-placement.json`` as a
            dict) to fill.
        atespace: the atespace of the probes and the derived templates.
        template_name, template_atespace: the base template the derived
            templates come from; the server's defaults when None.
        run_id: the prefix of the probe environment names: the pass appends
            the domain and image indexes in the plan, in base 36, and the
            attempt (``-<domain>-<image>-<attempt>``), the server the round and the
            slot, and a call's whole run id is at most 20 characters of
            ``[a-z0-9-]``. Empty gives ``pf-<6 random
            characters>``, unique per pass, so two passes in one atespace
            never share a probe name.
        probes_per_node: how many probes a round starts per node of a
            domain; a round holds its probes until all are placed, so the
            scheduler spreads them. A domain whose ``node_slots`` is below
            it gets ``node_slots`` probes per node. 1 to 8; ate-env-api
            also refuses more than its ``-warm-max-probes-per-node`` (2
            unless raised), which shows as short pairs with that reason.
        image_concurrency: how many images a domain warms at once, each its
            own ``warm_image`` call with its own rounds; a domain's
            ``node_slots`` caps it at ``node_slots // probes_per_node``.
            Across calls ate-env-api caps the probes in flight per node of
            one worker selector, so calls beyond that cap wait for slots on
            the server; the coverage's ``waited_ms`` sums the wait.
        max_rounds: the rounds per (image, domain) before the pair is
            reported short; 1 to 10, the most ate-env-api takes.
        golden_timeout: seconds; give up on the goldens still baking once
            none became ready or failed for that long.
        golden_poll: seconds between polls of the baking goldens.
        golden_wait: False does not wait for a golden that is still baking
            once its template is ensured: the image is probed at once (its
            probes boot cold and fill the nodes' layer caches; the golden
            keeps baking on the server) and its golden is recorded as
            ``pending``, so the coverage is not ``complete``.
        probe_without_golden: False leaves out the probes of an image whose
            golden is not ready (failed, or still pending after the wait):
            its pairs are reported short with the error ``golden not
            ready``. True, the default, probes it anyway, for the layer
            caches.
        probe_timeout: seconds; bounds one probe on the server: create,
            resume and node read (the delete has the server's own bound,
            60 s). A call the control plane cuts off
            is repeated, with its own run id, for as long again (at most
            nine attempts). At least a microsecond, the wire's unit.
        ensure_width: how many templates are ensured, and later polled, at once.
        images, domains: restrict the pass to those of the plan.
        observer: records the phases ``ensure_template``, ``preflight_golden``
            and ``preflight_probe``; None records nothing.
        backend: the ``backend`` label those phases carry (``substrate`` when
            a fleet runs the pass; ``-`` for a pass of its own).
    """

    def __init__(
        self,
        client: Client,
        plan: ImagePlacement | Mapping[str, Any],
        *,
        atespace: str = DEFAULT_ATESPACE,
        template_name: str | None = None,
        template_atespace: str | None = None,
        run_id: str = "",
        probes_per_node: int = 2,
        image_concurrency: int = 4,
        max_rounds: int = 3,
        golden_timeout: float = 30 * 60,
        golden_poll: float = 10.0,
        golden_wait: bool = True,
        probe_without_golden: bool = True,
        probe_timeout: float = 10 * 60,
        ensure_width: int = 8,
        images: Sequence[str] | None = None,
        domains: Sequence[str] | None = None,
        observer: Observer | None = None,
        plan_path: str = "",
        backend: str = "-",
    ):
        self.client = client
        self.plan = plan if isinstance(plan, ImagePlacement) else ImagePlacement.from_dict(plan)
        self.plan_path = plan_path
        self.atespace = atespace
        self.template_name = template_name
        self.template_atespace = template_atespace
        if not run_id:
            run_id = _new_run_id()
        if len(run_id) > _MAX_RUN_ID or not _RUN_ID.match(run_id):
            raise ValueError(
                f"run_id {run_id!r} must match {_RUN_ID.pattern} and be at most {_MAX_RUN_ID} characters"
            )
        self.run_id = run_id
        # Backoff of a repeated warm call, seconds; tests shorten it.
        self._retry_backoff = 2.0
        # What ate-env-api refuses is refused here: a refused warm call only
        # shows as a short pair, after the goldens were waited for.
        bad = warm_knob_error(
            probes_per_node, max_rounds, probe_timeout,
            names=("probes_per_node", "max_rounds", "probe_timeout"),
        )
        if bad:
            raise ValueError(bad)
        for name, value in (
            ("image_concurrency", image_concurrency),
            ("ensure_width", ensure_width),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be an int >= 1, got {value!r}")
        for name, value in (
            ("golden_timeout", golden_timeout),
            ("golden_poll", golden_poll),
        ):
            # As FleetConfig: a bool is not a number of seconds, and a string
            # is a ValueError here, not a TypeError from the comparison.
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a finite number > 0, got {value!r}")
        for name, value in (
            ("golden_wait", golden_wait),
            ("probe_without_golden", probe_without_golden),
        ):
            # No bool(): that turns "false" into True.
            if not isinstance(value, bool):
                raise ValueError(f"{name} must be a bool, got {value!r}")
        self.probes_per_node = probes_per_node
        self.image_concurrency = image_concurrency
        self.max_rounds = max_rounds
        self.golden_timeout = golden_timeout
        self.golden_poll = golden_poll
        self.golden_wait = golden_wait
        self.probe_without_golden = probe_without_golden
        self.probe_timeout = probe_timeout
        self.ensure_width = ensure_width
        self.observer = observer
        self.backend = backend
        for name in domains or ():
            if self.plan.domain(name) is None:
                raise ValueError(f"the plan has no domain {name!r}")
        for img in images or ():
            if self.plan.entry(img) is None:
                raise ValueError(f"the plan does not place {img!r}")
        self._domains = [
            d for d in self.plan.domains if not domains or d.name in set(domains)
        ]
        wanted = set(images or ())
        held = {d.name for d in self._domains}
        self._images = [
            e.image
            for e in self.plan.images
            if (not wanted or e.image in wanted) and any(d in held for d in e.domains)
        ]
        if not self._images:
            raise ValueError("no image of the pass is placed in its domains")
        # The longest run id a call of this pass sends: the highest plan
        # indexes among the pass's own domains and images, not the plan's, so
        # a subset of a large plan is not refused for pairs it never warms.
        in_pass = set(self._images)
        longest = _warm_run_id(
            run_id,
            max(i for i, d in enumerate(self.plan.domains) if d.name in held),
            max(i for i, e in enumerate(self.plan.images) if e.image in in_pass),
            _MAX_WARM_ATTEMPTS,
        )
        if len(longest) > _MAX_RUN_ID:
            raise ValueError(
                f"run_id {run_id!r} with the domain, image and attempt indexes the pass appends "
                f"({longest}) exceeds the {_MAX_RUN_ID} characters ate-env-api takes; use a shorter one"
            )
        self._goldens: dict[str, _Golden] = {}
        self._changed = asyncio.Event()
        self._progress = 0.0
        # The domains a cancelled run left part-way, by name (see run()).
        self._partial: dict[str, DomainCoverage] = {}
        # The last run's report, also after a cancel (see run()).
        self.coverage: PreflightCoverage | None = None

    @property
    def images(self) -> list[str]:
        """The images of the pass, in plan order."""
        return list(self._images)

    @property
    def domains(self) -> list[PlacementDomain]:
        """The domains of the pass, in plan order."""
        return list(self._domains)

    # -- run ---------------------------------------------------------------

    async def run(self) -> PreflightCoverage:
        """Run the pass and return the coverage report.

        The report is returned whether or not the pass completed, and a
        probe the pass could not delete does not raise either: check
        ``clean`` (``complete`` and no ``undeleted_probes``) and
        ``shortfall()``. A cancelled run stops its golden
        poller, ends the ``warm_image`` calls in flight (the server deletes
        their probes) and re-raises; what it covered until then stays in
        ``coverage``: the domains that finished, the domains cut part-way
        with the images they covered and the ones in flight marked
        ``cancelled`` (with the probes the stream had reported), and the
        goldens it was still waiting for recorded as ``pending`` with the
        error ``cancelled``. A cancelled call reports no coverage, so the
        probes it could not delete are not listed; the server sweeps them.
        """
        now = time.monotonic()
        coverage = PreflightCoverage(
            run_id=self.run_id, started_at=datetime.now(timezone.utc), plan=self.plan_path
        )
        self._goldens = {img: _Golden(img, now) for img in self._images}
        self._changed = asyncio.Event()
        self._progress = now
        self._partial = {}
        self.coverage = coverage
        domain_tasks: list[asyncio.Task[DomainCoverage]] = []
        try:
            await self._ensure_templates()
            if not self.golden_wait:
                # No wait: what the ensure left baking is settled as pending
                # here, so its probes start at once and the poller has
                # nothing to do.
                for img in self._pending():
                    self._settle(
                        img,
                        GoldenRecord(image=img, state="pending", error="golden not waited for"),
                        False,
                    )
            poller = asyncio.create_task(self._poll_goldens())
            domain_tasks = [asyncio.create_task(self._cover_domain(d)) for d in self._domains]
            try:
                # return_exceptions keeps a cancelled or failed domain from
                # ending this await while the others still delete their
                # probes; the first error is raised below, once all are done.
                results = await asyncio.gather(*domain_tasks, return_exceptions=True)
            except asyncio.CancelledError:
                await _await_all(domain_tasks)
                raise
            finally:
                # A run that ends early (cancelled, or a domain failed) must
                # not wait for the goldens still baking: stop the poller, which
                # settles them as cancelled, and let a cancel of this task
                # through once it has.
                if not poller.done():
                    poller.cancel()
                await asyncio.gather(poller, return_exceptions=True)
        finally:
            coverage.goldens = [self._goldens[img].record for img in self._images]
            coverage.finished_at = datetime.now(timezone.utc)
            # What a run that ended early covered: the domains that finished,
            # and the ones a cancel cut part-way, in plan order.
            by_name = dict(self._partial)
            for t in domain_tasks:
                if t.done() and not t.cancelled() and t.exception() is None:
                    by_name[t.result().name] = t.result()
            coverage.domains = [by_name[d.name] for d in self._domains if d.name in by_name]
        domains: list[DomainCoverage] = []
        for r in results:
            if isinstance(r, BaseException):
                raise r
            domains.append(r)
        coverage.domains = domains
        coverage.complete = all(g.ok for g in self._goldens.values()) and all(
            d.complete for d in domains
        )
        return coverage

    # -- goldens -----------------------------------------------------------

    async def _ensure_templates(self) -> None:
        sem = asyncio.Semaphore(self.ensure_width)

        async def ensure(img: str) -> None:
            async with sem:
                golden = self._goldens[img]
                try:
                    with self._phase("ensure_template"):
                        st = await self.client.image_status(
                            img,
                            atespace=self.atespace,
                            template_name=self.template_name,
                            template_atespace=self.template_atespace,
                            ensure=True,
                        )
                except Exception as e:
                    golden.ensured = time.monotonic()
                    self._settle(img, GoldenRecord(image=img, state="failed", error=str(e)), False)
                    return
                golden.ensured = time.monotonic()
                self._settle_from_status(img, st, ensured=True)

        await asyncio.gather(*(ensure(img) for img in self._images))

    def _settle_from_status(self, img: str, st: ImageStatus, *, ensured: bool) -> None:
        rec = GoldenRecord(
            image=img, template=st.template.name, tag=st.golden_tag, error=st.golden_error
        )
        # Kept for a golden that settles later without a status (not waited
        # for, given up on, cancelled): its record still names the template.
        known = self._goldens[img].record
        known.template, known.tag = st.template.name or known.template, st.golden_tag or known.tag
        if st.golden == GoldenSnapshotState.READY:
            rec.state = "ready"
            self._settle(img, rec, True)
        elif st.golden == GoldenSnapshotState.FAILED:
            rec.state, rec.error = "failed", f"golden bake failed: {st.golden_error}"
            self._settle(img, rec, False)
        elif st.golden == GoldenSnapshotState.NONE:
            if st.template_exists:
                # Goldens are disabled on the base template: the probes boot
                # cold and still fill the layer caches.
                rec.state = "none"
                self._settle(img, rec, True)
            elif ensured:
                rec.state, rec.error = "failed", "the template was not created"
                self._settle(img, rec, False)

    def _settle(self, img: str, rec: GoldenRecord, ok: bool) -> None:
        golden = self._goldens[img]
        if golden.settled:
            return
        now = time.monotonic()
        rec.template = rec.template or golden.record.template
        rec.tag = rec.tag or golden.record.tag
        rec.wait_ms = max(0.0, (now - golden.ensured) * 1000.0)
        golden.settled, golden.ok, golden.record = True, ok, rec
        self._progress = now
        golden.event.set()
        self._changed.set()
        self._changed = asyncio.Event()
        if self.observer is not None:
            self.observer.record_phase(
                "preflight_golden",
                rec.wait_ms / 1000.0,
                backend=self.backend,
                status="ok" if ok else "error",
            )
        if not ok:
            logger.warning("preflight: golden of %s not ready: %s %s", img, rec.state, rec.error)

    def _pending(self) -> list[str]:
        return [img for img in self._images if not self._goldens[img].settled]

    async def _poll_goldens(self) -> None:
        """Poll the baking goldens until all settle, none has settled for
        ``golden_timeout``, or the run is cancelled."""
        sem = asyncio.Semaphore(self.ensure_width)
        self._progress = time.monotonic()

        async def poll(img: str) -> None:
            async with sem:
                try:
                    st = await self.client.image_status(
                        img,
                        atespace=self.atespace,
                        template_name=self.template_name,
                        template_atespace=self.template_atespace,
                    )
                except Exception as e:
                    logger.warning("preflight: image status of %s failed: %s", img, e)
                    return
                self._settle_from_status(img, st, ensured=False)

        try:
            while pending := self._pending():
                # No sleep beyond the deadline: with golden_timeout below
                # golden_poll the wait ends at the timeout, after one last poll.
                left = self._progress + self.golden_timeout - time.monotonic()
                await asyncio.sleep(max(0.0, min(self.golden_poll, left)))
                await asyncio.gather(*(poll(img) for img in pending))
                if time.monotonic() - self._progress >= self.golden_timeout:
                    for img in self._pending():
                        self._settle(
                            img,
                            GoldenRecord(
                                image=img,
                                state="pending",
                                error=f"golden still pending after {self.golden_timeout:g}s without progress",
                            ),
                            False,
                        )
                    return
        except asyncio.CancelledError:
            for img in self._pending():
                self._settle(img, GoldenRecord(image=img, state="pending", error="cancelled"), False)
            raise

    # -- coverage ----------------------------------------------------------

    def _probes_for(self, d: PlacementDomain) -> int:
        """The probes per node for the domain: ``probes_per_node``, bounded by
        the domain's ``node_slots`` when the plan has it, so a small pool
        bounds its own probes and no other domain's."""
        if d.node_slots:
            return max(1, min(self.probes_per_node, d.node_slots))
        return self.probes_per_node

    def _image_width(self, d: PlacementDomain) -> int:
        k = self.image_concurrency
        if d.node_slots:
            k = min(k, d.node_slots // self._probes_for(d))
        return max(k, 1)

    async def _cover_domain(self, d: PlacementDomain) -> DomainCoverage:
        """Warm the domain's images, up to ``_image_width`` at once, each taken
        as soon as its golden has settled (earliest in plan order first)."""
        items = [
            (ii, e.image)
            for ii, e in enumerate(self.plan.images)
            if e.image in self._goldens and d.name in e.domains
        ]
        covs: dict[int, ImageCoverage] = {}
        started: set[int] = set()
        sem = asyncio.Semaphore(self._image_width(d))
        tasks: list[asyncio.Task[None]] = []

        async def cover(k: int, ii: int, image: str) -> None:
            async with sem:
                # The coverage object is registered before the call, so a
                # cancel leaves what the image covered until then in place.
                ic = ImageCoverage(image=image, wanted=d.nodes)
                covs[k] = ic
                if not self.probe_without_golden and not self._goldens[image].ok:
                    ic.error = "golden not ready"
                    return
                await self._cover_image(d, ii, image, ic)

        try:
            while len(started) < len(items):
                changed = self._changed
                pick = next(
                    (
                        k
                        for k, (_ii, image) in enumerate(items)
                        if k not in started and self._goldens[image].settled
                    ),
                    None,
                )
                if pick is None:
                    await changed.wait()
                    continue
                started.add(pick)
                ii, image = items[pick]
                tasks.append(asyncio.create_task(cover(pick, ii, image)))
            for r in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(r, BaseException):
                    raise r
        except asyncio.CancelledError:
            # The calls still running end before this re-raises (the server
            # deletes their probes); a task that was already cancelled
            # through the gather above is not cancelled again. What the
            # domain covered until then is kept for the report.
            for t in tasks:
                if not t.done():
                    t.cancel()
            await _await_all(tasks)
            self._partial[d.name] = self._domain_coverage(d, items, covs, cancelled=True)
            raise
        return self._domain_coverage(d, items, covs, cancelled=False)

    def _domain_coverage(
        self,
        d: PlacementDomain,
        items: list[tuple[int, str]],
        covs: dict[int, ImageCoverage],
        *,
        cancelled: bool,
    ) -> DomainCoverage:
        dc = DomainCoverage(name=d.name, nodes=d.nodes)
        seen: set[str] = set()
        for k, (_ii, image) in enumerate(items):
            ic = covs.get(k)
            if ic is None:
                dc.complete = False
                dc.images.append(ImageCoverage(image=image, wanted=d.nodes, error="not started"))
                continue
            if cancelled and not ic.complete and not ic.error:
                ic.error = "cancelled"
            seen.update(ic.nodes)
            if not ic.complete:
                dc.complete = False
            dc.images.append(ic)
        dc.nodes_seen = sorted(seen)
        if len(dc.nodes_seen) > d.nodes:
            logger.warning(
                "preflight: domain %s has %d nodes, the plan says %d",
                d.name,
                len(dc.nodes_seen),
                d.nodes,
            )
        return dc

    async def _cover_image(
        self, d: PlacementDomain, ii: int, image: str, ic: ImageCoverage
    ) -> None:
        """One ``warm_image`` call for the image in the domain, filling ``ic``:
        the server runs the rounds of probes until every node of the domain
        has run the image or the rounds run out, and reports the coverage. A
        call the control plane cuts off (Unavailable, DeadlineExceeded,
        Unknown, Internal, Aborted) is repeated with a backoff for up to
        ``probe_timeout`` and ``_MAX_WARM_ATTEMPTS`` attempts, unless its
        stream showed every node already; each attempt sends its own run id,
        so the probes of a broken call that an api server restart left
        behind never share a name with the repeat's. The pair's coverage
        sums its attempts. A call that ends in an error, or a cancel, counts
        with what its stream showed (the server sends its coverage only at
        the end), and the probes a call streamed before an error are listed
        in ``undeleted_probes``: the server deletes them when it can, and
        the call that would have said so never ended."""
        di = self.plan.domains.index(d)
        # The pair's coverage is the sum of its attempts: the nodes are the
        # union, the counters add up. An attempt that ended in an error
        # counts with what its stream showed, a clean one with the server's
        # coverage. ``done`` holds the attempts that ended, ``att`` the one
        # in flight as its stream shows it.
        seen: set[str] = set()
        done = {"probes": 0, "failed": 0, "rounds": 0}
        att: dict[str, Any] = {}
        undeleted: list[str] = []
        waited = 0.0

        def fill() -> None:
            ic.nodes = sorted(seen)
            ic.probes = done["probes"] + att["probes"]
            ic.failed = done["failed"] + att["failed"]
            ic.rounds = done["rounds"] + att["rounds"]
            ic.complete = len(ic.nodes) >= d.nodes
            ic.undeleted_probes = sorted(undeleted)
            ic.waited_ms = waited * 1000.0

        def on_probe(p: WarmProbe) -> None:
            if self.observer is not None:
                self.observer.record_phase(
                    "preflight_probe",
                    p.duration,
                    backend=self.backend,
                    status="error" if p.error else "ok",
                )
            att["ids"].append(p.id)
            att["probes"] += 1
            att["rounds"] = max(att["rounds"], p.round)
            if p.error:
                att["failed"] += 1
                ic.error = p.error
            elif p.node:
                seen.add(p.node)
            # Kept current, so a cancel leaves what the stream showed.
            ic.probes = done["probes"] + att["probes"]
            ic.failed = done["failed"] + att["failed"]
            ic.rounds = done["rounds"] + att["rounds"]

        wait = self._retry_backoff
        give_up: float | None = None
        attempt = 0
        while True:
            attempt += 1
            att.update(ids=[], probes=0, failed=0, rounds=0)
            try:
                cov = await self.client.warm_image(
                    image,
                    worker_selector=d.selector,
                    nodes=d.nodes,
                    probes_per_node=self._probes_for(d),
                    max_rounds=self.max_rounds,
                    probe_timeout=self.probe_timeout,
                    run_id=_warm_run_id(self.run_id, di, ii, attempt),
                    atespace=self.atespace,
                    template_name=self.template_name,
                    template_atespace=self.template_atespace,
                    on_probe=on_probe,
                )
            except asyncio.CancelledError:
                # The server is there and deletes the call's probes.
                fill()
                raise
            except Exception as e:
                # The server deletes a round's probes after it streamed them
                # and names the ones it could not delete in the coverage this
                # call never sent; an api server that restarted left them
                # all. The pass cannot tell, so every probe the call streamed
                # is listed.
                undeleted.extend(att["ids"])
                fill()
                ic.error = str(e)
                # The stream can have shown every node before the call broke:
                # the pair is covered, and a repeat would only boot the image
                # again.
                # Every line about a cut-off call names the call's run id:
                # its probes are <run id>-<round>-<slot>, and when the api
                # server restarted nothing else records them.
                call = (image, d.name, _warm_run_id(self.run_id, di, ii, attempt), attempt)
                if ic.complete:
                    logger.warning(
                        "preflight: warm call for %s in %s (run id %s, attempt %d) cut off after "
                        "its stream showed every node, not repeated; its probes may be left: %s",
                        *call, e,
                    )
                    return
                if not _transient(e):
                    return
                now = time.monotonic()
                if give_up is None:
                    give_up = now + self.probe_timeout
                # The repeats last until give_up, not until a whole backoff
                # no longer fits before it: the wait is cut to what is left.
                if attempt >= _MAX_WARM_ATTEMPTS or now >= give_up:
                    logger.warning(
                        "preflight: warm call for %s in %s (run id %s, attempt %d) cut off, giving "
                        "up; the probes of the cut-off calls may be left: %s",
                        *call, e,
                    )
                    return
                for key in done:
                    done[key] += att[key]
                att.update(ids=[], probes=0, failed=0, rounds=0)
                logger.warning(
                    "preflight: warm call for %s in %s (run id %s, attempt %d) cut off, repeating: %s",
                    *call, e,
                )
                await asyncio.sleep(min(wait, give_up - now))
                wait = min(wait * 2, 5 * self._retry_backoff)
                continue
            seen.update(cov.nodes)
            att.update(probes=cov.probes, failed=cov.failed, rounds=cov.rounds)
            undeleted.extend(cov.undeleted_probes)
            waited += cov.waited
            fill()
            if attempt == 1:
                # A clean call alone is complete when the server says so.
                ic.complete = cov.complete and ic.complete
            ic.error = cov.error
            return

    def _phase(self, name: str) -> Any:
        if self.observer is None:
            return _NoPhase()
        return self.observer.phase(name, backend=self.backend)


async def _await_all(tasks: list[asyncio.Task[Any]]) -> None:
    """Wait for every task to finish, through cancels of the waiter: a task
    cancelled during its cleanup would otherwise strand what it was deleting."""
    while True:
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            return
        except asyncio.CancelledError:
            if all(t.done() for t in tasks):
                raise
            continue


def _base36(n: int) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    while True:
        n, r = divmod(n, 36)
        out = digits[r] + out
        if n == 0:
            return out


def _warm_run_id(run_id: str, domain: int, image: int, attempt: int) -> str:
    """The run id of one ``warm_image`` call: the pass's run id with the plan
    indexes of the domain and the image and the attempt. The indexes are in
    base 36 (0-9, a-z): with the default ``pf-<6>`` run id that leaves seven
    characters for the two, enough for 46,656 domains with 1,679,616 images
    (decimal ran out at 351 domains with 36,841 images). ``warmRunID`` in the
    Go loader writes the same names."""
    return f"{run_id}-{_base36(domain)}-{_base36(image)}-{attempt}"


def _new_run_id() -> str:
    """``pf-`` and six random characters, as ate-env-api's ``WarmImage`` does."""
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "pf-" + "".join(secrets.choice(alphabet) for _ in range(6))


def _transient(e: BaseException) -> bool:
    """True when a warm call ended in a failure a control plane under load
    hands out for a while (the api server restarting, a deadline, an
    internal or aborted call): the call is repeated."""
    if isinstance(e, SandboxUnavailableError):
        return True
    # The SDK's RpcError carries the status as an attribute; a raw grpc error
    # has it as a method.
    code = e.code() if isinstance(e, grpc.RpcError) else getattr(e, "code", None)
    return code in _TRANSIENT


class _NoPhase:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> None:
        return None
