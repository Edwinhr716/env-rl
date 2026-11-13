# System invariants and concurrency pitfalls

Companion to [architecture](../architecture.md) and [code layout](./code-layout.md).

## In-guest process lifecycle and signaling

### 1. PID 1 child subreaper re-exec avoids `cmd.Wait` race

Re-exec the process as a subreaper under PID 1 before starting the gRPC daemon.

- **Location**: `cmd/ate-env-guest/init_linux.go` (`runIfPID1`, `supervise`).
- **Why**: Direct `wait4(-1, ...)` calls inside the gRPC daemon race with `os/exec.Cmd.Wait()` in
  `guest/process.Tracker`, stealing exit codes.
- **Mechanism**: `ate-env-guest` sets `PR_SET_CHILD_SUBREAPER`, re-execs via `os.StartProcess`, and
  the outer PID 1 runs `syscall.Wait4(-1, &status, syscall.WNOHANG, nil)` and forwards signals.

### 2. Process groups and negative-PID signal delivery

Start subprocesses in dedicated process groups and signal via negative PIDs (`-pid`).

- **Location**: `guest/process/tracker.go` (`Setpgid`, `Tracker.Signal`, `Tracker.Start`),
  `guest/process/pty_linux.go` (`Setsid`), `cmd/ate-env-guest/sidecar.go`.
- **Why**: Signaling only the parent PID leaves child worker trees running on cancellation.
- **Mechanism**: Non-PTYs set `Setpgid: true` and PTYs `Setsid: true`; `(*Service).SignalProcess`
  and timeouts call `Tracker.Signal` (`syscall.Kill(-pid, sig)`) on the process group.

### 3. Spool cursor notification subscription before file read

Acquire `OutputChanged()` before reading from disk to avoid missed write notifications.

- **Location**: `guest/process/service.go` (`StreamProcessOutput`), `guest/process/tracker.go`.
- **Why**: Subscribing to `OutputChanged()` after reading misses concurrent disk writes and stalls.
- **Mechanism**: `StreamProcessOutput` acquires `OutputChanged()` before reading and waits on it
  when zero bytes are read while running; writers close it under mutex after flushing.

## Server networking and Substrate boundaries

### 4. Router `ate-target-actor` header deduplication and SPIFFE mTLS

Append `ate-target-actor` via interceptors and do not forward incoming caller metadata.

- **Location**: `internal/apiservice/server.go` (`guestConn`, `routerCredentials`),
  `internal/apiservice/wakeretry.go` (`ensureAwake`), `internal/ate/client.go` (`DialGuest`),
  `internal/routertls/routertls.go` (`TransportCredentials`, `RejectedCertificate`).
- **Why**: Forwarding caller metadata duplicates `ate-target-actor`, which `atenet-router` rejects.
- **Mechanism**: `guestConn` and `DialGuest` append `ate-target-actor` without forwarding caller
  metadata. `internal/routertls` hot-reloads kubelet `-atenet-client-cert` and `servicedns`
  `-atenet-ca-file` on handshakes. `routertls.TransportCredentials` (`alertConn.Write`) reads TLS
  1.3 post-handshake alerts on initial HTTP/2 `EPIPE` writes, letting `ensureAwake` match
  `routertls.RejectedCertificate` before any wait, while `DialGuest` gets the alert.

### 5. Actor wake budgets, fail-fast start errors, and actor deletion

Verify readiness within wake budgets, fail fast on unrecoverable errors, and evict on delete.

- **Location**: `internal/apiservice/wakeretry.go` (`ensureAwake`, `StartFailureKey`),
  `internal/ate/client.go` (`Delete`, `evictGuest`).
- **Wake budgets**: Traffic to a suspended actor resumes it in `atenet`. `ensureAwake` polls
  `GetProcess("ate-env-wake-probe")` under `DefaultWakeBudget` (5m) and `DefaultWakeRunningBudget`
  (20s once `RUNNING`), returning `Canceled` or `DeadlineExceeded` probe errors immediately. The
  state read after an `Unavailable` probe gets the rest of `WakeBudget`, and of
  `WakeNoCapacityGrace` during a no-room run, but never more than `WakeStateReadCap`
  (`-wake-state-read-cap`, `DefaultWakeStateReadCap` = 5s): the control dial sets no keepalive and
  no per-call timeout, so a read that hangs without an error is cut there. `Unavailable`,
  `DeadlineExceeded` or `ResourceExhausted` from that read, or one cut at its deadline, leaves the
  state unknown, and the wait goes on. When all reads are cut, the wait probes once per
  `-wake-state-read-cap` plus backoff, ending at `-wake-budget` with `Unavailable` "while UNKNOWN".
- **Fail-fast start errors**: `ensureAwake` exits early on unrecoverable states:
  `routertls.RejectedCertificate` returns `codes.FailedPrecondition` with
  `routertls.RejectionAdvice` and no trailer, `CRASHED` returns `codes.FailedPrecondition` with
  `ActorStatus.Crash.Message` and trailer `ate-env-start-failure: crashed`, `DELETING` returns
  `codes.NotFound`, and `"no worker has room for the actor"` persisting across `WakeNoCapacityGrace`
  while not `RUNNING` (default `0` waits `WakeBudget`) returns `codes.ResourceExhausted`
  (`ate-env-start-failure: no-capacity`).
- **Delete sequence**: `internal/ate.Client.Delete` calls `DeleteActor` directly in any state
  including `CRASHED` (terminating workloads, releasing slots, and deleting snapshots without
  pre-suspending) and calls `evictGuest` to drop the pooled `guestPool` connection.

### 6. Idempotent derived `ActorTemplate` creation and failed golden-snapshot recovery

Serialize derived templates (`imageTemplateLocks`); `AlreadyExists` succeeds on matching digest.

- **Location**: `internal/apiservice/imagetemplate.go` (`DeriveImageTemplate`),
  `internal/apiservice/templaterecover.go` (`imageTemplateLocks`, `imageTemplateLock`,
  `recreateFailedImageTemplate`, `failedTemplateRecreateAfter`, `templateUsers`, `keptUID`),
  `internal/apiservice/server.go` (`ensureImageTemplate`, `getOrCreateImageTemplate`,
  `sameImageDigest`).
- **Why**: Concurrent workers race to create `<base>-<digest:12>` (`imageTemplateDigestLen = 12`),
  Substrate leaves terminal `golden_snapshot_status.error_message` failures unretried, and deleting
  an in-use template strips rootfs volumes upon resume.
- **Mechanism**: `CreateEnvironment` takes `imageTemplateLock` only when `rootfs_image` is set;
  direct `Template.Name` creates skip it. `ensureImageTemplate` holds a per-template read lock, held
  in this process only (`imageTemplateLocks`), through `CreateActor` (see
  `docs/plans/ate-env-api/acceptance-criteria.md`). `getOrCreateImageTemplate` reuses on cross-path
  `sameImageDigest`, fails with `codes.FailedPrecondition` on mismatch, and treats
  `codes.AlreadyExists` as success after the winner's digest check. If a golden failed
  `>= failedTemplateRecreateAfter` (10m) ago, `ensureImageTemplate` write-locks and calls
  `recreateFailedImageTemplate` (by UID at 0 `templateUsers`).

## Client connection pooling and event loops

### 7. Python `LoopRunner` thread bridging and HTTP/2 stream concurrency

Dispatch synchronous Python calls to a persistent `LoopRunner` background thread.

- **Location**: `clients/python/src/ate_env_rl/loop_runner.py`,
  `clients/python/src/ate_env_rl/fleet.py`, `clients/python/src/ate_env_rl/backend/substrate.py`,
  `cmd/ate-env-api/main.go`.
- **Why**: `asyncio.run()` creates short loops that destroy HTTP/2 gRPC channels on exit.
- **Mechanism**: `LoopRunner` runs a persistent daemon event loop and dispatches coroutines via
  `asyncio.run_coroutine_threadsafe`. `SandboxFleet` sizes connections via `FleetConfig.connections`
  or `ceil(max_concurrent / 200)`, and `SubstrateBackendDriver` pools gRPC channels against
  `DefaultMaxConcurrentStreams = 4096`.

### 8. Python release error tracking and teardown retries

Record release errors in `RunReport` and retry unreleased actors during `fleet.teardown()`.

- **Location**: `clients/python/src/ate_env_rl/backend/substrate.py` (release errors, retries),
  `clients/python/src/ate_env_rl/fleet.py`, `async_fleet.py`, `strategies.py` (error raised).
- **Why**: Network errors during `delete` must not crash workers or leak orphan actors.
- **Mechanism**: `SubstrateBackendDriver` logs failed deletes in `RunReport.release_errors` and
  keeps `_owned_actors`; `fleet.teardown()` retries (`InfrastructureError` on failure), and a
  teardown error never replaces the body's error: `SandboxFleet.run()` and `with fleet:` log it and
  raise the body's error (or interrupt), raising the teardown error only when the body succeeded.

### 9. Python warm-pool bookkeeping across awaits

Re-check teardown state after each await before pooling an actor, and reserve warm slots upfront.

- **Location**: `clients/python/src/ate_env_rl/backend/substrate.py` (`_release_internal`,
  `_warm_pool_internal`, `_acquire_internal`, `_reap_internal`, `_return_popped`,
  `_delete_unpooled`, `_keep_for_any_reap`, `_may_pool`, `_cancel_warm_calls`,
  `_own_abandoned_create`, `_spawn_background_delete`, `_wait_background_deletes`, `close`),
  `clients/python/src/ate_env_rl/loop_runner.py` (`stop`),
  `clients/python/tests/test_warm_pool_accounting.py`, `clients/python/tests/test_loop_runner.py`.
- **Why**: Recycles pop actors from `_owned_actors` during `suspend()`; pooling after a concurrent
  `reap` leaks actors past teardown, and unreserved shortfalls over-provision.
- **Teardown & claim checks**: `reap` bumps `_teardowns` on entry/exit (`_reaping` counts active
  reaps); recycles, `warm_pool`, and claims read `_teardowns` before awaiting and pool only via
  `_may_pool` (`_teardowns` unchanged and `_reaping == 0`), else delete; claims re-check after
  `get()` in one lock or delete stale still-owned actors and retry.
- **Ownership & generations**: Actors stay in `_owned_actors` until deleted. A cold `acquire`
  overlapping its run's reap also owns the actor under `WARM_POOL_RUN_ID` (handle keeps run id); a
  cancelled create (e.g., `acquire` `timeout_s`) takes `WARM_POOL_RUN_ID` via
  `_own_abandoned_create` in case it landed. `_reap_internal` clears `_warm_inflight` (per pool key,
  `_teardowns`, and `_pool_gens`; subtracted from shortfalls) on entry/exit, dropping warm
  reservations on reap. `unwarm_pool` and `resize_pool` below pooled + in-flight bump `_pool_gens`
  (`_cancel_warm_calls`), so earlier `warm_pool` calls (`drained` in `_warm_pool_internal`) delete
  instead of pooling. `_delete_unpooled` unconditionally re-tags failed/cancelled deletes under
  `WARM_POOL_RUN_ID` via `_keep_for_any_reap` (unlike `release`, which routes via `_return_popped`
  and `_may_pool`). `reap` updates the warm-replica gauge (tested via Hypothesis state machine).
- **Entry snapshot & background deletes**: `_acquire_internal` reads `_teardowns` once on entry
  (`entry_teardowns`) and checks `_run_reaped_since` against that value when it owns the actor and
  in `_own_abandoned_create`, so a warm claim paused in `get()` across its own run's reap tags the
  actor of its cold fallback, or of a retried claim on an actor another run pooled after the reap,
  under `WARM_POOL_RUN_ID`, and a cancel in that fallback create starts the delete. A cancelled cold
  create or `warm_pool` call that finds a reap ran since it started deletes its actors through
  `_spawn_background_delete`, which records each task with its actor id in `_background_deletes`.
  `_reap_actors`, `close()` and `close_async()` join them through `_wait_background_deletes`
  (bounded by `background_delete_timeout_s`, 10 s by default), so a teardown does not return, and
  `close()` does not close the clients or stop the loop, with a delete in flight. `close()` first
  cancels the other tasks on its own `LoopRunner` and waits for them (`_quiesce`), so an acquire
  still in `create()` starts its delete while the channels are open: a channel closed first cancels
  the create, and the handler's delete would dial a new client that `stop()` then cancels.
  `close_async()` does not cancel the caller's tasks; after the channels close it joins the deletes
  that close triggered and closes the client they dialed, and `AsyncSandboxFleet.__aexit__` awaits
  it. Both count a teardown first (`_count_teardown` bumps `_teardowns`): the cancel handler of a
  call that started after the run's reap, or with no reap at all, then starts its delete too
  (`_may_pool`), and a recycle or `warm_pool` that finishes during the shutdown deletes instead of
  pooling. `_count_teardown` also sets `_closing`: a `release` the shutdown cancels (in `suspend()`
  or `delete()`; it popped its actor, so no reap saw it) and a cancelled `_delete_unpooled` (a
  `warm_pool` call that finished after the reap, say) start a background delete, which the shutdown
  joins. Outside a close a cancelled `_delete_unpooled` only re-tags the actor for the next reap.
  `_return_popped` (a `release` cancelled in `suspend()` or `delete()`, or whose delete fails)
  starts the background delete after any teardown since the release popped the actor (a reap ran or
  runs, `_may_pool`), not only in a close, as `_own_abandoned_create` does: that reap could not see
  the actor, and a fleet reaps its run once. A reap still running joins the delete; one that fails
  leaves the actor under `WARM_POOL_RUN_ID`. With no teardown since the pop the actor goes back
  under its run id and no delete starts. A reap that the shutdown cuts off does the same for each
  actor it had not deleted (`_delete_unreaped_in_close`): `close()` cancels a reap in flight on its
  own `LoopRunner` (another thread's `teardown()`), and a channel close raises `CancelledError` in
  the deletes of a reap on the caller's loop, which `gather(return_exceptions=True)` returns as
  results, so that reap would return with the actors live. The handler is in `_reap_one` (a delete
  in flight, or a wait for the semaphore) and around the `gather` (a `_reap_one` cancelled before
  its first step runs no handler); an actor whose background delete is pending gets no second one. A
  reap cancelled outside a close leaves its actors owned as they were. `close_async()` on the
  caller's loop cancels no task, so the channel close frees the semaphore (16 slots) of
  `_reap_actors` and `_warm_pool_internal` for the `_reap_one` and `_warm_one` calls that waited on
  it. Those would send a foreground `delete()` or `create()` on a client dialed for them, which the
  shutdown does not join and then closes under them. `_stop_if_closing` raises `CancelledError` in
  them when they get the slot (in `_warm_one` also between `create()` and `suspend()`), so they take
  the path of the calls that were cut off. `_shutdown_async` also repeats its join and client close
  while an owned client exists (`_STOP_ROUNDS` rounds at most): a caller's task that was cut off and
  went on to another call has that call cut off on the redialed client, and its handler's delete
  dials one more. `unwarm_pool_async` and a shrinking `resize_pool_async` re-tag the actors they pop
  from the pool under `WARM_POOL_RUN_ID` in the same lock hold (`_own_drained_for_any_reap`): in the
  pool any run's reap deleted them, and out of it they carry the run id that `warm_pool` or a
  recycle left, so a drain cancelled while an actor waited on `_delete_drained`'s semaphore, or
  before its delete task's first step (`_delete_unpooled` never ran, so nothing re-tagged it), left
  that actor to one run's reap. `_delete_drained` has the reap's handlers: `_delete_one` checks
  `_stop_if_closing` at the semaphore and calls `_delete_unreaped_in_close` when cancelled, and so
  does the handler around its `gather`. The `gather` over `_delete_unpooled` at the end of
  `_warm_pool_internal` (a call that finished after a teardown, an `unwarm_pool` or a shrinking
  `resize_pool`) has the handler around it too: `close()` can cancel those tasks before their first
  step. A background delete that is cancelled does not start another. A warm claim cancelled in
  `get()` (`except BaseException` in `_acquire_internal`) puts the popped actor back in the pool, or
  after a teardown re-tags it under `WARM_POOL_RUN_ID` and starts a background delete: the actor may
  carry an earlier run's id (a recycled one), which no later reap matches. A `warm_pool` call whose
  `create()` or `suspend()` the channel close cancels, with the call itself not cancelled, raises
  that `CancelledError` out of its `gather(return_exceptions=True)`, so its cancel path owns and
  deletes every actor it started. The join waits only for deletes on its own loop whose actors are
  still owned (one a reap's own delete removed holds nothing up), and a delete task cancelled before
  its first step is recorded by its done callback, because its `finally` never runs. A delete still
  running at the deadline is not cancelled: its actor stays under `WARM_POOL_RUN_ID`, the reap adds
  its id to the `InfrastructureError` it raises, and `close()` logs it. A joined delete that failed
  or was cancelled is reported the same way (`_background_delete` records it in
  `_failed_background_deletes` before its task ends; NotFound counts as gone), so a delete that
  fails fast after the reap's snapshot cannot let the reap return with the actor live.
  `background_delete_timeout_s` is a finite number `>= 0`; `0` means no wait. `LoopRunner.stop`
  cancels the loop's tasks once, then waits in bounded rounds (`_STOP_ROUNDS`) for the tasks their
  cancel handlers started, so a delete started when `stop()` cancels an acquire is awaited, not
  abandoned by `loop.stop()`.
