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

"""``e2b.Sandbox`` on ate-env environments.

The class keeps E2B's data plane and replaces its control plane. Commands,
files and PTYs are the SDK's own code, talking to the real envd that the
E2B base template runs in each environment, through ate-env-api's port
proxy. ``create``, ``connect``, ``pause``, ``kill``, ``get_info`` and
``is_running`` go to ate-env-api's EnvironmentService instead of E2B's API
(``_methods``, ``_core``). Each other public method of the SDK class raises
``SandboxException``; it never does nothing, and never calls E2B.

The class overrides public names of the SDK only.
"""

from __future__ import annotations

import threading
from collections.abc import Awaitable
from typing import Any

import e2b
from ate_env_rl.loop_runner import LoopRunner

from . import _core
from ._methods import install

# One background loop for the process runs the coroutines of the control
# plane: a gRPC channel belongs to the loop that made it, thus the loop stays.
_lock = threading.Lock()
_runner: LoopRunner | None = None


def _run(awaitable: Awaitable[Any]) -> Any:
    """Runs one call of the control plane on the background loop.

    An interrupt of the wait (Ctrl-C) cancels the call and waits for its
    cleanup: a create that is under way deletes its environment.
    """
    global _runner
    try:
        _core.use()
    except BaseException:
        awaitable.close()
        raise
    with _lock:
        if _runner is None:
            _runner = LoopRunner(name="ate-env-e2b")
        runner = _runner
    ended = threading.Event()
    errors: list[BaseException] = []

    async def call() -> Any:
        try:
            return await awaitable
        except BaseException as e:
            errors.append(e)
            raise
        finally:
            ended.set()

    future = runner.submit(call())
    try:
        return future.result()
    except BaseException as e:
        if not future.done():
            future.cancel()
            ended.wait(_core.CLEANUP_TIMEOUT_S + 5)
            for note in getattr(errors[0], "__notes__", ()) if errors else ():
                e.add_note(note)
        raise


@install(_run)
class Sandbox(e2b.Sandbox):
    """E2B's ``Sandbox`` with ate-env environments behind it.

    The endpoint, the caller token and the atespace come from
    ``SUBSTRATE_ENV_API``, ``SUBSTRATE_ENV_AUTH_TOKEN`` and
    ``SUBSTRATE_ENV_ATESPACE``. No E2B API key is needed or used.
    ``sandbox_id`` is the environment id.
    """
