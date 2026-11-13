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

"""The mapped methods of ``Sandbox`` and ``AsyncSandbox``.

Each function here is one public method of the SDK class. It runs in the
thread of the caller: it binds the arguments to the SDK's signature, refuses
what the class cannot honour, warns, reads the settings, and returns a
coroutine of ``_core``. ``install`` puts them on a class; the sync class
passes a ``run`` that runs the coroutine on its background loop, and the
async class returns the coroutine to ``await``.

The couplings to the SDK are here: its constructor, ``ConnectionConfig`` and
``SandboxInfo``. All are public names of ``e2b``.
"""

from __future__ import annotations

import dataclasses
import datetime
from collections.abc import Awaitable, Callable
from typing import Any

from ate_env_rl import EnvironmentInfo
from e2b import ConnectionConfig, SandboxInfo, SandboxState
from packaging.version import Version

from . import _core
from ._settings import Settings
from ._surface import bind, dual, guard, sdk_base, warn_timeout

# The envd that integrations/e2b/layer builds. The SDK reads it only to pick
# between old and new envd calls; its newest threshold is 0.6.4.
ENVD_VERSION = Version("0.10.2")

# The API key and the API address of each ConnectionConfig. The class maps or
# refuses each SDK method that calls E2B's API; a path it missed gets no key
# and a name that never resolves (RFC 6761), not the E2B_API_KEY of the program.
UNUSED_API_KEY = "unused"
UNUSED_DOMAIN = "ate-env-e2b.invalid"


@dataclasses.dataclass(frozen=True)
class _State:
    """What a sandbox object keeps for its later calls."""

    settings: Settings
    request_timeout: float | None


def _config(settings: Settings, env_id: str, values: dict[str, Any]) -> ConnectionConfig:
    """The SDK's connection configuration of one call; raises as the SDK
    does for an option value that it refuses (``http_version="3"``)."""
    return ConnectionConfig(
        domain=UNUSED_DOMAIN,
        debug=False,
        api_key=UNUSED_API_KEY,
        api_url=f"http://{UNUSED_DOMAIN}",
        sandbox_url=settings.sandbox_url,
        extra_sandbox_headers=settings.proxy_headers(env_id),
        request_timeout=values.get("request_timeout"),
        http_version=values.get("http_version"),
        logger=values.get("logger"),
    )


def _attach(cls: type, settings: Settings, env_id: str, values: dict[str, Any]) -> Any:
    """Builds the SDK object for an environment, with no call to it."""
    sandbox = cls(
        sandbox_id=env_id,
        sandbox_domain=None,
        envd_version=ENVD_VERSION,
        envd_access_token=None,
        traffic_access_token=None,
        connection_config=_config(settings, env_id, values),
    )
    sandbox._ate = _State(settings, values.get("request_timeout"))
    return sandbox


def _target(sandbox: Any, values: dict[str, Any]) -> tuple[Settings, str, float | None]:
    """The settings, the environment and the request_timeout of one call.

    Each form of each method checks the options of the call as the SDK
    does, also a form that builds no sandbox object from them.
    """
    if sandbox is None:
        settings = Settings.from_env(values.get("sandbox_url"))
        env_id, request_timeout = values["sandbox_id"], values.get("request_timeout")
    else:
        state = getattr(sandbox, "_ate", None) or _State(Settings.from_env(), None)
        settings = state.settings.with_sandbox_url(values.get("sandbox_url"))
        env_id, request_timeout = sandbox.sandbox_id, values.get("request_timeout")
        if request_timeout is None:
            request_timeout = state.request_timeout
    _config(settings, env_id, values)
    return settings, env_id, request_timeout


def create(cls: type, _sandbox: None, args: tuple, kwargs: dict[str, Any]) -> Awaitable[Any]:
    values = bind(sdk_base(cls), "create", args, kwargs)
    warn_timeout(values["timeout"])
    settings = Settings.from_env(values.get("sandbox_url"))
    _config(settings, "", values)  # an option the SDK refuses creates nothing

    async def go() -> Any:
        env_id = await _core.create(
            settings, values["template"], values["envs"], values.get("request_timeout")
        )
        try:
            return _attach(cls, settings, env_id, values)
        except BaseException as e:
            await _core.discard(settings, env_id, e)
            raise

    return go()


def connect(cls: type, sandbox: Any, args: tuple, kwargs: dict[str, Any]) -> Awaitable[Any]:
    values = bind(sdk_base(cls), "connect", args, kwargs, class_form=sandbox is None)
    warn_timeout(values["timeout"])
    settings, env_id, request_timeout = _target(sandbox, values)
    if sandbox is None:
        sandbox = _attach(cls, settings, env_id, values)

    async def go() -> Any:
        await _core.resume(settings, env_id, request_timeout)
        return sandbox

    return go()


def _sandbox_info(env_id: str, info: EnvironmentInfo) -> SandboxInfo:
    epoch = datetime.datetime.fromtimestamp(0, datetime.UTC)
    known = dict(
        sandbox_id=env_id,
        template_id=info.template.name if info.template else "",
        metadata={},
        started_at=epoch,
        end_at=epoch,
        state=SandboxState.PAUSED if info.status in _core.PAUSED else SandboxState.RUNNING,
        cpu_count=0,
        memory_mb=0,
        envd_version=str(ENVD_VERSION),
        volume_mounts=[],
    )
    # None for each other field, also one that a newer SDK adds.
    return SandboxInfo(**{f.name: known.get(f.name) for f in dataclasses.fields(SandboxInfo)})


def get_info(cls: type, sandbox: Any, args: tuple, kwargs: dict[str, Any]) -> Awaitable[Any]:
    values = bind(sdk_base(cls), "get_info", args, kwargs, class_form=sandbox is None)
    settings, env_id, request_timeout = _target(sandbox, values)

    async def go() -> SandboxInfo:
        return _sandbox_info(env_id, await _core.info(settings, env_id, request_timeout))

    return go()


def _simple(name: str, call: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """A method that is one call of the core with no result to build."""

    def method(cls: type, sandbox: Any, args: tuple, kwargs: dict[str, Any]) -> Awaitable[Any]:
        values = bind(sdk_base(cls), name, args, kwargs, class_form=sandbox is None)
        return call(*_target(sandbox, values))

    return method


def _is_running(cls: type, sandbox: Any, args: tuple, kwargs: dict[str, Any]) -> Awaitable[bool]:
    if sandbox is None:
        raise TypeError("is_running() is a method of a sandbox object")
    return _core.is_running(*_target(sandbox, bind(sdk_base(cls), "is_running", args, kwargs)))


METHODS: dict[str, Callable[..., Awaitable[Any]]] = {
    "connect": connect,
    "get_info": get_info,
    "is_running": _is_running,
    "pause": _simple("pause", _core.pause),
    "beta_pause": _simple("beta_pause", _core.pause),
    "kill": _simple("kill", _core.kill),
}


def install(run: Callable[[Awaitable[Any]], Any]) -> Callable[[type], type]:
    """A class decorator: the mapped methods, and a refusal for the rest."""

    def decorate(cls: type) -> type:
        base = sdk_base(cls)

        def create_(c: type, *args: Any, **kwargs: Any) -> Any:
            return run(create(c, None, args, kwargs))

        create_.__name__, create_.__doc__ = "create", base.create.__doc__
        cls.create = classmethod(create_)
        for name, method in METHODS.items():
            setattr(cls, name, dual(name, method, run))
        guard(cls, base)
        return cls

    return decorate
