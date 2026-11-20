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

"""Which part of the SDK's public surface the classes take, and how.

Three rules, each bound to the SDK's public names at run time, thus a newer
SDK cannot widen what the classes do:

- ``bind`` binds the arguments of a call to the public signature of the SDK's
  method, and refuses each argument that has a value and is not in ``ALLOWED``.
- ``guard`` refuses each public callable of the SDK class that the class does
  not map and that is not in ``INHERITED``.
- ``dual`` gives a mapped method the SDK's two forms, ``sandbox.kill()`` and
  ``Sandbox.kill(sandbox_id)``.
"""

from __future__ import annotations

import inspect
import warnings
from collections.abc import Callable
from typing import Any

from e2b import SandboxException

from ._core import DOC

ANY = object()

# Per mapped method: the arguments that can have a value, and which values
# (ANY, or a tuple). Each other argument must be None or its default in the
# SDK's signature; an empty container is a value. A method that is not here
# is not mapped.
ALLOWED: dict[str, dict[str, Any]] = {
    "create": {
        "template": ANY,
        "timeout": ANY,  # warns: nothing enforces it
        "envs": ANY,
        "logger": ANY,
        "allow_internet_access": (True,),
    },
    "connect": {"timeout": ANY, "logger": ANY},
    "pause": {"keep_memory": (True,), "mode": ("full",)},
    "beta_pause": {"keep_memory": (True,), "mode": ("full",)},
    "kill": {},
    "get_info": {},
    "is_running": {"request_timeout": ANY},
}
# The same for E2B's per-call options (``**opts``, the SDK's ApiParams).
ALLOWED_OPTS: dict[str, Any] = {
    "request_timeout": ANY,
    "sandbox_url": ANY,
    "http_version": ANY,
    "debug": (False,),
    # Accepted and never sent: a program that passes its E2B key still runs.
    "api_key": ANY,
    "validate_api_key": ANY,
}
# Arguments that only the class form of a method has, after its positional ones.
CLASS_FORM_ONLY = {"connect": ("logger",)}

# What the classes take from the SDK unchanged: the data plane, which talks
# to envd through the port proxy, and plain data.
INHERITED = frozenset(
    {
        "commands",
        "files",
        "pty",
        "git",
        "connection_config",
        "sandbox_id",
        "sandbox_domain",
        "envd_api_url",
        "envd_direct_url",
        "traffic_access_token",
        "default_template",
        "default_mcp_template",
        "default_sandbox_timeout",
        "mcp_port",
        # They call kill(), which is mapped.
        "__enter__",
        "__exit__",
        "__aenter__",
        "__aexit__",
    }
)


class TimeoutNotEnforcedWarning(UserWarning):
    """A sandbox ``timeout`` was given, and no server ends the environment."""


def unsupported(what: str) -> SandboxException:
    return SandboxException(f"{what} is not supported on ate-env; see {DOC}")


def warn_timeout(timeout: int | None) -> None:
    """Warns at the line of the program: the caller of the method that called this."""
    if timeout is not None:
        warnings.warn(
            f"timeout={timeout} is not enforced on ate-env: the environment runs until kill() "
            f"or the end of the `with` block; see {DOC}",
            TimeoutNotEnforcedWarning,
            stacklevel=4,
        )


def sdk_base(cls: type) -> type:
    """The SDK class that cls extends."""
    return next(c for c in cls.__mro__ if c.__module__.split(".")[0] == "e2b")


def _shown(value: Any) -> str:
    return repr(value) if isinstance(value, bool | int | str) and len(repr(value)) < 40 else "..."


def bind(
    base: type, name: str, args: tuple, kwargs: dict[str, Any], *, class_form: bool = False
) -> dict[str, Any]:
    """The arguments of a call by name, bound as the SDK's method binds them.

    ``**opts`` is flattened into the result. In the class form the first
    argument is ``sandbox_id``. Raises ``SandboxException`` for an argument
    that has a value the class cannot honour, also one that this version of
    the class does not know.
    """
    params = list(inspect.signature(getattr(base, name)).parameters.values())
    if params and params[0].name == "self":
        params = params[1:]
    if class_form:
        positional = inspect.Parameter.POSITIONAL_OR_KEYWORD
        extra = [inspect.Parameter(n, positional, default=None) for n in CLASS_FORM_ONLY.get(name, ())]
        at = sum(p.kind is positional for p in params)
        params = [inspect.Parameter("sandbox_id", positional), *params[:at], *extra, *params[at:]]
    bound = inspect.Signature(params).bind(*args, **kwargs)
    bound.apply_defaults()

    values: dict[str, Any] = {}
    for param in params:
        value = bound.arguments[param.name]
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            checks = [(key, v, None, ALLOWED_OPTS.get(key, ())) for key, v in value.items()]
        elif param.name == "sandbox_id" and class_form:
            checks = [(param.name, value, value, ANY)]
        else:
            checks = [(param.name, value, param.default, ALLOWED[name].get(param.name, ()))]
        for key, v, default, allowed in checks:
            if not (v is None or v == default or allowed is ANY or v in allowed):
                raise unsupported(f"{name}({key}={_shown(v)})")
            values[key] = v
    return values


class dual:
    """A method in the SDK's two forms: ``sandbox.name(...)`` and ``Class.name(sandbox_id, ...)``.

    ``method(cls, sandbox, args, kwargs)`` gets the sandbox, or None in the
    class form, and returns an awaitable; ``run`` turns it into the result
    (the identity for the async class).
    """

    def __init__(self, name: str, method: Callable[..., Any], run: Callable[[Any], Any]) -> None:
        self.name, self.method, self.run = name, method, run

    def __get__(self, obj: Any, cls: type) -> Callable[..., Any]:
        def call(*args: Any, **kwargs: Any) -> Any:
            sandbox = obj
            if sandbox is None and args and isinstance(args[0], cls):
                sandbox, args = args[0], args[1:]  # Class.name(sandbox, ...)
            return self.run(self.method(cls, sandbox, args, kwargs))

        call.__name__ = self.name
        call.__doc__ = getattr(sdk_base(cls), self.name).__doc__
        return call


def _refused(name: str) -> staticmethod:
    """A method that raises in both its instance form and its class form."""

    def method(*args: Any, **kwargs: Any) -> Any:
        raise unsupported(f"Sandbox.{name}")

    method.__name__ = name
    method.__doc__ = f"Not supported on ate-env: raises ``SandboxException``. See {DOC}."
    return staticmethod(method)


def guard(cls: type, base: type) -> list[str]:
    """Refuses each public callable of the SDK class that cls neither maps nor inherits on purpose.

    A method that a newer SDK adds is refused, not inherited: an inherited
    one would call E2B's API. Returns the names it refused.
    """
    refused = []
    for name in dir(base):
        if name in INHERITED or (name.startswith("_") and name not in ("__enter__", "__aenter__")):
            continue
        static = inspect.getattr_static(base, name)
        if inspect.getattr_static(cls, name) is not static:
            continue  # mapped: the class has its own
        if isinstance(static, property) or not (callable(static) or hasattr(static, "__get__")):
            continue  # data
        setattr(cls, name, _refused(name))
        refused.append(name)
    return refused
