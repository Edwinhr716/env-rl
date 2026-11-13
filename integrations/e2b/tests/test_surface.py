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

"""What the classes take from the SDK: public names only, and nothing by default.

Each public name of the SDK classes is mapped, inherited on purpose or
refused. A method or an argument that a newer SDK adds is refused at run
time; the tests here simulate both.
"""

from __future__ import annotations

import ast
import inspect
import pathlib

import e2b
import pytest
from e2b import SandboxException

import ate_env_e2b
from ate_env_e2b import AsyncSandbox, Sandbox, _surface
from ate_env_e2b._surface import ALLOWED, INHERITED, bind, guard

CLASSES = [(Sandbox, e2b.Sandbox), (AsyncSandbox, e2b.AsyncSandbox)]
REFUSED_DOC = r"not supported on ate-env; see integrations/e2b/README\.md"

# The control plane is ate-env-api.
MAPPED = set(ALLOWED)
# Refused: what guard() finds at e2b 2.63.
REFUSED = {
    "set_timeout",
    "list",
    "get_host",
    "download_url",
    "upload_url",
    "update_network",
    "fork",
    "create_snapshot",
    "list_snapshots",
    "delete_snapshot",
    "get_metrics",
    "get_mcp_url",
    "get_mcp_token",
}
CONTEXT = {"__enter__", "__exit__", "__aenter__", "__aexit__"}


def _own(cls: type) -> set[str]:
    """What the class body and its decorator define; of the dunder names only
    the ones that would override the SDK's constructor or context manager."""
    sdk_dunders = CONTEXT | {"__init__", "__new__", "__getattr__", "__getattribute__"}
    return {name for name in vars(cls) if not name.startswith("__") or name in sdk_dunders}


@pytest.mark.parametrize(("cls", "base"), CLASSES)
def test_each_public_name_of_the_sdk_class_is_mapped_inherited_or_refused(cls, base):
    public = {name for name in dir(base) if not name.startswith("_")}
    assert public == MAPPED | REFUSED | (INHERITED - CONTEXT)
    assert not (MAPPED & REFUSED or MAPPED & INHERITED or REFUSED & INHERITED)
    # The class defines the mapped and the refused names, and nothing else.
    assert _own(cls) == MAPPED | REFUSED


@pytest.mark.parametrize(("cls", "base"), CLASSES)
def test_the_class_overrides_no_private_name_of_the_sdk(cls, base):
    own = _own(cls)
    assert not {name for name in own if name.startswith("_")}
    # The private names of the SDK class, and its constructor.
    private = {name for name in dir(base) if name.startswith("_") and not name.startswith("__")}
    assert len(private) > 20
    for name in private | {"__init__"}:
        assert inspect.getattr_static(cls, name) is inspect.getattr_static(base, name), name
    # What a sandbox object keeps of its own does not shadow one either.
    assert "_ate" not in dir(base)


def test_the_package_uses_public_names_of_the_sdk_only():
    """Each import from e2b is `from e2b import <public name>` or `import e2b`,
    and no attribute that starts with `_` is read from an SDK object."""
    private_of_the_sdk = {
        name
        for base in (e2b.Sandbox, e2b.AsyncSandbox, e2b.ConnectionConfig)
        for name in dir(base)
        if name.startswith("_") and not name.startswith("__")
    } | {"class_method_variant", "_envd_api"}
    assert {"_envd_api", "_resolve_api_params", "_cls_kill", "_create"} <= private_of_the_sdk
    sources = sorted(pathlib.Path(ate_env_e2b.__file__).parent.glob("*.py"))
    assert len(sources) >= 6
    for source in sources:
        for node in ast.walk(ast.parse(source.read_text())):
            where = f"{source.name}:{getattr(node, 'lineno', 0)}"
            if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == "e2b":
                assert node.module == "e2b", f"{where}: from {node.module} import ..."
                for alias in node.names:
                    assert not alias.name.startswith("_") and alias.name in e2b.__all__, where
            if isinstance(node, ast.Import):
                assert not any(a.name.startswith("e2b.") for a in node.names), where
            if isinstance(node, ast.Attribute | ast.Name):
                name = node.attr if isinstance(node, ast.Attribute) else node.id
                assert name not in private_of_the_sdk, f"{where}: {name}"
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert node.value not in private_of_the_sdk, f"{where}: {node.value!r}"


@pytest.mark.parametrize(("cls", "base"), CLASSES)
@pytest.mark.parametrize("name", sorted(REFUSED))
def test_a_refused_method_raises_in_the_class_form_and_names_the_document(cls, base, name):
    with pytest.raises(SandboxException, match=REFUSED_DOC):
        getattr(cls, name)("e2b-0123456789ab")


@pytest.mark.parametrize(("cls", "base"), CLASSES)
def test_a_method_that_a_newer_sdk_adds_is_refused_and_not_inherited(cls, base, monkeypatch):
    def teleport(self_or_id, *args, **kwargs):
        raise AssertionError("the SDK's method ran: it would call E2B's API")

    monkeypatch.setattr(base, "teleport", teleport, raising=False)
    monkeypatch.setattr(base, "teleport_all", classmethod(teleport), raising=False)
    monkeypatch.setattr(base, "region", "eu", raising=False)  # data is not a method

    # As the import of the package does it, with the newer SDK installed.
    @ate_env_e2b._methods.install(lambda awaitable: awaitable)
    class Newer(base):
        pass

    assert set(_own(Newer)) == MAPPED | REFUSED | {"teleport", "teleport_all"}
    for call in (Newer.teleport, Newer.teleport_all):
        with pytest.raises(SandboxException, match=REFUSED_DOC):
            call("e2b-0123456789ab")
    assert Newer.region == "eu"
    # guard() names what it refused, and leaves a mapped method alone.
    assert guard(type("Again", (cls,), {}), base) == ["teleport", "teleport_all"]


@pytest.mark.parametrize(("cls", "base"), CLASSES)
@pytest.mark.parametrize("name", sorted(MAPPED))
def test_an_argument_that_a_newer_sdk_adds_is_refused_when_it_has_a_value(
    cls, base, name, monkeypatch
):
    signature = inspect.signature(getattr(base, name))
    keyword_only, var_keyword = inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.VAR_KEYWORD
    parameters = list(signature.parameters.values())
    newer = signature.replace(
        parameters=[
            *(p for p in parameters if p.kind is not var_keyword),
            inspect.Parameter("region", keyword_only, default=None),
            inspect.Parameter("tier", keyword_only, default="standard"),
            *(p for p in parameters if p.kind is var_keyword),
        ]
    )
    # The public signature of the SDK's method is what bind() reads.
    monkeypatch.setattr(_surface.inspect, "signature", lambda method: newer)

    assert bind(base, name, (), {})["tier"] == "standard"
    assert bind(base, name, (), {"region": None, "tier": "standard"})["region"] is None
    for kwargs in ({"region": "eu"}, {"tier": "fast"}):
        with pytest.raises(SandboxException, match=rf"{name}\((region|tier)=.*{REFUSED_DOC}"):
            bind(base, name, (), kwargs)
        if name != "is_running":  # it has no class form
            args = () if name == "create" else ("e2b-0123456789ab",)
            with pytest.raises(SandboxException, match=REFUSED_DOC):
                getattr(cls, name)(*args, **kwargs)


@pytest.mark.parametrize(("cls", "base"), CLASSES)
def test_the_refusal_table_names_only_arguments_of_the_sdk_signatures(cls, base):
    for name, allowed in ALLOWED.items():
        parameters = inspect.signature(getattr(base, name)).parameters
        extra = set(_surface.CLASS_FORM_ONLY.get(name, ()))
        assert set(allowed) <= set(parameters) | extra, name
        assert "opts" in parameters or name == "is_running", name
    assert set(_surface.ALLOWED_OPTS) <= set(e2b.ApiParams.__annotations__)


def test_bind_binds_as_the_sdk_does_in_both_forms():
    assert bind(e2b.Sandbox, "connect", (60,), {})["timeout"] == 60
    values = bind(e2b.Sandbox, "connect", ("e2b-1", 60, None), {}, class_form=True)
    assert (values["sandbox_id"], values["timeout"], values["logger"]) == ("e2b-1", 60, None)
    assert bind(e2b.Sandbox, "kill", (), {"sandbox_id": "e2b-1"}, class_form=True) == {
        "sandbox_id": "e2b-1"
    }
    # As Python does for a call that does not fit the signature.
    with pytest.raises(TypeError):
        bind(e2b.Sandbox, "kill", ("e2b-1",), {})
    with pytest.raises(TypeError):
        bind(e2b.Sandbox, "kill", (), {}, class_form=True)


def test_the_package_exports_exist_in_e2b():
    for name in ate_env_e2b.__all__:
        if name not in ("Sandbox", "AsyncSandbox", "TimeoutNotEnforcedWarning"):
            assert getattr(ate_env_e2b, name) is getattr(e2b, name), name
