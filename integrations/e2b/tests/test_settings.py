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

"""The endpoint, the names and the template rules, with no server."""

from __future__ import annotations

import re

import pytest
from e2b import InvalidArgumentException, TemplateException

from ate_env_e2b._settings import WAKE_WAIT_HEADER, Settings

IMAGE = "docker.io/library/python@sha256:" + "a" * 64


def _settings(monkeypatch: pytest.MonkeyPatch, **env: str) -> Settings:
    for name in (
        "SUBSTRATE_ENV_API",
        "SUBSTRATE_ENV_AUTH_TOKEN",
        "SUBSTRATE_ENV_ATESPACE",
        "SUBSTRATE_ENV_E2B_TEMPLATES",
        "SUBSTRATE_ENV_E2B_WAKE_WAIT",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in {"SUBSTRATE_ENV_API": "127.0.0.1:7777", **env}.items():
        monkeypatch.setenv(name, value)
    return Settings.from_env()


@pytest.mark.parametrize(
    ("endpoint", "target", "tls", "url"),
    [
        ("127.0.0.1:7777", "127.0.0.1:7777", False, "http://127.0.0.1:7777"),
        ("http://api.example:7777", "api.example:7777", False, "http://api.example:7777"),
        ("https://api.example", "api.example:443", True, "https://api.example:443"),
        ("https://api.example:8443/", "api.example:8443", True, "https://api.example:8443"),
        ("[::1]:7777", "[::1]:7777", False, "http://[::1]:7777"),
    ],
)
def test_the_endpoint_gives_the_grpc_target_and_the_sandbox_url(
    monkeypatch, endpoint, target, tls, url
):
    s = _settings(monkeypatch, SUBSTRATE_ENV_API=endpoint)
    assert (s.target, s.tls, s.sandbox_url) == (target, tls, url)


def test_no_endpoint_is_an_error_that_names_the_variable(monkeypatch):
    monkeypatch.delenv("SUBSTRATE_ENV_API", raising=False)
    with pytest.raises(InvalidArgumentException, match="SUBSTRATE_ENV_API"):
        Settings.from_env()


@pytest.mark.parametrize(
    "endpoint",
    [
        "ftp://api.example:21",
        "api.example",  # no port, and only https has a default
        "http://api.example",
        "api.example:0",
        "api.example:port",
        "http://api.example:99999",
        "http://api.example:7777/prefix",  # ate-env-api serves at the root
        "api.example:7777?x=1",
        "http://api.example:7777/;x=1",  # the parameters of the path
        "https://api.example/;x=1",
        "http://api.example:7777/#x",
        "http://user:secret@api.example:7777",
        "::1",
    ],
)
def test_an_endpoint_the_class_cannot_use_whole_is_an_error(monkeypatch, endpoint):
    monkeypatch.setenv("SUBSTRATE_ENV_API", endpoint)
    with pytest.raises(InvalidArgumentException, match="SUBSTRATE_ENV_API"):
        Settings.from_env()


def test_the_repr_of_the_settings_does_not_show_the_token(monkeypatch):
    s = _settings(monkeypatch, SUBSTRATE_ENV_AUTH_TOKEN="very-secret")
    assert s.token == "very-secret" and "very-secret" not in repr(s)


def test_defaults_and_overrides_of_the_names(monkeypatch):
    s = _settings(monkeypatch)
    assert (s.token, s.atespace, s.base_template) == (None, "ate-env", "e2b-base")
    s = _settings(
        monkeypatch,
        SUBSTRATE_ENV_AUTH_TOKEN=" tok ",
        SUBSTRATE_ENV_ATESPACE="team",
        SUBSTRATE_ENV_E2B_TEMPLATES="base=e2b-big",
        # Other integrations give this variable their own template: not read here.
        SUBSTRATE_ENV_TEMPLATE="default-template",
    )
    assert (s.token, s.atespace, s.base_template) == ("tok", "team", "e2b-big")


def test_a_template_resolves_in_the_order_mapping_image_default(monkeypatch):
    s = _settings(monkeypatch, SUBSTRATE_ENV_E2B_TEMPLATES="node=e2b-node")
    assert s.resolve_template("node") == ("e2b-node", None)
    assert s.resolve_template(IMAGE) == ("e2b-base", IMAGE)
    for default in (None, "", "base"):
        assert s.resolve_template(default) == ("e2b-base", None)
    # A mapping of E2B's default name is the base template, also of an image.
    s = _settings(monkeypatch, SUBSTRATE_ENV_E2B_TEMPLATES="node=e2b-node, base=e2b-mine")
    assert s.resolve_template("base") == s.resolve_template(None) == ("e2b-mine", None)
    assert s.resolve_template(IMAGE) == ("e2b-mine", IMAGE)


@pytest.mark.parametrize(
    "template", ["python:3.13", "rki5dems9wqfm4r03t7g", "e2b-base", IMAGE + "\n", IMAGE[:-1]]
)
def test_an_unknown_template_is_an_error(monkeypatch, template):
    with pytest.raises(TemplateException, match="SUBSTRATE_ENV_E2B_TEMPLATES"):
        _settings(monkeypatch).resolve_template(template)


def test_a_bad_template_mapping_is_an_error(monkeypatch):
    with pytest.raises(InvalidArgumentException, match="name=template"):
        _settings(monkeypatch, SUBSTRATE_ENV_E2B_TEMPLATES="node")


def test_the_proxy_headers_route_through_the_port_proxy_and_carry_no_envd_token(monkeypatch):
    routing = {
        "X-Env-Id": "e2b-1",
        "X-Env-Atespace": "ate-env",
        "X-Env-Port": "49983",
        WAKE_WAIT_HEADER: "60",
    }
    assert _settings(monkeypatch).proxy_headers("e2b-1") == routing
    s = _settings(monkeypatch, SUBSTRATE_ENV_AUTH_TOKEN="tok")
    assert s.proxy_headers("e2b-1") == {**routing, "X-Env-Authorization": "Bearer tok"}


@pytest.mark.parametrize(("raw", "sent"), [("", "60"), ("5", "5"), (" 120 ", "120"), ("0", None)])
def test_the_wake_wait_is_whole_seconds_and_zero_sends_no_header(monkeypatch, raw, sent):
    headers = _settings(monkeypatch, SUBSTRATE_ENV_E2B_WAKE_WAIT=raw).proxy_headers("e2b-1")
    assert headers.get(WAKE_WAIT_HEADER) == sent
    # The form that the port proxy takes: 0, or 1 to 9 digits with no leading zero.
    assert sent is None or re.fullmatch(r"[1-9][0-9]{0,8}", sent)


@pytest.mark.parametrize("raw", ["-1", "+5", "1.5", "5s", "05", "1e3", "1234567890", "on"])
def test_a_wake_wait_that_the_port_proxy_would_refuse_is_an_error(monkeypatch, raw):
    with pytest.raises(InvalidArgumentException, match="SUBSTRATE_ENV_E2B_WAKE_WAIT"):
        _settings(monkeypatch, SUBSTRATE_ENV_E2B_WAKE_WAIT=raw)
