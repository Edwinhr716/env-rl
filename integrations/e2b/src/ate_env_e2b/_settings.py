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

"""Where the class finds ate-env-api, and which template an E2B name means.

Everything comes from the environment variables the ``ate-env`` CLI and the
other integrations read. E2B's own ``api_key``, ``api_url`` and ``domain``
are never used: a key meant for E2B's cloud must not reach ate-env-api.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from urllib.parse import urlparse

from ate_env_rl import DEFAULT_ATESPACE
from e2b import InvalidArgumentException, TemplateException

ENV_API = "SUBSTRATE_ENV_API"
ENV_AUTH_TOKEN = "SUBSTRATE_ENV_AUTH_TOKEN"
ENV_ATESPACE = "SUBSTRATE_ENV_ATESPACE"
ENV_TEMPLATES = "SUBSTRATE_ENV_E2B_TEMPLATES"

# The template that integrations/e2b/hack/base-template.sh prints.
DEFAULT_BASE_TEMPLATE = "e2b-base"
# E2B's name for its default template. SUBSTRATE_ENV_E2B_TEMPLATES can map it
# to another base template (base=<template>).
E2B_DEFAULT_TEMPLATE = "base"

# envd's port in the environment.
ENVD_PORT = 49983

# Asks the port proxy to wake a suspended environment and to hold the request
# until the guest answers, for at most this many seconds, instead of answering
# 503 with Retry-After. This is the one place that holds the name. The value
# is whole decimal seconds with no sign, fraction, unit or leading zero: a
# server with the feature answers 400 to another form, and a server without
# it ignores the header.
WAKE_WAIT_HEADER = "X-Env-Wake-Wait"
ENV_WAKE_WAIT = "SUBSTRATE_ENV_E2B_WAKE_WAIT"
DEFAULT_WAKE_WAIT_S = 60
_WAKE_WAIT = re.compile(r"0|[1-9][0-9]{0,8}")

_DIGEST_PINNED = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}")


@dataclass(frozen=True)
class Settings:
    """One ate-env-api and the names the class uses on it."""

    # host:port that the gRPC channel dials.
    target: str
    # Whether the endpoint is https: TLS for gRPC and for envd's HTTP.
    tls: bool
    # The URL the E2B SDK sends envd's requests to: the port proxy.
    sandbox_url: str
    # Not in the repr: a Settings in a log or a traceback must not show it.
    token: str | None = field(repr=False)
    atespace: str
    templates: Mapping[str, str]
    # Seconds that the port proxy holds a request to a waking environment; 0
    # sends no header.
    wake_wait: int = DEFAULT_WAKE_WAIT_S

    @classmethod
    def from_env(cls, sandbox_url: str | None = None) -> Settings:
        """Reads the environment. sandbox_url is E2B's per-call option of the
        same name: the URL for envd's requests when it is not the endpoint."""
        endpoint = os.environ.get(ENV_API, "").strip()
        if not endpoint:
            raise InvalidArgumentException(
                f"{ENV_API} is not set: it names the ate-env-api endpoint, as host:port or as a URL"
            )
        bad = InvalidArgumentException(
            f"{ENV_API}={endpoint!r}: want host:port, http://host:port or https://host[:port]"
        )
        url = urlparse(endpoint if "://" in endpoint else f"//{endpoint}")
        try:
            port = url.port
        except ValueError:
            raise bad from None
        tls = url.scheme == "https"
        # ate-env-api serves at the root of one address: a path, its
        # parameters (";x=1"), a query or a user would be dropped without a
        # word. Only https has a default port.
        if (
            url.scheme not in ("", "http", "https")
            or not url.hostname
            or url.path not in ("", "/")
            or url.params
            or url.query
            or url.fragment
            or url.username is not None
            or port == 0
            or (port is None and not tls)
        ):
            raise bad
        host = f"[{url.hostname}]" if ":" in url.hostname else url.hostname
        target = f"{host}:{port or 443}"
        return cls(
            target=target,
            tls=tls,
            sandbox_url=sandbox_url or f"{'https' if tls else 'http'}://{target}",
            token=os.environ.get(ENV_AUTH_TOKEN, "").strip() or None,
            atespace=os.environ.get(ENV_ATESPACE, "").strip() or DEFAULT_ATESPACE,
            templates=_parse_templates(os.environ.get(ENV_TEMPLATES, "")),
            wake_wait=_parse_wake_wait(os.environ.get(ENV_WAKE_WAIT, "")),
        )

    def resolve_template(self, template: str | None) -> tuple[str, str | None]:
        """Returns the ActorTemplate and, for an image, the rootfs image.

        In order: no name is E2B's ``base``; a name in the mapping is that
        registered template; ``base`` is the base template; a digest-pinned
        image runs on the base template. Anything else is an error, because
        E2B template ids and unpinned images have no meaning here.
        """
        template = template or E2B_DEFAULT_TEMPLATE
        if template in self.templates:
            return self.templates[template], None
        if template == E2B_DEFAULT_TEMPLATE:
            return self.base_template, None
        if _DIGEST_PINNED.fullmatch(template):
            return self.base_template, template
        raise TemplateException(
            f"template {template!r} is not known: name it in {ENV_TEMPLATES} (name=template,...), "
            "or pass a digest-pinned image (repo@sha256:...)"
        )

    @property
    def base_template(self) -> str:
        """The template of E2B's ``base`` and of each image."""
        return self.templates.get(E2B_DEFAULT_TEMPLATE, DEFAULT_BASE_TEMPLATE)

    def with_sandbox_url(self, sandbox_url: str | None) -> Settings:
        return replace(self, sandbox_url=sandbox_url) if sandbox_url else self

    def proxy_headers(self, env_id: str) -> dict[str, str]:
        """The headers that route an envd request through the port proxy."""
        headers = {
            "X-Env-Id": env_id,
            "X-Env-Atespace": self.atespace,
            "X-Env-Port": str(ENVD_PORT),
        }
        if self.wake_wait:
            headers[WAKE_WAIT_HEADER] = str(self.wake_wait)
        if self.token:
            # The proxy checks and removes it; envd never sees the API token.
            headers["X-Env-Authorization"] = f"Bearer {self.token}"
        return headers


def _parse_wake_wait(raw: str) -> int:
    raw = raw.strip()
    if not raw:
        return DEFAULT_WAKE_WAIT_S
    if not _WAKE_WAIT.fullmatch(raw):
        raise InvalidArgumentException(
            f"{ENV_WAKE_WAIT}={raw!r}: want whole seconds, 0 (no hold) to 999999999"
        )
    return int(raw)


def _parse_templates(raw: str) -> dict[str, str]:
    templates: dict[str, str] = {}
    for pair in filter(None, (p.strip() for p in raw.split(","))):
        name, sep, template = pair.partition("=")
        if not sep or not name.strip() or not template.strip():
            raise InvalidArgumentException(
                f"{ENV_TEMPLATES}: {pair!r} is not name=template"
            )
        templates[name.strip()] = template.strip()
    return templates
