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

"""The lifecycle of the sync class against the fake ate-env-api.

The fake has no envd beyond /health, /init and one process start, thus these
tests cover what the class itself does: the control plane calls, the wait,
the root check, the cleanup, the deadlines, the refusals. The commands, files
and PTYs of the SDK need a real envd: tests/test_sandbox_e2e.py runs them on
a cluster. The fake's HTTP listener is not on its gRPC port, thus each test
passes E2B's ``sandbox_url``; the default, the endpoint itself, is covered by
its derivation in test_settings.py and by the cluster test.
"""

from __future__ import annotations

import asyncio
import os
import select
import signal
import threading
import time
import warnings

import e2b
import grpc
import pytest
from ate_env_rl import EnvironmentStatus
from e2b import (
    AuthenticationException,
    FileNotFoundException,
    NotFoundException,
    SandboxException,
    SandboxNotFoundException,
    SandboxState,
    TemplateException,
    TimeoutException,
)

from ate_env_e2b import Sandbox, TimeoutNotEnforcedWarning, _core
from ate_env_e2b._settings import WAKE_WAIT_HEADER

from .conftest import TOKEN, Ate

IMAGE = "docker.io/library/python@sha256:" + "a" * 64
REFUSED = r"is not supported on ate-env; see integrations/e2b/README\.md"
START = "/process.Process/Start"


def _create(ate: Ate, *args, **kwargs) -> Sandbox:
    return Sandbox.create(*args, sandbox_url=ate.url, **kwargs)


# --- create ---


def test_create_makes_the_environment_waits_for_envd_and_checks_the_user(ate: Ate):
    sandbox = _create(ate)
    env_id = sandbox.sandbox_id
    assert ate.state.creates == [(env_id, "ate-env", "e2b-base", "")]
    # No /init: envd has no token to get, and the program gave no variables.
    assert ate.state.requests == [("GET", "/health", env_id), ("POST", START, env_id)]
    assert ate.state.guest_commands == []
    assert sandbox.is_running()
    assert sandbox.kill() is True


def test_create_gives_envd_the_variables_and_no_access_token(ate: Ate):
    sandbox = _create(ate, envs={"A": "1"})
    env_id = sandbox.sandbox_id
    assert ate.state.inits[env_id] == [{"envVars": {"A": "1"}}]
    assert [path for _, path, _ in ate.state.requests] == ["/health", "/init", START]
    sandbox.is_running()
    assert "x-access-token" not in ate.state.header_names
    assert "x-env-authorization" in ate.state.header_names


def test_create_runs_an_image_on_the_base_template_and_a_mapped_name_as_is(ate: Ate, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_ENV_E2B_TEMPLATES", "node=e2b-node")
    monkeypatch.setenv("SUBSTRATE_ENV_ATESPACE", "team")
    by_image, by_name = _create(ate, IMAGE), _create(ate, template="node")
    assert ate.state.creates == [
        (by_image.sandbox_id, "team", "e2b-base", IMAGE),
        (by_name.sandbox_id, "team", "e2b-node", ""),
    ]


def test_an_unknown_template_creates_nothing(ate: Ate):
    with pytest.raises(TemplateException):
        _create(ate, "python:3.13")
    assert ate.state.creates == []


def test_create_waits_while_the_proxy_has_no_answer_yet(ate: Ate):
    ate.state.health_status = 502
    timer = threading.Timer(1.0, lambda: setattr(ate.state, "health_status", None))
    timer.start()
    try:
        sandbox = _create(ate)
    finally:
        timer.cancel()
    assert ate.state.requests.count(("GET", "/health", sandbox.sandbox_id)) > 1


def test_the_wait_goes_on_through_the_answers_of_a_wake(ate: Ate):
    # The port proxy passes the router's 5xx through during a wake, and a
    # request can reach the router before it knows the new environment.
    for status in (500, 503, 404):
        ate.state.health_status = status
        timer = threading.Timer(0.8, lambda: setattr(ate.state, "health_status", None))
        timer.start()
        try:
            assert _create(ate).is_running(), status
        finally:
            timer.cancel()


def test_each_request_to_the_port_proxy_asks_it_to_wait_for_a_wake(ate: Ate):
    sandbox = _create(ate)
    assert WAKE_WAIT_HEADER.lower() in ate.state.header_names
    # Also the requests of the SDK's own data plane.
    ate.state.header_names.clear()
    with pytest.raises(FileNotFoundException):
        sandbox.files.read("/etc/hostname")  # the fake envd has no /files
    assert WAKE_WAIT_HEADER.lower() in ate.state.header_names


# --- the cleanup of a failed create ---


def test_a_create_that_fails_after_the_environment_exists_deletes_it(ate: Ate):
    ate.state.health_status = 502
    with pytest.raises(TimeoutException, match=r"within 1s: envd's /health answered last: 502"):
        _create(ate, request_timeout=1)
    assert len(ate.state.creates) == 1 and ate.state.envs == {}

    # A crashed environment ends the wait at once.
    ate.state.health_status = 410
    with pytest.raises(SandboxException, match="410"):
        _create(ate)
    assert len(ate.state.creates) == 2 and ate.state.envs == {}

    ate.state.health_status, ate.state.init_status = None, 500
    with pytest.raises(SandboxException, match=r"refused /init: 500"):
        _create(ate, envs={"A": "1"})
    assert len(ate.state.creates) == 3 and ate.state.envs == {}


def test_a_failed_cleanup_says_that_the_environment_may_remain_and_names_it(ate: Ate):
    ate.state.health_status = 410
    ate.state.delete_code = grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(SandboxException, match="410") as raised:
        _create(ate)
    (env_id,) = ate.state.envs
    assert f"environment {env_id} in atespace ate-env may remain" in str(raised.value)
    assert "delete refused" in str(raised.value)


def test_a_create_whose_answer_is_lost_still_deletes_the_environment(ate: Ate):
    # The server made the environment and the call failed on the way back.
    ate.state.create_code_after = grpc.StatusCode.UNAVAILABLE
    with pytest.raises(SandboxException):
        _create(ate)
    assert len(ate.state.creates) == 1 and ate.state.envs == {}


def test_a_create_that_the_server_refused_sends_no_delete(ate: Ate, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", "not-" + TOKEN)
    with pytest.raises(AuthenticationException) as raised:
        _create(ate)
    assert ate.state.deletes == [] and "may remain" not in str(raised.value)


def test_a_deadline_during_the_create_rpc_waits_for_it_and_deletes_the_environment(ate: Ate):
    # The server completes the create after the deadline of the caller. A
    # delete sent at the deadline would find nothing and leave the environment.
    ate.state.delays["CreateEnvironment"] = 1.5
    start = time.monotonic()
    with pytest.raises(TimeoutException, match="the create of environment e2b-.* within 0.3s"):
        _create(ate, request_timeout=0.3)
    assert 1.4 < time.monotonic() - start < 5
    assert len(ate.state.creates) == 1 and ate.state.envs == {}
    assert ate.state.deletes == [ate.state.creates[0][0]]


def test_an_interrupt_during_create_deletes_the_environment(ate: Ate):
    ate.state.delays["CreateEnvironment"] = 1.0
    timer = threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGINT))
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        _create(ate)
    # The call returned only after its cleanup: the create ended, then the delete.
    assert len(ate.state.creates) == 1 and ate.state.envs == {}
    assert ate.state.deletes == [ate.state.creates[0][0]]

    ate.state.delays["CreateEnvironment"] = 0
    ate.state.health_status = 502  # the interrupt arrives during the wait for envd
    ate.state.delete_code = grpc.StatusCode.UNAVAILABLE
    timer = threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGINT))
    timer.start()
    with pytest.raises(KeyboardInterrupt) as raised:
        _create(ate)
    (env_id,) = ate.state.envs
    assert any(f"environment {env_id} in atespace ate-env may remain" in n
               for n in raised.value.__notes__)


# --- the image must run as root ---


def test_create_refuses_an_image_that_does_not_run_as_root_and_deletes_the_environment(ate: Ate):
    ate.state.image_user = (1000, "node")
    with pytest.raises(SandboxException) as raised:
        _create(ate, IMAGE)
    message = str(raised.value)
    assert f"image {IMAGE} runs as user 1000 (node), not as root" in message
    assert "needs an image that runs as root" in message
    # What envd answered, and where the limit is written down.
    assert "(envd: error starting process: fork/exec /bin/sh: operation not permitted)" in message
    assert "see integrations/e2b/README.md" in message
    assert len(ate.state.creates) == 1 and ate.state.envs == {}
    # One process start through envd; the guest names the user only after it failed.
    assert [path for _, path, _ in ate.state.requests].count(START) == 1
    assert len(ate.state.guest_commands) == 1


def test_create_refuses_a_template_where_root_cannot_start_a_process(ate: Ate):
    # As on a template without SETUID and SETGID: the image runs as root, and
    # envd still cannot give a process its user.
    ate.state.start_error = "error starting process: fork/exec /bin/sh: operation not permitted"
    with pytest.raises(SandboxException) as raised:
        _create(ate)
    message = str(raised.value)
    assert "(template e2b-base) cannot start a process" in message
    assert "operation not permitted" in message and "capabilities" in message
    assert ate.state.envs == {}


# --- what leaves the process ---


def test_without_an_api_token_the_proxy_gets_no_credential(ate: Ate, monkeypatch):
    ate.state.require_token = None
    monkeypatch.delenv("SUBSTRATE_ENV_AUTH_TOKEN")
    sandbox = _create(ate, envs={"A": "1"})
    assert ate.state.inits[sandbox.sandbox_id] == [{"envVars": {"A": "1"}}]
    assert not any("Bearer" in value for value in ate.state.header_values)
    assert not {"x-env-authorization", "x-access-token"} & ate.state.header_names


def test_an_e2b_api_key_never_reaches_ate_env_api(ate: Ate, monkeypatch):
    key = "e2b_" + "9" * 40
    monkeypatch.setenv("E2B_API_KEY", key)
    sandbox = Sandbox.create(sandbox_url=ate.url, api_key=key)
    sandbox.is_running()
    with pytest.raises(FileNotFoundException):
        sandbox.files.read("/etc/hostname")
    sandbox.kill()
    # HTTP headers and gRPC metadata alike.
    assert any(TOKEN in value for value in ate.state.header_values)
    assert not any(key in value for value in ate.state.header_values)


def test_the_sdk_object_cannot_reach_the_api_of_e2b(ate: Ate, monkeypatch):
    key = "e2b_" + "9" * 40
    monkeypatch.setenv("E2B_API_KEY", key)
    monkeypatch.setenv("E2B_DOMAIN", "e2b.example")
    monkeypatch.setenv("E2B_API_URL", "https://api.e2b.example")
    monkeypatch.setenv("E2B_SANDBOX_URL", "https://sandbox.e2b.example")
    monkeypatch.setenv("E2B_DEBUG", "true")
    for sandbox in (_create(ate, api_key=key), Sandbox.connect(_create(ate).sandbox_id,
                                                              sandbox_url=ate.url)):
        config = sandbox.connection_config
        assert (config.api_key, config.api_url) == ("unused", "http://ate-env-e2b.invalid")
        assert config.domain == "ate-env-e2b.invalid" and config.debug is False
        assert sandbox.envd_api_url == sandbox.envd_direct_url == ate.url
        # What each SDK method that calls E2B's API builds its request from.
        params = config.get_api_params()
        assert (params["api_key"], params["api_url"], params["domain"]) == (
            "unused",
            "http://ate-env-e2b.invalid",
            "ate-env-e2b.invalid",
        )
        assert key not in repr(vars(config)) and "e2b.example" not in repr(vars(config))


# --- kill, pause, connect, state ---


def test_the_with_block_deletes_the_environment_also_when_the_body_raises(ate: Ate):
    with _create(ate) as sandbox:
        assert sandbox.sandbox_id in ate.state.envs
    assert ate.state.envs == {}
    with pytest.raises(RuntimeError, match="body"), _create(ate):
        raise RuntimeError("body")
    assert ate.state.envs == {}


def test_kill_is_true_once_and_false_for_an_environment_that_is_gone(ate: Ate):
    sandbox = _create(ate)
    assert sandbox.kill() is True
    assert sandbox.kill() is False
    assert Sandbox.kill(sandbox.sandbox_id, sandbox_url=ate.url) is False
    other = _create(ate)
    assert Sandbox.kill(other.sandbox_id, sandbox_url=ate.url) is True
    assert Sandbox.kill(sandbox_id=_create(ate).sandbox_id, sandbox_url=ate.url) is True
    assert Sandbox.kill(_create(ate)) is True  # Class.method(sandbox), as in the SDK
    assert ate.state.envs == {}


def test_pause_suspends_durably_and_is_false_when_already_suspended(ate: Ate):
    sandbox = _create(ate)
    assert sandbox.pause() is True
    # The client sends a durable suspend as the unset mode, never LOCAL (2).
    assert ate.state.suspends == [(sandbox.sandbox_id, 0)]
    assert sandbox.pause() is False
    assert sandbox.beta_pause() is False
    assert Sandbox.pause(sandbox.sandbox_id, sandbox_url=ate.url) is False
    assert len(ate.state.suspends) == 1

    for status in (EnvironmentStatus.SUSPENDING, EnvironmentStatus.PAUSING, EnvironmentStatus.PAUSED):
        ate.state.set_status(sandbox.sandbox_id, status)
        assert sandbox.pause() is False


def test_pause_refuses_what_a_suspend_cannot_do(ate: Ate):
    sandbox = _create(ate)
    for kwargs in ({"mode": "filesystem"}, {"keep_memory": False}, {"strict": True}):
        with pytest.raises(SandboxException, match=REFUSED):
            sandbox.pause(**kwargs)
        with pytest.raises(SandboxException, match=REFUSED):
            Sandbox.pause(sandbox.sandbox_id, sandbox_url=ate.url, **kwargs)
    for kwargs in ({"mode": "filesystem"}, {"keep_memory": False}, {"mode": "half"}):
        with pytest.raises(SandboxException, match=REFUSED):
            sandbox.beta_pause(**kwargs)
    with pytest.raises(TypeError):
        sandbox.beta_pause(strict=False, mode="full", keep_memory=True, extra=1, **{"strict": 1})
    assert ate.state.suspends == []
    assert sandbox.pause(keep_memory=True, mode="full", strict=False) is True


def test_pause_of_an_environment_that_is_gone_raises(ate: Ate):
    sandbox = _create(ate)
    sandbox.kill()
    with pytest.raises(SandboxNotFoundException):
        sandbox.pause()


def test_is_running_reads_the_status_and_never_wakes_a_suspended_environment(ate: Ate):
    sandbox = _create(ate)
    sandbox.pause()
    seen = len(ate.state.requests)
    assert sandbox.is_running() is False
    assert len(ate.state.requests) == seen
    assert ate.state.status(sandbox.sandbox_id) is EnvironmentStatus.SUSPENDED

    ate.state.set_status(sandbox.sandbox_id, EnvironmentStatus.CRASHED)
    assert sandbox.is_running() is False
    sandbox.kill()
    assert sandbox.is_running() is False


def test_connect_from_another_process_wakes_the_environment(ate: Ate):
    first = _create(ate, envs={"A": "1"})
    first.pause()
    ate.state.wake_503s = 2

    second = Sandbox.connect(first.sandbox_id, sandbox_url=ate.url)

    assert second.sandbox_id == first.sandbox_id and second is not first
    assert ate.state.status(first.sandbox_id) is EnvironmentStatus.RUNNING
    # A connect sends no /init: the variables of the create stay.
    assert ate.state.inits[first.sandbox_id] == [{"envVars": {"A": "1"}}]
    assert first.connect() is first
    assert second.is_running() and second.kill() is True


def test_connect_to_a_missing_or_crashed_environment_raises_at_once(ate: Ate):
    with pytest.raises(SandboxNotFoundException):
        Sandbox.connect("e2b-000000000000", sandbox_url=ate.url)
    sandbox = _create(ate)
    seen = len(ate.state.requests)
    ate.state.set_status(sandbox.sandbox_id, EnvironmentStatus.CRASHED)
    with pytest.raises(SandboxException, match="has crashed"):
        sandbox.connect()
    ate.state.set_status(sandbox.sandbox_id, EnvironmentStatus.DELETING)
    with pytest.raises(SandboxNotFoundException):
        sandbox.connect()
    assert len(ate.state.requests) == seen


def test_connect_refuses_a_reboot(ate: Ate):
    sandbox = _create(ate)
    for on_resume in ("reboot", "later"):
        with pytest.raises(SandboxException, match=rf'connect\(on_resume=\'{on_resume}\'\) {REFUSED}'):
            sandbox.connect(on_resume=on_resume)
    with pytest.raises(SandboxException, match=REFUSED):
        Sandbox.connect(sandbox.sandbox_id, on_resume="reboot", sandbox_url=ate.url)
    assert sandbox.connect(on_resume="restore") is sandbox


def test_get_info_maps_the_status_and_names_the_template(ate: Ate):
    sandbox = _create(ate)
    info = sandbox.get_info()
    assert (info.sandbox_id, info.template_id, info.state) == (
        sandbox.sandbox_id,
        "e2b-base",
        SandboxState.RUNNING,
    )
    assert info.envd_version == "0.10.2" and info.metadata == {}
    sandbox.pause()
    assert sandbox.get_info().state is SandboxState.PAUSED
    assert Sandbox.get_info(sandbox.sandbox_id, sandbox_url=ate.url).state is SandboxState.PAUSED
    ate.state.set_status(sandbox.sandbox_id, EnvironmentStatus.RESUMING)
    assert sandbox.get_info().state is SandboxState.RUNNING
    ate.state.set_status(sandbox.sandbox_id, EnvironmentStatus.CRASHED)
    with pytest.raises(SandboxException, match="has crashed"):
        sandbox.get_info()
    sandbox.kill()
    with pytest.raises(SandboxNotFoundException):
        sandbox.get_info()


def test_a_data_plane_call_on_an_environment_that_is_gone_or_crashed(ate: Ate):
    """What integrations/e2b/README.md says a program gets: the SDK's own
    mapping of the port proxy's 404 and 410."""
    sandbox = _create(ate)
    ate.state.set_status(sandbox.sandbox_id, EnvironmentStatus.CRASHED)
    with pytest.raises(SandboxException, match="has crashed") as crashed:
        sandbox.commands.run("true")
    assert type(crashed.value) is SandboxException
    with pytest.raises(SandboxException, match="410: .*has crashed") as crashed:
        sandbox.files.read("/etc/hostname")
    assert type(crashed.value) is SandboxException

    # The port proxy answers 404 for an environment that is being deleted,
    # and the request does not wake it.
    ate.state.set_status(sandbox.sandbox_id, EnvironmentStatus.DELETING)
    with pytest.raises(FileNotFoundException, match="is being deleted"):
        sandbox.files.read("/etc/hostname")
    assert ate.state.status(sandbox.sandbox_id) is EnvironmentStatus.DELETING

    sandbox.kill()
    with pytest.raises(NotFoundException, match="was not found") as gone:
        sandbox.commands.run("true")
    assert type(gone.value) is NotFoundException
    for call in (lambda: sandbox.files.read("/etc/hostname"), lambda: sandbox.files.list("/")):
        with pytest.raises(FileNotFoundException, match="was not found"):
            call()


# --- refusals ---


@pytest.mark.parametrize(
    "kwargs",
    [
        {"allow_internet_access": False},
        {"network": {"deny_out": ["0.0.0.0/0"]}},
        {"metadata": {"run": "1"}},
        {"lifecycle": {"on_timeout": "pause"}},
        {"idempotency_key": "k"},
        {"iam": {"role": "reader"}},
        {"volume_mounts": {"/data": "volume"}},
        {"mcp": {"github": {"token": "t"}}},
        # A value that is not None is refused, also an empty one: the SDK
        # starts its MCP gateway for an empty mcp, for example.
        {"mcp": {}},
        {"mcp": {}, "template": "base"},
        {"network": {}},
        {"metadata": {}},
        {"volume_mounts": {}},
        {"lifecycle": {}},
        {"iam": {}},
        {"idempotency_key": ""},
        # envd has no access token: a program that asks for one does not get less.
        {"secure": True},
        {"secure": False},
        # E2B's per-call options with no meaning here.
        {"domain": "e2b.example"},
        {"api_url": "https://api.e2b.example"},
        {"headers": {"X-A": "1"}},
        {"api_headers": {}},
        {"proxy": "http://127.0.0.1:3128"},
        {"retries": 0},
        {"debug": True},
    ],
)
def test_create_refuses_an_argument_it_cannot_honour_and_creates_nothing(ate: Ate, kwargs):
    (name,) = set(kwargs) - {"template"}
    with pytest.raises(SandboxException, match=rf"create\({name}=.*\) {REFUSED}"):
        _create(ate, **kwargs)
    assert ate.state.creates == [] and ate.state.requests == []


def test_create_takes_the_values_that_mean_what_it_does(ate: Ate):
    sandbox = _create(
        ate, "base", None, None, None, None, True, debug=False, api_key="e2b_x",
        validate_api_key=False, http_version="1.1", request_timeout=30,
    )
    assert sandbox.kill() is True


def test_an_argument_the_sdk_does_not_know_raises_and_is_never_dropped(ate: Ate):
    with pytest.raises(SandboxException, match=rf"create\(deny_everything=True\) {REFUSED}"):
        _create(ate, deny_everything=True)
    assert ate.state.creates == []
    sandbox = _create(ate)
    for call in (sandbox.kill, sandbox.pause, sandbox.beta_pause, sandbox.get_info, sandbox.connect):
        with pytest.raises(SandboxException, match=rf"\(force=True\) {REFUSED}"):
            call(force=True)
    with pytest.raises(SandboxException, match=rf"kill\(force=True\) {REFUSED}"):
        Sandbox.kill(sandbox.sandbox_id, force=True)
    with pytest.raises(TypeError):
        sandbox.is_running(force=True)
    assert sandbox.sandbox_id in ate.state.envs


def test_refused_methods_raise_on_an_instance(ate: Ate):
    sandbox = _create(ate)
    for call in (
        lambda: sandbox.set_timeout(60),
        lambda: sandbox.get_host(8080),
        lambda: sandbox.download_url("/tmp/a"),
        lambda: sandbox.upload_url("/tmp/a"),
        lambda: sandbox.update_network({"allow_out": []}),
        sandbox.fork,
        sandbox.create_snapshot,
        sandbox.list_snapshots,
        sandbox.get_metrics,
        sandbox.get_mcp_url,
        sandbox.get_mcp_token,
        Sandbox.list,
    ):
        with pytest.raises(SandboxException, match=REFUSED):
            call()


def test_a_timeout_warns_that_no_server_enforces_it(ate: Ate):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _create(ate)  # the default program passes none: no warning
    with pytest.warns(TimeoutNotEnforcedWarning, match="timeout=120 is not enforced"):
        sandbox = _create(ate, timeout=120)
    with pytest.warns(TimeoutNotEnforcedWarning):
        sandbox.connect(timeout=60)
    # On each call, in each form, and at the line of the caller.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(2):
            sandbox.connect(timeout=60)
        Sandbox.connect(sandbox.sandbox_id, timeout=60, sandbox_url=ate.url)
        Sandbox.create(timeout=60, sandbox_url=ate.url)
    assert [w.category for w in caught] == [TimeoutNotEnforcedWarning] * 4
    assert {w.filename for w in caught} == {__file__}


def test_a_wrong_token_is_an_authentication_error(ate: Ate, monkeypatch):
    sandbox = _create(ate)
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", "not-" + TOKEN)
    with pytest.raises(AuthenticationException):
        _create(ate)
    assert len(ate.state.creates) == 1
    # A refused token is not "the sandbox is not running".
    ate.state.require_token = "rotated"
    with pytest.raises(AuthenticationException):
        sandbox.is_running()


def test_an_option_the_sdk_refuses_raises_in_each_form_and_sends_nothing(ate: Ate):
    # The SDK checks http_version when it builds its connection
    # configuration. The forms that build none check it too.
    sandbox = _create(ate)
    env_id = sandbox.sandbox_id
    before = (list(ate.state.creates), list(ate.state.deletes), list(ate.state.requests))
    opts = {"http_version": "3"}
    for call in (
        lambda: _create(ate, **opts),
        lambda: sandbox.get_info(**opts),
        lambda: sandbox.connect(**opts),
        lambda: sandbox.pause(**opts),
        lambda: sandbox.beta_pause(**opts),
        lambda: sandbox.kill(**opts),
        lambda: Sandbox.get_info(env_id, sandbox_url=ate.url, **opts),
        lambda: Sandbox.connect(env_id, sandbox_url=ate.url, **opts),
        lambda: Sandbox.pause(env_id, sandbox_url=ate.url, **opts),
        lambda: Sandbox.kill(env_id, sandbox_url=ate.url, **opts),
    ):
        with pytest.raises(e2b.InvalidArgumentException, match="http_version"):
            call()
    assert (ate.state.creates, ate.state.deletes, ate.state.requests) == before
    assert ate.state.status(env_id) is EnvironmentStatus.RUNNING
    assert sandbox.kill(http_version="2") is True


# --- deadlines ---


def test_request_timeout_bounds_each_control_plane_call(ate: Ate):
    sandbox = _create(ate)
    ate.state.delays.update(GetEnvironment=3, DeleteEnvironment=3)
    for call, what in (
        (lambda: sandbox.is_running(request_timeout=0.3), "the check of"),
        (lambda: sandbox.get_info(request_timeout=0.3), "the read of"),
        (lambda: sandbox.pause(request_timeout=0.3), "the pause of"),
        (lambda: sandbox.connect(request_timeout=0.3), "the connect to"),
        (lambda: sandbox.kill(request_timeout=0.3), "the kill of"),
        (lambda: Sandbox.kill(sandbox.sandbox_id, sandbox_url=ate.url, request_timeout=0.3),
         "the kill of"),
    ):
        start = time.monotonic()
        with pytest.raises(TimeoutException, match=rf"{what} .*within 0.3s \(request_timeout"):
            call()
        assert time.monotonic() - start < 1.5


def test_the_request_timeout_of_create_bounds_the_later_calls_of_the_sandbox(ate: Ate):
    # As in E2B: the option applies to each request of the returned sandbox,
    # thus also to the kill at the end of the `with` block.
    sandbox = _create(ate, request_timeout=0.4)
    ate.state.delays["DeleteEnvironment"] = 3
    start = time.monotonic()
    with pytest.raises(TimeoutException, match="the kill of"), sandbox:
        pass
    assert time.monotonic() - start < 1.5
    ate.state.delays.clear()
    assert sandbox.kill(request_timeout=5) is True


@pytest.mark.parametrize("path", ["/init", START])
def test_a_slow_envd_during_create_ends_at_the_request_timeout_as_a_timeout(
    ate: Ate, monkeypatch, path
):
    # The limit of one /health request is not the limit of /init or of the
    # root check: a request_timeout longer than it is honoured, and the
    # error is a TimeoutException, not a SandboxException for a transport error.
    monkeypatch.setattr(_core, "HEALTH_TIMEOUT_S", 0.2)
    ate.state.delays[path] = 3
    start = time.monotonic()
    with pytest.raises(TimeoutException, match=r"the create of .* within 1s \(request_timeout"):
        _create(ate, envs={"A": "1"}, request_timeout=1)
    assert 0.9 < time.monotonic() - start < 2.5
    assert ate.state.envs == {} and len(ate.state.deletes) == 1


def test_a_call_without_a_request_timeout_has_a_deadline_and_zero_is_the_longest():
    assert _core.limit(None, _core.SHORT_TIMEOUT_S) == 60
    assert _core.limit(None, _core.LONG_TIMEOUT_S) == 300
    assert _core.limit(0, _core.SHORT_TIMEOUT_S) == 300  # E2B's "no timeout"
    assert _core.limit(2.5, _core.LONG_TIMEOUT_S) == 2.5
    with pytest.raises(e2b.InvalidArgumentException):
        _core.limit(-1, 60)


# --- threads and processes ---


def test_the_class_works_from_a_thread_and_from_the_executor_of_a_running_loop(ate: Ate):
    def lifecycle() -> bool:
        sandbox = _create(ate)
        return sandbox.is_running() and sandbox.pause() and sandbox.connect().kill()

    results: list[bool] = []
    thread = threading.Thread(target=lambda: results.append(lifecycle()))
    thread.start()
    thread.join(60)
    assert results == [True]

    async def from_a_loop() -> bool:
        return await asyncio.get_running_loop().run_in_executor(None, lifecycle)

    assert asyncio.run(from_a_loop()) is True
    assert ate.state.envs == {}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
def test_a_call_in_a_child_of_fork_fails_at_once_and_the_parent_goes_on(ate: Ate):
    sandbox = _create(ate)
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:  # the child: no pytest teardown, no exit handlers
        answers = []
        for call in (sandbox.is_running, sandbox.kill, lambda: _create(ate)):
            try:
                answers.append(repr(call()))
            except BaseException as e:
                answers.append(f"{type(e).__name__}: {e}")
        os.write(write, "\n".join(answers).encode())
        os._exit(0)
    try:
        ready, _, _ = select.select([read], [], [], 20)
        assert ready, "the call in the child did not return"
        answers = os.read(read, 65536).decode().splitlines()
    finally:
        if not ready:
            os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        os.close(read)
        os.close(write)
    assert len(answers) == 3
    for answer in answers:
        assert answer.startswith("SandboxException: this process is a child of os.fork()")
        assert "integrations/e2b/README.md" in answer
    assert sandbox.is_running() and sandbox.kill() is True
