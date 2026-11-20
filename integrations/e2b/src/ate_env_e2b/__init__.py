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

"""The E2B ``Sandbox`` and ``AsyncSandbox`` classes on ate-env environments.

    from ate_env_e2b import Sandbox      # was: from e2b import Sandbox
    from ate_env_e2b import AsyncSandbox # was: from e2b import AsyncSandbox

E2B's exceptions and the types its calls return are exported here too, thus
that one import line is the only change to a program.
"""

from e2b import (
    AuthenticationException,
    CommandExitException,
    CommandHandle,
    CommandResult,
    EntryInfo,
    FileNotFoundException,
    FilesystemEvent,
    FilesystemEventType,
    FileType,
    InvalidArgumentException,
    NotEnoughSpaceException,
    NotFoundException,
    OutputHandler,
    ProcessInfo,
    PtyOutput,
    PtySize,
    RateLimitException,
    SandboxException,
    SandboxInfo,
    SandboxNotFoundException,
    SandboxNotRunningException,
    SandboxState,
    SandboxUnreachableException,
    Stderr,
    Stdout,
    TemplateException,
    TimeoutException,
    WatchHandle,
    WriteInfo,
)

from ._surface import TimeoutNotEnforcedWarning
from .sandbox import Sandbox
from .sandbox_async import AsyncSandbox

__all__ = [
    "AsyncSandbox",
    "AuthenticationException",
    "CommandExitException",
    "CommandHandle",
    "CommandResult",
    "EntryInfo",
    "FileNotFoundException",
    "FileType",
    "FilesystemEvent",
    "FilesystemEventType",
    "InvalidArgumentException",
    "NotEnoughSpaceException",
    "NotFoundException",
    "OutputHandler",
    "ProcessInfo",
    "PtyOutput",
    "PtySize",
    "RateLimitException",
    "Sandbox",
    "SandboxException",
    "SandboxInfo",
    "SandboxNotFoundException",
    "SandboxNotRunningException",
    "SandboxState",
    "SandboxUnreachableException",
    "Stderr",
    "Stdout",
    "TemplateException",
    "TimeoutException",
    "TimeoutNotEnforcedWarning",
    "WatchHandle",
    "WriteInfo",
]
