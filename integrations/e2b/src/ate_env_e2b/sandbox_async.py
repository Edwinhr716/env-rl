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

"""``e2b.AsyncSandbox`` on ate-env environments.

The same mapping as ``Sandbox`` (``_methods``, ``_core``), awaited on the
loop of the caller.
"""

from __future__ import annotations

import e2b

from ._methods import install


@install(lambda awaitable: awaitable)
class AsyncSandbox(e2b.AsyncSandbox):
    """E2B's ``AsyncSandbox`` with ate-env environments behind it.

    See ``Sandbox``. A gRPC channel belongs to an event loop: the class
    keeps one client per running loop.
    """
