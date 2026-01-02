#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Pluggable time provider for Airflow.

This module provides a pluggable time provider mechanism that allows external
orchestrators (like Temporal) to inject their own time source for deterministic
execution during workflow replay.

Design:
- Uses ContextVar for thread-safe provider storage
- Fallback to real time when no provider is set
- No dependencies on external orchestrator modules
"""
from __future__ import annotations

from contextvars import ContextVar
from datetime import datetime
from typing import Callable

from airflow._shared.timezones import timezone

# Thread-safe context variable for time provider function
_time_provider: ContextVar[Callable[[], datetime] | None] = ContextVar(
    "time_provider", default=None
)


def get_current_time() -> datetime:
    """
    Get current time from the registered provider or fallback to real time.

    When a time provider is registered (e.g., by Temporal workflow),
    returns time from that provider for deterministic execution.
    Otherwise falls back to real system time.

    Returns:
        datetime: Current time (from provider or real)

    Examples:
        >>> # Normal Airflow execution - returns real time
        >>> get_current_time()  # Returns timezone.utcnow()

        >>> # With custom provider (e.g., Temporal workflow)
        >>> set_time_provider(workflow.now)  # Register Temporal's time function
        >>> get_current_time()  # Returns workflow.now()
    """
    provider = _time_provider.get()
    if provider is not None:
        return provider()
    return timezone.utcnow()


def set_time_provider(provider: Callable[[], datetime]) -> None:
    """
    Register a custom time provider function.

    Called by external orchestrators (like Temporal) to inject their
    deterministic time source into Airflow code.

    Args:
        provider: A callable that returns the current datetime.
                  For Temporal, this would be `workflow.now`.

    Example:
        >>> from temporalio import workflow
        >>> set_time_provider(workflow.now)
    """
    _time_provider.set(provider)


def clear_time_provider() -> None:
    """
    Clear the time provider, reverting to real time.

    Should be called when the orchestrator context ends (e.g., workflow completes)
    or in finally blocks to ensure cleanup.
    """
    _time_provider.set(None)
