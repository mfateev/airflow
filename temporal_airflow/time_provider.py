"""
Time provider for Temporal workflow determinism.

This module provides deterministic time injection for Temporal workflows
while maintaining backward compatibility with standard Airflow execution.

Design:
- Uses ContextVar for thread-safe time injection
- Fallback to real time when not in Temporal workflow
- No impact on standard Airflow execution paths
"""
from __future__ import annotations

from datetime import datetime
from contextvars import ContextVar

from airflow._shared.timezones import timezone

# Thread-safe context variable for workflow time
_current_time: ContextVar[datetime | None] = ContextVar('current_time', default=None)


def get_current_time() -> datetime:
    """
    Get current time - either from context (Temporal workflow) or real time.

    When running in Temporal workflow, returns deterministic workflow time.
    Otherwise falls back to real system time.

    Returns:
        datetime: Current time (injected or real)

    Examples:
        >>> # Normal Airflow execution
        >>> get_current_time()  # Returns timezone.utcnow()

        >>> # In Temporal workflow
        >>> set_workflow_time(datetime(2025, 1, 1, 12, 0, 0))
        >>> get_current_time()  # Returns datetime(2025, 1, 1, 12, 0, 0)
    """
    time = _current_time.get()
    if time is None:
        # Fallback to real time (for non-Temporal execution)
        return timezone.utcnow()
    return time


def set_workflow_time(time: datetime) -> None:
    """
    Set deterministic time for workflow execution.

    Called by Temporal workflow to inject workflow.now() into Airflow code.

    Args:
        time: The workflow time to inject

    Note:
        This should only be called from Temporal workflow code.
    """
    _current_time.set(time)


def clear_workflow_time() -> None:
    """
    Clear workflow time context.

    Resets to using real time. Should be called when workflow completes
    or in finally blocks.
    """
    _current_time.set(None)
