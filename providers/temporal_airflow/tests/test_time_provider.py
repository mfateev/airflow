"""Tests for time provider module."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from temporal_airflow.time_provider import (
    get_current_time,
    set_workflow_time,
    clear_workflow_time,
)
from airflow._shared.timezones import timezone


class TestTimeProvider:
    """Test suite for time provider."""

    def test_get_current_time_without_injection(self):
        """Test that get_current_time returns real time when not injected."""
        # Clear any existing injection
        clear_workflow_time()

        # Get current time
        before = timezone.utcnow()
        current = get_current_time()
        after = timezone.utcnow()

        # Should be real time (within 1 second tolerance)
        assert before <= current <= after + timedelta(seconds=1)

    def test_get_current_time_with_injection(self):
        """Test that get_current_time returns injected time."""
        try:
            # Set a specific time
            test_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            set_workflow_time(test_time)

            # Should return injected time
            assert get_current_time() == test_time

        finally:
            clear_workflow_time()

    def test_clear_workflow_time(self):
        """Test that clear resets to real time."""
        try:
            # Set injected time
            test_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            set_workflow_time(test_time)
            assert get_current_time() == test_time

            # Clear it
            clear_workflow_time()

            # Should now return real time
            before = timezone.utcnow()
            current = get_current_time()
            after = timezone.utcnow()

            assert before <= current <= after + timedelta(seconds=1)
            assert current != test_time  # Not the injected time

        finally:
            clear_workflow_time()

    def test_context_isolation(self):
        """Test that time injection is isolated per context."""
        import threading

        results = []

        def worker(time_value):
            set_workflow_time(time_value)
            # Small delay to ensure overlap
            import time
            time.sleep(0.01)
            results.append(get_current_time())
            clear_workflow_time()

        # Start two threads with different times
        time1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        time2 = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

        t1 = threading.Thread(target=worker, args=(time1,))
        t2 = threading.Thread(target=worker, args=(time2,))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Each thread should have seen its own time
        assert time1 in results
        assert time2 in results
