"""Tests for time provider module."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from airflow.utils.time_provider import (
    get_current_time,
    set_time_provider,
    clear_time_provider,
)
from airflow._shared.timezones import timezone


class TestTimeProvider:
    """Test suite for time provider."""

    def test_get_current_time_without_provider(self):
        """Test that get_current_time returns real time when no provider is set."""
        # Clear any existing provider
        clear_time_provider()

        # Get current time
        before = timezone.utcnow()
        current = get_current_time()
        after = timezone.utcnow()

        # Should be real time (within 1 second tolerance)
        assert before <= current <= after + timedelta(seconds=1)

    def test_get_current_time_with_provider(self):
        """Test that get_current_time returns time from provider function."""
        try:
            # Set a provider function that returns a specific time
            test_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            set_time_provider(lambda: test_time)

            # Should return time from provider
            assert get_current_time() == test_time

        finally:
            clear_time_provider()

    def test_provider_is_called_each_time(self):
        """Test that provider function is called on each get_current_time call."""
        try:
            call_count = [0]
            times = [
                datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2025, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 1, 12, 0, 2, tzinfo=timezone.utc),
            ]

            def provider():
                idx = min(call_count[0], len(times) - 1)
                call_count[0] += 1
                return times[idx]

            set_time_provider(provider)

            # Each call should get the next time
            assert get_current_time() == times[0]
            assert get_current_time() == times[1]
            assert get_current_time() == times[2]
            assert call_count[0] == 3

        finally:
            clear_time_provider()

    def test_clear_time_provider(self):
        """Test that clear resets to real time."""
        try:
            # Set a provider
            test_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            set_time_provider(lambda: test_time)
            assert get_current_time() == test_time

            # Clear it
            clear_time_provider()

            # Should now return real time
            before = timezone.utcnow()
            current = get_current_time()
            after = timezone.utcnow()

            assert before <= current <= after + timedelta(seconds=1)
            assert current != test_time  # Not the provider time

        finally:
            clear_time_provider()

    def test_context_isolation_with_threads(self):
        """Test that time provider is isolated per thread context."""
        import threading

        results = []

        def worker(time_value):
            set_time_provider(lambda tv=time_value: tv)
            # Small delay to ensure overlap
            import time
            time.sleep(0.01)
            results.append(get_current_time())
            clear_time_provider()

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
