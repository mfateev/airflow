#!/usr/bin/env python3
"""
Watch temporal tests and stop at first warning/error.
Shows only the error line and 10 lines before it.
Timeout after 1 minute.
"""

import subprocess
import sys
import signal
import os
import threading
import atexit
from collections import deque

# Global flag for timeout
timeout_occurred = False

def cleanup_breeze():
    """Stop breeze containers on exit."""
    try:
        # Get list of running breeze containers
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=breeze", "-q"],
            capture_output=True,
            text=True,
            timeout=5
        )
        container_ids = result.stdout.strip().split('\n')
        container_ids = [cid for cid in container_ids if cid]  # Remove empty strings

        # Stop containers if any are running
        if container_ids:
            subprocess.run(
                ["docker", "stop"] + container_ids,
                capture_output=True,
                timeout=30
            )
    except Exception:
        # Silently ignore cleanup errors
        pass

# Register cleanup handler
atexit.register(cleanup_breeze)

def timeout_handler(process, line_buffer):
    """Handle timeout by killing the process and showing last lines."""
    global timeout_occurred
    timeout_occurred = True
    print("\n" + "=" * 80, flush=True)
    print("TIMEOUT: Test execution exceeded 60 seconds", flush=True)
    print("=" * 80, flush=True)
    print("Last 10 lines of output:", flush=True)
    print("=" * 80, flush=True)
    for line in line_buffer:
        print(line, end='', flush=True)
    print("=" * 80, flush=True)

    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()

def run_tests_with_watch():
    # Clean up any existing breeze containers first
    print("Cleaning up existing breeze containers...", flush=True)
    cleanup_breeze()

    # Command to run
    cmd = [
        "breeze", "shell", "--answer", "n", "-c",
        "pytest /opt/airflow/providers/temporal_airflow/tests/ -v -s"
    ]

    # Buffer to keep last 10 lines
    line_buffer = deque(maxlen=10)

    # Error patterns to watch for (case-insensitive - all lowercase)
    error_patterns = [
        "failed",
        "error",
        "[error",  # Matches [error], [ERROR], [error    ], etc.
        "error:",
        "exception:",
        "traceback",
        "assertionerror",
        "[warning",  # Matches [warning], [WARNING], [warning  ], etc.
        "[warn",
        "warning:",
    ]

    # Start the process with unbuffered output
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # Line buffered
        universal_newlines=True,
        env=env,
        preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_IGN)
    )

    # Set up timeout timer (60 seconds)
    timer = threading.Timer(60.0, timeout_handler, args=(process, line_buffer))
    timer.daemon = True
    timer.start()

    line_count = 0
    try:
        for line in iter(process.stdout.readline, ''):
            if not line:
                break

            # Check if timeout occurred
            if timeout_occurred:
                return 1

            line_count += 1

            # Check if this line contains an error pattern (case-insensitive)
            found_error = False
            matched_pattern = None
            line_lower = line.lower()

            # Skip lines that are pytest test results (contain "PASSED", "SKIPPED", etc.)
            if " passed" in line_lower or " skipped" in line_lower or "::test_" in line_lower:
                line_buffer.append(line)
                continue

            # Skip Temporal SDK timeout warnings (OK to ignore)
            # But DO NOT ignore "invalid history builder state" - that's critical!
            if "temporalio_sdk_core" in line_lower:
                if "task not found when completing" in line_lower:
                    line_buffer.append(line)
                    continue
                # Also skip timeout during worker shutdown (not a test failure)
                if "timeout expired" in line_lower and "beginning worker shutdown" in "".join(line_buffer).lower():
                    line_buffer.append(line)
                    continue

            for pattern in error_patterns:
                if pattern.lower() in line_lower:
                    found_error = True
                    matched_pattern = pattern
                    break

            if found_error:
                # Cancel the timer
                timer.cancel()

                # Print the buffered lines (context before error)
                print("=" * 80, flush=True)
                print(f"ERROR/WARNING DETECTED (pattern: '{matched_pattern}')", flush=True)
                print("Context (last 10 lines):", flush=True)
                print("=" * 80, flush=True)
                for buffered_line in line_buffer:
                    print(buffered_line, end='', flush=True)

                # Print the error line itself
                print("\n" + "=" * 80, flush=True)
                print("ERROR/WARNING LINE:", flush=True)
                print("=" * 80, flush=True)
                print(line, end='', flush=True)
                print("=" * 80, flush=True)
                print(f"\nStopped after reading {line_count} lines", flush=True)

                # Kill the process
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

                return 1

            # Add line to buffer
            line_buffer.append(line)

        # Cancel the timer if we finished normally
        timer.cancel()

        # Process completed
        return_code = process.wait()

        if timeout_occurred:
            return 1

        print(f"\nProcess completed. Read {line_count} lines total.", flush=True)

        if return_code == 0:
            print("=" * 80, flush=True)
            print("All tests passed! No errors or warnings detected.", flush=True)
            print("=" * 80, flush=True)
        else:
            print(f"Process exited with code {return_code}", flush=True)

        return return_code

    except KeyboardInterrupt:
        timer.cancel()
        print("\n\nInterrupted by user", flush=True)
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        return 130
    except Exception as e:
        timer.cancel()
        print(f"Exception occurred: {e}", flush=True)
        process.terminate()
        return 1

if __name__ == "__main__":
    sys.exit(run_tests_with_watch())
