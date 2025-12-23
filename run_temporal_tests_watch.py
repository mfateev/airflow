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
from collections import deque

# Global flag for timeout
timeout_occurred = False

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
    # Command to run
    cmd = [
        "breeze", "shell", "-c",
        "pytest /files/temporal_airflow/tests/ -v -s"
    ]

    # Buffer to keep last 10 lines
    line_buffer = deque(maxlen=10)

    # Error patterns to watch for (case-insensitive)
    error_patterns = [
        "FAILED",
        "ERROR",
        "[error",  # Matches [error] and [error    ]
        "Error:",
        "Exception:",
        "Traceback",
        "AssertionError",
        "[warning",  # Matches [warning] and [warning  ]
        "[WARN",
        "Warning:",
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
