# Phase 1: Time Provider & Determinism - Detailed Specification

**Version**: 1.0
**Date**: 2025-12-20
**Status**: Ready for Review
**Complexity**: Medium
**Risk**: Low

---

## Overview

**Goal**: Introduce deterministic time injection for Temporal workflows while maintaining backward compatibility with standard Airflow execution.

**Approach**: Create a time provider module that can inject workflow time when running in Temporal, or fallback to real time for normal Airflow operations.

**Scope**: Minimal changes to Airflow core scheduler code.

---

## Success Criteria

1. ✅ Time provider module created with tests
2. ✅ All identified time usage in `dagrun.py` replaced
3. ✅ No regression in existing Airflow tests
4. ✅ Time injection works in test environment
5. ✅ Backward compatibility maintained (non-Temporal execution unaffected)

---

## Files to Create

### File 1: `temporal_airflow/time_provider.py`

**Purpose**: Central module for deterministic time injection.

**Location**: Create new directory `temporal_airflow/` in repo root

**Full Implementation**:

```python
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
```

**File Structure**:
```
temporal_airflow/
├── __init__.py          (empty)
└── time_provider.py     (above code)
```

---

### File 2: `temporal_airflow/tests/test_time_provider.py`

**Purpose**: Unit tests for time provider

**Full Implementation**:

```python
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
```

---

## Files to Modify

### File 3: `airflow-core/src/airflow/models/dagrun.py`

**Current State**: Uses `timezone.utcnow()` and `func.now()` for time operations

**Target State**: Uses `get_current_time()` from time_provider module

#### Change Inventory

| Change # | Method | Line Range | Type | Priority |
|----------|--------|------------|------|----------|
| 0 | Module imports | ~1-50 | Add import | Required |
| 1 | `update_state` | ~1182 | Replace | Required |
| 2 | `schedule_tis` | ~2086 | Replace | Required |
| 3 | `schedule_tis` | ~2108 | Replace | Required |
| 4 | `next_dagruns_to_examine` | ~613 | Replace | Required |
| 5 | `get_queued_dag_runs_to_set_running` | ~699 | Replace | Required |

**Note**: Line numbers are approximate. Use pattern matching to find exact locations.

---

#### Change 0: Add Module Import

**Location**: Top of file, after existing imports

**Pattern to Find**:
```python
from airflow._shared.timezones import timezone
```

**Change**:
Add after the above line:
```python
from temporal_airflow.time_provider import get_current_time
```

**Complete After State**:
```python
from airflow._shared.timezones import timezone
from temporal_airflow.time_provider import get_current_time
```

**Validation**:
- Import appears in import section (not in methods)
- Import is after `timezone` import for logical grouping
- No duplicate imports

---

#### Change 1: `update_state` Method

**Location**: Inside `DagRun.update_state()` method

**Pattern to Find**:
```python
start_dttm = timezone.utcnow()
```

**Context** (for verification):
```python
def update_state(
    self, session: Session = NEW_SESSION, execute_callbacks: bool = True
) -> tuple[list[TI], DagCallbackRequest | None]:
    """
    Determine the overall state of the DagRun based on the state of its TaskInstances.
    ...
    """
    # ... code ...
    start_dttm = timezone.utcnow()  # ← THIS LINE
    self.last_scheduling_decision = start_dttm
```

**Change**:
```python
# BEFORE:
start_dttm = timezone.utcnow()

# AFTER:
start_dttm = get_current_time()
```

**Validation**:
- Only one occurrence in `update_state` method
- Variable name `start_dttm` unchanged
- Next line still assigns to `self.last_scheduling_decision`

---

#### Change 2: `schedule_tis` Method (First Occurrence)

**Location**: Inside `DagRun.schedule_tis()` method

**Pattern to Find**:
```python
scheduled_dttm=timezone.utcnow(),
```

**Context** (for verification):
```python
def schedule_tis(
    self,
    schedulable_tis: Iterable[TI],
    session: Session = NEW_SESSION,
    max_tis_per_query: int | None = None,
) -> int:
    """
    Set the given task instances in to the scheduled state.
    ...
    """
    # ... code ...
    session.execute(
        update(TI)
        .where(TI.dag_id == dag_id, TI.run_id == run_id, TI.task_id.in_(schedulable_ti_ids))
        .values(
            state=TaskInstanceState.SCHEDULED,
            scheduled_dttm=timezone.utcnow(),  # ← THIS LINE
            ...
        )
    )
```

**Change**:
```python
# BEFORE:
scheduled_dttm=timezone.utcnow(),

# AFTER:
scheduled_dttm=get_current_time(),
```

**Validation**:
- Inside `session.execute()` call
- Part of `.values()` parameters
- Used in UPDATE query for SCHEDULED state

---

#### Change 3: `schedule_tis` Method (Second Occurrence)

**Location**: Inside `DagRun.schedule_tis()` method, EmptyOperator handling

**Pattern to Find**:
```python
start_date=timezone.utcnow(),
end_date=timezone.utcnow(),
```

**Context** (for verification):
```python
# EmptyOperator handling - mark as SUCCESS directly
session.execute(
    update(TI)
    .where(TI.dag_id == dag_id, TI.run_id == run_id, TI.task_id.in_(empty_ti_ids))
    .values(
        state=TaskInstanceState.SUCCESS,
        start_date=timezone.utcnow(),  # ← THIS LINE
        end_date=timezone.utcnow(),    # ← AND THIS LINE
        ...
    )
)
```

**Change**:
```python
# BEFORE:
start_date=timezone.utcnow(),
end_date=timezone.utcnow(),

# AFTER:
start_date=get_current_time(),
end_date=get_current_time(),
```

**Validation**:
- Both lines in same `.values()` call
- Used for EmptyOperator success marking
- Part of UPDATE query for SUCCESS state

---

#### Change 4: `next_dagruns_to_examine` Method

**Location**: Inside class method `DagRun.next_dagruns_to_examine()`

**Pattern to Find**:
```python
query = query.where(DagRun.run_after <= func.now())
```

**Context** (for verification):
```python
@classmethod
@internal_api_call
@provide_session
def next_dagruns_to_examine(
    cls,
    state: DagRunState,
    session: Session = NEW_SESSION,
) -> Query:
    """
    Return the next DagRuns that the scheduler should attempt to schedule.
    ...
    """
    # ... code ...
    query = query.where(DagRun.run_after <= func.now())  # ← THIS LINE
```

**Change**:
```python
# BEFORE:
query = query.where(DagRun.run_after <= func.now())

# AFTER:
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)
```

**Rationale**:
- Cannot use `func.now()` in SQL with injected time
- Must evaluate time in Python and pass as parameter

**Validation**:
- Two lines instead of one (extract time first)
- Uses `current_time` variable (not `start_dttm` or other names)
- Same query chaining pattern

---

#### Change 5: `get_queued_dag_runs_to_set_running` Method

**Location**: Inside class method `DagRun.get_queued_dag_runs_to_set_running()`

**Pattern to Find**:
```python
query = query.where(DagRun.run_after <= func.now())
```

**Context** (for verification):
```python
@classmethod
@internal_api_call
@provide_session
def get_queued_dag_runs_to_set_running(
    cls,
    ...,
    session: Session = NEW_SESSION,
) -> Query:
    """
    Get all dag runs in queued state that are ready to run.
    ...
    """
    # ... code ...
    query = query.where(DagRun.run_after <= func.now())  # ← THIS LINE
```

**Change**:
```python
# BEFORE:
query = query.where(DagRun.run_after <= func.now())

# AFTER:
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)
```

**Validation**:
- Same pattern as Change 4
- Different method (`get_queued_dag_runs_to_set_running` not `next_dagruns_to_examine`)
- Uses `current_time` variable

---

## Additional Files to Audit

### Files to Check

These files may contain additional time usages that affect DAG execution:

1. **`airflow-core/src/airflow/models/taskinstance.py`**
   - Search for: `timezone.utcnow()`, `func.now()`
   - Focus on: Task state transitions, scheduling decisions
   - Expected: May have some usages, evaluate if in execution path

2. **`airflow-core/src/airflow/ti_deps/deps/*.py`**
   - All files in `ti_deps/deps/` directory
   - Search for: `timezone.utcnow()`, `func.now()`
   - Focus on: Dependency checking logic
   - Expected: Likely few or no usages

3. **`airflow-core/src/airflow/models/trigger.py`**
   - Search for: `timezone.utcnow()`, `func.now()`
   - Focus on: Trigger state management
   - Expected: May have usages but triggers deferred to Phase 6

### Audit Process

For each file:

1. **Search**:
   ```bash
   # Search for timezone.utcnow()
   grep -n "timezone.utcnow()" <file_path>

   # Search for func.now()
   grep -n "func.now()" <file_path>
   ```

2. **Evaluate Each Occurrence**:
   - Is it in the DAG execution path? (scheduling, state updates)
   - Is it just for logging/metadata? (can skip)
   - Does it affect task scheduling decisions? (must patch)

3. **Apply Same Pattern**:
   - Add `from temporal_airflow.time_provider import get_current_time` at top
   - Replace `timezone.utcnow()` with `get_current_time()`
   - Replace `func.now()` with `get_current_time()` (extract to variable)

4. **Document**:
   - File: `<file_path>`
   - Line: `<line_number>`
   - Method: `<method_name>`
   - Change: `<before> → <after>`

### Acceptance Criteria for Audit

- [ ] All files checked
- [ ] All occurrences documented
- [ ] Execution path occurrences patched
- [ ] Non-execution path occurrences justified as safe to skip
- [ ] Changes follow same pattern as `dagrun.py`

---

## Testing Requirements

### Unit Tests

**Location**: `temporal_airflow/tests/test_time_provider.py` (created above)

**Coverage Requirements**:
- ✅ Test default behavior (returns real time)
- ✅ Test injection (returns injected time)
- ✅ Test clearing (resets to real time)
- ✅ Test context isolation (thread-safe)

**Run Command**:
```bash
pytest temporal_airflow/tests/test_time_provider.py -v
```

**Expected Result**: All tests pass

---

### Integration Tests

**Test 1: Backward Compatibility**

**Purpose**: Verify existing Airflow tests still pass

**Command**:
```bash
# Run existing DagRun tests
pytest airflow-core/tests/models/test_dagrun.py -v
```

**Expected Result**:
- All existing tests pass
- No new failures introduced
- Test execution time similar to before

**If Failures**:
- Review failure details
- Check if related to time logic
- Verify import doesn't break anything

---

**Test 2: Time Injection Works**

**Purpose**: Verify time can be injected and used

**Location**: Create `temporal_airflow/tests/test_time_injection.py`

**Implementation**:
```python
"""Integration test for time injection in DagRun."""
from __future__ import annotations

from datetime import datetime

import pytest

from airflow._shared.timezones import timezone
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from temporal_airflow.time_provider import set_workflow_time, clear_workflow_time


class TestTimeInjectionIntegration:
    """Test that time injection works with DagRun."""

    def test_dagrun_uses_injected_time(self, session):
        """Test that DagRun.update_state uses injected time."""
        try:
            # Set a specific time
            test_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            set_workflow_time(test_time)

            # Create a simple DagRun (mock setup omitted for brevity)
            # In real test, create actual DAG and DagRun
            dag_run = DagRun(
                dag_id="test_dag",
                run_id="test_run",
                state=DagRunState.RUNNING,
            )
            # Add to session, create TIs, etc.

            # Call update_state
            # This should use get_current_time() internally
            # schedulable_tis, callback = dag_run.update_state(session=session)

            # Verify last_scheduling_decision uses injected time
            # assert dag_run.last_scheduling_decision == test_time

            # (Simplified - actual test would be more complete)
            pass

        finally:
            clear_workflow_time()
```

**Run Command**:
```bash
pytest temporal_airflow/tests/test_time_injection.py -v
```

**Expected Result**: Test passes, demonstrating time injection works

---

## Implementation Checklist

### Pre-Implementation

- [ ] Review this specification
- [ ] Clarify any questions
- [ ] Ensure development environment set up
- [ ] Create feature branch: `git checkout -b feature/phase1-time-provider`

### Implementation Steps

**Step 1: Create Time Provider Module**
- [ ] Create `temporal_airflow/` directory
- [ ] Create `temporal_airflow/__init__.py` (empty)
- [ ] Create `temporal_airflow/time_provider.py` (per spec)
- [ ] Create `temporal_airflow/tests/` directory
- [ ] Create `temporal_airflow/tests/__init__.py` (empty)
- [ ] Create `temporal_airflow/tests/test_time_provider.py` (per spec)
- [ ] Run unit tests: `pytest temporal_airflow/tests/test_time_provider.py -v`
- [ ] Verify all tests pass

**Step 2: Modify dagrun.py**
- [ ] Open `airflow-core/src/airflow/models/dagrun.py`
- [ ] Apply Change 0 (add import)
- [ ] Apply Change 1 (`update_state` method)
- [ ] Apply Change 2 (`schedule_tis` first occurrence)
- [ ] Apply Change 3 (`schedule_tis` second occurrence)
- [ ] Apply Change 4 (`next_dagruns_to_examine` method)
- [ ] Apply Change 5 (`get_queued_dag_runs_to_set_running` method)
- [ ] Verify syntax (no import errors)

**Step 3: Run Existing Tests**
- [ ] Run DagRun tests: `pytest airflow-core/tests/models/test_dagrun.py -v`
- [ ] Verify no new failures
- [ ] If failures, debug and fix

**Step 4: Audit Other Files**
- [ ] Check `taskinstance.py` (document findings)
- [ ] Check `ti_deps/deps/*.py` (document findings)
- [ ] Check `trigger.py` (document findings)
- [ ] Apply patches if needed (follow same pattern)
- [ ] Document all changes

**Step 5: Integration Testing**
- [ ] Create integration test (per spec)
- [ ] Run integration test
- [ ] Verify time injection works

**Step 6: Documentation**
- [ ] Add docstrings if missing
- [ ] Update comments
- [ ] Create `docs/temporal/PHASE_1_RESULTS.md` with findings

**Step 7: Code Review**
- [ ] Self-review all changes
- [ ] Check for typos
- [ ] Verify all imports correct
- [ ] Verify all tests pass
- [ ] Create PR or prepare for review

---

## Risk Assessment

### Low Risk Items ✅

- **Time provider module**: New code, no impact on existing functionality
- **Import addition**: Only adds import, doesn't change logic
- **Unit tests**: New tests, no risk

### Medium Risk Items ⚠️

- **`update_state` changes**: Core scheduler logic
  - **Mitigation**: Existing tests verify behavior unchanged

- **SQL query changes** (Changes 4, 5): Query logic modified
  - **Mitigation**: Logic equivalent (time evaluated in Python vs SQL)
  - **Testing**: Integration tests verify correctness

### Potential Issues & Mitigations

**Issue 1: Import Path**
- **Risk**: `temporal_airflow` might not be in Python path
- **Mitigation**: Ensure proper installation or add to path
- **Test**: Import in Python REPL before running tests

**Issue 2: Timezone Issues**
- **Risk**: Mixing timezone-aware and naive datetimes
- **Mitigation**: Always use timezone-aware datetimes
- **Test**: Unit tests verify timezone handling

**Issue 3: Thread Safety**
- **Risk**: ContextVar might not work as expected
- **Mitigation**: Unit test explicitly tests thread isolation
- **Test**: `test_context_isolation` verifies thread safety

**Issue 4: Performance**
- **Risk**: Function call overhead vs direct `timezone.utcnow()`
- **Mitigation**: Negligible overhead (single function call)
- **Test**: Performance not expected to change measurably

---

## Rollback Plan

If issues arise:

1. **Revert Changes**:
   ```bash
   git checkout main airflow-core/src/airflow/models/dagrun.py
   ```

2. **Remove Time Provider**:
   ```bash
   rm -rf temporal_airflow/
   ```

3. **Verify Tests Pass**:
   ```bash
   pytest airflow-core/tests/models/test_dagrun.py -v
   ```

Clean rollback with no side effects.

---

## Success Metrics

After implementation:

- ✅ All new tests pass (time_provider unit tests)
- ✅ All existing tests pass (no regression)
- ✅ Time injection demonstrated working
- ✅ Code reviewed and approved
- ✅ Documentation complete
- ✅ Ready for Phase 3 (can use time provider)

---

## Next Phase Preparation

**Phase 3 Dependencies**:
- Requires Phase 1 complete (time provider available)
- Will use `set_workflow_time()` in workflow code
- Will use `get_current_time()` pattern for any new time needs

**Readiness Criteria**:
- [ ] Phase 1 merged to main branch
- [ ] Time provider module tested and stable
- [ ] Pattern established for time usage

---

## Questions for Reviewer

1. Are the line numbers accurate enough, or should I provide exact line ranges?
2. Should we add more integration tests?
3. Any concerns about the ContextVar approach for thread safety?
4. Should we add type stubs (`.pyi` files) for better IDE support?
5. Any other files that should be audited beyond the three listed?

---

**Specification Status**: ✅ Ready for Review
**Estimated Implementation Time**: 2-3 hours (with LLM assistance)
**Risk Level**: Low
**Dependencies**: None (can start immediately)
