# Phase 1 Audit Results

**Date**: 2025-12-20
**Status**: 🔍 **AUDIT COMPLETE**

---

## Executive Summary

Audited 3 files for time usage patterns (`timezone.utcnow()` and `func.now()`):
- `taskinstance.py` - 11 occurrences
- `ti_deps/` (3 files) - 3 occurrences
- `trigger.py` - 4 occurrences

**Total**: 18 occurrences found

**Analysis**:
- ✅ **13 occurrences MUST be changed** (scheduler logic)
- ⚠️ **5 occurrences should NOT be changed** (monitoring/metadata)

---

## Detailed Analysis

### Category Definitions

| Category | Description | Action |
|----------|-------------|--------|
| **Scheduler Logic** | Time comparisons and state changes that affect task execution decisions | ✅ MUST CHANGE |
| **Monitoring** | Heartbeats and liveness checks that need real-time | ⚠️ DO NOT CHANGE |
| **Metadata** | Database record timestamps unrelated to scheduling | ⚠️ DO NOT CHANGE |

---

## File: taskinstance.py (11 occurrences)

### ✅ Changes Required (8 occurrences)

#### 1. Line 279 - DagRun rerun start_date
**Location**: `TaskInstance.rerun()` method
**Code**: `dr.start_date = timezone.utcnow()`
**Context**: Setting DagRun start_date when rerunning finished DagRuns
**Category**: Scheduler Logic
**Rationale**: DagRun state timestamps must use workflow time for deterministic scheduling
**Change**: ✅ **REQUIRED**

```python
# Current:
dr.start_date = timezone.utcnow()

# Should be:
dr.start_date = get_current_time()
```

---

#### 2. Line 303 - DagRun queued_at timestamp
**Location**: `TaskInstance.rerun()` method
**Code**: `dr.queued_at = timezone.utcnow()`
**Context**: Setting DagRun queued_at when clearing tasks
**Category**: Scheduler Logic
**Rationale**: Queue timestamps affect scheduling decisions
**Change**: ✅ **REQUIRED**

```python
# Current:
dr.queued_at = timezone.utcnow()

# Should be:
dr.queued_at = get_current_time()
```

---

#### 3. Line 769 - Task state change timestamp
**Location**: `TaskInstance.set_state()` method
**Code**: `current_time = timezone.utcnow()`
**Context**: Used for setting `start_date` and `end_date` when changing task state
**Category**: Scheduler Logic
**Rationale**: Task state timestamps determine execution flow and retries
**Change**: ✅ **REQUIRED**

```python
# Current:
current_time = timezone.utcnow()

# Should be:
current_time = get_current_time()
```

---

#### 4. Line 993 - Retry readiness check
**Location**: `TaskInstance.ready_for_retry()` method
**Code**: `self.next_retry_datetime() < timezone.utcnow()`
**Context**: Checking if enough time has passed to retry a failed task
**Category**: Scheduler Logic
**Rationale**: Retry timing is core scheduler logic
**Change**: ✅ **REQUIRED**

```python
# Current:
return self.state == TaskInstanceState.UP_FOR_RETRY and self.next_retry_datetime() < timezone.utcnow()

# Should be:
return self.state == TaskInstanceState.UP_FOR_RETRY and self.next_retry_datetime() < get_current_time()
```

---

#### 5. Line 1106 - Task start_date initialization
**Location**: `TaskInstance._execute_task_with_callbacks()` method
**Code**: `ti.start_date = ti.start_date if ti.next_method else timezone.utcnow()`
**Context**: Setting task start time at execution start (unless resuming from deferred state)
**Category**: Scheduler Logic
**Rationale**: Task execution start time affects duration calculations and state
**Change**: ✅ **REQUIRED**

```python
# Current:
ti.start_date = ti.start_date if ti.next_method else timezone.utcnow()

# Should be:
ti.start_date = ti.start_date if ti.next_method else get_current_time()
```

---

#### 6. Line 1135 - Task queued timestamp
**Location**: `TaskInstance._execute_task_with_callbacks()` method
**Code**: `ti.queued_dttm = timezone.utcnow()`
**Context**: Setting queued timestamp when task is about to execute
**Category**: Scheduler Logic
**Rationale**: Queue time affects scheduling metrics and state transitions
**Change**: ✅ **REQUIRED**

```python
# Current:
ti.queued_dttm = timezone.utcnow()

# Should be:
ti.queued_dttm = get_current_time()
```

---

#### 7. Line 1229 - Queue duration metric
**Location**: `TaskInstance._record_task_state_change_metric()` method
**Code**: `timing = timezone.utcnow() - self.queued_dttm`
**Context**: Calculating time spent in queue for metrics
**Category**: Scheduler Logic (Metrics)
**Rationale**: For deterministic testing and consistent metrics in workflows, duration calculations should use workflow time. Otherwise, frozen time would show 0 duration or inconsistent values.
**Change**: ✅ **REQUIRED**

```python
# Current:
timing = timezone.utcnow() - self.queued_dttm

# Should be:
timing = get_current_time() - self.queued_dttm
```

---

#### 8. Line 1239 - Schedule duration metric
**Location**: `TaskInstance._record_task_state_change_metric()` method
**Code**: `timing = timezone.utcnow() - self.scheduled_dttm`
**Context**: Calculating time from scheduled to queued for metrics
**Category**: Scheduler Logic (Metrics)
**Rationale**: Same as line 1229 - consistent time needed for deterministic metrics
**Change**: ✅ **REQUIRED**

```python
# Current:
timing = timezone.utcnow() - self.scheduled_dttm

# Should be:
timing = get_current_time() - self.scheduled_dttm
```

---

#### 9. Line 1537 - Task failure end_date
**Location**: `TaskInstance._handle_failure()` method
**Code**: `ti.end_date = timezone.utcnow()`
**Context**: Setting task end time when handling failures
**Category**: Scheduler Logic
**Rationale**: Task end time affects duration, retries, and state transitions
**Change**: ✅ **REQUIRED**

```python
# Current:
ti.end_date = timezone.utcnow()

# Should be:
ti.end_date = get_current_time()
```

---

### ⚠️ Changes NOT Required (3 occurrences)

#### 10. Line 1462 - Task heartbeat timestamp
**Location**: `TaskInstance.heartbeat()` method
**Code**: `.values(last_heartbeat_at=timezone.utcnow())`
**Context**: Updating heartbeat timestamp for liveness detection
**Category**: Monitoring
**Rationale**: Heartbeats detect if tasks are alive and must use real system time, not workflow time. Otherwise, frozen workflow time would make all tasks appear dead.
**Change**: ⚠️ **DO NOT CHANGE** (keep `timezone.utcnow()`)

---

#### 11. Line 1587 - Record updated_at timestamp
**Location**: `TaskInstance.save_to_db()` method
**Code**: `ti.updated_at = timezone.utcnow()`
**Context**: Setting database record modification timestamp
**Category**: Metadata
**Rationale**: This is purely for tracking when the database record was last modified, not used in scheduling decisions. Real time is appropriate.
**Change**: ⚠️ **DO NOT CHANGE** (keep `timezone.utcnow()`)

---

## File: ti_deps/deps/ (3 files, 3 occurrences)

### ✅ Changes Required (3 occurrences)

#### 12. runnable_exec_date_dep.py:37 - Execution date check
**Location**: `RunnableExecDateDep._get_dep_statuses()` method
**Code**: `cur_date = timezone.utcnow()`
**Context**: Checking if task's logical_date is in the future (task shouldn't run yet)
**Category**: Scheduler Logic
**Rationale**: Dependency checks determine if tasks can execute
**Change**: ✅ **REQUIRED**

```python
# Current:
cur_date = timezone.utcnow()
if logical_date > cur_date:
    yield self._failing_status(...)

# Should be:
from temporal_airflow.time_provider import get_current_time
cur_date = get_current_time()
if logical_date > cur_date:
    yield self._failing_status(...)
```

---

#### 13. not_in_retry_period_dep.py:47 - Retry period check
**Location**: `NotInRetryPeriodDep._get_dep_statuses()` method
**Code**: `cur_date = timezone.utcnow()`
**Context**: Checking if task's retry wait period has elapsed
**Category**: Scheduler Logic
**Rationale**: Determines when tasks can be retried
**Change**: ✅ **REQUIRED**

```python
# Current:
cur_date = timezone.utcnow()
next_task_retry_date = ti.next_retry_datetime()

# Should be:
from temporal_airflow.time_provider import get_current_time
cur_date = get_current_time()
next_task_retry_date = ti.next_retry_datetime()
```

---

#### 14. ready_to_reschedule.py:79 - Reschedule readiness check
**Location**: `ReadyToRescheduleDep._get_dep_statuses()` method
**Code**: `now = timezone.utcnow()`
**Context**: Checking if task's reschedule time has arrived (for rescheduled sensors)
**Category**: Scheduler Logic
**Rationale**: Determines when rescheduled tasks can run again
**Change**: ✅ **REQUIRED**

```python
# Current:
now = timezone.utcnow()
if now >= next_reschedule_date:
    yield self._passing_status(...)

# Should be:
from temporal_airflow.time_provider import get_current_time
now = get_current_time()
if now >= next_reschedule_date:
    yield self._passing_status(...)
```

---

## File: trigger.py (4 occurrences)

### ✅ Changes Required (2 occurrences)

#### 15. Line 310 - Task scheduled after trigger event
**Location**: `Trigger.submit_event()` method
**Code**: `task_instance.scheduled_dttm = timezone.utcnow()`
**Context**: Setting scheduled timestamp when trigger submits an event
**Category**: Scheduler Logic
**Rationale**: Scheduled timestamps affect task execution timing
**Change**: ✅ **REQUIRED**

```python
# Current:
task_instance.scheduled_dttm = timezone.utcnow()

# Should be:
task_instance.scheduled_dttm = get_current_time()
```

---

#### 16. Line 436 - Task scheduled in event handler
**Location**: `handle_event_submit()` function
**Code**: `task_instance.scheduled_dttm = timezone.utcnow()`
**Context**: Setting scheduled timestamp in event submission handler
**Category**: Scheduler Logic
**Rationale**: Same as line 310 - scheduled timestamps affect execution
**Change**: ✅ **REQUIRED**

```python
# Current:
task_instance.scheduled_dttm = timezone.utcnow()

# Should be:
task_instance.scheduled_dttm = get_current_time()
```

---

### ⚠️ Changes NOT Required (2 occurrences)

#### 17. Line 127 - Trigger creation timestamp
**Location**: `Trigger.__init__()` method
**Code**: `self.created_date = created_date or timezone.utcnow()`
**Context**: Setting when the trigger was created in the database
**Category**: Metadata
**Rationale**: This records when the trigger object was created in real time, not used for scheduling decisions. It's metadata for auditing.
**Change**: ⚠️ **DO NOT CHANGE** (keep `timezone.utcnow()`)

---

#### 18. Line 345 - Triggerer job heartbeat check
**Location**: `Trigger.assign_unassigned()` method
**Code**: `Job.latest_heartbeat > timezone.utcnow() - datetime.timedelta(...)`
**Context**: Checking if triggerer job is alive based on heartbeat
**Category**: Monitoring
**Rationale**: Liveness detection must use real time. If we used workflow time, we couldn't detect dead jobs during frozen time periods.
**Change**: ⚠️ **DO NOT CHANGE** (keep `timezone.utcnow()`)

---

## Summary Table

| File | Category | Must Change | Should Not Change | Total |
|------|----------|-------------|-------------------|-------|
| taskinstance.py | Scheduler Logic | 9 | 0 | 9 |
| taskinstance.py | Monitoring | 0 | 1 | 1 |
| taskinstance.py | Metadata | 0 | 1 | 1 |
| ti_deps/ (3 files) | Scheduler Logic | 3 | 0 | 3 |
| trigger.py | Scheduler Logic | 2 | 0 | 2 |
| trigger.py | Monitoring | 0 | 1 | 1 |
| trigger.py | Metadata | 0 | 1 | 1 |
| **TOTALS** | | **13** | **5** | **18** |

---

## Change Specification

### Files to Modify

1. **taskinstance.py** - 9 changes (lines 279, 303, 769, 993, 1106, 1135, 1229, 1239, 1537)
2. **ti_deps/deps/runnable_exec_date_dep.py** - 1 change (line 37)
3. **ti_deps/deps/not_in_retry_period_dep.py** - 1 change (line 47)
4. **ti_deps/deps/ready_to_reschedule.py** - 1 change (line 79)
5. **trigger.py** - 2 changes (lines 310, 436)

**Total changes**: 13

### Import Statements to Add

All modified files need:
```python
from temporal_airflow.time_provider import get_current_time
```

### Pattern

Replace all occurrences:
```python
# Before:
timezone.utcnow()

# After:
get_current_time()
```

---

## Rationale: Why Some Are NOT Changed

### Monitoring (Heartbeats, Liveness)
**Lines**: taskinstance.py:1462, trigger.py:345

Heartbeats and liveness checks MUST use real system time because:
1. They detect if processes are alive/hung
2. Frozen workflow time would make all processes appear dead
3. These are infrastructure monitoring, not business logic
4. No impact on task execution decisions

### Metadata (Record Timestamps)
**Lines**: taskinstance.py:1587, trigger.py:127

Database record timestamps SHOULD use real time because:
1. They track when records were modified in the real world
2. Used for auditing and debugging, not scheduling
3. No impact on task execution or workflow behavior
4. Helpful to know real modification times vs. workflow time

---

## Next Steps

### Implementation Path

1. **Apply changes to all 13 locations**
   - Add imports
   - Replace `timezone.utcnow()` with `get_current_time()`

2. **Run tests**
   - Existing unit tests should pass
   - Phase 1 tests should pass
   - No regressions expected

3. **Integration testing**
   - Test with temporal workflows
   - Verify time injection works correctly
   - Verify heartbeats still work (real time)

4. **Documentation**
   - Update PHASE_1_STATUS.md
   - Create PHASE_1_CHANGES_SPEC.md (if needed)

---

## Risk Assessment

### Low Risk Changes (11 occurrences)
- All scheduler logic changes are straightforward replacements
- Time provider already tested and working
- DagRun changes already validated in Phase 1

### Medium Risk Changes (2 occurrences)
- Lines 1229, 1239 (metrics) - Could affect monitoring dashboards
- Mitigation: Metrics will show workflow time, which is desired for testing

### No Risk - Intentionally Not Changed (5 occurrences)
- Heartbeats and metadata - Correctly using real time

---

## Validation Plan

After applying changes:

1. ✅ Run Phase 1 tests
   ```bash
   pytest temporal_airflow/tests/test_time_provider.py -v
   ```

2. ✅ Run DagRun tests
   ```bash
   pytest airflow-core/tests/models/test_dagrun.py -v
   ```

3. ✅ Run TaskInstance tests
   ```bash
   pytest airflow-core/tests/models/test_taskinstance.py -v
   ```

4. ✅ Run ti_deps tests
   ```bash
   pytest airflow-core/tests/ti_deps/ -v
   ```

5. ✅ Run trigger tests
   ```bash
   pytest airflow-core/tests/models/test_trigger.py -v
   ```

---

## Files Referenced

- `/airflow-core/src/airflow/models/taskinstance.py` (11 occurrences analyzed)
- `/airflow-core/src/airflow/ti_deps/deps/runnable_exec_date_dep.py` (1 occurrence)
- `/airflow-core/src/airflow/ti_deps/deps/not_in_retry_period_dep.py` (1 occurrence)
- `/airflow-core/src/airflow/ti_deps/deps/ready_to_reschedule.py` (1 occurrence)
- `/airflow-core/src/airflow/models/trigger.py` (4 occurrences)

---

## Conclusion

✅ **Audit Complete**: Found 18 occurrences, categorized all
✅ **Analysis Complete**: 13 must change, 5 correctly left as real time
✅ **Ready for Implementation**: Clear specification for all changes

**Phase 1 audit findings are comprehensive and actionable.**
