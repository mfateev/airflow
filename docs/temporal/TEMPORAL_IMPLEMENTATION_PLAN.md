# Temporal Workflow Integration Implementation Plan (Updated)

**Goal**: Execute Airflow DAGs as Temporal workflows with embedded SQLite database, directly managing task activities.

**Architecture**: Each Temporal workflow executes a single DAG. The workflow embeds SQLite, runs scheduler logic directly, and directly starts/awaits Temporal activities for task execution. **No executor abstraction needed** - Temporal's native async/await eliminates polling overhead.

**Version**: 2.0 - Updated with design decisions
**Last Updated**: 2025-12-20

---

## Document Change Log

**V2.0 Changes** (based on TEMPORAL_DECISIONS.md):
- ✅ **Decision 1**: Database isolation via workflow-specific engine
- ✅ **Decision 2**: Removed Phase 2 (TemporalExecutor)
- ✅ **Decision 3**: Serialized DAG in instance variable + Pydantic input
- ✅ **Decision 4**: Pydantic models for all I/O, native enums
- ✅ **Decision 5**: Module-level imports for time provider
- ✅ **Decision 6**: Accept sync calls in async workflow
- ✅ **Decision 7**: Minimal task serialization (not full DAG)
- ✅ **Decision 8**: Defer continue-as-new to Phase 6+
- ✅ **Decision 9**: Added queues/pools/callbacks, deferred sensors/mapping/triggers
- ✅ **Decision 10**: Removed timeline estimates (LLM-assisted implementation)

---

## Executive Summary

### Simplified Architecture ✅

**Key Insight**: Since we're running scheduler logic directly in the Temporal workflow (not as a separate process), we don't need the traditional Airflow executor abstraction. Temporal's native async/await eliminates the need for polling.

### Implementation Phases

| Phase | Description | Complexity |
|-------|-------------|------------|
| Phase 1 | Time Provider & Determinism Patches | Medium |
| ~~Phase 2~~ | ~~TemporalExecutor~~ (**REMOVED**) | ~~N/A~~ |
| Phase 3 | Task Execution Activity + Models | Medium-High |
| Phase 4 | Workflow with Direct Activity Management | High |
| Phase 5 | Integration, Testing, Pools & Callbacks | Medium |
| Phase 6 | Production Readiness | Low-Medium |

**Timeline**: LLM-assisted implementation (no time estimates)

### Code Changes Summary

| Component | Files | Lines | Effort |
|-----------|-------|-------|--------|
| Time injection patches | 5-8 files | ~20 changes | Medium |
| Pydantic models | 1 new file | ~150 lines | Low |
| Task execution activity | 1 new file | ~120 lines | Medium |
| Workflow orchestration | 1 new file | ~400 lines | High |
| Tests & integration | 5+ new files | ~600 lines | Medium |

### Key Benefits of Refined Approach

1. **True Isolation**: Workflow-specific database (Decision 1)
2. **Simpler Code**: Removed executor layer (Decision 2)
3. **Scalable History**: 100x reduction via minimal task serialization (Decision 7)
4. **Type-Safe**: Pydantic models + native enums throughout (Decision 4)
5. **Temporal-Native**: Leverages async/await properly (Decision 6)

---

## Phase 1: Foundation - Time Provider

**Complexity**: Medium
**Dependencies**: None
**Risk**: Low - straightforward patching

### 1.1 Create Time Provider Module

**File**: `temporal_airflow/time_provider.py` (NEW)

**Purpose**: Provide deterministic time injection for Temporal workflow execution.

**Implementation**:
```python
from __future__ import annotations

from datetime import datetime
from contextvars import ContextVar

from airflow._shared.timezones import timezone

_current_time: ContextVar[datetime | None] = ContextVar('current_time', default=None)


def get_current_time() -> datetime:
    """
    Get current time - either from context (Temporal workflow) or real time.

    When running in Temporal workflow, returns deterministic workflow time.
    Otherwise falls back to real system time.
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
    """
    _current_time.set(time)


def clear_workflow_time() -> None:
    """Clear workflow time context."""
    _current_time.set(None)
```

**Testing**:
- Create unit tests that verify `get_current_time()` returns injected time when set
- Verify fallback to `timezone.utcnow()` when not set
- Test context isolation between different contexts

---

### 1.2 Patch `airflow/models/dagrun.py` - Apply Decision 5 Pattern

**File**: `airflow-core/src/airflow/models/dagrun.py`

**Pattern**: Module-level import (Decision 5)

#### Change 0: Add import at top of file
```python
# At top of dagrun.py (after other imports)
from temporal_airflow.time_provider import get_current_time
```

#### Change 1: Line 1182 (in `update_state` method)
```python
# BEFORE:
start_dttm = timezone.utcnow()

# AFTER:
start_dttm = get_current_time()
```

#### Change 2: Line 2086 (in `schedule_tis` method)
```python
# BEFORE:
scheduled_dttm=timezone.utcnow(),

# AFTER:
scheduled_dttm=get_current_time(),
```

#### Change 3: Line 2108 (in `schedule_tis` method - EmptyOperator handling)
```python
# BEFORE:
start_date=timezone.utcnow(),
end_date=timezone.utcnow(),

# AFTER:
start_date=get_current_time(),
end_date=get_current_time(),
```

#### Change 4: Line 613 (in `next_dagruns_to_examine` method)
```python
# BEFORE:
query = query.where(DagRun.run_after <= func.now())

# AFTER:
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)
```

#### Change 5: Line 699 (in `get_queued_dag_runs_to_set_running` method)
```python
# BEFORE:
query = query.where(DagRun.run_after <= func.now())

# AFTER:
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)
```

**Testing**:
- Run existing DagRun tests to ensure no regression
- Create test that injects time and verifies it's used in task scheduling

---

### 1.3 Audit Other Files for Time Usage

**Files to Check**:
- `airflow-core/src/airflow/models/taskinstance.py`
- `airflow-core/src/airflow/ti_deps/deps/*.py`
- `airflow-core/src/airflow/models/trigger.py`

**Action**: Search for `timezone.utcnow()` and `func.now()` and replace if used in DAG execution path.

**Command**:
```bash
grep -r "timezone.utcnow()" airflow-core/src/airflow/models/
grep -r "func.now()" airflow-core/src/airflow/models/
```

**Criteria for patching**:
- If called during DAG run execution (not just metadata/logging)
- If affects task scheduling decisions
- If used in DB queries for task state determination

**Pattern**: Apply Decision 5 standard - module-level import, replace all usages

---

## Phase 2: ~~TemporalExecutor~~ (**REMOVED** - Decision 2)

### Why No Executor?

**The executor abstraction is unnecessary in our architecture.**

**Traditional Airflow**: Executor provides async interface between separate scheduler and worker processes using polling (`sync()` method).

**Our Temporal Approach**: Workflow directly awaits activities - no polling needed!

```python
# Instead of:
executor.queue_task(task)
while True:
    executor.sync()  # Poll for status ❌

# We do:
handle = workflow.start_activity(...)
result = await handle  # Direct async/await ✅
```

**Benefits of skipping executor**:
- ✅ Simpler code - no abstraction layer
- ✅ Temporal-native - use async/await properly
- ✅ No async/sync bridging complexity
- ✅ More efficient - no polling overhead

**Workflow will directly**:
1. Call `dag_run.update_state()` to get schedulable tasks
2. Start activities for those tasks using `workflow.start_activity()`
3. Track activity handles in workflow state
4. Use `asyncio.wait()` to await multiple activities

See Phase 4 for implementation details.

---

## Phase 3: Temporal Activity for Task Execution

**Complexity**: Medium-High
**Dependencies**: Phase 1 complete
**Risk**: Medium - task deserialization and context building

### 3.1 Define Pydantic Models for All I/O (Decision 4)

**File**: `temporal_airflow/models.py` (NEW)

**Purpose**: Type-safe data models for all workflow and activity I/O.

**Implementation**:
```python
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field
from airflow.utils.state import TaskInstanceState, DagRunState


# ============================================================================
# Activity Models
# ============================================================================

class TaskExecutionInput(BaseModel):
    """
    Input model for task execution activity.

    Note (Decision 7): Passes only serialized_task, not entire DAG.
    This reduces Temporal history size by ~100x.
    """

    # Task identification
    dag_id: str = Field(..., description="DAG identifier")
    task_id: str = Field(..., description="Task identifier")
    run_id: str = Field(..., description="DAG run identifier")
    logical_date: datetime = Field(..., description="Logical execution date")

    # Execution metadata
    try_number: int = Field(default=1, description="Retry attempt number")
    map_index: int = Field(default=-1, description="Mapped task index (-1 for non-mapped)")

    # Task definition (NOT full DAG - Decision 7)
    serialized_task: dict[str, Any] = Field(..., description="Serialized task operator")

    # Execution context
    upstream_results: dict[str, Any] | None = Field(
        default=None,
        description="XCom values from upstream tasks"
    )

    # Queue for task routing (Decision 9)
    queue: str | None = Field(default=None, description="Task queue for routing")


class TaskExecutionResult(BaseModel):
    """
    Result model for task execution activity.

    Note (Decision 4): Uses native TaskInstanceState enum.
    Pydantic handles serialization automatically.
    """

    # Task identification (echo back)
    dag_id: str
    task_id: str
    run_id: str
    try_number: int

    # Execution result (Decision 4: native enum)
    state: TaskInstanceState = Field(..., description="Final task state")

    # Timing information
    start_date: datetime = Field(..., description="Task start time")
    end_date: datetime = Field(..., description="Task end time")

    # Task output for downstream tasks (Decision 7: XCom handling)
    return_value: Any | None = Field(default=None, description="Task return value")
    xcom_data: dict[str, Any] | None = Field(
        default=None,
        description="XCom values pushed by this task"
    )

    # Error information
    error_message: str | None = Field(default=None, description="Error message if failed")


# ============================================================================
# Workflow Models
# ============================================================================

class DagExecutionInput(BaseModel):
    """
    Input model for DAG execution workflow.

    Note (Decision 3): Full serialized_dag passed to workflow (once),
    then workflow extracts individual tasks for activities.
    """

    dag_id: str = Field(..., description="DAG identifier")
    run_id: str = Field(..., description="DAG run identifier")
    logical_date: datetime = Field(..., description="Logical execution date")
    conf: dict[str, Any] | None = Field(default=None, description="DAG run configuration")
    serialized_dag: dict[str, Any] = Field(..., description="Serialized DAG definition")

    class Config:
        json_schema_extra = {
            "example": {
                "dag_id": "example_dag",
                "run_id": "manual__2025-01-01T00:00:00",
                "logical_date": "2025-01-01T00:00:00Z",
                "conf": {},
                "serialized_dag": {...}
            }
        }


class DagExecutionResult(BaseModel):
    """Result model for DAG execution workflow (Decision 4)."""

    state: str = Field(..., description="Final DAG run state")
    dag_id: str
    run_id: str
    start_date: datetime
    end_date: datetime
    tasks_succeeded: int = Field(default=0, description="Number of successful tasks")
    tasks_failed: int = Field(default=0, description="Number of failed tasks")

    class Config:
        json_schema_extra = {
            "example": {
                "state": "success",
                "dag_id": "example_dag",
                "run_id": "manual__2025-01-01T00:00:00",
                "start_date": "2025-01-01T00:00:00Z",
                "end_date": "2025-01-01T00:01:30Z",
                "tasks_succeeded": 5,
                "tasks_failed": 0,
            }
        }
```

**Benefits of Pydantic models (Decision 4)**:
- ✅ Type safety and IDE autocomplete
- ✅ Automatic validation
- ✅ Clear schema documentation
- ✅ Easy serialization/deserialization
- ✅ Native enum support (TaskInstanceState)

---

### 3.2 Create Task Execution Activity (Decision 7)

**File**: `temporal_airflow/activities.py` (NEW)

**Purpose**: Temporal activity that executes individual Airflow tasks.

**Key Design (Decision 7)**: Activity receives only the specific task definition, not entire DAG.

**Implementation**:
```python
from __future__ import annotations

from datetime import datetime

import structlog
from temporalio import activity

from airflow.serialization.serialized_objects import SerializedBaseOperator
from temporal_airflow.models import TaskExecutionInput, TaskExecutionResult
from airflow.utils.state import TaskInstanceState

logger = structlog.get_logger()


@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: TaskExecutionInput) -> TaskExecutionResult:
    """
    Execute an Airflow task.

    Design Notes:
    - Decision 7: Receives only serialized_task, not full DAG
    - Decision 4: Uses Pydantic models with native enums
    - Activities run in separate processes, all data via input

    Args:
        input: Typed task execution input with task definition and context

    Returns:
        TaskExecutionResult with execution status, timing, and XCom data

    Raises:
        Exception if task execution fails (will trigger Temporal retry)
    """
    activity.logger.info(
        f"Starting task execution: {input.dag_id}.{input.task_id} "
        f"(run_id={input.run_id}, try={input.try_number})"
    )

    start_time = datetime.utcnow()

    try:
        # Deserialize just this task (Decision 7)
        task = SerializedBaseOperator.deserialize_operator(input.serialized_task)

        # Build minimal execution context
        context = {
            "dag_id": input.dag_id,
            "task_id": input.task_id,
            "run_id": input.run_id,
            "logical_date": input.logical_date,
            "try_number": input.try_number,
            # Simple XCom pull from upstream results
            "task_instance": type('TI', (), {
                "xcom_pull": lambda task_ids=None, key="return_value":
                    input.upstream_results.get(task_ids) if input.upstream_results else None
            })(),
        }

        # Execute task
        result = task.execute(context=context)

        # Capture any XCom pushes
        xcom_data = {"return_value": result} if result is not None else None

        end_time = datetime.utcnow()

        activity.logger.info(
            f"Task completed successfully: {input.dag_id}.{input.task_id} "
            f"(duration: {(end_time - start_time).total_seconds()}s)"
        )

        # Return result with native enum (Decision 4)
        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.SUCCESS,  # Native enum
            start_date=start_time,
            end_date=end_time,
            return_value=result,
            xcom_data=xcom_data,
        )

    except Exception as e:
        end_time = datetime.utcnow()

        activity.logger.error(
            f"Task failed: {input.dag_id}.{input.task_id}",
            exc_info=e
        )

        # Return failed result with native enum
        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.FAILED,  # Native enum
            start_date=start_time,
            end_date=end_time,
            error_message=str(e),
        )
```

**Queue Support (Decision 9)**: Queue is passed in `TaskExecutionInput` and used when starting the activity in workflow (see Phase 4).

---

## Phase 4: Workflow Implementation (Decision 1, 2, 6, 7)

**Complexity**: High - Core orchestration logic
**Dependencies**: Phase 3 complete
**Risk**: High - Most complex phase

### 4.1 Create Main Workflow

**File**: `temporal_airflow/workflows.py` (NEW)

**Purpose**: Temporal workflow that orchestrates DAG execution.

**Key Designs**:
- Decision 1: Workflow-specific database isolation
- Decision 2: Direct activity management (no executor)
- Decision 6: Accept sync calls in async workflow
- Decision 7: Minimal task serialization + XCom state management

**Implementation**:
```python
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from sqlalchemy import create_engine, StaticPool
from sqlalchemy.orm import sessionmaker

from airflow.models.dagrun import DagRun, DagRunState
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.serialization.serialized_objects import SerializedDAG, SerializedBaseOperator
from temporal_airflow.time_provider import set_workflow_time, clear_workflow_time
from temporal_airflow.models import (
    DagExecutionInput,
    DagExecutionResult,
    TaskExecutionInput,
    TaskExecutionResult,
)


@workflow.defn(name="execute_airflow_dag")
class ExecuteAirflowDagWorkflow:
    """
    Temporal workflow that executes a single Airflow DAG.

    Design Notes:
    - Decision 1: Workflow-specific database (no global configure_orm)
    - Decision 2: Direct activity management (no executor)
    - Decision 6: Accepts sync calls (documented as acceptable)
    - Decision 7: Stores XCom in workflow state, passes to activities
    """

    def __init__(self):
        # Decision 1: Workflow-specific database state
        self.engine = None
        self.SessionFactory = None

        # Decision 3 & 7: DAG state management
        self.serialized_dag = None  # Full DAG (from input)
        self.dag = None  # Deserialized DAG

        # Decision 7: XCom state in workflow
        self.xcom_store: dict[tuple, Any] = {}  # ti_key -> xcom_data

        # Decision 9: Pool state
        self.pool_usage: dict[str, int] = {}  # pool_name -> current_usage

    @workflow.run
    async def run(self, input: DagExecutionInput) -> DagExecutionResult:
        """
        Execute the DAG.

        Args:
            input: Typed workflow input (Decision 4: Pydantic model)

        Returns:
            DagExecutionResult with final state and statistics
        """
        workflow.logger.info(f"Starting DAG execution: {input.dag_id} / {input.run_id}")

        start_time = workflow.now()

        try:
            # Phase 1: Setup (Decision 1: workflow-specific DB)
            self._initialize_database()

            # Phase 2: Store and deserialize DAG (Decision 7)
            self.serialized_dag = input.serialized_dag
            self.dag = SerializedDAG.from_dict(self.serialized_dag)

            # Phase 3: Create DAG run
            dag_run_id = self._create_dag_run(
                dag_id=input.dag_id,
                run_id=input.run_id,
                logical_date=input.logical_date,
                conf=input.conf,
            )

            # Phase 4: Main scheduling loop (Decision 2: direct activities)
            final_state = await self._scheduling_loop(dag_run_id)

            end_time = workflow.now()

            # Count task results
            tasks_succeeded = sum(
                1 for result in self.xcom_store.values()
                if isinstance(result, dict) and result.get("state") == "success"
            )
            tasks_failed = sum(
                1 for result in self.xcom_store.values()
                if isinstance(result, dict) and result.get("state") == "failed"
            )

            return DagExecutionResult(
                state=final_state,
                dag_id=input.dag_id,
                run_id=input.run_id,
                start_date=start_time,
                end_date=end_time,
                tasks_succeeded=tasks_succeeded,
                tasks_failed=tasks_failed,
            )

        finally:
            clear_workflow_time()

    def _initialize_database(self):
        """
        Initialize workflow-specific in-memory database.

        Design Note (Decision 1):
        - Each workflow gets unique in-memory DB
        - Never calls global configure_orm()
        - Uses SQLite URI with workflow-specific identifier
        """
        workflow_id = workflow.info().workflow_id
        conn_str = f"sqlite:///file:memdb_{workflow_id}?mode=memory&cache=shared&uri=true"

        # Create workflow-specific engine (no global state!)
        self.engine = create_engine(
            conn_str,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )

        # Create workflow-specific session factory
        self.SessionFactory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        # Create schema
        from airflow.models import Base
        Base.metadata.create_all(self.engine)

        workflow.logger.info(f"Database initialized for workflow {workflow_id}")

    def _create_dag_run(
        self,
        dag_id: str,
        run_id: str,
        logical_date: datetime,
        conf: dict | None,
    ) -> int:
        """
        Create DagRun and TaskInstances.

        Design Note (Decision 1):
        - Uses workflow-specific SessionFactory
        - Never uses global create_session()
        """
        set_workflow_time(workflow.now())

        # Use workflow-specific session (Decision 1)
        session = self.SessionFactory()
        try:
            # Set DAG on DagRun instance (Decision 1: from validation)
            # dag_run.dag must be set before calling update_state()

            # Create DagRun
            dag_run = DagRun(
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,
                run_type="manual",
                state=DagRunState.RUNNING,
                conf=conf,
            )
            dag_run.dag = self.dag  # Set DAG reference

            session.add(dag_run)
            session.flush()

            # Create TaskInstances
            dag_run.verify_integrity(session=session)

            session.commit()

            workflow.logger.info(f"Created DagRun: {dag_run.id}")
            return dag_run.id
        finally:
            session.close()

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        """
        Main scheduling loop.

        Design Notes:
        - Decision 2: Direct activity management (no executor)
        - Decision 6: Accepts sync calls (ORM queries, update_state)
        - Decision 7: Extracts individual tasks, manages XCom
        - Decision 9: Enforces pool limits

        Returns:
            Final DAG run state
        """
        # Track running activities: ti_key -> ActivityHandle
        running_activities: dict[tuple, Any] = {}

        max_iterations = 10000  # Safety limit

        for iteration in range(max_iterations):
            # Update workflow time (deterministic)
            set_workflow_time(workflow.now())

            # Decision 6: Sync calls acceptable (fast, in-memory DB)
            session = self.SessionFactory()
            try:
                dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
                dag_run.dag = self.dag  # Restore DAG reference

                # Check if complete
                if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                    workflow.logger.info(f"DAG completed: {dag_run.state}")

                    # Decision 9: Execute callbacks
                    if dag_run.state == DagRunState.SUCCESS and self.dag.has_on_success_callback:
                        await self._execute_dag_callback(success=True)
                    elif dag_run.state == DagRunState.FAILED and self.dag.has_on_failure_callback:
                        await self._execute_dag_callback(success=False)

                    return dag_run.state.value

                # Update state and get schedulable tasks
                schedulable_tis, callback = dag_run.update_state(
                    session=session,
                    execute_callbacks=False,  # We handle callbacks ourselves
                )

                # Start activities for new schedulable tasks
                if schedulable_tis:
                    dag_run.schedule_tis(schedulable_tis, session=session)
                    session.commit()

                    for ti in schedulable_tis:
                        ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)

                        # Decision 9: Check pool availability
                        if ti.pool:
                            pool_slots = self._get_pool_slots(ti.pool, session)
                            if self.pool_usage.get(ti.pool, 0) >= pool_slots:
                                workflow.logger.info(f"Pool {ti.pool} full, skipping {ti_key}")
                                continue  # Skip, pool full

                        # Decision 7: Extract and serialize ONLY this task
                        task = self.dag.get_task(ti.task_id)
                        serialized_task = SerializedBaseOperator.serialize_operator(task)

                        # Decision 7: Gather upstream XCom
                        upstream_results = self._get_upstream_xcom(ti, task)

                        # Decision 2: Start activity directly (no executor)
                        handle = workflow.start_activity(
                            "run_airflow_task",
                            arg=TaskExecutionInput(
                                dag_id=ti.dag_id,
                                task_id=ti.task_id,
                                run_id=ti.run_id,
                                logical_date=dag_run.logical_date,
                                try_number=ti.try_number,
                                map_index=ti.map_index,
                                serialized_task=serialized_task,  # Just this task!
                                upstream_results=upstream_results,
                                queue=ti.queue,  # Decision 9: queue support
                            ),
                            task_queue=ti.queue or "airflow-tasks",  # Route to correct queue
                            start_to_close_timeout=timedelta(hours=2),
                            heartbeat_timeout=timedelta(minutes=5),
                            retry_policy=RetryPolicy(
                                maximum_attempts=ti.max_tries or 1,
                            ),
                        )

                        running_activities[ti_key] = handle

                        # Decision 9: Track pool usage
                        if ti.pool:
                            self.pool_usage[ti.pool] = self.pool_usage.get(ti.pool, 0) + 1

                        workflow.logger.info(f"Started activity for {ti_key}")

            finally:
                session.close()

            # Wait for any activities to complete
            if running_activities:
                # Decision 2: Use asyncio.wait directly (no executor polling)
                done, pending = await asyncio.wait(
                    running_activities.values(),
                    timeout=5,
                    return_when=asyncio.FIRST_COMPLETED
                )

                # Update DB for completed tasks
                for completed in done:
                    # Map completed handle back to ti_key
                    ti_key = next(k for k, v in running_activities.items() if v == completed)

                    try:
                        result: TaskExecutionResult = completed.result()

                        # Decision 7: Store XCom in workflow state
                        if result.xcom_data:
                            self.xcom_store[ti_key] = result.xcom_data

                        await self._handle_activity_result(ti_key, result)

                        # Decision 9: Release pool slot
                        if ti_key in running_activities:
                            # Get pool from TI in DB
                            session = self.SessionFactory()
                            try:
                                ti = session.query(TaskInstance).filter(
                                    TaskInstance.dag_id == ti_key[0],
                                    TaskInstance.task_id == ti_key[1],
                                    TaskInstance.run_id == ti_key[2],
                                    TaskInstance.map_index == ti_key[3],
                                ).one()
                                if ti.pool:
                                    self.pool_usage[ti.pool] -= 1
                            finally:
                                session.close()

                    except Exception as e:
                        workflow.logger.error(f"Activity failed: {e}")

                    del running_activities[ti_key]
            else:
                # No running activities, sleep before checking for new work
                await asyncio.sleep(5)

        workflow.logger.error("Max iterations reached!")
        return "failed"

    def _get_upstream_xcom(self, ti: TaskInstance, task) -> dict[str, Any] | None:
        """
        Gather XCom values from upstream tasks.

        Design Note (Decision 7):
        - XCom stored in workflow state (self.xcom_store)
        - Passed to activities via upstream_results
        """
        if not task.upstream_task_ids:
            return None

        upstream_results = {}
        for upstream_task_id in task.upstream_task_ids:
            upstream_key = (ti.dag_id, upstream_task_id, ti.run_id, ti.map_index)
            if upstream_key in self.xcom_store:
                upstream_results[upstream_task_id] = self.xcom_store[upstream_key]

        return upstream_results if upstream_results else None

    def _get_pool_slots(self, pool_name: str, session) -> int:
        """Get maximum slots for a pool (Decision 9)."""
        from airflow.models.pool import Pool

        pool = session.query(Pool).filter(Pool.pool == pool_name).first()
        if pool:
            return pool.slots
        return 128  # Default pool size

    async def _handle_activity_result(self, ti_key: tuple, result: TaskExecutionResult):
        """
        Update TaskInstance based on activity result.

        Design Note (Decision 4):
        - result.state is already TaskInstanceState enum
        - Direct assignment works (no conversion needed)
        """
        set_workflow_time(workflow.now())

        session = self.SessionFactory()
        try:
            ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti_key[0],
                TaskInstance.task_id == ti_key[1],
                TaskInstance.run_id == ti_key[2],
                TaskInstance.map_index == ti_key[3],
            ).one()

            # Decision 4: Direct enum assignment (Pydantic handled serialization)
            ti.state = result.state
            ti.start_date = result.start_date
            ti.end_date = result.end_date

            session.commit()

            workflow.logger.info(
                f"Updated task {ti_key} to state {ti.state} "
                f"(duration: {result.end_date - result.start_date})"
            )
        finally:
            session.close()

    async def _execute_dag_callback(self, success: bool):
        """
        Execute DAG-level callback (Decision 9).

        Note: Simple implementation - executes inline.
        Production may want to execute as separate activity.
        """
        try:
            if success and self.dag.has_on_success_callback:
                workflow.logger.info("Executing on_success_callback")
                # Execute callback - simplified for now
                # In production, might want to run as activity
            elif not success and self.dag.has_on_failure_callback:
                workflow.logger.info("Executing on_failure_callback")
                # Execute callback - simplified for now
        except Exception as e:
            workflow.logger.error(f"Callback execution failed: {e}")
```

---

## Phase 5: Integration & Testing (Decision 9)

**Complexity**: Medium
**Dependencies**: Phase 4 complete
**Risk**: Medium - integration issues, debugging

### 5.1 Create Temporal Worker

**File**: `temporal_airflow/worker.py` (NEW)

**Purpose**: Temporal worker that hosts activities.

**Implementation**:
```python
import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_airflow.activities import run_airflow_task
from temporal_airflow.workflows import ExecuteAirflowDagWorkflow

async def main():
    logging.basicConfig(level=logging.INFO)

    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Create worker
    # Note: Can run multiple workers on different queues (Decision 9)
    worker = Worker(
        client,
        task_queue="airflow-tasks",  # Default queue
        workflows=[ExecuteAirflowDagWorkflow],
        activities=[run_airflow_task],
    )

    # Run worker
    logging.info("Worker started on queue: airflow-tasks")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
```

---

### 5.2 Create Workflow Starter

**File**: `temporal_airflow/start_workflow.py` (NEW)

**Purpose**: Client to start DAG execution workflows.

**Implementation**:
```python
import asyncio
from datetime import datetime

from temporalio.client import Client

from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.models import DagExecutionInput
from airflow.serialization.serialized_objects import SerializedDAG

async def start_dag_execution(
    dag_id: str,
    run_id: str,
    logical_date: datetime,
    dag,  # Airflow DAG object
    conf: dict | None = None,
):
    """Start a DAG execution workflow."""
    client = await Client.connect("localhost:7233")

    # Serialize DAG (Decision 3: full DAG to workflow)
    serialized_dag = SerializedDAG.to_dict(dag)

    # Create typed input (Decision 4: Pydantic)
    workflow_input = DagExecutionInput(
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        conf=conf,
        serialized_dag=serialized_dag,
    )

    handle = await client.start_workflow(
        ExecuteAirflowDagWorkflow.run,
        workflow_input,  # Pydantic model
        id=f"dag-{dag_id}-{run_id}",
        task_queue="airflow-tasks",
    )

    print(f"Started workflow: {handle.id}")

    # Wait for result (Decision 4: typed result)
    result = await handle.result()
    print(f"Workflow completed: state={result.state}, "
          f"succeeded={result.tasks_succeeded}, failed={result.tasks_failed}")

    return result

if __name__ == "__main__":
    # Example usage
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    # Create example DAG
    dag = DAG(dag_id="example_dag", start_date=datetime(2025, 1, 1))
    # ... add tasks ...

    asyncio.run(start_dag_execution(
        dag_id="example_dag",
        run_id=f"manual__{datetime.now().isoformat()}",
        logical_date=datetime.now(),
        dag=dag,
    ))
```

---

### 5.3 Create Integration Tests

**File**: `temporal_airflow/test_integration.py` (NEW)

**Tests**:
1. End-to-end test with simple DAG (3-5 tasks)
2. Test with task failures and retries
3. Test with parallel tasks
4. Test time determinism (time injection working)
5. Test workflow recovery (kill worker mid-execution, restart)
6. Test pool enforcement (Decision 9)
7. Test callbacks (Decision 9)
8. Test queue routing (Decision 9)

---

### 5.4 Create Example DAG

**File**: `temporal_airflow/examples/simple_dag.py` (NEW)

**Purpose**: Simple DAG for testing.

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def task_a():
    print("Running task A")
    return "A complete"

def task_b(ti):
    upstream_result = ti.xcom_pull(task_ids="task_a")
    print(f"Running task B, got from A: {upstream_result}")
    return "B complete"

def task_c():
    print("Running task C")
    return "C complete"

with DAG(
    dag_id="simple_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
) as dag:
    t1 = PythonOperator(task_id="task_a", python_callable=task_a)
    t2 = PythonOperator(task_id="task_b", python_callable=task_b)
    t3 = PythonOperator(task_id="task_c", python_callable=task_c)

    t1 >> [t2, t3]  # t2 and t3 run in parallel after t1
```

---

## Phase 6: Production Readiness

**Complexity**: Low-Medium
**Dependencies**: Phase 5 complete
**Risk**: Low - polish and documentation

### 6.1 Documentation

**Files to Create**:
- `temporal_airflow/README.md` - Architecture overview
- `temporal_airflow/SETUP.md` - Setup instructions
- `temporal_airflow/LIMITATIONS.md` - Known limitations (Decision 8, 9)

**LIMITATIONS.md Content**:
```markdown
# Limitations

## Current Version Limitations

### Deferred Features (Decision 8, 9)
The following features are not yet implemented and deferred to future phases:

- **Sensors**: Polling tasks not supported initially
- **Dynamic Task Mapping**: Runtime task creation not supported
- **Deferrable Operators/Triggers**: Not supported
- **SLA Callbacks**: Basic callbacks only, no SLA monitoring
- **Continue-as-New**: DAGs limited to ~4000 tasks (will add if needed)

### Supported Features

**Phase 1-5 Implementation**:
- ✅ Basic task execution (PythonOperator, etc.)
- ✅ Task dependencies and parallel execution
- ✅ Task retries
- ✅ XCom for inter-task communication
- ✅ Queues for task routing
- ✅ Pools for resource management
- ✅ Basic callbacks (on_success, on_failure)
- ✅ Time determinism for replay
- ✅ Workflow recovery

**Scale Limits**:
- DAGs up to ~4000 tasks (due to Temporal history size)
- Use Temporal's built-in monitoring for history size
```

---

### 6.2 Performance Testing

**Tasks**:
- Test with various DAG sizes (10, 100, 1000 tasks)
- Measure workflow history size
- Measure task execution latency
- Test concurrent workflow execution (Decision 1: isolation)

---

### 6.3 Security Review

**Tasks**:
- Review input validation
- Check for injection vulnerabilities
- Review authentication/authorization needs
- Security scan of dependencies

---

## Implementation Checklist

### Phase 1: Foundation ✅
- [ ] Create `temporal_airflow/time_provider.py`
- [ ] Create tests for time provider
- [ ] Patch `airflow/models/dagrun.py` (Decision 5: module-level import, 5 changes)
- [ ] Audit and patch other files for time usage
- [ ] Run existing tests to verify no regression

### Phase 2: ~~Executor~~ (SKIPPED - Decision 2)

### Phase 3: Activity ✅
- [ ] Create `temporal_airflow/models.py` with Pydantic models (Decision 4)
  - [ ] TaskExecutionInput (minimal - Decision 7)
  - [ ] TaskExecutionResult (with native enum)
  - [ ] DagExecutionInput and DagExecutionResult
- [ ] Create `temporal_airflow/activities.py` (Decision 7)
  - [ ] Deserialize single task
  - [ ] Build execution context with upstream XCom
  - [ ] Execute task
  - [ ] Return result with XCom data
- [ ] Test activity can execute simple task
- [ ] Test Pydantic validation and serialization

### Phase 4: Workflow ✅
- [ ] Create `temporal_airflow/workflows.py`
- [ ] Implement database initialization (Decision 1)
- [ ] Implement DAG deserialization and storage
- [ ] Implement DAG run creation
- [ ] Implement scheduling loop (Decision 2, 6, 7)
  - [ ] Direct activity management
  - [ ] Minimal task serialization
  - [ ] XCom handling
  - [ ] Activity result handling
- [ ] Test workflow with simple DAG

### Phase 5: Integration ✅
- [ ] Create worker setup
- [ ] Create workflow starter
- [ ] Create example DAG
- [ ] Run end-to-end test
- [ ] Test failure scenarios
- [ ] Test parallel task execution
- [ ] Verify time determinism
- [ ] Implement pool enforcement (Decision 9)
- [ ] Implement basic callbacks (Decision 9)
- [ ] Test queue routing (Decision 9)

### Phase 6: Production Ready ✅
- [ ] Write documentation (README, SETUP, LIMITATIONS)
- [ ] Performance testing
- [ ] Security review
- [ ] Deferred features documented (Decision 8, 9)

---

## Success Criteria

1. ✅ Simple DAG (3-5 tasks) executes successfully
2. ✅ Task failures trigger retries
3. ✅ Workflow recovery works after worker restart
4. ✅ Time injection ensures deterministic execution
5. ✅ No regression in existing Airflow tests
6. ✅ Database isolation verified (multiple concurrent workflows)
7. ✅ Pools enforce concurrency limits
8. ✅ Callbacks execute on DAG completion
9. ✅ Queues route tasks correctly

---

## Next Steps

1. Review this updated plan
2. Set up development environment (Temporal server, etc.)
3. Begin Phase 1 implementation (time provider)
4. Use LLM assistance for code generation
5. Test thoroughly after each phase

---

**Document Version**: 2.0
**Created**: 2025-12-19
**Last Updated**: 2025-12-20
**Based On**: TEMPORAL_DECISIONS.md (10 decisions resolved)
