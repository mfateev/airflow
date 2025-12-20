# Temporal Workflow Integration Implementation Plan

**Goal**: Execute Airflow DAGs as Temporal workflows with embedded SQLite database, directly managing task activities.

**Architecture**: Each Temporal workflow executes a single DAG. The workflow embeds SQLite, runs scheduler logic directly, and directly starts/awaits Temporal activities for task execution. **No executor abstraction needed** - Temporal's native async/await eliminates polling overhead.

---

## Executive Summary

### Simplified Architecture ✅

**Key Insight**: Since we're running scheduler logic directly in the Temporal workflow (not as a separate process), we don't need the traditional Airflow executor abstraction. Temporal's native async/await eliminates the need for polling.

### Implementation Phases

| Phase | Description | Duration | Complexity |
|-------|-------------|----------|------------|
| Phase 1 | Time Provider & Determinism Patches | 2 days | Medium |
| ~~Phase 2~~ | ~~TemporalExecutor~~ (REMOVED) | ~~3 days~~ | ~~N/A~~ |
| Phase 3 | Task Execution Activity | 2 days | Low |
| Phase 4 | Workflow with Direct Activity Management | 4 days | Medium |
| Phase 5 | Integration & Testing | 3 days | Medium |
| Phase 6 | Production Readiness | 2+ days | Low-Medium |
| **Total** | **End-to-end implementation** | **~13 days** | **Reduced by 3 days!** |

### Code Changes Summary

| Component | Files | Lines | Effort |
|-----------|-------|-------|--------|
| Time injection patches | 5-8 files | ~20 changes | Medium |
| ~~TemporalExecutor~~ | ~~1 file~~ | ~~❌ Removed~~ | ~~N/A~~ |
| Pydantic models | 1 new file | ~80 lines | Low |
| Task execution activity | 1 new file | ~100 lines | Low |
| Workflow orchestration | 1 new file | ~300 lines | Medium |
| Tests & integration | 5+ new files | ~500 lines | Medium |

### Key Benefits of Simplified Approach

1. **Faster Implementation**: Saves ~3 days by removing executor layer
2. **Simpler Code**: Direct activity management vs. polling abstraction
3. **More Efficient**: Native async/await instead of periodic polling
4. **Easier to Maintain**: Fewer components, clearer code flow
5. **More Temporal-Native**: Leverages Temporal's strengths properly

---

## Phase 1: Foundation - Time Provider (Week 1, Days 1-2)

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

### 1.2 Patch `airflow/models/dagrun.py` - Python Time Calls

**File**: `airflow-core/src/airflow/models/dagrun.py`

**Changes Required**: 3 locations

#### Change 1: Line 1182 (in `update_state` method)
```python
# BEFORE:
start_dttm = timezone.utcnow()

# AFTER:
from temporal_airflow.time_provider import get_current_time
start_dttm = get_current_time()
```

#### Change 2: Line 2086 (in `schedule_tis` method)
```python
# BEFORE:
scheduled_dttm=timezone.utcnow(),

# AFTER:
from temporal_airflow.time_provider import get_current_time
scheduled_dttm=get_current_time(),
```

#### Change 3: Line 2108 (in `schedule_tis` method - EmptyOperator handling)
```python
# BEFORE:
start_date=timezone.utcnow(),
end_date=timezone.utcnow(),

# AFTER:
from temporal_airflow.time_provider import get_current_time
start_date=get_current_time(),
end_date=get_current_time(),
```

**Testing**:
- Run existing DagRun tests to ensure no regression
- Create test that injects time and verifies it's used in task scheduling

---

### 1.3 Patch `airflow/models/dagrun.py` - SQL Time Queries

**File**: `airflow-core/src/airflow/models/dagrun.py`

**Changes Required**: 2 locations

#### Change 1: Line 613 (in `next_dagruns_to_examine` method)
```python
# BEFORE:
query = query.where(DagRun.run_after <= func.now())

# AFTER:
from temporal_airflow.time_provider import get_current_time
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)
```

#### Change 2: Line 699 (in `get_queued_dag_runs_to_set_running` method)
```python
# BEFORE:
query = query.where(DagRun.run_after <= func.now())

# AFTER:
from temporal_airflow.time_provider import get_current_time
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)
```

**Testing**:
- Test with injected past/future times to verify query filtering works correctly
- Verify queries return expected DAG runs based on injected time

---

### 1.4 Audit Other Files for Time Usage

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

---

## ~~Phase 2: TemporalExecutor~~ (REMOVED - Not Needed!)

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
4. Use `asyncio.gather()` to await multiple activities in parallel

See Phase 3 (Workflow) for implementation details.

---

## Phase 3: Temporal Activity for Task Execution (Week 2, Days 1-2)

### 3.1 Define Pydantic Models for Activity I/O

**File**: `temporal_airflow/models.py` (NEW)

**Purpose**: Type-safe data models for Temporal activity input/output.

**Implementation**:
```python
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TaskExecutionInput(BaseModel):
    """Input model for task execution activity."""

    # Task identification
    dag_id: str = Field(..., description="DAG identifier")
    task_id: str = Field(..., description="Task identifier")
    run_id: str = Field(..., description="DAG run identifier")
    logical_date: datetime = Field(..., description="Logical execution date")

    # Execution metadata
    try_number: int = Field(default=1, description="Retry attempt number")
    map_index: int = Field(default=-1, description="Mapped task index (-1 for non-mapped)")

    # Task configuration (optional, can be added as needed)
    pool: str | None = Field(default=None, description="Pool for task execution")
    queue: str | None = Field(default=None, description="Queue for task execution")

    # Serialized DAG definition (for task loading)
    serialized_dag: dict[str, Any] | None = Field(default=None, description="Serialized DAG definition")

    class Config:
        json_schema_extra = {
            "example": {
                "dag_id": "example_dag",
                "task_id": "task_1",
                "run_id": "manual__2025-01-01T00:00:00",
                "logical_date": "2025-01-01T00:00:00Z",
                "try_number": 1,
                "map_index": -1,
            }
        }


class TaskExecutionResult(BaseModel):
    """Result model for task execution activity."""

    # Task identification (echo back)
    dag_id: str
    task_id: str
    run_id: str
    try_number: int

    # Execution result
    state: str = Field(..., description="Final task state: success, failed, skipped, etc.")

    # Timing information
    start_date: datetime = Field(..., description="Task start time")
    end_date: datetime = Field(..., description="Task end time")

    # Additional metadata
    return_value: Any | None = Field(default=None, description="Task return value if any")
    error_message: str | None = Field(default=None, description="Error message if failed")

    class Config:
        json_schema_extra = {
            "example": {
                "dag_id": "example_dag",
                "task_id": "task_1",
                "run_id": "manual__2025-01-01T00:00:00",
                "try_number": 1,
                "state": "success",
                "start_date": "2025-01-01T00:00:00Z",
                "end_date": "2025-01-01T00:01:30Z",
            }
        }
```

---

### 3.2 Create Task Execution Activity

**File**: `temporal_airflow/activities.py` (NEW)

**Purpose**: Temporal activity that executes Airflow tasks using existing supervisor.

**Implementation**:
```python
from __future__ import annotations

from datetime import datetime

import structlog
from temporalio import activity

from airflow.sdk.execution_time.supervisor import supervise
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TIRunContext
from temporal_airflow.models import TaskExecutionInput, TaskExecutionResult

logger = structlog.get_logger()


@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: TaskExecutionInput) -> TaskExecutionResult:
    """
    Execute an Airflow task.

    Args:
        input: Typed task execution input with all required metadata

    Returns:
        TaskExecutionResult with execution status and timing

    Raises:
        Exception if task execution fails (will trigger Temporal retry)
    """
    activity.logger.info(
        f"Starting task execution: {input.dag_id}.{input.task_id} "
        f"(run_id={input.run_id}, try={input.try_number})"
    )

    start_time = datetime.utcnow()

    try:
        # Send heartbeat to Temporal (indicates activity is alive)
        activity.heartbeat()

        # TODO: Actual task execution integration
        # This needs to:
        # 1. Deserialize DAG from input.serialized_dag
        # 2. Load task definition from DAG
        # 3. Create execution context
        # 4. Call supervisor to run task
        # 5. Capture task output/state

        # Placeholder implementation:
        # In real implementation:
        # dag = deserialize_dag(input.serialized_dag)
        # task = dag.get_task(input.task_id)
        # result = await supervise(ti=ti_context, ...)

        end_time = datetime.utcnow()

        activity.logger.info(f"Task completed successfully: {input.dag_id}.{input.task_id}")

        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state="success",
            start_date=start_time,
            end_date=end_time,
        )

    except Exception as e:
        end_time = datetime.utcnow()

        activity.logger.error(
            f"Task failed: {input.dag_id}.{input.task_id}",
            exc_info=e
        )

        # Return failed result (or re-raise for Temporal retry)
        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state="failed",
            start_date=start_time,
            end_date=end_time,
            error_message=str(e),
        )
```

**Benefits of Pydantic models:**
- ✅ Type safety and IDE autocomplete
- ✅ Automatic validation
- ✅ Clear schema documentation
- ✅ Easy serialization/deserialization
- ✅ JSON schema generation for API docs

**Key Integration Points**:
1. How to load DAG/task definition in activity?
2. How to connect to workflow's SQLite DB? (Answer: Can't directly - need different approach)
3. How does task report state back? (Via Airflow HTTP API or return value?)

**IMPORTANT DESIGN DECISION NEEDED**:

The activity needs access to:
- DAG definition (to know what to execute)
- Task instance state (for retries, etc.)

**Options**:
1. **Pass serialized DAG to activity** - Simple but may be large
2. **Activity connects to external DAG storage** - More complex, external dependency
3. **Workflow passes minimal context, activity loads from registry** - Clean separation

**Recommended**: Option 1 for simplicity initially

---

### 3.2 Handle Task State Updates

**Challenge**: Task state needs to be updated in workflow's SQLite DB.

**Options**:

#### Option A: Activity returns state, workflow updates DB
- Clean separation
- Activity is stateless
- Workflow maintains all state

#### Option B: Activity connects to same SQLite (not possible - in-memory)
- Not feasible with in-memory DB

#### Option C: Pass DB connection string to activity
- Could use file-based SQLite instead of in-memory
- Activity can write directly to DB
- Need to handle concurrent access

**Recommended**: Option A - Activity returns result, workflow updates DB

**Implementation**:
```python
# In workflow after activity completes:
result = await activity_handle
if result["status"] == "success":
    with create_session() as session:
        ti = session.query(TaskInstance).filter(...).one()
        ti.state = TaskInstanceState.SUCCESS
        ti.end_date = get_current_time()
        session.commit()
```

---

## Phase 4: Workflow Implementation (Week 2, Days 3-5)

### 4.1 Create Main Workflow

**File**: `temporal_airflow/workflows.py` (NEW)

**Purpose**: Temporal workflow that orchestrates DAG execution.

**Implementation Structure**:
```python
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from airflow.models.dagrun import DagRun, DagRunState
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.settings import configure_orm
from airflow.utils.session import create_session
from temporal_airflow.time_provider import set_workflow_time, clear_workflow_time
from temporal_airflow.models import TaskExecutionInput, TaskExecutionResult


@workflow.defn(name="execute_airflow_dag")
class ExecuteAirflowDagWorkflow:
    """
    Temporal workflow that executes a single Airflow DAG.
    """

    @workflow.run
    async def run(self, input: dict[str, Any]) -> dict:
        """
        Execute the DAG.

        Input:
        {
            "dag_id": "my_dag",
            "run_id": "manual__2025-01-01T00:00:00",
            "logical_date": "2025-01-01T00:00:00Z",
            "conf": {},  # Optional DAG run config
            "serialized_dag": {...}  # Serialized DAG definition
        }

        Returns:
        {
            "state": "success" | "failed",
            "start_date": "...",
            "end_date": "...",
        }
        """
        dag_id = input["dag_id"]
        run_id = input["run_id"]
        logical_date = datetime.fromisoformat(input["logical_date"])

        workflow.logger.info(f"Starting DAG execution: {dag_id} / {run_id}")

        try:
            # Phase 1: Setup
            self._initialize_database()
            executor = TemporalExecutor()
            executor.start()

            # Phase 2: Create DAG run
            dag_run_id = self._create_dag_run(
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,
                conf=input.get("conf"),
                serialized_dag=input["serialized_dag"],
            )

            # Phase 3: Main scheduling loop
            final_state = await self._scheduling_loop(executor, dag_run_id)

            # Phase 4: Cleanup
            executor.end()

            return {
                "state": final_state,
                "dag_id": dag_id,
                "run_id": run_id,
            }

        finally:
            clear_workflow_time()

    def _initialize_database(self):
        """Initialize in-memory SQLite database."""
        import os
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:///:memory:"

        configure_orm()

        # Create schema
        from airflow.models import Base
        from airflow.settings import engine
        Base.metadata.create_all(engine)

        workflow.logger.info("Database initialized")

    def _create_dag_run(
        self,
        dag_id: str,
        run_id: str,
        logical_date: datetime,
        conf: dict | None,
        serialized_dag: dict,
    ) -> int:
        """Create DagRun and TaskInstances."""
        # Set workflow time
        set_workflow_time(workflow.now())

        with create_session() as session:
            # TODO: Deserialize and store DAG
            # For now, assume DAG is loaded/available

            # Create DagRun
            dag_run = DagRun(
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,
                run_type="manual",
                state=DagRunState.RUNNING,
                conf=conf,
            )
            session.add(dag_run)
            session.flush()

            # Create TaskInstances
            # dag_run.verify_integrity(session=session, dag_version_id=...)
            # TODO: Implement DAG loading and TI creation

            session.commit()

            workflow.logger.info(f"Created DagRun: {dag_run.id}")
            return dag_run.id

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        """
        Main scheduling loop.

        Returns final DAG run state.
        """
        # Track running activities: task_instance_key -> activity_handle
        running_activities: dict[tuple, Any] = {}

        max_iterations = 1000  # Safety limit
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            # Update workflow time (deterministic)
            set_workflow_time(workflow.now())

            with create_session() as session:
                dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()

                # Check if complete
                if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                    workflow.logger.info(f"DAG completed: {dag_run.state}")
                    return dag_run.state.value

                # Update state and get schedulable tasks
                schedulable_tis, callback = dag_run.update_state(
                    session=session,
                    execute_callbacks=False,
                )

                # Start activities for new schedulable tasks
                if schedulable_tis:
                    dag_run.schedule_tis(schedulable_tis, session=session)
                    session.commit()

                    for ti in schedulable_tis:
                        ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)

                        # Create typed input for activity
                        task_input = TaskExecutionInput(
                            dag_id=ti.dag_id,
                            task_id=ti.task_id,
                            run_id=ti.run_id,
                            logical_date=dag_run.logical_date,
                            try_number=ti.try_number,
                            map_index=ti.map_index,
                            pool=ti.pool,
                            queue=ti.queue,
                            serialized_dag=serialized_dag,  # Pass from workflow state
                        )

                        # Start activity with typed input
                        handle = workflow.start_activity(
                            "run_airflow_task",
                            arg=task_input,  # Single Pydantic model argument
                            task_queue="airflow-tasks",
                            start_to_close_timeout=timedelta(hours=2),
                            heartbeat_timeout=timedelta(minutes=5),
                            retry_policy=RetryPolicy(
                                maximum_attempts=ti.max_tries or 1,
                            ),
                        )

                        running_activities[ti_key] = handle
                        workflow.logger.info(f"Started activity for task {ti_key}")

            # Wait for any activities to complete
            if running_activities:
                # Convert dict to list for asyncio.wait
                pending_tasks = [
                    asyncio.create_task(handle)
                    for handle in running_activities.values()
                ]

                # Wait for at least one to complete (or timeout)
                done, pending = await asyncio.wait(
                    pending_tasks,
                    timeout=5,  # Check every 5 seconds
                    return_when=asyncio.FIRST_COMPLETED
                )

                # Update DB for completed tasks
                for task in done:
                    try:
                        result = await task
                        ti_key = self._find_ti_key_for_task(running_activities, task)

                        if ti_key:
                            await self._handle_activity_result(ti_key, result)
                            del running_activities[ti_key]

                    except Exception as e:
                        workflow.logger.error(f"Activity failed: {e}")
                        # Task will be marked failed via result handling
            else:
                # No running activities, sleep before checking for new work
                await asyncio.sleep(5)

        workflow.logger.error("Max iterations reached!")
        return "failed"

    def _find_ti_key_for_task(self, activities: dict, task) -> tuple | None:
        """Find task instance key for completed asyncio task."""
        # This is a helper to map asyncio task back to ti_key
        # Implementation depends on how we structure the handle tracking
        # For now, simplified version
        for ti_key, handle in activities.items():
            # TODO: Proper handle -> ti_key mapping
            return ti_key
        return None
```

**Key Components**:
1. Database initialization
2. DAG run creation
3. Scheduling loop with **direct activity management**
4. Async/await for activity completion
5. State management

**Benefits of this approach**:
- ✅ No executor polling overhead
- ✅ Native Temporal async/await
- ✅ Efficient - waits for completions, doesn't busy-loop
- ✅ Simpler code flow

---

### 4.2 Handle DAG Serialization/Deserialization

**Challenge**: Workflow needs access to DAG definition to create TaskInstances.

**Options**:
1. Pass serialized DAG as workflow input
2. Load from SerializedDagModel (requires DB setup)
3. Fetch from external service

**Recommended**: Pass as workflow input initially

**Implementation**:
```python
# Before starting workflow:
from airflow.serialization.serialized_objects import SerializedDAG

dag = ...  # Load DAG
serialized_dag = SerializedDAG.to_dict(dag)

# Start workflow
await client.execute_workflow(
    ExecuteAirflowDagWorkflow.run,
    args=[{
        "dag_id": dag.dag_id,
        "serialized_dag": serialized_dag,
        ...
    }],
    ...
)
```

---

### 4.3 Implement Activity Result Handling

**File**: `temporal_airflow/workflows.py` (addition to workflow)

**Purpose**: Update TaskInstance state when activity completes.

**Implementation**:
```python
async def _handle_activity_result(self, ti_key: tuple, result: TaskExecutionResult):
    """
    Update TaskInstance based on activity result.

    Args:
        ti_key: Tuple of (dag_id, task_id, run_id, map_index)
        result: Typed activity result with execution status
    """
    set_workflow_time(workflow.now())

    with create_session() as session:
        ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == ti_key[0],
            TaskInstance.task_id == ti_key[1],
            TaskInstance.run_id == ti_key[2],
            TaskInstance.map_index == ti_key[3],
        ).one()

        # Update state from typed result
        ti.state = TaskInstanceState(result.state)  # "success" -> TaskInstanceState.SUCCESS
        ti.start_date = result.start_date
        ti.end_date = result.end_date

        # Store return value if present
        if result.return_value is not None:
            ti.xcom_push(key="return_value", value=result.return_value)

        session.commit()

        workflow.logger.info(
            f"Updated task {ti_key} to state {result.state} "
            f"(duration: {result.end_date - result.start_date})"
        )
```

---

## Phase 5: Integration & Testing (Week 3)

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
    worker = Worker(
        client,
        task_queue="airflow-tasks",
        workflows=[ExecuteAirflowDagWorkflow],
        activities=[run_airflow_task],
    )

    # Run worker
    logging.info("Worker started")
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

async def start_dag_execution(
    dag_id: str,
    run_id: str,
    logical_date: datetime,
    serialized_dag: dict,
):
    """Start a DAG execution workflow."""
    client = await Client.connect("localhost:7233")

    handle = await client.start_workflow(
        ExecuteAirflowDagWorkflow.run,
        args=[{
            "dag_id": dag_id,
            "run_id": run_id,
            "logical_date": logical_date.isoformat(),
            "serialized_dag": serialized_dag,
        }],
        id=f"dag-{dag_id}-{run_id}",
        task_queue="airflow-tasks",
    )

    print(f"Started workflow: {handle.id}")

    # Wait for result
    result = await handle.result()
    print(f"Workflow completed: {result}")

    return result

if __name__ == "__main__":
    # Example usage
    asyncio.run(start_dag_execution(
        dag_id="example_dag",
        run_id=f"manual__{datetime.now().isoformat()}",
        logical_date=datetime.now(),
        serialized_dag={},  # TODO: Pass real serialized DAG
    ))
```

---

### 5.3 Create Integration Tests

**File**: `temporal_airflow/test_integration.py` (NEW)

**Tests**:
1. End-to-end test with simple DAG (2-3 tasks)
2. Test with task failures and retries
3. Test with parallel tasks
4. Test time injection (tasks scheduled at specific times)
5. Test workflow recovery (kill worker mid-execution, restart)

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

def task_b():
    print("Running task B")
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

## Phase 6: Refinements & Production Readiness (Week 4)

### 6.1 Handle Edge Cases

**File**: Various

**Tasks**:
1. Implement proper task timeout handling
2. Add support for task retries (respect max_tries)
3. Handle task dependencies (trigger rules)
4. Implement task pools and queues
5. Add support for mapped tasks (dynamic task mapping)

---

### 6.2 Optimize Database Performance

**Considerations**:
1. In-memory SQLite performance for large DAGs
2. Index optimization
3. Query optimization
4. Consider file-based SQLite for persistence across failures

---

### 6.3 Add Observability

**File**: `temporal_airflow/observability.py` (NEW)

**Features**:
1. Metrics export (task duration, success/failure rates)
2. Structured logging
3. OpenTelemetry integration
4. Temporal metrics integration

---

### 6.4 Documentation

**Files**:
- `temporal_airflow/README.md` - Architecture overview
- `temporal_airflow/SETUP.md` - Setup instructions
- `temporal_airflow/LIMITATIONS.md` - Known limitations
- `temporal_airflow/MIGRATION.md` - Migration guide from standard Airflow

---

## Implementation Checklist

### Phase 1: Foundation (Days 1-2)
- [ ] Create `temporal_airflow/time_provider.py`
- [ ] Create tests for time provider
- [ ] Patch `airflow/models/dagrun.py` (5 changes)
- [ ] Audit and patch other files for time usage
- [ ] Run existing tests to verify no regression

### ~~Phase 2: Executor~~ (SKIPPED - Not needed!)

### Phase 3: Activity (Days 3-4)
- [ ] Create `temporal_airflow/models.py` with Pydantic models
- [ ] Define `TaskExecutionInput` model
- [ ] Define `TaskExecutionResult` model
- [ ] Create `temporal_airflow/activities.py`
- [ ] Implement `run_airflow_task` activity with typed I/O
- [ ] Decide on task state update mechanism
- [ ] Test activity can execute simple task
- [ ] Test Pydantic validation and serialization
- [ ] Implement activity heartbeating

### Phase 4: Workflow (Days 5-8)
- [ ] Create `temporal_airflow/workflows.py`
- [ ] Implement database initialization
- [ ] Implement DAG run creation
- [ ] Implement scheduling loop with **direct activity management**
- [ ] Handle activity results with async/await
- [ ] Implement activity tracking and completion handling
- [ ] Test workflow with simple DAG

### Phase 5: Integration (Days 9-11)
- [ ] Create worker setup
- [ ] Create workflow starter
- [ ] Create example DAG
- [ ] Run end-to-end test
- [ ] Test failure scenarios
- [ ] Test parallel task execution
- [ ] Verify time determinism

### Phase 6: Production Ready (Days 12-13+)
- [ ] Handle edge cases
- [ ] Optimize performance
- [ ] Add observability
- [ ] Write documentation
- [ ] Performance testing
- [ ] Security review

---

## Key Design Decisions to Finalize

1. ~~**Async/Sync Bridge**~~: ✅ Resolved - Use native async/await, no executor needed
2. **Task State Updates**: Activity returns state vs. direct DB writes?
3. **DAG Loading**: Pass serialized DAG vs. load from external source?
4. **SQLite Storage**: In-memory vs. file-based for durability?
5. **Activity Return Values**: What does activity return to workflow?
6. **Activity Tracking**: Best way to map completed asyncio tasks back to TI keys?

---

## Dependencies

**Python Packages**:
```
temporalio>=1.0.0
sqlalchemy>=2.0.0
apache-airflow>=2.10.0
```

**External Services**:
- Temporal Server (localhost:7233 for development)

---

## Risk Areas

1. **Workflow History Size**: Long-running DAGs may exceed history limits
   - Mitigation: Use continue-as-new for large DAGs

2. **Time Injection Coverage**: Missing time injection points cause non-determinism
   - Mitigation: Comprehensive audit and testing

3. **SQLite Limitations**: In-memory DB lost on workflow failure
   - Mitigation: Use file-based SQLite, or accept re-execution on failure

4. **Activity Handle Tracking**: Mapping completed activities back to task instances
   - Mitigation: Careful design of handle→TI_key mapping structure

---

## Success Criteria

1. ✅ Simple DAG (3-5 tasks) executes successfully
2. ✅ Task failures trigger retries
3. ✅ Workflow recovery works after worker restart
4. ✅ Time injection ensures deterministic execution
5. ✅ No regression in existing Airflow tests
6. ✅ End-to-end execution time < 2x standard Airflow

---

## Next Steps

1. Review this plan with team
2. Finalize design decisions (see section above)
3. Set up development environment (Temporal server, etc.)
4. Begin Phase 1 implementation
5. Schedule daily standups to track progress

---

**Document Version**: 1.0
**Created**: 2025-12-19
**Last Updated**: 2025-12-19
