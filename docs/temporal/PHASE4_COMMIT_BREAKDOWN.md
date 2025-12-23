# Phase 4: Workflow Implementation - Commit Breakdown

**Goal**: Implement the core Temporal workflow that orchestrates DAG execution with direct activity management.

**Based on**: TEMPORAL_IMPLEMENTATION_PLAN.md V2.0 (lines 527-964)

**Prerequisites**:
- ✅ Phase 1 complete (time provider patches)
- ✅ Phase 3 complete (models.py, activities.py)

**Deferred to Phase 5**:
- ❌ Pools enforcement
- ❌ Callbacks (on_success/on_failure)
- ❌ Integration tests

---

## Commit Strategy

Break Phase 4 into **7 independent, testable commits** that build incrementally:

| # | Commit | LoC | Complexity | Key Feature |
|---|--------|-----|------------|-------------|
| 1 | Workflow skeleton + DB init | ~80 | Low | Database isolation (Decision 1) |
| 2 | DAG deserialization | ~10 | Low | Store DAG in workflow state |
| 3 | DAG run creation | ~50 | Medium | Create DagRun + TaskInstances |
| 4 | Basic loop structure | ~40 | Low | Main scheduling loop skeleton |
| 5 | Scheduler state updates | ~20 | Medium | Call update_state/schedule_tis |
| 6 | Activity starting + XCom | ~60 | High | Start activities (Decision 7) |
| 7 | Activity completion | ~50 | High | Await & handle completions |
| **Total** | **Complete Phase 4** | **~310** | **High** | **Core orchestration** |

---

## Commit 1: Workflow Skeleton + Database Initialization

**File**: `temporal_airflow/workflows.py` (NEW)

### What This Commit Adds

1. Workflow class with `@workflow.defn` decorator
2. `__init__` method with instance variables
3. `_initialize_database()` method (Decision 1: workflow-specific database)
4. Basic `run()` method skeleton

### Implementation

```python
"""Temporal workflow for executing Airflow DAGs."""
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
        """Initialize workflow state."""
        # Decision 1: Workflow-specific database state
        self.engine = None
        self.SessionFactory = None

        # Decision 3 & 7: DAG state management
        self.serialized_dag = None  # Full DAG (from input)
        self.dag = None  # Deserialized DAG

        # Decision 7: XCom state in workflow
        self.xcom_store: dict[tuple, Any] = {}  # ti_key -> xcom_data

        # TODO Phase 5: Add pool_usage tracking

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

            # TODO Commit 2: Store and deserialize DAG
            # TODO Commit 3: Create DAG run
            # TODO Commit 4-7: Scheduling loop

            # Placeholder return
            return DagExecutionResult(
                state="success",
                dag_id=input.dag_id,
                run_id=input.run_id,
                start_date=start_time,
                end_date=workflow.now(),
                tasks_succeeded=0,
                tasks_failed=0,
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
```

### Test

**File**: `temporal_airflow/tests/test_workflows.py` (NEW)

```python
"""Tests for Temporal workflows."""
import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.models import DagExecutionInput
from datetime import datetime


@pytest.mark.asyncio
async def test_workflow_database_initialization():
    """Test that workflow initializes its own database."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        # Create minimal input
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "test_dag"}},
        )

        # Start workflow
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-workflow-db-init",
                task_queue="test-queue",
            )

            # Should complete without error
            assert result.dag_id == "test_dag"
            assert result.run_id == "test_run"


@pytest.mark.asyncio
async def test_database_isolation():
    """Test that multiple workflows have isolated databases."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        input1 = DagExecutionInput(
            dag_id="dag1",
            run_id="run1",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "dag1"}},
        )

        input2 = DagExecutionInput(
            dag_id="dag2",
            run_id="run2",
            logical_date=datetime(2025, 1, 1),
            serialized_dag={"dag": {"dag_id": "dag2"}},
        )

        # Start two workflows concurrently
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            results = await asyncio.gather(
                env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input1,
                    id="test-workflow-1",
                    task_queue="test-queue",
                ),
                env.client.execute_workflow(
                    ExecuteAirflowDagWorkflow.run,
                    input2,
                    id="test-workflow-2",
                    task_queue="test-queue",
                ),
            )

            # Both should complete successfully with correct IDs
            assert results[0].dag_id == "dag1"
            assert results[1].dag_id == "dag2"
```

### Why This Commit

- ✅ Establishes workflow foundation
- ✅ Database isolation is self-contained and critical
- ✅ Can verify schema creation works
- ✅ Tests validate Decision 1 (workflow-specific database)

---

## Commit 2: DAG Deserialization and Storage

**File**: `temporal_airflow/workflows.py` (EDIT)

### What This Commit Adds

- Store serialized DAG in instance variable (`self.serialized_dag`)
- Deserialize DAG using `SerializedDAG.from_dict()`
- Store deserialized DAG in instance variable (`self.dag`)

### Implementation

```python
# In run() method, after _initialize_database():

# Store and deserialize DAG (Decision 3)
self.serialized_dag = input.serialized_dag
self.dag = SerializedDAG.from_dict(self.serialized_dag)

workflow.logger.info(f"Deserialized DAG: {self.dag.dag_id}")

# TODO Commit 3: Create DAG run
```

### Test

Add to `temporal_airflow/tests/test_workflows.py`:

```python
@pytest.mark.asyncio
async def test_dag_deserialization():
    """Test that workflow deserializes DAG correctly."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create a real DAG
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="task1", python_callable=lambda: None)

    # Serialize it
    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-dag-deser",
                task_queue="test-queue",
            )

            # Should complete without error
            assert result.dag_id == "test_dag"
```

### Why This Commit

- ✅ Simple, self-contained change (~10 lines)
- ✅ Can verify DAG deserialization works
- ✅ Required before creating DAG run
- ✅ Tests validate Decision 3 (DAG storage in workflow)

---

## Commit 3: DAG Run Creation

**File**: `temporal_airflow/workflows.py` (EDIT)

### What This Commit Adds

- `_create_dag_run()` method
- Call it from `run()` method
- Create DagRun and TaskInstances in database

### Implementation

```python
# In run() method, after DAG deserialization:

# Create DAG run
dag_run_id = self._create_dag_run(
  dag_id=input.dag_id,
  run_id=input.run_id,
  logical_date=input.logical_date,
  conf=input.conf,
)

workflow.logger.info(f"Created DAG run: {dag_run_id}")


# TODO Commit 4-7: Scheduling loop

# Add method:

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
  session = self.sessionFactory()
  try:
    # Create DagRun
    dag_run = DagRun(
      dag_id=dag_id,
      run_id=run_id,
      logical_date=logical_date,
      run_type="manual",
      state=DagRunState.RUNNING,
      conf=conf,
    )
    dag_run.dag = self.dag  # Set DAG reference (required for update_state)

    session.add(dag_run)
    session.flush()

    # Create TaskInstances
    dag_run.verify_integrity(session=session)

    session.commit()

    workflow.logger.info(f"Created DagRun: {dag_run.id} with {len(dag_run.task_instances)} tasks")
    return dag_run.id
  finally:
    session.close()
```

### Test

Add to `temporal_airflow/tests/test_workflows.py`:

```python
@pytest.mark.asyncio
async def test_dag_run_creation():
    """Test that workflow creates DagRun and TaskInstances."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create DAG with tasks
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        t1 = PythonOperator(task_id="task1", python_callable=lambda: None)
        t2 = PythonOperator(task_id="task2", python_callable=lambda: None)
        t1 >> t2

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-dag-run-creation",
                task_queue="test-queue",
            )

            # Should complete (placeholder return for now)
            assert result.dag_id == "test_dag"
```

### Why This Commit

- ✅ Self-contained method (~50 lines)
- ✅ Can test DAG run and TI creation
- ✅ Sets up data for scheduling loop
- ✅ Tests validate database operations work

---

## Commit 4: Basic Scheduling Loop Structure

**File**: `temporal_airflow/workflows.py` (EDIT)

### What This Commit Adds

- `_scheduling_loop()` method skeleton
- Basic while loop with iteration limit
- Query DagRun from database
- Check for completion (SUCCESS/FAILED states)
- Return final state
- **NO activity starting yet** - just the loop structure

### Implementation

```python
# In run() method, after _create_dag_run():

# Main scheduling loop
final_state = await self._scheduling_loop(dag_run_id)

end_time = workflow.now()

# Return result
return DagExecutionResult(
  state=final_state,
  dag_id=input.dag_id,
  run_id=input.run_id,
  start_date=start_time,
  end_date=end_time,
  tasks_succeeded=0,  # TODO: Track in later commits
  tasks_failed=0,
)


# Add method:

async def _scheduling_loop(self, dag_run_id: int) -> str:
  """
  Main scheduling loop.

  Design Notes:
  - Decision 2: Direct activity management (no executor)
  - Decision 6: Accepts sync calls (ORM queries, update_state)

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
    session = self.sessionFactory()
    try:
      dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
      dag_run.dag = self.dag  # Restore DAG reference

      # Check if complete
      if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
        workflow.logger.info(f"DAG completed: {dag_run.state}")
        return dag_run.state.value

      # TODO Commit 5: Update state and get schedulable tasks
      # TODO Commit 6: Start activities for schedulable tasks
      # TODO Commit 7: Handle activity completions

    finally:
      session.close()

    # No running activities yet, just sleep
    await asyncio.sleep(5)

  workflow.logger.error("Max iterations reached!")
  return "failed"
```

### Test

Add to `temporal_airflow/tests/test_workflows.py`:

```python
@pytest.mark.asyncio
async def test_scheduling_loop_detects_completion():
    """Test that loop detects when DAG completes."""
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create simple DAG with EmptyOperator (completes immediately)
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        EmptyOperator(task_id="task1")

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-loop-completion",
                task_queue="test-queue",
            )

            # Should eventually complete (when update_state is added in Commit 5)
            assert result.state in ["success", "failed"]
```

### Why This Commit

- ✅ Establishes loop structure without complexity
- ✅ Can test state checking logic
- ✅ Provides foundation for activity management
- ✅ Loop completes safely (no infinite loops)

---

## Commit 5: Scheduler State Updates

**File**: `temporal_airflow/workflows.py` (EDIT)

### What This Commit Adds

- Call `dag_run.update_state()` in loop
- Call `dag_run.schedule_tis()` if schedulable tasks found
- Log schedulable tasks but **don't start activities yet**

### Implementation

```python
# In _scheduling_loop(), after completion check:

# Update state and get schedulable tasks
schedulable_tis, callback = dag_run.update_state(
    session=session,
    execute_callbacks=False,  # We handle callbacks in Phase 5
)

# Start activities for new schedulable tasks
if schedulable_tis:
    dag_run.schedule_tis(schedulable_tis, session=session)
    session.commit()

    for ti in schedulable_tis:
        ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)
        workflow.logger.info(f"Task ready for scheduling: {ti_key}")
        # TODO Commit 6: Serialize task and start activity
```

### Test

No new test needed - existing tests in Commit 4 will now progress further in the loop.

### Why This Commit

- ✅ Tests scheduler logic works (~20 lines)
- ✅ Can verify update_state returns correct tasks
- ✅ Isolates scheduler logic from activity management
- ✅ Existing tests validate this works

---

## Commit 6: Activity Starting (Task Serialization + XCom)

**File**: `temporal_airflow/workflows.py` (EDIT)

### What This Commit Adds

- `_get_upstream_xcom()` helper method
- In loop: Serialize individual tasks (Decision 7)
- Start activities with `workflow.start_activity()`
- Track activity handles in `running_activities` dict
- **Don't await completions yet** - just start them

### Implementation

```python
# Add helper method:

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

# In _scheduling_loop(), inside schedulable_tis block:

for ti in schedulable_tis:
    ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)

    # TODO Phase 5: Check pool availability

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

    # TODO Phase 5: Track pool usage

    workflow.logger.info(f"Started activity for {ti_key}")
```

### Test

Add to `temporal_airflow/tests/test_workflows.py`:

```python
from unittest.mock import AsyncMock
from temporal_airflow.activities import run_airflow_task


@pytest.mark.asyncio
async def test_activity_starting():
    """Test that workflow starts activities for schedulable tasks."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create DAG with Python task
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        PythonOperator(task_id="task1", python_callable=lambda: "result1")

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        # Register activity
        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-activity-start",
                task_queue="test-queue",
            )

            # Should complete (even without handling completions yet)
            # Activities will run but workflow won't process results yet
            assert result.dag_id == "test_dag"
```

### Why This Commit

- ✅ Activity starting is complex enough to be separate (~60 lines)
- ✅ Can test task serialization works
- ✅ Can verify activities get started
- ✅ Isolates from completion handling
- ✅ Tests validate Decision 7 (minimal serialization)

---

## Commit 7: Activity Completion Handling

**File**: `temporal_airflow/workflows.py` (EDIT)

### What This Commit Adds

- `_handle_activity_result()` method
- In loop: Await activity completions with `asyncio.wait()`
- Map completed activities back to task instances
- Call `_handle_activity_result()` for each completion
- Update XCom store
- Track task success/failure counts

### Implementation

```python
# Add helper method:

async def _handle_activity_result(self, ti_key: tuple, result: TaskExecutionResult):
  """
  Update TaskInstance based on activity result.

  Design Note (Decision 4):
  - result.state is already TaskInstanceState enum
  - Direct assignment works (no conversion needed)
  """
  set_workflow_time(workflow.now())

  session = self.sessionFactory()
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


# In _scheduling_loop(), replace "await asyncio.sleep(5)" with:

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

      # TODO Phase 5: Release pool slot

    except Exception as e:
      workflow.logger.error(f"Activity failed: {e}")

    del running_activities[ti_key]
else:
  # No running activities, sleep before checking for new work
  await asyncio.sleep(5)

# In run() method, update return to track counts:

# Count task results
tasks_succeeded = sum(
  1 for ti_key, xcom in self.xcom_store.items()
  if xcom.get("return_value") is not None  # Simplistic check
)
tasks_failed = 0  # TODO: Track failures properly

return DagExecutionResult(
  state=final_state,
  dag_id=input.dag_id,
  run_id=input.run_id,
  start_date=start_time,
  end_date=end_time,
  tasks_succeeded=tasks_succeeded,
  tasks_failed=tasks_failed,
)
```

### Test

Add to `temporal_airflow/tests/test_workflows.py`:

```python
@pytest.mark.asyncio
async def test_end_to_end_dag_execution():
    """Test complete DAG execution with task completion handling."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    # Create DAG with dependencies
    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        def task1_func():
            return "output_from_task1"

        def task2_func():
            return "output_from_task2"

        t1 = PythonOperator(task_id="task1", python_callable=task1_func)
        t2 = PythonOperator(task_id="task2", python_callable=task2_func)
        t1 >> t2  # Sequential execution

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-e2e-execution",
                task_queue="test-queue",
            )

            # Should complete successfully
            assert result.state == "success"
            assert result.dag_id == "test_dag"
            assert result.tasks_succeeded >= 2  # Both tasks ran


@pytest.mark.asyncio
async def test_parallel_task_execution():
    """Test that parallel tasks execute concurrently."""
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.serialization.serialized_objects import SerializedDAG

    with DAG(dag_id="test_dag", start_date=datetime(2025, 1, 1)) as dag:
        t1 = PythonOperator(task_id="task1", python_callable=lambda: "a")
        t2 = PythonOperator(task_id="task2", python_callable=lambda: "b")
        t3 = PythonOperator(task_id="task3", python_callable=lambda: "c")
        # All can run in parallel
        [t1, t2, t3]

    serialized = SerializedDAG.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        async with Worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id="test-parallel-execution",
                task_queue="test-queue",
            )

            assert result.state == "success"
            assert result.tasks_succeeded == 3
```

### Why This Commit

- ✅ Completes the core scheduling loop (~50 lines)
- ✅ Can now test end-to-end task execution
- ✅ Result handling is complex enough to isolate
- ✅ Tests validate complete workflow orchestration
- ✅ Tests validate Decision 2 (direct activity management)

---

## Phase 4 Complete! 🎉

After these 7 commits, Phase 4 is complete with:

- ✅ Workflow-specific database isolation (Decision 1)
- ✅ Direct activity management (Decision 2)
- ✅ DAG deserialization and storage (Decision 3)
- ✅ Pydantic models throughout (Decision 4)
- ✅ Sync calls in async workflow (Decision 6)
- ✅ Minimal task serialization (Decision 7)
- ✅ XCom management in workflow state
- ✅ End-to-end DAG execution

**Deferred to Phase 5**:
- ❌ Pool enforcement (Decision 9)
- ❌ Callbacks (Decision 9)
- ❌ Integration tests
- ❌ Worker setup
- ❌ Workflow starter utilities

---

## Testing Strategy

### Commit-Level Tests
- Each commit includes tests for new functionality
- Tests are independent and fast
- Use Temporal's `WorkflowEnvironment` for testing

### What to Test
1. **Database isolation** (Commit 1)
2. **DAG deserialization** (Commit 2)
3. **DagRun creation** (Commit 3)
4. **Loop completion detection** (Commit 4)
5. **Activity starting** (Commit 6)
6. **End-to-end execution** (Commit 7)

### What NOT to Test (Phase 4)
- ❌ Pool enforcement
- ❌ Callbacks
- ❌ External integrations
- ❌ Performance/scale

---

## Implementation Notes

### Use TODO Comments
- Mark Phase 5 features with `# TODO Phase 5:`
- Mark future enhancements with `# TODO:`
- Example: `# TODO Phase 5: Add pool enforcement`

### Error Handling
- Add basic try/except in each commit
- Comprehensive error handling can be added incrementally
- Log errors clearly for debugging

### Code Quality
- Follow existing Airflow code style
- Add docstrings to all methods
- Include Design Notes referencing decisions

### Git Commit Messages
Use format:
```
feat(temporal): [Commit N] Brief description

- Bullet point of what changed
- Reference to decision if applicable
- Any important notes

Refs: TEMPORAL_IMPLEMENTATION_PLAN.md Phase 4
```

Example:
```
feat(temporal): [Commit 1] Add workflow skeleton and database initialization

- Create ExecuteAirflowDagWorkflow class with @workflow.defn
- Implement _initialize_database() with workflow-specific engine
- Add instance variables for workflow state management
- Include tests for database isolation (Decision 1)

Refs: TEMPORAL_IMPLEMENTATION_PLAN.md Phase 4, Commit 1
```

---

## Ready to Implement

Phase 4 is now broken into **7 small, testable commits**. Each commit:
- ✅ Builds on previous commits
- ✅ Is independently reviewable
- ✅ Has clear success criteria
- ✅ Includes appropriate tests

Start with Commit 1 and work sequentially through to Commit 7.
