# Temporal Implementation Design Decisions

**Date**: 2025-12-20
**Status**: In Progress

This document tracks all design decisions made during the validation and refinement of the Temporal implementation plan.

---

## Decision Log

### Decision 1: Database Isolation Strategy
**Status**: ✅ Resolved
**Priority**: Critical (Blocker)
**Context**: Multiple workflows in the same worker process need isolated databases. Investigation shows Airflow's session management uses global state.

**Question**: Can we bypass Airflow's global session management by using workflow-specific engines and passing sessions explicitly?

**Validation Results**: ✅ **YES - This approach is VIABLE!**

**Key Findings**:
1. ✅ All critical scheduler methods accept explicit `session` parameter:
   - `dag_run.update_state(session=session)` ✓
   - `dag_run.schedule_tis(tis, session=session)` ✓
   - `dag_run.task_instance_scheduling_decisions(session=session)` ✓
   - All internal queries pass session through parameters ✓

2. ✅ No global session usage found in scheduler code path:
   - `dagrun.py`: Zero `create_session()` calls
   - `dagrun.py`: No imports of `engine` or `Session` from `settings`
   - All DB operations use passed session parameter

3. ⚠️ One edge case found (not critical):
   - `TaskInstance.update_heartbeat()` uses global `create_session()`
   - Only called by task execution workers, NOT by scheduler
   - Not in workflow's code path

4. ✅ DAG loading doesn't require DB:
   - `dag_run.get_dag()` returns `self.dag` (instance variable)
   - No DB query needed - just deserialize and attach

**Decision**: **Option 3 - Bypass Global Session Management**

**Implementation**:
```python
from sqlalchemy import create_engine, StaticPool
from sqlalchemy.orm import sessionmaker

class ExecuteAirflowDagWorkflow:
    def _initialize_database(self):
        workflow_id = workflow.info().workflow_id
        conn_str = f"sqlite:///file:memdb_{workflow_id}?mode=memory&cache=shared&uri=true"

        # Create workflow-specific engine - NO configure_orm()!
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

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        while True:
            # Create session from workflow-specific factory
            session = self.SessionFactory()
            try:
                dag_run = session.query(DagRun).filter(...).one()

                # Pass session explicitly to all methods
                schedulable_tis, callback = dag_run.update_state(
                    session=session,
                    execute_callbacks=False,
                )

                if schedulable_tis:
                    dag_run.schedule_tis(schedulable_tis, session=session)
                    session.commit()
            finally:
                session.close()
```

**Rationale**:
- ✅ True isolation - unique in-memory DB per workflow
- ✅ Supports thousands of concurrent workflows
- ✅ Zero global state mutation
- ✅ No monkey-patching needed
- ✅ Works with Temporal replay semantics (ephemeral DB)
- ✅ Validated against actual Airflow codebase

**Requirements**:
1. Always pass `session=` parameter explicitly to all Airflow methods
2. Never call `configure_orm()` in workflow code
3. Never use `create_session()` helper (creates global session)
4. Deserialize DAG and attach to `dag_run.dag` before calling methods

---

### Decision 2: Remove TemporalExecutor References
**Status**: ✅ Resolved
**Priority**: Critical (Bug)
**Context**: Phase 2 removed the TemporalExecutor concept, but workflow code still references it on lines 548-549 and 561.

**Question**: Should we remove all TemporalExecutor references and use direct activity management?

**Decision**: **YES - Remove all TemporalExecutor references**

**Corrected Activity Management Pattern**:

```python
async def _scheduling_loop(self, dag_run_id: int) -> str:
    # Track running activities: ti_key -> ActivityHandle
    running_activities: dict[tuple, Any] = {}

    while True:
        set_workflow_time(workflow.now())

        session = self.sessionFactory()
        try:
            dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()

            if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                return dag_run.state.value

            schedulable_tis, _ = dag_run.update_state(session=session, execute_callbacks=False)

            if schedulable_tis:
                dag_run.schedule_tis(schedulable_tis, session=session)
                session.commit()

                for ti in schedulable_tis:
                    ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)

                    # start_activity returns ActivityHandle (already awaitable!)
                    handle = workflow.start_activity(
                        "run_airflow_task",
                        arg=TaskExecutionInput(...),
                        task_queue="airflow-tasks",
                        start_to_close_timeout=timedelta(hours=2),
                    )

                    running_activities[ti_key] = handle
        finally:
            session.close()

        # Wait for any activities to complete
        if running_activities:
            # asyncio.wait accepts awaitables directly
            done, pending = await asyncio.wait(
                running_activities.values(),
                timeout=5,
                return_when=asyncio.FIRST_COMPLETED
            )

            # Map completed awaitables back to ti_keys
            for completed in done:
                ti_key = next(k for k, v in running_activities.items() if v == completed)
                result: TaskExecutionResult = completed.result()
                await self._handle_activity_result(ti_key, result)
                del running_activities[ti_key]
        else:
            await asyncio.sleep(5)
```

**Key Points**:
- ✅ `workflow.start_activity()` returns `ActivityHandle` (already awaitable)
- ✅ No need for `asyncio.create_task()` wrapper
- ✅ `asyncio.wait()` accepts awaitables directly
- ✅ Use reverse lookup to map completed awaitable → ti_key
- ❌ No executor abstraction needed

**Rationale**: Direct activity management is simpler, more efficient, and more Temporal-native than an executor abstraction.

---

### Decision 3: Undefined `serialized_dag` Variable & Workflow Input Type
**Status**: ✅ Resolved
**Priority**: High (Bug + Design)
**Context**: Line 676 references undefined `serialized_dag` variable. Also, workflow uses `dict` for input instead of Pydantic model.

**Question 1**: Where should we store the serialized DAG for activity access?
**Decision**: **Option A - Store as workflow instance variable**

**Question 2**: Should workflow input be a Pydantic model instead of dict?
**Decision**: **YES - Use Pydantic for type safety and consistency**

**Implementation**:
```python
# Define workflow input model
class DagExecutionInput(BaseModel):
    """Input model for DAG execution workflow."""

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


@workflow.defn(name="execute_airflow_dag")
class ExecuteAirflowDagWorkflow:
    def __init__(self):
        self.engine = None
        self.SessionFactory = None
        self.serialized_dag = None  # Store for activities

    @workflow.run
    async def run(self, input: DagExecutionInput) -> dict:
        """
        Execute the DAG.

        Args:
            input: Typed workflow input with all DAG execution parameters

        Returns:
            Execution result with final state
        """
        workflow.logger.info(f"Starting DAG execution: {input.dag_id} / {input.run_id}")

        # Store serialized DAG in instance variable
        self.serialized_dag = input.serialized_dag

        try:
            # Phase 1: Setup
            self._initialize_database()

            # Phase 2: Create DAG run
            dag_run_id = self._create_dag_run(
                dag_id=input.dag_id,
                run_id=input.run_id,
                logical_date=input.logical_date,
                conf=input.conf,
            )

            # Phase 3: Main scheduling loop
            final_state = await self._scheduling_loop(dag_run_id)

            return {
                "state": final_state,
                "dag_id": input.dag_id,
                "run_id": input.run_id,
            }

        finally:
            clear_workflow_time()

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        ...
        # Now self.serialized_dag is available
        task_input = TaskExecutionInput(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            logical_date=dag_run.logical_date,
            try_number=ti.try_number,
            map_index=ti.map_index,
            serialized_dag=self.serialized_dag,  # ✅ Defined!
        )
```

**Benefits of Pydantic for Workflow Input**:
- ✅ Type safety and validation (same as activities)
- ✅ IDE autocomplete for `input.dag_id` vs `input["dag_id"]`
- ✅ Automatic validation of required fields
- ✅ Clear schema documentation
- ✅ Consistency across workflow and activity inputs

**Rationale**:
- Instance variable storage is simple and Temporal-native
- Pydantic models provide same benefits for workflows as activities
- Keeps codebase consistent and type-safe throughout

---

### Decision 4: Pydantic Models for All I/O & Native Enums
**Status**: ✅ Resolved
**Priority**: High (Type Safety & Consistency)
**Context**: Need consistent type-safe I/O across workflows and activities. Should use enums directly vs strings.

**Question 1**: Should all workflow/activity inputs AND outputs use Pydantic models?
**Decision**: **YES - Use Pydantic for all I/O**

**Question 2**: Should we use Airflow's native enums (TaskInstanceState) directly in Pydantic models?
**Decision**: **YES - Pydantic handles enum serialization automatically**

**Key Insight**: Pydantic automatically serializes enums to their string values and deserializes them back to enum objects!

**Complete Model Definitions**:
```python
from pydantic import BaseModel, Field
from airflow.utils.state import TaskInstanceState, DagRunState

# Activity Models
class TaskExecutionInput(BaseModel):
    """Input model for task execution activity."""
    dag_id: str
    task_id: str
    run_id: str
    logical_date: datetime
    try_number: int = 1
    map_index: int = -1
    serialized_dag: dict[str, Any]

class TaskExecutionResult(BaseModel):
    """Result model for task execution activity."""
    dag_id: str
    task_id: str
    run_id: str
    try_number: int

    # ✅ Use enum directly - Pydantic handles serialization!
    state: TaskInstanceState = Field(..., description="Final task state")

    start_date: datetime
    end_date: datetime
    return_value: Any | None = None
    error_message: str | None = None

# Workflow Models
class DagExecutionInput(BaseModel):
    """Input model for DAG execution workflow."""
    dag_id: str
    run_id: str
    logical_date: datetime
    conf: dict[str, Any] | None = None
    serialized_dag: dict[str, Any]

class DagExecutionResult(BaseModel):
    """✅ Result model for DAG execution workflow (was dict before)."""
    state: str  # DagRunState value
    dag_id: str
    run_id: str
    start_date: datetime
    end_date: datetime
    tasks_succeeded: int = 0
    tasks_failed: int = 0
```

**Workflow Implementation**:
```python
@workflow.defn(name="execute_airflow_dag")
class ExecuteAirflowDagWorkflow:
    @workflow.run
    async def run(self, input: DagExecutionInput) -> DagExecutionResult:  # ✅ Pydantic out
        start_time = workflow.now()
        ...
        final_state = await self._scheduling_loop(dag_run_id)

        return DagExecutionResult(  # ✅ Type-safe return
            state=final_state,
            dag_id=input.dag_id,
            run_id=input.run_id,
            start_date=start_time,
            end_date=workflow.now(),
            tasks_succeeded=succeeded_count,
            tasks_failed=failed_count,
        )

    async def _handle_activity_result(self, ti_key: tuple, result: TaskExecutionResult):
        """Update TaskInstance based on activity result."""
        set_workflow_time(workflow.now())

        session = self.SessionFactory()
        try:
            ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti_key[0],
                TaskInstance.task_id == ti_key[1],
                TaskInstance.run_id == ti_key[2],
                TaskInstance.map_index == ti_key[3],
            ).one()

            # ✅ result.state is already TaskInstanceState enum!
            ti.state = result.state  # Direct assignment - no conversion needed!
            ti.start_date = result.start_date
            ti.end_date = result.end_date

            if result.return_value is not None:
                ti.xcom_push(key="return_value", value=result.return_value, session=session)

            session.commit()

            workflow.logger.info(
                f"Updated task {ti_key} to state {ti.state} "
                f"(duration: {result.end_date - result.start_date})"
            )
        finally:
            session.close()
```

**Activity Implementation**:
```python
@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: TaskExecutionInput) -> TaskExecutionResult:
    start_time = datetime.utcnow()

    try:
        # Execute task...
        # ...

        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.SUCCESS,  # ✅ Return enum directly!
            start_date=start_time,
            end_date=datetime.utcnow(),
        )

    except Exception as e:
        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.FAILED,  # ✅ Return enum directly!
            start_date=start_time,
            end_date=datetime.utcnow(),
            error_message=str(e),
        )
```

**How Pydantic Serializes Enums**:
```python
# Activity returns
result = TaskExecutionResult(state=TaskInstanceState.SUCCESS, ...)

# Temporal serializes to JSON
# {"state": "success", ...}  ← String value

# Workflow receives and deserializes
result: TaskExecutionResult = await activity_handle
result.state  # TaskInstanceState.SUCCESS ← Enum object!
```

**Benefits**:
- ✅ **Type safety everywhere**: No string→enum conversion needed
- ✅ **IDE autocomplete**: `result.state == TaskInstanceState.SUCCESS`
- ✅ **Validation**: Invalid enum values fail at deserialization
- ✅ **Consistency**: Pydantic for all inputs and outputs
- ✅ **Native Airflow types**: Use actual Airflow enums throughout
- ✅ **Automatic serialization**: Temporal SDK handles Pydantic models natively

**Rationale**: Pydantic models + native enums provide maximum type safety with zero conversion overhead. Temporal SDK and Pydantic handle all serialization automatically.

---

### Decision 5: Time Provider Import Pattern
**Status**: ✅ Resolved
**Priority**: Medium (Code Quality)
**Context**: Phase 1 patches Airflow files to use `get_current_time()` instead of `timezone.utcnow()`. Need standard import pattern.

**Question 1**: Should imports be module-level or inline at each usage?
**Decision**: **Module-level imports (Option C - Alias Pattern)**

**Question 2**: Should this be documented as standard pattern for all time patches?
**Decision**: **YES - Apply consistently across all patched files**

**Standard Pattern for All Time Injection Patches**:
```python
# At top of file (e.g., airflow/models/dagrun.py)
from __future__ import annotations

# ... other imports ...
from airflow._shared.timezones import timezone  # Keep for other uses
from temporal_airflow.time_provider import get_current_time  # ✅ Add once

# Then in code, replace all timezone.utcnow() calls:
class DagRun:
    def update_state(self, ...):
        start_dttm = get_current_time()  # Was: timezone.utcnow()
        ...

    def schedule_tis(self, ...):
        scheduled_dttm = get_current_time()  # Was: timezone.utcnow()
        start_date = get_current_time()      # Was: timezone.utcnow()
        end_date = get_current_time()        # Was: timezone.utcnow()

    def next_dagruns_to_examine(cls, ...):
        current_time = get_current_time()
        query = query.where(DagRun.run_after <= current_time)  # Was: <= func.now()
```

**Updated Phase 1 Changes**:

**File: `airflow/models/dagrun.py`**
```python
# Change 0: Add import at top of file
from temporal_airflow.time_provider import get_current_time

# Change 1: Line 1182 (in update_state method)
# BEFORE:
start_dttm = timezone.utcnow()
# AFTER:
start_dttm = get_current_time()

# Change 2: Line 2086 (in schedule_tis method)
# BEFORE:
scheduled_dttm=timezone.utcnow(),
# AFTER:
scheduled_dttm=get_current_time(),

# Change 3: Line 2108 (in schedule_tis method)
# BEFORE:
start_date=timezone.utcnow(),
end_date=timezone.utcnow(),
# AFTER:
start_date=get_current_time(),
end_date=get_current_time(),

# Change 4: Line 613 (in next_dagruns_to_examine method)
# BEFORE:
query = query.where(DagRun.run_after <= func.now())
# AFTER:
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)

# Change 5: Line 699 (in get_queued_dag_runs_to_set_running method)
# BEFORE:
query = query.where(DagRun.run_after <= func.now())
# AFTER:
current_time = get_current_time()
query = query.where(DagRun.run_after <= current_time)
```

**Apply Same Pattern to Other Files**:
- `airflow/models/taskinstance.py` - Add import once, replace all occurrences
- `airflow/ti_deps/deps/*.py` - Add import once per file, replace all
- Any other scheduler-related files found during audit

**Benefits**:
- ✅ Single import per file (clean)
- ✅ Standard Python practice
- ✅ Easy to review in PR (one import added, N usages changed)
- ✅ Keeps existing `timezone` import for other methods (convert_to_utc, etc.)
- ✅ Consistent pattern across all patched files

**Rationale**: Module-level imports are standard Python practice and make the changes cleaner and easier to review. This pattern should be applied consistently across all time injection patches.

---

### Decision 6: Sync/Async Code Mixing Strategy
**Status**: ✅ Resolved
**Priority**: Medium (Architecture)
**Context**: Async workflow calls synchronous Airflow scheduler code (ORM queries, update_state, etc.).

**Question 1**: Should we accept sync calls in async workflow, or wrap in thread executor?
**Decision**: **Option A - Accept sync calls directly**

**Question 2**: Should this be documented as acceptable pattern?
**Decision**: **YES - Document why sync-in-async is appropriate here**

**Implementation**:

```python
async def _scheduling_loop(self, dag_run_id: int) -> str:
    """
    Main scheduling loop.

    Note: This method mixes async and sync code. The sync calls (SQLAlchemy ORM,
    dag_run.update_state()) are intentionally not wrapped in asyncio.to_thread()
    because:
    1. They're fast (in-memory SQLite, <50ms per iteration)
    2. Workflow is single-threaded anyway (no concurrency benefit)
    3. Temporal workflows are coordinators, not high-concurrency processors
    4. Thread wrapping adds complexity without meaningful benefit
    """
    running_activities: dict[tuple, Any] = {}

    while True:
        set_workflow_time(workflow.now())

        # Sync calls - acceptable here
        session = self.sessionFactory()
        try:
            dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()

            if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                return dag_run.state.value

            # Sync scheduler logic - fast enough to not warrant thread wrapping
            schedulable_tis, _ = dag_run.update_state(
                session=session,
                execute_callbacks=False,
            )

            if schedulable_tis:
                dag_run.schedule_tis(schedulable_tis, session=session)
                session.commit()

                # Start activities (async)
                for ti in schedulable_tis:
                    ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)
                    handle = workflow.start_activity(...)
                    running_activities[ti_key] = handle

        finally:
            session.close()

        # Async activity coordination
        if running_activities:
            done, pending = await asyncio.wait(
                running_activities.values(),
                timeout=5,
                return_when=asyncio.FIRST_COMPLETED
            )
            # Handle completions...
        else:
            await asyncio.sleep(5)
```

**Rationale**:

**Why sync calls are acceptable**:
1. **Performance**: In-memory SQLite queries + scheduler logic = ~5-50ms per iteration
2. **Workflow nature**: Temporal workflows are single-threaded coordinators, not concurrent processors
3. **Time breakdown**:
   - Sync scheduler logic: <1% of total time
   - Async activity waiting: >99% of total time
4. **Simplicity**: No thread wrapping complexity, no session threading issues
5. **Temporal design**: Workflows are meant to be lightweight coordinators that wait on activities

**What runs async (the important parts)**:
- ✅ Activity execution (the actual work)
- ✅ Waiting for multiple activities concurrently
- ✅ `asyncio.wait()` for activity completion
- ✅ Sleep between iterations

**What runs sync (the fast parts)**:
- ✅ SQLite queries (microseconds)
- ✅ Scheduler state computation (milliseconds)
- ✅ Session management

**Performance Impact**: Negligible - workflow spends >99% of time waiting for activities, not in scheduler logic.

**Alternative Considered**: Wrapping sync code in `asyncio.to_thread()` adds complexity (extra methods, thread overhead, session threading issues) for <1% time savings.

**Conclusion**: Accept sync calls for simplicity. If profiling later shows scheduler logic is a bottleneck (unlikely with in-memory SQLite), we can revisit.

---

### Decision 7: Activity Task Execution - Pass Only Task Definition
**Status**: ✅ Resolved
**Priority**: Critical (Performance & Scalability)
**Context**: Activities need task execution info. Passing entire serialized DAG bloats Temporal history. Activities run in separate processes.

**Question**: Pass full DAG or just task definition to activities?
**Decision**: **Option 1 - Pass only serialized task definition**

**Key Principle**: Activities can run in separate processes/machines, so all data must come through input parameters. No shared memory or databases.

**Updated Models**:
```python
class TaskExecutionInput(BaseModel):
    """Input model for task execution activity - minimal data."""

    # Task identification
    dag_id: str
    task_id: str
    run_id: str
    logical_date: datetime
    try_number: int = 1
    map_index: int = -1

    # Task definition (NOT full DAG)
    serialized_task: dict[str, Any] = Field(..., description="Serialized task operator")

    # Execution context
    upstream_results: dict[str, Any] | None = Field(
        default=None,
        description="XCom values from upstream tasks"
    )

class TaskExecutionResult(BaseModel):
    """Result model for task execution activity."""
    dag_id: str
    task_id: str
    run_id: str
    try_number: int

    state: TaskInstanceState

    start_date: datetime
    end_date: datetime

    # Task output for downstream tasks
    return_value: Any | None = None
    xcom_data: dict[str, Any] | None = Field(
        default=None,
        description="XCom values pushed by this task"
    )

    error_message: str | None = None
```

**Workflow Implementation**:
```python
@workflow.defn(name="execute_airflow_dag")
class ExecuteAirflowDagWorkflow:
    def __init__(self):
        self.engine = None
        self.SessionFactory = None
        self.serialized_dag = None
        self.dag = None  # Deserialized DAG for workflow use
        self.xcom_store: dict[tuple, Any] = {}  # Store XCom in workflow state

    async def run(self, input: DagExecutionInput) -> DagExecutionResult:
        self.serialized_dag = input.serialized_dag

        # Deserialize DAG once in workflow
        from airflow.serialization.serialized_objects import SerializedDAG
        self.dag = SerializedDAG.from_dict(self.serialized_dag)

        ...

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        running_activities: dict[tuple, Any] = {}

        while True:
            session = self.SessionFactory()
            try:
                dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()

                if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                    return dag_run.state.value

                schedulable_tis, _ = dag_run.update_state(session=session, execute_callbacks=False)

                if schedulable_tis:
                    dag_run.schedule_tis(schedulable_tis, session=session)
                    session.commit()

                    for ti in schedulable_tis:
                        ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)

                        # Get task from deserialized DAG
                        task = self.dag.get_task(ti.task_id)

                        # Serialize ONLY this task (not entire DAG)
                        from airflow.serialization.serialized_objects import SerializedBaseOperator
                        serialized_task = SerializedBaseOperator.serialize_operator(task)

                        # Gather upstream XCom results for this task
                        upstream_results = self._get_upstream_xcom(ti, task, session)

                        handle = workflow.start_activity(
                            "run_airflow_task",
                            arg=TaskExecutionInput(
                                dag_id=ti.dag_id,
                                task_id=ti.task_id,
                                run_id=ti.run_id,
                                logical_date=dag_run.logical_date,
                                try_number=ti.try_number,
                                map_index=ti.map_index,
                                serialized_task=serialized_task,  # ✅ Just this task!
                                upstream_results=upstream_results,
                            ),
                            task_queue="airflow-tasks",
                            start_to_close_timeout=timedelta(hours=2),
                        )

                        running_activities[ti_key] = handle
            finally:
                session.close()

            # Handle completions
            if running_activities:
                done, _ = await asyncio.wait(
                    running_activities.values(),
                    timeout=5,
                    return_when=asyncio.FIRST_COMPLETED
                )

                for completed in done:
                    ti_key = next(k for k, v in running_activities.items() if v == completed)
                    result: TaskExecutionResult = completed.result()

                    # Store XCom in workflow state
                    if result.xcom_data:
                        self.xcom_store[ti_key] = result.xcom_data

                    await self._handle_activity_result(ti_key, result)
                    del running_activities[ti_key]
            else:
                await asyncio.sleep(5)

    def _get_upstream_xcom(self, ti: TI, task, session) -> dict[str, Any] | None:
        """Gather XCom values from upstream tasks."""
        if not task.upstream_task_ids:
            return None

        upstream_results = {}
        for upstream_task_id in task.upstream_task_ids:
            upstream_key = (ti.dag_id, upstream_task_id, ti.run_id, ti.map_index)
            if upstream_key in self.xcom_store:
                upstream_results[upstream_task_id] = self.xcom_store[upstream_key]

        return upstream_results if upstream_results else None
```

**Activity Implementation**:
```python
from airflow.serialization.serialized_objects import SerializedBaseOperator

@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: TaskExecutionInput) -> TaskExecutionResult:
    """
    Execute a single Airflow task.

    Note: This activity runs in a separate process and receives all necessary
    data through the input parameter.
    """
    start_time = datetime.utcnow()

    try:
        activity.heartbeat()

        # Deserialize just this task
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

        # Capture any XCom pushes (if task pushed data)
        xcom_data = {"return_value": result} if result is not None else None

        activity.logger.info(f"Task {input.task_id} completed successfully")

        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.SUCCESS,
            start_date=start_time,
            end_date=datetime.utcnow(),
            return_value=result,
            xcom_data=xcom_data,
        )

    except Exception as e:
        activity.logger.error(f"Task {input.task_id} failed: {e}", exc_info=True)

        return TaskExecutionResult(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            state=TaskInstanceState.FAILED,
            start_date=start_time,
            end_date=datetime.utcnow(),
            error_message=str(e),
        )
```

**Benefits**:
- ✅ **Scalability**: Minimal Temporal history size (no full DAG per activity)
- ✅ **Process Isolation**: Activities get all data through input (can run anywhere)
- ✅ **XCom Support**: Workflow manages XCom state, passes to downstream tasks
- ✅ **Matches Airflow**: Similar to executor pattern (minimal task info)
- ✅ **Performance**: Workflow deserializes DAG once, extracts tasks as needed

**XCom Handling**:
- Workflow stores XCom in `self.xcom_store` dict (in-memory, survives replay)
- Upstream results gathered and passed to activities via `upstream_results`
- Activity returns XCom data via `xcom_data` field
- Workflow updates XCom store on task completion

**History Size Comparison**:
- Old approach: 100 tasks × 1MB DAG = 100MB history ❌
- New approach: 100 tasks × 10KB task = 1MB history ✅

**Rationale**: Activities are process-isolated by design. All data must flow through typed inputs. This approach minimizes history size while maintaining full functionality.

---

### Decision 8: Continue-As-New Strategy for Long DAGs
**Status**: ✅ Resolved (Deferred)
**Priority**: Medium (Production Feature)
**Context**: Temporal workflows have history size limits (~50MB). Long-running DAGs with many tasks could exceed limits.

**Question**: Should we implement continue-as-new now or defer to later phase?
**Decision**: **Option B - Defer to Phase 6 (or later based on real-world needs)**

**Rationale**:

**Why defer**:
1. **Current efficiency is good**: With Decision 7 (minimal task serialization), we use ~12KB per task
   - 4000 tasks = ~48MB (under 50MB limit)
   - Covers vast majority of real-world DAGs

2. **Typical DAG sizes are manageable**:
   - Small: 10-100 tasks (most common)
   - Large: 500-1000 tasks
   - Very large: 2000-4000 tasks (rare)
   - Extreme: >4000 tasks (very rare)

3. **Complexity not justified yet**:
   - Continue-as-new requires state transfer logic
   - Must handle running activities carefully
   - Better to validate basic architecture first
   - YAGNI principle: build when actually needed

4. **Temporal provides monitoring**:
   - Built-in history size metrics available
   - No custom monitoring code needed
   - Can track usage in production and decide if needed

**Implementation Plan**:
- **Phase 1-5**: No continue-as-new implementation
- **Documentation**: Note limitation "Supports DAGs up to ~4000 tasks"
- **Monitoring**: Use Temporal's built-in history metrics
- **Phase 6+**: Implement if customers need >4000 task DAGs

**Fallback Strategy**:
If large DAGs are needed before Phase 6:
1. Check Temporal history size metrics
2. Implement Option C (dynamic monitoring + continue-as-new)
3. Use history length threshold (~40K events = ~80% of limit)

**No Action Required**: Temporal platform already provides history monitoring. Decision is to defer implementation, not add monitoring code.

---

### Decision 9: Missing Features Triage
**Status**: ✅ Resolved
**Priority**: Medium (Scope Management)
**Context**: Several Airflow features not addressed in initial plan. Need to prioritize for Phases 1-5 vs defer.

**Question**: Which features should be in initial implementation (Phases 1-5) vs deferred to Phase 6+?
**Decision**: **Implement essential features (Queues, Pools, Basic Callbacks) in Phases 1-5. Defer advanced features to Phase 6+.**

**Feature Prioritization**:

| Feature | Priority | Phase | Rationale |
|---------|----------|-------|-----------|
| **Queues** | High | Phase 3 | Trivial - just pass `ti.queue` to activity task_queue |
| **Pools** | High | Phase 5 | Essential for production resource management |
| **Basic Callbacks** | Medium | Phase 5 | Common for notifications (success/failure) |
| **Sensors** | Medium | Phase 6+ (Deferred) | Can work around, medium complexity |
| **SLA Callbacks** | Low | Phase 6+ (Deferred) | Less common than basic callbacks |
| **Dynamic Mapping** | Medium | Phase 6+ (Deferred) | Complex, can be added later |
| **Triggers (Deferrable)** | Low | Future/Out of Scope | Complex, advanced feature |

**Updated Implementation Phases**:

**Phase 1**: Time Provider & Determinism ✅
- Time injection patches
- Module-level imports (Decision 5)

**Phase 2**: ~~TemporalExecutor~~ (REMOVED) ✅

**Phase 3**: Task Execution Activity
- Pydantic models (Decision 4)
- Activity implementation (Decision 7)
- ✅ **Queue support** - Pass `ti.queue` to activity `task_queue` parameter

**Phase 4**: Workflow Implementation
- Database initialization (Decision 1)
- DAG run creation
- Scheduling loop (Decision 2, 6)
- Direct activity management

**Phase 5**: Integration & Testing
- End-to-end testing
- ✅ **Pool enforcement** - Track pool usage in workflow state
- ✅ **Basic callbacks** - Execute on_success/on_failure callbacks

**Phase 6+**: Deferred Features (Future Work)
- ❌ Sensors (polling tasks)
- ❌ SLA callbacks
- ❌ Dynamic task mapping
- ❌ Triggers (deferrable operators)
- ❌ Continue-as-new (Decision 8)
- Performance optimization
- Observability enhancements

**Implementation Details for Phase 3-5 Features**:

**Queues (Phase 3 - Trivial)**:
```python
# In workflow when starting activity
handle = workflow.start_activity(
    "run_airflow_task",
    arg=task_input,
    task_queue=ti.queue or "airflow-tasks",  # ✅ Use task's queue
    ...
)
```

**Pools (Phase 5 - Medium)**:
```python
class ExecuteAirflowDagWorkflow:
    def __init__(self):
        ...
        self.pool_usage: dict[str, int] = {}  # Track pool slots

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        ...
        for ti in schedulable_tis:
            # Check pool availability
            if ti.pool:
                pool_slots = self._get_pool_slots(ti.pool, session)
                if self.pool_usage.get(ti.pool, 0) >= pool_slots:
                    continue  # Skip, pool full

            # Start activity
            handle = workflow.start_activity(...)

            # Track pool usage
            if ti.pool:
                self.pool_usage[ti.pool] = self.pool_usage.get(ti.pool, 0) + 1

        # When activity completes
        if ti.pool:
            self.pool_usage[ti.pool] -= 1
```

**Basic Callbacks (Phase 5 - Medium)**:
```python
async def _scheduling_loop(self, dag_run_id: int) -> str:
    ...
    # When DAG completes
    if dag_run.state == DagRunState.SUCCESS:
        if self.dag.has_on_success_callback:
            # Execute callback (as activity or inline)
            await self._execute_dag_callback(self.dag, success=True)

    elif dag_run.state == DagRunState.FAILED:
        if self.dag.has_on_failure_callback:
            await self._execute_dag_callback(self.dag, success=False)
```

**Rationale**:
- **Focus on MVP**: Get core functionality working end-to-end
- **Essential vs Nice-to-have**: Queues, Pools, Basic Callbacks are essential for production
- **Complexity management**: Defer complex features (Sensors, Dynamic Mapping, Triggers)
- **Iteration**: Can add deferred features based on real-world needs
- **Timeline**: Keeps Phases 1-5 focused and achievable

**Documentation**:
Create LIMITATIONS.md documenting deferred features:
- Sensors: Not supported initially
- Dynamic task mapping: Not supported initially
- Deferrable operators: Not supported initially
- SLA monitoring: Basic only, no SLA callbacks

---

### Decision 10: Implementation Phases & Complexity Assessment
**Status**: ✅ Resolved
**Priority**: High (Project Planning)
**Context**: Need clear phase breakdown and complexity assessment. Timeline estimates not relevant as implementation will use LLM assistance.

**Question**: What's the updated phase breakdown and complexity assessment?
**Decision**: **Focus on phase structure and complexity, not timelines (LLM-assisted implementation)**

**Updated Implementation Phases**:

### **Phase 1: Time Provider & Determinism**
**Complexity**: Medium
**Key Tasks**:
- ✅ Create `temporal_airflow/time_provider.py` module
- ✅ Patch `airflow/models/dagrun.py` (5 changes, module-level import per Decision 5)
- ✅ Audit and patch other files (`taskinstance.py`, `ti_deps/**`)
- ✅ Unit tests for time injection
- ✅ Verify no regression in existing Airflow tests

**Dependencies**: None
**Risk**: Low - straightforward patching

---

### **Phase 2: ~~TemporalExecutor~~**
**Status**: **REMOVED** (Decision 2)

---

### **Phase 3: Task Execution Activity**
**Complexity**: Medium-High
**Key Tasks**:
- ✅ Create `temporal_airflow/models.py` with Pydantic models (Decision 4)
  - `TaskExecutionInput` (minimal, no full DAG - Decision 7)
  - `TaskExecutionResult` (with TaskInstanceState enum)
  - `DagExecutionInput` and `DagExecutionResult`
- ✅ Create `temporal_airflow/activities.py`
  - Deserialize single task from `serialized_task`
  - Build execution context with upstream XCom
  - Execute task via `task.execute(context)`
  - Return result with XCom data
- ✅ Queue support - pass `ti.queue` to activity `task_queue` (trivial)
- ✅ Activity tests

**Dependencies**: Phase 1 complete
**Risk**: Medium - task deserialization and context building

---

### **Phase 4: Workflow Implementation**
**Complexity**: High
**Key Tasks**:
- ✅ Create `temporal_airflow/workflows.py`
- ✅ Database initialization (Decision 1)
  - Workflow-specific engine with unique in-memory DB
  - Session factory (no global `configure_orm()`)
- ✅ DAG deserialization and storage in workflow state
- ✅ DAG run creation
- ✅ Scheduling loop (Decision 2, 6)
  - Accept sync calls (Decision 6)
  - Direct activity management with `workflow.start_activity()`
  - Serialize individual tasks (Decision 7)
  - Gather upstream XCom for each task
  - Track running activities
  - Handle completions with `asyncio.wait()`
- ✅ XCom management in workflow state
- ✅ Activity result handling with state updates

**Dependencies**: Phase 3 complete
**Risk**: High - most complex phase, core orchestration logic

---

### **Phase 5: Integration & Testing**
**Complexity**: Medium
**Key Tasks**:
- ✅ Create `temporal_airflow/worker.py`
- ✅ Create `temporal_airflow/start_workflow.py`
- ✅ Create example DAG for testing
- ✅ End-to-end tests (simple DAG 3-5 tasks)
- ✅ Pool enforcement
  - Track pool usage in workflow state
  - Check availability before starting activities
- ✅ Basic callbacks (on_success/on_failure)
  - Execute when DAG completes
- ✅ Test failure scenarios
  - Task failures
  - Retries
  - Parallel tasks
  - Time determinism

**Dependencies**: Phase 4 complete
**Risk**: Medium - integration issues, debugging

---

### **Phase 6: Production Readiness**
**Complexity**: Low-Medium
**Key Tasks**:
- ✅ Documentation
  - `README.md` - Architecture overview
  - `SETUP.md` - Setup instructions
  - `LIMITATIONS.md` - Deferred features (sensors, dynamic mapping, triggers)
  - Code comments
- ✅ Performance testing
- ✅ Security review
- ❌ **Deferred Features** (Decision 8, 9):
  - Continue-as-new (add if needed)
  - Sensors
  - SLA callbacks
  - Dynamic task mapping
  - Triggers/deferrable operators

**Dependencies**: Phase 5 complete
**Risk**: Low - polish and documentation

---

### **Complexity Assessment**

| Phase | Complexity | Critical Path | LLM Assistance Suitability |
|-------|------------|---------------|----------------------------|
| Phase 1 | Medium | Yes | High - straightforward patching |
| Phase 3 | Medium-High | Yes | High - clear patterns to follow |
| Phase 4 | High | Yes | Medium - complex logic, needs careful review |
| Phase 5 | Medium | Yes | High - testing and integration |
| Phase 6 | Low-Medium | No | High - documentation |

**Critical Success Factors**:
1. **Phase 4 Workflow Logic** - Most complex, requires careful implementation
2. **Database Isolation** (Decision 1) - Must work correctly for multiple workflows
3. **XCom Handling** (Decision 7) - Proper state management
4. **Activity Result Mapping** - Correctly map completions to task instances

**Implementation Approach**:
- Use LLM assistance for code generation
- Focus on incremental implementation (phase by phase)
- Test each phase thoroughly before moving to next
- Leverage decisions document for implementation guidance

**Rationale**: LLM-assisted implementation makes timeline estimates less relevant. Focus on clear phase structure, complexity assessment, and risk areas instead.

---

## Decisions Summary
- **Total Decisions**: 10
- **Completed**: 10
- **Pending**: 0
- **Deferred**: 0

### Quick Reference

| # | Decision | Status | Impact |
|---|----------|--------|--------|
| 1 | Database Isolation | ✅ Resolved | Workflow-specific engine + explicit sessions |
| 2 | Remove TemporalExecutor | ✅ Resolved | Direct activity management |
| 3 | Serialized DAG Storage | ✅ Resolved | Instance variable + Pydantic input |
| 4 | Pydantic & Enums | ✅ Resolved | Use everywhere for type safety |
| 5 | Time Injection Imports | ✅ Resolved | Module-level standard pattern |
| 6 | Sync/Async Mixing | ✅ Resolved | Accept sync calls (fast enough) |
| 7 | Activity Task Execution | ✅ Resolved | Pass only task definition + XCom |
| 8 | Continue-As-New | ✅ Deferred | Defer to Phase 6+ |
| 9 | Missing Features Triage | ✅ Resolved | Essential features in Phases 1-5 |
| 10 | Implementation Phases | ✅ Resolved | Clear phases, no timelines |

---

## Impact on Original Plan

### Architecture Changes
1. **✅ Simplified**: Removed TemporalExecutor (saved complexity)
2. **✅ Improved**: Workflow-specific database isolation (Decision 1)
3. **✅ Improved**: Pydantic models everywhere (Decision 4)
4. **✅ Optimized**: Minimal task serialization (Decision 7 - 100x history reduction)
5. **✅ Clarified**: Accept sync calls documented (Decision 6)
6. **✅ Scoped**: Clear feature prioritization (Decision 9)

### Scope Changes
- **Removed**: Phase 2 (TemporalExecutor) - not needed
- **Added**: Queue support (Phase 3)
- **Added**: Pool enforcement (Phase 5)
- **Added**: Basic callbacks (Phase 5)
- **Deferred**: Sensors, Dynamic Mapping, Triggers, Continue-as-new

### Code Complexity
- **Reduced**: No executor layer
- **Increased**: Minimal task serialization logic
- **Increased**: XCom workflow state management
- **Net**: Similar overall complexity, but cleaner architecture

### Key Improvements Over Original Plan
1. Database isolation properly addressed
2. No TemporalExecutor complexity
3. Scalable history size (100x improvement)
4. Type-safe throughout (Pydantic + enums)
5. Clear scope and phases

---

## Next Steps

### 1. Update TEMPORAL_IMPLEMENTATION_PLAN.md
Apply all 10 decisions to the original plan:
- Update Phase 1 with Decision 5 (imports)
- Remove Phase 2 entirely
- Update Phase 3 with Decisions 4, 7 (models, minimal serialization)
- Update Phase 4 with Decisions 1, 2, 6, 7 (database, direct activities, sync/async, XCom)
- Update Phase 5 with Decision 9 (pools, callbacks)
- Update Phase 6 with Decisions 8, 9 (deferred features)

### 2. Begin Implementation
Start with Phase 1:
- Create `temporal_airflow/time_provider.py`
- Patch `airflow/models/dagrun.py`
- Audit other files
- Write tests

### 3. Iterative Approach
- Complete one phase before starting next
- Test thoroughly at each phase
- Use LLM assistance for code generation
- Review critical sections (Phase 4 workflow logic)

---

**Document Complete**: All critical design decisions resolved. Ready to update implementation plan and begin coding.
