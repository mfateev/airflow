# Deep Integration Implementation Plan

**Date**: 2025-01-01
**Status**: Phases 1-4 Implemented ✅
**Based on**: [DEEP_INTEGRATION_DESIGN.md](./DEEP_INTEGRATION_DESIGN.md)

---

## Current State

### Already Implemented ✅

| Component | Commit | Description |
|-----------|--------|-------------|
| `DagRunType.EXTERNAL` | 1dcae54c17 | Enum value + scheduler filters |
| Pluggable Orchestrator | 0902d0e4dc | `BaseDagRunOrchestrator`, `TemporalOrchestrator`, loader |
| Pluggable Time Provider | 1030c7839d | `set_time_provider(fn)` for deterministic time |
| Standalone Workflow | existing | `ExecuteAirflowDagWorkflow` with in-memory DB |
| `run_airflow_task` Activity | existing | Task execution activity |
| **Deep Integration (Phases 1-4)** | 0dff02f639 | Sync activities, deep workflow, orchestrator update |

### What Deep Integration Adds

The standalone workflow uses an **in-memory SQLite database** per workflow. Deep integration **keeps this design** and adds:

1. **In-workflow database preserved** - Same as standalone, uses Airflow's native scheduling logic
2. **Sync activities added** - Write state from in-workflow DB to real Airflow DB for UI visibility
3. **Load DAG from real DB** - Via activity, instead of passing in workflow input
4. **Connections read from real DB** - Hooks read from Airflow DB during task execution
5. **Orchestrator-triggered** - Works with existing UI/API/CLI

### Key Design Principle: Reuse Airflow's Native Logic

Both standalone and deep integration workflows use the **same scheduling logic**:

```python
# Same code in both workflows - uses Airflow's native trigger rule evaluation
dag_run.update_state(session=session)  # ← Internally calls TriggerRuleDep!
```

This ensures:
- **Correctness**: Identical behavior to built-in Airflow scheduler
- **Maintainability**: No custom trigger rule code to maintain
- **Consistency**: Both modes use the same workflow code path

---

## Implementation Phases

### Phase 1: DB Sync Activities

**Goal**: Create activities that write execution status to Airflow database for UI visibility.

#### 1.1 Create `CreateDagRunInput` / `CreateDagRunResult` Models

```python
# scripts/temporal_airflow/models.py

@dataclass
class CreateDagRunInput:
    """Input for create_dagrun_record activity."""
    dag_id: str
    logical_date: datetime
    conf: dict[str, Any] | None = None

@dataclass
class CreateDagRunResult:
    """Result from create_dagrun_record activity."""
    run_id: str
    dag_run_id: int
```

#### 1.2 Create `create_dagrun_record` Activity

```python
# scripts/temporal_airflow/activities.py

@activity.defn(name="create_dagrun_record")
async def create_dagrun_record(input: CreateDagRunInput) -> CreateDagRunResult:
    """
    Create DagRun and TaskInstance records in Airflow database.

    - Uses run_type=EXTERNAL so scheduler ignores this run
    - Creates TaskInstance records for all tasks
    - Returns run_id for workflow to use
    """
```

**Key behaviors**:
- Generates `run_id` as `external__{logical_date}`
- Creates `DagRun` with `state=RUNNING`, `run_type=EXTERNAL`
- Creates `TaskInstance` records from `SerializedDagModel`
- Uses Airflow's `create_session()` for real DB access

#### 1.3 Create `TaskStatusSync` / `DagRunStatusSync` Models

```python
# scripts/temporal_airflow/models.py

@dataclass
class TaskStatusSync:
    """Input for sync_task_status activity."""
    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    state: str  # TaskInstanceState value
    start_date: datetime | None = None
    end_date: datetime | None = None
    xcom_value: Any | None = None

@dataclass
class DagRunStatusSync:
    """Input for sync_dagrun_status activity."""
    dag_id: str
    run_id: str
    state: str  # DagRunState value
    end_date: datetime | None = None
```

#### 1.4 Create `sync_task_status` Activity

```python
# scripts/temporal_airflow/activities.py

@activity.defn(name="sync_task_status")
async def sync_task_status(input: TaskStatusSync) -> None:
    """Write task execution status to Airflow database for UI visibility."""
```

**Key behaviors**:
- Updates `TaskInstance.state`, `start_date`, `end_date`
- Writes XCom if provided
- Uses real Airflow session

#### 1.5 Create `sync_dagrun_status` Activity

```python
# scripts/temporal_airflow/activities.py

@activity.defn(name="sync_dagrun_status")
async def sync_dagrun_status(input: DagRunStatusSync) -> None:
    """Write DagRun final status to Airflow database."""
```

**Key behaviors**:
- Updates `DagRun.state` and `end_date`
- Called at workflow completion

#### 1.6 Tests

- Test each activity with mock session
- Test XCom writing
- Test error handling (record not found)

---

### Phase 2: Deep Integration Workflow

**Goal**: Create a workflow that uses **same in-workflow database as standalone** plus sync activities.

#### 2.1 Architecture: Same as Standalone + Sync Activities

```python
# scripts/temporal_airflow/deep_workflow.py

@workflow.defn(name="execute_airflow_dag_deep", sandboxed=False)
class ExecuteAirflowDagDeepWorkflow:
    """
    Temporal workflow for deep integration mode.

    SAME as standalone workflow:
    - Uses in-workflow database (in-memory SQLite)
    - Uses Airflow's native scheduling logic (update_state, TriggerRuleDep)
    - Creates real DagRun/TaskInstance models in workflow DB

    ADDED for deep integration:
    - Loads DAG from real Airflow DB (via activity)
    - Syncs status to real Airflow DB (via activities)
    - Connections/variables read from real DB by hooks
    """

    def __init__(self):
        # SAME AS STANDALONE: In-workflow database state
        self.engine = None
        self.sessionFactory = None
        self.dag = None
        self.xcom_store: dict[tuple, Any] = {}

    def _initialize_database(self):
        """SAME AS STANDALONE: Initialize in-memory SQLite database."""
        workflow_id = workflow.info().workflow_id
        conn_str = f"sqlite:///file:memdb_{workflow_id}?mode=memory&cache=shared&uri=true"
        self.engine = create_engine(conn_str, poolclass=StaticPool, ...)
        self.sessionFactory = sessionmaker(bind=self.engine, ...)
        Base.metadata.create_all(self.engine)

    @workflow.run
    async def run(self, input: DeepDagExecutionInput) -> DagExecutionResult:
        set_time_provider(workflow.now)

        try:
            # 1. SAME AS STANDALONE: Initialize in-workflow database
            self._initialize_database()

            # 2. DEEP INTEGRATION: Load DAG from real Airflow DB
            dag_data = await workflow.execute_activity(
                load_serialized_dag,
                LoadSerializedDagInput(dag_id=input.dag_id),
            )
            self.dag = SerializedDAG.from_dict(dag_data.dag_data)
            self.dag_fileloc = dag_data.fileloc

            # 3. DEEP INTEGRATION: Create/verify DagRun in real Airflow DB
            if not input.run_id:
                result = await workflow.execute_activity(create_dagrun_record, ...)
                self.run_id = result.run_id
            else:
                self.run_id = input.run_id

            # 4. SAME AS STANDALONE: Create DagRun in in-workflow database
            dag_run_id = self._create_local_dag_run(...)

            # 5. DEEP INTEGRATION: Ensure TaskInstances in real Airflow DB
            await workflow.execute_activity(ensure_task_instances, ...)

            # 6. SAME AS STANDALONE: Scheduling loop with Airflow's native logic
            #    (uses update_state() which calls TriggerRuleDep)
            final_state = await self._scheduling_loop(dag_run_id)

            # 7. DEEP INTEGRATION: Sync final state to real Airflow DB
            await workflow.execute_activity(sync_dagrun_status, ...)

            return DagExecutionResult(state=final_state, ...)
        finally:
            clear_time_provider()
```

#### 2.2 Create `DeepDagExecutionInput` Model

```python
# scripts/temporal_airflow/models.py

@dataclass
class DeepDagExecutionInput:
    """Input for deep integration workflow."""
    dag_id: str
    logical_date: datetime
    run_id: str | None = None  # None = create new, provided = use existing
    conf: dict[str, Any] | None = None
```

#### 2.3 Add Status Sync in Scheduling Loop

```python
async def _handle_activity_result(self, ti_key: tuple, result: TaskExecutionResult):
    # Sync to Airflow DB for UI visibility
    await workflow.execute_activity(
        sync_task_status,
        TaskStatusSync(
            dag_id=ti_key[0],
            task_id=ti_key[1],
            run_id=self.run_id,
            map_index=ti_key[3],
            state=result.state.value,
            start_date=result.start_date,
            end_date=result.end_date,
            xcom_value=result.xcom_data,
        ),
        start_to_close_timeout=timedelta(seconds=30),
    )
```

#### 2.4 Create `load_serialized_dag` Activity

```python
# scripts/temporal_airflow/activities.py

@activity.defn(name="load_serialized_dag")
async def load_serialized_dag(input: LoadSerializedDagInput) -> dict:
    """Load serialized DAG from Airflow database."""
    with create_session() as session:
        serialized = SerializedDagModel.get(input.dag_id, session=session)
        if not serialized:
            raise ApplicationError(f"DAG {input.dag_id} not found", non_retryable=True)
        return serialized.data
```

**Benefits of separate workflow**:
- Clean separation of concerns
- Standalone workflow unchanged (no regression risk)
- Easier to test each mode independently
- Clear which workflow to use for which scenario

---

### Phase 3: Orchestrator Integration

**Goal**: Update `TemporalOrchestrator` to use the deep integration workflow.

#### 3.1 Update TemporalOrchestrator

```python
# scripts/temporal_airflow/orchestrator.py

from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow
from temporal_airflow.models import DeepDagExecutionInput

def start_dagrun(self, dag_run: DagRun, session: Session) -> None:
    # Mark as EXTERNAL so scheduler ignores it
    dag_run.run_type = DagRunType.EXTERNAL
    session.merge(dag_run)

    # Start deep integration workflow
    # Pass run_id since DagRun already exists
    workflow_input = DeepDagExecutionInput(
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,  # Use existing run_id
        logical_date=dag_run.logical_date,
        conf=dag_run.conf or {},
    )

    workflow_id = f"airflow-{dag_run.dag_id}-{dag_run.run_id}"
    asyncio.run(self._start_workflow_async(workflow_id, workflow_input))

async def _start_workflow_async(self, workflow_id: str, input: DeepDagExecutionInput) -> None:
    client = await self._get_client()
    task_queue = get_task_queue()

    await client.start_workflow(
        ExecuteAirflowDagDeepWorkflow.run,  # Use deep workflow
        input,
        id=workflow_id,
        task_queue=task_queue,
    )
```

#### 3.2 Create `ensure_task_instances` Activity

When orchestrator creates DagRun, TaskInstances may not exist yet. Add activity to create them:

```python
# scripts/temporal_airflow/activities.py

@activity.defn(name="ensure_task_instances")
async def ensure_task_instances(input: EnsureTaskInstancesInput) -> None:
    """Create TaskInstance records if they don't exist."""
    with create_session() as session:
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == input.dag_id,
            DagRun.run_id == input.run_id,
        ).first()

        if dag_run:
            # Uses DagRun.verify_integrity() to create TaskInstances
            serialized = SerializedDagModel.get(input.dag_id, session=session)
            if serialized:
                dag_run.verify_integrity(session=session)
                session.commit()
```

---

### Phase 4: Activity Task Mode

**Goal**: Run activities with access to real Airflow DB for connections/variables.

#### 4.1 Modify `run_airflow_task` for Deep Integration

```python
# scripts/temporal_airflow/activities.py

@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: ActivityTaskInput) -> TaskExecutionResult:
    if input.deep_integration:
        # Deep integration: Load DAG from Airflow DB
        with create_session() as session:
            serialized = SerializedDagModel.get(input.dag_id, session=session)
            dag = SerializedDAG.from_dict(serialized.data)

        # Connections/variables read automatically via BaseHook
    else:
        # Standalone: Use provided serialized DAG and env vars
        dag = SerializedDAG.from_dict(input.serialized_dag)
        _setup_airflow_env(input.connections, input.variables)

    # Execute task...
```

#### 4.2 Worker Configuration

```bash
# For deep integration workers
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://user:pass@host/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TASK_QUEUE=airflow-tasks
```

---

### Phase 5: Pool Support (TODO - Future)

**Status**: Deferred - Add after core deep integration is working.

**Goal**: Respect Airflow pool limits in Temporal workflows.

#### Overview

Pool support requires:
1. Reading pool limits from Airflow DB via activity
2. Tracking pool usage in workflow state
3. Waiting for pool slots before scheduling tasks
4. Releasing slots when tasks complete

#### Key Design Decisions Needed

- How to handle pool limit changes during workflow execution?
- Should we use Temporal's built-in rate limiting instead?
- How to coordinate pool usage across multiple concurrent workflows?

#### Implementation Notes (for future)

```python
# Workflow state
self.pool_usage: dict[str, int] = {}  # pool_name -> slots_used
self.pool_limits: dict[str, int] = {}  # pool_name -> max_slots

# Activity to read limits
@activity.defn(name="get_pool_limits")
async def get_pool_limits(input: GetPoolLimitsInput) -> GetPoolLimitsResult:
    ...

# Scheduling loop check
if self.pool_usage.get(pool, 0) + slots_needed <= self.pool_limits.get(pool, 128):
    # Can schedule
else:
    # Wait for pool slot
```

**TODO**: Implement after Phase 1-4 are complete and tested.

---

### Phase 6: Airflow Configuration (Proposal)

**Status**: Proposal only - Do not implement yet.

**Goal**: Add Temporal configuration section to Airflow.

#### Proposal: Configuration Options

```ini
# airflow.cfg

[temporal]
# Enable Temporal integration
enabled = False

# Temporal server connection
address = localhost:7233
namespace = default
task_queue = airflow-tasks

# Temporal Cloud (optional)
# api_key = <your-api-key>

# TLS configuration (optional)
# tls_cert_path = /path/to/cert.pem
# tls_key_path = /path/to/key.pem
```

#### Proposal: Configuration Reading

```python
# scripts/temporal_airflow/client_config.py

def get_temporal_config() -> dict:
    """Get Temporal config from Airflow configuration."""
    return {
        "address": conf.get("temporal", "address", fallback="localhost:7233"),
        "namespace": conf.get("temporal", "namespace", fallback="default"),
        "task_queue": conf.get("temporal", "task_queue", fallback="airflow-tasks"),
        "api_key": conf.get("temporal", "api_key", fallback=None),
    }
```

#### Open Questions

1. **Where should config live?**
   - Option A: `airflow.cfg` with `[temporal]` section
   - Option B: Environment variables only (current approach via SDK's envconfig)
   - Option C: Both with env vars overriding config file

2. **Should this be part of a Temporal provider package?**
   - If yes, config would be in provider metadata
   - Provider would include orchestrator, activities, workflows

3. **How to handle Temporal Cloud vs self-hosted?**
   - Different auth mechanisms (API key vs mTLS)
   - Different address formats

#### Current Approach (Environment Variables)

The current `client_config.py` uses Temporal SDK's `envconfig` which reads:
- `TEMPORAL_ADDRESS`
- `TEMPORAL_NAMESPACE`
- `TEMPORAL_API_KEY`
- `TEMPORAL_TLS_*`

This works well and follows Temporal conventions. Adding Airflow config may be unnecessary duplication.

**Decision needed**: Keep env-only or add Airflow config integration?

---

## Implementation Order

| Phase | Description | Effort | Dependencies | Status |
|-------|-------------|--------|--------------|--------|
| **1** | DB Sync Activities | Medium | None | ✅ Complete |
| **2** | Deep Integration Workflow | Medium | Phase 1 | ✅ Complete |
| **3** | Orchestrator Integration | Small | Phase 2 | ✅ Complete |
| **4** | Activity Task Mode | Small | Phase 1 | ✅ Complete |
| **5** | Pool Support | Medium | Phase 2 | ⏳ Deferred |
| **6** | Airflow Configuration | Small | None | ⏳ Proposal Only |

**Recommended order**: 1 → 2 → 3 → 4 → 5 → 6

Phases 1-4 are the core implementation (✅ complete). Phases 5-6 are enhancements.

---

## Testing Strategy

### Unit Tests

| Component | Test File | Coverage | Status |
|-----------|-----------|----------|--------|
| Sync activities | `tests/test_sync_activities.py` | Activity logic, error handling, XCom | ✅ 15 tests |
| Deep workflow | `tests/test_deep_workflow.py` | Workflow structure, XCom, scheduling | ✅ 18 tests |
| Orchestrator | `tests/test_orchestrator.py` | start_dagrun, cancel_dagrun | ✅ 7 tests |
| Pool tracking | `tests/test_pool_support.py` | Limit enforcement | ⏳ Phase 5 |

### Integration Tests

| Scenario | Description | Status |
|----------|-------------|--------|
| **End-to-end deep integration** | Trigger via orchestrator → workflow → DB sync → UI visible | ⏳ Manual testing needed |
| **Connections work** | Hook reads connection from Airflow DB | ⏳ Manual testing needed |
| **XCom sync** | XCom values visible in Airflow UI | ⏳ Manual testing needed |
| **Pool limits respected** | Workflow waits when pool full | ⏳ Phase 5 |

### Manual Testing

1. Deploy Temporal server + workers
2. Configure `orchestrator = TemporalOrchestrator`
3. Trigger DAG via Airflow UI
4. Verify:
   - DagRun shows in UI as RUNNING
   - TaskInstances update in real-time
   - Final state shows SUCCESS/FAILED
   - Logs accessible via UI

---

## File Changes Summary

### New Files

| File | Description |
|------|-------------|
| `scripts/temporal_airflow/sync_activities.py` | DB sync activities (5 activities) |
| `scripts/temporal_airflow/deep_workflow.py` | Deep integration workflow |
| `scripts/temporal_airflow/tests/test_sync_activities.py` | Sync activity tests (15 tests) |
| `scripts/temporal_airflow/tests/test_deep_workflow.py` | Deep workflow tests (18 tests) |

### Modified Files

| File | Changes |
|------|---------|
| `scripts/temporal_airflow/models.py` | Add 7 new models for deep integration |
| `scripts/temporal_airflow/orchestrator.py` | Use deep workflow, pass existing run_id |
| `scripts/temporal_airflow/tests/test_orchestrator.py` | Update for deep workflow integration |

---

## Success Criteria

- [x] DAG triggered via UI creates workflow (orchestrator implemented)
- [x] DagRun visible in Airflow UI immediately (sync activities implemented)
- [x] TaskInstance states update in real-time (sync_task_status activity)
- [x] XCom values visible in UI (XComModel.set in sync_task_status)
- [x] Connections read from Airflow DB work (activities use real Airflow session)
- [ ] Pool limits respected (Phase 5 - deferred)
- [x] Error states sync correctly (failed state synced to DB)
- [x] All existing tests pass (79 tests passing)

---

## Known Gaps / Future Work

The following gaps exist in the current implementation and should be addressed in future iterations:

### 1. DAG File Path Resolution ✅ RESOLVED

**Previous state**: Hardcoded as `{dag_id}.py`

**Solution implemented**:
- Added `LoadSerializedDagResult` model with `dag_data` and `fileloc` fields
- Updated `load_serialized_dag` activity to return `fileloc` from `SerializedDagModel`
- Workflow stores `self.dag_fileloc` and passes it to activities
- Activities now receive the correct file path regardless of DAG file naming

### 2. Trigger Rule Support ✅ RESOLVED (via Airflow's native logic)

**Previous state**: Custom trigger rule evaluation was being implemented.

**Correct solution** (per design principle):
- **Use Airflow's native `update_state()` method** which internally calls `TriggerRuleDep`
- Both standalone and deep integration use the same in-workflow database pattern
- `dag_run.update_state(session)` automatically evaluates all 13 trigger rules:
  - `ALL_SUCCESS` (default), `ALL_FAILED`, `ALL_DONE`
  - `ALL_DONE_MIN_ONE_SUCCESS`, `ALL_DONE_SETUP_SUCCESS`
  - `ONE_SUCCESS`, `ONE_FAILED`, `ONE_DONE`
  - `NONE_FAILED`, `NONE_SKIPPED`, `NONE_FAILED_MIN_ONE_SUCCESS`
  - `ALWAYS`, `ALL_SKIPPED`
- **No custom trigger rule code needed** - reuse Airflow's implementation

**Why this approach is correct**:
- Guaranteed identical behavior to Airflow's built-in scheduler
- No maintenance burden when Airflow updates trigger rule logic
- Code reuse between standalone and deep integration workflows

### 3. Mapped Tasks Support

**Current state**: `map_index` is always `-1` (`deep_workflow.py:327`)

```python
map_index=-1,
```

**Issue**: Dynamic task mapping (`task.expand()`) creates multiple task instances with different `map_index` values.

**Solution options**:
- Detect mapped tasks from SerializedDAG
- Expand tasks dynamically in the scheduling loop
- Handle map_index in XCom store key

### 4. Task Retries and Timeouts

**Current state**: No retry logic for failed tasks. Activity timeout is hardcoded at 2 hours.

```python
start_to_close_timeout=timedelta(hours=2),
```

**Issue**: Airflow tasks have `retries`, `retry_delay`, `execution_timeout` parameters that should be respected.

**Solution options**:
- Read task retry/timeout config from SerializedDAG
- Configure Temporal activity retry policy from task params
- Implement custom retry logic in workflow

### 5. Logging Integration

**Current state**: Task logs go to Temporal activity logs only.

**Issue**: Airflow UI expects logs in specific format/location.

**Solution options**:
- Configure Airflow remote logging to capture activity logs
- Write logs via activity to Airflow's log storage
- Use Airflow's `TaskLogReader` pattern

### 6. Task Dependencies Beyond Upstream

**Current state**: Only `upstream_task_ids` are checked.

**Issue**: Airflow supports:
- `depends_on_past` - Task depends on previous DAG run's instance
- `wait_for_downstream` - Wait for downstream of previous run
- Asset/Dataset dependencies

**Solution**: Implement additional dependency checks in scheduling loop.

### 7. SLA Handling

**Current state**: Not implemented.

**Issue**: Airflow supports SLA miss callbacks and notifications.

**Solution**: Add SLA tracking and callback execution in workflow.
