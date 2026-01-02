# Deep Integration Implementation Plan

**Date**: 2025-01-01
**Status**: Implementation Plan
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

### What Deep Integration Adds

The standalone workflow uses an **in-memory SQLite database** per workflow. Deep integration changes this to:

1. **Write to Airflow DB** - So UI shows real-time status
2. **Read config from Airflow DB** - Connections, variables, pools
3. **Orchestrator-triggered** - Works with existing UI/API/CLI

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

**Goal**: Create a separate workflow for deep integration that uses real Airflow DB.

#### 2.1 Create New Workflow File

```python
# scripts/temporal_airflow/deep_workflow.py

@workflow.defn(name="execute_airflow_dag_deep", sandboxed=False)
class ExecuteAirflowDagDeepWorkflow:
    """
    Temporal workflow for deep integration mode.

    Unlike the standalone workflow, this:
    - Uses real Airflow DB (not in-memory SQLite)
    - Creates DagRun/TaskInstance via activities
    - Syncs status back to Airflow DB for UI visibility
    - Reads connections/variables from Airflow DB
    """

    @workflow.run
    async def run(self, input: DeepDagExecutionInput) -> DagExecutionResult:
        set_time_provider(workflow.now)

        try:
            # Create or verify DagRun in Airflow DB
            if not input.run_id:
                result = await workflow.execute_activity(
                    create_dagrun_record,
                    CreateDagRunInput(
                        dag_id=input.dag_id,
                        logical_date=input.logical_date,
                        conf=input.conf,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                )
                self.run_id = result.run_id
            else:
                # DagRun already exists (orchestrator created it)
                self.run_id = input.run_id

            # Load serialized DAG from Airflow DB
            dag_data = await workflow.execute_activity(
                load_serialized_dag,
                LoadSerializedDagInput(dag_id=input.dag_id),
                start_to_close_timeout=timedelta(seconds=30),
            )
            self.dag = SerializedDAG.from_dict(dag_data)

            # Execute scheduling loop with status sync
            final_state = await self._scheduling_loop()

            # Sync final state
            await workflow.execute_activity(
                sync_dagrun_status,
                DagRunStatusSync(
                    dag_id=input.dag_id,
                    run_id=self.run_id,
                    state=final_state,
                    end_date=workflow.now(),
                ),
                start_to_close_timeout=timedelta(seconds=30),
            )

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

| Phase | Description | Effort | Dependencies |
|-------|-------------|--------|--------------|
| **1** | DB Sync Activities | Medium | None |
| **2** | Workflow Mode Config | Medium | Phase 1 |
| **3** | Orchestrator Integration | Small | Phase 2 |
| **4** | Activity Task Mode | Small | Phase 1 |
| **5** | Pool Support | Medium | Phase 2 |
| **6** | Airflow Configuration | Small | None |

**Recommended order**: 1 → 2 → 3 → 4 → 5 → 6

Phases 1-4 are the core implementation. Phases 5-6 are enhancements.

---

## Testing Strategy

### Unit Tests

| Component | Test File | Coverage |
|-----------|-----------|----------|
| Sync activities | `test_sync_activities.py` | Activity logic, error handling |
| Workflow modes | `test_workflow_modes.py` | Standalone vs deep integration |
| Pool tracking | `test_pool_support.py` | Limit enforcement |

### Integration Tests

| Scenario | Description |
|----------|-------------|
| **End-to-end deep integration** | Trigger via orchestrator → workflow → DB sync → UI visible |
| **Connections work** | Hook reads connection from Airflow DB |
| **XCom sync** | XCom values visible in Airflow UI |
| **Pool limits respected** | Workflow waits when pool full |

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
| `scripts/temporal_airflow/sync_activities.py` | DB sync activities |
| `airflow-core/tests/unit/temporal/test_sync_activities.py` | Activity tests |

### Modified Files

| File | Changes |
|------|---------|
| `scripts/temporal_airflow/models.py` | Add sync input/result models |
| `scripts/temporal_airflow/workflows.py` | Add mode configuration, sync calls |
| `scripts/temporal_airflow/activities.py` | Add deep integration mode |
| `scripts/temporal_airflow/orchestrator.py` | Set deep_integration=True |
| `scripts/temporal_airflow/client_config.py` | Read from Airflow config |

---

## Success Criteria

- [ ] DAG triggered via UI creates workflow
- [ ] DagRun visible in Airflow UI immediately
- [ ] TaskInstance states update in real-time
- [ ] XCom values visible in UI
- [ ] Connections read from Airflow DB work
- [ ] Pool limits respected
- [ ] Error states sync correctly
- [ ] All existing tests pass
