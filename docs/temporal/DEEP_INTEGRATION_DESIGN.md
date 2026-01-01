# Deep Integration: Temporal Execution with Airflow UI/Database

**Date**: 2025-12-31
**Status**: Design Document
**Purpose**: Execute Airflow DAGs via Temporal while preserving full Airflow UI/Database experience

---

## Executive Summary

This document describes **Deep Integration Mode** - a deployment model where:
- **Temporal executes DAGs** (replaces scheduler's task-level scheduling and executors)
- **Airflow UI unchanged** (users see familiar interface)
- **Airflow Database preserved** (connections, variables, pools, status visibility)
- **Database is NOT execution driver** (only config store + status sink)

**Key Principle**: Temporal owns execution, Airflow owns user experience.

---

## Design Principles

1. **Temporal owns execution** - All scheduling decisions, retries, timeouts handled by Temporal workflows
2. **Airflow DB is a read model** - Status written for UI visibility, never read to drive execution
3. **Airflow DB stores configuration** - Connections, Variables, Pools read by activities
4. **UI unchanged** - Zero changes to Airflow webserver, users see familiar interface
5. **Gradual migration** - Can run alongside traditional executors

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AIRFLOW LAYER                                    │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────────┐    │
│  │  Airflow UI  │    │  Airflow API │    │  Scheduler (reduced)   │    │
│  │  (unchanged) │    │  (extended)  │    │  - Parses DAGs         │    │
│  │              │    │              │    │  - Serializes to DB    │    │
│  │  • View runs │    │  • Trigger → │────│→ NO DagRun creation    │    │
│  │  • View logs │    │    workflow  │    │  - NO task scheduling  │    │
│  └──────┬───────┘    └──────────────┘    └────────────────────────┘    │
│         │ read                                                          │
│         ▼                                                               │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                      AIRFLOW DATABASE                               ││
│  │                                                                     ││
│  │   CONFIG (read by workers)      │    STATUS (written by workflow)  ││
│  │  ┌─────────────┐ ┌───────────┐  │  ┌─────────────┐ ┌─────────────┐ ││
│  │  │ Connections │ │ Variables │  │  │  DagRun     │ │TaskInstance │ ││
│  │  │ (secrets)   │ │ (config)  │  │  │  (created   │ │  (created   │ ││
│  │  └─────────────┘ └───────────┘  │  │  by workflow│ │  by workflow│ ││
│  │  ┌─────────────┐ ┌───────────┐  │  └─────────────┘ └─────────────┘ ││
│  │  │   Pools     │ │Serialized │  │  ┌─────────────┐ ┌─────────────┐ ││
│  │  │ (limits)    │ │   DAGs    │  │  │    XCom     │ │   Logs      │ ││
│  │  └─────────────┘ └───────────┘  │  │  (results)  │ │ (metadata)  │ ││
│  └────────────────────────────────────└─────────────┘ └─────────────┘─┘│
└─────────────────────────────────────────────────────────────────────────┘
                    │                              ▲
                    │ read config                  │ write status (workflow creates records)
                    ▼                              │
┌─────────────────────────────────────────────────────────────────────────┐
│                        TEMPORAL LAYER                                    │
│  ┌────────────────────────────────────────────────────────────────────┐│
│  │                      TEMPORAL SERVER                                ││
│  │   • Durable workflow state (source of truth for execution)         ││
│  │   • Activity task queues                                           ││
│  │   • Retry/timeout management                                       ││
│  │   • Temporal UI (alternative monitoring)                           ││
│  └────────────────────────────────────────────────────────────────────┘│
│                              │                                          │
│  ┌───────────────────────────┴──────────────────────────────────────┐  │
│  │                      TEMPORAL WORKERS                             │  │
│  │                                                                   │  │
│  │  ExecuteAirflowDagWorkflow                                        │  │
│  │  ├─ FIRST: create_dagrun_record activity (creates DB records)    │  │
│  │  ├─ Deserialize DAG from input                                    │  │
│  │  ├─ Resolve task dependencies                                     │  │
│  │  ├─ Start activities for ready tasks                              │  │
│  │  ├─ Handle completions/failures                                   │  │
│  │  └─ Call sync_status activity → updates Airflow DB                │  │
│  │                                                                   │  │
│  │  create_dagrun_record Activity (FIRST activity)                   │  │
│  │  ├─ Creates DagRun record with state=RUNNING                      │  │
│  │  ├─ Creates TaskInstance records for all tasks                    │  │
│  │  └─ Returns run_id for subsequent activities                      │  │
│  │                                                                   │  │
│  │  run_airflow_task Activity                                        │  │
│  │  ├─ Load operator from DAG file
                        │  │
│  │  ├─ Hooks read connections from Airflow DB (standard path)        │  │
│  │  ├─ Execute operator.execute()                                    │  │
│  │  └─ Return result + XCom                                          │  │
│  │                                                                   │  │
│  │  sync_task_status Activity                                        │  │
│  │  └─ Update TaskInstance state in Airflow DB                       │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Comparison: Traditional vs Deep Integration

| Component | Traditional Airflow | Deep Integration |
|-----------|--------------------|-----------------------|
| **UI** | Airflow webserver | **Unchanged** |
| **Database** | Source of truth for execution | Config store + status sink |
| **Scheduler** | Parses DAGs + schedules tasks | Parses DAGs only (reduced role) |
| **Executor** | Distributes tasks to workers | **Replaced by Temporal** |
| **Workers** | Airflow workers execute tasks | Temporal workers run activities |
| **Task state machine** | DB-driven, scheduler-managed | Temporal workflow state |
| **Retries/timeouts** | Scheduler-managed | Temporal-managed |
| **Connections** | Stored in DB, read by hooks | **Unchanged** - hooks read from DB |
| **Variables** | Stored in DB | **Unchanged** - read from DB |
| **XCom** | Stored in DB | Workflow state + synced to DB |
| **Pools** | DB-enforced limits | Read from DB, enforced by workflow |

---

## Data Flow

### Flow 1: User Triggers DAG Run

```
1. User clicks "Trigger DAG" in Airflow UI
                    │
                    ▼
2. Airflow API starts Temporal workflow DIRECTLY
   - Passes dag_id, conf, logical_date
   - Workflow ID = "airflow-{dag_id}-{logical_date}"
   - NO database write yet!
                    │
                    ▼
3. Workflow's FIRST activity: create_dagrun_record
   - Creates DagRun in DB with state = RUNNING
   - Creates TaskInstance records for all tasks
   - Returns run_id for subsequent activities
                    │
                    ▼
4. Workflow executes tasks via activities
   - Each completion triggers sync_task_status
   - UI shows real-time updates
                    │
                    ▼
5. Workflow completes, calls sync_dagrun_status
   - Updates DagRun state = SUCCESS/FAILED
                    │
                    ▼
6. User sees final state in Airflow UI
```

**Why workflow-first?**
- **No orphan records**: If workflow fails to start, no stale DagRun in DB
- **Atomic**: Record creation is part of the durable workflow
- **Simpler API**: Just calls Temporal, no DB transaction + workflow coordination
- **Single source of truth**: Temporal owns execution from the very beginning

### Flow 2: Task Execution with Connections

```
Workflow identifies task ready for execution
                    │
                    ▼
Workflow starts run_airflow_task activity
                    │
                    ▼
Activity loads DAG file, extracts operator
                    │
                    ▼
Operator executes, hook needs connection
                    │
                    ▼
Hook calls BaseHook.get_connection("my_conn")
                    │
                    ▼
Airflow's connection lookup reads from DB
   - Uses AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
   - Standard Airflow path, zero changes needed!
                    │
                    ▼
Operator completes, returns result
                    │
                    ▼
Activity returns TaskExecutionResult to workflow
                    │
                    ▼
Workflow calls sync_task_status activity
   - Writes state to TaskInstance table
   - Writes XCom to xcom table
                    │
                    ▼
Airflow UI shows updated task state
```

### Flow 3: Schedule-Based Execution

```
1. Airflow Scheduler parses DAG files (unchanged)
   - Evaluates timetables/schedules
   - Determines next logical_date
                    │
                    ▼
2. Temporal Schedule Service (replaces scheduler's triggering)
   - Uses Temporal Schedules feature OR
   - Polls serialized_dag table for due schedules
   - Starts workflow directly (no DagRun created yet)
                    │
                    ▼
3. Workflow's FIRST activity: create_dagrun_record
   - Creates DagRun with run_type = scheduled
   - state = RUNNING
   - Returns run_id
                    │
                    ▼
4. (Same as Flow 1, step 4 onwards)
```

**Alternative: Temporal Native Schedules**
```
1. DAG schedule info stored in Temporal as a Schedule
   - Temporal handles cron evaluation natively
   - No need for Airflow scheduler for triggering
                    │
                    ▼
2. Temporal Schedule triggers workflow at scheduled time
   - Passes dag_id, logical_date from schedule
                    │
                    ▼
3. (Same as above - workflow creates DagRun)
```

---

## Implementation Components

### Component 1: Temporal Schedule Trigger Service

Evaluates DAG schedules and starts Temporal workflows directly (workflow creates DagRun):

```python
# airflow-core/src/airflow/executors/temporal_schedule_trigger.py

class TemporalScheduleTrigger:
    """
    Evaluates DAG schedules and starts Temporal workflows.

    Key design: Workflow creates the DagRun record, not this service.
    This ensures no orphan DagRuns if workflow fails to start.

    The scheduler still handles:
    - DAG file parsing
    - Serializing DAGs to database

    This service handles:
    - Schedule evaluation (which DAGs are due)
    - Starting Temporal workflows
    """

    def __init__(self):
        self.temporal_client: Client | None = None
        self.running: bool = False

    async def start(self):
        """Initialize and start the trigger service."""
        self.temporal_client = await create_temporal_client()
        self.running = True

        log.info("Temporal schedule trigger started")
        log.info(f"  Temporal: {self.temporal_client.service_client.config.target_host}")
        log.info(f"  Task queue: {conf.get('temporal', 'task_queue')}")

        await self._run_loop()

    async def _run_loop(self):
        """Main loop - check for due schedules."""
        task_queue = conf.get("temporal", "task_queue", fallback="airflow-tasks")

        while self.running:
            try:
                await self._process_due_schedules(task_queue)
            except Exception as e:
                log.exception(f"Error processing schedules: {e}")

            await asyncio.sleep(1)

    async def _process_due_schedules(self, task_queue: str):
        """Find DAGs due for execution and start workflows."""
        with create_session() as session:
            # Find active DAGs with schedules
            active_dags = session.query(SerializedDagModel).filter(
                SerializedDagModel.is_active == True,
            ).all()

            for dag_model in active_dags:
                if self._is_due(dag_model):
                    logical_date = self._calculate_logical_date(dag_model)
                    await self._start_workflow(
                        dag_model, logical_date, task_queue, run_type="scheduled"
                    )

    async def _start_workflow(
        self,
        dag_model: SerializedDagModel,
        logical_date: datetime,
        task_queue: str,
        run_type: str = "manual",
        conf: dict | None = None,
    ):
        """Start Temporal workflow - workflow will create DagRun."""
        # Workflow ID uses logical_date (run_id assigned by workflow)
        workflow_id = f"airflow-{dag_model.dag_id}-{logical_date.isoformat()}"

        # Check if workflow already exists (idempotency)
        # Temporal will reject duplicate workflow IDs

        input_data = DagExecutionInput(
            dag_id=dag_model.dag_id,
            logical_date=logical_date,
            serialized_dag=dag_model.data,
            conf=conf or {},
            run_type=run_type,
            # run_id is NOT set - workflow's first activity will generate it
        )

        await self.temporal_client.start_workflow(
            ExecuteAirflowDagWorkflow.run,
            input_data,
            id=workflow_id,
            task_queue=task_queue,
        )

        log.info(f"Started workflow {workflow_id} for {dag_model.dag_id}")
```

### Component 2: DagRun Creation Activity

The workflow's first activity creates the DagRun record:

```python
# scripts/temporal_airflow/activities.py (additions)

from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState, DagRunState


@dataclass
class CreateDagRunInput:
    """Input for creating DagRun record."""
    dag_id: str
    logical_date: datetime
    conf: dict
    run_type: str  # "manual", "scheduled", "backfill"
    external_trigger: bool = False


@dataclass
class CreateDagRunResult:
    """Result from creating DagRun record."""
    run_id: str
    dag_run_id: int


@activity.defn(name="create_dagrun_record")
async def create_dagrun_record(input: CreateDagRunInput) -> CreateDagRunResult:
    """
    Create DagRun and TaskInstance records in Airflow database.

    This is the FIRST activity called by the workflow, ensuring:
    - No orphan DagRuns if workflow fails to start
    - Temporal owns execution from the beginning
    - Airflow UI shows the run immediately after workflow starts
    """
    activity.logger.info(
        f"Creating DagRun for {input.dag_id} at {input.logical_date}"
    )

    with create_session() as session:
        # Generate run_id
        run_id = f"{input.run_type}__{input.logical_date.strftime('%Y-%m-%dT%H:%M:%S')}"

        # Create DagRun
        dag_run = DagRun(
            dag_id=input.dag_id,
            run_id=run_id,
            logical_date=input.logical_date,
            conf=input.conf,
            state=DagRunState.RUNNING,  # Already running!
            run_type=DagRunType(input.run_type),
            external_trigger=input.external_trigger,
            start_date=datetime.utcnow(),
        )
        session.add(dag_run)
        session.flush()  # Get the ID

        # Create TaskInstance records for all tasks in the DAG
        # (Workflow will update their states as they execute)
        serialized = SerializedDagModel.get(input.dag_id)
        if serialized:
            dag = serialized.dag
            for task in dag.tasks:
                ti = TaskInstance(
                    dag_id=input.dag_id,
                    task_id=task.task_id,
                    run_id=run_id,
                    map_index=-1,
                    state=TaskInstanceState.SCHEDULED,
                )
                session.add(ti)

        session.commit()

        activity.logger.info(f"Created DagRun {run_id} with ID {dag_run.id}")
        return CreateDagRunResult(run_id=run_id, dag_run_id=dag_run.id)
```

### Component 3: Status Sync Activities

Activities that write execution status back to Airflow DB:

```python
# scripts/temporal_airflow/activities.py (additions)


@dataclass
class TaskStatusSync:
    """Input for syncing task status to Airflow DB."""
    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    state: str
    start_date: datetime
    end_date: datetime | None
    duration: float | None
    try_number: int
    xcom_value: Any | None = None


@dataclass
class DagRunStatusSync:
    """Input for syncing DagRun status to Airflow DB."""
    dag_id: str
    run_id: str
    state: str
    start_date: datetime
    end_date: datetime | None


@activity.defn(name="sync_task_status")
async def sync_task_status(input: TaskStatusSync) -> None:
    """
    Write task execution status to Airflow database.

    This allows the Airflow UI to display real-time task status
    while Temporal owns the actual execution.
    """
    activity.logger.info(
        f"Syncing task status: {input.dag_id}.{input.task_id} -> {input.state}"
    )

    with create_session() as session:
        ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == input.dag_id,
            TaskInstance.task_id == input.task_id,
            TaskInstance.run_id == input.run_id,
            TaskInstance.map_index == input.map_index,
        ).first()

        if not ti:
            # Create TaskInstance if it doesn't exist
            # (workflow may have created tasks not yet in DB)
            ti = TaskInstance(
                dag_id=input.dag_id,
                task_id=input.task_id,
                run_id=input.run_id,
                map_index=input.map_index,
            )
            session.add(ti)

        # Update state
        ti.state = TaskInstanceState(input.state)
        ti.start_date = input.start_date
        ti.end_date = input.end_date
        ti.duration = input.duration
        ti.try_number = input.try_number

        # Store XCom if present
        if input.xcom_value is not None:
            from airflow.models import XCom
            XCom.set(
                key="return_value",
                value=input.xcom_value,
                dag_id=input.dag_id,
                task_id=input.task_id,
                run_id=input.run_id,
                map_index=input.map_index,
                session=session,
            )

        session.commit()


@activity.defn(name="sync_dagrun_status")
async def sync_dagrun_status(input: DagRunStatusSync) -> None:
    """
    Write DagRun status to Airflow database.

    Called when workflow starts and completes to keep
    Airflow UI in sync.
    """
    activity.logger.info(
        f"Syncing DagRun status: {input.dag_id}/{input.run_id} -> {input.state}"
    )

    with create_session() as session:
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == input.dag_id,
            DagRun.run_id == input.run_id,
        ).first()

        if dag_run:
            dag_run.state = DagRunState(input.state)
            dag_run.start_date = input.start_date
            dag_run.end_date = input.end_date
            session.commit()
```

### Component 4: Modified Workflow (Workflow-First Pattern)

The workflow creates the DagRun as its first action, then executes tasks:

```python
# scripts/temporal_airflow/workflows.py (modifications)

@workflow.defn(name="execute_airflow_dag", sandboxed=False)
class ExecuteAirflowDagWorkflow:

    @workflow.run
    async def run(self, input: DagExecutionInput) -> DagExecutionResult:
        # ... existing initialization ...

        # FIRST: Create DagRun record in Airflow DB
        # This ensures no orphan records - if we get here, workflow started successfully
        dagrun_result = await workflow.execute_activity(
            create_dagrun_record,
            CreateDagRunInput(
                dag_id=input.dag_id,
                logical_date=input.logical_date,
                conf=input.conf,
                run_type=input.run_type,
                external_trigger=input.run_type == "manual",
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Store run_id for use in subsequent activities
        self.run_id = dagrun_result.run_id

        # ... existing scheduling loop (unchanged) ...

        # On completion, sync final state
        final_state = "success" if self.tasks_failed == 0 else "failed"
        await self._sync_dagrun_status(final_state, self.start_date, workflow.now())

        return result

    async def _sync_dagrun_status(
        self,
        state: str,
        start_date: datetime,
        end_date: datetime | None,
    ):
        """Sync DagRun status to Airflow DB."""
        await workflow.execute_activity(
            sync_dagrun_status,
            DagRunStatusSync(
                dag_id=self.dag_id,
                run_id=self.run_id,
                state=state,
                start_date=start_date,
                end_date=end_date,
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )

    async def _handle_activity_completion(
        self,
        ti_key: tuple,
        result: TaskExecutionResult,
    ):
        """Handle task completion and sync to Airflow DB."""
        dag_id, task_id, run_id, map_index = ti_key

        # Update in-memory state (existing code)
        self._update_task_instance_state(ti_key, result)

        # Sync to Airflow DB for UI visibility
        await workflow.execute_activity(
            sync_task_status,
            TaskStatusSync(
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
                state=result.state,
                start_date=result.start_date,
                end_date=result.end_date,
                duration=result.duration,
                try_number=result.try_number,
                xcom_value=result.xcom_data,
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )
```

### Component 5: Extended API (Direct Workflow Trigger)

API starts workflow directly - workflow creates DagRun record:

```python
# airflow-core/src/airflow/api_fastapi/core_api/routes/dag_run.py (modifications)

@router.post("/dags/{dag_id}/dagRuns")
async def trigger_dag_run(
    dag_id: str,
    body: TriggerDAGRunPostBody,
    session: Session = Depends(get_session),
):
    """
    Trigger a new DAG run via Temporal workflow.

    Key design: We do NOT create DagRun in database here.
    The workflow's first activity creates the record, ensuring:
    - No orphan DagRuns if workflow fails to start
    - Temporal owns execution from the beginning
    """
    # Validate DAG exists
    dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
    if not dag_model:
        raise HTTPException(status_code=404, detail=f"DAG {dag_id} not found")

    # Get serialized DAG
    serialized = SerializedDagModel.get(dag_id)
    if not serialized:
        raise HTTPException(status_code=404, detail=f"Serialized DAG {dag_id} not found")

    logical_date = body.logical_date or utcnow()

    # Start Temporal workflow - workflow will create DagRun record
    try:
        temporal_client = await get_temporal_client()

        # Workflow ID uses logical_date for idempotency
        workflow_id = f"airflow-{dag_id}-{logical_date.isoformat()}"

        handle = await temporal_client.start_workflow(
            ExecuteAirflowDagWorkflow.run,
            DagExecutionInput(
                dag_id=dag_id,
                logical_date=logical_date,
                serialized_dag=serialized.data,
                conf=body.conf or {},
                run_type="manual",
                # run_id will be generated by create_dagrun_record activity
            ),
            id=workflow_id,
            task_queue=conf.get("temporal", "task_queue"),
        )

        # Return workflow info - DagRun record will be created by workflow
        return {
            "workflow_id": workflow_id,
            "dag_id": dag_id,
            "logical_date": logical_date,
            "message": "Workflow started. DagRun will appear in UI shortly.",
        }

    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"DAG run for {dag_id} at {logical_date} already exists"
        )
    except Exception as e:
        log.exception(f"Failed to start Temporal workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

### Component 6: Connection Access (No Changes Needed!)

**Key insight**: Airflow hooks already read from the database. Workers just need the database connection configured:

```python
# This already works - no code changes needed!

# In any operator
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("my_postgres_conn")
# ^ This reads from Airflow DB using AIRFLOW__DATABASE__SQL_ALCHEMY_CONN

# Variables work the same way
from airflow.models import Variable
api_key = Variable.get("my_api_key")
# ^ This reads from Airflow DB
```

Worker environment configuration:
```bash
# Workers need database access for connections/variables
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql://user:pass@host/airflow"

# And Temporal connection
export TEMPORAL_ADDRESS="localhost:7233"
export TEMPORAL_TASK_QUEUE="airflow-tasks"

# Start worker
python scripts/temporal_airflow/worker.py
```

---

## Configuration

### Airflow Configuration

```ini
# airflow.cfg

[temporal]
# Enable Temporal integration
enabled = True

# Temporal server address
host = localhost:7233

# Temporal namespace
namespace = default

# Task queue for Airflow workflows
task_queue = airflow-tasks

# Directly trigger workflows from API (vs waiting for trigger service)
direct_trigger = True

# For Temporal Cloud:
# host = my-namespace.tmprl.cloud:7233
# api_key = <your-api-key>
# tls_enabled = True
```

### Environment Variables

```bash
# Temporal configuration (SDK envconfig)
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_TASK_QUEUE=airflow-tasks

# For Temporal Cloud
export TEMPORAL_ADDRESS=my-namespace.tmprl.cloud:7233
export TEMPORAL_API_KEY=your-api-key

# Airflow database (for connections/variables)
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://user:pass@host/airflow

# DAGs folder
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
```

---

## Deployment Architecture

### Option A: Minimal Changes (Trigger Service)

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Airflow         │     │ Airflow         │     │ Temporal        │
│ Webserver       │     │ Scheduler       │     │ Trigger Service │
│ (unchanged)     │     │ (unchanged)     │     │ (new)           │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │ read                  │ write DagRuns         │ start workflows
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow Database                          │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 │ read config, write status
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Temporal Server                           │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Temporal Workers                          │
│  • ExecuteAirflowDagWorkflow                                     │
│  • run_airflow_task, sync_task_status, sync_dagrun_status        │
└─────────────────────────────────────────────────────────────────┘
```

### Option B: Direct API Integration

```
┌─────────────────┐     ┌─────────────────┐
│ Airflow         │     │ Airflow         │
│ Webserver       │     │ Scheduler       │
│ (unchanged)     │     │ (unchanged)     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │ API triggers          │ scheduled runs create
         │ start workflow        │ DagRuns, trigger service
         │ directly              │ starts workflows
         ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow Database                          │
│                              +                                   │
│                   Temporal Trigger Service                       │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                    (Same as Option A below)
```

---

## Benefits

### Operational Benefits

| Benefit | Description |
|---------|-------------|
| **Familiar UX** | Users keep the Airflow UI they know |
| **Existing secrets work** | No migration of connections/variables |
| **Better reliability** | Temporal's durability guarantees |
| **Simpler workers** | No Celery/Redis/Flower complexity |
| **Better debugging** | Temporal UI shows complete workflow history |
| **Gradual migration** | Can run both modes side by side |

### Technical Benefits

| Benefit | Description |
|---------|-------------|
| **Durable execution** | Workflows survive worker crashes |
| **Built-in retries** | Temporal handles retry logic |
| **Activity timeouts** | Per-task timeout enforcement |
| **Workflow replay** | Debug by replaying execution |
| **No DB contention** | Execution doesn't poll database |
| **Horizontal scaling** | Add workers without coordination |

---

## Challenges and Mitigations

### Challenge 1: Two Sources of Truth

**Issue**: Temporal owns execution state, Airflow DB shows status (eventual consistency)

**Mitigation**:
- Sync activities called immediately after state changes
- Temporal is authoritative for "what's running"
- Airflow DB is authoritative for "what happened" (audit)

### Challenge 2: Worker Configuration

**Issue**: Workers need both Temporal and Airflow DB access

**Mitigation**:
- Single environment configuration
- Same database connection as traditional Airflow
- Add Temporal connection (just host:port)

### Challenge 3: Scheduler Still Needed

**Issue**: DAG parsing and schedule management still require scheduler

**Mitigation**:
- Scheduler scope reduced (no task scheduling)
- Could eventually use Temporal Schedules for cron
- DAG parsing could move to workers (future)

### Challenge 4: Pool Support

**Issue**: Pools limit concurrent tasks, currently DB-enforced

**Mitigation**:
- Workflow reads pool limits from DB
- Implements slot counting in workflow state
- Activity acquires/releases slots

### Challenge 5: Dataset/Sensor Integration

**Issue**: Dataset triggers and sensors rely on DB polling

**Mitigation**:
- Dataset: Could use Temporal signals when datasets update
- Sensors: Run as activities with heartbeat
- Long-term: Native Temporal patterns for waiting

---

## Implementation Phases

| Phase | Work | Effort | Dependencies |
|-------|------|--------|--------------|
| **Phase 1** | Add create_dagrun_record activity (workflow-first pattern) | Small | None |
| **Phase 2** | Add sync activities (sync_task_status, sync_dagrun_status) | Small | Phase 1 |
| **Phase 3** | Modify workflow to call create_dagrun first, then sync activities | Small | Phase 2 |
| **Phase 4** | Create Temporal Schedule Trigger Service | Medium | Phase 3 |
| **Phase 5** | Extend API for direct workflow trigger (no DB write) | Small | Phase 3 |
| **Phase 6** | Add Airflow configuration section | Small | Phase 4 |
| **Phase 7** | Pool support (read limits, enforce in workflow) | Medium | Phase 4 |
| **Phase 8** | Documentation and migration guide | Medium | Phase 6 |
| **Phase 9** | Dataset trigger integration | Large | Phase 8 |
| **Phase 10** | Sensor as durable activity pattern | Large | Phase 8 |

---

## Migration Path

### From Traditional Airflow to Deep Integration

**Step 1: Deploy Temporal Infrastructure**
- Start Temporal server (or use Temporal Cloud)
- Deploy Temporal workers with Airflow activities

**Step 2: Enable Trigger Service**
- Deploy Temporal Trigger Service
- Configure to watch specific DAGs (gradual rollout)

**Step 3: Migrate DAGs Gradually**
- Tag DAGs for Temporal execution
- Trigger service only processes tagged DAGs
- Monitor in both Airflow UI and Temporal UI

**Step 4: Validate and Expand**
- Verify task states sync correctly
- Verify connections/variables work
- Expand to more DAGs

**Step 5: Full Migration**
- All DAGs on Temporal
- Disable traditional executor
- Keep scheduler for DAG parsing only

---

## Comparison: Standalone vs Deep Integration

| Aspect | Standalone Mode | Deep Integration |
|--------|-----------------|------------------|
| **UI** | Temporal UI only | Airflow UI (unchanged) |
| **Database** | None (or optional) | Required (config + status) |
| **Scheduler** | Not needed | Required (DAG parsing) |
| **Connections** | Passed at submit time | Read from Airflow DB |
| **Variables** | Passed at submit time | Read from Airflow DB |
| **Triggering** | CLI/API script | Airflow UI/API/Schedule |
| **Status visibility** | Temporal UI | Airflow UI |
| **Migration effort** | New deployment | Incremental |
| **Use case** | New projects, simple needs | Existing Airflow deployments |

---

## Conclusion

Deep Integration mode provides the best of both worlds:
- **User experience**: Familiar Airflow UI and workflows
- **Execution reliability**: Temporal's proven durability
- **Operational simplicity**: Remove Celery/Redis/Kubernetes executor complexity
- **Gradual adoption**: Migrate DAGs incrementally

The key architectural insight is separating concerns:
- **Airflow**: User interface, configuration storage, status display
- **Temporal**: Execution engine, durability, retry management

This separation allows teams to leverage Temporal's superior execution guarantees while maintaining the Airflow experience their users expect.
