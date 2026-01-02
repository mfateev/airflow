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

### Core Architectural Decision: In-Workflow Database + Sync Activities

Deep integration follows the **same design as standalone mode** with one key addition:

1. **In-Workflow Database**: Each workflow has its own in-memory SQLite database with real Airflow models (DagRun, TaskInstance, etc.)
2. **Reuse Airflow's Native Logic**: All scheduling decisions use Airflow's built-in code:
   - `dag_run.update_state()` - evaluates trigger rules via TriggerRuleDep
   - `dag_run.verify_integrity()` - creates TaskInstance records
   - `dag_run.schedule_tis()` - schedules tasks
3. **Sync Activities**: Write state from in-workflow DB to real Airflow DB for UI visibility

```
┌─────────────────────────────────────────────────────────────────┐
│  Temporal Workflow                                               │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  In-Workflow Database (SQLite in-memory)                   │  │
│  │  ├── DagRun (real Airflow model)                          │  │
│  │  ├── TaskInstance (real Airflow model)                    │  │
│  │  └── Uses Airflow's native: update_state(), TriggerRuleDep│  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                              │ sync_task_status() activity       │
│                              ▼                                   │
└──────────────────────────────┼───────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  Airflow Database (PostgreSQL/MySQL)                             │
│  ├── DagRun (synced for UI visibility)                          │
│  ├── TaskInstance (synced for UI visibility)                    │
│  ├── Connections (read by hooks during task execution)          │
│  └── Variables (read by operators during task execution)        │
└─────────────────────────────────────────────────────────────────┘
```

**Why this design?**
- **Correctness**: Reusing Airflow's trigger rule logic guarantees identical behavior to the built-in scheduler
- **Maintainability**: No need to reimplement and maintain separate scheduling logic
- **Consistency**: Both standalone and deep integration use the same workflow code path

### What Changes Where

| Layer | Component | Change Required |
|-------|-----------|-----------------|
| **Airflow Core** | `DagRunType` enum | Add `EXTERNAL` value (~3 lines) |
| **Airflow Core** | Scheduler queries | Filter out `EXTERNAL` runs (~4 lines) |
| **Airflow Core** | DagRun Orchestrator (future) | Pluggable extension point (~140 lines) |
| **Temporal** | Workflow | `ExecuteAirflowDagWorkflow` - orchestrates DAG execution |
| **Temporal** | Activities | DB sync, task execution, DagRun creation |
| **Temporal** | Trigger Service | Starts workflows for scheduled DAGs |

### Implementation Approaches

**Minimal Approach (~7 lines Airflow changes):**
- Add `DagRunType.EXTERNAL` + scheduler filter
- External Trigger Service polls for due DAGs and starts workflows
- Workflows write status back to Airflow DB

**Full Integration (~140 lines Airflow changes):**
- Add pluggable DagRun Orchestrator extension point
- `TemporalOrchestrator` routes all DagRuns to Temporal workflows
- No external Trigger Service needed - works with existing UI/API/CLI

---

## Table of Contents

1. [Design Principles](#design-principles)
2. [Architecture Overview](#architecture-overview)
3. **[Part A: Airflow Changes](#part-a-airflow-changes)**
   - [Minimal Changes: DagRunType.EXTERNAL](#minimal-changes-dagruntype-external)
   - [Extension Point: Pluggable DagRun Orchestrator](#extension-point-pluggable-dagrun-orchestrator)
4. **[Part B: Temporal Components](#part-b-temporal-components)**
   - [Workflow: ExecuteAirflowDagWorkflow](#workflow-executeairflowdagworkflow)
   - [Activity: create_dagrun_record](#activity-create_dagrun_record)
   - [Activity: sync_task_status / sync_dagrun_status](#activity-sync_task_status--sync_dagrun_status)
   - [Activity: run_airflow_task](#activity-run_airflow_task)
   - [Trigger Service](#trigger-service)
5. [Data Flow](#data-flow)
6. [Configuration](#configuration)
7. [Deployment Architecture](#deployment-architecture)
8. [Implementation Phases](#implementation-phases)
9. [Benefits](#benefits)
10. [Challenges and Mitigations](#challenges-and-mitigations)
11. [Migration Path](#migration-path)
12. [Appendix: Executor Pattern Background](#appendix-executor-pattern-background)
13. [Appendix: Executor Delegation (Future)](#appendix-executor-delegation-future)
14. [Appendix: HTTP API Approach (Future)](#appendix-http-api-approach-future)

---

## Design Principles

1. **Temporal owns execution** - All scheduling decisions, retries, timeouts handled by Temporal workflows
2. **Airflow DB is a read model** - Status written for UI visibility, never read to drive execution
3. **Airflow DB stores configuration** - Connections, Variables, Pools read by activities
4. **UI unchanged** - Zero changes to Airflow webserver, users see familiar interface
5. **Gradual migration** - Can run alongside traditional executors
6. **Direct DB access** - Activities import Airflow models and write directly to database (no REST API limitations)

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
                    │ read config                  │ write status
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
│  │  ├─ FIRST: create_dagrun_record activity                         │  │
│  │  ├─ Deserialize DAG, resolve dependencies                        │  │
│  │  ├─ Execute tasks via run_airflow_task activity                  │  │
│  │  └─ Sync status via sync_task_status activity                    │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Comparison: Traditional vs Deep Integration

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

---

# Part A: Airflow Changes

This section describes all changes required in the Airflow codebase.

---

## Minimal Changes: DagRunType.EXTERNAL

**Total: ~7 lines of code**

These changes allow external systems to create DagRuns that the scheduler ignores.

### Change 1: Add EXTERNAL to DagRunType Enum (~3 lines)

```python
# airflow/utils/types.py

class DagRunType(str, enum.Enum):
    """Class with DagRun types."""
    BACKFILL_JOB = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    ASSET_TRIGGERED = "asset_triggered"
    EXTERNAL = "external"  # ← ADD THIS: Externally managed (Temporal, etc.)
```

### Change 2: Filter External Runs from Scheduler (~4 lines)

```python
# airflow/models/dagrun.py

# In get_running_dag_runs_to_examine() method (line ~595):
query = (
    select(cls)
    .where(cls.state == DagRunState.RUNNING)
    .where(cls.run_type != DagRunType.EXTERNAL)  # ← ADD THIS
    # ... rest of query
)

# In get_queued_dag_runs_to_set_running() method (line ~635):
# Same filter added
```

### What These Changes Enable

- External systems create DagRuns with `run_type='external'`
- Airflow scheduler completely ignores these runs
- Temporal has full control over execution and state management

### What These Changes Do NOT Provide

- No hook/extension point for starting workflows when DAGs are triggered
- Still need external Trigger Service, modified API, or pluggable orchestrator

---

## Extension Point: Pluggable DagRun Orchestrator

**Total: ~140 lines of code**

This is a cleaner long-term solution that provides an extension point in Airflow itself, similar to pluggable executors.

### Problem: No Extension Point

When a user triggers a DAG (UI, API, CLI, or schedule), Airflow:
1. Creates a `DagRun` record directly in the database
2. Scheduler picks it up and manages task execution via the configured executor

**There's no hook to intercept DagRun creation and route it to an external orchestrator.**

Existing listener hooks (`on_dag_run_running`, `on_dag_run_success`, etc.) only fire on **state changes**, not on creation.

### Solution: System-Wide Pluggable Orchestrator

Configure the orchestrator system-wide in `airflow.cfg`:

```ini
[core]
# System-wide orchestrator selection (like executor)
orchestrator = TemporalOrchestrator
# OR: orchestrator = DefaultOrchestrator (current behavior)
```

### Base Interface (~40 lines)

```python
# airflow/orchestrators/base_orchestrator.py

from abc import ABC, abstractmethod
from airflow.models import DagRun

class BaseDagRunOrchestrator(ABC):
    """
    Base class for DagRun orchestrators.

    Orchestrators control how DagRuns are executed. The default orchestrator
    uses Airflow's scheduler + executor pattern. Alternative orchestrators
    route execution to external systems like Temporal.

    Configured system-wide via [core] orchestrator setting.
    """

    @abstractmethod
    def start_dagrun(self, dag_run: DagRun) -> None:
        """
        Start orchestrating a DagRun.

        Called when a DagRun is created/triggered. The orchestrator is
        responsible for managing the execution of all tasks in the DAG.
        """
        pass

    @abstractmethod
    def cancel_dagrun(self, dag_run: DagRun) -> None:
        """Cancel a running DagRun."""
        pass


class DefaultOrchestrator(BaseDagRunOrchestrator):
    """Default orchestrator - uses Airflow scheduler + executor."""

    def start_dagrun(self, dag_run: DagRun) -> None:
        # No-op: Scheduler will pick up the DagRun automatically
        pass

    def cancel_dagrun(self, dag_run: DagRun) -> None:
        dag_run.set_state(DagRunState.FAILED)
```

### Orchestrator Loader (~20 lines)

```python
# airflow/orchestrators/orchestrator_loader.py

_orchestrator: BaseDagRunOrchestrator | None = None

def get_orchestrator() -> BaseDagRunOrchestrator:
    """Get the configured orchestrator (singleton)."""
    global _orchestrator
    if _orchestrator is None:
        orchestrator_name = conf.get("core", "orchestrator", fallback="DefaultOrchestrator")
        _orchestrator = import_string(ORCHESTRATOR_CLASSES[orchestrator_name])()
    return _orchestrator
```

### Integration Point (~5 lines)

```python
# airflow/models/dagrun.py (or API layer)

def create_dagrun(...) -> DagRun:
    dag_run = DagRun(...)
    session.add(dag_run)
    session.flush()

    # NEW: Start via configured orchestrator
    orchestrator = get_orchestrator()
    orchestrator.start_dagrun(dag_run)

    return dag_run
```

### TemporalOrchestrator Implementation (~60 lines)

```python
# airflow/providers/temporal/orchestrators/temporal_orchestrator.py

class TemporalOrchestrator(BaseDagRunOrchestrator):
    """Routes ALL DagRun execution to Temporal workflows."""

    def __init__(self):
        self.client: Client | None = None

    async def start_dagrun(self, dag_run: DagRun) -> None:
        """Start Temporal workflow for this DagRun."""
        if not self.client:
            self.client = await create_temporal_client()

        # Mark as EXTERNAL so scheduler ignores it
        dag_run.run_type = DagRunType.EXTERNAL

        await self.client.start_workflow(
            ExecuteAirflowDagWorkflow.run,
            DagExecutionInput(
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                logical_date=dag_run.logical_date,
                conf=dag_run.conf,
            ),
            id=f"airflow-{dag_run.dag_id}-{dag_run.run_id}",
            task_queue=conf.get("temporal", "task_queue"),
        )

    async def cancel_dagrun(self, dag_run: DagRun) -> None:
        """Cancel Temporal workflow."""
        handle = self.client.get_workflow_handle(
            f"airflow-{dag_run.dag_id}-{dag_run.run_id}"
        )
        await handle.cancel()
```

### Benefits of Orchestrator Extension Point

| Aspect | Minimal Approach | Orchestrator Extension |
|--------|------------------|------------------------|
| **Airflow changes** | ~7 lines | ~140 lines |
| **External components** | Trigger Service required | None needed |
| **Works with UI/API/CLI** | Requires workarounds | Yes, natively |
| **Configuration** | External service config | Single Airflow setting |
| **Future extensibility** | Limited | Per-DAG routing possible |

---

# Part B: Temporal Components

This section describes all Temporal-side components (no Airflow code changes).

---

## Workflow: ExecuteAirflowDagDeepWorkflow

The deep integration workflow uses an **in-workflow database** and reuses Airflow's native scheduling logic,
with sync activities to mirror state to the real Airflow DB for UI visibility.

### Architecture: In-Workflow Database

Deep integration uses the **same architecture as standalone mode**:

```python
# scripts/temporal_airflow/deep_workflow.py

@workflow.defn(name="execute_airflow_dag_deep", sandboxed=False)
class ExecuteAirflowDagDeepWorkflow:

    def __init__(self):
        # In-workflow database (same as standalone)
        self.engine = None
        self.sessionFactory = None
        self.dag = None
        self.xcom_store: dict[tuple, Any] = {}

    def _initialize_database(self):
        """Initialize workflow-specific in-memory SQLite database."""
        workflow_id = workflow.info().workflow_id
        conn_str = f"sqlite:///file:memdb_{workflow_id}?mode=memory&cache=shared&uri=true"
        self.engine = create_engine(conn_str, poolclass=StaticPool, ...)
        self.sessionFactory = sessionmaker(bind=self.engine, ...)
        Base.metadata.create_all(self.engine)

    @workflow.run
    async def run(self, input: DeepDagExecutionInput) -> DagExecutionResult:
        # 1. Initialize in-workflow database
        self._initialize_database()

        # 2. Load serialized DAG from Airflow DB (via activity)
        dag_data = await workflow.execute_activity(
            load_serialized_dag,
            LoadSerializedDagInput(dag_id=input.dag_id),
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.dag = SerializedDAG.from_dict(dag_data.dag_data)
        self.dag_fileloc = dag_data.fileloc

        # 3. Create DagRun in BOTH databases
        # First: Create in real Airflow DB (if not exists)
        create_result = await workflow.execute_activity(
            create_dagrun_record,
            CreateDagRunInput(dag_id=input.dag_id, logical_date=input.logical_date, ...),
        )
        self.run_id = create_result.run_id

        # Then: Create in in-workflow DB (for Airflow's scheduling logic)
        self._create_local_dag_run(...)

        # 4. Ensure TaskInstances exist in real Airflow DB
        await workflow.execute_activity(ensure_task_instances, ...)

        # 5. Main scheduling loop (uses Airflow's native logic)
        final_state = await self._scheduling_loop(...)

        # 6. Sync final state
        await workflow.execute_activity(sync_dagrun_status, ...)
        return result
```

### Key Design: Reuse Airflow's Native Scheduling Logic

The scheduling loop uses Airflow's built-in methods instead of custom logic:

```python
async def _scheduling_loop(self, dag_run_id: int) -> str:
    """Uses Airflow's native scheduling logic."""

    session = self.sessionFactory()
    dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
    dag_run.dag = self.dag

    # CRITICAL: Use Airflow's native update_state() method
    # This internally uses TriggerRuleDep to evaluate trigger rules!
    schedulable_tis, callback = dag_run.update_state(
        session=session,
        execute_callbacks=False,
    )

    # Airflow's native schedule_tis() marks tasks as QUEUED
    if schedulable_tis:
        dag_run.schedule_tis(schedulable_tis, session=session)

    # Start activities for schedulable tasks
    for ti in schedulable_tis:
        # Sync task state to real Airflow DB BEFORE starting activity
        await workflow.execute_activity(sync_task_status, TaskStatusSync(
            dag_id=ti.dag_id, task_id=ti.task_id, run_id=self.run_id,
            state=ti.state.value, start_date=workflow.now(),
        ))

        # Start task execution activity
        handle = workflow.start_activity(run_airflow_task, ...)
```

### Comparison: Standalone vs Deep Integration

| Aspect | Standalone | Deep Integration |
|--------|------------|------------------|
| **In-workflow database** | ✅ Yes | ✅ Yes (same design) |
| **Uses update_state()** | ✅ Yes | ✅ Yes (same logic) |
| **Uses TriggerRuleDep** | ✅ Yes (via update_state) | ✅ Yes (via update_state) |
| **DAG source** | Passed in workflow input | Loaded from Airflow DB via activity |
| **Connections/Variables** | Passed in workflow input | Read from real Airflow DB by hooks |
| **State visibility** | Workflow state only | Synced to real Airflow DB |
| **Airflow UI** | Not visible | Full visibility |

**The only difference**: Deep integration adds sync activities to write state to the real Airflow DB,
and loads DAG/connections from the real DB instead of workflow input.

### Why Reuse Airflow's Native Logic?

#### Trigger Rules in Airflow

Airflow supports 13 different trigger rules that determine when a task should run:

| Trigger Rule | Description |
|--------------|-------------|
| `all_success` | (Default) Run when all upstream tasks succeed |
| `all_failed` | Run when all upstream tasks fail |
| `all_done` | Run when all upstream tasks complete (any state) |
| `all_skipped` | Run when all upstream tasks are skipped |
| `one_success` | Run when at least one upstream succeeds |
| `one_failed` | Run when at least one upstream fails |
| `one_done` | Run when at least one upstream completes |
| `none_failed` | Run when no upstream tasks failed (skipped OK) |
| `none_failed_min_one_success` | None failed and at least one succeeded |
| `none_skipped` | Run when no upstream tasks are skipped |
| `always` | Run regardless of upstream state |
| `no_trigger` | Externally triggered only |

#### How the Built-in Scheduler Handles Trigger Rules

The Airflow scheduler evaluates trigger rules through a dependency chain:

```
Scheduler Loop
     │
     ▼
dag_run.update_state(session)
     │
     ▼
TaskInstance.evaluate_dep_context()
     │
     ▼
TriggerRuleDep.get_dep_statuses()
     │
     ├─ Counts upstream states (success, failed, skipped, etc.)
     ├─ Applies trigger rule logic
     └─ Returns DepStatus (met/not met)
```

The key class is `TriggerRuleDep` (located in `airflow/ti_deps/deps/trigger_rule_dep.py`).
This class:
1. Queries upstream task states
2. Counts how many are in each state
3. Applies the trigger rule logic
4. Returns whether the dependency is met

#### Why NOT Reimplement Trigger Rule Logic

Reimplementing trigger rules in the Temporal workflow would be:

1. **Error-prone**: 13 rules with subtle edge cases
2. **Maintenance burden**: Must track Airflow changes
3. **Inconsistent**: Could behave differently than Airflow scheduler
4. **Unnecessary**: Airflow's code already handles this perfectly

#### How We Reuse Airflow's Logic

By using an in-workflow database with real Airflow models:

```python
# In the workflow scheduling loop:
dag_run.update_state(session=session)  # ← This calls TriggerRuleDep internally!
```

`dag_run.update_state()` internally:
1. Iterates through all TaskInstances
2. Calls `ti._get_dep_statuses()` which includes `TriggerRuleDep`
3. Returns tasks that are now schedulable

**Result**: Deep integration gets identical trigger rule behavior to Airflow's built-in scheduler
with zero custom logic required.

---

## Activity: create_dagrun_record

Creates DagRun and TaskInstance records in Airflow database:

```python
# scripts/temporal_airflow/activities.py

@dataclass
class CreateDagRunInput:
    dag_id: str
    logical_date: datetime
    conf: dict

@dataclass
class CreateDagRunResult:
    run_id: str
    dag_run_id: int

@activity.defn(name="create_dagrun_record")
async def create_dagrun_record(input: CreateDagRunInput) -> CreateDagRunResult:
    """
    Create DagRun and TaskInstance records in Airflow database.

    Uses DIRECT DATABASE ACCESS via Airflow models:
    - No REST API limitations
    - Can set any state including RUNNING
    - Uses run_type='external' so scheduler ignores this run
    """
    with create_session() as session:
        run_id = f"external__{input.logical_date.strftime('%Y-%m-%dT%H:%M:%S')}"

        dag_run = DagRun(
            dag_id=input.dag_id,
            run_id=run_id,
            logical_date=input.logical_date,
            conf=input.conf,
            state=DagRunState.RUNNING,
            run_type=DagRunType.EXTERNAL,  # ← Scheduler ignores this
            start_date=datetime.utcnow(),
        )
        session.add(dag_run)
        session.flush()

        # Create TaskInstance records for all tasks
        serialized = SerializedDagModel.get(input.dag_id, session=session)
        if serialized:
            for task in serialized.dag.tasks:
                ti = TaskInstance(
                    dag_id=input.dag_id,
                    task_id=task.task_id,
                    run_id=run_id,
                    state=TaskInstanceState.SCHEDULED,
                )
                session.add(ti)

        session.commit()
        return CreateDagRunResult(run_id=run_id, dag_run_id=dag_run.id)
```

---

## Activity: sync_task_status / sync_dagrun_status

Write execution status back to Airflow DB for UI visibility:

```python
# scripts/temporal_airflow/activities.py

@dataclass
class TaskStatusSync:
    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    state: str
    start_date: datetime
    end_date: datetime | None
    xcom_value: Any | None = None

@activity.defn(name="sync_task_status")
async def sync_task_status(input: TaskStatusSync) -> None:
    """Write task status to Airflow database for UI visibility."""
    with create_session() as session:
        ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == input.dag_id,
            TaskInstance.task_id == input.task_id,
            TaskInstance.run_id == input.run_id,
        ).first()

        if ti:
            ti.state = TaskInstanceState(input.state)
            ti.start_date = input.start_date
            ti.end_date = input.end_date

            if input.xcom_value is not None:
                XCom.set(
                    key="return_value",
                    value=input.xcom_value,
                    dag_id=input.dag_id,
                    task_id=input.task_id,
                    run_id=input.run_id,
                    session=session,
                )

            session.commit()


@dataclass
class DagRunStatusSync:
    dag_id: str
    run_id: str
    state: str
    end_date: datetime | None

@activity.defn(name="sync_dagrun_status")
async def sync_dagrun_status(input: DagRunStatusSync) -> None:
    """Write DagRun status to Airflow database."""
    with create_session() as session:
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == input.dag_id,
            DagRun.run_id == input.run_id,
        ).first()

        if dag_run:
            dag_run.state = DagRunState(input.state)
            dag_run.end_date = input.end_date
            session.commit()
```

---

## Activity: run_airflow_task

Executes actual Airflow operators:

```python
# scripts/temporal_airflow/activities.py

@activity.defn(name="run_airflow_task")
async def run_airflow_task(input: ActivityTaskInput) -> TaskExecutionResult:
    """
    Execute an Airflow task operator.

    Hooks read connections from Airflow DB automatically via
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN environment variable.
    """
    # Load DAG and extract operator
    dag = deserialize_dag(input.serialized_dag)
    task = dag.get_task(input.task_id)

    # Execute operator
    result = task.execute(context=build_context(input))

    return TaskExecutionResult(
        state="success",
        xcom_data=result,
        end_date=datetime.utcnow(),
    )
```

**Key insight**: No changes needed for connections/variables - hooks already read from Airflow DB.

---

## Trigger Service

Evaluates DAG schedules and starts Temporal workflows (only needed with minimal approach):

```python
# scripts/temporal_airflow/trigger_service.py

class TemporalScheduleTrigger:
    """
    Evaluates DAG schedules and starts Temporal workflows.

    Only needed when NOT using the pluggable orchestrator approach.
    With TemporalOrchestrator, this service is unnecessary.
    """

    async def _run_loop(self):
        while self.running:
            await self._process_due_schedules()
            await asyncio.sleep(1)

    async def _process_due_schedules(self):
        with create_session() as session:
            active_dags = session.query(SerializedDagModel).filter(
                SerializedDagModel.is_active == True,
            ).all()

            for dag_model in active_dags:
                if self._is_due(dag_model):
                    await self._start_workflow(dag_model)

    async def _start_workflow(self, dag_model, logical_date):
        workflow_id = f"airflow-{dag_model.dag_id}-{logical_date.isoformat()}"

        await self.temporal_client.start_workflow(
            ExecuteAirflowDagWorkflow.run,
            DagExecutionInput(
                dag_id=dag_model.dag_id,
                logical_date=logical_date,
                serialized_dag=dag_model.data,
            ),
            id=workflow_id,
            task_queue=conf.get("temporal", "task_queue"),
        )
```

---

## Data Flow

### Flow 1: User Triggers DAG Run (Deep Integration)

```
1. User clicks "Trigger DAG" in Airflow UI
                    │
                    ▼
2. API/Orchestrator starts Temporal workflow
   - Passes dag_id, conf, logical_date, run_id (if exists)
   - Workflow ID = "airflow-{dag_id}-{run_id}"
                    │
                    ▼
3. Workflow initializes IN-WORKFLOW DATABASE
   - Creates in-memory SQLite with Airflow schema
   - Same pattern as standalone workflow
                    │
                    ▼
4. Activity: load_serialized_dag
   - Loads DAG from SerializedDagModel in real Airflow DB
   - Returns serialized dict + fileloc
                    │
                    ▼
5. Activity: create_dagrun_record (if needed)
   - Creates DagRun in REAL Airflow DB with run_type=EXTERNAL
   - Returns run_id for syncing
                    │
                    ▼
6. Workflow creates DagRun in IN-WORKFLOW DB
   - Uses Airflow's DagRun model directly
   - verify_integrity() creates TaskInstances
   - This enables Airflow's native scheduling logic
                    │
                    ▼
7. SCHEDULING LOOP (using Airflow's native logic)
   ┌─────────────────────────────────────────────────┐
   │ dag_run.update_state(session)                   │
   │    ├─ Evaluates TriggerRuleDep for each task    │
   │    └─ Returns schedulable_tis                   │
   │                                                 │
   │ dag_run.schedule_tis(schedulable_tis)          │
   │    └─ Marks tasks as QUEUED in in-workflow DB   │
   │                                                 │
   │ For each schedulable task:                      │
   │    ├─ sync_task_status activity → Real DB       │
   │    └─ run_airflow_task activity → Execute       │
   │                                                 │
   │ On task completion:                             │
   │    ├─ Update in-workflow DB                     │
   │    └─ sync_task_status activity → Real DB       │
   └─────────────────────────────────────────────────┘
                    │
                    ▼
8. Workflow completes
   - sync_dagrun_status activity → Real DB
   - Airflow UI shows SUCCESS/FAILED
```

### Flow 2: Scheduling Logic (In-Workflow Database)

```
┌───────────────────────────────────────────────────────────────┐
│  IN-WORKFLOW DATABASE (SQLite in-memory)                       │
│                                                                │
│  ┌─────────────────┐     ┌──────────────────────────────────┐ │
│  │    DagRun       │     │        TaskInstances              │ │
│  │  state=RUNNING  │     │  task_a: SUCCESS                  │ │
│  │                 │     │  task_b: SUCCESS                  │ │
│  │                 │     │  task_c: NONE (waiting)           │ │
│  └─────────────────┘     └──────────────────────────────────┘ │
│                                     │                          │
│  dag_run.update_state(session)      │                          │
│           │                         │                          │
│           ▼                         ▼                          │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  TriggerRuleDep.get_dep_statuses() for task_c            │ │
│  │                                                          │ │
│  │  task_c.trigger_rule = "all_success"                     │ │
│  │  upstream = [task_a, task_b]                             │ │
│  │  upstream_states = {SUCCESS: 2, FAILED: 0}               │ │
│  │                                                          │ │
│  │  RESULT: Dependencies MET → task_c is schedulable!       │ │
│  └──────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
                              │
                              │ sync_task_status activity
                              ▼
┌───────────────────────────────────────────────────────────────┐
│  REAL AIRFLOW DATABASE (PostgreSQL/MySQL)                      │
│                                                                │
│  TaskInstance for task_c: state = QUEUED (synced for UI)       │
└───────────────────────────────────────────────────────────────┘
```

### Flow 3: Task Execution with Connections

```
Workflow starts run_airflow_task activity
                    │
                    ▼
Activity loads DAG from file (using dag_fileloc)
                    │
                    ▼
Operator executes, hook needs connection
                    │
                    ▼
Hook calls BaseHook.get_connection("my_conn")
                    │
                    ▼
Airflow's connection lookup reads from REAL DB
   (standard path, zero changes needed!)
                    │
                    ▼
Operator completes, activity returns result
                    │
                    ▼
Workflow updates IN-WORKFLOW DB
                    │
                    ▼
Workflow calls sync_task_status activity → REAL DB
```

---

## Configuration

### Airflow Configuration

```ini
# airflow.cfg

[core]
# For orchestrator approach (recommended)
orchestrator = TemporalOrchestrator

[temporal]
enabled = True
host = localhost:7233
namespace = default
task_queue = airflow-tasks

# For Temporal Cloud:
# host = my-namespace.tmprl.cloud:7233
# api_key = <your-api-key>
```

### Worker Environment Variables

```bash
# Temporal configuration
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_TASK_QUEUE=airflow-tasks

# Airflow database (for connections/variables)
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://user:pass@host/airflow

# DAGs folder
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags

# Start worker
python scripts/temporal_airflow/worker.py
```

---

## Deployment Architecture

### With Orchestrator Extension (Recommended)

```
┌─────────────────┐     ┌─────────────────┐
│ Airflow         │     │ Airflow         │
│ Webserver       │     │ Scheduler       │
│ (unchanged)     │     │ (DAG parsing)   │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │ UI triggers →         │ schedules trigger →
         │ orchestrator          │ orchestrator
         ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  Orchestrator: TemporalOrchestrator                              │
│  → Starts Temporal workflow for each DagRun                      │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Temporal Server                           │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  Temporal Workers                                                │
│  • ExecuteAirflowDagWorkflow                                     │
│  • Activities: create_dagrun, run_task, sync_status              │
└─────────────────────────────────────────────────────────────────┘
```

### With Minimal Approach + Trigger Service

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Airflow         │     │ Airflow         │     │ Temporal        │
│ Webserver       │     │ Scheduler       │     │ Trigger Service │
│ (unchanged)     │     │ (DAG parsing)   │     │ (NEW component) │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │                       │                       │ polls for
         │                       │                       │ due schedules
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow Database                          │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
                    (Same Temporal stack below)
```

---

## Implementation Phases

### Phase 0: Airflow Core Changes (PR to Airflow)

| Change | Effort | Lines |
|--------|--------|-------|
| Add `DagRunType.EXTERNAL` | Tiny | ~3 |
| Add scheduler filter | Tiny | ~4 |
| `BaseDagRunOrchestrator` interface | Small | ~40 |
| `orchestrator_loader.py` | Small | ~20 |
| **Total** | **Small** | **~70** |

### Phase 1: Deep Workflow Foundation

Create `ExecuteAirflowDagDeepWorkflow` following standalone pattern:

| Component | Description | Effort |
|-----------|-------------|--------|
| In-workflow database | SQLite in-memory with Airflow models | Copy from standalone |
| `_initialize_database()` | Create workflow-specific DB | Copy from standalone |
| `_create_local_dag_run()` | Create DagRun in in-workflow DB | Copy from standalone |
| Reuse `update_state()` | Airflow's native trigger rule evaluation | No custom code |
| Reuse `schedule_tis()` | Airflow's native task scheduling | No custom code |

### Phase 2: Sync Activities

Activities to sync in-workflow state to real Airflow DB:

| Activity | Description | Effort |
|----------|-------------|--------|
| `load_serialized_dag` | Load DAG from SerializedDagModel | Small |
| `create_dagrun_record` | Create DagRun in real Airflow DB | Small |
| `ensure_task_instances` | Create TaskInstance records in real DB | Small |
| `sync_task_status` | Sync TaskInstance state to real DB | Small |
| `sync_dagrun_status` | Sync DagRun state to real DB | Small |

### Phase 3: Integration with Orchestrator

| Component | Description | Effort |
|-----------|-------------|--------|
| `TemporalOrchestrator` | Routes DagRun creation to Temporal workflow | Small |
| Input model | `DeepDagExecutionInput` (dag_id, run_id, logical_date, conf) | Small |
| Worker configuration | Support Airflow DB connection | Small |

### Phase 4: Feature Parity

| Feature | How Handled | Effort |
|---------|-------------|--------|
| Trigger rules | Via `update_state()` → TriggerRuleDep | ✅ Already works |
| XCom | Store in workflow state + sync to real DB | Small |
| Pools | Read limits from real DB, enforce in workflow | Medium |
| Task retries | Temporal retry policy | Small |

### Design Principle: Maximize Code Reuse

```
┌────────────────────────────────────────────────────────────────┐
│  Standalone Workflow (workflows.py)                             │
│  ├── _initialize_database()                                    │
│  ├── _create_dag_run()                                         │
│  ├── _scheduling_loop()                                        │
│  │      └── dag_run.update_state() ← Airflow's native logic    │
│  └── _handle_activity_result()                                 │
└────────────────────────────────────────────────────────────────┘
                              │
                              │ Copy/share these methods
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  Deep Workflow (deep_workflow.py)                               │
│  ├── _initialize_database()     (same as standalone)           │
│  ├── _create_local_dag_run()    (same as standalone)           │
│  ├── _scheduling_loop()         (same + sync activities)       │
│  │      └── dag_run.update_state() ← Same native logic!        │
│  ├── _handle_activity_result()  (same + sync activities)       │
│  └── NEW: Sync activities to write to real Airflow DB          │
└────────────────────────────────────────────────────────────────┘
```

**Key insight**: The deep workflow should look almost identical to standalone,
just with sync activities added at key state transition points.

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

### Technical Benefits

| Benefit | Description |
|---------|-------------|
| **Durable execution** | Workflows survive worker crashes |
| **Built-in retries** | Temporal handles retry logic |
| **Activity timeouts** | Per-task timeout enforcement |
| **No DB contention** | Execution doesn't poll database |
| **Horizontal scaling** | Add workers without coordination |

---

## Challenges and Mitigations

| Challenge | Mitigation |
|-----------|------------|
| **Two sources of truth** | Sync activities called immediately; Temporal authoritative for running state |
| **Worker configuration** | Single environment config; same DB connection as traditional Airflow |
| **Scheduler still needed** | Reduced scope (DAG parsing only); could use Temporal Schedules for cron |
| **Pool support** | Workflow reads limits from DB, enforces in workflow state |
| **Dataset/Sensor integration** | Could use Temporal signals; sensors run as activities with heartbeat |

---

## Migration Path

1. **Deploy Temporal Infrastructure** - Start Temporal server or use Temporal Cloud
2. **Deploy Temporal Workers** - With Airflow activities
3. **Enable for specific DAGs** - Gradual rollout with tagging
4. **Validate** - Verify status sync, connections work
5. **Full migration** - All DAGs on Temporal, disable traditional executor

---

## Appendix: Executor Pattern Background

Traditional Airflow executors use a **pull-based event buffer pattern**:

```python
class BaseExecutor:
    def __init__(self):
        self.event_buffer: dict[TaskInstanceKey, tuple[state, info]] = {}

    def sync(self) -> None:
        """Called by scheduler to poll workers."""
        pass

    def success(self, key: TaskInstanceKey):
        """Store success in buffer (doesn't write to DB)."""
        self.event_buffer[key] = (TaskInstanceState.SUCCESS, info)
```

The scheduler processes events and writes to DB. For `DagRunType.EXTERNAL` runs, the scheduler is bypassed entirely, so Temporal activities can write directly to the database.

---

## Appendix: Executor Delegation (Future)

For gradual migration, Temporal activities can delegate task execution to existing Airflow executors (Celery, Kubernetes, Local), allowing teams to reuse existing worker infrastructure while Temporal handles DAG orchestration.

### Why Delegate to Executors?

| Scenario | Benefit |
|----------|---------|
| **Existing Celery cluster** | Reuse workers, queues, routing rules |
| **Kubernetes executor** | Reuse pod templates, resource configs |
| **Gradual migration** | Change orchestration without changing execution |
| **Specialized workers** | Keep task-specific worker pools |

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Temporal Worker                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  ExecutorActivityManager                                   │  │
│  │  ├─ Shared executor instance (Celery/K8s/Local)           │  │
│  │  ├─ Background sync loop (polls executor)                 │  │
│  │  └─ Pending futures (routes completions to activities)    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                              │ queue_command()                   │
│                              ▼                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Airflow Executor (CeleryExecutor, KubernetesExecutor)    │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  Existing Infrastructure                                         │
│  • Celery workers + Redis/RabbitMQ broker                       │
│  • Kubernetes pods                                              │
│  • Local subprocess pool                                        │
└─────────────────────────────────────────────────────────────────┘
```

### Implementation Pattern

The key challenge is that executors use a **poll-based async pattern** (scheduler calls `sync()` and reads `event_buffer`), while activities want to block until completion. Solution: shared executor with background sync loop that routes completions to waiting activities via futures.

```python
class ExecutorActivityManager:
    """Manages shared Airflow executor for Temporal activities."""

    def __init__(self, executor_type: str | None = None):
        self.executor_type = executor_type
        self._executor = None
        self._pending: dict[TaskInstanceKey, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._job: Job | None = None

    def start(self):
        """Initialize executor (call once at worker startup)."""
        # Load configured executor
        if self.executor_type:
            self._executor = ExecutorLoader.load_executor(self.executor_type)
        else:
            self._executor = ExecutorLoader.get_default_executor()

        # Create real job record for visibility in Airflow
        with create_session() as session:
            self._job = Job(
                job_type="TemporalWorker",
                state=JobState.RUNNING,
                hostname=socket.getfqdn(),
            )
            session.add(self._job)
            session.commit()
            self._executor.job_id = self._job.id

        self._executor.start()

        # Start background loops
        self._sync_task = asyncio.create_task(self._sync_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _sync_loop(self):
        """Background loop polling executor for completions."""
        while True:
            async with self._lock:
                self._executor.sync()
                events = self._executor.get_event_buffer()

                # Route completions to waiting activities
                for key, (state, info) in events.items():
                    if key in self._pending:
                        self._pending[key].set_result((state, info))
                        del self._pending[key]

            await asyncio.sleep(0.5)

    async def _heartbeat_loop(self):
        """Update job heartbeat so Airflow knows worker is alive."""
        while True:
            with create_session() as session:
                job = session.query(Job).get(self._job.id)
                if job:
                    job.latest_heartbeat = timezone.utcnow()
                    session.commit()
            await asyncio.sleep(30)

    @activity.defn(name="run_task_via_executor")
    async def run_task(self, input: ActivityTaskInput) -> TaskExecutionResult:
        """Execute task via configured executor, wait for completion."""
        key = TaskInstanceKey(
            dag_id=input.dag_id,
            task_id=input.task_id,
            run_id=input.run_id,
            try_number=input.try_number,
            map_index=input.map_index,
        )

        command = ["airflow", "tasks", "run", input.dag_id, input.task_id,
                   input.logical_date.isoformat(), "--local", "--raw"]

        # Create future for completion notification
        future = asyncio.get_event_loop().create_future()

        async with self._lock:
            self._pending[key] = future
            self._executor.queue_command(key, command, priority=1, queue=input.queue)

        # Wait for sync loop to resolve our future
        while not future.done():
            await asyncio.sleep(2)
            activity.heartbeat()

        state, info = future.result()
        return TaskExecutionResult(state=state.value, info=info)
```

### Executor Compatibility

| Executor | Compatibility | Notes |
|----------|---------------|-------|
| **LocalExecutor** | ✅ Full | Subprocess pool, works seamlessly |
| **CeleryExecutor** | ✅ Full | Submits to existing Celery workers |
| **KubernetesExecutor** | ✅ Full | Creates pods via K8s API |
| **SequentialExecutor** | ✅ Full | Synchronous, trivial |
| **Custom executors** | ⚠️ Varies | Depends on implementation |

### Benefits

| Benefit | Description |
|---------|-------------|
| **Zero worker changes** | Existing Celery/K8s workers unchanged |
| **Reuse infrastructure** | Keep existing queues, routing, pod templates |
| **Visibility** | Temporal workers appear in Airflow job table |
| **Gradual migration** | Change orchestration layer without disruption |

### When to Use

- **Use executor delegation** when you have significant investment in executor infrastructure (Celery routing rules, K8s pod templates, specialized worker pools)
- **Use direct execution** (default) for simpler deployments or when you want Temporal to fully own the execution environment

---

## Appendix: HTTP API Approach (Future)

While direct DB access is recommended for initial implementation, an HTTP-based approach could provide better decoupling.

### Current REST API Limitations

| Operation | Status | Limitation |
|-----------|--------|------------|
| Create DagRun | ✅ Works | Cannot set `run_type=external` |
| Set DagRun → RUNNING | ❌ Blocked | Enum excludes RUNNING |
| Create TaskInstance | ❌ No endpoint | Endpoint doesn't exist |

### Required Changes (~70 lines)

1. Allow `run_type` in POST /dagRuns
2. Allow RUNNING in PATCH /dagRuns
3. Allow RUNNING/SCHEDULED in PATCH /taskInstances
4. Add POST /taskInstances endpoint

**Recommendation**: Start with direct DB access, consider HTTP API when decoupling becomes important.

---

## Conclusion

Deep Integration provides the best of both worlds:
- **User experience**: Familiar Airflow UI and workflows
- **Execution reliability**: Temporal's proven durability
- **Operational simplicity**: Remove Celery/Redis/Kubernetes executor complexity

**Implementation options**:
1. **Minimal approach** (~7 lines Airflow) + Trigger Service
2. **Orchestrator extension** (~140 lines Airflow) - cleaner, works with existing UI/API/CLI

The key architectural insight is separating concerns:
- **Airflow**: User interface, configuration storage, status display
- **Temporal**: Execution engine, durability, retry management
