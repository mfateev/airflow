"""Temporal workflow for executing Airflow DAGs with deep integration."""
from __future__ import annotations

import asyncio
import weakref
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ApplicationError, ActivityError

# Pass through Airflow imports to avoid sandbox reloading issues
with workflow.unsafe.imports_passed_through():
    import pendulum  # Must be imported first to avoid metaclass conflicts
    from airflow.models.dagrun import DagRun, DagRunState
    from airflow.models.dag_version import DagVersion
    from airflow.models.taskinstance import TaskInstance, TaskInstanceState
    from airflow.models.trigger import Trigger  # Required for TaskInstance foreign key
    from airflow.models.tasklog import LogTemplate  # Required for DagRun foreign key
    from airflow.serialization.serialized_objects import SerializedDAG
    from airflow._shared.timezones import timezone as airflow_timezone
    from airflow.utils.time_provider import set_time_provider, clear_time_provider
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool
    from sqlalchemy.orm import sessionmaker
    from airflow.models import Base

from temporal_airflow.models import (
    DeepDagExecutionInput,
    DagExecutionResult,
    DagExecutionFailureDetails,
    ActivityTaskInput,
    TaskExecutionResult,
    TaskExecutionFailureDetails,
    CreateDagRunInput,
    TaskStatusSync,
    BatchTaskStatusSync,
    DagRunStatusSync,
    LoadSerializedDagInput,
    EnsureTaskInstancesInput,
)
from temporal_airflow.activities import run_airflow_task
from temporal_airflow.sync_activities import (
    create_dagrun_record,
    sync_task_status,
    sync_task_status_batch,
    sync_dagrun_status,
    load_serialized_dag,
    ensure_task_instances,
)


@workflow.defn(name="execute_airflow_dag_deep", sandboxed=False)
class ExecuteAirflowDagDeepWorkflow:
    """
    Temporal workflow for deep integration mode.

    Design: Same as standalone workflow + sync activities.

    SAME as standalone workflow:
    - Uses in-workflow database (in-memory SQLite)
    - Uses Airflow's native scheduling logic (update_state, TriggerRuleDep)
    - Creates real DagRun/TaskInstance models in workflow DB

    ADDED for deep integration:
    - Loads DAG from real Airflow DB (via activity)
    - Syncs status to real Airflow DB (via activities)
    - Connections/variables read from real DB by hooks

    Memory cleanup on cache eviction:
        Uses weakref.finalize to register engine.dispose() callback when workflow
        instance is garbage collected (happens when evicted from Temporal's sticky
        cache). This prevents in-memory SQLite databases from leaking memory.

        See _initialize_database() for implementation.
    """

    def __init__(self):
        """Initialize workflow state."""
        # In-workflow database (same as standalone)
        self.engine = None
        self.sessionFactory = None

        # DAG state
        self.dag = None
        self.dag_fileloc: str | None = None

        # Deep integration state
        self.run_id: str | None = None

        # XCom state in workflow (for passing to activities)
        self.xcom_store: dict[tuple, Any] = {}  # ti_key -> xcom_data

        # Task tracking
        self.tasks_succeeded: int = 0
        self.tasks_failed: int = 0

    def _initialize_database(self):
        """
        Initialize workflow-specific in-memory database.

        Each workflow execution gets a fresh database. We use workflow_id + run_id
        to create unique databases per execution. The run_id changes on continue-as-new
        but stays the same during replay of the same execution.

        IMPORTANT: We drop all tables first to handle replay scenarios where
        the in-memory database may have stale data from a previous replay attempt
        within the same worker process.
        """
        workflow_id = workflow.info().workflow_id
        run_id = workflow.info().run_id
        # Use both workflow_id and run_id for unique database per execution
        db_name = f"memdb_{workflow_id}_{run_id}".replace("-", "_")
        conn_str = f"sqlite:///file:{db_name}?mode=memory&cache=shared&uri=true"

        # Create workflow-specific engine (no global state!)
        self.engine = create_engine(
            conn_str,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )

        # Register cleanup callback for when workflow is garbage collected
        # (happens when evicted from Temporal's sticky cache)
        # NOTE: The callback must NOT reference self to avoid preventing GC
        def _dispose_engine(engine, db_name):
            engine.dispose()
            # workflow.logger is not available here, uncomment for debugging:
            # print(f"[CLEANUP] Disposed engine for {db_name}")

        self._db_cleanup = weakref.finalize(self, _dispose_engine, self.engine, db_name)

        # Create workflow-specific session factory
        self.sessionFactory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        # Create only the tables we need (not all Airflow tables)
        # This avoids the deadlock from creating hundreds of tables
        # Order matters for foreign key dependencies
        required_tables = [
            LogTemplate.__table__,  # DagRun.log_template_id FK
            DagVersion.__table__,   # DagRun.dag_version_id FK
            Trigger.__table__,      # TaskInstance.trigger_id FK
            DagRun.__table__,
            TaskInstance.__table__,
        ]

        # Drop and recreate only required tables to ensure fresh state on replay
        for table in required_tables:
            table.drop(self.engine, checkfirst=True)
        for table in required_tables:
            table.create(self.engine, checkfirst=True)

        workflow.logger.info(f"Database initialized for workflow {workflow_id} (run_id={run_id})")

    def _create_local_dag_run(
        self,
        dag_id: str,
        run_id: str,
        logical_date: datetime,
        conf: dict | None,
    ) -> int:
        """
        Create DagRun and TaskInstances in IN-WORKFLOW database.

        SAME AS STANDALONE: Uses workflow-specific SessionFactory.
        This enables Airflow's native scheduling logic to work.

        Note: The database is always fresh (dropped/recreated in _initialize_database)
        so we don't need to check for existing records.
        """
        session = self.sessionFactory()
        try:
            # Create DagVersion for this execution
            dag_version = DagVersion(
                dag_id=dag_id,
                version_number=1,
            )
            session.add(dag_version)
            session.flush()

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
            dag_run.created_dag_version = dag_version  # Link to version

            session.add(dag_run)
            session.flush()

            # Create TaskInstances using Airflow's native method
            dag_run.verify_integrity(session=session, dag_version_id=dag_version.id)

            session.commit()

            workflow.logger.info(
                f"Created local DagRun: {dag_run.id} with {len(dag_run.task_instances)} tasks"
            )
            return dag_run.id
        finally:
            session.close()

    def _get_upstream_xcom(self, ti: TaskInstance, task) -> dict[str, Any] | None:
        """
        Gather XCom values from upstream tasks.

        SAME AS STANDALONE: XCom stored in workflow state.
        """
        if not task.upstream_task_ids:
            return None

        upstream_results = {}
        for upstream_task_id in task.upstream_task_ids:
            upstream_key = (ti.dag_id, upstream_task_id, ti.run_id, ti.map_index)
            if upstream_key in self.xcom_store:
                upstream_results[upstream_task_id] = self.xcom_store[upstream_key]

        return upstream_results if upstream_results else None

    @workflow.run
    async def run(self, input: DeepDagExecutionInput) -> DagExecutionResult:
        """
        Execute the DAG with deep integration to Airflow DB.

        Args:
            input: Workflow input with dag_id, logical_date, optional run_id

        Returns:
            DagExecutionResult with final state and statistics
        """
        workflow.logger.info(
            f"Starting deep integration workflow: {input.dag_id} "
            f"(run_id={input.run_id or 'will be created'})"
        )

        # Set Temporal's deterministic time provider for Airflow code
        set_time_provider(workflow.now)

        start_time = workflow.now()

        try:
            # Phase 1: Initialize IN-WORKFLOW database (same as standalone)
            self._initialize_database()

            # Phase 2: Load serialized DAG from REAL Airflow DB (deep integration)
            dag_result = await workflow.execute_activity(
                load_serialized_dag,
                LoadSerializedDagInput(dag_id=input.dag_id),
                start_to_close_timeout=timedelta(seconds=30),
            )
            # Note: from_dict() is fast (<1ms) when imports are pre-warmed
            # See deep_worker.py _prewarm_imports() which warms up deserialization
            self.dag = SerializedDAG.from_dict(dag_result.dag_data)
            self.dag_fileloc = dag_result.fileloc
            workflow.logger.info(
                f"Loaded DAG: {self.dag.dag_id} with {len(self.dag.task_dict)} tasks "
                f"(fileloc={self.dag_fileloc})"
            )

            # Phase 3: Create/verify DagRun in REAL Airflow DB (deep integration)
            if input.run_id:
                # DagRun already exists (e.g., created by orchestrator)
                self.run_id = input.run_id
                workflow.logger.info(f"Using existing DagRun: {self.run_id}")

                # Ensure TaskInstances exist in real DB
                await workflow.execute_activity(
                    ensure_task_instances,
                    EnsureTaskInstancesInput(
                        dag_id=input.dag_id,
                        run_id=self.run_id,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                )
            else:
                # Create new DagRun in real Airflow DB
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
                workflow.logger.info(f"Created DagRun in real DB: {self.run_id}")

            # Phase 4: Create DagRun in IN-WORKFLOW database (same as standalone)
            # This enables Airflow's native scheduling logic (update_state, TriggerRuleDep)
            dag_run_id = self._create_local_dag_run(
                dag_id=input.dag_id,
                run_id=self.run_id,
                logical_date=input.logical_date,
                conf=input.conf,
            )

            # Phase 5: Execute scheduling loop with native Airflow logic + sync
            final_state = await self._scheduling_loop(dag_run_id)

            end_time = workflow.now()

            # Phase 6: Sync final state to REAL Airflow DB (deep integration)
            await workflow.execute_activity(
                sync_dagrun_status,
                DagRunStatusSync(
                    dag_id=input.dag_id,
                    run_id=self.run_id,
                    state=final_state,
                    end_date=end_time,
                ),
                start_to_close_timeout=timedelta(seconds=30),
            )

            # If DAG failed, raise ApplicationError
            if final_state == "failed":
                failure_details = DagExecutionFailureDetails(
                    dag_id=input.dag_id,
                    run_id=self.run_id,
                    start_date=start_time,
                    end_date=end_time,
                    tasks_succeeded=self.tasks_succeeded,
                    tasks_failed=self.tasks_failed,
                    error_message=f"DAG execution failed: {self.tasks_failed} task(s) failed",
                )
                raise ApplicationError(
                    f"DAG execution failed: {input.dag_id} / {self.run_id}",
                    failure_details,
                    type="DagExecutionFailure",
                    non_retryable=True,
                )

            return DagExecutionResult(
                state=final_state,
                dag_id=input.dag_id,
                run_id=self.run_id,
                start_date=start_time,
                end_date=end_time,
                tasks_succeeded=self.tasks_succeeded,
                tasks_failed=self.tasks_failed,
            )

        finally:
            clear_time_provider()

    async def _update_local_task_state(self, ti_key: tuple, result: TaskExecutionResult):
        """
        Update TaskInstance in IN-WORKFLOW DB based on activity result.

        This only updates the local in-memory DB. Sync to real Airflow DB
        is done via batched sync_task_status_batch activity.
        """
        session = self.sessionFactory()
        try:
            ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti_key[0],
                TaskInstance.task_id == ti_key[1],
                TaskInstance.run_id == ti_key[2],
                TaskInstance.map_index == ti_key[3],
            ).one()

            # Update in in-workflow DB (same as standalone)
            ti.state = result.state
            ti.start_date = result.start_date
            ti.end_date = result.end_date

            session.commit()

            workflow.logger.info(
                f"Updated local task {ti_key} to state {ti.state} "
                f"(duration: {result.end_date - result.start_date})"
            )
        finally:
            session.close()

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        """
        Main scheduling loop using Airflow's native logic.

        SAME AS STANDALONE: Uses dag_run.update_state() which internally
        calls TriggerRuleDep for trigger rule evaluation.

        DEEP INTEGRATION ADDITION: Syncs state to real Airflow DB.

        Returns:
            Final DAG run state ("success" or "failed")
        """
        # Track running activities: ti_key -> ActivityHandle
        running_activities: dict[tuple, Any] = {}
        final_state: str | None = None

        max_iterations = 100

        for iteration in range(max_iterations):
            workflow.logger.info(
                f"Scheduling loop iteration {iteration + 1}: "
                f"{len(running_activities)} activities running"
            )

            session = self.sessionFactory()
            try:
                dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
                dag_run.dag = self.dag  # Restore DAG reference

                # Check if complete
                if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                    workflow.logger.info(f"DAG completed: {dag_run.state}")
                    final_state = dag_run.state.value if hasattr(dag_run.state, 'value') else str(dag_run.state)
                    break

                # CRITICAL: Use Airflow's native update_state() method
                # This internally uses TriggerRuleDep to evaluate trigger rules!
                schedulable_tis, callback = dag_run.update_state(
                    session=session,
                    execute_callbacks=False,
                )

                # Commit state changes
                session.commit()

                # Start activities for new schedulable tasks
                if schedulable_tis:
                    dag_run.schedule_tis(schedulable_tis, session=session)
                    session.commit()

                    # NOTE: We skip syncing QUEUED state to reduce round-trips.
                    # Tasks go directly from None → Running → Success/Failed in Airflow UI.
                    # The completion sync (after task finishes) provides the important state.

                    # Start all task activities
                    for ti in schedulable_tis:
                        ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)

                        # Gather upstream XCom
                        task = self.dag.get_task(ti.task_id)
                        upstream_results = self._get_upstream_xcom(ti, task)

                        activity_queue = workflow.info().task_queue

                        workflow.logger.info(
                            f"Starting activity for {ti_key} on queue '{activity_queue}' "
                            f"(dag_fileloc={self.dag_fileloc})"
                        )

                        # Deep integration: No connections/variables passed
                        # Activities read from real Airflow DB via hooks
                        handle = workflow.start_activity(
                            run_airflow_task,
                            arg=ActivityTaskInput(
                                dag_id=ti.dag_id,
                                task_id=ti.task_id,
                                run_id=ti.run_id,
                                logical_date=dag_run.logical_date,
                                try_number=ti.try_number,
                                map_index=ti.map_index,
                                dag_rel_path=self.dag_fileloc,
                                upstream_results=upstream_results,
                                queue=ti.queue,
                                pool_slots=ti.pool_slots,
                                # Deep integration: connections/variables from Airflow DB
                                connections=None,
                                variables=None,
                            ),
                            task_queue=activity_queue,
                            start_to_close_timeout=timedelta(hours=2),
                            heartbeat_timeout=timedelta(minutes=5),
                        )

                        running_activities[ti_key] = handle

                        workflow.logger.info(f"Activity handle created for {ti_key}")

            finally:
                session.close()

            # Wait for any activities to complete
            if running_activities:
                workflow.logger.info(
                    f"Waiting for {len(running_activities)} activities to complete..."
                )

                # Use workflow.wait() instead of asyncio.wait() for determinism
                # asyncio.wait() internally uses set() which is non-deterministic
                done, pending = await workflow.wait(
                    running_activities.values(),
                    timeout=5,
                    return_when=asyncio.FIRST_COMPLETED
                )

                workflow.logger.info(
                    f"Activity wait completed: {len(done)} done, {len(pending)} pending"
                )

                # Process completed activities and collect syncs for batching
                completion_syncs = []
                completed_keys = []

                for completed in done:
                    ti_key = next(k for k, v in running_activities.items() if v == completed)
                    completed_keys.append(ti_key)

                    try:
                        result: TaskExecutionResult = completed.result()
                        workflow.logger.info(
                            f"Activity completed for {ti_key}: state={result.state}"
                        )

                        # Store XCom in workflow state
                        if result.xcom_data:
                            self.xcom_store[ti_key] = result.xcom_data

                        # Update task counts
                        if result.state == TaskInstanceState.SUCCESS:
                            self.tasks_succeeded += 1
                        elif result.state == TaskInstanceState.FAILED:
                            self.tasks_failed += 1

                        # Update in-workflow DB (local)
                        await self._update_local_task_state(ti_key, result)

                        # Collect sync for batching
                        completion_syncs.append(TaskStatusSync(
                            dag_id=ti_key[0],
                            task_id=ti_key[1],
                            run_id=self.run_id,
                            map_index=ti_key[3],
                            state=result.state.value,
                            start_date=result.start_date,
                            end_date=result.end_date,
                            xcom_value=result.return_value if hasattr(result, 'return_value') else None,
                        ))

                    except ActivityError as e:
                        workflow.logger.error(
                            f"ActivityError caught for {ti_key}: {e.message}, cause type: {type(e.cause)}"
                        )

                        self.tasks_failed += 1

                        # Create failed result
                        failed_result = TaskExecutionResult(
                            dag_id=ti_key[0],
                            task_id=ti_key[1],
                            run_id=ti_key[2],
                            try_number=1,
                            state=TaskInstanceState.FAILED,
                            start_date=workflow.now(),
                            end_date=workflow.now(),
                            error_message=str(e.message),
                        )

                        await self._update_local_task_state(ti_key, failed_result)

                        # Collect sync for batching
                        completion_syncs.append(TaskStatusSync(
                            dag_id=ti_key[0],
                            task_id=ti_key[1],
                            run_id=self.run_id,
                            map_index=ti_key[3],
                            state=TaskInstanceState.FAILED.value,
                            start_date=failed_result.start_date,
                            end_date=failed_result.end_date,
                        ))

                    except Exception as e:
                        workflow.logger.error(
                            f"Unexpected error for {ti_key}: {type(e).__name__}: {e}"
                        )
                        self.tasks_failed += 1

                # Batch sync all completion states in single activity
                if completion_syncs:
                    await workflow.execute_activity(
                        sync_task_status_batch,
                        BatchTaskStatusSync(syncs=completion_syncs),
                        start_to_close_timeout=timedelta(seconds=30),
                    )

                # Remove completed from running
                for ti_key in completed_keys:
                    del running_activities[ti_key]

            else:
                # No running activities, sleep before checking for new work
                await asyncio.sleep(5)

        # Check if we broke out of loop due to completion
        if final_state:
            return final_state

        workflow.logger.error("Max iterations reached!")
        return "failed"
