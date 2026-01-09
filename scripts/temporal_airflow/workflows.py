"""Temporal workflow for executing Airflow DAGs."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError, ActivityError

# Pass through Airflow imports to avoid sandbox reloading issues
# This prevents configuration initialization, YAML parsing, and pendulum metaclass conflicts
with workflow.unsafe.imports_passed_through():
    import pendulum  # Must be imported first to avoid metaclass conflicts
    from airflow.models.dagrun import DagRun, DagRunState
    from airflow.models.dag_version import DagVersion
    from airflow.models.taskinstance import TaskInstance, TaskInstanceState
    from airflow.models.trigger import Trigger  # Required for TaskInstance.trigger_id FK
    from airflow.models.tasklog import LogTemplate  # Required for DagRun.log_template_id FK
    from airflow.serialization.serialized_objects import SerializedDAG  # Still needed for DAG deserialization
    from airflow._shared.timezones import timezone as airflow_timezone
    from airflow.utils.time_provider import set_time_provider, clear_time_provider
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool
    from sqlalchemy.orm import sessionmaker

from temporal_airflow.models import (
    DagExecutionInput,
    DagExecutionResult,
    DagExecutionFailureDetails,
    ActivityTaskInput,
    TaskExecutionResult,
    TaskExecutionFailureDetails,
)
from temporal_airflow.activities import run_airflow_task


@workflow.defn(name="execute_airflow_dag", sandboxed=False)
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
        self.sessionFactory = None

        # Decision 3 & 7: DAG state management
        self.serialized_dag = None  # Full DAG (from input)
        self.dag = None  # Deserialized DAG

        # Decision 7: XCom state in workflow
        self.xcom_store: dict[tuple, Any] = {}  # ti_key -> xcom_data

        # Standalone mode: connections/variables passed to activities
        self.connections: dict[str, dict[str, Any]] | None = None
        self.variables: dict[str, str] | None = None

        # Task tracking
        self.tasks_succeeded: int = 0
        self.tasks_failed: int = 0

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

        # Set Temporal's deterministic time provider for Airflow code
        # workflow.now is context-aware and returns correct time per workflow
        set_time_provider(workflow.now)

        start_time = workflow.now()

        try:
            # Phase 1: Setup (Decision 1: workflow-specific DB)
            self._initialize_database()

            # Commit 2: Store and deserialize DAG (Decision 3)
            self.serialized_dag = input.serialized_dag
            self.dag = SerializedDAG.from_dict(self.serialized_dag)

            # Standalone mode: Store connections/variables for passing to activities
            self.connections = input.connections
            self.variables = input.variables

            workflow.logger.info(f"Deserialized DAG: {self.dag.dag_id}")

            # Commit 3: Create DAG run
            dag_run_id = self._create_dag_run(
                dag_id=input.dag_id,
                run_id=input.run_id,
                logical_date=input.logical_date,
                conf=input.conf,
            )

            workflow.logger.info(f"Created DAG run: {dag_run_id}")

            # Commit 4-7: Main scheduling loop
            final_state = await self._scheduling_loop(dag_run_id)

            end_time = workflow.now()

            # If DAG failed, raise ApplicationError with structured details
            if final_state == "failed":
                failure_details = DagExecutionFailureDetails(
                    dag_id=input.dag_id,
                    run_id=input.run_id,
                    start_date=start_time,
                    end_date=end_time,
                    tasks_succeeded=self.tasks_succeeded,
                    tasks_failed=self.tasks_failed,
                    error_message=f"DAG execution failed: {self.tasks_failed} task(s) failed",
                )
                raise ApplicationError(
                    f"DAG execution failed: {input.dag_id} / {input.run_id}",
                    failure_details,
                    type="DagExecutionFailure",
                    non_retryable=True,
                )

            # Return result for successful execution
            return DagExecutionResult(
                state=final_state,
                dag_id=input.dag_id,
                run_id=input.run_id,
                start_date=start_time,
                end_date=end_time,
                tasks_succeeded=self.tasks_succeeded,
                tasks_failed=self.tasks_failed,
            )

        finally:
            clear_time_provider()

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
        self.sessionFactory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        # Create only the required tables (not all Airflow tables)
        # This avoids the slow Base.metadata.create_all() that triggers deadlock detection
        required_tables = [
            LogTemplate.__table__,  # Required for DagRun.log_template_id FK
            DagVersion.__table__,   # Required for DagRun foreign key
            Trigger.__table__,      # Required for TaskInstance.trigger_id FK
            DagRun.__table__,
            TaskInstance.__table__,
        ]
        for table in required_tables:
            table.create(self.engine, checkfirst=True)

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
        # Use workflow-specific session (Decision 1)
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

            # Create TaskInstances
            dag_run.verify_integrity(session=session, dag_version_id=dag_version.id)

            session.commit()

            workflow.logger.info(f"Created DagRun: {dag_run.id} with {len(dag_run.task_instances)} tasks")
            return dag_run.id
        finally:
            session.close()

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

    async def _update_local_task_state(self, ti_key: tuple, result: TaskExecutionResult):
        """
        Update TaskInstance in IN-WORKFLOW DB based on activity result.

        This only updates the local in-memory DB. For standalone mode,
        there is no external DB to sync to.
        """
        session = self.sessionFactory()
        try:
            ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti_key[0],
                TaskInstance.task_id == ti_key[1],
                TaskInstance.run_id == ti_key[2],
                TaskInstance.map_index == ti_key[3],
            ).one()

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

    def _check_dag_completion(self, dag_run_id: int) -> str | None:
        """
        Check if DAG run is complete.

        Returns:
            Final state string ("success" or "failed") if complete, None otherwise.
        """
        session = self.sessionFactory()
        try:
            dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
            if dag_run.state in (DagRunState.SUCCESS, DagRunState.FAILED):
                return dag_run.state.value if hasattr(dag_run.state, 'value') else str(dag_run.state)
            return None
        finally:
            session.close()

    def _get_and_schedule_ready_tasks(self, dag_run_id: int) -> list[TaskInstance]:
        """
        Find schedulable tasks using Airflow's native logic and mark them scheduled.

        Uses dag_run.update_state() which internally calls TriggerRuleDep
        for trigger rule evaluation.

        Returns:
            List of TaskInstance objects ready to execute.
        """
        session = self.sessionFactory()
        try:
            dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
            dag_run.dag = self.dag

            # CRITICAL: Use Airflow's native update_state() method
            schedulable_tis, _ = dag_run.update_state(
                session=session,
                execute_callbacks=False,
            )
            session.commit()

            if schedulable_tis:
                dag_run.schedule_tis(schedulable_tis, session=session)
                session.commit()

            return schedulable_tis or []
        finally:
            session.close()

    def _start_task_activity(self, ti: TaskInstance, dag_run_id: int) -> tuple[tuple, Any]:
        """
        Start a Temporal activity for a single task instance.

        Args:
            ti: TaskInstance to execute
            dag_run_id: DAG run database ID

        Returns:
            Tuple of (ti_key, activity_handle)
        """
        ti_key = (ti.dag_id, ti.task_id, ti.run_id, ti.map_index)

        task = self.dag.get_task(ti.task_id)
        upstream_results = self._get_upstream_xcom(ti, task)
        activity_queue = workflow.info().task_queue

        # Get logical date from dag_run
        logical_date = self._get_logical_date(dag_run_id)

        # Create DAG file path (relative to DAGS_FOLDER)
        dag_rel_path = f"{ti.dag_id}.py"

        workflow.logger.info(
            f"Starting activity for {ti_key} on queue '{activity_queue}'"
        )

        handle = workflow.start_activity(
            run_airflow_task,
            arg=ActivityTaskInput(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                logical_date=logical_date,
                try_number=ti.try_number,
                map_index=ti.map_index,
                dag_rel_path=dag_rel_path,
                upstream_results=upstream_results,
                queue=ti.queue,
                pool_slots=ti.pool_slots,
                connections=self.connections,
                variables=self.variables,
            ),
            task_queue=activity_queue,
            start_to_close_timeout=timedelta(hours=2),
            heartbeat_timeout=timedelta(minutes=5),
            summary=f"Task: {ti.dag_id}.{ti.task_id}",
        )

        return ti_key, handle

    async def _process_activity_result(
        self,
        completed: Any,
        ti_key: tuple,
    ) -> None:
        """
        Process a completed activity and update workflow state.

        Args:
            completed: Completed activity handle
            ti_key: Task instance key tuple
        """
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

            # Update in-workflow DB
            await self._update_local_task_state(ti_key, result)

        except ActivityError as e:
            workflow.logger.error(
                f"ActivityError for {ti_key}: {e.message}"
            )
            self.tasks_failed += 1

            # Check if the cause is an ApplicationError with details
            if isinstance(e.cause, ApplicationError) and e.cause.details:
                failure_data = e.cause.details[0]
                if isinstance(failure_data, dict):
                    failure_details = TaskExecutionFailureDetails(**failure_data)
                    failed_result = TaskExecutionResult(
                        dag_id=failure_details.dag_id,
                        task_id=failure_details.task_id,
                        run_id=failure_details.run_id,
                        try_number=failure_details.try_number,
                        state=TaskInstanceState.FAILED,
                        start_date=failure_details.start_date,
                        end_date=failure_details.end_date,
                        error_message=failure_details.error_message,
                    )
                    await self._update_local_task_state(ti_key, failed_result)
                    return

            # Fallback: create minimal failed result
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

        except Exception as e:
            workflow.logger.error(
                f"Unexpected error for {ti_key}: {type(e).__name__}: {e}"
            )
            self.tasks_failed += 1

    async def _await_and_process_completions(
        self,
        running_activities: dict[tuple, Any],
    ) -> list[tuple]:
        """
        Wait for activities to complete and process results.

        Args:
            running_activities: Dict of ti_key -> activity handle

        Returns:
            List of completed ti_keys to remove from running_activities
        """
        workflow.logger.info(
            f"Waiting for {len(running_activities)} activities..."
        )

        done, pending = await workflow.wait(
            running_activities.values(),
            return_when=asyncio.FIRST_COMPLETED
        )

        workflow.logger.info(
            f"Activity wait: {len(done)} done, {len(pending)} pending"
        )

        # Process completions
        completed_keys = []

        for completed in done:
            ti_key = next(k for k, v in running_activities.items() if v == completed)
            completed_keys.append(ti_key)
            await self._process_activity_result(completed, ti_key)

        return completed_keys

    def _get_logical_date(self, dag_run_id: int) -> datetime:
        """Get the logical date for a DAG run."""
        session = self.sessionFactory()
        try:
            dag_run = session.query(DagRun).filter(DagRun.id == dag_run_id).one()
            return dag_run.logical_date
        finally:
            session.close()

    async def _scheduling_loop(self, dag_run_id: int) -> str:
        """
        Main scheduling loop using Airflow's native logic.

        Returns:
            Final DAG run state ("success" or "failed")
        """
        running_activities: dict[tuple, Any] = {}

        iteration = 0
        while True:
            iteration += 1
            workflow.logger.info(
                f"Scheduling iteration {iteration}: "
                f"{len(running_activities)} running"
            )

            # Find schedulable tasks (calls update_state() which may mark DAG complete)
            schedulable_tis = self._get_and_schedule_ready_tasks(dag_run_id)

            # Check if DAG is complete (must be AFTER update_state() call)
            final_state = self._check_dag_completion(dag_run_id)
            if final_state:
                workflow.logger.info(f"DAG completed: {final_state}")
                return final_state

            # Start activities for schedulable tasks
            for ti in schedulable_tis:
                ti_key, handle = self._start_task_activity(ti, dag_run_id)
                running_activities[ti_key] = handle

            # Fail if stuck (no activities to wait for)
            # TODO: When sensors/deferrable operators are implemented,
            # wait for external events (Temporal signals) instead of failing.
            if not running_activities:
                raise ApplicationError(
                    f"Workflow stuck: no running activities but DAG not complete. "
                    f"DAG: {self.dag.dag_id}, "
                    f"succeeded: {self.tasks_succeeded}, failed: {self.tasks_failed}",
                    type="WorkflowStuck",
                    non_retryable=True,
                )

            # Wait for completions
            completed_keys = await self._await_and_process_completions(running_activities)
            for key in completed_keys:
                del running_activities[key]
