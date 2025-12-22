"""Temporal workflow for executing Airflow DAGs."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

# Pass through Airflow imports to avoid sandbox reloading issues
# This prevents configuration initialization, YAML parsing, and pendulum metaclass conflicts
with workflow.unsafe.imports_passed_through():
    import pendulum  # Must be imported first to avoid metaclass conflicts
    from airflow.models.dagrun import DagRun, DagRunState
    from airflow.models.dag_version import DagVersion
    from airflow.models.taskinstance import TaskInstance, TaskInstanceState
    from airflow.models.trigger import Trigger  # Required for Callback foreign key
    from airflow.serialization.serialized_objects import SerializedDAG, SerializedBaseOperator
    from airflow._shared.timezones import timezone as airflow_timezone
    from temporal_airflow.time_provider import set_workflow_time, clear_workflow_time
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool
    from sqlalchemy.orm import sessionmaker
    from airflow.models import Base

from temporal_airflow.models import (
    DagExecutionInput,
    DagExecutionResult,
    TaskExecutionInput,
    TaskExecutionResult,
)


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

            # Commit 2: Store and deserialize DAG (Decision 3)
            self.serialized_dag = input.serialized_dag
            self.dag = SerializedDAG.from_dict(self.serialized_dag)

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

            # Commit 7: Count task results
            session = self.sessionFactory()
            try:
                task_instances = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == input.dag_id,
                    TaskInstance.run_id == input.run_id,
                ).all()

                tasks_succeeded = sum(1 for ti in task_instances if ti.state == TaskInstanceState.SUCCESS)
                tasks_failed = sum(1 for ti in task_instances if ti.state == TaskInstanceState.FAILED)
            finally:
                session.close()

            # Return result
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
        self.sessionFactory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        # Create schema
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

                # Commit 5: Update state and get schedulable tasks
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

                        # TODO Phase 5: Check pool availability

                        # Commit 6: Extract and serialize ONLY this task (Decision 7)
                        task = self.dag.get_task(ti.task_id)
                        serialized_task = SerializedBaseOperator.serialize_operator(task)

                        # Commit 6: Gather upstream XCom (Decision 7)
                        upstream_results = self._get_upstream_xcom(ti, task)

                        # Commit 6: Start activity directly (Decision 2)
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
                        )

                        running_activities[ti_key] = handle

                        # TODO Phase 5: Track pool usage

                        workflow.logger.info(f"Started activity for {ti_key}")

            finally:
                session.close()

            # Commit 7: Wait for any activities to complete
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

        workflow.logger.error("Max iterations reached!")
        return "failed"
