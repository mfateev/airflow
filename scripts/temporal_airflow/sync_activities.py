# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
DB sync activities for deep integration mode.

These activities write execution status to Airflow database so that
the Airflow UI shows real-time status of Temporal-orchestrated DAG runs.
"""

from __future__ import annotations

from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from temporal_airflow.models import (
    BatchTaskStatusSync,
    CreateDagRunInput,
    CreateDagRunResult,
    DagRunStatusSync,
    EnsureTaskInstancesInput,
    LoadSerializedDagInput,
    LoadSerializedDagResult,
    TaskStatusSync,
)


@activity.defn(name="create_dagrun_record")
def create_dagrun_record(input: CreateDagRunInput) -> CreateDagRunResult:
    """
    Create DagRun and TaskInstance records in Airflow database.

    This activity:
    - Generates run_id as external__{logical_date}
    - Creates DagRun with state=RUNNING, run_type=EXTERNAL
    - Creates TaskInstance records for all tasks from SerializedDagModel
    - Returns run_id for workflow to use

    The EXTERNAL run_type ensures the Airflow scheduler ignores this run
    while allowing the UI to display it.
    """
    activity.logger.info(
        f"Creating DagRun record for {input.dag_id} at {input.logical_date}"
    )

    with create_session() as session:
        # Check if DagRun already exists
        existing = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == input.dag_id,
                DagRun.logical_date == input.logical_date,
            )
            .first()
        )

        if existing:
            activity.logger.info(
                f"DagRun already exists: {existing.run_id} (id={existing.id})"
            )
            return CreateDagRunResult(
                run_id=existing.run_id,
                dag_run_id=existing.id,
            )

        # Generate run_id for external runs
        run_id = f"external__{input.logical_date.isoformat()}"

        # Get serialized DAG to create TaskInstances
        serialized = SerializedDagModel.get(input.dag_id, session=session)
        if not serialized:
            raise ApplicationError(
                f"DAG {input.dag_id} not found in SerializedDagModel",
                non_retryable=True,
            )

        # Create DagRun with EXTERNAL type and RUNNING state
        # In Airflow 3.x, use data_interval instead of data_interval_start/end
        from airflow.timetables.base import DataInterval

        dag_run = DagRun(
            dag_id=input.dag_id,
            run_id=run_id,
            logical_date=input.logical_date,
            run_type=DagRunType.EXTERNAL,
            state=DagRunState.RUNNING,
            conf=input.conf,
            data_interval=DataInterval(start=input.logical_date, end=input.logical_date),
        )
        session.add(dag_run)
        session.flush()  # Get ID before creating TaskInstances

        activity.logger.info(
            f"Created DagRun: {dag_run.run_id} (id={dag_run.id})"
        )

        # Create TaskInstance records for all tasks
        # Use verify_integrity which handles TaskInstance creation
        # In Airflow 3.x, dag_version_id is required and DAG object must be set
        dag_run.dag = serialized.dag
        dag_run.verify_integrity(session=session, dag_version_id=serialized.dag_version_id)
        session.commit()

        activity.logger.info(
            f"Created TaskInstance records for DagRun {dag_run.run_id}"
        )

        return CreateDagRunResult(
            run_id=dag_run.run_id,
            dag_run_id=dag_run.id,
        )


@activity.defn(name="sync_task_status")
def sync_task_status(input: TaskStatusSync) -> None:
    """
    Write task execution status to Airflow database for UI visibility.

    This activity:
    - Updates TaskInstance state, start_date, end_date
    - Writes XCom if provided
    - Uses real Airflow session

    Called by the deep integration workflow after each task completes.
    """
    activity.logger.info(
        f"Syncing task status: {input.dag_id}.{input.task_id} "
        f"(run_id={input.run_id}, state={input.state})"
    )

    with create_session() as session:
        # Find the TaskInstance
        ti = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == input.dag_id,
                TaskInstance.task_id == input.task_id,
                TaskInstance.run_id == input.run_id,
                TaskInstance.map_index == input.map_index,
            )
            .first()
        )

        if not ti:
            activity.logger.warning(
                f"TaskInstance not found: {input.dag_id}.{input.task_id} "
                f"(run_id={input.run_id}, map_index={input.map_index})"
            )
            return

        # Update TaskInstance state
        ti.state = TaskInstanceState(input.state)

        if input.start_date:
            ti.start_date = input.start_date

        if input.end_date:
            ti.end_date = input.end_date

        # Write XCom if provided
        if input.xcom_value is not None:
            XComModel.set(
                key="return_value",
                value=input.xcom_value,
                dag_id=input.dag_id,
                task_id=input.task_id,
                run_id=input.run_id,
                map_index=input.map_index,
                session=session,
            )
            activity.logger.info(
                f"Wrote XCom for {input.dag_id}.{input.task_id}"
            )

        session.commit()

        activity.logger.info(
            f"Synced TaskInstance {input.dag_id}.{input.task_id} to state={input.state}"
        )


@activity.defn(name="sync_task_status_batch")
def sync_task_status_batch(input: BatchTaskStatusSync) -> None:
    """
    Batch update multiple task statuses in a single database transaction.

    This is more efficient than calling sync_task_status multiple times
    because it reduces the number of activity round-trips and commits.
    """
    if not input.syncs:
        return

    activity.logger.info(
        f"Batch syncing {len(input.syncs)} task statuses"
    )

    with create_session() as session:
        for sync in input.syncs:
            # Find the TaskInstance
            ti = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == sync.dag_id,
                    TaskInstance.task_id == sync.task_id,
                    TaskInstance.run_id == sync.run_id,
                    TaskInstance.map_index == sync.map_index,
                )
                .first()
            )

            if not ti:
                activity.logger.warning(
                    f"TaskInstance not found: {sync.dag_id}.{sync.task_id} "
                    f"(run_id={sync.run_id}, map_index={sync.map_index})"
                )
                continue

            # Update TaskInstance state
            ti.state = TaskInstanceState(sync.state)

            if sync.start_date:
                ti.start_date = sync.start_date

            if sync.end_date:
                ti.end_date = sync.end_date

            # Write XCom if provided
            if sync.xcom_value is not None:
                XComModel.set(
                    key="return_value",
                    value=sync.xcom_value,
                    dag_id=sync.dag_id,
                    task_id=sync.task_id,
                    run_id=sync.run_id,
                    map_index=sync.map_index,
                    session=session,
                )

            activity.logger.debug(
                f"Prepared sync: {sync.dag_id}.{sync.task_id} -> {sync.state}"
            )

        # Single commit for all updates
        session.commit()

        activity.logger.info(
            f"Batch synced {len(input.syncs)} task statuses in single transaction"
        )


@activity.defn(name="sync_dagrun_status")
def sync_dagrun_status(input: DagRunStatusSync) -> None:
    """
    Write DagRun final status to Airflow database.

    Called at workflow completion to update the final DagRun state.
    """
    activity.logger.info(
        f"Syncing DagRun status: {input.dag_id}/{input.run_id} -> {input.state}"
    )

    with create_session() as session:
        dag_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == input.dag_id,
                DagRun.run_id == input.run_id,
            )
            .first()
        )

        if not dag_run:
            activity.logger.warning(
                f"DagRun not found: {input.dag_id}/{input.run_id}"
            )
            return

        dag_run.state = DagRunState(input.state)

        if input.end_date:
            dag_run.end_date = input.end_date

        session.commit()

        activity.logger.info(
            f"Synced DagRun {input.dag_id}/{input.run_id} to state={input.state}"
        )


@activity.defn(name="load_serialized_dag")
def load_serialized_dag(input: LoadSerializedDagInput) -> LoadSerializedDagResult:
    """
    Load serialized DAG from Airflow database.

    Returns the serialized DAG data dict and file location. The data can be
    deserialized using SerializedDAG.from_dict(). The fileloc is the path
    to the DAG file relative to DAGS_FOLDER.
    """
    activity.logger.info(f"Loading serialized DAG: {input.dag_id}")

    with create_session() as session:
        serialized = SerializedDagModel.get(input.dag_id, session=session)

        if not serialized:
            raise ApplicationError(
                f"DAG {input.dag_id} not found in SerializedDagModel",
                non_retryable=True,
            )

        # In Airflow 3.x, fileloc is in dag_model or data['dag']['fileloc']
        fileloc = (
            serialized.dag_model.fileloc
            if serialized.dag_model
            else serialized.data.get("dag", {}).get("fileloc", "")
        )
        activity.logger.info(
            f"Loaded serialized DAG: {input.dag_id} (fileloc={fileloc})"
        )
        return LoadSerializedDagResult(
            dag_data=serialized.data,
            fileloc=fileloc,
        )


@activity.defn(name="ensure_task_instances")
def ensure_task_instances(input: EnsureTaskInstancesInput) -> None:
    """
    Create TaskInstance records if they don't exist.

    This is called when the orchestrator creates a DagRun but
    TaskInstances haven't been created yet. Uses DagRun.verify_integrity()
    which handles TaskInstance creation.
    """
    activity.logger.info(
        f"Ensuring TaskInstances for {input.dag_id}/{input.run_id}"
    )

    with create_session() as session:
        dag_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == input.dag_id,
                DagRun.run_id == input.run_id,
            )
            .first()
        )

        if not dag_run:
            raise ApplicationError(
                f"DagRun not found: {input.dag_id}/{input.run_id}",
                non_retryable=True,
            )

        # Get serialized DAG to get dag_version_id (required in Airflow 3.x)
        serialized = SerializedDagModel.get(input.dag_id, session=session)
        if not serialized:
            raise ApplicationError(
                f"DAG {input.dag_id} not found in SerializedDagModel",
                non_retryable=True,
            )

        # verify_integrity creates TaskInstance records
        # In Airflow 3.x, dag_version_id is required and DAG object must be set
        dag_run.dag = serialized.dag
        dag_run.verify_integrity(session=session, dag_version_id=serialized.dag_version_id)
        session.commit()

        activity.logger.info(
            f"Ensured TaskInstances for {input.dag_id}/{input.run_id}"
        )
