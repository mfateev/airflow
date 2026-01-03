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
"""Temporal orchestrator for routing DagRun execution to Temporal workflows."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from temporalio.client import Client

from airflow.orchestrators.base_orchestrator import BaseDagRunOrchestrator
from airflow.utils.types import DagRunType

from temporal_airflow.client_config import create_temporal_client, get_task_queue
from temporal_airflow.models import DeepDagExecutionInput
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun

log = logging.getLogger(__name__)


class TemporalOrchestrator(BaseDagRunOrchestrator):
    """
    Routes ALL DagRun execution to Temporal workflows.

    When configured as the orchestrator, every DagRun created in Airflow
    will be executed via Temporal instead of the traditional scheduler+executor.

    Configuration:
        Set in airflow.cfg:

        [core]
        orchestrator = temporal_airflow.orchestrator.TemporalOrchestrator

    The DagRun is marked as EXTERNAL so the Airflow scheduler ignores it.
    Temporal then owns the entire execution lifecycle.

    Note: This orchestrator is synchronous but uses asyncio internally
    to interact with the Temporal client.
    """

    def __init__(self):
        """Initialize the orchestrator."""
        self._client: Client | None = None
        self._lock = asyncio.Lock()

    async def _get_client(self) -> Client:
        """Get or create the Temporal client (async, with lock for thread safety)."""
        async with self._lock:
            if self._client is None:
                self._client = await create_temporal_client()
                log.info("Created Temporal client")
            return self._client

    def start_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        Start Temporal workflow for this DagRun using deep integration.

        This method:
        1. Marks the DagRun as EXTERNAL so the scheduler ignores it
        2. Starts a deep integration Temporal workflow

        The deep workflow:
        - Loads serialized DAG from Airflow DB via activity
        - Syncs status back to Airflow DB for UI visibility
        - Uses real Airflow DB for connections/variables

        :param dag_run: The DagRun to orchestrate
        :param session: Database session for DB operations
        """
        # Mark as EXTERNAL so scheduler ignores this run
        dag_run.run_type = DagRunType.EXTERNAL
        session.merge(dag_run)

        # Build workflow input for deep integration
        # Note: No serialized_dag passed - workflow loads it via activity
        # Manual triggers in Airflow 3.x may have logical_date=None
        logical_date = dag_run.logical_date or datetime.now(timezone.utc)
        workflow_input = DeepDagExecutionInput(
            dag_id=dag_run.dag_id,
            logical_date=logical_date,
            run_id=dag_run.run_id,  # Pass existing run_id
            conf=dag_run.conf or {},
        )

        # Start the Temporal workflow
        workflow_id = f"airflow-{dag_run.dag_id}-{dag_run.run_id}"

        try:
            asyncio.run(self._start_workflow_async(workflow_id, workflow_input))
            log.info(
                "Started Temporal deep workflow %s for DagRun %s/%s",
                workflow_id,
                dag_run.dag_id,
                dag_run.run_id,
            )
        except Exception as e:
            log.exception(
                "Failed to start Temporal workflow for DagRun %s/%s: %s",
                dag_run.dag_id,
                dag_run.run_id,
                e,
            )
            # Re-raise to let caller handle the failure
            raise

    async def _start_workflow_async(
        self,
        workflow_id: str,
        input: DeepDagExecutionInput,
    ) -> None:
        """Start the Temporal deep integration workflow (async implementation)."""
        client = await self._get_client()
        task_queue = get_task_queue()

        await client.start_workflow(
            ExecuteAirflowDagDeepWorkflow.run,
            input,
            id=workflow_id,
            task_queue=task_queue,
        )

    def cancel_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        Cancel a running DagRun by cancelling its Temporal workflow.

        :param dag_run: The DagRun to cancel
        :param session: Database session for DB operations
        """
        workflow_id = f"airflow-{dag_run.dag_id}-{dag_run.run_id}"

        try:
            asyncio.run(self._cancel_workflow_async(workflow_id))
            log.info(
                "Cancelled Temporal workflow %s for DagRun %s/%s",
                workflow_id,
                dag_run.dag_id,
                dag_run.run_id,
            )
        except Exception as e:
            log.exception(
                "Failed to cancel Temporal workflow for DagRun %s/%s: %s",
                dag_run.dag_id,
                dag_run.run_id,
                e,
            )
            # Update DB state to failed even if workflow cancel fails
            from airflow.utils.state import DagRunState

            dag_run.state = DagRunState.FAILED
            session.merge(dag_run)

    async def _cancel_workflow_async(self, workflow_id: str) -> None:
        """Cancel a Temporal workflow (async implementation)."""
        client = await self._get_client()
        handle = client.get_workflow_handle(workflow_id)
        await handle.cancel()
