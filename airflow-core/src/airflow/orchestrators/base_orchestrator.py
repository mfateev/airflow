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
"""Base class for DagRun orchestrators."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun


class BaseDagRunOrchestrator(ABC):
    """
    Base class for DagRun orchestrators.

    Orchestrators control how DagRuns are executed. The default orchestrator
    uses Airflow's scheduler + executor pattern. Alternative orchestrators
    route execution to external systems like Temporal.

    Configured system-wide via [core] orchestrator setting.

    Example configuration in airflow.cfg:
        [core]
        orchestrator = DefaultOrchestrator

    Or for Temporal:
        [core]
        orchestrator = temporal_airflow.orchestrator.TemporalOrchestrator
    """

    @abstractmethod
    def start_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        Start orchestrating a DagRun.

        Called when a DagRun is created/triggered. The orchestrator is
        responsible for managing the execution of all tasks in the DAG.

        For the default orchestrator, this is a no-op as the scheduler
        picks up the DagRun automatically.

        For external orchestrators (like Temporal), this may start
        a workflow and mark the DagRun as EXTERNAL so the scheduler
        ignores it.

        :param dag_run: The DagRun to orchestrate
        :param session: Database session for any DB operations
        """
        pass

    @abstractmethod
    def cancel_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        Cancel a running DagRun.

        :param dag_run: The DagRun to cancel
        :param session: Database session for any DB operations
        """
        pass


class DefaultOrchestrator(BaseDagRunOrchestrator):
    """
    Default orchestrator - uses Airflow scheduler + executor.

    This orchestrator is a no-op: DagRuns are picked up by the scheduler
    automatically based on their state in the database.
    """

    def start_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        No-op: Scheduler will pick up the DagRun automatically.

        :param dag_run: The DagRun (unused)
        :param session: Database session (unused)
        """
        pass

    def cancel_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        Cancel a DagRun by setting its state to FAILED.

        :param dag_run: The DagRun to cancel
        :param session: Database session for committing state change
        """
        from airflow.utils.state import DagRunState

        dag_run.state = DagRunState.FAILED
        session.merge(dag_run)
