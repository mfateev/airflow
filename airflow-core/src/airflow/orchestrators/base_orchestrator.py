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

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.timetables.base import DataInterval


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

    def should_schedule_dagrun(
        self,
        dag_model: DagModel,
        data_interval: DataInterval,
        session: Session,
    ) -> bool:
        """
        Check if the scheduler should create a DagRun for this scheduled execution.

        Called by the scheduler BEFORE creating a DagRun when a DAG's
        next_dagrun_create_after time has been reached. This allows orchestrators
        that implement native scheduling (like Temporal) to bypass Airflow's
        DagRun creation and handle scheduling themselves.

        When this method returns False:
        - Scheduler will NOT create a DagRun
        - Scheduler WILL still update dag_model.next_dagrun fields
        - Orchestrator is responsible for triggering execution at the right time

        When this method returns True (default):
        - Scheduler creates DagRun as normal
        - Scheduler calls start_dagrun() after creation

        This is not an abstract method because most orchestrators will use the
        default behavior (return True). Only orchestrators with native scheduling
        capabilities need to override this.

        :param dag_model: The DagModel for the DAG being scheduled
        :param data_interval: The data interval for the potential DagRun
        :param session: Database session for any DB operations
        :return: True if scheduler should create DagRun, False to skip
        """
        return True

    def sync_pause_state(self, dag_id: str, is_paused: bool) -> None:
        """
        Sync DAG pause state to external systems.

        Called when a DAG's is_paused state changes in the Airflow UI/API.
        For orchestrators with native scheduling (like Temporal), this allows
        pausing/unpausing the external schedule to match Airflow's state.

        Default implementation is a no-op. Orchestrators with native scheduling
        should override this to sync pause state to their scheduling system.

        :param dag_id: The DAG ID whose pause state changed
        :param is_paused: True if DAG was paused, False if unpaused
        """
        pass

    def on_dag_deleted(self, dag_id: str) -> None:
        """
        Handle DAG deletion cleanup.

        Called when a DAG is deleted from Airflow. For orchestrators with
        native scheduling (like Temporal), this allows deleting the external
        schedule that was created for this DAG.

        Default implementation is a no-op. Orchestrators with native scheduling
        should override this to clean up their scheduling resources.

        :param dag_id: The DAG ID that was deleted
        """
        pass

    def on_timetable_changed(self, dag_model: DagModel, session: Session) -> None:
        """
        Handle DAG timetable/schedule change.

        Called when a DAG's timetable changes (detected during DAG parsing).
        For orchestrators with native scheduling (like Temporal), this allows
        updating the external schedule to match the new timetable.

        Default implementation is a no-op. Orchestrators with native scheduling
        should override this to update their scheduling configuration.

        :param dag_model: The DagModel with updated timetable information
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
