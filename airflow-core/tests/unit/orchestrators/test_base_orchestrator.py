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
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.orchestrators.base_orchestrator import BaseDagRunOrchestrator, DefaultOrchestrator
from airflow.utils.state import DagRunState


class TestBaseDagRunOrchestrator:
    """Tests for BaseDagRunOrchestrator abstract class."""

    def test_cannot_instantiate_abstract_class(self):
        """BaseDagRunOrchestrator cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseDagRunOrchestrator()

    def test_subclass_must_implement_start_dagrun(self):
        """Subclass must implement start_dagrun method."""

        class IncompleteOrchestrator(BaseDagRunOrchestrator):
            def cancel_dagrun(self, dag_run, session):
                pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteOrchestrator()

    def test_subclass_must_implement_cancel_dagrun(self):
        """Subclass must implement cancel_dagrun method."""

        class IncompleteOrchestrator(BaseDagRunOrchestrator):
            def start_dagrun(self, dag_run, session):
                pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteOrchestrator()

    def test_complete_subclass_can_be_instantiated(self):
        """A complete subclass can be instantiated."""

        class CompleteOrchestrator(BaseDagRunOrchestrator):
            def start_dagrun(self, dag_run, session):
                pass

            def cancel_dagrun(self, dag_run, session):
                pass

        orchestrator = CompleteOrchestrator()
        assert orchestrator is not None


class TestDefaultOrchestrator:
    """Tests for DefaultOrchestrator."""

    def test_can_instantiate(self):
        """DefaultOrchestrator can be instantiated."""
        orchestrator = DefaultOrchestrator()
        assert orchestrator is not None

    def test_start_dagrun_is_noop(self):
        """start_dagrun should be a no-op (scheduler picks up DagRun naturally)."""
        orchestrator = DefaultOrchestrator()
        mock_dag_run = MagicMock()
        mock_session = MagicMock()

        # Should not raise any exception
        orchestrator.start_dagrun(mock_dag_run, mock_session)

        # Should not modify dag_run or session
        mock_dag_run.assert_not_called()
        mock_session.assert_not_called()

    def test_cancel_dagrun_sets_state_to_failed(self):
        """cancel_dagrun should set DagRun state to FAILED."""
        orchestrator = DefaultOrchestrator()
        mock_dag_run = MagicMock()
        mock_session = MagicMock()

        orchestrator.cancel_dagrun(mock_dag_run, mock_session)

        # Should set state to FAILED
        assert mock_dag_run.state == DagRunState.FAILED
        # Should merge the dag_run to session
        mock_session.merge.assert_called_once_with(mock_dag_run)
