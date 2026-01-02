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

from airflow.orchestrators import orchestrator_loader
from airflow.orchestrators.base_orchestrator import BaseDagRunOrchestrator, DefaultOrchestrator

from tests_common.test_utils.config import conf_vars


class FakeOrchestrator(BaseDagRunOrchestrator):
    """Fake orchestrator for testing custom path loading."""

    def start_dagrun(self, dag_run, session):
        pass

    def cancel_dagrun(self, dag_run, session):
        pass


@pytest.fixture
def clean_orchestrator_loader():
    """Reset orchestrator loader state before and after each test."""
    orchestrator_loader.reset_orchestrator()
    yield
    orchestrator_loader.reset_orchestrator()


@pytest.mark.usefixtures("clean_orchestrator_loader")
class TestOrchestratorLoader:
    """Tests for orchestrator loader."""

    def test_default_orchestrator_when_not_configured(self):
        """Returns DefaultOrchestrator when no orchestrator is configured."""
        with conf_vars({("core", "orchestrator"): None}):
            orchestrator = orchestrator_loader.get_orchestrator()
            assert isinstance(orchestrator, DefaultOrchestrator)

    def test_default_orchestrator_by_name(self):
        """Returns DefaultOrchestrator when configured by name."""
        with conf_vars({("core", "orchestrator"): "DefaultOrchestrator"}):
            orchestrator = orchestrator_loader.get_orchestrator()
            assert isinstance(orchestrator, DefaultOrchestrator)

    def test_custom_orchestrator_by_path(self):
        """Loads custom orchestrator by module path."""
        with conf_vars(
            {("core", "orchestrator"): "unit.orchestrators.test_orchestrator_loader.FakeOrchestrator"}
        ):
            orchestrator = orchestrator_loader.get_orchestrator()
            assert isinstance(orchestrator, FakeOrchestrator)

    def test_singleton_behavior(self):
        """get_orchestrator returns the same instance on subsequent calls."""
        with conf_vars({("core", "orchestrator"): "DefaultOrchestrator"}):
            orchestrator1 = orchestrator_loader.get_orchestrator()
            orchestrator2 = orchestrator_loader.get_orchestrator()
            assert orchestrator1 is orchestrator2

    def test_reset_clears_singleton(self):
        """reset_orchestrator clears the singleton instance."""
        with conf_vars({("core", "orchestrator"): "DefaultOrchestrator"}):
            orchestrator1 = orchestrator_loader.get_orchestrator()
            orchestrator_loader.reset_orchestrator()
            orchestrator2 = orchestrator_loader.get_orchestrator()
            # Should be equal but not the same instance
            assert type(orchestrator1) == type(orchestrator2)
            assert orchestrator1 is not orchestrator2

    def test_fallback_on_import_error(self):
        """Falls back to DefaultOrchestrator on import error."""
        with conf_vars({("core", "orchestrator"): "nonexistent.module.Orchestrator"}):
            orchestrator = orchestrator_loader.get_orchestrator()
            # Should fallback to DefaultOrchestrator
            assert isinstance(orchestrator, DefaultOrchestrator)

    def test_fallback_on_invalid_class(self):
        """Falls back to DefaultOrchestrator when class cannot be instantiated."""
        with conf_vars({("core", "orchestrator"): "os.path"}):  # Not a class
            orchestrator = orchestrator_loader.get_orchestrator()
            # Should fallback to DefaultOrchestrator
            assert isinstance(orchestrator, DefaultOrchestrator)


class TestOrchestratorClasses:
    """Tests for ORCHESTRATOR_CLASSES registry."""

    def test_default_orchestrator_in_registry(self):
        """DefaultOrchestrator should be in the registry."""
        assert "DefaultOrchestrator" in orchestrator_loader.ORCHESTRATOR_CLASSES
        assert (
            orchestrator_loader.ORCHESTRATOR_CLASSES["DefaultOrchestrator"]
            == "airflow.orchestrators.base_orchestrator.DefaultOrchestrator"
        )
