# Test Temporal Integration

Run temporal_airflow tests and Airflow regression tests in Breeze environment.

## Steps

1. Start Breeze shell
2. Verify temporal_airflow is installed
3. Run all temporal_airflow tests
4. Run Airflow regression tests (DagRun)
5. Report results

## Commands

```bash
# Verify temporal_airflow installation
breeze shell --python 3.10 -c "python -c 'from temporal_airflow.time_provider import get_current_time; print(\"✓ temporal_airflow installed\")'"

# Verify Airflow integration (dagrun imports without errors)
breeze shell --python 3.10 -c "python -c 'from airflow.models.dagrun import DagRun; print(\"✓ dagrun imports successfully\")'"

# Run all temporal_airflow tests (time_provider + models + workflows)
breeze shell --python 3.10 -c "pytest temporal_airflow/tests/ -v --tb=short --continue-on-collection-errors"

# Run DagRun regression tests (full TestDagRun suite - 62 tests)
breeze shell --python 3.10 -c "cd /opt/airflow && pytest airflow-core/tests/unit/models/test_dagrun.py::TestDagRun -v --tb=short --maxfail=5"

# Quick smoke test (single update_state test)
breeze shell --python 3.10 -c "cd /opt/airflow && pytest airflow-core/tests/unit/models/test_dagrun.py -k 'test_update_state_one_unfinished' -v"
```

## Expected Results

- ✅ temporal_airflow module imports successfully
- ✅ dagrun imports successfully (no import errors from time_provider)
- ✅ All temporal_airflow tests pass:
  - **Currently in container:** `test_time_provider.py` (4 tests)
  - **After image rebuild:** All 3 test files with ~31 tests total:
    - `test_time_provider.py` - Time provider functionality (4 tests)
    - `test_models.py` - Temporal data models (19 tests)
    - `test_workflows.py` - Workflow integration tests (8 tests)
- ✅ All 62 TestDagRun tests pass (~9s, no regressions)

**Note:** If you see only 4 tests running instead of 31, the Breeze Docker image needs to be rebuilt to include new test files. See troubleshooting section below.

## Important Notes

- Tests must run inside Breeze container using `breeze shell --python 3.10 -c "command"`
- DagRun test path in container: `/opt/airflow/airflow-core/tests/unit/models/test_dagrun.py`
- Use `cd /opt/airflow &&` prefix for dagrun tests to ensure correct working directory
- All temporal_airflow tests are in: `/opt/airflow/temporal_airflow/tests/`

## Troubleshooting

### Only 4 Tests Running (Image Caching Issue)

If pytest only finds 4 tests instead of 31, new test files aren't visible in the Breeze container due to Docker image caching:

**Check what files are in container:**
```bash
breeze shell --python 3.10 -c "ls -la /opt/airflow/temporal_airflow/tests/"
```

**If missing test_models.py or test_workflows.py, rebuild image:**
```bash
breeze down  # Stop and remove containers
breeze build-image --python 3.10 --force-build
```

Then re-run tests. See CLAUDE.md "Breeze Image Caching and New Files" section for details.

### temporal_airflow Not Installed

If you see import errors:
```bash
breeze shell --python 3.10 -c "uv pip install -e /opt/airflow/temporal_airflow/"
```

## Test Files

Current test coverage:
- `temporal_airflow/tests/test_time_provider.py` - Time provider with Temporal context (4 tests)
- `temporal_airflow/tests/test_models.py` - Temporal data models (TaskExecutionInput, etc.) (19 tests)
- `temporal_airflow/tests/test_workflows.py` - Workflow integration tests (8 async tests)

## Documentation

See:
- `docs/temporal/TESTING_QUICK_START.md` - Quick reference
- `docs/temporal/TESTING_PROCEDURE.md` - Full testing guide
- `docs/temporal/PHASE_1_STATUS.md` - Implementation status
