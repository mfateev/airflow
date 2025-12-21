# Test Phase 1 Changes

Run Phase 1 tests in Breeze environment.

## Steps

1. Start Breeze shell
2. Verify temporal_airflow is installed
3. Run time_provider tests
4. Run dagrun regression tests
5. Report results

## Commands

```bash
# Verify temporal_airflow installation
breeze shell --python 3.10 -c "python -c 'from temporal_airflow.time_provider import get_current_time; print(\"✓ temporal_airflow installed\")'"

# Verify dagrun imports without errors
breeze shell --python 3.10 -c "python -c 'from airflow.models.dagrun import DagRun; print(\"✓ dagrun imports successfully\")'"

# Run time provider tests (4 tests)
breeze shell --python 3.10 -c "pytest temporal_airflow/tests/test_time_provider.py -v"

# Run dagrun regression tests (full TestDagRun suite - 62 tests)
breeze shell --python 3.10 -c "cd /opt/airflow && pytest airflow-core/tests/unit/models/test_dagrun.py::TestDagRun -v --tb=short --maxfail=5"

# Quick smoke test (single update_state test)
breeze shell --python 3.10 -c "cd /opt/airflow && pytest airflow-core/tests/unit/models/test_dagrun.py -k 'test_update_state_one_unfinished' -v"
```

## Expected Results

- ✅ temporal_airflow module imports successfully
- ✅ dagrun imports successfully (no import errors from time_provider)
- ✅ All 4 time_provider tests pass (~1.6s)
- ✅ All 62 TestDagRun tests pass (~9s, no regressions)

## Important Notes

- Tests must run inside Breeze container using `breeze shell --python 3.10 -c "command"`
- DagRun test path in container: `/opt/airflow/airflow-core/tests/unit/models/test_dagrun.py`
- Use `cd /opt/airflow &&` prefix for dagrun tests to ensure correct working directory

## If Tests Fail

If temporal_airflow is not installed:
```bash
uv pip install -e /opt/airflow/temporal_airflow/
```

If breeze image is outdated:
```bash
exit  # Exit breeze first
breeze build-image --python 3.10 --force-build
```

## Documentation

See:
- `docs/temporal/TESTING_QUICK_START.md` - Quick reference
- `docs/temporal/TESTING_PROCEDURE.md` - Full testing guide
- `docs/temporal/PHASE_1_STATUS.md` - Implementation status
