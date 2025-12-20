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
# Start breeze
breeze shell

# Verify installation
python -c "from temporal_airflow.time_provider import get_current_time; print('✓ temporal_airflow installed')"
python -c "from airflow.models.dagrun import DagRun; print('✓ dagrun imports successfully')"

# Run time provider tests
pytest temporal_airflow/tests/test_time_provider.py -v

# Run dagrun tests (regression check)
pytest airflow-core/tests/models/test_dagrun.py -v -k "test_update_state or test_schedule"

# Quick smoke test
pytest airflow-core/tests/models/test_dagrun.py::TestDagRun::test_clear -v
```

## Expected Results

- ✅ temporal_airflow module imports successfully
- ✅ dagrun imports successfully (no import errors from time_provider)
- ✅ All 4 time_provider tests pass
- ✅ Dagrun tests pass (no regression)

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
