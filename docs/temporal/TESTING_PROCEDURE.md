# Testing Phase 1 Changes

---

## Quick Start

The `temporal_airflow` package is now included in the Breeze image build. Just start Breeze and run tests:

```bash
# Start Breeze (rebuilds image if needed)
breeze shell

# Run tests
pytest temporal_airflow/tests/test_time_provider.py -v
pytest airflow-core/tests/models/test_dagrun.py -v
```

---

## If Using Existing Image (Before Rebuild)

If you already have a breeze image and don't want to rebuild yet:

```bash
# Start Breeze
breeze shell

# Install temporal_airflow manually (only needed once per container session)
uv pip install -e /opt/airflow/temporal_airflow/

# Run tests
pytest temporal_airflow/tests/test_time_provider.py -v
pytest airflow-core/tests/models/test_dagrun.py -v
```

---

## Force Image Rebuild

To rebuild the breeze image with the latest changes:

```bash
breeze build-image --python 3.10 --force-build
```

Then start breeze normally - temporal_airflow will be pre-installed.

---

## Test Commands

### Time Provider Tests
```bash
# All time provider tests
pytest temporal_airflow/tests/test_time_provider.py -v

# Specific test
pytest temporal_airflow/tests/test_time_provider.py::TestTimeProvider::test_get_current_time_with_injection -v
```

### DagRun Tests (Regression Check)
```bash
# All dagrun tests
pytest airflow-core/tests/models/test_dagrun.py -v

# Smoke test
pytest airflow-core/tests/models/test_dagrun.py::TestDagRun::test_clear -v

# Tests related to our changes
pytest airflow-core/tests/models/test_dagrun.py -k "update_state" -v
pytest airflow-core/tests/models/test_dagrun.py -k "schedule" -v
```

### Using breeze testing Command
```bash
# From host (outside breeze)
breeze testing core-tests temporal_airflow/tests/test_time_provider.py
breeze testing core-tests airflow-core/tests/models/test_dagrun.py
```

---

## Verification

Verify temporal_airflow is available:

```bash
# Inside breeze
python -c "from temporal_airflow.time_provider import get_current_time; print('✓ OK')"
```

Verify dagrun import works:

```bash
# Inside breeze
python -c "from airflow.models.dagrun import DagRun; print('✓ OK')"
```

---

## Troubleshooting

**"No module named 'temporal_airflow'"**

Solution 1: Rebuild image:
```bash
breeze build-image --force-build
```

Solution 2: Install manually (temporary):
```bash
uv pip install -e /opt/airflow/temporal_airflow/
```

**Tests failing**

Run with more verbosity:
```bash
pytest temporal_airflow/tests/test_time_provider.py -vv -s
```
