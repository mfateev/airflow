# Quick Start: Testing Phase 1 Changes

**TL;DR**: `temporal_airflow` is now built into the Breeze image!

---

## 🚀 Fastest Way to Test (< 1 minute)

```bash
# 1. Start Breeze (temporal_airflow pre-installed)
breeze shell

# 2. Run tests
pytest temporal_airflow/tests/test_time_provider.py -v
pytest airflow-core/tests/models/test_dagrun.py::TestDagRun::test_update_state -v
```

Done! ✅

**Note**: First-time users or after `Dockerfile.ci` changes, you may need to rebuild:
```bash
breeze build-image --force-build
```

---

## 📋 Complete Test Checklist

### Phase 1 Tests

```bash
# Inside breeze shell
uv pip install -e /opt/airflow/temporal_airflow/

# 1. Time provider tests (should all pass)
pytest temporal_airflow/tests/test_time_provider.py -v

# 2. DagRun smoke test (verify no import errors)
pytest airflow-core/tests/models/test_dagrun.py::TestDagRun::test_clear -v

# 3. DagRun update_state test (tests our Change #1)
pytest airflow-core/tests/models/test_dagrun.py -k "update_state" -v

# 4. DagRun schedule_tis tests (tests our Changes #2 and #3)
pytest airflow-core/tests/models/test_dagrun.py -k "schedule" -v

# 5. Full dagrun test suite (verify no regression)
pytest airflow-core/tests/models/test_dagrun.py -v
```

### Expected Results

- ✅ All time_provider tests pass (4/4)
- ✅ All dagrun tests pass (same as before changes)
- ✅ No import errors
- ✅ No new failures

---

## 🐛 If Something Goes Wrong

### "No module named 'temporal_airflow'"

```bash
# Make sure you're inside breeze
breeze shell

# Install the package
uv pip install -e /opt/airflow/temporal_airflow/

# Verify
python -c "from temporal_airflow.time_provider import get_current_time; print('✓ Works!')"
```

### "Tests are failing"

```bash
# Run with more debug output
pytest temporal_airflow/tests/test_time_provider.py -vv -s

# Check if import works
python -c "from airflow.models.dagrun import DagRun; print('✓ Import OK')"

# Run just one test
pytest temporal_airflow/tests/test_time_provider.py::TestTimeProvider::test_get_current_time_without_injection -v
```

---

## 💡 Pro Tips

### Interactive Testing

```bash
# Start breeze
breeze shell

# Install package
uv pip install -e /opt/airflow/temporal_airflow/

# Start Python REPL
python

# Test interactively
>>> from temporal_airflow.time_provider import *
>>> from datetime import datetime
>>> from airflow._shared.timezones import timezone
>>>
>>> # Test injection
>>> test_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
>>> set_workflow_time(test_time)
>>> print(get_current_time())
>>> # Should print: 2025-01-01 00:00:00+00:00
```

### Watch Mode (Re-run tests on change)

```bash
# Install pytest-watch
uv pip install pytest-watch

# Watch for changes
ptw temporal_airflow/tests/test_time_provider.py -- -v
```

### Run with Coverage

```bash
pytest temporal_airflow/tests/ --cov=temporal_airflow --cov-report=term-missing
```

---

## 📝 What Each Test File Tests

### `temporal_airflow/tests/test_time_provider.py`
- ✓ Real time fallback (when not injected)
- ✓ Injected time (when set)
- ✓ Context clearing
- ✓ Thread isolation (ContextVar safety)

### `airflow-core/tests/models/test_dagrun.py`
- ✓ No regression in existing functionality
- ✓ Time-dependent operations still work
- ✓ Import of modified dagrun.py succeeds

---

## 🎯 Success Criteria

Before moving to next phase:

- [ ] All 4 time_provider tests pass
- [ ] All dagrun tests pass (no regression)
- [ ] Manual verification:
  ```bash
  python -c "
  from temporal_airflow.time_provider import get_current_time, set_workflow_time
  from datetime import datetime
  from airflow._shared.timezones import timezone

  # Test
  t = datetime(2025, 1, 1, tzinfo=timezone.utc)
  set_workflow_time(t)
  assert get_current_time() == t
  print('✅ Manual test passed')
  "
  ```

---

## 📚 More Info

See [TESTING_PROCEDURE.md](./TESTING_PROCEDURE.md) for:
- Different testing options
- Permanent installation methods
- CI integration approaches
- Troubleshooting guide
