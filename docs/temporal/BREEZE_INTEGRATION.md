# Breeze Image Integration

**Date**: 2025-12-20
**Change**: Added `temporal_airflow` to Breeze CI image

---

## Summary

The `temporal_airflow` package is now automatically installed in the Breeze development environment, making testing easier and faster.

---

## Changes Made

### Modified: `Dockerfile.ci`

**Location**: Line 869

**Change**: Added `--editable ./temporal_airflow \` to the installation command flags

**Before**:
```dockerfile
installation_command_flags=" --editable .[${AIRFLOW_EXTRAS}] \
      --editable ./airflow-core --editable ./task-sdk --editable ./airflow-ctl \
      --editable ./kubernetes-tests --editable ./docker-tests --editable ./helm-tests \
      ...
```

**After**:
```dockerfile
installation_command_flags=" --editable .[${AIRFLOW_EXTRAS}] \
      --editable ./airflow-core --editable ./task-sdk --editable ./airflow-ctl \
      --editable ./temporal_airflow \
      --editable ./kubernetes-tests --editable ./docker-tests --editable ./helm-tests \
      ...
```

---

## Impact

### For Developers

**Before**:
1. Start breeze
2. Manually install temporal_airflow: `uv pip install -e /opt/airflow/temporal_airflow/`
3. Run tests

**After**:
1. Start breeze (package pre-installed)
2. Run tests

### For CI

- CI builds will automatically include temporal_airflow
- No additional installation steps needed in CI workflows
- Tests can directly import from temporal_airflow

---

## Rebuild Required

After this change, developers need to rebuild their breeze image once:

```bash
breeze build-image --python 3.10 --force-build
```

Subsequent breeze sessions will have temporal_airflow pre-installed.

---

## Verification

After rebuilding, verify the package is installed:

```bash
breeze shell
python -c "from temporal_airflow.time_provider import get_current_time; print('✓ temporal_airflow is installed')"
```

---

## Package Location

- **Source**: `/opt/airflow/temporal_airflow/` (mounted from repo root)
- **Installed as**: Editable package (changes to source immediately reflected)
- **Import**: `from temporal_airflow.time_provider import ...`

---

## Why This Approach?

**Pros**:
- ✅ Zero setup for developers after image rebuild
- ✅ Consistent environment across all developers
- ✅ Works in CI without modifications
- ✅ Editable install (code changes immediately reflected)
- ✅ Follows existing Airflow package structure pattern

**Cons**:
- ⚠️ Requires one-time image rebuild
- ⚠️ Adds ~1-2 seconds to image build time

**Alternatives Considered**:
1. Manual installation in breeze shell - Rejected (tedious, error-prone)
2. Init script in `files/airflow-breeze-config/` - Rejected (adds startup delay every time)
3. Move to `airflow-core/src/airflow/` - Deferred (may do later if temporal becomes permanent)

---

## Maintenance

If `temporal_airflow/pyproject.toml` changes:
- Developers need to rebuild: `breeze build-image --force-build`
- Or temporarily: `uv pip install -e /opt/airflow/temporal_airflow/ --force-reinstall`

If code in `temporal_airflow/*.py` changes:
- No rebuild needed (editable install picks up changes automatically)
