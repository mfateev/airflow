# Custom Skills for Airflow Temporal Project

This directory contains custom skills for Claude Code to help with the Temporal integration project.

## Available Skills

### `/test-phase1` - Run Phase 1 Tests

Runs the complete Phase 1 test suite in Breeze:
- Verifies temporal_airflow installation
- Runs time_provider unit tests
- Runs dagrun regression tests
- Reports results

**Usage**:
```
/test-phase1
```

**What it does**:
1. Starts Breeze shell
2. Verifies packages are installed
3. Runs time_provider tests (4 tests)
4. Runs relevant dagrun tests
5. Provides summary of results

**When to use**:
- After making changes to temporal_airflow code
- After modifying dagrun.py
- Before committing Phase 1 changes
- To verify breeze image has temporal_airflow installed

## Adding New Skills

To add a new skill:

1. Create a markdown file in this directory (e.g., `my-skill.md`)
2. Document what the skill does
3. Include commands to run
4. Add expected results
5. The skill name matches the filename (e.g., `/my-skill`)

Skills are automatically discovered by Claude Code.

## Documentation

For more info on the Temporal implementation:
- `../temporal/TESTING_QUICK_START.md` - Quick testing guide
- `../temporal/PHASE_1_STATUS.md` - Current status
- `../temporal/TEMPORAL_IMPLEMENTATION_PLAN.md` - Full plan
