# GitHub Submission Plan

This document captures the recommended cleanup and submission order for the current repository state.

## Goals

- Keep private credentials and machine-specific data out of Git.
- Submit the configuration refactor and metadata pipeline refactor as coherent reviewable changes.
- Verify that a fresh clone can install dependencies and run a non-private test suite.

## Submission Batches

### 1. Repository Hygiene

Files:

- `.gitignore`
- `requirements.txt`
- `docs/CODEX_WORKFLOW.md`
- `docs/GITHUB_SUBMISSION_PLAN.md`
- `src/config/config_tecent_backend_server_example.yaml`
- `src/config/README_tecent_config.md`

Purpose:

- remove cache/log/data noise from version control
- provide a safe example config for contributors
- make fresh-clone setup reproducible

### 2. Config System Refactor

Files:

- `src/config/__init__.py`
- `src/config/config_loader.py`
- `tests/config_tests/test_config_loader.py`
- `scripts/verify_new_config_system.py`

Do not include:

- `src/config/config_tecent_backend_server_use.yaml`
- `src/config/config_tecent_backend_server_test.yaml`
- `src/config/config_tecent_backend_server_mimic.yaml`

Purpose:

- ship the config API changes without leaking real credentials
- keep automated config tests pointed at the example config

### 3. Metadata Pipeline Refactor

Files:

- `src/docset_hub/metadata/`
- `tests/metadata/`
- any call sites updated to the new metadata pipeline

Include deletions in the same batch:

- `src/docset_hub/metadata/extractor.py`
- `src/docset_hub/metadata/validator.py`
- `src/docset_hub/input_adapters/`

Purpose:

- present the new pipeline as a complete replacement, not an extra parallel implementation

### 4. Indexing / Storage / Database Follow-up

Files:

- `src/docset_hub/indexing/`
- `src/docset_hub/storage/`
- `database/`
- `scripts/setup_*.py`
- `tests/indexing/`
- `tests/db/`
- `tests/storage/`

Purpose:

- keep database and runtime behavior changes grouped together

## Suggested Verification Commands

Run inside the `langtaosha` Conda environment:

```bash
pip install -r requirements.txt
pip install -e .
pytest tests/config_tests/test_config_loader.py -v
pytest tests/metadata -v
pytest tests/storage/test_version_utils.py -v
```

Notes:

- The command set above avoids tests that require private Tencent services or a real PostgreSQL instance.
- Integration tests that depend on private configs should be run only on a machine with local private YAML files.

## Fresh Clone Verification

After pushing the cleaned commits:

```bash
git clone <repo-url> /home/wnlab/langtaosha/Langtaosha_git_new
cd /home/wnlab/langtaosha/Langtaosha_git_new
pip install -r requirements.txt
pip install -e .
pytest tests/config_tests/test_config_loader.py -v
pytest tests/metadata -v
pytest tests/storage/test_version_utils.py -v
```

Success criteria:

- install succeeds from repository files alone
- example config is present and usable for config tests
- no private YAML is required for the selected verification suite
