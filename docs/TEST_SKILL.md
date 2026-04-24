---
name: repo-test-skill
description: Use when adding or editing tests in this repository, especially for storage, metadata, and indexing flows that should reuse shared pytest fixtures, real test_data payloads, and the standard _test.yaml config path.
---

# Repo Test Skill

## Use This Skill When

- Adding a new test under `tests/`
- Refactoring an existing test file to match current repository conventions
- Touching `MetadataDB`, `VectorDB`, `PaperIndexer`, or `MetadataTransformer`
- Needing shared paper payloads from `test_data/`

## First Checks

1. Match the test path to the source path.
   `src/docset_hub/storage/*` -> `tests/storage/*`
   `src/docset_hub/indexing/*` -> `tests/indexing/*`
   `src/docset_hub/metadata/*` -> `tests/metadata/*`
2. Reuse existing test style before inventing a new one.
3. Prefer the shared fixtures in [conftest.py](/home/wnlab/langtaosha/Langtaosha_PD/tests/conftest.py).

## Shared Data Rules

- Real sample payloads come from repository-root `test_data/`.
- Shared pytest fixtures are:
  - `test_papers`
  - `test_paper_files`
- Source keys must stay consistent:
  - `langtaosha`
  - `biorxiv_history`
  - `biorxiv_daily`

## Config Rules

- Default config file is [config_tecent_backend_server_test.yaml](/home/wnlab/langtaosha/Langtaosha_PD/src/config/config_tecent_backend_server_test.yaml).
- Test-local config resolution should follow this order:
  1. `--config-path`
  2. test-specific environment variable
  3. default `_test.yaml`
- When parsing CLI args inside a test file, guard against pytest argv collisions with `parse_known_args()` and an `is_pytest` check.

## Fixture Rules

- Use `scope="session"` for expensive shared resources or immutable test data.
- Use `scope="function"` for DB cleanup and mutable service clients.
- Prefer fixture-driven setup over direct module-global initialization.
- If a test class needs one-time setup plus teardown, prefer a class-scoped autouse fixture with `yield`.

## Test Authoring Rules

- For dict-based ingest flows, use `test_papers`.
- For file-based ingest or transformer flows, use `test_paper_files`.
- Unit tests should isolate external systems with `Mock` or `patch` where practical.
- Integration tests must include cleanup for created DB rows, vector documents, or collections.
- Keep assertions focused on behavior, not incidental implementation details.

## Avoid

- Duplicating `load_test_papers()` in new test files
- Reintroducing `tests/db/` now that storage tests live under `tests/storage/`
- Mixing old source labels like `bio_arxiv` into new storage/indexing tests
- Leaving real-service test data behind after a test run

## Verification Workflow

1. Run `python3 -m py_compile` for changed test files when doing structural refactors.
2. Run targeted pytest on affected files first.
3. Use the shared `_test.yaml` config when real services are required.
4. If a cleanup-heavy integration suite fails midway, verify DB/vector cleanup before finishing.
