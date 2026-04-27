---
name: repo-test-skill
description: Repository testing workflow for Langtaosha_PD. Use whenever Codex creates, edits, reviews, debugs, or runs tests under Langtaosha_PD/tests, or touches test-adjacent code in src/docset_hub/storage, src/docset_hub/indexing, src/docset_hub/metadata, MetadataDB, VectorDB, PaperIndexer, MetadataTransformer, shared pytest fixtures, test_data payloads, or config_tecent_backend_server_test.yaml. Enforces shared fixtures, real repository test payloads, source-name consistency, cleanup for real services, and targeted pytest verification.
---

# Repo Test Skill

## Discovery Requirement

For automatic Codex skill discovery, install this content as:

`$CODEX_HOME/skills/repo-test-skill/SKILL.md`

or:

`~/.codex/skills/repo-test-skill/SKILL.md`

A repository documentation file named `TEST_SKILL.md` is useful reference material, but it is not guaranteed to trigger automatically unless the user explicitly links it or another installed skill points to it.

## Mandatory Workflow

1. Start from repository context:
   - Work from `/home/wnlab/langtaosha/Langtaosha_PD` when running tests or resolving paths.
   - Inspect the existing neighboring test file before writing a new pattern.
   - Read `/home/wnlab/langtaosha/Langtaosha_PD/tests/conftest.py` before adding test-data loaders or fixtures.
2. Place tests by source ownership:
   - `src/docset_hub/storage/*` -> `tests/storage/*`
   - `src/docset_hub/indexing/*` -> `tests/indexing/*`
   - `src/docset_hub/metadata/*` -> `tests/metadata/*`
   - repository scripts -> `tests/scripts/*`
   - cross-component behavior -> `tests/integration/*`
3. Reuse shared fixtures and real payloads:
   - Use `test_papers` for dict-based ingest/index/metadata flows.
   - Use `test_paper_files` for file-based ingest, transformer, or indexer flows.
   - Do not duplicate `load_test_papers()` in new test files.
4. Choose isolation level deliberately:
   - Unit tests should mock external services, network calls, LLM clients, vector DB calls, and slow storage where behavior can be asserted locally.
   - Integration tests may use real DB/vector services only when the behavior cannot be proven with mocks.
   - Any test that creates DB rows, vector documents, collections, generated keywords, or source records must include cleanup in a `yield` fixture or `finally` block.
5. Verify with the smallest useful command first:
   - Structural Python-only change: `python3 -m py_compile <changed files>`.
   - Test behavior change: `python3 -m pytest <affected test file or node>`.
   - Shared fixture or cross-module change: run the affected directory after the targeted file passes.
6. Report verification honestly in the final response:
   - List the exact compile/pytest commands run.
   - Say whether they passed, failed, or were not run.
   - If real-service tests fail or are skipped because services/config are unavailable, say that explicitly.

## Shared Data Rules

- Real sample payloads come from repository-root `/home/wnlab/langtaosha/Langtaosha_PD/test_data/`.
- Shared pytest fixtures from `/home/wnlab/langtaosha/Langtaosha_PD/tests/conftest.py` are:
  - `test_papers`
  - `test_paper_files`
- Source keys must stay exactly:
  - `langtaosha`
  - `biorxiv_history`
  - `biorxiv_daily`
- Use `copy.deepcopy()` before mutating fixture payloads.
- Make payloads unique when inserting into real storage by changing stable fields such as title, DOI, date, version, or source-specific identifier.

## Config Rules

- Default test config is `/home/wnlab/langtaosha/Langtaosha_PD/src/config/config_tecent_backend_server_test.yaml`.
- Test-local config resolution should follow this order:
  1. `--config-path`
  2. test-specific environment variable
  3. default `_test.yaml`
- When parsing CLI args inside a test file, use `parse_known_args()` and an `is_pytest` check to avoid pytest argv collisions.
- Do not silently switch tests to a production config.

## Fixture Rules

- Use `scope="session"` for expensive shared resources or immutable test data.
- Use `scope="function"` for DB cleanup and mutable service clients.
- Prefer fixture-driven setup over direct module-global initialization.
- If a test class needs one-time setup plus teardown, prefer a class-scoped autouse fixture with `yield`.
- Put cleanup as close as possible to the resource creation site.

## Test Authoring Rules

- For dict-based ingest flows, use `test_papers`.
- For file-based ingest or transformer flows, use `test_paper_files`.
- Unit tests should isolate external systems with `Mock` or `patch` where practical.
- Integration tests must include cleanup for created DB rows, vector documents, or collections.
- Keep assertions focused on behavior, not incidental implementation details.
- Prefer existing repository helpers and factories over new local helper functions.
- Prefer precise assertions over broad "not None" checks when the expected contract is known.
- Use source labels consistently; do not introduce aliases unless the production code explicitly supports them.

## Hard Stops

Stop and inspect existing code before proceeding if a test change would:

- Duplicate `load_test_papers()` outside `tests/conftest.py`.
- Add a new test directory such as `tests/db/` instead of using `tests/storage/`.
- Use old source labels such as `bio_arxiv` in storage, metadata, or indexing tests.
- Require production config, production data, or destructive cleanup.
- Leave real-service test data behind after a failed run.
- Depend on network access, live LLM calls, or external services without an explicit integration-test reason.

## Command Patterns

Run commands from `/home/wnlab/langtaosha/Langtaosha_PD`.

```bash
python3 -m py_compile tests/<area>/<test_file>.py
python3 -m pytest tests/<area>/<test_file>.py
python3 -m pytest tests/<area>/<test_file>.py::test_name
```

For real-service integration suites, ensure the test config path resolves to:

```bash
src/config/config_tecent_backend_server_test.yaml
```

If a cleanup-heavy integration suite fails midway, inspect and clean only records/documents created by the test identifiers. Do not delete broad collections, source rows, or unrelated user data.

## Final Response Checklist

Before finishing, include:

- What test files or fixtures changed.
- Which shared fixtures or real payload sources were used.
- Exact verification commands and outcomes.
- Any remaining service/config cleanup risk.
