# Langtaosha Retrieval Feedback JSON Testbed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a JSON-first retrieval feedback testbed pipeline where `import_retrieval_feedback_testbed.py` exports frozen `query -> work_id -> label` artifacts and `run_retrieval_testbed.py` reads those artifacts directly to evaluate multiple retrieval strategies.

**Architecture:** Treat Study Mode tables as raw evidence only. The import step resolves topic-query feedback into a portable JSON artifact with summary metadata and per-query labels. The run step reads that JSON, calls `PaperIndexer` strategies against the configured corpus, and emits per-query, per-strategy ranked outputs annotated with known labels plus aggregate metrics. Database persistence becomes optional support infrastructure rather than the primary evaluation path.

**Tech Stack:** Python 3, PostgreSQL, SQLAlchemy, `PaperIndexer`, pytest, JSON reports, Markdown implementation logs.

**Design Reference:** `docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md`

---

## File Structure

Modify:

```text
scripts/import_retrieval_feedback_testbed.py
scripts/run_retrieval_testbed.py
src/docset_hub/evaluation/contracts.py
src/docset_hub/evaluation/runner.py
tests/scripts/test_import_retrieval_feedback_testbed.py
tests/scripts/test_run_retrieval_testbed.py
tests/evaluation/test_runner.py
docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md
```

Create:

```text
src/docset_hub/evaluation/json_testbed.py
tests/evaluation/test_json_testbed.py
```

Keep as-is unless a later task proves they are still needed:

```text
src/docset_hub/evaluation/testbed_repository.py
database/migrations/20260608_retrieval_feedback_testbed.sql
database/schema.sql
```

Implementation rule for this revision:

- `import_retrieval_feedback_testbed.py` reads and writes within one metadata DB only.
- The import script must always write a frozen JSON artifact.
- `run_retrieval_testbed.py` must accept that JSON artifact as its primary input.
- Per-query output must include returned `work_id`, `rank`, `score`, and `label` where `label in {1, 0, null}`.
- Aggregate metrics continue to use known-judgment semantics: unjudged results are unknown, not negative.

---

### Task 1: Define JSON Testbed Contract

**Files:**
- Create: `src/docset_hub/evaluation/json_testbed.py`
- Create: `tests/evaluation/test_json_testbed.py`
- Modify: `src/docset_hub/evaluation/contracts.py`

- [ ] **Step 1: Write the failing JSON contract tests**

Add tests covering:

```python
def test_build_testbed_document_serializes_summary_and_queries():
    queries = [
        TestbedQuery(query_id=1, query_text="brain computer interface", judgments={"W1": 1, "W2": 0}),
    ]
    document = build_testbed_document(
        testbed_name="topic-v1",
        query_type="topic",
        source_environment="mimic",
        config_path="src/config/config_tecent_backend_server_mimic.yaml",
        config_fingerprint={"metadata_db_name": "langtaosha_mimic"},
        summary={"raw_feedback_count": 10},
        queries=queries,
    )
    assert document["queries"][0]["labels"] == [
        {"work_id": "W1", "label": 1},
        {"work_id": "W2", "label": 0},
    ]


def test_load_testbed_queries_round_trips_json_labels(tmp_path):
    payload = {
        "testbed_name": "topic-v1",
        "queries": [
            {
                "query_id": 1,
                "query_text": "synapse",
                "labels": [{"work_id": "W1", "label": 1}, {"work_id": "W9", "label": 0}],
            }
        ],
    }
    path = tmp_path / "testbed.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    loaded = load_testbed_queries(path)
    assert loaded[0].judgments == {"W1": 1, "W9": 0}
```

- [ ] **Step 2: Run the new tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_json_testbed.py -v
```

Expected: FAIL because `json_testbed.py` does not exist.

- [ ] **Step 3: Add explicit JSON helpers**

Implement in `src/docset_hub/evaluation/json_testbed.py`:

```python
def build_testbed_document(
    *,
    testbed_name: str,
    query_type: str,
    source_environment: str,
    config_path: str,
    config_fingerprint: dict[str, Any],
    summary: dict[str, Any],
    queries: Sequence[TestbedQuery],
) -> dict[str, Any]: ...

def save_testbed_document(path: Path, payload: Mapping[str, Any]) -> None: ...

def load_testbed_document(path: Path) -> dict[str, Any]: ...

def load_testbed_queries(path: Path) -> list[TestbedQuery]: ...
```

JSON query item shape:

```json
{
  "query_id": 1,
  "query_text": "brain computer interface",
  "labels": [
    {"work_id": "W1", "label": 1},
    {"work_id": "W2", "label": 0}
  ]
}
```

- [ ] **Step 4: Keep contract types focused**

In `contracts.py`, keep:

```python
@dataclass(frozen=True)
class TestbedQuery:
    query_id: int
    query_text: str
    judgments: dict[str, int]
```

Do not add DB-only fields to `TestbedQuery`.

- [ ] **Step 5: Run compile and tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile src/docset_hub/evaluation/json_testbed.py src/docset_hub/evaluation/contracts.py
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_json_testbed.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/docset_hub/evaluation/json_testbed.py src/docset_hub/evaluation/contracts.py tests/evaluation/test_json_testbed.py
git commit -m "feat: add json retrieval testbed contract"
```

---

### Task 2: Refactor Import Script to Export Frozen JSON

**Files:**
- Modify: `scripts/import_retrieval_feedback_testbed.py`
- Modify: `tests/scripts/test_import_retrieval_feedback_testbed.py`

- [ ] **Step 1: Write the failing import-script tests**

Extend the script test so `main()` writes a JSON file with:

```python
assert payload["testbed_name"] == "topic-v1"
assert payload["source_environment"] == "mimic"
assert payload["queries"][0]["query_text"] == "brain computer interface"
assert payload["queries"][0]["labels"] == [{"work_id": "W1", "label": 1}]
```

Mock `select_topic_feedback()` and `resolve_feedback_with_report()` to return one resolved query/document label pair. Do not require any DB write assertions in this task.

- [ ] **Step 2: Run the import-script tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/scripts/test_import_retrieval_feedback_testbed.py -v
```

Expected: FAIL because the script still writes only summary metadata.

- [ ] **Step 3: Simplify CLI to single-DB JSON export**

Keep or confirm these arguments:

```text
--config-path
--confirm-database
--origin-environment
--freeze-version-name
--include-unknown-route
--output-report
```

Behavior:

- read feedback from the metadata DB referenced by `--config-path`
- resolve topic-query judgments
- convert resolved judgments into `TestbedQuery` objects
- write a JSON artifact to `--output-report`

- [ ] **Step 4: Remove JSON generation from DB persistence concerns**

The main path must look like:

```python
raw_feedback = feedback_repository.load_raw_feedback(...)
selected_feedback = select_topic_feedback(raw_feedback, ...)
resolved_judgments, resolution_report = resolve_feedback_with_report(selected_feedback)
queries = build_testbed_queries_from_resolved_judgments(resolved_judgments)
payload = build_testbed_document(...)
save_testbed_document(output_path, payload)
```

If optional DB persistence remains in the file, guard it behind a non-default flag and keep it out of the main path.

- [ ] **Step 5: Print a compact import summary**

At the end, print a JSON summary including:

```json
{
  "config_path": "...",
  "database_name": "langtaosha_mimic",
  "feedback_origin_environment": "mimic",
  "query_count": 29,
  "positive_count": 99,
  "negative_count": 109,
  "output_report": "local_data/retrieval_testbed/import_topic_v1_mimic.json"
}
```

- [ ] **Step 6: Run compile and tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile scripts/import_retrieval_feedback_testbed.py
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/scripts/test_import_retrieval_feedback_testbed.py tests/evaluation/test_json_testbed.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scripts/import_retrieval_feedback_testbed.py tests/scripts/test_import_retrieval_feedback_testbed.py src/docset_hub/evaluation/json_testbed.py tests/evaluation/test_json_testbed.py
git commit -m "feat: export retrieval feedback testbed json"
```

---

### Task 3: Refactor Runner to Read Testbed JSON

**Files:**
- Modify: `scripts/run_retrieval_testbed.py`
- Modify: `tests/scripts/test_run_retrieval_testbed.py`

- [ ] **Step 1: Write the failing runner-script tests**

Replace the repository-backed test with a JSON-backed test:

```python
payload = {
    "testbed_name": "topic-v1",
    "queries": [
        {
            "query_id": 1,
            "query_text": "synapse",
            "labels": [{"work_id": "W1", "label": 1}],
        }
    ],
}
```

Expect `parse_args()` to accept:

```text
--evaluation-config-path
--confirm-evaluation-database
--testbed-json
--strategies
--top-k
--ks
--output-dir
```

- [ ] **Step 2: Run the runner-script tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/scripts/test_run_retrieval_testbed.py -v
```

Expected: FAIL because the script still expects `--testbed-version-id`.

- [ ] **Step 3: Change the runner CLI to JSON input**

Update `run_retrieval_testbed.py` so:

```text
--testbed-json
```

replaces:

```text
--testbed-version-id
```

The runner must load queries via `load_testbed_queries(Path(args.testbed_json))`.

- [ ] **Step 4: Keep evaluation DB checks, but use DB only for retrieval**

The script should still:

- load evaluation config
- confirm metadata DB name
- construct `PaperIndexer`

But it must not query `retrieval_testbed_*` tables to get labels.

- [ ] **Step 5: Preserve comparison-level metadata**

The output JSON top-level should include:

```json
{
  "testbed_name": "topic-v1",
  "testbed_json": ".../topic-v1.json",
  "evaluation_config_path": "...",
  "query_count": 29,
  "top_k": 10,
  "ks": [5, 10]
}
```

- [ ] **Step 6: Run compile and tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile scripts/run_retrieval_testbed.py
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/scripts/test_run_retrieval_testbed.py tests/evaluation/test_json_testbed.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scripts/run_retrieval_testbed.py tests/scripts/test_run_retrieval_testbed.py
git commit -m "feat: run retrieval evaluation from testbed json"
```

---

### Task 4: Expand Runner Output to Per-Query Per-Strategy Results

**Files:**
- Modify: `src/docset_hub/evaluation/runner.py`
- Modify: `tests/evaluation/test_runner.py`

- [ ] **Step 1: Write the failing runner behavior tests**

Add a test asserting each run result includes:

```python
assert outcome["per_query"][0]["query_text"] == "query"
assert outcome["per_query"][0]["results"][0] == {
    "work_id": "W1",
    "rank": 1,
    "score": 0.9,
    "label": 1,
    "is_judged": True,
    "retrieval_debug": {},
}
```

and for unjudged results:

```python
assert outcome["per_query"][0]["results"][1]["label"] is None
```

- [ ] **Step 2: Run the targeted runner tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_runner.py::test_runner_persists_ranked_results_and_aggregate_metrics -v
```

Expected: FAIL because `run_queries()` does not yet return per-query result payloads.

- [ ] **Step 3: Return per-query diagnostic payloads from the runner**

Make `run_queries()` return:

```python
{
    "run_id": run_id,
    "status": status,
    "aggregate_metrics": aggregate_metrics,
    "query_failures": query_failures,
    "per_query": [
        {
            "query_id": query.query_id,
            "query_text": query.query_text,
            "metrics": metrics,
            "results": rows,
        }
    ],
}
```

Use the serialized rows from `_serialize_ranked_documents()`, but rename `relevance` to `label` in the outward-facing `results` payload.

- [ ] **Step 4: Keep repository writes optional**

Do not remove repository support entirely, but make sure `run_queries()` still works when the repository only records runs and the caller primarily consumes the returned `per_query` payload.

- [ ] **Step 5: Run compile and tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile src/docset_hub/evaluation/runner.py
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_runner.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/docset_hub/evaluation/runner.py tests/evaluation/test_runner.py
git commit -m "feat: return per-query retrieval diagnostics"
```

---

### Task 5: Align Design and Usage Docs with JSON-First Flow

**Files:**
- Modify: `docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md`
- Modify: `docs/implementation_log/20260608/retrieval_feedback_testbed_implementation_plan_20260608.md`

- [ ] **Step 1: Update the design wording**

Revise the design doc so the primary pipeline is:

```text
Study Mode tables
-> import_retrieval_feedback_testbed.py
-> frozen testbed JSON
-> run_retrieval_testbed.py
-> comparison JSON
```

Clarify that DB-backed frozen versions are optional support infrastructure in v1 rather than the mandatory evaluation source.

- [ ] **Step 2: Add canonical example commands**

Document these commands in the design or a nearby log section:

```bash
python scripts/import_retrieval_feedback_testbed.py \
  --config-path src/config/config_tecent_backend_server_mimic.yaml \
  --confirm-database langtaosha_mimic \
  --origin-environment mimic \
  --freeze-version-name topic-v1 \
  --output-report local_data/retrieval_testbed/import_topic_v1_mimic.json

python scripts/run_retrieval_testbed.py \
  --evaluation-config-path src/config/config_tecent_backend_server_mimic.yaml \
  --confirm-evaluation-database langtaosha_mimic \
  --testbed-json local_data/retrieval_testbed/import_topic_v1_mimic.json \
  --strategies dense sparse hybrid hybrid_retrieval \
  --top-k 10 \
  --ks 5 10 \
  --output-dir local_data/retrieval_testbed/runs/topic_v1_mimic
```

- [ ] **Step 3: Sanity-check plan coverage**

Verify the plan covers:

- JSON artifact schema
- import refactor
- runner refactor
- per-query output
- doc updates

- [ ] **Step 4: Commit**

```bash
git add docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md docs/implementation_log/20260608/retrieval_feedback_testbed_implementation_plan_20260608.md
git commit -m "docs: update retrieval testbed plan for json-first flow"
```

---

## Self-Review

- Spec coverage check: covered import artifact shape, JSON-backed evaluation input, per-query strategy output, and documentation updates.
- Placeholder scan: removed `TBD`/`TODO` language and gave concrete CLI, file paths, and payload shapes.
- Type consistency: `TestbedQuery` remains the shared query carrier across JSON helpers, import, and evaluation.

## Recommended Execution Order

1. Task 1 (`json_testbed.py`)
2. Task 2 (`import_retrieval_feedback_testbed.py`)
3. Task 4 (`runner.py`)
4. Task 3 (`run_retrieval_testbed.py`)
5. Task 5 (docs)

Task 4 comes before Task 3 in execution even though the file order above is different, because the runner must expose per-query payloads before the CLI can serialize them cleanly.

---

## Follow-up Plan: Feedback Review Page on Testbed JSON

**Date:** 2026-06-08

**Goal:** Move `/feedback-review` out of `app/main.py` into a dedicated module and switch its data source from the legacy case-study JSONL export to `local_data/retrieval_testbed/import_topic_v1_mimic.json`, with metadata hydrated from the current metadata DB by `work_id`.

**Architecture:** The feedback review page becomes a query-level testbed review surface instead of a historical search-event replay surface. Query IDs, annotators, and labels come from the frozen testbed JSON. Human-readable paper metadata comes from a batch database lookup keyed by the labeled `work_id` values. The page URL and route stay unchanged.

**Files:**
- Create: `app/feedback_review_page.py`
- Modify: `app/main.py`
- Modify: `templates/feedback_review.html`
- Create: `tests/app/test_feedback_review_page.py`

### Task 6: Split Feedback Review into a Dedicated Module

- [x] Add `app/feedback_review_page.py` with:
  - `DEFAULT_FEEDBACK_REVIEW_TESTBED_PATH`
  - `load_feedback_review_testbed(path)`
  - `hydrate_feedback_review_queries(testbed_payload, engine)`
  - `register_feedback_review_routes(app, api_success, api_error)`
- [x] Keep `/feedback-review` and `/api/study/feedback-review-data` URLs unchanged.
- [x] Change the API payload from:
  - `searches`
  to:
  - `testbed_name`
  - `summary`
  - `queries`
  - `source`
- [x] Batch-query metadata DB by labeled `work_id` to attach:
  - `title`
  - `abstract`
  - `authors`
  - `source`
  - `online_date`
  - `doi`
  - `link`
- [x] Update `templates/feedback_review.html` to use query-level testbed fields:
  - `query_id`
  - `annotator_ids`
  - `annotator_count`
  - `query_text`
  - `label_summary`
  - `results[{work_id,label,title,...}]`
- [x] Remove UI controls that depend on legacy search-event structure:
  - `User ID`
  - `Search Event`
  - `Qualified feedback only`
- [x] Add backend tests that prove:
  - the loader reads the configured testbed JSON
  - metadata hydration joins `work_id` to display fields
  - the API returns `queries`, not legacy `searches`

### Task 6 Execution Note

**Implemented on:** 2026-06-08

What landed:

- `/feedback-review` now renders through `app/feedback_review_page.py` instead of carrying the page logic directly inside `app/main.py`.
- Added standalone entrypoints:
  - `app/feedback_review_app.py`
  - `app/run_feedback_review.py`
- `/api/study/feedback-review-data` now reads the frozen testbed JSON artifact, defaulting to `local_data/retrieval_testbed/import_topic_v1_mimic.json`.
- The route supports override through `FEEDBACK_REVIEW_TESTBED_PATH`, so we can point the page at another frozen artifact without changing code.
- Query-level labels still come from JSON only; display metadata is hydrated in batch from the current metadata DB by `work_id`.
- The page is now aligned with the retrieval-testbed workflow rather than the older search-event replay workflow.

Standalone run command:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD
export PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_mimic.yaml
export FEEDBACK_REVIEW_TESTBED_PATH=/home/wnlab/langtaosha/Langtaosha_PD/local_data/retrieval_testbed/import_topic_v1_mimic.json
python app/run_feedback_review.py
```

Verification:

- `python -m py_compile app/feedback_review_page.py app/main.py`
- `PYTHONPATH=/home/wnlab/langtaosha/Langtaosha_PD:/home/wnlab/langtaosha/Langtaosha_PD/src python -m pytest tests/app/test_feedback_review_page.py -v`
- `PYTHONPATH=/home/wnlab/langtaosha/Langtaosha_PD:/home/wnlab/langtaosha/Langtaosha_PD/src python -m pytest tests/app/test_search_api_contract.py -v`
