# Retrieval Testbed Filtered IDs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make retrieval branches honor an explicit `candidate_work_ids` filter so a labeled candidate set can be pushed all the way down into Tencent VectorDB and DB-backed recall paths.

**Architecture:** Treat `candidate_work_ids` as a first-class retrieval constraint in `PaperIndexer` and every downstream retrieval helper. Dense and sparse VectorDB searches will translate the list into Tencent VectorDB `work_id` filters, while keyword lookup and expanded sparse SQL helpers will add `p.work_id IN (...)` clauses. A final in-memory guard will keep hybrid fusion from leaking any out-of-scope document if one branch misbehaves.

**Tech Stack:** Python 3, pytest, Tencent VectorDB filters, SQLAlchemy text queries, `PaperIndexer`, Markdown implementation logs.

**Design References:**
- `docs/implementation_log/20260610/retrieval_testbed_labeled_candidate_scope_implementation_plan_20260610.md`
- `docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md`

---

## Decision

This plan only covers the filtered-IDs retrieval plumbing.

It assumes a caller already has a `candidate_work_ids` list for the current query and wants every retrieval branch to respect it. This means the plan does not define the CLI, runner, or testbed selection logic that decides when the filter is used.

Use the following normalization rule everywhere:

```text
candidate_work_ids -> sorted unique non-empty string work_ids
```

Keep full-corpus behavior available by passing `candidate_work_ids=None` or an empty list.

## File Structure

Modify:

```text
src/docset_hub/evaluation/search_strategies.py
src/docset_hub/indexing/paper_indexer.py
src/docset_hub/indexing/paper_keyword_lookup.py
src/docset_hub/indexing/expanded_sparse_retrieval.py
src/docset_hub/storage/vector_db.py
src/docset_hub/storage/vector_db_client.py
src/docset_hub/storage/metadata_db.py
tests/evaluation/test_search_strategies.py
tests/storage/test_vector_db_candidate_filters.py
tests/storage/test_metadata_db_candidate_filters.py
```

Create:

```text
tests/storage/test_vector_db_candidate_filters.py
tests/storage/test_metadata_db_candidate_filters.py
```

Implementation rules:

- `candidate_work_ids` must be optional everywhere.
- Empty candidate lists must behave like no filter.
- Hybrid retrieval must never emit a document outside the candidate set.
- Dense and sparse VectorDB searches should push the filter into the search request, not post-filter after the search.

---

### Task 1: Add Candidate ID Pass-Through to Search Strategies

**Files:**
- Modify: `src/docset_hub/evaluation/search_strategies.py`
- Modify: `tests/evaluation/test_search_strategies.py`

- [ ] **Step 1: Write failing strategy tests**

Add these tests to `tests/evaluation/test_search_strategies.py`:

```python
def test_paper_indexer_strategy_passes_candidate_work_ids():
    fake_indexer = FakeIndexer()
    strategy = PaperIndexerSearchStrategy(indexer=fake_indexer, search_type="dense")

    strategy.search("synapse", top_k=10, candidate_work_ids=["W1", "W2"])

    assert fake_indexer.search_calls[0]["candidate_work_ids"] == ["W1", "W2"]


def test_hybrid_retrieval_strategy_passes_candidate_work_ids():
    fake_indexer = FakeIndexer()
    strategy = HybridRetrievalSearchStrategy(indexer=fake_indexer, source_list=["biorxiv_history"])

    strategy.search("synapse", top_k=10, candidate_work_ids=["W1"])

    assert fake_indexer.hybrid_calls[0]["candidate_work_ids"] == ["W1"]
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_search_strategies.py -v
```

Expected: FAIL because the strategy methods do not accept `candidate_work_ids`.

- [ ] **Step 3: Add pass-through parameters**

Change the strategy method signatures to:

```python
def search(self, query: str, top_k: int, candidate_work_ids: Optional[list[str]] = None) -> list[RankedDocument]:
```

Pass the argument through to `PaperIndexer.search()` and `PaperIndexer.hybrid_retrieval_search()`:

```python
rows = self.indexer.search(
    query=query,
    source_list=self.source_list,
    top_k=top_k,
    hydrate=False,
    search_type=self.search_type,
    candidate_work_ids=candidate_work_ids,
)
```

```python
rows = self.indexer.hybrid_retrieval_search(
    query=query,
    source_list=self.source_list,
    top_k=top_k,
    hydrate=False,
    candidate_work_ids=candidate_work_ids,
)
```

Update the existing expected call dictionaries in tests to include:

```python
"candidate_work_ids": None,
```

- [ ] **Step 4: Run tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_search_strategies.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/docset_hub/evaluation/search_strategies.py tests/evaluation/test_search_strategies.py
git commit -m "feat: pass candidate work ids through search strategies"
```

---

### Task 2: Push Candidate IDs Into Tencent VectorDB Search

**Files:**
- Modify: `src/docset_hub/storage/vector_db_client.py`
- Modify: `src/docset_hub/storage/vector_db.py`
- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Create: `tests/storage/test_vector_db_candidate_filters.py`

- [ ] **Step 1: Write filter builder tests**

Create `tests/storage/test_vector_db_candidate_filters.py`:

```python
from __future__ import annotations

from src.docset_hub.storage.vector_db import VectorDB


def test_build_work_id_filter_returns_none_for_empty_values():
    assert VectorDB._build_work_id_filter(None) is None
    assert VectorDB._build_work_id_filter([]) is None


def test_build_work_id_filter_uses_equality_for_single_candidate():
    assert VectorDB._build_work_id_filter(["W1"]) == 'work_id="W1"'


def test_build_work_id_filter_uses_in_for_multiple_candidates():
    assert VectorDB._build_work_id_filter(["W2", "W1", "W1"]) == 'work_id in ("W1", "W2")'
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/storage/test_vector_db_candidate_filters.py -v
```

Expected: FAIL because `_build_work_id_filter()` does not exist.

- [ ] **Step 3: Add dense search `filter` support in the client**

In `src/docset_hub/storage/vector_db_client.py`, update `search_documents()` to accept:

```python
filter: Optional[str] = None,
```

Build the search payload as:

```python
search = {
    "embeddingItems": [query_text],
    "limit": limit,
    "retrieveVector": retrieve_vector,
    "outputFields": output_fields,
}
if filter:
    search["filter"] = filter
```

- [ ] **Step 4: Add a work-id filter helper and wire it into VectorDB**

In `src/docset_hub/storage/vector_db.py`, add:

```python
@staticmethod
def _build_work_id_filter(candidate_work_ids: Optional[List[str]]) -> Optional[str]:
    if not candidate_work_ids:
        return None
    values = sorted({str(work_id) for work_id in candidate_work_ids if str(work_id)})
    if not values:
        return None

    def quote(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    if len(values) == 1:
        return f'work_id={quote(values[0])}'
    return f'work_id in ({", ".join(quote(value) for value in values)})'
```

Add `candidate_work_ids: Optional[List[str]] = None` to:

```python
dense_search(...)
sparse_search(...)
hybrid_search(...)
search(...)
```

Pass the built filter into the client calls:

```python
filter=work_id_filter
```

For `hybrid_search()`, pass the same candidate list into both dense and sparse branches.

- [ ] **Step 5: Thread candidate IDs through PaperIndexer**

In `src/docset_hub/indexing/paper_indexer.py`, add `candidate_work_ids: Optional[List[str]] = None` to:

```python
search(...)
hybrid_retrieval_search(...)
```

Pass the argument through to `VectorDB.search()` and to the hybrid branches.

- [ ] **Step 6: Run targeted compile and tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile src/docset_hub/storage/vector_db_client.py src/docset_hub/storage/vector_db.py src/docset_hub/indexing/paper_indexer.py
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/storage/test_vector_db_candidate_filters.py tests/evaluation/test_search_strategies.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/docset_hub/storage/vector_db_client.py src/docset_hub/storage/vector_db.py src/docset_hub/indexing/paper_indexer.py tests/storage/test_vector_db_candidate_filters.py
git commit -m "feat: filter vector retrieval by candidate work ids"
```

---

### Task 3: Restrict DB-Backed Recall Helpers by Candidate IDs

**Files:**
- Modify: `src/docset_hub/indexing/paper_keyword_lookup.py`
- Modify: `src/docset_hub/indexing/expanded_sparse_retrieval.py`
- Modify: `src/docset_hub/storage/metadata_db.py`
- Create: `tests/storage/test_metadata_db_candidate_filters.py`

- [ ] **Step 1: Write SQL helper tests**

Create `tests/storage/test_metadata_db_candidate_filters.py`:

```python
from __future__ import annotations

from src.docset_hub.storage.metadata_db import MetadataDB


def test_keyword_lookup_work_id_filter_returns_empty_for_empty_values():
    params: dict[str, object] = {}

    assert MetadataDB._keyword_lookup_work_id_filter("p.work_id", [], params, "candidate") == ""
    assert params == {}


def test_keyword_lookup_work_id_filter_binds_sorted_unique_values():
    params: dict[str, object] = {}

    sql = MetadataDB._keyword_lookup_work_id_filter("p.work_id", ["W2", "W1", "W1"], params, "candidate")

    assert sql == "p.work_id IN (:candidate_0, :candidate_1)"
    assert params == {"candidate_0": "W1", "candidate_1": "W2"}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/storage/test_metadata_db_candidate_filters.py -v
```

Expected: FAIL because `_keyword_lookup_work_id_filter()` does not exist.

- [ ] **Step 3: Add the SQL helper**

In `src/docset_hub/storage/metadata_db.py`, add:

```python
@classmethod
def _keyword_lookup_work_id_filter(
    cls,
    column: str,
    candidate_work_ids: Sequence[str],
    params: Dict[str, Any],
    prefix: str,
) -> str:
    values = sorted({str(work_id) for work_id in candidate_work_ids if str(work_id)})
    if not values:
        return ""
    placeholders = []
    for index, value in enumerate(values):
        param_name = f"{prefix}_{index}"
        placeholders.append(f":{param_name}")
        params[param_name] = value
    return f"{column} IN ({', '.join(placeholders)})"
```

- [ ] **Step 4: Add candidate filters to keyword lookup queries**

Add `candidate_work_ids: Optional[Sequence[str]] = None` to:

```python
lookup_papers_by_keyword_terms(...)
lookup_papers_by_keyword_lookup_terms(...)
```

Add a candidate filter built from `p.work_id` and use it in the final SQL `WHERE`/`HAVING` path so only the candidate papers can survive to the final grouped result.

- [ ] **Step 5: Add candidate filters to expanded sparse queries**

Add `candidate_work_ids: Optional[Sequence[str]] = None` to:

```python
lookup_papers_by_expanded_sparse_groups(...)
```

Apply the candidate filter on every `papers p` join path so title, abstract, and keyword matches only evaluate candidate papers.

- [ ] **Step 6: Pass candidate IDs through the indexing wrappers**

In `src/docset_hub/indexing/paper_keyword_lookup.py`, add `candidate_work_ids` to:

```python
match_paper_keywords_using_span_matcher(...)
match_paper_keywords_with_lookup_plan(...)
```

In `src/docset_hub/indexing/expanded_sparse_retrieval.py`, add `candidate_work_ids` to:

```python
match_papers_by_expanded_sparse_plan(...)
```

Pass the list straight through to `MetadataDB`.

- [ ] **Step 7: Guard hybrid fusion output**

In `src/docset_hub/indexing/paper_indexer.py`, add an optional `candidate_work_ids` check inside hybrid merge so any branch output not in the candidate set is skipped before `SearchResult` objects are emitted.

- [ ] **Step 8: Run targeted compile and tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile src/docset_hub/indexing/paper_keyword_lookup.py src/docset_hub/indexing/expanded_sparse_retrieval.py src/docset_hub/storage/metadata_db.py
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/storage/test_metadata_db_candidate_filters.py tests/storage/test_vector_db_candidate_filters.py tests/evaluation/test_search_strategies.py -v
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/docset_hub/indexing/paper_keyword_lookup.py src/docset_hub/indexing/expanded_sparse_retrieval.py src/docset_hub/storage/metadata_db.py tests/storage/test_metadata_db_candidate_filters.py
git commit -m "feat: restrict db-backed recall by candidate work ids"
```

---

## Acceptance Criteria

- Search strategies accept `candidate_work_ids` and pass them through.
- Dense and sparse VectorDB search calls can apply Tencent VectorDB `work_id` filters.
- DB-backed keyword lookup and expanded sparse helpers can restrict paper recall with `p.work_id IN (...)`.
- Hybrid retrieval does not return a `work_id` outside the candidate set.
- `None` or empty candidate lists behave like the current full-corpus path.

## Self-Review Notes

- Spec coverage: The plan covers the strategy adapter, VectorDB search filters, SQL-backed recall helpers, and the hybrid output guard.
- Placeholder scan: No TODO/TBD placeholders are left in the plan body.
- Type consistency: The filter parameter is consistently named `candidate_work_ids` everywhere, with `Optional[list[str]]` at strategy boundaries and `Optional[Sequence[str]]` for read-only DB helpers.
