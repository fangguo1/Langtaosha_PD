# Retrieval Testbed Labeled Candidate Scope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in evaluation mode that ranks each testbed query only within that query's labeled `work_id` set while preserving the current full-corpus mode as the default.

**Architecture:** Introduce `candidate_scope` at the evaluation runner and CLI boundary, with `corpus` keeping today's behavior and `labeled` passing `query.judgments.keys()` down as `candidate_work_ids`. VectorDB-backed dense, sparse, and hybrid retrieval will push `work_id` filters into Tencent VectorDB search requests; DB-backed keyword lookup and expanded sparse branches will add SQL `p.work_id IN (...)` constraints so every retrieval branch respects the same candidate set.

**Tech Stack:** Python 3, pytest, Tencent VectorDB search filters, SQLAlchemy text queries, `PaperIndexer`, JSON comparison reports, Markdown implementation logs.

**Design References:**
- `docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md`
- `docs/implementation_log/20260608/retrieval_feedback_testbed_implementation_plan_20260608.md`

---

## Decision

Use per-query labeled candidate scope:

```text
query candidate set = sorted(query.judgments.keys())
```

Do not use the union of all labeled docs across the testbed. That union changes the meaning of negatives and creates many query-document pairs with no judgment. Per-query scope directly answers: "Given the documents we have labels for this query, can the strategy rank positives above negatives?"

Keep current full-corpus behavior available and default:

```text
--candidate-scope corpus   # default, current behavior
--candidate-scope labeled  # new restricted-candidate behavior
```

## File Structure

Modify:

```text
scripts/evaluation/run_retrieval_testbed.py
src/docset_hub/evaluation/runner.py
src/docset_hub/evaluation/search_strategies.py
src/docset_hub/indexing/paper_indexer.py
src/docset_hub/indexing/paper_keyword_lookup.py
src/docset_hub/indexing/expanded_sparse_retrieval.py
src/docset_hub/storage/vector_db.py
src/docset_hub/storage/vector_db_client.py
src/docset_hub/storage/metadata_db.py
tests/evaluation/test_runner.py
tests/evaluation/test_search_strategies.py
tests/scripts/test_run_retrieval_testbed.py
docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md
```

Create:

```text
tests/storage/test_vector_db_candidate_filters.py
tests/storage/test_metadata_db_candidate_filters.py
```

Implementation rules:

- `corpus` mode must remain byte-for-byte compatible at the CLI contract level except for extra metadata fields in reports.
- `labeled` mode must never return a ranked result whose `work_id` is outside the query's labeled set.
- `labeled` mode must request `top_k = min(requested_top_k, len(candidate_work_ids))` at the strategy boundary so empty or tiny pools behave predictably.
- Empty candidate lists return no results and still record query metrics with the query's known judgments.
- Metrics remain known-judgment metrics; only the retrieval candidate universe changes.

---

### Task 1: Add Candidate Scope to Runner and CLI

**Files:**
- Modify: `src/docset_hub/evaluation/runner.py`
- Modify: `scripts/evaluation/run_retrieval_testbed.py`
- Modify: `tests/evaluation/test_runner.py`
- Modify: `tests/scripts/test_run_retrieval_testbed.py`

- [ ] **Step 1: Write failing runner tests**

Add this strategy double to `tests/evaluation/test_runner.py`:

```python
class CandidateAwareStrategy:
    def __init__(self):
        self.name = "dense"
        self.calls: list[dict] = []

    def search(self, query: str, top_k: int, candidate_work_ids: list[str] | None = None) -> list[RankedDocument]:
        self.calls.append(
            {
                "query": query,
                "top_k": top_k,
                "candidate_work_ids": candidate_work_ids,
            }
        )
        return [
            RankedDocument(work_id=work_id, rank=index + 1, score=1.0 - index)
            for index, work_id in enumerate(candidate_work_ids or ["NEW", "W1"])
        ][:top_k]
```

Add this test:

```python
def test_runner_labeled_candidate_scope_passes_query_judgment_work_ids():
    strategy = CandidateAwareStrategy()
    repository = _repo()
    runner = RetrievalEvaluationRunner(repository=repository)

    query = EvalQuery(query_id=1, query_text="query", judgments={"W2": 0, "W1": 1})
    outcome = runner.run_queries(
        strategy=strategy,
        queries=[query],
        top_k=10,
        ks=(1, 2),
        candidate_scope="labeled",
    )

    assert strategy.calls == [
        {
            "query": "query",
            "top_k": 2,
            "candidate_work_ids": ["W1", "W2"],
        }
    ]
    assert repository.runs[0]["candidate_scope"] == "labeled"
    assert outcome["per_query"][0]["candidate_count"] == 2
    assert {row["work_id"] for row in outcome["per_query"][0]["results"]} == {"W1", "W2"}
```

Add this compatibility test:

```python
def test_runner_corpus_candidate_scope_keeps_existing_search_signature():
    strategy = FakeStrategy("dense", {"query": [RankedDocument(work_id="W1", rank=1, score=0.9)]})
    repository = _repo()
    runner = RetrievalEvaluationRunner(repository=repository)

    query = EvalQuery(query_id=1, query_text="query", judgments={"W1": 1})
    runner.run_queries(strategy=strategy, queries=[query], top_k=3, ks=(1,), candidate_scope="corpus")

    assert strategy.calls == [("query", 3)]
    assert repository.runs[0]["candidate_scope"] == "corpus"
```

- [ ] **Step 2: Run runner tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_runner.py -v
```

Expected: FAIL because `run_queries()` does not accept `candidate_scope`.

- [ ] **Step 3: Implement runner candidate scope**

Change `run_queries()` signature in `src/docset_hub/evaluation/runner.py`:

```python
def run_queries(
    self,
    *,
    strategy: Any,
    queries: Sequence[TestbedQuery],
    top_k: int,
    ks: Sequence[int] = (5, 10),
    run_metadata: dict[str, Any] | None = None,
    candidate_scope: str = "corpus",
) -> dict[str, Any]:
```

Add validation near the start:

```python
if candidate_scope not in {"corpus", "labeled"}:
    raise ValueError("candidate_scope must be 'corpus' or 'labeled'")
```

Include the scope in `run_payload`:

```python
run_payload = {
    "strategy_name": getattr(strategy, "name", strategy.__class__.__name__),
    "requested_top_k": top_k,
    "candidate_scope": candidate_scope,
    "metadata": metadata,
}
```

Replace the strategy call in the query loop with:

```python
candidate_work_ids = None
effective_top_k = top_k
if candidate_scope == "labeled":
    candidate_work_ids = sorted(str(work_id) for work_id in query.judgments.keys())
    effective_top_k = min(max(0, int(top_k)), len(candidate_work_ids))

if candidate_scope == "labeled":
    ranked_documents = strategy.search(
        query.query_text,
        effective_top_k,
        candidate_work_ids=candidate_work_ids,
    )
else:
    ranked_documents = strategy.search(query.query_text, effective_top_k)
```

Add candidate metadata to each `per_query` success row:

```python
"candidate_scope": candidate_scope,
"candidate_count": len(candidate_work_ids) if candidate_work_ids is not None else None,
```

Add the same fields to failure rows.

- [ ] **Step 4: Add CLI argument and report metadata**

In `scripts/evaluation/run_retrieval_testbed.py`, add:

```python
parser.add_argument(
    "--candidate-scope",
    choices=("corpus", "labeled"),
    default="corpus",
    help="Retrieval candidate universe: full configured corpus or per-query labeled work_ids.",
)
```

Add to `comparison`:

```python
"candidate_scope": args.candidate_scope,
```

Pass into `runner.run_queries()`:

```python
candidate_scope=args.candidate_scope,
```

Add to `run_metadata`:

```python
"candidate_scope": args.candidate_scope,
```

- [ ] **Step 5: Update script tests**

In `tests/scripts/test_run_retrieval_testbed.py`, extend `test_run_cli_supports_json_testbed_strategies()`:

```python
"--candidate-scope", "labeled",
```

Assert:

```python
assert args.candidate_scope == "labeled"
```

In `FakeRunner.run_queries()`, accept `candidate_scope` and assert it in the test:

```python
def run_queries(self, strategy, queries, top_k, ks, run_metadata, candidate_scope):
    assert candidate_scope == "labeled"
    return {
        "run_id": 1,
        "status": "completed",
        "aggregate_metrics": {"query_count": len(queries), "known_positive_recall@10": 1.0},
        "query_failures": [],
        "per_query": [],
    }
```

Pass `--candidate-scope labeled` in the `module.main([...])` call and assert:

```python
comparison = json.loads((output_dir / "comparison.json").read_text(encoding="utf-8"))
assert comparison["candidate_scope"] == "labeled"
```

- [ ] **Step 6: Run task tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_runner.py tests/scripts/test_run_retrieval_testbed.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/docset_hub/evaluation/runner.py scripts/evaluation/run_retrieval_testbed.py tests/evaluation/test_runner.py tests/scripts/test_run_retrieval_testbed.py
git commit -m "feat: add retrieval testbed candidate scope"
```

---

### Task 2: Pass Candidate Work IDs Through Search Strategies

**Files:**
- Modify: `src/docset_hub/evaluation/search_strategies.py`
- Modify: `tests/evaluation/test_search_strategies.py`

- [ ] **Step 1: Write failing strategy tests**

Add to `tests/evaluation/test_search_strategies.py`:

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

- [ ] **Step 2: Run strategy tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_search_strategies.py -v
```

Expected: FAIL because strategy `search()` methods do not accept `candidate_work_ids`.

- [ ] **Step 3: Implement strategy pass-through**

In `PaperIndexerSearchStrategy.search()`, use:

```python
def search(
    self,
    query: str,
    top_k: int,
    candidate_work_ids: Optional[list[str]] = None,
) -> list[RankedDocument]:
    rows = self.indexer.search(
        query=query,
        source_list=self.source_list,
        top_k=top_k,
        hydrate=False,
        search_type=self.search_type,
        candidate_work_ids=candidate_work_ids,
    )
    return normalize_results(rows)
```

In `HybridRetrievalSearchStrategy.search()`, use:

```python
def search(
    self,
    query: str,
    top_k: int,
    candidate_work_ids: Optional[list[str]] = None,
) -> list[RankedDocument]:
    rows = self.indexer.hybrid_retrieval_search(
        query=query,
        source_list=self.source_list,
        top_k=top_k,
        hydrate=False,
        candidate_work_ids=candidate_work_ids,
    )
    return normalize_results(rows)
```

Update the existing expected call dictionaries in tests to include:

```python
"candidate_work_ids": None,
```

- [ ] **Step 4: Run strategy tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation/test_search_strategies.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/docset_hub/evaluation/search_strategies.py tests/evaluation/test_search_strategies.py
git commit -m "feat: pass labeled candidates through strategies"
```

---

### Task 3: Push Work ID Filters Into Tencent VectorDB Retrieval

**Files:**
- Modify: `src/docset_hub/storage/vector_db_client.py`
- Modify: `src/docset_hub/storage/vector_db.py`
- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Create: `tests/storage/test_vector_db_candidate_filters.py`

- [ ] **Step 1: Write VectorDB filter unit tests**

Create `tests/storage/test_vector_db_candidate_filters.py`:

```python
from __future__ import annotations

from src.docset_hub.storage.vector_db import VectorDB


def test_build_work_id_filter_returns_empty_for_none_or_empty():
    assert VectorDB._build_work_id_filter(None) is None
    assert VectorDB._build_work_id_filter([]) is None


def test_build_work_id_filter_uses_equality_for_single_candidate():
    assert VectorDB._build_work_id_filter(["W1"]) == 'work_id="W1"'


def test_build_work_id_filter_uses_in_for_multiple_candidates():
    assert VectorDB._build_work_id_filter(["W2", "W1", "W1"]) == 'work_id in ("W1", "W2")'


def test_build_work_id_filter_escapes_quotes_and_backslashes():
    assert VectorDB._build_work_id_filter(['W"1', r"W\2"]) == r'work_id in ("W\"1", "W\\2")'
```

- [ ] **Step 2: Run filter tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/storage/test_vector_db_candidate_filters.py -v
```

Expected: FAIL because `_build_work_id_filter()` does not exist.

- [ ] **Step 3: Add dense search filter support in client**

In `src/docset_hub/storage/vector_db_client.py`, change `search_documents()` signature:

```python
def search_documents(
    self,
    database: str,
    collection: str,
    query_text: str,
    limit: int = 10,
    output_fields: Optional[List[str]] = None,
    retrieve_vector: bool = False,
    filter: Optional[str] = None,
) -> Dict[str, Any]:
```

Build `search` before `request_data`:

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

Then use:

```python
request_data = {
    "database": database,
    "collection": collection,
    "search": search,
}
```

- [ ] **Step 4: Add work_id filter builder and pass-through in VectorDB**

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
            return f"work_id={quote(values[0])}"
        return f"work_id in ({', '.join(quote(value) for value in values)})"
```

Add `candidate_work_ids: Optional[List[str]] = None` to:

```python
dense_search(...)
sparse_search(...)
hybrid_search(...)
search(...)
```

In `dense_search()` before the source loop:

```python
work_id_filter = self._build_work_id_filter(candidate_work_ids)
```

Pass into `self.client.search_documents(...)`:

```python
filter=work_id_filter,
```

In `sparse_search()` before the source loop:

```python
work_id_filter = self._build_work_id_filter(candidate_work_ids)
```

Pass into `self.client.fulltext_search_documents(...)`:

```python
filter=work_id_filter,
```

In `hybrid_search()`, pass `candidate_work_ids` to both branches:

```python
dense_results = self.dense_search(
    query=query,
    source_list=source_list,
    top_k=candidate_k,
    candidate_work_ids=candidate_work_ids,
)
sparse_results = self.sparse_search(
    query=query,
    source_list=source_list,
    top_k=candidate_k,
    candidate_work_ids=candidate_work_ids,
)
```

In `search()`, pass `candidate_work_ids` to each branch.

- [ ] **Step 5: Pass candidate_work_ids through PaperIndexer simple search**

In `src/docset_hub/indexing/paper_indexer.py`, add `candidate_work_ids: Optional[List[str]] = None` to `search()` and pass it to:

```python
self.hybrid_retrieval_search(..., candidate_work_ids=candidate_work_ids)
self.vector_db.search(..., candidate_work_ids=candidate_work_ids)
```

Add `candidate_work_ids` to `hybrid_retrieval_search()` signature. The branch propagation is handled in Task 4.

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

### Task 4: Restrict Hybrid Retrieval DB-Backed Branches

**Files:**
- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Modify: `src/docset_hub/indexing/paper_keyword_lookup.py`
- Modify: `src/docset_hub/indexing/expanded_sparse_retrieval.py`
- Modify: `src/docset_hub/storage/metadata_db.py`
- Create: `tests/storage/test_metadata_db_candidate_filters.py`

- [ ] **Step 1: Add SQL helper tests**

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

- [ ] **Step 2: Run SQL helper tests to verify failure**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/storage/test_metadata_db_candidate_filters.py -v
```

Expected: FAIL because `_keyword_lookup_work_id_filter()` does not exist.

- [ ] **Step 3: Add MetadataDB candidate filter helper**

In `src/docset_hub/storage/metadata_db.py`, near `_keyword_lookup_in_filter()`, add:

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

- [ ] **Step 4: Restrict keyword lookup SQL methods**

Add `candidate_work_ids: Optional[Sequence[str]] = None` to both methods:

```python
lookup_papers_by_keyword_terms(...)
lookup_papers_by_keyword_lookup_terms(...)
```

After `paper_source_filter`, add:

```python
candidate_filter = self._keyword_lookup_work_id_filter(
    column="p.work_id",
    candidate_work_ids=candidate_work_ids or [],
    params=params,
    prefix="lookup_candidate_work_id",
)
```

Because both SQL statements join `papers p` only in the final SELECT, append the candidate filter to the final SELECT by adding:

```sql
WHERE {candidate_filter or "1 = 1"}
```

For `lookup_papers_by_keyword_terms()`, place it after:

```sql
FROM concept_matches cm
JOIN papers p ON p.paper_id = cm.paper_id
```

For `lookup_papers_by_keyword_lookup_terms()`, place it after:

```sql
FROM group_matches gm
JOIN papers p ON p.paper_id = gm.paper_id
```

- [ ] **Step 5: Restrict expanded sparse SQL**

Add `candidate_work_ids: Optional[Sequence[str]] = None` to:

```python
lookup_papers_by_expanded_sparse_groups(...)
```

Build:

```python
candidate_filter = self._keyword_lookup_work_id_filter(
    column="p.work_id",
    candidate_work_ids=candidate_work_ids or [],
    params=params,
    prefix="expanded_candidate_work_id",
)
candidate_clause = f" AND {candidate_filter}" if candidate_filter else ""
```

Apply `candidate_clause` to every `papers p` source in expanded sparse SQL:

```sql
WHERE 1 = 1 {paper_source_clause} {candidate_clause}
```

For `keyword_matches`, keep the existing `keyword_source_clause` and `paper_source_clause`, and add:

```sql
{candidate_clause}
```

- [ ] **Step 6: Pass candidate_work_ids through indexing wrappers**

In `src/docset_hub/indexing/paper_keyword_lookup.py`, add `candidate_work_ids` to:

```python
match_paper_keywords_using_span_matcher(...)
match_paper_keywords_with_lookup_plan(...)
```

Pass it to `metadata_db.lookup_papers_by_keyword_terms(...)` and `metadata_db.lookup_papers_by_keyword_lookup_terms(...)`.

In `src/docset_hub/indexing/expanded_sparse_retrieval.py`, add `candidate_work_ids` to:

```python
match_papers_by_expanded_sparse_plan(...)
```

Pass it to `metadata_db.lookup_papers_by_expanded_sparse_groups(...)`.

- [ ] **Step 7: Pass candidate_work_ids through PaperIndexer hybrid retrieval branches**

In `src/docset_hub/indexing/paper_indexer.py`, add `candidate_work_ids: Optional[Sequence[str]] = None` to:

```python
hybrid_retrieval_search(...)
_run_dense_retrieval_branch(...)
_run_sparse_retrieval_branch(...)
_run_keyword_lookup_retrieval_branch(...)
_run_expanded_sparse_retrieval_branch(...)
```

Pass to vector branches:

```python
self.vector_db.dense_search(..., candidate_work_ids=list(candidate_work_ids or []))
self.vector_db.sparse_search(..., candidate_work_ids=list(candidate_work_ids or []))
```

Pass to DB-backed wrappers:

```python
match_paper_keywords_with_lookup_plan(..., candidate_work_ids=candidate_work_ids)
match_papers_by_expanded_sparse_plan(..., candidate_work_ids=candidate_work_ids)
```

In `hybrid_retrieval_search()`, when `candidate_work_ids` is not empty, cap `candidate_k`:

```python
if candidate_work_ids:
    candidate_k = min(candidate_k, len(set(candidate_work_ids)))
```

- [ ] **Step 8: Add defensive final filter before RRF output**

In `_weighted_rrf_merge_retrieval_branches()`, add optional `candidate_work_ids: Optional[Sequence[str]] = None`.

Before appending each `SearchResult`, skip out-of-scope rows:

```python
candidate_set = {str(work_id) for work_id in candidate_work_ids or []}
...
work_id = str(entry.get("work_id") or "")
if candidate_set and work_id not in candidate_set:
    continue
```

Pass `candidate_work_ids` from `hybrid_retrieval_search()` into `_weighted_rrf_merge_retrieval_branches(...)`.

- [ ] **Step 9: Run targeted tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile src/docset_hub/indexing/paper_indexer.py src/docset_hub/indexing/paper_keyword_lookup.py src/docset_hub/indexing/expanded_sparse_retrieval.py src/docset_hub/storage/metadata_db.py
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/storage/test_metadata_db_candidate_filters.py tests/evaluation/test_runner.py tests/evaluation/test_search_strategies.py -v
```

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/docset_hub/indexing/paper_indexer.py src/docset_hub/indexing/paper_keyword_lookup.py src/docset_hub/indexing/expanded_sparse_retrieval.py src/docset_hub/storage/metadata_db.py tests/storage/test_metadata_db_candidate_filters.py
git commit -m "feat: restrict hybrid retrieval to candidate work ids"
```

---

### Task 5: Update Documentation and Run Regression Tests

**Files:**
- Modify: `docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md`
- Modify: `docs/implementation_log/20260610/retrieval_testbed_labeled_candidate_scope_implementation_plan_20260610.md`

- [ ] **Step 1: Update design document semantics**

In `docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md`, update the confirmed decisions section from:

```text
- Run every evaluated method against the complete configured corpus.
```

to:

```text
- Default evaluation runs every method against the complete configured corpus.
- Optional `candidate_scope=labeled` evaluation ranks only within each query's frozen labeled `work_id` set.
```

Add a short subsection under Scope:

```markdown
### Candidate Scope Modes

`candidate_scope=corpus` is the default regression mode. It measures whether a retrieval method can find known relevant documents from the configured corpus, while unjudged returned documents remain unknown.

`candidate_scope=labeled` is an opt-in judged-pool ranking mode. For each query, the candidate set is exactly the frozen labeled `work_id` set for that query. This mode measures ranking quality among known judgments and must not be reported as corpus recall.
```

- [ ] **Step 2: Add command example**

Add this command near the existing run example:

```bash
python scripts/evaluation/run_retrieval_testbed.py \
  --evaluation-config-path src/config/config_tecent_backend_server_mimic.yaml \
  --confirm-evaluation-database langtaosha_mimic \
  --testbed-json local_data/retrieval_testbed/import_topic_v1_mimic.json \
  --strategies dense sparse hybrid hybrid_retrieval \
  --candidate-scope labeled \
  --top-k 10 \
  --ks 5 10 \
  --output-dir local_data/retrieval_testbed/runs/topic_v1_mimic_labeled
```

- [ ] **Step 3: Run regression tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/evaluation tests/scripts/test_run_retrieval_testbed.py tests/storage/test_vector_db_candidate_filters.py tests/storage/test_metadata_db_candidate_filters.py -v
```

Expected: PASS.

- [ ] **Step 4: Run compile check for touched source files**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile \
  scripts/evaluation/run_retrieval_testbed.py \
  src/docset_hub/evaluation/runner.py \
  src/docset_hub/evaluation/search_strategies.py \
  src/docset_hub/indexing/paper_indexer.py \
  src/docset_hub/indexing/paper_keyword_lookup.py \
  src/docset_hub/indexing/expanded_sparse_retrieval.py \
  src/docset_hub/storage/vector_db.py \
  src/docset_hub/storage/vector_db_client.py \
  src/docset_hub/storage/metadata_db.py
```

Expected: command exits with status 0.

- [ ] **Step 5: Record final verification in this implementation log**

Append a section to this file:

```markdown
## Implementation Verification

- `pytest tests/evaluation tests/scripts/test_run_retrieval_testbed.py tests/storage/test_vector_db_candidate_filters.py tests/storage/test_metadata_db_candidate_filters.py -v`: PASS
- `py_compile` touched source files: PASS
- Manual CLI smoke command: not run against Tencent VectorDB in this implementation session
```

- [ ] **Step 6: Commit docs and final test updates**

```bash
git add docs/implementation_log/20260608/retrieval_feedback_testbed_design_20260608.md docs/implementation_log/20260610/retrieval_testbed_labeled_candidate_scope_implementation_plan_20260610.md
git commit -m "docs: plan labeled candidate retrieval testbed mode"
```

---

## Acceptance Criteria

- `run_retrieval_testbed.py` accepts `--candidate-scope corpus|labeled`.
- Default `candidate_scope=corpus` preserves current behavior.
- `candidate_scope=labeled` passes per-query labeled `work_id` candidates into every strategy.
- Dense VectorDB search sends a Tencent search `filter` for `work_id`.
- Sparse VectorDB search sends a Tencent fullTextSearch `filter` for `work_id`.
- `hybrid` and `hybrid_retrieval` do not leak non-candidate `work_id` results in labeled mode.
- Keyword lookup and expanded sparse SQL branches include candidate `work_id` constraints.
- Comparison JSON records `candidate_scope`.
- Per-query output records `candidate_scope` and `candidate_count`.
- Documentation states that labeled mode is judged-pool ranking, not corpus recall.

## Self-Review Notes

- Spec coverage: The plan covers CLI, runner, strategy adapter, VectorDB dense/sparse filters, hybrid retrieval, DB-backed branches, tests, and docs.
- Placeholder scan: No placeholder tasks are left for implementers; each code-changing task includes target files, code snippets, commands, and expected outcomes.
- Type consistency: The candidate parameter is consistently named `candidate_work_ids` and typed as `Optional[list[str]]` at strategy boundaries and `Optional[Sequence[str]]` in indexing/storage helpers where read-only sequences are enough.
