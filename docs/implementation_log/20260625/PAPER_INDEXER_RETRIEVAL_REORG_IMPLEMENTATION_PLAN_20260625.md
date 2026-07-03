# PaperIndexer Retrieval Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize retrieval code in `paper_indexer.py` into three clear layers — L1 minimal recall methods, L2 composition wrappers (`search`, `hybrid_retrieval_search`), L3 Langtaosha frontend flow (`smart_search`) — with all post-recall concerns (filter, hydrate, coverage, timing, RRF adaptation) moved into a new `retrieval_helper.py`.

**Architecture:** L1 vector recall methods (`dense_search`, `sparse_search`) accept only `(query, source_list, top_k)` and return `List[RankedResult]`. L1 keyword-backed recall methods (`expanded_sparse_search`, internal `_keyword_lookup_search`) additionally require `keyword_sources` because semantic plan building and MetadataDB keyword matching both depend on it. A new `retrieval_helper.py` owns filtering, presentation, coverage annotation, timing, and fusion adapters. L2 resolves sources, forwards `keyword_sources` where needed, calls L1, applies helper pipelines, and returns API-shaped `List[Dict]`. L3 runs query understanding, routes author vs semantic paths, groups sources, and delegates to L2.

**Tech Stack:** Python, existing `PaperIndexer`, `VectorDB`, `MetadataDB`, `dense_result_filter`, `coverage_engine`, `expanded_sparse_retrieval`, pytest

---

## Scope Decisions Locked For This Plan

- Intermediate recall type is named **`RankedResult`** (not `RankedHit`).
- L1 **vector** recall methods (`dense_search`, `sparse_search`) expose **only** `(query: str, source_list: List[str], top_k: int = 10)` — caller must already resolve `source_list`.
- L1 **keyword-backed** recall is an intentional exception:
  - `expanded_sparse_search(query, source_list, top_k, keyword_sources=None)`
  - `_keyword_lookup_search(query, source_list, top_k, keyword_sources=None)` (internal hybrid branch)
  - `keyword_sources` is **required for correct recall semantics**, not a presentation concern. It must not be dropped or moved solely into helper.
- **Filter (except keyword-source-scoped recall), hydrate, dev coverage re-annotation, timing, lightweight serialization, and RRF branch adaptation** live in `retrieval_helper.py`, not in L1.
- **`keyword_lookup`** is an internal fourth recall path for hybrid only; it is not a public L1 method and not a `search_type` dispatch target.
- **`expanded_sparse` is not part of the default hybrid** composition. Default hybrid remains `dense + sparse + keyword_lookup`. Expanded sparse stays a standalone L1 / `search_type` path unless explicitly injected via `retrievers`.
- For **expanded sparse**, L1 computes recall score (`coverage_ratio`) during match — that is part of recall semantics. Helper is responsible for **presenting** coverage fields (`similarity`, `coverage`, `matched_spans`, optional strict/loose re-annotation for dev).
- **`smart_search` semantic route default** should call `hybrid_retrieval_search()` directly (not `search(search_type=...)`), aligning with Langtaosha production retrieval.
- **`search(mode="vector")` in scholar API** continues to call `search(search_type="dense")` — this returns **raw dense recall + hydrate** (Scheme B); production-quality filtered dense lives in `hybrid_retrieval_search`.
- **Scheme B (locked):** L1 `dense_search` / `sparse_search` are **raw recall only**. `search()` does **not** apply hard filter, coverage, or timing — only `present_search_results(hydrate=...)`. Filter + RRF belong to `hybrid_retrieval_search` (and dev HTTP routes for coverage annotation).
- Do **not** create additional orchestrator files beyond `retrieval_helper.py` in this plan.
- Indexing paths (`index_dict`, `index_file`, vectorization, keyword enrichment) are **unchanged**.

---

## Current Problems Being Fixed

1. `search()` dense/sparse paths call `_run_*_retrieval_branch` (returns branch dicts) then pass results to `_hydrate_search_results` (expects `SearchResult` objects) — **broken type contract**.
2. `search()` contains unreachable dead code (lines ~422–464) for coverage/timing.
3. `search_type="hybrid_retrieval"` routing is missing from current `search()` despite tests and docs expecting it.
4. Retrieval helpers (`_hydrate_*`, `_annotate_*`, adapters, RRF) are scattered through the middle of `paper_indexer.py`, mixed with public API and indexing code.
5. `expanded_sparse_search` implements its own hydrate/present path instead of sharing helper logic.
6. Prior draft assumed `expanded_sparse_search` could use only `(query, source_list, top_k)` — **incorrect**; `keyword_sources` is required at recall time (plan build + MetadataDB lookup).

---

## Target Layer Model

```text
L1  PaperIndexer.dense_search / sparse_search
      query + source_list + top_k  ->  List[RankedResult]

L1  PaperIndexer.expanded_sparse_search / _keyword_lookup_search
      query + source_list + top_k + keyword_sources  ->  List[RankedResult]

Helper  retrieval_helper.py
      filter_* / present_* / annotate_* / timing / hits_to_branch_results / weighted_rrf_merge / run_retrievers_parallel

L2  PaperIndexer.search
      resolve sources -> L1 (raw) -> present_search_results(hydrate) -> List[Dict]
      search_type in {dense, sparse, expanded_sparse} only

L2  PaperIndexer.hybrid_retrieval_search
      L1 (raw) -> filter_* -> RRF -> present_search_results(hydrate) -> List[Dict]

L3  PaperIndexer.smart_search
      query understanding -> author DB or grouped L2 -> Dict payload for scholar API
```

---

## File Map

### Create

- `src/docset_hub/indexing/retrieval_helper.py`
  - `RankedResult` dataclass
  - filter helpers wrapping `dense_result_filter` and positive-score pruning
  - present helpers (`hydrate_results`, `to_lightweight_dicts`, `present_search_results`)
  - coverage helpers wrapping `coverage_engine`
  - timing helpers (`RetrievalTimings`, `timed_section`)
  - fusion helpers (`hits_to_branch_results`, `weighted_rrf_merge`, `run_retrievers_parallel`)
  - conversion helpers from `SearchResult`, `ExpandedSparseCandidate`, `PaperKeywordLookupResult` → `RankedResult`
- `tests/indexing/test_retrieval_helper.py`
  - unit tests for present/filter/coverage/timing/fusion helpers

### Modify

- `src/docset_hub/indexing/paper_indexer.py`
  - add L1 recall methods: vector paths use 3 args; keyword-backed paths include `keyword_sources`
  - rewrite `search()` as dispatch + helper pipeline
  - rewrite `hybrid_retrieval_search()` to inject L1 recall callables
  - update `smart_search()` to call L2 with default `search_type="hybrid_retrieval"`
  - remove migrated helpers; keep indexing private methods at bottom
- `src/docset_hub/indexing/__init__.py`
  - export `RankedResult` if useful for tests/dev tools
- `tests/indexing/test_paper_indexer.py`
  - update smart_search tests to pass explicit `search_type` where mocking `search()` only
- `tests/indexing/test_expanded_sparse_retrieval.py`
  - adjust expectations if `expanded_sparse_search` signature changes (L1 returns `RankedResult`; L2/hydrate via helper)
- `tests/indexing/test_paper_indexer_three_way_hybrid_retrieval.py`
  - patch L1 recall methods instead of `_run_*_retrieval_branch` where applicable

### Reference (no change unless helper integration requires import path only)

- `src/docset_hub/indexing/dense_result_filter.py`
- `src/docset_hub/indexing/coverage_engine.py`
- `src/docset_hub/indexing/expanded_sparse_retrieval.py`
- `app/routes/scholar.py`

---

## RankedResult Contract

```python
@dataclass(frozen=True)
class RankedResult:
    work_id: str
    paper_id: Optional[int]
    source_name: str
    score: float
    text_type: str
    retriever: str
    rank: int
    retrieval_debug: Dict[str, Any] = field(default_factory=dict)

    # expanded_sparse recall evidence; coverage dict fields filled at present time
    matched_spans: Optional[List[Dict[str, Any]]] = None
    total_span_count: Optional[int] = None
    matched_span_count: Optional[int] = None
```

Rules:

- `score` is the retriever-native ranking score (dense similarity, BM25 score, coverage_ratio, keyword_lookup_score).
- `rank` is 1-based order within the L1 result list.
- `retriever` values: `"dense"`, `"sparse"`, `"expanded_sparse"`, `"keyword_lookup"`.
- API field `similarity` is assigned from `score` during `present_search_results`.

---

## expanded_sparse_search And keyword_sources

`expanded_sparse_search` **cannot** be reduced to `(query, source_list, top_k)` alone. Current code and tests depend on `keyword_sources` in **two recall stages**:

### Stage 1 — Semantic plan building

```text
build_query_semantic_plan(query, source_list, keyword_sources)
  -> _build_span_matcher_profile(..., keyword_sources=keyword_sources)
  -> SpanMatcherPipeline(..., profile.keyword_sources)
  -> QuerySemanticPlan
```

`keyword_sources` controls which `paper_keywords.source` values participate in **keyword surface span matching** when building the plan. Tests assert e.g. `keyword_sources=["paper_metadata"]` is forwarded into the profile (`tests/indexing/test_expanded_sparse_retrieval.py`).

### Stage 2 — MetadataDB expanded sparse lookup

```text
match_papers_by_expanded_sparse_plan(..., keyword_sources=keyword_sources)
  -> metadata_db.lookup_papers_by_expanded_sparse_groups(..., keyword_sources=keyword_sources)
```

In SQL, `keyword_sources` filters `paper_keywords pk` via `pk.source` (`metadata_db.py:1821–1827`). When `keyword_sources` is `None` or empty, **no keyword-source filter is applied** (all keyword sources eligible). Callers that need a specific keyword index (e.g. `paper_metadata` vs generated keywords) must pass `keyword_sources` explicitly.

### Parameter ownership after reorg

| Parameter | Owned by | Passed to |
| --- | --- | --- |
| `query`, `top_k`, `hydrate`, `include_coverage`, `timings_ms` | **L2** `search()` | helper + dispatch |
| `source_list` resolve | **L2** | L1 resolved list |
| `keyword_sources` | **L2** (from HTTP `keyword_sources=` / caller) | L1 `expanded_sparse_search`, L1 `_keyword_lookup_search`, L2 dense hard-filter helper, L2 coverage plan rebuild |

L1 signature for keyword-backed recall:

```python
def expanded_sparse_search(
    self,
    query: str,
    source_list: List[str],
    top_k: int = 10,
    keyword_sources: Optional[Sequence[str]] = None,
) -> List[RankedResult]:
    plan = self.build_query_semantic_plan(
        query=query,
        source_list=source_list,
        keyword_sources=keyword_sources,
    )
    if plan is None:
        return []
    candidates = match_papers_by_expanded_sparse_plan(
        metadata_db=self.metadata_db,
        plan=plan,
        source_list=source_list,
        keyword_sources=keyword_sources,
        top_k=top_k,
    )
    return [from_expanded_sparse_candidate(c, plan=plan) for c in candidates]
```

**Do not** resolve a hidden default inside L1 unless product explicitly defines one in config. Keep `None` semantics aligned with current MetadataDB behavior; L2/API callers supply values when needed (see `app/routes/paper.py` → `keyword_sources=_parse_source_list(request.args.get("keyword_sources"))`).

The same `keyword_sources` argument applies to internal `_keyword_lookup_search` (SpanMatcher profile + `match_paper_keywords_with_lookup_plan`).

---

## retrieval_helper.py Section Layout

```text
1. Types          RankedResult, RetrievalTimings
2. Converters     from_search_result, from_expanded_sparse_candidate, from_keyword_lookup_result
3. Filter         filter_dense_results, filter_positive_score_results, filter_keyword_lookup_results
4. Coverage       build_coverage_document_fields, extract_keyword_texts,
                  annotate_strict_coverage, annotate_loose_coverage,
                  build_expanded_sparse_present_fields
5. Present        hydrate_results, to_lightweight_dicts, present_search_results
6. Timing         timed_section, RetrievalTimings.record
7. Fusion         hits_to_branch_results, weighted_rrf_merge, run_retrievers_parallel, retrieval_dedupe_key
8. Utils          safe_float
```

---

## Task 1: Introduce RankedResult And retrieval_helper Skeleton

**Files:**

- Create: `src/docset_hub/indexing/retrieval_helper.py`
- Create: `tests/indexing/test_retrieval_helper.py`
- Modify: `src/docset_hub/indexing/__init__.py`

- [ ] **Step 1: Write failing tests for RankedResult converters and present helpers**

```python
def test_present_search_results_maps_ranked_result_to_api_dict():
    hit = RankedResult(
        work_id="W1",
        paper_id=1,
        source_name="langtaosha",
        score=0.88,
        text_type="abstract",
        retriever="dense",
        rank=1,
    )
    rows = present_search_results([hit], metadata_db=FakeMetadataDB(), hydrate=False)
    assert rows[0]["work_id"] == "W1"
    assert rows[0]["similarity"] == 0.88
```

- [ ] **Step 2: Implement RankedResult + from_search_result + present_search_results + to_lightweight_dicts**

- [ ] **Step 3: Run tests**

```bash
cd <repo-root>
pytest tests/indexing/test_retrieval_helper.py -q
```

Expected: PASS

---

## Task 2: Move Filter, Coverage, Timing, Fusion Into retrieval_helper

**Files:**

- Modify: `src/docset_hub/indexing/retrieval_helper.py`
- Modify: `tests/indexing/test_retrieval_helper.py`

- [ ] **Step 1: Port filter helpers**

Move logic from:

- `PaperIndexer._run_dense_retrieval_branch` → `filter_dense_results(query, hits, metadata_db, min_similarity, keyword_sources)`
- `PaperIndexer._adapt_search_results_to_branch_results(drop_non_positive=True)` → `filter_positive_score_results`
- `PaperIndexer._adapt_keyword_lookup_results_to_branch_results` positive check → `filter_keyword_lookup_results`

- [ ] **Step 2: Port coverage helpers**

Move from `PaperIndexer._annotate_results_with_coverage`, `_annotate_results_with_loose_coverage`, `_coverage_document_fields`, `_extract_keyword_texts` into helper functions operating on `List[Dict]` after present.

Add `build_expanded_sparse_present_fields(result: RankedResult, plan)` using `coverage_engine.summarize_expanded_sparse_matches`.

- [ ] **Step 3: Port fusion helpers**

Move from:

- `_adapt_*_to_branch_results` → `hits_to_branch_results`
- `_weighted_rrf_merge_retrieval_branches` → `weighted_rrf_merge` (returns `List[RankedResult]`)
- ThreadPool parallel block from `hybrid_retrieval_search` → `run_retrievers_parallel`
- `_retrieval_dedupe_key`, `_safe_float`, `_resolve_hybrid_retrieval_weights`

- [ ] **Step 4: Port timing helpers**

```python
@contextmanager
def timed_section(timings: Optional[RetrievalTimings], name: str):
    ...
```

- [ ] **Step 5: Add unit tests for filter, RRF, timing; run**

```bash
pytest tests/indexing/test_retrieval_helper.py tests/indexing/test_paper_indexer_three_way_hybrid_retrieval.py -q
```

Note: hybrid tests may still pass against old method names until Task 4; run helper tests first.

Expected: helper tests PASS

---

## Task 3: Implement L1 Recall Methods On PaperIndexer

**Files:**

- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Modify: `tests/indexing/test_expanded_sparse_retrieval.py`

- [ ] **Step 1: Add L1 methods returning List[RankedResult]**

```python
def dense_search(self, query: str, source_list: List[str], top_k: int = 10) -> List[RankedResult]:
    """Vector dense recall only; no hard filter, no hydrate."""

def sparse_search(self, query: str, source_list: List[str], top_k: int = 10) -> List[RankedResult]:
    """BM25 sparse recall only; no positive-score filter."""

def expanded_sparse_search(
    self,
    query: str,
    source_list: List[str],
    top_k: int = 10,
    keyword_sources: Optional[Sequence[str]] = None,
) -> List[RankedResult]:
    """Semantic plan + expanded sparse DB match; score = coverage_ratio."""

def _keyword_lookup_search(
    self,
    query: str,
    source_list: List[str],
    top_k: int = 10,
    keyword_sources: Optional[Sequence[str]] = None,
) -> List[RankedResult]:
    """Internal hybrid branch; span matcher + paper keyword lookup."""
```

Implementation notes:

- `dense_search` calls `self.vector_db.dense_search`, converts via `from_search_result`.
- `sparse_search` calls `self.vector_db.sparse_search`, converts via `from_search_result`.
- `expanded_sparse_search` passes the **same** `keyword_sources` to both `build_query_semantic_plan` and `match_papers_by_expanded_sparse_plan` (see section above).
- `_keyword_lookup_search` passes `keyword_sources` into `_build_span_matcher_profile` and `match_paper_keywords_with_lookup_plan`.

- [ ] **Step 2: Add/adjust tests asserting L1 returns RankedResult without metadata**

```python
def test_dense_search_returns_ranked_result_without_metadata(monkeypatch):
    ...
    hits = indexer.dense_search("renal adhesion", ["langtaosha"], top_k=5)
    assert all(isinstance(h, RankedResult) for h in hits)
    assert "metadata" not in hits[0].__dict__

def test_expanded_sparse_search_forwards_keyword_sources_to_plan_and_lookup(monkeypatch):
    ...
    indexer.expanded_sparse_search(
        "adhesion protein in kidney",
        ["biorxiv_daily"],
        top_k=7,
        keyword_sources=["paper_metadata"],
    )
    assert captured_plan["keyword_sources"] == ["paper_metadata"]
    assert captured_lookup["keyword_sources"] == ["paper_metadata"]
```

- [ ] **Step 3: Run expanded sparse tests**

```bash
pytest tests/indexing/test_expanded_sparse_retrieval.py -q -k "expanded_sparse_search or branch"
```

Expected: may FAIL until Task 4 rewires public API — document and proceed.

---

## Task 4: Rewrite L2 — search() And hybrid_retrieval_search()

**Scheme B locked:** `search()` is a **thin single-path wrapper** — L1 raw recall + `present_search_results` only. No filter, no coverage, no timing, no `hybrid_retrieval` dispatch.

**Files:**

- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Modify: `src/docset_hub/indexing/retrieval_helper.py` (expanded_sparse present fields in `present_search_results`)
- Modify: `app/routes/paper.py` (move `include_coverage` / `include_loose_coverage` / `timings_ms` orchestration to HTTP layer)
- Modify: `tests/indexing/test_paper_indexer_search_type.py`
- Modify: `tests/indexing/test_paper_indexer_three_way_hybrid_retrieval.py`
- Modify: `tests/indexing/test_expanded_sparse_retrieval.py`
- Modify: `tests/app/test_paper_routes.py` (if coverage route tests exist)

- [ ] **Step 1: Rewrite `search()` as thin single-path wrapper**

```python
_SINGLE_PATH_SEARCH_TYPES = frozenset({"dense", "sparse", "expanded_sparse"})

def search(
    self,
    query: str,
    source_list: Optional[List[str]] = None,
    top_k: int = 10,
    hydrate: bool = True,
    search_type: str = "dense",
    *,
    keyword_sources: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    resolved = self._resolve_source_list(source_list)
    if search_type not in self._SINGLE_PATH_SEARCH_TYPES:
        raise ValueError(
            f"search() only supports {sorted(self._SINGLE_PATH_SEARCH_TYPES)}; "
            f"use hybrid_retrieval_search() for hybrid retrieval"
        )

    if search_type == "dense":
        hits = self.dense_search(query, resolved, top_k)
    elif search_type == "sparse":
        hits = self.sparse_search(query, resolved, top_k)
    else:
        hits = self.expanded_sparse_search(
            query, resolved, top_k, keyword_sources=keyword_sources
        )

    return present_search_results(
        hits,
        metadata_db=self.metadata_db,
        hydrate=hydrate,
    )
```

**Remove from `search()` signature and body:**

- `include_coverage`, `include_loose_coverage`, `timings_ms`, `dense_min_similarity`
- `search_type="hybrid_retrieval"` routing (callers use `hybrid_retrieval_search()` directly)
- expanded_sparse special-case plan rebuild / coverage annotate inside indexer

**Move dev coverage to HTTP** (`app/routes/paper.py`):

```text
hits_dicts = indexer.search(..., hydrate=True)
if include_coverage or include_loose_coverage:
    plan = indexer.build_query_semantic_plan(query, source_list, keyword_sources)
    if plan:
        annotate_strict_coverage / annotate_loose_coverage on hits_dicts
```

- [ ] **Step 2: Enrich `present_search_results` for expanded_sparse**

When `hit.retriever == "expanded_sparse"`, merge coverage-shaped fields from `RankedResult` (`score`, `matched_spans`, counts) into API dict — no second plan build in `search()`.

- [ ] **Step 3: Rewrite `hybrid_retrieval_search` — filter lives here (Scheme B)**

```python
def hybrid_retrieval_search(...):
    # L1 raw recall (parallel)
    branch_raw = run_retrievers_parallel({
        "dense": self.dense_search,
        "sparse": self.sparse_search,
        "keyword_lookup": partial(self._keyword_lookup_search, keyword_sources=keyword_sources),
    }, ...)

    # filter after recall
    branch_filtered = {
        "dense": filter_dense_results(branch_raw["dense"], query=..., metadata_db=..., ...)[0],
        "sparse": filter_positive_score_results(branch_raw["sparse"]),
        "keyword_lookup": filter_keyword_lookup_results(branch_raw["keyword_lookup"]),
    }

    branch_results = {name: hits_to_branch_results(hits) for name, hits in branch_filtered.items()}
    fused = weighted_rrf_merge(branch_results, ...)
    return present_search_results(fused, metadata_db=self.metadata_db, hydrate=hydrate)
```

- [ ] **Step 4: Remove obsolete `_run_*_retrieval_branch` usage from `search()`**

Candidates for removal (once tests green):

- `_run_dense_retrieval_branch`
- `_run_sparse_retrieval_branch`
- `_run_keyword_lookup_retrieval_branch`
- `_run_expanded_sparse_retrieval_branch`
- `_adapt_dense_payloads_to_branch_results`
- `_adapt_search_results_to_branch_results`
- `_adapt_keyword_lookup_results_to_branch_results`
- `_adapt_expanded_sparse_results_to_branch_results`
- `_weighted_rrf_merge_retrieval_branches`
- `_hydrate_search_results`
- `_search_results_to_lightweight_dicts`
- `_annotate_results_with_coverage`
- `_annotate_results_with_loose_coverage`
- `_coverage_document_fields`
- `_extract_keyword_texts`
- `_search_result_to_filter_payload`
- `_retrieval_dedupe_key`
- `_safe_float` (if fully moved)

Keep on PaperIndexer:

- `_resolve_source_list`, `_resolve_source_name`
- `_build_span_matcher_profile`, `build_query_semantic_plan`
- `merge_source_list`
- indexing private methods

- [ ] **Step 5: Run retrieval test suite**

```bash
pytest tests/indexing/test_paper_indexer_search_type.py \
       tests/indexing/test_paper_indexer_three_way_hybrid_retrieval.py \
       tests/indexing/test_expanded_sparse_retrieval.py \
       tests/indexing/test_retrieval_helper.py -q
```

Expected: PASS

---

## Task 5: Rewrite L3 — smart_search()

**Files:**

- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Modify: `tests/indexing/test_paper_indexer.py` (smart_search section)

- [ ] **Step 1: Add optional `search_type` param — semantic route calls `hybrid_retrieval_search()` directly**

```python
def smart_search(
    self,
    query: str,
    ...
    hydrate: bool = True,
) -> Dict[str, Any]:
    # semantic route per source group:
    result = self.hybrid_retrieval_search(
        query=search_query,
        source_list=grouped_source_list,
        top_k=top_k,
        hydrate=hydrate,
        keyword_sources=...,  # if needed from caller
    )
```

Do **not** route production semantic search through `search()` — Scheme B keeps `search()` as raw single-path + hydrate only.

Preserve existing return payload:

```python
{
    "success": bool,
    "query": str,
    "search_query": Optional[str],
    "query_understanding": dict,
    "expanded_search_queries": list,
    "results": List[Tuple[str, List[Dict]]],
}
```

- [ ] **Step 3: Update unit tests**

Tests that monkeypatch `indexer.search` remain valid.

Tests that assert default search behavior should either:

- monkeypatch `hybrid_retrieval_search`, or
- pass `search_type="dense"` explicitly when verifying vector-route mock calls.

- [ ] **Step 4: Run smart_search tests**

```bash
pytest tests/indexing/test_paper_indexer.py -q -k smart_search
pytest tests/app/test_scholar_routes.py -q -k smart
```

Expected: PASS (scholar tests use FakeIndexer; verify no breakage)

---

## Task 6: Reorganize paper_indexer.py Section Order

**Files:**

- Modify: `src/docset_hub/indexing/paper_indexer.py`

- [ ] **Step 1: Reorder class methods into documented sections**

```text
# === Initialization ===
__init__

# === Public: Indexing ===
index_dict, index_file, read, delete

# === Public: Retrieval L1 ===
dense_search, sparse_search, expanded_sparse_search

# === Public: Retrieval L2 ===
search, hybrid_retrieval_search

# === Public: Retrieval L3 ===
smart_search

# === Domain: Semantic plan ===
build_query_semantic_plan, merge_source_list

# === Private: Retrieval L2 helpers ===
_search_single_type, _search_dense, _search_sparse, _search_expanded_sparse
_keyword_lookup_search, _compute_candidate_k, _apply_search_type_filters

# === Private: Indexing ===
(existing indexing helpers)
```

- [ ] **Step 2: Update module docstring to describe L1/L2/L3 model**

- [ ] **Step 3: Full indexing test run**

```bash
pytest tests/indexing/ -q
```

Expected: PASS

---

## Task 7: Documentation Sync

**Files:**

- Modify: `docs/core/shared/docset_hub/indexing/PAPER_INDEXER_FUNCTION_MAP.md`
- Modify: `docs/core/shared/docset_hub/flows/SEARCH_RETRIEVAL_FLOW.md`

- [ ] **Step 1: Document L1/L2/L3 split and RankedResult contract**

- [ ] **Step 2: Note smart_search default search_type = hybrid_retrieval**

- [ ] **Step 3: Add retrieval_helper.py to function map**

---

## API Compatibility Matrix

| Entry | Before | After |
| --- | --- | --- |
| `PaperIndexer.search(...)` | many optional dev params + hybrid dispatch | **thin wrapper:** `(query, source_list, top_k, hydrate, search_type, keyword_sources)` only; raw L1 + present |
| `PaperIndexer.hybrid_retrieval_search(...)` | hard-coded `_run_*_branch` | L1 raw → filter (Scheme B) → RRF → present |
| `PaperIndexer.smart_search(...)` | calls `search()` default dense | semantic route calls `hybrid_retrieval_search()` |
| `GET /api/search` dev coverage flags | on `indexer.search(include_coverage=...)` | orchestrated in `app/routes/paper.py` after `search()` |
| `PaperIndexer.expanded_sparse_search(...)` | L1 returns `List[RankedResult]`; hydrate via `search(search_type="expanded_sparse", hydrate=True)` |

Known callers to verify during implementation:

- `tests/indexing/test_expanded_sparse_retrieval.py`
- `app/routes/paper.py` (uses `indexer.search`, not direct `expanded_sparse_search`)
- `src/docset_hub/evaluation/search_strategies.py`

---

## Verification Checklist

- [ ] `pytest tests/indexing/test_retrieval_helper.py -q` green
- [ ] `pytest tests/indexing/test_paper_indexer_three_way_hybrid_retrieval.py -q` green
- [ ] `pytest tests/indexing/test_expanded_sparse_retrieval.py -q` green
- [ ] expanded sparse tests confirm `keyword_sources` forwarded to plan + MetadataDB lookup
- [ ] `pytest tests/indexing/test_paper_indexer_search_type.py -q` green
- [ ] `pytest tests/indexing/test_paper_indexer.py -q -k smart_search` green
- [ ] `pytest tests/app/test_scholar_routes.py -q` green
- [ ] No unreachable dead code remains in `search()`
- [ ] `paper_indexer.py` retrieval section reads L1 → L2 → L3 top-to-bottom

---

## Out Of Scope

- Adding `expanded_sparse` into default hybrid RRF weights
- Removing legacy VectorDB `search_type="hybrid"` storage hybrid
- Changing scholar HTTP response mapping (`app/routes/scholar.py`) — except `paper.py` dev coverage orchestration move
- Splitting `retrieval_helper.py` into multiple files
- Indexing pipeline refactor (`index_dict` / `index_file` deduplication)

---

## Suggested Implementation Order For Agents

1. Task 1 — RankedResult + present skeleton
2. Task 2 — move filter/coverage/fusion/timing to helper
3. Task 3 — L1 recall methods
4. Task 4 — L2 rewrite (fixes main bugs)
5. Task 5 — smart_search default hybrid
6. Task 6 — section reorder + full test run
7. Task 7 — docs sync
