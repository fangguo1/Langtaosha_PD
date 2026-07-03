# Expanded Sparse Retrieval And Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend span matching into a stable query semantic plan, add an independent expanded sparse retrieval branch, and define full-text coverage ratio computation over `title + abstract + paper_keywords`.

**Architecture:** Reuse the current `QueryPhraseAnalyzer -> SpanMatcherExecutor -> MaximalConceptSelector` pipeline to produce a new `QuerySemanticPlan` contract. Feed that plan into a new expanded sparse retrieval module and a coverage engine that both share the same span-group semantics and full-text field matching rules.

**Tech Stack:** Python, existing `src/docset_hub/indexing/*` orchestration, pytest unit tests, existing MetadataDB-backed keyword sources, existing PaperIndexer retrieval orchestration.

---

## Scope Decisions Locked For This Plan

- `Expanded Sparse Retrieval` is a new retrieval branch, not an enhancement of the current `keyword_lookup` branch.
- `Coverage Engine` first version works over `title + abstract + paper_keywords`.
- `Coverage Engine` uses span-average scoring, not binary span coverage:
  - any matched parent `own_term` gives the span a full score of `1.0`
  - otherwise, if the span has child spans, `span_score = matched_children / total_children`
  - otherwise, `span_score = 0.0`
- `coverage_ratio = sum(span_score for span in plan.spans) / total_span_count`
- `matched_span_count` remains a compatibility/debug count of spans where `span_score > 0`
- `tier2_terms` come only from ontology linker evidence. Do not mix in keyword-surface aliases, DB candidate terms, or substring expansion terms.
- RRF and branch weighting are out of scope for this plan. The branch should be implemented and validated independently first.
- The three required test areas are:
  - `1)` Span matcher extension tests
  - `2)` Expanded sparse retrieval tests
  - `3)` Coverage ratio tests

## File Map

**Create**

- `src/docset_hub/indexing/query_semantic_plan.py`
  - Own the `QuerySemanticPlan` and `SemanticSpanGroup` dataclasses plus plan-building helpers.
- `src/docset_hub/indexing/expanded_sparse_retrieval.py`
  - Own expanded sparse query construction, grouped term matching, and candidate result assembly.
- `src/docset_hub/indexing/coverage_engine.py`
  - Own field-level span coverage matching and `coverage_ratio` calculation.
- `tests/indexing/test_query_semantic_plan.py`
  - Unit tests for plan generation from selected concepts and span evidence.
- `tests/indexing/test_expanded_sparse_retrieval.py`
  - Unit tests for grouped alias retrieval behavior and branch result shaping.
- `tests/indexing/test_coverage_engine.py`
  - Unit tests for field-level coverage matching and ratio calculation.

**Modify**

- `src/docset_hub/indexing/span_matcher.py`
  - Keep the existing matching pipeline intact, but expose helper functions so plan building can consume the final ontology-backed `SelectedConcept[]` without re-implementing span selection rules.
- `src/docset_hub/indexing/paper_indexer.py`
  - Add a plan-producing helper and an isolated expanded sparse retrieval branch entrypoint without changing fusion yet.
- `src/docset_hub/indexing/__init__.py`
  - Export the new plan / retrieval / coverage APIs.
- `tests/indexing/test_span_matcher.py`
  - Add regression coverage proving span matcher output remains compatible with the plan builder.
- `src/docset_hub/storage/metadata_db.py`
  - Add a PostgreSQL-backed full-text expanded sparse query entrypoint over `papers` and `paper_keywords`.

## Task 1: Define The Query Semantic Plan Contract

**Files:**

- Create: `src/docset_hub/indexing/query_semantic_plan.py`
- Modify: `src/docset_hub/indexing/span_matcher.py`
- Modify: `src/docset_hub/indexing/__init__.py`
- Test: `tests/indexing/test_query_semantic_plan.py`
- Test: `tests/indexing/test_span_matcher.py`

**Implementation clarification for existing span matcher**

- Do **not** rewrite `RemoteOntologySpanMatcher`, `KeywordSurfaceSpanMatcher`, `SpanMatcherExecutor`, or `MaximalConceptSelector`.
- The current matching chain remains:

```text
QueryPhraseAnalyzer / extractor
  -> SpanMatcherExecutor.match_candidates(...)
  -> MaximalConceptSelector.select(...)
  -> SelectedConcept[]
```

- The change in this task is to make that output consumable as a stable semantic-plan contract:
  - add helper logic in `span_matcher.py` only if needed to expose normalized ontology evidence cleanly
  - add a new builder in `query_semantic_plan.py` that converts `SelectedConcept[] + SpanMatchResult[]` into `QuerySemanticPlan`
- `tier2_terms` must be collected only from ontology-linker-backed evidence items:
  - `evidence.source in {"umls", "mesh"}`
  - ontology aliases from `evidence.aliases`
  - ontology canonical forms from non-primary ontology evidence
- `tier1_terms` remain rooted in the selected span surface and primary canonical:
  - `candidate.text`
  - `candidate.normalized_text`
  - `primary_evidence.canonical`
- If a selected concept has only keyword-surface evidence and no ontology evidence, the builder must still emit the span group, but with `tier2_terms = []`.

- [ ] **Step 1: Write failing tests for semantic plan generation**

Add tests that lock these rules:

- one `SelectedConcept` becomes one `SemanticSpanGroup`
- `surface_text` comes from the selected candidate text
- `canonical_text` comes from `primary_evidence.canonical`
- `tier1_terms` contain only high-precision forms:
  - selected candidate text
  - normalized candidate text
  - primary canonical
- `tier2_terms` contain only ontology-linker-derived high-confidence alias forms:
  - ontology evidence aliases
  - non-primary ontology evidence canonical strings
- keyword-surface aliases and DB expansion terms must not enter `tier2_terms`
- low-value noise terms are deduped and excluded

Target examples:

```python
def test_build_query_semantic_plan_from_selected_concepts():
    plan = build_query_semantic_plan(
        original_query="adhesion protein in kidney",
        normalized_query="adhesion protein in kidney",
        selected_concepts=[...],
        span_results=[...],
    )
    assert [group.surface_text for group in plan.spans] == [
        "adhesion protein",
        "kidney",
    ]
    assert "cell adhesion molecule" in plan.spans[0].tier2_terms
    assert "renal" in plan.spans[1].tier2_terms
    assert "keyword-only-alias" not in plan.spans[0].tier2_terms
```

Also add a regression in `tests/indexing/test_span_matcher.py` that proves current span matcher evidence remains consumable by the plan builder.

- [ ] **Step 2: Run the targeted plan tests to verify they fail first**

Run:

```bash
python3 -m pytest tests/indexing/test_query_semantic_plan.py tests/indexing/test_span_matcher.py -q
```

Expected:

- missing import / missing builder / assertion failure because the plan module does not exist yet

- [ ] **Step 3: Implement `QuerySemanticPlan` and `SemanticSpanGroup`**

Implement dataclasses shaped like:

```python
@dataclass
class SemanticSpanGroup:
    span_id: str
    surface_text: str
    normalized_text: str
    start: int
    end: int
    canonical_text: str
    tier1_terms: list[str]
    tier2_terms: list[str]
    evidence: list[ConceptMatchEvidence]


@dataclass
class QuerySemanticPlan:
    original_query: str
    normalized_query: str
    spans: list[SemanticSpanGroup]
```

Provide a builder function that consumes `selected_concepts` and optional `span_results` and returns a deterministic, deduped plan.

`tier2_terms` must be sourced only from ontology linker evidence buckets. If a selected concept is backed only by keyword-surface evidence, keep `tier1_terms` populated and leave `tier2_terms` empty unless ontology evidence is also present.

If `span_matcher.py` needs edits in this task, limit them to small helper utilities such as:

- ontology-evidence filtering helpers reused by the builder
- deterministic evidence sorting helpers exposed for plan construction

Do not move plan-building logic into `span_matcher.py`; keep the new contract in `query_semantic_plan.py`.

- [ ] **Step 4: Re-run the semantic plan tests**

Run:

```bash
python3 -m pytest tests/indexing/test_query_semantic_plan.py tests/indexing/test_span_matcher.py -q
```

Expected:

- all targeted semantic plan tests pass

- [ ] **Step 5: Commit the semantic plan contract**

```bash
git add src/docset_hub/indexing/query_semantic_plan.py src/docset_hub/indexing/span_matcher.py src/docset_hub/indexing/__init__.py tests/indexing/test_query_semantic_plan.py tests/indexing/test_span_matcher.py
git commit -m "feat: add query semantic plan contract"
```

## Task 2: Add Expanded Sparse Retrieval Query Construction

**Files:**

- Create: `src/docset_hub/indexing/expanded_sparse_retrieval.py`
- Modify: `src/docset_hub/storage/metadata_db.py`
- Modify: `src/docset_hub/indexing/__init__.py`
- Test: `tests/indexing/test_expanded_sparse_retrieval.py`

**PostgreSQL implementation clarification for Tencent Cloud**

This task is not just an in-memory matcher. The retrieval primitive must be backed by the existing Tencent Cloud PostgreSQL metadata store.

Use these existing tables and fields:

- `papers.paper_id`
- `papers.work_id`
- `papers.canonical_title`
- `papers.canonical_abstract`
- `paper_keywords.paper_id`
- `paper_keywords.keyword`
- `paper_keywords.keyword_type`
- `paper_keywords.source`
- `paper_sources.source_name` for source filtering when needed

Add a new `MetadataDB` query method dedicated to expanded sparse recall, for example:

```python
def lookup_papers_by_expanded_sparse_groups(
    self,
    span_groups: Sequence[Mapping[str, Any]],
    source_list: Optional[Sequence[str]] = None,
    keyword_sources: Optional[Sequence[str]] = None,
    top_k: int = 50,
) -> List[Dict[str, Any]]:
    ...
```

The SQL semantics must be:

- input is a `VALUES` CTE carrying:
  - `group_id`
  - `span_id`
  - `canonical_text`
  - `term`
  - `term_tier` (`tier1` or `tier2`)
- build field-specific raw matches against:
  - `lower(p.canonical_title)`
  - `lower(p.canonical_abstract)`
  - `lower(pk.keyword)`
- first version uses phrase containment semantics, not full PostgreSQL `tsvector` parsing:
  - `POSITION(q.term IN lower(COALESCE(p.canonical_title, ''))) > 0`
  - same for abstract
  - exact normalized equality for keywords: `lower(pk.keyword) = q.term`
- emit one raw row per `(paper_id, group_id, field_name, term)`
- aggregate up to one span/group match per paper/group
- return:
  - matched spans
  - matched terms
  - matched fields
  - `matched_span_count`
  - `total_span_count`

This means Task 2 has two layers:

1. `expanded_sparse_retrieval.py`
   - convert `QuerySemanticPlan` into DB-ready grouped term payloads
2. `metadata_db.py`
   - execute the grouped PostgreSQL query and return structured recall rows

- [ ] **Step 1: Write failing tests for grouped expanded sparse retrieval**

Write unit tests that lock the retrieval semantics:

- each semantic span becomes one retrieval group
- group members are `tier1_terms + tier2_terms`
- groups are preserved independently; retrieval must not flatten all aliases into one bag
- output preserves which term matched which field

Target examples:

```python
def test_build_expanded_sparse_groups_keeps_group_boundaries():
    groups = build_expanded_sparse_groups(plan)
    assert groups[0].terms == [
        "adhesion protein",
        "cell adhesion protein",
        "adhesion molecule",
        "cell adhesion molecule",
    ]
    assert groups[1].terms == [
        "kidney",
        "renal",
        "kidney tissue",
        "renal tissue",
    ]


def test_match_document_requires_group_level_evidence_not_flat_term_hits():
    result = match_expanded_sparse_document(plan, document_fields={...})
    assert result.matched_span_count == 2
    assert result.total_span_count == 2
```

- [ ] **Step 2: Run the expanded sparse retrieval tests and verify they fail**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- module import error or missing function failures

- [ ] **Step 3: Implement expanded sparse retrieval primitives**

Add:

- a grouped query representation derived from `QuerySemanticPlan`
- a PostgreSQL-backed lookup primitive in `MetadataDB.lookup_papers_by_expanded_sparse_groups(...)`
- field scanning over:
  - `papers.canonical_title`
  - `papers.canonical_abstract`
  - `paper_keywords.keyword`
- result shaping that keeps:
  - `span_id`
  - matched boolean
  - matched terms
  - matched fields
  - `matched_span_count`
  - `total_span_count`
  - `coverage_ratio`

Suggested public entrypoints:

```python
def build_expanded_sparse_groups(plan: QuerySemanticPlan) -> list[ExpandedSparseGroup]:
    ...


def match_expanded_sparse_document(
    plan: QuerySemanticPlan,
    document_fields: Mapping[str, Any],
) -> ExpandedSparseCandidate:
    ...
```

Also add the database-backed retrieval API used by PaperIndexer later:

```python
def match_papers_by_expanded_sparse_plan(
    metadata_db: MetadataDB,
    plan: QuerySemanticPlan,
    source_list: Optional[Sequence[str]] = None,
    keyword_sources: Optional[Sequence[str]] = None,
    top_k: int = 50,
) -> list[ExpandedSparseCandidate]:
    ...
```

That function must:

- convert the plan into DB query rows
- call `MetadataDB.lookup_papers_by_expanded_sparse_groups(...)`
- adapt returned mappings into typed candidate results

- [ ] **Step 4: Re-run the expanded sparse retrieval tests**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- all expanded sparse retrieval unit tests pass

- [ ] **Step 5: Commit the expanded sparse retrieval module**

```bash
git add src/docset_hub/indexing/expanded_sparse_retrieval.py src/docset_hub/storage/metadata_db.py src/docset_hub/indexing/__init__.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "feat: add expanded sparse retrieval branch primitives"
```

## Task 3: Define Coverage Engine And Coverage Ratio

**Files:**

- Create: `src/docset_hub/indexing/coverage_engine.py`
- Modify: `src/docset_hub/storage/metadata_db.py`
- Test: `tests/indexing/test_coverage_engine.py`
- Test: `tests/indexing/test_expanded_sparse_retrieval.py`

**Coverage-engine implementation clarification**

- `coverage_engine.py` must **not** invent a second matching contract separate from Task 2.
- The coverage engine must reuse the same field semantics already defined for expanded sparse retrieval:
  - `papers.canonical_title`
  - `papers.canonical_abstract`
  - `paper_keywords.keyword`
- The first version of coverage is an explanation / analysis layer over the same grouped term model:
  - same `QuerySemanticPlan`
  - same `tier1` / `tier2` term buckets
  - same span/group identity
- Coverage scoring is no longer binary at the parent span level:
  - if any parent `own_term` matches, that span gets `span_score = 1.0`
  - otherwise the span gets child partial credit based on matched child span count
  - a span with no parent match and no child spans gets `span_score = 0.0`
- For local unit tests, `coverage_engine.py` may analyze an in-memory document payload.
- For retrieval explanations, it should be able to consume the structured match rows returned by `MetadataDB.lookup_papers_by_expanded_sparse_groups(...)`.
- The PostgreSQL grouped retrieval summary in `metadata_db.py` must be upgraded to emit enough structure to compute the same per-span score as the in-memory path:
  - `own_term_matched`
  - `matched_child_count`
  - `total_child_count`
  - `span_score`

This means coverage must support two inputs:

1. direct document fields for deterministic unit tests
2. DB-returned grouped matches for retrieval/explanation paths

Suggested public APIs:

```python
def analyze_document_coverage(
    plan: QuerySemanticPlan,
    document_fields: Mapping[str, Any],
) -> CoverageReport:
    ...


def summarize_expanded_sparse_matches(
    plan: QuerySemanticPlan,
    matched_spans: Sequence[Mapping[str, Any]],
) -> CoverageReport:
    ...
```

Where `matched_spans` is the structured group-level output from the PostgreSQL expanded sparse query.

- [ ] **Step 1: Write failing tests for coverage ratio computation**

Write tests that lock these rules:

- if any parent `own_term` matches, `span_score = 1.0`
- otherwise, if the span has children, `span_score = matched_children / total_children`
- otherwise, `span_score = 0.0`
- `coverage_ratio = average(span_score)`
- `matched_span_count` still counts spans where `span_score > 0`
- `matched_terms` and `matched_fields` must remain inspectable for explanation/debug
- per-span debug output should expose at least:
  - `span_score`
  - `own_term_matched`
  - `matched_child_count`
  - `total_child_count`
- a DB span-match summary and an in-memory field scan must produce the same `matched_span_count` and `coverage_ratio`

Target examples:

```python
def test_coverage_engine_returns_full_coverage_for_alias_hits():
    result = analyze_document_coverage(
        plan=plan,
        document_fields={
            "title": "Cell adhesion molecules in renal epithelial injury",
            "abstract": "",
            "paper_keywords": [],
        },
    )
    assert result.matched_span_count == 2
    assert result.total_span_count == 2
    assert result.coverage_ratio == 1.0


def test_coverage_engine_returns_half_coverage_for_partial_child_hits():
    result = analyze_document_coverage(
        plan=plan,
        document_fields={
            "title": "Adhesion signaling in disease",
            "abstract": "",
            "paper_keywords": [],
        },
    )
    assert result.matched_span_count == 1
    assert result.matched_spans[0]["span_score"] == 0.5
    assert result.coverage_ratio == 0.5


def test_summarize_expanded_sparse_matches_matches_in_memory_coverage():
    db_summary = summarize_expanded_sparse_matches(
        plan=plan,
        matched_spans=[
            {
                "span_id": "s1",
                "matched_terms": ["adhesion"],
                "matched_fields": ["title"],
                "matched_scopes": ["child"],
                "matched_child_span_ids": ["s1.1"],
                "own_term_matched": False,
                "matched_child_count": 1,
                "total_child_count": 2,
                "span_score": 0.5,
            },
            {
                "span_id": "s2",
                "matched_terms": ["renal"],
                "matched_fields": ["title"],
                "matched_scopes": ["parent"],
                "matched_child_span_ids": [],
                "own_term_matched": True,
                "matched_child_count": 0,
                "total_child_count": 0,
                "span_score": 1.0,
            },
        ],
    )
    assert db_summary.matched_span_count == 2
    assert db_summary.coverage_ratio == 0.75
```

- [ ] **Step 2: Run the coverage tests and verify they fail**

Run:

```bash
python3 -m pytest tests/indexing/test_coverage_engine.py tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- missing coverage engine module or failing assertions

- [ ] **Step 3: Implement the coverage engine**

Add:

- field normalization helper for `title`, `abstract`, and `paper_keywords`
- span-level field matching helper that distinguishes parent-term hits from child-span hits
- coverage result dataclasses
- one public API that returns a full coverage report from direct document fields
- one public API that converts DB span-match rows into the same coverage report shape
- shared per-span score computation used by both direct-field analysis and DB summary normalization

Suggested shape:

```python
def analyze_document_coverage(
    plan: QuerySemanticPlan,
    document_fields: Mapping[str, Any],
) -> CoverageReport:
    ...


def summarize_expanded_sparse_matches(
    plan: QuerySemanticPlan,
    matched_spans: Sequence[Mapping[str, Any]],
) -> CoverageReport:
    ...
```

`CoverageReport` should be the single explanation contract shared by both paths. It should contain:

- `matched_span_count`
- `total_span_count`
- `coverage_ratio`
- `matched_spans`
- `missing_spans`
- per-span matched terms / matched fields
- per-span score/debug fields:
  - `span_score`
  - `own_term_matched`
  - `matched_child_count`
  - `total_child_count`

Do not add a second ratio formula. Both direct-field analysis and DB-match summarization must use:

```text
coverage_ratio = average(span_score)
```

- [ ] **Step 4: Re-run the coverage tests**

Run:

```bash
python3 -m pytest tests/indexing/test_coverage_engine.py tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- coverage tests pass and expanded sparse retrieval still passes

- [ ] **Step 5: Commit the coverage engine**

```bash
git add src/docset_hub/indexing/coverage_engine.py src/docset_hub/storage/metadata_db.py tests/indexing/test_coverage_engine.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "feat: add full-text coverage engine"
```

## Task 4: Wire The New Plan And Branch Into `PaperIndexer` Without RRF Changes

**Files:**

- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Test: `tests/indexing/test_expanded_sparse_retrieval.py`

- [ ] **Step 1: Write failing tests for the PaperIndexer branch entrypoint**

Lock the initial integration boundary:

- PaperIndexer can build a `QuerySemanticPlan` from a query
- PaperIndexer can expose an isolated expanded sparse retrieval call
- this integration does not change the existing `dense / sparse / keyword_lookup` orchestration yet

Suggested target:

```python
def test_paper_indexer_builds_query_semantic_plan_for_expanded_sparse_branch():
    plan = indexer._build_query_semantic_plan("adhesion protein in kidney", ...)
    assert len(plan.spans) == 2
```

- [ ] **Step 2: Run the indexer integration tests and verify they fail**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- missing entrypoint or failing integration assertions

- [ ] **Step 3: Implement the isolated indexer helpers**

Add helpers along the lines of:

- `_build_query_semantic_plan(...)`
- `_run_expanded_sparse_retrieval_branch(...)`

Keep them isolated so later RRF integration can compose them without redesigning the plan or coverage contracts.

- [ ] **Step 4: Re-run the branch integration tests**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- indexer branch integration tests pass

- [ ] **Step 5: Commit the indexer integration**

```bash
git add src/docset_hub/indexing/paper_indexer.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "feat: wire expanded sparse branch into paper indexer"
```

## Task 5: Verification Sweep

**Files:**

- Verify only

- [ ] **Step 1: Run structural compile checks**

Run:

```bash
python3 -m py_compile \
  src/docset_hub/indexing/query_semantic_plan.py \
  src/docset_hub/indexing/expanded_sparse_retrieval.py \
  src/docset_hub/indexing/coverage_engine.py \
  src/docset_hub/indexing/paper_indexer.py \
  tests/indexing/test_query_semantic_plan.py \
  tests/indexing/test_expanded_sparse_retrieval.py \
  tests/indexing/test_coverage_engine.py \
  tests/indexing/test_span_matcher.py
```

Expected:

- no syntax errors

- [ ] **Step 2: Run the new unit suites**

Run:

```bash
python3 -m pytest \
  tests/indexing/test_query_semantic_plan.py \
  tests/indexing/test_expanded_sparse_retrieval.py \
  tests/indexing/test_coverage_engine.py \
  tests/indexing/test_span_matcher.py -q
```

Expected:

- all local unit tests for plan / retrieval / coverage pass

- [ ] **Step 3: Run adjacent regression tests**

Run:

```bash
python3 -m pytest \
  tests/indexing/test_entity_filter_policy.py \
  tests/indexing/test_span_matcher.py \
  tests/integration/test_span_matcher_real_services.py \
  tests/scripts/test_run_span_matcher_trace.py -q
```

Expected:

- local tests pass
- real-service integration test may skip unless explicitly enabled by env

- [ ] **Step 4: Commit any final test-only follow-up**

```bash
git add tests/indexing tests/integration/test_span_matcher_real_services.py tests/scripts/test_run_span_matcher_trace.py
git commit -m "test: add regression coverage for semantic plan and coverage engine"
```

## Self-Review

Spec coverage check:

- span matcher extension is covered by Task 1
- expanded sparse retrieval branch is covered by Tasks 2 and 4
- coverage ratio computation is covered by Task 3
- explicit tests for items `1/2/3` are covered by Tasks 1, 2, and 3 plus the verification sweep
- RRF is intentionally deferred

Placeholder scan:

- no `TODO` / `TBD` placeholders remain

Type consistency check:

- the plan consistently uses `QuerySemanticPlan`, `SemanticSpanGroup`, `ExpandedSparseCandidate`, and `CoverageReport`
