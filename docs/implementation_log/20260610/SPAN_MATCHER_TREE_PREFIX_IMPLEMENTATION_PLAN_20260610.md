# Span Matcher Tree And Prefix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the query semantic plan from flat span groups to one-level semantic span trees with structured term match modes, then propagate that contract into expanded sparse retrieval, coverage analysis, and trace output.

**Architecture:** Keep the existing `QueryPhraseAnalyzer -> SpanMatcherExecutor -> MaximalConceptSelector` pipeline unchanged for span selection. Refactor the plan layer so each selected top-level span produces `own_terms` plus one level of `children` derived only from `subphrase` candidates, and represent every term as `{text, match_mode}` where `match_mode in {"exact", "prefix"}`. Then adapt downstream consumers to read the richer contract instead of the current flat `tier1_terms` / `tier2_terms` lists.

**Tech Stack:** Python, pytest, existing `src/docset_hub/indexing/*` modules, existing `MetadataDB` PostgreSQL lookup path, existing trace script under `scripts/run_span_matcher_trace.py`.

---

## Scope Decisions Locked For This Plan

- Top-level semantic spans still come from `SelectedConcept[]`.
- Children come only from `subphrase` candidates already produced by `SpanMatcherExecutor.expand_candidates(...)`.
- Child expansion is exactly one level. No grandchildren.
- Only two term match modes are supported:
  - `exact`
  - `prefix`
- Prefix syntax is only `alias-`.
- `alias-` means strict token-prefix matching on `alias`, not substring matching.
- Expanded sparse retrieval and coverage engine must both read the new term structure.
- This plan does not add new retrieval scoring rules beyond preserving parent/child and exact/prefix match information.

## File Map

**Modify**

- `src/docset_hub/indexing/query_semantic_plan.py`
  - Replace flat term lists with structured term objects and one-level child spans.
- `src/docset_hub/indexing/expanded_sparse_retrieval.py`
  - Convert tree-shaped semantic spans into DB payload rows that preserve parent/child scope and match mode.
- `src/docset_hub/indexing/coverage_engine.py`
  - Evaluate coverage over structured exact/prefix terms from both parent spans and child spans.
- `src/docset_hub/indexing/paper_indexer.py`
  - Keep orchestration stable while passing through the richer `QuerySemanticPlan`.
- `src/docset_hub/indexing/__init__.py`
  - Re-export any new dataclasses or helpers introduced by the refactor.
- `src/docset_hub/storage/metadata_db.py`
  - Extend expanded sparse SQL input rows with term scope and prefix match behavior.
- `scripts/run_span_matcher_trace.py`
  - Render parent own terms, child spans, and prefix match modes in the trace output.
- `tests/indexing/test_query_semantic_plan.py`
  - Replace flat-plan expectations with parent/child term-structure expectations.
- `tests/indexing/test_expanded_sparse_retrieval.py`
  - Lock parent/child payload construction and prefix row generation.
- `tests/indexing/test_coverage_engine.py`
  - Lock exact and prefix coverage behavior across parent/child terms.
- `tests/indexing/test_paper_indexer.py`
  - Keep orchestration regression coverage aligned with the new plan contract.
- `tests/scripts/test_run_span_matcher_trace.py`
  - Update trace assertions to verify child span and match-mode output.

**Keep Unchanged Unless A Test Forces It**

- `src/docset_hub/indexing/span_matcher.py`
  - Do not rewrite matcher selection rules. Only read `SpanMatchResult[]` and `SelectedConcept[]`.

---

### Task 1: Refactor Query Semantic Plan Into Tree-Shaped Structured Terms

**Files:**
- Modify: `src/docset_hub/indexing/query_semantic_plan.py`
- Modify: `src/docset_hub/indexing/__init__.py`
- Test: `tests/indexing/test_query_semantic_plan.py`

- [ ] **Step 1: Write failing semantic-plan tests for term objects and children**

Add tests that lock the new contract:

```python
def test_build_query_semantic_plan_builds_parent_and_child_spans():
    plan = build_query_semantic_plan(
        original_query="adhesion protein in kidney",
        normalized_query="adhesion protein in kidney",
        selected_concepts=[parent_adhesion, parent_kidney],
        span_results=[adhesion_parent_result, adhesion_child_result, protein_child_result, kidney_result],
    )
    assert [span.span_id for span in plan.spans] == ["s1", "s2"]
    assert plan.spans[0].own_terms.tier1[0].text == "adhesion protein"
    assert plan.spans[0].own_terms.tier1[0].match_mode == "exact"
    assert [child.surface_text for child in plan.spans[0].children] == ["adhesion", "protein"]


def test_build_query_semantic_plan_parses_trailing_dash_alias_as_prefix_term():
    plan = build_query_semantic_plan(
        original_query="kidney",
        normalized_query="kidney",
        selected_concepts=[kidney_selected],
        span_results=[kidney_parent_result],
    )
    assert ("renal", "exact") in [(t.text, t.match_mode) for t in plan.spans[0].own_terms.tier2]
    assert ("renal", "prefix") in [(t.text, t.match_mode) for t in plan.spans[0].own_terms.tier2]
```

- [ ] **Step 2: Run the targeted semantic-plan tests and verify they fail**

Run:

```bash
python3 -m pytest tests/indexing/test_query_semantic_plan.py -q
```

Expected:

- failures because `SemanticSpanGroup` has no `own_terms`
- failures because children and term objects do not exist yet

- [ ] **Step 3: Implement structured plan dataclasses and builder helpers**

Refactor `query_semantic_plan.py` around explicit term containers:

```python
@dataclass(frozen=True)
class SemanticTerm:
    text: str
    match_mode: str


@dataclass
class SemanticTermBucket:
    tier1: list[SemanticTerm] = field(default_factory=list)
    tier2: list[SemanticTerm] = field(default_factory=list)


@dataclass
class SemanticChildSpan:
    span_id: str
    surface_text: str
    normalized_text: str
    start: int
    end: int
    canonical_text: str
    own_terms: SemanticTermBucket = field(default_factory=SemanticTermBucket)
    evidence: list[ConceptMatchEvidence] = field(default_factory=list)


@dataclass
class SemanticSpanGroup:
    span_id: str
    surface_text: str
    normalized_text: str
    start: int
    end: int
    canonical_text: str
    own_terms: SemanticTermBucket = field(default_factory=SemanticTermBucket)
    children: list[SemanticChildSpan] = field(default_factory=list)
    evidence: list[ConceptMatchEvidence] = field(default_factory=list)
```

Builder responsibilities:

- build top-level groups from `selected_concepts`
- find child nodes from `span_results` where:
  - candidate kind is `subphrase_ngram`
  - child span is strictly contained within the parent span
  - child has usable evidence
- keep child expansion to one level
- parse aliases ending with `-` into `SemanticTerm(text=<trimmed>, match_mode="prefix")`
- keep normal aliases as `match_mode="exact"`
- dedupe by `(normalized_text, match_mode)`

- [ ] **Step 4: Re-run the semantic-plan tests**

Run:

```bash
python3 -m pytest tests/indexing/test_query_semantic_plan.py -q
```

Expected:

- all semantic-plan tests pass

- [ ] **Step 5: Commit the semantic-plan refactor**

```bash
git add src/docset_hub/indexing/query_semantic_plan.py src/docset_hub/indexing/__init__.py tests/indexing/test_query_semantic_plan.py
git commit -m "feat: add tree-shaped query semantic plan"
```

---

### Task 2: Render The New Plan Structure In Span Matcher Trace Output

**Files:**
- Modify: `scripts/run_span_matcher_trace.py`
- Test: `tests/scripts/test_run_span_matcher_trace.py`

- [ ] **Step 1: Write failing trace tests for parent/child and prefix rendering**

Extend the script test so it expects richer plan output:

```python
def test_run_span_matcher_trace_renders_children_and_match_modes(...):
    report = render_trace_report(...)
    assert "children:" in report
    assert "s1.1: adhesion" in report
    assert "tier2=renal [exact], renal [prefix]" in report
```

- [ ] **Step 2: Run the trace test to verify it fails**

Run:

```bash
python3 -m pytest tests/scripts/test_run_span_matcher_trace.py -q
```

Expected:

- assertion failure because `_format_semantic_plan(...)` only prints flat `tier1` / `tier2` strings

- [ ] **Step 3: Implement trace formatting for structured terms**

Update `_format_semantic_plan(...)` to render parent own terms and child spans explicitly:

```python
def _format_term_list(terms):
    return ", ".join(f"{term.text} [{term.match_mode}]" for term in terms) if terms else "-"


def _format_semantic_plan(semantic_plan: QuerySemanticPlan) -> list[str]:
    lines = []
    for span in semantic_plan.spans:
        lines.append(f"- {span.span_id}: {span.surface_text} (canonical={span.canonical_text}, span={span.start}:{span.end})")
        lines.append(f"  own.tier1={_format_term_list(span.own_terms.tier1)}")
        lines.append(f"  own.tier2={_format_term_list(span.own_terms.tier2)}")
        if not span.children:
            lines.append("  children=-")
            continue
        lines.append("  children:")
        for child in span.children:
            lines.append(f"    - {child.span_id}: {child.surface_text} (canonical={child.canonical_text}, span={child.start}:{child.end})")
            lines.append(f"      own.tier1={_format_term_list(child.own_terms.tier1)}")
            lines.append(f"      own.tier2={_format_term_list(child.own_terms.tier2)}")
    return lines
```

- [ ] **Step 4: Re-run the trace test**

Run:

```bash
python3 -m pytest tests/scripts/test_run_span_matcher_trace.py -q
```

Expected:

- trace rendering test passes

- [ ] **Step 5: Commit the trace update**

```bash
git add scripts/run_span_matcher_trace.py tests/scripts/test_run_span_matcher_trace.py
git commit -m "test: render semantic span children in trace output"
```

---

### Task 3: Refactor Expanded Sparse Retrieval Payload Construction

**Files:**
- Modify: `src/docset_hub/indexing/expanded_sparse_retrieval.py`
- Test: `tests/indexing/test_expanded_sparse_retrieval.py`

- [ ] **Step 1: Write failing retrieval-construction tests for parent/child scope and prefix mode**

Add tests that lock the DB payload shape:

```python
def test_build_expanded_sparse_groups_emits_parent_and_child_term_rows():
    groups = build_expanded_sparse_groups(plan_with_children_and_prefix())
    assert groups[0].own_tier1_terms == [{"text": "adhesion protein", "match_mode": "exact"}]
    assert groups[0].children[0]["span_id"] == "s1.1"
    assert ("renal", "prefix") in [(term["text"], term["match_mode"]) for term in groups[1].own_tier2_terms]


def test_build_expanded_sparse_query_rows_preserves_term_scope():
    rows = build_expanded_sparse_query_rows(plan_with_children_and_prefix())
    assert {"span_scope": "parent", "match_mode": "exact", "term": "adhesion protein"} in rows
    assert {"span_scope": "child", "child_span_id": "s1.1", "match_mode": "exact", "term": "adhesion"} in rows
    assert {"span_scope": "parent", "match_mode": "prefix", "term": "renal"} in rows
```

- [ ] **Step 2: Run the expanded sparse retrieval test to verify it fails**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- failures because the module still returns flat string term lists

- [ ] **Step 3: Implement richer retrieval payload builders**

Refactor `expanded_sparse_retrieval.py` to separate:

```python
def build_expanded_sparse_groups(plan: QuerySemanticPlan) -> list[ExpandedSparseGroup]:
    ...


def build_expanded_sparse_query_rows(plan: QuerySemanticPlan) -> list[dict[str, Any]]:
    ...
```

Recommended row shape:

```python
{
    "group_id": 1,
    "span_id": "s1",
    "canonical_text": "Adhesion protein",
    "span_scope": "parent",
    "child_span_id": None,
    "term_tier": "tier1",
    "match_mode": "exact",
    "term": "adhesion protein",
}
```

Child example:

```python
{
    "group_id": 1,
    "span_id": "s1",
    "canonical_text": "Adhesion protein",
    "span_scope": "child",
    "child_span_id": "s1.1",
    "term_tier": "tier1",
    "match_mode": "exact",
    "term": "adhesion",
}
```

- [ ] **Step 4: Re-run the expanded sparse retrieval unit test**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- payload construction tests pass

- [ ] **Step 5: Commit the retrieval payload refactor**

```bash
git add src/docset_hub/indexing/expanded_sparse_retrieval.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "feat: build expanded sparse rows from semantic span trees"
```

---

### Task 4: Extend MetadataDB Expanded Sparse SQL For Prefix And Child Scope

**Files:**
- Modify: `src/docset_hub/storage/metadata_db.py`
- Modify: `tests/indexing/test_expanded_sparse_retrieval.py`

- [ ] **Step 1: Write failing integration-style unit coverage for prefix and child rows**

Add a fake-DB or row-normalization test around the SQL input normalization helper:

```python
def test_normalize_expanded_sparse_groups_keeps_match_mode_and_scope():
    rows = metadata_db._normalize_expanded_sparse_groups([
        {
            "group_id": 2,
            "span_id": "s2",
            "canonical_text": "Kidney",
            "span_scope": "parent",
            "child_span_id": None,
            "term_tier": "tier2",
            "match_mode": "prefix",
            "term": "renal",
        }
    ])
    assert rows[0]["match_mode"] == "prefix"
    assert rows[0]["span_scope"] == "parent"
```

- [ ] **Step 2: Run the targeted test to verify it fails**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- failures because `lookup_papers_by_expanded_sparse_groups(...)` and its normalization helper do not accept `match_mode` or child scope fields yet

- [ ] **Step 3: Implement SQL row support for exact and prefix matching**

Extend the SQL input CTE to carry:

```python
("group_id", "span_id", "canonical_text", "span_scope", "child_span_id", "term", "term_tier", "match_mode")
```

Use SQL semantics like:

```sql
POSITION(qt.term IN lower(COALESCE(p.canonical_title, ''))) > 0
```

for `match_mode = 'exact'`, and token-prefix regex for `match_mode = 'prefix'`, for example:

```sql
lower(COALESCE(p.canonical_title, '')) ~ ('(^|[^a-z0-9])' || regexp_replace(qt.term, '([.^$*+?(){}\\[\\]|\\\\])', '\\\\\\1', 'g'))
```

with an added trailing character class that permits token continuation:

```sql
lower(COALESCE(p.canonical_title, '')) ~ ('(^|[^a-z0-9])' || escaped_term || '[a-z0-9_-]*')
```

Also preserve scope information in `matched_spans`:

```python
{
    "group_id": group_id,
    "span_id": span_id,
    "canonical_text": canonical_text,
    "matched_terms": matched_terms,
    "matched_fields": matched_fields,
    "matched_scopes": matched_scopes,
    "matched_child_span_ids": matched_child_span_ids,
}
```

- [ ] **Step 4: Re-run the expanded sparse retrieval unit tests**

Run:

```bash
python3 -m pytest tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- tests covering `match_mode` and child scope now pass

- [ ] **Step 5: Commit the MetadataDB SQL extension**

```bash
git add src/docset_hub/storage/metadata_db.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "feat: support prefix and child scope in expanded sparse SQL"
```

---

### Task 5: Update Coverage Engine To Consume Parent And Child Structured Terms

**Files:**
- Modify: `src/docset_hub/indexing/coverage_engine.py`
- Test: `tests/indexing/test_coverage_engine.py`

- [ ] **Step 1: Write failing coverage tests for child matches and prefix matches**

Add tests like:

```python
def test_coverage_engine_counts_parent_span_as_matched_when_child_term_hits():
    result = analyze_document_coverage(
        plan=plan_with_children_and_prefix(),
        document_fields={"title": "Adhesion control in epithelial tissue", "abstract": "", "paper_keywords": []},
    )
    assert result.matched_span_count == 1
    assert result.matched_spans[0]["matched_scopes"] == ["child"]


def test_coverage_engine_supports_prefix_term_matching_without_substring_false_positive():
    positive = analyze_document_coverage(
        plan=kidney_prefix_plan(),
        document_fields={"title": "renalac transport", "abstract": "", "paper_keywords": []},
    )
    negative = analyze_document_coverage(
        plan=kidney_prefix_plan(),
        document_fields={"title": "adrenal signaling", "abstract": "", "paper_keywords": []},
    )
    assert positive.matched_span_count == 1
    assert negative.matched_span_count == 0
```

- [ ] **Step 2: Run the coverage tests to verify they fail**

Run:

```bash
python3 -m pytest tests/indexing/test_coverage_engine.py -q
```

Expected:

- failures because the engine only iterates `span.tier1_terms + span.tier2_terms` and only performs substring matching

- [ ] **Step 3: Implement structured exact/prefix matching in coverage engine**

Refactor coverage evaluation to iterate:

- parent own Tier 1
- parent own Tier 2
- child own Tier 1
- child own Tier 2

Use helpers such as:

```python
def _iter_span_terms(span: SemanticSpanGroup) -> list[dict[str, Any]]:
    ...


def _term_matches_field(term: SemanticTerm, field_value: str) -> bool:
    if term.match_mode == "exact":
        return normalized_term in field_value
    if term.match_mode == "prefix":
        return bool(re.search(r"(^|[^a-z0-9])" + re.escape(term.text) + r"[a-z0-9_-]*", field_value))
    return False
```

Include scope in the matched report:

```python
{
    "span_id": span.span_id,
    "canonical_text": span.canonical_text,
    "matched_terms": ["adhesion"],
    "matched_fields": ["title"],
    "matched_scopes": ["child"],
    "matched_child_span_ids": ["s1.1"],
}
```

- [ ] **Step 4: Re-run the coverage tests**

Run:

```bash
python3 -m pytest tests/indexing/test_coverage_engine.py -q
```

Expected:

- all coverage tests pass

- [ ] **Step 5: Commit the coverage-engine refactor**

```bash
git add src/docset_hub/indexing/coverage_engine.py tests/indexing/test_coverage_engine.py
git commit -m "feat: analyze coverage across semantic span children"
```

---

### Task 6: Update PaperIndexer Regressions And End-To-End Branch Adapters

**Files:**
- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Modify: `tests/indexing/test_paper_indexer.py`
- Modify: `tests/indexing/test_expanded_sparse_retrieval.py`

- [ ] **Step 1: Write failing orchestration tests for the new plan contract**

Add or update tests to assert that `PaperIndexer` accepts the new plan object unchanged:

```python
def test_paper_indexer_runs_expanded_sparse_branch_with_tree_plan(monkeypatch):
    plan = plan_with_children_and_prefix()
    monkeypatch.setattr(indexer, "_build_query_semantic_plan", lambda query, source_list, keyword_sources=None: plan)
    monkeypatch.setattr(
        "src.docset_hub.indexing.paper_indexer.match_papers_by_expanded_sparse_plan",
        lambda metadata_db, plan, source_list, keyword_sources=None, top_k=50: [fake_result],
    )
    branch = indexer._run_expanded_sparse_retrieval_branch(...)
    assert branch[0]["retrieval_debug"]["matched_spans"][0]["matched_scopes"] == ["parent", "child"]
```

- [ ] **Step 2: Run the targeted indexer tests to verify they fail**

Run:

```bash
python3 -m pytest tests/indexing/test_paper_indexer.py tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- assertion failures where old flat structures are assumed

- [ ] **Step 3: Implement minimal adapter updates in `paper_indexer.py`**

Keep orchestration simple:

```python
results = match_papers_by_expanded_sparse_plan(
    metadata_db=self.metadata_db,
    plan=plan,
    source_list=source_list,
    keyword_sources=keyword_sources,
    top_k=top_k,
)
```

Only ensure branch adaptation preserves the richer retrieval debug payload:

```python
"retrieval_debug": {
    "retriever": "expanded_sparse",
    "matched_span_count": matched_span_count,
    "total_span_count": total_span_count,
    "coverage_ratio": coverage_ratio,
    "matched_spans": list(getattr(result, "matched_spans", []) or []),
}
```

The important constraint is: do not flatten or strip `matched_scopes` / `matched_child_span_ids`.

- [ ] **Step 4: Re-run the targeted indexer tests**

Run:

```bash
python3 -m pytest tests/indexing/test_paper_indexer.py tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- targeted indexer and retrieval adapter tests pass

- [ ] **Step 5: Commit the orchestration update**

```bash
git add src/docset_hub/indexing/paper_indexer.py tests/indexing/test_paper_indexer.py tests/indexing/test_expanded_sparse_retrieval.py
git commit -m "test: preserve semantic span tree payload in retrieval orchestration"
```

---

### Task 7: Full Regression Pass

**Files:**
- Modify if needed: any files above based on failures

- [ ] **Step 1: Run the focused regression suite**

Run:

```bash
python3 -m pytest \
  tests/indexing/test_query_semantic_plan.py \
  tests/indexing/test_expanded_sparse_retrieval.py \
  tests/indexing/test_coverage_engine.py \
  tests/indexing/test_paper_indexer.py \
  tests/scripts/test_run_span_matcher_trace.py -q
```

Expected:

- all focused tests pass

- [ ] **Step 2: Run the span matcher trace manually**

Run:

```bash
bash scripts/run_span_matcher_db.sh --trace --use-db-keywords
```

Then enter:

```text
adhesion protein in kidney
```

Expected in output:

- top-level spans print `own.tier1` and `own.tier2`
- child spans `s1.1` and `s1.2` are rendered
- prefix aliases print as `[prefix]`

- [ ] **Step 3: Fix any regression exposed by the focused pass**

Apply only minimal fixes required by the failing assertions or manual trace output. Typical fix shapes:

```python
if child.span_id not in matched_child_span_ids:
    matched_child_span_ids.append(child.span_id)
```

or:

```python
normalized = normalized[:-1] if normalized.endswith("-") else normalized
```

- [ ] **Step 4: Re-run the focused regression suite**

Run:

```bash
python3 -m pytest \
  tests/indexing/test_query_semantic_plan.py \
  tests/indexing/test_expanded_sparse_retrieval.py \
  tests/indexing/test_coverage_engine.py \
  tests/indexing/test_paper_indexer.py \
  tests/scripts/test_run_span_matcher_trace.py -q
```

Expected:

- all focused tests pass cleanly

- [ ] **Step 5: Commit the completed feature**

```bash
git add src/docset_hub/indexing/query_semantic_plan.py src/docset_hub/indexing/expanded_sparse_retrieval.py src/docset_hub/indexing/coverage_engine.py src/docset_hub/indexing/paper_indexer.py src/docset_hub/indexing/__init__.py src/docset_hub/storage/metadata_db.py scripts/run_span_matcher_trace.py tests/indexing/test_query_semantic_plan.py tests/indexing/test_expanded_sparse_retrieval.py tests/indexing/test_coverage_engine.py tests/indexing/test_paper_indexer.py tests/scripts/test_run_span_matcher_trace.py
git commit -m "feat: add tree-shaped semantic spans with prefix-aware sparse retrieval"
```

---

## Self-Review

Spec coverage against `Span Matcher Modification Design_20260610.md`:

- top-level `own + children` structure: covered by Task 1
- children only from `subphrase candidate`: covered by Task 1 and Task 5
- one-level child expansion: covered by Task 1
- term objects with `exact | prefix`: covered by Task 1
- trailing `-` prefix parsing: covered by Task 1 and Task 5
- prefix-aware sparse retrieval: covered by Task 3 and Task 4
- coverage engine reads parent and child terms: covered by Task 5
- trace output reflects the new structure: covered by Task 2

Placeholder scan:

- no `TODO`
- no `TBD`
- no unresolved file references

Type consistency:

- semantic term type is consistently `SemanticTerm`
- child type is consistently `SemanticChildSpan`
- scope field is consistently `span_scope`
- prefix behavior is consistently `match_mode == "prefix"`

