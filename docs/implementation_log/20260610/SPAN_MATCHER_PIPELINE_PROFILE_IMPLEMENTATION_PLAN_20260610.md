# Span Matcher Pipeline Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a single end-to-end span matcher pipeline with explicit profiles so scripts, pages, expanded sparse retrieval, and `PaperIndexer` all build span matcher results through the same contract.

**Architecture:** Add a deep `SpanMatcherPipeline` module whose small interface accepts a query and returns normalized query, candidates, span results, selected concepts, semantic plan, timings, and optional trace details. Add a pure `SpanMatcherProfile` value object that names runtime behavior such as `ontology_plus_keyword`, `keyword_only`, and `ontology_only`; factories use the profile plus runtime adapters such as `MetadataDB` to build analyzer, matchers, executor, and selector. `PaperIndexer` online retrieval defaults to `ontology_plus_keyword`, not `keyword_only`.

**Tech Stack:** Python dataclasses, pytest, existing `src/docset_hub/indexing/*` span matcher modules, existing `MetadataDBPhraseLexicon`, existing `RemoteOntologySpanMatcher`, Flask develop pages, scripts under `scripts/`.

---

## Scope Decisions Locked For This Plan

- `SpanMatcherProfile` is pure configuration. It must not own heavy runtime objects such as `MetadataDB`, spaCy pipelines, HTTP sessions, or Flask app state.
- `SpanMatcherPipeline` owns orchestration from input query to `SpanMatcherRunResult`.
- The pipeline must support the following named profiles:
  - `keyword_only`
  - `ontology_only`
  - `ontology_plus_keyword`
- `ontology_plus_keyword` is the default for:
  - `scripts/run_span_matcher_trace.py`
  - `app/span_matcher_page.py`
  - `app/expanded_compare_page.py`
  - `PaperIndexer` online retrieval semantic-plan construction
- `keyword_only` remains available for targeted DB-only debugging, but it must be explicit.
- Runtime profiles keep `enable_scispacy=True` by default unless `SKIP_SCISPACY=1` or `--skip-scispacy` is set. Unit tests may pass `enable_scispacy=False` to avoid depending on the local `en_core_sci_lg` model.
- `run_span_matcher_trace.py` remains responsible for report formatting. It must stop owning pipeline assembly.
- Existing trace report sections should remain stable unless a test requires a small naming adjustment.
- `QuerySemanticPlan` shape from `SPAN_MATCHER_TREE_PREFIX_IMPLEMENTATION_PLAN_20260610.md` remains unchanged.
- This plan does not change `MaximalConceptSelector` selection rules.

## File Map

**Create**

- `src/docset_hub/indexing/span_matcher_pipeline.py`
  - Define `SpanMatcherProfile`, `SpanMatcherRunResult`, `SpanMatcherTrace`, `SpanMatcherPipeline`, and factory helpers.

**Modify**

- `src/docset_hub/indexing/__init__.py`
  - Re-export the new pipeline/profile/result classes.
- `scripts/run_span_matcher_trace.py`
  - Replace local pipeline assembly with `SpanMatcherPipeline.from_profile(...).run(...)`.
  - Keep report rendering in the script.
- `app/span_matcher_page.py`
  - Replace `_get_span_matcher_context(...)` and manual run steps with `SpanMatcherPipeline`.
- `src/docset_hub/indexing/paper_indexer.py`
  - Replace duplicated keyword-only semantic-plan construction with `SpanMatcherPipeline`.
  - Default online retrieval profile to `ontology_plus_keyword`.
  - Allow explicit keyword-only profile for DB-only smoke/debug paths.
- `app/expanded_compare_page.py`
  - Build the semantic plan through the same pipeline/profile used by online retrieval.
- `tests/indexing/test_span_matcher_pipeline.py`
  - New focused tests for profile defaults, factory behavior, and run result contract.
- `tests/scripts/test_run_span_matcher_trace.py`
  - Update tests to assert the script delegates to the pipeline while preserving output.
- `tests/app/test_span_matcher_page.py`
  - Update page tests for pipeline use.
- `tests/app/test_expanded_compare_page.py`
  - Lock expanded compare to `ontology_plus_keyword`.
- `tests/indexing/test_expanded_sparse_retrieval.py`
  - Update `PaperIndexer` tests to verify online retrieval uses the shared profile.

**Keep Unchanged Unless A Test Forces It**

- `src/docset_hub/indexing/span_matcher.py`
  - Keep evidence matcher, executor, and selector behavior as-is.
- `src/docset_hub/indexing/query_semantic_plan.py`
  - Keep semantic plan dataclasses and tree-building semantics as-is.

---

### Task 1: Add Profile And Pipeline Result Contracts

**Files:**
- Create: `src/docset_hub/indexing/span_matcher_pipeline.py`
- Modify: `src/docset_hub/indexing/__init__.py`
- Test: `tests/indexing/test_span_matcher_pipeline.py`

- [ ] **Step 1: Write failing tests for profile presets**

Create `tests/indexing/test_span_matcher_pipeline.py` with:

```python
from src.docset_hub.indexing.span_matcher_pipeline import SpanMatcherProfile


def test_ontology_plus_keyword_profile_is_default_online_profile():
    profile = SpanMatcherProfile.ontology_plus_keyword()

    assert profile.name == "ontology_plus_keyword"
    assert profile.enable_ontology is True
    assert profile.enable_keyword is True
    assert profile.include_subphrases is True
    assert profile.build_semantic_plan is True
    assert profile.paper_sources == ("langtaosha", "biorxiv_history", "biorxiv_daily")


def test_keyword_only_profile_is_explicit_db_only_mode():
    profile = SpanMatcherProfile.keyword_only(paper_sources=("langtaosha",))

    assert profile.name == "keyword_only"
    assert profile.enable_ontology is False
    assert profile.enable_keyword is True
    assert profile.paper_sources == ("langtaosha",)


def test_ontology_only_profile_disables_keyword_matcher():
    profile = SpanMatcherProfile.ontology_only(ontology_sources=("mesh",))

    assert profile.name == "ontology_only"
    assert profile.enable_ontology is True
    assert profile.enable_keyword is False
    assert profile.ontology_sources == ("mesh",)
```

- [ ] **Step 2: Run the profile tests and verify they fail**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- import failure because `span_matcher_pipeline.py` does not exist yet.

- [ ] **Step 3: Implement `SpanMatcherProfile` and result dataclasses**

Create `src/docset_hub/indexing/span_matcher_pipeline.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .query_phrase_analyzer import PhraseCandidate
from .query_semantic_plan import QuerySemanticPlan
from .span_matcher import ConceptMatchEvidence, SelectedConcept, SpanMatchResult


DEFAULT_PAPER_SOURCES = ("langtaosha", "biorxiv_history", "biorxiv_daily")
DEFAULT_ONTOLOGY_SOURCES = ("umls", "mesh")
DEFAULT_ONTOLOGY_LINKER_URL = "http://127.0.0.1:8765"
DEFAULT_SCISPACY_MODEL = "en_core_sci_lg"


@dataclass(frozen=True)
class SpanMatcherProfile:
    name: str
    enable_scispacy: bool = True
    scispacy_model: str = DEFAULT_SCISPACY_MODEL
    enable_ontology: bool = True
    ontology_base_url: str = DEFAULT_ONTOLOGY_LINKER_URL
    ontology_sources: Tuple[str, ...] = DEFAULT_ONTOLOGY_SOURCES
    ontology_top_k: int = 2
    ontology_threshold: float = 0.9
    ontology_timeout: float = 20.0
    enable_keyword: bool = True
    paper_sources: Tuple[str, ...] = DEFAULT_PAPER_SOURCES
    keyword_sources: Tuple[str, ...] = ()
    include_subphrases: bool = True
    evidence_threshold: Optional[float] = None
    build_semantic_plan: bool = True

    @classmethod
    def ontology_plus_keyword(cls, **overrides: Any) -> "SpanMatcherProfile":
        return cls(name="ontology_plus_keyword", enable_ontology=True, enable_keyword=True, **overrides)

    @classmethod
    def keyword_only(cls, **overrides: Any) -> "SpanMatcherProfile":
        return cls(name="keyword_only", enable_ontology=False, enable_keyword=True, **overrides)

    @classmethod
    def ontology_only(cls, **overrides: Any) -> "SpanMatcherProfile":
        return cls(name="ontology_only", enable_ontology=True, enable_keyword=False, **overrides)


@dataclass
class SpanMatcherTrace:
    raw_ontology_items: Dict[str, List[Mapping[str, Any]]] = field(default_factory=dict)
    filtered_ontology_evidence: Dict[str, List[ConceptMatchEvidence]] = field(default_factory=dict)
    keyword_evidence: Dict[str, List[ConceptMatchEvidence]] = field(default_factory=dict)


@dataclass
class SpanMatcherRunResult:
    profile_name: str
    query: str
    normalized_query: str
    extractor_candidates: List[PhraseCandidate] = field(default_factory=list)
    expanded_candidates: List[PhraseCandidate] = field(default_factory=list)
    span_results: List[SpanMatchResult] = field(default_factory=list)
    selected_concepts: List[SelectedConcept] = field(default_factory=list)
    semantic_plan: Optional[QuerySemanticPlan] = None
    timings_ms: Dict[str, float] = field(default_factory=dict)
    trace: Optional[SpanMatcherTrace] = None
```

- [ ] **Step 4: Re-export the new contracts**

Modify `src/docset_hub/indexing/__init__.py`:

```python
from .span_matcher_pipeline import (
    SpanMatcherPipeline,
    SpanMatcherProfile,
    SpanMatcherRunResult,
    SpanMatcherTrace,
)
```

Add the four names to `__all__`.

- [ ] **Step 5: Run the profile tests and verify they pass**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- the three profile tests pass.

- [ ] **Step 6: Commit the profile contracts**

```bash
git add src/docset_hub/indexing/span_matcher_pipeline.py src/docset_hub/indexing/__init__.py tests/indexing/test_span_matcher_pipeline.py
git commit -m "feat: add span matcher profile contracts"
```

---

### Task 2: Implement End-To-End Pipeline Execution

**Files:**
- Modify: `src/docset_hub/indexing/span_matcher_pipeline.py`
- Test: `tests/indexing/test_span_matcher_pipeline.py`

- [ ] **Step 1: Write failing tests for `SpanMatcherPipeline.run(...)`**

Append tests to `tests/indexing/test_span_matcher_pipeline.py`:

```python
from types import SimpleNamespace

from src.docset_hub.indexing.query_phrase_analyzer import PhraseCandidate
from src.docset_hub.indexing.span_matcher import ConceptMatchEvidence, SpanMatchResult
from src.docset_hub.indexing.span_matcher_pipeline import SpanMatcherPipeline, SpanMatcherProfile


def _candidate(text: str, start: int, end: int) -> PhraseCandidate:
    return PhraseCandidate(
        text=text,
        normalized_text=text,
        kind="connector_split",
        start=start,
        end=end,
    )


def _evidence(candidate_text: str) -> ConceptMatchEvidence:
    return ConceptMatchEvidence(
        source="keyword",
        concept_id=f"keyword:{candidate_text}",
        canonical=candidate_text,
        candidate_text=candidate_text,
        match_type="keyword_exact",
        confidence=1.0,
        aliases=[],
        payload={},
    )


def test_pipeline_run_returns_selected_concepts_and_semantic_plan():
    normalizer = SimpleNamespace(
        normalize_query=lambda query: SimpleNamespace(original_query=query, normalized_query=query)
    )
    candidates = [_candidate("kidney", 0, 6)]
    analyzer = SimpleNamespace(
        normalizer=normalizer,
        scispacy_pipeline=None,
        extractor=SimpleNamespace(extract=lambda normalized_query, scispacy_doc=None: candidates),
    )
    executor = SimpleNamespace(
        expand_candidates=lambda items: list(items),
        match_candidates=lambda items: [
            SpanMatchResult(candidate=candidates[0], evidence=[_evidence("kidney")])
        ],
    )

    pipeline = SpanMatcherPipeline(
        profile=SpanMatcherProfile.keyword_only(enable_scispacy=False),
        analyzer=analyzer,
        executor=executor,
    )

    result = pipeline.run("kidney")

    assert result.profile_name == "keyword_only"
    assert result.normalized_query == "kidney"
    assert [concept.candidate.text for concept in result.selected_concepts] == ["kidney"]
    assert result.semantic_plan is not None
    assert [span.surface_text for span in result.semantic_plan.spans] == ["kidney"]
    assert set(result.timings_ms) >= {"normalize", "extract", "match", "select", "build_plan"}


def test_pipeline_applies_evidence_threshold_before_selection():
    normalizer = SimpleNamespace(
        normalize_query=lambda query: SimpleNamespace(original_query=query, normalized_query=query)
    )
    candidates = [_candidate("kidney", 0, 6)]
    weak_evidence = _evidence("kidney")
    weak_evidence.confidence = 0.5
    analyzer = SimpleNamespace(
        normalizer=normalizer,
        scispacy_pipeline=None,
        extractor=SimpleNamespace(extract=lambda normalized_query, scispacy_doc=None: candidates),
    )
    executor = SimpleNamespace(
        expand_candidates=lambda items: list(items),
        match_candidates=lambda items: [
            SpanMatchResult(candidate=candidates[0], evidence=[weak_evidence])
        ],
    )
    pipeline = SpanMatcherPipeline(
        profile=SpanMatcherProfile.keyword_only(enable_scispacy=False, evidence_threshold=0.9),
        analyzer=analyzer,
        executor=executor,
    )

    result = pipeline.run("kidney")

    assert result.selected_concepts == []
    assert result.semantic_plan is not None
    assert result.semantic_plan.spans == []
```

- [ ] **Step 2: Run the pipeline tests and verify they fail**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- failures because `SpanMatcherPipeline` is not implemented.

- [ ] **Step 3: Implement `SpanMatcherPipeline.run(...)`**

Add to `src/docset_hub/indexing/span_matcher_pipeline.py`:

```python
import time

from .query_semantic_plan import build_query_semantic_plan
from .span_matcher import MaximalConceptSelector, SpanMatchResult


class SpanMatcherPipeline:
    def __init__(
        self,
        *,
        profile: SpanMatcherProfile,
        analyzer: Any,
        executor: Any,
        selector: Optional[MaximalConceptSelector] = None,
    ) -> None:
        self.profile = profile
        self.analyzer = analyzer
        self.executor = executor
        self.selector = selector or MaximalConceptSelector()

    def run(self, query: str, *, trace: bool = False) -> SpanMatcherRunResult:
        normalized_query = (query or "").strip()
        if not normalized_query:
            raise ValueError("query 不能为空")

        timings_ms: Dict[str, float] = {}

        normalize_started = time.perf_counter()
        normalized = self.analyzer.normalizer.normalize_query(normalized_query)
        timings_ms["normalize"] = round((time.perf_counter() - normalize_started) * 1000.0, 3)

        scispacy_doc = None
        if self.profile.enable_scispacy and self.analyzer.scispacy_pipeline is not None and normalized.normalized_query:
            scispacy_doc = self.analyzer.scispacy_pipeline(normalized.normalized_query)

        extract_started = time.perf_counter()
        extractor_candidates = self.analyzer.extractor.extract(
            normalized.normalized_query,
            scispacy_doc=scispacy_doc,
        )
        timings_ms["extract"] = round((time.perf_counter() - extract_started) * 1000.0, 3)

        expanded_candidates = self.executor.expand_candidates(extractor_candidates)

        match_started = time.perf_counter()
        span_results = self.executor.match_candidates(extractor_candidates)
        timings_ms["match"] = round((time.perf_counter() - match_started) * 1000.0, 3)

        filtered_results = self._filter_span_results(span_results)

        select_started = time.perf_counter()
        selected_concepts = self.selector.select(filtered_results)
        timings_ms["select"] = round((time.perf_counter() - select_started) * 1000.0, 3)

        semantic_plan = None
        if self.profile.build_semantic_plan:
            plan_started = time.perf_counter()
            semantic_plan = build_query_semantic_plan(
                original_query=normalized.original_query,
                normalized_query=normalized.normalized_query,
                selected_concepts=selected_concepts,
                span_results=filtered_results,
            )
            timings_ms["build_plan"] = round((time.perf_counter() - plan_started) * 1000.0, 3)

        return SpanMatcherRunResult(
            profile_name=self.profile.name,
            query=normalized.original_query,
            normalized_query=normalized.normalized_query,
            extractor_candidates=list(extractor_candidates),
            expanded_candidates=list(expanded_candidates),
            span_results=list(filtered_results),
            selected_concepts=list(selected_concepts),
            semantic_plan=semantic_plan,
            timings_ms=timings_ms,
            trace=SpanMatcherTrace() if trace else None,
        )

    def _filter_span_results(self, span_results: Sequence[SpanMatchResult]) -> List[SpanMatchResult]:
        if self.profile.evidence_threshold is None:
            return list(span_results)
        return [
            SpanMatchResult(
                candidate=result.candidate,
                evidence=[
                    evidence
                    for evidence in result.evidence
                    if float(evidence.confidence) > float(self.profile.evidence_threshold)
                ],
            )
            for result in span_results
        ]
```

- [ ] **Step 4: Run the pipeline tests and verify they pass**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- all profile and pipeline tests pass.

- [ ] **Step 5: Commit the pipeline run contract**

```bash
git add src/docset_hub/indexing/span_matcher_pipeline.py tests/indexing/test_span_matcher_pipeline.py
git commit -m "feat: run span matcher pipeline end to end"
```

---

### Task 3: Add Runtime Factory From Profile

**Files:**
- Modify: `src/docset_hub/indexing/span_matcher_pipeline.py`
- Test: `tests/indexing/test_span_matcher_pipeline.py`

- [ ] **Step 1: Write failing factory tests**

Append tests:

```python
from src.docset_hub.indexing.span_matcher import CompositeSpanMatcher, KeywordSurfaceSpanMatcher, RemoteOntologySpanMatcher


class FakeMetadataDB:
    default_sources = ["langtaosha"]


def test_from_profile_builds_ontology_plus_keyword_pipeline():
    pipeline = SpanMatcherPipeline.from_profile(
        profile=SpanMatcherProfile.ontology_plus_keyword(
            enable_scispacy=False,
            ontology_base_url="http://127.0.0.1:8765",
            paper_sources=("langtaosha",),
        ),
        metadata_db=FakeMetadataDB(),
    )

    assert pipeline.profile.name == "ontology_plus_keyword"
    assert isinstance(pipeline.executor.matcher, CompositeSpanMatcher)
    matcher_types = {type(matcher) for matcher in pipeline.executor.matcher.matchers}
    assert RemoteOntologySpanMatcher in matcher_types
    assert KeywordSurfaceSpanMatcher in matcher_types


def test_from_profile_builds_keyword_only_pipeline_without_ontology():
    pipeline = SpanMatcherPipeline.from_profile(
        profile=SpanMatcherProfile.keyword_only(enable_scispacy=False, paper_sources=("langtaosha",)),
        metadata_db=FakeMetadataDB(),
    )

    matcher_types = {type(matcher) for matcher in pipeline.executor.matcher.matchers}
    assert RemoteOntologySpanMatcher not in matcher_types
    assert KeywordSurfaceSpanMatcher in matcher_types
```

- [ ] **Step 2: Run factory tests and verify they fail**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- failures because `from_profile` does not exist.

- [ ] **Step 3: Implement `from_profile(...)`**

Add imports:

```python
from .query_phrase_analyzer import MetadataDBPhraseLexicon, QueryPhraseAnalyzer
from .span_matcher import CompositeSpanMatcher, KeywordSurfaceSpanMatcher, RemoteOntologySpanMatcher, SpanMatcherExecutor
```

Add helper and factory:

```python
def _load_scispacy_pipeline(profile: SpanMatcherProfile) -> Optional[Any]:
    if not profile.enable_scispacy:
        return None
    try:
        import spacy
    except ImportError:
        return None
    try:
        return spacy.load(profile.scispacy_model)
    except OSError:
        return None


class SpanMatcherPipeline:
    ...

    @classmethod
    def from_profile(
        cls,
        *,
        profile: SpanMatcherProfile,
        metadata_db: Optional[Any] = None,
    ) -> "SpanMatcherPipeline":
        if profile.enable_keyword and metadata_db is None:
            raise ValueError("metadata_db is required when enable_keyword=True")

        lexicon = None
        matchers: List[Any] = []
        if profile.enable_ontology:
            matchers.append(
                RemoteOntologySpanMatcher(
                    base_url=profile.ontology_base_url,
                    sources=profile.ontology_sources,
                    top_k=profile.ontology_top_k,
                    threshold=profile.ontology_threshold,
                    timeout=profile.ontology_timeout,
                )
            )
        if profile.enable_keyword:
            lexicon = MetadataDBPhraseLexicon(
                metadata_db=metadata_db,
                paper_source_names=profile.paper_sources,
                keyword_sources=profile.keyword_sources,
            )
            matchers.append(KeywordSurfaceSpanMatcher(lexicon=lexicon))

        analyzer = QueryPhraseAnalyzer(
            lexicon=lexicon,
            scispacy_pipeline=_load_scispacy_pipeline(profile),
        )
        executor = SpanMatcherExecutor(
            matcher=CompositeSpanMatcher(matchers),
            include_subphrases=profile.include_subphrases,
        )
        return cls(
            profile=profile,
            analyzer=analyzer,
            executor=executor,
            selector=MaximalConceptSelector(),
        )
```

- [ ] **Step 4: Run factory tests and verify they pass**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- all pipeline tests pass.

- [ ] **Step 5: Commit the profile factory**

```bash
git add src/docset_hub/indexing/span_matcher_pipeline.py tests/indexing/test_span_matcher_pipeline.py
git commit -m "feat: build span matcher pipeline from profile"
```

---

### Task 4: Migrate Trace Script To Pipeline

**Files:**
- Modify: `scripts/run_span_matcher_trace.py`
- Test: `tests/scripts/test_run_span_matcher_trace.py`

- [ ] **Step 1: Write failing trace delegation test**

In `tests/scripts/test_run_span_matcher_trace.py`, add a test that monkeypatches `SpanMatcherPipeline.from_profile` and asserts `run_trace(...)` uses it:

```python
from types import SimpleNamespace


def test_run_trace_uses_span_matcher_pipeline(monkeypatch):
    import scripts.run_span_matcher_trace as trace_script

    captured = {}

    class FakePipeline:
        def run(self, query, *, trace=False):
            captured["query"] = query
            captured["trace"] = trace
            return SimpleNamespace(
                query=query,
                normalized_query=query,
                extractor_candidates=[],
                expanded_candidates=[],
                trace=SimpleNamespace(
                    raw_ontology_items={},
                    filtered_ontology_evidence={},
                    keyword_evidence={},
                ),
                span_results=[],
                selected_concepts=[],
                semantic_plan=SimpleNamespace(spans=[]),
            )

    monkeypatch.setattr(
        trace_script.SpanMatcherPipeline,
        "from_profile",
        classmethod(lambda cls, **kwargs: FakePipeline()),
    )

    report = trace_script.run_trace(
        SimpleNamespace(
            ontology_linker_url="http://127.0.0.1:8765",
            ontology_source_list="umls,mesh",
            ontology_top_k=2,
            ontology_threshold=0.9,
            skip_scispacy=True,
            scispacy_model="en_core_sci_lg",
            no_subphrase_ngram=False,
            use_db_keywords=True,
            config_path="src/config/config_tecent_backend_server_mimic.yaml",
            paper_source_list="langtaosha",
        ),
        "kidney",
    )

    assert captured == {"query": "kidney", "trace": True}
    assert "=== Query Semantic Plan ===" in report
```

- [ ] **Step 2: Run the trace test and verify it fails**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/scripts/test_run_span_matcher_trace.py -q
```

Expected:

- failure because `run_trace` still assembles the pipeline manually.

- [ ] **Step 3: Update `run_span_matcher_trace.py` to build a profile**

Import:

```python
from docset_hub.indexing.span_matcher_pipeline import SpanMatcherPipeline, SpanMatcherProfile
```

Add helper:

```python
def build_profile(args: argparse.Namespace) -> SpanMatcherProfile:
    if args.use_db_keywords:
        factory = SpanMatcherProfile.ontology_plus_keyword
    else:
        factory = SpanMatcherProfile.ontology_only
    return factory(
        enable_scispacy=not args.skip_scispacy,
        scispacy_model=args.scispacy_model,
        ontology_base_url=args.ontology_linker_url,
        ontology_sources=tuple(parse_csv(args.ontology_source_list)),
        ontology_top_k=args.ontology_top_k,
        ontology_threshold=args.ontology_threshold,
        paper_sources=tuple(parse_csv(args.paper_source_list)),
        include_subphrases=not args.no_subphrase_ngram,
    )
```

Change `run_trace(...)` to:

```python
def run_trace(args: argparse.Namespace, query: str) -> str:
    profile = build_profile(args)
    metadata_db = MetadataDB(config_path=args.config_path) if profile.enable_keyword else None
    result = SpanMatcherPipeline.from_profile(
        profile=profile,
        metadata_db=metadata_db,
    ).run(query, trace=True)
    trace = result.trace or SpanMatcherTrace()
    return render_trace_report(
        query=result.query,
        normalized_query=result.normalized_query,
        extractor_candidates=result.extractor_candidates,
        expanded_candidates=result.expanded_candidates,
        raw_ontology_items=trace.raw_ontology_items,
        filtered_ontology_evidence=trace.filtered_ontology_evidence,
        keyword_evidence=trace.keyword_evidence,
        span_results=result.span_results,
        selected_concepts=result.selected_concepts,
        semantic_plan=result.semantic_plan or QuerySemanticPlan(
            original_query=result.query,
            normalized_query=result.normalized_query,
            spans=[],
        ),
    )
```

- [ ] **Step 4: Preserve raw ontology trace details**

If the existing trace tests require raw ontology evidence, extend `SpanMatcherPipeline.run(trace=True)` so it stores:

```python
SpanMatcherTrace(
    raw_ontology_items=...,
    filtered_ontology_evidence=...,
    keyword_evidence=...,
)
```

Do this by moving the script's existing `collect_ontology_trace(...)` / keyword bucket logic into pipeline-private helpers. Keep report formatting in the script.

- [ ] **Step 5: Run trace tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/scripts/test_run_span_matcher_trace.py tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- trace output tests pass.
- pipeline tests pass.

- [ ] **Step 6: Commit trace migration**

```bash
git add scripts/run_span_matcher_trace.py tests/scripts/test_run_span_matcher_trace.py src/docset_hub/indexing/span_matcher_pipeline.py tests/indexing/test_span_matcher_pipeline.py
git commit -m "refactor: run span matcher trace through pipeline"
```

---

### Task 5: Migrate Span Matcher Page To Pipeline

**Files:**
- Modify: `app/span_matcher_page.py`
- Test: `tests/app/test_span_matcher_page.py`

- [ ] **Step 1: Write failing page pipeline test**

Update `tests/app/test_span_matcher_page.py` so `run_span_matcher_test(...)` monkeypatches `SpanMatcherPipeline.from_profile` instead of individual analyzer/executor/selector helpers:

```python
def test_run_span_matcher_test_uses_ontology_plus_keyword_pipeline(monkeypatch):
    captured = {}

    class FakePipeline:
        def run(self, query):
            captured["query"] = query
            return SimpleNamespace(
                query=query,
                normalized_query=query,
                selected_concepts=[],
                semantic_plan=SimpleNamespace(
                    original_query=query,
                    normalized_query=query,
                    spans=[],
                ),
                timings_ms={"normalize": 1.0, "extract": 1.0, "match": 1.0, "select": 1.0, "build_plan": 1.0},
            )

    def fake_from_profile(*, profile, metadata_db):
        captured["profile_name"] = profile.name
        captured["enable_ontology"] = profile.enable_ontology
        captured["enable_keyword"] = profile.enable_keyword
        return FakePipeline()

    monkeypatch.setattr(
        "app.span_matcher_page.SpanMatcherPipeline.from_profile",
        fake_from_profile,
    )

    result = run_span_matcher_test("kidney", paper_indexer=SimpleNamespace(metadata_db=object(), default_sources=["langtaosha"]))

    assert captured["query"] == "kidney"
    assert captured["profile_name"] == "ontology_plus_keyword"
    assert captured["enable_ontology"] is True
    assert captured["enable_keyword"] is True
    assert result["semantic_plan"]["spans"] == []
```

- [ ] **Step 2: Run page tests and verify they fail**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/app/test_span_matcher_page.py -q
```

Expected:

- failure because the page still uses `_get_span_matcher_context(...)`.

- [ ] **Step 3: Replace page context assembly with pipeline profile**

In `app/span_matcher_page.py`, add:

```python
from src.docset_hub.indexing import SpanMatcherPipeline, SpanMatcherProfile
```

Add:

```python
def _build_span_matcher_profile(paper_indexer: Any) -> SpanMatcherProfile:
    return SpanMatcherProfile.ontology_plus_keyword(
        enable_scispacy=os.environ.get("SKIP_SCISPACY", "0") != "1",
        scispacy_model=os.environ.get("SCISPACY_MODEL", DEFAULT_SPAN_SCISPACY_MODEL),
        ontology_base_url=(os.environ.get("ONTOLOGY_LINKER_URL", DEFAULT_ONTOLOGY_LINKER_URL) or "").strip(),
        ontology_sources=tuple(_parse_csv_items(os.environ.get("ONTOLOGY_SOURCE_LIST"), default=["umls", "mesh"])),
        ontology_top_k=_env_int("ONTOLOGY_TOP_K", 2),
        ontology_threshold=_env_float("ONTOLOGY_THRESHOLD", 0.9),
        ontology_timeout=_env_float("ONTOLOGY_TIMEOUT", 20.0),
        paper_sources=tuple(_parse_csv_items(os.environ.get("PAPER_SOURCES"), default=list(paper_indexer.default_sources))),
        keyword_sources=tuple(_parse_csv_items(os.environ.get("KEYWORD_SOURCE"))),
        include_subphrases=os.environ.get("NO_SUBPHRASE_NGRAM", "0") != "1",
        evidence_threshold=SPAN_MATCH_DISPLAY_THRESHOLD,
    )
```

Update `run_span_matcher_test(...)`:

```python
profile = _build_span_matcher_profile(paper_indexer)
pipeline = SpanMatcherPipeline.from_profile(
    profile=profile,
    metadata_db=paper_indexer.metadata_db,
)
result = pipeline.run(normalized_query)
selected_candidates = [
    _serialize_selected_candidate(concept)
    for concept in result.selected_concepts
]
return {
    "success": True,
    "query": result.query,
    "normalized_query": result.normalized_query,
    "count": len(selected_candidates),
    "selected_candidates": selected_candidates,
    "semantic_plan": _serialize_semantic_plan(result.semantic_plan),
    "elapsed_ms": round(sum(result.timings_ms.values()), 3),
    "timings_ms": result.timings_ms,
}
```

- [ ] **Step 4: Run page tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/app/test_span_matcher_page.py tests/indexing/test_span_matcher_pipeline.py -q
```

Expected:

- page tests pass.
- pipeline tests pass.

- [ ] **Step 5: Commit page migration**

```bash
git add app/span_matcher_page.py tests/app/test_span_matcher_page.py
git commit -m "refactor: use span matcher pipeline in develop page"
```

---

### Task 6: Migrate PaperIndexer Online Retrieval To Ontology Plus Keyword

**Files:**
- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Test: `tests/indexing/test_expanded_sparse_retrieval.py`
- Test: `tests/indexing/test_paper_indexer.py`

- [ ] **Step 1: Write failing test for online retrieval default profile**

In `tests/indexing/test_expanded_sparse_retrieval.py`, update or add:

```python
def test_paper_indexer_builds_semantic_plan_with_ontology_plus_keyword_profile(monkeypatch):
    captured = {}

    class FakePipeline:
        def run(self, query):
            captured["query"] = query
            return SimpleNamespace(
                semantic_plan=SimpleNamespace(spans=[SimpleNamespace(span_id="s1")]),
                selected_concepts=[object()],
            )

    def fake_from_profile(*, profile, metadata_db):
        captured["profile_name"] = profile.name
        captured["enable_ontology"] = profile.enable_ontology
        captured["enable_keyword"] = profile.enable_keyword
        captured["paper_sources"] = profile.paper_sources
        return FakePipeline()

    monkeypatch.setattr(
        "src.docset_hub.indexing.paper_indexer.SpanMatcherPipeline.from_profile",
        fake_from_profile,
    )

    indexer = PaperIndexer.__new__(PaperIndexer)
    indexer.metadata_db = object()
    indexer.default_sources = ["langtaosha"]

    plan = indexer._build_query_semantic_plan(
        query="adhesion protein in kidney",
        source_list=["langtaosha"],
        keyword_sources=None,
    )

    assert plan is not None
    assert captured["query"] == "adhesion protein in kidney"
    assert captured["profile_name"] == "ontology_plus_keyword"
    assert captured["enable_ontology"] is True
    assert captured["enable_keyword"] is True
    assert captured["paper_sources"] == ("langtaosha",)
```

- [ ] **Step 2: Run the PaperIndexer test and verify it fails**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_expanded_sparse_retrieval.py::test_paper_indexer_builds_semantic_plan_with_ontology_plus_keyword_profile -q
```

Expected:

- failure because `_build_query_semantic_plan(...)` still assembles keyword-only matcher manually.

- [ ] **Step 3: Implement profile helper in `PaperIndexer`**

In `src/docset_hub/indexing/paper_indexer.py`, import:

```python
from .span_matcher_pipeline import SpanMatcherPipeline, SpanMatcherProfile
```

Add method:

```python
def _build_span_matcher_profile(
    self,
    *,
    source_list: Sequence[str],
    keyword_sources: Optional[Sequence[str]] = None,
    profile_name: str = "ontology_plus_keyword",
) -> SpanMatcherProfile:
    if profile_name == "keyword_only":
        factory = SpanMatcherProfile.keyword_only
    elif profile_name == "ontology_only":
        factory = SpanMatcherProfile.ontology_only
    else:
        factory = SpanMatcherProfile.ontology_plus_keyword
    return factory(
        paper_sources=tuple(source_list or self.default_sources),
        keyword_sources=tuple(keyword_sources or ()),
    )
```

Replace `_build_query_semantic_plan(...)` body with:

```python
def _build_query_semantic_plan(
    self,
    query: str,
    source_list: List[str],
    keyword_sources: Optional[Sequence[str]] = None,
    profile_name: str = "ontology_plus_keyword",
):
    profile = self._build_span_matcher_profile(
        source_list=source_list,
        keyword_sources=keyword_sources,
        profile_name=profile_name,
    )
    result = SpanMatcherPipeline.from_profile(
        profile=profile,
        metadata_db=self.metadata_db,
    ).run(query)
    if not result.selected_concepts:
        return None
    return result.semantic_plan
```

- [ ] **Step 4: Preserve explicit keyword-only helper paths**

If `_run_keyword_lookup_retrieval_branch(...)` must remain DB-only, call:

```python
plan = self._build_query_semantic_plan(
    query=query,
    source_list=source_list,
    keyword_sources=keyword_sources,
    profile_name="keyword_only",
)
```

For expanded sparse online retrieval, keep the default:

```python
plan = self._build_query_semantic_plan(
    query=query,
    source_list=source_list,
    keyword_sources=keyword_sources,
)
```

This means expanded sparse online retrieval now uses `ontology_plus_keyword`.

- [ ] **Step 5: Run affected PaperIndexer tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/indexing/test_expanded_sparse_retrieval.py tests/indexing/test_paper_indexer.py -q
```

Expected:

- tests pass.
- assertions confirm online semantic-plan construction uses `ontology_plus_keyword`.

- [ ] **Step 6: Commit PaperIndexer migration**

```bash
git add src/docset_hub/indexing/paper_indexer.py tests/indexing/test_expanded_sparse_retrieval.py tests/indexing/test_paper_indexer.py
git commit -m "refactor: use ontology profile for paper indexer span plans"
```

---

### Task 7: Migrate Expanded Compare To Shared Online Profile

**Files:**
- Modify: `app/expanded_compare_page.py`
- Test: `tests/app/test_expanded_compare_page.py`

- [ ] **Step 1: Write failing expanded compare profile test**

Add to `tests/app/test_expanded_compare_page.py`:

```python
def test_expanded_compare_uses_paper_indexer_online_span_profile(monkeypatch):
    captured = {}

    class FakeIndexer:
        default_sources = ["langtaosha"]
        metadata_db = object()

        def search(self, **kwargs):
            return []

        def _build_query_semantic_plan(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                original_query=kwargs["query"],
                normalized_query=kwargs["query"],
                spans=[],
            )

    monkeypatch.setattr("app.expanded_compare_page.build_expanded_sparse_query_rows", lambda plan: [])
    monkeypatch.setattr("app.expanded_compare_page.match_papers_by_expanded_sparse_plan", lambda **kwargs: [])

    app = Flask(__name__)
    register_expanded_compare_api_routes(app, FakeIndexer(), _json_success(app), _json_error(app))

    response = app.test_client().get("/api/expanded-compare?query=adhesion%20protein%20in%20kidney")

    assert response.status_code == 200
    assert captured["query"] == "adhesion protein in kidney"
    assert captured.get("profile_name", "ontology_plus_keyword") == "ontology_plus_keyword"
```

- [ ] **Step 2: Run expanded compare tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/app/test_expanded_compare_page.py -q
```

Expected:

- pass if expanded compare already delegates to `PaperIndexer._build_query_semantic_plan(...)`.
- fail if the app constructs its own profile or calls keyword-only helpers.

- [ ] **Step 3: Make the profile explicit at the call site**

In `app/expanded_compare_page.py`, call:

```python
plan = indexer._build_query_semantic_plan(
    query=query,
    source_list=source_list or list(getattr(indexer, "default_sources", []) or []),
    keyword_sources=keyword_sources,
    profile_name="ontology_plus_keyword",
)
```

- [ ] **Step 4: Run expanded compare tests**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/app/test_expanded_compare_page.py tests/indexing/test_expanded_sparse_retrieval.py -q
```

Expected:

- expanded compare tests pass.
- PaperIndexer profile tests pass.

- [ ] **Step 5: Commit expanded compare migration**

```bash
git add app/expanded_compare_page.py tests/app/test_expanded_compare_page.py
git commit -m "refactor: use ontology span profile in expanded compare"
```

---

### Task 8: End-To-End Regression For `adhesion protein in kidney`

**Files:**
- Test: `tests/integration/test_span_matcher_pipeline_real_services.py`
- Modify only if needed: `src/docset_hub/indexing/span_matcher_pipeline.py`

- [ ] **Step 1: Write opt-in real-service regression test**

Create `tests/integration/test_span_matcher_pipeline_real_services.py`:

```python
from pathlib import Path
import os

import pytest

from src.config import _reset_config, init_config
from src.docset_hub.indexing import SpanMatcherPipeline, SpanMatcherProfile
from src.docset_hub.storage.metadata_db import MetadataDB


MIMIC_CONFIG_PATH = Path("src/config/config_tecent_backend_server_mimic.yaml")


@pytest.mark.integration
def test_ontology_plus_keyword_pipeline_selects_adhesion_protein_and_kidney():
    if os.environ.get("RUN_REAL_SPAN_MATCHER_PIPELINE_INTEGRATION") != "1":
        pytest.skip("set RUN_REAL_SPAN_MATCHER_PIPELINE_INTEGRATION=1 to run live span matcher pipeline checks")

    _reset_config()
    init_config(MIMIC_CONFIG_PATH, force_reload=True)
    metadata_db = MetadataDB(config_path=MIMIC_CONFIG_PATH)
    pipeline = SpanMatcherPipeline.from_profile(
        profile=SpanMatcherProfile.ontology_plus_keyword(
            enable_scispacy=False,
            paper_sources=("langtaosha", "biorxiv_history", "biorxiv_daily"),
        ),
        metadata_db=metadata_db,
    )

    result = pipeline.run("adhesion protein in kidney")

    surfaces = [concept.candidate.text for concept in result.selected_concepts]
    assert "kidney" in surfaces
    assert any(surface in {"adhesion protein", "adhesion"} for surface in surfaces)
```

- [ ] **Step 2: Run default integration collection**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/integration/test_span_matcher_pipeline_real_services.py -q
```

Expected:

- skipped unless `RUN_REAL_SPAN_MATCHER_PIPELINE_INTEGRATION=1`.

- [ ] **Step 3: Run the live check when ontology linker is available**

Run:

```bash
RUN_REAL_SPAN_MATCHER_PIPELINE_INTEGRATION=1 /home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest tests/integration/test_span_matcher_pipeline_real_services.py -q
```

Expected:

- passes when MetadataDB and ontology linker are reachable.
- if ontology linker is unavailable, fail with the concrete connection error; do not silently downgrade to keyword-only.

- [ ] **Step 4: Commit the regression test**

```bash
git add tests/integration/test_span_matcher_pipeline_real_services.py
git commit -m "test: cover ontology keyword span matcher profile"
```

---

### Task 9: Remove Duplicate Pipeline Assembly And Update Docs

**Files:**
- Modify: `scripts/run_span_matcher_trace.py`
- Modify: `app/span_matcher_page.py`
- Modify: `src/docset_hub/indexing/paper_indexer.py`
- Modify: `docs/implementation_log/20260610/SPAN_MATCHER_PIPELINE_PROFILE_IMPLEMENTATION_PLAN_20260610.md`

- [ ] **Step 1: Search for leftover manual pipeline assembly**

Run:

```bash
grep -RIn "QueryPhraseAnalyzer\\|SpanMatcherExecutor\\|MaximalConceptSelector\\|RemoteOntologySpanMatcher\\|KeywordSurfaceSpanMatcher" scripts app src/docset_hub/indexing | grep -v span_matcher_pipeline.py
```

Expected:

- remaining references are adapter definitions, tests, or code paths that intentionally bypass the end-to-end pipeline.

- [ ] **Step 2: Delete obsolete local context helpers**

Remove local helpers that only existed to assemble the pipeline:

- `app/span_matcher_page.py`:
  - `_SPAN_MATCHER_CONTEXTS`
  - `_load_span_scispacy_pipeline`
  - `_get_span_matcher_context`
- `scripts/run_span_matcher_trace.py`:
  - `build_analyzer`
  - `build_keyword_matcher`
  - `build_final_results`
  - any trace helper moved into `span_matcher_pipeline.py`

Keep serializers and report formatting helpers.

- [ ] **Step 3: Run full affected test set**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m pytest \
  tests/indexing/test_span_matcher_pipeline.py \
  tests/scripts/test_run_span_matcher_trace.py \
  tests/app/test_span_matcher_page.py \
  tests/app/test_expanded_compare_page.py \
  tests/indexing/test_expanded_sparse_retrieval.py \
  tests/indexing/test_paper_indexer.py \
  -q
```

Expected:

- all selected tests pass.

- [ ] **Step 4: Compile affected Python files**

Run:

```bash
/home/wnlab/miniconda3/envs/langtaosha-pd-test/bin/python -m py_compile \
  src/docset_hub/indexing/span_matcher_pipeline.py \
  scripts/run_span_matcher_trace.py \
  app/span_matcher_page.py \
  app/expanded_compare_page.py \
  src/docset_hub/indexing/paper_indexer.py
```

Expected:

- no compile errors.

- [ ] **Step 5: Mark implementation plan tasks completed during execution**

As each task is completed by the executing agent, update this file's checkboxes from `[ ]` to `[x]`. Do not mark a task complete before its verification command has passed.

- [ ] **Step 6: Commit cleanup and docs**

```bash
git add scripts/run_span_matcher_trace.py app/span_matcher_page.py app/expanded_compare_page.py src/docset_hub/indexing/paper_indexer.py docs/implementation_log/20260610/SPAN_MATCHER_PIPELINE_PROFILE_IMPLEMENTATION_PLAN_20260610.md
git commit -m "docs: record span matcher pipeline profile rollout"
```

---

## Final Verification Checklist

- [ ] `SpanMatcherProfile.ontology_plus_keyword()` exists and enables both ontology and keyword evidence.
- [ ] `PaperIndexer._build_query_semantic_plan(...)` defaults to `ontology_plus_keyword`.
- [ ] DB-only behavior is still available through explicit `profile_name="keyword_only"` or `SpanMatcherProfile.keyword_only(...)`.
- [ ] `run_span_matcher_trace.py` delegates orchestration to `SpanMatcherPipeline`.
- [ ] `app/span_matcher_page.py` delegates orchestration to `SpanMatcherPipeline`.
- [ ] `app/expanded_compare_page.py` uses the same online profile as `PaperIndexer`.
- [ ] A test locks the `adhesion protein in kidney` profile behavior.
- [ ] No caller manually repeats normalize/extract/match/select/build-plan unless it is a focused unit test.
