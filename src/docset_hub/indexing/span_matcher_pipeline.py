"""End-to-end span matcher pipeline profiles and run results."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .entity_filter_policy import filter_ontology_evidence_items
from .query_phrase_analyzer import MetadataDBPhraseLexicon, PhraseCandidate, QueryPhraseAnalyzer
from .query_semantic_plan import QuerySemanticPlan, build_query_semantic_plan
from .span_matcher import (
    CompositeSpanMatcher,
    ConceptMatchEvidence,
    KeywordSurfaceSpanMatcher,
    MaximalConceptSelector,
    RemoteOntologySpanMatcher,
    SelectedConcept,
    SpanMatcherExecutor,
    SpanMatchResult,
)


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

    def run(self, query: str, *, trace: bool = False) -> SpanMatcherRunResult:
        normalized_query = (query or "").strip()
        if not normalized_query:
            raise ValueError("query 不能为空")

        timings_ms: Dict[str, float] = {}

        normalize_started = time.perf_counter()
        normalized = self.analyzer.normalizer.normalize_query(normalized_query)
        timings_ms["normalize"] = round((time.perf_counter() - normalize_started) * 1000.0, 3)

        scispacy_doc = None
        if (
            self.profile.enable_scispacy
            and getattr(self.analyzer, "scispacy_pipeline", None) is not None
            and normalized.normalized_query
        ):
            scispacy_doc = self.analyzer.scispacy_pipeline(normalized.normalized_query)

        extract_started = time.perf_counter()
        extractor_candidates = self.analyzer.extractor.extract(
            normalized.normalized_query,
            scispacy_doc=scispacy_doc,
        )
        timings_ms["extract"] = round((time.perf_counter() - extract_started) * 1000.0, 3)

        expanded_candidates = self.executor.expand_candidates(extractor_candidates)

        match_started = time.perf_counter()
        span_results, trace_payload = self._match_candidates(
            extractor_candidates=extractor_candidates,
            expanded_candidates=expanded_candidates,
            collect_trace=trace,
        )
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
            trace=trace_payload if trace else None,
        )

    def _match_candidates(
        self,
        *,
        extractor_candidates: Sequence[PhraseCandidate],
        expanded_candidates: Sequence[PhraseCandidate],
        collect_trace: bool,
    ) -> tuple[List[SpanMatchResult], Optional[SpanMatcherTrace]]:
        if not collect_trace:
            return self.executor.match_candidates(extractor_candidates), None

        matcher = getattr(self.executor, "matcher", None)
        if not isinstance(matcher, CompositeSpanMatcher):
            return self.executor.match_candidates(extractor_candidates), SpanMatcherTrace()

        raw_ontology_items: Dict[str, List[Mapping[str, Any]]] = {}
        filtered_ontology_evidence: Dict[str, List[ConceptMatchEvidence]] = {}
        keyword_evidence: Dict[str, List[ConceptMatchEvidence]] = {}
        evidence_by_label: Dict[str, List[ConceptMatchEvidence]] = {
            _candidate_label(candidate): []
            for candidate in expanded_candidates
        }

        for child_matcher in matcher.matchers:
            if isinstance(child_matcher, RemoteOntologySpanMatcher):
                raw_items, filtered_items = self._collect_ontology_trace(child_matcher, expanded_candidates)
                raw_ontology_items.update(raw_items)
                filtered_ontology_evidence.update(filtered_items)
                for label, evidence_items in filtered_items.items():
                    evidence_by_label.setdefault(label, []).extend(evidence_items)
                continue
            if isinstance(child_matcher, KeywordSurfaceSpanMatcher):
                keyword_items = child_matcher.match_many(expanded_candidates)
                for candidate, evidence_items in zip(expanded_candidates, keyword_items):
                    label = _candidate_label(candidate)
                    keyword_evidence[label] = list(evidence_items)
                    evidence_by_label.setdefault(label, []).extend(evidence_items)
                continue

            matcher_items = child_matcher.match_many(expanded_candidates)
            for candidate, evidence_items in zip(expanded_candidates, matcher_items):
                label = _candidate_label(candidate)
                evidence_by_label.setdefault(label, []).extend(evidence_items)

        span_results: List[SpanMatchResult] = []
        for candidate in expanded_candidates:
            bucket = list(evidence_by_label.get(_candidate_label(candidate), []))
            bucket.sort(key=matcher._sort_key)
            span_results.append(SpanMatchResult(candidate=candidate, evidence=bucket))

        return span_results, SpanMatcherTrace(
            raw_ontology_items=raw_ontology_items,
            filtered_ontology_evidence=filtered_ontology_evidence,
            keyword_evidence=keyword_evidence,
        )

    @staticmethod
    def _collect_ontology_trace(
        matcher: RemoteOntologySpanMatcher,
        candidates: Sequence[PhraseCandidate],
    ) -> tuple[Dict[str, List[Mapping[str, Any]]], Dict[str, List[ConceptMatchEvidence]]]:
        candidate_ids = [f"c{index}" for index in range(len(candidates))]
        payload = {
            "sources": list(matcher.sources),
            "top_k": matcher.top_k,
            "threshold": matcher.threshold,
            "candidates": [
                {
                    "id": candidate_id,
                    "text": candidate.text,
                    "normalized_text": candidate.normalized_text,
                    "kind": candidate.kind,
                    "start": candidate.start,
                    "end": candidate.end,
                }
                for candidate_id, candidate in zip(candidate_ids, candidates)
            ],
        }
        response_payload = matcher._post(payload)
        raw_results = matcher._results_by_candidate_id(response_payload)

        raw_by_surface: Dict[str, List[Mapping[str, Any]]] = {}
        filtered_by_surface: Dict[str, List[ConceptMatchEvidence]] = {}
        for candidate_id, candidate in zip(candidate_ids, candidates):
            raw_items = raw_results.get(candidate_id, [])
            label = _candidate_label(candidate)
            raw_by_surface[label] = list(raw_items)
            filtered_items = filter_ontology_evidence_items(raw_items)
            filtered_by_surface[label] = [
                matcher._to_evidence(candidate, item)
                for item in filtered_items
            ]
        return raw_by_surface, filtered_by_surface

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


def _candidate_label(candidate: PhraseCandidate) -> str:
    return (
        f"{candidate.normalized_text} "
        f"(kind={candidate.kind}, span={candidate.start}:{candidate.end})"
    )
