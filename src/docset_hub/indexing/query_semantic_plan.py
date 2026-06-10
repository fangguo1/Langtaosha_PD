"""Query semantic plan built from span matcher output."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Sequence

from .query_phrase_analyzer import QueryPhraseNormalizer
from .span_matcher import ConceptMatchEvidence, SelectedConcept, SpanMatchResult


ONTOLOGY_SOURCES = {"umls", "mesh"}


@dataclass(frozen=True)
class SemanticTerm:
    text: str
    match_mode: str = "exact"


@dataclass
class SemanticTermBucket:
    tier1: List[SemanticTerm] = field(default_factory=list)
    tier2: List[SemanticTerm] = field(default_factory=list)


@dataclass
class SemanticChildSpan:
    span_id: str
    surface_text: str
    normalized_text: str
    start: int
    end: int
    canonical_text: str
    own_terms: SemanticTermBucket = field(default_factory=SemanticTermBucket)
    evidence: List[ConceptMatchEvidence] = field(default_factory=list)


@dataclass
class SemanticSpanGroup:
    span_id: str
    surface_text: str
    normalized_text: str
    start: int
    end: int
    canonical_text: str
    own_terms: SemanticTermBucket = field(default_factory=SemanticTermBucket)
    children: List[SemanticChildSpan] = field(default_factory=list)
    evidence: List[ConceptMatchEvidence] = field(default_factory=list)


@dataclass
class QuerySemanticPlan:
    original_query: str
    normalized_query: str
    spans: List[SemanticSpanGroup] = field(default_factory=list)


def build_query_semantic_plan(
    *,
    original_query: str,
    normalized_query: str,
    selected_concepts: Sequence[SelectedConcept],
    span_results: Sequence[SpanMatchResult] | None = None,
) -> QuerySemanticPlan:
    """Build a stable semantic plan from final selected span concepts."""

    normalizer = QueryPhraseNormalizer()
    spans: List[SemanticSpanGroup] = []
    for index, concept in enumerate(selected_concepts, start=1):
        if not concept.evidence:
            continue
        primary = concept.primary_evidence
        own_terms = SemanticTermBucket(
            tier1=_build_exact_terms(
                [concept.candidate.text, concept.candidate.normalized_text, primary.canonical],
                normalizer,
            ),
            tier2=_ontology_tier2_terms(concept.evidence, normalizer),
        )
        children = _build_child_spans(
            parent_id=f"s{index}",
            parent_concept=concept,
            span_results=span_results or [],
            normalizer=normalizer,
        )
        spans.append(
            SemanticSpanGroup(
                span_id=f"s{index}",
                surface_text=concept.candidate.text,
                normalized_text=concept.candidate.normalized_text,
                start=concept.candidate.start,
                end=concept.candidate.end,
                canonical_text=primary.canonical,
                own_terms=own_terms,
                children=children,
                evidence=list(concept.evidence),
            )
        )
    return QuerySemanticPlan(
        original_query=original_query,
        normalized_query=normalized_query,
        spans=spans,
    )


def _build_child_spans(
    *,
    parent_id: str,
    parent_concept: SelectedConcept,
    span_results: Sequence[SpanMatchResult],
    normalizer: QueryPhraseNormalizer,
) -> List[SemanticChildSpan]:
    children: List[SemanticChildSpan] = []
    child_index = 0
    parent_candidate = parent_concept.candidate
    for result in span_results:
        candidate = result.candidate
        if candidate.kind != "subphrase_ngram":
            continue
        if not result.evidence:
            continue
        if candidate.start < parent_candidate.start or candidate.end > parent_candidate.end:
            continue
        if candidate.start == parent_candidate.start and candidate.end == parent_candidate.end:
            continue
        child_index += 1
        primary = result.primary_evidence
        if primary is None:
            continue
        children.append(
            SemanticChildSpan(
                span_id=f"{parent_id}.{child_index}",
                surface_text=candidate.text,
                normalized_text=candidate.normalized_text,
                start=candidate.start,
                end=candidate.end,
                canonical_text=primary.canonical,
                own_terms=SemanticTermBucket(
                    tier1=_build_exact_terms(
                        [candidate.text, candidate.normalized_text, primary.canonical],
                        normalizer,
                    ),
                    tier2=_ontology_tier2_terms(result.evidence, normalizer),
                ),
                evidence=list(result.evidence),
            )
        )
    return children


def _ontology_tier2_terms(
    evidence_items: Sequence[ConceptMatchEvidence],
    normalizer: QueryPhraseNormalizer,
) -> List[SemanticTerm]:
    terms: List[SemanticTerm] = []
    seen = set()
    for index, evidence in enumerate(evidence_items):
        if evidence.source not in ONTOLOGY_SOURCES:
            continue
        raw_terms: List[str] = []
        if index > 0:
            raw_terms.append(evidence.canonical)
        raw_terms.extend(evidence.aliases)
        for term in _build_alias_terms(raw_terms, normalizer):
            key = (term.text, term.match_mode)
            if key in seen:
                continue
            seen.add(key)
            terms.append(term)
    return sorted(terms, key=lambda item: (item.text, item.match_mode))


def _build_exact_terms(
    terms: Iterable[str],
    normalizer: QueryPhraseNormalizer,
) -> List[SemanticTerm]:
    normalized_terms = _unique_normalized_terms(terms, normalizer)
    return [SemanticTerm(text=term, match_mode="exact") for term in normalized_terms]


def _build_alias_terms(
    terms: Iterable[str],
    normalizer: QueryPhraseNormalizer,
) -> List[SemanticTerm]:
    built: List[SemanticTerm] = []
    for term in terms:
        raw_term = str(term or "").strip()
        if not raw_term:
            continue
        match_mode = "prefix" if raw_term.endswith("-") else "exact"
        normalized = normalizer.normalize_phrase(raw_term)
        if match_mode == "prefix":
            normalized = normalized[:-1].strip() if normalized.endswith("-") else normalized
        if not normalized:
            continue
        built.append(SemanticTerm(text=normalized, match_mode=match_mode))
    return built


def _unique_normalized_terms(
    terms: Iterable[str],
    normalizer: QueryPhraseNormalizer,
) -> List[str]:
    seen = set()
    normalized_terms: List[str] = []
    for term in terms:
        normalized = normalizer.normalize_phrase(term)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_terms.append(normalized)
    return sorted(normalized_terms)
