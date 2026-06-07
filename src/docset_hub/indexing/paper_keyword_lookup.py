"""Keyword lookup recall from span-matcher query concepts.

This module starts after span matching has already run. The legacy entry point
still consumes only ``SelectedConcept`` objects. The enhanced lookup-plan path
also consumes selector-side support spans, such as sub concepts, while keeping
their route and weight explicit.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from .query_phrase_analyzer import GENERIC_HEADS, QueryPhraseNormalizer
from .span_matcher import ConceptMatchEvidence, SelectedConcept, SpanMatchResult

if TYPE_CHECKING:
    from ..storage.metadata_db import MetadataDB


ITEM_WEIGHTS = {
    "selected_concept": 1.00,
    "sub_concept": 0.55,
    "broad_sub_concept": 0.30,
    "substring_candidate": 1.00,
}

ROUTE_WEIGHTS = {
    "selected_exact": 1.00,
    "selected_surface_variant": 0.90,
    "sub_concept_exact": 1.00,
    "sub_concept_surface_variant": 0.90,
    "db_substring_candidate": 0.50,
    "db_prefix_candidate": 0.50,
    "db_trigram_candidate": 0.50,
    "db_compositional_candidate": 0.70,
}

TERM_ROLE_WEIGHTS = {
    "primary_canonical": 1.00,
    "canonical": 0.95,
    "candidate_text": 0.90,
    "normalized_text": 0.85,
    "alias": 0.80,
    "db_candidate": 1.00,
}

SUPPORT_CONFIDENCE_THRESHOLD = 0.75
GROUP_SUPPORT_CAP = 0.85
SURFACE_VARIANT_MATCH_TYPES = {
    "hyphen_space_variant",
    "keyword_normalized",
    "plural_variant",
}
TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9+_-]*")
BROAD_SUB_CONCEPT_TERMS = GENERIC_HEADS | {
    "cell",
    "cells",
    "gene",
    "genes",
    "protein",
    "proteins",
    "t-cell",
    "t-cells",
    "lymphocyte",
    "lymphocytes",
}


@dataclass(frozen=True)
class KeywordLookupItem:
    """A query-side evidence item that may produce lookup terms."""

    group_id: int
    item_id: str
    concept_idx: int
    concept_id: str
    concept_label: str
    text: str
    normalized_text: str
    role: str
    item_weight: float
    evidence_confidence: float
    evidence_source: str
    parent_item_id: Optional[str] = None
    parent_text: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KeywordLookupTerm:
    """A normalized keyword lookup term for one selected query concept."""

    concept_idx: int
    concept_id: str
    concept_label: str
    term: str
    term_role: str
    term_weight: float
    group_id: Optional[int] = None
    item_id: Optional[str] = None
    item_role: str = "selected_concept"
    item_weight: float = 1.0
    route: str = "selected_exact"
    route_weight: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        if payload["group_id"] is None:
            payload["group_id"] = self.concept_idx
        if payload["item_id"] is None:
            payload["item_id"] = f"g{payload['group_id']}:selected:{self.concept_idx}"
        return payload


@dataclass(frozen=True)
class PaperKeywordLookupResult:
    """A paper recalled by keyword lookup against selected query concepts."""

    paper_id: int
    work_id: str
    matched_concept_count: int
    total_concept_count: int
    keyword_lookup_score: float
    matched_concepts: List[Dict[str, Any]]
    recall_sources: List[str]
    retrieval_debug: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_keyword_lookup_terms_from_selected_concepts(
    selected_concepts: Sequence[SelectedConcept],
    normalizer: Optional[QueryPhraseNormalizer] = None,
    max_terms_per_concept: int = 16,
) -> List[KeywordLookupTerm]:
    """Expand ``MaximalConceptSelector`` output into weighted lookup terms.

    Scoring rule here is term-level only: primary canonical=1.0, other
    canonical=0.95, alias=0.80, candidate text=0.90, normalized text=0.85.
    Final paper scoring/ranking is handled by
    ``MetadataDB.lookup_papers_by_keyword_terms``.
    """

    return build_keyword_lookup_plan(
        selected_concepts=selected_concepts,
        span_results=None,
        normalizer=normalizer,
        include_sub_concepts=False,
        include_substring_candidates=False,
        max_terms_per_item=max_terms_per_concept,
    )


def build_keyword_lookup_plan(
    selected_concepts: Sequence[SelectedConcept],
    span_results: Optional[Sequence[SpanMatchResult]] = None,
    normalizer: Optional[QueryPhraseNormalizer] = None,
    include_sub_concepts: bool = True,
    include_substring_candidates: bool = False,
    max_terms_per_item: int = 16,
    support_confidence_threshold: float = SUPPORT_CONFIDENCE_THRESHOLD,
) -> List[KeywordLookupTerm]:
    """Build grouped lookup terms from selected concepts and support spans.

    ``selected_concepts`` remain the primary evidence. If ``span_results`` are
    supplied, contained/overlapping support spans are added to the same group
    with lower ``item_weight`` so they can recover recall without outranking a
    full selected-concept hit.
    """

    if not selected_concepts:
        return []
    _validate_selected_concepts(selected_concepts)

    phrase_normalizer = normalizer or QueryPhraseNormalizer()
    terms: List[KeywordLookupTerm] = []
    max_terms = max(1, int(max_terms_per_item))
    selected_records = []
    selected_keys = set()

    for concept_idx, concept in enumerate(selected_concepts, start=1):
        primary = concept.primary_evidence
        concept_label = primary.canonical or concept.candidate.text
        concept_id = _concept_id_for(primary, concept_idx, phrase_normalizer)
        item = KeywordLookupItem(
            group_id=concept_idx,
            item_id=f"g{concept_idx}:selected:{concept_idx}",
            concept_idx=concept_idx,
            concept_id=concept_id,
            concept_label=concept_label,
            text=concept.candidate.text,
            normalized_text=concept.candidate.normalized_text,
            role="selected_concept",
            item_weight=ITEM_WEIGHTS["selected_concept"],
            evidence_confidence=float(primary.confidence),
            evidence_source=primary.source,
        )
        selected_records.append((concept, item))
        selected_keys.add(_candidate_key(concept.candidate.start, concept.candidate.end, concept.candidate.normalized_text))
        terms.extend(
            _terms_for_item(
                item=item,
                candidate_text=concept.candidate.text,
                normalized_text=concept.candidate.normalized_text,
                evidence_items=concept.evidence,
                normalizer=phrase_normalizer,
                max_terms=max_terms,
            )
        )

    if include_sub_concepts and span_results:
        for result in span_results:
            if not result.evidence:
                continue
            selected_item = _support_parent_item(result, selected_records, selected_keys)
            if selected_item is None:
                continue

            support_evidence = [
                evidence
                for evidence in result.evidence
                if float(evidence.confidence) >= float(support_confidence_threshold)
            ]
            if not support_evidence:
                continue

            normalized_text = phrase_normalizer.normalize_phrase(result.candidate.normalized_text)
            item_role = "broad_sub_concept" if _is_broad_sub_concept(normalized_text) else "sub_concept"
            item = KeywordLookupItem(
                group_id=selected_item.group_id,
                item_id=(
                    f"g{selected_item.group_id}:sub:"
                    f"{result.candidate.start}:{result.candidate.end}:{normalized_text}"
                ),
                concept_idx=selected_item.concept_idx,
                concept_id=_concept_id_for(support_evidence[0], selected_item.concept_idx, phrase_normalizer),
                concept_label=support_evidence[0].canonical or result.candidate.text,
                text=result.candidate.text,
                normalized_text=normalized_text,
                role=item_role,
                item_weight=ITEM_WEIGHTS[item_role],
                evidence_confidence=float(support_evidence[0].confidence),
                evidence_source=support_evidence[0].source,
                parent_item_id=selected_item.item_id,
                parent_text=selected_item.text,
            )
            terms.extend(
                _terms_for_item(
                    item=item,
                    candidate_text=result.candidate.text,
                    normalized_text=result.candidate.normalized_text,
                    evidence_items=support_evidence,
                    normalizer=phrase_normalizer,
                    max_terms=max_terms,
                )
            )

    return _dedupe_lookup_terms(terms)


def match_paper_keywords_using_span_matcher(
    metadata_db: MetadataDB,
    selected_concepts: Sequence[SelectedConcept],
    source_list: Optional[Sequence[str]] = None,
    keyword_sources: Optional[Sequence[str]] = None,
    top_k: int = 50,
    strict_all_concepts: bool = False,
    max_terms_per_concept: int = 16,
) -> List[PaperKeywordLookupResult]:
    """Recall papers whose ``paper_keywords`` match span-selected concepts.

    ``selected_concepts`` must be the final ``SelectedConcept[]`` returned by
    ``MaximalConceptSelector.select(...)``. Callers should run
    ``QueryPhraseAnalyzer`` + ``SpanMatcherExecutor`` + ``MaximalConceptSelector``
    before invoking this function.

    Ranking rule is delegated to ``MetadataDB.lookup_papers_by_keyword_terms``:
    this function only builds weighted concept terms, passes filters/top_k, and
    wraps returned rows as ``PaperKeywordLookupResult``.
    """

    query_terms = build_keyword_lookup_terms_from_selected_concepts(
        selected_concepts=selected_concepts,
        max_terms_per_concept=max_terms_per_concept,
    )
    if not query_terms:
        return []

    rows = metadata_db.lookup_papers_by_keyword_terms(
        query_terms=[term.to_dict() for term in query_terms],
        source_list=source_list,
        keyword_sources=keyword_sources,
        top_k=top_k,
        strict_all_concepts=strict_all_concepts,
    )
    return [_result_from_row(row) for row in rows]


def match_paper_keywords_with_lookup_plan(
    metadata_db: MetadataDB,
    selected_concepts: Sequence[SelectedConcept],
    span_results: Optional[Sequence[SpanMatchResult]] = None,
    source_list: Optional[Sequence[str]] = None,
    keyword_sources: Optional[Sequence[str]] = None,
    top_k: int = 50,
    strict_all_groups: bool = False,
    include_sub_concepts: bool = True,
    include_substring_candidates: bool = True,
    max_terms_per_item: int = 16,
    max_candidate_terms_per_item: int = 5,
) -> List[PaperKeywordLookupResult]:
    """Recall papers with selected concepts, sub concepts, and candidate terms."""

    query_terms = build_keyword_lookup_plan(
        selected_concepts=selected_concepts,
        span_results=span_results,
        include_sub_concepts=include_sub_concepts,
        include_substring_candidates=False,
        max_terms_per_item=max_terms_per_item,
    )
    if include_substring_candidates:
        query_terms = _add_db_candidate_terms(
            metadata_db=metadata_db,
            query_terms=query_terms,
            keyword_sources=keyword_sources,
            max_candidate_terms_per_item=max_candidate_terms_per_item,
        )
    if not query_terms:
        return []

    rows = metadata_db.lookup_papers_by_keyword_lookup_terms(
        query_terms=[term.to_dict() for term in query_terms],
        source_list=source_list,
        keyword_sources=keyword_sources,
        top_k=top_k,
        strict_all_groups=strict_all_groups,
        group_support_cap=GROUP_SUPPORT_CAP,
    )
    return [_result_from_row(row) for row in rows]


def _validate_selected_concepts(selected_concepts: Sequence[SelectedConcept]) -> None:
    for index, concept in enumerate(selected_concepts, start=1):
        if not isinstance(concept, SelectedConcept):
            raise TypeError(
                "selected_concepts must contain SelectedConcept objects from "
                f"MaximalConceptSelector.select(); item {index} is {type(concept)!r}"
            )
        if not concept.evidence:
            raise ValueError(f"SelectedConcept item {index} has no evidence")


def _concept_id_for(
    evidence: ConceptMatchEvidence,
    concept_idx: int,
    normalizer: QueryPhraseNormalizer,
) -> str:
    if evidence.concept_id:
        return evidence.concept_id
    canonical = normalizer.normalize_phrase(evidence.canonical)
    if canonical:
        return f"keyword:{canonical}"
    return f"concept:{concept_idx}"


def _terms_for_item(
    item: KeywordLookupItem,
    candidate_text: str,
    normalized_text: str,
    evidence_items: Sequence[ConceptMatchEvidence],
    normalizer: QueryPhraseNormalizer,
    max_terms: int,
) -> List[KeywordLookupTerm]:
    if not evidence_items:
        return []

    primary = evidence_items[0]
    term_candidates: List[Tuple[str, str, float, ConceptMatchEvidence]] = [
        (primary.canonical, "primary_canonical", TERM_ROLE_WEIGHTS["primary_canonical"], primary),
    ]
    for evidence in evidence_items[1:]:
        term_candidates.append((evidence.canonical, "canonical", TERM_ROLE_WEIGHTS["canonical"], evidence))
    for evidence in evidence_items:
        for alias in evidence.aliases:
            term_candidates.append((alias, "alias", TERM_ROLE_WEIGHTS["alias"], evidence))
    term_candidates.append((candidate_text, "candidate_text", TERM_ROLE_WEIGHTS["candidate_text"], primary))
    term_candidates.append((normalized_text, "normalized_text", TERM_ROLE_WEIGHTS["normalized_text"], primary))

    best_terms: Dict[str, KeywordLookupTerm] = {}
    for raw_term, role, term_weight, evidence in term_candidates:
        normalized_term = normalizer.normalize_phrase(raw_term)
        if not normalized_term:
            continue
        route = _classify_route(item.role, role, evidence)
        candidate = KeywordLookupTerm(
            concept_idx=item.concept_idx,
            concept_id=item.concept_id,
            concept_label=item.concept_label,
            term=normalized_term,
            term_role=role,
            term_weight=float(term_weight),
            group_id=item.group_id,
            item_id=item.item_id,
            item_role=item.role,
            item_weight=float(item.item_weight),
            route=route,
            route_weight=ROUTE_WEIGHTS[route],
        )
        existing = best_terms.get(normalized_term)
        if existing is None or _term_sort_key(candidate) < _term_sort_key(existing):
            best_terms[normalized_term] = candidate

    return sorted(best_terms.values(), key=_term_sort_key)[:max_terms]


def _classify_route(item_role: str, term_role: str, evidence: ConceptMatchEvidence) -> str:
    if item_role in {"sub_concept", "broad_sub_concept"}:
        if term_role in {"primary_canonical", "canonical"} and _is_surface_variant_evidence(evidence):
            return "sub_concept_surface_variant"
        return "sub_concept_exact"
    if term_role in {"primary_canonical", "canonical"} and _is_surface_variant_evidence(evidence):
        return "selected_surface_variant"
    return "selected_exact"


def _is_surface_variant_evidence(evidence: ConceptMatchEvidence) -> bool:
    match_type = str(evidence.match_type or "").lower()
    surface_match_type = str((evidence.payload or {}).get("surface_match_type") or "").lower()
    return match_type in SURFACE_VARIANT_MATCH_TYPES or surface_match_type in SURFACE_VARIANT_MATCH_TYPES


def _support_parent_item(
    result: SpanMatchResult,
    selected_records: Sequence[Tuple[SelectedConcept, KeywordLookupItem]],
    selected_keys: Set[Tuple[int, int, str]],
) -> Optional[KeywordLookupItem]:
    key = _candidate_key(result.candidate.start, result.candidate.end, result.candidate.normalized_text)
    if key in selected_keys:
        return None

    span = (result.candidate.start, result.candidate.end)
    for concept, item in selected_records:
        selected_span = (concept.candidate.start, concept.candidate.end)
        if _contains_span(selected_span, span) or _overlaps_span(selected_span, span):
            return item
    return None


def _candidate_key(start: int, end: int, normalized_text: str) -> Tuple[int, int, str]:
    return (int(start), int(end), str(normalized_text or ""))


def _contains_span(parent: Tuple[int, int], child: Tuple[int, int]) -> bool:
    return parent[0] <= child[0] and child[1] <= parent[1]


def _overlaps_span(left: Tuple[int, int], right: Tuple[int, int]) -> bool:
    return left[0] < right[1] and right[0] < left[1]


def _is_broad_sub_concept(normalized_text: str) -> bool:
    tokens = normalized_text.split()
    return len(tokens) == 1 and tokens[0] in BROAD_SUB_CONCEPT_TERMS


def _add_db_candidate_terms(
    metadata_db: MetadataDB,
    query_terms: Sequence[KeywordLookupTerm],
    keyword_sources: Optional[Sequence[str]],
    max_candidate_terms_per_item: int,
) -> List[KeywordLookupTerm]:
    if not query_terms or not hasattr(metadata_db, "suggest_query_terms"):
        return list(query_terms)

    normalizer = QueryPhraseNormalizer()
    existing = {(term.group_id or term.concept_idx, term.term) for term in query_terms}
    added: Dict[Tuple[int, str], KeywordLookupTerm] = {}
    group_context: Dict[int, str] = {}
    for term in query_terms:
        group_id = term.group_id or term.concept_idx
        group_context.setdefault(group_id, "")
        group_context[group_id] = f"{group_context[group_id]} {term.term}".strip()
    source_terms = [
        term for term in query_terms
        if term.item_role in {"selected_concept", "sub_concept"}
        and term.term_role in {"primary_canonical", "candidate_text", "normalized_text"}
    ]

    for source_term in source_terms:
        try:
            candidates = metadata_db.suggest_query_terms(
                source_term.term,
                limit=max(1, int(max_candidate_terms_per_item)),
                sources=list(keyword_sources) if keyword_sources else None,
                candidate_pool_limit=max(10, int(max_candidate_terms_per_item) * 4),
            )
        except Exception:
            continue

        for index, candidate in enumerate(candidates[:max_candidate_terms_per_item], start=1):
            candidate_keyword = normalizer.normalize_phrase(str(candidate.get("keyword") or ""))
            if not candidate_keyword:
                continue
            group_id = source_term.group_id or source_term.concept_idx
            if (group_id, candidate_keyword) in existing:
                continue
            route_context = f"{source_term.term} {group_context.get(group_id, '')}"
            if not _db_candidate_has_informative_anchor(route_context, candidate_keyword):
                continue
            route = _classify_db_candidate_route(
                route_context,
                candidate_keyword,
            )
            lookup_term = KeywordLookupTerm(
                concept_idx=source_term.concept_idx,
                concept_id=source_term.concept_id,
                concept_label=source_term.concept_label,
                term=candidate_keyword,
                term_role="db_candidate",
                term_weight=TERM_ROLE_WEIGHTS["db_candidate"],
                group_id=group_id,
                item_id=f"{source_term.item_id}:db:{index}",
                item_role="substring_candidate",
                item_weight=ITEM_WEIGHTS["substring_candidate"],
                route=route,
                route_weight=ROUTE_WEIGHTS[route],
            )
            key = (group_id, candidate_keyword)
            existing_term = added.get(key)
            if existing_term is None or _term_sort_key(lookup_term) < _term_sort_key(existing_term):
                added[key] = lookup_term
            existing.add(key)

    return _dedupe_lookup_terms([*query_terms, *added.values()])


def _classify_db_candidate_route(query_term: str, candidate_keyword: str) -> str:
    query_tokens = _keyword_tokens(query_term)
    candidate_normalized = QueryPhraseNormalizer().normalize_phrase(candidate_keyword)
    informative = [
        token for token in query_tokens
        if len(token) >= 4 and token not in BROAD_SUB_CONCEPT_TERMS
    ]
    component_tokens = [token for token in query_tokens if len(token) >= 3]
    anchor_hit = any(_token_stem(token) in candidate_normalized for token in informative)
    component_hits = 0
    seen_components = set()
    for token in component_tokens:
        component = _token_stem(token)
        if component in seen_components:
            continue
        seen_components.add(component)
        if (
            token in candidate_normalized
            or token.replace("-", " ") in candidate_normalized
            or component in candidate_normalized
        ):
            component_hits += 1
    if anchor_hit and component_hits >= 2:
        return "db_compositional_candidate"
    return "db_substring_candidate"


def _db_candidate_has_informative_anchor(query_term: str, candidate_keyword: str) -> bool:
    candidate_normalized = QueryPhraseNormalizer().normalize_phrase(candidate_keyword)
    for token in _keyword_tokens(query_term):
        if len(token) < 4 or token in BROAD_SUB_CONCEPT_TERMS:
            continue
        if _token_stem(token) in candidate_normalized:
            return True
    return False


def _keyword_tokens(value: str) -> List[str]:
    return list(dict.fromkeys(token.lower() for token in TOKEN_RE.findall(value or "")))


def _token_stem(token: str) -> str:
    token = token.lower()
    for suffix in ("ing", "ion", "ed", "es", "s"):
        if token.endswith(suffix) and len(token) > len(suffix) + 3:
            return token[: -len(suffix)]
    return token


def _dedupe_lookup_terms(terms: Sequence[KeywordLookupTerm]) -> List[KeywordLookupTerm]:
    best_terms: Dict[Tuple[int, str, str], KeywordLookupTerm] = {}
    for term in terms:
        group_id = term.group_id or term.concept_idx
        key = (group_id, term.item_id or "", term.term)
        existing = best_terms.get(key)
        if existing is None or _term_sort_key(term) < _term_sort_key(existing):
            best_terms[key] = term
    return sorted(best_terms.values(), key=_term_sort_key)


def _term_sort_key(term: KeywordLookupTerm) -> Tuple[int, float, str, str, str]:
    route_score = float(term.item_weight) * float(term.term_weight) * float(term.route_weight)
    return (int(term.group_id or term.concept_idx), -route_score, term.item_id or "", term.term, term.term_role)


def _result_from_row(row: Dict[str, Any]) -> PaperKeywordLookupResult:
    matched_count = int(row.get("matched_concept_count") or row.get("matched_group_count") or 0)
    total_count = int(row.get("total_concept_count") or row.get("total_group_count") or 0)
    return PaperKeywordLookupResult(
        paper_id=int(row["paper_id"]),
        work_id=str(row.get("work_id") or ""),
        matched_concept_count=matched_count,
        total_concept_count=total_count,
        keyword_lookup_score=float(row.get("keyword_lookup_score") or 0.0),
        matched_concepts=list(row.get("matched_concepts") or []),
        recall_sources=list(row.get("recall_sources") or ["keyword_lookup"]),
        retrieval_debug=dict(row.get("retrieval_debug") or {}),
    )
