"""Expanded sparse retrieval over semantic span groups."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence

from .query_semantic_plan import QuerySemanticPlan, SemanticChildSpan, SemanticTerm

if TYPE_CHECKING:
    from ..storage.metadata_db import MetadataDB


@dataclass(frozen=True)
class ExpandedSparseGroup:
    group_id: int
    span_id: str
    canonical_text: str
    own_tier1_terms: List[Dict[str, str]]
    own_tier2_terms: List[Dict[str, str]]
    children: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExpandedSparseCandidate:
    paper_id: int
    work_id: str
    matched_span_count: int
    total_span_count: int
    coverage_ratio: float
    matched_spans: List[Dict[str, Any]] = field(default_factory=list)
    retrieval_debug: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def build_expanded_sparse_groups(plan: QuerySemanticPlan) -> List[ExpandedSparseGroup]:
    groups: List[ExpandedSparseGroup] = []
    for index, span in enumerate(plan.spans, start=1):
        groups.append(
            ExpandedSparseGroup(
                group_id=index,
                span_id=span.span_id,
                canonical_text=span.canonical_text,
                own_tier1_terms=_serialize_terms(span.own_terms.tier1),
                own_tier2_terms=_serialize_terms(span.own_terms.tier2),
                children=[_serialize_child(child) for child in span.children],
            )
        )
    return groups


def build_expanded_sparse_query_rows(plan: QuerySemanticPlan) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    for group in build_expanded_sparse_groups(plan):
        for term in group.own_tier1_terms:
            _append_query_row(
                rows,
                seen,
                group_id=group.group_id,
                span_id=group.span_id,
                canonical_text=group.canonical_text,
                span_scope="parent",
                child_span_id=None,
                term_tier="tier1",
                term=term["text"],
                match_mode=term["match_mode"],
            )
        for term in group.own_tier2_terms:
            _append_query_row(
                rows,
                seen,
                group_id=group.group_id,
                span_id=group.span_id,
                canonical_text=group.canonical_text,
                span_scope="parent",
                child_span_id=None,
                term_tier="tier2",
                term=term["text"],
                match_mode=term["match_mode"],
            )
        for child in group.children:
            for term in child.get("own_tier1_terms", []):
                _append_query_row(
                    rows,
                    seen,
                    group_id=group.group_id,
                    span_id=group.span_id,
                    canonical_text=group.canonical_text,
                    span_scope="child",
                    child_span_id=child["span_id"],
                    term_tier="tier1",
                    term=term["text"],
                    match_mode=term["match_mode"],
                )
            for term in child.get("own_tier2_terms", []):
                _append_query_row(
                    rows,
                    seen,
                    group_id=group.group_id,
                    span_id=group.span_id,
                    canonical_text=group.canonical_text,
                    span_scope="child",
                    child_span_id=child["span_id"],
                    term_tier="tier2",
                    term=term["text"],
                    match_mode=term["match_mode"],
                )
    return rows


def match_papers_by_expanded_sparse_plan(
    *,
    metadata_db: MetadataDB,
    plan: QuerySemanticPlan,
    source_list: Optional[Sequence[str]] = None,
    keyword_sources: Optional[Sequence[str]] = None,
    top_k: int = 50,
) -> List[ExpandedSparseCandidate]:
    rows_payload = build_expanded_sparse_query_rows(plan)
    rows = metadata_db.lookup_papers_by_expanded_sparse_groups(
        span_groups=rows_payload,
        source_list=source_list,
        keyword_sources=keyword_sources,
        top_k=top_k,
    )
    return [_candidate_from_row(row) for row in rows]


def _candidate_from_row(row: Mapping[str, Any]) -> ExpandedSparseCandidate:
    return ExpandedSparseCandidate(
        paper_id=int(row.get("paper_id") or 0),
        work_id=str(row.get("work_id") or ""),
        matched_span_count=int(row.get("matched_span_count") or 0),
        total_span_count=int(row.get("total_span_count") or 0),
        coverage_ratio=float(row.get("coverage_ratio") or 0.0),
        matched_spans=list(row.get("matched_spans") or []),
        retrieval_debug=dict(row.get("retrieval_debug") or {}),
    )


def _serialize_terms(terms: Sequence[SemanticTerm]) -> List[Dict[str, str]]:
    serialized: List[Dict[str, str]] = []
    seen = set()
    for term in terms:
        normalized_text = _normalize_term(term.text)
        if not normalized_text:
            continue
        key = (normalized_text, term.match_mode)
        if key in seen:
            continue
        seen.add(key)
        serialized.append({"text": normalized_text, "match_mode": term.match_mode})
    return sorted(serialized, key=lambda item: (item["text"], item["match_mode"]))


def _serialize_child(child: SemanticChildSpan) -> Dict[str, Any]:
    return {
        "span_id": child.span_id,
        "surface_text": child.surface_text,
        "canonical_text": child.canonical_text,
        "start": child.start,
        "end": child.end,
        "own_tier1_terms": _serialize_terms(child.own_terms.tier1),
        "own_tier2_terms": _serialize_terms(child.own_terms.tier2),
    }


def _append_query_row(
    rows: List[Dict[str, Any]],
    seen: set,
    *,
    group_id: int,
    span_id: str,
    canonical_text: str,
    span_scope: str,
    child_span_id: Optional[str],
    term_tier: str,
    term: str,
    match_mode: str,
) -> None:
    normalized_term = _normalize_term(term)
    if not normalized_term:
        return
    key = (group_id, span_scope, child_span_id, term_tier, normalized_term, match_mode)
    if key in seen:
        return
    seen.add(key)
    rows.append(
        {
            "group_id": group_id,
            "span_id": span_id,
            "canonical_text": canonical_text,
            "span_scope": span_scope,
            "child_span_id": child_span_id,
            "term_tier": term_tier,
            "match_mode": match_mode,
            "term": normalized_term,
        }
    )


def _normalize_term(term: str) -> str:
    return " ".join(str(term or "").strip().lower().split())
