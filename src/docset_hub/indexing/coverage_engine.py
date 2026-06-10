"""Coverage analysis for query semantic plans."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from typing import Any, Dict, List, Mapping, Sequence

from .query_semantic_plan import QuerySemanticPlan, SemanticSpanGroup, SemanticTerm


@dataclass
class CoverageReport:
    matched_span_count: int
    total_span_count: int
    coverage_ratio: float
    matched_spans: List[Dict[str, Any]] = field(default_factory=list)
    missing_spans: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def analyze_document_coverage(
    *,
    plan: QuerySemanticPlan,
    document_fields: Mapping[str, Any],
) -> CoverageReport:
    # Normalize supported document fields up front so every term is matched
    # against a single lowercase, whitespace-collapsed representation.
    normalized_fields = _normalize_document_fields(document_fields)
    matched_spans: List[Dict[str, Any]] = []
    missing_spans: List[Dict[str, Any]] = []

    for span in plan.spans:
        matched_terms: List[str] = []
        matched_fields: List[str] = []
        matched_scopes: List[str] = []
        matched_child_span_ids: List[str] = []
        own_term_matched = False
        # A parent span is considered covered if any of its own terms or any
        # child-span term matches at least one normalized document field.
        for term_info in _iter_span_terms(span):
            term = term_info["term"]
            for field_name, field_value in normalized_fields.items():
                if _term_matches_field(term, field_value):
                    if term.text not in matched_terms:
                        matched_terms.append(term.text)
                    if field_name not in matched_fields:
                        matched_fields.append(field_name)
                    scope = str(term_info["scope"])
                    if scope not in matched_scopes:
                        matched_scopes.append(scope)
                    if scope == "parent":
                        own_term_matched = True
                    child_span_id = term_info.get("child_span_id")
                    if child_span_id and child_span_id not in matched_child_span_ids:
                        matched_child_span_ids.append(child_span_id)
        matched_child_count = len(matched_child_span_ids)
        total_child_count = len(span.children)
        span_score = _calculate_span_score(
            own_term_matched=own_term_matched,
            matched_child_count=matched_child_count,
            total_child_count=total_child_count,
        )
        if span_score > 0.0:
            # Keep only the most informative matched terms for reporting; this
            # does not change whether the span counts as covered.
            matched_terms = _prune_subsumed_terms(matched_terms)
            matched_spans.append(
                {
                    "span_id": span.span_id,
                    "canonical_text": span.canonical_text,
                    "matched_terms": matched_terms,
                    "matched_fields": matched_fields,
                    "matched_scopes": matched_scopes,
                    "matched_child_span_ids": matched_child_span_ids,
                    "own_term_matched": own_term_matched,
                    "matched_child_count": matched_child_count,
                    "total_child_count": total_child_count,
                    "span_score": span_score,
                }
            )
        else:
            missing_spans.append(
                {
                    "span_id": span.span_id,
                    "canonical_text": span.canonical_text,
                }
            )

    return _build_report(plan, matched_spans, missing_spans)


def summarize_expanded_sparse_matches(
    *,
    plan: QuerySemanticPlan,
    matched_spans: Sequence[Mapping[str, Any]],
) -> CoverageReport:
    span_index = {span.span_id: span for span in plan.spans}
    normalized_matches = [
        _normalize_matched_span(item, span_index)
        for item in matched_spans
        if item.get("span_id")
    ]
    matched_ids = {item["span_id"] for item in normalized_matches}
    missing_spans = [
        {
            "span_id": span.span_id,
            "canonical_text": span.canonical_text,
        }
        for span in plan.spans
        if span.span_id not in matched_ids
    ]
    return _build_report(plan, normalized_matches, missing_spans)


def _build_report(
    plan: QuerySemanticPlan,
    matched_spans: Sequence[Mapping[str, Any]],
    missing_spans: Sequence[Mapping[str, Any]],
) -> CoverageReport:
    total_span_count = len(plan.spans)
    matched_span_count = len(matched_spans)
    total_score = sum(float(item.get("span_score") or 0.0) for item in matched_spans)
    coverage_ratio = total_score / float(total_span_count) if total_span_count else 0.0
    return CoverageReport(
        matched_span_count=matched_span_count,
        total_span_count=total_span_count,
        coverage_ratio=coverage_ratio,
        matched_spans=[dict(item) for item in matched_spans],
        missing_spans=[dict(item) for item in missing_spans],
    )


def _normalize_document_fields(document_fields: Mapping[str, Any]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for field_name in ("title", "abstract", "paper_keywords"):
        raw_value = document_fields.get(field_name)
        if isinstance(raw_value, (list, tuple)):
            text = " ".join(str(item) for item in raw_value if item is not None)
        else:
            text = str(raw_value or "")
        # Match against a forgiving textual view of each field so coverage is
        # insensitive to case differences and irregular whitespace.
        normalized[field_name] = " ".join(text.strip().lower().split())
    return normalized


def _iter_span_terms(span: SemanticSpanGroup) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    # Flatten parent and child terms into a single iterable so the caller can
    # evaluate coverage without separate parent/child matching code paths.
    for term in span.own_terms.tier1:
        items.append({"term": term, "scope": "parent", "child_span_id": None, "tier": "tier1"})
    for term in span.own_terms.tier2:
        items.append({"term": term, "scope": "parent", "child_span_id": None, "tier": "tier2"})
    for child in span.children:
        for term in child.own_terms.tier1:
            items.append({"term": term, "scope": "child", "child_span_id": child.span_id, "tier": "tier1"})
        for term in child.own_terms.tier2:
            items.append({"term": term, "scope": "child", "child_span_id": child.span_id, "tier": "tier2"})
    return items


def _term_matches_field(term: SemanticTerm, field_value: str) -> bool:
    normalized_term = " ".join(str(term.text or "").strip().lower().split())
    if not normalized_term or not field_value:
        return False
    if term.match_mode == "prefix":
        # Prefix terms are intended for tree-style expansions such as
        # "immun-" matching "immunotherapy" or "immune-related".
        return bool(re.search(r"(^|[^a-z0-9])" + re.escape(normalized_term) + r"[a-z0-9_-]*", field_value))
    # Exact terms should match a whole token or phrase boundary, not an
    # arbitrary substring inside a longer token.
    return bool(
        re.search(
            r"(^|[^a-z0-9])" + re.escape(normalized_term) + r"([^a-z0-9]|$)",
            field_value,
        )
    )


def _prune_subsumed_terms(terms: Sequence[str]) -> List[str]:
    normalized = sorted({" ".join(str(term).strip().lower().split()) for term in terms if str(term).strip()})
    kept: List[str] = []
    for term in sorted(normalized, key=lambda item: (-len(item), item)):
        if any(term in existing for existing in kept):
            continue
        kept.append(term)
    return sorted(kept)


def _normalize_matched_span(
    item: Mapping[str, Any],
    span_index: Mapping[str, SemanticSpanGroup],
) -> Dict[str, Any]:
    span_id = str(item.get("span_id") or "")
    plan_span = span_index.get(span_id)
    matched_scopes = list(item.get("matched_scopes") or [])
    matched_child_span_ids = list(item.get("matched_child_span_ids") or [])
    own_term_matched = item.get("own_term_matched")
    if own_term_matched is None:
        own_term_matched = "parent" in matched_scopes
    total_child_count = item.get("total_child_count")
    if total_child_count is None:
        total_child_count = len(plan_span.children) if plan_span is not None else 0
    matched_child_count = item.get("matched_child_count")
    if matched_child_count is None:
        matched_child_count = len(matched_child_span_ids)
    span_score = item.get("span_score")
    if span_score is None:
        span_score = _calculate_span_score(
            own_term_matched=bool(own_term_matched),
            matched_child_count=int(matched_child_count or 0),
            total_child_count=int(total_child_count or 0),
        )
    return {
        "span_id": span_id,
        "canonical_text": str(item.get("canonical_text") or ""),
        "matched_terms": list(item.get("matched_terms") or []),
        "matched_fields": list(item.get("matched_fields") or []),
        "matched_scopes": matched_scopes,
        "matched_child_span_ids": matched_child_span_ids,
        "own_term_matched": bool(own_term_matched),
        "matched_child_count": int(matched_child_count or 0),
        "total_child_count": int(total_child_count or 0),
        "span_score": float(span_score or 0.0),
    }


def _calculate_span_score(*, own_term_matched: bool, matched_child_count: int, total_child_count: int) -> float:
    if own_term_matched:
        return 1.0
    if total_child_count > 0:
        return float(matched_child_count) / float(total_child_count)
    return 0.0
