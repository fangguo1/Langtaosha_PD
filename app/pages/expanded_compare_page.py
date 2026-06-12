from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from flask import render_template, request

from src.docset_hub.indexing import serialize_semantic_plan
from src.docset_hub.indexing.coverage_engine import (
    analyze_document_coverage,
    summarize_expanded_sparse_matches,
)
from src.docset_hub.indexing.expanded_sparse_retrieval import (
    build_expanded_sparse_query_rows,
    match_papers_by_expanded_sparse_plan,
)


DEFAULT_TOP_K = 10
MAX_TOP_K = 50


def register_expanded_compare_page_routes(app) -> None:
    @app.route("/expanded-compare")
    def expanded_compare_page() -> str:
        initial_query = (request.args.get("q") or "").strip()
        return render_template(
            "expanded_compare.html",
            initial_query=initial_query,
            default_top_k=DEFAULT_TOP_K,
        )


def register_expanded_compare_api_routes(
    app,
    indexer: Any,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
) -> None:
    @app.route("/api/expanded-compare", methods=["GET"])
    def api_expanded_compare():
        try:
            query = (request.args.get("query") or "").strip()
            if not query:
                return api_error("query 不能为空", status_code=400, code="INVALID_REQUEST")

            top_k = _normalize_top_k(request.args.get("top_k", default=DEFAULT_TOP_K, type=int))
            source_list = _parse_csv_items(request.args.get("source_list"))
            keyword_sources = _parse_csv_items(request.args.get("keyword_sources"))
            errors: Dict[str, str] = {}

            try:
                dense_results = indexer.search(
                    query=query,
                    source_list=source_list,
                    top_k=top_k,
                    hydrate=True,
                    search_type="dense",
                )
            except Exception as exc:  # noqa: BLE001
                dense_results = []
                errors["dense"] = str(exc)

            try:
                sparse_results = indexer.search(
                    query=query,
                    source_list=source_list,
                    top_k=top_k,
                    hydrate=True,
                    search_type="sparse",
                )
            except Exception as exc:  # noqa: BLE001
                sparse_results = []
                errors["sparse"] = str(exc)

            plan = indexer._build_query_semantic_plan(
                query=query,
                source_list=source_list or list(getattr(indexer, "default_sources", []) or []),
                keyword_sources=keyword_sources,
                profile_name="ontology_plus_keyword",
            )
            if plan is None:
                return api_success(
                    {
                        "query": query,
                        "top_k": top_k,
                        "semantic_plan": None,
                        "expanded_query_rows": [],
                        "highlight_terms": [],
                        "errors": errors,
                        "results": {
                            "dense": [_serialize_sparse_result(item, rank) for rank, item in enumerate(dense_results, start=1)],
                            "sparse": [_serialize_sparse_result(item, rank) for rank, item in enumerate(sparse_results, start=1)],
                            "expanded_sparse": [],
                        },
                    }
                )

            expanded_query_rows = build_expanded_sparse_query_rows(plan)
            expanded_candidates = match_papers_by_expanded_sparse_plan(
                metadata_db=indexer.metadata_db,
                plan=plan,
                source_list=source_list,
                keyword_sources=keyword_sources,
                top_k=top_k,
            )
            highlight_terms = _extract_highlight_terms(expanded_query_rows)

            return api_success(
                {
                    "query": query,
                    "top_k": top_k,
                    "semantic_plan": serialize_semantic_plan(plan),
                    "expanded_query_rows": expanded_query_rows,
                    "highlight_terms": highlight_terms,
                    "errors": errors,
                    "results": {
                        "dense": [
                            _serialize_dense_or_sparse_result(plan, item, rank)
                            for rank, item in enumerate(dense_results, start=1)
                        ],
                        "sparse": [
                            _serialize_dense_or_sparse_result(plan, item, rank)
                            for rank, item in enumerate(sparse_results, start=1)
                        ],
                        "expanded_sparse": [
                            _serialize_expanded_result(plan, indexer, item, rank)
                            for rank, item in enumerate(expanded_candidates, start=1)
                        ],
                    },
                }
            )
        except ValueError as exc:
            return api_error(str(exc), status_code=400, code="INVALID_REQUEST")
        except Exception as exc:  # noqa: BLE001
            return api_error(str(exc), status_code=500, code="EXPANDED_COMPARE_FAILED")


def _normalize_top_k(raw_value: Any) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return DEFAULT_TOP_K
    return min(max(value, 1), MAX_TOP_K)


def _parse_csv_items(raw_value: Optional[str]) -> Optional[List[str]]:
    if raw_value is None:
        return None
    items = [item.strip() for item in raw_value.split(",") if item.strip()]
    return items or None


def _extract_highlight_terms(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, str]]:
    seen = set()
    terms: List[Dict[str, str]] = []
    for row in rows:
        text = _normalize_text(row.get("term"))
        match_mode = str(row.get("match_mode") or "exact")
        if not text:
            continue
        key = (text, match_mode)
        if key in seen:
            continue
        seen.add(key)
        terms.append({"text": text, "match_mode": match_mode})
    return terms


def _serialize_sparse_result(item: Mapping[str, Any], rank: int) -> Dict[str, Any]:
    metadata = dict(item.get("metadata") or {})
    return {
        "rank": rank,
        "paper_id": item.get("paper_id") or metadata.get("paper_id"),
        "work_id": item.get("work_id") or metadata.get("work_id"),
        "score": item.get("similarity") or item.get("score"),
        "title": _first_text(metadata, "canonical_title", "title"),
        "abstract": _first_text(metadata, "canonical_abstract", "abstract"),
        "keywords": _extract_keywords(metadata),
        "source_name": item.get("source_name") or metadata.get("source_name"),
        "retrieval_debug": dict(item.get("retrieval_debug") or {}),
    }


def _serialize_dense_or_sparse_result(plan: Any, item: Mapping[str, Any], rank: int) -> Dict[str, Any]:
    serialized = _serialize_sparse_result(item, rank)
    coverage = analyze_document_coverage(
        plan=plan,
        document_fields=_build_document_fields(serialized),
    )
    serialized["coverage_ratio"] = float(coverage.coverage_ratio or 0.0)
    serialized["coverage"] = coverage.to_dict()
    serialized["matched_span_count"] = int(coverage.matched_span_count or 0)
    serialized["total_span_count"] = int(coverage.total_span_count or 0)
    serialized["matched_spans"] = list(coverage.matched_spans or [])
    return serialized


def _serialize_expanded_result(plan: Any, indexer: Any, candidate: Any, rank: int) -> Dict[str, Any]:
    work_id = str(getattr(candidate, "work_id", "") or "")
    metadata = _read_metadata_by_work_id(indexer, work_id)
    coverage = summarize_expanded_sparse_matches(
        plan=plan,
        matched_spans=list(getattr(candidate, "matched_spans", []) or []),
    )
    return {
        "rank": rank,
        "paper_id": getattr(candidate, "paper_id", None) or metadata.get("paper_id"),
        "work_id": work_id or metadata.get("work_id"),
        "score": float(coverage.coverage_ratio or 0.0),
        "coverage_ratio": float(coverage.coverage_ratio or 0.0),
        "coverage": coverage.to_dict(),
        "matched_span_count": int(coverage.matched_span_count or 0),
        "total_span_count": int(coverage.total_span_count or 0),
        "matched_spans": list(coverage.matched_spans or []),
        "title": _first_text(metadata, "canonical_title", "title"),
        "abstract": _first_text(metadata, "canonical_abstract", "abstract"),
        "keywords": _extract_keywords(metadata),
        "source_name": metadata.get("source_name"),
        "retrieval_debug": dict(getattr(candidate, "retrieval_debug", {}) or {}),
    }


def _read_metadata_by_work_id(indexer: Any, work_id: str) -> Dict[str, Any]:
    if not work_id:
        return {}
    try:
        return dict(indexer.metadata_db.read_paper_by_work_id(work_id) or {})
    except Exception:  # noqa: BLE001
        return {}


def _first_text(metadata: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = metadata.get(key)
        if isinstance(value, list):
            value = next((item for item in value if item), "")
        if value:
            return str(value)
    return ""


def _extract_keywords(metadata: Mapping[str, Any]) -> List[str]:
    raw_keywords = metadata.get("paper_keywords") or metadata.get("keywords") or []
    keywords: List[str] = []
    if isinstance(raw_keywords, str):
        raw_keywords = [raw_keywords]
    for item in raw_keywords:
        if isinstance(item, Mapping):
            value = item.get("keyword") or item.get("text") or item.get("name")
        else:
            value = item
        text = _normalize_text(value)
        if text and text not in keywords:
            keywords.append(text)
    return keywords


def _build_document_fields(item: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "title": item.get("title") or "",
        "abstract": item.get("abstract") or "",
        "paper_keywords": list(item.get("keywords") or []),
    }


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())
