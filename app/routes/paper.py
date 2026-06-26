from __future__ import annotations

import time
from typing import Any, Callable, List, Optional

from flask import request

from docset_hub.indexing.retrieval_helper import (
    annotate_loose_coverage,
    annotate_strict_coverage,
)

SUPPORTED_SEARCH_TYPES = ("dense", "sparse", "hybrid", "hybrid_retrieval", "expanded_sparse")
HYBRID_SEARCH_TYPES = {"hybrid", "hybrid_retrieval"}


def _normalize_top_k(raw_value: Any, default: int = 10) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    return min(max(value, 1), 50)


def _parse_source_list(raw_value: Optional[str]) -> Optional[List[str]]:
    if raw_value is None:
        return None
    items = [item.strip() for item in raw_value.split(",") if item.strip()]
    return items or None


def _parse_hydrate(raw_value: Optional[str]) -> bool:
    if raw_value is None:
        return True
    normalized = raw_value.strip().lower()
    if normalized in {"0", "false", "no", "off"}:
        return False
    return True


def _parse_bool_flag(raw_value: Optional[str]) -> bool:
    if raw_value is None:
        return False
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _annotate_search_results_with_coverage(
    *,
    indexer: Any,
    query: str,
    source_list: Optional[List[str]],
    keyword_sources: Optional[List[str]],
    results: List[dict],
    include_coverage: bool,
    include_loose_coverage: bool,
    timings_ms: dict[str, float],
) -> None:
    if not results or not (include_coverage or include_loose_coverage):
        return

    plan = indexer.build_query_semantic_plan(
        query=query,
        source_list=source_list or indexer.default_sources,
        keyword_sources=keyword_sources,
    )
    if plan is None:
        return

    if include_coverage:
        coverage_started = time.perf_counter()
        annotate_strict_coverage(results, plan=plan)
        timings_ms["strict_coverage"] = round(
            (time.perf_counter() - coverage_started) * 1000.0,
            3,
        )
    if include_loose_coverage:
        loose_started = time.perf_counter()
        annotate_loose_coverage(results=results, plan=plan)
        timings_ms["loose_coverage"] = round(
            (time.perf_counter() - loose_started) * 1000.0,
            3,
        )


def register_paper_indexer_api_routes(
    app,
    indexer: Any,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
) -> None:
    @app.route("/api/health", methods=["GET"])
    def api_health():
        return api_success({"status": "ok", "service": "paper_indexer"})

    @app.route("/api/search", methods=["GET"])
    def api_search():
        try:
            started_at = time.perf_counter()
            query = (request.args.get("query") or "").strip()
            if not query:
                return api_error("query 不能为空", status_code=400, code="INVALID_REQUEST")

            top_k = _normalize_top_k(request.args.get("top_k", default=10, type=int))
            search_type = (request.args.get("search_type") or "dense").strip().lower()
            if search_type not in SUPPORTED_SEARCH_TYPES:
                return api_error(
                    f"search_type 只能是 {', '.join(SUPPORTED_SEARCH_TYPES)}",
                    status_code=400,
                    code="INVALID_REQUEST",
                )

            source_list = _parse_source_list(request.args.get("source_list"))
            keyword_sources = _parse_source_list(request.args.get("keyword_sources"))
            hydrate = _parse_hydrate(request.args.get("hydrate"))
            include_coverage = _parse_bool_flag(request.args.get("include_coverage"))
            include_loose_coverage = _parse_bool_flag(request.args.get("include_loose_coverage"))
            timings_ms: dict[str, float] = {}

            search_started = time.perf_counter()
            if search_type in HYBRID_SEARCH_TYPES:
                results = indexer.hybrid_retrieval_search(
                    query=query,
                    source_list=source_list,
                    top_k=top_k,
                    hydrate=hydrate,
                    keyword_sources=keyword_sources,
                )
            else:
                results = indexer.search(
                    query=query,
                    source_list=source_list,
                    top_k=top_k,
                    hydrate=hydrate,
                    search_type=search_type,
                    keyword_sources=keyword_sources,
                )
            timings_ms["search"] = round((time.perf_counter() - search_started) * 1000.0, 3)

            if search_type in {"dense", "sparse"} and hydrate:
                _annotate_search_results_with_coverage(
                    indexer=indexer,
                    query=query,
                    source_list=source_list,
                    keyword_sources=keyword_sources,
                    results=results,
                    include_coverage=include_coverage,
                    include_loose_coverage=include_loose_coverage,
                    timings_ms=timings_ms,
                )

            elapsed_ms = round((time.perf_counter() - started_at) * 1000.0, 3)
            return api_success(
                {
                    "query": query,
                    "top_k": top_k,
                    "search_type": search_type,
                    "results": results,
                    "timings_ms": timings_ms,
                    "elapsed_ms": elapsed_ms,
                }
            )
        except ValueError as exc:
            return api_error(str(exc), status_code=400, code="INVALID_REQUEST")
        except Exception as exc:  # noqa: BLE001
            return api_error(str(exc), status_code=500, code="SEARCH_FAILED")
