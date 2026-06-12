from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from flask import request

LANGTAOSHA_SOURCES = ("langtaosha",)
BIORXIV_SOURCES = ("biorxiv_history", "biorxiv_daily")
DEFAULT_INDEXER_SEARCH_TYPE = "hybrid_retrieval"


def _normalize_top_k(raw_value: Any) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = 100
    return min(max(value, 1), 100)


def _validate_search_mode(raw_value: Any) -> str:
    mode = (str(raw_value or "smart")).strip().lower()
    if mode not in {"smart", "vector"}:
        raise ValueError("mode 只能是 smart 或 vector")
    return mode


def _parse_source_list(raw_value: Optional[str]) -> Optional[List[str]]:
    text = (raw_value or "").strip()
    if not text:
        return None
    return [item.strip() for item in text.split(",") if item.strip()]


def _resolve_search_sources(indexer: Any, source_list: Optional[List[str]]) -> List[str]:
    default_sources = list(getattr(indexer, "default_sources", []) or [])
    sources = source_list if source_list else default_sources
    valid_sources = set(default_sources)
    resolved_sources: List[str] = []
    invalid_sources: List[str] = []

    for source in sources:
        if source not in valid_sources:
            invalid_sources.append(source)
            continue
        if source not in resolved_sources:
            resolved_sources.append(source)

    if invalid_sources:
        raise ValueError(
            f"source_list 包含未知 source: {invalid_sources}; 合法 sources: {default_sources}"
        )

    return resolved_sources


def _dedupe_search_results(
    result_groups: List[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    merged_results: List[Dict[str, Any]] = []
    seen_work_ids = set()

    for results in result_groups:
        for item in results:
            work_id = item.get("work_id")
            if work_id:
                if work_id in seen_work_ids:
                    continue
                seen_work_ids.add(work_id)
            merged_results.append(item)

    return merged_results


def _search_by_prioritized_sources(
    indexer: Any,
    query: str,
    source_list: Optional[List[str]],
    top_k: int,
) -> List[Dict[str, Any]]:
    resolved_sources = _resolve_search_sources(indexer, source_list)
    langtaosha_sources = [
        source for source in resolved_sources if source in LANGTAOSHA_SOURCES
    ]
    biorxiv_sources = [
        source for source in resolved_sources if source in BIORXIV_SOURCES
    ]
    other_sources = [
        source
        for source in resolved_sources
        if source not in LANGTAOSHA_SOURCES and source not in BIORXIV_SOURCES
    ]

    result_groups: List[List[Dict[str, Any]]] = []
    for grouped_sources in (langtaosha_sources, biorxiv_sources, other_sources):
        if not grouped_sources:
            result_groups.append([])
            continue
        result_groups.append(
            indexer.search(
                query=query,
                source_list=grouped_sources,
                top_k=top_k,
                hydrate=True,
                search_type=DEFAULT_INDEXER_SEARCH_TYPE,
            )
        )

    return _dedupe_search_results(result_groups)


def _format_date_ymd(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    if len(text) >= 10:
        return text[:10]
    return text or None


def _extract_authors(metadata: Dict[str, Any]) -> str:
    author_items = metadata.get("authors") or []
    if isinstance(author_items, str):
        return author_items
    names = [
        str(item.get("name", "")).strip()
        for item in author_items
        if isinstance(item, dict) and item.get("name")
    ]
    return ", ".join(names)


def _get_preferred_source(metadata: Dict[str, Any]) -> Dict[str, Any]:
    sources = metadata.get("sources") or []
    canonical_source_id = metadata.get("canonical_source_id")
    if canonical_source_id is not None:
        for source in sources:
            if source.get("paper_source_id") == canonical_source_id:
                return source
    return sources[0] if sources else {}


def _extract_doi(metadata: Dict[str, Any]) -> Optional[str]:
    preferred_source = _get_preferred_source(metadata)
    if preferred_source.get("doi"):
        return preferred_source.get("doi")
    for source in metadata.get("sources") or []:
        if source.get("doi"):
            return source.get("doi")
    return metadata.get("doi")


def _normalize_source_label(source_name: Optional[str]) -> str:
    if not source_name:
        return "-"
    if source_name == "langtaosha":
        return "Langtaosha"
    if source_name.startswith("biorxiv_") or source_name == "biorxiv":
        return "Biorxiv"
    return source_name


def _normalize_source_key(source_name: Optional[str]) -> str:
    if not source_name:
        return "unknown"
    if source_name == "langtaosha":
        return "langtaosha"
    if source_name.startswith("biorxiv_") or source_name == "biorxiv":
        return "biorxiv"
    return source_name.lower()


def _extract_paper_link(metadata: Dict[str, Any], doi: Optional[str]) -> Optional[str]:
    preferred_source = _get_preferred_source(metadata)
    if preferred_source.get("source_url"):
        return preferred_source.get("source_url")
    if metadata.get("link"):
        return metadata.get("link")
    if doi:
        return f"https://doi.org/{doi}"
    return None


def _get_ranking_score(item: Dict[str, Any]) -> Optional[float]:
    for key in ("ranking_score", "score", "similarity_score", "similarity"):
        value = item.get(key)
        if value is not None:
            try:
                return round(float(value), 4)
            except (TypeError, ValueError):
                return None
    return None


def _format_reason_score(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _build_match_reasons(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(item.get("match_reasons"), list):
        return item["match_reasons"]

    retrieval_debug = item.get("retrieval_debug") or {}
    matched_retrievers = set(retrieval_debug.get("matched_retrievers") or [])
    reasons: List[Dict[str, Any]] = []

    dense_score = retrieval_debug.get("dense_score")
    if "dense" in matched_retrievers or dense_score is not None:
        reason: Dict[str, Any] = {"key": "dense_recall", "label": "Dense recall"}
        score = _format_reason_score(dense_score)
        if score is not None:
            reason["score"] = score
        reasons.append(reason)

    sparse_score = retrieval_debug.get("sparse_score")
    if "sparse" in matched_retrievers or sparse_score is not None:
        reason = {"key": "sparse_recall", "label": "Sparse recall"}
        score = _format_reason_score(sparse_score)
        if score is not None:
            reason["score"] = score
        reasons.append(reason)

    keyword_score = retrieval_debug.get("keyword_lookup_score")
    if "keyword_lookup" in matched_retrievers or keyword_score is not None:
        reason = {"key": "keyword_recall", "label": "Keyword recall"}
        score = _format_reason_score(keyword_score)
        if score is not None:
            reason["score"] = score
        reasons.append(reason)

    return reasons


def _map_search_item(item: Dict[str, Any], rank: int) -> Dict[str, Any]:
    metadata = item.get("metadata") or item
    preferred_source = _get_preferred_source(metadata)
    source_name = preferred_source.get("source_name") or item.get("source_name")
    doi = _extract_doi(metadata)
    return {
        "work_id": item.get("work_id"),
        "rank": rank,
        "title": metadata.get("canonical_title") or metadata.get("title"),
        "abstract": metadata.get("canonical_abstract") or metadata.get("abstract"),
        "authors": _extract_authors(metadata),
        "source": _normalize_source_label(source_name),
        "source_key": _normalize_source_key(source_name),
        "online_date": _format_date_ymd(
            metadata.get("online_at") or item.get("online_at")
        ),
        "link": _extract_paper_link(metadata, doi),
        "doi": doi,
        "ranking_score": _get_ranking_score(item),
        "match_reasons": _build_match_reasons(item),
    }


def _build_query_notice(
    query: str,
    search_query: Optional[str],
    understanding: Optional[Dict[str, Any]],
    search_mode: str,
) -> Optional[Dict[str, Any]]:
    if search_mode == "vector":
        return {
            "type": "vector",
            "message": "已按原 query 执行向量检索。",
            "action": None,
        }

    if not understanding:
        return None

    intent = understanding.get("intent")
    route = understanding.get("route")
    matched_author = understanding.get("matched_author") or search_query
    suggested_author = understanding.get("suggested_author")
    corrected_query = understanding.get("corrected_query")
    normalized_query = understanding.get("normalized_query") or query

    if intent == "author_name" and route == "metadata_author" and matched_author:
        return {
            "type": "author_name",
            "message": f"已识别为作者名，正在根据作者 {matched_author} 完成搜索。",
            "action": {
                "label": "改用向量检索",
                "mode": "vector",
                "query": query,
            },
        }

    if intent == "author_name" and route == "author_suggestion" and suggested_author:
        return {
            "type": "author_suggestion",
            "message": f'未找到 "{query}" 的高置信作者匹配，是否搜索作者 {suggested_author}？',
            "action": {
                "label": f"搜索作者 {suggested_author}",
                "mode": "smart",
                "query": suggested_author,
            },
        }

    if corrected_query and corrected_query != normalized_query:
        return {
            "type": "query_correction",
            "message": f"已识别到可能的拼写错误，实际搜索 query 为: {corrected_query}",
            "action": {
                "label": "使用原 query 检索",
                "mode": "vector",
                "query": query,
            },
        }

    return None


def _build_query_payload(
    input_query: str,
    executed_query: Optional[str],
    search_mode: str,
    understanding: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "input": input_query,
        "executed": executed_query,
        "mode": search_mode,
        "intent": understanding.get("intent") or "unknown",
        "route": understanding.get("route") or "none",
        "corrected_query": understanding.get("corrected_query"),
        "matched_author": understanding.get("matched_author"),
        "suggested_author": understanding.get("suggested_author"),
    }


def run_scholar_search(
    *,
    indexer: Any,
    query: str,
    top_k: Any = None,
    source_list: Optional[List[str]] = None,
    search_mode: str = "smart",
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    started_at = time.time()
    normalized_query = (query or "").strip()
    if not normalized_query:
        raise ValueError("query 不能为空")

    normalized_top_k = _normalize_top_k(top_k)
    normalized_mode = _validate_search_mode(search_mode)

    if normalized_mode == "vector":
        results = _search_by_prioritized_sources(
            indexer=indexer,
            query=normalized_query,
            source_list=source_list,
            top_k=normalized_top_k,
        )
        search_query = normalized_query
        understanding = {
            "original_query": normalized_query,
            "normalized_query": normalized_query,
            "intent": "semantic_search",
            "route": "vector",
            "corrected_query": None,
            "matched_author": None,
            "suggested_author": None,
        }
    else:
        understanding_result = indexer.query_understanding.analyze(normalized_query)
        understanding = understanding_result.to_dict()
        route = understanding_result.route

        if route == "none":
            results = []
            search_query = None
        elif route == "metadata_author":
            search_query = (
                understanding_result.matched_author
                or understanding_result.normalized_query
            )
            results = indexer.metadata_db.search_by_author(
                author_name=search_query,
                limit=normalized_top_k,
                source_list=_resolve_search_sources(indexer, source_list),
                fuzzy=True,
            )
        elif route == "author_suggestion":
            results = []
            search_query = None
        else:
            search_query = (
                understanding_result.corrected_query
                or understanding_result.normalized_query
            )
            results = _search_by_prioritized_sources(
                indexer=indexer,
                query=search_query,
                source_list=source_list,
                top_k=normalized_top_k,
            )

    mapped_results = [
        _map_search_item(item, rank=index + 1)
        for index, item in enumerate(results[:normalized_top_k])
    ]
    elapsed_ms = int((time.time() - started_at) * 1000)

    return {
        "success": True,
        "query": _build_query_payload(
            input_query=normalized_query,
            executed_query=search_query,
            search_mode=normalized_mode,
            understanding=understanding,
        ),
        "meta": {
            "count": len(mapped_results),
            "elapsed_ms": elapsed_ms,
            "request_id": request_id,
        },
        "notice": _build_query_notice(
            query=normalized_query,
            search_query=search_query,
            understanding=understanding,
            search_mode=normalized_mode,
        ),
        "results": mapped_results,
    }


def _collect_request_args() -> Dict[str, Any]:
    return {
        "query": (request.args.get("query") or "").strip(),
        "mode": request.args.get("mode") or "smart",
        "top_k": request.args.get("top_k"),
        "source_list": request.args.get("source_list"),
    }


def register_scholar_search_api_routes(
    app: Any,
    indexer: Any,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
    *,
    request_id_getter: Optional[Callable[[], str]] = None,
    record_frontend_search_request: Optional[Callable[..., Any]] = None,
    client_surface_getter: Optional[Callable[[], str]] = None,
) -> None:
    def current_request_id() -> str:
        if request_id_getter is None:
            return ""
        return request_id_getter()

    @app.route("/api/scholar/search", methods=["GET"])
    def api_scholar_search():
        try:
            request_id = current_request_id()
            data = run_scholar_search(
                indexer=indexer,
                query=(request.args.get("query") or "").strip(),
                top_k=request.args.get("top_k"),
                source_list=_parse_source_list(request.args.get("source_list")),
                search_mode=request.args.get("mode") or "smart",
                request_id=request_id,
            )
            if record_frontend_search_request is not None:
                record_frontend_search_request(
                    request_args=_collect_request_args(),
                    response_body=dict(data),
                    status_code=200,
                    client_surface=(
                        client_surface_getter() if client_surface_getter else "unknown"
                    ),
                    request_path=request.path,
                    request_method=request.method,
                )
            return api_success(data)
        except ValueError as exc:
            response, status_code = api_error(
                str(exc),
                status_code=400,
                code="INVALID_REQUEST",
            )
            if record_frontend_search_request is not None:
                record_frontend_search_request(
                    request_args=_collect_request_args(),
                    response_body=response.get_json() or {},
                    status_code=status_code,
                    client_surface=(
                        client_surface_getter() if client_surface_getter else "unknown"
                    ),
                    request_path=request.path,
                    request_method=request.method,
                )
            return response, status_code
        except Exception as exc:  # noqa: BLE001
            response, status_code = api_error(
                str(exc),
                status_code=500,
                code="SEARCH_FAILED",
            )
            if record_frontend_search_request is not None:
                record_frontend_search_request(
                    request_args=_collect_request_args(),
                    response_body=response.get_json() or {},
                    status_code=status_code,
                    client_surface=(
                        client_surface_getter() if client_surface_getter else "unknown"
                    ),
                    request_path=request.path,
                    request_method=request.method,
                )
            return response, status_code
