from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from flask import request

DEFAULT_INDEXER_SEARCH_TYPE = "hybrid_retrieval"
VECTOR_MODE_SEARCH_TYPE = "dense"


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


def _get_display_source(item: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
    sources = metadata.get("sources") or []
    matched_source_name = item.get("matched_source_name") or item.get("source_name")
    if matched_source_name:
        for source in sources:
            if source.get("source_name") == matched_source_name:
                return source
    return _get_preferred_source(metadata)


def _extract_doi(metadata: Dict[str, Any], item: Optional[Dict[str, Any]] = None) -> Optional[str]:
    preferred_source = _get_display_source(item or {}, metadata)
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


def _extract_paper_link(
    metadata: Dict[str, Any],
    doi: Optional[str],
    item: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    preferred_source = _get_display_source(item or {}, metadata)
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
    preferred_source = _get_display_source(item, metadata)
    source_name = preferred_source.get("source_name") or item.get("source_name")
    doi = _extract_doi(metadata, item)
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
        "link": _extract_paper_link(metadata, doi, item),
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
            "message": f'您是想搜索 "{corrected_query}" 吗？',
            "action": {
                "label": "使用原 query 检索",
                "mode": "vector",
                "query": query,
            },
        }

    return None


def _normalize_legacy_notice(
    query: str,
    search_query: Optional[str],
    understanding: Optional[Dict[str, Any]],
    search_mode: str,
) -> Optional[Dict[str, Any]]:
    notice = _build_query_notice(
        query=query,
        search_query=search_query,
        understanding=understanding,
        search_mode=search_mode,
    )
    if not notice:
        return None

    action = notice.get("action") or {}
    if action:
        notice["fallback_mode"] = action.get("mode")
        notice["fallback_query"] = action.get("query")
        notice["action_label"] = action.get("label")
    return notice


def _emit_smart_search_debug(
    *,
    query: str,
    search_mode: str,
    search_query: Optional[str],
    understanding: Optional[Dict[str, Any]],
    result_count: int,
    notice: Optional[Dict[str, Any]],
) -> None:
    if search_mode != "smart":
        return
    understanding = understanding or {}
    debug_line = (
        "SMART SEARCH DEBUG | "
        f"query={query} | "
        f"executed={search_query} | "
        f"route={understanding.get('route')} | "
        f"intent={understanding.get('intent')} | "
        f"corrected={understanding.get('corrected_query')} | "
        f"matched_author={understanding.get('matched_author')} | "
        f"suggested_author={understanding.get('suggested_author')} | "
        f"result_count={result_count} | "
        f"notice_type={(notice or {}).get('type')}"
    )
    print(debug_line)


def _emit_smart_search_result_summary(
    *,
    search_mode: str,
    raw_results: Any,
    preview_limit: int = 3,
) -> None:
    if search_mode != "smart":
        return

    if not isinstance(raw_results, list):
        print("SMART SEARCH RESULTS | no_results")
        return

    grouped_lines: List[str] = []
    grouped_detected = False
    for item in raw_results:
        if (
            isinstance(item, (list, tuple))
            and len(item) == 2
            and isinstance(item[0], str)
            and isinstance(item[1], list)
        ):
            grouped_detected = True
            source_prefix = item[0]
            source_results = item[1]
            preview_items = []
            for result in source_results[:preview_limit]:
                if not isinstance(result, dict):
                    continue
                metadata = result.get("metadata") or result
                preview_items.append(
                    {
                        "work_id": result.get("work_id"),
                        "title": metadata.get("canonical_title") or metadata.get("title"),
                        "source": (
                            (result.get("source_name"))
                            or ((metadata.get("sources") or [{}])[0].get("source_name"))
                        ),
                    }
                )
            grouped_lines.append(
                f"SMART SEARCH RESULTS | group={source_prefix} | count={len(source_results)} | preview={preview_items}"
            )

    if grouped_detected:
        for line in grouped_lines:
            print(line)
        return

    preview_items = []
    for result in raw_results[:preview_limit]:
        if not isinstance(result, dict):
            continue
        metadata = result.get("metadata") or result
        preview_items.append(
            {
                "work_id": result.get("work_id"),
                "title": metadata.get("canonical_title") or metadata.get("title"),
                "source": (
                    result.get("source_name")
                    or ((metadata.get("sources") or [{}])[0].get("source_name"))
                ),
            }
        )
    print(
        f"SMART SEARCH RESULTS | group=flat | count={len(raw_results)} | preview={preview_items}"
    )


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


def _normalize_smart_search_payload(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    return {
        "success": payload.get("success"),
        "query": payload.get("query"),
        "search_query": payload.get("search_query"),
        "expanded_search_queries": list(payload.get("expanded_search_queries") or []),
        "query_understanding": dict(payload.get("query_understanding") or {}),
        "results": list(payload.get("results") or []),
    }


def _map_grouped_smart_search_results(raw_results: Any) -> tuple[List[Any], int]:
    if not isinstance(raw_results, list):
        return [], 0

    mapped_grouped_results: List[Any] = []
    next_rank = 1
    total_count = 0

    for item in raw_results:
        if (
            isinstance(item, (list, tuple))
            and len(item) == 2
            and isinstance(item[0], str)
            and isinstance(item[1], list)
        ):
            source_prefix = item[0]
            source_results = item[1]
            mapped_items = []
            for source_item in source_results:
                if not isinstance(source_item, dict):
                    continue
                mapped_items.append(_map_search_item(source_item, rank=next_rank))
                next_rank += 1
            mapped_grouped_results.append((source_prefix, mapped_items))
            total_count += len(mapped_items)

    if mapped_grouped_results:
        return mapped_grouped_results, total_count

    mapped_flat_results = []
    for source_item in raw_results:
        if not isinstance(source_item, dict):
            continue
        mapped_flat_results.append(_map_search_item(source_item, rank=next_rank))
        next_rank += 1
    return mapped_flat_results, len(mapped_flat_results)


def _run_vector_search(
    *,
    indexer: Any,
    query: str,
    source_list: List[str],
    top_k: int,
) -> List[Dict[str, Any]]:
    return indexer.search(
        query=query,
        source_list=source_list,
        top_k=top_k,
        hydrate=True,
        search_type=VECTOR_MODE_SEARCH_TYPE,
    )


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
    resolved_sources = _resolve_search_sources(indexer, source_list)

    if normalized_mode == "vector":
        raw_results = _run_vector_search(
            indexer=indexer,
            query=normalized_query,
            source_list=resolved_sources,
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
        smart_payload = indexer.smart_search(
            query=normalized_query,
            source_list=resolved_sources,
            top_k=normalized_top_k,
            hydrate=True,
        )
        raw_smart_results = smart_payload.get("results")
        _emit_smart_search_result_summary(
            search_mode=normalized_mode,
            raw_results=raw_smart_results,
        )
        raw_results, mapped_result_count = _map_grouped_smart_search_results(
            raw_smart_results
        )
        search_query = smart_payload.get("search_query")
        understanding = dict(smart_payload.get("query_understanding") or {})
        normalized_smart_payload = _normalize_smart_search_payload(smart_payload)
        mapped_results = raw_results
    if normalized_mode == "vector":
        mapped_results = [
            _map_search_item(item, rank=index + 1)
            for index, item in enumerate(raw_results[:normalized_top_k])
        ]
        mapped_result_count = len(mapped_results)
    elapsed_ms = int((time.time() - started_at) * 1000)
    notice = _normalize_legacy_notice(
        query=normalized_query,
        search_query=search_query,
        understanding=understanding,
        search_mode=normalized_mode,
    )
    _emit_smart_search_debug(
        query=normalized_query,
        search_mode=normalized_mode,
        search_query=search_query,
        understanding=understanding,
        result_count=mapped_result_count,
        notice=notice,
    )

    return {
        "success": True,
        "query": _build_query_payload(
            input_query=normalized_query,
            executed_query=search_query,
            search_mode=normalized_mode,
            understanding=understanding,
        ),
        "search_query": search_query,
        "search_mode": normalized_mode,
        "query_understanding": understanding,
        "smart_search": (
            normalized_smart_payload if normalized_mode == "smart" else None
        ),
        "meta": {
            "count": mapped_result_count,
            "elapsed_ms": elapsed_ms,
            "request_id": request_id,
        },
        "notice": notice,
        "count": mapped_result_count,
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
