from __future__ import annotations

from typing import Any, Callable, List, Optional

from flask import request

SUPPORTED_SEARCH_TYPES = ("dense", "sparse", "hybrid", "hybrid_retrieval", "expanded_sparse")


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

            results = indexer.search(
                query=query,
                source_list=_parse_source_list(request.args.get("source_list")),
                top_k=top_k,
                hydrate=_parse_hydrate(request.args.get("hydrate")),
                search_type=search_type,
                keyword_sources=_parse_source_list(request.args.get("keyword_sources")),
                include_coverage=_parse_bool_flag(request.args.get("include_coverage")),
            )
            return api_success(
                {
                    "query": query,
                    "top_k": top_k,
                    "search_type": search_type,
                    "results": results,
                }
            )
        except ValueError as exc:
            return api_error(str(exc), status_code=400, code="INVALID_REQUEST")
        except Exception as exc:  # noqa: BLE001
            return api_error(str(exc), status_code=500, code="SEARCH_FAILED")
