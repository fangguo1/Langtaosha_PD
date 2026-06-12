"""Dev-only semantic plan inspection API.

只在 develop API app（main_develop）注册，production app/main.py 不挂载。
定位与 /api/span-matcher 一致：错误分析与人工调试用，不承诺接口稳定性。
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from flask import request

from src.docset_hub.indexing import (
    build_expanded_sparse_query_rows,
    serialize_semantic_plan,
)


def _parse_csv_items(raw_value: Optional[str]) -> Optional[List[str]]:
    if raw_value is None:
        return None
    items = [item.strip() for item in raw_value.split(",") if item.strip()]
    return items or None


def _extract_highlight_terms(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, str]]:
    seen = set()
    terms: List[Dict[str, str]] = []
    for row in rows:
        text = " ".join(str(row.get("term") or "").strip().lower().split())
        match_mode = str(row.get("match_mode") or "exact")
        if not text:
            continue
        key = (text, match_mode)
        if key in seen:
            continue
        seen.add(key)
        terms.append({"text": text, "match_mode": match_mode})
    return terms


def register_semantic_plan_api_routes(
    app,
    indexer: Any,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
) -> None:
    @app.route("/api/semantic-plan", methods=["GET"])
    def api_semantic_plan():
        try:
            query = (request.args.get("query") or "").strip()
            if not query:
                return api_error("query 不能为空", status_code=400, code="INVALID_REQUEST")

            source_list = _parse_csv_items(request.args.get("source_list"))
            keyword_sources = _parse_csv_items(request.args.get("keyword_sources"))
            profile_name = (request.args.get("profile") or "ontology_plus_keyword").strip()

            plan = indexer.build_query_semantic_plan(
                query=query,
                source_list=source_list or list(getattr(indexer, "default_sources", []) or []),
                keyword_sources=keyword_sources,
                profile_name=profile_name,
            )
            if plan is None:
                return api_success(
                    {
                        "query": query,
                        "semantic_plan": None,
                        "expanded_query_rows": [],
                        "highlight_terms": [],
                    }
                )

            rows = build_expanded_sparse_query_rows(plan)
            return api_success(
                {
                    "query": query,
                    "semantic_plan": serialize_semantic_plan(plan),
                    "expanded_query_rows": rows,
                    "highlight_terms": _extract_highlight_terms(rows),
                }
            )
        except ValueError as exc:
            return api_error(str(exc), status_code=400, code="INVALID_REQUEST")
        except Exception as exc:  # noqa: BLE001
            return api_error(str(exc), status_code=500, code="SEMANTIC_PLAN_FAILED")
