#!/usr/bin/env python3
"""Scholar-like search web app."""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template, request
from sqlalchemy import text

# 项目根目录（Langtaosha_PD）
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from config.config_loader import get_db_engine, init_config
from src.docset_hub.indexing import PaperIndexer, build_search_highlight


def _resolve_config_path() -> Path:
    """优先使用环境变量，否则使用腾讯后端配置。"""
    default_cfg = ROOT / "src" / "config" / "config_tecent_backend_server_mimic.yaml"
    return Path(
        os.environ.get(
            "PD_BACKEND_CONFIG",
            os.environ.get("PD_TEST_CONFIG", str(default_cfg)),
        )
    )


CONFIG_PATH = _resolve_config_path()
init_config(CONFIG_PATH)

indexer = PaperIndexer(
    config_path=CONFIG_PATH,
    enable_vectorization=True,
)

app = Flask(
    __name__,
    root_path=str(ROOT),
    template_folder="templates",
)

LANGTAOSHA_SOURCES = ("langtaosha",)
BIORXIV_SOURCES = ("biorxiv_history", "biorxiv_daily")
SOURCE_SEARCH_TOP_K = 10


def _extract_doi(metadata: Dict[str, Any]) -> Optional[str]:
    sources = metadata.get("sources") or []
    canonical_source_id = metadata.get("canonical_source_id")

    if canonical_source_id is not None:
        for source in sources:
            if source.get("paper_source_id") == canonical_source_id and source.get("doi"):
                return source.get("doi")

    for source in sources:
        if source.get("doi"):
            return source.get("doi")
    return None


def _extract_authors(metadata: Dict[str, Any]) -> str:
    author_items = metadata.get("authors") or []
    names = [item.get("name", "").strip() for item in author_items if item.get("name")]
    return ", ".join(names)


def _normalize_source_label(source_name: Optional[str]) -> str:
    if not source_name:
        return "-"
    if source_name.startswith("biorxiv_"):
        return "Biorxiv"
    if source_name == "langtaosha":
        return "Langtaosha"
    return source_name


def _normalize_source_key(source_name: Optional[str]) -> str:
    if not source_name:
        return "unknown"
    if source_name.startswith("biorxiv_"):
        return "biorxiv"
    if source_name == "langtaosha":
        return "langtaosha"
    return source_name.lower()


def _get_preferred_source(metadata: Dict[str, Any]) -> Dict[str, Any]:
    sources = metadata.get("sources") or []
    canonical_source_id = metadata.get("canonical_source_id")

    if canonical_source_id is not None:
        for source in sources:
            if source.get("paper_source_id") == canonical_source_id:
                return source

    if sources:
        return sources[0]
    return {}


def _extract_paper_link(metadata: Dict[str, Any], doi: Optional[str]) -> Optional[str]:
    preferred_source = _get_preferred_source(metadata)
    if preferred_source.get("source_url"):
        return preferred_source.get("source_url")

    source_name = preferred_source.get("source_name")
    if source_name and source_name.startswith("biorxiv_") and doi:
        return f"https://www.biorxiv.org/content/{doi}"

    return None


def _build_link(source_name: Optional[str], source_url: Optional[str], doi: Optional[str]) -> Optional[str]:
    if source_url:
        return source_url
    if source_name and source_name.startswith("biorxiv_") and doi:
        return f"https://www.biorxiv.org/content/{doi}"
    return None


def _extract_authors_from_json(authors_json: Any) -> str:
    if not authors_json or not isinstance(authors_json, list):
        return ""
    names = []
    for item in authors_json:
        if isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            if name:
                names.append(name)
    return ", ".join(names)


def _format_date_ymd(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.strftime("%Y-%m-%d")
    except Exception:
        s = str(value)
        if len(s) >= 10:
            return s[:10]
        return s


def _get_similarity_score(item: Dict[str, Any]) -> Optional[float]:
    score = item.get("similarity_score", item.get("similarity"))
    if score is None:
        return None
    try:
        return float(score)
    except (TypeError, ValueError):
        return None


def _resolve_search_sources(source_list: Optional[List[str]]) -> List[str]:
    sources = source_list if source_list else list(indexer.default_sources)
    valid_sources = set(indexer.default_sources)
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
            f"source_list 包含未知 source: {invalid_sources}; "
            f"合法 sources: {indexer.default_sources}"
        )

    return resolved_sources


def _dedupe_search_results(result_groups: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
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


def _prioritized_vector_search(
    query: str,
    source_list: Optional[List[str]],
    per_source_top_k: int = SOURCE_SEARCH_TOP_K,
) -> List[Dict[str, Any]]:
    resolved_sources = _resolve_search_sources(source_list)
    langtaosha_sources = [
        source for source in resolved_sources
        if source in LANGTAOSHA_SOURCES
    ]
    biorxiv_sources = [
        source for source in resolved_sources
        if source in BIORXIV_SOURCES
    ]
    other_sources = [
        source for source in resolved_sources
        if source not in LANGTAOSHA_SOURCES and source not in BIORXIV_SOURCES
    ]

    langtaosha_results = []
    if langtaosha_sources:
        langtaosha_results = indexer.search(
            query=query,
            source_list=langtaosha_sources,
            top_k=per_source_top_k,
            hydrate=True,
        )

    biorxiv_results = []
    if biorxiv_sources:
        biorxiv_results = indexer.search(
            query=query,
            source_list=biorxiv_sources,
            top_k=per_source_top_k,
            hydrate=True,
        )

    other_results = []
    if other_sources:
        other_results = indexer.search(
            query=query,
            source_list=other_sources,
            top_k=per_source_top_k,
            hydrate=True,
        )

    return _dedupe_search_results(
        [langtaosha_results, biorxiv_results, other_results]
    )


def _map_search_item(
    item: Dict[str, Any],
    highlight: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    metadata = item.get("metadata") or item
    doi = _extract_doi(metadata)
    preferred_source = _get_preferred_source(metadata)
    raw_source_name = preferred_source.get("source_name") or item.get("source_name")
    online_at_raw = metadata.get("online_at")
    similarity_score = _get_similarity_score(item)
    mapped = {
        "work_id": item.get("work_id"),
        "paper_id": item.get("paper_id"),
        "source_name": item.get("source_name"),
        "similarity": similarity_score,
        "similarity_score": similarity_score,
        "title": metadata.get("canonical_title"),
        "abstract": metadata.get("canonical_abstract"),
        "authors": _extract_authors(metadata),
        "doi": doi,
        "online_date": _format_date_ymd(online_at_raw),
        "source": _normalize_source_label(raw_source_name),
        "source_key": _normalize_source_key(raw_source_name),
        "link": _extract_paper_link(metadata, doi),
    }
    if highlight:
        mapped["highlight"] = highlight
    return mapped


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
            "action_label": "改用向量检索",
            "fallback_mode": "vector",
            "fallback_query": query,
        }

    if intent == "author_name" and route == "author_suggestion" and suggested_author:
        return {
            "type": "author_suggestion",
            "message": f'未找到 "{query}" 的高置信作者匹配，是否搜索作者 {suggested_author}？',
            "action_label": f"搜索作者 {suggested_author}",
            "fallback_mode": "smart",
            "fallback_query": suggested_author,
        }

    if corrected_query and corrected_query != normalized_query:
        return {
            "type": "query_correction",
            "message": f"已识别到可能的拼写错误，实际搜索 query 为: {corrected_query}",
            "action_label": "使用原 query 检索",
            "fallback_mode": "vector",
            "fallback_query": query,
        }

    return None


def _get_daily_new_papers(limit: int = 10) -> List[Dict[str, Any]]:
    limit = max(1, min(limit, 20))
    engine = get_db_engine(db_key="metadata_db")
    sql = text(
        """
        SELECT
            p.paper_id,
            p.work_id,
            p.canonical_title AS title,
            COALESCE(p.online_at, ps.online_at) AS online_at,
            ps.source_name,
            ps.source_url,
            ps.doi,
            paa.authors
        FROM papers p
        LEFT JOIN paper_author_affiliation paa ON paa.paper_id = p.paper_id
        LEFT JOIN LATERAL (
            SELECT
                ps1.source_name,
                ps1.source_url,
                ps1.doi,
                ps1.online_at,
                ps1.paper_source_id
            FROM paper_sources ps1
            WHERE ps1.paper_id = p.paper_id
            ORDER BY
                CASE WHEN ps1.paper_source_id = p.canonical_source_id THEN 0 ELSE 1 END,
                ps1.online_at DESC NULLS LAST,
                ps1.paper_source_id DESC
            LIMIT 1
        ) ps ON TRUE
        WHERE COALESCE(p.online_at, ps.online_at) IS NOT NULL
        ORDER BY COALESCE(p.online_at, ps.online_at) DESC
        LIMIT :limit
        """
    )
    rows: List[Dict[str, Any]] = []
    with engine.connect() as conn:
        result = conn.execute(sql, {"limit": limit})
        for row in result.mappings():
            source_name = row.get("source_name")
            doi = row.get("doi")
            source_url = row.get("source_url")
            online_at = row.get("online_at")
            rows.append(
                {
                    "paper_id": row.get("paper_id"),
                    "work_id": row.get("work_id"),
                    "title": row.get("title"),
                    "authors": _extract_authors_from_json(row.get("authors")),
                    "online_at": online_at.isoformat() if online_at else None,
                    "online_date": _format_date_ymd(online_at),
                    "source": _normalize_source_label(source_name),
                    "source_key": _normalize_source_key(source_name),
                    "link": _build_link(source_name, source_url, doi),
                }
            )
    return rows


@app.route("/")
def index() -> str:
    return render_template("welcome.html")


@app.route("/search")
def search_page() -> str:
    query = (request.args.get("q") or "").strip()
    return render_template("search.html", initial_query=query)


@app.route("/api/scholar/search", methods=["GET"])
def api_scholar_search():
    query = (request.args.get("query") or "").strip()
    if not query:
        return jsonify({"success": False, "error": "query 不能为空"}), 400

    top_k = request.args.get("top_k", default=100, type=int)
    if top_k is None or top_k <= 0:
        top_k = 100
    if top_k > 100:
        top_k = 100

    source_list_raw = (request.args.get("source_list") or "").strip()
    source_list: Optional[List[str]] = None
    if source_list_raw:
        source_list = [x.strip() for x in source_list_raw.split(",") if x.strip()]

    search_mode = (request.args.get("mode") or "smart").strip().lower()
    if search_mode not in {"smart", "vector"}:
        return jsonify({"success": False, "error": "mode 只能是 smart 或 vector"}), 400

    try:
        if search_mode == "vector":
            results = _prioritized_vector_search(
                query=query,
                source_list=source_list,
            )
            search_query = query
            understanding = {
                "original_query": query,
                "normalized_query": query,
                "intent": "semantic_search",
                "route": "vector",
                "corrected_query": None,
                "matched_author": None,
                "suggested_author": None,
                "confidence": 0.0,
                "candidates": [],
                "corrections": [],
                "reason": "forced_vector_search",
            }
        else:
            understanding_result = indexer.query_understanding.analyze(query)
            understanding = understanding_result.to_dict()

            if understanding_result.route == "none":
                results = []
                search_query = None
            elif understanding_result.route == "metadata_author":
                resolved_sources = _resolve_search_sources(source_list)
                search_query = (
                    understanding_result.matched_author
                    or understanding_result.normalized_query
                )
                results = indexer.metadata_db.search_by_author(
                    author_name=search_query,
                    limit=top_k,
                    source_list=resolved_sources,
                    fuzzy=True,
                )
            elif understanding_result.route == "author_suggestion":
                results = []
                search_query = None
            else:
                search_query = (
                    understanding_result.corrected_query
                    or understanding_result.normalized_query
                )
                results = _prioritized_vector_search(
                    query=search_query,
                    source_list=source_list,
                )

        highlight = build_search_highlight(
            query=query,
            search_query=search_query,
            understanding=understanding,
            search_mode=search_mode,
        )
        mapped_results = [_map_search_item(item, highlight=highlight) for item in results]
        return jsonify(
            {
                "success": True,
                "query": query,
                "search_query": search_query,
                "search_mode": search_mode,
                "query_understanding": understanding,
                "result_policy": {
                    "langtaosha_top_k": SOURCE_SEARCH_TOP_K,
                    "biorxiv_top_k": SOURCE_SEARCH_TOP_K,
                    "dedupe_key": "work_id",
                    "default_frontend_source": "langtaosha",
                },
                "notice": _build_query_notice(
                    query=query,
                    search_query=search_query,
                    understanding=understanding,
                    search_mode=search_mode,
                ),
                "count": len(mapped_results),
                "results": mapped_results,
            }
        )
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.route("/api/scholar/daily_new", methods=["GET"])
def api_daily_new():
    limit = request.args.get("limit", default=10, type=int)
    if limit is None or limit <= 0:
        limit = 10
    try:
        papers = _get_daily_new_papers(limit=limit)
        return jsonify({"success": True, "count": len(papers), "results": papers})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5004"))
    print("=" * 60)
    print("Scholar Search Web 启动")
    print("=" * 60)
    print(f"配置文件: {CONFIG_PATH}")
    print(f"访问地址: http://localhost:{port}")
    print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=True)
