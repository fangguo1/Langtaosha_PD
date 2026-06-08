from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from flask import render_template
from sqlalchemy import text
from sqlalchemy.engine import Engine


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FEEDBACK_REVIEW_TESTBED_PATH = (
    ROOT / "local_data" / "retrieval_testbed" / "import_topic_v1_mimic.json"
)


def load_feedback_review_testbed(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"feedback review testbed not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _feedback_review_testbed_path(explicit_path: Optional[Path] = None) -> Path:
    if explicit_path is not None:
        return explicit_path
    configured = os.environ.get("FEEDBACK_REVIEW_TESTBED_PATH", "").strip()
    if configured:
        return Path(configured)
    return DEFAULT_FEEDBACK_REVIEW_TESTBED_PATH


def _display_source_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _extract_authors_from_json(authors_json: Any) -> str:
    if isinstance(authors_json, str):
        try:
            authors_json = json.loads(authors_json)
        except json.JSONDecodeError:
            return authors_json
    if isinstance(authors_json, list):
        return ", ".join(
            str(item.get("name", "")).strip()
            for item in authors_json
            if isinstance(item, dict) and item.get("name")
        )
    return ""


def _normalize_source_label(source_name: Optional[str]) -> str:
    source = str(source_name or "").strip().lower()
    if "langtaosha" in source:
        return "Langtaosha"
    if "biorxiv" in source:
        return "Biorxiv"
    return source_name or "-"


def _normalize_source_key(source_name: Optional[str]) -> str:
    source = str(source_name or "").strip().lower()
    if "langtaosha" in source:
        return "langtaosha"
    if "biorxiv" in source:
        return "biorxiv"
    return source or "unknown"


def _build_link(source_name: Optional[str], source_url: Optional[str], doi: Optional[str]) -> Optional[str]:
    if source_url:
        return str(source_url)
    if doi:
        return f"https://doi.org/{doi}"
    return None


def _format_date_ymd(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.date().isoformat()
    except AttributeError:
        text_value = str(value)
        return text_value[:10] if text_value else None


def _load_metadata_rows_by_work_id(engine: Engine, work_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not work_ids:
        return {}

    sql = text(
        """
        SELECT
            p.paper_id,
            p.work_id,
            p.canonical_title AS title,
            p.canonical_abstract AS abstract,
            paa.authors,
            COALESCE(p.online_at, ps.online_at) AS online_at,
            ps.source_name,
            ps.source_url,
            ps.doi
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
        WHERE p.work_id = ANY(:work_ids)
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"work_ids": work_ids}).mappings().all()

    return {
        str(row["work_id"]): {
            "paper_id": row.get("paper_id"),
            "work_id": row.get("work_id"),
            "title": row.get("title"),
            "abstract": row.get("abstract"),
            "authors": _extract_authors_from_json(row.get("authors")),
            "online_date": _format_date_ymd(row.get("online_at")),
            "source": _normalize_source_label(row.get("source_name")),
            "source_key": _normalize_source_key(row.get("source_name")),
            "doi": row.get("doi"),
            "link": _build_link(row.get("source_name"), row.get("source_url"), row.get("doi")),
        }
        for row in rows
    }


def build_feedback_review_payload(testbed_payload: dict[str, Any], engine: Engine) -> dict[str, Any]:
    queries = list(testbed_payload.get("queries") or [])
    work_ids = [
        str(label["work_id"])
        for query in queries
        for label in (query.get("labels") or [])
        if label.get("work_id")
    ]
    metadata_by_work_id = _load_metadata_rows_by_work_id(engine, work_ids)

    hydrated_queries: list[dict[str, Any]] = []
    for query in queries:
        labels = list(query.get("labels") or [])
        positive_count = sum(1 for label in labels if int(label.get("label") or 0) > 0)
        negative_count = sum(1 for label in labels if int(label.get("label") or 0) <= 0)
        results = []
        for label in labels:
            work_id = str(label.get("work_id"))
            metadata = metadata_by_work_id.get(work_id, {})
            results.append(
                {
                    "work_id": work_id,
                    "label": int(label.get("label") or 0),
                    **metadata,
                }
            )

        hydrated_queries.append(
            {
                "query_id": int(query["query_id"]),
                "annotator_ids": list(query.get("annotator_ids") or []),
                "annotator_count": int(query.get("annotator_count") or 0),
                "query_text": str(query.get("query_text") or ""),
                "label_summary": {
                    "positive": positive_count,
                    "negative": negative_count,
                },
                "results": results,
            }
        )

    return {
        "testbed_name": testbed_payload.get("testbed_name"),
        "summary": dict(testbed_payload.get("summary") or {}),
        "queries": hydrated_queries,
    }


def register_feedback_review_routes(
    app,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
    *,
    testbed_path: Optional[Path] = None,
    engine_factory: Optional[Callable[[], Engine]] = None,
) -> None:
    from config.config_loader import get_db_engine

    resolved_engine_factory = engine_factory or (lambda: get_db_engine(db_key="metadata_db"))

    @app.route("/feedback-review")
    def feedback_review_page() -> str:
        return render_template("feedback_review.html")

    @app.route("/api/study/feedback-review-data", methods=["GET"])
    def api_feedback_review_data():
        try:
            resolved_path = _feedback_review_testbed_path(testbed_path)
            testbed_payload = load_feedback_review_testbed(resolved_path)
            payload = build_feedback_review_payload(testbed_payload, resolved_engine_factory())
            return api_success(
                {
                    "testbed_name": payload.get("testbed_name"),
                    "summary": payload.get("summary") or {},
                    "source": _display_source_path(resolved_path),
                    "queries": payload.get("queries") or [],
                }
            )
        except FileNotFoundError as exc:
            return api_error(str(exc), status_code=404, code="NOT_FOUND")
        except Exception as exc:  # noqa: BLE001
            return api_error(str(exc), status_code=500, code="FEEDBACK_REVIEW_FAILED")


"""
cd /home/wnlab/langtaosha/Langtaosha_PD

export PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_mimic.yaml
export FEEDBACK_REVIEW_TESTBED_PATH=/home/wnlab/langtaosha/Langtaosha_PD/local_data/retrieval_testbed/import_topic_v1_mimic.json

python app/run_feedback_review.py
"""
