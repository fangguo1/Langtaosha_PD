from __future__ import annotations

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from config.config_loader import get_db_engine, get_frontend_search_logging_config


logger = logging.getLogger(__name__)
event_logger = logging.getLogger(f"{__name__}.event")
event_logger.propagate = False
if not event_logger.handlers:
    event_logger.addHandler(logging.NullHandler())

FRONTEND_SEARCH_LOG_EVENT = "frontend_scholar_search"
DEFAULT_CLIENT_SURFACE = "unknown"
DEFAULT_REQUEST_PATH = "/api/scholar/search"
DEFAULT_REQUEST_METHOD = "GET"


def _json_payload(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _coerce_optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_limited_text(value: Any, max_length: Optional[int] = None) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    if max_length is not None:
        return text_value[:max_length]
    return text_value


def build_frontend_search_jsonl_path(
    root_dir: Path,
    filename_pattern: str,
    partition_by_year: bool,
    now: datetime,
) -> Path:
    local_now = now.astimezone()
    date_value = local_now.date().isoformat()
    filename = filename_pattern.format(date=date_value)
    if partition_by_year:
        return root_dir / str(local_now.year) / filename
    return root_dir / filename


def _compute_frontend_search_status(response_body: Dict[str, Any], status_code: int) -> str:
    if status_code >= 400 or response_body.get("success") is False:
        return "failed"

    meta = response_body.get("meta") or {}
    result_count = meta.get("count") if isinstance(meta, dict) else None
    if result_count is None:
        result_count = len(response_body.get("results") or [])
    try:
        return "empty" if int(result_count or 0) <= 0 else "ok"
    except (TypeError, ValueError):
        return "ok"


def _truncate_frontend_search_results(
    results: List[Any],
    max_results: int,
) -> Tuple[List[Any], bool, int, int]:
    full_count = len(results)
    logged_results = [_compact_logged_result_item(item) for item in results[:max_results]]
    return (
        logged_results,
        full_count > max_results,
        len(logged_results),
        full_count,
    )


def _compact_logged_result_item(item: Any) -> Any:
    if isinstance(item, dict):
        return _compact_logged_paper(item)
    if (
        isinstance(item, (list, tuple))
        and len(item) == 2
        and isinstance(item[0], str)
        and isinstance(item[1], list)
    ):
        return (
            item[0],
            [_compact_logged_paper(entry) for entry in item[1] if isinstance(entry, dict)],
        )
    return item


def _compact_logged_paper(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "work_id": item.get("work_id"),
        "title": item.get("title") or ((item.get("metadata") or {}).get("canonical_title")),
    }


def build_frontend_search_log_payload(
    request_args: Dict[str, Any],
    response_body: Dict[str, Any],
    client_surface: str,
    status_code: int,
    request_path: str = DEFAULT_REQUEST_PATH,
    request_method: str = DEFAULT_REQUEST_METHOD,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    config = get_frontend_search_logging_config()
    max_results = max(1, int((config.get("response_log") or {}).get("max_results", 10)))
    now = now or datetime.now(timezone.utc)

    response_payload = deepcopy(response_body or {})
    results = list(response_payload.get("results") or [])
    logged_results, results_truncated, results_logged_count, results_full_count = _truncate_frontend_search_results(
        results,
        max_results=max_results,
    )
    response_payload["results"] = logged_results
    _compact_nested_smart_search_payload(response_payload, max_results=max_results)

    request_id = (
        ((response_payload.get("meta") or {}).get("request_id") if isinstance(response_payload.get("meta"), dict) else None)
        or response_payload.get("request_id")
    )

    return {
        "event_type": FRONTEND_SEARCH_LOG_EVENT,
        "timestamp": now.astimezone().isoformat(),
        "request_id": request_id,
        "client_surface": _optional_limited_text(client_surface, 64) or DEFAULT_CLIENT_SURFACE,
        "http": {
            "path": request_path,
            "method": request_method,
            "status_code": status_code,
        },
        "request_args": dict(request_args or {}),
        "response_body": response_payload,
        "results_truncated": results_truncated,
        "results_logged_count": results_logged_count,
        "results_full_count": results_full_count,
        "status": _compute_frontend_search_status(response_payload, status_code),
    }


def _compact_nested_smart_search_payload(
    response_payload: Dict[str, Any],
    *,
    max_results: int,
) -> None:
    smart_payload = response_payload.get("smart_search")
    if not isinstance(smart_payload, dict):
        return
    smart_results = list(smart_payload.get("results") or [])
    logged_results, _, _, _ = _truncate_frontend_search_results(
        smart_results,
        max_results=max_results,
    )
    smart_payload["results"] = logged_results


def _build_frontend_search_summary_params(payload: Dict[str, Any]) -> Dict[str, Any]:
    response_body = payload.get("response_body") or {}
    query_payload = response_body.get("query") or {}
    meta_payload = response_body.get("meta") or {}
    notice_payload = response_body.get("notice") or {}
    compact_payload = {
        "notice": {"type": notice_payload.get("type")} if isinstance(notice_payload, dict) and notice_payload.get("type") else None,
        "meta": {"request_id": meta_payload.get("request_id")} if meta_payload.get("request_id") else None,
        "results_logged_count": payload.get("results_logged_count", 0),
        "results_full_count": payload.get("results_full_count", 0),
    }
    compact_payload = {key: value for key, value in compact_payload.items() if value is not None}

    return {
        "request_id": payload.get("request_id"),
        "client_surface": _optional_limited_text(payload.get("client_surface"), 64) or DEFAULT_CLIENT_SURFACE,
        "query_input": _optional_limited_text(query_payload.get("input")) or "",
        "query_executed": _optional_limited_text(query_payload.get("executed")),
        "search_mode": _optional_limited_text(query_payload.get("mode"), 32) or "smart",
        "query_intent": _optional_limited_text(query_payload.get("intent"), 64),
        "query_route": _optional_limited_text(query_payload.get("route"), 64),
        "corrected_query": _optional_limited_text(query_payload.get("corrected_query")),
        "matched_author": _optional_limited_text(query_payload.get("matched_author")),
        "suggested_author": _optional_limited_text(query_payload.get("suggested_author")),
        "notice_type": _optional_limited_text(notice_payload.get("type"), 64) if isinstance(notice_payload, dict) else None,
        "result_count": _coerce_optional_int(meta_payload.get("count")) or 0,
        "limit_count": _coerce_optional_int(meta_payload.get("limit")),
        "offset_count": _coerce_optional_int(meta_payload.get("offset")),
        "has_more": meta_payload.get("has_more") if isinstance(meta_payload.get("has_more"), bool) else None,
        "elapsed_ms": _coerce_optional_int(meta_payload.get("elapsed_ms")),
        "status": _optional_limited_text(payload.get("status"), 32) or "failed",
        "payload_json": _json_payload(compact_payload),
    }


def insert_frontend_search_request_log(payload: Dict[str, Any]) -> int:
    config = get_frontend_search_logging_config()
    if not (config.get("db_summary") or {}).get("enabled", True):
        return 0

    params = _build_frontend_search_summary_params(payload)
    engine = get_db_engine(db_key="metadata_db")
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO frontend_search_request_logs (
                    request_id,
                    client_surface,
                    query_input,
                    query_executed,
                    search_mode,
                    query_intent,
                    query_route,
                    corrected_query,
                    matched_author,
                    suggested_author,
                    notice_type,
                    result_count,
                    limit_count,
                    offset_count,
                    has_more,
                    elapsed_ms,
                    status,
                    payload_json
                )
                VALUES (
                    :request_id,
                    :client_surface,
                    :query_input,
                    :query_executed,
                    :search_mode,
                    :query_intent,
                    :query_route,
                    :corrected_query,
                    :matched_author,
                    :suggested_author,
                    :notice_type,
                    :result_count,
                    :limit_count,
                    :offset_count,
                    :has_more,
                    :elapsed_ms,
                    :status,
                    CAST(:payload_json AS JSONB)
                )
                ON CONFLICT (request_id) DO NOTHING
                """
            ),
            params,
        )
    return int(result.rowcount or 0)


def emit_frontend_search_log(payload: Dict[str, Any], now: Optional[datetime] = None) -> None:
    config = get_frontend_search_logging_config()
    event_logger.info("%s %s", FRONTEND_SEARCH_LOG_EVENT, _json_payload(payload))

    local_jsonl_config = config.get("local_jsonl") or {}
    if not local_jsonl_config.get("enabled", True):
        return

    now = now or datetime.now(timezone.utc)
    root_dir = Path(str(local_jsonl_config.get("root_dir") or "local_data/search_api_logs"))
    jsonl_path = build_frontend_search_jsonl_path(
        root_dir=root_dir,
        filename_pattern=str(local_jsonl_config.get("filename_pattern") or "{date}_frontend_search_requests.jsonl"),
        partition_by_year=bool(local_jsonl_config.get("partition_by_year", True)),
        now=now,
    )
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(_json_payload(payload) + "\n")


def record_frontend_search_request(
    request_args: Dict[str, Any],
    response_body: Dict[str, Any],
    status_code: int,
    client_surface: str,
    request_path: str = DEFAULT_REQUEST_PATH,
    request_method: str = DEFAULT_REQUEST_METHOD,
) -> None:
    try:
        config = get_frontend_search_logging_config()
        if not config.get("enabled", True):
            return
        payload = build_frontend_search_log_payload(
            request_args=request_args,
            response_body=response_body,
            client_surface=client_surface,
            status_code=status_code,
            request_path=request_path,
            request_method=request_method,
        )
        emit_frontend_search_log(payload)
        insert_frontend_search_request_log(payload)
    except Exception:
        logger.exception("frontend_search_request_logging_failed")
