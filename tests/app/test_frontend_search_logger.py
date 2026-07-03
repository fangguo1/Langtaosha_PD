from datetime import datetime, timezone
from pathlib import Path
import json
import logging
from importlib import reload

from src.docset_hub.logging.frontend_search_logger import build_frontend_search_jsonl_path
import src.docset_hub.logging.frontend_search_logger as frontend_search_logger
from config.config_loader import _reset_config, get_frontend_search_logging_config, init_config


def test_frontend_search_event_logger_is_silent_by_default():
    assert frontend_search_logger.event_logger.propagate is False
    assert any(isinstance(handler, logging.NullHandler) for handler in frontend_search_logger.event_logger.handlers)


def test_build_frontend_search_jsonl_path_partitions_by_year():
    path = build_frontend_search_jsonl_path(
        root_dir=Path("/tmp/search_api_logs"),
        filename_pattern="{date}_frontend_search_requests.jsonl",
        partition_by_year=True,
        now=datetime(2026, 6, 9, 10, 0, tzinfo=timezone.utc),
    )

    assert path == Path("/tmp/search_api_logs/2026/2026-06-09_frontend_search_requests.jsonl")


def test_build_frontend_search_jsonl_path_without_year_partition():
    path = build_frontend_search_jsonl_path(
        root_dir=Path("/tmp/search_api_logs"),
        filename_pattern="{date}_frontend_search_requests.jsonl",
        partition_by_year=False,
        now=datetime(2026, 6, 9, 10, 0, tzinfo=timezone.utc),
    )

    assert path == Path("/tmp/search_api_logs/2026-06-09_frontend_search_requests.jsonl")


def test_build_frontend_search_log_payload_truncates_results_and_preserves_response_shape(monkeypatch):
    monkeypatch.setattr(
        frontend_search_logger,
        "get_frontend_search_logging_config",
        lambda: {
            "enabled": True,
            "local_jsonl": {"enabled": True, "root_dir": "local_data/search_api_logs", "partition_by_year": True, "filename_pattern": "{date}_frontend_search_requests.jsonl"},
            "db_summary": {"enabled": True},
            "response_log": {"max_results": 10},
        },
    )
    payload = frontend_search_logger.build_frontend_search_log_payload(
        request_args={
            "query": "Nav1.7",
            "mode": "smart",
            "limit": 12,
            "offset": 0,
            "source_list": "langtaosha",
            "top_k": None,
        },
        response_body={
            "success": True,
            "query": {"input": "Nav1.7", "executed": "Nav1.7", "mode": "smart", "route": "vector"},
            "meta": {
                "count": 12,
                "limit": 12,
                "offset": 0,
                "has_more": False,
                "elapsed_ms": 12,
                "request_id": "req-001",
            },
            "notice": None,
            "results": [{"work_id": f"W{i}", "rank": i} for i in range(1, 13)],
        },
        client_surface="search_page",
        status_code=200,
    )

    assert payload["request_id"] == "req-001"
    assert payload["client_surface"] == "search_page"
    assert payload["response_body"]["meta"]["request_id"] == "req-001"
    assert len(payload["response_body"]["results"]) == 10
    assert payload["results_truncated"] is True
    assert payload["results_logged_count"] == 10
    assert payload["results_full_count"] == 12


def test_build_frontend_search_log_payload_keeps_only_work_id_and_title_for_grouped_results(monkeypatch):
    monkeypatch.setattr(
        frontend_search_logger,
        "get_frontend_search_logging_config",
        lambda: {
            "enabled": True,
            "local_jsonl": {"enabled": True, "root_dir": "local_data/search_api_logs", "partition_by_year": True, "filename_pattern": "{date}_frontend_search_requests.jsonl"},
            "db_summary": {"enabled": True},
            "response_log": {"max_results": 10},
        },
    )

    payload = frontend_search_logger.build_frontend_search_log_payload(
        request_args={
            "query": "machine learning",
            "mode": "smart",
            "source_list": None,
            "top_k": 10,
        },
        response_body={
            "success": True,
            "query": {"input": "machine learning", "executed": "machine learning", "mode": "smart", "route": "vector"},
            "meta": {
                "count": 4,
                "elapsed_ms": 12,
                "request_id": "req-grouped-001",
            },
            "smart_search": {
                "success": True,
                "query": "machine learning",
                "search_query": "machine learning",
                "query_understanding": {"route": "vector"},
                "results": [
                    (
                        "langtaosha",
                        [
                            {"work_id": "W1", "title": "Paper 1", "abstract": "long text", "authors": "A"},
                            {"work_id": "W2", "title": "Paper 2", "abstract": "long text", "authors": "B"},
                        ],
                    ),
                    (
                        "biorxiv",
                        [
                            {"work_id": "W3", "title": "Paper 3", "abstract": "long text", "authors": "C"},
                            {"work_id": "W4", "title": "Paper 4", "abstract": "long text", "authors": "D"},
                        ],
                    ),
                ],
            },
            "results": [
                (
                    "langtaosha",
                    [
                        {"work_id": "W1", "title": "Paper 1", "abstract": "long text", "authors": "A"},
                        {"work_id": "W2", "title": "Paper 2", "abstract": "long text", "authors": "B"},
                    ],
                ),
                (
                    "biorxiv",
                    [
                        {"work_id": "W3", "title": "Paper 3", "abstract": "long text", "authors": "C"},
                        {"work_id": "W4", "title": "Paper 4", "abstract": "long text", "authors": "D"},
                    ],
                ),
            ],
        },
        client_surface="search_page",
        status_code=200,
    )

    assert payload["results_truncated"] is False
    assert payload["results_logged_count"] == 2
    assert payload["results_full_count"] == 2
    assert payload["response_body"]["results"] == [
        (
            "langtaosha",
            [
                {"work_id": "W1", "title": "Paper 1"},
                {"work_id": "W2", "title": "Paper 2"},
            ],
        ),
        (
            "biorxiv",
            [
                {"work_id": "W3", "title": "Paper 3"},
                {"work_id": "W4", "title": "Paper 4"},
            ],
        ),
    ]
    assert payload["response_body"]["smart_search"]["results"] == [
        (
            "langtaosha",
            [
                {"work_id": "W1", "title": "Paper 1"},
                {"work_id": "W2", "title": "Paper 2"},
            ],
        ),
        (
            "biorxiv",
            [
                {"work_id": "W3", "title": "Paper 3"},
                {"work_id": "W4", "title": "Paper 4"},
            ],
        ),
    ]


def test_insert_frontend_search_request_log_writes_summary_row(monkeypatch):
    captured = {}

    class FakeResult:
        rowcount = 1

    class FakeConn:
        def execute(self, stmt, params):
            captured["params"] = params
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def begin(self):
            return FakeConn()

    monkeypatch.setattr(
        frontend_search_logger,
        "get_frontend_search_logging_config",
        lambda: {
            "enabled": True,
            "local_jsonl": {"enabled": True, "root_dir": "local_data/search_api_logs", "partition_by_year": True, "filename_pattern": "{date}_frontend_search_requests.jsonl"},
            "db_summary": {"enabled": True},
            "response_log": {"max_results": 10},
        },
    )
    monkeypatch.setattr(frontend_search_logger, "get_db_engine", lambda db_key="metadata_db": FakeEngine())

    inserted = frontend_search_logger.insert_frontend_search_request_log(
        {
            "request_id": "req-001",
            "client_surface": "search_page",
            "response_body": {
                "query": {
                    "input": "Nav1.7",
                    "executed": "Nav1.7",
                    "mode": "smart",
                    "intent": "semantic_search",
                    "route": "vector",
                    "corrected_query": None,
                    "matched_author": None,
                    "suggested_author": None,
                },
                "meta": {
                    "count": 12,
                    "limit": 12,
                    "offset": 0,
                    "has_more": False,
                    "elapsed_ms": 18,
                    "request_id": "req-001",
                },
                "notice": {"type": "query_correction"},
            },
            "results_logged_count": 10,
            "results_full_count": 12,
            "status": "ok",
        }
    )

    assert inserted == 1
    assert captured["params"]["request_id"] == "req-001"
    assert captured["params"]["client_surface"] == "search_page"
    assert captured["params"]["query_input"] == "Nav1.7"
    assert captured["params"]["query_route"] == "vector"
    assert captured["params"]["notice_type"] == "query_correction"
    assert captured["params"]["result_count"] == 12
    assert captured["params"]["status"] == "ok"


def test_emit_frontend_search_log_appends_jsonl_file(monkeypatch, tmp_path):
    lines = []

    class FakeLogger:
        def info(self, message, *args):
            lines.append((message, args))

    log_path = tmp_path / "search_api_logs" / "2026" / "2026-06-09_frontend_search_requests.jsonl"
    monkeypatch.setattr(frontend_search_logger, "event_logger", FakeLogger())
    monkeypatch.setattr(
        frontend_search_logger,
        "get_frontend_search_logging_config",
        lambda: {
            "enabled": True,
            "local_jsonl": {
                "enabled": True,
                "root_dir": str(tmp_path / "search_api_logs"),
                "partition_by_year": True,
                "filename_pattern": "{date}_frontend_search_requests.jsonl",
            },
            "db_summary": {"enabled": True},
            "response_log": {"max_results": 10},
        },
    )

    payload = {
        "event_type": "frontend_scholar_search",
        "request_id": "req-emit-001",
        "status": "ok",
        "response_body": {"meta": {"request_id": "req-emit-001"}},
    }

    frontend_search_logger.emit_frontend_search_log(
        payload,
        now=datetime(2026, 6, 9, 10, 0, tzinfo=timezone.utc),
    )

    assert lines[0][0] == "%s %s"
    assert lines[0][1][0] == "frontend_scholar_search"
    assert log_path.exists() is True
    file_lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(file_lines) == 1
    assert json.loads(file_lines[0])["request_id"] == "req-emit-001"


def test_logger_uses_same_config_module_as_main_runtime():
    _reset_config()
    try:
        reload(frontend_search_logger)
        init_config(Path("src/config/config_tecent_backend_server_example.yaml"))

        config = get_frontend_search_logging_config()
        logger_config = frontend_search_logger.get_frontend_search_logging_config()

        assert config["enabled"] is True
        assert logger_config["enabled"] == config["enabled"]
        assert logger_config["local_jsonl"]["root_dir"] == config["local_jsonl"]["root_dir"]
    finally:
        _reset_config()
