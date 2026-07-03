from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_module():
    module_path = PROJECT_ROOT / "scripts" / "verify_retrieval" / "verify_smart_search_api.py"
    spec = importlib.util.spec_from_file_location("verify_smart_search_api", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_request_url_uses_smart_defaults():
    module = load_module()

    args = module.parse_args(["kidney fibrosis"])
    url = module.build_request_url(args)

    assert "query=kidney+fibrosis" in url
    assert "mode=smart" in url
    assert "top_k=10" in url


def test_build_request_url_includes_optional_correction_decision():
    module = load_module()

    args = module.parse_args(
        [
            "kidney fibrosis",
            "--mode",
            "smart",
            "--correction-decision",
            "accept",
        ]
    )
    url = module.build_request_url(args)

    assert "mode=smart" in url
    assert "correction_decision=accept" in url


def test_main_prints_summary_from_success_payload():
    module = load_module()
    stdout = io.StringIO()
    stderr = io.StringIO()

    payload = {
        "success": True,
        "query": {
            "input": "niang",
            "executed": "Niang, X.",
            "mode": "smart",
            "intent": "author_name",
            "route": "author_suggestion",
        },
        "search_query": "Niang, X.",
        "search_mode": "smart",
        "query_understanding": {
            "route": "author_suggestion",
            "intent": "author_name",
            "normalized_query": "niang",
            "suggested_author": "Niang, X.",
            "timings": {
                "author_match_elapsed_ms": 12.5,
                "total_elapsed_ms": 18.0,
            },
        },
        "meta": {
            "count": 0,
            "elapsed_ms": 60,
            "request_id": "req-smart-001",
            "query_understanding_elapsed_ms": 18.0,
            "frontend_logging_elapsed_ms": 123.0,
            "server_elapsed_ms": 190.0,
        },
        "notice": {
            "type": "author_suggestion",
            "message": "未找到 \"niang\" 的高置信作者匹配，是否搜索作者 Niang, X.？",
        },
        "results": [],
        "count": 0,
    }

    class FakeResponse:
        status = 200

        def __init__(self, body: dict):
            self._body = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout=30):
        return FakeResponse(payload)

    exit_code = module.main(
        ["niang", "--mode", "smart"],
        stdout=stdout,
        stderr=stderr,
        urlopen=fake_urlopen,
    )

    assert exit_code == 0
    assert stderr.getvalue() == ""
    output = stdout.getvalue()
    assert "smart search" in output
    assert "author_suggestion" in output
    assert "Niang, X." in output
    assert "req-smart-001" in output
    assert "client_elapsed_ms" in output
    assert "query_understanding_elapsed_ms" in output
    assert "frontend_logging_elapsed_ms" in output
    assert "server_elapsed_ms" in output
    assert "author_match_elapsed_ms" in output
    assert "== response summary ==" in output
    assert "== papers ==" not in output


def test_summarize_query_includes_timing_breakdown():
    module = load_module()

    payload = {
        "success": True,
        "query": {
            "input": "breast cancer risk",
            "executed": None,
        },
        "search_mode": "smart",
        "search_query": None,
        "query_understanding": {
            "route": "vector",
            "intent": "semantic_search",
            "timings": {
                "author_match_elapsed_ms": 8.0,
                "query_correction_elapsed_ms": 22.0,
                "query_expansion_elapsed_ms": 5100.0,
                "total_elapsed_ms": 5150.0,
            },
        },
        "meta": {
            "count": 0,
            "request_id": "req-smart-timing-001",
            "query_understanding_elapsed_ms": 5150.0,
            "frontend_logging_elapsed_ms": 2900.0,
            "server_elapsed_ms": 8100.0,
        },
        "notice": {
            "type": "query_correction",
            "message": "您是想搜索 \"breast cancer st\" 吗？",
        },
        "results": [],
        "count": 0,
    }

    summary = module._summarize_query(payload)

    assert summary["query_understanding_elapsed_ms"] == 5150.0
    assert summary["frontend_logging_elapsed_ms"] == 2900.0
    assert summary["server_elapsed_ms"] == 8100.0
    assert summary["author_match_elapsed_ms"] == 8.0
    assert summary["query_correction_elapsed_ms"] == 22.0
    assert summary["query_expansion_elapsed_ms"] == 5100.0


def test_main_handles_grouped_results_payload():
    module = load_module()
    stdout = io.StringIO()
    stderr = io.StringIO()

    payload = {
        "success": True,
        "query": {
            "input": "kidney fibrosis",
            "executed": "kidney fibrosis",
            "mode": "smart",
            "intent": "semantic_search",
            "route": "vector",
        },
        "search_query": "kidney fibrosis",
        "search_mode": "smart",
        "query_understanding": {
            "route": "vector",
            "intent": "semantic_search",
            "normalized_query": "kidney fibrosis",
        },
        "meta": {
            "count": 2,
            "elapsed_ms": 51,
            "request_id": "req-smart-002",
        },
        "notice": {},
        "results": [
            (
                "langtaosha",
                [
                    {
                        "work_id": "W1",
                        "paper_id": 101,
                        "score": 0.93,
                        "metadata": {
                            "canonical_title": "Langtaosha kidney fibrosis study",
                        },
                    }
                ],
            ),
            (
                "biorxiv",
                [
                    {
                        "work_id": "W2",
                        "paper_id": 202,
                        "score": 0.88,
                        "metadata": {
                            "canonical_title": "Biorxiv kidney fibrosis study",
                        },
                    }
                ],
            ),
        ],
        "count": 2,
    }

    class FakeResponse:
        status = 200

        def __init__(self, body: dict):
            self._body = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout=30):
        return FakeResponse(payload)

    exit_code = module.main(
        ["kidney fibrosis", "--mode", "smart"],
        stdout=stdout,
        stderr=stderr,
        urlopen=fake_urlopen,
    )

    assert exit_code == 0
    assert stderr.getvalue() == ""
    output = stdout.getvalue()
    assert "req-smart-002" in output
    assert "Langtaosha kidney fibrosis study" in output
    assert "Biorxiv kidney fibrosis study" in output
    assert '"work_id": "W1"' in output
    assert '"work_id": "W2"' in output


def test_main_reports_timeout_helpfully():
    module = load_module()
    stdout = io.StringIO()
    stderr = io.StringIO()

    def fake_urlopen(request, timeout=30):
        raise TimeoutError("timed out")

    exit_code = module.main(
        ["niang"],
        stdout=stdout,
        stderr=stderr,
        urlopen=fake_urlopen,
    )

    assert exit_code == 1
    assert "timed out" in stderr.getvalue()
    assert "--timeout" in stderr.getvalue()
    assert "smart search" in stdout.getvalue()
