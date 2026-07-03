from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_module():
    module_path = PROJECT_ROOT / "scripts" / "verify_retrieval" / "verify_search_api.py"
    spec = importlib.util.spec_from_file_location("verify_search_api", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_request_url_uses_requested_search_type():
    module = load_module()

    args = module.parse_args(["kidney fibrosis", "--search-types", "sparse"])
    url = module.build_request_url(args, "sparse")

    assert "query=kidney+fibrosis" in url
    assert "search_type=sparse" in url
    assert "top_k=10" in url


def test_parse_args_accepts_hybrid_retrieval_alias():
    module = load_module()

    args = module.parse_args(["kidney fibrosis", "--search-types", "hybrid_retrieval"])

    assert args.search_types == ["hybrid_retrieval"]


def test_main_runs_default_search_types_and_prints_results():
    module = load_module()
    stdout = io.StringIO()
    stderr = io.StringIO()
    requested_urls: list[str] = []

    payloads = {
        "dense": {
            "success": True,
            "query": "kidney fibrosis",
            "search_type": "dense",
            "top_k": 3,
            "request_id": "req-dense-001",
            "elapsed_ms": 11.1,
            "timings_ms": {"search": 8.2},
            "results": [
                {
                    "work_id": "D1",
                    "paper_id": 101,
                    "score": 0.91,
                    "metadata": {"canonical_title": "Dense kidney fibrosis study"},
                }
            ],
        },
        "sparse": {
            "success": True,
            "query": "kidney fibrosis",
            "search_type": "sparse",
            "top_k": 3,
            "request_id": "req-sparse-001",
            "elapsed_ms": 12.2,
            "timings_ms": {"search": 9.1},
            "results": [
                {
                    "work_id": "S1",
                    "paper_id": 102,
                    "score": 0.82,
                    "metadata": {"canonical_title": "Sparse kidney fibrosis study"},
                }
            ],
        },
        "hybrid_retrieval": {
            "success": True,
            "query": "kidney fibrosis",
            "search_type": "hybrid_retrieval",
            "top_k": 3,
            "request_id": "req-hybrid-001",
            "elapsed_ms": 13.3,
            "timings_ms": {"search": 10.4},
            "results": [
                {
                    "work_id": "H1",
                    "paper_id": 103,
                    "score": 0.77,
                    "metadata": {"canonical_title": "Hybrid kidney fibrosis study"},
                }
            ],
        },
    }

    class FakeResponse:
        status = 200

        def __init__(self, body: dict):
            self._body = json.dumps(body).encode("utf-8")
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout=30):
        url = request.full_url
        requested_urls.append(url)
        if "search_type=dense" in url:
            return FakeResponse(payloads["dense"])
        if "search_type=sparse" in url:
            return FakeResponse(payloads["sparse"])
        if "search_type=hybrid_retrieval" in url:
            return FakeResponse(payloads["hybrid_retrieval"])
        raise AssertionError(f"unexpected url: {url}")

    exit_code = module.main(
        ["kidney fibrosis", "--top-k", "3"],
        stdout=stdout,
        stderr=stderr,
        urlopen=fake_urlopen,
    )

    assert exit_code == 0
    assert stderr.getvalue() == ""
    assert len(requested_urls) == 3
    assert "search_type=dense" in requested_urls[0]
    assert "search_type=sparse" in requested_urls[1]
    assert "search_type=hybrid_retrieval" in requested_urls[2]

    output = stdout.getvalue()
    assert "== dense request ==" in output
    assert "== sparse request ==" in output
    assert "== hybrid_retrieval request ==" in output
    assert "Dense kidney fibrosis study" in output
    assert "Sparse kidney fibrosis study" in output
    assert "Hybrid kidney fibrosis study" in output
    assert output.count("== response summary ==") == 3
    assert output.count("== papers ==") == 3


def test_main_returns_nonzero_when_any_request_fails():
    module = load_module()
    stdout = io.StringIO()
    stderr = io.StringIO()

    class FakeResponse:
        status = 200

        def __init__(self, body: dict):
            self._body = json.dumps(body).encode("utf-8")
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout=30):
        url = request.full_url
        if "search_type=sparse" in url:
            return FakeResponse({"success": False, "search_type": "sparse", "results": []})
        return FakeResponse({"success": True, "results": []})

    exit_code = module.main(
        ["kidney fibrosis"],
        stdout=stdout,
        stderr=stderr,
        urlopen=fake_urlopen,
    )

    assert exit_code == 1
    assert "success=false" in stderr.getvalue()
