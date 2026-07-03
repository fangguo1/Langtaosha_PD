from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_module():
    module_path = PROJECT_ROOT / "scripts" / "verify_retrieval" / "verify_hybrid_retrieval_api.py"
    spec = importlib.util.spec_from_file_location("verify_hybrid_retrieval_api", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_request_url_uses_hybrid_retrieval_defaults():
    module = load_module()

    args = module.parse_args(["kidney fibrosis"])
    url = module.build_request_url(args)

    assert "query=kidney+fibrosis" in url
    assert "search_type=hybrid_retrieval" in url
    assert "top_k=10" in url


def test_main_prints_summary_from_success_payload():
    module = load_module()
    stdout = io.StringIO()
    stderr = io.StringIO()

    payload = {
        "success": True,
        "query": "kidney fibrosis",
        "search_type": "hybrid_retrieval",
        "top_k": 3,
        "request_id": "req-hybrid-001",
        "elapsed_ms": 45.6,
        "timings_ms": {
            "search": 30.1,
        },
        "results": [
            {
                "work_id": "W1",
                "paper_id": 101,
                "score": 0.8,
                "metadata": {
                    "canonical_title": "Kidney fibrosis study",
                },
            }
        ],
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
        return FakeResponse(payload)

    exit_code = module.main(
        ["kidney fibrosis", "--top-k", "3"],
        stdout=stdout,
        stderr=stderr,
        urlopen=fake_urlopen,
    )

    assert exit_code == 0
    assert stderr.getvalue() == ""
    output = stdout.getvalue()
    assert "hybrid_retrieval" in output
    assert "req-hybrid-001" in output
    assert "Kidney fibrosis study" in output
    assert "client_elapsed_ms" in output
    assert '"work_id": "W1"' in output
    assert "== papers ==" in output
    assert "== response body ==" not in output


def test_main_reports_timeout_helpfully():
    module = load_module()
    stdout = io.StringIO()
    stderr = io.StringIO()

    def fake_urlopen(request, timeout=30):
        raise TimeoutError("timed out")

    exit_code = module.main(
        ["kidney fibrosis"],
        stdout=stdout,
        stderr=stderr,
        urlopen=fake_urlopen,
    )

    assert exit_code == 1
    assert "timed out" in stderr.getvalue()
    assert "--timeout" in stderr.getvalue()
    assert "hybrid_retrieval" in stdout.getvalue()
