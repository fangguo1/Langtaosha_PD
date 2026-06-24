from __future__ import annotations

import json

from flask import Flask

from app.routes.paper import register_paper_indexer_api_routes


def _json_success(app):
    def api_success(payload=None, status_code=200):
        return (
            app.response_class(
                json.dumps({"success": True, **(payload or {})}),
                mimetype="application/json",
            ),
            status_code,
        )

    return api_success


def _json_error(app):
    def api_error(message, status_code=500, code="ERR", extra=None):
        return (
            app.response_class(
                json.dumps(
                    {"success": False, "error": message, "error_code": code, **(extra or {})}
                ),
                mimetype="application/json",
            ),
            status_code,
        )

    return api_error


class FakeIndexer:
    def __init__(self):
        self.captured = {}

    def search(self, **kwargs):
        self.captured.update(kwargs)
        return [{"work_id": "W1", "similarity": 0.9}]


def test_api_search_accepts_expanded_sparse_type():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get("/api/search?query=renal&search_type=expanded_sparse")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["search_type"] == "expanded_sparse"
    assert indexer.captured["search_type"] == "expanded_sparse"


def test_api_search_passes_keyword_sources_and_include_coverage():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get(
        "/api/search?query=renal&search_type=dense"
        "&keyword_sources=paper_metadata,mesh&include_coverage=1"
    )

    assert response.status_code == 200
    assert indexer.captured["keyword_sources"] == ["paper_metadata", "mesh"]
    assert indexer.captured["include_coverage"] is True


def test_api_search_defaults_include_coverage_false():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    app.test_client().get("/api/search?query=renal")

    assert indexer.captured["include_coverage"] is False
    assert indexer.captured["include_loose_coverage"] is False
    assert indexer.captured["keyword_sources"] is None


def test_api_search_passes_include_loose_coverage_and_returns_timings():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get(
        "/api/search?query=renal&search_type=dense&include_loose_coverage=1"
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert indexer.captured["include_loose_coverage"] is True
    assert isinstance(indexer.captured["timings_ms"], dict)
    assert "elapsed_ms" in payload
    assert "timings_ms" in payload
