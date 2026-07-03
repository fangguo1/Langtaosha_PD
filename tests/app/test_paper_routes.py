from __future__ import annotations

import json

import pytest
from flask import Flask

from app.routes.paper import register_paper_indexer_api_routes
from src.docset_hub.indexing.query_semantic_plan import (
    QuerySemanticPlan,
    SemanticSpanGroup,
    SemanticTerm,
    SemanticTermBucket,
)


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


def _fake_plan():
    return QuerySemanticPlan(
        original_query="renal",
        normalized_query="renal",
        spans=[
            SemanticSpanGroup(
                span_id="s1",
                surface_text="renal",
                normalized_text="renal",
                start=0,
                end=5,
                canonical_text="Renal",
                own_terms=SemanticTermBucket(
                    tier1=[SemanticTerm(text="renal", match_mode="exact")],
                    tier2=[],
                ),
                children=[],
                evidence=[],
            )
        ],
    )


@pytest.fixture(autouse=True)
def stub_route_coverage_annotators(monkeypatch):
    def annotate_strict(results, *, plan):
        for item in results:
            item["coverage_ratio"] = 0.5
            item["coverage"] = {"matched_span_count": 1, "total_span_count": 2}
            item["matched_spans"] = []

    def annotate_loose(results, *, plan):
        for item in results:
            item["loose_coverage_ratio"] = 0.3
            item["loose_coverage"] = {"matched_span_count": 1, "total_span_count": 2}
            item["loose_matched_spans"] = []

    monkeypatch.setattr("app.routes.paper.annotate_strict_coverage", annotate_strict)
    monkeypatch.setattr("app.routes.paper.annotate_loose_coverage", annotate_loose)


class FakeIndexer:
    def __init__(self):
        self.captured = {}
        self.hybrid_captured = {}
        self.retrieve_captured = {}
        self.default_sources = ["langtaosha"]

    def search(self, **kwargs):
        self.captured.update(kwargs)
        return [
            {
                "work_id": "W1",
                "similarity": 0.9,
                "metadata": {
                    "canonical_title": "Kidney adhesion paper",
                    "canonical_abstract": "Renal epithelial adhesion study.",
                    "paper_keywords": [{"keyword": "kidney"}],
                },
            }
        ]

    def hybrid_retrieval_search(self, **kwargs):
        self.hybrid_captured.update(kwargs)
        return [{"work_id": "W1", "similarity": 0.8}]

    def build_query_semantic_plan(self, query, source_list, keyword_sources=None):
        return _fake_plan()

    def retrieve_papers_by_time_interval(self, date_from, date_to, source_name="langtaosha"):
        self.retrieve_captured = {
            "date_from": date_from,
            "date_to": date_to,
            "source_name": source_name,
        }
        return [
            {"paper_id": 1, "work_id": "W1"},
            {"paper_id": 2, "work_id": "W2"},
        ]


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
        "&keyword_sources=paper_metadata,mesh&include_coverage=1&hydrate=1"
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert indexer.captured["keyword_sources"] == ["paper_metadata", "mesh"]
    assert "include_coverage" not in indexer.captured
    assert "coverage_ratio" in payload["results"][0]


def test_api_search_defaults_include_coverage_false():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get("/api/search?query=renal&hydrate=1")
    payload = response.get_json()

    assert response.status_code == 200
    assert indexer.captured["keyword_sources"] is None
    assert "coverage_ratio" not in payload["results"][0]
    assert "loose_coverage_ratio" not in payload["results"][0]


def test_api_search_passes_include_loose_coverage_and_returns_timings():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get(
        "/api/search?query=renal&search_type=dense&include_loose_coverage=1&hydrate=1"
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert "include_loose_coverage" not in indexer.captured
    assert "loose_coverage_ratio" in payload["results"][0]
    assert isinstance(payload["timings_ms"], dict)
    assert "elapsed_ms" in payload
    assert "timings_ms" in payload
    assert "search" in payload["timings_ms"]
    assert "loose_coverage" in payload["timings_ms"]


def test_api_search_routes_hybrid_retrieval_to_hybrid_method():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get(
        "/api/search?query=renal&search_type=hybrid_retrieval&top_k=5"
    )

    assert response.status_code == 200
    assert indexer.captured == {}
    assert indexer.hybrid_captured["top_k"] == 5


def test_api_search_routes_hybrid_alias_to_hybrid_method():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get("/api/search?query=renal&search_type=hybrid&top_k=3")

    assert response.status_code == 200
    assert indexer.captured == {}
    assert indexer.hybrid_captured["top_k"] == 3


def test_api_search_accepts_hybrid_retreival_via_indexer_search():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().get(
        "/api/search?query=renal&search_type=hybrid_retreival&top_k=4"
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["search_type"] == "hybrid_retreival"
    assert indexer.hybrid_captured == {}
    assert indexer.captured["search_type"] == "hybrid_retreival"
    assert indexer.captured["top_k"] == 4


def test_register_paper_routes_can_skip_health_registration():
    app = Flask(__name__)
    indexer = FakeIndexer()

    @app.route("/api/health", methods=["GET"])
    def custom_health():
        return _json_success(app)({"status": "ok", "service": "custom"})

    register_paper_indexer_api_routes(
        app,
        indexer,
        _json_success(app),
        _json_error(app),
        include_health_route=False,
    )

    response = app.test_client().get("/api/health")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["service"] == "custom"


def test_api_retrieve_papers_by_time_interval_accepts_json_body():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().post(
        "/api/retrieve_papers_by_time_interval",
        json={"date_from": "2026-04-01", "date_to": "2026-04-10"},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["date_from"] == "2026-04-01"
    assert payload["date_to"] == "2026-04-10"
    assert payload["papers"] == [
        {"paper_id": 1, "work_id": "W1"},
        {"paper_id": 2, "work_id": "W2"},
    ]
    assert indexer.retrieve_captured == {
        "date_from": "2026-04-01",
        "date_to": "2026-04-10",
        "source_name": "langtaosha",
    }


def test_api_retrieve_papers_by_time_interval_requires_both_dates():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().post(
        "/api/retrieve_papers_by_time_interval",
        json={"date_from": "2026-04-01"},
    )
    payload = response.get_json()

    assert response.status_code == 400
    assert payload["success"] is False
    assert payload["error_code"] == "INVALID_REQUEST"


def test_api_retrieve_papers_by_time_interval_accepts_explicit_source_name():
    app = Flask(__name__)
    indexer = FakeIndexer()
    register_paper_indexer_api_routes(app, indexer, _json_success(app), _json_error(app))

    response = app.test_client().post(
        "/api/retrieve_papers_by_time_interval",
        json={
            "date_from": "2026-04-01",
            "date_to": "2026-04-10",
            "source_name": "biorxiv_history",
        },
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert indexer.retrieve_captured == {
        "date_from": "2026-04-01",
        "date_to": "2026-04-10",
        "source_name": "biorxiv_history",
    }
