from __future__ import annotations

import json
from types import SimpleNamespace

from flask import Flask

from app.pages.expanded_compare_page import (
    register_expanded_compare_api_routes,
    register_expanded_compare_page_routes,
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


def test_expanded_compare_page_renders():
    app = Flask(__name__, template_folder="../../templates")
    register_expanded_compare_page_routes(app)

    response = app.test_client().get("/expanded-compare?q=renal%20adhesion")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Expanded Sparse Compare" in html
    assert "renal adhesion" in html


def test_expanded_compare_api_returns_sparse_expanded_rows_and_terms(monkeypatch):
    app = Flask(__name__)
    captured = {}

    plan = SimpleNamespace(
        original_query="renal adhesion",
        normalized_query="renal adhesion",
        spans=[
            SimpleNamespace(
                span_id="s1",
                surface_text="renal",
                normalized_text="renal",
                start=0,
                end=5,
                canonical_text="Renal",
                own_terms=SimpleNamespace(
                    tier1=[SimpleNamespace(text="renal", match_mode="exact")],
                    tier2=[SimpleNamespace(text="kidney", match_mode="exact")],
                ),
                children=[],
            )
        ],
    )

    class FakeMetadataDB:
        def read_paper_by_work_id(self, work_id):
            return {
                "paper_id": 10,
                "work_id": work_id,
                "canonical_title": "Kidney adhesion paper",
                "canonical_abstract": "Renal epithelial adhesion study.",
                "paper_keywords": [{"keyword": "kidney"}, {"keyword": "adhesion"}],
            }

    class FakeIndexer:
        default_sources = ["langtaosha"]
        metadata_db = FakeMetadataDB()

        def search(self, **kwargs):
            if kwargs["search_type"] == "dense":
                return [
                    {
                        "paper_id": 11,
                        "work_id": "W11",
                        "similarity": 0.77,
                        "metadata": {
                            "canonical_title": "Renal molecule dense paper",
                            "canonical_abstract": "Kidney molecule study.",
                            "paper_keywords": [{"keyword": "kidney molecule"}],
                        },
                    }
                ]
            assert kwargs["search_type"] == "sparse"
            return [
                {
                    "paper_id": 10,
                    "work_id": "W10",
                    "similarity": 0.42,
                    "metadata": {
                        "canonical_title": "Kidney adhesion paper",
                        "canonical_abstract": "Renal epithelial adhesion study.",
                        "paper_keywords": [{"keyword": "kidney"}],
                    },
                }
            ]

        def _build_query_semantic_plan(self, **kwargs):
            captured.update(kwargs)
            return plan

    monkeypatch.setattr(
        "app.pages.expanded_compare_page.build_expanded_sparse_query_rows",
        lambda received_plan: [
            {
                "group_id": 1,
                "span_id": "s1",
                "canonical_text": "Renal",
                "span_scope": "parent",
                "child_span_id": None,
                "term_tier": "tier2",
                "match_mode": "exact",
                "term": "kidney",
            }
        ],
    )
    monkeypatch.setattr(
        "app.pages.expanded_compare_page.match_papers_by_expanded_sparse_plan",
        lambda **kwargs: [
            SimpleNamespace(
                paper_id=10,
                work_id="W10",
                matched_span_count=1,
                total_span_count=1,
                coverage_ratio=1.0,
                matched_spans=[
                    {
                        "span_id": "s1",
                        "canonical_text": "Renal",
                        "matched_terms": ["kidney"],
                        "matched_fields": ["paper_keywords"],
                        "matched_scopes": ["parent"],
                        "own_term_matched": True,
                        "matched_child_count": 0,
                        "total_child_count": 0,
                        "span_score": 1.0,
                    }
                ],
            )
        ],
    )

    register_expanded_compare_api_routes(
        app,
        FakeIndexer(),
        _json_success(app),
        _json_error(app),
    )

    response = app.test_client().get("/api/expanded-compare?query=renal%20adhesion")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert captured["profile_name"] == "ontology_plus_keyword"
    assert payload["expanded_query_rows"][0]["term"] == "kidney"
    assert payload["highlight_terms"] == [{"text": "kidney", "match_mode": "exact"}]
    assert payload["results"]["dense"][0]["title"] == "Renal molecule dense paper"
    assert payload["results"]["dense"][0]["coverage_ratio"] == 1.0
    assert payload["results"]["dense"][0]["coverage"]["matched_span_count"] == 1
    assert payload["results"]["sparse"][0]["title"] == "Kidney adhesion paper"
    assert payload["results"]["sparse"][0]["coverage_ratio"] == 1.0
    assert payload["results"]["sparse"][0]["coverage"]["matched_span_count"] == 1
    assert payload["results"]["expanded_sparse"][0]["coverage_ratio"] == 1.0
    assert payload["results"]["expanded_sparse"][0]["coverage"]["matched_span_count"] == 1
    assert payload["results"]["expanded_sparse"][0]["matched_spans"][0]["matched_terms"] == ["kidney"]


def test_expanded_compare_api_keeps_expanded_results_when_sparse_fails(monkeypatch):
    app = Flask(__name__)
    plan = SimpleNamespace(
        original_query="synaptic plasticity and memory",
        normalized_query="synaptic plasticity and memory",
        spans=[],
    )

    class FakeMetadataDB:
        def read_paper_by_work_id(self, work_id):
            return {"work_id": work_id, "canonical_title": "Memory paper"}

    class FakeIndexer:
        default_sources = ["langtaosha"]
        metadata_db = FakeMetadataDB()

        def search(self, **kwargs):
            if kwargs["search_type"] == "dense":
                return [
                    {
                        "paper_id": 21,
                        "work_id": "W21",
                        "similarity": 0.81,
                        "metadata": {
                            "canonical_title": "Dense memory paper",
                            "canonical_abstract": "Synaptic memory mechanisms.",
                            "paper_keywords": ["memory"],
                        },
                    }
                ]
            raise RuntimeError("BM25 service unavailable")

        def _build_query_semantic_plan(self, **kwargs):
            return plan

    monkeypatch.setattr(
        "app.pages.expanded_compare_page.build_expanded_sparse_query_rows",
        lambda received_plan: [{"term": "memory", "match_mode": "exact"}],
    )
    monkeypatch.setattr(
        "app.pages.expanded_compare_page.match_papers_by_expanded_sparse_plan",
        lambda **kwargs: [
            SimpleNamespace(
                paper_id=20,
                work_id="W20",
                matched_span_count=1,
                total_span_count=1,
                coverage_ratio=1.0,
                matched_spans=[],
            )
        ],
    )

    register_expanded_compare_api_routes(
        app,
        FakeIndexer(),
        _json_success(app),
        _json_error(app),
    )

    response = app.test_client().get("/api/expanded-compare?query=synaptic%20plasticity%20and%20memory")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["errors"]["sparse"] == "BM25 service unavailable"
    assert payload["results"]["dense"][0]["title"] == "Dense memory paper"
    assert payload["results"]["sparse"] == []
    assert payload["results"]["expanded_sparse"][0]["title"] == "Memory paper"
