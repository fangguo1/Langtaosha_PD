from __future__ import annotations

import json
from types import SimpleNamespace

from flask import Flask

from app.pages.span_matcher_page import (
    register_span_matcher_api_routes,
    register_span_matcher_page_routes,
    run_span_matcher_test,
)


def test_span_matcher_api_returns_semantic_plan(monkeypatch):
    app = Flask(__name__)

    monkeypatch.setattr(
        "app.pages.span_matcher_page.run_span_matcher_test",
        lambda query, paper_indexer: {
            "success": True,
            "query": query,
            "normalized_query": query,
            "count": 1,
            "selected_candidates": [
                {
                    "text": "adhesion protein",
                    "normalized_text": "adhesion protein",
                    "kind": "connector_split",
                    "start": 0,
                    "end": 16,
                    "matches": [],
                }
            ],
            "semantic_plan": {
                "original_query": query,
                "normalized_query": query,
                "spans": [
                    {
                        "span_id": "s1",
                        "surface_text": "adhesion protein",
                        "normalized_text": "adhesion protein",
                        "start": 0,
                        "end": 16,
                        "canonical_text": "Adhesion protein",
                        "own_terms": {
                            "tier1": [{"text": "adhesion protein", "match_mode": "exact"}],
                            "tier2": [{"text": "cell adhesion molecule", "match_mode": "exact"}],
                        },
                        "children": [
                            {
                                "span_id": "s1.1",
                                "surface_text": "adhesion",
                                "normalized_text": "adhesion",
                                "start": 0,
                                "end": 8,
                                "canonical_text": "Adhesion",
                                "own_terms": {
                                    "tier1": [{"text": "adhesion", "match_mode": "exact"}],
                                    "tier2": [],
                                },
                            }
                        ],
                    }
                ],
            },
            "elapsed_ms": 42.5,
            "timings_ms": {
                "normalize": 1.0,
                "extract": 2.0,
                "match": 30.0,
                "select": 4.0,
                "build_plan": 5.5,
            },
        },
    )

    register_span_matcher_api_routes(
        app,
        lambda payload=None, status_code=200: (
            app.response_class(
                json.dumps({"success": True, **(payload or {})}),
                mimetype="application/json",
            ),
            status_code,
        ),
        lambda message, status_code=500, code="ERR", extra=None: (
            app.response_class(
                json.dumps({"success": False, "error": message, **(extra or {})}),
                mimetype="application/json",
            ),
            status_code,
        ),
        paper_indexer=object(),
    )

    response = app.test_client().get("/api/span-matcher?query=adhesion protein in kidney")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["semantic_plan"]["spans"][0]["span_id"] == "s1"
    assert payload["semantic_plan"]["spans"][0]["own_terms"]["tier1"] == [
        {"text": "adhesion protein", "match_mode": "exact"}
    ]
    assert payload["semantic_plan"]["spans"][0]["children"][0]["span_id"] == "s1.1"
    assert payload["elapsed_ms"] == 42.5
    assert payload["timings_ms"]["match"] == 30.0


def test_span_matcher_page_renders_semantic_span_ui():
    app = Flask(__name__, template_folder="../../templates")
    register_span_matcher_page_routes(app)

    response = app.test_client().get("/span-matcher")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Semantic spans" in html
    assert "响应时间" in html


def test_run_span_matcher_test_uses_ontology_plus_keyword_pipeline(monkeypatch):
    captured = {}

    class FakePipeline:
        def run(self, query):
            captured["query"] = query
            return SimpleNamespace(
                query=query,
                normalized_query=query,
                selected_concepts=[],
                timings_ms={
                    "normalize": 1.0,
                    "extract": 2.0,
                    "match": 3.0,
                    "select": 4.0,
                    "build_plan": 5.0,
                },
                semantic_plan=SimpleNamespace(
                    original_query=query,
                    normalized_query=query,
                    spans=[
                        SimpleNamespace(
                            span_id="s1",
                            surface_text="adhesion protein",
                            normalized_text="adhesion protein",
                            start=0,
                            end=16,
                            canonical_text="Adhesion protein",
                            own_terms=SimpleNamespace(
                                tier1=[SimpleNamespace(text="adhesion protein", match_mode="exact")],
                                tier2=[SimpleNamespace(text="cell adhesion molecule", match_mode="exact")],
                            ),
                            children=[
                                SimpleNamespace(
                                    span_id="s1.1",
                                    surface_text="adhesion",
                                    normalized_text="adhesion",
                                    start=0,
                                    end=8,
                                    canonical_text="Adhesion",
                                    own_terms=SimpleNamespace(
                                        tier1=[SimpleNamespace(text="adhesion", match_mode="exact")],
                                        tier2=[],
                                    ),
                                )
                            ],
                        )
                    ],
                ),
            )

    def fake_from_profile(*, profile, metadata_db):
        captured["profile_name"] = profile.name
        captured["enable_ontology"] = profile.enable_ontology
        captured["enable_keyword"] = profile.enable_keyword
        captured["metadata_db"] = metadata_db
        return FakePipeline()

    monkeypatch.setattr(
        "app.pages.span_matcher_page.SpanMatcherPipeline.from_profile",
        fake_from_profile,
    )

    result = run_span_matcher_test(
        "adhesion protein in kidney",
        paper_indexer=SimpleNamespace(metadata_db=object(), default_sources=["langtaosha"]),
    )

    assert captured["query"] == "adhesion protein in kidney"
    assert captured["profile_name"] == "ontology_plus_keyword"
    assert captured["enable_ontology"] is True
    assert captured["enable_keyword"] is True
    assert result["semantic_plan"]["spans"][0]["own_terms"]["tier1"] == [
        {"text": "adhesion protein", "match_mode": "exact"}
    ]
    assert result["semantic_plan"]["spans"][0]["children"][0]["surface_text"] == "adhesion"
    assert result["elapsed_ms"] >= 0.0
    assert set(result["timings_ms"]) == {"normalize", "extract", "match", "select", "build_plan"}
