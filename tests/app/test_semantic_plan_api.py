from __future__ import annotations

import json
from types import SimpleNamespace

from flask import Flask

from app.dev.semantic_plan_api import register_semantic_plan_api_routes


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


def _make_plan():
    return SimpleNamespace(
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


def test_semantic_plan_api_returns_plan_rows_and_highlight_terms(monkeypatch):
    app = Flask(__name__)
    captured = {}
    plan = _make_plan()

    class FakeIndexer:
        default_sources = ["langtaosha"]

        def build_query_semantic_plan(self, **kwargs):
            captured.update(kwargs)
            return plan

    monkeypatch.setattr(
        "app.dev.semantic_plan_api.build_expanded_sparse_query_rows",
        lambda received_plan: [
            {"term": "kidney", "match_mode": "exact"},
            {"term": "kidney", "match_mode": "exact"},
            {"term": "renal", "match_mode": "prefix"},
        ],
    )

    register_semantic_plan_api_routes(app, FakeIndexer(), _json_success(app), _json_error(app))

    response = app.test_client().get("/api/semantic-plan?query=renal%20adhesion")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert captured["profile_name"] == "ontology_plus_keyword"
    assert captured["source_list"] == ["langtaosha"]
    assert payload["semantic_plan"]["spans"][0]["span_id"] == "s1"
    assert len(payload["expanded_query_rows"]) == 3
    assert payload["highlight_terms"] == [
        {"text": "kidney", "match_mode": "exact"},
        {"text": "renal", "match_mode": "prefix"},
    ]


def test_semantic_plan_api_rejects_empty_query():
    app = Flask(__name__)

    class FakeIndexer:
        default_sources = ["langtaosha"]

    register_semantic_plan_api_routes(app, FakeIndexer(), _json_success(app), _json_error(app))

    response = app.test_client().get("/api/semantic-plan?query=")

    assert response.status_code == 400


def test_semantic_plan_api_returns_empty_payload_when_plan_is_none():
    app = Flask(__name__)

    class FakeIndexer:
        default_sources = ["langtaosha"]

        def build_query_semantic_plan(self, **kwargs):
            return None

    register_semantic_plan_api_routes(app, FakeIndexer(), _json_success(app), _json_error(app))

    response = app.test_client().get("/api/semantic-plan?query=unknown%20term")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["semantic_plan"] is None
    assert payload["expanded_query_rows"] == []
    assert payload["highlight_terms"] == []
