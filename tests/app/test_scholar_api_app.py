from __future__ import annotations

import sys
from types import SimpleNamespace

from app.dev.scholar_api_app import create_scholar_api_app


class FakeUnderstandingResult:
    route = "vector"
    intent = "semantic_search"
    corrected_query = None
    matched_author = None
    suggested_author = None

    def __init__(self, normalized_query: str):
        self.normalized_query = normalized_query

    def to_dict(self):
        return {
            "route": self.route,
            "intent": self.intent,
            "normalized_query": self.normalized_query,
            "corrected_query": self.corrected_query,
            "matched_author": self.matched_author,
            "suggested_author": self.suggested_author,
        }


class FakeIndexer:
    default_sources = ["langtaosha"]

    def __init__(self):
        self.smart_search_calls = []
        self.query_understanding = SimpleNamespace(
            analyze=lambda query: FakeUnderstandingResult(query)
        )

    def search(self, *, query, source_list, top_k, hydrate, search_type):
        return [
            {
                "work_id": "W1",
                "score": 0.9,
                "metadata": {
                    "canonical_title": "Paper A",
                    "canonical_abstract": "Abstract A",
                    "authors": [{"name": "Alice"}],
                    "online_at": "2026-04-13T00:00:00",
                    "sources": [
                        {
                            "source_name": source_list[0],
                            "source_url": "https://example.org/a",
                            "doi": "10.1000/a",
                        }
                    ],
                },
            }
        ]

    def smart_search(self, *, query, source_list, top_k, hydrate):
        self.smart_search_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
                "hydrate": hydrate,
            }
        )
        return {
            "success": True,
            "query": query,
            "search_query": query,
            "query_understanding": {
                "route": "vector",
                "intent": "semantic_search",
                "normalized_query": query,
                "corrected_query": None,
                "matched_author": None,
                "suggested_author": None,
            },
            "results": self.search(
                query=query,
                source_list=source_list or self.default_sources,
                top_k=top_k,
                hydrate=hydrate,
                search_type="hybrid_retrieval",
            ),
        }


def test_create_scholar_api_app_registers_clean_search_api_without_legacy_main():
    sys.modules.pop("app.main", None)

    app = create_scholar_api_app(
        paper_indexer=FakeIndexer(),
        allowed_origins=("http://localhost:5173",),
        request_id_factory=lambda: "req-clean-api",
    )
    client = app.test_client()

    response = client.get(
        "/api/scholar/search?query=Nav1.7&top_k=5&source_list=langtaosha",
        headers={"Origin": "http://localhost:5173"},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["meta"]["request_id"] == "req-clean-api"
    assert payload["results"][0]["work_id"] == "W1"
    assert response.headers["X-Request-Id"] == "req-clean-api"
    assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:5173"
    assert "app.main" not in sys.modules


def test_scholar_api_app_has_health_and_rejects_non_api_routes():
    app = create_scholar_api_app(
        paper_indexer=FakeIndexer(),
        request_id_factory=lambda: "req-health",
    )
    client = app.test_client()

    health = client.get("/api/health")
    missing_page = client.get("/")

    assert health.status_code == 200
    assert health.get_json() == {
        "success": True,
        "status": "ok",
        "service": "scholar_search_api",
        "request_id": "req-health",
    }
    assert missing_page.status_code == 404
