from __future__ import annotations

import sys
from types import SimpleNamespace

from app.dev.main_search_use import (
    DEFAULT_API_PORT,
    DEFAULT_CONFIG_PATH,
    DEFAULT_FRONTEND_PORT,
    create_search_use_api_app,
    create_search_use_frontend_app,
)


class FakeIndexer:
    default_sources = ["langtaosha"]

    def __init__(self):
        self.smart_search_calls = []
        self.query_understanding = SimpleNamespace(
            analyze=lambda query: SimpleNamespace(
                route="vector",
                intent="semantic_search",
                corrected_query=None,
                normalized_query=query,
                matched_author=None,
                suggested_author=None,
                to_dict=lambda: {
                    "route": "vector",
                    "intent": "semantic_search",
                    "normalized_query": query,
                    "corrected_query": None,
                    "matched_author": None,
                    "suggested_author": None,
                },
            )
        )

    def search(self, *, query, source_list, top_k, hydrate, search_type):
        return [
            {
                "work_id": "W1",
                "score": 0.92,
                "metadata": {
                    "canonical_title": "Paper A",
                    "canonical_abstract": "Abstract A",
                    "authors": [{"name": "Alice"}],
                    "online_at": "2026-04-13T00:00:00",
                    "sources": [
                        {
                            "source_name": "langtaosha",
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
            "expanded_search_queries": [],
            "query_understanding": {
                "route": "vector",
                "intent": "semantic_search",
                "normalized_query": query,
                "corrected_query": None,
                "matched_author": None,
                "suggested_author": None,
            },
            "results": [
                (
                    "langtaosha",
                    self.search(
                        query=query,
                        source_list=source_list or self.default_sources,
                        top_k=top_k,
                        hydrate=hydrate,
                        search_type="hybrid_retrieval",
                    ),
                )
            ],
        }


def test_create_search_use_frontend_app_serves_page_and_not_legacy_index():
    app = create_search_use_frontend_app(api_base_url="http://127.0.0.1:5016")
    client = app.test_client()

    response = client.get("/search-use?q=Nav1.7")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Langtaosha Smart Search" in html
    assert "Nav1.7" in html


def test_create_search_use_api_app_uses_clean_scholar_routes():
    sys.modules.pop("app.main", None)

    app = create_search_use_api_app(
        paper_indexer=FakeIndexer(),
        allowed_origins=("http://localhost:5015",),
        request_id_factory=lambda: "req-search-use",
    )
    client = app.test_client()

    response = client.get(
        "/api/scholar/search?query=Nav1.7&limit=5",
        headers={"Origin": "http://localhost:5015"},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["meta"]["request_id"] == "req-search-use"
    assert payload["search_mode"] == "smart"
    assert payload["search_query"] == "Nav1.7"
    assert payload["query_understanding"]["route"] == "vector"
    assert payload["smart_search"]["search_query"] == "Nav1.7"
    assert payload["count"] == payload["meta"]["count"]
    assert payload["results"][0][0] == "langtaosha"
    assert payload["results"][0][1][0]["work_id"] == "W1"
    assert response.headers["Access-Control-Allow-Origin"] == "http://localhost:5015"


def test_search_use_defaults_point_to_use_config():
    assert str(DEFAULT_CONFIG_PATH).endswith("src/config/config_tecent_backend_server_use.yaml")
    assert DEFAULT_FRONTEND_PORT == 5015
    assert DEFAULT_API_PORT == 5016


def test_create_search_use_api_app_has_health_and_rejects_non_api_routes():
    app = create_search_use_api_app(
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


def test_create_search_use_api_app_preserves_request_id_header():
    app = create_search_use_api_app(
        paper_indexer=FakeIndexer(),
        request_id_factory=lambda: "req-search-use-002",
    )
    client = app.test_client()

    response = client.get("/api/scholar/search?query=Nav1.7&limit=5")
    payload = response.get_json()

    assert response.headers["X-Request-Id"] == "req-search-use-002"
    assert payload["request_id"] == "req-search-use-002"
    assert payload["meta"]["request_id"] == "req-search-use-002"
