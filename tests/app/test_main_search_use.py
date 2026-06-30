from __future__ import annotations

import logging
import sys
from types import SimpleNamespace

from app.dev.main_search_use import (
    DEFAULT_API_PORT,
    DEFAULT_CONFIG_PATH,
    DEFAULT_FRONTEND_PORT,
    _configure_logging,
    create_search_use_api_app,
    create_search_use_frontend_app,
)


class FakeIndexer:
    default_sources = ["langtaosha"]

    def __init__(self):
        self.smart_search_calls = []
        self.search_calls = []
        self.hybrid_search_calls = []
        self.retrieve_calls = []
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

    def _build_result(self, *, work_id: str, score: float, title: str):
        return {
            "work_id": work_id,
            "score": score,
            "metadata": {
                "canonical_title": title,
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

    def search(self, *, query, source_list, top_k, hydrate, search_type, keyword_sources=None):
        self.search_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
                "hydrate": hydrate,
                "search_type": search_type,
                "keyword_sources": keyword_sources,
            }
        )
        return [self._build_result(work_id="W1", score=0.92, title="Paper A")]

    def hybrid_retrieval_search(
        self,
        *,
        query,
        source_list,
        top_k,
        hydrate,
        keyword_sources=None,
    ):
        self.hybrid_search_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
                "hydrate": hydrate,
                "keyword_sources": keyword_sources,
            }
        )
        return [self._build_result(work_id="HW1", score=0.89, title="Hybrid Paper A")]

    def smart_search(self, *, query, source_list, top_k, hydrate, correction_decision=None):
        self.smart_search_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
                "hydrate": hydrate,
                "correction_decision": correction_decision,
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

    def retrieve_papers_by_time_interval(self, date_from, date_to, source_name="langtaosha"):
        self.retrieve_calls.append(
            {
                "date_from": date_from,
                "date_to": date_to,
                "source_name": source_name,
            }
        )
        return [
            {
                "paper_id": 1,
                "work_id": "W1",
                "canonical_title": "Paper A",
            }
        ]


def test_create_search_use_frontend_app_serves_page_and_not_legacy_index():
    app = create_search_use_frontend_app(api_base_url="http://127.0.0.1:5016")
    client = app.test_client()

    response = client.get("/search-use?q=Nav1.7")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "LangTaosha Smart Search Demo" in html
    assert "<h1>Smart Search Demo</h1>" not in html
    assert "Langtaosha Smart Search" not in html
    assert "Nav1.7" in html


def test_create_search_use_frontend_app_serves_logo_asset():
    app = create_search_use_frontend_app(api_base_url="http://127.0.0.1:5016")
    client = app.test_client()

    response = client.get("/lib/ui-library/src/resources/ltslogo_new.png")

    assert response.status_code == 200
    assert response.mimetype == "image/png"
    assert response.data.startswith(b"\x89PNG")


def test_create_search_use_frontend_app_serves_png_favicon_asset():
    app = create_search_use_frontend_app(api_base_url="http://127.0.0.1:5016")
    client = app.test_client()

    response = client.get("/lib/ui-library/src/resources/favicon_en.png")

    assert response.status_code == 200
    assert response.mimetype == "image/png"
    assert response.data.startswith(b"\x89PNG")


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


def test_create_search_use_api_app_exposes_paper_search_route():
    indexer = FakeIndexer()
    app = create_search_use_api_app(
        paper_indexer=indexer,
        request_id_factory=lambda: "req-paper-search",
    )
    client = app.test_client()

    response = client.get("/api/search?query=Nav1.7&search_type=expanded_sparse&top_k=3")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["query"] == "Nav1.7"
    assert payload["search_type"] == "expanded_sparse"
    assert payload["results"][0]["work_id"] == "W1"
    assert payload["request_id"] == "req-paper-search"
    assert payload["timings_ms"]["search"] >= 0
    assert indexer.search_calls == [
        {
            "query": "Nav1.7",
            "source_list": None,
            "top_k": 3,
            "hydrate": True,
            "search_type": "expanded_sparse",
            "keyword_sources": None,
        }
    ]


def test_create_search_use_api_app_keeps_scholar_and_paper_routes():
    indexer = FakeIndexer()
    app = create_search_use_api_app(
        paper_indexer=indexer,
        request_id_factory=lambda: "req-both-routes",
    )
    client = app.test_client()

    scholar_response = client.get("/api/scholar/search?query=Nav1.7")
    paper_response = client.get("/api/search?query=Nav1.7&search_type=dense")

    assert scholar_response.status_code == 200
    assert paper_response.status_code == 200
    assert scholar_response.get_json()["search_mode"] == "smart"
    assert paper_response.get_json()["search_type"] == "dense"


def test_create_search_use_api_app_exposes_retrieve_papers_by_time_interval_route():
    indexer = FakeIndexer()
    app = create_search_use_api_app(
        paper_indexer=indexer,
        request_id_factory=lambda: "req-retrieve",
    )
    client = app.test_client()

    response = client.post(
        "/api/retrieve_papers_by_time_interval",
        json={"date_from": "2026-04-01", "date_to": "2026-04-10"},
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["request_id"] == "req-retrieve"
    assert payload["date_from"] == "2026-04-01"
    assert payload["date_to"] == "2026-04-10"
    assert payload["papers"] == [
        {
            "paper_id": 1,
            "work_id": "W1",
            "canonical_title": "Paper A",
        }
    ]
    assert indexer.retrieve_calls == [
        {
            "date_from": "2026-04-01",
            "date_to": "2026-04-10",
            "source_name": "langtaosha",
        }
    ]


def test_search_use_defaults_point_to_use_config():
    assert str(DEFAULT_CONFIG_PATH).endswith("src/config/config_tecent_backend_server_use.yaml")
    assert DEFAULT_FRONTEND_PORT == 5015


def test_configure_logging_sets_info_level_when_root_is_quiet(monkeypatch):
    root_logger = logging.getLogger()
    original_level = root_logger.level
    original_handlers = list(root_logger.handlers)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    root_logger.setLevel(logging.WARNING)

    monkeypatch.delenv("SEARCH_USE_LOG_LEVEL", raising=False)

    try:
        _configure_logging()
        assert logging.getLogger().level == logging.INFO
        assert logging.getLogger().handlers
    finally:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)
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
        "service": "search_use_api",
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
