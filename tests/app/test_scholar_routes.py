from __future__ import annotations

from types import SimpleNamespace

from flask import Flask, jsonify

from app.routes.scholar import register_scholar_search_api_routes


def _json_success(app):
    def api_success(payload=None, status_code=200):
        body = dict(payload or {})
        body.setdefault("success", True)
        body.setdefault("request_id", "req-test")
        return jsonify(body), status_code

    return api_success


def _json_error(app):
    def api_error(message, status_code=500, code="ERR", extra=None):
        body = {
            "success": False,
            "error": message,
            "error_code": code,
            "error_detail": {
                "code": code,
                "message": message,
                "request_id": "req-test",
            },
            "request_id": "req-test",
        }
        if extra:
            body.update(extra)
        return jsonify(body), status_code

    return api_error


class FakeUnderstandingResult:
    def __init__(
        self,
        *,
        route="vector",
        intent="semantic_search",
        normalized_query="Nav1.7",
        corrected_query=None,
        matched_author=None,
        suggested_author=None,
    ):
        self.route = route
        self.intent = intent
        self.normalized_query = normalized_query
        self.corrected_query = corrected_query
        self.matched_author = matched_author
        self.suggested_author = suggested_author

    def to_dict(self):
        return {
            "route": self.route,
            "intent": self.intent,
            "normalized_query": self.normalized_query,
            "corrected_query": self.corrected_query,
            "matched_author": self.matched_author,
            "suggested_author": self.suggested_author,
        }


class FakeMetadataDB:
    def __init__(self):
        self.author_calls = []

    def search_by_author(self, *, author_name, limit, source_list, fuzzy):
        self.author_calls.append(
            {
                "author_name": author_name,
                "limit": limit,
                "source_list": source_list,
                "fuzzy": fuzzy,
            }
        )
        return [
            {
                "work_id": "W_AUTHOR",
                "score": 0.7,
                "metadata": {
                    "canonical_title": "Author paper",
                    "canonical_abstract": "Author abstract",
                    "authors": [{"name": "Nieng Yan"}],
                    "online_at": "2026-04-13T00:00:00",
                    "sources": [
                        {
                            "source_name": "langtaosha",
                            "source_url": "https://example.org/author",
                            "doi": "10.1000/author",
                        }
                    ],
                },
            }
        ]


class FakeIndexer:
    def __init__(self, understanding_result=None):
        self.default_sources = ["langtaosha", "biorxiv_daily"]
        self.metadata_db = FakeMetadataDB()
        self.search_calls = []
        self.query_understanding = SimpleNamespace(
            analyze=lambda query: understanding_result
            or FakeUnderstandingResult(normalized_query=query)
        )

    def search(self, *, query, source_list, top_k, hydrate, search_type):
        self.search_calls.append(
            {
                "query": query,
                "source_list": source_list,
                "top_k": top_k,
                "hydrate": hydrate,
                "search_type": search_type,
            }
        )
        return [
            {
                "work_id": f"W_{source_list[0]}",
                "score": 0.91,
                "source_name": source_list[0],
                "metadata": {
                    "canonical_title": f"Paper from {source_list[0]}",
                    "canonical_abstract": "Abstract A",
                    "authors": [{"name": "Alice"}, {"name": "Bob"}],
                    "online_at": "2026-04-13T00:00:00",
                    "sources": [
                        {
                            "source_name": source_list[0],
                            "source_url": f"https://example.org/{source_list[0]}",
                            "doi": "10.1000/a",
                        }
                    ],
                },
                "retrieval_debug": {
                    "matched_retrievers": ["dense"],
                    "dense_score": 0.91,
                },
            }
        ]


def _client(indexer=None, request_id="req-route-001", recorder=None):
    app = Flask(__name__)
    app.json.ensure_ascii = False
    register_scholar_search_api_routes(
        app,
        indexer or FakeIndexer(),
        _json_success(app),
        _json_error(app),
        request_id_getter=lambda: request_id,
        record_frontend_search_request=recorder,
        client_surface_getter=lambda: "test_surface",
    )
    return app.test_client()


def test_scholar_search_returns_v11_shape_and_no_legacy_fields():
    client = _client()

    response = client.get(
        "/api/scholar/search?query=Nav1.7&mode=smart&top_k=5&source_list=langtaosha,biorxiv_daily"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["success"] is True
    assert data["query"] == {
        "input": "Nav1.7",
        "executed": "Nav1.7",
        "mode": "smart",
        "intent": "semantic_search",
        "route": "vector",
        "corrected_query": None,
        "matched_author": None,
        "suggested_author": None,
    }
    assert data["meta"]["count"] == 2
    assert data["meta"]["request_id"] == "req-route-001"
    assert set(data["meta"]) == {"count", "elapsed_ms", "request_id"}
    assert data["notice"] is None
    assert [item["rank"] for item in data["results"]] == [1, 2]
    assert data["results"][0]["source_key"] == "langtaosha"
    assert data["results"][1]["source_key"] == "biorxiv"

    forbidden_top_level = {
        "search_query",
        "search_mode",
        "query_understanding",
        "result_policy",
        "count",
    }
    assert forbidden_top_level.isdisjoint(data)

    result_keys = set(data["results"][0])
    assert result_keys == {
        "work_id",
        "rank",
        "title",
        "abstract",
        "authors",
        "source",
        "source_key",
        "online_date",
        "link",
        "doi",
        "ranking_score",
        "match_reasons",
    }


def test_scholar_search_ignores_limit_and_offset_for_public_v11_api():
    indexer = FakeIndexer()
    client = _client(indexer=indexer)

    response = client.get(
        "/api/scholar/search?query=Nav1.7&mode=smart&top_k=5&limit=1&offset=99&source_list=langtaosha,biorxiv_daily"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["meta"]["count"] == 2
    assert "limit" not in data["meta"]
    assert "offset" not in data["meta"]
    assert "has_more" not in data["meta"]
    assert [call["top_k"] for call in indexer.search_calls] == [5, 5]


def test_scholar_search_normalizes_top_k_to_one_to_one_hundred():
    indexer_low = FakeIndexer()
    _client(indexer=indexer_low).get(
        "/api/scholar/search?query=Nav1.7&top_k=0&source_list=langtaosha"
    )
    assert indexer_low.search_calls[0]["top_k"] == 1

    indexer_high = FakeIndexer()
    _client(indexer=indexer_high).get(
        "/api/scholar/search?query=Nav1.7&top_k=999&source_list=langtaosha"
    )
    assert indexer_high.search_calls[0]["top_k"] == 100

    indexer_invalid = FakeIndexer()
    _client(indexer=indexer_invalid).get(
        "/api/scholar/search?query=Nav1.7&top_k=abc&source_list=langtaosha"
    )
    assert indexer_invalid.search_calls[0]["top_k"] == 100


def test_scholar_search_rejects_empty_query_and_invalid_mode():
    client = _client()

    empty_response = client.get("/api/scholar/search?query=%20%20")
    empty_data = empty_response.get_json()
    assert empty_response.status_code == 400
    assert empty_data["error_code"] == "INVALID_REQUEST"
    assert empty_data["error"] == "query 不能为空"

    mode_response = client.get("/api/scholar/search?query=Nav1.7&mode=keyword")
    mode_data = mode_response.get_json()
    assert mode_response.status_code == 400
    assert mode_data["error_code"] == "INVALID_REQUEST"
    assert mode_data["error"] == "mode 只能是 smart 或 vector"


def test_vector_mode_adds_vector_notice():
    indexer = FakeIndexer()
    client = _client(indexer=indexer)

    response = client.get(
        "/api/scholar/search?query=Nav1.7&mode=vector&source_list=langtaosha"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["query"]["mode"] == "vector"
    assert data["query"]["route"] == "vector"
    assert data["notice"] == {
        "type": "vector",
        "message": "已按原 query 执行向量检索。",
        "action": None,
    }


def test_query_correction_notice_uses_corrected_query_but_action_keeps_original_query():
    indexer = FakeIndexer(
        FakeUnderstandingResult(
            normalized_query="machi learningn",
            corrected_query="machine learning",
        )
    )
    client = _client(indexer=indexer)

    response = client.get(
        "/api/scholar/search?query=machi%20learningn&mode=smart&source_list=langtaosha"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["query"]["executed"] == "machine learning"
    assert indexer.search_calls[0]["query"] == "machine learning"
    assert data["notice"] == {
        "type": "query_correction",
        "message": "已识别到可能的拼写错误，实际搜索 query 为: machine learning",
        "action": {
            "label": "使用原 query 检索",
            "mode": "vector",
            "query": "machi learningn",
        },
    }


def test_author_suggestion_returns_no_results_and_action_query():
    indexer = FakeIndexer(
        FakeUnderstandingResult(
            route="author_suggestion",
            intent="author_name",
            normalized_query="niang yan",
            suggested_author="Nieng Yan",
        )
    )
    client = _client(indexer=indexer)

    response = client.get(
        "/api/scholar/search?query=niang%20yan&mode=smart&source_list=langtaosha"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["query"]["executed"] is None
    assert data["query"]["suggested_author"] == "Nieng Yan"
    assert data["meta"]["count"] == 0
    assert data["results"] == []
    assert data["notice"] == {
        "type": "author_suggestion",
        "message": '未找到 "niang yan" 的高置信作者匹配，是否搜索作者 Nieng Yan？',
        "action": {
            "label": "搜索作者 Nieng Yan",
            "mode": "smart",
            "query": "Nieng Yan",
        },
    }


def test_author_match_uses_metadata_author_search_and_notice_action():
    indexer = FakeIndexer(
        FakeUnderstandingResult(
            route="metadata_author",
            intent="author_name",
            normalized_query="Nieng Yan",
            matched_author="Nieng Yan",
        )
    )
    client = _client(indexer=indexer)

    response = client.get(
        "/api/scholar/search?query=Nieng%20Yan&mode=smart&source_list=langtaosha"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert indexer.metadata_db.author_calls == [
        {
            "author_name": "Nieng Yan",
            "limit": 100,
            "source_list": ["langtaosha"],
            "fuzzy": True,
        }
    ]
    assert data["query"]["executed"] == "Nieng Yan"
    assert data["notice"] == {
        "type": "author_name",
        "message": "已识别为作者名，正在根据作者 Nieng Yan 完成搜索。",
        "action": {
            "label": "改用向量检索",
            "mode": "vector",
            "query": "Nieng Yan",
        },
    }


def test_scholar_search_records_optional_frontend_search_log():
    captured = {}

    def recorder(**kwargs):
        captured.update(kwargs)

    client = _client(recorder=recorder)

    response = client.get("/api/scholar/search?query=Nav1.7&mode=smart&top_k=5")

    assert response.status_code == 200
    assert captured["status_code"] == 200
    assert captured["client_surface"] == "test_surface"
    assert captured["request_path"] == "/api/scholar/search"
    assert captured["request_method"] == "GET"
    assert captured["request_args"] == {
        "query": "Nav1.7",
        "mode": "smart",
        "top_k": "5",
        "source_list": None,
    }
    assert captured["response_body"]["meta"]["request_id"] == "req-route-001"
