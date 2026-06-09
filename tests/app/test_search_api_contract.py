from types import SimpleNamespace

import app.main as main


def test_run_scholar_search_returns_query_meta_notice_results(monkeypatch):
    monkeypatch.setattr(
        main,
        "indexer",
        SimpleNamespace(
            default_sources=["langtaosha"],
            query_understanding=SimpleNamespace(
                analyze=lambda query: SimpleNamespace(
                    route="vector",
                    corrected_query=None,
                    normalized_query=query,
                    matched_author=None,
                    suggested_author=None,
                    intent="semantic_search",
                    to_dict=lambda: {
                        "route": "vector",
                        "intent": "semantic_search",
                        "normalized_query": query,
                        "corrected_query": None,
                        "matched_author": None,
                        "suggested_author": None,
                    },
                )
            ),
        ),
    )
    monkeypatch.setattr(
        main,
        "_prioritized_vector_search",
        lambda query, source_list=None, per_source_top_k=10: [
            {
                "work_id": "W1",
                "paper_id": 1,
                "source_name": "langtaosha",
                "score": 0.91,
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
        ],
    )
    monkeypatch.setattr(main, "build_search_highlight", lambda **kwargs: {"tokens": ["Nav1.7"]})

    result = main.run_scholar_search(
        query="Nav1.7",
        limit=5,
        offset=0,
        source_list=["langtaosha"],
        search_mode="smart",
    )

    assert result["success"] is True
    assert result["query"]["input"] == "Nav1.7"
    assert result["query"]["executed"] == "Nav1.7"
    assert result["meta"]["count"] == 1
    assert result["meta"]["limit"] == 5
    assert result["meta"]["offset"] == 0
    assert "request_id" not in result["meta"]
    assert result["notice"] is None
    assert result["results"][0]["rank"] == 1
    assert result["results"][0]["ranking_score"] == 0.91


def test_build_query_correction_notice_uses_action_schema():
    notice = main._build_query_notice(
        query="machi learningn",
        search_query="machine learning",
        understanding={
            "intent": "semantic_search",
            "route": "vector",
            "normalized_query": "machi learningn",
            "corrected_query": "machine learning",
        },
        search_mode="smart",
    )

    assert notice == {
        "type": "query_correction",
        "message": "已识别到可能的拼写错误，实际搜索 query 为: machine learning",
        "action": {
            "label": "使用原 query 检索",
            "mode": "vector",
            "query": "machi learningn",
        },
    }


def test_normalize_limit_and_offset_support_top_k_fallback():
    assert main._normalize_limit(limit=5, top_k=None) == 5
    assert main._normalize_limit(limit=None, top_k=7) == 7
    assert main._normalize_limit(limit=None, top_k=None) == 100
    assert main._normalize_offset(0) == 0


def test_insert_user_study_search_event_reads_new_shape(monkeypatch):
    captured = {}

    class FakeResult:
        def scalar_one(self):
            return 1

    class FakeConn:
        def execute(self, stmt, params):
            captured.setdefault("calls", []).append(params)
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def begin(self):
            return FakeConn()

    monkeypatch.setattr(main, "get_db_engine", lambda db_key="metadata_db": FakeEngine())

    event_id, query_index = main.insert_user_study_search_event(
        study_session_id="s_1",
        participant_id="p01",
        search_data={
            "query": {
                "input": "Nav1.7",
                "executed": "Nav1.7",
                "mode": "smart",
                "route": "vector",
            },
            "meta": {"count": 2},
            "notice": None,
            "results": [],
        },
    )

    assert event_id == 1
    assert query_index == 1
    assert captured["calls"][-1]["query"] == "Nav1.7"
    assert captured["calls"][-1]["search_query"] == "Nav1.7"
    assert captured["calls"][-1]["search_mode"] == "smart"
    assert captured["calls"][-1]["query_understanding_route"] == "vector"
    assert captured["calls"][-1]["result_count"] == 2


def test_study_search_response_keeps_study_block(monkeypatch):
    client = main.app.test_client()
    monkeypatch.setattr(
        main,
        "run_scholar_search",
        lambda **kwargs: {
            "success": True,
            "query": {
                "input": "Nav1.7",
                "executed": "Nav1.7",
                "mode": "smart",
                "route": "vector",
            },
            "meta": {
                "count": 1,
                "limit": 5,
                "offset": 0,
                "has_more": False,
                "elapsed_ms": 10,
            },
            "notice": None,
            "results": [{"work_id": "W1", "rank": 1, "title": "Paper A"}],
        },
    )
    monkeypatch.setattr(main, "insert_user_study_search_event", lambda **kwargs: (123, 1))
    monkeypatch.setattr(main, "insert_user_study_search_results", lambda **kwargs: 1)

    response = client.get("/api/study/search?participant_id=p01&query=Nav1.7&limit=5")
    data = response.get_json()

    assert response.status_code == 200
    assert data["study"]["search_event_id"] == 123
    assert data["meta"]["count"] == 1


def test_api_scholar_search_generates_backend_request_id_and_passes_limit_offset(monkeypatch):
    client = main.app.test_client()
    captured = {}

    def fake_run_scholar_search(**kwargs):
        captured.update(kwargs)
        return {
            "success": True,
            "query": {
                "input": "Nav1.7",
                "executed": "Nav1.7",
                "mode": "smart",
                "route": "vector",
            },
            "meta": {
                "count": 1,
                "limit": 5,
                "offset": 10,
                "has_more": False,
                "elapsed_ms": 12,
            },
            "notice": None,
            "results": [{"work_id": "W1", "rank": 11, "title": "Paper A"}],
        }

    monkeypatch.setattr(main, "run_scholar_search", fake_run_scholar_search)
    monkeypatch.setattr(main.uuid, "uuid4", lambda: SimpleNamespace(hex="backend-generated-req-001"))

    response = client.get(
        "/api/scholar/search?query=Nav1.7&mode=smart&limit=5&offset=10",
        headers={"X-Request-Id": "req-test-001"},
    )
    data = response.get_json()

    assert response.status_code == 200
    assert captured["limit"] == 5
    assert captured["offset"] == 10
    assert data["meta"]["request_id"] == "backend-generated-req-001"
    assert data["request_id"] == "backend-generated-req-001"
    assert response.headers["X-Request-Id"] == "backend-generated-req-001"


def test_api_scholar_search_logs_and_persists_summary(monkeypatch):
    client = main.app.test_client()
    captured = {}

    monkeypatch.setattr(
        main,
        "run_scholar_search",
        lambda **kwargs: {
            "success": True,
            "query": {
                "input": "Nav1.7",
                "executed": "Nav1.7",
                "mode": "smart",
                "intent": "semantic_search",
                "route": "vector",
                "corrected_query": None,
                "matched_author": None,
                "suggested_author": None,
            },
            "meta": {
                "count": 1,
                "limit": 5,
                "offset": 0,
                "has_more": False,
                "elapsed_ms": 12,
            },
            "notice": None,
            "results": [{"work_id": "W1", "rank": 1, "title": "Paper A"}],
        },
    )
    monkeypatch.setattr(
        main,
        "record_frontend_search_request",
        lambda **kwargs: captured.setdefault("kwargs", kwargs),
    )
    monkeypatch.setattr(main.uuid, "uuid4", lambda: SimpleNamespace(hex="backend-generated-req-002"))

    response = client.get(
        "/api/scholar/search?query=Nav1.7&mode=smart&limit=5&offset=0",
        headers={"X-Langtaosha-Client-Surface": "search_api_test"},
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["meta"]["request_id"] == "backend-generated-req-002"
    assert captured["kwargs"]["client_surface"] == "search_api_test"
    assert captured["kwargs"]["status_code"] == 200
    assert captured["kwargs"]["response_body"]["request_id"] == "backend-generated-req-002"
    assert captured["kwargs"]["response_body"]["meta"]["request_id"] == "backend-generated-req-002"


def test_api_error_response_keeps_utf8_chinese_text():
    client = main.app.test_client()

    response = client.get("/api/scholar/search?query=")
    body = response.get_data(as_text=True)

    assert response.status_code == 400
    assert "query 不能为空" in body
    assert "\\u4e0d" not in body


def test_search_api_test_page_renders_when_legacy_pages_allowed(monkeypatch):
    client = main.app.test_client()
    monkeypatch.setenv("ALLOW_DIRECT_LEGACY_PAGES", "1")

    response = client.get("/search-api-test")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Search API Test Console" in body
    assert "/api/scholar/search" in body
