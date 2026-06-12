# Scholar Search Route API v1.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `/api/scholar/search` in `app/routes/scholar.py` according to `docs/api/frontend_api_0612_xiongye.md`, without legacy compatibility and without modifying `app/main.py`.

**Architecture:** `app/routes/scholar.py` becomes a self-contained Flask route module for the new application. It exposes `register_scholar_search_api_routes(...)`, accepts an injected `PaperIndexer`-compatible object plus API response helpers, and owns request parsing, smart/vector search orchestration, v1.1 response shaping, notice generation, and optional request logging. `app/main.py` is treated as old application code and is not part of this plan.

**Tech Stack:** Flask, pytest, fake indexer unit tests, existing `PaperIndexer` smart-search dependencies (`query_understanding`, `metadata_db.search_by_author`, `search`), existing `build_search_highlight` helper only if needed for internal matching context.

---

## Scope Decisions

1. Do not modify `app/main.py`.
2. Do not preserve the v1 / 0608 compatibility fields for the new `/api/scholar/search` route.
3. Do not implement `limit` / `offset`; if a client sends them, ignore them.
4. Do not add a service-layer extraction in this task.
5. Do not require real database or vector services in route tests; use fake indexer objects.
6. Public response must follow `docs/api/frontend_api_0612_xiongye.md`.

## Contract Summary

Request:

```text
GET /api/scholar/search
query       required string, trim before use
mode        optional string, default smart, allowed smart/vector
top_k       optional integer, default 100, normalized to 1..100
source_list optional CSV string, None means indexer default sources
```

Success response:

```json
{
  "success": true,
  "query": {
    "input": "Nav1.7",
    "executed": "Nav1.7",
    "mode": "smart",
    "intent": "semantic_search",
    "route": "vector",
    "corrected_query": null,
    "matched_author": null,
    "suggested_author": null
  },
  "meta": {
    "count": 1,
    "elapsed_ms": 12,
    "request_id": "backend-request-id"
  },
  "notice": null,
  "results": []
}
```

Fields explicitly excluded from the new public route:

```text
limit
offset
has_more
search_query
search_mode
query_understanding
result_policy
top-level count
similarity
similarity_score
retrieval_reasons
retrieval_reason_tags
paper_id
source_name
highlight
```

## File Map

**Modify**

- `app/routes/scholar.py`
  - Replace placeholder with route registration and v1.1 search implementation.

**Create**

- `tests/app/test_scholar_routes.py`
  - Route-module contract tests independent from `app/main.py`.

**Do Not Modify**

- `app/main.py`
- `tests/app/test_search_api_contract.py`
  - That file covers the old app. Leave it alone unless a later cleanup explicitly removes the old app contract.

All commands below run from:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/.worktrees/feature-span-matcher-expanded-sparse-retrieval
```

---

## Task 1: Add Scholar Route Contract Tests

**Files:**
- Create: `tests/app/test_scholar_routes.py`
- Modify: `app/routes/scholar.py`

- [ ] **Step 1: Replace the placeholder import smoke expectation with failing route tests**

Affected files:

```text
Create: tests/app/test_scholar_routes.py
Read: app/routes/scholar.py
```

Create `tests/app/test_scholar_routes.py`:

```python
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
    assert data["results"][1]["source_key"] == "biorxiv_daily"

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
    _client(indexer=indexer_low).get("/api/scholar/search?query=Nav1.7&top_k=0&source_list=langtaosha")
    assert indexer_low.search_calls[0]["top_k"] == 1

    indexer_high = FakeIndexer()
    _client(indexer=indexer_high).get("/api/scholar/search?query=Nav1.7&top_k=999&source_list=langtaosha")
    assert indexer_high.search_calls[0]["top_k"] == 100

    indexer_invalid = FakeIndexer()
    _client(indexer=indexer_invalid).get("/api/scholar/search?query=Nav1.7&top_k=abc&source_list=langtaosha")
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


def test_vector_mode_adds_vector_notice_and_skips_query_understanding():
    indexer = FakeIndexer()
    client = _client(indexer=indexer)

    response = client.get("/api/scholar/search?query=Nav1.7&mode=vector&source_list=langtaosha")
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

    response = client.get("/api/scholar/search?query=machi%20learningn&mode=smart&source_list=langtaosha")
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

    response = client.get("/api/scholar/search?query=niang%20yan&mode=smart&source_list=langtaosha")
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

    response = client.get("/api/scholar/search?query=Nieng%20Yan&mode=smart&source_list=langtaosha")
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
```

- [ ] **Step 2: Run the tests to verify failure**

Affected files:

```text
Read: tests/app/test_scholar_routes.py
Read: app/routes/scholar.py
Write: pytest cache files only
```

Run:

```bash
PYTHONPATH=. python3 -m pytest tests/app/test_scholar_routes.py -q
```

Expected: FAIL with `ImportError: cannot import name 'register_scholar_search_api_routes'`.

- [ ] **Step 3: Commit the failing contract tests**

Affected files:

```text
Commit: tests/app/test_scholar_routes.py
```

```bash
git add tests/app/test_scholar_routes.py
git commit -m "test: add scholar route v11 API contract"
```

---

## Task 2: Implement `app/routes/scholar.py`

**Files:**
- Modify: `app/routes/scholar.py`
- Test: `tests/app/test_scholar_routes.py`

- [ ] **Step 1: Replace the placeholder module with imports, constants, and parameter helpers**

Affected files:

```text
Modify: app/routes/scholar.py
```

Replace `app/routes/scholar.py` with this module skeleton:

```python
from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from flask import request

LANGTAOSHA_SOURCES = ("langtaosha",)
BIORXIV_SOURCES = ("biorxiv_history", "biorxiv_daily")
DEFAULT_INDEXER_SEARCH_TYPE = "hybrid_retrieval"


def _normalize_top_k(raw_value: Any) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = 100
    return min(max(value, 1), 100)


def _validate_search_mode(raw_value: Any) -> str:
    mode = (str(raw_value or "smart")).strip().lower()
    if mode not in {"smart", "vector"}:
        raise ValueError("mode 只能是 smart 或 vector")
    return mode


def _parse_source_list(raw_value: Optional[str]) -> Optional[List[str]]:
    text = (raw_value or "").strip()
    if not text:
        return None
    return [item.strip() for item in text.split(",") if item.strip()]
```

- [ ] **Step 2: Add source resolution and prioritized vector search**

Affected files:

```text
Modify: app/routes/scholar.py
```

Add:

```python
def _resolve_search_sources(indexer: Any, source_list: Optional[List[str]]) -> List[str]:
    default_sources = list(getattr(indexer, "default_sources", []) or [])
    sources = source_list if source_list else default_sources
    valid_sources = set(default_sources)
    resolved_sources: List[str] = []
    invalid_sources: List[str] = []

    for source in sources:
        if source not in valid_sources:
            invalid_sources.append(source)
            continue
        if source not in resolved_sources:
            resolved_sources.append(source)

    if invalid_sources:
        raise ValueError(
            f"source_list 包含未知 source: {invalid_sources}; 合法 sources: {default_sources}"
        )

    return resolved_sources


def _dedupe_search_results(result_groups: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    merged_results: List[Dict[str, Any]] = []
    seen_work_ids = set()

    for results in result_groups:
        for item in results:
            work_id = item.get("work_id")
            if work_id:
                if work_id in seen_work_ids:
                    continue
                seen_work_ids.add(work_id)
            merged_results.append(item)

    return merged_results


def _search_by_prioritized_sources(
    indexer: Any,
    query: str,
    source_list: Optional[List[str]],
    top_k: int,
) -> List[Dict[str, Any]]:
    resolved_sources = _resolve_search_sources(indexer, source_list)
    langtaosha_sources = [source for source in resolved_sources if source in LANGTAOSHA_SOURCES]
    biorxiv_sources = [source for source in resolved_sources if source in BIORXIV_SOURCES]
    other_sources = [
        source
        for source in resolved_sources
        if source not in LANGTAOSHA_SOURCES and source not in BIORXIV_SOURCES
    ]

    result_groups: List[List[Dict[str, Any]]] = []
    for grouped_sources in (langtaosha_sources, biorxiv_sources, other_sources):
        if not grouped_sources:
            result_groups.append([])
            continue
        result_groups.append(
            indexer.search(
                query=query,
                source_list=grouped_sources,
                top_k=top_k,
                hydrate=True,
                search_type=DEFAULT_INDEXER_SEARCH_TYPE,
            )
        )

    return _dedupe_search_results(result_groups)
```

- [ ] **Step 3: Add v1.1 result mapping helpers**

Affected files:

```text
Modify: app/routes/scholar.py
```

Add:

```python
def _format_date_ymd(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    if len(text) >= 10:
        return text[:10]
    return text or None


def _extract_authors(metadata: Dict[str, Any]) -> str:
    author_items = metadata.get("authors") or []
    if isinstance(author_items, str):
        return author_items
    names = [
        str(item.get("name", "")).strip()
        for item in author_items
        if isinstance(item, dict) and item.get("name")
    ]
    return ", ".join(names)


def _get_preferred_source(metadata: Dict[str, Any]) -> Dict[str, Any]:
    sources = metadata.get("sources") or []
    canonical_source_id = metadata.get("canonical_source_id")
    if canonical_source_id is not None:
        for source in sources:
            if source.get("paper_source_id") == canonical_source_id:
                return source
    return sources[0] if sources else {}


def _extract_doi(metadata: Dict[str, Any]) -> Optional[str]:
    preferred_source = _get_preferred_source(metadata)
    if preferred_source.get("doi"):
        return preferred_source.get("doi")
    for source in metadata.get("sources") or []:
        if source.get("doi"):
            return source.get("doi")
    return metadata.get("doi")


def _normalize_source_label(source_name: Optional[str]) -> str:
    if source_name == "langtaosha":
        return "Langtaosha"
    if source_name in {"biorxiv_history", "biorxiv_daily", "biorxiv"}:
        return "Biorxiv"
    return source_name or ""


def _normalize_source_key(source_name: Optional[str]) -> str:
    if source_name in {"biorxiv_history", "biorxiv_daily", "biorxiv"}:
        return source_name or "biorxiv"
    return source_name or ""


def _extract_paper_link(metadata: Dict[str, Any], doi: Optional[str]) -> Optional[str]:
    preferred_source = _get_preferred_source(metadata)
    if preferred_source.get("source_url"):
        return preferred_source.get("source_url")
    if metadata.get("link"):
        return metadata.get("link")
    if doi:
        return f"https://doi.org/{doi}"
    return None


def _get_ranking_score(item: Dict[str, Any]) -> Optional[float]:
    for key in ("ranking_score", "score", "similarity_score", "similarity"):
        value = item.get(key)
        if value is not None:
            try:
                return round(float(value), 4)
            except (TypeError, ValueError):
                return None
    return None


def _format_reason_score(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _build_match_reasons(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(item.get("match_reasons"), list):
        return item["match_reasons"]

    retrieval_debug = item.get("retrieval_debug") or {}
    matched_retrievers = set(retrieval_debug.get("matched_retrievers") or [])
    reasons: List[Dict[str, Any]] = []

    dense_score = retrieval_debug.get("dense_score")
    if "dense" in matched_retrievers or dense_score is not None:
        reason: Dict[str, Any] = {"key": "dense_recall", "label": "Dense recall"}
        score = _format_reason_score(dense_score)
        if score is not None:
            reason["score"] = score
        reasons.append(reason)

    sparse_score = retrieval_debug.get("sparse_score")
    if "sparse" in matched_retrievers or sparse_score is not None:
        reason = {"key": "sparse_recall", "label": "Sparse recall"}
        score = _format_reason_score(sparse_score)
        if score is not None:
            reason["score"] = score
        reasons.append(reason)

    keyword_score = retrieval_debug.get("keyword_lookup_score")
    if "keyword_lookup" in matched_retrievers or keyword_score is not None:
        reason = {"key": "keyword_recall", "label": "Keyword recall"}
        score = _format_reason_score(keyword_score)
        if score is not None:
            reason["score"] = score
        reasons.append(reason)

    return reasons


def _map_search_item(item: Dict[str, Any], rank: int) -> Dict[str, Any]:
    metadata = item.get("metadata") or item
    preferred_source = _get_preferred_source(metadata)
    source_name = preferred_source.get("source_name") or item.get("source_name")
    doi = _extract_doi(metadata)
    return {
        "work_id": item.get("work_id"),
        "rank": rank,
        "title": metadata.get("canonical_title") or metadata.get("title"),
        "abstract": metadata.get("canonical_abstract") or metadata.get("abstract"),
        "authors": _extract_authors(metadata),
        "source": _normalize_source_label(source_name),
        "source_key": _normalize_source_key(source_name),
        "online_date": _format_date_ymd(metadata.get("online_at") or item.get("online_at")),
        "link": _extract_paper_link(metadata, doi),
        "doi": doi,
        "ranking_score": _get_ranking_score(item),
        "match_reasons": _build_match_reasons(item),
    }
```

- [ ] **Step 4: Add notice/query/meta builders**

Affected files:

```text
Modify: app/routes/scholar.py
```

Add:

```python
def _build_query_notice(
    query: str,
    search_query: Optional[str],
    understanding: Optional[Dict[str, Any]],
    search_mode: str,
) -> Optional[Dict[str, Any]]:
    if search_mode == "vector":
        return {
            "type": "vector",
            "message": "已按原 query 执行向量检索。",
            "action": None,
        }

    if not understanding:
        return None

    intent = understanding.get("intent")
    route = understanding.get("route")
    matched_author = understanding.get("matched_author") or search_query
    suggested_author = understanding.get("suggested_author")
    corrected_query = understanding.get("corrected_query")
    normalized_query = understanding.get("normalized_query") or query

    if intent == "author_name" and route == "metadata_author" and matched_author:
        return {
            "type": "author_name",
            "message": f"已识别为作者名，正在根据作者 {matched_author} 完成搜索。",
            "action": {
                "label": "改用向量检索",
                "mode": "vector",
                "query": query,
            },
        }

    if intent == "author_name" and route == "author_suggestion" and suggested_author:
        return {
            "type": "author_suggestion",
            "message": f'未找到 "{query}" 的高置信作者匹配，是否搜索作者 {suggested_author}？',
            "action": {
                "label": f"搜索作者 {suggested_author}",
                "mode": "smart",
                "query": suggested_author,
            },
        }

    if corrected_query and corrected_query != normalized_query:
        return {
            "type": "query_correction",
            "message": f"已识别到可能的拼写错误，实际搜索 query 为: {corrected_query}",
            "action": {
                "label": "使用原 query 检索",
                "mode": "vector",
                "query": query,
            },
        }

    return None


def _build_query_payload(
    input_query: str,
    executed_query: Optional[str],
    search_mode: str,
    understanding: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "input": input_query,
        "executed": executed_query,
        "mode": search_mode,
        "intent": understanding.get("intent") or "unknown",
        "route": understanding.get("route") or "none",
        "corrected_query": understanding.get("corrected_query"),
        "matched_author": understanding.get("matched_author"),
        "suggested_author": understanding.get("suggested_author"),
    }
```

- [ ] **Step 5: Add `run_scholar_search`**

Affected files:

```text
Modify: app/routes/scholar.py
```

Add:

```python
def run_scholar_search(
    *,
    indexer: Any,
    query: str,
    top_k: Any = None,
    source_list: Optional[List[str]] = None,
    search_mode: str = "smart",
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    started_at = time.time()
    normalized_query = (query or "").strip()
    if not normalized_query:
        raise ValueError("query 不能为空")

    normalized_top_k = _normalize_top_k(top_k)
    normalized_mode = _validate_search_mode(search_mode)

    if normalized_mode == "vector":
        results = _search_by_prioritized_sources(
            indexer=indexer,
            query=normalized_query,
            source_list=source_list,
            top_k=normalized_top_k,
        )
        search_query = normalized_query
        understanding = {
            "original_query": normalized_query,
            "normalized_query": normalized_query,
            "intent": "semantic_search",
            "route": "vector",
            "corrected_query": None,
            "matched_author": None,
            "suggested_author": None,
        }
    else:
        understanding_result = indexer.query_understanding.analyze(normalized_query)
        understanding = understanding_result.to_dict()
        route = understanding_result.route

        if route == "none":
            results = []
            search_query = None
        elif route == "metadata_author":
            search_query = (
                understanding_result.matched_author
                or understanding_result.normalized_query
            )
            results = indexer.metadata_db.search_by_author(
                author_name=search_query,
                limit=normalized_top_k,
                source_list=_resolve_search_sources(indexer, source_list),
                fuzzy=True,
            )
        elif route == "author_suggestion":
            results = []
            search_query = None
        else:
            search_query = (
                understanding_result.corrected_query
                or understanding_result.normalized_query
            )
            results = _search_by_prioritized_sources(
                indexer=indexer,
                query=search_query,
                source_list=source_list,
                top_k=normalized_top_k,
            )

    mapped_results = [
        _map_search_item(item, rank=index + 1)
        for index, item in enumerate(results[:normalized_top_k])
    ]
    elapsed_ms = int((time.time() - started_at) * 1000)

    return {
        "success": True,
        "query": _build_query_payload(
            input_query=normalized_query,
            executed_query=search_query,
            search_mode=normalized_mode,
            understanding=understanding,
        ),
        "meta": {
            "count": len(mapped_results),
            "elapsed_ms": elapsed_ms,
            "request_id": request_id,
        },
        "notice": _build_query_notice(
            query=normalized_query,
            search_query=search_query,
            understanding=understanding,
            search_mode=normalized_mode,
        ),
        "results": mapped_results,
    }
```

- [ ] **Step 6: Add request logging helpers and route registration**

Affected files:

```text
Modify: app/routes/scholar.py
Read: src/docset_hub/logging/frontend_search_logger.py
```

Add:

```python
def _collect_request_args() -> Dict[str, Any]:
    return {
        "query": (request.args.get("query") or "").strip(),
        "mode": request.args.get("mode") or "smart",
        "top_k": request.args.get("top_k"),
        "source_list": request.args.get("source_list"),
    }


def register_scholar_search_api_routes(
    app: Any,
    indexer: Any,
    api_success: Callable[..., Any],
    api_error: Callable[..., Any],
    *,
    request_id_getter: Optional[Callable[[], str]] = None,
    record_frontend_search_request: Optional[Callable[..., Any]] = None,
    client_surface_getter: Optional[Callable[[], str]] = None,
) -> None:
    def current_request_id() -> str:
        if request_id_getter is None:
            return ""
        return request_id_getter()

    @app.route("/api/scholar/search", methods=["GET"])
    def api_scholar_search():
        try:
            request_id = current_request_id()
            data = run_scholar_search(
                indexer=indexer,
                query=(request.args.get("query") or "").strip(),
                top_k=request.args.get("top_k"),
                source_list=_parse_source_list(request.args.get("source_list")),
                search_mode=request.args.get("mode") or "smart",
                request_id=request_id,
            )
            if record_frontend_search_request is not None:
                record_frontend_search_request(
                    request_args=_collect_request_args(),
                    response_body=dict(data),
                    status_code=200,
                    client_surface=client_surface_getter() if client_surface_getter else "unknown",
                    request_path=request.path,
                    request_method=request.method,
                )
            return api_success(data)
        except ValueError as exc:
            response, status_code = api_error(
                str(exc),
                status_code=400,
                code="INVALID_REQUEST",
            )
            if record_frontend_search_request is not None:
                record_frontend_search_request(
                    request_args=_collect_request_args(),
                    response_body=response.get_json() or {},
                    status_code=status_code,
                    client_surface=client_surface_getter() if client_surface_getter else "unknown",
                    request_path=request.path,
                    request_method=request.method,
                )
            return response, status_code
        except Exception as exc:  # noqa: BLE001
            response, status_code = api_error(
                str(exc),
                status_code=500,
                code="SEARCH_FAILED",
            )
            if record_frontend_search_request is not None:
                record_frontend_search_request(
                    request_args=_collect_request_args(),
                    response_body=response.get_json() or {},
                    status_code=status_code,
                    client_surface=client_surface_getter() if client_surface_getter else "unknown",
                    request_path=request.path,
                    request_method=request.method,
                )
            return response, status_code
```

- [ ] **Step 7: Run targeted tests**

Affected files:

```text
Read: app/routes/scholar.py
Read: tests/app/test_scholar_routes.py
Write: pytest cache files only
```

```bash
PYTHONPATH=. python3 -m pytest tests/app/test_scholar_routes.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit implementation**

Affected files:

```text
Commit: app/routes/scholar.py
Commit: tests/app/test_scholar_routes.py
```

```bash
git add app/routes/scholar.py tests/app/test_scholar_routes.py
git commit -m "feat: add scholar search route v11 API"
```

---

## Task 3: Verify Route Module Isolation

**Files:**
- Test only: `tests/app/test_scholar_routes.py`, `tests/app/test_paper_routes.py`, `tests/app/test_app_directory_imports.py`

- [ ] **Step 1: Confirm `main.py` was not modified**

Affected files:

```text
Read: app/main.py
```

Run:

```bash
git diff -- app/main.py
```

Expected: no output.

- [ ] **Step 2: Run app route tests that should remain independent**

Affected files:

```text
Read: app/routes/scholar.py
Read: app/routes/paper.py
Read: app/routes/__init__.py
Read: app/pages/__init__.py
Read: app/dev/__init__.py
Read: tests/app/test_scholar_routes.py
Read: tests/app/test_paper_routes.py
Read: tests/app/test_app_directory_imports.py
Write: pytest cache files only
```

Run:

```bash
PYTHONPATH=. python3 -m pytest \
  tests/app/test_scholar_routes.py \
  tests/app/test_paper_routes.py \
  tests/app/test_app_directory_imports.py \
  -q
```

Expected: PASS.

- [ ] **Step 3: Run import compilation for the new route module**

Affected files:

```text
Read: app/routes/scholar.py
Write: app/routes/__pycache__/scholar.*.pyc
```

Run:

```bash
PYTHONPATH=. python3 -m py_compile app/routes/scholar.py
```

Expected: no output and exit code 0.

- [ ] **Step 4: Commit verification-only adjustments if any were needed**

Affected files:

```text
Commit if changed: app/routes/scholar.py
Commit if changed: tests/app/test_scholar_routes.py
```

If Step 2 or Step 3 required test-only fixes, commit them:

```bash
git add app/routes/scholar.py tests/app/test_scholar_routes.py
git commit -m "test: verify scholar route module isolation"
```

If no files changed, do not create an empty commit.

---

## Self-Review Checklist

- [ ] The route is implemented in `app/routes/scholar.py`.
- [ ] `app/main.py` is not modified.
- [ ] `limit` and `offset` are ignored by the public v1.1 route.
- [ ] The response does not include old v1 fields.
- [ ] `top_k` normalization is `1..100` with non-integer defaulting to `100`.
- [ ] `source_list` is parsed from CSV and validated against `indexer.default_sources`.
- [ ] Smart search routes author matches to `metadata_db.search_by_author`.
- [ ] Smart search returns author suggestion without vector search.
- [ ] Query correction searches the corrected query and offers a vector retry with the original query.
- [ ] Vector mode skips query understanding and adds the vector notice.
- [ ] Tests use fake indexers and do not require real services.
