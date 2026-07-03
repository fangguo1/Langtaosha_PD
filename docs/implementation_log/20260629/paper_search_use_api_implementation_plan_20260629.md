# Paper Search Use API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the paper-style `/api/search` route from `main_search_use.py` while keeping the existing scholar routes available and avoiding duplicate health-route registration.

**Architecture:** Reuse `app.routes.paper.register_paper_indexer_api_routes()` as the source of truth for the paper-format search contract, but add a small registration flag so embedded apps can opt out of the duplicate `/api/health` route. Then wire `create_search_use_api_app()` to register both route families against the same `PaperIndexer` instance.

**Tech Stack:** Flask, Python 3, existing app route modules, pytest-style app tests

---

### Task 1: Capture the desired `main_search_use` API surface

**Files:**
- Modify: `tests/app/test_main_search_use.py`
- Reference: `app/dev/main_search_use.py`

- [ ] **Step 1: Write the failing test**

```python
def test_create_search_use_api_app_exposes_paper_search_route():
    app = create_search_use_api_app(
        paper_indexer=FakeIndexer(),
        request_id_factory=lambda: "req-paper-search",
    )
    client = app.test_client()

    response = client.get("/api/search?query=Nav1.7&search_type=expanded_sparse&top_k=3")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["search_type"] == "expanded_sparse"
    assert payload["query"] == "Nav1.7"
    assert payload["results"][0]["work_id"] == "W1"
    assert payload["request_id"] == "req-paper-search"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/app/test_main_search_use.py::test_create_search_use_api_app_exposes_paper_search_route -q`
Expected: FAIL because `/api/search` is not registered in `create_search_use_api_app()`

- [ ] **Step 3: Write minimal implementation**

```python
from app.routes.paper import register_paper_indexer_api_routes

register_paper_indexer_api_routes(
    app,
    resolved_indexer,
    _api_success,
    _api_error,
    include_health_route=False,
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/app/test_main_search_use.py::test_create_search_use_api_app_exposes_paper_search_route -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/app/test_main_search_use.py app/dev/main_search_use.py app/routes/paper.py
git commit -m "feat: expose paper search route in search use api"
```

### Task 2: Prevent duplicate `/api/health` registration when embedding paper routes

**Files:**
- Modify: `app/routes/paper.py`
- Test: `tests/app/test_paper_routes.py`

- [ ] **Step 1: Write the failing test**

```python
def test_register_paper_routes_can_skip_health_registration():
    app = Flask(__name__)
    indexer = FakeIndexer()

    @app.route("/api/health", methods=["GET"])
    def api_health():
        return _json_success(app)({"status": "ok", "service": "custom"})

    register_paper_indexer_api_routes(
        app,
        indexer,
        _json_success(app),
        _json_error(app),
        include_health_route=False,
    )

    response = app.test_client().get("/api/health")

    assert response.status_code == 200
    assert response.get_json()["service"] == "custom"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/app/test_paper_routes.py::test_register_paper_routes_can_skip_health_registration -q`
Expected: FAIL because `register_paper_indexer_api_routes()` does not yet accept `include_health_route`

- [ ] **Step 3: Write minimal implementation**

```python
def register_paper_indexer_api_routes(
    app,
    indexer,
    api_success,
    api_error,
    *,
    include_health_route: bool = True,
) -> None:
    if include_health_route:
        @app.route("/api/health", methods=["GET"])
        def api_health():
            return api_success({"status": "ok", "service": "paper_indexer"})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/app/test_paper_routes.py::test_register_paper_routes_can_skip_health_registration -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/routes/paper.py tests/app/test_paper_routes.py
git commit -m "refactor: make paper route health registration optional"
```

### Task 3: Verify both route families coexist in the search-use API app

**Files:**
- Modify: `tests/app/test_main_search_use.py`
- Reference: `app/dev/main_search_use.py`, `app/routes/scholar.py`, `app/routes/paper.py`

- [ ] **Step 1: Write the failing coexistence assertion**

```python
def test_create_search_use_api_app_keeps_scholar_and_paper_routes():
    app = create_search_use_api_app(
        paper_indexer=FakeIndexer(),
        request_id_factory=lambda: "req-both-routes",
    )
    client = app.test_client()

    scholar_response = client.get("/api/scholar/search?query=Nav1.7")
    paper_response = client.get("/api/search?query=Nav1.7&search_type=dense")

    assert scholar_response.status_code == 200
    assert paper_response.status_code == 200
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/app/test_main_search_use.py::test_create_search_use_api_app_keeps_scholar_and_paper_routes -q`
Expected: FAIL before both route families are registered together

- [ ] **Step 3: Write minimal implementation**

```python
register_paper_indexer_api_routes(..., include_health_route=False)
register_scholar_search_api_routes(...)
```

- [ ] **Step 4: Run targeted tests to verify they pass**

Run: `python3 -m pytest tests/app/test_main_search_use.py tests/app/test_paper_routes.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/dev/main_search_use.py app/routes/paper.py tests/app/test_main_search_use.py tests/app/test_paper_routes.py
git commit -m "test: cover paper and scholar search routes together"
```

### Task 4: Structural verification for the edited files

**Files:**
- Verify: `app/dev/main_search_use.py`
- Verify: `app/routes/paper.py`
- Verify: `tests/app/test_main_search_use.py`
- Verify: `tests/app/test_paper_routes.py`

- [ ] **Step 1: Run Python compile checks**

Run: `python3 -m py_compile app/dev/main_search_use.py app/routes/paper.py tests/app/test_main_search_use.py tests/app/test_paper_routes.py`
Expected: command exits with status 0

- [ ] **Step 2: Run targeted pytest suite**

Run: `python3 -m pytest tests/app/test_main_search_use.py tests/app/test_paper_routes.py -q`
Expected: PASS; if pytest is unavailable in the environment, record that limitation explicitly in the handoff

- [ ] **Step 3: Summarize remaining risks**

```text
- Frontend still uses scholar-style `/api/scholar/search` payloads for smart-search UX.
- The new `/api/search` endpoint is paper-format only and is intended for direct API/debug usage.
- If a caller needs structured per-branch timings in scholar responses, that is a separate follow-up.
```
