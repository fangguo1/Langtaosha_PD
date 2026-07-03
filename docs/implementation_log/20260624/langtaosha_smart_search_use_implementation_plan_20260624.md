# Langtaosha Smart Search Use Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/search-use` use the new `_use` launcher/config while matching `http://43.143.246.163:5004/search` display behavior for smart search, including author recognition, query correction, source/year filtering, and result presentation.

**Architecture:** Treat `templates/search.html` as the behavioral source of truth and migrate its rendering contract into `templates/langtaosha_smart_search.html` instead of inventing a lighter-weight variant. Keep `app/dev/main_search_use.py` as the dedicated `_use` entrypoint, use `app/routes/scholar.py` as the adapter from `PaperIndexer.smart_search()` to the legacy frontend contract, and add explicit backend debug logging so returned smart-search state is visible in the API server console.

**Tech Stack:** Flask, Jinja templates, vanilla JavaScript, existing `PaperIndexer.smart_search`, existing frontend search logger, pytest

---

## File Structure

### Modified files

- `templates/langtaosha_smart_search.html`
  Migrate the old `templates/search.html` interaction model and rendering behavior, while keeping the new page route and the approved color scheme.
- `app/routes/scholar.py`
  Return the fields the old search page expects, sourced from `PaperIndexer.smart_search()`, and add an opt-in or default-readable backend debug summary for smart search responses.
- `tests/app/test_langtaosha_smart_search_page.py`
  Lock the page into the old search-page behavior: query notice actions, default source selection, empty-result messaging, filter behavior, and long-text wrapping.
- `tests/app/test_scholar_routes.py`
  Lock the API contract into the old page’s expectations and cover backend-visible debug output behavior.
- `tests/app/test_main_search_use.py`
  Keep launcher coverage green while the frontend/API contract is tightened.

### Existing files referenced but not modified unless verification proves necessary

- `templates/search.html`
  Behavioral reference for old page rendering logic.
- `app/main.py`
  Reference for old page data flow and search endpoint behavior.
- `src/docset_hub/indexing/paper_indexer.py`
  Source of truth for smart-search query understanding and routing.
- `app/dev/main_search_use.py`
  Existing `_use` launcher should remain the dedicated entrypoint.

---

### Task 1: Lock the API Contract to the Old Search Page’s Smart-Search Expectations

**Files:**
- Modify: `tests/app/test_scholar_routes.py`
- Reference: `templates/search.html`
- Reference: `app/main.py`
- Reference: `src/docset_hub/indexing/paper_indexer.py:713`

- [ ] **Step 1: Add failing tests for the full smart-search contract expected by the old page**

```python
def test_smart_mode_returns_legacy_page_contract_with_smart_payload():
    client = _client()

    response = client.get(
        "/api/scholar/search?query=brain+computer+interface&mode=smart&top_k=10&source_list=langtaosha,biorxiv_daily"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["success"] is True
    assert data["search_mode"] == "smart"
    assert data["search_query"] == "brain computer interface"
    assert data["query_understanding"]["route"] == "vector"
    assert data["smart_search"]["query_understanding"]["route"] == "vector"
    assert "results" in data["smart_search"]
    assert "notice" in data
```

```python
def test_smart_mode_author_suggestion_matches_old_page_notice_contract():
    indexer = FakeIndexer(
        FakeUnderstandingResult(
            route="author_suggestion",
            intent="author_name",
            normalized_query="niang yan",
            suggested_author="Nieng Yan",
        )
    )
    client = _client(indexer=indexer)

    response = client.get("/api/scholar/search?query=niang+yan&mode=smart&top_k=10")
    data = response.get_json()

    assert response.status_code == 200
    assert data["query"]["executed"] is None
    assert data["notice"]["type"] == "author_suggestion"
    assert data["notice"]["fallback_mode"] == "smart"
    assert data["notice"]["fallback_query"] == "Nieng Yan"
    assert data["notice"]["action_label"] == "搜索作者 Nieng Yan"
```

```python
def test_smart_mode_query_correction_matches_old_page_notice_contract():
    indexer = FakeIndexer(
        FakeUnderstandingResult(
            normalized_query="adhesion protin in kidney",
            corrected_query="adhesion protein in kidney",
        )
    )
    client = _client(indexer=indexer)

    response = client.get(
        "/api/scholar/search?query=adhesion+protin+in+kidney&mode=smart&top_k=10"
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["query"]["executed"] == "adhesion protein in kidney"
    assert data["notice"]["type"] == "query_correction"
    assert data["notice"]["fallback_mode"] == "vector"
    assert data["notice"]["fallback_query"] == "adhesion protin in kidney"
```

```python
def test_scholar_search_prints_backend_debug_summary_for_smart_requests(capsys):
    client = _client()

    response = client.get("/api/scholar/search?query=niang+yan&mode=smart&top_k=10")

    assert response.status_code == 200
    captured = capsys.readouterr()
    assert "SMART SEARCH DEBUG" in captured.out
    assert "query=niang yan" in captured.out
```

- [ ] **Step 2: Run the focused route tests to verify they fail before implementation**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest tests/app/test_scholar_routes.py -v
```

Expected:

```text
FAIL on missing old-page smart-search contract details and missing backend debug summary
```

- [ ] **Step 3: Commit the red-state API contract tests**

```bash
git add tests/app/test_scholar_routes.py
git commit -m "test: specify old search page smart contract"
```

---

### Task 2: Make `scholar.py` an Adapter for the Old Search Page Contract

**Files:**
- Modify: `app/routes/scholar.py`
- Test: `tests/app/test_scholar_routes.py`
- Reference: `app/main.py:858-938`
- Reference: `src/docset_hub/indexing/paper_indexer.py:713-773`

- [ ] **Step 1: Preserve the full smart-search payload and old-page notice fields**

```python
def _normalize_smart_search_payload(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    return {
        "success": payload.get("success"),
        "query": payload.get("query"),
        "search_query": payload.get("search_query"),
        "expanded_search_queries": list(payload.get("expanded_search_queries") or []),
        "query_understanding": dict(payload.get("query_understanding") or {}),
        "results": list(payload.get("results") or []),
    }
```

```python
notice = _normalize_legacy_notice(
    query=normalized_query,
    search_query=search_query,
    understanding=understanding,
    search_mode=normalized_mode,
)

return {
    "success": True,
    "query": _build_query_payload(...),
    "search_query": search_query,
    "search_mode": normalized_mode,
    "query_understanding": understanding,
    "smart_search": normalized_smart_payload if normalized_mode == "smart" else None,
    "notice": notice,
    "meta": {
        "count": len(mapped_results),
        "elapsed_ms": elapsed_ms,
        "request_id": request_id,
    },
    "count": len(mapped_results),
    "results": mapped_results,
}
```

- [ ] **Step 2: Add backend-readable debug output for smart-search responses**

```python
def _emit_smart_search_debug(
    *,
    query: str,
    search_mode: str,
    search_query: Optional[str],
    understanding: Dict[str, Any],
    result_count: int,
    notice: Optional[Dict[str, Any]],
) -> None:
    if search_mode != "smart":
        return
    print(
        "SMART SEARCH DEBUG | "
        f"query={query} | "
        f"executed={search_query} | "
        f"route={understanding.get('route')} | "
        f"intent={understanding.get('intent')} | "
        f"corrected={understanding.get('corrected_query')} | "
        f"suggested_author={understanding.get('suggested_author')} | "
        f"result_count={result_count} | "
        f"notice_type={(notice or {}).get('type')}"
    )
```

```python
_emit_smart_search_debug(
    query=normalized_query,
    search_mode=normalized_mode,
    search_query=search_query,
    understanding=understanding,
    result_count=len(mapped_results),
    notice=notice,
)
```

- [ ] **Step 3: Run the focused route tests to verify they now pass**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest tests/app/test_scholar_routes.py -v
```

Expected:

```text
PASS
```

- [ ] **Step 4: Commit the API adapter changes**

```bash
git add app/routes/scholar.py tests/app/test_scholar_routes.py
git commit -m "feat: align scholar api with old search smart behavior"
```

---

### Task 3: Migrate Old `search.html` Rendering Logic into `langtaosha_smart_search.html`

**Files:**
- Modify: `templates/langtaosha_smart_search.html`
- Test: `tests/app/test_langtaosha_smart_search_page.py`
- Reference: `templates/search.html:700-900`

- [ ] **Step 1: Add failing template tests for old-page rendering hooks and empty-state behavior**

```python
def test_langtaosha_page_keeps_old_search_notice_hooks():
    app = Flask(__name__, template_folder=TEMPLATE_DIR)
    register_langtaosha_smart_search_page_routes(app)

    html = app.test_client().get("/search-use").get_data(as_text=True)

    assert "renderQueryNotice(payload.notice)" in html
    assert 'selectedSource = allResults.some((item) => item.source_key === "langtaosha") ? "langtaosha" : "all";' in html
    assert "route === \"author_suggestion\"" in html
```

```python
def test_langtaosha_page_keeps_long_text_wrapping_guards():
    app = Flask(__name__, template_folder=TEMPLATE_DIR)
    register_langtaosha_smart_search_page_routes(app)

    html = app.test_client().get("/search-use").get_data(as_text=True)

    assert "overflow-wrap: anywhere;" in html
    assert "word-break: break-word;" in html
```

- [ ] **Step 2: Run the focused template tests to verify they fail before migration**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest tests/app/test_langtaosha_smart_search_page.py -v
```

Expected:

```text
FAIL on missing old-page rendering branches and hooks
```

- [ ] **Step 3: Migrate the old page’s search-state behavior into the new template while preserving approved styling**

```javascript
async function runSearch(query, mode = "smart") {
  const endpoint = form.dataset.apiEndpoint;
  const clientSurface = form.dataset.clientSurface;
  const url = new URL(endpoint, window.location.origin);
  url.searchParams.set("query", query);
  url.searchParams.set("mode", mode);
  url.searchParams.set("top_k", "100");

  statusEl.textContent = "Searching...";
  clearQueryNotice();
  filterBarEl.style.display = "none";
  resultsEl.innerHTML = "";

  const response = await fetch(url.toString(), {
    headers: {
      "X-Langtaosha-Client-Surface": clientSurface,
    },
  });
  const payload = await response.json();

  if (!response.ok || !payload.success) {
    statusEl.textContent = payload.error || "Search failed.";
    return;
  }

  currentSearchMeta = {
    query: payload.query,
    search_mode: payload.search_mode,
    search_query: payload.search_query,
  };
  allResults = payload.results || [];
  selectedSource = allResults.some((item) => item.source_key === "langtaosha")
    ? "langtaosha"
    : "all";
  selectedYear = "all";
  renderQueryNotice(payload.notice);

  if (!allResults.length) {
    const route = payload.query_understanding && payload.query_understanding.route;
    statusEl.textContent = route === "author_suggestion"
      ? "发现可能的作者名匹配，请确认是否按建议作者搜索。"
      : `Results: ${payload.meta.count}`;
    resultsEl.innerHTML = "<div class='paper-card'>暂无结果</div>";
    return;
  }

  filterBarEl.style.display = "flex";
  renderFilters();
  renderFilteredResults();
}
```

- [ ] **Step 4: Run the template tests to verify the migrated behavior now passes**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest tests/app/test_langtaosha_smart_search_page.py -v
```

Expected:

```text
PASS
```

- [ ] **Step 5: Commit the frontend migration**

```bash
git add templates/langtaosha_smart_search.html tests/app/test_langtaosha_smart_search_page.py
git commit -m "feat: migrate old search page smart rendering to search-use"
```

---

### Task 4: Verify Launcher Integration Still Matches the `_use` Entry Flow

**Files:**
- Modify: `tests/app/test_main_search_use.py`
- Reference: `app/dev/main_search_use.py`

- [ ] **Step 1: Add or update launcher tests to confirm the page and API still use the dedicated `_use` entrypoint**

```python
def test_create_search_use_api_app_uses_clean_scholar_routes():
    app = search_use.create_search_use_api_app(
        config_path="/tmp/config_use.yaml",
        record_frontend_search_request=lambda **kwargs: None,
    )
    client = app.test_client()

    response = client.get("/api/scholar/search?query=Nav1.7&mode=smart&top_k=5")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["search_mode"] == "smart"
    assert "smart_search" in payload
```

- [ ] **Step 2: Run the focused launcher tests**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest tests/app/test_main_search_use.py -v
```

Expected:

```text
PASS
```

- [ ] **Step 3: Commit any launcher-test updates if needed**

```bash
git add tests/app/test_main_search_use.py
git commit -m "test: keep search-use launcher aligned with old search contract"
```

---

### Task 5: Run Cross-Checks for the Full Search-Use Slice

**Files:**
- Verify only

- [ ] **Step 1: Run structural verification for changed Python files**

Run:

```bash
python3 -m py_compile \
  app/routes/scholar.py \
  tests/app/test_scholar_routes.py \
  tests/app/test_langtaosha_smart_search_page.py \
  tests/app/test_main_search_use.py
```

Expected:

```text
No output, exit 0
```

- [ ] **Step 2: Run the affected test slice**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/pytest \
  tests/app/test_scholar_routes.py \
  tests/app/test_langtaosha_smart_search_page.py \
  tests/app/test_main_search_use.py \
  tests/app/test_scholar_api_app.py \
  -v
```

Expected:

```text
All selected tests PASS
```

- [ ] **Step 3: Manual verification against the running `_use` app**

Run:

```bash
PYTHONPATH=. /home/wnlab/miniconda3/bin/python app/dev/main_search_use.py api
```

Then verify in browser or with `curl`:

```bash
curl 'http://127.0.0.1:5016/api/scholar/search?query=niang+yan&mode=smart&top_k=10'
curl 'http://127.0.0.1:5016/api/scholar/search?query=adhesion+protin+in+kidney&mode=smart&top_k=10'
```

Expected:

```text
Console prints SMART SEARCH DEBUG lines with route/correction/suggestion summary.
Author-suggestion and query-correction responses include the old-page notice contract.
The `/search-use` page shows the same smart-search notice behavior as the old `/search` page.
```

- [ ] **Step 4: Commit the final verification checkpoint**

```bash
git add app/routes/scholar.py templates/langtaosha_smart_search.html \
  tests/app/test_scholar_routes.py tests/app/test_langtaosha_smart_search_page.py \
  tests/app/test_main_search_use.py \
  docs/implementation_log/20260624/langtaosha_smart_search_use_implementation_plan_20260624.md
git commit -m "feat: align search-use with old smart search page behavior"
```

---

## Self-Review

- Spec coverage:
  - Old `/search` page rendering behavior as source of truth: covered by Tasks 1-3.
  - Smart-search author recognition and query correction display: covered by Tasks 1-3.
  - Backend-visible response summary: covered by Task 2 and manual verification in Task 5.
  - `_use` launcher/config preservation: covered by Task 4.
  - Page overflow fix for long text: covered by Task 3 template assertions.
- Placeholder scan:
  - No `TODO`/`TBD` placeholders remain.
  - Each task includes exact files, commands, and concrete code/test snippets.
- Type consistency:
  - Response keys consistently use `search_mode`, `search_query`, `query_understanding`, `smart_search`, `notice`, and `results`.

