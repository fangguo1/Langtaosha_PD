# Search Use Home Recent Langtaosha Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/search-use` show a clean homepage when no query is present: one main search box plus recent Langtaosha papers from the last month using `retrieve_papers_by_time_interval`.

**Architecture:** Keep one Flask template, `templates/langtaosha_smart_search.html`, and split behavior by `initial_query`. Empty query renders a homepage section and loads recent papers; non-empty query keeps the existing search result flow. Reuse the existing `POST /api/retrieve_papers_by_time_interval` route from `app.routes.paper` through the existing frontend proxy.

**Tech Stack:** Flask/Jinja templates, browser JavaScript, existing `main_search_use` frontend/API split, pytest template assertions

---

## File Structure

- Modify: `templates/langtaosha_smart_search.html`
  - Add homepage-only markup for recent Langtaosha papers.
  - Add CSS for the homepage search layout and recent-paper list.
  - Add JavaScript for homepage detection, one-month date range calculation, recent-paper API loading, response normalization, and search submit URL navigation.
- Modify: `tests/app/test_langtaosha_smart_search_page.py`
  - Add template contract tests for homepage containers, API endpoint usage, date interval logic, and preservation of existing search-result hooks.
- Reference: `app/pages/langtaosha_smart_search_page.py`
  - No expected code change; it already passes `initial_query`, `default_top_k`, and `client_surface`.
- Reference: `app/routes/paper.py`
  - No expected code change; it already exposes `POST /api/retrieve_papers_by_time_interval`.
- Reference: `tests/app/test_main_search_use.py`
  - No expected code change; it already verifies the interval API is exposed in `main_search_use`.

## Design Decisions

- Empty `/search-use` is the homepage state.
- `/search-use?q=<query>` is the search results state.
- Recent papers load only in homepage state.
- The recent interval is computed in the browser using local calendar dates: `date_to = today`, `date_from = today minus one month`.
- Search form submission navigates to `/search-use?q=<encoded query>` before running the existing search result logic. This keeps searched URLs refreshable and shareable.
- Recent-paper rendering accepts both paper-indexer metadata fields and scholar-normalized fields so the UI survives small backend payload differences.

---

### Task 1: Capture the Homepage Contract in Tests

**Files:**
- Modify: `tests/app/test_langtaosha_smart_search_page.py`
- Reference: `templates/langtaosha_smart_search.html`

- [ ] **Step 1: Add the failing homepage contract test**

Add this test below `test_langtaosha_smart_search_page_renders_template_with_query_defaults`:

```python
def test_langtaosha_smart_search_page_renders_home_recent_papers_shell():
    app = Flask(__name__, template_folder="../../templates")
    register_langtaosha_smart_search_page_routes(app)

    response = app.test_client().get("/search-use")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'data-has-initial-query="false"' in html
    assert 'id="homeRecentSection"' in html
    assert 'id="recentStatus"' in html
    assert 'id="recentList"' in html
    assert "Recent Langtaosha Papers" in html
    assert "loadRecentLangtaoshaPapers" in html
    assert "/api/retrieve_papers_by_time_interval" in html
```

- [ ] **Step 2: Add the search-state contract test**

Add this test below the homepage shell test:

```python
def test_langtaosha_smart_search_page_marks_search_state_when_query_is_present():
    app = Flask(__name__, template_folder="../../templates")
    register_langtaosha_smart_search_page_routes(app)

    response = app.test_client().get("/search-use?q=kidney%20fibrosis")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'data-has-initial-query="true"' in html
    assert "kidney fibrosis" in html
    assert "void runSearch(input.value.trim());" in html
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
python3 -m pytest tests/app/test_langtaosha_smart_search_page.py::test_langtaosha_smart_search_page_renders_home_recent_papers_shell tests/app/test_langtaosha_smart_search_page.py::test_langtaosha_smart_search_page_marks_search_state_when_query_is_present -q
```

Expected: FAIL because the template does not yet expose `data-has-initial-query`, homepage recent-paper containers, or `loadRecentLangtaoshaPapers`.

- [ ] **Step 4: Commit after implementation passes**

Commit only after Tasks 2-4 pass:

```bash
git add tests/app/test_langtaosha_smart_search_page.py templates/langtaosha_smart_search.html
git commit -m "feat: add search use homepage recent papers"
```

---

### Task 2: Add Homepage Markup and State Detection

**Files:**
- Modify: `templates/langtaosha_smart_search.html`
- Test: `tests/app/test_langtaosha_smart_search_page.py`

- [ ] **Step 1: Add template state to `<main>`**

Change the opening `<main>` tag to:

```html
<main
  class="page-shell"
  data-has-initial-query="{{ 'true' if initial_query else 'false' }}"
>
```

- [ ] **Step 2: Add the recent papers section after the search panel**

Place this section after the existing `</section>` that closes `.search-panel`, before `</main>`:

```html
    <section id="homeRecentSection" class="home-recent-section" aria-labelledby="homeRecentTitle">
      <div class="home-recent-header">
        <div>
          <p class="hero-kicker">Recent New</p>
          <h2 id="homeRecentTitle">Recent Langtaosha Papers</h2>
        </div>
        <p id="recentRange" class="recent-range"></p>
      </div>
      <div id="recentStatus" class="recent-status" aria-live="polite">Loading latest Langtaosha papers...</div>
      <div id="recentList" class="recent-list"></div>
    </section>
```

- [ ] **Step 3: Add homepage CSS**

Add these rules before the existing `@media (max-width: 720px)` block:

```css
    .home-recent-section {
      display: none;
      margin-top: 24px;
      padding: 20px;
      border: 1px solid var(--border-light);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.96);
      box-shadow: 0 12px 32px rgba(15, 76, 129, 0.05);
    }

    .page-shell[data-has-initial-query="false"] .home-recent-section {
      display: block;
    }

    .home-recent-header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 12px;
    }

    .home-recent-header h2 {
      margin: 0;
      color: var(--primary);
      font-size: 24px;
      line-height: 1.25;
    }

    .recent-range,
    .recent-status {
      color: var(--text-secondary);
      font-size: 14px;
      line-height: 1.6;
    }

    .recent-range {
      margin: 0;
      white-space: nowrap;
    }

    .recent-list {
      display: grid;
      gap: 14px;
      max-height: 640px;
      overflow-y: auto;
      padding-right: 4px;
    }
```

- [ ] **Step 4: Add mobile CSS**

Inside the existing `@media (max-width: 720px)` block, add:

```css
      .home-recent-header {
        flex-direction: column;
      }

      .recent-range {
        white-space: normal;
      }
```

- [ ] **Step 5: Run targeted tests**

Run:

```bash
python3 -m pytest tests/app/test_langtaosha_smart_search_page.py::test_langtaosha_smart_search_page_renders_home_recent_papers_shell tests/app/test_langtaosha_smart_search_page.py::test_langtaosha_smart_search_page_marks_search_state_when_query_is_present -q
```

Expected: The `data-has-initial-query` and container assertions pass. The test may still fail on `loadRecentLangtaoshaPapers` until Task 3 is implemented.

---

### Task 3: Load Recent Langtaosha Papers from the Interval API

**Files:**
- Modify: `templates/langtaosha_smart_search.html`
- Test: `tests/app/test_langtaosha_smart_search_page.py`

- [ ] **Step 1: Add JavaScript element references and homepage state**

Near the existing JavaScript constants, after `const resultsEl = document.getElementById("results");`, add:

```javascript
    const pageShell = document.querySelector(".page-shell");
    const hasInitialQuery = pageShell?.dataset.hasInitialQuery === "true";
    const homeRecentSectionEl = document.getElementById("homeRecentSection");
    const recentRangeEl = document.getElementById("recentRange");
    const recentStatusEl = document.getElementById("recentStatus");
    const recentListEl = document.getElementById("recentList");
```

- [ ] **Step 2: Add date interval helpers**

Add these functions after `formatDate(value)`:

```javascript
    function toDateOnly(value) {
      const year = value.getFullYear();
      const month = String(value.getMonth() + 1).padStart(2, "0");
      const day = String(value.getDate()).padStart(2, "0");
      return `${year}-${month}-${day}`;
    }

    function getRecentOneMonthInterval(referenceDate = new Date()) {
      const dateTo = new Date(referenceDate.getFullYear(), referenceDate.getMonth(), referenceDate.getDate());
      const dateFrom = new Date(dateTo);
      dateFrom.setMonth(dateFrom.getMonth() - 1);
      return {
        dateFrom: toDateOnly(dateFrom),
        dateTo: toDateOnly(dateTo),
      };
    }
```

- [ ] **Step 3: Add recent-paper normalization**

Add these functions after `renderItem(item)` so recent records can reuse the existing card renderer:

```javascript
    function normalizeAuthors(value) {
      if (Array.isArray(value)) {
        return value.map((author) => {
          if (typeof author === "string") return author;
          return author?.name || author?.full_name || "";
        }).filter(Boolean).join(", ");
      }
      return value || "";
    }

    function firstSourceRecord(item) {
      if (Array.isArray(item.sources) && item.sources.length) {
        return item.sources[0] || {};
      }
      return {};
    }

    function normalizeRecentPaper(item) {
      const sourceRecord = firstSourceRecord(item);
      return {
        title: item.title || item.canonical_title || "(Untitled)",
        abstract: item.abstract || item.canonical_abstract || "(No abstract)",
        authors: normalizeAuthors(item.authors || item.canonical_authors),
        doi: item.doi || sourceRecord.doi || "-",
        online_date: item.online_date || item.online_at || item.published_at || "",
        source: item.source || sourceRecord.source_name || "langtaosha",
        source_key: "langtaosha",
        link: item.link || item.url || sourceRecord.source_url || "",
      };
    }
```

- [ ] **Step 4: Add recent-paper rendering and API loading**

Add these functions after `normalizeRecentPaper(item)`:

```javascript
    function renderRecentPapers(papers) {
      if (!recentListEl || !recentStatusEl) return;

      const normalizedPapers = (papers || []).map(normalizeRecentPaper);
      if (!normalizedPapers.length) {
        recentStatusEl.textContent = "最近一个月暂无浪淘沙文章";
        recentListEl.innerHTML = "<div class='paper-card'>最近一个月暂无浪淘沙文章</div>";
        return;
      }

      recentStatusEl.textContent = `Latest papers: ${normalizedPapers.length}`;
      recentListEl.innerHTML = normalizedPapers.map((item) => renderItem(item)).join("");
    }

    async function loadRecentLangtaoshaPapers() {
      if (!homeRecentSectionEl || hasInitialQuery) return;

      const interval = getRecentOneMonthInterval();
      if (recentRangeEl) {
        recentRangeEl.textContent = `${interval.dateFrom} ~ ${interval.dateTo}`;
      }
      if (recentStatusEl) {
        recentStatusEl.textContent = "Loading latest Langtaosha papers...";
      }

      try {
        const response = await fetch("/api/retrieve_papers_by_time_interval", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            date_from: interval.dateFrom,
            date_to: interval.dateTo,
          }),
        });
        const payload = await response.json();

        if (!response.ok || !payload.success) {
          throw new Error(payload.error || "Failed to load latest Langtaosha papers");
        }

        renderRecentPapers(payload.papers || []);
      } catch (err) {
        if (recentStatusEl) {
          recentStatusEl.textContent = `Error: ${err.message}`;
        }
      }
    }
```

- [ ] **Step 5: Start recent loading only in homepage state**

Replace the final boot block:

```javascript
    if (input.value.trim()) {
      void runSearch(input.value.trim());
    }
```

with:

```javascript
    if (hasInitialQuery && input.value.trim()) {
      void runSearch(input.value.trim());
    } else {
      void loadRecentLangtaoshaPapers();
    }
```

- [ ] **Step 6: Add test assertions for the helper names**

Extend `test_langtaosha_smart_search_page_renders_home_recent_papers_shell` with:

```python
    assert "getRecentOneMonthInterval" in html
    assert "normalizeRecentPaper" in html
    assert "renderRecentPapers" in html
    assert "date_from: interval.dateFrom" in html
    assert "date_to: interval.dateTo" in html
```

- [ ] **Step 7: Run targeted tests**

Run:

```bash
python3 -m pytest tests/app/test_langtaosha_smart_search_page.py::test_langtaosha_smart_search_page_renders_home_recent_papers_shell tests/app/test_langtaosha_smart_search_page.py::test_langtaosha_smart_search_page_marks_search_state_when_query_is_present -q
```

Expected: PASS.

---

### Task 4: Preserve Shareable Search URLs on Submit

**Files:**
- Modify: `templates/langtaosha_smart_search.html`
- Test: `tests/app/test_langtaosha_smart_search_page.py`

- [ ] **Step 1: Update form submit behavior**

In the existing `form.addEventListener("submit", ...)` block, replace:

```javascript
      void runSearch(query, mode, correctionDecision);
```

with:

```javascript
      if (!hasInitialQuery && mode === "smart" && !correctionDecision) {
        const searchUrl = new URL(window.location.pathname, window.location.origin);
        searchUrl.searchParams.set("q", query);
        window.location.assign(searchUrl.toString());
        return;
      }

      void runSearch(query, mode, correctionDecision);
```

- [ ] **Step 2: Add a template test assertion for URL navigation**

Add these assertions to `test_langtaosha_smart_search_page_keeps_legacy_page_hooks`:

```python
    assert 'searchUrl.searchParams.set("q", query)' in html
    assert "window.location.assign(searchUrl.toString())" in html
```

- [ ] **Step 3: Run targeted tests**

Run:

```bash
python3 -m pytest tests/app/test_langtaosha_smart_search_page.py -q
```

Expected: PASS.

---

### Task 5: Verify the Existing API Contract Still Supports the Homepage

**Files:**
- Verify: `app/dev/main_search_use.py`
- Verify: `app/routes/paper.py`
- Verify: `tests/app/test_main_search_use.py`
- Verify: `tests/app/test_paper_routes.py`

- [ ] **Step 1: Run the interval API tests**

Run:

```bash
python3 -m pytest tests/app/test_main_search_use.py::test_create_search_use_api_app_exposes_retrieve_papers_by_time_interval_route tests/app/test_paper_routes.py::test_api_retrieve_papers_by_time_interval_accepts_json_body tests/app/test_paper_routes.py::test_api_retrieve_papers_by_time_interval_requires_both_dates -q
```

Expected: PASS.

- [ ] **Step 2: Run the frontend page tests**

Run:

```bash
python3 -m pytest tests/app/test_langtaosha_smart_search_page.py -q
```

Expected: PASS.

- [ ] **Step 3: Run compile checks on touched files**

Run:

```bash
python3 -m py_compile app/pages/langtaosha_smart_search_page.py app/dev/main_search_use.py app/routes/paper.py tests/app/test_langtaosha_smart_search_page.py
```

Expected: command exits with status 0.

- [ ] **Step 4: Optional manual smoke test with both servers**

Run:

```bash
python3 app/dev/main_search_use.py both
```

Open:

```text
http://127.0.0.1:5015/search-use
```

Expected:

- Empty homepage shows the main search box and a `Recent Langtaosha Papers` section.
- Browser network panel shows `POST /api/retrieve_papers_by_time_interval`.
- Searching for `kidney fibrosis` navigates to `/search-use?q=kidney%20fibrosis`.
- Search results render with the existing source/year filters.

Stop the server with `Ctrl+C`.

---

## Self-Review

- Spec coverage: The plan covers homepage-only behavior, recent one-month interval loading, reuse of `retrieve_papers_by_time_interval`, search result preservation, error/empty states, and targeted verification.
- Placeholder scan: No implementation step depends on unspecified names or deferred work.
- Type consistency: The plan uses existing Flask/Jinja variables (`initial_query`, `default_top_k`, `client_surface`) and existing API payload fields (`success`, `papers`, `date_from`, `date_to`).
- Scope check: This is one template-level feature backed by an existing API route; no new backend route or database behavior is required.
