from __future__ import annotations

from flask import Flask

from app.pages.langtaosha_smart_search_page import (
    DEFAULT_CLIENT_SURFACE,
    DEFAULT_TOP_K,
    register_langtaosha_smart_search_page_routes,
)


def test_langtaosha_smart_search_page_uses_higher_default_top_k():
    assert DEFAULT_TOP_K == 100


def test_langtaosha_smart_search_page_renders_template_with_query_defaults():
    app = Flask(__name__, template_folder="../../templates")
    register_langtaosha_smart_search_page_routes(app)

    response = app.test_client().get("/search-use?q=renal%20adhesion")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "LangTaosha Smart Search Demo" in html
    assert "<h1>Smart Search Demo</h1>" not in html
    assert "Langtaosha Smart Search" not in html
    assert "font-weight: 400;" not in html
    assert "Making academic search understand you better." in html
    assert 'class="hero-content"' in html
    assert "padding: 12px 32px;" in html
    assert "min-height: 116px;" in html
    assert 'class="hero-subtitle"' in html
    assert "font-size: 15px;" in html
    assert "color: #D1D5DB;" in html
    assert "Search preprints by concept, keyword, or author" not in html
    assert "hero-copy" not in html
    assert "Langtaosha Academic Search" not in html
    assert 'class="hero-brand"' in html
    assert "font-size: 44px;" in html
    assert '<span class="brand-initial">L</span>ang<span class="brand-initial">T</span>ao<span class="brand-initial">S</span>ha' in html
    assert '<span class="hero-brand-title">Smart Search Demo</span>' in html
    assert ".brand-initial" in html
    assert "color: #F2D16B;" in html
    assert 'class="header-left"' in html
    assert 'href="https://langtaosha.org.cn/"' in html
    assert 'href="http://43.143.246.163:5015/search-use"' in html
    assert 'class="logo-link"' in html
    assert 'class="logo-image"' in html
    assert 'src="/lib/ui-library/src/resources/ltslogo_new.png"' in html
    assert "width: 116px;" in html
    assert "height: 116px;" in html
    assert "width: 94px;" in html
    assert "background: #244C94;" in html
    assert "border: 1px solid #244C94;" in html
    assert "background: #244C94;\n      box-shadow: none;" in html
    assert '<link rel="icon" type="image/png" href="/lib/ui-library/src/resources/favicon_en.png" />' in html
    assert "--primary: #0F4C81;" in html
    assert "--accent: #D98C2B;" in html
    assert 'id="search-form"' in html
    assert 'id="search-input"' in html
    assert 'id="results"' in html
    assert 'id="sourceFilters"' in html
    assert 'id="yearFilters"' in html
    assert 'id="sortSelect"' in html
    assert 'class="filter-main"' in html
    assert "grid-template-columns: minmax(0, 1fr) 220px;" in html
    assert "align-self: start;" in html
    assert 'filterBarEl.style.display = "grid";' in html
    assert "sort-group" in html
    assert "height: 26px;" in html
    assert "font-size: 12px;" in html
    assert '<option value="relevance" selected>Sort by Relevance</option>' in html
    assert '<option value="recency">Sort by Recency</option>' in html
    assert 'let selectedSort = "relevance";' in html
    assert 'function parseOnlineDateForSort(item)' in html
    assert 'function sortResults(items)' in html
    assert 'selectedSort === "recency"' in html
    assert 'sortSelectEl.addEventListener("change"' in html
    assert ".search-loading" in html
    assert ".search-spinner" in html
    assert "@keyframes spin" in html
    assert "function showSearchingStatus()" in html
    assert 'statusEl.innerHTML = `<span class="search-loading">' in html
    assert "showSearchingStatus();" in html
    assert "evidence-tags" not in html
    assert "renderRetrievalReasons" not in html
    assert "Reason:" not in html
    assert ".concept-keyword-tags" in html
    assert ".concept-keyword-tag" in html
    assert "border: 1px solid rgba(217, 140, 43, 0.24);" in html
    assert "background: rgba(217, 140, 43, 0.08);" in html
    assert "function renderConceptKeywords(item)" in html
    assert 'item.source_key !== "langtaosha"' in html
    assert 'String(keyword?.source || "").trim().toLowerCase() === "langtaosha"' in html
    assert 'keyword?.keyword_type || "").trim().toLowerCase() === "concept"' in html
    assert "<strong>Keywords:</strong>" in html
    assert "keyword?.keyword_text" in html
    assert "${renderConceptKeywords(item)}" in html
    assert "overflow-wrap: anywhere;" in html
    assert "word-break: break-word;" in html
    assert "min-width: 0;" in html
    assert 'data-api-endpoint="/api/scholar/search"' in html
    assert f'data-client-surface="{DEFAULT_CLIENT_SURFACE}"' in html
    assert "renal adhesion" in html
    assert str(DEFAULT_TOP_K) in html


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
    assert 'id="recentRange"' not in html
    assert "Recent Langtaosha Papers" in html
    assert "loadRecentLangtaoshaPapers" in html
    assert "/api/retrieve_papers_by_time_interval" in html
    assert "getRecentOneMonthInterval" in html
    assert "normalizeRecentPaper" in html
    assert "renderRecentPapers" in html
    assert "date_from: interval.dateFrom" in html
    assert "date_to: interval.dateTo" in html
    assert "Latest papers:" not in html
    assert ".page-shell[data-has-initial-query=\"false\"] #status" in html
    assert ".page-shell[data-has-initial-query=\"false\"] #queryNotice" in html


def test_langtaosha_smart_search_page_marks_search_state_when_query_is_present():
    app = Flask(__name__, template_folder="../../templates")
    register_langtaosha_smart_search_page_routes(app)

    response = app.test_client().get("/search-use?q=kidney%20fibrosis")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert 'data-has-initial-query="true"' in html
    assert "kidney fibrosis" in html
    assert "void runSearch(input.value.trim());" in html


def test_langtaosha_smart_search_page_module_is_importable():
    import app.pages.langtaosha_smart_search_page as page_module

    assert hasattr(page_module, "register_langtaosha_smart_search_page_routes")


def test_langtaosha_smart_search_page_keeps_legacy_page_hooks():
    app = Flask(__name__, template_folder="../../templates")
    register_langtaosha_smart_search_page_routes(app)

    response = app.test_client().get("/search-use")
    html = response.get_data(as_text=True)

    assert 'url.searchParams.set("mode", mode)' in html
    assert 'X-Langtaosha-Client-Surface' in html
    assert "renderQueryNotice" in html
    assert ".query-notice-actions" in html
    assert "gap: 6px;" in html
    assert 'actionsEl.className = "query-notice-actions";' in html
    assert "actionsEl.appendChild(btn);" in html
    assert "renderFilters()" in html
    assert "renderFilteredResults()" in html
    assert "function renderItem(item)" in html
    assert "fallback_mode" in html
    assert "fallback_query" in html
    assert "action_label" in html
    assert "currentSearchMeta" in html
    assert 'url.searchParams.set("top_k", document.getElementById("top-k-input").value)' in html
    assert 'id="correction-decision-input"' in html
    assert 'correctionDecisionInput.value = correctionDecision || "";' in html
    assert 'form.requestSubmit();' in html
    assert 'const correctionDecision = correctionDecisionInput.value || null;' in html
    assert "const displayedQuery = payload.search_query || payload.query?.executed || query;" in html
    assert "input.value = displayedQuery;" in html
    assert 'selectedSource = "langtaosha";' in html
    assert 'route === "author_suggestion"' in html
    assert "renderBiorxivJump" in html
    assert "符合要求的浪淘沙的结果已展示，想查看来自Biorxiv的结果？" in html
    assert 'data-action="show-biorxiv-link"' in html
    assert "submitNoticeAction(actionQuery, action.mode || \"smart\", action.correction_decision || null);" in html
    assert 'searchUrl.searchParams.set("q", query)' in html
    assert "window.location.assign(searchUrl.toString())" in html
