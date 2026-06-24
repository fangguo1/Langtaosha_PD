from __future__ import annotations

from flask import Flask

from app.pages.langtaosha_smart_search_page import (
    DEFAULT_CLIENT_SURFACE,
    DEFAULT_TOP_K,
    register_langtaosha_smart_search_page_routes,
)


def test_langtaosha_smart_search_page_renders_template_with_query_defaults():
    app = Flask(__name__, template_folder="../../templates")
    register_langtaosha_smart_search_page_routes(app)

    response = app.test_client().get("/search-use?q=renal%20adhesion")
    html = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Langtaosha Smart Search" in html
    assert "Search preprints by concept, keyword, or author" in html
    assert "--primary: #0F4C81;" in html
    assert "--accent: #D98C2B;" in html
    assert 'id="search-form"' in html
    assert 'id="search-input"' in html
    assert 'id="results"' in html
    assert 'id="sourceFilters"' in html
    assert 'id="yearFilters"' in html
    assert "overflow-wrap: anywhere;" in html
    assert "word-break: break-word;" in html
    assert "min-width: 0;" in html
    assert 'data-api-endpoint="/api/scholar/search"' in html
    assert f'data-client-surface="{DEFAULT_CLIENT_SURFACE}"' in html
    assert "renal adhesion" in html
    assert str(DEFAULT_TOP_K) in html


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
    assert "renderFilters()" in html
    assert "renderFilteredResults()" in html
    assert "function renderItem(item)" in html
    assert "fallback_mode" in html
    assert "fallback_query" in html
    assert "action_label" in html
    assert "currentSearchMeta" in html
    assert 'url.searchParams.set("top_k", document.getElementById("top-k-input").value)' in html
    assert 'selectedSource = "langtaosha";' in html
    assert 'route === "author_suggestion"' in html
    assert "renderBiorxivJump" in html
    assert 'input.value = notice.fallback_query;' in html
