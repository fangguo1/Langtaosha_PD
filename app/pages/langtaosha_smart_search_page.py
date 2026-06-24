from __future__ import annotations

from flask import render_template, request


DEFAULT_TOP_K = 10
DEFAULT_CLIENT_SURFACE = "search_use_page"


def register_langtaosha_smart_search_page_routes(app) -> None:
    @app.route("/search-use")
    def langtaosha_smart_search_page() -> str:
        initial_query = (request.args.get("q") or "").strip()
        return render_template(
            "langtaosha_smart_search.html",
            initial_query=initial_query,
            default_top_k=DEFAULT_TOP_K,
            client_surface=DEFAULT_CLIENT_SURFACE,
        )
