from __future__ import annotations

from flask import render_template, request


DEFAULT_TOP_K = 10


def register_expanded_compare_page_routes(app) -> None:
    @app.route("/expanded-compare")
    def expanded_compare_page() -> str:
        initial_query = (request.args.get("q") or "").strip()
        return render_template(
            "expanded_compare.html",
            initial_query=initial_query,
            default_top_k=DEFAULT_TOP_K,
        )
