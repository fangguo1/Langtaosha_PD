from __future__ import annotations

from flask import render_template, request

DEFAULT_COMPARE_STRATEGIES = ("dense", "sparse", "hybrid", "hybrid_retrieval")


def register_retrieval_compare_page_routes(app) -> None:
    @app.route("/retrieval-compare")
    def retrieval_compare_page() -> str:
        initial_query = (request.args.get("q") or "").strip()
        return render_template(
            "retrieval_compare.html",
            initial_query=initial_query,
            compare_strategies=list(DEFAULT_COMPARE_STRATEGIES),
        )
