from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from flask import Flask, jsonify
from sqlalchemy.engine import Engine

from app.feedback_review_page import register_feedback_review_routes
from config.config_loader import init_config


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT / "src" / "config" / "config_tecent_backend_server_mimic.yaml"


def _resolve_config_path(config_path: Optional[str | Path] = None) -> Path:
    if config_path is not None:
        candidate = Path(config_path)
    else:
        candidate = Path(
            os.environ.get(
                "PD_BACKEND_CONFIG",
                os.environ.get("PD_TEST_CONFIG", str(DEFAULT_CONFIG_PATH)),
            )
        )
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    return candidate


def _api_success(payload: Optional[Dict[str, Any]] = None, status_code: int = 200):
    body = dict(payload or {})
    body.setdefault("success", True)
    return jsonify(body), status_code


def _api_error(
    message: str,
    status_code: int = 500,
    code: str = "INTERNAL_ERROR",
    extra: Optional[Dict[str, Any]] = None,
):
    body: Dict[str, Any] = {
        "success": False,
        "error": message,
        "error_code": code,
    }
    if extra:
        body.update(extra)
    return jsonify(body), status_code


def create_feedback_review_app(
    *,
    config_path: Optional[str | Path] = None,
    testbed_path: Optional[Path] = None,
    engine_factory: Optional[Callable[[], Engine]] = None,
) -> Flask:
    resolved_config_path = _resolve_config_path(config_path)
    init_config(resolved_config_path)

    app = Flask(
        __name__,
        root_path=str(ROOT),
        template_folder="templates",
    )
    app.json.ensure_ascii = False
    register_feedback_review_routes(
        app,
        _api_success,
        _api_error,
        testbed_path=testbed_path,
        engine_factory=engine_factory,
    )
    return app


def main() -> int:
    app = create_feedback_review_app()
    host = os.environ.get("FEEDBACK_REVIEW_HOST", "0.0.0.0")
    port = int(os.environ.get("FEEDBACK_REVIEW_PORT", "5005"))
    debug = os.environ.get("FEEDBACK_REVIEW_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    app.run(host=host, port=port, debug=debug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
