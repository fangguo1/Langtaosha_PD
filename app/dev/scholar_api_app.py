from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from flask import Flask, g, jsonify, request


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from app.routes.scholar import register_scholar_search_api_routes
from config.config_loader import init_config
from src.docset_hub.indexing import PaperIndexer
from src.docset_hub.logging import record_frontend_search_request


DEFAULT_CONFIG_PATH = ROOT / "src" / "config" / "config_tecent_backend_server_mimic.yaml"
DEFAULT_PORT = 5173
CLIENT_SURFACE_HEADER = "X-Langtaosha-Client-Surface"
DEFAULT_ALLOWED_ORIGINS = (
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:5004",
    "http://127.0.0.1:5004",
)


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


def _parse_allowed_origins(raw_value: str) -> tuple[str, ...]:
    if not raw_value.strip():
        return DEFAULT_ALLOWED_ORIGINS
    return tuple(item.strip() for item in raw_value.split(",") if item.strip())


def _resolve_cors_origin(
    origin: Optional[str],
    allowed_origins: tuple[str, ...],
) -> Optional[str]:
    if "*" in allowed_origins:
        return origin or "*"
    if origin and origin in allowed_origins:
        return origin
    return None


def _api_success(payload: Optional[Dict[str, Any]] = None, status_code: int = 200):
    body = dict(payload or {})
    body.setdefault("success", True)
    body["request_id"] = _request_id()
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
        "error_detail": {
            "code": code,
            "message": message,
            "request_id": _request_id(),
        },
        "request_id": _request_id(),
    }
    if extra:
        body.update(extra)
    return jsonify(body), status_code


def _request_id() -> str:
    return getattr(g, "request_id", "")


def _resolve_client_surface() -> str:
    return (request.headers.get(CLIENT_SURFACE_HEADER) or "scholar_api_app").strip()


def _create_paper_indexer(
    *,
    config_path: Path,
    paper_indexer_factory: Optional[Callable[[Path], Any]] = None,
) -> Any:
    if paper_indexer_factory is not None:
        return paper_indexer_factory(config_path)
    init_config(config_path)
    return PaperIndexer(config_path=config_path, enable_vectorization=True)


def create_scholar_api_app(
    *,
    config_path: Optional[str | Path] = None,
    paper_indexer: Optional[Any] = None,
    paper_indexer_factory: Optional[Callable[[Path], Any]] = None,
    allowed_origins: Optional[tuple[str, ...]] = None,
    request_id_factory: Optional[Callable[[], str]] = None,
) -> Flask:
    resolved_config_path = _resolve_config_path(config_path)
    resolved_indexer = paper_indexer or _create_paper_indexer(
        config_path=resolved_config_path,
        paper_indexer_factory=paper_indexer_factory,
    )
    resolved_allowed_origins = allowed_origins or _parse_allowed_origins(
        os.environ.get("FRONTEND_ALLOWED_ORIGINS", "")
    )
    request_id_factory = request_id_factory or (lambda: uuid.uuid4().hex)

    app = Flask(__name__)
    app.json.ensure_ascii = False

    @app.before_request
    def assign_request_id():
        g.request_id = request_id_factory()
        if request.path.startswith("/api/"):
            return None
        return "Not Found", 404

    @app.after_request
    def attach_api_headers(response):
        response.headers["X-Request-Id"] = _request_id()
        if request.path.startswith("/api/"):
            allowed_origin = _resolve_cors_origin(
                request.headers.get("Origin"),
                resolved_allowed_origins,
            )
            if allowed_origin:
                response.headers["Access-Control-Allow-Origin"] = allowed_origin
                response.headers["Access-Control-Allow-Credentials"] = "true"
                response.headers["Vary"] = "Origin"
            response.headers["Access-Control-Allow-Headers"] = (
                f"Authorization, Content-Type, X-Request-Id, X-Correlation-Id, {CLIENT_SURFACE_HEADER}"
            )
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        return response

    @app.route("/api/health", methods=["GET"])
    def api_health():
        return _api_success({"status": "ok", "service": "scholar_search_api"})

    register_scholar_search_api_routes(
        app,
        resolved_indexer,
        _api_success,
        _api_error,
        request_id_getter=_request_id,
        record_frontend_search_request=record_frontend_search_request,
        client_surface_getter=_resolve_client_surface,
    )

    return app


def _run_flask_app(app: Flask, *, host: str, port: int, debug: bool) -> None:
    app.run(host=host, port=port, debug=debug, use_reloader=False)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Langtaosha scholar search API server")
    parser.add_argument("--host", default=os.environ.get("SCHOLAR_API_HOST", "0.0.0.0"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("SCHOLAR_API_PORT", str(DEFAULT_PORT))),
    )
    parser.add_argument(
        "--config-path",
        default=os.environ.get("PD_BACKEND_CONFIG") or os.environ.get("PD_TEST_CONFIG"),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=os.environ.get("SCHOLAR_API_DEBUG", "").strip().lower()
        in {"1", "true", "yes", "on"},
    )
    args = parser.parse_args(argv)

    app = create_scholar_api_app(config_path=args.config_path)
    _run_flask_app(app, host=args.host, port=args.port, debug=args.debug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
