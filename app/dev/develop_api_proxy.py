from __future__ import annotations

from typing import Iterable, Optional

import requests
from flask import Response, request


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def _forward_request_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    for key, value in request.headers.items():
        if key.lower() in HOP_BY_HOP_HEADERS:
            continue
        if key.lower() == "host":
            continue
        headers[key] = value
    return headers


def _build_proxy_response(upstream: requests.Response) -> Response:
    excluded_headers = HOP_BY_HOP_HEADERS | {"content-encoding", "content-length"}
    headers = [
        (key, value)
        for key, value in upstream.headers.items()
        if key.lower() not in excluded_headers
    ]
    return Response(upstream.content, upstream.status_code, headers)


def register_develop_api_proxy(app, *, api_base_url: str) -> None:
    normalized_base = api_base_url.rstrip("/")

    @app.route("/api/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    @app.route("/api/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    def proxy_develop_api(path: str):
        target_url = f"{normalized_base}/api/{path}"
        try:
            upstream = requests.request(
                method=request.method,
                url=target_url,
                params=request.args,
                data=request.get_data(),
                headers=_forward_request_headers(),
                cookies=request.cookies,
                allow_redirects=False,
                timeout=120,
            )
        except requests.RequestException as exc:
            return Response(
                f'{{"success": false, "error": "develop API unavailable: {exc}"}}',
                status=502,
                mimetype="application/json",
            )
        return _build_proxy_response(upstream)


def register_develop_api_cors(
    app,
    *,
    allowed_origins: Optional[Iterable[str]] = None,
) -> None:
    resolved_origins = list(allowed_origins or ())

    @app.after_request
    def attach_develop_api_cors(response):
        if not request.path.startswith("/api/"):
            return response

        origin = request.headers.get("Origin")
        if origin and (not resolved_origins or origin in resolved_origins):
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        return response
