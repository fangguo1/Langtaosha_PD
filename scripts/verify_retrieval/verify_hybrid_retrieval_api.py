#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:5016")
DEFAULT_TIMEOUT_SECONDS = 30.0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify the real /api/search endpoint for hybrid retrieval.",
    )
    parser.add_argument("query", help="Query string sent to /api/search")
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Base API URL, default: %(default)s",
    )
    parser.add_argument("--top-k", type=int, default=10, help="Search top_k")
    parser.add_argument(
        "--source-list",
        default="langtaosha,biorxiv_history,biorxiv_daily",
        help="Comma-separated source_list",
    )
    parser.add_argument(
        "--keyword-sources",
        default=os.environ.get("KEYWORD_SOURCES"),
        help="Comma-separated keyword_sources",
    )
    parser.add_argument(
        "--hydrate",
        default="1",
        choices=("0", "1"),
        help="Whether to hydrate results, default: %(default)s",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds, default: %(default)s",
    )
    parser.add_argument(
        "--request-id",
        default=None,
        help="Optional X-Request-Id header prefix. Defaults to a generated UUID4 hex.",
    )
    parser.add_argument(
        "--auth-token",
        default=os.environ.get("API_AUTH_TOKEN") or os.environ.get("API_AUTH_TOKENS", ""),
        help="Optional bearer token. Defaults to API_AUTH_TOKEN or API_AUTH_TOKENS.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print only the response summary without the full JSON body.",
    )
    return parser.parse_args(argv)


def build_request_url(args: argparse.Namespace) -> str:
    params = {
        "query": args.query,
        "search_type": "hybrid_retrieval",
        "top_k": str(args.top_k),
        "hydrate": args.hydrate,
    }
    if args.source_list:
        params["source_list"] = args.source_list
    if args.keyword_sources:
        params["keyword_sources"] = args.keyword_sources
    query_string = urllib.parse.urlencode(params)
    return f"{args.base_url.rstrip('/')}/api/search?{query_string}"


def build_headers(args: argparse.Namespace) -> Dict[str, str]:
    request_id_prefix = args.request_id or uuid.uuid4().hex
    headers = {
        "Accept": "application/json",
        "X-Request-Id": request_id_prefix,
    }
    if args.auth_token:
        headers["Authorization"] = f"Bearer {args.auth_token.split(',')[0].strip()}"
    return headers


def perform_request(
    *,
    url: str,
    headers: Dict[str, str],
    timeout: float,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
) -> Dict[str, Any]:
    request = urllib.request.Request(url, headers=headers, method="GET")
    started = time.perf_counter()
    try:
        with urlopen(request, timeout=timeout) as response:
            body_bytes = response.read()
            status_code = getattr(response, "status", None) or response.getcode()
            response_headers = dict(getattr(response, "headers", {}))
    except urllib.error.HTTPError as exc:
        body_bytes = exc.read()
        status_code = exc.code
        response_headers = dict(exc.headers.items())
    client_elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
    body_text = body_bytes.decode("utf-8", errors="replace")
    return {
        "status_code": status_code,
        "headers": response_headers,
        "body_text": body_text,
        "client_elapsed_ms": client_elapsed_ms,
    }


def _best_effort_json(body_text: str) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else {"raw": payload}


def summarize_payload(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = payload or {}
    results = payload.get("results") or []
    first_result = results[0] if results else {}
    metadata = first_result.get("metadata") or {}
    return {
        "success": payload.get("success"),
        "request_id": payload.get("request_id"),
        "query": payload.get("query"),
        "search_type": payload.get("search_type"),
        "top_k": payload.get("top_k"),
        "server_elapsed_ms": payload.get("elapsed_ms"),
        "timings_ms": payload.get("timings_ms"),
        "result_count": len(results),
        "first_work_id": first_result.get("work_id"),
        "first_paper_id": first_result.get("paper_id"),
        "first_score": first_result.get("score") or first_result.get("similarity"),
        "first_title": metadata.get("canonical_title") or first_result.get("title"),
    }


def summarize_results(payload: Optional[Dict[str, Any]]) -> list[Dict[str, Any]]:
    payload = payload or {}
    rows: list[Dict[str, Any]] = []
    for item in payload.get("results") or []:
        metadata = item.get("metadata") or {}
        rows.append(
            {
                "work_id": item.get("work_id"),
                "title": metadata.get("canonical_title") or item.get("title"),
            }
        )
    return rows


def main(
    argv: Optional[list[str]] = None,
    *,
    stdout = sys.stdout,
    stderr = sys.stderr,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
) -> int:
    args = parse_args(argv)
    url = build_request_url(args)
    headers = build_headers(args)
    stdout.write("== hybrid_retrieval request ==\n")
    stdout.write(f"url: {url}\n")
    stdout.write(f"request_id: {headers['X-Request-Id']}\n")
    stdout.write("\n")

    try:
        result = perform_request(
            url=url,
            headers=headers,
            timeout=args.timeout,
            urlopen=urlopen,
        )
    except (TimeoutError, socket.timeout) as exc:
        stderr.write(
            "Request timed out before the API returned a response. "
            f"timeout={args.timeout}s; error={exc}\n"
        )
        stderr.write("Try a larger timeout, for example: --timeout 120\n")
        return 1
    except urllib.error.URLError as exc:
        stderr.write(f"Request failed before receiving a response: {exc}\n")
        return 1

    payload = _best_effort_json(result["body_text"])
    summary = summarize_payload(payload)
    summary["http_status"] = result["status_code"]
    summary["client_elapsed_ms"] = result["client_elapsed_ms"]
    result_rows = summarize_results(payload)

    stdout.write("== response summary ==\n")
    stdout.write(json.dumps(summary, ensure_ascii=False, indent=2))
    stdout.write("\n")

    if result_rows:
        stdout.write("\n== papers ==\n")
        stdout.write(json.dumps(result_rows, ensure_ascii=False, indent=2))
        stdout.write("\n")
    elif not args.summary_only and payload is None:
        stdout.write("\n== raw response body ==\n")
        stdout.write(result["body_text"])
        stdout.write("\n")

    if result["status_code"] >= 400:
        stderr.write(f"Request failed with HTTP {result['status_code']}\n")
        return 1
    if payload is not None and payload.get("success") is False:
        stderr.write("API returned success=false\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
