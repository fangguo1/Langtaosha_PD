#!/usr/bin/env python3
"""Utilities for downloading bioRxiv metadata from the official API."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

BASE_URL = "https://connect.biorxiv.org/api/detail"
PAGE_SIZE = 100
USER_AGENT = "bioexiv-biorxiv-api-client/1.0"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def fetch_json(url: str, timeout: int) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    with urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        raw = response.read().decode(charset)
    return json.loads(raw)


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def append_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_details_url(
    server: str,
    start_date: str,
    end_date: str,
    cursor: int,
    category: str | None = None,
) -> str:
    del server
    if category:
        raise ValueError("The connect.biorxiv.org detail API does not support category filtering.")
    return f"{BASE_URL}/{start_date}/{end_date}/{cursor}"


def record_key(record: dict[str, Any]) -> str:
    return f"{record.get('doi', '')}::v{record.get('version', '')}"


def load_existing_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    if not path.exists():
        return keys

    if path.is_file():
        candidates = [path]
    else:
        candidates = sorted(path.rglob("*.jsonl"))

    for candidate in candidates:
        with candidate.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                keys.add(record_key(record))
    return keys


def append_records_by_date(root_dir: Path, records: list[dict[str, Any]]) -> list[str]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        day = str(record.get("date", "")).strip()
        if not day:
            raise RuntimeError("Daily update record is missing 'date'")
        grouped.setdefault(day, []).append(record)

    written_paths: list[str] = []
    for day, items in sorted(grouped.items()):
        year = day[:4]
        day_path = root_dir / year / f"{day}.jsonl"
        append_jsonl(day_path, items)
        written_paths.append(str(day_path))
    return written_paths


def extract_status(payload: dict[str, Any]) -> str:
    messages = payload.get("messages", [])
    if not messages:
        return ""
    return str(messages[0].get("status", ""))


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class RangeFetchResult:
    server: str
    start_date: str
    end_date: str
    category: str | None
    fetched_at: str
    records: list[dict[str, Any]]
    raw_pages: list[dict[str, Any]]


def fetch_date_range(
    server: str,
    start_date: str,
    end_date: str,
    timeout: int,
    category: str | None = None,
) -> RangeFetchResult:
    cursor = 0
    records: list[dict[str, Any]] = []
    raw_pages: list[dict[str, Any]] = []

    while True:
        url = build_details_url(server, start_date, end_date, cursor, category)
        payload = fetch_json(url, timeout=timeout)
        raw_pages.append(payload)

        status = extract_status(payload).lower()
        if status == "no posts found":
            break
        if status != "ok":
            raise RuntimeError(f"API returned status={status!r} for URL: {url}")

        page_records = payload.get("collection", [])
        if not isinstance(page_records, list):
            raise RuntimeError(f"Unexpected response shape for URL: {url}")

        records.extend(page_records)

        message = payload["messages"][0]
        total = parse_int(message.get("total"), default=len(records))
        count = parse_int(message.get("count"), default=len(page_records))

        if not page_records:
            break
        cursor += len(page_records)
        if cursor >= total:
            break
        if count < PAGE_SIZE or len(page_records) < PAGE_SIZE:
            break

    return RangeFetchResult(
        server=server,
        start_date=start_date,
        end_date=end_date,
        category=category,
        fetched_at=utc_now_iso(),
        records=records,
        raw_pages=raw_pages,
    )


def save_raw_pages(raw_pages: list[dict[str, Any]], directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for index, payload in enumerate(raw_pages, start=1):
        page_path = directory / f"page_{index:03d}.json"
        write_json(page_path, payload)


def find_latest_records(
    server: str,
    timeout: int,
    lookback_days: int,
    max_lookback_days: int,
    category: str | None = None,
) -> tuple[RangeFetchResult, str, list[dict[str, Any]]]:
    today = date.today()
    checked_days = 0
    last_result: RangeFetchResult | None = None

    while checked_days < max_lookback_days:
        window_limit = min(lookback_days, max_lookback_days - checked_days)
        for offset in range(window_limit):
            day = today - timedelta(days=checked_days + offset)
            day_str = day.isoformat()
            result = fetch_date_range(
                server=server,
                start_date=day_str,
                end_date=day_str,
                timeout=timeout,
                category=category,
            )
            last_result = result
            if result.records:
                return result, day_str, result.records
        checked_days += window_limit
        lookback_days *= 2

    raise RuntimeError(
        "No records found in latest lookup window. "
        f"Last checked interval: {last_result.start_date if last_result else '?'} "
        f"to {last_result.end_date if last_result else '?'}."
    )


def command_historical(args: argparse.Namespace) -> int:
    result = fetch_date_range(
        server=args.server,
        start_date=args.start_date,
        end_date=args.end_date,
        timeout=args.timeout,
        category=args.category,
    )
    payload = {
        "mode": "historical",
        "server": result.server,
        "query": {
            "start_date": result.start_date,
            "end_date": result.end_date,
            "category": result.category,
        },
        "fetched_at": result.fetched_at,
        "count": len(result.records),
        "records": result.records,
    }
    write_json(Path(args.output), payload)
    if args.raw_pages_dir:
        save_raw_pages(result.raw_pages, Path(args.raw_pages_dir))
    return 0


def command_latest(args: argparse.Namespace) -> int:
    result, latest_date, latest_records = find_latest_records(
        server=args.server,
        timeout=args.timeout,
        lookback_days=args.lookback_days,
        max_lookback_days=args.max_lookback_days,
        category=args.category,
    )
    payload = {
        "mode": "latest",
        "server": result.server,
        "query": {
            "lookback_days": args.lookback_days,
            "max_lookback_days": args.max_lookback_days,
            "category": result.category,
            "window_start_date": result.start_date,
            "window_end_date": result.end_date,
        },
        "fetched_at": result.fetched_at,
        "latest_available_date": latest_date,
        "count": len(latest_records),
        "records": latest_records,
    }
    write_json(Path(args.output), payload)
    if args.raw_pages_dir:
        save_raw_pages(result.raw_pages, Path(args.raw_pages_dir))
    return 0


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def command_daily_update(args: argparse.Namespace) -> int:
    output_path = Path(args.output)
    state_path = Path(args.state_file)
    state = load_state(state_path)

    today = date.today()
    last_max_date = state.get("last_max_date")
    if last_max_date:
        start = parse_date(last_max_date) - timedelta(days=args.overlap_days)
    else:
        start = today - timedelta(days=args.bootstrap_days - 1)
    end = today

    result = fetch_date_range(
        server=args.server,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        timeout=args.timeout,
        category=args.category,
    )

    existing_keys = load_existing_keys(output_path)
    new_records = []
    for record in result.records:
        key = record_key(record)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_records.append(record)

    written_paths: list[str] = []
    if new_records:
        if output_path.suffix.lower() == ".jsonl":
            append_jsonl(output_path, new_records)
            written_paths = [str(output_path)]
        else:
            written_paths = append_records_by_date(output_path, new_records)

    max_seen_date = state.get("last_max_date")
    if result.records:
        max_seen_date = max(str(record.get("date", "")) for record in result.records)

    new_state = {
        "server": args.server,
        "category": args.category,
        "last_run_at": utc_now_iso(),
        "last_query_start_date": result.start_date,
        "last_query_end_date": result.end_date,
        "last_max_date": max_seen_date,
        "total_records_in_store": len(existing_keys),
        "last_run_new_records": len(new_records),
    }
    write_json(state_path, new_state)

    if args.raw_pages_dir:
        save_raw_pages(result.raw_pages, Path(args.raw_pages_dir))

    summary = {
        "status": "ok",
        "output": str(output_path),
        "written_paths": written_paths,
        "state_file": str(state_path),
        "query_start_date": result.start_date,
        "query_end_date": result.end_date,
        "new_records": len(new_records),
        "total_records_in_store": len(existing_keys),
        "last_max_date": max_seen_date,
    }
    sys.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    return 0


def add_common_fetch_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--server",
        default="biorxiv",
        choices=["biorxiv", "medrxiv"],
        help="Server name kept for CLI compatibility. Only biorxiv is supported by this script.",
    )
    parser.add_argument(
        "--category",
        default=None,
        help="Optional subject category filter supported by the official API.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--raw-pages-dir",
        default=None,
        help="Optional directory for saving raw paginated API responses.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download bioRxiv metadata from the official API."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    historical = subparsers.add_parser(
        "historical", help="Download metadata for a historical date range."
    )
    add_common_fetch_arguments(historical)
    historical.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    historical.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    historical.add_argument(
        "--output",
        required=True,
        help="Output JSON file for merged metadata records.",
    )
    historical.set_defaults(func=command_historical)

    latest = subparsers.add_parser(
        "latest",
        help="Download the newest available metadata by scanning a recent date window.",
    )
    add_common_fetch_arguments(latest)
    latest.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Initial number of recent days to scan.",
    )
    latest.add_argument(
        "--max-lookback-days",
        type=int,
        default=60,
        help="Maximum recent-day scan window if no records are found.",
    )
    latest.add_argument(
        "--output",
        required=True,
        help="Output JSON file containing only the newest available date's records.",
    )
    latest.set_defaults(func=command_latest)

    daily_update = subparsers.add_parser(
        "daily-update",
        help="Incrementally append metadata to a JSONL file for daily automation.",
    )
    add_common_fetch_arguments(daily_update)
    daily_update.add_argument(
        "--output",
        required=True,
        help="JSONL file or directory used as the long-term metadata store.",
    )
    daily_update.add_argument(
        "--state-file",
        required=True,
        help="JSON file used to remember the last successful run.",
    )
    daily_update.add_argument(
        "--bootstrap-days",
        type=int,
        default=7,
        help="Initial lookback window used when no state file exists.",
    )
    daily_update.add_argument(
        "--overlap-days",
        type=int,
        default=1,
        help="Re-fetch overlap days on each run to tolerate late indexing.",
    )
    daily_update.set_defaults(func=command_daily_update)

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if getattr(args, "server", "biorxiv") != "biorxiv":
        raise ValueError("Only --server biorxiv is supported with the connect.biorxiv.org detail API")
    if getattr(args, "lookback_days", 1) < 1:
        raise ValueError("--lookback-days must be >= 1")
    if getattr(args, "max_lookback_days", 1) < getattr(args, "lookback_days", 1):
        raise ValueError("--max-lookback-days must be >= --lookback-days")
    if getattr(args, "bootstrap_days", 1) < 1:
        raise ValueError("--bootstrap-days must be >= 1")
    if getattr(args, "overlap_days", 0) < 0:
        raise ValueError("--overlap-days must be >= 0")

    start_date = getattr(args, "start_date", None)
    end_date = getattr(args, "end_date", None)
    if start_date and end_date:
        start = parse_date(start_date)
        end = parse_date(end_date)
        if end < start:
            raise ValueError("--end-date must be >= --start-date")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        validate_args(args)
        return args.func(args)
    except Exception as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
