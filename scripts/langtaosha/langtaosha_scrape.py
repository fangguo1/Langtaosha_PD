#!/usr/bin/env python3
"""Scrape LangTaoSha preprint metadata from sitemap and article pages.

Modes:
- snapshot: refresh the full sitemap/raw/normalized snapshot
- daily-update: write per-day normalized JSONL files under daily/YYYY/YYYY-MM-DD.jsonl

Snapshot outputs:
- sitemap/langtaosha_en_sitemap.xml
- urls/preprint_urls.json
- raw/articles_raw.jsonl
- normalized/papers_external_search.jsonl
- summary.json

Daily outputs:
- daily/YYYY/YYYY-MM-DD.jsonl
- daily_state.json
- daily_summary.json
"""

from __future__ import annotations

import argparse
import json
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SITEMAP_URL = "https://langtaosha.org.cn/lts/en/sitemap"
USER_AGENT = "langtaosha-scraper/1.0"
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_lastmod_date(value: str | None) -> date | None:
    if not value:
        return None
    match = DATE_PATTERN.search(value)
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(0))
    except ValueError:
        return None


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    ensure_parent(path)
    path.write_text(text, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, payloads: list[dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for item in payloads:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_text(url: str, timeout: int, retries: int, sleep_seconds: float) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
            with urlopen(req, timeout=timeout) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, errors="replace")
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            if attempt == retries:
                raise
            time.sleep(sleep_seconds * attempt)
    if last_error is None:
        raise RuntimeError(f"Failed to fetch {url}")
    raise last_error


class MetaTagParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.meta_tags: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "meta":
            return
        record: dict[str, str] = {}
        for key, value in attrs:
            if value is None:
                continue
            record[key.lower()] = value
        self.meta_tags.append(record)


def parse_sitemap(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    rows: list[dict[str, Any]] = []
    for node in root.findall("sm:url", ns):
        rows.append(
            {
                "loc": node.findtext("sm:loc", default="", namespaces=ns),
                "lastmod": node.findtext("sm:lastmod", default=None, namespaces=ns),
                "changefreq": node.findtext("sm:changefreq", default=None, namespaces=ns),
                "priority": node.findtext("sm:priority", default=None, namespaces=ns),
            }
        )
    return rows


def collect_meta(html_text: str) -> dict[str, list[str]]:
    parser = MetaTagParser()
    parser.feed(html_text)
    meta: dict[str, list[str]] = {}
    for tag in parser.meta_tags:
        name = tag.get("name") or tag.get("property")
        content = tag.get("content")
        if not name or content is None:
            continue
        meta.setdefault(name, []).append(content)
    return meta


def single(meta: dict[str, list[str]], key: str) -> str | None:
    values = meta.get(key) or []
    if not values:
        return None
    return values[0]


def normalize_keywords(value: str | None) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[;,|]", value)
    return [p.strip() for p in parts if p.strip()]


def derive_paper_id(doi: str | None, url: str) -> str:
    if doi:
        return doi
    match = re.search(r"/preprint/view/(\d+)", url)
    return match.group(1) if match else url


def build_normalized(raw: dict[str, Any]) -> dict[str, Any]:
    meta = raw["meta"]
    authors = []
    author_names = meta.get("citation_author", [])
    author_institutions = meta.get("citation_author_institution", [])
    for i, name in enumerate(author_names):
        item: dict[str, Any] = {"name": name}
        if i < len(author_institutions):
            item["institution"] = author_institutions[i]
        authors.append(item)

    publication_date = single(meta, "citation_publication_date") or single(meta, "citation_date")
    year = None
    if publication_date:
        try:
            year = int(publication_date.split("/")[0])
        except ValueError:
            year = None

    pdf_url = single(meta, "citation_pdf_url")
    keywords = normalize_keywords(single(meta, "citation_keywords"))
    title = single(meta, "citation_title")

    return {
        "paperId": derive_paper_id(single(meta, "citation_doi"), raw["url"]),
        "title": title,
        "venue": "LangTaoSha Preprint Server",
        "year": year,
        "citationCount": None,
        "openAccessPdf": {
            "url": pdf_url or "",
            "status": "OPEN" if pdf_url else "UNKNOWN",
            "license": None,
            "disclaimer": None,
        },
        "fieldsOfStudy": keywords,
        "publicationDate": publication_date,
        "authors": authors,
        "abstract": single(meta, "citation_abstract"),
        "doi": single(meta, "citation_doi"),
        "url": raw["url"],
        "publisher": single(meta, "citation_publisher"),
        "language": single(meta, "citation_language"),
        "abstractUrl": single(meta, "citation_abstract_html_url"),
        "pdfUrl": pdf_url,
    }


@dataclass
class OutputPaths:
    root: Path
    sitemap_xml: Path
    urls_json: Path
    raw_jsonl: Path
    normalized_jsonl: Path
    summary_json: Path
    daily_root: Path
    daily_state_json: Path
    daily_summary_json: Path


def build_paths(root_dir: Path) -> OutputPaths:
    return OutputPaths(
        root=root_dir,
        sitemap_xml=root_dir / "sitemap" / "langtaosha_en_sitemap.xml",
        urls_json=root_dir / "urls" / "preprint_urls.json",
        raw_jsonl=root_dir / "raw" / "articles_raw.jsonl",
        normalized_jsonl=root_dir / "normalized" / "papers_external_search.jsonl",
        summary_json=root_dir / "summary.json",
        daily_root=root_dir / "daily",
        daily_state_json=root_dir / "daily_state.json",
        daily_summary_json=root_dir / "daily_summary.json",
    )


def fetch_preprint_rows(
    rows: list[dict[str, Any]],
    *,
    timeout: int,
    retries: int,
    sleep_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    raw_items: list[dict[str, Any]] = []
    normalized_items: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    for row in rows:
        url = row["loc"]
        try:
            html_text = fetch_text(
                url,
                timeout=timeout,
                retries=retries,
                sleep_seconds=sleep_seconds,
            )
            meta = collect_meta(html_text)
            raw_item = {
                "url": url,
                "sitemap_lastmod": row.get("lastmod"),
                "meta": meta,
                "fetched_at": utc_now_iso(),
            }
            raw_items.append(raw_item)
            normalized_items.append(build_normalized(raw_item))
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        except Exception as exc:
            failures.append({"url": url, "error": str(exc)})

    return raw_items, normalized_items, failures


def prepare_preprint_rows(
    *,
    timeout: int,
    retries: int,
    sleep_seconds: float,
    limit: int,
    paths: OutputPaths,
) -> list[dict[str, Any]]:
    sitemap_xml = fetch_text(
        SITEMAP_URL,
        timeout=timeout,
        retries=retries,
        sleep_seconds=sleep_seconds,
    )
    write_text(paths.sitemap_xml, sitemap_xml)

    sitemap_rows = parse_sitemap(sitemap_xml)
    preprint_rows = [row for row in sitemap_rows if "/preprint/view/" in row["loc"]]
    if limit > 0:
        preprint_rows = preprint_rows[:limit]
    write_json(paths.urls_json, preprint_rows)
    return preprint_rows


def iter_dates(start_date: date, end_date: date) -> list[date]:
    current = start_date
    values: list[date] = []
    while current <= end_date:
        values.append(current)
        current += timedelta(days=1)
    return values


def build_daily_path(paths: OutputPaths, target_date: date) -> Path:
    return paths.daily_root / f"{target_date.year:04d}" / f"{target_date.isoformat()}.jsonl"


def group_rows_by_lastmod_date(preprint_rows: list[dict[str, Any]]) -> dict[date, list[dict[str, Any]]]:
    grouped: dict[date, list[dict[str, Any]]] = {}
    for row in preprint_rows:
        lastmod_date = parse_lastmod_date(row.get("lastmod"))
        if lastmod_date is None:
            continue
        grouped.setdefault(lastmod_date, []).append(row)
    return grouped


def run_snapshot(args: argparse.Namespace, paths: OutputPaths) -> int:
    preprint_rows = prepare_preprint_rows(
        timeout=args.timeout,
        retries=args.retries,
        sleep_seconds=args.sleep_seconds,
        limit=args.limit,
        paths=paths,
    )
    raw_items, normalized_items, failures = fetch_preprint_rows(
        preprint_rows,
        timeout=args.timeout,
        retries=args.retries,
        sleep_seconds=args.sleep_seconds,
    )

    write_jsonl(paths.raw_jsonl, raw_items)
    write_jsonl(paths.normalized_jsonl, normalized_items)

    summary = {
        "mode": "snapshot",
        "run_at": utc_now_iso(),
        "sitemap_url": SITEMAP_URL,
        "total_preprint_urls": len(preprint_rows),
        "raw_records_written": len(raw_items),
        "normalized_records_written": len(normalized_items),
        "failed_urls": failures,
        "output": {
            "sitemap_xml": str(paths.sitemap_xml),
            "urls_json": str(paths.urls_json),
            "raw_jsonl": str(paths.raw_jsonl),
            "normalized_jsonl": str(paths.normalized_jsonl),
        },
    }
    write_json(paths.summary_json, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def run_daily_update(args: argparse.Namespace, paths: OutputPaths) -> int:
    preprint_rows = prepare_preprint_rows(
        timeout=args.timeout,
        retries=args.retries,
        sleep_seconds=args.sleep_seconds,
        limit=args.limit,
        paths=paths,
    )
    grouped_rows = group_rows_by_lastmod_date(preprint_rows)

    state_path = Path(args.state_file) if args.state_file else paths.daily_state_json
    state = load_json(state_path)

    end_date = parse_date(args.end_date) if args.end_date else date.today()
    if args.start_date:
        start_date = parse_date(args.start_date)
    else:
        last_processed_date = state.get("last_processed_date")
        if last_processed_date:
            start_date = parse_date(last_processed_date) + timedelta(days=1)
        else:
            start_date = end_date

    if end_date < start_date:
        summary = {
            "mode": "daily-update",
            "status": "noop",
            "run_at": utc_now_iso(),
            "query_start_date": start_date.isoformat(),
            "query_end_date": end_date.isoformat(),
            "message": "No pending dates to process.",
            "daily_root": str(paths.daily_root),
            "state_file": str(state_path),
        }
        write_json(paths.daily_summary_json, summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    requested_dates = iter_dates(start_date, end_date)
    failures: list[dict[str, str]] = []
    days_written: list[str] = []
    nonempty_days: list[str] = []
    total_records_written = 0

    for target_date in requested_dates:
        day_rows = grouped_rows.get(target_date, [])
        _, normalized_items, day_failures = fetch_preprint_rows(
            day_rows,
            timeout=args.timeout,
            retries=args.retries,
            sleep_seconds=args.sleep_seconds,
        )
        day_path = build_daily_path(paths, target_date)
        write_jsonl(day_path, normalized_items)

        days_written.append(target_date.isoformat())
        if normalized_items:
            nonempty_days.append(target_date.isoformat())
        total_records_written += len(normalized_items)

        for failure in day_failures:
            failures.append(
                {
                    "date": target_date.isoformat(),
                    "url": failure["url"],
                    "error": failure["error"],
                }
            )

    new_state = {
        "mode": "daily-update",
        "last_run_at": utc_now_iso(),
        "last_processed_date": requested_dates[-1].isoformat(),
        "last_run_start_date": requested_dates[0].isoformat(),
        "last_run_end_date": requested_dates[-1].isoformat(),
        "last_run_days_written": len(days_written),
        "last_run_nonempty_days": nonempty_days,
        "last_run_records_written": total_records_written,
    }
    write_json(state_path, new_state)

    summary = {
        "mode": "daily-update",
        "status": "ok",
        "run_at": utc_now_iso(),
        "sitemap_url": SITEMAP_URL,
        "total_preprint_urls": len(preprint_rows),
        "query_start_date": requested_dates[0].isoformat(),
        "query_end_date": requested_dates[-1].isoformat(),
        "days_written": days_written,
        "nonempty_days": nonempty_days,
        "records_written": total_records_written,
        "failed_urls": failures,
        "output": {
            "daily_root": str(paths.daily_root),
            "state_file": str(state_path),
            "sitemap_xml": str(paths.sitemap_xml),
            "urls_json": str(paths.urls_json),
        },
    }
    write_json(paths.daily_summary_json, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape LangTaoSha article metadata.")
    parser.add_argument("--root-dir", required=True)
    parser.add_argument(
        "--mode",
        default="snapshot",
        choices=["snapshot", "daily-update"],
        help="snapshot rewrites full exports; daily-update writes per-day JSONL files.",
    )
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--limit", type=int, default=0, help="Optional limit for testing.")
    parser.add_argument("--start-date", default=None, help="daily-update only, YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="daily-update only, YYYY-MM-DD")
    parser.add_argument(
        "--state-file",
        default=None,
        help="daily-update only, defaults to <root-dir>/daily_state.json",
    )
    args = parser.parse_args()

    root_dir = Path(args.root_dir)
    paths = build_paths(root_dir)

    if args.mode == "daily-update":
        return run_daily_update(args, paths)
    return run_snapshot(args, paths)


if __name__ == "__main__":
    raise SystemExit(main())
