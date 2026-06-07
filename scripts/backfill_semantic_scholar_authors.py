#!/usr/bin/env python3
"""Backfill author full-name enrichment from Semantic Scholar.

The script is safe by default: mismatched author counts are recorded in the
manifest and never overwrite the current author JSON.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from src.docset_hub.metadata.semantic_scholar_authors import (  # noqa: E402
    DEFAULT_API_KEY_TEST_PAPER_ID,
    SemanticScholarClient,
    enrich_author_list,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def process_papers(
    metadata_db: MetadataDB,
    client: SemanticScholarClient,
    papers: List[Dict[str, Any]],
    dry_run: bool,
    request_sleep_seconds: float,
    record_status: bool,
) -> Dict[str, Any]:
    stats = {
        "total": len(papers),
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "status_counts": {},
        "items": [],
    }

    for paper in papers:
        item = {
            "paper_id": paper["paper_id"],
            "work_id": paper.get("work_id"),
            "source_name": paper.get("source_name"),
            "doi": paper.get("doi"),
            "version": paper.get("version"),
        }
        try:
            semantic_paper = client.fetch_paper_by_doi(paper["doi"])
            enrichment = enrich_author_list(paper.get("authors") or [], semantic_paper)
            status = enrichment["status"]
            item.update(
                {
                    "status": status,
                    "current_author_count": enrichment["current_author_count"],
                    "semantic_author_count": enrichment["semantic_author_count"],
                    "semantic_scholar_paper_id": semantic_paper.paper_id,
                    "semantic_scholar_title": semantic_paper.title,
                    "semantic_scholar_publication_date": semantic_paper.publication_date,
                    "semantic_scholar_authors": [
                        {
                            "name": author.name,
                            "authorId": author.author_id,
                            "externalIds": author.external_ids,
                            "affiliations": author.affiliations,
                        }
                        for author in semantic_paper.authors
                    ],
                }
            )

            if enrichment["should_update"]:
                if not dry_run:
                    metadata_db.update_author_enrichment(
                        paper_id=paper["paper_id"],
                        authors=enrichment["authors"],
                        semantic_scholar_paper_id=semantic_paper.paper_id,
                        doi=paper.get("doi"),
                    )
                stats["updated"] += 1
            else:
                stats["skipped"] += 1

            stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1

        except HTTPError as exc:
            status = "semantic_scholar_not_found" if exc.code == 404 else f"http_{exc.code}"
            item.update({"status": status, "error": str(exc)})
            stats["failed"] += 1
            stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1
        except Exception as exc:  # pragma: no cover - network dependent
            status = "error"
            item.update({"status": status, "error": str(exc)})
            stats["failed"] += 1
            stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1

        stats["items"].append(item)
        if record_status and not dry_run:
            metadata_db.record_author_enrichment_status(item)
        logging.info("%s %s", item.get("status"), item.get("doi"))
        if request_sleep_seconds > 0:
            time.sleep(request_sleep_seconds)

    return stats


def process_batch_item(
    metadata_db: MetadataDB,
    paper: Dict[str, Any],
    semantic_paper: Optional[Any],
    dry_run: bool,
) -> Dict[str, Any]:
    item = {
        "paper_id": paper["paper_id"],
        "work_id": paper.get("work_id"),
        "source_name": paper.get("source_name"),
        "doi": paper.get("doi"),
        "version": paper.get("version"),
    }
    if semantic_paper is None:
        item.update({"status": "semantic_scholar_not_found"})
        return item

    enrichment = enrich_author_list(paper.get("authors") or [], semantic_paper)
    status = enrichment["status"]
    item.update(
        {
            "status": status,
            "current_author_count": enrichment["current_author_count"],
            "semantic_author_count": enrichment["semantic_author_count"],
            "semantic_scholar_paper_id": semantic_paper.paper_id,
            "semantic_scholar_title": semantic_paper.title,
            "semantic_scholar_publication_date": semantic_paper.publication_date,
            "semantic_scholar_authors": [
                {
                    "name": author.name,
                    "authorId": author.author_id,
                    "externalIds": author.external_ids,
                    "affiliations": author.affiliations,
                }
                for author in semantic_paper.authors
            ],
        }
    )
    if enrichment["should_update"] and not dry_run:
        metadata_db.update_author_enrichment(
            paper_id=paper["paper_id"],
            authors=enrichment["authors"],
            semantic_scholar_paper_id=semantic_paper.paper_id,
            doi=paper.get("doi"),
        )
    return item


def process_papers_with_batch_api(
    metadata_db: MetadataDB,
    client: SemanticScholarClient,
    papers: List[Dict[str, Any]],
    dry_run: bool,
    request_sleep_seconds: float,
    record_status: bool,
    semantic_batch_size: int,
) -> Dict[str, Any]:
    stats = {
        "total": len(papers),
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "status_counts": {},
        "items": [],
    }
    semantic_batch_size = max(1, semantic_batch_size)

    for start in range(0, len(papers), semantic_batch_size):
        chunk = papers[start : start + semantic_batch_size]
        doi_ids = [f"DOI:{paper['doi']}" for paper in chunk]
        try:
            semantic_papers = client.fetch_papers_by_ids(doi_ids)
        except HTTPError as exc:
            if exc.code == 400:
                logging.warning("http_400 batch_size=%s; falling back to per-DOI lookup", len(chunk))
                for paper in chunk:
                    try:
                        semantic_paper = client.fetch_paper_by_doi(paper["doi"])
                        item = process_batch_item(metadata_db, paper, semantic_paper, dry_run=dry_run)
                    except HTTPError as item_exc:
                        status = "semantic_scholar_not_found" if item_exc.code == 404 else f"http_{item_exc.code}"
                        item = {
                            "paper_id": paper["paper_id"],
                            "work_id": paper.get("work_id"),
                            "source_name": paper.get("source_name"),
                            "doi": paper.get("doi"),
                            "version": paper.get("version"),
                            "status": status,
                            "error": str(item_exc),
                        }
                    except Exception as item_exc:  # pragma: no cover - network dependent
                        item = {
                            "paper_id": paper["paper_id"],
                            "work_id": paper.get("work_id"),
                            "source_name": paper.get("source_name"),
                            "doi": paper.get("doi"),
                            "version": paper.get("version"),
                            "status": "error",
                            "error": str(item_exc),
                        }

                    status = item["status"]
                    if status == "matched_author_count":
                        stats["updated"] += 1
                    elif status == "semantic_scholar_not_found":
                        stats["failed"] += 1
                    else:
                        stats["skipped"] += 1
                    stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1
                    stats["items"].append(item)
                    if record_status and not dry_run:
                        metadata_db.record_author_enrichment_status(item)
                    logging.info("%s %s", item.get("status"), item.get("doi"))
                continue

            status = f"http_{exc.code}"
            for paper in chunk:
                item = {
                    "paper_id": paper["paper_id"],
                    "work_id": paper.get("work_id"),
                    "source_name": paper.get("source_name"),
                    "doi": paper.get("doi"),
                    "version": paper.get("version"),
                    "status": status,
                    "error": str(exc),
                }
                stats["items"].append(item)
                stats["failed"] += 1
                stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1
                if record_status and not dry_run:
                    metadata_db.record_author_enrichment_status(item)
            logging.warning("%s batch_size=%s", status, len(chunk))
        except Exception as exc:  # pragma: no cover - network dependent
            status = "error"
            for paper in chunk:
                item = {
                    "paper_id": paper["paper_id"],
                    "work_id": paper.get("work_id"),
                    "source_name": paper.get("source_name"),
                    "doi": paper.get("doi"),
                    "version": paper.get("version"),
                    "status": status,
                    "error": str(exc),
                }
                stats["items"].append(item)
                stats["failed"] += 1
                stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1
                if record_status and not dry_run:
                    metadata_db.record_author_enrichment_status(item)
            logging.warning("%s batch_size=%s", status, len(chunk))
        else:
            semantic_by_doi: Dict[str, Any] = {}
            for semantic_paper in semantic_papers:
                if semantic_paper is None:
                    continue
                doi = (semantic_paper.external_ids or {}).get("DOI")
                if doi:
                    semantic_by_doi[str(doi).strip().lower()] = semantic_paper
            if len(semantic_papers) != len(chunk):
                logging.warning(
                    "Semantic Scholar batch response length mismatch: %s != %s; missing DOI ids will be marked not_found.",
                    len(semantic_papers),
                    len(chunk),
                )

            for paper in chunk:
                semantic_paper = semantic_by_doi.get(str(paper.get("doi") or "").strip().lower())
                item = process_batch_item(metadata_db, paper, semantic_paper, dry_run=dry_run)
                status = item["status"]
                if status == "matched_author_count":
                    stats["updated"] += 1
                elif status == "semantic_scholar_not_found":
                    stats["failed"] += 1
                else:
                    stats["skipped"] += 1
                stats["status_counts"][status] = stats["status_counts"].get(status, 0) + 1
                stats["items"].append(item)
                if record_status and not dry_run:
                    metadata_db.record_author_enrichment_status(item)
                logging.info("%s %s", item.get("status"), item.get("doi"))

        if request_sleep_seconds > 0 and start + semantic_batch_size < len(papers):
            time.sleep(request_sleep_seconds)

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Semantic Scholar author-name enrichment.")
    parser.add_argument("--config-path", default="src/config/config_tecent_backend_server_test.yaml")
    parser.add_argument("--source-name", action="append", dest="source_names", help="Filter by source_name; repeatable.")
    parser.add_argument("--date", dest="target_date", help="Only process records whose source date matches YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--include-already-enriched", action="store_true")
    parser.add_argument(
        "--record-status",
        action="store_true",
        help="Persist every processed paper to semantic_scholar_author_enrichment_status.",
    )
    parser.add_argument(
        "--skip-recorded-status",
        action="store_true",
        help="Skip papers already present in semantic_scholar_author_enrichment_status.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--manifest", default="local_data/semantic_scholar_author_enrichment/manifest.json")
    parser.add_argument("--jsonl", default=None, help="Optional JSONL copy of per-paper results.")
    parser.add_argument("--api-key-env", default="SEMANTIC_SCHOLAR_API_KEY")
    parser.add_argument(
        "--require-api-key",
        action="store_true",
        help="Exit before DB writes if the configured Semantic Scholar API key env var is missing.",
    )
    parser.add_argument(
        "--validate-api-key",
        action="store_true",
        help="Make one Semantic Scholar Graph API request before processing papers.",
    )
    parser.add_argument(
        "--check-api-key-only",
        action="store_true",
        help="Only validate the Semantic Scholar API key and exit.",
    )
    parser.add_argument("--api-key-test-paper-id", default=DEFAULT_API_KEY_TEST_PAPER_ID)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--request-sleep-seconds", type=float, default=1.1)
    parser.add_argument(
        "--use-batch-api",
        action="store_true",
        help="Use Semantic Scholar POST /graph/v1/paper/batch for DOI lookups.",
    )
    parser.add_argument(
        "--semantic-batch-size",
        type=int,
        default=100,
        help="Number of DOI ids per Semantic Scholar batch request when --use-batch-api is set.",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")

    api_key = os.environ.get(args.api_key_env)
    client = SemanticScholarClient(
        api_key=api_key,
        timeout=args.timeout,
        max_retries=args.max_retries,
        sleep_seconds=args.sleep_seconds,
    )
    api_key_validation: Optional[Dict[str, Any]] = None
    if args.require_api_key and not api_key:
        print(f"missing required env var: {args.api_key_env}")
        return 2
    if args.validate_api_key or args.check_api_key_only:
        try:
            api_key_validation = client.validate_api_key(args.api_key_test_paper_id)
        except HTTPError as exc:
            api_key_validation = {
                "ok": False,
                "status": f"http_{exc.code}",
                "error": str(exc),
                "test_paper_id": args.api_key_test_paper_id,
            }
        except Exception as exc:
            api_key_validation = {
                "ok": False,
                "status": "error",
                "error": str(exc),
                "test_paper_id": args.api_key_test_paper_id,
            }
        if args.check_api_key_only:
            print(json.dumps(api_key_validation, ensure_ascii=False, indent=2))
            return 0 if api_key_validation.get("ok") else 1
        if not api_key_validation.get("ok"):
            print(json.dumps(api_key_validation, ensure_ascii=False, indent=2))
            return 2

    config_path = Path(args.config_path)
    if not config_path.exists():
        print(f"config file not found: {config_path}")
        return 1

    from src.docset_hub.storage.metadata_db import MetadataDB  # noqa: E402

    metadata_db = MetadataDB(config_path=config_path)
    if args.record_status or args.skip_recorded_status:
        metadata_db.ensure_author_enrichment_status_table()
    papers = metadata_db.iter_papers_for_author_enrichment(
        source_names=args.source_names,
        limit=args.limit,
        only_missing=not args.include_already_enriched,
        target_date=args.target_date,
        skip_recorded_status=args.skip_recorded_status,
    )

    if args.use_batch_api:
        stats = process_papers_with_batch_api(
            metadata_db,
            client,
            papers,
            dry_run=args.dry_run,
            request_sleep_seconds=args.request_sleep_seconds,
            record_status=args.record_status,
            semantic_batch_size=args.semantic_batch_size,
        )
    else:
        stats = process_papers(
            metadata_db,
            client,
            papers,
            dry_run=args.dry_run,
            request_sleep_seconds=args.request_sleep_seconds,
            record_status=args.record_status,
        )
    manifest = {
        "mode": "semantic_scholar_author_enrichment",
        "dry_run": args.dry_run,
        "created_at": utc_now_iso(),
        "config_path": str(config_path),
        "source_names": args.source_names,
        "target_date": args.target_date,
        "limit": args.limit,
        "api_key_env": args.api_key_env,
        "api_key_present": bool(api_key),
        "api_key_validation": api_key_validation,
        "use_batch_api": args.use_batch_api,
        "semantic_batch_size": args.semantic_batch_size if args.use_batch_api else None,
        **stats,
    }
    write_json(Path(args.manifest), manifest)
    if args.jsonl:
        write_jsonl(Path(args.jsonl), stats["items"])

    print(json.dumps({k: v for k, v in manifest.items() if k != "items"}, ensure_ascii=False, indent=2))
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
