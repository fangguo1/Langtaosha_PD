#!/usr/bin/env python3
"""Import Study Mode feedback into the retrieval feedback testbed."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from src.docset_hub.evaluation.config_identity import (  # noqa: E402
    build_config_fingerprint,
    create_metadata_engine_from_config,
    load_config_mapping,
)
from src.docset_hub.evaluation.feedback_importer import (  # noqa: E402
    resolve_feedback_with_report,
    select_topic_feedback,
)
from src.docset_hub.evaluation.json_testbed import (  # noqa: E402
    build_testbed_document,
    build_testbed_queries_from_resolved_judgments,
    save_testbed_document,
)
from src.docset_hub.evaluation.testbed_repository import FeedbackSourceRepository  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config-path",
        default="src/config/config_tecent_backend_server_mimic.yaml",
    )
    parser.add_argument(
        "--confirm-database",
        required=True,
        help="Exact metadata_db.name of the metadata DB to read from.",
    )
    parser.add_argument(
        "--origin-environment",
        default=None,
        help="Stored in imported judgments so we can trace where labels came from.",
    )
    parser.add_argument(
        "--freeze-version-name",
        default="retrieval-feedback-topic-v1",
    )
    parser.add_argument(
        "--include-unknown-route",
        action="store_true",
        help="Also import feedback rows whose route is NULL.",
    )
    parser.add_argument(
        "--output-report",
        default="local_data/retrieval_testbed/import_report.json",
    )
    return parser.parse_args(argv)


def _assert_confirmed_database(config_fingerprint: dict[str, object], expected_name: str) -> None:
    actual_name = str(config_fingerprint.get("metadata_db_name") or "")
    if actual_name != expected_name:
        raise SystemExit(
            f"Refusing to write testbed data: expected database '{expected_name}', got '{actual_name}'."
        )


def _infer_origin_environment(config_path: str, database_name: str) -> str:
    lowered_path = str(config_path).lower()
    lowered_db = str(database_name).lower()
    if "mimic" in lowered_path or "mimic" in lowered_db:
        return "mimic"
    if "use" in lowered_path or "use" in lowered_db:
        return "use"
    return database_name


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    config = load_config_mapping(args.config_path)
    config_fingerprint = build_config_fingerprint(config)
    _assert_confirmed_database(config_fingerprint, args.confirm_database)

    metadata_engine = create_metadata_engine_from_config(config)
    origin_environment = args.origin_environment or _infer_origin_environment(
        args.config_path,
        str(config_fingerprint.get("metadata_db_name") or ""),
    )

    feedback_repository = FeedbackSourceRepository(
        metadata_engine,
        origin_environment=origin_environment,
    )
    raw_feedback = feedback_repository.load_raw_feedback(
        include_unknown_route=args.include_unknown_route
    )
    selected_feedback = select_topic_feedback(
        raw_feedback,
        include_unknown_route=args.include_unknown_route,
    )
    resolved_judgments, resolution_report = resolve_feedback_with_report(selected_feedback)
    queries = build_testbed_queries_from_resolved_judgments(resolved_judgments)
    positive_count = sum(
        1
        for query in queries
        for label in query.judgments.values()
        if label > 0
    )
    negative_count = sum(
        1
        for query in queries
        for label in query.judgments.values()
        if label <= 0
    )
    summary = {
        "raw_feedback_count": len(raw_feedback),
        "topic_feedback_count": len(selected_feedback),
        "resolved_judgment_count": len(resolved_judgments),
        "query_count": len(queries),
        "positive_count": positive_count,
        "negative_count": negative_count,
        "conflict_count": resolution_report.conflict_count,
    }
    report = build_testbed_document(
        testbed_name=args.freeze_version_name,
        query_type="topic",
        source_environment=origin_environment,
        config_path=args.config_path,
        config_fingerprint=config_fingerprint,
        summary=summary,
        queries=queries,
    )
    report.update(
        {
            "database_name": config_fingerprint.get("metadata_db_name"),
            "feedback_origin_environment": origin_environment,
            "freeze_version_name": args.freeze_version_name,
            "output_report": args.output_report,
        }
    )

    output_path = Path(args.output_report)
    save_testbed_document(output_path, report)
    print(
        json.dumps(
            {
                "config_path": args.config_path,
                "database_name": config_fingerprint.get("metadata_db_name"),
                "feedback_origin_environment": origin_environment,
                "query_count": len(queries),
                "positive_count": positive_count,
                "negative_count": negative_count,
                "output_report": str(output_path),
            },
            ensure_ascii=False,
        )
    )
    return 0
'''
python scripts/import_retrieval_feedback_testbed.py \
  --config-path src/config/config_tecent_backend_server_mimic.yaml \
  --confirm-database langtaosha_mimic \
  --origin-environment mimic \
  --freeze-version-name topic-v1 \
  --output-report local_data/retrieval_testbed/import_topic_v1_mimic.json
'''
if __name__ == "__main__":
    raise SystemExit(main())
