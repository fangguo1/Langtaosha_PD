from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .contracts import TestbedQuery


def build_testbed_document(
    *,
    testbed_name: str,
    query_type: str,
    source_environment: str,
    config_path: str,
    config_fingerprint: dict[str, Any],
    summary: dict[str, Any],
    queries: Sequence[TestbedQuery],
) -> dict[str, Any]:
    return {
        "testbed_name": testbed_name,
        "query_type": query_type,
        "source_environment": source_environment,
        "config_path": config_path,
        "config_fingerprint": config_fingerprint,
        "summary": summary,
        "queries": [
            {
                "query_id": query.query_id,
                "annotator_ids": _collect_query_annotator_ids(query),
                "annotator_count": len(_collect_query_annotator_ids(query)),
                "query_text": query.query_text,
                "labels": [
                    {"work_id": work_id, "label": label}
                    for work_id, label in sorted(query.judgments.items())
                ],
            }
            for query in queries
        ],
    }


def save_testbed_document(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def load_testbed_document(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_testbed_queries(path: Path) -> list[TestbedQuery]:
    payload = load_testbed_document(path)
    queries: list[TestbedQuery] = []
    for item in payload.get("queries") or []:
        judgments = {
            str(label_row["work_id"]): int(label_row["label"])
            for label_row in item.get("labels") or []
        }
        judgment_metadata = {
            str(label_row["work_id"]): {
                "annotator_ids": list(item.get("annotator_ids") or []),
                "annotator_count": int(item.get("annotator_count") or 0),
            }
            for label_row in item.get("labels") or []
        }
        queries.append(
            TestbedQuery(
                query_id=int(item["query_id"]),
                query_text=str(item["query_text"]),
                judgments=judgments,
                judgment_metadata=judgment_metadata,
            )
        )
    return queries


def _collect_query_annotator_ids(query: TestbedQuery) -> list[str]:
    annotator_ids: set[str] = set()
    for metadata in query.judgment_metadata.values():
        annotator_ids.update(str(value) for value in metadata.get("annotator_ids") or [])
    return sorted(annotator_ids)


def build_testbed_queries_from_resolved_judgments(
    judgments: Sequence[Any],
) -> list[TestbedQuery]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"query_text": "", "judgments": {}, "judgment_metadata": {}}
    )
    for judgment in judgments:
        bucket = grouped[judgment.normalized_query]
        bucket["query_text"] = judgment.query_text
        bucket["judgments"][judgment.resolved_work_id] = judgment.relevance
        bucket["judgment_metadata"][judgment.resolved_work_id] = {
            "annotator_ids": list(judgment.annotator_ids),
            "annotator_count": judgment.annotator_count,
        }

    queries: list[TestbedQuery] = []
    for index, normalized_query in enumerate(sorted(grouped), start=1):
        item = grouped[normalized_query]
        queries.append(
            TestbedQuery(
                query_id=index,
                query_text=item["query_text"],
                judgments=dict(item["judgments"]),
                judgment_metadata=dict(item["judgment_metadata"]),
            )
        )
    return queries
