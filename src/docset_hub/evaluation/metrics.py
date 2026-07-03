from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def _dedupe_ranked_work_ids(ranked_work_ids: Sequence[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for work_id in ranked_work_ids:
        normalized = str(work_id or "")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def evaluate_query(
    ranked_work_ids: Sequence[str],
    judgments: Mapping[str, int],
    ks: Sequence[int] = (5, 10),
) -> dict[str, float | int | None]:
    deduped = _dedupe_ranked_work_ids(ranked_work_ids)
    normalized_judgments = {str(work_id): int(label) for work_id, label in judgments.items()}
    positives = {work_id for work_id, label in normalized_judgments.items() if label > 0}
    negatives = {work_id for work_id, label in normalized_judgments.items() if label <= 0}

    metrics: dict[str, float | int | None] = {
        "query_result_count": len(deduped),
        "positive_count": len(positives),
        "negative_count": len(negatives),
    }

    first_positive_rank: int | None = None
    for rank, work_id in enumerate(deduped, start=1):
        if work_id in positives:
            first_positive_rank = rank
            break
    metrics["known_positive_mrr"] = None if first_positive_rank is None else 1.0 / first_positive_rank

    for k in ks:
        cutoff = max(0, int(k))
        top_k = deduped[:cutoff]
        judged_count = sum(1 for work_id in top_k if work_id in normalized_judgments)
        hit_count = sum(1 for work_id in top_k if work_id in positives)
        negative_count = sum(1 for work_id in top_k if work_id in negatives)
        metrics[f"judged_count@{cutoff}"] = judged_count
        metrics[f"unjudged_count@{cutoff}"] = max(0, cutoff - judged_count)
        metrics[f"known_negative_count@{cutoff}"] = negative_count
        metrics[f"known_positive_recall@{cutoff}"] = (
            None if not positives else hit_count / len(positives)
        )

    return metrics


def aggregate_query_metrics(
    query_metrics: Sequence[Mapping[str, float | int | None]],
) -> dict[str, float | int | None]:
    aggregate: dict[str, float | int | None] = {
        "query_count": len(query_metrics),
        "known_positive_query_count": 0,
    }
    if not query_metrics:
        return aggregate

    keys = {key for row in query_metrics for key in row.keys()}
    for key in sorted(keys):
        values = [row.get(key) for row in query_metrics]
        if key == "positive_count":
            aggregate["known_positive_query_count"] = sum(
                1 for value in values if isinstance(value, (int, float)) and value > 0
            )
        numeric = [float(value) for value in values if isinstance(value, (int, float))]
        if key in {"query_count", "known_positive_query_count"}:
            continue
        if key.startswith("known_positive_"):
            numeric = [float(value) for value in values if value is not None]
            aggregate[key] = None if not numeric else sum(numeric) / len(numeric)
        elif numeric:
            aggregate[key] = sum(numeric) / len(numeric)
        else:
            aggregate[key] = None

    return aggregate
