from __future__ import annotations

from src.docset_hub.evaluation.metrics import (
    aggregate_query_metrics,
    evaluate_query,
)


def test_evaluate_query_uses_known_positives_and_negatives():
    judgments = {"W1": 1, "W2": 1, "W3": 0}
    ranked = ["W3", "NEW", "W1", "W2"]

    metrics = evaluate_query(ranked, judgments, ks=(2, 4))

    assert metrics["positive_count"] == 2
    assert metrics["negative_count"] == 1
    assert metrics["known_positive_recall@2"] == 0.0
    assert metrics["known_positive_recall@4"] == 1.0
    assert metrics["known_negative_count@2"] == 1
    assert metrics["judged_count@2"] == 1
    assert metrics["unjudged_count@2"] == 1
    assert metrics["known_positive_mrr"] == 1 / 3


def test_evaluate_query_deduplicates_ranked_work_ids_at_first_rank():
    metrics = evaluate_query(["W1", "W1", "W2"], {"W1": 1, "W2": 1}, ks=(2,))

    assert metrics["known_positive_recall@2"] == 1.0
    assert metrics["judged_count@2"] == 2


def test_aggregate_metrics_excludes_no_positive_query_from_recall_macro():
    aggregate = aggregate_query_metrics(
        [
            {
                "positive_count": 1,
                "negative_count": 1,
                "known_positive_recall@5": 1.0,
                "known_positive_mrr": 1.0,
                "judged_count@5": 2,
            },
            {
                "positive_count": 0,
                "negative_count": 1,
                "known_positive_recall@5": None,
                "known_positive_mrr": None,
                "judged_count@5": 1,
            },
        ]
    )

    assert aggregate["query_count"] == 2
    assert aggregate["known_positive_query_count"] == 1
    assert aggregate["known_positive_recall@5"] == 1.0
    assert aggregate["known_positive_mrr"] == 1.0
    assert aggregate["judged_count@5"] == 1.5
