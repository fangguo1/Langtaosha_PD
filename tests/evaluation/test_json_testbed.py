from __future__ import annotations

import json

from src.docset_hub.evaluation.contracts import TestbedQuery as EvalQuery
from src.docset_hub.evaluation.json_testbed import (
    build_testbed_document,
    load_testbed_queries,
)


def test_build_testbed_document_serializes_summary_and_queries():
    queries = [
        EvalQuery(
            query_id=1,
            query_text="brain computer interface",
            judgments={"W1": 1, "W2": 0},
            judgment_metadata={
                "W1": {"annotator_ids": ["alice"], "annotator_count": 1},
                "W2": {"annotator_ids": ["bob"], "annotator_count": 1},
            },
        )
    ]
    document = build_testbed_document(
        testbed_name="topic-v1",
        query_type="topic",
        source_environment="mimic",
        config_path="src/config/config_tecent_backend_server_mimic.yaml",
        config_fingerprint={"metadata_db_name": "langtaosha_mimic"},
        summary={"raw_feedback_count": 10},
        queries=queries,
    )

    assert document["summary"] == {"raw_feedback_count": 10}
    assert document["queries"][0]["annotator_ids"] == ["alice", "bob"]
    assert document["queries"][0]["annotator_count"] == 2
    assert document["queries"][0]["labels"] == [
        {"work_id": "W1", "label": 1},
        {"work_id": "W2", "label": 0},
    ]


def test_load_testbed_queries_round_trips_json_labels(tmp_path):
    payload = {
        "testbed_name": "topic-v1",
        "queries": [
            {
                "query_id": 1,
                "annotator_ids": ["alice", "bob"],
                "annotator_count": 2,
                "query_text": "synapse",
                "labels": [
                    {"work_id": "W1", "label": 1},
                    {"work_id": "W9", "label": 0},
                ],
            }
        ],
    }
    path = tmp_path / "testbed.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_testbed_queries(path)

    assert loaded[0].judgments == {"W1": 1, "W9": 0}
    assert loaded[0].judgment_metadata["W1"]["annotator_ids"] == ["alice", "bob"]
