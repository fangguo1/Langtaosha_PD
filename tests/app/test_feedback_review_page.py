from __future__ import annotations

import json
from pathlib import Path

from flask import Flask

from app.pages.feedback_review_page import (
    build_feedback_review_payload,
    load_feedback_review_testbed,
    register_feedback_review_routes,
)


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class FakeConnection:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, stmt, params):
        return FakeResult(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(self, rows):
        self._rows = rows

    def connect(self):
        return FakeConnection(self._rows)


def test_build_feedback_review_payload_hydrates_queries_and_metadata():
    testbed_payload = {
        "testbed_name": "topic-v1",
        "summary": {"query_count": 1},
        "queries": [
            {
                "query_id": 1,
                "annotator_ids": ["alice"],
                "annotator_count": 1,
                "query_text": "synapse",
                "labels": [
                    {"work_id": "W1", "label": 1},
                    {"work_id": "W2", "label": 0},
                ],
            }
        ],
    }
    engine = FakeEngine(
        [
            {
                "paper_id": 10,
                "work_id": "W1",
                "title": "Paper One",
                "abstract": "Abstract One",
                "authors": '[{"name":"Alice"}]',
                "online_at": None,
                "source_name": "langtaosha",
                "source_url": "https://example.org/one",
                "doi": "10.1000/one",
            },
            {
                "paper_id": 11,
                "work_id": "W2",
                "title": "Paper Two",
                "abstract": "Abstract Two",
                "authors": '[{"name":"Bob"}]',
                "online_at": None,
                "source_name": "biorxiv_history",
                "source_url": "https://example.org/two",
                "doi": "10.1000/two",
            },
        ]
    )

    payload = build_feedback_review_payload(testbed_payload, engine)

    assert payload["testbed_name"] == "topic-v1"
    assert payload["queries"][0]["label_summary"] == {"positive": 1, "negative": 1}
    assert payload["queries"][0]["results"][0]["title"] == "Paper One"
    assert payload["queries"][0]["results"][0]["label"] == 1
    assert payload["queries"][0]["results"][1]["source"] == "Biorxiv"


def test_feedback_review_api_returns_queries_from_testbed_json(tmp_path):
    testbed_path = tmp_path / "testbed.json"
    testbed_path.write_text(
        json.dumps(
            {
                "testbed_name": "topic-v1",
                "summary": {"query_count": 1},
                "queries": [
                    {
                        "query_id": 1,
                        "annotator_ids": ["alice"],
                        "annotator_count": 1,
                        "query_text": "synapse",
                        "labels": [{"work_id": "W1", "label": 1}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[2] / "templates"))
    register_feedback_review_routes(
        app,
        lambda payload=None, status_code=200: (app.response_class(
            json.dumps({"success": True, **(payload or {})}), mimetype="application/json"
        ), status_code),
        lambda message, status_code=500, code="ERR", extra=None: (
            app.response_class(
                json.dumps({"success": False, "error": message, **(extra or {})}),
                mimetype="application/json",
            ),
            status_code,
        ),
        testbed_path=testbed_path,
        engine_factory=lambda: FakeEngine(
            [
                {
                    "paper_id": 10,
                    "work_id": "W1",
                    "title": "Paper One",
                    "abstract": "Abstract One",
                    "authors": '[{"name":"Alice"}]',
                    "online_at": None,
                    "source_name": "langtaosha",
                    "source_url": "https://example.org/one",
                    "doi": "10.1000/one",
                }
            ]
        ),
    )

    response = app.test_client().get("/api/study/feedback-review-data")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["success"] is True
    assert payload["testbed_name"] == "topic-v1"
    assert "queries" in payload
    assert "searches" not in payload
