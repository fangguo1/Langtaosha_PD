from __future__ import annotations

import json


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


def test_create_feedback_review_app_serves_page_and_api(tmp_path, monkeypatch):
    from app.dev import feedback_review_app as feedback_app_module

    monkeypatch.setattr(feedback_app_module, "init_config", lambda path: None)

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

    app = feedback_app_module.create_feedback_review_app(
        config_path="src/config/config_tecent_backend_server_mimic.yaml",
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

    client = app.test_client()
    page_response = client.get("/feedback-review")
    api_response = client.get("/api/study/feedback-review-data")
    payload = api_response.get_json()

    assert page_response.status_code == 200
    assert api_response.status_code == 200
    assert payload["success"] is True
    assert payload["testbed_name"] == "topic-v1"
    assert payload["queries"][0]["results"][0]["title"] == "Paper One"


def test_create_feedback_review_app_sets_utf8_json(monkeypatch):
    from app.dev import feedback_review_app as feedback_app_module

    monkeypatch.setattr(feedback_app_module, "init_config", lambda path: None)

    app = feedback_app_module.create_feedback_review_app(
        config_path="src/config/config_tecent_backend_server_mimic.yaml",
        engine_factory=lambda: FakeEngine([]),
    )

    assert app.json.ensure_ascii is False
