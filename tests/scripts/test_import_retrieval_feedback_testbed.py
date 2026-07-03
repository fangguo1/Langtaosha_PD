from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from src.docset_hub.evaluation.contracts import TestbedQuery as EvalQuery


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def load_module():
    module_path = PROJECT_ROOT / "scripts" / "import_retrieval_feedback_testbed.py"
    spec = importlib.util.spec_from_file_location("import_retrieval_feedback_testbed", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_import_cli_supports_single_database_mode():
    module = load_module()
    args = module.parse_args(
        [
            "--config-path", "src/config/config_tecent_backend_server_mimic.yaml",
            "--confirm-database", "langtaosha_mimic",
        ]
    )

    assert args.include_unknown_route is False
    assert args.confirm_database == "langtaosha_mimic"


def test_import_main_wires_feedback_selection_and_freeze(monkeypatch, tmp_path):
    module = load_module()

    monkeypatch.setattr(module, "load_config_mapping", lambda path: {"metadata_db": {"name": "langtaosha_mimic"}})
    monkeypatch.setattr(module, "build_config_fingerprint", lambda config: {"metadata_db_name": config["metadata_db"]["name"]})
    monkeypatch.setattr(module, "create_metadata_engine_from_config", lambda config: object())
    monkeypatch.setattr(
        module,
        "build_testbed_document",
        lambda **kwargs: {
            "testbed_name": kwargs["testbed_name"],
            "source_environment": kwargs["source_environment"],
            "queries": [
                {
                    "query_id": 1,
                    "annotator_ids": ["alice"],
                    "annotator_count": 1,
                    "query_text": "brain computer interface",
                    "labels": [{"work_id": "W1", "label": 1}],
                }
            ],
        },
    )
    monkeypatch.setattr(
        module,
        "build_testbed_queries_from_resolved_judgments",
        lambda judgments: [
            EvalQuery(
                query_id=1,
                query_text="brain computer interface",
                judgments={"W1": 1},
                judgment_metadata={"W1": {"annotator_ids": ["alice"], "annotator_count": 1}},
            )
        ],
    )

    class FakeFeedbackRepo:
        def __init__(self, engine, origin_environment):
            self.origin_environment = origin_environment

        def load_raw_feedback(self, include_unknown_route=False):
            return ["raw1", "raw2"]

    monkeypatch.setattr(module, "FeedbackSourceRepository", FakeFeedbackRepo)
    monkeypatch.setattr(module, "select_topic_feedback", lambda rows, include_unknown_route=False: rows)
    monkeypatch.setattr(module, "resolve_feedback_with_report", lambda rows: (["resolved"], type("R", (), {"conflict_count": 0})()))

    output = tmp_path / "import.json"
    rc = module.main(
        [
            "--config-path", "src/config/config_tecent_backend_server_mimic.yaml",
            "--confirm-database", "langtaosha_mimic",
            "--freeze-version-name", "topic-v1",
            "--output-report", str(output),
        ]
    )

    assert rc == 0
    assert output.exists()
    payload = module.json.loads(output.read_text(encoding="utf-8"))
    assert payload["testbed_name"] == "topic-v1"
    assert payload["source_environment"] == "mimic"
    assert payload["queries"][0]["annotator_ids"] == ["alice"]
    assert payload["queries"][0]["annotator_count"] == 1
    assert payload["queries"][0]["query_text"] == "brain computer interface"
    assert payload["queries"][0]["labels"] == [{"work_id": "W1", "label": 1}]
