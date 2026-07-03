from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_script_module():
    module_path = PROJECT_ROOT / "scripts" / "query_understanding_use.py"
    spec = importlib.util.spec_from_file_location("query_understanding_use", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_default_config_path_points_to_use_yaml():
    script = load_script_module()

    assert str(script.DEFAULT_CONFIG_PATH).endswith("src/config/config_tecent_backend_server_use.yaml")


def test_run_query_understanding_uses_indexer_factory():
    script = load_script_module()
    captured = {}

    class FakeUnderstandingResult:
        def to_dict(self):
            return {
                "original_query": "niang yan",
                "normalized_query": "niang yan",
                "intent": "semantic_search",
                "route": "vector",
            }

    class FakeIndexer:
        def __init__(self):
            self.query_understanding = self

        def analyze(self, query):
            captured["query"] = query
            return FakeUnderstandingResult()

    def fake_factory(config_path):
        captured["config_path"] = str(config_path)
        return FakeIndexer()

    result = script.run_query_understanding(
        query="niang yan",
        paper_indexer_factory=fake_factory,
    )

    assert captured["query"] == "niang yan"
    assert captured["config_path"].endswith("src/config/config_tecent_backend_server_use.yaml")
    assert result["normalized_query"] == "niang yan"
    assert result["route"] == "vector"


def test_main_writes_json_output():
    script = load_script_module()
    stdout = io.StringIO()
    stderr = io.StringIO()

    class FakeUnderstandingResult:
        def to_dict(self):
            return {
                "original_query": "machin learning",
                "normalized_query": "machin learning",
                "intent": "semantic_search",
                "route": "vector",
                "corrected_query": None,
            }

    class FakeIndexer:
        def __init__(self):
            self.query_understanding = self

        def analyze(self, query):
            return FakeUnderstandingResult()

    exit_code = script.main(
        ["machin learning"],
        stdout=stdout,
        stderr=stderr,
        paper_indexer_factory=lambda config_path: FakeIndexer(),
    )

    assert exit_code == 0
    assert stderr.getvalue() == ""
    payload = json.loads(stdout.getvalue())
    assert payload["original_query"] == "machin learning"
    assert payload["route"] == "vector"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("query", "expected_route", "expected_name"),
    [
        ("huaichong shen", "author_suggestion", "Huaizong Shen"),
        ("wwei wang", "metadata_author", "Wei Wang"),
        ("we wang", "metadata_author", "Wei Wang"),
        ("nieng yang", "metadata_author", "Nieng Yan"),
        ("nieng yan", "metadata_author", "Nieng Yan"),
    ],
)
def test_run_query_understanding_use_recognizes_real_author_typos(
    query,
    expected_route,
    expected_name,
):
    script = load_script_module()

    result = script.run_query_understanding(query=query)

    assert result["intent"] == "author_name"
    assert result["route"] == expected_route
    if expected_route == "author_suggestion":
        assert result["suggested_author"] == expected_name
        assert result["matched_author"] is None
    else:
        assert result["matched_author"] == expected_name
        assert result["suggested_author"] is None
    assert result["candidates"][0]["name"] == expected_name
