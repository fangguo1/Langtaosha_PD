from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def load_module():
    module_path = PROJECT_ROOT / "scripts" / "run_retrieval_testbed.py"
    spec = importlib.util.spec_from_file_location("run_retrieval_testbed", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_run_cli_supports_json_testbed_strategies():
    module = load_module()
    args = module.parse_args(
        [
            "--evaluation-config-path", "src/config/config_tecent_backend_server_use.yaml",
            "--confirm-evaluation-database", "langtaosha_use",
            "--testbed-json", "local_data/retrieval_testbed/topic-v1.json",
            "--strategies", "dense", "hybrid_retrieval",
        ]
    )
    assert args.strategies == ["dense", "hybrid_retrieval"]


def test_run_main_builds_strategies_and_writes_comparison(monkeypatch, tmp_path):
    module = load_module()

    monkeypatch.setattr(module, "load_config_mapping", lambda path: {"metadata_db": {"name": "langtaosha_use"}})
    monkeypatch.setattr(module, "build_config_fingerprint", lambda config: {"metadata_db_name": "langtaosha_use"})
    monkeypatch.setattr(module, "load_testbed_document", lambda path: {"testbed_name": "topic-v1"})
    monkeypatch.setattr(
        module,
        "load_testbed_queries",
        lambda path: [module.TestbedQuery(query_id=1, query_text="synapse", judgments={"W1": 1})],
    )

    class FakeRunner:
        def __init__(self, repository):
            self.repository = repository

        def run_queries(self, strategy, queries, top_k, ks, run_metadata):
            return {
                "run_id": 1,
                "status": "completed",
                "aggregate_metrics": {"query_count": len(queries), "known_positive_recall@10": 1.0},
                "query_failures": [],
                "per_query": [],
            }

    class FakeIndexer:
        def __init__(self, config_path, enable_vectorization):
            self.config_path = config_path
            self.enable_vectorization = enable_vectorization

    monkeypatch.setattr(module, "create_metadata_engine_from_config", lambda config: object())
    monkeypatch.setattr(module, "RetrievalEvaluationRunner", FakeRunner)
    monkeypatch.setattr(module, "PaperIndexer", FakeIndexer)

    output_dir = tmp_path / "runs"
    testbed_json = tmp_path / "topic-v1.json"
    testbed_json.write_text("{}", encoding="utf-8")
    rc = module.main(
        [
            "--evaluation-config-path", "src/config/config_tecent_backend_server_use.yaml",
            "--confirm-evaluation-database", "langtaosha_use",
            "--testbed-json", str(testbed_json),
            "--strategies", "dense", "hybrid_retrieval",
            "--output-dir", str(output_dir),
        ]
    )

    assert rc == 0
    assert (output_dir / "comparison.json").exists()
