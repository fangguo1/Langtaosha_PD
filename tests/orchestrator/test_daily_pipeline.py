from __future__ import annotations

from datetime import date
from pathlib import Path
from subprocess import CompletedProcess

import scripts.run_daily_orchestrator as daily_cli
from src.docset_hub.orchestrator.daily_pipeline import DailyPipeline, DailyPipelineConfig


def _pipeline(tmp_path: Path, **overrides) -> DailyPipeline:
    defaults = {
        "project_root": tmp_path,
        "config_path": Path("src/config/config_tecent_backend_server_test.yaml"),
        "target_date": date(2026, 6, 6),
        "run_author_enrichment": False,
        "python_executable": "/test/python",
    }
    defaults.update(overrides)
    return DailyPipeline(DailyPipelineConfig(**defaults))


def test_sparse_command_uses_resume_state_file(tmp_path):
    pipeline = _pipeline(tmp_path)

    assert pipeline._sparse_backfill_command() == [
        "/test/python",
        "scripts/backfill_sparse_collections.py",
        "--config-path",
        "src/config/config_tecent_backend_server_test.yaml",
        "--batch-size",
        "300",
        "--resume",
        "--state-file",
        str(tmp_path / "local_data" / "sparse_bm25_backfill_state_test.json"),
        "--no-progress",
    ]


def test_cli_skip_sparse_flag():
    args = daily_cli.parse_args(["--skip-sparse"])

    assert args.skip_sparse is True


def test_run_adds_sparse_after_ingest(tmp_path, monkeypatch):
    pipeline = _pipeline(tmp_path)
    calls = []

    monkeypatch.setattr(pipeline, "_fetch_biorxiv", lambda _path: {"step": "fetch_biorxiv", "status": "ok"})
    monkeypatch.setattr(pipeline, "_fetch_langtaosha", lambda: {"step": "fetch_langtaosha", "status": "ok"})
    monkeypatch.setattr(
        pipeline,
        "_ensure_empty_file",
        lambda _path, source: {"step": f"ensure_empty_{source}", "status": "ok"},
    )
    monkeypatch.setattr(
        pipeline,
        "_ingest_file",
        lambda _path, source: {"step": f"ingest_{source}", "status": "ok"},
    )

    def fake_sparse():
        calls.append("sparse")
        return {"step": "backfill_sparse_collections", "status": "ok"}

    monkeypatch.setattr(pipeline, "_backfill_sparse_collections", fake_sparse)

    manifest = pipeline.run()

    assert calls == ["sparse"]
    data_steps = [
        step["step"]
        for step in manifest["steps"]
        if step["step"].startswith("ingest_") or step["step"] == "backfill_sparse_collections"
    ]
    assert data_steps == [
        "ingest_biorxiv_daily",
        "ingest_langtaosha",
        "backfill_sparse_collections",
    ]
    assert manifest["status"] == "ok"


def test_skip_sparse_omits_sparse_step(tmp_path, monkeypatch):
    pipeline = _pipeline(tmp_path, run_sparse_stage=False)

    monkeypatch.setattr(pipeline, "_fetch_biorxiv", lambda _path: {"step": "fetch_biorxiv", "status": "ok"})
    monkeypatch.setattr(pipeline, "_fetch_langtaosha", lambda: {"step": "fetch_langtaosha", "status": "ok"})
    monkeypatch.setattr(
        pipeline,
        "_ensure_empty_file",
        lambda _path, source: {"step": f"ensure_empty_{source}", "status": "ok"},
    )
    monkeypatch.setattr(
        pipeline,
        "_ingest_file",
        lambda _path, source: {"step": f"ingest_{source}", "status": "ok"},
    )
    monkeypatch.setattr(
        pipeline,
        "_backfill_sparse_collections",
        lambda: (_ for _ in ()).throw(AssertionError("sparse should be skipped")),
    )

    manifest = pipeline.run()

    sparse_step = next(
        step for step in manifest["steps"] if step["step"] == "backfill_sparse_collections"
    )
    assert sparse_step["status"] == "skipped_expected"
    assert sparse_step["reason"] == "config_disabled"
    assert manifest["status"] == "ok"


def test_sparse_failure_is_partial_and_author_enrichment_continues(tmp_path, monkeypatch):
    pipeline = _pipeline(tmp_path, run_author_enrichment=True)
    enriched = []

    monkeypatch.setattr(pipeline, "_fetch_biorxiv", lambda _path: {"step": "fetch_biorxiv", "status": "ok"})
    monkeypatch.setattr(pipeline, "_fetch_langtaosha", lambda: {"step": "fetch_langtaosha", "status": "ok"})
    monkeypatch.setattr(
        pipeline,
        "_ensure_empty_file",
        lambda _path, source: {"step": f"ensure_empty_{source}", "status": "ok"},
    )
    monkeypatch.setattr(
        pipeline,
        "_ingest_file",
        lambda _path, source: {"step": f"ingest_{source}", "status": "ok"},
    )
    monkeypatch.setattr(
        pipeline,
        "_backfill_sparse_collections",
        lambda: {
            "step": "backfill_sparse_collections",
            "status": "degraded",
            "required": False,
            "returncode": 1,
        },
    )

    def fake_enrich(source):
        enriched.append(source)
        return {"step": f"enrich_authors_{source}", "status": "ok"}

    monkeypatch.setattr(pipeline, "_enrich_authors", fake_enrich)

    manifest = pipeline.run()

    assert enriched == ["biorxiv_daily", "langtaosha"]
    assert manifest["status"] == "degraded"


def test_sparse_dry_run_returns_command_without_running_it(tmp_path, monkeypatch):
    pipeline = _pipeline(tmp_path, dry_run=True)
    monkeypatch.setattr(
        pipeline,
        "_run_command",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("command should not run")),
    )

    step = pipeline._backfill_sparse_collections()

    assert step["step"] == "backfill_sparse_collections"
    assert step["status"] == "dry_run"
    assert "--resume" in step["command"]


def test_ingest_step_attaches_pg_and_dense_substeps(tmp_path, monkeypatch):
    pipeline = _pipeline(tmp_path)
    input_path = tmp_path / "records.jsonl"
    input_path.write_text('{"title": "example"}\n', encoding="utf-8")

    def fake_run(command, **_kwargs):
        summary_path = Path(command[command.index("--summary-json") + 1])
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            """
{
  "status": "ok",
  "substeps": [
    {"step": "pg_backfill", "status": "ok", "metrics": {"success": 1, "failed": 0}},
    {"step": "dense_backfill", "status": "ok", "metrics": {"succeeded": 1, "failed": 0}}
  ]
}
""".strip()
            + "\n",
            encoding="utf-8",
        )
        return CompletedProcess(command, 0, stdout="done", stderr="")

    monkeypatch.setattr("src.docset_hub.orchestrator.daily_pipeline.subprocess.run", fake_run)

    step = pipeline._ingest_file(input_path, "biorxiv_daily")

    assert step["status"] == "ok"
    assert step["required"] is True
    assert [substep["step"] for substep in step["substeps"]] == [
        "pg_backfill",
        "dense_backfill",
    ]
    assert step["substeps"][0]["metrics"]["success"] == 1
    assert step["substeps"][1]["metrics"]["succeeded"] == 1


def test_sparse_step_attaches_metrics_and_degrades_on_failure(tmp_path, monkeypatch):
    pipeline = _pipeline(tmp_path)

    def fake_run(command, **_kwargs):
        summary_path = Path(command[command.index("--summary-json") + 1])
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            '{"status":"failed","metrics":{"fetched":10,"indexed":8,"failed":2}}\n',
            encoding="utf-8",
        )
        return CompletedProcess(command, 1, stdout="", stderr="sparse failed")

    monkeypatch.setattr("src.docset_hub.orchestrator.daily_pipeline.subprocess.run", fake_run)

    step = pipeline._backfill_sparse_collections()

    assert step["status"] == "degraded"
    assert step["required"] is False
    assert step["metrics"] == {"fetched": 10, "indexed": 8, "failed": 2}


def test_pipeline_status_distinguishes_required_failure_and_optional_degraded():
    assert DailyPipeline._compute_pipeline_status(
        [{"status": "failed", "required": True}]
    ) == "failed"
    assert DailyPipeline._compute_pipeline_status(
        [{"status": "degraded", "required": False}]
    ) == "degraded"
    assert DailyPipeline._compute_pipeline_status(
        [{"status": "skipped_expected", "required": True}]
    ) == "ok"


def test_fetch_langtaosha_attaches_matching_daily_summary(tmp_path, monkeypatch):
    pipeline = _pipeline(tmp_path)
    summary_path = tmp_path / "local_data" / "langtaosha" / "daily_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        """
{
  "query_start_date": "2026-06-06",
  "query_end_date": "2026-06-06",
  "records_written": 4,
  "failed_urls": [{"url": "bad"}],
  "total_preprint_urls": 100
}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        pipeline,
        "_run_command",
        lambda *_args, **_kwargs: {
            "step": "fetch_langtaosha",
            "status": "ok",
            "required": True,
            "returncode": 0,
        },
    )

    step = pipeline._fetch_langtaosha()

    assert step["metrics"] == {
        "records_out": 4,
        "failed_urls": 1,
        "total_preprint_urls": 100,
    }
