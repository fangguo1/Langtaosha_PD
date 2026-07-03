from scripts.backfill_source_records import _build_summary_payload


def test_build_summary_payload_keeps_pg_and_dense_separate():
    payload = _build_summary_payload(
        stage="all",
        pg_stats={"total": 3, "success": 2, "failed": 1},
        vector_stats={"processed": 2, "succeeded": 2, "failed": 0},
        dry_run=False,
    )

    assert payload["status"] == "failed"
    assert payload["substeps"] == [
        {
            "step": "pg_backfill",
            "status": "failed",
            "metrics": {"total": 3, "success": 2, "failed": 1},
        },
        {
            "step": "dense_backfill",
            "status": "ok",
            "metrics": {"processed": 2, "succeeded": 2, "failed": 0},
        },
    ]


def test_build_summary_payload_marks_disabled_dense_stage():
    payload = _build_summary_payload(
        stage="pg",
        pg_stats={"total": 1, "success": 1, "failed": 0},
        vector_stats=None,
        dry_run=False,
    )

    assert payload["status"] == "ok"
    assert payload["substeps"][1] == {
        "step": "dense_backfill",
        "status": "skipped_expected",
        "reason": "stage_disabled",
    }
