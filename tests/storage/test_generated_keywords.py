"""Tests for MetadataDB.upsert_generated_keywords.

Uses real payloads from test_data via the shared test_papers fixture.
"""

from __future__ import annotations

import argparse
import copy
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import _reset_config, get_db_engine, init_config
from src.docset_hub.metadata.transformer import MetadataTransformer
from src.docset_hub.storage.metadata_db import MetadataDB


_global_config_path = None


def get_config_path_from_args() -> Path:
    global _global_config_path
    if _global_config_path:
        return _global_config_path

    config_path = None
    is_pytest = any("pytest" in arg for arg in sys.argv)
    if not is_pytest:
        parser = argparse.ArgumentParser()
        parser.add_argument("--config-path", type=str, default=None)
        args, _ = parser.parse_known_args()
        if args.config_path:
            config_path = Path(args.config_path)

    if config_path is None and os.environ.get("QUERY_UNDERSTANDING_TEST_CONFIG"):
        config_path = Path(os.environ["QUERY_UNDERSTANDING_TEST_CONFIG"])

    if config_path is None:
        config_path = Path(__file__).resolve().parents[2] / "src" / "config" / "config_tecent_backend_server_test.yaml"

    if not config_path.exists():
        raise ValueError(f"Config file not found: {config_path}")

    _global_config_path = config_path
    return config_path


@pytest.fixture(scope="session")
def db_engine():
    _reset_config()
    init_config(get_config_path_from_args(), force_reload=True)
    return get_db_engine("metadata_db")


@pytest.fixture(scope="function")
def metadata_db():
    return MetadataDB(config_path=get_config_path_from_args())


@pytest.fixture(scope="session")
def transformer():
    return MetadataTransformer()


def _unique_payload(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(raw_payload)
    suffix = uuid.uuid4().hex
    numeric_id = str(910000000000 + int(suffix[:10], 16) % 99999999999)
    url = f"https://langtaosha.org.cn/lts/en/preprint/view/{numeric_id}"
    payload["url"] = url
    payload.setdefault("meta", {})["citation_abstract_html_url"] = [url]
    payload["meta"]["citation_doi"] = [f"10.65215/query-understanding-generated.{suffix}"]
    return payload


def _insert_real_payload(metadata_db, transformer, raw_payload, source_name="langtaosha") -> int:
    transform_result = transformer.transform_dict(raw_payload, source_name=source_name)
    assert transform_result.success, transform_result.error
    write_result = metadata_db.insert_paper(
        db_payload=transform_result.db_payload,
        upsert_key=transform_result.upsert_key,
    )
    assert write_result["paper_id"] is not None
    return write_result["paper_id"]


@pytest.fixture(scope="function")
def test_paper(metadata_db, transformer, test_papers):
    paper_id = _insert_real_payload(
        metadata_db,
        transformer,
        _unique_payload(test_papers["langtaosha"][0]),
        source_name="langtaosha",
    )
    try:
        yield paper_id
    finally:
        metadata_db.delete_paper_by_paper_id(paper_id)


def _fetch_keywords(db_engine, paper_id):
    with db_engine.connect() as conn:
        return conn.execute(
            text(
                """
                SELECT keyword_type, keyword, weight, source
                FROM paper_keywords
                WHERE paper_id = :paper_id
                ORDER BY source, keyword
                """
            ),
            {"paper_id": paper_id},
        ).mappings().all()


def test_upsert_generated_keywords_inserts_new_keywords(metadata_db, db_engine, test_paper):
    result = metadata_db.upsert_generated_keywords(
        test_paper,
        [
            {"keyword_type": "concept", "keyword": "machine learning", "weight": 0.9},
            {"keyword_type": "method", "keyword": "contrastive learning", "weight": 0.8},
        ],
        source="scispacy-en_core_sci_lg-generated-test",
    )

    assert result["inserted"] == 2
    rows = _fetch_keywords(db_engine, test_paper)
    assert {"machine learning", "contrastive learning"}.issubset({row["keyword"] for row in rows})


def test_upsert_generated_keywords_is_idempotent_for_same_batch(metadata_db, db_engine, test_paper):
    keywords = [{"keyword_type": "concept", "keyword": "machine learning", "weight": 0.9}]

    first = metadata_db.upsert_generated_keywords(test_paper, keywords, source="scispacy-en_core_sci_lg-generated-test")
    second = metadata_db.upsert_generated_keywords(test_paper, keywords, source="scispacy-en_core_sci_lg-generated-test")

    generated_rows = [
        row for row in _fetch_keywords(db_engine, test_paper)
        if row["source"] == "scispacy-en_core_sci_lg-generated-test" and row["keyword"] == "machine learning"
    ]
    assert first["inserted"] == 1
    assert second["updated"] == 1
    assert len(generated_rows) == 1


def test_upsert_generated_keywords_updates_weight_for_same_source(metadata_db, db_engine, test_paper):
    metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "machine learning", "weight": 0.6}],
        source="scispacy-en_core_sci_lg-generated-test",
    )
    metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "machine learning", "weight": 0.95}],
        source="scispacy-en_core_sci_lg-generated-test",
    )

    rows = [
        row for row in _fetch_keywords(db_engine, test_paper)
        if row["source"] == "scispacy-en_core_sci_lg-generated-test" and row["keyword"] == "machine learning"
    ]
    assert len(rows) == 1
    assert rows[0]["weight"] == pytest.approx(0.95)


def test_upsert_generated_keywords_keeps_different_sources_separate(metadata_db, db_engine, test_paper):
    metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "machine learning", "weight": 0.8}],
        source="scispacy-en_core_sci_lg-generated-test",
    )
    metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "machine learning", "weight": 1.0}],
        source="langtaosha",
    )

    rows = [row for row in _fetch_keywords(db_engine, test_paper) if row["keyword"] == "machine learning"]
    assert {"scispacy-en_core_sci_lg-generated-test", "langtaosha"}.issubset({row["source"] for row in rows})


def test_upsert_generated_keywords_rejects_missing_paper_id(metadata_db):
    with pytest.raises(ValueError, match="paper_id does not exist"):
        metadata_db.upsert_generated_keywords(
            999999999,
            [{"keyword_type": "concept", "keyword": "machine learning", "weight": 0.8}],
            source="scispacy-en_core_sci_lg-generated-test",
        )


def test_upsert_generated_keywords_filters_empty_keyword(metadata_db, test_paper):
    result = metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "   ", "weight": 0.8}],
        source="scispacy-en_core_sci_lg-generated-test",
    )

    assert result["inserted"] == 0
    assert result["skipped"] == 1


def test_upsert_generated_keywords_rejects_unknown_keyword_type(metadata_db, test_paper):
    result = metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "unknown", "keyword": "machine learning", "weight": 0.8}],
        source="scispacy-en_core_sci_lg-generated-test",
    )

    assert result["inserted"] == 0
    assert result["skipped"] == 1
    assert result["errors"]


def test_upsert_generated_keywords_clamps_or_rejects_invalid_weight(metadata_db, db_engine, test_paper):
    metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "machine learning", "weight": 2.0}],
        source="scispacy-en_core_sci_lg-generated-test",
    )

    rows = [
        row for row in _fetch_keywords(db_engine, test_paper)
        if row["source"] == "scispacy-en_core_sci_lg-generated-test" and row["keyword"] == "machine learning"
    ]
    assert rows[0]["weight"] == pytest.approx(1.0)


def test_upsert_generated_keywords_deduplicates_case_insensitive_terms(metadata_db, db_engine, test_paper):
    result = metadata_db.upsert_generated_keywords(
        test_paper,
        [
            {"keyword_type": "concept", "keyword": "CRISPR", "weight": 0.9},
            {"keyword_type": "concept", "keyword": "crispr", "weight": 0.8},
        ],
        source="scispacy-en_core_sci_lg-generated-test",
    )

    rows = [row for row in _fetch_keywords(db_engine, test_paper) if row["source"] == "scispacy-en_core_sci_lg-generated-test"]
    assert result["inserted"] == 1
    assert result["skipped"] == 1
    assert len(rows) == 1


def test_upsert_generated_keywords_is_case_insensitive_across_batches(metadata_db, db_engine, test_paper):
    first = metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "CRISPR", "weight": 0.7}],
        source="scispacy-en_core_sci_lg-generated-test",
    )
    second = metadata_db.upsert_generated_keywords(
        test_paper,
        [{"keyword_type": "concept", "keyword": "crispr", "weight": 0.95}],
        source="scispacy-en_core_sci_lg-generated-test",
    )

    rows = [
        row for row in _fetch_keywords(db_engine, test_paper)
        if row["source"] == "scispacy-en_core_sci_lg-generated-test"
        and row["keyword"].lower() == "crispr"
    ]
    assert first["inserted"] == 1
    assert second["updated"] == 1
    assert len(rows) == 1
    assert rows[0]["keyword"] == "CRISPR"
    assert rows[0]["weight"] == pytest.approx(0.95)
