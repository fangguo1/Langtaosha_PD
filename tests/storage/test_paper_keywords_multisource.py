"""Tests for paper_keywords multi-source primary key behavior.

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
    numeric_id = str(920000000000 + int(suffix[:10], 16) % 99999999999)
    url = f"https://langtaosha.org.cn/lts/en/preprint/view/{numeric_id}"
    payload["url"] = url
    payload.setdefault("meta", {})["citation_abstract_html_url"] = [url]
    payload["meta"]["citation_doi"] = [f"10.65215/query-understanding-multisource.{suffix}"]
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


def _insert_keyword(db_engine, paper_id, source, weight=1.0):
    with db_engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO paper_keywords (paper_id, keyword_type, keyword, weight, source)
                VALUES (:paper_id, 'concept', 'CRISPR', :weight, :source)
                ON CONFLICT (paper_id, keyword_type, keyword, source)
                DO UPDATE SET weight = EXCLUDED.weight
                """
            ),
            {"paper_id": paper_id, "weight": weight, "source": source},
        )
        conn.commit()


def _fetch_keywords(db_engine, paper_id, keyword="CRISPR"):
    with db_engine.connect() as conn:
        return conn.execute(
            text(
                """
                SELECT keyword_type, keyword, weight, source
                FROM paper_keywords
                WHERE paper_id = :paper_id AND keyword = :keyword
                ORDER BY source
                """
            ),
            {"paper_id": paper_id, "keyword": keyword},
        ).mappings().all()


def test_paper_keywords_primary_key_includes_source(db_engine):
    with db_engine.connect() as conn:
        columns = conn.execute(
            text(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a
                  ON a.attrelid = i.indrelid
                 AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'paper_keywords'::regclass
                  AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)
                """
            )
        ).scalars().all()

    assert columns == ["paper_id", "keyword_type", "keyword", "source"]


def test_same_keyword_can_have_multiple_sources(db_engine, test_paper):
    _insert_keyword(db_engine, test_paper, "biorxiv")
    _insert_keyword(db_engine, test_paper, "scispacy-en_core_sci_lg-generated")

    rows = _fetch_keywords(db_engine, test_paper)
    assert {row["source"] for row in rows} == {"biorxiv", "scispacy-en_core_sci_lg-generated"}


def test_same_keyword_same_source_upserts_weight(db_engine, test_paper):
    _insert_keyword(db_engine, test_paper, "scispacy-en_core_sci_lg-generated", weight=0.7)
    _insert_keyword(db_engine, test_paper, "scispacy-en_core_sci_lg-generated", weight=0.9)

    rows = _fetch_keywords(db_engine, test_paper)
    assert len(rows) == 1
    assert rows[0]["weight"] == pytest.approx(0.9)


def test_null_source_is_backfilled_before_not_null_constraint(db_engine):
    with db_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = 'paper_keywords'
                  AND column_name = 'source'
                """
            )
        ).fetchone()

    assert row[0] == "NO"
    assert "paper_metadata" in row[1]


def test_legacy_and_generated_sources_coexist(db_engine, test_paper):
    _insert_keyword(db_engine, test_paper, "langtaosha", weight=1.0)
    _insert_keyword(db_engine, test_paper, "scispacy-en_core_sci_lg-generated", weight=0.8)

    rows = _fetch_keywords(db_engine, test_paper)
    assert {row["source"] for row in rows} == {"langtaosha", "scispacy-en_core_sci_lg-generated"}
