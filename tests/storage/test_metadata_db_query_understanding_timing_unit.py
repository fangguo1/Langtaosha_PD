"""Unit tests for MetadataDB query-understanding timing diagnostics."""

from __future__ import annotations

import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.storage.metadata_db import MetadataDB


class FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = list(rows or [])
        self._scalar_value = scalar_value

    def fetchall(self):
        return list(self._rows)

    def scalar(self):
        return self._scalar_value


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.engine.executed_sql.append(sql)
        self.engine.executed_params.append(dict(params or {}))
        if "CREATE EXTENSION IF NOT EXISTS pg_trgm" in sql:
            return FakeResult()
        if "FROM pg_extension" in sql:
            return FakeResult(scalar_value=self.engine.pg_trgm_exists)
        if "COUNT(DISTINCT paper_id) AS paper_count" in sql:
            return FakeResult(rows=self.engine.author_rows)
        if "COUNT(DISTINCT paper_id) AS doc_count" in sql:
            return FakeResult(rows=self.engine.keyword_rows)
        raise AssertionError(f"Unexpected SQL: {sql}")

    def commit(self):
        self.engine.commit_count += 1


class FakeEngine:
    def __init__(self, *, author_rows=None, keyword_rows=None, pg_trgm_exists=True):
        self.author_rows = list(author_rows or [])
        self.keyword_rows = list(keyword_rows or [])
        self.pg_trgm_exists = pg_trgm_exists
        self.executed_sql = []
        self.executed_params = []
        self.commit_count = 0

    def connect(self):
        return FakeConnection(self)


def _build_metadata_db(engine: FakeEngine) -> MetadataDB:
    metadata_db = MetadataDB.__new__(MetadataDB)
    metadata_db.engine = engine
    return metadata_db


def test_ensure_pg_trgm_extension_runs_once_during_initialization(caplog):
    metadata_db = _build_metadata_db(FakeEngine())

    with caplog.at_level(logging.INFO):
        metadata_db._ensure_pg_trgm_extension()

    assert metadata_db._pg_trgm_available is True
    assert metadata_db.engine.commit_count == 1
    assert any("CREATE EXTENSION IF NOT EXISTS pg_trgm" in sql for sql in metadata_db.engine.executed_sql)
    assert "pg_trgm extension ensured" in caplog.text


def test_suggest_author_names_logs_sql_breakdown_without_extension_sql(caplog):
    metadata_db = _build_metadata_db(
        FakeEngine(author_rows=[("Alice Zhang", 3, 0, 2, 0.97)])
    )
    metadata_db._pg_trgm_available = True

    with caplog.at_level(logging.INFO):
        results = metadata_db.suggest_author_names("Alice Zhang", limit=1)

    assert results[0]["name"] == "Alice Zhang"
    assert not any("CREATE EXTENSION IF NOT EXISTS pg_trgm" in sql for sql in metadata_db.engine.executed_sql)
    assert "suggest_author_names" in caplog.text
    assert "sql_elapsed_ms=" in caplog.text
    assert "postprocess_elapsed_ms=" in caplog.text
    assert any("source_name = :author_source_name" in sql for sql in metadata_db.engine.executed_sql)
    assert any(params.get("author_source_name") == "langtaosha" for params in metadata_db.engine.executed_params)


def test_suggest_query_terms_logs_sql_breakdown(caplog):
    metadata_db = _build_metadata_db(
        FakeEngine(keyword_rows=[("breast cancer risk", "concept", "scispacy", 4, 1.2)])
    )
    metadata_db._pg_trgm_available = False

    with caplog.at_level(logging.INFO):
        results = metadata_db.suggest_query_terms("breast cancer risk", limit=1)

    assert results[0]["keyword"] == "breast cancer risk"
    assert "suggest_query_terms" in caplog.text
    assert "sql_elapsed_ms=" in caplog.text
    assert "postprocess_elapsed_ms=" in caplog.text


def test_suggest_query_terms_defaults_to_top_ten_candidates():
    rows = [
        (f"keyword-{index}", "concept", "scispacy", index + 1, 1.0)
        for index in range(12)
    ]
    metadata_db = _build_metadata_db(FakeEngine(keyword_rows=rows))
    metadata_db._pg_trgm_available = False

    results = metadata_db.suggest_query_terms("breast cancer risk", limit=20)

    assert len(results) == 10
    assert [item["keyword"] for item in results] == [f"keyword-{index}" for index in range(10)]
