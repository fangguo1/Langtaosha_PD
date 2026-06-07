"""Unit tests for case-insensitive keyword upsert behavior."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.storage.metadata_db import MetadataDB


class FakeResult:
    def __init__(self, rowcount=0, row=None):
        self.rowcount = rowcount
        self._row = row

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self):
        self.rows = []
        self.insert_sql = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        if "UPDATE paper_keywords" in sql:
            matched = 0
            for row in self.rows:
                if (
                    row["paper_id"] == params["paper_id"]
                    and row["keyword_type"].lower() == params["keyword_type"].lower()
                    and row["keyword"].lower() == params["keyword"].lower()
                    and row["source"] == params["source"]
                ):
                    row["weight"] = params["weight"]
                    matched += 1
            return FakeResult(rowcount=matched)

        if "INSERT INTO paper_keywords" in sql:
            self.insert_sql.append(sql)
            self.rows.append(
                {
                    "paper_id": params["paper_id"],
                    "keyword_type": params["keyword_type"],
                    "keyword": params["keyword"],
                    "weight": params["weight"],
                    "source": params["source"],
                }
            )
            return FakeResult(row=(True,))

        raise AssertionError(f"Unexpected SQL: {sql}")


def test_upsert_keyword_case_insensitive_updates_existing_lower_match():
    metadata_db = MetadataDB.__new__(MetadataDB)
    conn = FakeConn()

    first = metadata_db._upsert_keyword_case_insensitive(
        conn=conn,
        paper_id=1,
        keyword_type="concept",
        keyword="CRISPR",
        weight=0.7,
        source="scispacy-en_core_sci_lg-generated-test",
    )
    second = metadata_db._upsert_keyword_case_insensitive(
        conn=conn,
        paper_id=1,
        keyword_type="concept",
        keyword="crispr",
        weight=0.95,
        source="scispacy-en_core_sci_lg-generated-test",
    )

    assert first == "inserted"
    assert second == "updated"
    assert conn.rows == [
        {
            "paper_id": 1,
            "keyword_type": "concept",
            "keyword": "CRISPR",
            "weight": 0.95,
            "source": "scispacy-en_core_sci_lg-generated-test",
        }
    ]


def test_upsert_keyword_case_insensitive_keeps_sources_separate():
    metadata_db = MetadataDB.__new__(MetadataDB)
    conn = FakeConn()

    metadata_db._upsert_keyword_case_insensitive(
        conn=conn,
        paper_id=1,
        keyword_type="concept",
        keyword="CRISPR",
        weight=0.7,
        source="biorxiv",
    )
    metadata_db._upsert_keyword_case_insensitive(
        conn=conn,
        paper_id=1,
        keyword_type="concept",
        keyword="crispr",
        weight=0.95,
        source="scispacy-en_core_sci_lg-generated-test",
    )

    assert len(conn.rows) == 2
    assert {row["source"] for row in conn.rows} == {
        "biorxiv",
        "scispacy-en_core_sci_lg-generated-test",
    }


def test_upsert_keyword_insert_avoids_schema_specific_conflict_target():
    metadata_db = MetadataDB.__new__(MetadataDB)
    conn = FakeConn()

    metadata_db._upsert_keyword_case_insensitive(
        conn=conn,
        paper_id=1,
        keyword_type="category",
        keyword="neuroscience",
        weight=1.0,
        source="biorxiv",
    )

    assert len(conn.insert_sql) == 1
    assert "ON CONFLICT" not in conn.insert_sql[0]
