from __future__ import annotations

from sqlalchemy import create_engine, text

from src.docset_hub.indexing.dense_result_filter import (
    build_dense_keyword_filter_terms,
    filter_dense_results_by_hard_rules,
)


class FakeMetadataDB:
    def __init__(self):
        self.engine = create_engine("sqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE paper_keywords (
                        paper_id INTEGER NOT NULL,
                        keyword_type TEXT,
                        keyword TEXT NOT NULL,
                        source TEXT NOT NULL,
                        weight REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO paper_keywords (
                        paper_id,
                        keyword_type,
                        keyword,
                        source,
                        weight
                    ) VALUES
                        (1, 'concept', 'cryo-EM', 'generated', 1.0),
                        (2, 'concept', 'unrelated metabolism', 'generated', 1.0),
                        (3, 'concept', 'cryo-electron microscopy', 'generated', 1.0)
                    """
                )
            )


def test_build_dense_keyword_filter_terms_adds_hyphen_variants():
    terms = build_dense_keyword_filter_terms("cryo-em")

    assert "cryo-em" in terms
    assert "cryo em" in terms
    assert "cryo" in terms
    assert "em" not in terms


def test_filter_dense_results_prunes_by_similarity_and_keyword_presence():
    metadata_db = FakeMetadataDB()
    results = [
        {"paper_id": 1, "work_id": "W1", "similarity": 0.61},
        {"paper_id": 2, "work_id": "W2", "similarity": 0.72},
        {"paper_id": 3, "work_id": "W3", "similarity": 0.45},
    ]

    kept, report = filter_dense_results_by_hard_rules(
        metadata_db=metadata_db,
        query="cryo-em",
        results=results,
        min_similarity=0.46,
        keyword_sources=["generated"],
    )

    assert [item["work_id"] for item in kept] == ["W1"]
    assert report.initial_count == 3
    assert report.kept_count == 1
    assert report.score_pruned_count == 1
    assert report.keyword_pruned_count == 1
    assert kept[0]["retrieval_debug"]["dense_hard_filter"]["matched_keywords"][0]["keyword"] == "cryo-EM"


def test_filter_dense_results_keeps_keyword_substring_match():
    metadata_db = FakeMetadataDB()
    results = [
        {"paper_id": 3, "work_id": "W3", "similarity": 0.55},
    ]

    kept, report = filter_dense_results_by_hard_rules(
        metadata_db=metadata_db,
        query="electron microscopy",
        results=results,
        min_similarity=0.46,
        keyword_sources=["generated"],
    )

    assert [item["work_id"] for item in kept] == ["W3"]
    assert report.keyword_pruned_count == 0
