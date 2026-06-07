"""Tests for AuthorMatcher using a fake MetadataDB candidate source."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.indexing.query_understanding import AuthorMatcher


class FakeMetadataDB:
    def __init__(self, candidates_by_query):
        self.candidates_by_query = candidates_by_query
        self.calls = []

    def suggest_author_names(self, query, limit=5):
        self.calls.append({"query": query, "limit": limit})
        return list(self.candidates_by_query.get(query, []))


def _candidate(name, score, paper_count=1):
    return {
        "name": name,
        "normalized_name": " ".join(name.lower().replace(".", " ").replace(",", " ").split()),
        "score": score,
        "paper_count": paper_count,
    }


def test_author_matcher_exact_match_is_author():
    db = FakeMetadataDB({"Alice Zhang": [_candidate("Alice Zhang", 1.0, 3)]})

    result = AuthorMatcher(db).match("Alice Zhang")

    assert result["is_author"] is True
    assert result["matched_author"] == "Alice Zhang"
    assert result["confidence"] == 1.0
    assert result["reason"] == "author_candidate_exact_match"


def test_author_matcher_fuzzy_match_is_author_above_threshold():
    db = FakeMetadataDB({"Alce Zhang": [_candidate("Alice Zhang", 0.94, 3)]})

    result = AuthorMatcher(db).match("Alce Zhang")

    assert result["is_author"] is True
    assert result["matched_author"] == "Alice Zhang"
    assert result["confidence"] == 0.94


def test_author_matcher_topic_query_is_not_author():
    db = FakeMetadataDB({"machine learning": [_candidate("Machi Learning", 0.81, 1)]})

    result = AuthorMatcher(db).match("machine learning")

    assert result["is_author"] is False
    assert result["matched_author"] is None


def test_author_matcher_topic_hint_words_reduce_confidence():
    db = FakeMetadataDB({"cancer cell therapy": [_candidate("Cancer Cell Therapi", 0.95, 1)]})

    result = AuthorMatcher(db).match("cancer cell therapy")

    assert result["is_author"] is False
    assert result["confidence"] < 0.80
    assert result["reason"] == "topic_hint_reduced_confidence"


def test_author_matcher_single_token_multiple_candidates_is_ambiguous():
    db = FakeMetadataDB(
        {
            "Zhang": [
                _candidate("Alice Zhang", 0.95, 2),
                _candidate("Andrew Zhang", 0.94, 1),
            ]
        }
    )

    result = AuthorMatcher(db).match("Zhang")

    assert result["is_author"] is False
    assert result["reason"] == "author_candidate_ambiguous"
    assert [item["name"] for item in result["candidates"]] == ["Alice Zhang", "Andrew Zhang"]


def test_author_matcher_middle_score_returns_candidates_only():
    db = FakeMetadataDB({"Al Zhang": [_candidate("Alice Zhang", 0.86, 2)]})

    result = AuthorMatcher(db).match("Al Zhang")

    assert result["is_author"] is False
    assert result["reason"] == "author_candidate_middle_confidence"
    assert result["candidates"][0]["name"] == "Alice Zhang"


def test_author_matcher_no_candidates_is_not_author():
    db = FakeMetadataDB({"Unknown Person": []})

    result = AuthorMatcher(db).match("Unknown Person")

    assert result["is_author"] is False
    assert result["reason"] == "no_author_candidates"


def test_author_matcher_limits_metadata_db_candidate_query_size():
    db = FakeMetadataDB({"Alice Zhang": [_candidate("Alice Zhang", 1.0)]})

    AuthorMatcher(db).match("Alice Zhang")

    assert db.calls == [{"query": "Alice Zhang", "limit": 5}]
