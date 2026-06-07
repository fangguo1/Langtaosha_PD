"""Tests for phrase-aware query correction."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.indexing.query_understanding import (
    PhraseAwareQueryCorrector,
    PhraseSpan,
)


class FakeMetadataDB:
    def __init__(self, terms_by_query):
        self.terms_by_query = terms_by_query
        self.calls = []

    def suggest_query_terms(self, query, limit=20):
        self.calls.append({"query": query, "limit": limit})
        return list(self.terms_by_query.get(query, []))


class FakeSegmenter:
    def __init__(self, spans):
        self.spans = spans

    def segment(self, query):
        return list(self.spans)


class FailingSegmenter:
    def segment(self, query):
        raise RuntimeError("segmenter unavailable")


def _term(keyword, source="scispacy-en_core_sci_lg-generated", doc_count=2):
    return {
        "keyword": keyword,
        "keyword_type": "concept",
        "source": source,
        "doc_count": doc_count,
        "avg_weight": 1.0,
    }


def test_phrase_corrector_corrects_multiple_phrases_in_sentence():
    spans = [
        PhraseSpan("solvent formtion", 0, 16, "rule_split", 2),
        PhraseSpan("cancr cell therpy", 21, 38, "rule_split", 3),
    ]
    db = FakeMetadataDB(
        {
            "solvent formtion": [_term("solvent formation")],
            "cancr cell therpy": [_term("cancer cell therapy")],
        }
    )

    result = PhraseAwareQueryCorrector(db, phrase_segmenter=FakeSegmenter(spans)).correct(
        "solvent formtion for cancr cell therpy"
    )

    assert result["auto_apply"] is True
    assert result["corrected_query"] == "solvent formation for cancer cell therapy"
    assert [item["corrected"] for item in result["corrections"]] == [
        "solvent formation",
        "cancer cell therapy",
    ]


def test_phrase_corrector_matches_lowercase_query_casing():
    spans = [PhraseSpan("machine learing", 0, 15, "rule_split", 2)]
    db = FakeMetadataDB({"machine learing": [_term("Machine Learning")]})

    result = PhraseAwareQueryCorrector(db, phrase_segmenter=FakeSegmenter(spans)).correct(
        "machine learing for cells"
    )

    assert result["corrected_query"] == "machine learning for cells"
    assert result["corrections"][0]["corrected"] == "machine learning"


def test_phrase_corrector_preserves_uppercase_acronyms_from_candidate():
    spans = [PhraseSpan("RNA structre", 0, 12, "rule_split", 2)]
    db = FakeMetadataDB({"RNA structre": [_term("RNA structure")]})

    result = PhraseAwareQueryCorrector(db, phrase_segmenter=FakeSegmenter(spans)).correct(
        "RNA structre for cells"
    )

    assert result["corrected_query"] == "RNA structure for cells"


def test_phrase_corrector_reuses_metadata_db_suggest_query_terms_per_phrase():
    spans = [PhraseSpan("solvent formtion", 0, 16, "rule_split", 2)]
    db = FakeMetadataDB({"solvent formtion": [_term("solvent formation")]})

    PhraseAwareQueryCorrector(db, phrase_segmenter=FakeSegmenter(spans)).correct(
        "solvent formtion for cells"
    )

    assert db.calls == [{"query": "solvent formtion", "limit": 20}]


def test_phrase_corrector_returns_middle_confidence_suggestion_without_auto_apply():
    spans = [PhraseSpan("abcde", 0, 5, "rule_split", 1)]
    db = FakeMetadataDB({"abcde": [_term("abxde")]})
    corrector = PhraseAwareQueryCorrector(db, phrase_segmenter=FakeSegmenter(spans))
    corrector.SUGGEST_THRESHOLD = 0.5
    corrector.AUTO_APPLY_THRESHOLD = 0.99

    result = corrector.correct("abcde for cells")

    assert result["auto_apply"] is False
    assert result["corrected_query"] is None
    assert result["corrections"][0]["corrected"] == "abxde"


def test_phrase_corrector_selects_non_overlapping_highest_score_corrections():
    spans = [
        PhraseSpan("solvent formtion", 0, 16, "ngram", 2),
        PhraseSpan("formtion", 8, 16, "ngram", 1),
    ]
    db = FakeMetadataDB(
        {
            "solvent formtion": [_term("solvent formation")],
            "formtion": [_term("formation")],
        }
    )

    result = PhraseAwareQueryCorrector(db, phrase_segmenter=FakeSegmenter(spans)).correct(
        "solvent formtion for cells"
    )

    assert [item["original"] for item in result["corrections"]] == ["solvent formtion"]


def test_phrase_corrector_falls_back_to_whole_query_corrector_when_no_phrase_match():
    db = FakeMetadataDB({"solvent formtion for cells": [_term("solvent formation for cells")]})

    result = PhraseAwareQueryCorrector(db, phrase_segmenter=FailingSegmenter()).correct(
        "solvent formtion for cells"
    )

    assert result["corrected_query"] == "solvent formation for cells"
    assert result["fallback_reason"] == "phrase_segmentation_failed"


def test_phrase_corrector_does_not_correct_author_like_phrase():
    spans = [PhraseSpan("Alice Zhang", 0, 11, "rule_split", 2)]
    db = FakeMetadataDB({"Alice Zhang": [_term("Alice syndrome")]})

    result = PhraseAwareQueryCorrector(db, phrase_segmenter=FakeSegmenter(spans)).correct(
        "Alice Zhang for cells"
    )

    assert result["corrected_query"] is None
    assert result["corrections"] == []
    assert {"query": "Alice Zhang", "limit": 20} not in db.calls


def test_phrase_corrector_keeps_single_phrase_whole_query_behavior():
    db = FakeMetadataDB({"solvent formtion": [_term("solvent formation")]})

    result = PhraseAwareQueryCorrector(db).correct("solvent formtion")

    assert result["corrected_query"] == "solvent formation"
    assert result["reason"] == "query_term_high_confidence"
    assert result["corrections"] == []
