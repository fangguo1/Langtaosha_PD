"""Tests for phrase segmentation used by phrase-aware query correction."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.indexing.query_understanding import PhraseSegmenter


class FakeSpan:
    def __init__(self, text, start_char, end_char):
        self.text = text
        self.start_char = start_char
        self.end_char = end_char


class FakeDoc:
    def __init__(self, ents=None, noun_chunks=None):
        self.ents = ents or []
        self._noun_chunks = noun_chunks or []

    @property
    def noun_chunks(self):
        return iter(self._noun_chunks)


class FakeNLP:
    def __call__(self, text):
        return FakeDoc(
            ents=[FakeSpan("BRCA1 mutation", 0, 14)],
            noun_chunks=[FakeSpan("cancer cell therapy", 18, 37)],
        )


def _span_texts(spans):
    return [span.text for span in spans]


def test_phrase_segmenter_splits_on_prepositions_and_connectors():
    spans = PhraseSegmenter().segment("machien learing for cancr cell therpy")

    assert "machien learing" in _span_texts(spans)
    assert "cancr cell therpy" in _span_texts(spans)


def test_phrase_segmenter_keeps_offsets_in_normalized_query():
    query = "machien learing for cancr cell therpy"
    spans = PhraseSegmenter().segment(query)
    first = next(span for span in spans if span.text == "machien learing")

    assert query[first.start:first.end] == "machien learing"


def test_phrase_segmenter_uses_scispacy_spans_when_available():
    spans = PhraseSegmenter(nlp=FakeNLP()).segment("BRCA1 mutation in cancer cell therapy")

    assert any(span.text == "BRCA1 mutation" and span.source == "scispacy_entity" for span in spans)
    assert any(span.text == "cancer cell therapy" and span.source == "scispacy_noun_chunk" for span in spans)


def test_phrase_segmenter_falls_back_to_ngrams_when_scispacy_unavailable():
    spans = PhraseSegmenter(max_ngram=3).segment("solvent formtion")

    assert any(span.text == "solvent formtion" and span.source in {"rule_split", "ngram"} for span in spans)


def test_phrase_segmenter_filters_short_and_stopword_only_spans():
    spans = PhraseSegmenter().segment("AI for the")

    assert all(span.text.lower() != "ai" for span in spans)
    assert all(span.text.lower() != "the" for span in spans)


def test_phrase_segmenter_deduplicates_equivalent_spans():
    spans = PhraseSegmenter().segment("solvent formtion")
    matching = [span for span in spans if span.text == "solvent formtion"]

    assert len(matching) == 1


def test_phrase_segmenter_preserves_biomedical_symbols():
    spans = PhraseSegmenter(max_ngram=3).segment("CRISPR-Cas9 mediated repair")

    assert any("CRISPR-Cas9" in span.text for span in spans)
