"""Tests for retrieval-grade keyword candidate selection."""

from __future__ import annotations

from src.docset_hub.indexing.keyword_candidate_selection import KeywordCandidateSelector
from src.docset_hub.indexing.keyword_enrichment import KeywordCandidate


def ontology_candidate(text, source="umls", confidence=0.95, canonical=None):
    return KeywordCandidate.from_text(
        text=text,
        keyword_type="concept",
        source="scispacy-en_core_sci_lg-generated",
        extractor_name="unit",
        canonical=canonical or text,
        evidence={
            "ontology": [
                {
                    "source": source,
                    "canonical": canonical or text,
                    "concept_id": f"{source}:{text}",
                    "confidence": confidence,
                    "semantic_types": ["T001"],
                }
            ]
        },
    )


def test_selector_keeps_source_category_and_ontology_keyword():
    selector = KeywordCandidateSelector(max_keywords=10)
    category = KeywordCandidate.from_text(
        "neuroscience",
        "category",
        "biorxiv",
        "source_keywords",
        evidence={"source_provided": True},
    )
    microglia = ontology_candidate("microglia", source="mesh", canonical="Microglia")
    generic = KeywordCandidate.from_text(
        "these findings",
        "concept",
        "scispacy-en_core_sci_lg-noun-chunk",
        "unit",
    )

    result = selector.select([category, microglia, generic], title="Microglia regulate synapses")

    assert [item.keyword for item in result.selected] == ["neuroscience", "Microglia"]
    assert result.selected[0].reasons == ["source_keyword_or_category"]
    assert result.selected[1].ontology_sources == ["mesh"]
    assert result.unselected[0].keyword == "these findings"
    assert "low_information_surface" in result.unselected[0].reasons


def test_selector_merges_abbreviation_long_and_short_form():
    selector = KeywordCandidateSelector(max_keywords=10)
    long_form = KeywordCandidate.from_text(
        "single-cell RNA sequencing",
        "concept",
        "abbreviation-detected",
        "unit",
        canonical="single-cell RNA sequencing",
        aliases=["scRNA-seq"],
        evidence={
            "abbreviation_pair": True,
            "long_form": "single-cell RNA sequencing",
            "abbreviation": "scRNA-seq",
        },
    )
    short_form = KeywordCandidate.from_text(
        "scRNA-seq",
        "concept",
        "abbreviation-detected",
        "unit",
        canonical="single-cell RNA sequencing",
        aliases=["scRNA-seq"],
        evidence={
            "abbreviation_pair": True,
            "long_form": "single-cell RNA sequencing",
            "abbreviation": "scRNA-seq",
        },
    )

    result = selector.select([long_form, short_form])

    assert len(result.selected) == 1
    assert result.selected[0].keyword == "single-cell RNA sequencing"
    assert result.selected[0].aliases == ["scRNA-seq"]
    assert len(result.selected[0].candidates) == 2


def test_selector_filters_low_confidence_ontology_evidence():
    selector = KeywordCandidateSelector(max_keywords=10, ontology_threshold=0.9)
    loose = ontology_candidate("loose match", confidence=0.89)

    result = selector.select([loose])

    assert result.selected == []
    assert result.unselected[0].keyword == "loose match"
    assert "not_selected_by_retrieval_rules" in result.unselected[0].reasons


def test_selector_caps_selected_keywords():
    selector = KeywordCandidateSelector(max_keywords=2, ontology_cap=2)
    candidates = [
        ontology_candidate("CRISPR-Cas9", source="mesh"),
        ontology_candidate("single-cell RNA sequencing", source="mesh"),
        ontology_candidate("transcriptomics", source="mesh"),
    ]

    result = selector.select(candidates, title="CRISPR-Cas9 enables single-cell RNA sequencing")

    assert len(result.selected) == 2
    assert len(result.unselected) == 1
    assert result.selected[0].keyword in {"CRISPR-Cas9", "single-cell RNA sequencing"}
