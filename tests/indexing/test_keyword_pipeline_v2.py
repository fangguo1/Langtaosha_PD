"""Unit tests for audit-only keyword enrichment pipeline v2."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))


def load_keyword_enrichment_module():
    module_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "docset_hub"
        / "indexing"
        / "keyword_enrichment.py"
    )
    spec = importlib.util.spec_from_file_location("keyword_enrichment_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


kw_module = load_keyword_enrichment_module()
AbbreviationExtractor = kw_module.AbbreviationExtractor
KeywordCandidate = kw_module.KeywordCandidate
KeywordConceptEnricher = kw_module.KeywordConceptEnricher
KeywordEnrichmentPipeline = kw_module.KeywordEnrichmentPipeline
KeywordExtractionInput = kw_module.KeywordExtractionInput
KeywordExtractorBase = kw_module.KeywordExtractorBase
KeywordPruner = kw_module.KeywordPruner
OntologyConceptEnricher = kw_module.OntologyConceptEnricher
SourceKeywordExtractor = kw_module.SourceKeywordExtractor
TextPreprocessor = kw_module.TextPreprocessor


class StaticExtractor(KeywordExtractorBase):
    name = "static"
    source = "static-source"

    def __init__(self, candidates):
        self.candidates = candidates

    def extract(self, item: KeywordExtractionInput):
        return list(self.candidates)


class StaticEnricher(KeywordConceptEnricher):
    def enrich(self, candidates):
        return [
            candidate.copy_with(evidence={**candidate.evidence, "static_enriched": True})
            for candidate in candidates
        ]


class FakeEvidence:
    def __init__(self, canonical, aliases=None, semantic_types=None, confidence=0.99):
        self.canonical = canonical
        self.aliases = aliases or []
        self.semantic_types = semantic_types or []
        self.confidence = confidence
        self.source = "mesh"
        self.concept_id = "M000001"
        self.match_type = "mesh_entity_link"

    def to_dict(self):
        return {
            "source": self.source,
            "concept_id": self.concept_id,
            "canonical": self.canonical,
            "confidence": self.confidence,
            "match_type": self.match_type,
            "aliases": self.aliases,
            "semantic_types": self.semantic_types,
        }


class FakeMatcher:
    def match_many(self, candidates):
        return [[FakeEvidence("Cryoelectron Microscopy", aliases=["cryo EM"])] for _ in candidates]


class LowConfidenceMatcher:
    def match_many(self, candidates):
        return [[FakeEvidence("Loose Match", confidence=0.89)] for _ in candidates]


def test_text_preprocessor_strips_html_and_figure_noise():
    item = TextPreprocessor().preprocess(
        title='ALT="Figure 1" <b>CRISPR-Cas9</b> study',
        abstract="Figure 2: RNA-seq in cells",
    )

    assert "ALT=" not in item.text
    assert "<b>" not in item.text
    assert "CRISPR-Cas9" in item.text
    assert "RNA-seq" in item.text


def test_source_keyword_extractor_preserves_source_metadata():
    item = KeywordExtractionInput(
        text="",
        source_keywords=[
            {
                "keyword_type": "category",
                "keyword": "neuroscience",
                "source": "biorxiv",
                "weight": 1.0,
            }
        ],
    )

    candidates = SourceKeywordExtractor().extract(item)

    assert len(candidates) == 1
    assert candidates[0].keyword_type == "category"
    assert candidates[0].source == "biorxiv"
    assert candidates[0].text == "neuroscience"


def test_abbreviation_extractor_detects_long_form_pair():
    item = KeywordExtractionInput(
        text="Enhancing routine noninvasive prenatal testing (NIPT) with cell-free DNA."
    )

    candidates = AbbreviationExtractor().extract(item)

    assert {candidate.text for candidate in candidates} == {
        "noninvasive prenatal testing",
        "NIPT",
    }
    assert all(candidate.canonical == "noninvasive prenatal testing" for candidate in candidates)
    assert all("NIPT" in candidate.aliases for candidate in candidates)


def test_keyword_pruner_drops_only_explicit_drop_rules():
    pruner = KeywordPruner()
    hard_keyword = KeywordCandidate.from_text("study", "concept", "test", "unit")
    hard_phrase = KeywordCandidate.from_text("associated with", "concept", "test", "unit")
    noise = KeywordCandidate.from_text('ALT="Figure 1', "gene", "test", "unit")
    stopword = KeywordCandidate.from_text("and", "concept", "test", "unit")
    too_short = KeywordCandidate.from_text("x", "concept", "test", "unit")
    too_long = KeywordCandidate.from_text("x" * 201, "concept", "test", "unit")

    assert pruner.decide(hard_keyword).action == "drop"
    assert pruner.decide(hard_phrase).action == "drop"
    assert pruner.decide(noise).action == "drop"
    assert pruner.decide(stopword).action == "drop"
    assert pruner.decide(too_short).action == "drop"
    assert pruner.decide(too_long).action == "drop"


def test_keyword_pruner_keeps_everything_else_for_now():
    pruner = KeywordPruner()

    for keyword in (
        "cell",
        "cellular",
        "AD",
        "microglia",
        "synaptic",
        "cortical",
        "mitochondrial",
        "microbial",
        "neural network",
    ):
        decision = pruner.decide(
            KeywordCandidate.from_text(keyword, "concept", "test", "unit")
        )
        assert decision.action == "keep"


def test_ontology_enricher_adds_canonical_without_unanchored_candidates():
    candidate = KeywordCandidate.from_text("cryo-EM", "concept", "test", "unit")
    enricher = OntologyConceptEnricher(matcher=FakeMatcher())

    enriched = enricher.enrich([candidate])

    assert len(enriched) == 1
    assert enriched[0].canonical == "Cryoelectron Microscopy"
    assert "cryo EM" in enriched[0].aliases
    assert enriched[0].evidence["ontology"][0]["source"] == "mesh"


def test_ontology_enricher_filters_below_default_threshold():
    candidate = KeywordCandidate.from_text("cryo-EM", "concept", "test", "unit")
    enricher = OntologyConceptEnricher(matcher=LowConfidenceMatcher())

    enriched = enricher.enrich([candidate])

    assert len(enriched) == 1
    assert enriched[0].canonical is None
    assert "ontology" not in enriched[0].evidence


def test_pipeline_generates_enriches_and_prunes_without_ranking():
    candidates = [
        KeywordCandidate.from_text("associated with", "concept", "test", "unit"),
        KeywordCandidate.from_text("single-cell RNA sequencing", "concept", "test", "unit"),
    ]
    pipeline = KeywordEnrichmentPipeline(
        extractors=[StaticExtractor(candidates)],
        enrichers=[StaticEnricher()],
        pruner=KeywordPruner(),
    )

    result = pipeline.run(text="single-cell RNA sequencing associated with disease")

    assert [candidate.text for candidate in result.kept] == ["single-cell RNA sequencing"]
    assert result.dropped[0].candidate.text == "associated with"
    assert result.kept[0].evidence["static_enriched"] is True
