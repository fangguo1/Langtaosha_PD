"""Tests for scispaCy-based KeywordEnrichmentService."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.docset_hub.indexing.keyword_enrichment import (  # noqa: E402
    DEFAULT_KEYWORD_SOURCES,
    KeywordEnrichmentService,
)


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


class FakeEntity:
    def __init__(self, text, label):
        self.text = text
        self.label_ = label


class FakeDoc:
    def __init__(self, entities):
        self.ents = entities


class FakeNlp:
    def __init__(self, entities):
        self.entities = entities

    def __call__(self, text):
        return FakeDoc(self.entities)


def _service(max_keywords=12, model_names=None):
    return KeywordEnrichmentService(
        config_path=get_config_path_from_args(),
        model_names=model_names or ["en_core_sci_lg", "en_ner_bionlp13cg_md"],
        max_keywords=max_keywords,
    )


def _real_title(test_papers):
    return test_papers["langtaosha"][0]["meta"]["citation_title"][0]


def _real_abstract(test_papers):
    return test_papers["langtaosha"][0]["meta"]["citation_abstract"][0]


def test_extract_keywords_runs_two_scispacy_sources(monkeypatch):
    entities_by_model = {
        "en_core_sci_lg": [
            FakeEntity("single-cell RNA sequencing", "ENTITY"),
            FakeEntity("single-cell RNA sequencing", "ENTITY"),
            FakeEntity("study", "ENTITY"),
        ],
        "en_ner_bionlp13cg_md": [
            FakeEntity("TP53", "GENE_OR_GENE_PRODUCT"),
            FakeEntity("glioblastoma", "CANCER"),
        ],
    }
    service = _service()
    monkeypatch.setattr(service, "_load_model", lambda model: FakeNlp(entities_by_model[model]))

    result = service.extract_keywords(title="Title", abstract="Abstract")

    assert result.success is True
    assert {item["source"] for item in result.keywords} == {
        DEFAULT_KEYWORD_SOURCES["en_core_sci_lg"],
        DEFAULT_KEYWORD_SOURCES["en_ner_bionlp13cg_md"],
    }
    assert {"single-cell RNA sequencing", "TP53", "glioblastoma"}.issubset(
        {item["keyword"] for item in result.keywords}
    )
    assert "study" not in {item["keyword"].lower() for item in result.keywords}


def test_extract_keywords_maps_scispacy_labels_to_keyword_types(monkeypatch):
    service = _service(model_names=["en_ner_bionlp13cg_md"])
    monkeypatch.setattr(
        service,
        "_load_model",
        lambda model: FakeNlp(
            [
                FakeEntity("TP53", "GENE_OR_GENE_PRODUCT"),
                FakeEntity("aspirin", "SIMPLE_CHEMICAL"),
                FakeEntity("mouse", "ORGANISM"),
                FakeEntity("lymphoma", "CANCER"),
            ]
        ),
    )

    result = service.extract_keywords(title="Title", abstract="Abstract")

    assert {item["keyword"]: item["keyword_type"] for item in result.keywords} == {
        "TP53": "gene",
        "aspirin": "chemical",
        "mouse": "organism",
        "lymphoma": "disease",
    }


def test_extract_keywords_truncates_each_model_to_max_keywords(monkeypatch):
    service = _service(max_keywords=2, model_names=["en_core_sci_lg"])
    monkeypatch.setattr(
        service,
        "_load_model",
        lambda model: FakeNlp([FakeEntity(f"term {idx}", "ENTITY") for idx in range(5)]),
    )

    result = service.extract_keywords(title="Title", abstract="Abstract")

    assert len(result.keywords) == 2


def test_extract_keywords_deduplicates_normalized_keywords(monkeypatch):
    service = _service(model_names=["en_core_sci_lg"])
    monkeypatch.setattr(
        service,
        "_load_model",
        lambda model: FakeNlp([FakeEntity("CRISPR", "ENTITY"), FakeEntity("crispr", "ENTITY")]),
    )

    result = service.extract_keywords(title="Title", abstract="Abstract")

    assert len(result.keywords) == 1
    assert result.keywords[0]["keyword"] == "CRISPR"


def test_extract_keywords_handles_empty_title_and_abstract():
    service = _service()

    result = service.extract_keywords(title="", abstract="")

    assert result.success is False
    assert result.skipped is True
    assert result.skip_reason == "empty_title_and_abstract"


def test_extract_keywords_preserves_model_source_metadata():
    service = KeywordEnrichmentService(
        config_path=get_config_path_from_args(),
        model_names=["en_core_sci_lg"],
        source="scispacy-test-source",
    )

    result = service.extract_keywords(title="", abstract="")

    assert result.model_name == "en_core_sci_lg"
    assert result.source == "scispacy-test-source"
    assert result.prompt_version == "scispacy-v1"


def test_extract_keywords_returns_failure_when_all_models_fail(monkeypatch):
    service = _service(model_names=["missing_model"])

    def _raise(model):
        raise RuntimeError("not installed")

    monkeypatch.setattr(service, "_load_model", _raise)

    result = service.extract_keywords(title="Title", abstract="Abstract")

    assert result.success is False
    assert "missing_model" in result.error
    assert result.model_results[0]["success"] is False


@pytest.mark.integration
def test_extract_keywords_live_scispacy_models(test_papers):
    pytest.importorskip("en_core_sci_lg")
    pytest.importorskip("en_ner_bionlp13cg_md")

    service = KeywordEnrichmentService(config_path=get_config_path_from_args(), max_keywords=3)
    result = service.extract_keywords(
        title=_real_title(test_papers),
        abstract=_real_abstract(test_papers),
    )

    assert result.success is True
    assert len(result.keywords) > 0
    assert {item["source"] for item in result.keywords} == set(service.sources)
