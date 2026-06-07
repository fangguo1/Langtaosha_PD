"""PaperIndexer keyword enrichment integration tests."""

from __future__ import annotations

import argparse
import os
import copy
import json
import uuid
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import _reset_config, init_config
from src.docset_hub.indexing.keyword_enrichment import (
    DEFAULT_KEYWORD_SOURCES,
    KeywordExtractionResult,
)
from src.docset_hub.indexing import PaperIndexer
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


@pytest.fixture(scope="function")
def clean_db():
    context = {"paper_ids": []}
    yield context
    metadata_db = MetadataDB(config_path=get_config_path_from_args())
    for paper_id in context["paper_ids"]:
        metadata_db.delete_paper_by_paper_id(paper_id)


def _indexer(enable_keyword_enrichment=False):
    _reset_config()
    init_config(get_config_path_from_args(), force_reload=True)
    return PaperIndexer(
        config_path=get_config_path_from_args(),
        enable_vectorization=False,
        enable_keyword_enrichment=enable_keyword_enrichment,
    )


def _unique_langtaosha_payload(test_papers):
    payload = copy.deepcopy(test_papers["langtaosha"][0])
    suffix = uuid.uuid4().hex
    numeric_id = str(900000000000 + int(suffix[:10], 16) % 99999999999)
    url = f"https://langtaosha.org.cn/lts/en/preprint/view/{numeric_id}"
    doi = f"10.65215/query-understanding-test.{suffix}"
    payload["url"] = url
    payload.setdefault("meta", {})["citation_abstract_html_url"] = [url]
    payload["meta"]["citation_doi"] = [doi]
    payload["meta"]["citation_title"] = ["Machine learning for biomedical literature search"]
    payload["meta"]["citation_abstract"] = [
        "We study neural ranking models for biomedical literature retrieval."
    ]
    return payload


def _remember_result(context, result):
    paper_id = result.get("paper_id")
    if paper_id is not None:
        context["paper_ids"].append(paper_id)


def test_index_dict_does_not_enrich_when_disabled(clean_db, test_papers):
    indexer = _indexer(enable_keyword_enrichment=False)

    result = indexer.index_dict(
        raw_payload=_unique_langtaosha_payload(test_papers),
        source_name="langtaosha",
    )
    _remember_result(clean_db, result)

    assert result["success"] is True
    assert result["keyword_enrichment"]["enabled"] is False
    assert result["keyword_enrichment"]["skipped"] is True


def test_index_dict_keeps_success_when_keyword_enrichment_fails(clean_db, test_papers, monkeypatch):
    indexer = _indexer(enable_keyword_enrichment=True)
    monkeypatch.setattr(
        indexer.keyword_enrichment,
        "extract_keywords",
        lambda title, abstract: KeywordExtractionResult(
            success=False,
            source="scispacy-test-source",
            model_name="missing_model",
            error="scispaCy model missing",
        ),
    )

    result = indexer.index_dict(
        raw_payload=_unique_langtaosha_payload(test_papers),
        source_name="langtaosha",
    )
    _remember_result(clean_db, result)

    assert result["success"] is True
    assert result["keyword_enrichment"]["enabled"] is True
    assert result["keyword_enrichment"]["success"] is False
    assert "scispaCy model missing" in result["keyword_enrichment"]["error"]


def test_index_dict_keyword_enrichment_independent_from_vectorization_failure(clean_db, test_papers, monkeypatch):
    indexer = _indexer(enable_keyword_enrichment=True)
    monkeypatch.setattr(
        indexer.keyword_enrichment,
        "extract_keywords",
        lambda title, abstract: KeywordExtractionResult(
            success=True,
            source="scispacy-test-source",
            model_name="fake_scispacy",
            keywords=[
                {
                    "keyword_type": "concept",
                    "keyword": "biomedical literature retrieval",
                    "weight": 1.0,
                    "source": "scispacy-test-source",
                }
            ],
        ),
    )

    result = indexer.index_dict(
        raw_payload=_unique_langtaosha_payload(test_papers),
        source_name="langtaosha",
    )
    _remember_result(clean_db, result)

    assert result["success"] is True
    assert result["vectorization"]["skipped"] is True
    assert result["keyword_enrichment"]["success"] is True
    assert result["keyword_enrichment"]["keyword_count"] == 1


def test_index_file_returns_keyword_enrichment_result(clean_db, test_paper_files, tmp_path):
    indexer = _indexer(enable_keyword_enrichment=False)
    with open(test_paper_files["langtaosha"][0], "r", encoding="utf-8") as f:
        payload = json.load(f)
    payload = _unique_langtaosha_payload({"langtaosha": [payload]})
    input_path = tmp_path / "query_understanding_index_file.json"
    input_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    result = indexer.index_file(
        input_path=input_path,
        source_name="langtaosha",
    )
    _remember_result(clean_db, result)

    assert result["success"] is True
    assert "keyword_enrichment" in result


@pytest.mark.integration
def test_index_dict_enriches_keywords_for_insert_new_paper_with_scispacy(clean_db, test_papers):
    pytest.importorskip("en_core_sci_lg")
    pytest.importorskip("en_ner_bionlp13cg_md")
    indexer = _indexer(enable_keyword_enrichment=True)

    result = indexer.index_dict(
        raw_payload=_unique_langtaosha_payload(test_papers),
        source_name="langtaosha",
    )
    _remember_result(clean_db, result)

    assert result["success"] is True
    assert result["keyword_enrichment"]["success"] is True
    assert result["keyword_enrichment"]["keyword_count"] > 0
    assert set(result["keyword_enrichment"]["sources"]) == set(DEFAULT_KEYWORD_SOURCES.values())
