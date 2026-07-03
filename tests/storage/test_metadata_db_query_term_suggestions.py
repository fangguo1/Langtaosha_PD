"""Tests for typo-tolerant query term suggestions."""

from __future__ import annotations

import argparse
import copy
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config import _reset_config, get_db_engine, init_config
from src.docset_hub.indexing.query_understanding import QueryUnderstandingService
from src.docset_hub.metadata.transformer import MetadataTransformer
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
        config_path = Path(__file__).resolve().parents[2] / "src" / "config" / "config_tecent_backend_server_use.yaml"

    if not config_path.exists():
        raise ValueError(f"Config file not found: {config_path}")

    _global_config_path = config_path
    return config_path


@pytest.fixture(scope="session")
def db_engine():
    _reset_config()
    init_config(get_config_path_from_args(), force_reload=True)
    return get_db_engine("metadata_db")


@pytest.fixture(scope="function")
def metadata_db():
    return MetadataDB(config_path=get_config_path_from_args())


@pytest.fixture(scope="session")
def transformer():
    return MetadataTransformer()


def _unique_payload(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(raw_payload)
    suffix = uuid.uuid4().hex
    numeric_id = str(930000000000 + int(suffix[:10], 16) % 99999999999)
    url = f"https://langtaosha.org.cn/lts/en/preprint/view/{numeric_id}"
    payload["url"] = url
    payload.setdefault("meta", {})["citation_abstract_html_url"] = [url]
    payload["meta"]["citation_doi"] = [f"10.65215/query-term-suggest.{suffix}"]
    return payload


def _insert_real_payload(metadata_db, transformer, raw_payload, source_name="langtaosha") -> int:
    transform_result = transformer.transform_dict(raw_payload, source_name=source_name)
    assert transform_result.success, transform_result.error
    write_result = metadata_db.insert_paper(
        db_payload=transform_result.db_payload,
        upsert_key=transform_result.upsert_key,
    )
    assert write_result["paper_id"] is not None
    return write_result["paper_id"]


@pytest.fixture(scope="function")
def test_papers_pool(metadata_db, transformer, test_papers):
    paper_ids = []
    try:
        for _ in range(12):
            paper_ids.append(
                _insert_real_payload(
                    metadata_db,
                    transformer,
                    _unique_payload(test_papers["langtaosha"][0]),
                    source_name="langtaosha",
                )
            )
        yield paper_ids
    finally:
        for paper_id in paper_ids:
            metadata_db.delete_paper_by_paper_id(paper_id)


def _insert_keyword(db_engine, paper_id: int, keyword: str, weight: float = 1.0) -> None:
    with db_engine.connect() as conn:
        conn.execute(
            text(
                """
                INSERT INTO paper_keywords (paper_id, keyword_type, keyword, weight, source)
                VALUES (:paper_id, 'concept', :keyword, :weight, 'scispacy-en_core_sci_lg-generated-test')
                ON CONFLICT (paper_id, keyword_type, keyword, source)
                DO UPDATE SET weight = EXCLUDED.weight
                """
            ),
            {"paper_id": paper_id, "keyword": keyword, "weight": weight},
        )
        conn.commit()


def _normalize_keyword(value: str) -> str:
    return " ".join((value or "").lower().replace("-", " ").split())


def test_suggest_query_terms_prefers_phrase_trigram_match_for_multi_typo_query(
    metadata_db,
    db_engine,
    test_papers_pool,
):
    seeded_keywords = [
        "Machine Learning",
        "Active Learning",
        "Active Learning",
        "Deep Learning",
        "Deep Learning",
        "Deep Learning",
        "Clearance",
        "Clearance",
        "Clearance",
        "Clearance",
        "Cochlear",
        "Cochlear",
    ]

    for paper_id, keyword in zip(test_papers_pool, seeded_keywords):
        _insert_keyword(db_engine, paper_id, keyword)

    results = metadata_db.suggest_query_terms("machie learninng", limit=5)

    assert results
    assert results[0]["keyword"].lower() == "machine learning"
    assert {row["keyword"].lower() for row in results[:5]}.isdisjoint(
        {"clearance", "cochlear"}
    )


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("machnie learning", "machine learning"),
        ("protien folding", "protein folding"),
        ("genmoe editing", "genome editing"),
        ("epigentic regulation", "epigenetic regulation"),
        ("neurla network", "neural network"),
        ("transcirptome analysis", "transcriptome analysis"),
        ("metabloic pathway", "metabolic pathway"),
        ("immunotherpay", "immunotherapy"),
        ("single cel sequencing", "single cell sequencing"),
        ("differntial expression", "differential expression"),
        ("microboime composition", "microbiome composition"),
        ("protei folding", "protein folding"),
        ("genom sequencing", "genome sequencing"),
        ("transcriptom profiling", "transcriptome profiling"),
        ("celluar senescence", "cellular senescence"),
        ("mitochodrial dysfunction", "mitochondrial dysfunction"),
        ("epigenetc modification", "epigenetic modification"),
        ("proteinn folding", "protein folding"),
        ("genomme editing", "genome editing"),
        ("transcriptomme analysis", "transcriptome analysis"),
        pytest.param(
            "microbiomme diversity",
            "microbiome diversity",
            marks=pytest.mark.xfail(reason="current keyword lexicon does not contain 'microbiome diversity'"),
        ),
        ("immunnotherapy", "immunotherapy"),
        ("protwin folding", "protein folding"),
        ("genime editing", "genome editing"),
        ("transxriptome analysis", "transcriptome analysis"),
        ("epigenrtic regulation", "epigenetic regulation"),
        pytest.param(
            "microbiime community",
            "microbiome community",
            marks=pytest.mark.xfail(reason="current keyword lexicon does not contain 'microbiome community'"),
        ),
    ],
)
def test_query_understanding_recovers_common_multi_token_typos(metadata_db, query, expected):
    result = QueryUnderstandingService(metadata_db).analyze(query)

    assert result.corrected_query is not None, query
    assert _normalize_keyword(result.corrected_query) == _normalize_keyword(expected)
