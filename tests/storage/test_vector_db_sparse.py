from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml
from sqlalchemy import text

from config.config_loader import (
    _reset_config as _reset_runtime_config,
    get_db_engine as _runtime_get_db_engine,
    init_config as _runtime_init_config,
)
from src.config.config_loader import _reset_config as _reset_src_config
from src.docset_hub.storage.vector_db import VectorDB
from src.docset_hub.storage.vector_db_client import VectorDBClient


REPO_ROOT = Path(__file__).resolve().parents[2]
LIVE_CONFIG_PATH = Path(
    os.environ.get(
        "VECTOR_DB_SPARSE_TEST_CONFIG",
        REPO_ROOT / "src/config/config_tecent_backend_server_test.yaml",
    )
)

REAL_BM25_COMPLEX_QUERIES = [
    {
        "name": "focused ultrasound Alzheimer model",
        "query": "low-intensity focused ultrasound TgF344-AD microglial amyloid beta",
        "term_groups": [
            ("low-intensity", "focused ultrasound", "lifus"),
            ("tgf344-ad",),
            ("microglial", "microglia"),
            ("amyloid beta", "amyloid-beta"),
            ("alzheimer",),
        ],
        "min_hits": 3,
    },
    {
        "name": "longitudinal single-cell cancer tree inference",
        "query": "longitudinal scDNA-seq subclonal tree AML cancer cells",
        "term_groups": [
            ("scdna-seq", "scdnaseq"),
            ("longitudinal",),
            ("subclonal",),
            ("aml",),
            ("single-cell", "single cell"),
        ],
        "min_hits": 3,
    },
    {
        "name": "leukemic stem cell PI3K resistance",
        "query": "PI3 kinase inhibition leukemic stem cells EZH1 EZH2 AML",
        "term_groups": [
            ("pi3 kinase", "pi3k"),
            ("leukemic stem", "lscs"),
            ("ezh1",),
            ("ezh2",),
            ("aml",),
        ],
        "min_hits": 3,
    },
    {
        "name": "locus coeruleus hippocampal sleep states",
        "query": "locus coeruleus hippocampus norepinephrine NREM offline states",
        "term_groups": [
            ("locus coeruleus",),
            ("hippocampus", "hippocampal"),
            ("norepinephrine",),
            ("nrem", "non-rapid-eye"),
            ("offline",),
        ],
        "min_hits": 3,
    },
    {
        "name": "small protein cryo-EM template matching",
        "query": "sub-50 kDa cryo-EM 2D template matching small protein complexes",
        "term_groups": [
            ("cryo-em",),
            ("sub-50", "sub 50"),
            ("2d template", "2-d template"),
            ("small protein", "small proteins"),
            ("complexes",),
        ],
        "min_hits": 3,
    },
    {
        "name": "lantibiotic immunity transporter",
        "query": "LanFEG NisFEG lantibiotic immunity nisin transporter",
        "term_groups": [
            ("lanfeg",),
            ("nisfeg",),
            ("lantibiotic",),
            ("nisin",),
            ("transporter",),
        ],
        "min_hits": 3,
    },
]


class FakeSparseEncoder:
    def __init__(self):
        self.document_batches = []
        self.query_inputs = []

    def encode_documents(self, texts):
        self.document_batches.append(list(texts))
        return [[[11, 0.5]], [[22, 0.7]]][: len(texts)]

    def encode_query(self, query):
        self.query_inputs.append(query)
        return [[99, 1.0]]


def _write_config(tmp_path: Path, vector_overrides=None) -> Path:
    vector_config = {
        "url": "http://vector-db.test",
        "account": "root",
        "api_key": "secret",
        "embedding_source": "tecent_made",
        "embedding_model": "BAAI/bge-m3",
        "database": "langtaosha_test",
        "collection_prefix": "lt_test_",
        "allowed_sources": ["biorxiv_history", "langtaosha"],
    }
    if vector_overrides:
        vector_config.update(vector_overrides)
    config = {
        "default_sources": ["biorxiv_history", "langtaosha"],
        "vector_db": vector_config,
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def _make_vector_db(tmp_path: Path, vector_overrides=None):
    _reset_runtime_config()
    _reset_src_config()
    config_path = _write_config(tmp_path, vector_overrides)
    with patch("src.docset_hub.storage.vector_db.VectorDBClient") as client_cls:
        client = Mock()
        client_cls.return_value = client
        vector_db = VectorDB(config_path=config_path)
    return vector_db, client


def test_default_sparse_prefix_is_derived_from_collection_prefix(tmp_path):
    vector_db, _client = _make_vector_db(tmp_path)

    assert vector_db.sparse_collection_prefix == "lt_test_bm25_"
    assert vector_db._get_sparse_collection_name("biorxiv_history") == "lt_test_bm25_biorxiv_history"


def test_configured_sparse_prefix_overrides_default(tmp_path):
    vector_db, _client = _make_vector_db(
        tmp_path,
        {"sparse_collection_prefix": "custom_bm25_"},
    )

    assert vector_db._get_sparse_collection_name("langtaosha") == "custom_bm25_langtaosha"


def test_client_create_sparse_collection_payload():
    client = VectorDBClient(url="http://vector-db.test", account="root", api_key="secret")
    client._request = Mock(return_value={"code": 0})

    client.create_sparse_collection(
        database="langtaosha_test",
        collection="lt_test_bm25_biorxiv_history",
    )

    method, endpoint, payload = client._request.call_args.args
    assert method == "POST"
    assert endpoint == "/collection/create"
    assert payload["database"] == "langtaosha_test"
    assert payload["collection"] == "lt_test_bm25_biorxiv_history"
    assert {
        "fieldName": "sparse_vector",
        "fieldType": "sparseVector",
        "indexType": "inverted",
        "metricType": "IP",
    } in payload["indexes"]


def test_client_fulltext_search_payload_extracts_documents():
    client = VectorDBClient(url="http://vector-db.test", account="root", api_key="secret")
    client._request = Mock(
        return_value={
            "code": 0,
            "documents": [[{"work_id": "W1", "score": 3.2}]],
        }
    )

    result = client.fulltext_search_documents(
        database="langtaosha_test",
        collection="lt_test_bm25_biorxiv_history",
        sparse_vector=[[99, 1.0]],
        limit=5,
        output_fields=["work_id"],
        terminate_after=4000,
        cutoff_frequency=0.1,
    )

    method, endpoint, payload = client._request.call_args.args
    assert method == "POST"
    assert endpoint == "/document/fullTextSearch"
    assert payload["search"]["match"] == {
        "fieldName": "sparse_vector",
        "data": [[[99, 1.0]]],
        "terminateAfter": 4000,
        "cutoffFrequency": 0.1,
    }
    assert payload["search"]["outputFields"] == ["work_id"]
    assert result["_extracted_documents"] == [{"work_id": "W1", "score": 3.2}]


def test_ensure_sparse_collection_creates_missing_collection(tmp_path):
    vector_db, client = _make_vector_db(tmp_path)
    client.list_collections.return_value = ["lt_test_biorxiv_history"]
    client.create_sparse_collection.return_value = {"code": 0}

    assert vector_db.ensure_sparse_collection("biorxiv_history") is True

    client.create_sparse_collection.assert_called_once_with(
        database="langtaosha_test",
        collection="lt_test_bm25_biorxiv_history",
        disk_swap_enabled=None,
    )


def test_add_sparse_documents_encodes_and_upserts_batch(tmp_path):
    vector_db, client = _make_vector_db(tmp_path)
    vector_db._sparse_encoder = FakeSparseEncoder()
    client.list_collections.return_value = ["lt_test_bm25_biorxiv_history"]
    client.upsert_documents.return_value = {"affectedCount": 2}

    result = vector_db.add_sparse_documents(
        source_name="biorxiv_history",
        documents=[
            {"work_id": "W1", "paper_id": 1, "text": "CRISPR-Cas9", "text_type": "abstract"},
            {"work_id": "W2", "paper_id": 2, "text": "p53 mutation", "text_type": "abstract"},
        ],
    )

    assert result["success"] is True
    assert result["document_count"] == 2
    client.upsert_documents.assert_called_once()
    kwargs = client.upsert_documents.call_args.kwargs
    assert kwargs["collection"] == "lt_test_bm25_biorxiv_history"
    assert kwargs["documents"] == [
        {
            "id": "W1",
            "sparse_vector": [[11, 0.5]],
            "work_id": "W1",
            "source_name": "biorxiv_history",
            "text_type": "abstract",
            "paper_id": "1",
        },
        {
            "id": "W2",
            "sparse_vector": [[22, 0.7]],
            "work_id": "W2",
            "source_name": "biorxiv_history",
            "text_type": "abstract",
            "paper_id": "2",
        },
    ]


def test_sparse_search_uses_fulltext_search_documents(tmp_path):
    vector_db, client = _make_vector_db(tmp_path)
    vector_db._sparse_encoder = FakeSparseEncoder()
    client.list_collections.return_value = ["lt_test_bm25_biorxiv_history"]
    client.fulltext_search_documents.return_value = {
        "_extracted_documents": [
            {
                "work_id": "W1",
                "paper_id": "1",
                "source_name": "biorxiv_history",
                "text_type": "abstract",
                "score": 8.5,
            }
        ]
    }

    results = vector_db.sparse_search("CRISPR-Cas9", ["biorxiv_history"], top_k=5)

    assert len(results) == 1
    assert results[0].work_id == "W1"
    assert results[0].score == 8.5
    client.fulltext_search_documents.assert_called_once_with(
        database="langtaosha_test",
        collection="lt_test_bm25_biorxiv_history",
        sparse_vector=[[99, 1.0]],
        limit=5,
        output_fields=["work_id", "paper_id", "source_name", "text_type"],
        terminate_after=4000,
        cutoff_frequency=0.1,
    )


def _normalize_match_text(value: str | None) -> str:
    return (value or "").lower().replace("\u2010", "-").replace("\u2011", "-")


def _matched_term_groups(text_blob: str, term_groups) -> list[str]:
    normalized_text = _normalize_match_text(text_blob)
    matched = []
    for variants in term_groups:
        if any(_normalize_match_text(variant) in normalized_text for variant in variants):
            matched.append(variants[0])
    return matched


def _fetch_biorxiv_history_texts(engine) -> dict[str, str]:
    sql = text(
        """
        SELECT
            p.work_id,
            CONCAT_WS(
                ' ',
                p.canonical_title,
                p.canonical_abstract,
                COALESCE(STRING_AGG(DISTINCT pk.keyword, ' '), '')
            ) AS text_blob
        FROM papers p
        JOIN paper_sources ps ON ps.paper_id = p.paper_id
        LEFT JOIN paper_keywords pk ON pk.paper_id = p.paper_id
        WHERE ps.source_name = :source_name
        GROUP BY p.work_id, p.canonical_title, p.canonical_abstract
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"source_name": "biorxiv_history"}).fetchall()
    return {row[0]: row[1] for row in rows}


def _fetch_texts_for_work_ids(engine, work_ids: list[str]) -> dict[str, str]:
    if not work_ids:
        return {}

    params = {f"work_id_{index}": work_id for index, work_id in enumerate(work_ids)}
    placeholders = ", ".join(f":{name}" for name in params)
    sql = text(
        f"""
        SELECT
            p.work_id,
            CONCAT_WS(
                ' ',
                p.canonical_title,
                p.canonical_abstract,
                COALESCE(STRING_AGG(DISTINCT pk.keyword, ' '), '')
            ) AS text_blob
        FROM papers p
        LEFT JOIN paper_keywords pk ON pk.paper_id = p.paper_id
        WHERE p.work_id IN ({placeholders})
        GROUP BY p.work_id, p.canonical_title, p.canonical_abstract
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {row[0]: row[1] for row in rows}


@pytest.fixture(scope="session")
def live_metadata_engine():
    if not LIVE_CONFIG_PATH.exists():
        pytest.skip(f"Live test config does not exist: {LIVE_CONFIG_PATH}")

    _reset_runtime_config()
    _reset_src_config()
    _runtime_init_config(LIVE_CONFIG_PATH, force_reload=True)

    try:
        engine = _runtime_get_db_engine("metadata_db")
        with engine.connect() as conn:
            paper_count = conn.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT p.paper_id)
                    FROM papers p
                    JOIN paper_sources ps ON ps.paper_id = p.paper_id
                    WHERE ps.source_name = :source_name
                    """
                ),
                {"source_name": "biorxiv_history"},
            ).scalar_one()
    except Exception as exc:
        pytest.skip(f"Live MetadataDB is unavailable: {exc}")

    if paper_count < 100:
        pytest.skip(
            "Live biorxiv_history metadata has fewer than 100 papers; "
            "run scripts/backfill_source_records.py first."
        )

    return engine


@pytest.fixture(scope="session")
def live_sparse_vector_db():
    if not LIVE_CONFIG_PATH.exists():
        pytest.skip(f"Live test config does not exist: {LIVE_CONFIG_PATH}")

    _reset_runtime_config()
    _reset_src_config()
    try:
        vector_db = VectorDB(config_path=LIVE_CONFIG_PATH)
        sparse_collection = vector_db._get_sparse_collection_name("biorxiv_history")
        collections = vector_db.client.list_collections_with_info(vector_db.database)
    except Exception as exc:
        pytest.skip(f"Live VectorDB sparse collection is unavailable: {exc}")

    collection_info = next(
        (item for item in collections if item.get("collection") == sparse_collection),
        None,
    )
    if collection_info is None:
        pytest.skip(f"Sparse collection does not exist: {sparse_collection}")

    document_count = int(collection_info.get("documentCount") or 0)
    if document_count < 100:
        pytest.skip(
            f"Sparse collection {sparse_collection} has {document_count} docs; "
            "run scripts/backfill_sparse_collections.py first."
        )

    return vector_db


def test_live_complex_sparse_queries_are_grounded_in_ingested_metadata(live_metadata_engine):
    texts_by_work_id = _fetch_biorxiv_history_texts(live_metadata_engine)

    assert len(texts_by_work_id) >= 100
    for query_case in REAL_BM25_COMPLEX_QUERIES:
        best_match_count = max(
            len(_matched_term_groups(text_blob, query_case["term_groups"]))
            for text_blob in texts_by_work_id.values()
        )
        assert best_match_count >= query_case["min_hits"], query_case["name"]


@pytest.mark.parametrize("query_case", REAL_BM25_COMPLEX_QUERIES, ids=lambda item: item["name"])
def test_live_sparse_search_returns_lexically_relevant_real_papers(
    live_sparse_vector_db,
    live_metadata_engine,
    query_case,
):
    results = live_sparse_vector_db.sparse_search(
        query_case["query"],
        source_list=["biorxiv_history"],
        top_k=10,
    )

    assert results, f"BM25 returned no results for query: {query_case['query']}"
    assert all(result.source_name == "biorxiv_history" for result in results)
    assert all(result.score > 0 for result in results)

    texts_by_work_id = _fetch_texts_for_work_ids(
        live_metadata_engine,
        [result.work_id for result in results],
    )
    scored_hits = []
    for result in results:
        text_blob = texts_by_work_id.get(result.work_id, "")
        matched_groups = _matched_term_groups(text_blob, query_case["term_groups"])
        scored_hits.append(
            {
                "work_id": result.work_id,
                "score": result.score,
                "matched": matched_groups,
                "matched_count": len(matched_groups),
            }
        )

    best_hit = max(scored_hits, key=lambda item: item["matched_count"])
    assert best_hit["matched_count"] >= query_case["min_hits"], {
        "query": query_case["query"],
        "expected_min_hits": query_case["min_hits"],
        "top_hits": scored_hits,
    }
