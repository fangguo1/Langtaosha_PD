from __future__ import annotations

from src.docset_hub.evaluation.config_identity import (
    build_config_fingerprint,
    create_metadata_engine_from_config,
)


def _config(name: str, vector_database: str, prefix: str, sparse_prefix: str) -> dict:
    return {
        "default_sources": ["langtaosha", "biorxiv_history", "biorxiv_daily"],
        "vector_db": {
            "url": "http://example.internal:80",
            "account": "root",
            "api_key": "secret-api-key",
            "database": vector_database,
            "collection_prefix": prefix,
            "sparse_collection_prefix": sparse_prefix,
            "embedding_model": "BAAI/bge-m3",
        },
        "metadata_db": {
            "host": "172.21.0.9",
            "port": 5432,
            "user": "root",
            "password": "top-secret",
            "name": name,
        },
    }


def test_build_config_fingerprint_contains_only_non_secret_corpus_identity():
    fingerprint = build_config_fingerprint(
        _config("langtaosha_use", "langtaosha_use", "lt_", "lt_bm25_")
    )

    assert fingerprint == {
        "metadata_db_name": "langtaosha_use",
        "vector_db_database": "langtaosha_use",
        "collection_prefix": "lt_",
        "sparse_collection_prefix": "lt_bm25_",
        "embedding_model": "BAAI/bge-m3",
        "default_sources": ["langtaosha", "biorxiv_history", "biorxiv_daily"],
    }
    assert "secret" not in str(fingerprint)
    assert "172.21.0.9" not in str(fingerprint)
    assert "root" not in str(fingerprint)


def test_create_metadata_engine_from_config_builds_distinct_database_targets():
    mimic_engine = create_metadata_engine_from_config(
        _config("langtaosha_mimic", "langtaosha_mimic", "lt_mimic_", "lt_mimic_bm25_")
    )
    use_engine = create_metadata_engine_from_config(
        _config("langtaosha_use", "langtaosha_use", "lt_", "lt_bm25_")
    )

    assert str(mimic_engine.url.database) == "langtaosha_mimic"
    assert str(use_engine.url.database) == "langtaosha_use"
    assert str(mimic_engine.url) != str(use_engine.url)
