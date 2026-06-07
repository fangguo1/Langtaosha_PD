from __future__ import annotations

import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_display_module():
    module_path = PROJECT_ROOT / "scripts" / "display_db_storage_info_advanced.py"
    spec = importlib.util.spec_from_file_location("display_db_storage_info_advanced", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_summarize_sparse_collection_counts_sparse_vectors_from_documents():
    module = load_display_module()

    summary = module.summarize_tencent_collection_indexes(
        {
            "collection": "lt_test_bm25_biorxiv_history",
            "documentCount": 100,
            "indexes": [
                {"fieldName": "id", "fieldType": "string", "indexType": "primaryKey"},
                {
                    "fieldName": "sparse_vector",
                    "fieldType": "sparseVector",
                    "indexType": "inverted",
                    "metricType": "IP",
                },
            ],
        }
    )

    assert summary == {
        "dense_count": 0,
        "sparse_count": 100,
        "has_dense": False,
        "has_sparse": True,
        "collection_type": "sparse",
    }


def test_summarize_dense_collection_uses_dense_indexed_count():
    module = load_display_module()

    summary = module.summarize_tencent_collection_indexes(
        {
            "collection": "lt_test_biorxiv_history",
            "documentCount": 100,
            "indexes": [
                {"fieldName": "id", "fieldType": "string", "indexType": "primaryKey"},
                {
                    "fieldName": "vector",
                    "fieldType": "vector",
                    "indexType": "HNSW",
                    "indexedCount": 100,
                },
            ],
        }
    )

    assert summary["dense_count"] == 100
    assert summary["sparse_count"] == 0
    assert summary["collection_type"] == "dense"


def test_summarize_hybrid_collection_counts_both_indexes():
    module = load_display_module()

    summary = module.summarize_tencent_collection_indexes(
        {
            "collection": "lt_test_hybrid_biorxiv_history",
            "documentCount": 100,
            "indexes": [
                {"fieldName": "vector", "fieldType": "vector", "indexedCount": 100},
                {"fieldName": "sparse_vector", "fieldType": "sparseVector", "indexedCount": 98},
            ],
        }
    )

    assert summary["dense_count"] == 100
    assert summary["sparse_count"] == 98
    assert summary["collection_type"] == "hybrid"


def test_show_tencent_vector_db_info_survives_database_list_failure(monkeypatch, capsys):
    module = load_display_module()

    class FakeClient:
        def __init__(self, url, account, api_key):
            pass

        def list_databases(self):
            raise RuntimeError("temporary 502")

        def list_collections_with_info(self, database):
            assert database == "langtaosha_test"
            return [
                {
                    "collection": "lt_test_bm25_biorxiv_history",
                    "documentCount": 100,
                    "indexStatus": {"status": "ready"},
                    "indexes": [
                        {"fieldName": "sparse_vector", "fieldType": "sparseVector"},
                    ],
                }
            ]

    monkeypatch.setattr(module, "VectorDBClient", FakeClient)
    monkeypatch.setattr(module, "TENCENT_VDB_AVAILABLE", True)

    module.show_tencent_vector_db_info(
        {
            "vector_db": {
                "url": "http://vector-db.test",
                "account": "root",
                "api_key": "secret",
                "embedding_source": "tecent_made",
                "embedding_model": "BAAI/bge-m3",
                "database": "langtaosha_test",
                "collection_prefix": "lt_test_",
            }
        }
    )

    output = capsys.readouterr().out
    assert "获取数据库列表失败" in output
    assert "lt_test_bm25_biorxiv_history" in output
    assert "sparse" in output
