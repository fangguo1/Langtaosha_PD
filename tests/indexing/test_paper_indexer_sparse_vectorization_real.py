"""Real PaperIndexer sparse vectorization integration tests."""

from __future__ import annotations

import copy
import math
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.config import _reset_config, init_config
from src.docset_hub.indexing import PaperIndexer


CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "config"
    / "config_tecent_backend_server_test.yaml"
)
BATCH_SIZE = 12
MIN_RECALL_RATIO = 0.8


def _build_indexer() -> PaperIndexer:
    _reset_config()
    init_config(CONFIG_PATH, force_reload=True)
    return PaperIndexer(
        config_path=CONFIG_PATH,
        enable_vectorization=True,
        enable_keyword_enrichment=False,
    )


def _build_batch_payloads(test_papers, batch_size: int = BATCH_SIZE):
    source_payloads = test_papers["biorxiv_history"]
    run_id = uuid.uuid4().hex[:12]
    shared_token = f"ltsparsebatch_{run_id}"
    payloads = []

    for index in range(batch_size):
        payload = copy.deepcopy(source_payloads[index % len(source_payloads)])
        per_paper_token = f"{shared_token}_{index:02d}"
        payload["doi"] = f"10.1101/paper-indexer-sparse-batch.{run_id}.{index:02d}"
        payload["title"] = f"PaperIndexer Sparse Batch Real Test {index:02d} {per_paper_token}"
        payload["abstract"] = (
            f"Shared sparse batch token {shared_token}. "
            f"Per paper retrieval token {per_paper_token}. "
            "This article verifies BM25 sparse indexing through PaperIndexer."
        )
        payload["date"] = "2026-05-17"
        payload["version"] = "1"
        payloads.append(
            {
                "payload": payload,
                "token": per_paper_token,
                "shared_token": shared_token,
                "title": payload["title"],
            }
        )

    return run_id, shared_token, payloads


def _query_document(vector_db, collection_name: str, work_id: str, output_fields: List[str]):
    result = vector_db.client.query_documents(
        database=vector_db.database,
        collection=collection_name,
        ids=[work_id],
        output_fields=output_fields,
        limit=1,
        read_consistency="strongConsistency",
    )
    documents = result.get("documents", [])
    return documents[0] if documents else None


def _wait_for_document(vector_db, collection_name: str, work_id: str, output_fields: List[str]):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        document = _query_document(vector_db, collection_name, work_id, output_fields)
        if document is not None:
            return document
        time.sleep(2)
    return None


def _wait_for_sparse_batch_hits(
    vector_db,
    query: str,
    expected_work_ids: set[str],
    top_k: int,
    min_expected: int,
):
    deadline = time.monotonic() + 60
    last_results = []
    while time.monotonic() < deadline:
        last_results = vector_db.sparse_search(
            query=query,
            source_list=["biorxiv_history"],
            top_k=top_k,
        )
        retrieved_work_ids = {result.work_id for result in last_results}
        if len(expected_work_ids & retrieved_work_ids) >= min_expected:
            return last_results
        time.sleep(3)
    return last_results


def _wait_for_sparse_hit(vector_db, work_id: str, query: str, top_k: int = 5):
    deadline = time.monotonic() + 60
    last_results = []
    while time.monotonic() < deadline:
        last_results = vector_db.sparse_search(
            query=query,
            source_list=["biorxiv_history"],
            top_k=top_k,
        )
        if any(result.work_id == work_id for result in last_results):
            return last_results
        time.sleep(3)
    return last_results


def _delete_vector_document(vector_db, collection_name: str, work_id: str) -> None:
    try:
        collections = vector_db.client.list_collections(vector_db.database)
        if collection_name not in collections:
            return
        vector_db.client.delete_documents(
            database=vector_db.database,
            collection=collection_name,
            ids=[work_id],
        )
    except Exception:
        pass


def _cleanup_indexed(indexer: PaperIndexer, indexed: List[Dict[str, Any]]) -> None:
    if getattr(indexer, "vector_db", None):
        dense_collection = indexer.vector_db._get_collection_name("biorxiv_history")
        sparse_collection = indexer.vector_db._get_sparse_collection_name("biorxiv_history")
        for item in indexed:
            work_id = item.get("work_id")
            if not work_id:
                continue
            _delete_vector_document(indexer.vector_db, dense_collection, work_id)
            _delete_vector_document(indexer.vector_db, sparse_collection, work_id)

    for item in indexed:
        paper_id = item.get("paper_id")
        if paper_id is None:
            continue
        try:
            indexer.metadata_db.delete_paper_by_paper_id(paper_id)
        except Exception:
            pass


@pytest.mark.integration
def test_index_dict_writes_and_retrieves_multiple_real_sparse_documents(test_papers):
    if not CONFIG_PATH.exists():
        pytest.skip(f"test config does not exist: {CONFIG_PATH}")
    if len(test_papers["biorxiv_history"]) == 0:
        pytest.skip("biorxiv_history test payloads are unavailable")

    indexer = _build_indexer()
    _run_id, _shared_token, payloads = _build_batch_payloads(test_papers)
    indexed: List[Dict[str, Any]] = []

    try:
        for item in payloads:
            payload = item["payload"]
            result = indexer.index_dict(
                raw_payload=payload,
                source_name="biorxiv_history",
                mode="insert",
            )
            indexed_item = {
                "result": result,
                "paper_id": result.get("paper_id"),
                "work_id": result.get("work_id"),
                "token": item["token"],
                "title": item["title"],
                "payload": payload,
            }
            indexed.append(indexed_item)

            assert result["success"] is True, result
            assert result["metadata"]["status_code"] == "INSERT_NEW_PAPER", result
            assert result["vectorization"]["success"] is True, result
            assert result["sparse_vectorization"]["enabled"] is True, result
            assert result["sparse_vectorization"]["success"] is True, result
            assert indexed_item["paper_id"] is not None
            assert indexed_item["work_id"]

        work_ids = [item["work_id"] for item in indexed]
        paper_ids = [item["paper_id"] for item in indexed]
        assert len(indexed) == BATCH_SIZE
        assert len(set(work_ids)) == BATCH_SIZE
        assert len(set(paper_ids)) == BATCH_SIZE

        dense_collection = indexer.vector_db._get_collection_name("biorxiv_history")
        sparse_collection = indexer.vector_db._get_sparse_collection_name("biorxiv_history")

        for item in indexed:
            dense_document = _wait_for_document(
                indexer.vector_db,
                dense_collection,
                item["work_id"],
                ["id", "work_id", "paper_id", "source_name", "text_type", "text"],
            )
            assert dense_document is not None, item
            assert dense_document.get("work_id") == item["work_id"]
            assert dense_document.get("source_name") == "biorxiv_history"
            assert str(dense_document.get("paper_id")) == str(item["paper_id"])
            assert dense_document.get("text_type") == "abstract"
            assert item["title"] in (dense_document.get("text") or "")

            sparse_document = _wait_for_document(
                indexer.vector_db,
                sparse_collection,
                item["work_id"],
                ["id", "work_id", "paper_id", "source_name", "text_type"],
            )
            assert sparse_document is not None, item
            assert sparse_document.get("work_id") == item["work_id"]
            assert sparse_document.get("source_name") == "biorxiv_history"
            assert str(sparse_document.get("paper_id")) == str(item["paper_id"])
            assert sparse_document.get("text_type") == "abstract"

        for item in indexed:
            results = _wait_for_sparse_hit(
                indexer.vector_db,
                item["work_id"],
                item["token"],
                top_k=5,
            )
            matched = [result for result in results if result.work_id == item["work_id"]]
            assert matched, {
                "token": item["token"],
                "work_id": item["work_id"],
                "top_results": [result.work_id for result in results],
            }
            assert matched[0].score > 0

        expected_work_ids = set(work_ids)
        min_expected = math.ceil(BATCH_SIZE * MIN_RECALL_RATIO)
        batch_query = " ".join(item["token"] for item in indexed)
        batch_results = _wait_for_sparse_batch_hits(
            indexer.vector_db,
            batch_query,
            expected_work_ids,
            top_k=BATCH_SIZE * 2,
            min_expected=min_expected,
        )
        retrieved_work_ids = {result.work_id for result in batch_results}
        matched_count = len(expected_work_ids & retrieved_work_ids)
        assert matched_count >= min_expected, {
            "batch_query": batch_query,
            "matched_count": matched_count,
            "min_expected": min_expected,
            "missed_work_ids": sorted(expected_work_ids - retrieved_work_ids),
            "top_results": [result.work_id for result in batch_results],
        }

        original = indexed[0]
        older_payload = copy.deepcopy(original["payload"])
        older_payload["title"] = f"Older skipped sparse title {original['token']}"
        older_payload["abstract"] = "This older version should not rewrite sparse indexing."
        older_payload["date"] = "2026-05-01"
        older_payload["version"] = "0"

        skip_result = indexer.index_dict(
            raw_payload=older_payload,
            source_name="biorxiv_history",
            mode="insert",
        )
        assert skip_result["success"] is True, skip_result
        assert skip_result["metadata"]["status_code"] == "INSERT_SKIP_SAME_SOURCE", skip_result
        assert skip_result["sparse_vectorization"].get("skipped") is True, skip_result

        post_skip_results = _wait_for_sparse_hit(
            indexer.vector_db,
            original["work_id"],
            original["token"],
            top_k=5,
        )
        assert any(result.work_id == original["work_id"] for result in post_skip_results)

    finally:
        _cleanup_indexed(indexer, indexed)
