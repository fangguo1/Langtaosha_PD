"""PaperIndexer insert-only 与 canonical 触发规则单元测试（无外部依赖）"""

from unittest.mock import Mock
import sys
import types

if "sqlalchemy" not in sys.modules:
    sqlalchemy_module = types.ModuleType("sqlalchemy")
    sqlalchemy_module.text = lambda sql: sql
    sys.modules["sqlalchemy"] = sqlalchemy_module
if "sqlalchemy.engine" not in sys.modules:
    sqlalchemy_engine_module = types.ModuleType("sqlalchemy.engine")
    sqlalchemy_engine_module.Connection = object
    sys.modules["sqlalchemy.engine"] = sqlalchemy_engine_module
if "sqlalchemy.exc" not in sys.modules:
    sqlalchemy_exc_module = types.ModuleType("sqlalchemy.exc")
    sqlalchemy_exc_module.IntegrityError = Exception
    sys.modules["sqlalchemy.exc"] = sqlalchemy_exc_module


if "config" not in sys.modules:
    config_module = types.ModuleType("config")
    config_module.init_config = lambda *_args, **_kwargs: None
    config_module.get_default_sources = lambda: ["biorxiv_history"]
    sys.modules["config"] = config_module

if "config.config_loader" not in sys.modules:
    config_loader_module = types.ModuleType("config.config_loader")
    config_loader_module.init_config = lambda *_args, **_kwargs: None
    config_loader_module.get_db_engine = lambda *_args, **_kwargs: None
    config_loader_module.get_vector_db_config = lambda *_args, **_kwargs: {}
    sys.modules["config.config_loader"] = config_loader_module

from src.docset_hub.indexing.paper_indexer import PaperIndexer
from src.docset_hub.metadata.transformer import TransformResult


def _build_indexer(enable_vectorization: bool = True) -> PaperIndexer:
    indexer = PaperIndexer.__new__(PaperIndexer)  # 跳过 __init__
    indexer.enable_vectorization = enable_vectorization
    indexer.default_sources = ["biorxiv_history"]
    indexer.transformer = Mock()
    indexer.metadata_db = Mock()
    indexer.vector_db = Mock() if enable_vectorization else None
    return indexer


def _mock_transform_success(indexer: PaperIndexer, work_id: str = "W-test") -> None:
    indexer.transformer.transform_dict.return_value = TransformResult(
        success=True,
        input_path="",
        source_name="biorxiv_history",
        db_payload={
            "papers": {"canonical_title": "t", "canonical_abstract": "a"},
            "paper_sources": {"title": "t", "abstract": "a"}
        },
        upsert_key={"source_name": "biorxiv_history"},
        work_id=work_id,
    )


def test_index_dict_force_insert_mode_and_skip_vector_when_canonical_unchanged():
    indexer = _build_indexer(enable_vectorization=True)
    _mock_transform_success(indexer)
    indexer.metadata_db.insert_paper.return_value = {
        "paper_id": 1,
        "status_code": "INSERT_SKIP_SAME_SOURCE",
        "apply": {"action": "skip"},
        "canonical": {"changed": False, "canonical_source_id": 101},
    }
    indexer.metadata_db.get_source_name_by_paper_source_id.return_value = "biorxiv_history"

    result = indexer.index_dict(raw_payload={"x": 1}, source_name="biorxiv_history", mode="upsert")

    assert result["success"] is True
    assert result["mode"] == "insert"
    assert result["vectorization"]["skipped"] is True
    indexer.vector_db.add_document.assert_not_called()
    indexer.metadata_db.upsert_embedding_status_pending.assert_not_called()


def test_index_dict_vectorize_when_status_code_requires():
    indexer = _build_indexer(enable_vectorization=True)
    _mock_transform_success(indexer, work_id="W2")
    indexer.metadata_db.insert_paper.return_value = {
        "paper_id": 2,
        "status_code": "INSERT_APPEND_SOURCE",
        "apply": {"action": "insert"},
        "canonical": {"changed": True, "canonical_source_id": 202},
    }
    indexer.metadata_db.get_source_name_by_paper_source_id.return_value = "biorxiv_history"
    indexer.vector_db.add_document.return_value = {
        "success": True,
        "action": "inserted",
        "doc_id": "W2",
        "affected_count": 1
    }

    result = indexer.index_dict(raw_payload={"x": 1}, source_name="biorxiv_history", mode="insert")

    assert result["success"] is True
    assert result["vectorization"]["success"] is True
    indexer.metadata_db.upsert_embedding_status_pending.assert_called_once()
    indexer.metadata_db.mark_embedding_succeeded.assert_called_once_with(2)
    indexer.metadata_db.mark_embedding_failed.assert_not_called()


def test_append_without_canonical_change_should_skip():
    indexer = _build_indexer(enable_vectorization=True)
    _mock_transform_success(indexer, work_id="W4")
    indexer.metadata_db.insert_paper.return_value = {
        "paper_id": 4,
        "status_code": "INSERT_APPEND_SOURCE",
        "apply": {"action": "insert"},
        "canonical": {"changed": False, "canonical_source_id": 404},
    }
    indexer.metadata_db.get_source_name_by_paper_source_id.return_value = "other_source"

    result = indexer.index_dict(raw_payload={"x": 1}, source_name="biorxiv_history", mode="insert")

    assert result["success"] is True
    assert result["vectorization"]["skipped"] is True
    indexer.vector_db.add_document.assert_not_called()


def test_index_dict_mark_failed_when_vectorization_error():
    indexer = _build_indexer(enable_vectorization=True)
    _mock_transform_success(indexer, work_id="W3")
    indexer.metadata_db.insert_paper.return_value = {
        "paper_id": 3,
        "status_code": "INSERT_APPEND_SOURCE",
        "apply": {"action": "insert"},
        "canonical": {"changed": True, "canonical_source_id": 303},
    }
    indexer.metadata_db.get_source_name_by_paper_source_id.return_value = "biorxiv_history"
    indexer.vector_db.add_document.side_effect = RuntimeError("vectordb error")

    result = indexer.index_dict(raw_payload={"x": 1}, source_name="biorxiv_history", mode="insert")

    assert result["success"] is True
    assert result["vectorization"]["success"] is False
    indexer.metadata_db.mark_embedding_succeeded.assert_not_called()
    indexer.metadata_db.mark_embedding_failed.assert_called_once()
