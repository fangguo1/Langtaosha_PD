"""MetadataDB embedding_status 单元测试（无真实数据库依赖）"""

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


if "config.config_loader" not in sys.modules:
    config_loader_module = types.ModuleType("config.config_loader")
    config_loader_module.init_config = lambda *_args, **_kwargs: None
    config_loader_module.get_db_engine = lambda *_args, **_kwargs: None
    config_loader_module.get_vector_db_config = lambda *_args, **_kwargs: {}
    sys.modules["config.config_loader"] = config_loader_module

from src.docset_hub.storage.metadata_db import MetadataDB


def _build_mocked_metadata_db() -> tuple[MetadataDB, Mock]:
    db = MetadataDB.__new__(MetadataDB)  # 跳过 __init__
    conn = Mock()
    cm = Mock()
    cm.__enter__ = Mock(return_value=conn)
    cm.__exit__ = Mock(return_value=False)
    engine = Mock()
    engine.connect.return_value = cm
    db.engine = engine
    return db, conn


def test_upsert_embedding_status_pending_commits():
    db, conn = _build_mocked_metadata_db()

    db.upsert_embedding_status_pending(
        paper_id=1,
        work_id="W1",
        canonical_source_id=11,
        source_name="biorxiv_history",
        text_type="abstract"
    )

    assert conn.execute.call_count == 1
    conn.commit.assert_called_once()


def test_mark_embedding_succeeded_returns_true_when_row_exists():
    db, conn = _build_mocked_metadata_db()
    result = Mock()
    result.rowcount = 1
    conn.execute.return_value = result

    ok = db.mark_embedding_succeeded(paper_id=1)

    assert ok is True
    conn.commit.assert_called_once()


def test_mark_embedding_failed_returns_false_when_row_missing():
    db, conn = _build_mocked_metadata_db()
    result = Mock()
    result.rowcount = 0
    conn.execute.return_value = result

    ok = db.mark_embedding_failed(paper_id=999, error_message="failed")

    assert ok is False
    conn.commit.assert_called_once()


def test_list_embedding_candidates_maps_rows():
    db, conn = _build_mocked_metadata_db()
    conn.execute.return_value.fetchall.return_value = [
        (1, "W1", 11, "biorxiv_history", "abstract", "pending", 0, None, None, None)
    ]

    rows = db.list_embedding_candidates(source_name="biorxiv_history", statuses=["pending"], limit=10, offset=0)

    assert len(rows) == 1
    assert rows[0]["paper_id"] == 1
    assert rows[0]["work_id"] == "W1"
    assert rows[0]["status"] == "pending"
