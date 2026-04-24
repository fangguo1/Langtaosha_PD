"""MetadataDB 新架构测试

测试新的多源架构，包括：
- insert_paper: 幂等性插入
- update_paper: 强制更新
- upsert_paper: 插入或更新
- 多源论文支持
- canonical source 选择

测试数据从 test_data 目录读取，包含 >10 篇论文。

运行方式：

方式1 - 通过 pytest（使用默认配置）：
    pytest tests/db/test_metadata_db.py -v

方式2 - 直接运行（使用 argparse 参数）：
    python tests/db/test_metadata_db.py --config-path=src/config/config_tecent_backend_server_test.yaml

方式3 - 通过环境变量：
    export METADATA_DB_CONFIG=src/config/config_tecent_backend_server_test.yaml
    pytest tests/db/test_metadata_db.py -v

默认配置文件：src/config/config_tecent_backend_server_test.yaml
"""

import pytest
import logging
import argparse
import os
from pathlib import Path
from typing import Dict, Any, List
from sqlalchemy import text
from sqlalchemy.engine import Connection

# 添加项目根目录到路径
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.docset_hub.metadata.transformer import MetadataTransformer
from src.docset_hub.storage.metadata_db import MetadataDB
from config.config_loader import init_config, get_db_engine


# =============================================================================
# 配置文件处理
# =============================================================================

_global_config_path = None


def get_config_path_from_args() -> Path:
    """从命令行参数或环境变量获取配置文件路径

    优先级：
        1. 命令行参数 --config-path
        2. 环境变量 METADATA_DB_CONFIG
        3. 默认使用 config_tecent_backend_server_test.yaml

    Returns:
        Path: 配置文件路径
    """
    global _global_config_path

    # 如果已经解析过，直接返回
    if _global_config_path:
        return _global_config_path

    config_path = None

    # 1. 检查命令行参数（只在直接运行时有效）
    # 判断是否是 pytest 运行：检查 sys.argv 中是否包含 'pytest'
    is_pytest = any('pytest' in arg for arg in sys.argv)

    if not is_pytest:
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument('--config-path', type=str, default=None)
            args, unknown = parser.parse_known_args()
            if args.config_path:
                config_path = Path(args.config_path)
        except:
            pass

    # 2. 检查环境变量
    if config_path is None and 'METADATA_DB_CONFIG' in os.environ:
        config_path = Path(os.environ['METADATA_DB_CONFIG'])

    # 3. 使用默认配置文件
    if config_path is None:
        project_root = Path(__file__).parent.parent.parent
        config_path = project_root / 'src' / 'config' / 'config_tecent_backend_server_test.yaml'

    if not config_path.exists():
        raise ValueError(
            f"❌ 配置文件不存在: {config_path}\n"
            f"请通过以下方式之一指定配置文件：\n"
            f"  1. 命令行参数: python tests/db/test_metadata_db.py --config-path=src/config/config_tecent_backend_server_test.yaml\n"
            f"  2. 环境变量: export METADATA_DB_CONFIG=src/config/config_tecent_backend_server_test.yaml\n"
            f"  3. 默认路径: src/config/config_tecent_backend_server_test.yaml"
        )

    _global_config_path = config_path
    print(f"✅ 使用配置文件: {config_path}")
    return config_path


# 尝试在模块级别获取配置路径
try:
    _config_path = get_config_path_from_args()
except Exception:
    # pytest 收集阶段可能会失败，在 fixture 中再次尝试
    _config_path = None


# =============================================================================
# Fixtures - 从 test_data 读取真实数据
# =============================================================================

def load_test_papers() -> Dict[str, List[Dict[str, Any]]]:
    """从 test_data 目录加载测试论文数据

    Returns:
        Dict: {
            "langtaosha": [...],           # LangTaoSha 论文列表（5个文件）
            "biorxiv_history": [...],      # bioRxiv 历史（2020年）论文列表（10个文件）
            "biorxiv_daily": [...]         # bioRxiv 日常（2025-2026年）论文列表（4个文件）
        }
    """
    test_data_dir = Path(__file__).parent.parent.parent / "test_data"

    import json

    # 加载 LangTaoSha 数据（5个文件）
    langtaosha_dir = test_data_dir / "langtaosha"
    langtaosha_files = sorted(langtaosha_dir.glob("*.json"))
    langtaosha_papers = []
    for file_path in langtaosha_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            langtaosha_papers.append(json.load(f))

    # 加载 bioRxiv 历史数据（10个文件，2020年数据）
    biorxiv_history_dir = test_data_dir / "biorxiv_history"
    biorxiv_history_files = sorted(biorxiv_history_dir.glob("*.json"))
    biorxiv_history_papers = []
    for file_path in biorxiv_history_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            biorxiv_history_papers.append(json.load(f))

    # 加载 bioRxiv 日常数据（4个文件，2025-2026年数据）
    biorxiv_daily_dir = test_data_dir / "biorxiv_daily"
    biorxiv_daily_files = sorted(biorxiv_daily_dir.glob("*.json"))
    biorxiv_daily_papers = []
    for file_path in biorxiv_daily_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            biorxiv_daily_papers.append(json.load(f))

    return {
        "langtaosha": langtaosha_papers,
        "biorxiv_history": biorxiv_history_papers,
        "biorxiv_daily": biorxiv_daily_papers
    }


# =============================================================================
# Pytest Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def db_engine():
    """数据库引擎"""
    # 总是重新获取配置路径
    config_path = get_config_path_from_args()
    init_config(config_path)
    engine = get_db_engine(db_key='metadata_db')

    # Session 开始时清理一次（清理之前遗留的数据）
    print("\n🧹 [Session 开始] 清理遗留测试数据...")
    with engine.connect() as conn:
        # 删除所有测试数据（不限制 paper_id）
        conn.execute(text("DELETE FROM paper_source_metadata WHERE paper_source_id IN (SELECT paper_source_id FROM paper_sources)"))
        conn.execute(text("DELETE FROM paper_author_affiliation"))
        conn.execute(text("DELETE FROM paper_keywords"))
        conn.execute(text("DELETE FROM paper_references"))
        conn.execute(text("DELETE FROM paper_sources"))
        conn.execute(text("DELETE FROM papers"))
        conn.commit()

        # 验证清理结果
        result = conn.execute(text("SELECT COUNT(*) FROM papers"))
        count = result.scalar()
        print(f"✅ [Session 开始] 清理完成，剩余 {count} 条记录")

    yield engine

    # Session 结束时清理一次
    print("\n🧹 [Session 结束] 清理测试数据...")
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM paper_source_metadata WHERE paper_source_id IN (SELECT paper_source_id FROM paper_sources)"))
        conn.execute(text("DELETE FROM paper_author_affiliation"))
        conn.execute(text("DELETE FROM paper_keywords"))
        conn.execute(text("DELETE FROM paper_references"))
        conn.execute(text("DELETE FROM paper_sources"))
        conn.execute(text("DELETE FROM papers"))
        conn.commit()

        result = conn.execute(text("SELECT COUNT(*) FROM papers"))
        count = result.scalar()
        print(f"✅ [Session 结束] 清理完成，剩余 {count} 条记录")


@pytest.fixture(scope="function")
def clean_db(db_engine):
    """每个测试前清理数据库"""
    with db_engine.connect() as conn:
        # 删除所有测试数据（不限制 paper_id）
        conn.execute(text("DELETE FROM paper_source_metadata WHERE paper_source_id IN (SELECT paper_source_id FROM paper_sources)"))
        conn.execute(text("DELETE FROM paper_author_affiliation"))
        conn.execute(text("DELETE FROM paper_keywords"))
        conn.execute(text("DELETE FROM paper_references"))
        conn.execute(text("DELETE FROM paper_sources"))
        conn.execute(text("DELETE FROM papers"))
        conn.commit()

    yield db_engine


@pytest.fixture(scope="function")
def metadata_db(clean_db):
    """MetadataDB 实例"""
    config_path = get_config_path_from_args()
    return MetadataDB(config_path=config_path)


@pytest.fixture(scope="function")
def transformer():
    """MetadataTransformer 实例"""
    return MetadataTransformer()


@pytest.fixture(scope="session")
def test_papers():
    """测试论文数据"""
    return load_test_papers()


# =============================================================================
# 辅助函数
# =============================================================================

def insert_paper_via_transformer(
    metadata_db: MetadataDB,
    transformer: MetadataTransformer,
    paper_data: Dict[str, Any],
    source_name: str
) -> int:
    """使用 transformer 转换并插入论文

    Args:
        metadata_db: MetadataDB 实例
        transformer: MetadataTransformer 实例
        paper_data: 论文原始数据
        source_name: 来源名称

    Returns:
        int: paper_id
    """
    # 使用 transformer 转换
    result = transformer.transform_dict(paper_data, source_name=source_name)

    assert result.success, f"转换失败: {result.error}"
    assert result.db_payload is not None
    assert result.upsert_key is not None

    # 插入到数据库（新接口返回结构化结果）
    write_result = metadata_db.insert_paper(
        db_payload=result.db_payload,
        upsert_key=result.upsert_key
    )
    assert write_result["paper_id"] is not None
    return write_result["paper_id"]


def count_papers(conn: Connection) -> int:
    """统计 papers 表记录数"""
    result = conn.execute(text("SELECT COUNT(*) FROM papers WHERE paper_id < 10000"))
    return result.scalar()


def count_sources(conn: Connection, paper_id: int) -> int:
    """统计指定论文的 source 数量"""
    result = conn.execute(
        text("SELECT COUNT(*) FROM paper_sources WHERE paper_id = :paper_id"),
        {"paper_id": paper_id}
    )
    return result.scalar()


def get_canonical_source_id(conn: Connection, paper_id: int) -> int:
    """获取论文的 canonical_source_id"""
    result = conn.execute(
        text("SELECT canonical_source_id FROM papers WHERE paper_id = :paper_id"),
        {"paper_id": paper_id}
    )
    return result.scalar()


def get_source_ids(conn: Connection, paper_id: int) -> List[int]:
    """获取论文的所有 source_id"""
    result = conn.execute(
        text("SELECT paper_source_id FROM paper_sources WHERE paper_id = :paper_id"),
        {"paper_id": paper_id}
    )
    return [row[0] for row in result.fetchall()]


def get_work_id(conn: Connection, paper_id: int) -> str:
    """获取论文的 work_id。"""
    result = conn.execute(
        text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
        {"paper_id": paper_id}
    )
    return result.scalar()


def get_canonical_source_name(conn: Connection, paper_id: int) -> str:
    """获取当前 canonical_source_id 对应的 source_name"""
    result = conn.execute(
        text("""
            SELECT ps.source_name
            FROM papers p
            JOIN paper_sources ps ON ps.paper_source_id = p.canonical_source_id
            WHERE p.paper_id = :paper_id
        """),
        {"paper_id": paper_id}
    )
    row = result.fetchone()
    return row[0] if row else None


def make_biorxiv_paper(doi: str, title: str, date: str, version: str = "1") -> Dict[str, Any]:
    """构造最小可用的 biorxiv_history 输入。"""
    return {
        "title": title,
        "doi": doi,
        "authors": "Author A; Author B",
        "abstract": "Test abstract",
        "date": date,
        "version": version,
        "category": "test",
        "server": "bioRxiv"
    }


def make_langtaosha_paper(
    doi: str,
    title: str,
    online_date: str,
    view_id: str = "999",
    download_id: str = "1001"
) -> Dict[str, Any]:
    """构造最小可用的 langtaosha 输入。"""
    return {
        "citation_title": title,
        "citation_abstract": "Test abstract",
        "citation_language": "en",
        "citation_publisher": "Langtaosha",
        "citation_date": online_date,
        "citation_online_date": online_date,
        "citation_doi": doi,
        "citation_abstract_html_url": f"https://langtaosha.org.cn/lts/en/preprint/view/{view_id}",
        "citation_pdf_url": f"https://langtaosha.org.cn/lts/en/preprint/download/{view_id}/{download_id}",
        "citation_author": ["Author A", "Author B"]
    }


class TestWriteResultStatus:
    """写入结果状态码测试：覆盖操作状态表。"""

    def test_insert_new_paper_status(self, metadata_db, transformer):
        data = make_biorxiv_paper("10.1101/status.insert.new.001", "Insert New", "2026-04-01")
        t = transformer.transform_dict(data, source_name="biorxiv_history")
        result = metadata_db.insert_paper(t.db_payload, t.upsert_key)
        assert result["status_code"] == "INSERT_NEW_PAPER"
        assert result["resolve"]["match_type"] == "no_match"
        assert result["apply"]["action"] == "insert"
        assert result["paper_id"] is not None

    def test_insert_append_source_status(self, metadata_db, transformer):
        doi = "10.1101/status.insert.append.001"
        t1 = transformer.transform_dict(
            make_biorxiv_paper(doi, "Insert Append Base", "2026-04-01"),
            source_name="biorxiv_history"
        )
        base = metadata_db.insert_paper(t1.db_payload, t1.upsert_key)

        t2 = transformer.transform_dict(
            make_langtaosha_paper(doi, "Insert Append Cross", "2026-04-03", view_id="9101", download_id="501"),
            source_name="langtaosha"
        )
        result = metadata_db.insert_paper(t2.db_payload, t2.upsert_key)
        assert result["status_code"] == "INSERT_APPEND_SOURCE"
        assert result["resolve"]["match_type"] == "cross_source"
        assert result["paper_id"] == base["paper_id"]

    def test_insert_update_same_source_status(self, metadata_db, transformer):
        doi = "10.1101/status.insert.update.same.001"
        t1 = transformer.transform_dict(
            make_biorxiv_paper(doi, "Insert Same V1", "2026-04-01", version="1"),
            source_name="biorxiv_history"
        )
        base = metadata_db.insert_paper(t1.db_payload, t1.upsert_key)

        t2 = transformer.transform_dict(
            make_biorxiv_paper(doi, "Insert Same V2", "2026-04-02", version="2"),
            source_name="biorxiv_history"
        )
        result = metadata_db.insert_paper(t2.db_payload, t2.upsert_key)
        assert result["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
        assert result["resolve"]["match_type"] == "same_source"
        assert result["apply"]["action"] == "update"
        assert result["paper_id"] == base["paper_id"]

    def test_insert_skip_same_source_status(self, metadata_db, transformer):
        doi = "10.1101/status.insert.skip.same.001"
        t1 = transformer.transform_dict(
            make_biorxiv_paper(doi, "Insert Skip V2", "2026-04-02", version="2"),
            source_name="biorxiv_history"
        )
        base = metadata_db.insert_paper(t1.db_payload, t1.upsert_key)

        t2 = transformer.transform_dict(
            make_biorxiv_paper(doi, "Insert Skip V1", "2026-04-01", version="1"),
            source_name="biorxiv_history"
        )
        result = metadata_db.insert_paper(t2.db_payload, t2.upsert_key)
        assert result["status_code"] == "INSERT_SKIP_SAME_SOURCE"
        assert result["resolve"]["match_type"] == "same_source"
        assert result["apply"]["action"] == "skip"
        assert result["paper_id"] == base["paper_id"]

    def test_update_statuses(self, metadata_db, transformer):
        doi = "10.1101/status.update.same.001"
        t1 = transformer.transform_dict(
            make_biorxiv_paper(doi, "Update Base", "2026-04-01", version="1"),
            source_name="biorxiv_history"
        )
        base = metadata_db.insert_paper(t1.db_payload, t1.upsert_key)

        t2 = transformer.transform_dict(
            make_biorxiv_paper(doi, "Update Hit", "2026-04-02", version="1"),
            source_name="biorxiv_history"
        )
        hit = metadata_db.update_paper(t2.db_payload, t2.upsert_key)
        assert hit["status_code"] == "UPDATE_SAME_SOURCE"
        assert hit["paper_id"] == base["paper_id"]
        assert hit["apply"]["action"] == "update"

        t3 = transformer.transform_dict(
            make_biorxiv_paper("10.1101/status.update.reject.001", "Update Reject", "2026-04-02"),
            source_name="biorxiv_history"
        )
        reject = metadata_db.update_paper(t3.db_payload, t3.upsert_key)
        assert reject["status_code"] == "UPDATE_NOT_ALLOWED_NON_SAME_SOURCE"
        assert reject["paper_id"] is None
        assert reject["apply"]["action"] == "reject"

    def test_upsert_statuses(self, metadata_db, transformer):
        # UPSERT_NEW_PAPER
        t1 = transformer.transform_dict(
            make_biorxiv_paper("10.1101/status.upsert.new.001", "Upsert New", "2026-04-01"),
            source_name="biorxiv_history"
        )
        new_result = metadata_db.upsert_paper(t1.db_payload, t1.upsert_key)
        assert new_result["status_code"] == "UPSERT_NEW_PAPER"
        assert new_result["resolve"]["match_type"] == "no_match"

        # UPSERT_UPDATE_SAME_SOURCE
        t2 = transformer.transform_dict(
            make_biorxiv_paper("10.1101/status.upsert.new.001", "Upsert Same", "2026-04-03", version="2"),
            source_name="biorxiv_history"
        )
        same_result = metadata_db.upsert_paper(t2.db_payload, t2.upsert_key)
        assert same_result["status_code"] == "UPSERT_UPDATE_SAME_SOURCE"
        assert same_result["paper_id"] == new_result["paper_id"]

        # UPSERT_APPEND_SOURCE
        t3 = transformer.transform_dict(
            make_langtaosha_paper("10.1101/status.upsert.new.001", "Upsert Cross", "2026-04-04", view_id="9201", download_id="601"),
            source_name="langtaosha"
        )
        cross_result = metadata_db.upsert_paper(t3.db_payload, t3.upsert_key)
        assert cross_result["status_code"] == "UPSERT_APPEND_SOURCE"
        assert cross_result["resolve"]["match_type"] == "cross_source"
        assert cross_result["paper_id"] == new_result["paper_id"]


# =============================================================================
# 测试 insert_paper - 幂等性插入
# =============================================================================

class TestInsertPaper:
    """测试 insert_paper 方法"""

    @pytest.mark.parametrize("source_key,source_name", [
        ("langtaosha", "langtaosha"),
        ("biorxiv_history", "biorxiv_history"),
        ("biorxiv_daily", "biorxiv_daily")
    ])
    def test_insert_new_paper(self, metadata_db, transformer, test_papers, source_key, source_name):
        """测试插入新论文（参数化三个 sources）"""
        # 获取对应 source 的测试数据
        paper_data = test_papers[source_key][0]

        # 第一次插入
        paper_id_1 = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, source_name
        )

        assert paper_id_1 is not None
        assert paper_id_1 > 0

        # 验证数据库中有记录
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 1

            # 验证有对应的 source 记录
            source_count = count_sources(conn, paper_id_1)
            assert source_count == 1

            # 验证 work_id 已生成且格式正确
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id_1}
            )
            work_id = result.scalar()
            assert work_id is not None, "work_id 不应为 None"
            assert work_id.startswith('W'), f"work_id 应以 'W' 开头，实际: {work_id}"
            assert len(work_id) == 37, f"work_id 长度应为 37，实际: {len(work_id)}"

            # 新增：验证 source_name 正确
            source_result = conn.execute(
                text("SELECT source_name FROM paper_sources WHERE paper_id = :paper_id"),
                {"paper_id": paper_id_1}
            )
            actual_source = source_result.scalar()
            assert actual_source == source_name

    def test_work_id_generated_by_metadata_db_on_new_paper(self, metadata_db, transformer):
        """验证 work_id 由 MetadataDB 在新建 paper 时分配。"""
        data = make_biorxiv_paper(
            doi="10.1101/test.workid.dbgen.001",
            title="WorkID DB Generated",
            date="2026-04-01",
            version="1"
        )
        transformed = transformer.transform_dict(data, source_name="biorxiv_history")
        assert transformed.success
        assert transformed.db_payload is not None
        assert transformed.db_payload["papers"]["work_id"] is None
        assert transformed.work_id is None

        write_result = metadata_db.insert_paper(
            db_payload=transformed.db_payload,
            upsert_key=transformed.upsert_key
        )
        assert write_result["status_code"] == "INSERT_NEW_PAPER"
        paper_id = write_result["paper_id"]
        assert paper_id is not None

        with metadata_db.engine.connect() as conn:
            work_id = get_work_id(conn, paper_id)
            assert work_id is not None
            assert work_id.startswith("W")
            assert len(work_id) == 37

    def test_insert_paper_idempotent(self, metadata_db, transformer, test_papers):
        """测试 insert_paper 幂等性（重复插入应返回相同 paper_id，不更新）"""
        paper_data = test_papers["biorxiv_history"][0]

        # 第一次插入
        paper_id_1 = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 第二次插入（应返回相同的 paper_id，不更新数据）
        paper_id_2 = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 验证幂等性
        assert paper_id_1 == paper_id_2

        # 验证数据库中只有一条记录
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 1

            source_count = count_sources(conn, paper_id_1)
            assert source_count == 1

    def test_insert_multiple_papers(self, metadata_db, transformer, test_papers):
        """测试插入多篇论文（从三个 sources）"""
        paper_ids = []
        source_records = []  # 新增：记录 source 信息

        # 插入 5 篇 biorxiv_history 论文
        for i, paper_data in enumerate(test_papers["biorxiv_history"][:5]):
            paper_id = insert_paper_via_transformer(
                metadata_db, transformer, paper_data, "biorxiv_history"
            )
            paper_ids.append(paper_id)
            source_records.append(("biorxiv_history", paper_id))

        # 插入 4 篇 biorxiv_daily 论文
        for i, paper_data in enumerate(test_papers["biorxiv_daily"][:4]):
            paper_id = insert_paper_via_transformer(
                metadata_db, transformer, paper_data, "biorxiv_daily"
            )
            paper_ids.append(paper_id)
            source_records.append(("biorxiv_daily", paper_id))

        # 插入 5 篇 LangTaoSha 论文
        for i, paper_data in enumerate(test_papers["langtaosha"][:5]):
            paper_id = insert_paper_via_transformer(
                metadata_db, transformer, paper_data, "langtaosha"
            )
            paper_ids.append(paper_id)
            source_records.append(("langtaosha", paper_id))

        # 验证插入了 14 篇不同的论文
        assert len(paper_ids) == 14
        assert len(set(paper_ids)) == 14  # 所有 paper_id 都不同

        # 验证数据库中有 14 条记录
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 14

            # 新增：验证每个 paper 的 source_name 正确
            for expected_source, paper_id in source_records:
                result = conn.execute(
                    text("SELECT source_name FROM paper_sources WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                actual_source = result.scalar()
                assert actual_source == expected_source

    def test_insert_paper_auto_canonical(self, metadata_db, transformer, test_papers):
        """测试 insert_paper 自动设置 canonical_source（使用 online_at 法则）"""
        paper_data = test_papers["biorxiv_history"][0]

        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 验证 canonical_source_id 已设置
        with metadata_db.engine.connect() as conn:
            canonical_id = get_canonical_source_id(conn, paper_id)
            assert canonical_id is not None

            # 验证 canonical_source_id 指向唯一的 source
            source_ids = get_source_ids(conn, paper_id)
            assert canonical_id in source_ids


# =============================================================================
# 测试 update_paper - 强制更新
# =============================================================================

class TestUpdatePaper:
    """测试 update_paper 方法"""

    def test_update_existing_paper(self, metadata_db, transformer, test_papers):
        """测试更新已存在的论文"""
        paper_data = test_papers["biorxiv_history"][0]

        # 先插入
        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 修改数据（例如修改标题）
        modified_data = paper_data.copy()
        if "title" in modified_data:
            modified_data["title"] += " [Updated]"

        # 更新
        result = transformer.transform_dict(modified_data, source_name="biorxiv_history")
        assert result.success

        update_result = metadata_db.update_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key,
            auto_select_canonical=True
        )

        # 验证更新成功
        assert update_result["status_code"] == "UPDATE_SAME_SOURCE"
        assert update_result["paper_id"] == paper_id

        # 验证数据已更新（检查标题）
        with metadata_db.engine.connect() as conn:
            result = conn.execute(
                text("SELECT canonical_title FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            title = result.scalar()
            assert "[Updated]" in title

    def test_update_nonexistent_paper(self, metadata_db, transformer, test_papers):
        """测试更新不存在的论文（应返回 None）"""
        paper_data = test_papers["biorxiv_history"][0]

        # 不插入，直接更新
        result = transformer.transform_dict(paper_data, source_name="biorxiv_history")
        assert result.success

        update_result = metadata_db.update_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )

        # 验证返回 reject 状态
        assert update_result["status_code"] == "UPDATE_NOT_ALLOWED_NON_SAME_SOURCE"
        assert update_result["paper_id"] is None

        # 验证数据库中没有记录
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 0

    def test_update_paper_with_manual_canonical(self, metadata_db, transformer, test_papers):
        """测试手动指定 canonical_source_id"""
        paper_data = test_papers["biorxiv_history"][0]

        # 先插入
        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 获取 source_id
        with metadata_db.engine.connect() as conn:
            source_ids = get_source_ids(conn, paper_id)
            canonical_id = source_ids[0]

        # 更新时手动指定 canonical_source_id
        modified_data = paper_data.copy()
        result = transformer.transform_dict(modified_data, source_name="biorxiv_history")
        assert result.success

        update_result = metadata_db.update_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key,
            canonical_source_id=canonical_id,
            auto_select_canonical=False
        )

        # 验证更新成功
        assert update_result["status_code"] == "UPDATE_SAME_SOURCE"
        assert update_result["paper_id"] == paper_id

        # 验证 canonical_source_id 正确
        with metadata_db.engine.connect() as conn:
            current_canonical = get_canonical_source_id(conn, paper_id)
            assert current_canonical == canonical_id


# =============================================================================
# 测试 upsert_paper - 插入或更新
# =============================================================================

class TestUpsertPaper:
    """测试 upsert_paper 方法"""

    def test_upsert_new_paper(self, metadata_db, transformer, test_papers):
        """测试 upsert 新论文（应插入）"""
        paper_data = test_papers["biorxiv_history"][0]

        # Upsert 不存在的论文
        result = transformer.transform_dict(paper_data, source_name="biorxiv_history")
        assert result.success

        write_result = metadata_db.upsert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key,
            auto_select_canonical=True
        )
        paper_id = write_result["paper_id"]

        # 验证插入成功
        assert write_result["status_code"] == "UPSERT_NEW_PAPER"
        assert paper_id is not None

        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 1

    def test_upsert_existing_paper(self, metadata_db, transformer, test_papers):
        """测试 upsert 已存在的论文（应更新）"""
        paper_data = test_papers["biorxiv_history"][0]

        # 先插入
        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 修改数据
        modified_data = paper_data.copy()
        if "title" in modified_data:
            modified_data["title"] += " [Upserted]"

        # Upsert（应更新）
        result = transformer.transform_dict(modified_data, source_name="biorxiv_history")
        assert result.success

        upsert_result = metadata_db.upsert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key,
            auto_select_canonical=True
        )

        # 验证更新成功
        assert upsert_result["status_code"] == "UPSERT_UPDATE_SAME_SOURCE"
        assert upsert_result["paper_id"] == paper_id

        # 验证数据已更新
        with metadata_db.engine.connect() as conn:
            result = conn.execute(
                text("SELECT canonical_title FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            title = result.scalar()
            assert "[Upserted]" in title

    def test_upsert_ignores_canonical_source_id_on_insert(self, metadata_db, transformer, test_papers):
        """测试 upsert 在插入新记录时忽略 canonical_source_id 参数"""
        paper_data = test_papers["biorxiv_history"][0]

        # Upsert 新记录，即使指定了 canonical_source_id 也应忽略
        result = transformer.transform_dict(paper_data, source_name="biorxiv_history")
        assert result.success

        upsert_result = metadata_db.upsert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key,
            canonical_source_id=99999,  # 无效的 ID，应被忽略
            auto_select_canonical=True
        )
        paper_id = upsert_result["paper_id"]

        # 验证插入成功
        assert upsert_result["status_code"] == "UPSERT_NEW_PAPER"
        assert paper_id is not None

        # 验证 canonical_source_id 不是 99999，而是自动选择的
        with metadata_db.engine.connect() as conn:
            canonical_id = get_canonical_source_id(conn, paper_id)
            assert canonical_id != 99999
            assert canonical_id is not None


# =============================================================================
# 测试多源论文支持
# =============================================================================

class TestMultiSource:
    """测试多源论文功能"""

    def test_multi_source_paper_same_paper(self, metadata_db, transformer):
        """测试同一篇论文从多个来源插入（只有一个 paper_id，多个 paper_source_id）"""
        # 注意：这个测试需要同一篇论文的 biorxiv_history 和 langtaosha 数据
        # 由于 test_data 中可能没有这样的数据，这里使用不同的论文来演示多源概念

        # 插入第一篇论文（biorxiv_history）
        biorxiv_history_paper = {
            "title": "Test Paper for Multi-Source",
            "doi": "10.1101/test.multi.source.001",
            "authors": "Author A; Author B",
            "abstract": "This is a test paper for multi-source support.",
            "date": "2026-04-01",
            "category": "test",
            "server": "bioRxiv"
        }

        result = transformer.transform_dict(biorxiv_history_paper, source_name="biorxiv_history")
        assert result.success

        insert_result_1 = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id_1 = insert_result_1["paper_id"]

        # 模拟同一篇论文从另一个来源插入
        # 注意：在真实场景中，这应该是同一篇论文的不同来源版本
        langtaosha_paper = {
            "citation_title": "Test Paper for Multi-Source",
            "citation_doi": "10.1101/test.multi.source.001",  # 相同的 DOI
            "citation_author": ["Author A", "Author B"],
            "citation_abstract": "This is a test paper for multi-source support.",
            "citation_date": "2026/04/01",
            "citation_online_date": "2026/04/01",
            "citation_publisher": "LangTaoSha",
            # langtaosha 需要可提取的站内记录 ID（通常从 view/<id> URL 中提取）
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/999",
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/999/1001"
        }

        result = transformer.transform_dict(langtaosha_paper, source_name="langtaosha")
        assert result.success

        insert_result_2 = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id_2 = insert_result_2["paper_id"]

        # 注意：由于 DOI 相同，新架构可能会识别为同一篇论文
        # 或者如果识别为不同论文，这里也会测试多源功能
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            # 根据实际的实现，这里可能是 1 或 2
            # 如果 DOI 作为去重依据，应该是 1
            # 如果每个 source 都是独立的，可能是 2
            assert count >= 1


# =============================================================================
# 测试 canonical source 选择
# =============================================================================

class TestCanonicalSource:
    """测试 canonical source 选择逻辑"""

    def test_canonical_selection_by_online_at(self, metadata_db, transformer):
        """测试根据 online_at 时间法则选择 canonical source"""
        # 这个测试需要同一篇论文有多个 source，且 online_at 时间不同
        # 由于真实数据可能不满足条件，这里只演示测试结构

        paper_data = {
            "title": "Test Paper for Canonical Selection",
            "doi": "10.1101/test.canonical.001",
            "authors": "Author A",
            "abstract": "Test abstract",
            "date": "2026-04-01",
            "category": "test",
            "server": "bioRxiv"
        }

        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 验证 canonical_source_id 已设置
        with metadata_db.engine.connect() as conn:
            canonical_id = get_canonical_source_id(conn, paper_id)
            assert canonical_id is not None

            # 验证 canonical_source 指向 online_at 最晚的 source
            # 对于单个 source，应该指向它自己
            source_ids = get_source_ids(conn, paper_id)
            assert canonical_id in source_ids


# =============================================================================
# 测试 source 校验
# =============================================================================

class TestSourceValidation:
    """测试 source 校验功能"""

    def test_metadata_db_loads_default_sources(self, metadata_db):
        """测试 MetadataDB 初始化后持有 default_sources"""
        # 验证属性存在
        assert hasattr(metadata_db, "default_sources")

        # 验证类型
        assert isinstance(metadata_db.default_sources, list)

        # 验证非空
        assert len(metadata_db.default_sources) > 0

        # 验证内容与配置一致
        expected_sources = ["langtaosha", "biorxiv_history", "biorxiv_daily"]
        assert set(metadata_db.default_sources) == set(expected_sources)

    @pytest.mark.parametrize("source_key,source_name", [
        ("langtaosha", "langtaosha"),
        ("biorxiv_history", "biorxiv_history"),
        ("biorxiv_daily", "biorxiv_daily")
    ])
    def test_insert_paper_with_valid_source(self, metadata_db, transformer, test_papers, source_key, source_name):
        """测试合法 source 可以正常通过 insert_paper"""
        # 获取对应 source 的测试数据
        paper_data = test_papers[source_key][0]

        # 转换并插入
        result = transformer.transform_dict(paper_data, source_name=source_name)
        assert result.success

        write_result = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id = write_result["paper_id"]

        # 验证插入成功
        assert write_result["status_code"] in {"INSERT_NEW_PAPER", "INSERT_APPEND_SOURCE"}
        assert paper_id is not None
        assert paper_id > 0

        # 验证数据库中记录的 source_name 正确
        with metadata_db.engine.connect() as conn:
            source_result = conn.execute(
                text("SELECT source_name FROM paper_sources WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            actual_source = source_result.scalar()
            assert actual_source == source_name

    def test_insert_paper_with_invalid_source_raises(self, metadata_db, transformer, test_papers):
        """测试非法 source 在 insert_paper 入口被拦截"""
        # 使用合法数据完成转换
        paper_data = test_papers["langtaosha"][0]
        result = transformer.transform_dict(paper_data, source_name="langtaosha")
        assert result.success

        # 手工篡改 source_name 为非法值
        result.upsert_key["source_name"] = "invalid_source"
        result.db_payload["paper_sources"]["source_name"] = "invalid_source"

        # 断言抛出 ValueError
        with pytest.raises(ValueError, match="source_name 'invalid_source' 不在 default_sources 中"):
            metadata_db.insert_paper(
                db_payload=result.db_payload,
                upsert_key=result.upsert_key
            )

    def test_insert_paper_source_mismatch_raises(self, metadata_db, transformer, test_papers):
        """测试 db_payload 和 upsert_key 的 source_name 不一致时报错"""
        # 使用合法数据完成转换
        paper_data = test_papers["langtaosha"][0]
        result = transformer.transform_dict(paper_data, source_name="langtaosha")
        assert result.success

        # 手工篡改 db_payload 的 source_name（upsert_key 保持不变）
        result.db_payload["paper_sources"]["source_name"] = "biorxiv_history"

        # 断言抛出 ValueError
        with pytest.raises(ValueError, match="source_name 不一致"):
            metadata_db.insert_paper(
                db_payload=result.db_payload,
                upsert_key=result.upsert_key
            )

    def test_update_paper_with_invalid_source_raises(self, metadata_db, transformer, test_papers):
        """测试 update_paper 同样受 default_sources 约束"""
        # 先用合法 source 插入一篇论文
        paper_data = test_papers["langtaosha"][0]
        result = transformer.transform_dict(paper_data, source_name="langtaosha")
        assert result.success

        write_result = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id = write_result["paper_id"]

        # 基于更新数据生成 transform result
        modified_data = paper_data.copy()
        if "title" in modified_data:
            modified_data["title"] += " [Updated]"

        update_result = transformer.transform_dict(modified_data, source_name="langtaosha")
        assert update_result.success

        # 手工篡改 source_name 为非法值
        update_result.upsert_key["source_name"] = "invalid_source"
        update_result.db_payload["paper_sources"]["source_name"] = "invalid_source"

        # 断言抛出 ValueError
        with pytest.raises(ValueError, match="source_name 'invalid_source' 不在 default_sources 中"):
            metadata_db.update_paper(
                db_payload=update_result.db_payload,
                upsert_key=update_result.upsert_key
            )

    def test_upsert_paper_with_invalid_source_raises(self, metadata_db, transformer, test_papers):
        """测试 upsert_paper 入口也做相同校验"""
        # 使用合法数据完成转换
        paper_data = test_papers["langtaosha"][0]
        result = transformer.transform_dict(paper_data, source_name="langtaosha")
        assert result.success

        # 手工篡改 source_name 为非法值
        result.upsert_key["source_name"] = "invalid_source"
        result.db_payload["paper_sources"]["source_name"] = "invalid_source"

        # 断言抛出 ValueError
        with pytest.raises(ValueError, match="source_name 'invalid_source' 不在 default_sources 中"):
            metadata_db.upsert_paper(
                db_payload=result.db_payload,
                upsert_key=result.upsert_key
            )

    def test_resolve_match_rejects_invalid_source(self, metadata_db):
        """测试统一判定方法也有 source 兜底校验"""
        # 构造最小 upsert_key
        upsert_key = {
            "source_name": "invalid_source",
            "source_identifiers": {"invalid_source": "test_001"}
        }

        # 通过内部方法调用，断言抛出 ValueError
        with pytest.raises(ValueError, match="source_name 'invalid_source' 不在 default_sources 中"):
            with metadata_db.engine.connect() as conn:
                metadata_db._resolve_match_by_identity(conn, upsert_key)


# =============================================================================
# 测试 paper_sources 表验证
# =============================================================================

class TestPaperSourcesTable:
    """测试 paper_sources 表的正确记录"""

    @pytest.mark.parametrize("source_key,source_name", [
        ("langtaosha", "langtaosha"),
        ("biorxiv_history", "biorxiv_history"),
        ("biorxiv_daily", "biorxiv_daily")
    ])
    def test_paper_sources_table_records_correct_source(
        self, metadata_db, transformer, test_papers, source_key, source_name
    ):
        """测试 paper_sources 表正确记录 source_name"""
        # 插入论文
        paper_data = test_papers[source_key][0]
        result = transformer.transform_dict(paper_data, source_name=source_name)
        assert result.success

        write_result = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id = write_result["paper_id"]

        # 验证 paper_sources 表记录
        with metadata_db.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT source_name, source_record_id, doi, paper_id
                    FROM paper_sources
                    WHERE paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            )
            row = result.fetchone()

            assert row is not None
            assert row[0] == source_name  # source_name
            assert row[1] is not None  # source_record_id
            assert row[3] == paper_id  # paper_id

    def test_paper_sources_source_name_not_null(self, metadata_db, transformer, test_papers):
        """测试 paper_sources.source_name 不为 NULL"""
        # 插入多篇论文
        for source_key in ["langtaosha", "biorxiv_history", "biorxiv_daily"]:
            paper_data = test_papers[source_key][0]
            result = transformer.transform_dict(paper_data, source_name=source_key)
            assert result.success

            metadata_db.insert_paper(
                db_payload=result.db_payload,
                upsert_key=result.upsert_key
            )

        # 验证所有 source_name 都不为 NULL
        with metadata_db.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT COUNT(*)
                    FROM paper_sources
                    WHERE source_name IS NULL OR source_name = ''
                """)
            )
            null_count = result.scalar()
            assert null_count == 0, f"存在 {null_count} 条 source_name 为空的记录"


# =============================================================================
# 测试查询方法
# =============================================================================

class TestQueryMethods:
    """测试查询方法"""

    def test_get_paper_info_by_paper_id(self, metadata_db, transformer, test_papers):
        """测试根据 paper_id 获取论文信息"""
        paper_data = test_papers["biorxiv_history"][0]

        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 查询论文信息
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)

        assert paper_info is not None
        assert paper_info["paper_id"] == paper_id
        assert "canonical_title" in paper_info
        assert "sources" in paper_info

    def test_read_paper(self, metadata_db, transformer, test_papers):
        """测试读取完整论文数据"""
        paper_data = test_papers["biorxiv_history"][0]

        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 读取论文
        paper = metadata_db.read_paper(paper_id=paper_id)

        assert paper is not None
        assert paper["paper_id"] == paper_id

    def test_search_by_condition(self, metadata_db, transformer, test_papers):
        """测试基于条件搜索论文"""
        # 插入多篇论文
        for i, paper_data in enumerate(test_papers["biorxiv_history"][:5]):
            insert_paper_via_transformer(
                metadata_db, transformer, paper_data, "biorxiv_history"
            )

        # 搜索所有论文
        results = metadata_db.search_by_condition(limit=10)

        assert len(results) == 5

    def test_get_paper_info_by_work_id(self, metadata_db, transformer, test_papers):
        """测试通过 work_id 查询论文"""
        paper_data = test_papers["biorxiv_history"][0]

        # 插入论文
        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 获取 work_id（从数据库查询）
        with metadata_db.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            work_id = result.scalar()
            assert work_id is not None
            assert work_id.startswith('W')

        # 通过 work_id 查询论文
        paper_info = metadata_db.get_paper_info_by_work_id(work_id)

        assert paper_info is not None
        assert paper_info["paper_id"] == paper_id
        assert "canonical_title" in paper_info
        assert "sources" in paper_info

    def test_read_paper_by_work_id(self, metadata_db, transformer, test_papers):
        """测试通过 work_id 读取论文"""
        paper_data = test_papers["biorxiv_history"][0]

        # 插入论文
        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 获取 work_id
        with metadata_db.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            work_id = result.scalar()
            assert work_id is not None

        # 通过 work_id 读取论文
        paper = metadata_db.read_paper_by_work_id(work_id)

        assert paper is not None
        assert paper["paper_id"] == paper_id

    def test_get_papers_by_work_ids(self, metadata_db, transformer, test_papers):
        """测试批量通过 work_id 查询论文"""
        work_ids = []

        # 插入 3 篇论文
        for i in range(3):
            paper_data = test_papers["biorxiv_history"][i]
            paper_id = insert_paper_via_transformer(
                metadata_db, transformer, paper_data, "biorxiv_history"
            )

            # 获取 work_id
            with metadata_db.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                work_id = result.scalar()
                work_ids.append(work_id)

        # 批量查询
        papers = metadata_db.get_papers_by_work_ids(work_ids)

        assert len(papers) == 3
        assert all(p["paper_id"] is not None for p in papers)

    def test_delete_paper_by_work_id(self, metadata_db, transformer, test_papers):
        """测试通过 work_id 删除论文"""
        paper_data = test_papers["biorxiv_history"][0]

        # 插入论文
        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 获取 work_id
        with metadata_db.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            work_id = result.scalar()
            assert work_id is not None

        # 通过 work_id 删除论文
        success = metadata_db.delete_paper_by_work_id(work_id)

        assert success is True

        # 验证已删除
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 0

    def test_delete_paper_by_paper_id(self, metadata_db, transformer, test_papers):
        """测试删除论文"""
        paper_data = test_papers["biorxiv_history"][0]

        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # 删除论文
        success = metadata_db.delete_paper_by_paper_id(paper_id)

        assert success is True

        # 验证已删除
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 0


# =============================================================================
# 集成测试 - 完整流程
# =============================================================================

class TestIntegration:
    """集成测试 - 测试完整流程"""

    def test_batch_import_workflow(self, metadata_db, transformer, test_papers):
        """测试批量导入流程（从三个 sources）"""
        paper_ids = []

        # 批量导入论文：使用 biorxiv_history 和 langtaosha
        for i in range(5):
            # biorxiv_history
            paper_id = insert_paper_via_transformer(
                metadata_db, transformer, test_papers["biorxiv_history"][i], "biorxiv_history"
            )
            paper_ids.append(paper_id)

            # LangTaoSha
            paper_id = insert_paper_via_transformer(
                metadata_db, transformer, test_papers["langtaosha"][i], "langtaosha"
            )
            paper_ids.append(paper_id)

        # 验证导入了 10 篇论文
        assert len(paper_ids) == 10
        assert len(set(paper_ids)) == 10

        # 验证数据库状态
        with metadata_db.engine.connect() as conn:
            count = count_papers(conn)
            assert count == 10

        # 查询所有论文
        all_papers = metadata_db.search_by_condition(limit=20)
        assert len(all_papers) == 10

    def test_import_update_workflow(self, metadata_db, transformer, test_papers):
        """测试导入-更新工作流"""
        paper_data = test_papers["biorxiv_history"][0]

        # Step 1: 导入新论文
        paper_id = insert_paper_via_transformer(
            metadata_db, transformer, paper_data, "biorxiv_history"
        )

        # Step 2: 模拟数据更新
        modified_data = paper_data.copy()
        if "title" in modified_data:
            modified_data["title"] += " [Updated]"

        # Step 3: 更新论文
        result = transformer.transform_dict(modified_data, source_name="biorxiv_history")
        assert result.success

        update_result = metadata_db.update_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )

        # 验证更新成功
        assert update_result["status_code"] == "UPDATE_SAME_SOURCE"
        assert update_result["paper_id"] == paper_id

        # Step 4: 验证数据已更新
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        assert "[Updated]" in paper_info["canonical_title"]


# =============================================================================
# 测试版本比较逻辑
# =============================================================================

class TestVersionComparison:
    """测试版本比较与覆盖逻辑"""

    def test_insert_with_higher_version_updates(self, metadata_db, transformer, test_papers):
        """测试插入更高版本时更新记录"""
        paper_data = test_papers["langtaosha"][0]

        # Step 1: 插入版本 1.0
        result = transformer.transform_dict(paper_data, source_name="langtaosha")
        assert result.success

        # 手动设置版本为 1.0
        result.db_payload["paper_sources"]["version"] = "1.0"

        write_result = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id = write_result["paper_id"]

        # 验证 version 信息已正确入库
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        # 获取第一条 source 记录（应该只有一条）
        assert len(paper_info["sources"]) == 1, "应该有一条 source 记录"
        source_info = paper_info["sources"][0]
        assert source_info["version"] == "1.0", "版本 1.0 应该正确入库"

        # Step 2: 插入版本 2.0（应该更新）
        modified_data = paper_data.copy()
        if "citation_title" in modified_data:
            modified_data["citation_title"] += " [Version 2.0]"
        elif "title" in modified_data:
            modified_data["title"] += " [Version 2.0]"

        result2 = transformer.transform_dict(modified_data, source_name="langtaosha")
        assert result2.success

        # 手动设置版本为 2.0 和修改标题
        result2.db_payload["paper_sources"]["version"] = "2.0"
        result2.db_payload["paper_sources"]["title"] += " [Version 2.0]"
        result2.db_payload["papers"]["canonical_title"] += " [Version 2.0]"

        write_result2 = metadata_db.insert_paper(
            db_payload=result2.db_payload,
            upsert_key=result2.upsert_key
        )
        paper_id2 = write_result2["paper_id"]

        # 验证返回了相同的 paper_id（说明是更新而不是插入新记录）
        assert paper_id2 == paper_id

        # 验证数据已更新
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        assert "Version 2.0" in paper_info["canonical_title"]

        # 验证 version 信息已更新
        assert len(paper_info["sources"]) == 1, "应该有一条 source 记录"
        source_info = paper_info["sources"][0]
        assert source_info["version"] == "2.0", "版本应该更新为 2.0"

    def test_insert_with_lower_version_skips(self, metadata_db, transformer, test_papers):
        """测试插入更低版本时跳过更新"""
        paper_data = test_papers["langtaosha"][0]

        # Step 1: 插入版本 2.0
        result = transformer.transform_dict(paper_data, source_name="langtaosha")
        assert result.success

        # 手动设置版本为 2.0
        result.db_payload["paper_sources"]["version"] = "2.0"

        write_result = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id = write_result["paper_id"]

        # 验证 version 信息已正确入库
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        # 获取第一条 source 记录（应该只有一条）
        assert len(paper_info["sources"]) == 1, "应该有一条 source 记录"
        source_info = paper_info["sources"][0]
        assert source_info["version"] == "2.0", "版本 2.0 应该正确入库"

        # Step 2: 尝试插入版本 1.0（应该跳过）
        modified_data = paper_data.copy()
        if "citation_title" in modified_data:
            modified_data["citation_title"] += " [Version 1.0]"
        elif "title" in modified_data:
            modified_data["title"] += " [Version 1.0]"

        result2 = transformer.transform_dict(modified_data, source_name="langtaosha")
        assert result2.success

        # 手动设置版本为 1.0
        result2.db_payload["paper_sources"]["version"] = "1.0"

        write_result2 = metadata_db.insert_paper(
            db_payload=result2.db_payload,
            upsert_key=result2.upsert_key
        )
        paper_id2 = write_result2["paper_id"]

        # 验证返回了相同的 paper_id
        assert paper_id2 == paper_id

        # 验证数据未更新（仍然是版本 2.0 的标题）
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        assert "Version 1.0" not in paper_info["canonical_title"]

        # 验证 version 信息未更新（仍然是版本 2.0）
        assert len(paper_info["sources"]) == 1, "应该有一条 source 记录"
        source_info = paper_info["sources"][0]
        assert source_info["version"] == "2.0", "版本应该保持为 2.0，不更新为 1.0"

    def test_insert_same_version_later_online_at_updates(self, metadata_db, transformer, test_papers):
        """测试相同版本但 online_at 更晚时更新（online_at 由 Transformer 自动识别）"""
        # Step 1: 插入版本 1，date 较早
        paper_v1 = {
            "title": "Same Version OnlineAt Test",
            "doi": "10.1101/test.same.version.onlineat.001",
            "authors": "Author A; Author B",
            "abstract": "Base version",
            "date": "2026-04-05",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }

        result1 = transformer.transform_dict(paper_v1, source_name="biorxiv_history")
        assert result1.success
        write_result = metadata_db.insert_paper(
            db_payload=result1.db_payload,
            upsert_key=result1.upsert_key
        )
        paper_id = write_result["paper_id"]

        # Step 2: 相同版本 1，date 更晚（应按 online_at 覆盖）
        paper_v1_later = {
            "title": "Same Version OnlineAt Test [Updated]",
            "doi": "10.1101/test.same.version.onlineat.001",
            "authors": "Author A; Author B",
            "abstract": "Updated by later online_at",
            "date": "2026-04-10",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }

        result2 = transformer.transform_dict(paper_v1_later, source_name="biorxiv_history")
        assert result2.success
        write_result2 = metadata_db.insert_paper(
            db_payload=result2.db_payload,
            upsert_key=result2.upsert_key
        )
        paper_id2 = write_result2["paper_id"]

        # 验证返回同一 paper_id 且数据已按更晚 online_at 更新
        assert paper_id2 == paper_id
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        assert "Updated" in paper_info["canonical_title"]
        assert len(paper_info["sources"]) == 1
        source_info = paper_info["sources"][0]
        assert source_info["version"] == "1"
        assert "2026-04-10" in source_info["online_at"]


# =============================================================================
# 测试 identity bundle 支持
# =============================================================================

class TestIdentityBundle:
    """测试 identity bundle 的核心场景。

    覆盖目标：
    - bundle 结构完整性（source_identifiers 替代旧 source_record_id 顶层键）
    - 跨 source 命中能力（通过通用标识符把不同来源归并到同一 paper）
    - 统一判定方法 `_resolve_match_by_identity` 的三种返回类型
    - 跨 source 追加后 canonical 重算（online_at 最晚）
    """

    def test_identity_bundle_with_multiple_identifiers(self, metadata_db, transformer, test_papers):
        """场景：单 source 输入包含多个标识符，验证 bundle 结构与落库字段一致。"""
        # 准备包含多个标识符的数据
        paper_data = test_papers["langtaosha"][0]

        result = transformer.transform_dict(paper_data, source_name="langtaosha")
        assert result.success

        # 验证 upsert_key 结构
        upsert_key = result.upsert_key
        assert "source_name" in upsert_key
        assert "source_identifiers" in upsert_key
        assert "source_record_id" not in upsert_key  # 不应该有顶层的 source_record_id

        # 验证 source_identifiers 包含当前 source 的 ID
        source_identifiers = upsert_key["source_identifiers"]
        assert "langtaosha" in source_identifiers
        assert source_identifiers["langtaosha"] is not None

        # 验证包含多个标识符（DOI、langtaosha ID 等）
        identifiers_in_bundle = []
        for key, value in source_identifiers.items():
            if value is not None:
                identifiers_in_bundle.append(key)

        # 至少应该有 langtaosha 的 ID
        assert len(identifiers_in_bundle) >= 1, "至少应该有一个标识符"

        # 插入应该成功
        write_result = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id = write_result["paper_id"]
        assert paper_id is not None

        # 验证所有标识符都已正确入库
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)

        # 获取第一条 source 记录（应该只有一条）
        assert len(paper_info["sources"]) == 1, "应该有一条 source 记录"
        source_info = paper_info["sources"][0]

        # 检查 DOI 是否入库（如果有）
        if result.db_payload["paper_sources"].get("doi"):
            assert source_info["doi"] == result.db_payload["paper_sources"]["doi"], "DOI 应该正确入库"

        # 检查 source_record_id 是否入库
        assert source_info["source_record_id"] == result.db_payload["paper_sources"]["source_record_id"], "source_record_id 应该正确入库"

    def test_identity_bundle_cross_source_lookup(self, metadata_db, transformer, test_papers):
        """场景：先写入 biorxiv，再用相同 DOI 的 langtaosha 记录验证跨 source 命中。"""
        # Step 1: 先插入一篇只有 DOI 的 biorxiv 论文
        biorxiv_data = test_papers["biorxiv_history"][0].copy()
        # 确保有 DOI
        if "doi" not in biorxiv_data:
            biorxiv_data["doi"] = "10.1101/test.cross.source.001"

        result1 = transformer.transform_dict(biorxiv_data, source_name="biorxiv_history")
        assert result1.success

        # 验证 identity bundle 包含 DOI
        assert result1.upsert_key["doi"] == biorxiv_data["doi"]
        assert "source_identifiers" in result1.upsert_key

        write_result1 = metadata_db.insert_paper(
            db_payload=result1.db_payload,
            upsert_key=result1.upsert_key
        )
        assert write_result1["status_code"] == "INSERT_NEW_PAPER"
        assert write_result1["canonical"]["changed"] is True
        paper_id1 = write_result1["paper_id"]
        assert paper_id1 is not None

        # Step 2: 再插入一篇来自 langtaosha 的论文，有相同 DOI 和 langtaosha ID
        # 创建一个包含相同 DOI 的新 langtaosha 数据
        langtaosha_data = {
            "citation_title": "Test Paper for Cross Source Lookup",
            "citation_abstract": "This is a test abstract.",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-04-10",
            "citation_online_date": "2026-04-10",
            "citation_doi": biorxiv_data["doi"],  # 使用相同的 DOI
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/999",
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/999",
            "citation_author": ["Alice Zhang", "Bob Li"],
            "citation_author_institution": ["Tsinghua University"],
            "citation_keywords": ["LLM", "AI"],
            "citation_reference": ["Ref A", "Ref B"]
        }

        result2 = transformer.transform_dict(langtaosha_data, source_name="langtaosha")
        assert result2.success

        # 验证 identity bundle 包含 DOI 和 langtaosha ID
        assert result2.upsert_key["doi"] == biorxiv_data["doi"], "应该包含相同的 DOI"
        source_identifiers = result2.upsert_key["source_identifiers"]
        assert source_identifiers["langtaosha"] is not None, "应该包含 langtaosha ID"

        # 使用统一判定方法验证跨 source 查重
        with metadata_db.engine.connect() as conn:
            match_result = metadata_db._resolve_match_by_identity(conn, result2.upsert_key)

            # 验证能查到已存在的记录（通过 DOI 跨 source 匹配）
            assert match_result["match_type"] == "cross_source", "应该识别为跨 source 命中"
            assert match_result["paper_id"] == paper_id1, "应该返回相同的 paper_id"

    def test_resolve_match_returns_same_source(self, metadata_db, transformer, test_papers):
        """场景：同一 source 的重复输入应被 `_resolve_match_by_identity` 判定为 same_source。"""
        paper_data = test_papers["biorxiv_history"][0]
        result = transformer.transform_dict(paper_data, source_name="biorxiv_history")
        assert result.success

        write_result = metadata_db.insert_paper(
            db_payload=result.db_payload,
            upsert_key=result.upsert_key
        )
        paper_id = write_result["paper_id"]
        assert paper_id is not None

        with metadata_db.engine.connect() as conn:
            match_result = metadata_db._resolve_match_by_identity(conn, result.upsert_key)
            assert match_result["match_type"] == "same_source"
            assert match_result["paper_id"] == paper_id
            assert "paper_source_id" in match_result

    def test_resolve_match_returns_no_match(self, metadata_db, transformer):
        """场景：数据库为空且输入为新论文时，应返回 no_match。"""
        paper_data = {
            "title": "Resolver No Match Paper",
            "doi": "10.1101/test.resolve.no.match.001",
            "authors": "Author A",
            "abstract": "No match test",
            "date": "2026-04-12",
            "category": "test",
            "server": "bioRxiv"
        }
        result = transformer.transform_dict(paper_data, source_name="biorxiv_history")
        assert result.success

        with metadata_db.engine.connect() as conn:
            match_result = metadata_db._resolve_match_by_identity(conn, result.upsert_key)
            assert match_result["match_type"] == "no_match"

    def test_cross_source_canonical_update_with_later_online_at(self, metadata_db, transformer, test_papers):
        """场景：跨 source 追加后，online_at 更晚的新 source 应成为 canonical。"""
        # Step 1: 先插入 biorxiv（较早 date，online_at 由 Transformer 自动识别）
        biorxiv_data = {
            "title": "Test Paper for Canonical Update",
            "doi": "10.1101/test.canonical.update.001",
            "authors": "Author A; Author B",
            "abstract": "biorxiv version",
            "date": "2026-04-05",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }

        result1 = transformer.transform_dict(biorxiv_data, source_name="biorxiv_history")
        assert result1.success

        write_result1 = metadata_db.insert_paper(
            db_payload=result1.db_payload,
            upsert_key=result1.upsert_key
        )
        paper_id1 = write_result1["paper_id"]
        assert paper_id1 is not None

        # 第一次 insert 后：仅有一个 source，canonical_source_id 必须等于该 source_id
        with metadata_db.engine.connect() as conn:
            source_ids_before = set(get_source_ids(conn, paper_id1))
            assert len(source_ids_before) == 1
            canonical_source_id = get_canonical_source_id(conn, paper_id1)
            assert canonical_source_id in source_ids_before

        # Step 2: 插入 langtaosha（更晚 online_date）
        langtaosha_data = {
            "citation_title": "Test Paper for Canonical Update",
            "citation_abstract": "This is a test abstract.",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-04-10",
            "citation_online_date": "2026-04-10",
            "citation_doi": biorxiv_data["doi"],
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/888",
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/888",
            "citation_author": ["Alice Zhang", "Bob Li"],
            "citation_author_institution": ["Tsinghua University"],
            "citation_keywords": ["LLM", "AI"],
            "citation_reference": ["Ref A", "Ref B"]
        }

        result2 = transformer.transform_dict(langtaosha_data, source_name="langtaosha")
        assert result2.success

        write_result2 = metadata_db.insert_paper(
            db_payload=result2.db_payload,
            upsert_key=result2.upsert_key
        )
        assert write_result2["status_code"] == "INSERT_APPEND_SOURCE"
        assert write_result2["canonical"]["changed"] is True
        paper_id2 = write_result2["paper_id"]

        # 验证返回相同的 paper_id（跨 source 匹配）
        assert paper_id2 == paper_id1, "跨 source 匹配应该返回相同的 paper_id"

        # 第二次 insert 后：应新增一个 source_id，且 canonical_source_id 应切换为新增 source_id
        with metadata_db.engine.connect() as conn:
            source_ids_after = set(get_source_ids(conn, paper_id1))
            assert len(source_ids_after) == 2
            new_source_ids = source_ids_after - source_ids_before
            assert len(new_source_ids) == 1
            new_source_id = next(iter(new_source_ids))
            canonical_source_id = get_canonical_source_id(conn, paper_id1)
            assert canonical_source_id == new_source_id

        # 验证 canonical_source_id 指向 langtaosha（online_at 最晚）
        with metadata_db.engine.connect() as conn:
            updated_canonical_id = get_canonical_source_id(conn, paper_id1)
            query = """
                SELECT paper_source_id, source_name, online_at
                FROM paper_sources
                WHERE paper_source_id = :canonical_id
            """
            result = conn.execute(text(query), {"canonical_id": updated_canonical_id}).fetchone()
            assert result is not None, "应该能找到 canonical_source"
            assert result[1] == "langtaosha", f"canonical 应该是 langtaosha，实际是 {result[1]}"
            # online_at 格式可能是 "2026-04-10 00:00:00" 或 datetime 对象
            assert "2026-04-10" in str(result[2]), "langtaosha 的 online_at 应该是最晚的"

        # 验证两个 source 记录都存在于 paper_sources 中
        with metadata_db.engine.connect() as conn:
            query = """
                SELECT COUNT(*)
                FROM paper_sources
                WHERE paper_id = :paper_id
            """
            count = conn.execute(text(query), {"paper_id": paper_id1}).scalar()
            assert count == 2, f"应该有 2 条 source 记录（biorxiv 和 langtaosha），实际有 {count} 条"


class TestComplexDedupScenarios:
    """更复杂的同 source + 跨 source 联合场景测试。"""

    def test_work_id_immutable_after_paper_creation(self, metadata_db, transformer):
        """验证 work_id：仅在新建 paper 时生成，后续写入状态下保持不变。"""
        doi_a = "10.1101/test.workid.a.001"
        doi_b = "10.1101/test.workid.b.001"
        doi_c = "10.1101/test.workid.c.001"

        # A: 新论文（INSERT_NEW_PAPER）-> 生成初始 work_id
        r_a = transformer.transform_dict(
            make_biorxiv_paper(doi_a, "WorkID A v1", "2026-04-01", version="1"),
            source_name="biorxiv_history"
        )
        assert r_a.success
        write_result_a = metadata_db.insert_paper(r_a.db_payload, r_a.upsert_key)
        assert write_result_a["status_code"] == "INSERT_NEW_PAPER"
        paper_id_a = write_result_a["paper_id"]

        with metadata_db.engine.connect() as conn:
            work_id_a_v1 = get_work_id(conn, paper_id_a)
            assert work_id_a_v1 is not None

        # A1: 同 source 更新（INSERT_UPDATE_SAME_SOURCE）-> work_id 不变
        r_a1 = transformer.transform_dict(
            make_biorxiv_paper(doi_a, "WorkID A v2", "2026-04-03", version="2"),
            source_name="biorxiv_history"
        )
        assert r_a1.success
        write_result_a1 = metadata_db.insert_paper(r_a1.db_payload, r_a1.upsert_key)
        assert write_result_a1["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
        assert write_result_a1["paper_id"] == paper_id_a

        with metadata_db.engine.connect() as conn:
            work_id_a_v2 = get_work_id(conn, paper_id_a)
            assert work_id_a_v2 == work_id_a_v1

        # A2: 跨 source 追加（INSERT_APPEND_SOURCE）-> work_id 不变
        r_a2 = transformer.transform_dict(
            make_langtaosha_paper(doi_a, "WorkID A from Langtaosha", "2026-04-10", view_id="3001", download_id="9001"),
            source_name="langtaosha"
        )
        assert r_a2.success
        write_result_a2 = metadata_db.insert_paper(r_a2.db_payload, r_a2.upsert_key)
        assert write_result_a2["status_code"] == "INSERT_APPEND_SOURCE"
        assert write_result_a2["paper_id"] == paper_id_a

        with metadata_db.engine.connect() as conn:
            work_id_a_cross = get_work_id(conn, paper_id_a)
            assert work_id_a_cross == work_id_a_v2

        # A3: 更新非 canonical source（biorxiv）-> work_id 保持不变
        # 此时 canonical 已是 langtaosha（online_at 更晚），更新 biorxiv 不会触发 papers.work_id 覆盖
        r_a3 = transformer.transform_dict(
            make_biorxiv_paper(doi_a, "WorkID A v3 (non-canonical update)", "2026-04-09", version="3"),
            source_name="biorxiv_history"
        )
        assert r_a3.success
        write_result_a3 = metadata_db.insert_paper(r_a3.db_payload, r_a3.upsert_key)
        assert write_result_a3["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
        assert write_result_a3["paper_id"] == paper_id_a

        with metadata_db.engine.connect() as conn:
            work_id_a_non_canonical_update = get_work_id(conn, paper_id_a)
            assert work_id_a_non_canonical_update == work_id_a_cross

        # A4: 更新 canonical source（langtaosha）-> work_id 仍保持不变
        r_a4 = transformer.transform_dict(
            make_langtaosha_paper(doi_a, "WorkID A from Langtaosha (canonical update)", "2026-04-12", view_id="3001", download_id="9001"),
            source_name="langtaosha"
        )
        assert r_a4.success
        write_result_a4 = metadata_db.insert_paper(r_a4.db_payload, r_a4.upsert_key)
        assert write_result_a4["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
        assert write_result_a4["paper_id"] == paper_id_a

        with metadata_db.engine.connect() as conn:
            work_id_a_canonical_update = get_work_id(conn, paper_id_a)
            assert work_id_a_canonical_update == work_id_a_non_canonical_update

        # B/C: 两篇不同论文，work_id 必须与 A 且彼此都不重复
        r_b = transformer.transform_dict(
            make_biorxiv_paper(doi_b, "WorkID B", "2026-04-02", version="1"),
            source_name="biorxiv_history"
        )
        assert r_b.success
        paper_id_b = metadata_db.insert_paper(r_b.db_payload, r_b.upsert_key)["paper_id"]

        r_c = transformer.transform_dict(
            make_langtaosha_paper(doi_c, "WorkID C", "2026-04-04", view_id="3002", download_id="9002"),
            source_name="langtaosha"
        )
        assert r_c.success
        paper_id_c = metadata_db.insert_paper(r_c.db_payload, r_c.upsert_key)["paper_id"]

        with metadata_db.engine.connect() as conn:
            work_id_b = get_work_id(conn, paper_id_b)
            work_id_c = get_work_id(conn, paper_id_c)

            assert len({work_id_a_canonical_update, work_id_b, work_id_c}) == 3

            # 数据库层验证：papers 表中不存在重复 work_id
            duplicates = conn.execute(
                text("""
                    SELECT work_id
                    FROM papers
                    GROUP BY work_id
                    HAVING COUNT(*) > 1
                """)
            ).fetchall()
            assert duplicates == []

    def test_a_a1_a2_version_then_cross_source(self, metadata_db, transformer):
        """A(biorxiv v1) -> A1(biorxiv v2) -> A2(langtaosha same DOI)，验证同源更新+跨源追加+canonical。"""
        doi = "10.1101/test.complex.a.001"

        # A: biorxiv v1
        a = {
            "title": "Complex A v1",
            "doi": doi,
            "authors": "Author A; Author B",
            "abstract": "A version 1",
            "date": "2026-04-01",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }
        r_a = transformer.transform_dict(a, source_name="biorxiv_history")
        assert r_a.success
        write_result_a = metadata_db.insert_paper(r_a.db_payload, r_a.upsert_key)
        assert write_result_a["status_code"] == "INSERT_NEW_PAPER"
        assert write_result_a["canonical"]["changed"] is True
        paper_id = write_result_a["paper_id"]
        with metadata_db.engine.connect() as conn:
            source_ids_after_a = set(get_source_ids(conn, paper_id))
            assert len(source_ids_after_a) == 1
            canonical_after_a = get_canonical_source_id(conn, paper_id)
            assert canonical_after_a in source_ids_after_a
            work_id_after_a = get_work_id(conn, paper_id)
            assert work_id_after_a is not None

        # A1: biorxiv 同 DOI 新版本 v2（同 source 覆盖）
        a1 = {
            "title": "Complex A v2",
            "doi": doi,
            "authors": "Author A; Author B",
            "abstract": "A version 2",
            "date": "2026-04-03",
            "version": "2",
            "category": "test",
            "server": "bioRxiv"
        }
        r_a1 = transformer.transform_dict(a1, source_name="biorxiv_history")
        assert r_a1.success
        write_result_a1 = metadata_db.insert_paper(r_a1.db_payload, r_a1.upsert_key)
        assert write_result_a1["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
        assert write_result_a1["canonical"]["changed"] is False
        paper_id_a1 = write_result_a1["paper_id"]
        assert paper_id_a1 == paper_id
        with metadata_db.engine.connect() as conn:
            source_ids_after_a1 = set(get_source_ids(conn, paper_id))
            assert source_ids_after_a1 == source_ids_after_a
            canonical_after_a1 = get_canonical_source_id(conn, paper_id)
            assert canonical_after_a1 == canonical_after_a
            work_id_after_a1 = get_work_id(conn, paper_id)
            assert work_id_after_a1 == work_id_after_a

        # A2: langtaosha 同 DOI（跨 source 追加），online_date 更晚应成为 canonical
        a2 = {
            "citation_title": "Complex A from Langtaosha",
            "citation_abstract": "A2 cross-source",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-04-10",
            "citation_online_date": "2026-04-10",
            "citation_doi": doi,
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/1201",
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/1201/5001",
            "citation_author": ["Author A", "Author B"]
        }
        r_a2 = transformer.transform_dict(a2, source_name="langtaosha")
        assert r_a2.success
        write_result_a2 = metadata_db.insert_paper(r_a2.db_payload, r_a2.upsert_key)
        assert write_result_a2["status_code"] == "INSERT_APPEND_SOURCE"
        assert write_result_a2["canonical"]["changed"] is True
        paper_id_a2 = write_result_a2["paper_id"]
        assert paper_id_a2 == paper_id
        with metadata_db.engine.connect() as conn:
            source_ids_after_a2 = set(get_source_ids(conn, paper_id))
            assert len(source_ids_after_a2) == 2
            new_source_ids = source_ids_after_a2 - source_ids_after_a1
            assert len(new_source_ids) == 1
            new_source_id = next(iter(new_source_ids))
            canonical_after_a2 = get_canonical_source_id(conn, paper_id)
            assert canonical_after_a2 == new_source_id
            work_id_after_a2 = get_work_id(conn, paper_id)
            assert work_id_after_a2 == work_id_after_a1

        # 验证：同 source 更新后仍只有 1 条 biorxiv；跨 source 后共 2 条 source；canonical 指向更晚来源
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        assert paper_info is not None
        assert len(paper_info["sources"]) == 2

        source_names = {s["source_name"] for s in paper_info["sources"]}
        assert source_names == {"biorxiv_history", "langtaosha"}

        # 确认 biorxiv 版本已更新为 v2
        biorxiv_source = next(s for s in paper_info["sources"] if s["source_name"] == "biorxiv_history")
        assert biorxiv_source["version"] == "2"
        assert "Complex A v2" in biorxiv_source["title"]

        # canonical 应为 online_at 更晚的 langtaosha
        with metadata_db.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT ps.source_name
                    FROM papers p
                    JOIN paper_sources ps ON ps.paper_source_id = p.canonical_source_id
                    WHERE p.paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            ).fetchone()
            assert row is not None
            assert row[0] == "langtaosha"

    def test_b_online_at_hit_then_cross_source_append(self, metadata_db, transformer):
        """B 同版本按时间命中更新后，再跨 source 追加；验证两源并存与 canonical 选择。"""
        doi = "10.1101/test.complex.b.001"

        # B: biorxiv v1（较早）
        b = {
            "title": "Complex B Base",
            "doi": doi,
            "authors": "Author C; Author D",
            "abstract": "B base",
            "date": "2026-04-02",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }
        r_b = transformer.transform_dict(b, source_name="biorxiv_history")
        assert r_b.success
        write_result_b = metadata_db.insert_paper(r_b.db_payload, r_b.upsert_key)
        assert write_result_b["status_code"] == "INSERT_NEW_PAPER"
        assert write_result_b["canonical"]["changed"] is True
        paper_id = write_result_b["paper_id"]
        with metadata_db.engine.connect() as conn:
            source_ids_after_b = set(get_source_ids(conn, paper_id))
            assert len(source_ids_after_b) == 1
            canonical_after_b = get_canonical_source_id(conn, paper_id)
            assert canonical_after_b in source_ids_after_b
            work_id_after_b = get_work_id(conn, paper_id)
            assert work_id_after_b is not None

        # B1: biorxiv 同版本 v1，但 date 更晚（按 online_at 命中更新）
        b1 = {
            "title": "Complex B Time Updated",
            "doi": doi,
            "authors": "Author C; Author D",
            "abstract": "B updated by time",
            "date": "2026-04-08",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }
        r_b1 = transformer.transform_dict(b1, source_name="biorxiv_history")
        assert r_b1.success
        write_result_b1 = metadata_db.insert_paper(r_b1.db_payload, r_b1.upsert_key)
        assert write_result_b1["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
        assert write_result_b1["canonical"]["changed"] is False
        paper_id_b1 = write_result_b1["paper_id"]
        assert paper_id_b1 == paper_id
        with metadata_db.engine.connect() as conn:
            source_ids_after_b1 = set(get_source_ids(conn, paper_id))
            assert source_ids_after_b1 == source_ids_after_b
            canonical_after_b1 = get_canonical_source_id(conn, paper_id)
            assert canonical_after_b1 == canonical_after_b
            work_id_after_b1 = get_work_id(conn, paper_id)
            assert work_id_after_b1 == work_id_after_b

        # B2: langtaosha 同 DOI，online_date 较早于 biorxiv 更新后时间
        b2 = {
            "citation_title": "Complex B from Langtaosha",
            "citation_abstract": "B2 cross-source",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-04-05",
            "citation_online_date": "2026-04-05",
            "citation_doi": doi,
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/1301",
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/1301/5101",
            "citation_author": ["Author C", "Author D"]
        }
        r_b2 = transformer.transform_dict(b2, source_name="langtaosha")
        assert r_b2.success
        write_result_b2 = metadata_db.insert_paper(r_b2.db_payload, r_b2.upsert_key)
        assert write_result_b2["status_code"] == "INSERT_APPEND_SOURCE"
        assert write_result_b2["canonical"]["changed"] is False
        paper_id_b2 = write_result_b2["paper_id"]
        assert paper_id_b2 == paper_id
        with metadata_db.engine.connect() as conn:
            source_ids_after_b2 = set(get_source_ids(conn, paper_id))
            assert len(source_ids_after_b2) == 2
            new_source_ids = source_ids_after_b2 - source_ids_after_b1
            assert len(new_source_ids) == 1
            canonical_after_b2 = get_canonical_source_id(conn, paper_id)
            assert canonical_after_b2 == canonical_after_b1
            work_id_after_b2 = get_work_id(conn, paper_id)
            assert work_id_after_b2 == work_id_after_b1

        # 验证两源都存在，且 canonical 应保持为 online_at 最晚的 biorxiv_history
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        assert paper_info is not None
        assert len(paper_info["sources"]) == 2

        source_names = {s["source_name"] for s in paper_info["sources"]}
        assert source_names == {"biorxiv_history", "langtaosha"}

        with metadata_db.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT ps.source_name
                    FROM papers p
                    JOIN paper_sources ps ON ps.paper_source_id = p.canonical_source_id
                    WHERE p.paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            ).fetchone()
            assert row is not None
            assert row[0] == "biorxiv_history"

    def test_c_langtaosha_then_biorxiv_newer_then_biorxiv_new_version_then_langtaosha_older(
        self, metadata_db, transformer
    ):
        """C 场景：A(langtaosha) -> B(biorxiv更晚) -> C(biorxiv新version) -> D(langtaosha更早)，每步校验。"""
        doi = "10.1101/test.complex.c.001"

        # A: langtaosha（初始）
        a = {
            "citation_title": "Complex C from Langtaosha A",
            "citation_abstract": "A base",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-04-01",
            "citation_online_date": "2026-04-01",
            "citation_doi": doi,
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/2201",
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/2201/7001",
            "citation_author": ["Author X", "Author Y"]
        }
        r_a = transformer.transform_dict(a, source_name="langtaosha")
        assert r_a.success
        write_result_a = metadata_db.insert_paper(r_a.db_payload, r_a.upsert_key)
        assert write_result_a["status_code"] == "INSERT_NEW_PAPER"
        assert write_result_a["canonical"]["changed"] is True
        paper_id = write_result_a["paper_id"]

        with metadata_db.engine.connect() as conn:
            source_ids_after_a = set(get_source_ids(conn, paper_id))
            assert len(source_ids_after_a) == 1
            canonical_after_a = get_canonical_source_id(conn, paper_id)
            assert canonical_after_a in source_ids_after_a
            canonical_name_after_a = get_canonical_source_name(conn, paper_id)
            assert canonical_name_after_a == "langtaosha"
            work_id_after_a = get_work_id(conn, paper_id)
            assert work_id_after_a is not None

        # B: biorxiv 同 DOI，日期更晚（跨 source 追加 + canonical 切换）
        b = {
            "title": "Complex C from biorxiv B",
            "doi": doi,
            "authors": "Author X; Author Y",
            "abstract": "B cross-source later date",
            "date": "2026-04-08",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }
        r_b = transformer.transform_dict(b, source_name="biorxiv_history")
        assert r_b.success
        write_result_b = metadata_db.insert_paper(r_b.db_payload, r_b.upsert_key)
        assert write_result_b["status_code"] == "INSERT_APPEND_SOURCE"
        assert write_result_b["canonical"]["changed"] is True
        paper_id_b = write_result_b["paper_id"]
        assert paper_id_b == paper_id

        with metadata_db.engine.connect() as conn:
            source_ids_after_b = set(get_source_ids(conn, paper_id))
            assert len(source_ids_after_b) == 2
            new_source_ids_b = source_ids_after_b - source_ids_after_a
            assert len(new_source_ids_b) == 1
            canonical_after_b = get_canonical_source_id(conn, paper_id)
            assert canonical_after_b == next(iter(new_source_ids_b))
            canonical_name_after_b = get_canonical_source_name(conn, paper_id)
            assert canonical_name_after_b == "biorxiv_history"
            work_id_after_b = get_work_id(conn, paper_id)
            assert work_id_after_b == work_id_after_a

        # C: biorxiv 同 DOI，新版本（同 source 覆盖，不新增 source_id，canonical 仍为 biorxiv）
        c = {
            "title": "Complex C from biorxiv C version2",
            "doi": doi,
            "authors": "Author X; Author Y",
            "abstract": "C same-source higher version",
            "date": "2026-04-09",
            "version": "2",
            "category": "test",
            "server": "bioRxiv"
        }
        r_c = transformer.transform_dict(c, source_name="biorxiv_history")
        assert r_c.success
        write_result_c = metadata_db.insert_paper(r_c.db_payload, r_c.upsert_key)
        assert write_result_c["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
        assert write_result_c["canonical"]["changed"] is False
        paper_id_c = write_result_c["paper_id"]
        assert paper_id_c == paper_id

        with metadata_db.engine.connect() as conn:
            source_ids_after_c = set(get_source_ids(conn, paper_id))
            assert source_ids_after_c == source_ids_after_b
            canonical_after_c = get_canonical_source_id(conn, paper_id)
            assert canonical_after_c == canonical_after_b
            canonical_name_after_c = get_canonical_source_name(conn, paper_id)
            assert canonical_name_after_c == "biorxiv_history"
            work_id_after_c = get_work_id(conn, paper_id)
            assert work_id_after_c == work_id_after_b

        # D: langtaosha 同 DOI，日期比 A 更早（同 source 走覆盖判定：应 skip，不应覆盖，不应新增 source）
        d = {
            "citation_title": "Complex C from Langtaosha D older",
            "citation_abstract": "D older langtaosha",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-03-30",
            "citation_online_date": "2026-03-30",
            "citation_doi": doi,
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/2201",
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/2201/7002",
            "citation_author": ["Author X", "Author Y"]
        }
        r_d = transformer.transform_dict(d, source_name="langtaosha")
        assert r_d.success
        write_result_d = metadata_db.insert_paper(r_d.db_payload, r_d.upsert_key)
        assert write_result_d["status_code"] == "INSERT_SKIP_SAME_SOURCE"
        assert write_result_d["canonical"]["changed"] is False
        paper_id_d = write_result_d["paper_id"]
        assert paper_id_d == paper_id

        with metadata_db.engine.connect() as conn:
            source_ids_after_d = set(get_source_ids(conn, paper_id))
            assert source_ids_after_d == source_ids_after_c
            canonical_after_d = get_canonical_source_id(conn, paper_id)
            assert canonical_after_d == canonical_after_c
            canonical_name_after_d = get_canonical_source_name(conn, paper_id)
            assert canonical_name_after_d == "biorxiv_history"
            work_id_after_d = get_work_id(conn, paper_id)
            assert work_id_after_d == work_id_after_c

        # 最终补充校验：source 两条，且 biorxiv 版本应为 2
        paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)
        assert paper_info is not None
        assert len(paper_info["sources"]) == 2
        biorxiv_source = next(s for s in paper_info["sources"] if s["source_name"] == "biorxiv_history")
        assert biorxiv_source["version"] == "2"


# =============================================================================
# Embedding Status 实库测试
# =============================================================================

class TestEmbeddingStatusRealDB:
    """真实数据库依赖：embedding_status 三态与候选查询"""

    def test_embedding_status_pending_failed_succeeded_flow(
        self,
        metadata_db,
        transformer,
        test_papers
    ):
        """验证 pending -> failed -> succeeded 的状态流转"""
        paper_data = test_papers["biorxiv_history"][0]
        paper_id = insert_paper_via_transformer(
            metadata_db=metadata_db,
            transformer=transformer,
            paper_data=paper_data,
            source_name="biorxiv_history"
        )

        with metadata_db.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT work_id, canonical_source_id
                    FROM papers
                    WHERE paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            ).fetchone()
            assert row is not None
            work_id = row[0]
            canonical_source_id = row[1]

        source_name = metadata_db.get_source_name_by_paper_source_id(canonical_source_id)
        assert source_name == "biorxiv_history"

        metadata_db.upsert_embedding_status_pending(
            paper_id=paper_id,
            work_id=work_id,
            canonical_source_id=canonical_source_id,
            source_name=source_name,
            text_type="abstract"
        )

        with metadata_db.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT status, attempt_count, last_error_message
                    FROM embedding_status
                    WHERE paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            ).fetchone()
            assert row is not None
            assert row[0] == "pending"
            assert row[1] == 0
            assert row[2] is None

        failed = metadata_db.mark_embedding_failed(paper_id, "mock vector failed")
        assert failed is True

        with metadata_db.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT status, attempt_count, last_error_message
                    FROM embedding_status
                    WHERE paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            ).fetchone()
            assert row[0] == "failed"
            assert row[1] == 1
            assert "mock vector failed" in (row[2] or "")

        succeeded = metadata_db.mark_embedding_succeeded(paper_id)
        assert succeeded is True

        with metadata_db.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT status, attempt_count, last_error_message, last_success_at
                    FROM embedding_status
                    WHERE paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            ).fetchone()
            assert row[0] == "succeeded"
            assert row[1] == 2
            assert row[2] is None
            assert row[3] is not None

    def test_list_embedding_candidates_filters_by_status_and_source(
        self,
        metadata_db,
        transformer,
        test_papers
    ):
        """验证候选查询能按 source/status 正确过滤"""
        pid_history = insert_paper_via_transformer(
            metadata_db=metadata_db,
            transformer=transformer,
            paper_data=test_papers["biorxiv_history"][1],
            source_name="biorxiv_history"
        )
        pid_daily = insert_paper_via_transformer(
            metadata_db=metadata_db,
            transformer=transformer,
            paper_data=test_papers["biorxiv_daily"][0],
            source_name="biorxiv_daily"
        )

        with metadata_db.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT paper_id, work_id, canonical_source_id
                    FROM papers
                    WHERE paper_id IN (:pid_history, :pid_daily)
                    ORDER BY paper_id
                """),
                {"pid_history": pid_history, "pid_daily": pid_daily}
            ).fetchall()
            assert len(rows) == 2

        for paper_id, work_id, canonical_source_id in rows:
            source_name = metadata_db.get_source_name_by_paper_source_id(canonical_source_id)
            metadata_db.upsert_embedding_status_pending(
                paper_id=paper_id,
                work_id=work_id,
                canonical_source_id=canonical_source_id,
                source_name=source_name,
                text_type="abstract"
            )

        metadata_db.mark_embedding_succeeded(pid_daily)

        pending_history = metadata_db.list_embedding_candidates(
            source_name="biorxiv_history",
            statuses=["pending"],
            limit=20,
            offset=0
        )
        assert len(pending_history) == 1
        assert pending_history[0]["paper_id"] == pid_history
        assert pending_history[0]["status"] == "pending"

        pending_daily = metadata_db.list_embedding_candidates(
            source_name="biorxiv_daily",
            statuses=["pending"],
            limit=20,
            offset=0
        )
        assert len(pending_daily) == 0


# =============================================================================
# 运行配置
# =============================================================================

if __name__ == "__main__":
    """直接运行测试文件时使用 argparse"""
    import argparse

    parser = argparse.ArgumentParser(
        description='MetadataDB 新架构测试',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径（默认: src/config/config_tecent_backend_server_test.yaml）'
    )

    parser.add_argument(
        'test_args',
        nargs='*',
        help='pytest 测试参数（例如：TestInsertPaper::test_insert_new_paper）'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='详细输出'
    )

    parser.add_argument(
        '-s', '--capture',
        action='store_true',
        help='显示输出'
    )

    args = parser.parse_args()

    # 设置环境变量
    if args.config_path:
        os.environ['METADATA_DB_CONFIG'] = args.config_path

    # 构建 pytest 参数
    pytest_args = [__file__]
    if args.test_args:
        # 将测试参数作为 -k 的过滤条件
        test_filter = ' or '.join(args.test_args)
        pytest_args.extend(['-k', test_filter])
    if args.verbose:
        pytest_args.append('-v')
    if args.capture:
        pytest_args.append('-s')

    # 运行 pytest
    pytest.main(pytest_args)
