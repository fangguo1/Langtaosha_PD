"""测试新的 PaperIndexer

运行方式：
    pytest tests/indexing/test_paper_indexer.py -v
    pytest tests/indexing/test_paper_indexer.py::TestPaperIndexerInit -v
    pytest tests/indexing/test_paper_indexer.py -m integration -v

配置文件：
    默认: src/config/config_tecent_backend_server_test.yaml
    环境: PAPER_INDEXER_CONFIG=<path>
    命令行: --config-path=<path>
"""

import pytest
import logging
import argparse
import os
import uuid
from pathlib import Path
from typing import Dict, Any, List
from unittest.mock import Mock, patch
from sqlalchemy import text
import time

# 添加项目根目录到路径
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.docset_hub.indexing import PaperIndexer
from src.docset_hub.storage.vector_db import VectorDB
from src.docset_hub.metadata.transformer import MetadataTransformer
from src.config import (
    init_config,
    _reset_config,
    get_default_sources,
    load_config_from_yaml,
    get_db_engine,
)

# 配置 logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# =============================================================================
# 配置文件处理（参考 test_vector_db 和 test_metadata_db）
# =============================================================================

_global_config_path = None


def get_config_path_from_args() -> Path:
    """从命令行参数或环境变量获取配置文件路径

    优先级：
        1. 命令行参数 --config-path
        2. 环境变量 PAPER_INDEXER_CONFIG
        3. 默认使用 config_tecent_backend_server_test.yaml

    Returns:
        Path: 配置文件路径
    """
    global _global_config_path

    if _global_config_path:
        return _global_config_path

    config_path = None

    # 1. 检查命令行参数
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
    if config_path is None and 'PAPER_INDEXER_CONFIG' in os.environ:
        config_path = Path(os.environ['PAPER_INDEXER_CONFIG'])

    # 3. 使用默认配置文件
    if config_path is None:
        current_path = Path(__file__).resolve()
        project_root = current_path
        for parent in [current_path] + list(current_path.parents):
            if (parent / 'pyproject.toml').exists() or (parent / '.git').exists() or (parent / 'src').exists():
                project_root = parent
                break
        config_path = project_root / 'src' / 'config' / 'config_tecent_backend_server_test.yaml'

    if not config_path.exists():
        raise ValueError(f"❌ 配置文件不存在: {config_path}")

    _global_config_path = config_path
    return config_path


# =============================================================================
# Pytest Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def config_path():
    """配置文件路径（session 级别）"""
    path = get_config_path_from_args()
    print(f"\n✅ 使用配置文件: {path}")
    return path


@pytest.fixture(scope="session", autouse=True)
def clean_vector_db_before_tests(config_path):
    """测试开始前根据 yaml 配置清空向量库中的目标集合。"""
    cfg = load_config_from_yaml(config_path)
    vector_cfg = cfg.get("vector_db", {}) or {}
    database = vector_cfg.get("database")
    collection_prefix = vector_cfg.get("collection_prefix", "lt_")

    if not database:
        print("\n⚠️ 跳过向量库预清理：yaml 中未配置 vector_db.database")
        return

    vector_db = VectorDB(config_path=config_path)
    try:
        collections = vector_db.client.list_collections(database)
    except Exception as e:
        print(f"\n⚠️ 向量库预清理失败（无法列出集合）: {e}")
        return

    target_collections = [
        collection for collection in collections
        if isinstance(collection, str) and collection.startswith(collection_prefix)
    ]

    print(f"\n🧹 预清理向量库: database={database}, collections={target_collections}")
    for collection in target_collections:
        try:
            vector_db.client.drop_collection(database=database, collection=collection)
            print(f"  ✅ 已删除 collection: {collection}")
        except Exception as e:
            print(f"  ⚠️ 删除 collection 失败: {collection}, error={e}")


@pytest.fixture(scope="session")
def db_engine(config_path):
    """metadata_db 引擎（session 级别）。"""
    _reset_config()
    init_config(config_path)
    return get_db_engine(db_key='metadata_db')


@pytest.fixture(scope="function")
def clean_db(db_engine):
    """每个测试前清理 metadata_db。"""
    with db_engine.connect() as conn:
        conn.execute(text("DELETE FROM embedding_status"))
        conn.execute(text("DELETE FROM paper_source_metadata WHERE paper_source_id IN (SELECT paper_source_id FROM paper_sources)"))
        conn.execute(text("DELETE FROM paper_author_affiliation"))
        conn.execute(text("DELETE FROM paper_keywords"))
        conn.execute(text("DELETE FROM paper_references"))
        conn.execute(text("DELETE FROM paper_sources"))
        conn.execute(text("DELETE FROM papers"))
        conn.commit()

    yield db_engine


@pytest.fixture(scope="function")
def indexer(config_path, clean_db):
    """PaperIndexer 实例（每个测试函数创建新实例）"""
    # 重置配置缓存
    _reset_config()
    init_config(config_path)

    # 创建 indexer 实例
    indexer = PaperIndexer(
        config_path=config_path,
        enable_vectorization=True
    )

    yield indexer

    # 清理（如果需要）


@pytest.fixture(scope="session")
def transformer():
    """MetadataTransformer 实例（session 级别）"""
    return MetadataTransformer()


# =============================================================================
# 阶段 2: 初始化测试
# =============================================================================

class TestPaperIndexerInit:
    """测试 PaperIndexer 初始化"""

    def test_init_with_valid_config(self, config_path):
        """测试使用有效配置文件初始化"""
        _reset_config()
        indexer = PaperIndexer(config_path=config_path)

        assert indexer is not None
        assert indexer.config_path == config_path
        assert indexer.enable_vectorization is True
        assert isinstance(indexer.default_sources, list)
        assert len(indexer.default_sources) > 0
        print(f"✅ 初始化成功，default_sources={indexer.default_sources}")

    def test_init_with_invalid_config_path(self):
        """测试使用无效配置文件路径初始化"""
        invalid_path = Path("/nonexistent/config.yaml")

        with pytest.raises(ValueError, match="配置文件不存在"):
            PaperIndexer(config_path=invalid_path)

    def test_init_reads_default_sources(self, indexer):
        """测试初始化时正确读取 default_sources"""
        assert isinstance(indexer.default_sources, list)
        assert len(indexer.default_sources) > 0

        # 验证包含预期的 sources
        assert 'langtaosha' in indexer.default_sources
        assert 'biorxiv_history' in indexer.default_sources
        assert 'biorxiv_daily' in indexer.default_sources

    def test_init_with_vectorization_enabled(self, config_path):
        """测试启用向量化初始化"""
        _reset_config()
        indexer = PaperIndexer(
            config_path=config_path,
            enable_vectorization=True
        )

        assert indexer.enable_vectorization is True
        assert indexer.vector_db is not None

    def test_init_with_vectorization_disabled(self, config_path):
        """测试禁用向量化初始化"""
        _reset_config()
        indexer = PaperIndexer(
            config_path=config_path,
            enable_vectorization=False
        )

        assert indexer.enable_vectorization is False
        assert indexer.vector_db is None


# =============================================================================
# 阶段 3: Source 解析测试
# =============================================================================

class TestSourceResolution:
    """测试 source 解析逻辑"""

    def test_resolve_source_name_explicit_valid(self, indexer):
        """测试显式指定有效的 source_name"""
        result = indexer._resolve_source_name('langtaosha')
        assert result == 'langtaosha'

        result = indexer._resolve_source_name('biorxiv_history')
        assert result == 'biorxiv_history'

        result = indexer._resolve_source_name('biorxiv_daily')
        assert result == 'biorxiv_daily'

    def test_resolve_source_name_explicit_invalid(self, indexer):
        """测试显式指定无效的 source_name"""
        with pytest.raises(ValueError, match="不在 default_sources 中"):
            indexer._resolve_source_name('invalid_source')

    def test_resolve_source_list_with_valid_sources(self, indexer):
        """测试指定有效的 source_list"""
        result = indexer._resolve_source_list(['langtaosha', 'biorxiv_daily'])
        assert result == ['langtaosha', 'biorxiv_daily']

    def test_resolve_source_list_with_invalid_sources(self, indexer):
        """测试指定无效的 source_list"""
        with pytest.raises(ValueError, match="不在 default_sources 中"):
            indexer._resolve_source_list(['langtaosha', 'invalid_source'])

    def test_resolve_source_list_uses_defaults(self, indexer):
        """测试未指定 source_list 时使用默认值"""
        result = indexer._resolve_source_list(None)
        assert result == indexer.default_sources


# =============================================================================
# 阶段 4: 核心方法测试（使用 Mock）
# =============================================================================

class TestIndexDict:
    """测试 index_dict 方法"""

    @pytest.fixture
    def mock_indexer(self, config_path):
        """创建使用 mock 的 indexer"""
        _reset_config()
        init_config(config_path)

        # 创建真实的 indexer
        indexer = PaperIndexer(
            config_path=config_path,
            enable_vectorization=False  # 禁用向量化，简化测试
        )

        return indexer

    def test_index_dict_langtaosha_success(self, mock_indexer, test_papers):
        """测试索引 langtaosha 论文成功"""
        paper = test_papers['langtaosha'][0]

        result = mock_indexer.index_dict(
            raw_payload=paper,
            source_name='langtaosha',
            mode='insert'
        )

        assert result['success'] is True
        assert result['source_name'] == 'langtaosha'
        assert result['mode'] == 'insert'
        assert 'work_id' in result
        assert 'paper_id' in result
        print(f"✅ 索引成功: work_id={result['work_id']}, paper_id={result['paper_id']}")

    def test_index_dict_biorxiv_history_success(self, mock_indexer, test_papers):
        """测试索引 biorxiv_history 论文成功"""
        paper = test_papers['biorxiv_history'][0]

        result = mock_indexer.index_dict(
            raw_payload=paper,
            source_name='biorxiv_history',
            mode='insert'
        )

        assert result['success'] is True
        assert result['source_name'] == 'biorxiv_history'
        print(f"✅ biorxiv_history 索引成功: work_id={result['work_id']}")

    def test_index_dict_biorxiv_daily_success(self, mock_indexer, test_papers):
        """测试索引 biorxiv_daily 论文成功（重要）"""
        paper = test_papers['biorxiv_daily'][0]

        result = mock_indexer.index_dict(
            raw_payload=paper,
            source_name='biorxiv_daily',
            mode='insert'
        )

        assert result['success'] is True
        assert result['source_name'] == 'biorxiv_daily'
        print(f"✅ biorxiv_daily 索引成功: work_id={result['work_id']}")

    def test_index_dict_invalid_source(self, mock_indexer, test_papers):
        """测试索引时指定无效 source"""
        paper = test_papers['langtaosha'][0]

        result = mock_indexer.index_dict(
            raw_payload=paper,
            source_name='invalid_source',
            mode='insert'
        )

        assert result['success'] is False
        assert 'error' in result


class TestIndexFile:
    """测试 index_file 方法"""

    def test_index_file_langtaosha_success(self, indexer, test_paper_files):
        """测试索引 langtaosha 文件成功"""
        # 使用测试数据目录中的实际文件
        langtaosha_file = test_paper_files["langtaosha"][0]

        result = indexer.index_file(
            input_path=langtaosha_file,
            source_name='langtaosha',
            mode='insert'
        )

        assert result['success'] is True
        assert result['source_name'] == 'langtaosha'
        print(f"✅ 文件索引成功: {langtaosha_file.name}")

    def test_index_file_biorxiv_history_success(self, indexer, test_paper_files):
        """测试索引 biorxiv_history 文件成功"""
        biorxiv_file = test_paper_files["biorxiv_history"][0]

        result = indexer.index_file(
            input_path=biorxiv_file,
            source_name='biorxiv_history',
            mode='insert'
        )

        assert result['success'] is True
        assert result['source_name'] == 'biorxiv_history'

    def test_index_file_biorxiv_daily_success(self, indexer, test_paper_files):
        """测试索引 biorxiv_daily 文件成功（重要）"""
        biorxiv_file = test_paper_files["biorxiv_daily"][0]

        result = indexer.index_file(
            input_path=biorxiv_file,
            source_name='biorxiv_daily',
            mode='insert'
        )

        assert result['success'] is True
        assert result['source_name'] == 'biorxiv_daily'
        print(f"✅ biorxiv_daily 文件索引成功: {biorxiv_file.name}")

    def test_index_file_not_found(self, indexer):
        """测试索引不存在的文件"""
        result = indexer.index_file(
            input_path="/nonexistent/file.json",
            source_name='langtaosha'
        )

        assert result['success'] is False
        assert 'error' in result


class TestRead:
    """测试 read 方法"""

    def test_read_by_work_id(self, indexer, test_papers):
        """测试通过 work_id 读取论文"""
        # 先索引一篇论文
        paper = test_papers['langtaosha'][0]
        index_result = indexer.index_dict(
            raw_payload=paper,
            source_name='langtaosha',
            mode='insert'
        )

        assert index_result['success'] is True
        work_id = index_result['work_id']

        # 读取论文
        paper_info = indexer.read(work_id=work_id)

        assert paper_info is not None
        assert paper_info['work_id'] == work_id
        print(f"✅ 读取成功: work_id={work_id}")

    def test_read_nonexistent_work_id(self, indexer):
        """测试读取不存在的 work_id"""
        paper_info = indexer.read(work_id="Wnonexistent")
        assert paper_info is None

    def test_read_without_parameters_raises_error(self, indexer):
        """测试不提供参数时抛出错误"""
        with pytest.raises(ValueError, match="必须提供 work_id 或 paper_id 之一"):
            indexer.read()


# =============================================================================
# 阶段 5-6: 集成测试（统一索引，统一清理）
# =============================================================================

@pytest.mark.integration
class TestPaperIndexerIntegrated:
    """集成测试 - 在类开始时统一索引所有数据，结束时统一清理并验证

    设计原则：
        1. setup_class: 预先索引所有测试论文（每个 source 2-3 篇）
        2. 各测试方法: 直接使用预索引的数据，无需重复索引
        3. teardown_class: 统一删除所有数据，验证清理成功
    """

    @pytest.fixture(scope="class", autouse=True)
    def integrated_suite(self, request, config_path, test_papers):
        """测试类初始化：索引所有测试论文，并在类结束后统一清理。"""
        print("\n" + "="*70)
        print("🔄 TestPaperIndexerIntegrated - 开始预索引测试数据")
        print("="*70)

        # 初始化配置和 indexer
        _reset_config()
        init_config(config_path)

        request.cls.indexer = PaperIndexer(
            config_path=config_path,
            enable_vectorization=True
        )

        # 存储所有 work_ids
        request.cls.indexed_work_ids = {
            "langtaosha": [],
            "biorxiv_history": [],
            "biorxiv_daily": [],
            "all": []
        }

        # 为每个 source 索引 2-3 篇论文
        index_plan = [
            ('langtaosha', 3),
            ('biorxiv_history', 3),
            ('biorxiv_daily', 3)
        ]

        for source_name, count in index_plan:
            print(f"\n📥 索引 {source_name} ({count} 篇)...")
            for i in range(count):
                paper = test_papers[source_name][i]
                result = request.cls.indexer.index_dict(
                    raw_payload=paper,
                    source_name=source_name,
                    mode='insert'
                )

                if result['success']:
                    work_id_tuple = (result['work_id'], source_name)
                    request.cls.indexed_work_ids[source_name].append(work_id_tuple)
                    request.cls.indexed_work_ids["all"].append(work_id_tuple)
                    print(f"  ✅ [{i+1}/{count}] {result['work_id']}")
                else:
                    print(f"  ❌ [{i+1}/{count}] 索引失败: {result.get('error', 'Unknown error')}")

        # 等待向量化完成
        import time
        print(f"\n⏳ 等待向量化完成...")
        time.sleep(3)

        total_count = len(request.cls.indexed_work_ids["all"])
        print(f"\n✅ 预索引完成: 共 {total_count} 篇论文")
        print(f"   - langtaosha: {len(request.cls.indexed_work_ids['langtaosha'])} 篇")
        print(f"   - biorxiv_history: {len(request.cls.indexed_work_ids['biorxiv_history'])} 篇")
        print(f"   - biorxiv_daily: {len(request.cls.indexed_work_ids['biorxiv_daily'])} 篇")
        print("="*70 + "\n")

        yield

        cls = request.cls
        print("\n" + "="*70)
        print("🗑️  TestPaperIndexerIntegrated - 开始清理测试数据")
        print("="*70)

        remaining_count = len(cls.indexed_work_ids["all"])
        print(f"\n📊 剩余待清理数据: {remaining_count} 篇")
        print(f"   - langtaosha: {len(cls.indexed_work_ids['langtaosha'])} 篇")
        print(f"   - biorxiv_history: {len(cls.indexed_work_ids['biorxiv_history'])} 篇")
        print(f"   - biorxiv_daily: {len(cls.indexed_work_ids['biorxiv_daily'])} 篇")

        delete_failures = []
        for work_id, source_name in cls.indexed_work_ids["all"]:
            try:
                delete_result = cls.indexer.delete(
                    work_id=work_id,
                    source_name=source_name
                )
                if delete_result['success']:
                    print(f"  ✅ 清理成功: {work_id} ({source_name})")
                else:
                    error_msg = delete_result.get('error', 'Unknown error')
                    print(f"  ❌ 清理失败: {work_id} ({source_name}) - {error_msg}")
                    delete_failures.append((work_id, source_name, error_msg))
            except Exception as e:
                print(f"  ❌ 清理异常: {work_id} ({source_name}) - {e}")
                delete_failures.append((work_id, source_name, str(e)))

        print(f"\n🔍 验证清理结果...")

        verification_failures = []
        for work_id, source_name in cls.indexed_work_ids["all"]:
            paper_info = cls.indexer.read(work_id=work_id)
            if paper_info is not None:
                print(f"  ❌ 验证失败: {work_id} 仍然存在")
                verification_failures.append(work_id)
            else:
                print(f"  ✅ 验证成功: {work_id} 已删除")

        print("\n" + "="*70)
        if delete_failures or verification_failures:
            print(f"⚠️  清理完成，但有失败:")
            print(f"   - 删除失败: {len(delete_failures)} 个")
            print(f"   - 验证失败: {len(verification_failures)} 个")
            print("="*70 + "\n")
        else:
            print(f"✅ 清理完成: 所有 {remaining_count} 篇论文已成功删除并验证")
            print("="*70 + "\n")

    # =========================================================================
    # 测试方法：直接使用 cls.indexed_work_ids 中的预索引数据
    # =========================================================================

    def test_multi_sources_indexed(self):
        """测试所有 source 都已成功索引"""
        assert len(self.indexed_work_ids["all"]) > 0
        assert len(self.indexed_work_ids["langtaosha"]) == 3
        assert len(self.indexed_work_ids["biorxiv_history"]) == 3
        assert len(self.indexed_work_ids["biorxiv_daily"]) == 3
        print(f"✅ 多 source 索引验证通过")

    def test_read_indexed_papers(self):
        """测试读取已索引的论文"""
        # 验证每个 source 至少能读取 2 篇
        for source_name in ['langtaosha', 'biorxiv_history', 'biorxiv_daily']:
            work_ids = self.indexed_work_ids[source_name][:2]  # 取前 2 篇
            for work_id, _ in work_ids:
                paper_info = self.indexer.read(work_id=work_id)
                assert paper_info is not None
                assert paper_info['work_id'] == work_id
        print(f"✅ 读取验证通过: 所有索引的论文均可读取")

    def test_search_by_query(self):
        """测试通过查询文本搜索论文"""
        search_results = self.indexer.search(
            query="machine learning",
            top_k=5,
            hydrate=True
        )

        assert isinstance(search_results, list)
        assert len(search_results) > 0

        # 验证结果结构
        for result in search_results[:3]:
            assert 'work_id' in result
            assert 'source_name' in result
            assert 'similarity' in result
            assert 'metadata' in result
            assert isinstance(result['similarity'], float)
            assert 0 <= result['similarity'] <= 1

        print(f"✅ 搜索测试通过: 找到 {len(search_results)} 个结果")

    def test_search_with_source_filter(self):
        """测试带 source 过滤的搜索"""
        # 搜索特定 source
        search_results = self.indexer.search(
            query="research",
            source_list=['biorxiv_daily'],
            top_k=10,
            hydrate=False
        )

        # 验证结果只包含指定的 source
        for result in search_results:
            assert result['source_name'] == 'biorxiv_daily'

        print(f"✅ Source 过滤测试通过: 找到 {len(search_results)} 个 biorxiv_daily 结果")

    def test_search_without_hydrate(self):
        """测试不补全 metadata 的轻量级搜索"""
        search_results = self.indexer.search(
            query="research",
            top_k=5,
            hydrate=False
        )

        # 验证结果不包含完整 metadata
        assert len(search_results) > 0
        for result in search_results:
            assert 'work_id' in result
            assert 'similarity' in result
            assert 'metadata' not in result  # 没有 metadata 字段

        print(f"✅ 轻量级搜索测试通过")

    def test_search_different_queries(self):
        """测试不同查询的搜索结果"""
        test_queries = [
            "machine learning",
            "virus",
            "genomics"
        ]

        for query in test_queries:
            search_results = self.indexer.search(
                query=query,
                top_k=3,
                hydrate=True
            )
            print(f"  查询 '{query}': 找到 {len(search_results)} 个结果")

        print(f"✅ 多查询搜索测试通过")

    def test_delete_by_work_id(self):
        """测试删除单篇论文"""
        # 从 langtaosha 中取第一个 work_id 进行删除测试
        work_id, source_name = self.indexed_work_ids['langtaosha'][0]

        # 验证论文存在
        paper_info = self.indexer.read(work_id=work_id)
        assert paper_info is not None
        print(f"✅ 论文存在: {work_id}")

        # 删除论文
        delete_result = self.indexer.delete(
            work_id=work_id,
            source_name=source_name
        )

        assert delete_result['success'] is True
        assert delete_result['work_id'] == work_id
        assert delete_result['source_name'] == source_name
        assert 'metadata_deleted' in delete_result
        assert 'vector_deleted' in delete_result

        print(f"✅ 删除成功: metadata_deleted={delete_result['metadata_deleted']}, "
              f"vector_deleted={delete_result['vector_deleted']}")

        # 验证论文已删除
        paper_info = self.indexer.read(work_id=work_id)
        assert paper_info is None
        print(f"✅ 论文已确认删除: {work_id}")

        # 从列表中移除（已手动删除，teardown 时不需要再删除）
        self.indexed_work_ids['langtaosha'].remove((work_id, source_name))
        self.indexed_work_ids['all'].remove((work_id, source_name))

    def test_delete_multi_sources(self):
        """测试删除多个 source 的论文"""
        # 从每个 source 删除 1 篇
        sources_to_delete = ['biorxiv_history', 'biorxiv_daily']

        for source_name in sources_to_delete:
            # 取第一个 work_id
            if not self.indexed_work_ids[source_name]:
                continue

            work_id, _ = self.indexed_work_ids[source_name][0]

            delete_result = self.indexer.delete(
                work_id=work_id,
                source_name=source_name
            )

            assert delete_result['success'] is True
            assert delete_result['work_id'] == work_id

            # 验证删除成功
            paper_info = self.indexer.read(work_id=work_id)
            assert paper_info is None

            print(f"✅ 删除成功: {work_id} ({source_name})")

            # 从列表中移除
            self.indexed_work_ids[source_name].remove((work_id, source_name))
            self.indexed_work_ids['all'].remove((work_id, source_name))

# =============================================================================
# 阶段 7: MetadataTransformer 集成测试
# =============================================================================

class TestMetadataTransformerIntegration:
    """测试 MetadataTransformer 与 PaperIndexer 的集成"""

    def test_transformer_langtaosha(self, transformer, test_paper_files):
        """测试 MetadataTransformer 处理 langtaosha 数据"""
        file_path = test_paper_files['langtaosha'][0]

        result = transformer.transform_file(
            input_path=file_path,
            source_name='langtaosha'
        )

        assert result.success is True
        assert result.db_payload is not None
        assert result.work_id is None
        assert 'papers' in result.db_payload
        assert 'paper_sources' in result.db_payload
        print(f"✅ Transformer langtaosha 转换成功: work_id={result.work_id}")

    def test_transformer_biorxiv_history(self, transformer, test_paper_files):
        """测试 MetadataTransformer 处理 biorxiv_history 数据"""
        file_path = test_paper_files['biorxiv_history'][0]

        result = transformer.transform_file(
            input_path=file_path,
            source_name='biorxiv_history'
        )

        assert result.success is True
        assert result.db_payload is not None
        assert result.work_id is None
        print(f"✅ Transformer biorxiv_history 转换成功: work_id={result.work_id}")

    def test_transformer_biorxiv_daily(self, transformer, test_paper_files):
        """测试 MetadataTransformer 处理 biorxiv_daily 数据（重要）"""
        file_path = test_paper_files['biorxiv_daily'][0]

        result = transformer.transform_file(
            input_path=file_path,
            source_name='biorxiv_daily'
        )

        assert result.success is True
        assert result.db_payload is not None
        assert result.work_id is None
        print(f"✅ Transformer biorxiv_daily 转换成功: work_id={result.work_id}")

    @pytest.mark.slow
    def test_transformer_with_indexer_integration(self, indexer, test_paper_files):
        """测试 Transformer -> Indexer 完整流程

        这个测试验证：
        1. Transformer 可以正确转换文件
        2. Indexer 可以正确索引文件（内部会调用 Transformer）
        3. 索引后的数据可以通过 work_id 读取
        """
        added_work_ids = []

        try:
            # 测试 langtaosha
            file_path = test_paper_files['langtaosha'][0]

            # 使用 Indexer 索引文件（内部会调用 Transformer）
            index_result = indexer.index_file(
                input_path=file_path,
                source_name='langtaosha',
                mode='insert'
            )
            assert index_result['success'] is True
            work_id = index_result['work_id']
            added_work_ids.append((work_id, 'langtaosha'))

            # 验证：可以通过 work_id 读取论文
            paper_info = indexer.read(work_id=work_id)
            assert paper_info is not None
            assert paper_info['work_id'] == work_id

            print(f"✅ Transformer -> Indexer 集成测试通过: work_id={work_id}")

            # 测试 biorxiv_daily
            file_path = test_paper_files['biorxiv_daily'][0]
            index_result = indexer.index_file(
                input_path=file_path,
                source_name='biorxiv_daily',
                mode='insert'
            )
            assert index_result['success'] is True
            work_id = index_result['work_id']
            added_work_ids.append((work_id, 'biorxiv_daily'))

            # 验证：可以通过 work_id 读取论文
            paper_info = indexer.read(work_id=work_id)
            assert paper_info is not None
            assert paper_info['work_id'] == work_id

            print(f"✅ biorxiv_daily 集成测试通过: work_id={work_id}")

        finally:
            # 清理
            for work_id, source_name in added_work_ids:
                try:
                    indexer.delete(work_id=work_id, source_name=source_name)
                    print(f"🗑️  清理成功: {work_id}")
                except Exception as e:
                    print(f"⚠️  清理失败 {work_id}: {e}")


# =============================================================================
# 阶段 8: 真实数据库依赖测试（insert-only + embedding_status）
# =============================================================================

@pytest.mark.integration
class TestPaperIndexerRealDBEmbeddingStatus:
    """基于真实 PostgreSQL 的 PaperIndexer 行为测试"""

    def _get_embedding_status_row(self, indexer, paper_id: int):
        with indexer.metadata_db.engine.connect() as conn:
            return conn.execute(
                text("""
                    SELECT status, attempt_count, source_name, canonical_source_id, text_type
                    FROM embedding_status
                    WHERE paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            ).fetchone()

    def _get_embedding_status_counts(self, indexer):
        with indexer.metadata_db.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT status, COUNT(*)
                    FROM embedding_status
                    GROUP BY status
                """)
            ).fetchall()
            counts = {row[0]: int(row[1]) for row in rows}
            return {
                "pending": counts.get("pending", 0),
                "succeeded": counts.get("succeeded", 0),
                "failed": counts.get("failed", 0),
            }

    def _get_collection_doc_count(self, indexer, source_name: str) -> int:
        collection_name = indexer.vector_db._get_collection_name(source_name)
        database = indexer.vector_db.database

        collections = indexer.vector_db.client.list_collections(database)
        if collection_name not in collections:
            return 0

        info = indexer.vector_db.client.describe_collection(database=database, collection=collection_name) or {}
        print(info)
        if info.get("documentCount") is not None:
            return int(info.get("documentCount"))
        for key in ("document_count", "documentNum", "docCount", "count"):
            if info.get(key) is not None:
                return int(info.get(key))
        return 0

    def _get_vector_document(self, indexer, source_name: str, work_id: str, text_type: str = "abstract"):
        collection_name = indexer.vector_db._get_collection_name(source_name)
        doc_id = indexer.vector_db._generate_doc_id(source_name, work_id, text_type)
        result = indexer.vector_db.client.query_documents(
            database=indexer.vector_db.database,
            collection=collection_name,
            ids=[doc_id],
            output_fields=["id", "work_id", "paper_id", "source_name", "text_type", "text"],
            limit=1,
            read_consistency="strongConsistency"
        )
        docs = result.get("documents", [])
        return docs[0] if docs else None

    def test_insert_mode_and_four_status_vectorization_matrix(self, indexer):
        """复杂场景：验证四种 insert status 下向量写入与 embedding_status 更新是否正确。"""
        uid = uuid.uuid4().hex[:12]
        doi = f"10.1101/test.paper_indexer.matrix.{uid}"
        langtaosha_view_id = str(100000 + (int(uid[:6], 16) % 800000))
        langtaosha_download_id = str(1000 + (int(uid[6:10], 16) % 8000))
        work_id = None
        paper_id = None
        seen_work_ids = set()

        # Step 1: INSERT_NEW_PAPER（应向量化，写入 biorxiv_history）
        a = {
            "title": "Indexer Matrix A v1",
            "doi": doi,
            "authors": "Author A; Author B",
            "abstract": "A version 1",
            "date": "2026-04-01",
            "version": "1",
            "category": "test",
            "server": "bioRxiv"
        }

        # Step 2: INSERT_UPDATE_SAME_SOURCE（应更新向量）
        a1 = {
            "title": "Indexer Matrix A v2",
            "doi": doi,
            "authors": "Author A; Author B",
            "abstract": "A version 2",
            "date": "2026-04-03",
            "version": "2",
            "category": "test",
            "server": "bioRxiv"
        }

        # Step 3: INSERT_APPEND_SOURCE（canonical 切到 langtaosha，应向量化）
        a2 = {
            "citation_title": "Indexer Matrix A from Langtaosha version1",
            "citation_abstract": "A2 cross-source",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-04-10",
            "citation_online_date": "2026-04-10",
            "citation_doi": doi,
            "citation_abstract_html_url": f"https://langtaosha.org.cn/lts/en/preprint/view/{langtaosha_view_id}",
            "citation_pdf_url": f"https://langtaosha.org.cn/lts/en/preprint/download/{langtaosha_view_id}/{langtaosha_download_id}",
            "citation_author": ["Author A", "Author B"],
            "citation_author_institution": ["Test University"],
            "citation_keywords": ["LLM", "AI"],
            "citation_reference": ["Ref A", "Ref B"]
        }

        # Step 4: INSERT_SKIP_SAME_SOURCE（canonical 不变，不应二次向量化）
        a3 = {
            "citation_title": "Indexer Matrix A from Langtaosha version2",
            "citation_abstract": "A2 cross-source",
            "citation_language": "en",
            "citation_publisher": "Langtaosha",
            "citation_date": "2026-04-09",
            "citation_online_date": "2026-04-09",
            "citation_doi": doi,
            "citation_abstract_html_url": f"https://langtaosha.org.cn/lts/en/preprint/view/{langtaosha_view_id}",
            "citation_pdf_url": f"https://langtaosha.org.cn/lts/en/preprint/download/{langtaosha_view_id}/{langtaosha_download_id}",
            "citation_author": ["Author A", "Author B"],
            "citation_author_institution": ["Test University"],
            "citation_keywords": ["LLM", "AI"],
            "citation_reference": ["Ref A", "Ref B"]
        }

        try:
            r1 = indexer.index_dict(raw_payload=a, source_name="biorxiv_history", mode="insert")
            assert r1["success"] is True
            assert r1["mode"] == "insert"
            assert r1["metadata"]["status_code"] == "INSERT_NEW_PAPER"
            assert r1["metadata"]["canonical_changed"] is True
            work_id = r1["work_id"]
            seen_work_ids.add(work_id)
            paper_id = r1["paper_id"]

            s1 = self._get_embedding_status_row(indexer, paper_id)
            print(s1)
            assert s1 is not None
            assert s1[0] == "succeeded"
            assert s1[1] == 1
            assert s1[2] == "biorxiv_history"
            d1 = self._get_vector_document(indexer, "biorxiv_history", work_id)
            assert d1 is not None
            assert d1.get("id") == work_id
            assert d1.get("work_id") == work_id
            assert d1.get("source_name") == "biorxiv_history"
            assert d1.get("text_type") == "abstract"
            assert str(d1.get("paper_id")) == str(paper_id)
            assert "Indexer Matrix A v1" in (d1.get("text") or "")

            r2 = indexer.index_dict(raw_payload=a1, source_name="biorxiv_history", mode="insert")
            print(r2)
            assert r2["success"] is True
            assert r2["mode"] == "insert"
            assert r2["metadata"]["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
            assert r2["metadata"]["canonical_changed"] is False
            assert r2["vectorization"]["success"] is True

            s2 = self._get_embedding_status_row(indexer, paper_id)
            print(s2)
            assert s2 is not None
            assert s2[0] == "succeeded"
            assert s2[1] == 2
            assert s2[2] == "biorxiv_history"
            d2 = self._get_vector_document(indexer, "biorxiv_history", work_id)
            assert d2 is not None
            assert d2.get("id") == work_id
            assert d2.get("work_id") == work_id
            assert d2.get("source_name") == "biorxiv_history"
            assert d2.get("text_type") == "abstract"
            assert str(d2.get("paper_id")) == str(paper_id)
            assert "Indexer Matrix A v2" in (d2.get("text") or "")

            r3 = indexer.index_dict(raw_payload=a2, source_name="langtaosha", mode="insert")
            assert r3["success"] is True
            assert r3["mode"] == "insert"
            assert r3["metadata"]["status_code"] == "INSERT_APPEND_SOURCE"
            assert r3["metadata"]["canonical_changed"] is True
            work_id_after_append = r3["work_id"]
            seen_work_ids.add(r3["work_id"])

            s3 = self._get_embedding_status_row(indexer, paper_id)
            assert s3 is not None
            assert s3[0] == "succeeded"
            assert s3[1] == 3
            assert s3[2] == "langtaosha"
            d3_new = self._get_vector_document(indexer, "langtaosha", work_id_after_append)
            assert d3_new is not None
            assert d3_new.get("id") == work_id_after_append
            assert d3_new.get("work_id") == work_id_after_append
            assert d3_new.get("source_name") == "langtaosha"
            assert d3_new.get("text_type") == "abstract"
            assert str(d3_new.get("paper_id")) == str(paper_id)
            assert "Indexer Matrix A from Langtaosha version1" in (d3_new.get("text") or "")

            r4 = indexer.index_dict(raw_payload=a3, source_name="langtaosha", mode="insert")
            assert r4["success"] is True
            assert r4["mode"] == "insert"
            assert r4["metadata"]["status_code"] == "INSERT_SKIP_SAME_SOURCE"
            assert r4["metadata"]["canonical_changed"] is False
            assert r4["vectorization"].get("skipped") is True

            s4 = self._get_embedding_status_row(indexer, paper_id)
            assert s4 is not None
            assert s4[0] == "succeeded"
            assert s4[1] == 3
            assert s4[2] == "langtaosha"
            d4 = self._get_vector_document(indexer, "langtaosha", work_id_after_append)
            assert d4 is not None
            assert d4.get("id") == work_id_after_append
            assert d4.get("work_id") == work_id_after_append
            assert d4.get("source_name") == "langtaosha"
            assert d4.get("text_type") == "abstract"
            assert str(d4.get("paper_id")) == str(paper_id)
            assert "Indexer Matrix A from Langtaosha version2" not in (d4.get("text") or "")
        finally:
            for wid in seen_work_ids:
                try:
                    indexer.vector_db.delete_document("biorxiv_history", wid, "abstract")
                except Exception:
                    pass
                try:
                    indexer.vector_db.delete_document("langtaosha", wid, "abstract")
                except Exception:
                    pass
            if work_id:
                indexer.delete(work_id=work_id, source_name="langtaosha")

    @pytest.mark.slow
    def test_large_mixed_indexing_stability_with_vector_failures(self, indexer):
        """56 次混合 index：验证状态稳定性、embedding_status 一致性和向量数量一致性。"""
        uid = uuid.uuid4().hex[:10]
        all_work_ids = []

        # 测试开始前清空目标向量集合，避免受其他用例污染
        database = indexer.vector_db.database
        collections = indexer.vector_db.client.list_collections(database)
        for collection in collections:
            if collection.startswith(indexer.vector_db.collection_prefix):
                try:
                    indexer.vector_db.client.drop_collection(database=database, collection=collection)
                except Exception:
                    pass

        def make_biorxiv_payload(doi: str, title: str, date: str, version: str, abstract: str) -> Dict[str, Any]:
            return {
                "title": title,
                "doi": doi,
                "authors": "Author A; Author B",
                "abstract": abstract,
                "date": date,
                "version": version,
                "category": "test",
                "server": "bioRxiv"
            }

        def make_langtaosha_payload(doi: str, title: str, online_date: str, view_id: str, download_id: str) -> Dict[str, Any]:
            return {
                "citation_title": title,
                "citation_abstract": f"{title} abstract",
                "citation_language": "en",
                "citation_publisher": "Langtaosha",
                "citation_date": online_date,
                "citation_online_date": online_date,
                "citation_doi": doi,
                "citation_abstract_html_url": f"https://langtaosha.org.cn/lts/en/preprint/view/{view_id}",
                "citation_pdf_url": f"https://langtaosha.org.cn/lts/en/preprint/download/{view_id}/{download_id}",
                "citation_author": ["Author A", "Author B"]
            }

        status_counter = {
            "INSERT_NEW_PAPER": 0,
            "INSERT_UPDATE_SAME_SOURCE": 0,
            "INSERT_SKIP_SAME_SOURCE": 0,
            "INSERT_APPEND_SOURCE": 0,
        }

        successful_base_dois = []
        append_true_dois = []
        append_false_dois = []

        # =========================
        # 阶段 1: 30 新建 + 4 更新 + 4 不更新
        # =========================
        for i in range(30):
            doi = f"10.1101/test.stability.{uid}.base.{i:03d}"
            payload = make_biorxiv_payload(
                doi=doi,
                title=f"Stability Base {i}",
                date="2026-04-01",
                version="1",
                abstract=f"base abstract {i}"
            )
            r = indexer.index_dict(raw_payload=payload, source_name="biorxiv_history", mode="insert")
            assert r["success"] is True
            assert r["metadata"]["status_code"] == "INSERT_NEW_PAPER"
            status_counter["INSERT_NEW_PAPER"] += 1
            all_work_ids.append((r["work_id"], "biorxiv_history"))
            successful_base_dois.append(doi)

        # 4 个 biorxiv 更新（应更新）
        for i in range(4):
            doi = successful_base_dois[i]
            payload = make_biorxiv_payload(
                doi=doi,
                title=f"Stability Update {i}",
                date="2026-04-03",
                version="2",
                abstract=f"updated abstract {i}"
            )
            r = indexer.index_dict(raw_payload=payload, source_name="biorxiv_history", mode="insert")
            assert r["success"] is True
            assert r["metadata"]["status_code"] == "INSERT_UPDATE_SAME_SOURCE"
            status_counter["INSERT_UPDATE_SAME_SOURCE"] += 1

        # 4 个 biorxiv 不更新（应 skip）
        for i in range(4):
            doi = successful_base_dois[4 + i]
            payload = make_biorxiv_payload(
                doi=doi,
                title=f"Stability Skip {i}",
                date="2026-03-30",
                version="0",
                abstract=f"older abstract {i}"
            )
            r = indexer.index_dict(raw_payload=payload, source_name="biorxiv_history", mode="insert")
            assert r["success"] is True
            assert r["metadata"]["status_code"] == "INSERT_SKIP_SAME_SOURCE"
            status_counter["INSERT_SKIP_SAME_SOURCE"] += 1

        # 断点校验 1
        counts_phase1 = self._get_embedding_status_counts(indexer)
        assert counts_phase1["pending"] == 0
        assert counts_phase1["failed"] == 0
        assert counts_phase1["succeeded"] == 30
        time.sleep(3)
        assert self._get_collection_doc_count(indexer, "biorxiv_history") == 30
        assert self._get_collection_doc_count(indexer, "langtaosha") == 0

        # =========================
        # 阶段 2: 8 新建（6 成功 + 2 向量化失败）
        # =========================
        fail_markers = {"VEC_FAIL_1", "VEC_FAIL_2"}
        orig_add_document = indexer.vector_db.add_document

        def flaky_add_document(*args, **kwargs):
            text_value = kwargs.get("text")
            if text_value is None and len(args) >= 3:
                text_value = args[2]
            if text_value and any(m in text_value for m in fail_markers):
                raise RuntimeError("intentional vectorization failure for stability test")
            return orig_add_document(*args, **kwargs)

        with patch.object(indexer.vector_db, "add_document", side_effect=flaky_add_document):
            for i in range(8):
                doi = f"10.1101/test.stability.{uid}.extra.{i:03d}"
                marker = ""
                if i == 6:
                    marker = " VEC_FAIL_1"
                elif i == 7:
                    marker = " VEC_FAIL_2"
                payload = make_biorxiv_payload(
                    doi=doi,
                    title=f"Stability Extra {i}{marker}",
                    date="2026-04-05",
                    version="1",
                    abstract=f"extra abstract {i}{marker}"
                )
                r = indexer.index_dict(raw_payload=payload, source_name="biorxiv_history", mode="insert")
                assert r["success"] is True
                assert r["metadata"]["status_code"] == "INSERT_NEW_PAPER"
                status_counter["INSERT_NEW_PAPER"] += 1
                all_work_ids.append((r["work_id"], "biorxiv_history"))
                if i <= 5:
                    successful_base_dois.append(doi)

        # 断点校验 2
        counts_phase2 = self._get_embedding_status_counts(indexer)
        assert counts_phase2["pending"] == 0
        assert counts_phase2["failed"] == 2
        assert counts_phase2["succeeded"] == 36
        time.sleep(3)
        assert self._get_collection_doc_count(indexer, "biorxiv_history") == 36
        assert self._get_collection_doc_count(indexer, "langtaosha") == 0

        # =========================
        # 阶段 3: 4 append(true) + 4 append(false)
        # =========================
        append_true_dois = successful_base_dois[8:12]
        append_false_dois = successful_base_dois[12:16]

        for i, doi in enumerate(append_true_dois):
            view_id = str(300000 + i)
            download_id = str(8000 + i)
            payload = make_langtaosha_payload(
                doi=doi,
                title=f"Append True {i}",
                online_date="2026-05-10",
                view_id=view_id,
                download_id=download_id
            )
            r = indexer.index_dict(raw_payload=payload, source_name="langtaosha", mode="insert")
            assert r["success"] is True
            assert r["metadata"]["status_code"] == "INSERT_APPEND_SOURCE"
            assert r["metadata"]["canonical_changed"] is True
            status_counter["INSERT_APPEND_SOURCE"] += 1
            all_work_ids.append((r["work_id"], "langtaosha"))

        for i, doi in enumerate(append_false_dois):
            view_id = str(310000 + i)
            download_id = str(8100 + i)
            payload = make_langtaosha_payload(
                doi=doi,
                title=f"Append False {i}",
                online_date="2026-03-01",
                view_id=view_id,
                download_id=download_id
            )
            r = indexer.index_dict(raw_payload=payload, source_name="langtaosha", mode="insert")
            assert r["success"] is True
            assert r["metadata"]["status_code"] == "INSERT_APPEND_SOURCE"
            assert r["metadata"]["canonical_changed"] is False
            status_counter["INSERT_APPEND_SOURCE"] += 1

        # 最终校验：状态码计数、embedding_status 和向量总量一致
        assert status_counter["INSERT_NEW_PAPER"] == 38
        assert status_counter["INSERT_UPDATE_SAME_SOURCE"] == 4
        assert status_counter["INSERT_SKIP_SAME_SOURCE"] == 4
        assert status_counter["INSERT_APPEND_SOURCE"] == 8

        counts_final = self._get_embedding_status_counts(indexer)
        assert counts_final["pending"] == 0
        assert counts_final["failed"] == 2
        assert counts_final["succeeded"] == 36

        time.sleep(3)

        biorxiv_count = self._get_collection_doc_count(indexer, "biorxiv_history")
        langtaosha_count = self._get_collection_doc_count(indexer, "langtaosha")
        assert biorxiv_count == 36
        assert langtaosha_count == 4
        assert biorxiv_count + langtaosha_count == 40

        # 清理：删除 metadata，并尽力删除向量文档
        for work_id, source_name in set(all_work_ids):
            try:
                indexer.delete(work_id=work_id, source_name=source_name)
            except Exception:
                pass
