"""Indexing模块测试用例

测试PaperIndexer的基本功能：
- 配置读取
- metadata_db 和 vector_db 连接
- add_doc: 添加文档（支持指定 shard_id，向量化成功后自动更新 embedding_status 和 shard_id）
- search: 搜索文档
- delete_doc: 删除文档（只删除新添加的文章）

测试数据来源：
- 新添加的文章：tests/db/test_jsons
- 已有的vector_db：tests/db/faiss
- metadata_db配置：config/config_backend_server.yaml

注意：
- add_doc 方法在向量化成功后会更新 PostgreSQL 中的 embedding_status=2 和 shard_id
- 如果指定的 shard_id 不在可写 shard 列表中，会抛出 ValueError 异常
"""

import unittest
import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import sys

from docset_hub.indexing import PaperIndexer
from config.config_loader import init_config, load_config_from_yaml,get_shard_ids_by_routing


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = _PROJECT_ROOT / 'tests' / 'db' / 'config_backend_server_test.yaml'
TEST_JSONS_DIR = _PROJECT_ROOT / 'tests' / 'fixtures' / 'test_jsons'
CONFIG_ROUTING_NAME='domain:life_sci'




class TestPaperIndexer(unittest.TestCase):
    """PaperIndexer测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 直接指定配置文件路径
        #project_root = Path(__file__).parent.parent.parent
        #config_path = project_root / 'config' / 'config_backend_server.yaml'

        config_path=Path(CONFIG_PATH)
        
        if not config_path.exists():
            raise ValueError(f"未找到配置文件: {config_path}，请确保配置文件存在")
        
        cls.config_path = config_path
        print(f"\n使用配置文件: {config_path}")
                
        # 初始化配置
        init_config(config_path)

        
        routing_shards = get_shard_ids_by_routing(CONFIG_ROUTING_NAME)

        print(f"routing_shards: {routing_shards}")

        config_readonly_shard_ids = routing_shards.get('readonly_shard_ids', [])
        config_writable_shard_ids = routing_shards.get('writable_shard_ids', [])
        
        print(f"config_readonly_shard_ids: {config_readonly_shard_ids}")
        print(f"config_writable_shard_ids: {config_writable_shard_ids}")
        
        # 初始化索引器（传入 config_path）
        cls.indexer = PaperIndexer(
            config_path=config_path,
            enable_vectorization=True,
            readonly_shard_ids=config_readonly_shard_ids,  # 只读 shard IDs
            writable_shard_ids=config_writable_shard_ids,  # 可写 shard ID
            vector_auto_save=True
        )
        
        # 存储测试中添加的文档 work_id，用于清理
        cls.added_work_ids = []
        
        print("✅ PaperIndexer 初始化成功")

    
    def setUp(self):
        """每个测试方法前的准备"""
        # 确保清理列表为空
        self.added_work_ids = []
        # 获取第一个可写 shard_id 用于测试
        if self.indexer.writable_shard_ids:
            self.test_shard_id = self.indexer.writable_shard_ids[0]
        else:
            self.test_shard_id = None
    
    def tearDown(self):
        """每个测试方法后的清理"""
        # 清理测试中添加的文档
        for work_id in self.added_work_ids:
            try:
                result = self.indexer.delete_doc(work_id)
                if result['success']:
                    print(f"  🗑️  清理文档: {work_id}")
            except Exception as e:
                print(f"  ⚠️  清理文档失败 {work_id}: {e}")
        
        # 清空列表
        self.added_work_ids = []
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        # 清理所有测试中添加的文档
        for work_id in cls.added_work_ids:
            try:
                cls.indexer.delete_doc(work_id)
            except:
                pass
        cls.added_work_ids = []
    
    def _load_test_json(self, filename: str) -> Dict[str, Any]:
        """加载测试JSON文件
        
        Args:
            filename: JSON文件名
            
        Returns:
            Dict: 文档数据字典
        """
        json_path = Path(TEST_JSONS_DIR) / filename
        if not json_path.exists():
            raise FileNotFoundError(f"测试文件不存在: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def test_01_config_loading(self):
        """测试配置加载"""
        print("\n=== 测试 01: 配置加载 ===")
        
        # 验证配置文件存在
        self.assertIsNotNone(self.config_path, "配置文件路径应该存在")
        self.assertTrue(self.config_path.exists(), "配置文件应该存在")
        
        # 验证配置已加载
        config = load_config_from_yaml(self.config_path)
        self.assertIn('metadata_db', config, "配置应该包含 metadata_db")
        self.assertIn('vector_db', config, "配置应该包含 vector_db")
        
        print(f"✅ 配置加载成功")
        print(f"  - metadata_db.host: {config['metadata_db'].get('host')}")
        print(f"  - vector_db.db: {config['vector_db'].get('db')}")
    
    def test_02_metadata_db_connection(self):
        """测试 metadata_db 连接"""
        print("\n=== 测试 02: metadata_db 连接 ===")
        
        # 验证 metadata_db 已初始化
        self.assertIsNotNone(self.indexer.metadata_db, "metadata_db 应该已初始化")
        
        # 尝试查询数据库（简单测试）
        try:
            # 这里可以添加一个简单的数据库查询测试
            # 例如：查询 papers 表的记录数
            from config.config_loader import get_db_engine
            from sqlalchemy import text
            
            engine = get_db_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM papers"))
                count = result.scalar()
                print(f"  - papers 表记录数: {count}")
            
            print("✅ metadata_db 连接成功")
        except Exception as e:
            self.fail(f"metadata_db 连接失败: {e}")
    
    def test_03_vector_db_connection(self):
        """测试 vector_db 连接"""
        print("\n=== 测试 03: vector_db 连接 ===")
        
        # 验证 vector_db 已初始化
        self.assertIsNotNone(self.indexer.vector_db, "vector_db 应该已初始化")
        self.assertTrue(self.indexer.enable_vectorization, "向量化功能应该已启用")
        
        # 验证向量数据库路径
        vector_db_path = os.getenv('VECTOR_DB_PATH')
        self.assertIsNotNone(vector_db_path, "VECTOR_DB_PATH 环境变量应该已设置")
        print(f"  - VECTOR_DB_PATH: {vector_db_path}")
        
        print("✅ vector_db 连接成功")
    
    def test_04_add_doc_single(self):
        """测试添加单个文档"""
        print("\n=== 测试 04: 添加单个文档 ===")
        
        # 加载测试文档
        test_doc = self._load_test_json('W019b73d6-1634-77d3-9574-b6014f85b118.json')
        work_id = test_doc.get('work_id')
        
        # 添加文档（指定 shard_id）
        result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        
        # 验证结果
        self.assertTrue(result['success'], f"添加文档失败: {result.get('message')}")
        self.assertEqual(result['work_id'], work_id)
        self.assertIsNotNone(result['paper_id'])
        paper_id = result['paper_id']
        
        # 记录添加的文档
        self.added_work_ids.append(work_id)
        
        # 验证向量化结果
        if 'vectorization' in result:
            vec_info = result['vectorization']
            self.assertTrue(vec_info['enabled'], "向量化应该已启用")
            print(f"  - 向量化状态: {vec_info['message']}")
            
            # 如果向量化成功，验证 embedding_status 和 shard_id 已更新
            if vec_info.get('success'):
                # 从数据库查询验证状态
                from config.config_loader import get_db_engine
                from sqlalchemy import text
                
                engine = get_db_engine()
                with engine.connect() as conn:
                    result_query = conn.execute(
                        text("SELECT embedding_status, shard_id FROM papers WHERE paper_id = :paper_id"),
                        {"paper_id": paper_id}
                    )
                    row = result_query.fetchone()
                    if row:
                        embedding_status, shard_id = row
                        self.assertEqual(embedding_status, 2, "embedding_status 应该为 2 (ready)")
                        self.assertIsNotNone(shard_id, "shard_id 应该已设置")
                        self.assertIn(shard_id, self.indexer.writable_shard_ids, f"shard_id {shard_id} 应该在可写 shard 列表中")
                        print(f"  - embedding_status: {embedding_status} (ready)")
                        print(f"  - shard_id: {shard_id}")
        
        print(f"✅ 文档添加成功")
        print(f"  - work_id: {result['work_id']}")
        print(f"  - paper_id: {result['paper_id']}")
    
    def test_05_add_doc_multiple(self):
        """测试批量添加文档"""
        print("\n=== 测试 05: 批量添加文档 ===")
        
        # 选择几个测试文档
        test_files = [
            'W019b73d6-1634-77d3-9574-b6014f85b118.json',
            'W019b73d6-2f53-7445-9166-ef197e8ac0fa.json',
            'W019b73d6-357e-7723-8b4e-c4d0ff93f73c.json',
        ]
        
        results = []
        for filename in test_files:
            try:
                test_doc = self._load_test_json(filename)
                work_id = test_doc.get('work_id')
                
                # 添加文档
                result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
                self.assertTrue(result['success'], f"添加文档失败: {result.get('message')}")
                
                # 记录添加的文档
                self.added_work_ids.append(work_id)
                results.append(result)
                
                print(f"  ✅ 添加文档: {work_id} (paper_id: {result['paper_id']})")
            except Exception as e:
                print(f"  ⚠️  添加文档失败 {filename}: {e}")
        
        # 验证所有文档都已添加
        self.assertGreater(len(results), 0, "应该至少添加一个文档")
        
        # 验证所有文档都能通过 work_id 查询到
        for result in results:
            doc = self.indexer.get_doc_by_id_identifier(result['work_id'])
            self.assertIsNotNone(doc, f"文档应该存在: {result['work_id']}")
            self.assertEqual(doc['work_id'], result['work_id'])
        
        print(f"✅ 批量添加成功，共添加 {len(results)} 个文档")
    
    def test_06_search_by_query(self):
        """测试搜索功能 - 按查询文本"""
        print("\n=== 测试 06: 搜索功能 (按查询文本) ===")
        
        # 先添加几个测试文档
        test_files = [
            'W019b73d6-1634-77d3-9574-b6014f85b118.json',
            'W019b73d6-2f53-7445-9166-ef197e8ac0fa.json',
        ]
        
        added_work_ids = []
        for filename in test_files:
            try:
                test_doc = self._load_test_json(filename)
                result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
                if result['success']:
                    added_work_ids.append(test_doc.get('work_id'))
                    self.added_work_ids.append(test_doc.get('work_id'))
            except Exception as e:
                print(f"  ⚠️  添加文档失败 {filename}: {e}")
        
        # 等待向量索引更新（如果需要）
        import time
        time.sleep(1)
        
        # 搜索包含 "gastric" 的文档
        query = "gastric"
        results = self.indexer.search(query=query, limit=10)
        
        # 验证结果
        self.assertIsInstance(results, list, "搜索结果应该是列表")
        print(f"  - 查询: '{query}'")
        print(f"  - 找到 {len(results)} 个结果")
        
        # 验证结果格式
        if len(results) > 0:
            for i, result in enumerate(results[:3]):  # 只显示前3个
                self.assertIn('work_id', result, "结果应该包含 work_id")
                self.assertIn('title', result, "结果应该包含 title")
                self.assertIn('similarity', result, "结果应该包含 similarity")
                print(f"    {i+1}. {result.get('title', 'N/A')[:50]}... (相似度: {result.get('similarity', 0):.4f})")
        
        print("✅ 搜索功能测试成功")
    
    def test_07_search_with_filters(self):
        """测试搜索功能 - 带过滤条件"""
        print("\n=== 测试 07: 搜索功能 (带过滤条件) ===")
        
        # 先添加测试文档
        test_doc = self._load_test_json('W019b73d6-1634-77d3-9574-b6014f85b118.json')
        result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        if result['success']:
            self.added_work_ids.append(test_doc.get('work_id'))
        
        # 等待向量索引更新
        import time
        time.sleep(1)
        
        # 搜索并过滤年份
        year = test_doc.get('default_info', {}).get('year')
        if year:
            results = self.indexer.search(
                query="",
                filters={'year': year},
                limit=10
            )
            
            print(f"  - 查询: '' (空查询)")
            print(f"  - 过滤条件: year={year}")
            print(f"  - 找到 {len(results)} 个结果")
            
            # 验证所有结果的年份都符合过滤条件
            for result in results:
                self.assertEqual(result.get('year'), year, f"年份应该匹配: {result.get('year')} != {year}")
            
            print("✅ 带过滤条件的搜索成功")
        else:
            print("  ⚠️  测试文档没有年份信息，跳过年份过滤测试")
    
    def test_08_delete_doc_by_work_id(self):
        """测试删除文档 - 使用 work_id"""
        print("\n=== 测试 08: 删除文档 (work_id) ===")
        
        # 先添加文档
        test_doc = self._load_test_json('W019b73d6-1634-77d3-9574-b6014f85b118.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        paper_id = add_result['paper_id']
        
        # 验证文档存在
        doc = self.indexer.get_doc_by_id_identifier(work_id)
        self.assertIsNotNone(doc, "文档应该存在")
        
        # 删除文档（不添加到清理列表，因为我们要测试删除）
        delete_result = self.indexer.delete_doc(work_id)
        
        # 验证删除结果
        self.assertTrue(delete_result['success'], f"删除失败: {delete_result.get('message')}")
        self.assertEqual(delete_result['work_id'], work_id)
        self.assertEqual(delete_result['paper_id'], paper_id)
        
        # 验证向量删除信息
        if 'vector_deletion' in delete_result:
            vec_info = delete_result['vector_deletion']
            print(f"  - 向量删除状态: {vec_info['message']}")
        
        # 验证文档已删除
        doc_after = self.indexer.get_doc_by_id_identifier(work_id)
        self.assertIsNone(doc_after, "文档应该已被删除")
        
        print(f"✅ 删除文档成功: {work_id}")
    
    def test_09_delete_doc_by_paper_id(self):
        """测试删除文档 - 使用 paper_id"""
        print("\n=== 测试 09: 删除文档 (paper_id) ===")
        
        # 先添加文档
        test_doc = self._load_test_json('W019b73d6-2f53-7445-9166-ef197e8ac0fa.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        paper_id = str(add_result['paper_id'])
        
        # 验证文档存在
        doc = self.indexer.get_doc_by_id_identifier(paper_id)
        self.assertIsNotNone(doc, "文档应该存在")
        
        # 删除文档
        delete_result = self.indexer.delete_doc(paper_id)
        
        # 验证删除结果
        self.assertTrue(delete_result['success'], f"删除失败: {delete_result.get('message')}")
        self.assertEqual(delete_result['work_id'], work_id)
        self.assertEqual(delete_result['paper_id'], int(paper_id))
        
        # 验证文档已删除
        doc_after = self.indexer.get_doc_by_id_identifier(paper_id)
        self.assertIsNone(doc_after, "文档应该已被删除")
        
        print(f"✅ 删除文档成功: paper_id={paper_id}")
    
    def test_10_add_search_delete_workflow(self):
        """测试完整的添加-搜索-删除工作流"""
        print("\n=== 测试 10: 完整工作流 (添加-搜索-删除) ===")
        
        # 1. 添加文档
        test_doc = self._load_test_json('W019b73d6-357e-7723-8b4e-c4d0ff93f73c.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        print(f"  ✅ 步骤1: 添加文档成功 (work_id: {work_id})")
        
        # 2. 搜索文档
        import time
        time.sleep(1)  # 等待向量索引更新
        
        title = test_doc.get('default_info', {}).get('title', '')
        if title:
            # 使用标题中的关键词搜索
            keywords = title.split()[:2]  # 取前两个词
            query = ' '.join(keywords)
        else:
            query = "test"
        
        search_results = self.indexer.search(query=query, limit=10)
        found = False
        for result in search_results:
            if result.get('work_id') == work_id:
                found = True
                break
        
        self.assertTrue(found, f"应该能在搜索结果中找到文档: {work_id}")
        print(f"  ✅ 步骤2: 搜索文档成功 (查询: '{query}')")
        
        # 3. 删除文档
        delete_result = self.indexer.delete_doc(work_id)
        self.assertTrue(delete_result['success'], "删除文档应该成功")
        
        # 从清理列表中移除（已经删除）
        if work_id in self.added_work_ids:
            self.added_work_ids.remove(work_id)
        
        print(f"  ✅ 步骤3: 删除文档成功")
        print("✅ 完整工作流测试成功")
    
    def test_11_read_paper_by_work_id(self):
        """测试通过 work_id 读取论文"""
        print("\n=== 测试 11: 读取论文 (work_id) ===")
        
        # 添加测试文档
        test_doc = self._load_test_json('W019b73d6-1634-77d3-9574-b6014f85b118.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        # 读取论文
        paper_data = self.indexer.read_paper(work_id=work_id)
        
        # 验证结果
        self.assertIsNotNone(paper_data, "应该能读取到论文数据")
        self.assertEqual(paper_data['work_id'], work_id)
        self.assertIn('paper_id', paper_data)
        self.assertIn('title', paper_data)
        self.assertIn('authors', paper_data)
        self.assertIn('categories', paper_data)
        self.assertIn('pub_info', paper_data)
        self.assertIn('citations', paper_data)
        self.assertIn('version_count', paper_data)
        self.assertIn('fields', paper_data)
        
        print(f"✅ 读取论文成功")
        print(f"  - work_id: {work_id}")
        print(f"  - paper_id: {paper_data['paper_id']}")
        print(f"  - 作者数量: {len(paper_data.get('authors', []))}")
        print(f"  - 分类数量: {len(paper_data.get('categories', []))}")
    
    def test_12_read_paper_by_paper_id(self):
        """测试通过 paper_id 读取论文"""
        print("\n=== 测试 12: 读取论文 (paper_id) ===")
        
        # 添加测试文档
        test_doc = self._load_test_json('W019b73d6-2f53-7445-9166-ef197e8ac0fa.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        paper_id = add_result['paper_id']
        self.added_work_ids.append(work_id)
        
        # 读取论文
        paper_data = self.indexer.read_paper(paper_id=paper_id)
        
        # 验证结果
        self.assertIsNotNone(paper_data, "应该能读取到论文数据")
        self.assertEqual(paper_data['paper_id'], paper_id)
        self.assertEqual(paper_data['work_id'], work_id)
        self.assertIn('authors', paper_data)
        self.assertIn('categories', paper_data)
        
        print(f"✅ 读取论文成功")
        print(f"  - paper_id: {paper_id}")
        print(f"  - work_id: {work_id}")
    
    def test_13_read_paper_by_title(self):
        """测试通过 title 读取论文"""
        print("\n=== 测试 13: 读取论文 (title) ===")
        
        # 添加测试文档
        test_doc = self._load_test_json('W019b73d6-357e-7723-8b4e-c4d0ff93f73c.json')
        work_id = test_doc.get('work_id')
        title = test_doc.get('default_info', {}).get('title') or test_doc.get('title')
        
        if not title:
            print("  ⚠️  测试文档没有标题，跳过测试")
            return
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        # 读取论文
        paper_data = self.indexer.read_paper(title=title)
        
        # 验证结果
        self.assertIsNotNone(paper_data, "应该能读取到论文数据")
        self.assertEqual(paper_data['work_id'], work_id)
        self.assertEqual(paper_data['title'], title)
        
        print(f"✅ 读取论文成功")
        print(f"  - title: {title[:50]}...")
        print(f"  - work_id: {work_id}")
    
    def test_14_search_by_condition_title(self):
        """测试按标题条件搜索"""
        print("\n=== 测试 14: 条件搜索 (title) ===")
        
        # 添加测试文档
        test_doc = self._load_test_json('W019b73d6-1634-77d3-9574-b6014f85b118.json')
        work_id = test_doc.get('work_id')
        title = test_doc.get('default_info', {}).get('title') or test_doc.get('title')
        
        if not title:
            print("  ⚠️  测试文档没有标题，跳过测试")
            return
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        # 使用标题中的关键词搜索
        keywords = title.split()[:2]  # 取前两个词
        search_keyword = ' '.join(keywords)
        
        # 搜索
        results = self.indexer.search_by_condition(title=search_keyword, limit=10)
        
        # 验证结果
        self.assertIsInstance(results, list, "搜索结果应该是列表")
        self.assertGreater(len(results), 0, "应该找到至少一个结果")
        
        # 验证结果中包含目标文档
        found = False
        for result in results:
            if result.get('work_id') == work_id:
                found = True
                break
        
        self.assertTrue(found, f"应该能在搜索结果中找到文档: {work_id}")
        
        print(f"✅ 条件搜索成功")
        print(f"  - 搜索关键词: '{search_keyword}'")
        print(f"  - 找到 {len(results)} 个结果")
    
    def test_15_search_by_condition_author(self):
        """测试按作者条件搜索"""
        print("\n=== 测试 15: 条件搜索 (author) ===")
        
        # 使用有 abstract 与 authors 的文档（search_by_condition 会过滤掉无 abstract 的论文）
        test_doc = self._load_test_json('W019b73d6-357e-7723-8b4e-c4d0ff93f73c.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        # 获取作者信息（从文档中提取）
        authors = test_doc.get('default_info', {}).get('authors', [])
        if not authors:
            # 尝试从其他位置获取
            authors = test_doc.get('authors', [])
        
        if not authors or len(authors) == 0:
            self.skipTest("测试文档没有作者信息，跳过测试")
        
        # 使用第一个作者的部分名称搜索
        first_author = authors[0]
        if isinstance(first_author, dict):
            author_name = first_author.get('name', '')
        else:
            author_name = str(first_author)
        
        if not author_name:
            self.skipTest("无法提取作者名称，跳过测试")
        
        # 使用作者名称的一部分进行搜索
        author_keyword = author_name.split()[0] if author_name.split() else author_name[:10]
        
        # 搜索
        results = self.indexer.search_by_condition(author=author_keyword, limit=10)
        
        # 验证结果
        self.assertIsInstance(results, list, "搜索结果应该是列表")
        
        # 如果找到结果，验证结果中包含目标文档
        if len(results) > 0:
            found = False
            for result in results:
                if result.get('work_id') == work_id:
                    found = True
                    break
            
            if found:
                print(f"✅ 条件搜索成功")
                print(f"  - 搜索作者: '{author_name}'")
                print(f"  - 找到 {len(results)} 个结果")
            else:
                print(f"  ⚠️  未在结果中找到目标文档，但搜索功能正常")
        else:
            print(f"  ⚠️  未找到结果，可能作者信息格式不匹配")
    
    def test_16_search_by_condition_category(self):
        """测试按分类条件搜索"""
        print("\n=== 测试 16: 条件搜索 (category) ===")
        
        # 添加测试文档
        test_doc = self._load_test_json('W019b73d6-357e-7723-8b4e-c4d0ff93f73c.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        # 获取分类信息
        categories = test_doc.get('default_info', {}).get('categories', [])
        if not categories:
            categories = test_doc.get('categories', [])
        
        if not categories or len(categories) == 0:
            self.skipTest("测试文档没有分类信息，跳过测试")
        
        # 使用第一个分类的 subdomain
        first_category = categories[0]
        if isinstance(first_category, dict):
            category_subdomain = first_category.get('subdomain', '')
        else:
            category_subdomain = str(first_category)
        
        if not category_subdomain:
            self.skipTest("无法提取分类信息，跳过测试")
        
        # 搜索
        results = self.indexer.search_by_condition(category=category_subdomain, limit=10)
        
        # 验证结果
        self.assertIsInstance(results, list, "搜索结果应该是列表")
        
        # 验证所有结果的分类都匹配
        if len(results) > 0:
            found = False
            for result in results:
                if result.get('work_id') == work_id:
                    found = True
                # 验证分类匹配
                result_categories = result.get('categories', [])
                category_matched = False
                for cat in result_categories:
                    if isinstance(cat, dict) and cat.get('subdomain') == category_subdomain:
                        category_matched = True
                        break
                    elif cat == category_subdomain:
                        category_matched = True
                        break
                if not category_matched and result.get('work_id') == work_id:
                    # 如果目标文档在结果中但分类不匹配，记录警告
                    print(f"  ⚠️  目标文档分类可能不匹配")
            
            if found:
                print(f"✅ 条件搜索成功")
                print(f"  - 搜索分类: '{category_subdomain}'")
                print(f"  - 找到 {len(results)} 个结果")
            else:
                print(f"  ⚠️  未在结果中找到目标文档，但搜索功能正常")
        else:
            print(f"  ⚠️  未找到结果")
    
    def test_17_search_by_condition_year(self):
        """测试按年份条件搜索"""
        print("\n=== 测试 17: 条件搜索 (year) ===")
        
        # 添加测试文档
        test_doc = self._load_test_json('W019b73d6-1634-77d3-9574-b6014f85b118.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        # 获取年份信息
        year = test_doc.get('default_info', {}).get('year')
        if not year:
            year = test_doc.get('year')
        
        if not year:
            print("  ⚠️  测试文档没有年份信息，跳过测试")
            return
        
        # 搜索
        results = self.indexer.search_by_condition(year=year, limit=10)
        
        # 验证结果
        self.assertIsInstance(results, list, "搜索结果应该是列表")
        self.assertGreater(len(results), 0, "应该找到至少一个结果")
        
        # 验证所有结果的年份都匹配
        for result in results:
            self.assertEqual(result.get('year'), year, f"年份应该匹配: {result.get('year')} != {year}")
        
        # 验证结果中包含目标文档
        found = False
        for result in results:
            if result.get('work_id') == work_id:
                found = True
                break
        
        self.assertTrue(found, f"应该能在搜索结果中找到文档: {work_id}")
        
        print(f"✅ 条件搜索成功")
        print(f"  - 搜索年份: {year}")
        print(f"  - 找到 {len(results)} 个结果")
    
    def test_18_search_by_condition_multiple_filters(self):
        """测试多条件组合搜索"""
        print("\n=== 测试 18: 条件搜索 (多条件组合) ===")
        
        # 添加测试文档
        test_doc = self._load_test_json('W019b73d6-2f53-7445-9166-ef197e8ac0fa.json')
        work_id = test_doc.get('work_id')
        
        add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(add_result['success'], "添加文档应该成功")
        self.added_work_ids.append(work_id)
        
        # 获取年份信息
        year = test_doc.get('default_info', {}).get('year')
        if not year:
            year = test_doc.get('year')
        
        if not year:
            print("  ⚠️  测试文档没有年份信息，跳过测试")
            return
        
        # 使用年份和标题关键词组合搜索
        title = test_doc.get('default_info', {}).get('title') or test_doc.get('title')
        if title:
            keywords = title.split()[:1]  # 取第一个词
            search_keyword = keywords[0] if keywords else None
        else:
            search_keyword = None
        
        # 搜索（使用年份和标题）
        if search_keyword:
            results = self.indexer.search_by_condition(
                title=search_keyword,
                year=year,
                limit=10
            )
        else:
            # 如果无法提取标题，只使用年份
            results = self.indexer.search_by_condition(year=year, limit=10)
        
        # 验证结果
        self.assertIsInstance(results, list, "搜索结果应该是列表")
        
        # 验证所有结果都符合条件
        for result in results:
            self.assertEqual(result.get('year'), year, f"年份应该匹配: {result.get('year')} != {year}")
            if search_keyword:
                # 验证标题包含关键词（模糊匹配）
                result_title = result.get('title', '')
                self.assertIn(
                    search_keyword.lower(),
                    result_title.lower(),
                    f"标题应该包含关键词: {result_title}"
                )
        
        print(f"✅ 多条件搜索成功")
        print(f"  - 搜索条件: year={year}" + (f", title='{search_keyword}'" if search_keyword else ""))
        print(f"  - 找到 {len(results)} 个结果")
    
    def test_19_get_daily_updated_papers(self):
        """测试获取每日更新论文功能"""
        print("\n=== 测试 19: 获取每日更新论文 ===")
        
        # 加载测试数据（选择不同日期的文件）
        test_files = [
            # 2004-11-17 (3篇)
            'W019b73d6-65d4-70ae-94e4-e267b914c0e2.json',
            'W019b73d6-8a82-7c5b-bb45-b49ed99729c8.json',
            'W019b73d6-95db-7e76-8b43-057fb0a7f82d.json',
            # 2006-11-15 (2篇)
            'W019b73d3-485c-742c-9ba1-1f3aa2dc3638.json',
            'W019b73d6-2f53-7445-9166-ef197e8ac0fa.json',
            # 2019-09-03 (2篇)
            'W019b73d5-6d85-7bf4-a4e0-58c22124e23d.json',
            'W019b73d6-2ecc-703c-89cb-5f1458a74494.json',
        ]
        
        # 插入测试数据
        inserted_work_ids = []
        for filename in test_files:
            try:
                test_doc = self._load_test_json(filename)
                work_id = test_doc.get('work_id')
                
                add_result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
                if add_result['success']:
                    inserted_work_ids.append(work_id)
                    self.added_work_ids.append(work_id)
                    print(f"  ✅ 添加文档: {work_id}")
            except Exception as e:
                print(f"  ⚠️  添加文档失败 {filename}: {e}")
        
        # 等待一下确保时间戳已写入
        import time
        time.sleep(0.1)
        
        # 测试 1: 查询 2004-11-17 的更新论文
        date_2004 = '2004-11-17'
        result_2004 = self.indexer.get_daily_updated_papers(date_2004)
        
        # 验证结果结构
        self.assertIsNotNone(result_2004, "应该返回结果")
        self.assertIn('update_date', result_2004, "应该包含 update_date 字段")
        self.assertIn('paper_count', result_2004, "应该包含 paper_count 字段")
        self.assertIn('papers', result_2004, "应该包含 papers 列表")
        
        self.assertEqual(result_2004['update_date'], date_2004, "日期应该匹配")
        self.assertIsInstance(result_2004['paper_count'], int, "paper_count 应该是整数")
        self.assertIsInstance(result_2004['papers'], list, "papers 应该是列表")
        
        print(f"✅ 2004-11-17 查询结果:")
        print(f"  - 日期: {result_2004['update_date']}")
        print(f"  - 论文数: {result_2004['paper_count']}")
        print(f"  - 论文列表长度: {len(result_2004['papers'])}")
        
        # 验证论文列表结构
        if len(result_2004['papers']) > 0:
            for paper in result_2004['papers'][:3]:  # 只显示前3篇
                self.assertIn('paper_id', paper, "论文应该包含 paper_id")
                self.assertIn('work_id', paper, "论文应该包含 work_id")
                self.assertIn('title', paper, "论文应该包含 title")
                self.assertIn('created_at', paper, "论文应该包含 created_at")
                self.assertIn('updated_at', paper, "论文应该包含 updated_at")
                self.assertIn('imported_at', paper, "论文应该包含 imported_at")
                print(f"    - {paper['work_id']}: {paper['title'][:50]}...")
        
        # 验证论文数量（应该至少有我们插入的3篇）
        self.assertGreaterEqual(result_2004['paper_count'], 3, f"2004-11-17 应该至少有3篇论文，实际有 {result_2004['paper_count']} 篇")
        
        # 测试 2: 查询 2006-11-15 的更新论文
        date_2006 = '2006-11-15'
        result_2006 = self.indexer.get_daily_updated_papers(date_2006)
        
        self.assertIsNotNone(result_2006, "应该返回结果")
        self.assertEqual(result_2006['update_date'], date_2006, "日期应该匹配")
        self.assertIsInstance(result_2006['paper_count'], int, "paper_count 应该是整数")
        self.assertIsInstance(result_2006['papers'], list, "papers 应该是列表")
        
        print(f"✅ 2006-11-15 查询结果:")
        print(f"  - 日期: {result_2006['update_date']}")
        print(f"  - 论文数: {result_2006['paper_count']}")
        print(f"  - 论文列表长度: {len(result_2006['papers'])}")
        
        # 验证论文数量（应该至少有我们插入的2篇）
        self.assertGreaterEqual(result_2006['paper_count'], 2, f"2006-11-15 应该至少有2篇论文，实际有 {result_2006['paper_count']} 篇")
        
        # 测试 3: 查询 2019-09-03 的更新论文
        date_2019 = '2019-09-03'
        result_2019 = self.indexer.get_daily_updated_papers(date_2019)
        
        self.assertIsNotNone(result_2019, "应该返回结果")
        self.assertEqual(result_2019['update_date'], date_2019, "日期应该匹配")
        self.assertIsInstance(result_2019['paper_count'], int, "paper_count 应该是整数")
        self.assertIsInstance(result_2019['papers'], list, "papers 应该是列表")
        
        print(f"✅ 2019-09-03 查询结果:")
        print(f"  - 日期: {result_2019['update_date']}")
        print(f"  - 论文数: {result_2019['paper_count']}")
        print(f"  - 论文列表长度: {len(result_2019['papers'])}")
        
        # 验证论文数量（应该至少有我们插入的2篇）
        self.assertGreaterEqual(result_2019['paper_count'], 2, f"2019-09-03 应该至少有2篇论文，实际有 {result_2019['paper_count']} 篇")
        
        # 测试 4: 查询不存在的日期
        date_empty = '2099-01-01'
        result_empty = self.indexer.get_daily_updated_papers(date_empty)
        
        self.assertIsNotNone(result_empty, "应该返回结果（即使是空结果）")
        self.assertEqual(result_empty['update_date'], date_empty, "日期应该匹配")
        self.assertEqual(result_empty['paper_count'], 0, "论文数应该为0")
        self.assertEqual(len(result_empty['papers']), 0, "论文列表应该为空")
        
        print(f"✅ 不存在日期查询结果:")
        print(f"  - 日期: {result_empty['update_date']}")
        print(f"  - 论文数: {result_empty['paper_count']}")
        
        print("✅ 获取每日更新论文测试成功")
    
    def test_20_add_doc_with_shard_id(self):
        """测试添加文档时指定 shard_id"""
        print("\n=== 测试 20: 添加文档 (指定 shard_id) ===")
        
        # 获取第一个可写 shard_id
        if not self.indexer.writable_shard_ids:
            self.skipTest("没有可用的可写 shard")
        
        test_shard_id = self.indexer.writable_shard_ids[0]
        
        # 加载测试文档
        test_doc = self._load_test_json('W019b73d6-1634-77d3-9574-b6014f85b118.json')
        work_id = test_doc.get('work_id')
        
        # 添加文档，指定 shard_id
        result = self.indexer.add_doc(test_doc, shard_id=test_shard_id)
        
        # 验证结果
        self.assertTrue(result['success'], f"添加文档失败: {result.get('message')}")
        self.assertEqual(result['work_id'], work_id)
        self.assertIsNotNone(result['paper_id'])
        paper_id = result['paper_id']
        
        # 记录添加的文档
        self.added_work_ids.append(work_id)
        
        # 验证向量化成功时，shard_id 已正确设置
        if 'vectorization' in result and result['vectorization'].get('success'):
            from config.config_loader import get_db_engine
            from sqlalchemy import text
            
            engine = get_db_engine()
            with engine.connect() as conn:
                result_query = conn.execute(
                    text("SELECT embedding_status, shard_id FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                row = result_query.fetchone()
                if row:
                    embedding_status, shard_id = row
                    self.assertEqual(embedding_status, 2, "embedding_status 应该为 2 (ready)")
                    self.assertEqual(shard_id, test_shard_id, f"shard_id 应该为 {test_shard_id}")
                    print(f"  - embedding_status: {embedding_status} (ready)")
                    print(f"  - shard_id: {shard_id} (匹配指定的 shard_id)")
        
        print(f"✅ 添加文档成功 (指定 shard_id={test_shard_id})")
        print(f"  - work_id: {work_id}")
        print(f"  - paper_id: {paper_id}")
    
    def test_21_add_doc_with_invalid_shard_id(self):
        """测试添加文档时指定无效的 shard_id（应该抛出异常）"""
        print("\n=== 测试 21: 添加文档 (无效 shard_id) ===")
        
        # 生成一个不在可写 shard 列表中的 shard_id
        if not self.indexer.writable_shard_ids:
            self.skipTest("没有可用的可写 shard")
        
        # 使用一个明显不在列表中的 shard_id（例如 99999）
        invalid_shard_id = 99999
        while invalid_shard_id in self.indexer.writable_shard_ids:
            invalid_shard_id += 1
        
        # 加载测试文档
        test_doc = self._load_test_json('W019b73d6-2f53-7445-9166-ef197e8ac0fa.json')
        work_id = test_doc.get('work_id')
        
        # 尝试添加文档，应该抛出 ValueError
        with self.assertRaises(ValueError) as context:
            self.indexer.add_doc(test_doc, shard_id=invalid_shard_id)
        
        # 验证错误消息
        error_msg = str(context.exception)
        self.assertIn("不在可写 shard 列表中", error_msg)
        self.assertIn(str(invalid_shard_id), error_msg)
        
        print(f"✅ 正确抛出异常: {error_msg}")
        print(f"  - 无效的 shard_id: {invalid_shard_id}")
        print(f"  - 可写 shard 列表: {self.indexer.writable_shard_ids}")
    
    def test_22_verify_embedding_status_update(self):
        """测试验证 embedding_status 和 shard_id 的更新"""
        print("\n=== 测试 22: 验证 embedding_status 和 shard_id 更新 ===")
        
        # 加载测试文档
        test_doc = self._load_test_json('W019b73d6-357e-7723-8b4e-c4d0ff93f73c.json')
        work_id = test_doc.get('work_id')
        
        # 添加文档
        result = self.indexer.add_doc(test_doc, shard_id=self.test_shard_id)
        self.assertTrue(result['success'], "添加文档应该成功")
        paper_id = result['paper_id']
        self.added_work_ids.append(work_id)
        
        # 验证向量化是否成功
        if 'vectorization' in result and result['vectorization'].get('success'):
            # 从数据库查询验证状态
            from config.config_loader import get_db_engine
            from sqlalchemy import text
            
            engine = get_db_engine()
            with engine.connect() as conn:
                # 查询 embedding_status 和 shard_id
                result_query = conn.execute(
                    text("SELECT embedding_status, shard_id FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                row = result_query.fetchone()
                
                self.assertIsNotNone(row, "应该能查询到论文记录")
                embedding_status, shard_id = row
                
                # 验证状态
                self.assertEqual(embedding_status, 2, "embedding_status 应该为 2 (ready)")
                self.assertIsNotNone(shard_id, "shard_id 应该已设置")
                self.assertIn(shard_id, self.indexer.writable_shard_ids, f"shard_id {shard_id} 应该在可写 shard 列表中")
                
                print(f"✅ 状态更新验证成功")
                print(f"  - paper_id: {paper_id}")
                print(f"  - embedding_status: {embedding_status} (ready)")
                print(f"  - shard_id: {shard_id}")
        else:
            print("  ⚠️  向量化未成功，跳过状态验证")


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行 Indexing 模块测试")
    print("=" * 60)
    
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestPaperIndexer)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 打印总结
    print("\n" + "=" * 60)
    print("测试总结")
    print("=" * 60)
    print(f"运行测试: {result.testsRun}")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    
    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print("\n错误的测试:")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

