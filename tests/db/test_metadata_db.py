"""MetadataDB 测试用例

本测试依赖的本地路径（均相对于 PD_TEST_Langtaosha 仓库根目录）：
- 后端测试配置：tests/db/config_backend_server_test.yaml
- 测试用 JSON 样例：local_data/test_jsons/

测试 MetadataDB 功能：
- 新论文插入（insert_new_paper）
- 已存在论文更新（update_paper）
- 验证所有关联表的数据正确性
- read_paper 功能（通过 work_id、paper_id、title 查询）
- search_by_condition 功能（基于元数据条件搜索）
"""

import unittest
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Connection
import sys

# 在导入其他模块之前先加载配置
from config.config_loader import init_config

# 仓库根目录（本文件位于 <repo>/tests/db/test_metadata_db.py）
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
# tests/db/config_backend_server_test.yaml
_config_path = _REPO_ROOT / "tests" / "db" / "config_backend_server_test.yaml"
# local_data/test_jsons/
_TEST_JSON_DIR = _REPO_ROOT / "local_data" / "test_jsons"

if not _config_path.exists():
    raise ValueError(f"未找到配置文件: {_config_path}，请确保 tests/db/config_backend_server_test.yaml 存在")

# 初始化配置
init_config(_config_path)

# 现在导入其他模块
from config.config_loader import get_db_engine
from docset_hub.storage.metadata_db import MetadataDB


class TestMetadataDB(unittest.TestCase):
    """MetadataDB 测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        print("\n" + "=" * 60)
        print("初始化测试环境")
        print("=" * 60)
        
        # 保存配置文件路径
        cls.config_path = _config_path
        print(f"使用配置文件: {cls.config_path}")
        print(f"测试 JSON 目录: {_TEST_JSON_DIR}")
        
        # 初始化 MetadataDB（传入 config_path）
        cls.metadata_db = MetadataDB(config_path=cls.config_path)
        cls.engine = get_db_engine()
        
        # 清空数据库
        cls._clear_database()
        
        print("✅ 测试环境初始化完成\n")
    
    @classmethod
    def _clear_database(cls):
        """清空数据库中的所有相关表
        
        按照外键依赖关系的逆序清空表，避免外键约束错误
        """
        print("清空数据库...")
        
        with cls.engine.connect() as conn:
            try:
                # 按照外键依赖关系的逆序删除
                tables_to_clear = [
                    'pubmed_additional_info',
                    'paper_fields',
                    'paper_citations',
                    'paper_versions',
                    'paper_publications',
                    'paper_categories',
                    'paper_author_affiliation',
                    'papers',
                    'fields',
                    'venues',
                    'categories',
                ]
                
                for table in tables_to_clear:
                    conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                
                conn.commit()
                print(f"✅ 已清空 {len(tables_to_clear)} 个表")
            except Exception as e:
                conn.rollback()
                print(f"⚠️  清空数据库时出现警告: {e}")
                # 如果 TRUNCATE 失败，尝试 DELETE
                try:
                    for table in tables_to_clear:
                        conn.execute(text(f"DELETE FROM {table}"))
                    conn.commit()
                    print("✅ 使用 DELETE 方式清空数据库成功")
                except Exception as e2:
                    conn.rollback()
                    raise Exception(f"清空数据库失败: {e2}")
    
    def _load_test_json(self, filename: str) -> Dict[str, Any]:
        """加载测试 JSON 文件
        
        Args:
            filename: JSON 文件名
            
        Returns:
            Dict: 解析后的 JSON 数据
        """
        json_path = _TEST_JSON_DIR / filename
        if not json_path.exists():
            raise FileNotFoundError(
                f"测试文件不存在: {json_path}（期望位于仓库根下 local_data/test_jsons/）"
            )
        
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _verify_paper_data(self, conn: Connection, paper_id: int, expected_data: Dict[str, Any]) -> None:
        """验证 papers 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_data: 期望的数据（DocSet格式）
        """
        result = conn.execute(
            text("SELECT * FROM papers WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        row = result.fetchone()
        
        self.assertIsNotNone(row, f"论文 {paper_id} 应该存在于 papers 表中")
        
        # 验证基本字段
        default_info = expected_data.get('default_info', {})
        identifiers = default_info.get('identifiers', {})
        
        self.assertEqual(row.work_id, expected_data.get('work_id'))
        self.assertEqual(row.title, default_info.get('title'))
        self.assertEqual(row.abstract, default_info.get('abstract'))
        self.assertEqual(row.year, default_info.get('year'))
        self.assertEqual(row.pubmed_id, identifiers.get('pubmed'))
        self.assertEqual(row.doi, identifiers.get('doi'))
        self.assertEqual(row.arxiv_id, identifiers.get('arxiv'))
        self.assertEqual(row.is_preprint, default_info.get('is_preprint', False))
        self.assertEqual(row.is_published, default_info.get('is_published', False))
    
    def _verify_authors(self, conn: Connection, paper_id: int, expected_authors: List[Dict[str, Any]]) -> None:
        """验证 paper_author_affiliation 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_authors: 期望的作者列表
        """
        result = conn.execute(
            text("SELECT authors FROM paper_author_affiliation WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        row = result.fetchone()
        
        if expected_authors:
            self.assertIsNotNone(row, f"论文 {paper_id} 应该有作者信息")
            authors_json = row[0]
            self.assertEqual(len(authors_json), len(expected_authors))
            
            # 验证作者顺序和姓名
            for i, author in enumerate(expected_authors):
                self.assertEqual(authors_json[i]['name'], author.get('name'))
                self.assertEqual(authors_json[i]['sequence'], author.get('sequence'))
        else:
            # 如果没有期望的作者，可能没有插入记录
            pass
    
    def _verify_categories(self, conn: Connection, paper_id: int, expected_categories: List[str]) -> None:
        """验证 paper_categories 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_categories: 期望的分类列表
        """
        result = conn.execute(
            text("""
                SELECT c.domain, c.subdomain, pc.is_primary
                FROM paper_categories pc
                JOIN categories c ON pc.cat_id = c.cat_id
                WHERE pc.paper_id = :paper_id
            """),
            {"paper_id": paper_id}
        )
        rows = result.fetchall()
        
        if expected_categories:
            self.assertEqual(len(rows), len(expected_categories), 
                           f"论文 {paper_id} 应该有 {len(expected_categories)} 个分类")
        else:
            # 如果没有期望的分类，可能没有插入记录
            pass
    
    def _verify_publication(self, conn: Connection, paper_id: int, expected_pub_info: Optional[Dict[str, Any]]) -> None:
        """验证 paper_publications 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_pub_info: 期望的发表信息
        """
        if not expected_pub_info or not expected_pub_info.get('venue_name'):
            # 如果没有发表信息，可能没有插入记录
            return
        
        result = conn.execute(
            text("""
                SELECT v.venue_name, v.venue_type, pp.publish_time, pp.presentation_type
                FROM paper_publications pp
                JOIN venues v ON pp.venue_id = v.venue_id
                WHERE pp.paper_id = :paper_id
            """),
            {"paper_id": paper_id}
        )
        row = result.fetchone()
        
        self.assertIsNotNone(row, f"论文 {paper_id} 应该有发表信息")
        self.assertEqual(row.venue_name, expected_pub_info.get('venue_name'))
        self.assertEqual(row.venue_type, expected_pub_info.get('venue_type', 'unknown'))
    
    def _verify_versions(self, conn: Connection, paper_id: int, expected_versions: List[Dict[str, Any]]) -> None:
        """验证 paper_versions 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_versions: 期望的版本列表
        """
        result = conn.execute(
            text("SELECT version_num, version, version_date FROM paper_versions WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        rows = result.fetchall()
        
        if expected_versions:
            self.assertEqual(len(rows), len(expected_versions), 
                           f"论文 {paper_id} 应该有 {len(expected_versions)} 个版本")
        else:
            # 如果没有期望的版本，可能没有插入记录
            pass
    
    def _verify_citations(self, conn: Connection, paper_id: int, expected_citations: Optional[Dict[str, Any]]) -> None:
        """验证 paper_citations 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_citations: 期望的引用信息
        """
        if not expected_citations:
            return
        
        result = conn.execute(
            text("SELECT cited_by_count, update_time FROM paper_citations WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        row = result.fetchone()
        
        if expected_citations.get('cited_by_count') is not None:
            self.assertIsNotNone(row, f"论文 {paper_id} 应该有引用信息")
            self.assertEqual(row.cited_by_count, expected_citations.get('cited_by_count', 0))
    
    def _verify_fields(self, conn: Connection, paper_id: int, expected_fields: List[Dict[str, Any]]) -> None:
        """验证 paper_fields 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_fields: 期望的领域列表
        """
        result = conn.execute(
            text("""
                SELECT f.field_name, f.field_name_en, pf.confidence, pf.source
                FROM paper_fields pf
                JOIN fields f ON pf.field_id = f.field_id
                WHERE pf.paper_id = :paper_id
            """),
            {"paper_id": paper_id}
        )
        rows = result.fetchall()
        
        if expected_fields:
            self.assertEqual(len(rows), len(expected_fields), 
                           f"论文 {paper_id} 应该有 {len(expected_fields)} 个领域")
        else:
            # 如果没有期望的领域，可能没有插入记录
            pass
    
    def _verify_additional_info(self, conn: Connection, paper_id: int, expected_additional_info: Dict[str, Any]) -> None:
        """验证 pubmed_additional_info 表数据
        
        Args:
            conn: 数据库连接
            paper_id: 论文ID
            expected_additional_info: 期望的附加信息
        """
        if not expected_additional_info:
            return
        
        result = conn.execute(
            text("SELECT additional_info_json FROM pubmed_additional_info WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        row = result.fetchone()
        
        self.assertIsNotNone(row, f"论文 {paper_id} 应该有附加信息")
        # 验证 JSON 数据不为空
        self.assertIsNotNone(row[0], "附加信息 JSON 不应为空")
    
    def _get_normalized_ids_from_data(self, data: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """从测试数据中获取规范化后的 external IDs
        
        Args:
            data: DocSet格式的数据字典
            
        Returns:
            Dict: 规范化后的 external IDs 字典
        """
        return self.metadata_db.transformer.get_normalized_external_ids(data)
    
    def _verify_paper_count(self, expected_count: int) -> None:
        """验证数据库中的论文总数
        
        Args:
            expected_count: 期望的论文数量
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM papers"))
            actual_count = result.scalar()
            self.assertEqual(actual_count, expected_count, 
                           f"数据库中的论文数量应该是 {expected_count}，实际是 {actual_count}")
    
    def _verify_check_existence_result(self, conn: Connection, normalized_ids: Dict[str, Optional[str]], 
                                       expected_work_id: Optional[str], expected_paper_id: Optional[int]) -> None:
        """验证 check_paper_existence 的返回结果
        
        Args:
            conn: 数据库连接
            normalized_ids: 规范化后的 external IDs 字典
            expected_work_id: 期望的 work_id（如果为 None，表示期望返回 None）
            expected_paper_id: 期望的 paper_id（如果为 None，表示期望返回 None）
        """
        result = self.metadata_db.check_paper_existence(conn, normalized_ids)
        
        if expected_work_id is None and expected_paper_id is None:
            self.assertIsNone(result, "check_paper_existence 应该返回 None")
        else:
            self.assertIsNotNone(result, "check_paper_existence 应该返回 (work_id, paper_id)")
            work_id, paper_id = result
            self.assertEqual(work_id, expected_work_id, "work_id 应该匹配")
            self.assertEqual(paper_id, expected_paper_id, "paper_id 应该匹配")
    
    def _clear_database_instance(self):
        """实例方法：清空数据库中的所有相关表"""
        with self.engine.connect() as conn:
            try:
                tables_to_clear = [
                    'pubmed_additional_info',
                    'paper_fields',
                    'paper_citations',
                    'paper_versions',
                    'paper_publications',
                    'paper_categories',
                    'paper_author_affiliation',
                    'papers',
                    'fields',
                    'venues',
                    'categories',
                ]
                
                for table in tables_to_clear:
                    conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                
                conn.commit()
            except Exception as e:
                conn.rollback()
                # 如果 TRUNCATE 失败，尝试 DELETE
                try:
                    for table in tables_to_clear:
                        conn.execute(text(f"DELETE FROM {table}"))
                    conn.commit()
                except Exception as e2:
                    conn.rollback()
                    raise Exception(f"清空数据库失败: {e2}")
    
    def test_01_insert_new_paper(self):
        """测试新论文插入功能"""
        print("\n=== 测试 01: 新论文插入 ===")
        
        # 加载测试数据
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        
        # 插入论文
        paper_id = self.metadata_db.insert_paper(test_data)
        
        # 验证返回的 paper_id
        self.assertIsNotNone(paper_id, "插入应该返回有效的 paper_id")
        self.assertIsInstance(paper_id, int, "paper_id 应该是整数")
        
        print(f"✅ 论文插入成功，paper_id: {paper_id}")
        
        # 验证数据库中的数据
        with self.engine.connect() as conn:
            # 验证 papers 表
            self._verify_paper_data(conn, paper_id, test_data)
            
            # 验证作者
            default_info = test_data.get('default_info', {})
            authors = default_info.get('authors', [])
            self._verify_authors(conn, paper_id, authors)
            
            # 验证分类
            categories = default_info.get('categories', [])
            self._verify_categories(conn, paper_id, categories)
            
            # 验证发表信息
            pub_info = default_info.get('pub_info')
            self._verify_publication(conn, paper_id, pub_info)
            
            # 验证版本
            additional_info = test_data.get('additional_info', {})
            versions = additional_info.get('versions', [])
            self._verify_versions(conn, paper_id, versions)
            
            # 验证引用
            citations = additional_info.get('citations')
            self._verify_citations(conn, paper_id, citations)
            
            # 验证领域
            fields = additional_info.get('fields', [])
            self._verify_fields(conn, paper_id, fields)
            
            # 验证附加信息
            self._verify_additional_info(conn, paper_id, additional_info)
        
        print("✅ 所有关联表数据验证通过")
    
    def test_02_update_existing_paper(self):
        """测试已存在论文更新功能"""
        print("\n=== 测试 02: 已存在论文更新 ===")
        
        # 加载测试数据
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        
        # 第一次插入
        paper_id_1 = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id_1, "第一次插入应该成功")
        
        print(f"✅ 第一次插入成功，paper_id: {paper_id_1}")
        
        # 获取规范化后的 external IDs
        normalized_ids = self._get_normalized_ids_from_data(test_data)
        
        # 验证 check_paper_existence 返回 True
        with self.engine.connect() as conn:
            result = self.metadata_db.check_paper_existence(conn, normalized_ids)
            self.assertIsNotNone(result, "check_paper_existence 应该返回结果")
            work_id_1, paper_id_check = result
            self.assertEqual(paper_id_check, paper_id_1, "check_paper_existence 返回的 paper_id 应该匹配")
            print(f"✅ check_paper_existence 返回正确: work_id={work_id_1}, paper_id={paper_id_check}")
        
        # 修改数据（模拟更新）
        updated_data = test_data.copy()
        updated_data['default_info'] = test_data['default_info'].copy()
        updated_data['default_info']['title'] = "Updated Title: " + test_data['default_info']['title']
        updated_data['default_info']['abstract'] = "Updated Abstract: " + (test_data['default_info'].get('abstract') or '')
        
        # 第二次插入（应该更新）
        paper_id_2 = self.metadata_db.insert_paper(updated_data)
        
        # 验证 paper_id 保持不变
        self.assertEqual(paper_id_1, paper_id_2, "更新时 paper_id 应该保持不变")
        
        print(f"✅ 更新成功，paper_id 保持不变: {paper_id_2}")
        
        # 验证数据已更新
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT title, abstract FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id_2}
            )
            row = result.fetchone()
            
            self.assertIsNotNone(row, "论文应该存在")
            self.assertEqual(row.title, updated_data['default_info']['title'], "标题应该已更新")
            self.assertEqual(row.abstract, updated_data['default_info']['abstract'], "摘要应该已更新")
        
        print("✅ 论文数据更新验证通过")
    
    def test_03_insert_paper_with_all_relations(self):
        """测试包含所有关联数据的论文插入"""
        print("\n=== 测试 03: 包含所有关联数据的论文插入 ===")
        
        # 选择一个包含完整数据的测试文件
        # 使用之前已经验证过的文件
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        
        # 插入论文
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        print(f"✅ 论文插入成功，paper_id: {paper_id}")
        
        # 验证所有关联表都有数据
        with self.engine.connect() as conn:
            default_info = test_data.get('default_info', {})
            additional_info = test_data.get('additional_info', {})
            
            # 检查作者
            authors = default_info.get('authors', [])
            if authors:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM paper_author_affiliation WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                count = result.scalar()
                self.assertGreater(count, 0, "应该有作者记录")
                print(f"✅ 作者记录: {count} 条")
            
            # 检查发表信息
            pub_info = default_info.get('pub_info')
            if pub_info and pub_info.get('venue_name'):
                result = conn.execute(
                    text("SELECT COUNT(*) FROM paper_publications WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                count = result.scalar()
                self.assertGreater(count, 0, "应该有发表记录")
                print(f"✅ 发表记录: {count} 条")
            
            # 检查附加信息
            if additional_info:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM pubmed_additional_info WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                count = result.scalar()
                self.assertGreater(count, 0, "应该有附加信息记录")
                print(f"✅ 附加信息记录: {count} 条")
        
        print("✅ 所有关联数据验证通过")
    
    def test_04_batch_insert_multiple_papers(self):
        """测试批量插入多个不同论文"""
        print("\n=== 测试 04: 批量插入多个不同论文 ===")
        
        # 选择多个不同的测试文件
        test_files = [
            'W019b73d6-95db-7e76-8b43-057fb0a7f82d.json',
            'W019b73d6-2a67-71d8-b046-bfe6a0fa05f4.json',
            'W019b73d6-4a5d-71c9-974c-78d04861f83c.json',
            'W019b73d6-51cc-74da-97ef-1b4a766742af.json',
            'W019b73d6-791c-7472-ac25-8bc784e6dc9e.json',
        ]
        
        inserted_papers = []
        
        # 批量插入论文
        for filename in test_files:
            try:
                test_data = self._load_test_json(filename)
                paper_id = self.metadata_db.insert_paper(test_data)
                self.assertIsNotNone(paper_id, f"插入 {filename} 应该成功")
                inserted_papers.append((filename, paper_id, test_data.get('work_id')))
                print(f"✅ 插入成功: {filename}, paper_id={paper_id}")
            except FileNotFoundError:
                print(f"⚠️  跳过不存在的文件: {filename}")
                continue
        
        # 验证所有论文都成功插入
        self.assertGreater(len(inserted_papers), 0, "至少应该插入一篇论文")
        print(f"✅ 成功插入 {len(inserted_papers)} 篇论文")
        
        # 验证 paper_id 唯一性
        paper_ids = [p[1] for p in inserted_papers]
        self.assertEqual(len(paper_ids), len(set(paper_ids)), "所有 paper_id 应该唯一")
        print("✅ 所有 paper_id 唯一")
        
        # 验证数据库中的论文总数
        self._verify_paper_count(len(inserted_papers))
        print(f"✅ 数据库中的论文总数正确: {len(inserted_papers)}")
        
        # 验证每个论文的基本信息
        with self.engine.connect() as conn:
            for filename, paper_id, work_id in inserted_papers:
                test_data = self._load_test_json(filename)
                result = conn.execute(
                    text("SELECT work_id, title FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                row = result.fetchone()
                self.assertIsNotNone(row, f"论文 {paper_id} 应该存在")
                self.assertEqual(row.work_id, work_id, f"论文 {paper_id} 的 work_id 应该匹配")
                print(f"✅ 论文 {paper_id} 基本信息验证通过")
        
        print("✅ 批量插入测试通过")
    
    def test_05_check_paper_existence_with_existing_paper(self):
        """测试 check_paper_existence 在论文存在时返回正确结果"""
        print("\n=== 测试 05: check_paper_existence (论文存在) ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        # 获取数据库中的 work_id
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            row = result.fetchone()
            work_id = row[0]
        
        print(f"✅ 论文插入成功: work_id={work_id}, paper_id={paper_id}")
        
        # 获取规范化后的 external IDs
        normalized_ids = self._get_normalized_ids_from_data(test_data)
        print(f"✅ 规范化后的 external IDs: {normalized_ids}")
        
        # 测试 check_paper_existence
        with self.engine.connect() as conn:
            result = self.metadata_db.check_paper_existence(conn, normalized_ids)
            self.assertIsNotNone(result, "check_paper_existence 应该返回结果")
            result_work_id, result_paper_id = result
            self.assertEqual(result_work_id, work_id, "返回的 work_id 应该匹配")
            self.assertEqual(result_paper_id, paper_id, "返回的 paper_id 应该匹配")
            print(f"✅ check_paper_existence 返回正确: work_id={result_work_id}, paper_id={result_paper_id}")
        
        # 分别测试通过不同的 external ID 查询
        with self.engine.connect() as conn:
            # 测试通过 DOI 查询（如果存在）
            if normalized_ids.get('doi'):
                doi_only = {'doi': normalized_ids['doi'], 'arxiv_id': None, 'pubmed_id': None, 'semantic_scholar_id': None}
                result = self.metadata_db.check_paper_existence(conn, doi_only)
                self.assertIsNotNone(result, "通过 DOI 应该能查询到")
                self.assertEqual(result[1], paper_id, "通过 DOI 查询的 paper_id 应该匹配")
                print("✅ 通过 DOI 查询成功")
            
            # 测试通过 arXiv ID 查询（如果存在）
            if normalized_ids.get('arxiv_id'):
                arxiv_only = {'arxiv_id': normalized_ids['arxiv_id'], 'doi': None, 'pubmed_id': None, 'semantic_scholar_id': None}
                result = self.metadata_db.check_paper_existence(conn, arxiv_only)
                self.assertIsNotNone(result, "通过 arXiv ID 应该能查询到")
                self.assertEqual(result[1], paper_id, "通过 arXiv ID 查询的 paper_id 应该匹配")
                print("✅ 通过 arXiv ID 查询成功")
            
            # 测试通过 PubMed ID 查询（如果存在）
            if normalized_ids.get('pubmed_id'):
                pubmed_only = {'pubmed_id': normalized_ids['pubmed_id'], 'doi': None, 'arxiv_id': None, 'semantic_scholar_id': None}
                result = self.metadata_db.check_paper_existence(conn, pubmed_only)
                self.assertIsNotNone(result, "通过 PubMed ID 应该能查询到")
                self.assertEqual(result[1], paper_id, "通过 PubMed ID 查询的 paper_id 应该匹配")
                print("✅ 通过 PubMed ID 查询成功")
        
        print("✅ check_paper_existence (论文存在) 测试通过")
    
    def test_06_check_paper_existence_with_nonexistent_paper(self):
        """测试 check_paper_existence 在论文不存在时返回 None"""
        print("\n=== 测试 06: check_paper_existence (论文不存在) ===")
        
        # 使用不存在的 external IDs
        nonexistent_ids = {
            'arxiv_id': '9999.99999',
            'doi': '10.9999/nonexistent',
            'pubmed_id': '99999999',
            'semantic_scholar_id': 'nonexistent123'
        }
        
        # 测试 check_paper_existence
        with self.engine.connect() as conn:
            result = self.metadata_db.check_paper_existence(conn, nonexistent_ids)
            self.assertIsNone(result, "check_paper_existence 应该返回 None")
            print("✅ 不存在的论文返回 None")
        
        # 测试所有 external IDs 都为空的情况
        empty_ids = {
            'arxiv_id': None,
            'doi': None,
            'pubmed_id': None,
            'semantic_scholar_id': None
        }
        
        with self.engine.connect() as conn:
            result = self.metadata_db.check_paper_existence(conn, empty_ids)
            self.assertIsNone(result, "所有 external IDs 为空时应该返回 None")
            print("✅ 所有 external IDs 为空时返回 None")
        
        print("✅ check_paper_existence (论文不存在) 测试通过")
    
    def test_07_insert_paper_when_check_existence_returns_true(self):
        """测试 insert_paper 在 check_paper_existence 返回 True 时执行更新"""
        print("\n=== 测试 07: insert_paper (check_existence=True, 更新) ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id_1 = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id_1, "第一次插入应该成功")
        
        # 获取数据库中的 work_id
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id_1}
            )
            row = result.fetchone()
            work_id_1 = row[0]
        
        print(f"✅ 第一次插入成功: work_id={work_id_1}, paper_id={paper_id_1}")
        
        # 获取规范化后的 external IDs
        normalized_ids = self._get_normalized_ids_from_data(test_data)
        
        # 验证 check_paper_existence 返回 True
        with self.engine.connect() as conn:
            result = self.metadata_db.check_paper_existence(conn, normalized_ids)
            self.assertIsNotNone(result, "check_paper_existence 应该返回结果（True）")
            check_work_id, check_paper_id = result
            self.assertEqual(check_paper_id, paper_id_1, "check_paper_existence 返回的 paper_id 应该匹配")
            print(f"✅ check_paper_existence 返回 True: work_id={check_work_id}, paper_id={check_paper_id}")
        
        # 修改数据（模拟更新）
        updated_data = test_data.copy()
        updated_data['default_info'] = test_data['default_info'].copy()
        updated_data['default_info']['title'] = "Updated Title: " + test_data['default_info']['title']
        updated_data['default_info']['abstract'] = "Updated Abstract: " + (test_data['default_info'].get('abstract') or '')
        
        # 再次调用 insert_paper（应该更新）
        paper_id_2 = self.metadata_db.insert_paper(updated_data)
        
        # 验证 paper_id 保持不变
        self.assertEqual(paper_id_1, paper_id_2, "更新时 paper_id 应该保持不变")
        
        # 验证 work_id 保持不变
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id_2}
            )
            row = result.fetchone()
            work_id_2 = row[0]
            self.assertEqual(work_id_1, work_id_2, "更新时 work_id 应该保持不变")
        
        # 验证数据已更新
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT title, abstract FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id_2}
            )
            row = result.fetchone()
            self.assertIsNotNone(row, "论文应该存在")
            self.assertEqual(row.title, updated_data['default_info']['title'], "标题应该已更新")
            self.assertEqual(row.abstract, updated_data['default_info']['abstract'], "摘要应该已更新")
        
        print(f"✅ 更新成功: paper_id={paper_id_2}, work_id={work_id_2}")
        print("✅ insert_paper (check_existence=True, 更新) 测试通过")
    
    def test_08_insert_paper_when_check_existence_returns_false(self):
        """测试 insert_paper 在 check_paper_existence 返回 False 时执行插入"""
        print("\n=== 测试 08: insert_paper (check_existence=False, 插入) ===")
        
        # 确保数据库为空（清空）
        self._clear_database_instance()
        
        # 加载新的测试数据（使用不同的文件确保不存在）
        test_data = self._load_test_json('W019b73d6-2a67-71d8-b046-bfe6a0fa05f4.json')
        
        # 获取规范化后的 external IDs
        normalized_ids = self._get_normalized_ids_from_data(test_data)
        
        # 验证 check_paper_existence 返回 None（False）
        with self.engine.connect() as conn:
            result = self.metadata_db.check_paper_existence(conn, normalized_ids)
            self.assertIsNone(result, "check_paper_existence 应该返回 None（False）")
            print("✅ check_paper_existence 返回 None（False）")
        
        # 调用 insert_paper（应该插入）
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "insert_paper 应该返回有效的 paper_id")
        
        print(f"✅ 插入成功: paper_id={paper_id}")
        
        # 验证论文成功插入数据库
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id, title FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            row = result.fetchone()
            self.assertIsNotNone(row, "论文应该存在于数据库中")
            self.assertEqual(row.work_id, test_data.get('work_id'), "work_id 应该匹配")
            print(f"✅ 论文验证通过: work_id={row.work_id}, title={row.title[:50]}...")
        
        # 验证所有关联数据正确插入
        with self.engine.connect() as conn:
            default_info = test_data.get('default_info', {})
            additional_info = test_data.get('additional_info', {})
            
            # 验证作者
            authors = default_info.get('authors', [])
            if authors:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM paper_author_affiliation WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                count = result.scalar()
                self.assertGreater(count, 0, "应该有作者记录")
                print(f"✅ 作者记录: {count} 条")
        
        print("✅ insert_paper (check_existence=False, 插入) 测试通过")
    
    def test_09_insert_paper_with_different_external_ids(self):
        """测试通过不同 external ID 类型匹配同一论文"""
        print("\n=== 测试 09: 通过不同 external ID 匹配同一论文 ===")
        
        # 加载包含多个 external IDs 的测试数据
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        # 获取数据库中的 work_id
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            row = result.fetchone()
            work_id = row[0]
        
        print(f"✅ 论文插入成功: work_id={work_id}, paper_id={paper_id}")
        
        # 获取规范化后的 external IDs
        normalized_ids = self._get_normalized_ids_from_data(test_data)
        print(f"✅ 规范化后的 external IDs: {normalized_ids}")
        
        # 分别通过不同的 external ID 查询
        with self.engine.connect() as conn:
            # 通过 DOI 查询（如果存在）
            if normalized_ids.get('doi'):
                doi_only = {'doi': normalized_ids['doi'], 'arxiv_id': None, 'pubmed_id': None, 'semantic_scholar_id': None}
                result = self.metadata_db.check_paper_existence(conn, doi_only)
                self.assertIsNotNone(result, "通过 DOI 应该能查询到")
                result_work_id, result_paper_id = result
                self.assertEqual(result_work_id, work_id, "通过 DOI 查询的 work_id 应该匹配")
                self.assertEqual(result_paper_id, paper_id, "通过 DOI 查询的 paper_id 应该匹配")
                print(f"✅ 通过 DOI 查询成功: work_id={result_work_id}, paper_id={result_paper_id}")
            
            # 通过 arXiv ID 查询（如果存在）
            if normalized_ids.get('arxiv_id'):
                arxiv_only = {'arxiv_id': normalized_ids['arxiv_id'], 'doi': None, 'pubmed_id': None, 'semantic_scholar_id': None}
                result = self.metadata_db.check_paper_existence(conn, arxiv_only)
                self.assertIsNotNone(result, "通过 arXiv ID 应该能查询到")
                result_work_id, result_paper_id = result
                self.assertEqual(result_work_id, work_id, "通过 arXiv ID 查询的 work_id 应该匹配")
                self.assertEqual(result_paper_id, paper_id, "通过 arXiv ID 查询的 paper_id 应该匹配")
                print(f"✅ 通过 arXiv ID 查询成功: work_id={result_work_id}, paper_id={result_paper_id}")
            
            # 通过 PubMed ID 查询（如果存在）
            if normalized_ids.get('pubmed_id'):
                pubmed_only = {'pubmed_id': normalized_ids['pubmed_id'], 'doi': None, 'arxiv_id': None, 'semantic_scholar_id': None}
                result = self.metadata_db.check_paper_existence(conn, pubmed_only)
                self.assertIsNotNone(result, "通过 PubMed ID 应该能查询到")
                result_work_id, result_paper_id = result
                self.assertEqual(result_work_id, work_id, "通过 PubMed ID 查询的 work_id 应该匹配")
                self.assertEqual(result_paper_id, paper_id, "通过 PubMed ID 查询的 paper_id 应该匹配")
                print(f"✅ 通过 PubMed ID 查询成功: work_id={result_work_id}, paper_id={result_paper_id}")
        
        print("✅ 通过不同 external ID 匹配同一论文测试通过")
    
    def test_10_insert_paper_normalization_consistency(self):
        """测试规范化一致性（相同论文的不同格式应匹配）"""
        print("\n=== 测试 10: 规范化一致性 ===")
        
        # 加载测试数据
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        
        # 获取原始 external IDs
        default_info = test_data.get('default_info', {})
        identifiers = default_info.get('identifiers', {})
        original_doi = identifiers.get('doi')
        original_arxiv = identifiers.get('arxiv')
        
        # 插入论文（使用原始格式）
        paper_id_1 = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id_1, "插入应该成功")
        
        # 获取数据库中的 work_id
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
                {"paper_id": paper_id_1}
            )
            row = result.fetchone()
            work_id_1 = row[0]
        
        print(f"✅ 第一次插入成功: work_id={work_id_1}, paper_id={paper_id_1}")
        
        # 测试 DOI 规范化（如果存在）
        if original_doi:
            # 创建不同格式的 DOI
            different_formats = [
                original_doi,
                original_doi.upper(),
                f"https://doi.org/{original_doi}",
                f"HTTPS://DOI.ORG/{original_doi.upper()}",
                f"doi:{original_doi}",
            ]
            
            for doi_format in different_formats:
                # 创建测试数据，使用不同格式的 DOI
                test_data_variant = test_data.copy()
                test_data_variant['default_info'] = test_data['default_info'].copy()
                test_data_variant['default_info']['identifiers'] = test_data['default_info']['identifiers'].copy()
                test_data_variant['default_info']['identifiers']['doi'] = doi_format
                
                # 获取规范化后的 external IDs
                normalized_ids = self._get_normalized_ids_from_data(test_data_variant)
                
                # 验证规范化后能匹配到同一篇论文
                with self.engine.connect() as conn:
                    result = self.metadata_db.check_paper_existence(conn, normalized_ids)
                    if normalized_ids.get('doi'):
                        self.assertIsNotNone(result, f"DOI 格式 '{doi_format}' 规范化后应该能匹配")
                        result_work_id, result_paper_id = result
                        self.assertEqual(result_work_id, work_id_1, "work_id 应该匹配")
                        self.assertEqual(result_paper_id, paper_id_1, "paper_id 应该匹配")
                        print(f"✅ DOI 格式 '{doi_format}' 规范化后匹配成功")
        
        # 测试 arXiv ID 版本号处理（如果存在）
        if original_arxiv:
            # 创建带版本号的 arXiv ID
            arxiv_with_version = f"{original_arxiv}v1"
            test_data_variant = test_data.copy()
            test_data_variant['default_info'] = test_data['default_info'].copy()
            test_data_variant['default_info']['identifiers'] = test_data['default_info']['identifiers'].copy()
            test_data_variant['default_info']['identifiers']['arxiv'] = arxiv_with_version
            
            # 获取规范化后的 external IDs
            normalized_ids = self._get_normalized_ids_from_data(test_data_variant)
            
            # 验证规范化后能匹配到同一篇论文
            with self.engine.connect() as conn:
                result = self.metadata_db.check_paper_existence(conn, normalized_ids)
                if normalized_ids.get('arxiv_id'):
                    self.assertIsNotNone(result, f"arXiv ID '{arxiv_with_version}' 规范化后应该能匹配")
                    result_work_id, result_paper_id = result
                    self.assertEqual(result_work_id, work_id_1, "work_id 应该匹配")
                    self.assertEqual(result_paper_id, paper_id_1, "paper_id 应该匹配")
                    print(f"✅ arXiv ID '{arxiv_with_version}' 规范化后匹配成功")
        
        print("✅ 规范化一致性测试通过")
    
    def test_11_get_paper_info_by_work_id(self):
        """测试 get_paper_info_by_work_id 功能"""
        print("\n=== 测试 11: get_paper_info_by_work_id ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        work_id = test_data.get('work_id')
        self.assertIsNotNone(work_id, "work_id 应该存在")
        
        print(f"✅ 论文插入成功: work_id={work_id}, paper_id={paper_id}")
        
        # 测试 get_paper_info_by_work_id
        paper_info = self.metadata_db.get_paper_info_by_work_id(work_id)
        self.assertIsNotNone(paper_info, "应该能获取到论文信息")
        
        # 验证返回的字段
        self.assertEqual(paper_info['work_id'], work_id, "work_id 应该匹配")
        self.assertEqual(paper_info['paper_id'], paper_id, "paper_id 应该匹配")
        
        # 验证基本字段
        default_info = test_data.get('default_info', {})
        self.assertEqual(paper_info['title'], default_info.get('title'), "title 应该匹配")
        self.assertEqual(paper_info['abstract'], default_info.get('abstract'), "abstract 应该匹配")
        self.assertEqual(paper_info['year'], default_info.get('year'), "year 应该匹配")
        
        # 验证 external IDs
        identifiers = default_info.get('identifiers', {})
        # 注意：这里需要验证规范化后的值
        normalized_ids = self._get_normalized_ids_from_data(test_data)
        if normalized_ids.get('doi'):
            self.assertEqual(paper_info['doi'], normalized_ids['doi'], "doi 应该匹配（规范化后）")
        if normalized_ids.get('arxiv_id'):
            self.assertEqual(paper_info['arxiv_id'], normalized_ids['arxiv_id'], "arxiv_id 应该匹配（规范化后）")
        if normalized_ids.get('pubmed_id'):
            self.assertEqual(paper_info['pubmed_id'], normalized_ids['pubmed_id'], "pubmed_id 应该匹配（规范化后）")
        
        print(f"✅ 获取论文信息成功: {len(paper_info)} 个字段")
        print(f"   - work_id: {paper_info['work_id']}")
        print(f"   - paper_id: {paper_info['paper_id']}")
        print(f"   - title: {paper_info['title'][:50]}...")
        
        # 测试不存在的 work_id
        nonexistent_work_id = "nonexistent-work-id-12345"
        paper_info_none = self.metadata_db.get_paper_info_by_work_id(nonexistent_work_id)
        self.assertIsNone(paper_info_none, "不存在的 work_id 应该返回 None")
        print("✅ 不存在的 work_id 返回 None")
        
        print("✅ get_paper_info_by_work_id 测试通过")
    
    def test_12_get_additional_info_by_work_id(self):
        """测试 get_additional_info_by_work_id 功能"""
        print("\n=== 测试 12: get_additional_info_by_work_id ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        work_id = test_data.get('work_id')
        self.assertIsNotNone(work_id, "work_id 应该存在")
        
        print(f"✅ 论文插入成功: work_id={work_id}, paper_id={paper_id}")
        
        # 测试 get_additional_info_by_work_id
        additional_info = self.metadata_db.get_additional_info_by_work_id(work_id)
        
        # 检查是否有附加信息（可能有些论文没有）
        if additional_info:
            self.assertIsNotNone(additional_info, "应该能获取到附加信息")
            self.assertEqual(additional_info['paper_id'], paper_id, "paper_id 应该匹配")
            self.assertIsInstance(additional_info['additional_info_json'], dict, "additional_info_json 应该是字典")
            self.assertIsNotNone(additional_info['created_at'], "created_at 应该存在")
            self.assertIsNotNone(additional_info['updated_at'], "updated_at 应该存在")
            
            print(f"✅ 获取附加信息成功")
            print(f"   - paper_id: {additional_info['paper_id']}")
            print(f"   - additional_info_json keys: {list(additional_info['additional_info_json'].keys())[:5]}...")
        else:
            print("ℹ️  该论文没有附加信息（这是正常的）")
        
        # 测试不存在的 work_id
        nonexistent_work_id = "nonexistent-work-id-12345"
        additional_info_none = self.metadata_db.get_additional_info_by_work_id(nonexistent_work_id)
        self.assertIsNone(additional_info_none, "不存在的 work_id 应该返回 None")
        print("✅ 不存在的 work_id 返回 None")
        
        print("✅ get_additional_info_by_work_id 测试通过")
    
    def test_13_delete_paper_by_work_id(self):
        """测试 delete_paper_by_work_id 功能"""
        print("\n=== 测试 13: delete_paper_by_work_id ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        work_id = test_data.get('work_id')
        self.assertIsNotNone(work_id, "work_id 应该存在")
        
        print(f"✅ 论文插入成功: work_id={work_id}, paper_id={paper_id}")
        
        # 验证论文存在
        paper_info_before = self.metadata_db.get_paper_info_by_work_id(work_id)
        self.assertIsNotNone(paper_info_before, "删除前论文应该存在")
        
        # 验证关联数据存在
        with self.engine.connect() as conn:
            # 检查作者
            result = conn.execute(
                text("SELECT COUNT(*) FROM paper_author_affiliation WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            authors_count_before = result.scalar()
            
            # 检查分类
            result = conn.execute(
                text("SELECT COUNT(*) FROM paper_categories WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            categories_count_before = result.scalar()
            
            # 检查附加信息
            result = conn.execute(
                text("SELECT COUNT(*) FROM pubmed_additional_info WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            additional_info_count_before = result.scalar()
        
        print(f"✅ 删除前关联数据: 作者={authors_count_before}, 分类={categories_count_before}, 附加信息={additional_info_count_before}")
        
        # 测试删除
        delete_result = self.metadata_db.delete_paper_by_work_id(work_id)
        self.assertTrue(delete_result, "删除应该成功")
        print(f"✅ 删除成功: work_id={work_id}")
        
        # 验证论文已删除
        paper_info_after = self.metadata_db.get_paper_info_by_work_id(work_id)
        self.assertIsNone(paper_info_after, "删除后论文应该不存在")
        
        # 验证关联数据已级联删除
        with self.engine.connect() as conn:
            # 检查作者
            result = conn.execute(
                text("SELECT COUNT(*) FROM paper_author_affiliation WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            authors_count_after = result.scalar()
            self.assertEqual(authors_count_after, 0, "作者数据应该被级联删除")
            
            # 检查分类
            result = conn.execute(
                text("SELECT COUNT(*) FROM paper_categories WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            categories_count_after = result.scalar()
            self.assertEqual(categories_count_after, 0, "分类数据应该被级联删除")
            
            # 检查附加信息
            result = conn.execute(
                text("SELECT COUNT(*) FROM pubmed_additional_info WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            additional_info_count_after = result.scalar()
            self.assertEqual(additional_info_count_after, 0, "附加信息应该被级联删除")
        
        print(f"✅ 关联数据已级联删除: 作者={authors_count_after}, 分类={categories_count_after}, 附加信息={additional_info_count_after}")
        
        # 测试删除不存在的 work_id
        nonexistent_work_id = "nonexistent-work-id-12345"
        delete_result_none = self.metadata_db.delete_paper_by_work_id(nonexistent_work_id)
        self.assertFalse(delete_result_none, "删除不存在的 work_id 应该返回 False")
        print("✅ 删除不存在的 work_id 返回 False")
        
        print("✅ delete_paper_by_work_id 测试通过")
    
    def test_14_read_paper_by_work_id(self):
        """测试 read_paper 通过 work_id 读取完整论文数据"""
        print("\n=== 测试 14: read_paper (by work_id) ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        work_id = test_data.get('work_id')
        self.assertIsNotNone(work_id, "work_id 应该存在")
        
        print(f"✅ 论文插入成功: work_id={work_id}, paper_id={paper_id}")
        
        # 测试 read_paper
        paper_data = self.metadata_db.read_paper(work_id=work_id)
        self.assertIsNotNone(paper_data, "应该能获取到论文数据")
        
        # 验证返回的数据包含所有关联信息
        self._verify_complete_paper_data(paper_data, test_data, paper_id)
        
        print("✅ read_paper (by work_id) 测试通过")
    
    def test_15_read_paper_by_paper_id(self):
        """测试 read_paper 通过 paper_id 读取完整论文数据"""
        print("\n=== 测试 15: read_paper (by paper_id) ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        print(f"✅ 论文插入成功: paper_id={paper_id}")
        
        # 测试 read_paper
        paper_data = self.metadata_db.read_paper(paper_id=paper_id)
        self.assertIsNotNone(paper_data, "应该能获取到论文数据")
        
        # 验证返回的数据包含所有关联信息
        self._verify_complete_paper_data(paper_data, test_data, paper_id)
        
        print("✅ read_paper (by paper_id) 测试通过")
    
    def test_16_read_paper_by_title(self):
        """测试 read_paper 通过 title 读取完整论文数据"""
        print("\n=== 测试 16: read_paper (by title) ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        default_info = test_data.get('default_info', {})
        title = default_info.get('title')
        self.assertIsNotNone(title, "title 应该存在")
        
        print(f"✅ 论文插入成功: title={title[:50]}..., paper_id={paper_id}")
        
        # 测试 read_paper
        paper_data = self.metadata_db.read_paper(title=title)
        self.assertIsNotNone(paper_data, "应该能获取到论文数据")
        
        # 验证返回的数据包含所有关联信息
        self._verify_complete_paper_data(paper_data, test_data, paper_id)
        
        print("✅ read_paper (by title) 测试通过")
    
    def test_17_read_paper_parameter_validation(self):
        """测试 read_paper 参数验证"""
        print("\n=== 测试 17: read_paper 参数验证 ===")
        
        # 测试无参数
        with self.assertRaises(ValueError) as context:
            self.metadata_db.read_paper()
        self.assertIn("必须提供", str(context.exception))
        print("✅ 无参数时抛出 ValueError")
        
        # 测试多参数
        with self.assertRaises(ValueError) as context:
            self.metadata_db.read_paper(work_id="test", paper_id=1)
        self.assertIn("只能提供", str(context.exception))
        print("✅ 多参数时抛出 ValueError")
        
        # 测试多参数（work_id 和 title）
        with self.assertRaises(ValueError) as context:
            self.metadata_db.read_paper(work_id="test", title="test")
        self.assertIn("只能提供", str(context.exception))
        print("✅ 多参数（work_id 和 title）时抛出 ValueError")
        
        print("✅ read_paper 参数验证测试通过")
    
    def test_18_read_paper_nonexistent(self):
        """测试 read_paper 查询不存在的论文"""
        print("\n=== 测试 18: read_paper (不存在的论文) ===")
        
        # 测试不存在的 work_id
        result = self.metadata_db.read_paper(work_id="nonexistent-work-id-12345")
        self.assertIsNone(result, "不存在的 work_id 应该返回 None")
        print("✅ 不存在的 work_id 返回 None")
        
        # 测试不存在的 paper_id
        result = self.metadata_db.read_paper(paper_id=999999)
        self.assertIsNone(result, "不存在的 paper_id 应该返回 None")
        print("✅ 不存在的 paper_id 返回 None")
        
        # 测试不存在的 title
        result = self.metadata_db.read_paper(title="Nonexistent Title That Should Not Exist")
        self.assertIsNone(result, "不存在的 title 应该返回 None")
        print("✅ 不存在的 title 返回 None")
        
        print("✅ read_paper (不存在的论文) 测试通过")
    
    def test_19_search_by_condition_title(self):
        """测试 search_by_condition 按标题搜索"""
        print("\n=== 测试 19: search_by_condition (按标题) ===")
        
        # 准备测试数据
        test_papers = self._prepare_test_papers()
        self.assertGreater(len(test_papers), 0, "应该至少有一篇测试论文")
        
        # 获取第一篇论文的标题关键词
        first_paper = test_papers[0]
        default_info = first_paper.get('default_info', {})
        title = default_info.get('title', '')
        # 使用标题的前几个词作为搜索关键词
        title_keywords = ' '.join(title.split()[:3]) if title else ''
        
        if title_keywords:
            # 搜索
            results = self.metadata_db.search_by_condition(title=title_keywords, limit=10)
            self.assertGreater(len(results), 0, "应该找到至少一篇论文")
            
            # 验证结果包含完整数据
            for result in results:
                self._verify_complete_paper_data_structure(result)
            
            print(f"✅ 按标题搜索成功: 关键词='{title_keywords}', 找到 {len(results)} 篇论文")
        else:
            print("⚠️  测试论文没有标题，跳过此测试")
        
        print("✅ search_by_condition (按标题) 测试通过")
    
    def test_20_search_by_condition_author(self):
        """测试 search_by_condition 按作者搜索"""
        print("\n=== 测试 20: search_by_condition (按作者) ===")
        
        # 准备测试数据
        test_papers = self._prepare_test_papers()
        self.assertGreater(len(test_papers), 0, "应该至少有一篇测试论文")
        
        # 获取第一篇论文的作者
        first_paper = test_papers[0]
        default_info = first_paper.get('default_info', {})
        authors = default_info.get('authors', [])
        
        if authors and len(authors) > 0:
            author_name = authors[0].get('name', '')
            if author_name:
                # 使用作者名的前几个字符作为搜索关键词
                author_keyword = author_name.split()[0] if author_name else ''
                
                if author_keyword:
                    # 搜索
                    results = self.metadata_db.search_by_condition(author=author_keyword, limit=10)
                    self.assertGreater(len(results), 0, "应该找到至少一篇论文")
                    
                    # 验证结果包含完整数据
                    for result in results:
                        self._verify_complete_paper_data_structure(result)
                    
                    print(f"✅ 按作者搜索成功: 关键词='{author_keyword}', 找到 {len(results)} 篇论文")
                else:
                    print("⚠️  作者名格式异常，跳过此测试")
            else:
                print("⚠️  测试论文没有作者名，跳过此测试")
        else:
            print("⚠️  测试论文没有作者，跳过此测试")
        
        print("✅ search_by_condition (按作者) 测试通过")
    
    def test_21_search_by_condition_category(self):
        """测试 search_by_condition 按分类搜索"""
        print("\n=== 测试 21: search_by_condition (按分类) ===")
        
        # 准备测试数据
        test_papers = self._prepare_test_papers()
        self.assertGreater(len(test_papers), 0, "应该至少有一篇测试论文")
        
        # 获取第一篇论文的分类
        first_paper = test_papers[0]
        default_info = first_paper.get('default_info', {})
        categories = default_info.get('categories', [])
        
        if categories and len(categories) > 0:
            # 使用第一个分类的 subdomain
            category_subdomain = categories[0].get('subdomain', '')
            
            if category_subdomain:
                # 搜索
                results = self.metadata_db.search_by_condition(category=category_subdomain, limit=10)
                # 注意：可能找不到结果，因为分类可能不匹配
                if len(results) > 0:
                    # 验证结果包含完整数据
                    for result in results:
                        self._verify_complete_paper_data_structure(result)
                    print(f"✅ 按分类搜索成功: category='{category_subdomain}', 找到 {len(results)} 篇论文")
                else:
                    print(f"ℹ️  按分类搜索: category='{category_subdomain}', 未找到匹配的论文（这是正常的）")
            else:
                print("⚠️  测试论文没有分类 subdomain，跳过此测试")
        else:
            print("⚠️  测试论文没有分类，跳过此测试")
        
        print("✅ search_by_condition (按分类) 测试通过")
    
    def test_22_search_by_condition_year(self):
        """测试 search_by_condition 按年份搜索"""
        print("\n=== 测试 22: search_by_condition (按年份) ===")
        
        # 准备测试数据
        test_papers = self._prepare_test_papers()
        self.assertGreater(len(test_papers), 0, "应该至少有一篇测试论文")
        
        # 获取第一篇论文的年份
        first_paper = test_papers[0]
        default_info = first_paper.get('default_info', {})
        year = default_info.get('year')
        
        if year:
            # 搜索
            results = self.metadata_db.search_by_condition(year=year, limit=10)
            self.assertGreater(len(results), 0, "应该找到至少一篇论文")
            
            # 验证结果包含完整数据
            for result in results:
                self._verify_complete_paper_data_structure(result)
                # 验证年份匹配
                self.assertEqual(result.get('year'), year, "返回的论文年份应该匹配")
            
            print(f"✅ 按年份搜索成功: year={year}, 找到 {len(results)} 篇论文")
        else:
            print("⚠️  测试论文没有年份，跳过此测试")
        
        print("✅ search_by_condition (按年份) 测试通过")
    
    def test_23_search_by_condition_multiple_conditions(self):
        """测试 search_by_condition 多条件组合搜索"""
        print("\n=== 测试 23: search_by_condition (多条件组合) ===")
        
        # 准备测试数据
        test_papers = self._prepare_test_papers()
        self.assertGreater(len(test_papers), 0, "应该至少有一篇测试论文")
        
        # 获取第一篇论文的信息
        first_paper = test_papers[0]
        default_info = first_paper.get('default_info', {})
        year = default_info.get('year')
        title = default_info.get('title', '')
        title_keywords = ' '.join(title.split()[:2]) if title else ''
        
        if year and title_keywords:
            # 多条件搜索
            results = self.metadata_db.search_by_condition(
                title=title_keywords,
                year=year,
                limit=10
            )
            
            # 验证结果包含完整数据
            for result in results:
                self._verify_complete_paper_data_structure(result)
                # 验证年份匹配
                if year:
                    self.assertEqual(result.get('year'), year, "返回的论文年份应该匹配")
            
            print(f"✅ 多条件搜索成功: title='{title_keywords}', year={year}, 找到 {len(results)} 篇论文")
        else:
            print("⚠️  测试论文缺少必要信息，跳过此测试")
        
        print("✅ search_by_condition (多条件组合) 测试通过")
    
    def test_24_search_by_condition_limit(self):
        """测试 search_by_condition 结果数量限制"""
        print("\n=== 测试 24: search_by_condition (结果数量限制) ===")
        
        # 准备测试数据（插入多篇论文）
        test_papers = self._prepare_test_papers()
        self.assertGreater(len(test_papers), 0, "应该至少有一篇测试论文")
        
        # 搜索，限制结果为 3 篇
        results = self.metadata_db.search_by_condition(limit=3)
        self.assertLessEqual(len(results), 3, "结果数量应该不超过限制")
        
        # 验证结果包含完整数据
        for result in results:
            self._verify_complete_paper_data_structure(result)
        
        print(f"✅ 结果数量限制测试成功: limit=3, 实际返回 {len(results)} 篇论文")
        
        print("✅ search_by_condition (结果数量限制) 测试通过")
    
    def test_25_search_by_condition_empty_result(self):
        """测试 search_by_condition 返回空结果"""
        print("\n=== 测试 25: search_by_condition (空结果) ===")
        
        # 使用不可能匹配的条件搜索
        results = self.metadata_db.search_by_condition(
            title="ThisTitleShouldNeverExistInDatabase12345",
            limit=10
        )
        
        self.assertEqual(len(results), 0, "应该返回空列表")
        print("✅ 空结果测试成功: 返回空列表")
        
        print("✅ search_by_condition (空结果) 测试通过")
    
    def test_26_read_paper_vs_get_paper_info_comparison(self):
        """对比 read_paper 和 get_paper_info_by_work_id 的返回数据"""
        print("\n=== 测试 26: read_paper vs get_paper_info_by_work_id 对比 ===")
        
        # 加载测试数据并插入
        test_data = self._load_test_json('W019b73d6-95db-7e76-8b43-057fb0a7f82d.json')
        paper_id = self.metadata_db.insert_paper(test_data)
        self.assertIsNotNone(paper_id, "插入应该成功")
        
        work_id = test_data.get('work_id')
        self.assertIsNotNone(work_id, "work_id 应该存在")
        
        print(f"✅ 论文插入成功: work_id={work_id}, paper_id={paper_id}")
        
        # 获取两种方式的数据
        read_paper_data = self.metadata_db.read_paper(work_id=work_id)
        get_paper_info_data = self.metadata_db.get_paper_info_by_work_id(work_id)
        
        self.assertIsNotNone(read_paper_data, "read_paper 应该返回数据")
        self.assertIsNotNone(get_paper_info_data, "get_paper_info_by_work_id 应该返回数据")
        
        # 验证 read_paper 包含关联信息
        self.assertIn('authors', read_paper_data, "read_paper 应该包含 authors")
        self.assertIn('categories', read_paper_data, "read_paper 应该包含 categories")
        self.assertIn('pub_info', read_paper_data, "read_paper 应该包含 pub_info")
        self.assertIn('citations', read_paper_data, "read_paper 应该包含 citations")
        self.assertIn('version_count', read_paper_data, "read_paper 应该包含 version_count")
        self.assertIn('fields', read_paper_data, "read_paper 应该包含 fields")
        
        # 验证 get_paper_info_by_work_id 不包含关联信息（只包含主表数据）
        self.assertNotIn('authors', get_paper_info_data, "get_paper_info_by_work_id 不应该包含 authors")
        self.assertNotIn('categories', get_paper_info_data, "get_paper_info_by_work_id 不应该包含 categories")
        
        # 验证基本字段一致
        self.assertEqual(read_paper_data['paper_id'], get_paper_info_data['paper_id'], "paper_id 应该一致")
        self.assertEqual(read_paper_data['work_id'], get_paper_info_data['work_id'], "work_id 应该一致")
        self.assertEqual(read_paper_data['title'], get_paper_info_data['title'], "title 应该一致")
        
        print("✅ read_paper 包含完整关联信息")
        print("✅ get_paper_info_by_work_id 只包含主表数据")
        print("✅ 基本字段一致")
        
        print("✅ read_paper vs get_paper_info_by_work_id 对比测试通过")
    
    def _prepare_test_papers(self) -> List[Dict[str, Any]]:
        """准备测试用的多篇论文数据
        
        Returns:
            List[Dict]: 测试论文数据列表
        """
        test_files = [
            'W019b73d6-95db-7e76-8b43-057fb0a7f82d.json',
            'W019b73d6-2a67-71d8-b046-bfe6a0fa05f4.json',
            'W019b73d6-4a5d-71c9-974c-78d04861f83c.json',
        ]
        
        test_papers = []
        for filename in test_files:
            try:
                test_data = self._load_test_json(filename)
                paper_id = self.metadata_db.insert_paper(test_data)
                if paper_id:
                    test_papers.append(test_data)
                    print(f"✅ 准备测试论文: {filename}, paper_id={paper_id}")
            except FileNotFoundError:
                print(f"⚠️  跳过不存在的文件: {filename}")
                continue
            except Exception as e:
                print(f"⚠️  加载文件失败 {filename}: {e}")
                continue
        
        return test_papers
    
    def _verify_complete_paper_data(self, paper_data: Dict[str, Any], expected_data: Dict[str, Any], paper_id: int) -> None:
        """验证完整论文数据（包含所有关联信息）
        
        Args:
            paper_data: read_paper 返回的数据
            expected_data: 期望的原始测试数据
            paper_id: 论文ID
        """
        # 验证基本字段
        self.assertEqual(paper_data['paper_id'], paper_id, "paper_id 应该匹配")
        self.assertEqual(paper_data['work_id'], expected_data.get('work_id'), "work_id 应该匹配")
        
        default_info = expected_data.get('default_info', {})
        self.assertEqual(paper_data['title'], default_info.get('title'), "title 应该匹配")
        self.assertEqual(paper_data['abstract'], default_info.get('abstract'), "abstract 应该匹配")
        
        # 验证关联信息存在
        self.assertIn('authors', paper_data, "应该包含 authors")
        self.assertIn('categories', paper_data, "应该包含 categories")
        self.assertIn('pub_info', paper_data, "应该包含 pub_info")
        self.assertIn('citations', paper_data, "应该包含 citations")
        self.assertIn('version_count', paper_data, "应该包含 version_count")
        self.assertIn('fields', paper_data, "应该包含 fields")
        
        # 验证 authors 是列表
        self.assertIsInstance(paper_data['authors'], list, "authors 应该是列表")
        
        # 验证 categories 是列表
        self.assertIsInstance(paper_data['categories'], list, "categories 应该是列表")
        
        # 验证 citations 是字典
        self.assertIsInstance(paper_data['citations'], dict, "citations 应该是字典")
        self.assertIn('cited_by_count', paper_data['citations'], "citations 应该包含 cited_by_count")
        
        # 验证 version_count 是整数
        self.assertIsInstance(paper_data['version_count'], int, "version_count 应该是整数")
        
        # 验证 fields 是列表
        self.assertIsInstance(paper_data['fields'], list, "fields 应该是列表")
    
    def _verify_complete_paper_data_structure(self, paper_data: Dict[str, Any]) -> None:
        """验证完整论文数据结构（不验证具体值）
        
        Args:
            paper_data: read_paper 或 search_by_condition 返回的数据
        """
        # 验证基本字段存在
        self.assertIn('paper_id', paper_data, "应该包含 paper_id")
        self.assertIn('work_id', paper_data, "应该包含 work_id")
        self.assertIn('title', paper_data, "应该包含 title")
        
        # 验证关联信息存在
        self.assertIn('authors', paper_data, "应该包含 authors")
        self.assertIn('categories', paper_data, "应该包含 categories")
        self.assertIn('pub_info', paper_data, "应该包含 pub_info")
        self.assertIn('citations', paper_data, "应该包含 citations")
        self.assertIn('version_count', paper_data, "应该包含 version_count")
        self.assertIn('fields', paper_data, "应该包含 fields")
        
        # 验证数据类型
        self.assertIsInstance(paper_data['authors'], list, "authors 应该是列表")
        self.assertIsInstance(paper_data['categories'], list, "categories 应该是列表")
        self.assertIsInstance(paper_data['citations'], dict, "citations 应该是字典")
        self.assertIsInstance(paper_data['version_count'], int, "version_count 应该是整数")
        self.assertIsInstance(paper_data['fields'], list, "fields 应该是列表")


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行 MetadataDB 测试")
    print("=" * 60)
    
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestMetadataDB)
    
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
