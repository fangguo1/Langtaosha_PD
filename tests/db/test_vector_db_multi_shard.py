"""VectorDB 多 Shard 测试用例

测试 VectorDB 的多 shard 功能：
- test_0_add_documents: 从 test_jsons 添加测试数据
- test_1_shard_loading: 测试 shard 加载功能
- test_2_search_from_multi_shards: 测试多 shard 搜索功能
- test_3_search_from_multi_shards_with_top_k: 测试不同 top_k 值的搜索功能
"""

import unittest
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

# 在导入其他模块之前先加载配置
from config.config_loader import init_config, load_config_from_yaml


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = _PROJECT_ROOT / 'tests' / 'db' / 'config_backend_server_test.yaml'
CONFIG_ROUTING_NAME='source:pubmed'

config_path=CONFIG_PATH
# 初始化配置（会自动设置环境变量）
init_config(config_path)

# 加载配置字典（用于读取配置值）
configs = load_config_from_yaml(config_path)

# 现在导入其他模块
from docset_hub.storage.vector_db import VectorDB
from docset_hub.storage.metadata_db import MetadataDB
import logging

logger = logging.getLogger(__name__)

# 从配置读取 shards_dir 和模型名称
SHARDS_DIR = configs['vector_db'].get('db', '')
GRITLM_MODEL_NAME = configs['vector_db'].get('gritlm_model_name', 'GritLM/GritLM-7B')
TEST_JSONS_DIR = _PROJECT_ROOT / 'tests' / 'fixtures' / 'test_jsons'


class TestVectorDBMultiShard(unittest.TestCase):
    """VectorDB 多 Shard 测试类"""
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        print("\n" + "=" * 60)
        print("初始化 VectorDB 多 Shard 测试环境")
        print("=" * 60)
        
        # 从配置文件获取 shards 目录路径
        cls.shards_dir = Path(SHARDS_DIR) if SHARDS_DIR else None
        
        # 如果目录不存在，尝试使用环境变量
        if cls.shards_dir is None or not cls.shards_dir.exists():
            import os
            env_shards_dir = os.getenv('FAISS_SHARDS_DIR')
            if env_shards_dir:
                cls.shards_dir = Path(env_shards_dir)
        
        print(f"Shards 目录: {cls.shards_dir}")
        
        if cls.shards_dir is None or not cls.shards_dir.exists():
            print(f"⚠️  Shards 目录未配置或不存在，将跳过测试")
            print(f"   请配置 vector_db.faiss_shards_dir 或设置环境变量 FAISS_SHARDS_DIR")
            print(f"   或配置 vector_db.faiss_dir 或 vector_db.base_dir")
            cls.shards_dir = None
            return
        
        # 检查 shards 目录中是否有索引文件
        index_files = list(cls.shards_dir.glob("shard_*.index"))
        if not index_files:
            print(f"⚠️  Shards 目录中没有找到索引文件（shard_*.index），将跳过测试")
            cls.shards_dir = None
            return
        
        print(f"找到 {len(index_files)} 个 shard 索引文件")
        for idx_file in sorted(index_files)[:5]:  # 只显示前5个
            print(f"  - {idx_file.name}")
        if len(index_files) > 5:
            print(f"  ... 还有 {len(index_files) - 5} 个文件")
        
        # 从文件名中提取 shard IDs
        # 文件名格式：shard_{id:03d}.index，例如 shard_000.index -> shard_id=0
        shard_ids = []
        for idx_file in index_files:
            # 从文件名中提取数字部分，例如 "shard_000.index" -> "000" -> 0
            try:
                shard_id_str = idx_file.stem.replace("shard_", "")
                shard_id = int(shard_id_str)
                shard_ids.append(shard_id)
            except ValueError:
                logger.warning(f"无法从文件名中提取 shard ID: {idx_file.name}")
        
        # 分离只读 shard 和可写 shard（通常 999 是可写 shard）
        # 从配置读取 shard_ids 分类
        from config.config_loader import get_shard_ids_by_routing
        routing_shards = get_shard_ids_by_routing(CONFIG_ROUTING_NAME)

        print(f"routing_shards: {routing_shards}")

        config_readonly_shard_ids = routing_shards.get('readonly_shard_ids', [])
        config_writable_shard_ids = routing_shards.get('writable_shard_ids', [])
        
        print(f"config_readonly_shard_ids: {config_readonly_shard_ids}")
        print(f"config_writable_shard_ids: {config_writable_shard_ids}")
        # 如果配置中有，使用配置的；否则从文件系统检测
        if config_readonly_shard_ids:
            readonly_shard_ids = config_readonly_shard_ids
        else:
            readonly_shard_ids = [sid for sid in shard_ids if sid != 999]
        
        if config_writable_shard_ids:
            writable_shard_ids = config_writable_shard_ids
        else:
            # 如果配置中没有，使用检测到的 999 作为默认可写 shard
            writable_shard_ids = [999] if 999 in shard_ids else []
        
        print(f"检测到的 shard IDs: {sorted(shard_ids)}")
        print(f"只读 shard IDs: {sorted(readonly_shard_ids)}")
        print(f"可写 shard IDs: {sorted(writable_shard_ids)}")
        
        # 保存 shard_ids 供测试使用
        cls.readonly_shard_ids = readonly_shard_ids
        cls.writable_shard_ids = writable_shard_ids
        
        # 初始化 VectorDB 实例（用于搜索测试）
        try:
            cls.vector_db = VectorDB(
                shards_dir=str(cls.shards_dir),
                model_name=GRITLM_MODEL_NAME,
                readonly_shard_ids=readonly_shard_ids if readonly_shard_ids else None,
                writable_shard_ids=writable_shard_ids if writable_shard_ids else None
            )
            print("✅ VectorDB 实例初始化成功")
        except Exception as e:
            print(f"❌ VectorDB 实例初始化失败: {e}")
            cls.vector_db = None
        
        # 初始化 MetadataDB（用于添加测试数据）
        try:
            cls.metadata_db = MetadataDB(config_path=config_path)
            print("✅ MetadataDB 实例初始化成功")
        except Exception as e:
            print(f"⚠️  MetadataDB 实例初始化失败: {e}")
            cls.metadata_db = None
        
        # 记录添加的 paper_id，用于测试后清理
        cls.added_paper_ids = []
        
        print("✅ 测试环境初始化完成\n")
    
    def setUp(self):
        """每个测试方法前的准备"""
        if self.__class__.shards_dir is None:
            self.skipTest("Shards 目录未配置或不存在，跳过测试")
        
        if not hasattr(self.__class__, 'vector_db') or self.__class__.vector_db is None:
            self.skipTest("VectorDB 实例未初始化，跳过测试")
    
    def _load_test_json(self, filename: str) -> Dict[str, Any]:
        """加载测试 JSON 文件
        
        Args:
            filename: JSON 文件名
            
        Returns:
            Dict: 解析后的 JSON 数据
        """
        json_path = TEST_JSONS_DIR / filename
        if not json_path.exists():
            raise FileNotFoundError(f"测试文件不存在: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _extract_text_for_embedding(self, paper_data: Dict[str, Any]) -> str:
        """从论文数据中提取用于 embedding 的文本
        
        Args:
            paper_data: 论文数据字典
            
        Returns:
            str: 用于 embedding 的文本（title + abstract）
        """
        default_info = paper_data.get('default_info', {})
        title = default_info.get('title', '')
        abstract = default_info.get('abstract', '')
        
        if title or abstract:
            return f"{title} {abstract}".strip()
        else:
            return ""
    '''
    def test_0_add_documents(self):
        """测试0：从 test_jsons 添加测试数据到 vector_db"""
        print("\n" + "-" * 60)
        print("测试0：从 test_jsons 添加测试数据到 vector_db")
        print("-" * 60)
        
        
        vector_db = self.__class__.vector_db
        
        # 检查 test_jsons 目录是否存在
        if not TEST_JSONS_DIR.exists():
            self.skipTest(f"测试 JSON 目录不存在: {TEST_JSONS_DIR}")
        
        # 获取所有 JSON 文件（最多使用前 5 个）
        json_files = sorted(TEST_JSONS_DIR.glob("*.json"))[:5]
        if not json_files:
            self.skipTest("test_jsons 目录中没有找到 JSON 文件")
        
        print(f"找到 {len(json_files)} 个测试 JSON 文件，将添加前 {min(5, len(json_files))} 个")
        
        added_count = 0
        paper_ids = []
        texts = []
        
        for json_file in json_files:
            try:
                # 加载 JSON 数据
                paper_data = self._load_test_json(json_file.name)
                work_id = paper_data.get('work_id')
                
                if not work_id:
                    print(f"  ⚠️  跳过缺少 work_id 的文件: {json_file.name}")
                    continue
                
                # 检查是否已存在（避免重复插入）
                existing_paper = self.__class__.metadata_db.get_paper_info_by_work_id(work_id)
                if existing_paper:
                    paper_id = existing_paper['paper_id']
                    print(f"  ℹ️  论文已存在: work_id={work_id}, paper_id={paper_id}")
                else:
                    # 插入数据库获取 paper_id
                    paper_id = self.__class__.metadata_db.insert_paper(paper_data)
                    print(f"  ✅ 插入数据库: work_id={work_id}, paper_id={paper_id}")
                
                # 提取文本用于 embedding
                text = self._extract_text_for_embedding(paper_data)
                if not text:
                    print(f"  ⚠️  跳过文本为空的文件: {json_file.name}")
                    continue
                
                paper_ids.append(paper_id)
                texts.append(text)
                added_count += 1
                
            except Exception as e:
                print(f"  ❌ 处理文件失败 {json_file.name}: {e}")
                continue
        
        if not paper_ids:
            self.skipTest("没有成功准备任何测试数据")
        
        # 批量添加到 vector_db
        try:
            # 使用第一个可写 shard
            writable_shard_ids = self.__class__.writable_shard_ids
            if not writable_shard_ids:
                self.skipTest("没有可用的可写 shard")
            
            shard_id = writable_shard_ids[0]
            print(f"\n正在添加 {len(paper_ids)} 个文档到 vector_db...")
            result = vector_db.add(
                ids=paper_ids,
                texts=texts,
                shard_id=shard_id,  # 使用第一个可写 shard
                auto_save=True  # 自动保存
            )
            
            self.assertTrue(result, "添加文档应该返回 True")
            self.__class__.added_paper_ids.extend(paper_ids)
            
            print(f"  ✅ 成功添加 {len(paper_ids)} 个文档到 vector_db")
            print(f"  ✅ 已保存到 shard {shard_id}")
            
        except Exception as e:
            self.fail(f"添加文档到 vector_db 失败: {e}")
        
        print("✅ 测试0完成：成功添加测试数据")
    '''

    def test_1_shard_loading(self):
        """测试1：验证 shard 加载功能"""
        print("\n" + "-" * 60)
        print("测试1：验证 shard 加载功能")
        print("-" * 60)
        
        vector_db = self.__class__.vector_db
        
        # 验证 VectorDB 已初始化
        self.assertIsNotNone(vector_db, "VectorDB 实例应该已初始化")
        
        # 验证 shard registry 中有已加载的 shard
        loaded_shards = vector_db.registry.list_shards()
        self.assertGreater(len(loaded_shards), 0, "应该至少加载一个 shard")
        
        print(f"  ✅ 成功加载 {len(loaded_shards)} 个 shard")
        for shard_id in sorted(loaded_shards):
            shard = vector_db.registry.get(shard_id)
            if shard and shard.is_loaded:
                print(f"    - Shard {shard_id}: 已加载")
            else:
                print(f"    - Shard {shard_id}: 未加载")
        
        print("✅ 测试1完成：shard 加载功能正常")
    
    def test_2_search_from_multi_shards(self):
        """测试2：多 shard 搜索功能"""
        print("\n" + "-" * 60)
        print("测试2：多 shard 搜索功能")
        print("-" * 60)
        
        vector_db = self.__class__.vector_db
        
        # 测试查询
        test_queries = [
            "alphafold",
            "neural network",
            "Structure of the human Meckel-Gruber protein Meckelin",
            "Voltage-gated sodium (Nav) channels",
            "Teaching robots the art of human social synchrony."
        ]
        
        # 合并 readonly 和 writable shard_ids 用于搜索
        search_shard_ids = []
        if self.__class__.readonly_shard_ids:
            search_shard_ids.extend(self.__class__.readonly_shard_ids)
        if self.__class__.writable_shard_ids:
            search_shard_ids.extend(self.__class__.writable_shard_ids)
        
        if not search_shard_ids:
            self.skipTest("没有可用的 shard 进行搜索")
        
        for query in test_queries:
            print(f"\n查询: '{query}'")
            
            try:
                # 执行多 shard 搜索（必须指定 shard_ids）
                results = vector_db.search(
                    query=query,
                    top_k=10,
                    shard_ids=search_shard_ids
                )
                
                # 验证返回结果格式
                self.assertIsInstance(results, list, "search 应该返回列表")
                
                if len(results) > 0:
                    # 验证结果数量不超过 top_k
                    self.assertLessEqual(len(results), 10, "结果数量应该不超过 top_k (10)")
                    
                    # 验证结果格式：每个结果应该是 (VectorEntry, similarity_score) 元组
                    for i, result in enumerate(results):
                        self.assertIsInstance(result, tuple, f"结果 {i} 应该是元组")
                        self.assertEqual(len(result), 2, f"结果 {i} 应该包含 2 个元素")
                        
                        from docset_hub.storage.vector_db import VectorEntry
                        entry, score = result
                        self.assertIsInstance(entry, VectorEntry, f"结果 {i} 的第一个元素应该是 VectorEntry")
                        self.assertIsNotNone(entry.work_id, f"结果 {i} 的 work_id 不应为空")
                        self.assertIsInstance(entry.text, str, f"结果 {i} 的 text 应该是字符串")
                        self.assertIsInstance(score, (int, float), f"结果 {i} 的相似度分数应该是数字")
                    
                    # 验证结果按相似度降序排列（相似度应该递减或相等）
                    scores = [score for _, score in results]
                    for i in range(len(scores) - 1):
                        self.assertGreaterEqual(scores[i], scores[i + 1],
                                              f"结果应该按相似度降序排列，但 {i} 和 {i+1} 的顺序不正确")
                    
                    print(f"  ✅ 返回 {len(results)} 个结果")
                    print(f"  ✅ 相似度分数范围: {min(scores):.4f} - {max(scores):.4f}")
                    print(f"  ✅ 结果按相似度降序排列")
                    
                    # 显示前3个结果
                    for i, (entry, score) in enumerate(results[:3]):
                        print(f"    [{i+1}] work_id: {entry.work_id}, score: {score:.4f}")
                        print(f"        text: {entry.text[:100]}...")  # 只显示前100个字符
                else:
                    print(f"  ⚠️  查询返回空结果（可能是正常情况）")
                
            except Exception as e:
                self.fail(f"多 shard 搜索失败: {e}")
        
        print("\n✅ 测试2完成：多 shard 搜索功能正常")
    
    def test_3_search_from_multi_shards_with_top_k(self):
        """测试3：多 shard 搜索功能（测试不同的 top_k 值）"""
        print("\n" + "-" * 60)
        print("测试3：多 shard 搜索功能（测试不同的 top_k 值）")
        print("-" * 60)
        
        vector_db = self.__class__.vector_db
        
        query = "machine learning"
        top_k_values = [5, 10, 20]
        
        # 合并 readonly 和 writable shard_ids 用于搜索
        search_shard_ids = []
        if self.__class__.readonly_shard_ids:
            search_shard_ids.extend(self.__class__.readonly_shard_ids)
        if self.__class__.writable_shard_ids:
            search_shard_ids.extend(self.__class__.writable_shard_ids)
        
        if not search_shard_ids:
            self.skipTest("没有可用的 shard 进行搜索")
        
        for top_k in top_k_values:
            print(f"\n测试 top_k={top_k}")
            
            try:
                # 使用新的 search 方法（必须指定 shard_ids）
                results = vector_db.search(
                    query=query,
                    top_k=top_k,
                    shard_ids=search_shard_ids
                )
                
                self.assertIsInstance(results, list, "应该返回列表")
                self.assertLessEqual(len(results), top_k, f"结果数量应该不超过 top_k ({top_k})")
                
                print(f"  ✅ 返回 {len(results)} 个结果（最多 {top_k} 个）")
                
            except Exception as e:
                self.fail(f"多 shard 搜索失败 (top_k={top_k}): {e}")
        
        print("\n✅ 测试3完成：多 shard 搜索功能（不同 top_k 值）正常")


if __name__ == '__main__':
    # 设置日志级别
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    unittest.main()

