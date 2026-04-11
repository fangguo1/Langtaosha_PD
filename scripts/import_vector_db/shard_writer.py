#!/usr/bin/env python3
"""
Step 3: Shard Writer（FAISS 索引构建）

读取 embedding 文件，串行写入 FAISS 索引。
每个 shard 只允许一个 writer 进程，确保写入安全。
完成后更新 PostgreSQL 状态 1 → 2。
"""

import sys
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.config_loader import load_config_from_yaml, get_metadata_db_engine_from_config
from sqlalchemy import text

try:
    import numpy as np
    import faiss
except ImportError:
    raise ImportError("请安装 numpy 和 faiss: pip install numpy faiss-cpu (或 faiss-gpu)")

# 导入 manifest_manager（支持作为脚本运行和作为模块导入）
try:
    from .manifest_manager import ManifestManager
except ImportError:
    # 如果相对导入失败，使用绝对导入
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    try:
        from manifest_manager import ManifestManager
    except ImportError:
        # 最后尝试直接导入
        import import_vector_db
        from import_vector_db.manifest_manager import ManifestManager

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ShardWriter:
    """Shard Writer
    
    将 embedding 文件写入 FAISS 索引，每个 shard 一个索引文件。
    """
    
    def __init__(
        self,
        shard_id: int,
        embeddings_dir: Path,
        faiss_dir: Path,
        paper_id_range: Tuple[int, int],
        config: Dict[str, Any],
        vector_dim: int = 4096,
        checkpoint_interval: int = 10,
        update_status: bool = True
    ):
        """初始化 Shard Writer
        
        Args:
            shard_id: Shard ID
            embeddings_dir: Embeddings 目录（例如 embeddings/shard_000）
            faiss_dir: FAISS 输出目录（例如 faiss）
            paper_id_range: Paper ID 范围 (start_id, end_id)
            config: 配置字典
            vector_dim: 向量维度（默认: 4096，GritLM-7B）
            checkpoint_interval: Checkpoint 间隔（每处理多少个 batch 保存一次）
            update_status: 是否更新 PostgreSQL 状态（默认: True）
        """
        self.shard_id = shard_id
        self.embeddings_dir = Path(embeddings_dir)
        self.faiss_dir = Path(faiss_dir)
        self.paper_id_range = paper_id_range
        self.vector_dim = vector_dim
        self.checkpoint_interval = checkpoint_interval
        self.update_status = update_status
        
        # 目录结构
        self.shards_dir = self.faiss_dir / "shards"
        self.state_dir = self.faiss_dir / "state"
        self.shards_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # 文件路径
        self.index_path = self.shards_dir / f"shard_{shard_id:03d}.index"
        self.ids_path = self.shards_dir / f"shard_{shard_id:03d}.ids.npy"
        self.state_path = self.state_dir / f"shard_{shard_id:03d}.state.json"
        
        # 数据库引擎
        self.engine = get_metadata_db_engine_from_config(config)
        
        logger.info(f"初始化 ShardWriter: shard_id={shard_id}, paper_id_range={paper_id_range}")
    
    def load_checkpoint(self) -> Dict[str, Any]:
        """加载 checkpoint
        
        Returns:
            Dict: Checkpoint 数据，如果不存在返回空字典
        """
        if not self.state_path.exists():
            return {"processed_batches": [], "total_vectors": 0}
        
        try:
            with open(self.state_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"加载 checkpoint 失败: {e}，使用空 checkpoint")
            return {"processed_batches": [], "total_vectors": 0}
    
    def save_checkpoint(self, processed_batches: List[str], total_vectors: int):
        """保存 checkpoint
        
        Args:
            processed_batches: 已处理的 batch 文件名列表
            total_vectors: 总向量数
        """
        checkpoint = {
            "shard_id": f"{self.shard_id:03d}",
            "processed_batches": processed_batches,
            "total_vectors": total_vectors,
            "updated_at": json.dumps({}, default=str)  # 使用默认的 datetime 序列化
        }
        
        # 使用 datetime 的 ISO 格式
        from datetime import datetime
        checkpoint["updated_at"] = datetime.now().isoformat()
        
        with open(self.state_path, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, indent=2, ensure_ascii=False)
    
    def load_embedding_file(self, emb_path: Path) -> Tuple[np.ndarray, np.ndarray]:
        """加载 embedding 文件
        
        Args:
            emb_path: Embedding NPZ 文件路径
            
        Returns:
            Tuple[np.ndarray, np.ndarray]: (embeddings, paper_ids)
        """
        data = np.load(emb_path, allow_pickle=True)
        embeddings = data['embeddings']  # shape: [N, 4096]
        paper_ids = data['paper_ids']  # shape: [N]
        
        # 确保数据类型正确
        embeddings = embeddings.astype(np.float32)
        paper_ids = paper_ids.astype(np.int64)
        
        return embeddings, paper_ids
    
    def write_shard_index(self) -> bool:
        """写入 shard 索引
        
        Returns:
            bool: 是否成功
        """
        logger.info("=" * 60)
        logger.info(f"开始构建 Shard {self.shard_id:03d} 的 FAISS 索引")
        logger.info("=" * 60)
        
        # 查找所有 embedding 文件
        embedding_files = sorted(self.embeddings_dir.glob("batch_*.emb.npz"))
        if not embedding_files:
            logger.warning(f"没有找到 embedding 文件: {self.embeddings_dir}")
            return False
        
        logger.info(f"找到 {len(embedding_files)} 个 embedding 文件")
        
        # 加载 checkpoint
        checkpoint = self.load_checkpoint()
        processed_batches = set(checkpoint.get("processed_batches", []))
        start_vectors = checkpoint.get("total_vectors", 0)
        
        # 过滤已处理的文件
        remaining_files = [f for f in embedding_files if f.name not in processed_batches]
        
        if not remaining_files and len(processed_batches) == len(embedding_files):
            logger.info("所有 batch 已处理，跳过")
            return True
        
        if remaining_files:
            logger.info(f"从 checkpoint 恢复: 已处理 {len(processed_batches)} 个，剩余 {len(remaining_files)} 个")
        
        # 初始化或加载 FAISS 索引
        if self.index_path.exists() and start_vectors > 0:
            logger.info(f"加载现有索引: {self.index_path} ({start_vectors} 向量)")
            index = faiss.read_index(str(self.index_path))
            
            # 加载 paper_id 映射
            if self.ids_path.exists():
                all_paper_ids = np.load(self.ids_path)
            else:
                all_paper_ids = np.array([], dtype=np.int64)
        else:
            logger.info("创建新索引")
            # 创建 FAISS 索引（使用内积，因为向量已归一化）
            index = faiss.IndexFlatIP(self.vector_dim)
            all_paper_ids = np.array([], dtype=np.int64)
        
        # 处理每个 embedding 文件
        total_vectors = start_vectors
        processed_count = len(processed_batches)
        current_checkpoint_paper_ids = np.array([], dtype=np.int64)  # 本次 checkpoint 周期内的 paper_ids
        
        for i, emb_file in enumerate(remaining_files, 1):
            try:
                logger.info(f"处理 [{i}/{len(remaining_files)}] {emb_file.name}")
                
                # 加载 embedding
                embeddings, paper_ids = self.load_embedding_file(emb_file)
                
                # 添加到索引
                index.add(embeddings)
                
                # 追加 paper_id 映射
                all_paper_ids = np.concatenate([all_paper_ids, paper_ids])
                current_checkpoint_paper_ids = np.concatenate([current_checkpoint_paper_ids, paper_ids])
                
                total_vectors += len(embeddings)
                processed_batches.add(emb_file.name)
                processed_count += 1
                
                logger.info(f"  ✓ 添加 {len(embeddings)} 个向量，总计: {total_vectors}")
                
                # 定期保存 checkpoint 和索引
                if processed_count % self.checkpoint_interval == 0:
                    logger.info(f"  保存 checkpoint...")
                    faiss.write_index(index, str(self.index_path))
                    np.save(self.ids_path, all_paper_ids)
                    self.save_checkpoint(list(processed_batches), total_vectors)
                    
                    # 保存索引后，更新本次 checkpoint 周期内处理的 paper_ids 状态
                    if len(current_checkpoint_paper_ids) > 0 and self.update_status:
                        logger.info(f"  更新本次 checkpoint 的 {len(current_checkpoint_paper_ids)} 条记录状态...")
                        self.update_status_to_ready(current_checkpoint_paper_ids)
                        current_checkpoint_paper_ids = np.array([], dtype=np.int64)  # 清空本次周期的 paper_ids
            
            except Exception as e:
                logger.error(f"处理文件失败 {emb_file.name}: {e}", exc_info=True)
                # 继续处理下一个文件
        
        # 最终保存
        logger.info("保存最终索引...")
        faiss.write_index(index, str(self.index_path))
        np.save(self.ids_path, all_paper_ids)
        self.save_checkpoint(list(processed_batches), total_vectors)
        
        # 最终保存后，更新剩余的 paper_ids 状态
        if len(current_checkpoint_paper_ids) > 0 and self.update_status:
            logger.info(f"更新剩余的 {len(current_checkpoint_paper_ids)} 条记录状态...")
            self.update_status_to_ready(current_checkpoint_paper_ids)
        
        logger.info(f"✓ Shard {self.shard_id:03d} 索引构建完成: {total_vectors} 向量")
        logger.info(f"  索引文件: {self.index_path}")
        logger.info(f"  ID 映射: {self.ids_path}")
        
        return True
    
    def update_status_to_ready(self, paper_ids: np.ndarray):
        """更新 PostgreSQL 状态为 ready (2)
        
        根据实际处理的 paper_ids 批量更新状态和 shard_id 字段
        
        Args:
            paper_ids: 实际处理的 paper_id 数组
        """
        if len(paper_ids) == 0:
            logger.info("没有需要更新的 paper_ids，跳过状态更新")
            return
        
        logger.info(f"更新 PostgreSQL 状态 (shard_id={self.shard_id}, paper_ids 数量: {len(paper_ids)})")
        
        with self.engine.begin() as conn:
            # 使用 PostgreSQL 的 ANY 语法，避免 IN 子句参数限制
            result = conn.execute(text("""
                UPDATE papers
                SET embedding_status = 2,
                    shard_id = :shard_id
                WHERE embedding_status = 1
                  AND paper_id = ANY(:paper_ids)
            """), {
                "shard_id": self.shard_id,
                "paper_ids": paper_ids.tolist()
            })
            
            updated_count = result.rowcount
            logger.info(f"✓ 更新 {updated_count} 条记录的状态为 2 (ready), shard_id={self.shard_id}")



#python3 shard_writer.py --shard-id 0 --base-dir /home/wangyuanshi/pubmed_database/pubmed_vector_db_test_mini/ --config-path /home/wangyuanshi/remote_10.0.1.226/config/config_storage_server.yaml

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Shard Writer：构建 FAISS 索引（Step 3）',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--shard-id',
        type=int,
        required=True,
        help='Shard ID'
    )
    
    parser.add_argument(
        '--embeddings-dir',
        type=str,
        required=False,
        help='Embeddings 目录（例如 embeddings/shard_000）'
    )
    
    parser.add_argument(
        '--faiss-dir',
        type=str,
        default=None,
        help='FAISS 输出目录（默认: base-dir/faiss）'
    )
    
    parser.add_argument(
        '--base-dir',
        type=str,
        default='/mnt/lit_platform',
        help='基础目录路径（默认: /mnt/lit_platform）'
    )
    
    parser.add_argument(
        '--vector-dim',
        type=int,
        default=4096,
        help='向量维度（默认: 4096）'
    )
    
    parser.add_argument(
        '--checkpoint-interval',
        type=int,
        default=10,
        help='Checkpoint 间隔（默认: 10）'
    )
    
    parser.add_argument(
        '--no-status-update',
        action='store_true',
        help='不更新 PostgreSQL 状态'
    )
    
    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径'
    )
    
    args = parser.parse_args()
    
    # 加载配置
    if args.config_path:
        config_path = Path(args.config_path)
        if config_path.exists():
            config = load_config_from_yaml(config_path)
        else:
            logger.error(f"配置文件不存在: {config_path}")
            sys.exit(1)
    else:
        logger.error("请指定 --config-path")
        sys.exit(1)
    
    # 确定目录
    base_dir = Path(args.base_dir)
    faiss_dir = Path(args.faiss_dir) if args.faiss_dir else base_dir / "faiss"
    embeddings_dir = base_dir / "embeddings" / f"shard_{args.shard_id:03d}"

    batches_dir = base_dir / "batches"


    batches_manifest_path = batches_dir / "manifest.json"
    if not batches_manifest_path.exists():
        logger.error(f"Batches manifest 不存在: {batches_manifest_path}")
        logger.error("请先执行 Step 1")
        return
    
    import json
    with open(batches_manifest_path, 'r') as f:
        batches_manifest = json.load(f)
    
    shard_id=int(args.shard_id)
    
    shards = batches_manifest.get("shards", [])
    if shard_id is not None:
        shards = [s for s in shards if int(s["shard_id"]) == shard_id]


    for shard_info in shards:
        shard_id = int(shard_info["shard_id"])
        paper_id_range = shard_info["paper_id_range"]
        start_id, end_id = paper_id_range[0], paper_id_range[1]

    print(f"start_id: {start_id}, end_id: {end_id}")
    
    
    # 初始化 writer
    writer = ShardWriter(
        shard_id=args.shard_id,
        embeddings_dir=embeddings_dir,
        faiss_dir=faiss_dir,
        paper_id_range=(start_id, end_id),
        config=config,
        vector_dim=args.vector_dim,
        checkpoint_interval=args.checkpoint_interval,
        update_status=not args.no_status_update
    )
    
    try:
        # 构建索引（状态更新已在 write_shard_index 内部完成）
        success = writer.write_shard_index()
        
        if success:
            logger.info("Shard Writer 执行完成")
    
    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

