#!/usr/bin/env python3
"""
大规模文献向量化流水线主控脚本

编排整个流水线：Step 1 (导出) → Step 2 (Embedding) → Step 3 (索引构建)
支持分步执行或全流程执行。
"""

import sys
import logging
import argparse
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.config_loader import set_env_from_config

# 导入模块（支持作为脚本运行和作为模块导入）
import sys
import os
# 将当前目录添加到路径，以便导入同目录下的模块
_current_dir = Path(__file__).parent
if str(_current_dir) not in sys.path:
    sys.path.insert(0, str(_current_dir))

from export_batches import BatchExporter
from embed_worker import EmbeddingWorker
from shard_writer import ShardWriter
from manifest_manager import ManifestManager

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VectorizationPipeline:
    """向量化流水线
    
    管理整个向量化流程。
    """
    
    def __init__(
        self,
        base_dir: Path,
        batch_size: int = 10000,
        papers_per_shard: int = 2500000,
        model_path: Optional[str] = None,
        model_name: str = 'GritLM/GritLM-7B',
        vector_dim: int = 4096,
        limit: Optional[int] = None
    ):
        """初始化流水线
        
        Args:
            base_dir: 基础目录路径
            batch_size: Batch 大小
            papers_per_shard: 每个 shard 的文献数
            model_path: GritLM 模型路径
            model_name: GritLM 模型名称
            vector_dim: 向量维度
            limit: 导出文献数量上限（None 表示不限制，用于测试）
        """
        self.base_dir = Path(base_dir)
        self.batch_size = batch_size
        self.papers_per_shard = papers_per_shard
        self.model_path = model_path
        self.model_name = model_name
        self.vector_dim = vector_dim
        self.limit = limit
        
        # 目录结构
        self.batches_dir = self.base_dir / "batches"
        self.embeddings_dir = self.base_dir / "embeddings"
        self.faiss_dir = self.base_dir / "faiss"
        self.manifest_path = self.faiss_dir / "manifest.json"
        
        logger.info(f"初始化流水线: base_dir={base_dir}")
    
    def step1_export(self):
        """Step 1: 导出 Arrow Batches"""
        logger.info("=" * 60)
        logger.info("Step 1: PostgreSQL → Arrow Batches")
        logger.info("=" * 60)
        
        exporter = BatchExporter(
            base_dir=self.base_dir,
            batch_size=self.batch_size,
            papers_per_shard=self.papers_per_shard,
            limit=self.limit
        )
        
        stats = exporter.export_all()
        return stats
    
    def step2_embed(self, shard_id: Optional[int] = None, workers: int = 1):
        """Step 2: 计算 Embedding
        
        Args:
            shard_id: 如果指定，只处理该 shard；否则处理所有 shard
            workers: Worker 数量（目前单进程，未来可扩展为多进程）
        """
        logger.info("=" * 60)
        logger.info("Step 2: Arrow Batches → Embeddings")
        logger.info("=" * 60)
        
        # 加载 batches manifest 获取 shard 列表
        batches_manifest_path = self.batches_dir / "manifest.json"
        if not batches_manifest_path.exists():
            logger.error(f"Batches manifest 不存在: {batches_manifest_path}")
            logger.error("请先执行 Step 1")
            return
        
        import json
        with open(batches_manifest_path, 'r') as f:
            batches_manifest = json.load(f)
        
        shards = batches_manifest.get("shards", [])
        if shard_id is not None:
            shards = [s for s in shards if int(s["shard_id"]) == shard_id]
        
        if not shards:
            logger.warning("没有找到需要处理的 shard")
            return
        
        logger.info(f"处理 {len(shards)} 个 shard")
        
        # 处理每个 shard
        for shard_info in shards:
            shard_id = int(shard_info["shard_id"])
            shard_batches_dir = self.batches_dir / f"shard_{shard_id:03d}"
            
            if not shard_batches_dir.exists():
                logger.warning(f"Shard 目录不存在: {shard_batches_dir}")
                continue
            
            logger.info(f"处理 Shard {shard_id:03d}")
            
            worker = EmbeddingWorker(
                model_path=self.model_path,
                model_name=self.model_name,
                batches_dir=self.batches_dir,
                output_dir=self.embeddings_dir,
                shard_id=shard_id
            )
            
            worker.process_shard(shard_id, self.batches_dir, self.embeddings_dir)
        
        logger.info("Step 2 完成")
    
    def step3_write_index(self, shard_id: Optional[int] = None):
        """Step 3: 构建 FAISS 索引
        
        Args:
            shard_id: 如果指定，只处理该 shard；否则处理所有 shard
        """
        logger.info("=" * 60)
        logger.info("Step 3: Embeddings → FAISS Index")
        logger.info("=" * 60)
        
        # 加载 batches manifest 获取 shard 信息
        batches_manifest_path = self.batches_dir / "manifest.json"
        if not batches_manifest_path.exists():
            logger.error(f"Batches manifest 不存在: {batches_manifest_path}")
            logger.error("请先执行 Step 1")
            return
        
        import json
        with open(batches_manifest_path, 'r') as f:
            batches_manifest = json.load(f)
        
        shards = batches_manifest.get("shards", [])
        if shard_id is not None:
            shards = [s for s in shards if int(s["shard_id"]) == shard_id]
        
        if not shards:
            logger.warning("没有找到需要处理的 shard")
            return
        
        # 初始化 ManifestManager
        manifest_manager = ManifestManager(self.manifest_path)
        
        # 处理每个 shard
        for shard_info in shards:
            shard_id = int(shard_info["shard_id"])
            paper_id_range = shard_info["paper_id_range"]
            start_id, end_id = paper_id_range[0], paper_id_range[1]
            
            embeddings_shard_dir = self.embeddings_dir / f"shard_{shard_id:03d}"
            
            if not embeddings_shard_dir.exists():
                logger.warning(f"Embeddings shard 目录不存在: {embeddings_shard_dir}")
                continue
            
            logger.info(f"构建 Shard {shard_id:03d} 索引 (paper_id: {start_id} - {end_id})")
            
            writer = ShardWriter(
                shard_id=shard_id,
                embeddings_dir=embeddings_shard_dir,
                faiss_dir=self.faiss_dir,
                paper_id_range=(start_id, end_id),
                vector_dim=self.vector_dim
            )
            
            # 构建索引（状态更新已在 write_shard_index 内部完成）
            success = writer.write_shard_index()
            
            if success:
                # 更新 manifest
                index_path = f"shards/shard_{shard_id:03d}.index"
                ids_path = f"shards/shard_{shard_id:03d}.ids.npy"
                
                # 统计向量数（从索引文件读取）
                import faiss
                index = faiss.read_index(str(self.faiss_dir / index_path))
                total_vectors = index.ntotal
                
                manifest_manager.add_shard(
                    shard_id=f"{shard_id:03d}",
                    paper_id_range=paper_id_range,
                    index_path=index_path,
                    ids_path=ids_path,
                    total_vectors=total_vectors,
                    status="ready"
                )
        
        logger.info("Step 3 完成")
    
    def run_all(self, workers: int = 1):
        """运行完整流水线"""
        logger.info("=" * 60)
        logger.info("开始完整流水线执行")
        logger.info("=" * 60)
        
        # Step 1
        self.step1_export()
        
        # Step 2
        self.step2_embed(workers=workers)
        
        # Step 3
        self.step3_write_index()
        
        logger.info("=" * 60)
        logger.info("完整流水线执行完成")
        logger.info("=" * 60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='大规模文献向量化流水线',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 执行完整流水线
  python run_pipeline.py --step all --base-dir /mnt/lit_platform
  
  # 只执行 Step 1（小规模测试，限制导出 1000 条）
  python run_pipeline.py --step 1 --base-dir /mnt/lit_platform --limit 1000
  
  # 只执行 Step 2（处理特定 shard）
  python run_pipeline.py --step 2 --base-dir /mnt/lit_platform --shard-id 0
        """
    )
    
    parser.add_argument(
        '--step',
        type=str,
        choices=['1', '2', '3', 'all'],
        default='all',
        help='执行步骤（1=导出, 2=embedding, 3=索引构建, all=全部）'
    )
    
    parser.add_argument(
        '--base-dir',
        type=str,
        default='/mnt/lit_platform',
        help='基础目录路径（默认: /mnt/lit_platform）'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch 大小（默认: 10000）'
    )
    
    parser.add_argument(
        '--papers-per-shard',
        type=int,
        default=2500000,
        help='每个 shard 的文献数（默认: 2500000）'
    )
    
    parser.add_argument(
        '--model-path',
        type=str,
        default=None,
        help='GritLM 模型路径'
    )
    
    parser.add_argument(
        '--model-name',
        type=str,
        default='GritLM/GritLM-7B',
        help='GritLM 模型名称（默认: GritLM/GritLM-7B）'
    )
    
    parser.add_argument(
        '--vector-dim',
        type=int,
        default=4096,
        help='向量维度（默认: 4096）'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='导出文献数量上限（None 表示不限制，用于小规模测试）'
    )
    
    parser.add_argument(
        '--shard-id',
        type=int,
        default=None,
        help='指定 shard ID（仅用于 step 2 和 3）'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='Worker 数量（默认: 1，未来支持多进程）'
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
            set_env_from_config(override=True, config_path=config_path)
    
    # 初始化流水线
    pipeline = VectorizationPipeline(
        base_dir=Path(args.base_dir),
        batch_size=args.batch_size,
        papers_per_shard=args.papers_per_shard,
        model_path=args.model_path,
        model_name=args.model_name,
        vector_dim=args.vector_dim,
        limit=args.limit
    )
    
    try:
        if args.step == 'all':
            pipeline.run_all(workers=args.workers)
        elif args.step == '1':
            pipeline.step1_export()
        elif args.step == '2':
            pipeline.step2_embed(shard_id=args.shard_id, workers=args.workers)
        elif args.step == '3':
            pipeline.step3_write_index(shard_id=args.shard_id)
    
    except Exception as e:
        logger.error(f"流水线执行失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

