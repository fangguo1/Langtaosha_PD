#!/usr/bin/env python3
"""
Step 2: Embedding Worker（并行计算）

读取 Arrow batch 文件，使用 GritLM 模型计算 embedding，输出为 .emb.npz 文件。
此阶段不更新 PostgreSQL，仅产生文件系统输出。
"""

import sys
import logging
from pathlib import Path
from typing import List, Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import numpy as np
except ImportError:
    raise ImportError("请安装 pyarrow 和 numpy: pip install pyarrow numpy")

# 导入 GritLM embeddings
from docset_hub.storage.vector_db import GritLMEmbeddings

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmbeddingWorker:
    """Embedding Worker
    
    处理 Arrow batch 文件，计算 embedding 并输出为 NPZ 格式。
    """
    
    def __init__(
        self,
        model_path: Optional[str] = None,
        model_name: str = 'GritLM/GritLM-7B',
        batches_dir: Optional[Path] = None,
        output_dir: Optional[Path] = None,
        shard_id: Optional[int] = None
    ):
        """初始化 Embedding Worker
        
        Args:
            model_path: GritLM 模型本地路径（优先使用）
            model_name: GritLM 模型名称（HuggingFace 名称）
            batches_dir: Arrow batches 目录（默认: base_dir/batches）
            output_dir: Embedding 输出目录（默认: base_dir/embeddings）
            shard_id: Shard ID（如果指定，只处理该 shard）
        """
        # 初始化 embedding 模型
        logger.info(f"初始化 GritLM 模型: model_path={model_path}, model_name={model_name}")
        self.embeddings = GritLMEmbeddings(model_name=model_name, model_path=model_path)
        logger.info("GritLM 模型加载完成")
        
        self.batches_dir = Path(batches_dir) if batches_dir else None
        self.output_dir = Path(output_dir) if output_dir else None
        self.shard_id = shard_id
        
        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def read_arrow_batch(self, batch_path: Path) -> List[dict]:
        """读取 Arrow batch 文件
        
        Args:
            batch_path: Arrow 文件路径
            
        Returns:
            List[dict]: 数据列表，每个元素包含 paper_id, work_id, title, abstract
        """
        try:
            with pa.OSFile(str(batch_path), 'rb') as source:
                reader = ipc.open_stream(source)
                table = reader.read_all()
                
                # 转换为 Python 列表
                data = table.to_pylist()
                return data
        except Exception as e:
            logger.error(f"读取 Arrow 文件失败 {batch_path}: {e}")
            raise
    
    def process_batch(self, batch_path: Path, output_path: Optional[Path] = None) -> Path:
        """处理单个 batch 文件
        
        Args:
            batch_path: 输入的 Arrow batch 文件路径
            output_path: 输出的 NPZ 文件路径（如果为 None，自动生成）
            
        Returns:
            Path: 输出的 NPZ 文件路径
        """
        logger.info(f"处理 batch: {batch_path.name}")
        
        # 读取 Arrow 文件
        batch_data = self.read_arrow_batch(batch_path)
        
        if not batch_data:
            raise ValueError(f"Batch 文件为空: {batch_path}")
        
        # 提取文本（使用 title + abstract，如果 abstract 为空则只用 title）
        texts = []
        paper_ids = []
        work_ids = []
        
        for item in batch_data:
            paper_id = item.get('paper_id')
            work_id = item.get('work_id', '')
            title = item.get('title', '')
            abstract = item.get('abstract', '')
            
            # 组合文本（title + abstract，用空格分隔）
            if abstract:
                text = f"{title} . {abstract}".strip()
            else:
                text = title.strip()
            
            if not text:
                logger.warning(f"Paper {paper_id} 没有文本内容，跳过")
                continue
            
            texts.append(text)
            paper_ids.append(paper_id)
            work_ids.append(work_id)
        
        if not texts:
            raise ValueError(f"Batch 中没有有效文本: {batch_path}")
        
        logger.info(f"  提取 {len(texts)} 条文本，开始计算 embedding...")
        
        # 计算 embedding
        embeddings_list = self.embeddings.embed_documents(texts)
        
        # 转换为 numpy array
        embeddings_array = np.array(embeddings_list, dtype=np.float32)
        
        logger.info(f"  Embedding 计算完成: shape={embeddings_array.shape}")
        
        # 生成输出路径
        if output_path is None:
            if self.output_dir is None:
                # 使用输入文件的目录结构
                output_dir = batch_path.parent.parent / "embeddings" / batch_path.parent.name
                output_dir.mkdir(parents=True, exist_ok=True)
            else:
                # 使用指定的输出目录，保持 shard 目录结构
                if self.shard_id is not None:
                    output_dir = self.output_dir / f"shard_{self.shard_id:03d}"
                else:
                    # 从 batch_path 推断 shard_id
                    shard_dir_name = batch_path.parent.name
                    output_dir = self.output_dir / shard_dir_name
                output_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成输出文件名
            batch_filename = batch_path.stem  # batch_000001
            output_filename = f"{batch_filename}.emb.npz"
            output_path = output_dir / output_filename
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存为 NPZ 格式
        np.savez_compressed(
            str(output_path),
            paper_ids=np.array(paper_ids, dtype=np.int64),
            work_ids=np.array(work_ids, dtype=object),
            embeddings=embeddings_array,
            texts=np.array(texts, dtype=object)  # 用于调试
        )
        
        logger.info(f"  ✓ 保存完成: {output_path.name} ({len(paper_ids)} 条记录)")
        
        return output_path
    
    def process_shard(self, shard_id: int, batches_dir: Path, output_dir: Path):
        """处理整个 shard 的所有 batch 文件
        
        Args:
            shard_id: Shard ID
            batches_dir: Batches 目录
            output_dir: 输出目录
        """
        shard_dir = batches_dir / f"shard_{shard_id:03d}"
        if not shard_dir.exists():
            logger.warning(f"Shard 目录不存在: {shard_dir}")
            return
        
        batch_files = sorted(shard_dir.glob("batch_*.arrow"))
        if not batch_files:
            logger.warning(f"Shard {shard_id:03d} 没有 batch 文件")
            return
        
        logger.info(f"处理 Shard {shard_id:03d}: {len(batch_files)} 个 batch 文件")
        
        output_shard_dir = output_dir / f"shard_{shard_id:03d}"
        output_shard_dir.mkdir(parents=True, exist_ok=True)
        
        for batch_file in batch_files:
            try:
                # 检查输出文件是否已存在（支持断点续传）
                output_filename = f"{batch_file.stem}.emb.npz"
                output_path = output_shard_dir / output_filename
                
                if output_path.exists():
                    logger.info(f"  跳过已处理的 batch: {batch_file.name}")
                    continue
                
                self.process_batch(batch_file, output_path)
            except Exception as e:
                logger.error(f"处理 batch 失败 {batch_file.name}: {e}", exc_info=True)
                # 继续处理下一个 batch
        
        logger.info(f"Shard {shard_id:03d} 处理完成")


# python3 embed_worker.py --shard-id 0 --base-dir /data3/guofang/remote_storage_home_10.0.4.7/pubmed_database/pubmed_vector_db_test/ --config-path /data3/guofang/PD_TEST/config/config_backend_server.yaml
# PD_TEST/scripts/import_vector_db/embed_worker.py --shard-id 0 --base-dir /data3/guofang/remote_storage_home_10.0.4.7/pubmed_database/pubmed_vector_db_test/ --config-path /data3/guofang/PD_TEST/config/config_backend_server.yaml

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Embedding Worker：计算 Arrow batch 的 embedding（Step 2）',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--batch-file',
        type=str,
        default=None,
        help='单个 batch 文件路径（处理单个文件）'
    )
    
    parser.add_argument(
        '--shard-id',
        type=int,
        default=None,
        help='Shard ID（处理整个 shard）'
    )
    
    parser.add_argument(
        '--base-dir',
        type=str,
        default=None,
        help='base 目录'
    )
    
    
    parser.add_argument(
        '--model-path',
        type=str,
        default=None,
        help='GritLM 模型本地路径'
    )
    
    parser.add_argument(
        '--model-name',
        type=str,
        default='GritLM/GritLM-7B',
        help='GritLM 模型名称（默认: GritLM/GritLM-7B）'
    )
    
    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径'
    )
    
    args = parser.parse_args()
    
    # 确定目录
    base_dir = Path(args.base_dir)
    batches_dir = base_dir / "batches"
    output_dir = base_dir / "embeddings"
    
    # 初始化 worker
    worker = EmbeddingWorker(
        model_path=args.model_path,
        model_name=args.model_name,
        batches_dir=batches_dir,
        output_dir=output_dir,
        shard_id=args.shard_id
    )
    
    try:
        if args.batch_file:
            # 处理单个文件
            batch_path = Path(args.batch_file)
            output_path = worker.process_batch(batch_path)
            logger.info(f"处理完成: {output_path}")
        elif args.shard_id is not None:
            # 处理整个 shard
            worker.process_shard(args.shard_id, batches_dir, output_dir)
            logger.info(f"Shard {args.shard_id:03d} 处理完成")
        else:
            logger.error("请指定 --batch-file 或 --shard-id")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"处理失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

