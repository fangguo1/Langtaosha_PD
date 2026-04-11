#!/usr/bin/env python3
"""
Step 2: Embedding Worker（多进程并行计算）

读取 Arrow batch 文件，使用 GritLM 模型计算 embedding，输出为 .emb.npz 文件。
此阶段不更新 PostgreSQL，仅产生文件系统输出。

支持多进程并行处理，每个进程对应一个 GPU。
"""

import sys
import os
import logging
import multiprocessing
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from multiprocessing import Process, Queue

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.config_loader import set_env_from_config

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


def _get_available_gpu_count() -> int:
    """检测可用的 GPU 数量
    
    Returns:
        int: 可用 GPU 数量，如果没有 GPU 则返回 0
    """
    try:
        import torch
        if torch.cuda.is_available():
            return torch.cuda.device_count()
    except ImportError:
        # 如果没有安装 torch，尝试使用 nvidia-smi
        try:
            import subprocess
            result = subprocess.run(
                ['nvidia-smi', '--list-gpus'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                # 计算输出行数（每行一个 GPU）
                return len([line for line in result.stdout.strip().split('\n') if line.strip()])
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
            pass
    
    return 0


def _embedding_worker_process(
    gpu_id: Optional[int],
    model_path: Optional[str],
    model_name: str,
    task_queue: Queue,
    result_queue: Queue
):
    """Embedding Worker 进程函数
    
    每个进程绑定到一个 GPU，在启动时加载模型，然后持续从队列中获取任务并处理。
    
    Args:
        gpu_id: GPU ID（如果指定，设置 CUDA_VISIBLE_DEVICES 为该 GPU）
        model_path: 模型路径
        model_name: 模型名称
        task_queue: 任务队列，每个任务是 (batch_path, output_path) 元组
        result_queue: 结果队列，返回处理结果
    """
    # 设置 GPU（必须在导入模型之前设置）
    if gpu_id is not None:
        os.environ['CUDA_VISIBLE_DEVICES'] = str(gpu_id)
        logger.info(f"[GPU {gpu_id}] Worker 进程启动，设置 CUDA_VISIBLE_DEVICES={gpu_id}")
    else:
        logger.info("Worker 进程启动（CPU 模式）")
    
    try:
        # 创建 worker 实例（会加载模型到当前可见的 GPU）
        # 注意：必须在设置 CUDA_VISIBLE_DEVICES 之后创建
        worker = EmbeddingWorker(
            model_path=model_path,
            model_name=model_name
        )
        logger.info(f"[GPU {gpu_id}] 模型加载完成，开始处理任务")
        
        # 持续从队列中获取任务并处理
        import queue
        while True:
            try:
                # 从队列获取任务（timeout 用于检查是否有停止信号）
                task = task_queue.get(timeout=1)
                
                # 检查停止信号
                if task is None:
                    logger.info(f"[GPU {gpu_id}] 收到停止信号，退出")
                    break
                
                batch_path, output_path = task
                batch_name = Path(batch_path).name
                
                try:
                    # 处理 batch
                    worker.process_batch(Path(batch_path), Path(output_path))
                    
                    # 返回成功结果
                    result_queue.put({
                        'success': True,
                        'batch_name': batch_name,
                        'gpu_id': gpu_id,
                        'error': None
                    })
                    logger.info(f"[GPU {gpu_id}] ✓ 完成: {batch_name}")
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"[GPU {gpu_id}] ✗ 失败: {batch_name}: {error_msg}", exc_info=True)
                    
                    # 返回失败结果
                    result_queue.put({
                        'success': False,
                        'batch_name': batch_name,
                        'gpu_id': gpu_id,
                        'error': error_msg
                    })
                    
            except queue.Empty:
                # 队列为空，继续等待（用于检查停止信号）
                continue
            except Exception as e:
                logger.error(f"[GPU {gpu_id}] Worker 进程异常: {e}", exc_info=True)
                break
                
    except Exception as e:
        logger.error(f"[GPU {gpu_id}] Worker 进程初始化失败: {e}", exc_info=True)
        result_queue.put({
            'success': False,
            'batch_name': 'worker_init',
            'gpu_id': gpu_id,
            'error': f"Worker 初始化失败: {str(e)}"
        })


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
                text = f"{title} {abstract}".strip()
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
            embeddings=embeddings_array
            #texts=np.array(texts, dtype=object)  # 用于调试
        )
        
        logger.info(f"  ✓ 保存完成: {output_path.name} ({len(paper_ids)} 条记录)")
        
        return output_path
    
    def process_shard(
        self, 
        shard_id: int, 
        batches_dir: Path, 
        output_dir: Path
    ):
        """处理整个 shard 的所有 batch 文件（串行模式）
        
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
        
        # 过滤已处理的 batch（支持断点续传）
        pending_batches = []
        for batch_file in batch_files:
            output_filename = f"{batch_file.stem}.emb.npz"
            output_path = output_shard_dir / output_filename
            
            if output_path.exists():
                logger.info(f"  跳过已处理的 batch: {batch_file.name}")
            else:
                pending_batches.append((batch_file, output_path))
        
        if not pending_batches:
            logger.info(f"Shard {shard_id:03d} 所有 batch 已处理完成")
            return
        
        logger.info(f"待处理 batch 数量: {len(pending_batches)}")
        logger.info("使用串行模式处理")
        
        for batch_file, output_path in pending_batches:
            try:
                self.process_batch(batch_file, output_path)
            except Exception as e:
                logger.error(f"处理 batch 失败 {batch_file.name}: {e}", exc_info=True)
                # 继续处理下一个 batch


def process_shard_parallel(
    shard_id: int,
    batches_dir: Path,
    output_dir: Path,
    gpu_ids: List[int],
    model_path: Optional[str],
    model_name: str
):
    """并行处理 shard 的独立函数（不依赖 EmbeddingWorker 实例）
    
    为每个 GPU 启动一个独立的 worker 进程，每个 worker 绑定到特定的 GPU。
    
    Args:
        shard_id: Shard ID
        batches_dir: Batches 目录
        output_dir: 输出目录
        gpu_ids: GPU ID 列表（worker 数量 = len(gpu_ids)，一个 worker 对应一个 GPU）
        model_path: 模型路径
        model_name: 模型名称
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
    
    # 过滤已处理的 batch（支持断点续传）
    pending_batches = []
    for batch_file in batch_files:
        output_filename = f"{batch_file.stem}.emb.npz"
        output_path = output_shard_dir / output_filename
        
        if output_path.exists():
            logger.info(f"  跳过已处理的 batch: {batch_file.name}")
        else:
            pending_batches.append((batch_file, output_path))
    
    if not pending_batches:
        logger.info(f"Shard {shard_id:03d} 所有 batch 已处理完成")
        return
    
    logger.info(f"待处理 batch 数量: {len(pending_batches)}")
    logger.info(f"使用并行模式处理，启动 {len(gpu_ids)} 个 Worker 进程（每个对应一个 GPU）")
    logger.info(f"使用的 GPU ID: {gpu_ids}")
    
    # 创建任务队列和结果队列
    task_queue = Queue()
    result_queue = Queue()
    
    # 启动 worker 进程（每个 GPU 一个进程）
    worker_processes = []
    for gpu_id in gpu_ids:
        p = Process(
            target=_embedding_worker_process,
            args=(gpu_id, model_path, model_name, task_queue, result_queue)
        )
        p.start()
        worker_processes.append((p, gpu_id))
        logger.info(f"启动 Worker 进程 (GPU {gpu_id}), PID: {p.pid}")
    
    # 将所有任务放入队列
    for batch_file, output_path in pending_batches:
        task_queue.put((str(batch_file), str(output_path)))
    
    # 发送停止信号（每个 worker 一个）
    for _ in worker_processes:
        task_queue.put(None)
    
    # 收集结果
    success_count = 0
    fail_count = 0
    failed_batches = []
    completed = 0
    total = len(pending_batches)
    
    # 等待所有任务完成
    from multiprocessing.queues import Empty as MPQueueEmpty
    
    while completed < total:
        try:
            # 使用较长的超时时间（每个batch可能需要几分钟）
            result = result_queue.get(timeout=600)  # 10分钟超时
            completed += 1
            
            if result['success']:
                success_count += 1
                logger.info(f"[{completed}/{total}] ✓ {result['batch_name']} (GPU {result['gpu_id']})")
            else:
                fail_count += 1
                failed_batches.append((result['batch_name'], result['error']))
                logger.error(f"[{completed}/{total}] ✗ {result['batch_name']} (GPU {result['gpu_id']}): {result['error']}")
        except MPQueueEmpty:
            # 超时后，检查是否所有worker进程都已完成
            alive_workers = [p for p, _ in worker_processes if p.is_alive()]
            if not alive_workers:
                # 所有worker都已结束，尝试获取剩余结果（非阻塞）
                logger.info(f"所有 Worker 进程已结束，尝试获取剩余结果...")
                remaining = total - completed
                for _ in range(remaining):
                    try:
                        result = result_queue.get_nowait()
                        completed += 1
                        if result['success']:
                            success_count += 1
                            logger.info(f"[{completed}/{total}] ✓ {result['batch_name']} (GPU {result['gpu_id']})")
                        else:
                            fail_count += 1
                            failed_batches.append((result['batch_name'], result['error']))
                            logger.error(f"[{completed}/{total}] ✗ {result['batch_name']} (GPU {result['gpu_id']}): {result['error']}")
                    except MPQueueEmpty:
                        break
                break
            else:
                # 还有worker在运行，继续等待
                logger.warning(f"等待结果超时，已完成 {completed}/{total}，还有 {len(alive_workers)} 个 Worker 在运行，继续等待...")
                continue
        except Exception as e:
            logger.error(f"收集结果时发生异常: {e}", exc_info=True)
            raise
    
    # 等待所有 worker 进程结束
    logger.info("等待所有 Worker 进程结束...")
    for p, gpu_id in worker_processes:
        p.join(timeout=60)  # 最多等待60秒
        if p.is_alive():
            logger.warning(f"Worker 进程 (GPU {gpu_id}) 未正常结束，强制终止")
            p.terminate()
            p.join()
        else:
            logger.info(f"Worker 进程 (GPU {gpu_id}) 已正常结束")
    
    # 再次尝试获取剩余结果（防止有结果在worker结束前未到达）
    if completed < total:
        logger.info(f"尝试获取剩余结果，已完成 {completed}/{total}...")
        remaining = total - completed
        for _ in range(remaining):
            try:
                result = result_queue.get_nowait()
                completed += 1
                if result['success']:
                    success_count += 1
                    logger.info(f"[{completed}/{total}] ✓ {result['batch_name']} (GPU {result['gpu_id']})")
                else:
                    fail_count += 1
                    failed_batches.append((result['batch_name'], result['error']))
                    logger.error(f"[{completed}/{total}] ✗ {result['batch_name']} (GPU {result['gpu_id']}): {result['error']}")
            except MPQueueEmpty:
                break
    
    # 输出统计信息
    logger.info(f"Shard {shard_id:03d} 处理完成: 成功 {success_count}, 失败 {fail_count}")
    if failed_batches:
        logger.warning(f"失败的 batch ({len(failed_batches)} 个):")
        for batch_name, error in failed_batches[:10]:  # 只显示前10个
            logger.warning(f"  - {batch_name}: {error}")
        if len(failed_batches) > 10:
            logger.warning(f"  ... 还有 {len(failed_batches) - 10} 个失败")



#python3 embed_worker_mp.py --shard-id 0 --base-dir /data3/guofang/remote_storage_home_10.0.4.7/pubmed_database/pubmed_vector_db_test_mini_multi_shard/ --config-path /data3/guofang/PD_TEST/config/config_backend_server.yaml --workers 2 --gpu-ids 5,6
#python3 embed_worker_mp.py --shard-id 0 --base-dir /data3/guofang/remote_storage_home_10.0.4.7/pubmed_database/pubmed_vector_db_test/ --config-path /data3/guofang/PD_TEST/config/config_backend_server.yaml --workers 4 --gpu-ids 2,3,4,5

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Embedding Worker（多进程并行）：计算 Arrow batch 的 embedding（Step 2）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 串行处理（默认）
  python embed_worker_mp.py --shard-id 0 --base-dir /path/to/base
  
  # 使用 4 个 worker 并行处理（自动分配 GPU）
  python embed_worker_mp.py --shard-id 0 --base-dir /path/to/base --workers 4
  
  # 指定使用的 GPU ID
  python embed_worker_mp.py --shard-id 0 --base-dir /path/to/base --workers 4 --gpu-ids 0,1,2,3
  
  # 自动使用所有可用 GPU
  python embed_worker_mp.py --shard-id 0 --base-dir /path/to/base --auto-gpu
        """
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
    
    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='并行 worker 数量（默认: 1，串行处理）。建议设置为可用 GPU 数量'
    )
    
    parser.add_argument(
        '--gpu-ids',
        type=str,
        default=None,
        help='指定使用的 GPU ID 列表，用逗号分隔（例如: 0,1,2,3）。如果不指定，将自动检测并使用所有可用 GPU'
    )
    
    parser.add_argument(
        '--auto-gpu',
        action='store_true',
        help='自动检测并使用所有可用 GPU（worker 数量将等于 GPU 数量）'
    )
    
    args = parser.parse_args()
    
    # 加载配置
    if args.config_path:
        config_path = Path(args.config_path)
        if config_path.exists():
            set_env_from_config(override=True, config_path=config_path)
    
    # 确定目录
    base_dir = Path(args.base_dir)
    batches_dir = base_dir / "batches"
    output_dir = base_dir / "embeddings"
    
    # 处理 GPU 相关参数
    gpu_ids = None
    workers = args.workers
    
    if args.auto_gpu:
        # 自动检测 GPU 数量
        available_gpus = _get_available_gpu_count()
        if available_gpus == 0:
            logger.warning("未检测到可用 GPU，将使用 CPU 模式（workers=1）")
            workers = 1
        else:
            workers = available_gpus
            gpu_ids = list(range(available_gpus))
            logger.info(f"自动检测到 {available_gpus} 个 GPU，将使用 {workers} 个 worker")
    elif args.gpu_ids:
        # 解析指定的 GPU ID 列表
        try:
            gpu_ids = [int(x.strip()) for x in args.gpu_ids.split(',')]
            logger.info(f"指定使用 GPU: {gpu_ids}")
            # 如果未指定 workers，使用 GPU 数量
            if workers == 1:
                workers = len(gpu_ids)
        except ValueError:
            logger.error(f"无效的 GPU ID 格式: {args.gpu_ids}")
            sys.exit(1)
    
    try:
        if args.batch_file:
            # 处理单个文件（不支持并行，需要创建 worker）
            worker = EmbeddingWorker(
                model_path=args.model_path,
                model_name=args.model_name,
                batches_dir=batches_dir,
                output_dir=output_dir,
                shard_id=args.shard_id
            )
            batch_path = Path(args.batch_file)
            output_path = worker.process_batch(batch_path)
            logger.info(f"处理完成: {output_path}")
        elif args.shard_id is not None:
            # 处理整个 shard
            if workers == 1:
                # 串行模式：创建 worker 实例
                worker = EmbeddingWorker(
                    model_path=args.model_path,
                    model_name=args.model_name,
                    batches_dir=batches_dir,
                    output_dir=output_dir,
                    shard_id=args.shard_id
                )
                worker.process_shard(args.shard_id, batches_dir, output_dir)
            else:
                # 并行模式：不创建 worker，直接启动多个 worker 进程
                # 确定 GPU ID 列表
                if gpu_ids is None:
                    # 自动检测可用 GPU
                    available_gpus = _get_available_gpu_count()
                    if available_gpus == 0:
                        logger.warning("未检测到可用 GPU，将使用 CPU 模式（性能较低）")
                        gpu_ids = [None] * workers
                    else:
                        # 使用前 workers 个 GPU
                        gpu_ids = list(range(min(workers, available_gpus)))
                        if workers > available_gpus:
                            logger.warning(f"Worker 数量 ({workers}) 超过可用 GPU 数量 ({available_gpus})，只使用前 {available_gpus} 个 GPU")
                            workers = available_gpus
                            gpu_ids = list(range(available_gpus))
                
                # 确保 worker 数量 = GPU 数量
                if len(gpu_ids) != workers:
                    logger.warning(f"调整 worker 数量从 {workers} 到 {len(gpu_ids)} 以匹配 GPU 数量")
                    workers = len(gpu_ids)
                
                # 调用并行处理函数（不创建 worker 实例）
                process_shard_parallel(
                    args.shard_id,
                    batches_dir,
                    output_dir,
                    gpu_ids,
                    args.model_path,
                    args.model_name
                )
            logger.info(f"Shard {args.shard_id:03d} 处理完成")
        else:
            logger.error("请指定 --batch-file 或 --shard-id")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"处理失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    # 设置多进程启动方法（兼容性）
    if sys.platform != 'win32':
        multiprocessing.set_start_method('spawn', force=True)
    
    main()

