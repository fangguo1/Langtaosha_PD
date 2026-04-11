#!/usr/bin/env python3
"""
向量化流水线测试脚本

用于小规模测试各个步骤的功能。
"""

import sys
import logging
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.config_loader import set_env_from_config

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_export():
    """测试 Step 1: 导出"""
    logger.info("测试 Step 1: 导出 Arrow Batches")
    
    from import_vector_db.export_batches import BatchExporter
    
    # 使用小规模配置进行测试
    base_dir = Path("/tmp/test_vectorization")
    exporter = BatchExporter(
        base_dir=base_dir,
        batch_size=100,  # 小 batch 用于测试
        papers_per_shard=1000  # 小 shard 用于测试
    )
    
    try:
        stats = exporter.export_all()
        logger.info(f"导出测试完成: {stats}")
        return True
    except Exception as e:
        logger.error(f"导出测试失败: {e}", exc_info=True)
        return False


def test_embed_worker():
    """测试 Step 2: Embedding Worker"""
    logger.info("测试 Step 2: Embedding Worker")
    
    from import_vector_db.embed_worker import EmbeddingWorker
    
    base_dir = Path("/tmp/test_vectorization")
    batches_dir = base_dir / "batches"
    output_dir = base_dir / "embeddings"
    
    # 查找一个 batch 文件进行测试
    batch_files = list(batches_dir.glob("shard_*/batch_*.arrow"))
    if not batch_files:
        logger.warning("没有找到 batch 文件，请先运行 Step 1")
        return False
    
    batch_file = batch_files[0]
    logger.info(f"测试文件: {batch_file}")
    
    worker = EmbeddingWorker(
        batches_dir=batches_dir,
        output_dir=output_dir
    )
    
    try:
        output_path = worker.process_batch(batch_file)
        logger.info(f"Embedding 测试完成: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Embedding 测试失败: {e}", exc_info=True)
        return False


def test_shard_writer():
    """测试 Step 3: Shard Writer"""
    logger.info("测试 Step 3: Shard Writer")
    
    from import_vector_db.shard_writer import ShardWriter
    
    base_dir = Path("/tmp/test_vectorization")
    faiss_dir = base_dir / "faiss"
    
    # 查找一个 shard 的 embeddings
    embeddings_dirs = list((base_dir / "embeddings").glob("shard_*"))
    if not embeddings_dirs:
        logger.warning("没有找到 embeddings 目录，请先运行 Step 2")
        return False
    
    embeddings_dir = embeddings_dirs[0]
    shard_id = int(embeddings_dir.name.split("_")[1])
    
    logger.info(f"测试 Shard {shard_id}: {embeddings_dir}")
    
    # 从 batches manifest 获取 paper_id_range
    batches_manifest_path = base_dir / "batches" / "manifest.json"
    import json
    with open(batches_manifest_path, 'r') as f:
        manifest = json.load(f)
    
    shard_info = None
    for s in manifest["shards"]:
        if int(s["shard_id"]) == shard_id:
            shard_info = s
            break
    
    if not shard_info:
        logger.warning(f"没有找到 shard {shard_id} 的信息")
        return False
    
    paper_id_range = (shard_info["paper_id_range"][0], shard_info["paper_id_range"][1])
    
    writer = ShardWriter(
        shard_id=shard_id,
        embeddings_dir=embeddings_dir,
        faiss_dir=faiss_dir,
        paper_id_range=paper_id_range,
        vector_dim=4096,
        checkpoint_interval=5
    )
    
    try:
        success = writer.write_shard_index()
        if success:
            logger.info("Shard Writer 测试完成")
            # 不更新数据库状态（测试模式）
        return success
    except Exception as e:
        logger.error(f"Shard Writer 测试失败: {e}", exc_info=True)
        return False


def test_manifest_manager():
    """测试 Manifest Manager"""
    logger.info("测试 Manifest Manager")
    
    from import_vector_db.manifest_manager import ManifestManager
    
    manifest_path = Path("/tmp/test_vectorization/faiss/manifest.json")
    manager = ManifestManager(manifest_path)
    
    # 添加测试 shard
    manager.add_shard(
        shard_id="000",
        paper_id_range=[1, 1000],
        index_path="shards/shard_000.index",
        ids_path="shards/shard_000.ids.npy",
        total_vectors=1000,
        status="ready"
    )
    
    # 获取信息
    shard_info = manager.get_shard_info("000")
    logger.info(f"Shard 信息: {shard_info}")
    
    logger.info("Manifest Manager 测试完成")
    return True


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='向量化流水线测试')
    parser.add_argument(
        '--test',
        type=str,
        choices=['export', 'embed', 'writer', 'manifest', 'all'],
        default='all',
        help='测试项目'
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
    
    results = {}
    
    if args.test == 'all' or args.test == 'export':
        results['export'] = test_export()
    
    if args.test == 'all' or args.test == 'embed':
        results['embed'] = test_embed_worker()
    
    if args.test == 'all' or args.test == 'writer':
        results['writer'] = test_shard_writer()
    
    if args.test == 'all' or args.test == 'manifest':
        results['manifest'] = test_manifest_manager()
    
    # 汇总结果
    logger.info("=" * 60)
    logger.info("测试结果汇总")
    logger.info("=" * 60)
    for test_name, success in results.items():
        status = "✓ 通过" if success else "✗ 失败"
        logger.info(f"  {test_name}: {status}")
    
    # 返回退出码
    all_passed = all(results.values())
    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()



