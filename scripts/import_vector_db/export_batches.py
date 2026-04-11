#!/usr/bin/env python3
"""
Step 1: PostgreSQL → Arrow Batches 导出器

从 PostgreSQL 数据库中导出未处理的文献（embedding_status = 0）为 Apache Arrow 格式文件。
按 range-sharding 分配到不同 shard，导出成功后更新状态为 1（exported）。

支持 --start-over 选项：当设置为 True 时，不考虑 embedding_status，导出所有文献。
"""

import sys
import json
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.config_loader import load_config_from_yaml, get_metadata_db_engine_from_config
from sqlalchemy import text

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    raise ImportError("请安装 pyarrow: pip install pyarrow")

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchExporter:
    """Arrow Batch 导出器
    
    从 PostgreSQL 导出文献数据为 Arrow 格式，按 shard 和 batch 组织。
    """
    
    def __init__(
        self,
        base_dir: Path,
        config: Dict[str, Any],
        batch_size: int = 10000,
        papers_per_shard: int = 2500000,
        limit: Optional[int] = None,
        start_over: bool = False
    ):
        """初始化导出器
        
        Args:
            base_dir: 基础目录路径（例如 /mnt/lit_platform）
            config: 配置字典
            batch_size: 每个 batch 的文献数量（默认: 10000）
            papers_per_shard: 每个 shard 的文献数量（默认: 2500000）
            limit: 导出文献数量上限（None 表示不限制，用于测试）
            start_over: 是否重新导出所有文献（True 时忽略 embedding_status，默认: False）
        """
        self.base_dir = Path(base_dir)
        self.batch_size = batch_size
        self.papers_per_shard = papers_per_shard
        self.limit = limit
        self.start_over = start_over
        
        # 目录结构
        self.batches_dir = self.base_dir / "batches"
        self.manifest_path = self.batches_dir / "manifest.json"
        
        # 确保目录存在
        self.batches_dir.mkdir(parents=True, exist_ok=True)
        
        # 数据库引擎
        self.engine = get_metadata_db_engine_from_config(config)
        
        logger.info(f"初始化 BatchExporter: base_dir={base_dir}, batch_size={batch_size}, papers_per_shard={papers_per_shard}, limit={limit}, start_over={start_over}")
    
    def get_paper_id_range(self) -> Tuple[int, int]:
        """获取 papers 表的 paper_id 范围
        
        Returns:
            Tuple[int, int]: (min_paper_id, max_paper_id, total_count)
        """
        with self.engine.connect() as conn:
            if self.start_over:
                # start_over=True 时，不考虑 embedding_status，导出所有文献
                query = text("""
                    SELECT MIN(paper_id) as min_id, MAX(paper_id) as max_id, COUNT(*) as total
                    FROM papers
                """)
            else:
                # 默认只导出 embedding_status = 0 的文献
                query = text("""
                    SELECT MIN(paper_id) as min_id, MAX(paper_id) as max_id, COUNT(*) as total
                    FROM papers
                    WHERE embedding_status = 0
                """)
            result = conn.execute(query)
            row = result.fetchone()
            if row and row[0] is not None:
                return (row[0], row[1], row[2])
            else:
                return (0, 0, 0)
    
    def calculate_shard_ranges(self, min_id: int, max_id: int) -> List[Tuple[int, int, int]]:
        """计算 shard 的 paper_id 范围
        
        Args:
            min_id: 最小 paper_id
            max_id: 最大 paper_id
            
        Returns:
            List[Tuple[int, int, int]]: [(shard_id, start_id, end_id), ...]
        """
        if max_id < min_id:
            return []
        
        ranges = []
        shard_id = 0
        current_start = min_id
        
        while current_start <= max_id:
            current_end = min(current_start + self.papers_per_shard - 1, max_id)
            ranges.append((shard_id, current_start, current_end))
            shard_id += 1
            current_start = current_end + 1
        
        logger.info(f"计算得到 {len(ranges)} 个 shard 范围")
        return ranges
    
    def export_shard(
        self,
        shard_id: int,
        start_paper_id: int,
        end_paper_id: int,
        remaining_limit: Optional[int] = None
    ) -> Tuple[List[Path], int]:
        """导出单个 shard 的所有 batch
        
        Args:
            shard_id: Shard ID
            start_paper_id: 起始 paper_id
            end_paper_id: 结束 paper_id
            remaining_limit: 剩余可导出数量（None 表示不限制）
            
        Returns:
            Tuple[List[Path], int]: (生成的 batch 文件路径列表, 实际导出的数量)
        """
        shard_dir = self.batches_dir / f"shard_{shard_id:03d}"
        shard_dir.mkdir(parents=True, exist_ok=True)
        
        batch_files = []
        batch_num = 1
        offset = 0
        total_exported = 0
        
        logger.info(f"开始导出 shard_{shard_id:03d} (paper_id: {start_paper_id} - {end_paper_id})")
        
        while True:
            # 如果设置了剩余限制，检查是否已达到
            if remaining_limit is not None and remaining_limit <= 0:
                logger.info(f"  达到导出上限，停止导出 shard_{shard_id:03d}")
                break
            # 确定本次查询的数量（考虑剩余限制）
            query_limit = self.batch_size
            if remaining_limit is not None:
                query_limit = min(query_limit, remaining_limit)
            
            # 查询一批数据
            with self.engine.connect() as conn:
                if self.start_over:
                    # start_over=True 时，不考虑 embedding_status，导出所有文献
                    query = text("""
                        SELECT paper_id, work_id, title, abstract
                        FROM papers
                        WHERE paper_id >= :start_id
                          AND paper_id <= :end_id
                        ORDER BY paper_id
                        LIMIT :limit OFFSET :offset
                    """)
                else:
                    # 默认只导出 embedding_status = 0 的文献
                    query = text("""
                        SELECT paper_id, work_id, title, abstract
                        FROM papers
                        WHERE embedding_status = 0
                          AND paper_id >= :start_id
                          AND paper_id <= :end_id
                        ORDER BY paper_id
                        LIMIT :limit OFFSET :offset
                    """)
                
                result = conn.execute(query, {
                    "start_id": start_paper_id,
                    "end_id": end_paper_id,
                    "limit": query_limit,
                    "offset": offset
                })
                
                rows = result.fetchall()
                if not rows:
                    break
                
                # 转换为字典列表
                batch_data = []
                paper_ids = []
                for row in rows:
                    batch_data.append({
                        "paper_id": row[0],
                        "work_id": row[1] or "",
                        "title": row[2] or "",
                        "abstract": row[3] or ""
                    })
                    paper_ids.append(row[0])
                
                # 写入 Arrow 文件
                batch_filename = f"batch_{batch_num:06d}.arrow"
                batch_path = shard_dir / batch_filename
                
                # 创建 Arrow Table
                table = pa.Table.from_pylist(batch_data)
                
                # 写入 Arrow IPC 文件
                with pa.OSFile(str(batch_path), 'wb') as sink:
                    with pa.ipc.new_stream(sink, table.schema) as writer:
                        writer.write_table(table)
                
                batch_files.append(batch_path)
                logger.info(f"  ✓ 导出 batch {batch_num:06d}: {len(batch_data)} 条记录 -> {batch_path.name}")
                
                # 更新状态为 exported (1)
                self._update_status_to_exported(paper_ids)
                
                total_exported += len(rows)
                batch_num += 1
                offset += len(rows)
                
                # 更新剩余限制
                if remaining_limit is not None:
                    remaining_limit -= len(rows)
        
        logger.info(f"Shard_{shard_id:03d} 导出完成: 共 {batch_num - 1} 个 batch，{total_exported} 条记录")
        return batch_files, total_exported
    
    def _update_status_to_exported(self, paper_ids: List[int]):
        """批量更新状态为 exported (1)
        
        Args:
            paper_ids: Paper ID 列表
        """
        if not paper_ids:
            return
        
        with self.engine.begin() as conn:  # 使用 begin() 自动管理事务
            # 使用 PostgreSQL 的 ANY 操作符配合数组
            # 分批处理以避免参数过多
            batch_update_size = 1000
            for i in range(0, len(paper_ids), batch_update_size):
                batch_ids = paper_ids[i:i + batch_update_size]
                query = text("""
                    UPDATE papers
                    SET embedding_status = 1
                    WHERE paper_id = ANY(:paper_ids)
                """)
                conn.execute(query, {"paper_ids": batch_ids})
    
    def generate_manifest(self, shard_ranges: List[Tuple[int, int, int]]):
        """生成 manifest.json 文件
        
        Args:
            shard_ranges: Shard 范围列表 [(shard_id, start_id, end_id), ...]
        """
        manifest = {
            "version": "1.0",
            "created_at": datetime.now().isoformat(),
            "batch_size": self.batch_size,
            "papers_per_shard": self.papers_per_shard,
            "shards": []
        }
        
        for shard_id, start_id, end_id in shard_ranges:
            shard_dir = self.batches_dir / f"shard_{shard_id:03d}"
            batch_files = sorted(shard_dir.glob("batch_*.arrow"))
            
            manifest["shards"].append({
                "shard_id": f"{shard_id:03d}",
                "paper_id_range": [start_id, end_id],
                "batch_count": len(batch_files),
                "batch_files": [f.name for f in batch_files]
            })
        
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Manifest 已生成: {self.manifest_path}")
    
    def export_all(self) -> Dict[str, Any]:
        """导出所有未处理的文献
        
        Returns:
            Dict: 导出统计信息
        """
        logger.info("=" * 60)
        logger.info("开始导出 Arrow Batches")
        logger.info("=" * 60)
        
        # 获取 paper_id 范围
        min_id, max_id, total_count = self.get_paper_id_range()
        if self.start_over:
            logger.info(f"待处理文献（全部导出）: {total_count} 条 (paper_id: {min_id} - {max_id})")
        else:
            logger.info(f"待处理文献: {total_count} 条 (paper_id: {min_id} - {max_id})")
        
        if total_count == 0:
            if self.start_over:
                logger.warning("没有待处理的文献")
            else:
                logger.warning("没有待处理的文献（embedding_status = 0）")
            return {
                "total_papers": 0,
                "shards": 0,
                "batches": 0,
                "shard_ranges": []
            }
        
        # 应用 limit（如果设置）
        if self.limit is not None and self.limit > 0:
            total_count = min(total_count, self.limit)
            logger.info(f"应用导出上限: {self.limit}，实际将导出: {total_count} 条")
        
        # 计算 shard 范围
        shard_ranges = self.calculate_shard_ranges(min_id, max_id)
        
        # 导出每个 shard
        all_batch_files = []
        remaining_limit = self.limit
        total_exported = 0
        
        for shard_id, start_id, end_id in shard_ranges:
            # 如果达到限制，停止导出
            if remaining_limit is not None and remaining_limit <= 0:
                logger.info(f"达到导出上限，停止后续 shard 导出")
                break
            
            batch_files, exported_count = self.export_shard(shard_id, start_id, end_id, remaining_limit)
            all_batch_files.extend(batch_files)
            total_exported += exported_count
            
            # 更新剩余限制
            if remaining_limit is not None:
                remaining_limit -= exported_count
        
        # 生成 manifest
        self.generate_manifest(shard_ranges)
        
        # 统计信息（使用实际导出的数量）
        # 计算实际处理的 shard 数量
        processed_shard_ids = set()
        for batch_file in all_batch_files:
            shard_dir_name = batch_file.parent.name
            if shard_dir_name.startswith("shard_"):
                shard_id = int(shard_dir_name.split("_")[1])
                processed_shard_ids.add(shard_id)
        
        stats = {
            "total_papers": total_exported,
            "shards": len(processed_shard_ids),
            "batches": len(all_batch_files),
            "shard_ranges": shard_ranges,
            "limit": self.limit,
            "actual_exported": total_exported
        }
        
        logger.info("=" * 60)
        logger.info("导出完成！")
        logger.info(f"  实际导出: {stats['actual_exported']} 条记录")
        if self.limit:
            logger.info(f"  导出上限: {self.limit}")
        logger.info(f"  Shard 数: {stats['shards']}")
        logger.info(f"  Batch 数: {stats['batches']}")
        logger.info("=" * 60)
        
        return stats


#python3 export_batches.py --base-dir /home/wangyuanshi/pubmed_database/pubmed_vector_db_0123/ 
def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='从 PostgreSQL 导出文献为 Arrow Batches（Step 1）',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--base-dir',
        type=str,
        default='/home/wangyuanshi/pubmed_database/pubmed_vector_db_test',
        help='基础目录路径（默认: /mnt/lit_platform）'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='每个 batch 的文献数量（默认: 10000）'
    )
    
    parser.add_argument(
        '--papers-per-shard',
        type=int,
        default=3000000,
        help='每个 shard 的文献数量（默认: 2500000）'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='导出文献数量上限（None 表示不限制，用于测试）'
    )
    
    parser.add_argument(
        '--config-path',
        type=str,
        default='/home/wangyuanshi/remote_10.0.1.226/config/config_storage_server.yaml',
        help='配置文件路径（默认: 使用默认配置）'
    )
    
    parser.add_argument(
        '--start-over',
        action='store_true',
        help='重新导出所有文献（忽略 embedding_status，导出全部）'
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
    
    # 执行导出
    exporter = BatchExporter(
        base_dir=Path(args.base_dir),
        config=config,
        batch_size=args.batch_size,
        papers_per_shard=args.papers_per_shard,
        limit=args.limit,
        start_over=args.start_over
    )
    
    try:
        stats = exporter.export_all()
        logger.info("导出成功完成")
    except Exception as e:
        logger.error(f"导出失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

