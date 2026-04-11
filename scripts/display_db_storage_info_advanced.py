#!/usr/bin/env python3
"""
数据库存储信息显示脚本
读取 config.yaml 配置文件，显示论文存储情况统计和向量数据库存储情况
"""

import sys
import argparse
from pathlib import Path
from typing import Optional
import os

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config_loader import (
    load_config_from_yaml,
    get_vector_db_path_from_config,
    get_metadata_db_engine_from_config
)
from sqlalchemy import text, create_engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.engine import Engine

try:
    import faiss
    import numpy as np
except ImportError:
    faiss = None
    np = None


def execute_query(connection, query):
    """执行 SQL 查询并返回结果
    
    Args:
        connection: SQLAlchemy 数据库连接对象
        query: SQL 查询语句
        
    Returns:
        查询结果：
        - 单行单列：返回标量值
        - 多行或多列：返回列表（每行是元组）
    """
    try:
        result = connection.execute(text(query))
        results = result.fetchall()
        
        if not results:
            return None
        
        # 如果只有一行一列，返回标量值
        if len(results) == 1 and len(results[0]) == 1:
            return results[0][0]
        
        # 否则返回所有结果
        return results
    except SQLAlchemyError as e:
        print(f"❌ 查询执行失败: {e}")
        return None


def format_number(num):
    """格式化数字显示
    
    Args:
        num: 数字值（可能是 None）
        
    Returns:
        格式化后的字符串，None 返回 "0"
    """
    if num is None:
        return "0"
    try:
        return f"{int(num):,}"
    except (ValueError, TypeError):
        return str(num)


def calculate_percent(part, total):
    """计算百分比
    
    Args:
        part: 部分值
        total: 总值
        
    Returns:
        百分比字符串（保留2位小数），如果总值为0则返回 "0.00"
    """
    if total is None or total == 0:
        return "0.00"
    try:
        percent = (part if part else 0) * 100.0 / total
        return f"{percent:.2f}"
    except (TypeError, ZeroDivisionError):
        return "0.00"


def print_section_header(title, icon="📊"):
    """打印章节标题
    
    Args:
        title: 标题文本
        icon: 图标（可选）
    """
    print(f"{icon} {title}")
    print("━" * 60)


def show_banner():
    """显示 banner"""
    print("━" * 60)
    print("📊 Database Storage Information Display Tool")
    print("━" * 60)
    print()


def show_total_papers(connection):
    """显示总论文数统计"""
    print_section_header("总论文数统计")
    
    total = execute_query(connection, "SELECT COUNT(*) FROM papers;")
    print(f"  总论文数: {format_number(total)}")
    print()


def show_storage_completeness(connection):
    """显示存储完整性统计"""
    print_section_header("存储完整性统计", "✅")
    
    total = execute_query(connection, "SELECT COUNT(*) FROM papers;")
    with_metadata = execute_query(connection, "SELECT COUNT(*) FROM papers WHERE title IS NOT NULL AND title != '';")
    with_abstract = execute_query(connection, "SELECT COUNT(*) FROM papers WHERE abstract IS NOT NULL AND abstract != '';")
    with_authors = execute_query(connection, "SELECT COUNT(*) FROM paper_author_affiliation;")
    with_pdf = execute_query(connection, "SELECT COUNT(*) FROM paper_texts WHERE pdf_path IS NOT NULL AND pdf_path != '';")
    with_embedding = execute_query(connection, "SELECT COUNT(*) FROM paper_texts WHERE embedding IS NOT NULL;")
    
    print(f"  总论文数: {format_number(total)}")
    print()
    
    if total and total > 0:
        metadata_percent = calculate_percent(with_metadata, total)
        abstract_percent = calculate_percent(with_abstract, total)
        authors_percent = calculate_percent(with_authors, total)
        pdf_percent = calculate_percent(with_pdf, total)
        embedding_percent = calculate_percent(with_embedding, total)
        
        print(f"  {'项目':<30s} {'数量':>8s} / {'总数':<8s} ({'百分比':>6s}%)")
        print(f"  {'─' * 30} {'─' * 8} {'─' * 8} {'─' * 8}")
        print(f"  {'有标题':<30s} {format_number(with_metadata):>8s} / {format_number(total):<8s} ({metadata_percent:>6s}%)")
        print(f"  {'有摘要':<30s} {format_number(with_abstract):>8s} / {format_number(total):<8s} ({abstract_percent:>6s}%)")
        print(f"  {'有作者信息':<30s} {format_number(with_authors):>8s} / {format_number(total):<8s} ({authors_percent:>6s}%)")
        print(f"  {'有 PDF 路径':<30s} {format_number(with_pdf):>8s} / {format_number(total):<8s} ({pdf_percent:>6s}%)")
        print(f"  {'有 Embedding':<30s} {format_number(with_embedding):>8s} / {format_number(total):<8s} ({embedding_percent:>6s}%)")
    else:
        print("  暂无数据")
    print()


def show_pdf_storage(connection):
    """显示 PDF 存储情况"""
    print_section_header("PDF 存储情况", "📎")
    
    with_pdf_url = execute_query(connection, "SELECT COUNT(*) FROM papers WHERE pdf_url IS NOT NULL AND pdf_url != '';")
    with_pdf_path = execute_query(connection, "SELECT COUNT(*) FROM paper_texts WHERE pdf_path IS NOT NULL AND pdf_path != '';")
    total = execute_query(connection, "SELECT COUNT(*) FROM papers;")
    
    print(f"  有 PDF URL 的论文: {format_number(with_pdf_url)}")
    print(f"  有 PDF 路径的论文: {format_number(with_pdf_path)}")
    print(f"  总论文数: {format_number(total)}")
    
    if total and total > 0:
        pdf_url_percent = calculate_percent(with_pdf_url, total)
        pdf_path_percent = calculate_percent(with_pdf_path, total)
        print(f"  PDF URL 覆盖率: {pdf_url_percent}%")
        print(f"  PDF 路径覆盖率: {pdf_path_percent}%")
    print()


def show_embedding_storage(connection):
    """显示 Embedding 存储情况"""
    print_section_header("Embedding 存储情况", "🔢")
    
    with_embedding = execute_query(connection, "SELECT COUNT(*) FROM paper_texts WHERE embedding IS NOT NULL;")
    total = execute_query(connection, "SELECT COUNT(*) FROM papers;")
    
    print(f"  有 Embedding 的论文: {format_number(with_embedding)}")
    print(f"  总论文数: {format_number(total)}")
    
    if total and total > 0:
        embedding_percent = calculate_percent(with_embedding, total)
        print(f"  Embedding 覆盖率: {embedding_percent}%")
    print()


def show_source_statistics(connection):
    """显示按来源统计"""
    print_section_header("按来源统计", "📚")
    
    query = "SELECT source, COUNT(*) as count FROM papers GROUP BY source ORDER BY count DESC;"
    results = execute_query(connection, query)
    
    if results:
        print(f"  {'来源':<20s} {'数量':>10s}")
        print(f"  {'─' * 20} {'─' * 10}")
        for source, count in results:
            source_str = source if source else "未知"
            print(f"  {source_str:<20s} {format_number(count):>10s}")
    else:
        print("  暂无数据")
    print()


def show_year_statistics(connection):
    """显示按年份统计（TOP 10）"""
    print_section_header("按年份统计", "📅")
    
    query = "SELECT year, COUNT(*) as count FROM papers WHERE year IS NOT NULL GROUP BY year ORDER BY year DESC LIMIT 10;"
    results = execute_query(connection, query)
    
    if results:
        print(f"  {'年份':<10s} {'数量':>10s}")
        print(f"  {'─' * 10} {'─' * 10}")
        for year, count in results:
            year_str = str(year) if year else "未知"
            print(f"  {year_str:<10s} {format_number(count):>10s}")
    else:
        print("  暂无数据")
    print()


def show_table_sizes(connection):
    """显示数据库表大小"""
    print_section_header("数据库表大小", "💾")
    
    query = """
    SELECT 
        schemaname || '.' || tablename AS table_name,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
    FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
    """
    
    results = execute_query(connection, query)
    
    if results:
        print(f"  {'表名':<40s} {'大小':>15s}")
        print(f"  {'─' * 40} {'─' * 15}")
        for table_name, size in results:
            # 去掉 'public.' 前缀
            display_name = table_name.replace('public.', '')
            print(f"  {display_name:<40s} {size:>15s}")
    else:
        print("  无法获取表大小信息")
    print()





def show_vector_db_storage(vector_db_path: Path):
    """显示向量数据库存储情况
    
    Args:
        vector_db_path: 向量数据库根路径
    """
    print_section_header("向量数据库存储情况", "🔍")
    
    if not vector_db_path or not vector_db_path.exists():
        print(f"  ⚠️  向量数据库路径不存在: {vector_db_path}")
        print()
        return
    
    # 查找 faiss/shards/ 目录
    #faiss_shards_dir = vector_db_path / "faiss" / "shards"
    faiss_shards_dir = vector_db_path
    
    if not faiss_shards_dir.exists():
        print(f"  ⚠️  FAISS shards 目录不存在: {faiss_shards_dir}")
        print()
        return
    
    print(f"  向量数据库路径: {vector_db_path}")
    print(f"  FAISS shards 目录: {faiss_shards_dir}")
    print()
    
    # 查找所有 .index 文件
    index_files = sorted(faiss_shards_dir.glob("shard_*.index"))
    
    if not index_files:
        print("  ⚠️  未找到任何 FAISS 索引文件")
        print()
        return
    
    if faiss is None or np is None:
        print("  ⚠️  无法读取向量数据库：缺少 faiss 或 numpy 库")
        print("  请安装: pip install faiss-cpu numpy")
        print()
        return
    
    print(f"  {'Shard ID':<12s} {'向量数':>15s} {'索引文件大小':>20s} {'IDs文件大小':>20s}")
    print(f"  {'─' * 12} {'─' * 15} {'─' * 20} {'─' * 20}")
    
    total_vectors = 0
    total_index_size = 0
    total_ids_size = 0
    shard_info_list = []
    
    for index_file in index_files:
        try:
            # 提取 shard_id
            shard_id = index_file.stem.replace('shard_', '').replace('.index', '')
            
            # 读取索引文件获取向量数
            index = faiss.read_index(str(index_file))
            vector_count = index.ntotal
            
            # 获取文件大小
            index_size = index_file.stat().st_size
            
            # 查找对应的 .ids.npy 文件
            ids_file = faiss_shards_dir / f"shard_{shard_id}.ids.npy"
            ids_size = ids_file.stat().st_size if ids_file.exists() else 0
            
            # 验证 IDs 文件中的 paper_id 数量
            if ids_file.exists():
                try:
                    paper_ids = np.load(str(ids_file))
                    if len(paper_ids) != vector_count:
                        print(f"  ⚠️  Shard {shard_id}: 索引向量数 ({vector_count}) 与 IDs 文件数量 ({len(paper_ids)}) 不匹配")
                except Exception as e:
                    print(f"  ⚠️  Shard {shard_id}: 无法读取 IDs 文件: {e}")
            
            total_vectors += vector_count
            total_index_size += index_size
            total_ids_size += ids_size
            
            shard_info_list.append({
                'shard_id': shard_id,
                'vector_count': vector_count,
                'index_size': index_size,
                'ids_size': ids_size
            })
            
            # 格式化文件大小
            index_size_str = format_file_size(index_size)
            ids_size_str = format_file_size(ids_size) if ids_size > 0 else "N/A"
            
            print(f"  {shard_id:<12s} {format_number(vector_count):>15s} {index_size_str:>20s} {ids_size_str:>20s}")
            
        except Exception as e:
            print(f"  ⚠️  读取索引文件失败 {index_file.name}: {e}")
    
    print(f"  {'─' * 12} {'─' * 15} {'─' * 20} {'─' * 20}")
    print(f"  {'总计':<12s} {format_number(total_vectors):>15s} {format_file_size(total_index_size):>20s} {format_file_size(total_ids_size):>20s}")
    print()
    
    # 显示统计信息
    print(f"  Shard 总数: {len(shard_info_list)}")
    if shard_info_list:
        avg_vectors = total_vectors / len(shard_info_list)
        print(f"  平均每个 Shard 向量数: {format_number(int(avg_vectors))}")
        print(f"  总索引大小: {format_file_size(total_index_size)}")
        print(f"  总 IDs 文件大小: {format_file_size(total_ids_size)}")
        print(f"  总存储大小: {format_file_size(total_index_size + total_ids_size)}")
    print()


def format_file_size(size_bytes: int) -> str:
    """格式化文件大小
    
    Args:
        size_bytes: 字节数
        
    Returns:
        格式化后的字符串，例如 "1.5 GB"
    """
    if size_bytes == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.2f} {units[unit_index]}"


#python3 display_db_storage_info_advanced.py --config-path /home/wangyuanshi/remote_10.0.1.226/config/config_storage_server.yaml
#python3 scripts/display_db_storage_info_advanced.py --config-path local_data/config_backend_server_wangyuanshi.yaml 
def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='显示数据库和向量数据库存储信息统计',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径（默认: 项目根目录下的 config/config.yaml）'
    )
    
    args = parser.parse_args()
    
    # 显示 banner
    show_banner()
    
    # 确定配置文件路径
    if args.config_path:
        config_path = Path(args.config_path)
    else:
        config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'
    
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return 1
    
    print(f"📁 使用配置文件: {config_path}")
    print()
    
    # 读取配置信息
    try:
        config = load_config_from_yaml(config_path)
        if not config:
            print("❌ 配置文件为空或无法读取")
            return 1
    except Exception as e:
        print(f"❌ 无法读取配置文件: {e}")
        return 1
    
    # 从配置文件读取 metadata_db 信息并建立连接
    engine = None
    connection = None

    if 'metadata_db' in config:
        print("🔌 正在连接 metadata_db...")
        try:
            engine = get_metadata_db_engine_from_config(config)
            connection = engine.connect()
            
            # 测试连接
            test_result = execute_query(connection, "SELECT 1;")
            if test_result:
                db_config = config['metadata_db']
                print("✅ metadata_db 连接成功")
                print(f"  数据库: {db_config.get('name', 'N/A')}")
                print(f"  主机: {db_config.get('host', 'N/A')}")
                print(f"  端口: {db_config.get('port', 'N/A')}")
                print(f"  用户: {db_config.get('user', 'N/A')}")
                print()
            else:
                print("❌ metadata_db 连接失败：无法执行测试查询")
                return 1
        except Exception as e:
            print(f"❌ metadata_db 连接失败: {e}")
            return 1
    else:
        print("⚠️  配置文件中未找到 metadata_db 配置，跳过数据库统计")
        print()
    
    try:
        # 显示 metadata_db 统计信息
        if connection:
            show_total_papers(connection)
            show_storage_completeness(connection)
            show_pdf_storage(connection)
            show_embedding_storage(connection)
            show_source_statistics(connection)
            show_year_statistics(connection)
            show_table_sizes(connection)
        
        # 显示向量数据库存储情况
        vector_db_path = get_vector_db_path_from_config(config)
        if vector_db_path:
            show_vector_db_storage(vector_db_path)
        else:
            print_section_header("向量数据库存储情况", "🔍")
            print("  ⚠️  配置文件中未找到 vector_db 或 vector 配置")
            print()
        
        # 显示结束分隔线
        print("━" * 60)
        print("✅ 统计信息显示完成")
        print("━" * 60)
        
        return 0
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    sys.exit(main())

