#!/usr/bin/env python3
"""
数据库清理脚本
用于清理之前存储的数据，为测试做准备
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.db_config import get_connection
import psycopg2


def clean_database(confirm: bool = False):
    """清理数据库中的所有数据
    
    Args:
        confirm: 是否确认清理（安全措施）
    """
    if not confirm:
        print("⚠️  警告：此操作将删除数据库中的所有数据！")
        print("请设置 confirm=True 来确认执行")
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        print("开始清理数据库...")
        
        # 按照外键依赖顺序删除数据
        # 先删除关联表的数据（外键约束会自动处理，但显式删除更清晰）
        
        tables_to_clean = [
            'paper_fields',
            'paper_citations',
            'paper_versions',
            'paper_publications',
            'paper_categories',
            'paper_author_affiliation',
            'paper_texts',
            'meta_update_logs',
            'papers',  # 主表，最后删除
        ]
        
        for table in tables_to_clean:
            try:
                cursor.execute(f"DELETE FROM {table}")
                count = cursor.rowcount
                print(f"✅ 清理表 {table}: 删除了 {count} 条记录")
            except psycopg2.errors.UndefinedTable:
                print(f"⚠️  表 {table} 不存在，跳过")
            except Exception as e:
                print(f"❌ 清理表 {table} 失败: {e}")
        
        conn.commit()
        print("\n✅ 数据库清理完成！")
        
        # 验证清理结果
        cursor.execute("SELECT COUNT(*) FROM papers")
        count = cursor.fetchone()[0]
        print(f"验证：papers表中剩余 {count} 条记录")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ 清理失败: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def show_database_stats():
    """显示数据库统计信息"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        print("=" * 60)
        print("数据库统计信息")
        print("=" * 60)
        
        tables = [
            'papers',
            'paper_author_affiliation',
            'paper_categories',
            'paper_publications',
            'paper_versions',
            'paper_citations',
            'paper_fields',
        ]
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table:30s}: {count:6d} 条记录")
            except psycopg2.errors.UndefinedTable:
                print(f"{table:30s}: 表不存在")
            except Exception as e:
                print(f"{table:30s}: 查询失败 - {e}")
        
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库清理工具')
    parser.add_argument('--show', action='store_true', help='显示数据库统计信息')
    parser.add_argument('--clean', action='store_true', help='清理数据库（危险操作）')
    parser.add_argument('--confirm', action='store_true', help='确认清理操作')
    
    args = parser.parse_args()
    
    if args.show:
        show_database_stats()
    elif args.clean:
        if args.confirm:
            clean_database(confirm=True)
        else:
            print("⚠️  请使用 --confirm 参数来确认清理操作")
            print("示例: python scripts/clean_database.py --clean --confirm")
    else:
        parser.print_help()

