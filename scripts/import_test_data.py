#!/usr/bin/env python3
"""
导入测试数据到 meta_database 数据库
支持导入 arxiv_json 和 pubmed_json 目录下的 JSON 文件
"""
import sys
import json
from pathlib import Path
from typing import List, Dict, Any

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.config_loader import init_config
from src.docset_hub.indexing import PaperIndexer
from src.docset_hub.input_adapters import JSONAdapter

def import_json_files(folder_path: Path, config_path: Path, db_key: str = 'metadata_db', limit: int = None):
    """导入文件夹中的所有 JSON 文件
    
    Args:
        folder_path: JSON 文件所在文件夹路径
        config_path: 配置文件路径
        db_key: 数据库配置键名
        limit: 导入数量限制（None 表示不限制）
    
    Returns:
        tuple: (成功数量, 失败数量, 错误列表)
    """
    if not folder_path.exists():
        print(f"⚠️ 文件夹不存在: {folder_path}")
        return 0, 0, []
    
    # 获取所有 JSON 文件
    json_files = list(folder_path.glob('*.json'))
    
    if not json_files:
        print(f"⚠️ 文件夹中没有找到 JSON 文件: {folder_path}")
        return 0, 0, []
    
    if limit:
        json_files = json_files[:limit]
    
    print(f"📁 找到 {len(json_files)} 个 JSON 文件")
    
    # 初始化索引器
    indexer = PaperIndexer(
        config_path=config_path,
        db_key=db_key,
        enable_vectorization=False,  # 测试数据导入时不启用向量化
        vector_auto_save=False
    )
    
    success_count = 0
    fail_count = 0
    errors = []
    
    # 逐个导入文件
    for i, json_file in enumerate(json_files, 1):
        try:
            # 读取 JSON 文件
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 使用索引器导入
            result = indexer.add_doc(data, include_traceback=False)
            
            if result['success']:
                success_count += 1
                if i % 100 == 0:
                    print(f"  ✅ 已导入 {i}/{len(json_files)} 个文件...")
            else:
                fail_count += 1
                error_msg = result.get('message', '未知错误')
                errors.append({
                    'file': str(json_file),
                    'error': error_msg,
                    'error_type': result.get('error_type')
                })
                if fail_count <= 10:  # 只显示前10个错误
                    print(f"  ❌ 导入失败: {json_file.name} - {error_msg}")
        
        except Exception as e:
            fail_count += 1
            error_msg = str(e)
            errors.append({
                'file': str(json_file),
                'error': error_msg
            })
            if fail_count <= 10:  # 只显示前10个错误
                print(f"  ❌ 导入失败: {json_file.name} - {error_msg}")
    
    return success_count, fail_count, errors

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='导入测试数据到 meta_database 数据库')
    parser.add_argument(
        '--arxiv-dir',
        type=str,
        default='/data3/qiqinglin/PD_TEST/tests/data/arxiv_json',
        help='arXiv JSON 文件目录路径'
    )
    parser.add_argument(
        '--pubmed-dir',
        type=str,
        default='/data3/qiqinglin/PD_TEST/tests/data/pubmed_json',
        help='PubMed JSON 文件目录路径'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='每个目录导入数量限制（用于测试）'
    )
    parser.add_argument(
        '--skip-arxiv',
        action='store_true',
        help='跳过 arXiv 数据导入'
    )
    parser.add_argument(
        '--skip-pubmed',
        action='store_true',
        help='跳过 PubMed 数据导入'
    )
    
    args = parser.parse_args()
    
    # 初始化配置
    config_path = Path(__file__).parent.parent / 'src' / 'config' / 'config_backend_server.yaml'
    init_config(config_path)
    
    print("=" * 60)
    print("开始导入测试数据到 meta_database 数据库")
    print("=" * 60)
    print(f"配置文件: {config_path}")
    print(f"数据库: metadata_db (meta_database)")
    print()
    
    total_success = 0
    total_fail = 0
    all_errors = []
    
    # 导入 arXiv 数据
    if not args.skip_arxiv:
        arxiv_dir = Path(args.arxiv_dir)
        print(f"\n📚 导入 arXiv 数据: {arxiv_dir}")
        print("-" * 60)
        success, fail, errors = import_json_files(
            arxiv_dir, 
            config_path, 
            db_key='metadata_db',
            limit=args.limit
        )
        total_success += success
        total_fail += fail
        all_errors.extend(errors)
        print(f"✅ arXiv: 成功 {success} 个, 失败 {fail} 个")
    else:
        print("\n⏭️  跳过 arXiv 数据导入")
    
    # 导入 PubMed 数据
    if not args.skip_pubmed:
        pubmed_dir = Path(args.pubmed_dir)
        print(f"\n📚 导入 PubMed 数据: {pubmed_dir}")
        print("-" * 60)
        success, fail, errors = import_json_files(
            pubmed_dir, 
            config_path, 
            db_key='metadata_db',
            limit=args.limit
        )
        total_success += success
        total_fail += fail
        all_errors.extend(errors)
        print(f"✅ PubMed: 成功 {success} 个, 失败 {fail} 个")
    else:
        print("\n⏭️  跳过 PubMed 数据导入")
    
    # 输出总结
    print("\n" + "=" * 60)
    print("导入完成！")
    print("=" * 60)
    print(f"总计: 成功 {total_success} 个, 失败 {total_fail} 个")
    
    if all_errors:
        print(f"\n⚠️  错误详情（共 {len(all_errors)} 个错误）:")
        for i, error in enumerate(all_errors[:20], 1):  # 只显示前20个错误
            print(f"  {i}. {Path(error['file']).name}: {error['error']}")
        if len(all_errors) > 20:
            print(f"  ... 还有 {len(all_errors) - 20} 个错误未显示")
    
    # 验证导入结果
    print("\n📊 验证导入结果...")
    from src.config.config_loader import get_db_engine
    from sqlalchemy import text
    
    engine = get_db_engine(db_key='metadata_db')
    with engine.connect() as conn:
        # 统计各数据源的论文数量
        result = conn.execute(text("""
            SELECT source, COUNT(*) as count 
            FROM papers 
            GROUP BY source 
            ORDER BY source
        """))
        stats = result.fetchall()
        
        print("\n数据源统计:")
        for source, count in stats:
            print(f"  {source}: {count} 篇论文")
        
        # 统计总数
        result = conn.execute(text("SELECT COUNT(*) FROM papers"))
        total = result.scalar()
        print(f"\n总计: {total} 篇论文")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断导入")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

