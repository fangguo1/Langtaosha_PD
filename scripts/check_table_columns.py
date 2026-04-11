#!/usr/bin/env python3
"""
检查数据库表结构
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

from config.config_loader import init_config, get_db_engine
from sqlalchemy import text

_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = Path(__import__('os').environ.get('PD_TEST_CONFIG', str(_ROOT / 'tests' / 'db' / 'config_backend_server_test.yaml')))
DB_KEY = 'metadata_db'

def check_table_columns():
    """检查 papers 表的字段"""
    print("=" * 60)
    print("检查数据库表结构")
    print("=" * 60)
    
    # 初始化配置
    init_config(CONFIG_PATH)
    
    # 获取数据库引擎
    engine = get_db_engine(db_key=DB_KEY)
    
    with engine.connect() as conn:
        # 查询 papers 表的所有字段
        result = conn.execute(text("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'papers'
            ORDER BY ordinal_position
        """))
        
        columns = result.fetchall()
        
        print(f"\n📋 papers 表包含 {len(columns)} 个字段:\n")
        print(f"{'字段名':<30} {'类型':<20} {'长度':<10} {'可空':<8} {'默认值':<20}")
        print("-" * 90)
        
        for col in columns:
            col_name, data_type, max_length, is_nullable, default = col
            length_str = str(max_length) if max_length else '-'
            nullable_str = 'YES' if is_nullable == 'YES' else 'NO'
            default_str = str(default)[:20] if default else '-'
            
            print(f"{col_name:<30} {data_type:<20} {length_str:<10} {nullable_str:<8} {default_str:<20}")
        
        # 检查新字段是否存在
        print("\n" + "=" * 60)
        print("检查新字段是否存在:")
        print("=" * 60)
        
        new_fields = ['paper_type', 'primary_field', 'target_application_domain', 'is_llm_era', 'short_reasoning']
        column_names = [col[0] for col in columns]
        
        for field in new_fields:
            exists = field in column_names
            status = "✅ 存在" if exists else "❌ 不存在"
            print(f"  {field:<30} {status}")

if __name__ == '__main__':
    check_table_columns()
