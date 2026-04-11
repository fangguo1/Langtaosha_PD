#!/usr/bin/env python3
"""
在 meta_database 数据库中创建所有表
"""
import sys
import argparse
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.config_loader import init_config, get_db_engine, get_db_config
from sqlalchemy import text, create_engine
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus



#CONFIG_PATH = Path("/data3/guofang/AIgnite-Solutions/PD_TEST/local_data/config_backend_server.yaml")

def ensure_database_exists(config_path: Path) -> bool:
    """确保数据库存在，如果不存在则创建
    
    使用 SQLAlchemy 连接到默认数据库（postgres 或 template1），
    然后检查并创建目标数据库。
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        bool: 如果数据库已存在或创建成功返回 True，否则返回 False
    """
    # 初始化配置
    init_config(config_path)
    
    # 获取数据库配置
    db_config = get_db_config('metadata_db')
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    user = db_config.get('user')
    password = db_config.get('password')
    database = db_config.get('name')
    
    if not all([host, user, password, database]):
        print(f"❌ 数据库配置不完整，需要 host, user, password, name")
        return False
    
    # URL 编码密码和用户名中的特殊字符
    encoded_password = quote_plus(str(password))
    encoded_user = quote_plus(str(user))
    
    # 先连接到默认的 postgres 数据库来检查/创建目标数据库
    # 如果 postgres 数据库不可用，尝试 template1
    default_databases = ['postgres', 'template1']
    engine = None
    
    for default_db in default_databases:
        try:
            # 构建连接到默认数据库的连接字符串
            connection_string = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{default_db}"
            engine = create_engine(connection_string)
            # 测试连接
            with engine.connect() as test_conn:
                test_conn.execute(text("SELECT 1"))
            break  # 连接成功，退出循环
        except Exception:
            # 尝试下一个默认数据库
            engine = None
            continue
    
    if engine is None:
        print(f"❌ 无法连接到 PostgreSQL 服务器")
        print(f"   请确保 PostgreSQL 服务正在运行，并且配置信息正确")
        print(f"   配置信息: host={host}, port={port}, user={user}")
        return False
    
    try:
        # CREATE DATABASE 不能在事务中执行，需要使用 autocommit 模式
        with engine.connect() as conn:
            # 设置 autocommit 模式（CREATE DATABASE 不能在事务中执行）
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            
            # 检查数据库是否存在
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :database"),
                {"database": database}
            )
            exists = result.fetchone()
            
            if exists:
                print(f"✅ 数据库 '{database}' 已存在")
                return True
            
            # 数据库不存在，创建它
            print(f"📦 数据库 '{database}' 不存在，正在创建...")
            # 注意：PostgreSQL 的 CREATE DATABASE 不支持参数化查询，需要直接拼接
            # 但我们已经验证了 database 名称来自配置文件，相对安全
            conn.execute(text(f'CREATE DATABASE "{database}"'))
            print(f"✅ 数据库 '{database}' 创建成功")
            return True
            
    except OperationalError as e:
        error_msg = str(e)
        print(f"❌ 无法创建数据库 '{database}': {error_msg}")
        print(f"   请确保用户 '{user}' 有创建数据库的权限")
        return False
    except Exception as e:
        print(f"❌ 检查数据库时发生错误: {e}")
        return False
    finally:
        # 关闭引擎
        if engine:
            engine.dispose()


def create_tables(config_path: Path):
    """在 meta_database 数据库中创建所有表
    
    Args:
        config_path: 配置文件路径
    """
    
    # 确保数据库存在（内部会初始化配置）
    if not ensure_database_exists(config_path):
        raise RuntimeError("无法确保数据库存在，请检查配置和权限")
    
    # 重新初始化配置（因为 ensure_database_exists 可能已经初始化过，但为了确保一致性）
    init_config(config_path)
    
    # 获取数据库引擎（使用 metadata_db，即 meta_database）
    try:
        engine = get_db_engine(db_key='metadata_db')
    except OperationalError as e:
        error_msg = str(e)
        if "does not exist" in error_msg:
            print(f"❌ 数据库连接失败: {error_msg}")
            print(f"   即使已尝试创建数据库，连接仍然失败。")
            print(f"   请手动检查数据库配置和 PostgreSQL 服务状态。")
        raise
    
    # 读取 schema.sql 文件
    schema_file = Path(__file__).parent.parent / 'database' / 'schema.sql'
    
    if not schema_file.exists():
        raise FileNotFoundError(f"找不到 schema.sql 文件: {schema_file}")
    
    print(f"📖 读取 SQL 文件: {schema_file}")
    with open(schema_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # 执行 SQL 脚本
    print("🔨 开始创建数据库表...")
    with engine.connect() as conn:
        # 执行整个 SQL 脚本
        conn.execute(text(sql_content))
        conn.commit()
    
    print("✅ 数据库表创建完成！")
    
    # 验证表是否创建成功
    print("\n📊 验证表创建情况...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """))
        tables = [row[0] for row in result.fetchall()]
        
        expected_tables = [
            'papers', 'paper_keywords', 'paper_texts', 'paper_author_affiliation',
            'venues', 'paper_publications', 'categories', 'paper_categories',
            'paper_versions', 'paper_citations', 'meta_update_logs',
            'fields', 'paper_fields', 'pubmed_additional_info'
        ]
        
        print(f"\n已创建的表 ({len(tables)} 个):")
        for table in tables:
            status = "✅" if table in expected_tables else "⚠️"
            print(f"  {status} {table}")
        
        missing_tables = set(expected_tables) - set(tables)
        if missing_tables:
            print(f"\n⚠️ 缺少的表: {missing_tables}")
        else:
            print("\n✅ 所有表都已成功创建！")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='在 meta_database 数据库中创建所有表',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径（默认: 项目根目录下的 src/config/config_backend_server.yaml）'
    )
    
    args = parser.parse_args()
    
    # 确定配置文件路径
    if args.config_path:
        config_path = Path(args.config_path)
    else:
        config_path = Path(__file__).parent.parent / 'src' / 'config' / 'config_backend_server.yaml'
    
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return 1
    
    print(f"📁 使用配置文件: {config_path}")
    print()
    
    try:
        create_tables(config_path)
        return 0
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())

