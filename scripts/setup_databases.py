#!/usr/bin/env python3
"""
数据库初始化脚本

根据配置文件初始化 PostgreSQL metadata_db 和腾讯云 VectorDB。

功能:
    1. 确保 PostgreSQL metadata_db 存在
    2. 在 metadata_db 中创建所有表
    3. 执行 database/migrations 中的 SQL migrations
    4. 确保腾讯云 VectorDB 数据库存在
    5. 为每个允许的 source 创建 collection

使用方式:
    python scripts/setup_databases.py --config-path src/config/config_tecent_backend_server_mimic.yaml
    python scripts/setup_databases.py --config-path src/config/config_tecent_backend_server_mimic.yaml --force-recreate
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.config_loader import init_config, get_db_engine, get_db_config, load_config_from_yaml
from sqlalchemy import text, create_engine
from sqlalchemy.exc import OperationalError
from urllib.parse import quote_plus


# =============================================================================
# PostgreSQL metadata_db 初始化
# =============================================================================

def ensure_database_exists(config_path: Path) -> bool:
    """确保数据库存在，如果不存在则创建

    使用 SQLAlchemy 连接到默认数据库（postgres 或 template1），
    然后检查并创建目标数据库。

    Args:
        config_path: 配置文件路径

    Returns:
        bool: 如果数据库已存在或创建成功返回 True，否则返回 False
    """
    # 读取配置
    try:
        config = load_config_from_yaml(config_path)
        if not config:
            print("❌ 配置文件为空或无法读取")
            return False
    except Exception as e:
        print(f"❌ 无法读取配置文件: {e}")
        return False

    # 获取数据库配置
    if 'metadata_db' not in config:
        print("❌ 配置文件中未找到 metadata_db 配置")
        return False

    db_config = config['metadata_db']
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
    default_databases = ['postgres', 'template1']
    engine = None

    for default_db in default_databases:
        try:
            connection_string = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{default_db}"
            engine = create_engine(connection_string)
            with engine.connect() as test_conn:
                test_conn.execute(text("SELECT 1"))
            break
        except Exception:
            engine = None
            continue

    if engine is None:
        print(f"❌ 无法连接到 PostgreSQL 服务器")
        print(f"   请确保 PostgreSQL 服务正在运行，并且配置信息正确")
        print(f"   配置信息: host={host}, port={port}, user={user}")
        return False

    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")

            # 检查数据库是否存在
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :database"),
                {"database": database}
            )
            exists = result.fetchone()

            if exists:
                print(f"✅ PostgreSQL 数据库 '{database}' 已存在")
                return True

            # 数据库不存在，创建它
            print(f"📦 PostgreSQL 数据库 '{database}' 不存在，正在创建...")
            conn.execute(text(f'CREATE DATABASE "{database}"'))
            print(f"✅ PostgreSQL 数据库 '{database}' 创建成功")
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
        if engine:
            engine.dispose()


def drop_all_tables(engine) -> None:
    """删除数据库中的所有表

    按照外键依赖顺序删除表，避免外键约束错误。

    Args:
        engine: SQLAlchemy 数据库引擎
    """
    with engine.connect() as conn:
        # 获取所有表名
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """))
        tables = [row[0] for row in result.fetchall()]

        if not tables:
            print("📭 数据库中没有表")
            return

        print(f"🗑️  准备删除 {len(tables)} 个表: {', '.join(tables)}")

        # 按照依赖顺序删除表（子表先删，父表后删）
        # 根据 schema.sql 中的外键关系确定删除顺序
        drop_order = [
            'paper_source_artifacts',      # 依赖 paper_sources
            'paper_source_metadata',       # 依赖 paper_sources
            'paper_references',            # 依赖 papers, paper_sources
            'paper_keywords',              # 依赖 papers
            'paper_author_affiliation',    # 依赖 papers
            'paper_categories',            # 依赖 papers, categories
            'pubmed_additional_info',      # 依赖 papers
            'meta_update_logs',            # 依赖 papers
            'paper_sources',               # 依赖 papers
            'papers',
            'categories'
        ]

        # 只删除存在的表
        for table in drop_order:
            if table in tables:
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
                    print(f"  ✅ 删除表: {table}")
                except Exception as e:
                    print(f"  ⚠️  删除表失败 {table}: {e}")

        # 删除剩余的表（如果有的话）
        remaining_tables = set(tables) - set(drop_order)
        for table in remaining_tables:
            try:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
                print(f"  ✅ 删除表: {table}")
            except Exception as e:
                print(f"  ⚠️  删除表失败 {table}: {e}")

        conn.commit()
        print("✅ 所有表已删除")


def ensure_pgcrypto_extension(engine) -> bool:
    """确保 pgcrypto 扩展可用，以支持 gen_random_uuid()."""
    try:
        with engine.connect() as conn:
            conn.execute(text('CREATE EXTENSION IF NOT EXISTS pgcrypto'))
            conn.commit()
        print("✅ PostgreSQL 扩展 pgcrypto 已就绪")
        return True
    except Exception as e:
        print(f"❌ 初始化 pgcrypto 扩展失败: {e}")
        return False


def execute_sql_file(engine, sql_file: Path, description: str) -> bool:
    """执行单个 SQL 文件。"""
    if not sql_file.exists():
        print(f"❌ 找不到 SQL 文件: {sql_file}")
        return False

    print(f"📖 读取 {description}: {sql_file}")
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    print(f"🔨 执行 {description}...")
    try:
        with engine.connect() as conn:
            conn.execute(text(sql_content))
            conn.commit()
        print(f"✅ {description}执行完成！")
        return True
    except Exception as e:
        print(f"❌ {description}失败: {e}")
        return False


def run_metadata_migrations(engine) -> bool:
    """按文件名顺序执行 metadata_db migrations。"""
    migrations_dir = Path(__file__).parent.parent / 'database' / 'migrations'

    if not migrations_dir.exists():
        print(f"ℹ️  未找到 migrations 目录，跳过: {migrations_dir}")
        return True

    migration_files = sorted(
        path for path in migrations_dir.glob('*.sql')
        if path.is_file()
    )

    if not migration_files:
        print("ℹ️  migrations 目录中没有 SQL 文件，跳过")
        return True

    print()
    print(f"🧩 开始执行 migrations（共 {len(migration_files)} 个）...")

    for migration_file in migration_files:
        if not execute_sql_file(engine, migration_file, f"migration {migration_file.name}"):
            return False

    print("✅ 所有 migrations 执行完成！")
    return True


def setup_metadata_db(config_path: Path, reset_db: bool = False) -> bool:
    """初始化 PostgreSQL metadata_db

    Args:
        config_path: 配置文件路径
        reset_db: 是否重置数据库（删除所有表后重新创建）

    Returns:
        bool: 成功返回 True
    """
    print("=" * 60)
    print("🗄️  PostgreSQL metadata_db 初始化")
    print("=" * 60)
    print()

    # 确保数据库存在
    if not ensure_database_exists(config_path):
        print("❌ 无法确保数据库存在，请检查配置和权限")
        return False

    # 重新初始化配置
    init_config(config_path)

    # 获取数据库引擎
    try:
        engine = get_db_engine(db_key='metadata_db')
    except OperationalError as e:
        error_msg = str(e)
        if "does not exist" in error_msg:
            print(f"❌ 数据库连接失败: {error_msg}")
        raise

    # 如果需要重置数据库，先删除所有表
    if reset_db:
        print("🔄 重置模式：删除所有现有表...")
        drop_all_tables(engine)
        print()

    if not ensure_pgcrypto_extension(engine):
        return False

    schema_file = Path(__file__).parent.parent / 'database' / 'schema.sql'
    if not execute_sql_file(engine, schema_file, "schema.sql"):
        return False

    print()
    if not run_metadata_migrations(engine):
        return False

    # 验证表是否创建成功
    print()
    print("📊 验证表创建情况...")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        tables = [row[0] for row in result.fetchall()]

        expected_tables = [
            'papers', 'paper_keywords', 'paper_author_affiliation',
            'categories', 'paper_categories', 'meta_update_logs',
            'pubmed_additional_info', 'paper_sources', 'paper_source_metadata',
            'paper_references', 'paper_source_artifacts'
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

    print()
    return True


# =============================================================================
# 腾讯云 VectorDB 初始化
# =============================================================================

def is_collection_already_exists_error(error: Exception) -> bool:
    """判断 VectorDB 错误是否表示 collection 已存在。"""
    error_text = str(error).lower()
    return (
        "collection already exist" in error_text
        or "collection already exists" in error_text
        or "already exist:" in error_text
        or "already exists:" in error_text
        or "已存在" in error_text
    )


def is_collection_not_exist_error(error: Exception) -> bool:
    """判断 VectorDB 错误是否表示 collection 不存在。"""
    error_text = str(error).lower()
    return (
        "not found" in error_text
        or "does not exist" in error_text
        or "not exist" in error_text
        or "不存在" in error_text
    )


def verify_collection_accessible(client, database: str, collection_name: str) -> tuple[bool, str]:
    """校验 collection 是否真的可访问。"""
    try:
        client.describe_collection(database, collection_name)
        return True, ""
    except Exception as e:
        return False, str(e)


def find_collection_in_other_databases(client, target_database: str, collection_name: str) -> list[str]:
    """查找 collection 是否存在于其他 database 中。"""
    matched_databases: list[str] = []

    try:
        databases = client.list_databases()
    except Exception:
        return matched_databases

    for database in databases:
        if database == target_database:
            continue
        try:
            collections = client.list_collections(database)
        except Exception:
            continue
        if collection_name in collections:
            matched_databases.append(database)

    return matched_databases


def setup_vector_db(config_path: Path, force_recreate: bool = False) -> bool:
    """初始化腾讯云向量数据库

    Args:
        config_path: 配置文件路径
        force_recreate: 是否强制重建（删除已存在的 collections）

    Returns:
        bool: 成功返回 True
    """
    print("=" * 60)
    print("☁️  腾讯云 VectorDB 初始化")
    print("=" * 60)
    print()

    # 读取配置
    try:
        config = load_config_from_yaml(config_path)
        if not config:
            print("❌ 配置文件为空或无法读取")
            return False
    except Exception as e:
        print(f"❌ 无法读取配置文件: {e}")
        return False

    # 检查 vector_db 配置
    if 'vector_db' not in config:
        print("⚠️  配置文件中未找到 vector_db 配置，跳过 VectorDB 初始化")
        return True

    vector_db_config = config['vector_db']

    # 检查 embedding_source
    embedding_source = vector_db_config.get('embedding_source', '')
    if embedding_source != 'tecent_made':
        print(f"ℹ️  当前配置不是腾讯云模式 (embedding_source={embedding_source})，跳过 VectorDB 初始化")
        return True

    # 提取配置
    url = vector_db_config.get('url')
    account = vector_db_config.get('account')
    api_key = vector_db_config.get('api_key')
    database = vector_db_config.get('database', 'langtaosha_mimic')
    embedding_model = vector_db_config.get('embedding_model', 'BAAI/bge-m3')
    collection_prefix = vector_db_config.get('collection_prefix', 'lt_')

    # 从 default_sources 获取允许的 sources
    allowed_sources = config.get('default_sources', [])

    # 验证必需字段
    if not all([url, account, api_key]):
        print("❌ vector_db 配置缺少必需字段 (url, account, api_key)")
        return False

    if not allowed_sources:
        print("❌ 配置文件中缺少 default_sources")
        return False

    print(f"配置文件: {config_path}")
    print(f"服务地址: {url}")
    print(f"数据库: {database}")
    print(f"Embedding 模型: {embedding_model}")
    print(f"Collection 前缀: {collection_prefix}")
    print(f"允许的 Sources: {', '.join(allowed_sources)}")
    print()

    # 创建客户端
    try:
        from src.docset_hub.storage.vector_db_client import (
            VectorDBClient,
            VectorDBClientError,
            VectorDBServerError
        )

        client = VectorDBClient(
            url=url,
            account=account,
            api_key=api_key
        )
        print("✅ 客户端创建成功")
        print()
    except Exception as e:
        print(f"❌ 创建客户端失败: {e}")
        return False

    # 创建数据库
    try:
        databases = client.list_databases()

        if database in databases:
            print(f"✅ VectorDB 数据库已存在: {database}")
        else:
            print(f"📝 创建 VectorDB 数据库: {database}")
            client.create_database(database)
            print(f"✅ VectorDB 数据库创建成功: {database}")
        print()
    except (VectorDBClientError, VectorDBServerError) as e:
        print(f"❌ 处理 VectorDB 数据库失败: {e}")
        return False

    # 列出现有的 collections
    try:
        existing_collections = client.list_collections(database)
        print(f"现有 Collections: {len(existing_collections)} 个")
        if existing_collections:
            for coll in existing_collections:
                print(f"  - {coll}")
        print()
    except Exception as e:
        print(f"⚠️  获取现有 collections 失败: {e}")
        existing_collections = []

    # 为每个 source 创建 collection
    success_count = 0
    failed_sources = []

    for source in allowed_sources:
        collection_name = f"{collection_prefix}{source}"

        try:
            # 检查 collection 是否存在
            if collection_name in existing_collections:
                if force_recreate:
                    print(f"🗑️  删除已存在的 collection: {collection_name}")
                    client.drop_collection(database, collection_name)
                    print(f"✅ 删除成功")
                else:
                    print(f"✅ Collection 已存在: {collection_name}")
                    success_count += 1
                    continue
            elif force_recreate:
                print(f"🗑️  尝试删除 collection: {collection_name}")
                try:
                    client.drop_collection(database, collection_name)
                    print("✅ 删除成功")
                except (VectorDBClientError, VectorDBServerError) as e:
                    if is_collection_not_exist_error(e):
                        print("ℹ️  collection 不存在，跳过删除")
                    else:
                        raise

            # 创建 collection
            print(f"📝 创建 collection: {collection_name} (source: {source})")
            client.create_collection(
                database=database,
                collection=collection_name,
                embedding_field="text",
                embedding_model=embedding_model
            )
            print(f"✅ Collection 创建成功: {collection_name}")
            success_count += 1

        except (VectorDBClientError, VectorDBServerError) as e:
            if is_collection_already_exists_error(e):
                accessible, verify_error = verify_collection_accessible(
                    client,
                    database,
                    collection_name
                )
                if accessible:
                    print(f"✅ Collection 已存在且可访问: {collection_name}")
                    success_count += 1
                else:
                    conflict_databases = find_collection_in_other_databases(
                        client,
                        database,
                        collection_name
                    )
                    print(f"❌ Collection 处于异常状态: {collection_name}")
                    print("   create 返回 already exist，但 describe 无法访问该 collection")
                    if conflict_databases:
                        print(f"   检测到同名 collection 位于其他 database: {', '.join(conflict_databases)}")
                        print("   推测该 VectorDB 服务要求 collection 名称全局唯一")
                        print("   建议修改 collection_prefix，例如为测试环境使用 lt_test_")
                    print(f"   校验错误: {verify_error}")
                    failed_sources.append(source)
                continue

            print(f"❌ 创建 collection 失败: {collection_name}")
            print(f"   错误: {e}")
            failed_sources.append(source)

    print()
    print("=" * 60)
    print("📊 VectorDB 创建结果统计")
    print("=" * 60)
    print(f"总计: {len(allowed_sources)} 个 sources")
    print(f"成功: {success_count} 个 collections")
    print(f"失败: {len(failed_sources)} 个 sources")

    if failed_sources:
        print()
        print(f"失败的 sources: {', '.join(failed_sources)}")
        return False

    print()
    print("✅ 腾讯云 VectorDB 初始化完成！")
    print()
    return True


# =============================================================================
# 主函数
# =============================================================================

def setup_all_databases(
    config_path: Path,
    force_recreate: bool = False,
    reset_metadata_db: bool = False
) -> bool:
    """初始化所有数据库

    Args:
        config_path: 配置文件路径
        force_recreate: 是否强制重建 VectorDB collections
        reset_metadata_db: 是否重置 PostgreSQL metadata_db

    Returns:
        bool: 全部成功返回 True
    """
    print()
    print("╔" + "═" * 58 + "╗")
    print("║" + " " * 15 + "🚀 数据库初始化工具" + " " * 22 + "║")
    print("╚" + "═" * 58 + "╝")
    print()
    print(f"配置文件: {config_path}")
    print()

    # 初始化 PostgreSQL metadata_db
    metadata_success = setup_metadata_db(config_path, reset_db=reset_metadata_db)

    # 初始化腾讯云 VectorDB
    vector_success = setup_vector_db(config_path, force_recreate=force_recreate)

    # 总结
    print()
    print("=" * 60)
    print("📊 初始化结果总结")
    print("=" * 60)
    print(f"PostgreSQL metadata_db: {'✅ 成功' if metadata_success else '❌ 失败'}")
    print(f"腾讯云 VectorDB: {'✅ 成功' if vector_success else '❌ 失败'}")
    print("=" * 60)
    print()

    if metadata_success and vector_success:
        print("✅ 所有数据库初始化完成！")
        print()
        print("🔍 可以使用以下命令查看存储信息:")
        print(f"   python scripts/display_db_storage_info_advanced.py --config-path {config_path}")
    else:
        print("❌ 部分数据库初始化失败，请检查错误信息")
    print()

    return metadata_success and vector_success


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='初始化 PostgreSQL metadata_db（schema + migrations）和腾讯云 VectorDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 使用默认配置文件
    python scripts/setup_databases.py

    # 指定配置文件
    python scripts/setup_databases.py --config-path src/config/config_tecent_backend_server_mimic.yaml

    # 强制重建已存在的 VectorDB collections
    python scripts/setup_databases.py --force-recreate
        """
    )

    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径（默认: src/config/config_tecent_backend_server_mimic.yaml）'
    )

    parser.add_argument(
        '--force-recreate',
        action='store_true',
        help='强制重建已存在的 VectorDB collections（⚠️  会删除现有数据）'
    )

    parser.add_argument(
        '--reset-metadata-db',
        action='store_true',
        help='删除并重建 PostgreSQL metadata_db 中的所有表（⚠️  会删除现有数据）'
    )

    args = parser.parse_args()

    # 确定配置文件路径
    if args.config_path:
        config_path = Path(args.config_path)
    else:
        config_path = Path(__file__).parent.parent / 'src' / 'config' / 'config_tecent_backend_server_mimic.yaml'

    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return 1

    # 确认危险操作
    if args.force_recreate:
        print("⚠️  警告: --force-recreate 选项会删除所有已存在的 VectorDB collections 和数据！")
        response = input("确定要继续吗？(yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("操作已取消")
            return 0

    if args.reset_metadata_db:
        print("⚠️  警告: --reset-metadata-db 选项会删除 PostgreSQL metadata_db 中的所有表和数据！")
        response = input("确定要继续吗？(yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("操作已取消")
            return 0

    # 执行初始化
    success = setup_all_databases(
        config_path,
        force_recreate=args.force_recreate,
        reset_metadata_db=args.reset_metadata_db
    )

    return 0 if success else 1


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    sys.exit(main())
