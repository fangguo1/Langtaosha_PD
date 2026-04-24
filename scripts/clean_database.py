#!/usr/bin/env python3
"""
数据库清理脚本
用于删除 PostgreSQL metadata_db 和/或腾讯云 VectorDB 的整个 database。

使用方式:
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_test.yaml
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_test.yaml --confirm
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_test.yaml --scope metadata --confirm
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_test.yaml --scope vector --confirm
"""

import sys
import argparse
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

VALID_SCOPES = ("all", "metadata", "vector")


def clean_metadata_db(config_path: Path, confirm: bool = False) -> bool:
    """删除整个 PostgreSQL metadata_db。

    Args:
        config_path: 配置文件路径
        confirm: 是否确认清理（安全措施）

    Returns:
        bool: 成功返回 True
    """
    if not confirm:
        print("⚠️  警告：此操作将删除整个 PostgreSQL metadata_db！")
        print("请设置 --confirm 参数来确认执行")
        return False

    from src.config.config_loader import load_config_from_yaml
    from sqlalchemy import text, create_engine
    from urllib.parse import quote_plus

    try:
        config = load_config_from_yaml(config_path)
        if not config:
            print("❌ 配置文件为空或无法读取")
            return False
    except Exception as e:
        print(f"❌ 无法读取配置文件: {e}")
        return False

    if 'metadata_db' not in config:
        print("⚠️  配置文件中未找到 metadata_db 配置，跳过 PostgreSQL 删除")
        return True

    db_config = config['metadata_db']
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    user = db_config.get('user')
    password = db_config.get('password')
    database = db_config.get('name')

    if not all([host, user, password, database]):
        print("❌ metadata_db 配置不完整，需要 host, port, user, password, name")
        return False

    encoded_password = quote_plus(str(password))
    encoded_user = quote_plus(str(user))

    engine = None
    for default_db in ('postgres', 'template1'):
        try:
            connection_string = f"postgresql://{encoded_user}:{encoded_password}@{host}:{port}/{default_db}"
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            break
        except Exception:
            if engine is not None:
                engine.dispose()
            engine = None

    if engine is None:
        print("❌ 无法连接到 PostgreSQL 服务器")
        print(f"   配置信息: host={host}, port={port}, user={user}")
        return False

    print("🔌 删除 PostgreSQL metadata_db...")
    print(f"  数据库: {database}")

    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")

            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :database"),
                {"database": database}
            )
            exists = result.fetchone()

            if not exists:
                print(f"  ℹ️  数据库不存在，跳过删除: {database}")
                return True

            conn.execute(
                text("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = :database
                    AND pid <> pg_backend_pid()
                """),
                {"database": database}
            )

            conn.execute(text(f'DROP DATABASE "{database}"'))
            print(f"✅ PostgreSQL metadata_db 删除成功: {database}")
            return True
    except Exception as e:
        print(f"❌ 删除 metadata_db 失败: {e}")
        return False
    finally:
        engine.dispose()


def clean_vector_db(config_path: Path, confirm: bool = False) -> bool:
    """删除整个腾讯云 VectorDB database。

    Args:
        config_path: 配置文件路径
        confirm: 是否确认清理（安全措施）

    Returns:
        bool: 成功返回 True
    """
    if not confirm:
        print("⚠️  警告：此操作将删除整个腾讯云 VectorDB database！")
        print("请设置 --confirm 参数来确认执行")
        return False

    # 读取配置
    from src.config.config_loader import load_config_from_yaml

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
        print("⚠️  配置文件中未找到 vector_db 配置，跳过 VectorDB 清理")
        return True

    vector_db_config = config['vector_db']

    # 检查是否是腾讯云模式
    embedding_source = vector_db_config.get('embedding_source', '')
    if embedding_source != 'tecent_made':
        print(f"ℹ️  当前配置不是腾讯云模式 (embedding_source: {embedding_source})，跳过 VectorDB 清理")
        return True

    print("🔌 清理腾讯云 VectorDB...")

    try:
        from src.docset_hub.storage.vector_db_client import (
            VectorDBClient,
            VectorDBClientError,
            VectorDBServerError
        )

        # 创建客户端
        client = VectorDBClient(
            url=vector_db_config['url'],
            account=vector_db_config['account'],
            api_key=vector_db_config['api_key']
        )

        database = vector_db_config.get('database', 'langtaosha_test')
        print(f"  数据库: {database}")

        try:
            databases = client.list_databases()
        except Exception as e:
            print(f"  ⚠️  获取数据库列表失败: {e}")
            return False

        if database not in databases:
            print(f"  ℹ️  VectorDB database 不存在，跳过删除: {database}")
            return True

        client.drop_database(database)
        print(f"✅ 腾讯云 VectorDB database 删除成功: {database}")
        return True

    except ImportError:
        print("⚠️  无法导入腾讯云 VectorDB 客户端库，跳过 VectorDB 清理")
        return True
    except (VectorDBClientError, VectorDBServerError) as e:
        print(f"❌ 删除 VectorDB database 失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 清理 VectorDB 失败: {e}")
        return False


def clean_databases(config_path: Path, confirm: bool = False, scope: str = "all") -> bool:
    """按范围删除整个数据库。

    Args:
        config_path: 配置文件路径
        confirm: 是否确认清理
        scope: 清理范围，支持 all / metadata / vector

    Returns:
        bool: 目标范围全部成功返回 True
    """
    if scope not in VALID_SCOPES:
        print(f"❌ 不支持的清理范围: {scope}")
        print(f"   支持的范围: {', '.join(VALID_SCOPES)}")
        return False

    print("=" * 60)
    print("🚀 数据库清理工具")
    print("=" * 60)
    print(f"配置文件: {config_path}")
    print(f"清理范围: {scope}")
    print()

    if not confirm:
        print("⚠️  说明：当前脚本会删除整个数据库实例，而不是只清空数据。")
        print("计划删除内容:")
        if scope in ("all", "metadata"):
            print("  - PostgreSQL metadata_db（DROP DATABASE）")
        if scope in ("all", "vector"):
            print("  - 腾讯云 VectorDB（删除整个 database）")
        print()
        print("请设置 --confirm 参数来确认执行")
        print()
        return False

    metadata_success = True
    vector_success = True

    if scope in ("all", "metadata"):
        metadata_success = clean_metadata_db(config_path, confirm=True)
        print()

    if scope in ("all", "vector"):
        vector_success = clean_vector_db(config_path, confirm=True)
        print()

    # 总结
    print("=" * 60)
    print("📊 清理结果总结")
    print("=" * 60)
    if scope in ("all", "metadata"):
        print(f"PostgreSQL metadata_db: {'✅ 成功' if metadata_success else '❌ 失败'}")
    else:
        print("PostgreSQL metadata_db: ⏭️  未执行")

    if scope in ("all", "vector"):
        print(f"腾讯云 VectorDB: {'✅ 成功' if vector_success else '❌ 失败'}")
    else:
        print("腾讯云 VectorDB: ⏭️  未执行")
    print("=" * 60)
    print()

    return metadata_success and vector_success


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='删除 PostgreSQL metadata_db 和/或腾讯云 VectorDB 的整个 database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 查看需要清理的内容（不实际执行）
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_mimic.yaml

    # 删除全部 database（需要确认）
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_mimic.yaml --confirm

    # 仅删除 metadata_db
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_mimic.yaml --scope metadata --confirm

    # 仅删除 VectorDB database
    python scripts/clean_database.py --config-path src/config/config_tecent_backend_server_mimic.yaml --scope vector --confirm
        """
    )

    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径（默认: src/config/config_tecent_backend_server_mimic.yaml）'
    )

    parser.add_argument(
        '--confirm',
        action='store_true',
        help='确认删除操作（⚠️  危险操作，会删除整个 metadata_db 和/或 VectorDB database）'
    )

    parser.add_argument(
        '--scope',
        type=str,
        choices=VALID_SCOPES,
        default='all',
        help='删除范围：all（默认，删除 metadata 和 vector）、metadata（只删除 metadata_db）、vector（只删除 VectorDB）'
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

    # 执行清理
    success = clean_databases(config_path, confirm=args.confirm, scope=args.scope)

    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
