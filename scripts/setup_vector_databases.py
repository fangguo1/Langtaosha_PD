#!/usr/bin/env python3
"""
向量数据库初始化脚本

根据配置文件创建腾讯云 VectorDB 数据库和 collections。

使用方式:
    python scripts/setup_vector_databases.py --config-path src/config/config_tecent_backend_server_test.yaml

功能:
    1. 创建数据库（如果不存在）
    2. 为每个允许的 source 创建 collection（如果不存在）
    3. 显示数据库和 collections 的创建状态
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.config_loader import init_config, get_vector_db_config
from src.docset_hub.storage.vector_db_client import (
    VectorDBClient,
    VectorDBClientError,
    VectorDBServerError
)

def _is_collection_not_exist_error(error: Exception) -> bool:
    """判断是否为 collection 不存在错误。"""
    err = str(error).lower()
    return (
        'not found' in err
        or 'does not exist' in err
        or 'not exist' in err
        or '不存在' in str(error)
    )


def _is_collection_already_exists_error(error: Exception) -> bool:
    """判断是否为 collection 已存在错误。"""
    err = str(error).lower()
    return 'already exist' in err or '已存在' in str(error)


def _verify_collection_accessible(
    client: VectorDBClient,
    database: str,
    collection_name: str
) -> tuple[bool, str]:
    """校验 collection 是否真的可访问。

    某些服务端异常状态下，create 会返回 already exist，但 list/describe 仍不可见。
    这里强制再做一次 describe，避免把幽灵 collection 误判为成功。
    """
    try:
        client.describe_collection(database, collection_name)
        return True, ""
    except (VectorDBClientError, VectorDBServerError) as e:
        return False, str(e)


def _find_collection_in_other_databases(
    client: VectorDBClient,
    target_database: str,
    collection_name: str
) -> list[str]:
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


def setup_vector_database(config_path: Path, force_recreate: bool = False) -> bool:
    """设置向量数据库

    Args:
        config_path: 配置文件路径
        force_recreate: 是否强制重建（删除已存在的 collections）

    Returns:
        bool: 成功返回 True
    """
    # 读取配置
    try:
        init_config(config_path, force_reload=True)
        vector_db_config = get_vector_db_config()
    except Exception as e:
        print(f"❌ 无法读取配置文件: {e}")
        return False

    # 检查 embedding_source
    embedding_source = vector_db_config.get('embedding_source', '')
    if embedding_source != 'tecent_made':
        print(f"❌ 当前脚本仅支持腾讯云模式 (embedding_source=tecent_made)")
        print(f"   当前配置: embedding_source={embedding_source}")
        return False

    # 提取配置
    url = vector_db_config.get('url')
    account = vector_db_config.get('account')
    api_key = vector_db_config.get('api_key')
    database = vector_db_config.get('database', 'langtaosha_test')
    embedding_model = vector_db_config.get('embedding_model', 'BAAI/bge-m3')
    collection_prefix = vector_db_config.get('collection_prefix', 'lt_')
    allowed_sources = vector_db_config.get('allowed_sources', [])

    # 验证必需字段
    if not all([url, account, api_key]):
        print("❌ vector_db 配置缺少必需字段 (url, account, api_key)")
        return False

    if not allowed_sources:
        print("❌ vector_db 配置缺少 allowed_sources")
        return False

    print("=" * 60)
    print("🚀 向量数据库初始化")
    print("=" * 60)
    print()
    print(f"配置文件: {config_path}")
    print(f"服务地址: {url}")
    print(f"数据库: {database}")
    print(f"Embedding 模型: {embedding_model}")
    print(f"Collection 前缀: {collection_prefix}")
    print(f"允许的 Sources: {', '.join(allowed_sources)}")
    print()

    # 创建客户端
    try:
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
            print(f"✅ 数据库已存在: {database}")
        else:
            print(f"📝 创建数据库: {database}")
            client.create_database(database)
            print(f"✅ 数据库创建成功: {database}")
        print()
    except (VectorDBClientError, VectorDBServerError) as e:
        print(f"❌ 处理数据库失败: {e}")
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
            # force_recreate 模式下，不依赖 list 结果，直接尝试删除后重建
            if force_recreate:
                print(f"🗑️  尝试删除 collection: {collection_name}")
                try:
                    client.drop_collection(database, collection_name)
                    print("✅ 删除成功")
                except (VectorDBClientError, VectorDBServerError) as e:
                    if _is_collection_not_exist_error(e):
                        print("ℹ️  collection 不存在，跳过删除")
                    else:
                        raise
            elif collection_name in existing_collections:
                print(f"✅ Collection 已存在: {collection_name}")
                success_count += 1
                continue

            # 创建 collection
            print(f"📝 创建 collection: {collection_name} (source: {source})")
            try:
                client.create_collection(
                    database=database,
                    collection=collection_name,
                    embedding_field="text",
                    embedding_model=embedding_model
                )
                print(f"✅ Collection 创建成功: {collection_name}")
                success_count += 1
            except (VectorDBClientError, VectorDBServerError) as e:
                # list 可能返回不完整，但仍需确认 collection 可访问
                if _is_collection_already_exists_error(e):
                    accessible, verify_error = _verify_collection_accessible(
                        client,
                        database,
                        collection_name
                    )
                    if accessible:
                        print(f"ℹ️  Collection 已存在且可访问，按成功处理: {collection_name}")
                        success_count += 1
                    else:
                        conflict_databases = _find_collection_in_other_databases(
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
                else:
                    raise

        except (VectorDBClientError, VectorDBServerError) as e:
            print(f"❌ 创建 collection 失败: {collection_name}")
            print(f"   错误: {e}")
            failed_sources.append(source)

    print()
    print("=" * 60)
    print("📊 创建结果统计")
    print("=" * 60)
    print(f"总计: {len(allowed_sources)} 个 sources")
    print(f"成功: {success_count} 个 collections")
    print(f"失败: {len(failed_sources)} 个 sources")

    if failed_sources:
        print()
        print(f"失败的 sources: {', '.join(failed_sources)}")
        return False

    print()
    print("✅ 向量数据库初始化完成！")
    return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='初始化腾讯云向量数据库和 collections',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 使用默认配置文件
    python scripts/setup_vector_databases.py

    # 指定配置文件
    python scripts/setup_vector_databases.py --config-path src/config/config_tecent_backend_server_test.yaml

    # 强制重建已存在的 collections
    python scripts/setup_vector_databases.py --force-recreate
        """
    )

    parser.add_argument(
        '--config-path',
        type=str,
        default=None,
        help='配置文件路径（默认: src/config/config_tecent_backend_server_test.yaml）'
    )

    parser.add_argument(
        '--force-recreate',
        action='store_true',
        help='强制重建已存在的 collections（⚠️  会删除现有数据）'
    )

    args = parser.parse_args()

    # 确定配置文件路径
    if args.config_path:
        config_path = Path(args.config_path)
    else:
        config_path = Path(__file__).parent.parent / 'src' / 'config' / 'config_tecent_backend_server_test.yaml'

    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return 1

    # 确认危险操作
    if args.force_recreate:
        print("⚠️  警告: --force-recreate 选项会删除所有已存在的 collections 和数据！")
        response = input("确定要继续吗？(yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("操作已取消")
            return 0

    # 执行初始化
    success = setup_vector_database(config_path, args.force_recreate)

    return 0 if success else 1



# 使用默认配置文件
#python scripts/setup_vector_databases.py

# 指定配置文件
#python scripts/setup_vector_databases.py --config-path src/config/config_tecent_backend_server_test.yaml

# 强制重建（会删除现有数据）
#python scripts/setup_vector_databases.py --force-recreate
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    sys.exit(main())
