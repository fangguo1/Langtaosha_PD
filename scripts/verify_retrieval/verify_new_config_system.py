"""验证新配置系统的集成测试

1. 验证 default_sources 可以被正确读取
2. 验证 VectorDB 可以正常初始化（自动注入 allowed_sources）
3. 验证多 source 操作正常工作

运行方式：
    python scripts/verify_new_config_system.py
"""

from config.config_loader import init_config, get_default_sources, get_vector_db_config
from docset_hub.storage.vector_db import VectorDB
from pathlib import Path


def main():
    config_path = Path("src/config/config_tecent_backend_server_example.yaml")

    if not config_path.exists():
        raise FileNotFoundError(f"示例配置文件不存在: {config_path}")

    print("=" * 60)
    print("新配置系统集成测试")
    print("=" * 60)

    # 1. 测试 get_default_sources
    print("\n1. 测试 get_default_sources()")
    print("-" * 60)
    try:
        init_config(config_path)
        sources = get_default_sources()
        print(f"   ✅ 成功读取默认 sources: {sources}")
        assert len(sources) > 0, "default_sources 不能为空"
        assert 'langtaosha' in sources, "应该包含 langtaosha"
        assert 'biorxiv_history' in sources, "应该包含 biorxiv_history"
        assert 'biorxiv_daily' in sources, "应该包含 biorxiv_daily"
    except Exception as e:
        print(f"   ❌ 失败: {e}")
        raise

    # 2. 测试 get_vector_db_config 自动注入
    print("\n2. 测试 get_vector_db_config() 自动注入")
    print("-" * 60)
    try:
        vector_config = get_vector_db_config()
        print(f"   ✅ 成功获取 vector_db 配置")

        assert 'allowed_sources' in vector_config, "应该包含 allowed_sources"
        print(f"   ✅ allowed_sources 已自动注入: {vector_config['allowed_sources']}")

        assert vector_config['allowed_sources'] == sources, "allowed_sources 应该与 default_sources 一致"
        print(f"   ✅ allowed_sources 与 default_sources 一致")
    except Exception as e:
        print(f"   ❌ 失败: {e}")
        raise

    # 3. 测试 VectorDB 初始化
    print("\n3. 测试 VectorDB 初始化")
    print("-" * 60)
    try:
        vector_db = VectorDB(config_path=config_path)
        print(f"   ✅ VectorDB 初始化成功")

        print(f"   ✅ VectorDB.allowed_sources: {vector_db.allowed_sources}")
        assert vector_db.allowed_sources == sources, "VectorDB.allowed_sources 应该与 default_sources 一致"
        print(f"   ✅ VectorDB.allowed_sources 与 default_sources 一致")
    except Exception as e:
        print(f"   ❌ 失败: {e}")
        raise

    # 4. 测试 source 验证
    print("\n4. 测试 source 验证")
    print("-" * 60)
    try:
        all_valid = True
        for source in sources:
            try:
                vector_db._validate_source(source)
                print(f"   ✅ Source '{source}' 验证通过")
            except ValueError as e:
                print(f"   ❌ Source '{source}' 验证失败: {e}")
                all_valid = False

        if not all_valid:
            raise ValueError("部分 source 验证失败")
    except Exception as e:
        print(f"   ❌ 失败: {e}")
        raise

    # 5. 测试无效 source 被拒绝
    print("\n5. 测试无效 source 被拒绝")
    print("-" * 60)
    try:
        try:
            vector_db._validate_source('invalid_source')
            print(f"   ❌ 应该拒绝无效的 source")
            raise AssertionError("无效 source 未被拒绝")
        except ValueError as e:
            print(f"   ✅ 正确拒绝无效 source: {e}")
    except Exception as e:
        print(f"   ❌ 失败: {e}")
        raise

    # 6. 测试配置不可变性
    print("\n6. 测试配置不可变性（返回拷贝）")
    print("-" * 60)
    try:
        sources1 = get_default_sources()
        sources2 = get_default_sources()

        # 修改第一个列表
        sources1.append("test_source")

        # 第二个列表不应该受影响
        assert "test_source" not in sources2, "get_default_sources() 应该返回拷贝"
        print(f"   ✅ get_default_sources() 返回拷贝，防止外部修改")

        # 测试 vector_db_config 也是拷贝
        config1 = get_vector_db_config()
        config2 = get_vector_db_config()

        config1['allowed_sources'].append("test_source")
        assert "test_source" not in config2['allowed_sources'], "get_vector_db_config() 应该返回拷贝"
        print(f"   ✅ get_vector_db_config() 返回拷贝，防止外部修改")
    except Exception as e:
        print(f"   ❌ 失败: {e}")
        raise

    print("\n" + "=" * 60)
    print("✅ 所有集成测试通过！")
    print("=" * 60)
    print("\n总结:")
    print("  ✅ default_sources 可以正确读取")
    print("  ✅ get_vector_db_config() 自动注入 allowed_sources")
    print("  ✅ VectorDB 初始化成功并使用注入的 allowed_sources")
    print("  ✅ source 验证正常工作")
    print("  ✅ 无效 source 被正确拒绝")
    print("  ✅ 配置返回拷贝，防止意外修改")
    print("\n新配置系统运行正常！🎉")


if __name__ == "__main__":
    main()
