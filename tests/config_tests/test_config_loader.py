"""测试 config_loader 的新功能

运行方式：
    pytest tests/config/test_config_loader.py -v
"""

import pytest
import yaml
import tempfile
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.config_loader import (
    init_config,
    get_default_sources,
    get_vector_db_config,
    _reset_config
)


@pytest.fixture(autouse=True)
def reset_config_before_each_test():
    """每个测试前重置配置缓存"""
    _reset_config()
    yield
    _reset_config()


class TestDefaultSources:
    """测试 get_default_sources() 功能"""

    def test_get_default_sources_returns_list(self):
        """测试 get_default_sources 返回正确的列表"""
        config_path = Path("src/config/config_tecent_backend_server_example.yaml")
        init_config(config_path)

        sources = get_default_sources()

        assert isinstance(sources, list)
        assert len(sources) > 0
        assert all(isinstance(s, str) for s in sources)

    def test_get_default_sources_content(self):
        """测试 get_default_sources 返回预期的 sources"""
        config_path = Path("src/config/config_tecent_backend_server_example.yaml")
        init_config(config_path)

        sources = get_default_sources()

        # 验证包含预期的 sources
        assert 'langtaosha' in sources
        assert 'biorxiv_history' in sources
        assert 'biorxiv_daily' in sources

    def test_get_default_sources_returns_copy(self):
        """测试 get_default_sources 返回的是拷贝，不是原始列表"""
        config_path = Path("src/config/config_tecent_backend_server_example.yaml")
        init_config(config_path)

        sources1 = get_default_sources()
        sources2 = get_default_sources()

        # 修改返回的列表不应影响后续调用
        sources1.append("new_source")
        assert "new_source" not in sources2

    def test_get_default_sources_missing_raises_error(self):
        """测试缺少 default_sources 时抛出错误"""
        # 创建一个没有 default_sources 的临时配置
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'vector_db': {'url': 'http://test'}}, f)
            temp_config = Path(f.name)

        try:
            init_config(temp_config)

            with pytest.raises(ValueError, match="未找到 default_sources"):
                get_default_sources()
        finally:
            temp_config.unlink()

    def test_get_default_sources_empty_list_raises_error(self):
        """测试 default_sources 为空列表时抛出错误"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'default_sources': []}, f)
            temp_config = Path(f.name)

        try:
            init_config(temp_config)

            with pytest.raises(ValueError, match="不能为空列表"):
                get_default_sources()
        finally:
            temp_config.unlink()

    def test_get_default_sources_invalid_type_raises_error(self):
        """测试 default_sources 类型错误时抛出异常"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'default_sources': 'not_a_list'}, f)
            temp_config = Path(f.name)

        try:
            init_config(temp_config)

            with pytest.raises(ValueError, match="必须是列表类型"):
                get_default_sources()
        finally:
            temp_config.unlink()

    def test_get_default_sources_empty_string_raises_error(self):
        """测试 default_sources 包含空字符串时抛出错误"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'default_sources': ['source1', '', 'source2']}, f)
            temp_config = Path(f.name)

        try:
            init_config(temp_config)

            with pytest.raises(ValueError, match="不能为空字符串"):
                get_default_sources()
        finally:
            temp_config.unlink()

    def test_get_default_sources_non_string_element_raises_error(self):
        """测试 default_sources 包含非字符串元素时抛出错误"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'default_sources': ['source1', 123, 'source2']}, f)
            temp_config = Path(f.name)

        try:
            init_config(temp_config)

            with pytest.raises(ValueError, match="必须是字符串类型"):
                get_default_sources()
        finally:
            temp_config.unlink()


class TestVectorDBConfigAutoInjection:
    """测试 get_vector_db_config() 自动注入 allowed_sources"""

    def test_auto_inject_allowed_sources(self):
        """测试自动注入 allowed_sources"""
        config_path = Path("src/config/config_tecent_backend_server_example.yaml")
        init_config(config_path)

        config = get_vector_db_config()

        assert 'allowed_sources' in config
        assert config['allowed_sources'] is not None
        assert len(config['allowed_sources']) > 0

    def test_injected_allowed_sources_matches_default_sources(self):
        """测试注入的 allowed_sources 与 default_sources 一致"""
        config_path = Path("src/config/config_tecent_backend_server_example.yaml")
        init_config(config_path)

        vector_config = get_vector_db_config()
        default_sources = get_default_sources()

        assert vector_config['allowed_sources'] == default_sources

    def test_allowed_sources_always_from_default_sources(self):
        """测试 allowed_sources 始终从 default_sources 获取"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({
                'default_sources': ['source1', 'source2'],
                'vector_db': {
                    'url': 'http://test'
                }
            }, f)
            temp_config = Path(f.name)

        try:
            init_config(temp_config)
            config = get_vector_db_config()

            # 应该始终使用 default_sources
            assert config['allowed_sources'] == ['source1', 'source2']
        finally:
            temp_config.unlink()

    def test_missing_default_sources_raises_error(self):
        """测试缺少 default_sources 时抛出错误"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({
                'vector_db': {'url': 'http://test'}
            }, f)
            temp_config = Path(f.name)

        try:
            init_config(temp_config)

            with pytest.raises(ValueError, match="未找到 default_sources"):
                get_vector_db_config()
        finally:
            temp_config.unlink()

    def test_vector_db_config_returns_copy(self):
        """测试 get_vector_db_config 返回的是拷贝，不是原始字典"""
        config_path = Path("src/config/config_tecent_backend_server_example.yaml")
        init_config(config_path)

        config1 = get_vector_db_config()
        config2 = get_vector_db_config()

        # 修改返回的字典不应影响后续调用
        config1['allowed_sources'].append("new_source")
        assert "new_source" not in config2['allowed_sources']
