"""测试版本比较工具"""
import pytest
from docset_hub.storage.version_utils import compare_versions, should_update_by_version


class TestCompareVersions:
    """测试版本比较功能"""

    def test_compare_versions_higher(self):
        """测试新版本更高"""
        result, method = compare_versions("2.0.0", "1.0.0")
        assert result == 1
        assert method == "version"

    def test_compare_versions_lower(self):
        """测试新版本更低"""
        result, method = compare_versions("1.0.0", "2.0.0")
        assert result == -1
        assert method == "version"

    def test_compare_versions_equal(self):
        """测试版本相同"""
        result, method = compare_versions("1.0.0", "1.0.0")
        assert result == 0
        assert method == "equal"

    def test_compare_versions_both_none(self):
        """测试两个版本都为 None"""
        result, method = compare_versions(None, None)
        assert result == 0
        assert method == "equal"

    def test_compare_versions_one_none(self):
        """测试一个版本为 None"""
        with pytest.raises(ValueError, match="Cannot compare versions"):
            compare_versions("1.0.0", None)

        with pytest.raises(ValueError, match="Cannot compare versions"):
            compare_versions(None, "1.0.0")

    def test_compare_versions_semantic(self):
        """测试语义化版本比较"""
        result, method = compare_versions("1.10.0", "1.2.0")
        assert result == 1  # 1.10.0 > 1.2.0
        assert method == "version"

    def test_compare_versions_with_v_prefix(self):
        """测试带 v 前缀的版本"""
        result, method = compare_versions("v2.0.0", "v1.0.0")
        assert result == 1
        assert method == "version"

    def test_compare_versions_invalid_format(self):
        """测试无效的版本格式"""
        with pytest.raises(ValueError, match="Invalid version format"):
            compare_versions("invalid", "1.0.0")


class TestShouldUpdateByVersion:
    """测试是否应该根据版本更新"""

    def test_should_update_newer_version(self):
        """测试新版本更高，应该更新"""
        should_update, reason = should_update_by_version(
            "2.0.0", "1.0.0", "2026-04-10", "2026-04-05"
        )
        assert should_update is True
        assert "New version 2.0.0 > existing version 1.0.0" in reason

    def test_should_not_update_older_version(self):
        """测试新版本更低，不应该更新"""
        should_update, reason = should_update_by_version(
            "1.0.0", "2.0.0", "2026-04-10", "2026-04-05"
        )
        assert should_update is False
        assert "New version 1.0.0 < existing version 2.0.0" in reason

    def test_should_update_same_version_later_online_at(self):
        """测试版本相同但 online_at 更晚，应该更新"""
        should_update, reason = should_update_by_version(
            "1.0.0", "1.0.0", "2026-04-10", "2026-04-05"
        )
        assert should_update is True
        assert "Same version, new online_at 2026-04-10 > existing 2026-04-05" in reason

    def test_should_not_update_same_version_earlier_online_at(self):
        """测试版本相同但 online_at 更早，不应该更新"""
        should_update, reason = should_update_by_version(
            "1.0.0", "1.0.0", "2026-04-05", "2026-04-10"
        )
        assert should_update is False
        assert "Same version, new online_at 2026-04-05 <= existing 2026-04-10" in reason

    def test_should_not_update_no_comparable_info(self):
        """测试无可比较信息，不应该更新"""
        should_update, reason = should_update_by_version(
            None, None, None, None
        )
        assert should_update is False
        assert "not comparable" in reason

    def test_should_update_fallback_to_online_at(self):
        """测试版本比较失败，降级到 online_at 比较"""
        should_update, reason = should_update_by_version(
            "invalid", "1.0.0", "2026-04-10", "2026-04-05"
        )
        assert should_update is True
        assert "Version comparison failed, new online_at" in reason
