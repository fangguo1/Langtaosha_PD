"""测试路由器"""
import pytest

from docset_hub.metadata.router import MetadataRouter, RoutingError, RouteResult


class TestMetadataRouter:
    """测试元数据路由器"""

    def test_route_with_valid_source(self):
        """测试使用有效来源名称路由"""
        router = MetadataRouter()

        result = router.route(
            payload={"title": "Test"},
            source_name="langtaosha",
        )

        assert result.source_adapter == "langtaosha"
        assert "explicitly specified" in result.reason
        assert result.confidence == 1.0

    def test_route_with_invalid_source(self):
        """测试使用无效来源名称时抛出异常"""
        router = MetadataRouter()

        with pytest.raises(RoutingError, match="Unsupported source"):
            router.route(
                payload={"title": "Test"},
                source_name="invalid_source",
            )

    def test_route_error_message_includes_supported_sources(self):
        """测试错误消息包含支持的来源列表"""
        router = MetadataRouter()

        with pytest.raises(RoutingError) as exc_info:
            router.route(
                payload={"title": "Test"},
                source_name="invalid_source",
            )

        error_msg = str(exc_info.value)
        assert "invalid_source" in error_msg
        assert "langtaosha" in error_msg
        assert "biorxiv_history" in error_msg

    def test_route_all_supported_sources(self):
        """测试所有支持的来源"""
        router = MetadataRouter()

        for source in router.SUPPORTED_SOURCES:
            result = router.route(
                payload={"title": "Test"},
                source_name=source,
            )

            assert result.source_adapter == source
            assert result.confidence == 1.0

    def test_batch_route(self):
        """测试批量路由"""
        router = MetadataRouter()

        payloads = [
            {"title": "Test 1"},
            {"title": "Test 2"},
        ]
        source_names = ["langtaosha", "biorxiv_history"]

        results = router.batch_route(payloads, source_names)

        assert len(results) == 2
        assert results[0].source_adapter == "langtaosha"
        assert results[1].source_adapter == "biorxiv_history"

    def test_batch_route_length_mismatch(self):
        """测试批量路由时长度不匹配"""
        router = MetadataRouter()

        payloads = [{"title": "Test 1"}]
        source_names = ["langtaosha", "biorxiv_history"]  # 长度不匹配

        with pytest.raises(ValueError, match="must have the same length"):
            router.batch_route(payloads, source_names)

    def test_batch_route_with_invalid_source(self):
        """测试批量路由时包含无效来源"""
        router = MetadataRouter()

        payloads = [
            {"title": "Test 1"},
            {"title": "Test 2"},
        ]
        source_names = ["langtaosha", "invalid_source"]

        with pytest.raises(RoutingError, match="Unsupported source"):
            router.batch_route(payloads, source_names)

    def test_route_result_to_dict(self):
        """测试 RouteResult 转字典"""
        result = RouteResult(
            source_adapter="langtaosha",
            reason="test reason",
            confidence=0.9,
        )

        data = result.to_dict()

        assert data["source_adapter"] == "langtaosha"
        assert data["reason"] == "test reason"
        assert data["confidence"] == 0.9

    def test_route_ignores_payload_content(self):
        """测试路由器忽略 payload 内容，只使用 source_name"""
        router = MetadataRouter()

        # 即使 payload 包含 bioRxiv 的 DOI，如果指定为 langtaosha，也应该路由到 langtaosha
        payload_with_biorxiv_doi = {
            "title": "Test",
            "doi": "10.1101/2021.11.22.469359",  # bioRxiv DOI
        }

        result = router.route(payload_with_biorxiv_doi, source_name="langtaosha")

        assert result.source_adapter == "langtaosha"
