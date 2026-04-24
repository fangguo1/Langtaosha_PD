"""集成测试：测试完整的元数据处理流程"""
import pytest

from docset_hub.metadata.router import MetadataRouter
from docset_hub.metadata.source_adapters import LangtaoshaSourceAdapter, BiorxivSourceAdapter
from docset_hub.metadata.normalizer import MetadataNormalizer
from .fixtures import LANGTAOSHA_SAMPLE, BIORXIV_SAMPLE


class TestIntegration:
    """测试完整的元数据处理流程"""

    def test_langtaosha_full_pipeline(self):
        """测试 Langtaosha 完整处理流程"""
        # Step 1: 路由（显式指定来源）
        router = MetadataRouter()
        route_result = router.route(LANGTAOSHA_SAMPLE, source_name="langtaosha")

        assert route_result.source_adapter == "langtaosha"

        # Step 2: 字段映射
        source_adapter = LangtaoshaSourceAdapter()
        record = source_adapter.transform(LANGTAOSHA_SAMPLE)

        assert record.source_name == "langtaosha"
        assert record.core.title == LANGTAOSHA_SAMPLE["citation_title"]

        # Step 3: 归一化
        normalizer = MetadataNormalizer()
        normalized_record = normalizer.normalize(record)

        # 验证归一化结果
        assert normalized_record.identifiers.doi == "10.1234/langtaosha.001"
        assert normalized_record.core.language == "en"
        assert normalized_record.core.submitted_at == "2026-04-01"

        # 验证原始记录未被修改
        assert record.identifiers.doi == LANGTAOSHA_SAMPLE["citation_doi"]

    def test_biorxiv_full_pipeline(self):
        """测试 bioRxiv 完整处理流程"""
        # Step 1: 路由（显式指定来源）
        router = MetadataRouter()
        route_result = router.route(BIORXIV_SAMPLE, source_name="biorxiv_history")

        assert route_result.source_adapter == "biorxiv_history"

        # Step 2: 字段映射
        source_adapter = BiorxivSourceAdapter(source_name="biorxiv_history")
        record = source_adapter.transform(BIORXIV_SAMPLE)

        assert record.source_name == "biorxiv_history"
        assert record.core.title == BIORXIV_SAMPLE["title"]

        # Step 3: 归一化
        normalizer = MetadataNormalizer()
        normalized_record = normalizer.normalize(record)

        # 验证归一化结果
        assert normalized_record.identifiers.doi == BIORXIV_SAMPLE["doi"]
        assert normalized_record.core.online_at == "2026-04-08"

        # 验证作者被正确解析
        assert len(normalized_record.authors) == 10
        assert normalized_record.authors[0].name == "Zhang, J."

    def test_explicit_source_to_adapter_mapping(self):
        """测试显式指定来源与 adapter 的匹配"""
        router = MetadataRouter()

        # 测试 Langtaosha 路由（显式指定）
        langtaosha_result = router.route(LANGTAOSHA_SAMPLE, source_name="langtaosha")
        assert langtaosha_result.source_adapter == "langtaosha"

        # 根据路由结果选择 adapter
        if langtaosha_result.source_adapter == "langtaosha":
            adapter = LangtaoshaSourceAdapter()
        else:
            raise ValueError("Unexpected adapter")

        record = adapter.transform(LANGTAOSHA_SAMPLE)
        assert record.source_name == "langtaosha"

        # 测试 bioRxiv 路由（显式指定）
        biorxiv_result = router.route(BIORXIV_SAMPLE, source_name="biorxiv_history")
        assert biorxiv_result.source_adapter == "biorxiv_history"

        # 根据路由结果选择 adapter
        if biorxiv_result.source_adapter == "biorxiv_history":
            adapter = BiorxivSourceAdapter(source_name="biorxiv_history")
        else:
            raise ValueError("Unexpected adapter")

        record = adapter.transform(BIORXIV_SAMPLE)
        assert record.source_name == "biorxiv_history"

    def test_pipeline_with_validation(self):
        """测试包含校验的完整流程"""
        router = MetadataRouter()
        source_adapter = LangtaoshaSourceAdapter()
        normalizer = MetadataNormalizer()

        # 处理 Langtaosha 数据（显式指定来源）
        route_result = router.route(LANGTAOSHA_SAMPLE, source_name="langtaosha")
        record = source_adapter.transform(LANGTAOSHA_SAMPLE)
        normalized_record = normalizer.normalize(record)

        # 验证每一步都成功
        assert route_result.source_adapter == "langtaosha"
        assert normalized_record.core.title == LANGTAOSHA_SAMPLE["citation_title"]
        assert normalized_record.identifiers.langtaosha == "181"

        # 验证记录可以序列化
        serialized = normalized_record.to_dict()
        assert "source_name" in serialized
        assert "core" in serialized
        assert "identifiers" in serialized
