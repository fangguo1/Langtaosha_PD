"""测试 MetadataTransformer"""

import pytest
from pathlib import Path
from docset_hub.metadata.transformer import (
    MetadataTransformer,
    TransformResult,
    TransformStats,
    TransformerError,
)


class TestMetadataTransformer:
    """测试 MetadataTransformer"""

    def test_init(self):
        """测试初始化"""
        transformer = MetadataTransformer()
        assert transformer.parser_version == "1.0.0"
        assert transformer.source_schema_version == "2025-04-13"
        assert transformer.default_language == "en"

        # 测试自定义配置
        transformer = MetadataTransformer(
            parser_version="2.0.0",
            source_schema_version="2025-04-14",
            default_language="zh"
        )
        assert transformer.parser_version == "2.0.0"
        assert transformer.source_schema_version == "2025-04-14"
        assert transformer.default_language == "zh"

    def test_transform_file_success(self, fixtures_path):
        """测试成功转换单个文件"""
        transformer = MetadataTransformer()

        # 测试 Langtaosha 文件
        result = transformer.transform_file(
            input_path=fixtures_path / "langtaosha" / "article_184.json",
            source_name="langtaosha"
        )

        assert result.success is True
        assert result.source_name == "langtaosha"
        assert result.db_payload is not None
        assert result.upsert_key is not None
        assert result.work_id is None
        assert result.error is None
        assert result.execution_time > 0

        # 验证 upsert_key
        assert result.upsert_key["source_name"] == "langtaosha"
        assert result.upsert_key["source_identifiers"]["langtaosha"] == "184"

        # 验证数据库 payload
        payload = result.db_payload
        assert payload["papers"] is not None
        assert payload["papers"]["work_id"] is None
        assert payload["paper_sources"] is not None
        assert payload["paper_source_metadata"] is not None
        assert payload["paper_author_affiliation"] is not None
        assert isinstance(payload["paper_keywords"], list)
        assert isinstance(payload["paper_references"], list)

    def test_transform_file_biorxiv(self, fixtures_path):
        """测试转换 bioRxiv 文件"""
        transformer = MetadataTransformer()

        result = transformer.transform_file(
            input_path=fixtures_path / "biorxiv_daily" / "biorxiv_daily_1_10.1101_2025.05.05.652292.json",
            source_name="biorxiv_daily"
        )

        assert result.success is True
        assert result.source_name == "biorxiv_daily"
        assert result.db_payload is not None
        assert result.db_payload["papers"]["work_id"] is None
        assert result.work_id is None

        # 验证 upsert_key
        assert result.upsert_key["source_name"] == "biorxiv_daily"
        assert "10.1101" in result.upsert_key["doi"]

    def test_transform_file_not_found(self):
        """测试文件不存在"""
        transformer = MetadataTransformer()

        result = transformer.transform_file(
            input_path="nonexistent.json",
            source_name="langtaosha"
        )

        assert result.success is False
        assert result.error is not None
        assert "File not found" in result.error or "does not exist" in result.error

    def test_transform_file_unsupported_format(self, fixtures_path):
        """测试不支持的文件格式"""
        transformer = MetadataTransformer()

        result = transformer.transform_file(
            input_path=fixtures_path / "unsupported.txt",  # 假设存在这个文件
            source_name="langtaosha"
        )

        assert result.success is False
        assert result.error is not None
        assert "Unsupported file format" in result.error

    def test_transform_dict_success(self):
        """测试从字典转换"""
        transformer = MetadataTransformer()

        raw_data = {
            "url": "https://langtaosha.org.cn/lts/en/preprint/view/185",
            "meta": {
                "citation_title": ["Example Paper"],
                "citation_author": ["Author One", "Author Two"],
                "citation_abstract": ["This is an example abstract."],
                "citation_doi": ["10.65215/example.2026.04.14.000185"],
                "citation_date": ["2026/04/14"],
                "citation_abstract_html_url": ["https://langtaosha.org.cn/lts/en/preprint/view/185"],
            },
        }

        result = transformer.transform_dict(
            raw_payload=raw_data,
            source_name="langtaosha"
        )

        assert result.success is True
        assert result.source_name == "langtaosha"
        assert result.db_payload is not None
        assert result.upsert_key is not None
        assert result.db_payload["papers"]["work_id"] is None
        assert result.work_id is None

    def test_transform_dict_unsupported_source(self):
        """测试不支持的来源"""
        transformer = MetadataTransformer()

        result = transformer.transform_dict(
            raw_payload={},
            source_name="unsupported_source"
        )

        assert result.success is False
        assert result.error is not None
        assert "Unsupported source" in result.error

    def test_transform_batch_success(self, fixtures_path):
        """测试批量转换"""
        transformer = MetadataTransformer()

        batch = [
            {
                "input_path": fixtures_path / "langtaosha" / "article_184.json",
                "source_name": "langtaosha"
            },
            {
                "input_path": fixtures_path / "biorxiv_daily" / "biorxiv_daily_1_10.1101_2025.05.05.652292.json",
                "source_name": "biorxiv_daily"
            },
        ]

        results, stats = transformer.transform_batch(batch=batch)

        # 验证统计信息
        assert stats.total == 2
        assert stats.successful == 2
        assert stats.failed == 0
        assert stats.success_rate == 100.0

        # 验证结果
        assert len(results) == 2
        assert all(r.success for r in results)

    def test_transform_batch_with_errors(self, fixtures_path):
        """测试批量转换包含错误"""
        transformer = MetadataTransformer()

        batch = [
            {
                "input_path": fixtures_path / "langtaosha" / "article_184.json",
                "source_name": "langtaosha"
            },
            {
                "input_path": "nonexistent.json",
                "source_name": "langtaosha"
            },
        ]

        results, stats = transformer.transform_batch(batch=batch)

        # 验证统计信息
        assert stats.total == 2
        assert stats.successful == 1
        assert stats.failed == 1
        assert stats.success_rate == 50.0

        # 验证结果
        assert len(results) == 2
        assert results[0].success is True
        assert results[1].success is False

    def test_transform_batch_continue_on_error(self, fixtures_path):
        """测试遇到错误是否继续"""
        transformer = MetadataTransformer()

        batch = [
            {
                "input_path": "nonexistent1.json",
                "source_name": "langtaosha"
            },
            {
                "input_path": fixtures_path / "langtaosha" / "article_184.json",
                "source_name": "langtaosha"
            },
            {
                "input_path": "nonexistent2.json",
                "source_name": "langtaosha"
            },
        ]

        # 继续处理（默认）
        results, stats = transformer.transform_batch(batch=batch, continue_on_error=True)
        assert stats.total == 3
        assert stats.successful == 1
        assert stats.failed == 2

        # 遇到错误停止
        results, stats = transformer.transform_batch(batch=batch, continue_on_error=False)
        assert stats.total == 3
        # 第一个失败后停止，所以只处理了 1 个
        assert stats.successful == 0
        assert stats.failed >= 1

    def test_transform_batch_missing_fields(self):
        """测试批量任务缺少必要字段"""
        transformer = MetadataTransformer()

        batch = [
            {
                "input_path": "some.json"
                # 缺少 source_name
            },
            {
                "source_name": "langtaosha"
                # 缺少 input_path
            },
        ]

        results, stats = transformer.transform_batch(batch=batch)

        assert stats.total == 2
        assert stats.failed == 2
        assert stats.successful == 0

        # 验证错误信息
        assert all("Missing required field" in r.error for r in results)

    def test_get_input_adapter(self):
        """测试自动选择 input_adapter"""
        transformer = MetadataTransformer()

        # 测试 JSON
        adapter = transformer._get_input_adapter("test.json")
        assert adapter.__class__.__name__ == "JSONInputAdapter"

        # 测试 JSONL
        adapter = transformer._get_input_adapter("test.jsonl")
        assert adapter.__class__.__name__ == "JSONLInputAdapter"

        # 测试不支持的格式
        with pytest.raises(TransformerError) as exc_info:
            transformer._get_input_adapter("test.txt")
        assert "Unsupported file format" in str(exc_info.value)

    def test_get_source_adapter(self):
        """测试自动选择 source_adapter"""
        transformer = MetadataTransformer()

        # 测试 langtaosha
        adapter = transformer._get_source_adapter("langtaosha")
        assert adapter.__class__.__name__ == "LangtaoshaSourceAdapter"

        # 测试 biorxiv_daily
        adapter = transformer._get_source_adapter("biorxiv_daily")
        assert adapter.__class__.__name__ == "BiorxivSourceAdapter"

        # 测试不支持的来源
        with pytest.raises(TransformerError) as exc_info:
            transformer._get_source_adapter("unsupported")
        assert "Unsupported source" in str(exc_info.value)

    def test_transform_result_to_dict(self):
        """测试 TransformResult.to_dict()"""
        result = TransformResult(
            success=True,
            input_path="test.json",
            source_name="langtaosha",
            db_payload={"test": "data"},
            upsert_key={"source_name": "langtaosha", "source_record_id": "1"},
            execution_time=0.5
        )

        result_dict = result.to_dict()
        assert result_dict["success"] is True
        assert result_dict["input_path"] == "test.json"
        assert result_dict["source_name"] == "langtaosha"
        assert result_dict["db_payload"] == {"test": "data"}
        assert result_dict["upsert_key"] == {"source_name": "langtaosha", "source_record_id": "1"}
        assert result_dict["execution_time"] == 0.5

    def test_transform_stats(self):
        """测试 TransformStats"""
        stats = TransformStats(total=10, successful=8, failed=2)

        assert stats.total == 10
        assert stats.successful == 8
        assert stats.failed == 2
        assert stats.success_rate == 80.0

        # 测试空统计
        stats = TransformStats()
        assert stats.total == 0
        assert stats.successful == 0
        assert stats.failed == 0
        assert stats.success_rate == 0.0

    def test_end_to_end_integration(self, fixtures_path):
        """测试端到端集成"""
        transformer = MetadataTransformer(
            parser_version="1.0.0",
            source_schema_version="2025-04-13",
            default_language="en"
        )

        # 完整转换流程
        result = transformer.transform_file(
            input_path=fixtures_path / "langtaosha" / "article_184.json",
            source_name="langtaosha"
        )

        # 验证所有步骤都成功执行
        assert result.success is True

        # 验证数据库 payload 完整性
        payload = result.db_payload
        assert payload["papers"]["canonical_title"] is not None
        assert payload["papers"]["canonical_abstract"] is not None
        assert payload["paper_sources"]["source_name"] == "langtaosha"
        assert payload["paper_sources"]["doi"] is not None
        assert payload["paper_source_metadata"]["raw_metadata_json"] is not None
        assert payload["paper_source_metadata"]["normalized_json"] is not None
        assert payload["paper_source_metadata"]["parser_version"] == "1.0.0"
        assert payload["paper_source_metadata"]["source_schema_version"] == "2025-04-13"
        assert len(payload["paper_keywords"]) > 0
        assert len(payload["paper_references"]) > 0


# Fixtures
@pytest.fixture
def fixtures_path():
    """获取测试数据路径"""
    # 支持两种可能的测试数据位置
    possible_paths = [
        Path(__file__).parent.parent.parent / "test_data",  # Langtaosha_PD/test_data
        Path(__file__).parent / "fixtures",  # tests/metadata/fixtures
        Path(__file__).parent.parent / "test_data",  # Langtaosha_PD/tests/test_data
    ]

    for path in possible_paths:
        if path.exists():
            # 检查是否包含测试文件
            if (path / "langtaosha" / "article_184.json").exists():
                return path
            elif (path / "article_184.json").exists():
                return path

    # 如果都找不到，返回第一个路径（测试会失败，但至少有明确的错误）
    return possible_paths[0]
