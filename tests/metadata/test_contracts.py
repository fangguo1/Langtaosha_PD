"""测试 NormalizedRecord 契约校验"""
import pytest

from docset_hub.metadata.contracts import (
    NormalizedRecord,
    CoreMetadata,
    Identifiers,
    Author,
    Institution,
    Keyword,
    Reference,
    ValidationError,
)


class TestNormalizedRecord:
    """测试 NormalizedRecord 基本功能"""

    def test_create_valid_record(self):
        """测试创建有效记录"""
        record = NormalizedRecord(
            source_name="langtaosha",
            raw_metadata={"citation_title": "Test"},
            core=CoreMetadata(title="Test Paper"),
        )

        assert record.source_name == "langtaosha"
        assert record.core.title == "Test Paper"
        assert record.raw_metadata == {"citation_title": "Test"}

    def test_validate_success(self):
        """测试校验成功"""
        record = NormalizedRecord(
            source_name="langtaosha",
            raw_metadata={"citation_title": "Test"},
            core=CoreMetadata(title="Test Paper"),
        )

        # 应该不抛出异常
        record.validate()

    def test_validate_missing_source_name(self):
        """测试缺少 source_name 时抛出异常"""
        record = NormalizedRecord(
            source_name="",
            raw_metadata={"citation_title": "Test"},
            core=CoreMetadata(title="Test Paper"),
        )

        with pytest.raises(ValidationError, match="source_name is required"):
            record.validate()

    def test_validate_missing_raw_metadata(self):
        """测试缺少 raw_metadata 时抛出异常"""
        record = NormalizedRecord(
            source_name="langtaosha",
            raw_metadata={},
            core=CoreMetadata(title="Test Paper"),
        )

        with pytest.raises(ValidationError, match="raw_metadata is required"):
            record.validate()

    def test_validate_missing_title(self):
        """测试缺少 title 时抛出异常"""
        record = NormalizedRecord(
            source_name="langtaosha",
            raw_metadata={"citation_abstract": "Test"},
            core=CoreMetadata(title=""),
        )

        with pytest.raises(ValidationError, match="core.title is required"):
            record.validate()

    def test_to_dict_and_from_dict(self):
        """测试序列化与反序列化"""
        original = NormalizedRecord(
            source_name="langtaosha",
            platform="langtaosha",
            source_record_id="181",
            source_url="https://langtaosha.org.cn/view/181",
            raw_metadata={"citation_title": "Test"},
            core=CoreMetadata(
                title="Test Paper",
                abstract="Test Abstract",
                language="en",
            ),
            identifiers=Identifiers(doi="10.1234/test"),
            authors=[Author(name="Alice", sequence=1)],
            institutions=[Institution(name="Tsinghua")],
            keywords=[Keyword(keyword_type="concept", keyword="LLM", source="langtaosha")],
            references=[Reference(reference_raw="Ref A")],
        )

        # 转换为字典
        data = original.to_dict()

        # 从字典重建
        restored = NormalizedRecord.from_dict(data)

        # 验证关键字段
        assert restored.source_name == original.source_name
        assert restored.core.title == original.core.title
        assert restored.core.abstract == original.core.abstract
        assert len(restored.authors) == 1
        assert restored.authors[0].name == "Alice"
        assert len(restored.keywords) == 1
        assert restored.keywords[0].keyword == "LLM"

    def test_author_sequence_uniqueness(self):
        """测试作者序号唯一性检查"""
        record = NormalizedRecord(
            source_name="langtaosha",
            raw_metadata={"citation_title": "Test"},
            core=CoreMetadata(title="Test Paper"),
            authors=[
                Author(name="Alice", sequence=1),
                Author(name="Bob", sequence=1),  # 重复序号
            ],
        )

        with pytest.raises(ValidationError, match="author sequences must be unique"):
            record.validate()

    def test_empty_collections(self):
        """测试空集合字段的默认值"""
        record = NormalizedRecord(
            source_name="langtaosha",
            raw_metadata={"citation_title": "Test"},
            core=CoreMetadata(title="Test Paper"),
        )

        assert len(record.authors) == 0
        assert len(record.institutions) == 0
        assert len(record.keywords) == 0
        assert len(record.references) == 0
