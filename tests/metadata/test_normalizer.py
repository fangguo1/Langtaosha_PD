"""测试归一化器"""
import pytest

from docset_hub.metadata.normalizer import MetadataNormalizer
from docset_hub.metadata.contracts import (
    NormalizedRecord,
    CoreMetadata,
    Identifiers,
    Author,
    Keyword,
)
from .fixtures import NORMALIZATION_SAMPLES


class TestMetadataNormalizer:
    """测试元数据归一化器"""

    def test_normalize_doi(self):
        """测试 DOI 归一化"""
        normalizer = MetadataNormalizer()

        result = normalizer._normalize_doi("HTTPS://doi.org/10.1145/XXX")

        assert result == "10.1145/xxx"

    def test_normalize_doi_with_spaces(self):
        """测试带空格的 DOI"""
        normalizer = MetadataNormalizer()

        result = normalizer._normalize_doi(" 10.1145/ABC ")

        assert result == "10.1145/abc"

    def test_normalize_doi_none(self):
        """测试 None DOI"""
        normalizer = MetadataNormalizer()

        result = normalizer._normalize_doi(None)

        assert result is None

    def test_normalize_arxiv_id(self):
        """测试 arXiv ID 归一化"""
        normalizer = MetadataNormalizer()

        # 新格式
        result = normalizer._normalize_arxiv_id("2301.12345v1")
        assert result == "2301.12345"

        # 旧格式
        result = normalizer._normalize_arxiv_id("cs/2301012v3")
        assert result == "cs/2301012"

        # 无版本号
        result = normalizer._normalize_arxiv_id("2301.12345")
        assert result == "2301.12345"

    def test_normalize_pubmed_id(self):
        """测试 PubMed ID 归一化"""
        normalizer = MetadataNormalizer()

        result = normalizer._normalize_pubmed_id(" 12345678 ")

        assert result == "12345678"

    def test_normalize_date(self):
        """测试日期归一化"""
        normalizer = MetadataNormalizer()

        # YYYY-MM-DD
        result = normalizer._normalize_date("2026-04-01")
        assert result == "2026-04-01"

        # YYYY/MM/DD
        result = normalizer._normalize_date("2026/04/01")
        assert result == "2026-04-01"

        # ISO datetime
        result = normalizer._normalize_date("2026-04-01T00:00:00")
        assert result == "2026-04-01"

        # None
        result = normalizer._normalize_date(None)
        assert result is None

    def test_normalize_language(self):
        """测试语言归一化"""
        normalizer = MetadataNormalizer(default_language="en")

        # 小写转换
        result = normalizer._normalize_language("EN")
        assert result == "en"

        # 映射
        result = normalizer._normalize_language("English")
        assert result == "en"

        result = normalizer._normalize_language("中文")
        assert result == "zh"

        # None（使用默认值）
        result = normalizer._normalize_language(None)
        assert result == "en"

    def test_normalize_authors(self):
        """测试作者归一化"""
        normalizer = MetadataNormalizer()

        authors = [
            Author(name="  Alice  Zhang  ", sequence=1, affiliations=["Tsinghua"]),
            Author(name="Bob", sequence=2, affiliations=[]),
        ]

        result = normalizer._normalize_authors(authors)

        assert len(result) == 2
        assert result[0].name == "Alice Zhang"
        assert result[0].sequence == 1
        assert result[0].affiliations == ["Tsinghua"]
        assert result[1].name == "Bob"

    def test_normalize_keywords(self):
        """测试关键词归一化"""
        normalizer = MetadataNormalizer()

        keywords = [
            Keyword(keyword_type="concept", keyword="  LLM  ", source="langtaosha"),
            Keyword(keyword_type=None, keyword="reasoning", source=None),
        ]

        result = normalizer._normalize_keywords(keywords, "langtaosha")

        assert len(result) == 2
        assert result[0].keyword == "LLM"
        assert result[1].keyword_type == "concept"
        assert result[1].source == "langtaosha"

    def test_full_normalize(self):
        """测试完整的归一化流程"""
        normalizer = MetadataNormalizer()

        record = NormalizedRecord(
            source_name="langtaosha",
            raw_metadata={"citation_title": "Test"},
            core=CoreMetadata(
                title="Test Paper",
                language="EN",
                submitted_at="2026/04/01",
            ),
            identifiers=Identifiers(
                doi="HTTPS://doi.org/10.1145/ABC",
            ),
            authors=[
                Author(name="  Alice  ", sequence=1),
            ],
            keywords=[
                Keyword(keyword_type=None, keyword="  LLM  ", source=None),
            ],
        )

        normalized = normalizer.normalize(record)

        # 验证 DOI
        assert normalized.identifiers.doi == "10.1145/abc"

        # 验证日期
        assert normalized.core.submitted_at == "2026-04-01"

        # 验证语言
        assert normalized.core.language == "en"

        # 验证作者
        assert normalized.authors[0].name == "Alice"

        # 验证关键词
        assert normalized.keywords[0].keyword == "LLM"
        assert normalized.keywords[0].keyword_type == "concept"
        assert normalized.keywords[0].source == "langtaosha"

        # 验证原始记录未被修改
        assert record.identifiers.doi == "HTTPS://doi.org/10.1145/ABC"
        assert record.core.language == "EN"
