"""测试 Source Adapters"""
import pytest

from docset_hub.metadata.source_adapters import LangtaoshaSourceAdapter, BiorxivSourceAdapter
from docset_hub.metadata.contracts import ValidationError
from .fixtures import LANGTAOSHA_SAMPLE, LANGTAOSHA_JSONL_SAMPLE, BIORXIV_SAMPLE, INCOMPLETE_SAMPLES


class TestLangtaoshaSourceAdapter:
    """测试 Langtaosha Source Adapter"""

    def test_transform_success(self):
        """测试成功转换"""
        adapter = LangtaoshaSourceAdapter()

        record = adapter.transform(LANGTAOSHA_SAMPLE)

        # 验证基本信息
        assert record.source_name == "langtaosha"
        assert record.platform == "langtaosha"
        assert record.source_record_id == "181"

        # 验证核心元数据
        assert record.core.title == LANGTAOSHA_SAMPLE["citation_title"]
        assert record.core.abstract == LANGTAOSHA_SAMPLE["citation_abstract"]
        assert record.core.language == LANGTAOSHA_SAMPLE["citation_language"]
        assert record.core.publisher == LANGTAOSHA_SAMPLE["citation_publisher"]
        assert record.core.submitted_at == LANGTAOSHA_SAMPLE["citation_date"]
        assert record.core.online_at == LANGTAOSHA_SAMPLE["citation_online_date"]
        assert record.core.published_at == LANGTAOSHA_SAMPLE["citation_publication_date"]

        # 验证标识符
        assert record.identifiers.doi == LANGTAOSHA_SAMPLE["citation_doi"]
        assert record.identifiers.langtaosha == "181"

        # 验证 URL
        assert record.source_url == LANGTAOSHA_SAMPLE["citation_abstract_html_url"]
        assert record.abstract_url == LANGTAOSHA_SAMPLE["citation_abstract_html_url"]
        assert record.pdf_url == LANGTAOSHA_SAMPLE["citation_pdf_url"]

        # 验证作者
        assert len(record.authors) == 3
        assert record.authors[0].name == "Alice Zhang"
        assert record.authors[0].sequence == 1
        assert record.authors[1].name == "Bob Li"
        assert record.authors[1].sequence == 2

        # 验证机构
        assert len(record.institutions) == 3
        assert record.institutions[0].name == "Tsinghua University"

        # 验证关键词
        assert len(record.keywords) == 3
        assert record.keywords[0].keyword == "LLM"
        assert record.keywords[0].keyword_type == "concept"
        assert record.keywords[0].source == "langtaosha"

        # 验证引用
        assert len(record.references) == 2
        assert record.references[0].reference_raw == "Ref A: Previous work on LLM reasoning"

        # 验证原始元数据被保留
        assert record.raw_metadata == LANGTAOSHA_SAMPLE

    def test_transform_missing_title(self):
        """测试缺少标题时抛出异常"""
        adapter = LangtaoshaSourceAdapter()

        with pytest.raises(ValueError, match="citation_title is required"):
            adapter.transform(INCOMPLETE_SAMPLES["missing_title"])

    def test_transform_empty_metadata(self):
        """测试空元数据"""
        adapter = LangtaoshaSourceAdapter()

        with pytest.raises(ValueError, match="raw_metadata is empty"):
            adapter.transform(INCOMPLETE_SAMPLES["empty_metadata"])

    def test_extract_source_record_id_from_url(self):
        """测试从 URL 提取记录 ID"""
        adapter = LangtaoshaSourceAdapter()

        # 测试从 abstract URL 提取
        record_id = adapter.extract_source_record_id({
            "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/181",
        })
        assert record_id == "181"

        # 测试从 PDF URL 提取
        record_id = adapter.extract_source_record_id({
            "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/181",
        })
        assert record_id == "181"

        # 测试没有 URL 的情况
        record_id = adapter.extract_source_record_id({})
        assert record_id is None


class TestLangtaoshaSourceAdapterJSONL:
    """测试 Langtaosha Source Adapter (JSONL 格式)"""

    def test_transform_jsonl_format(self):
        """测试成功转换 JSONL 格式数据"""
        adapter = LangtaoshaSourceAdapter()

        record = adapter.transform(LANGTAOSHA_JSONL_SAMPLE)

        # 验证基本信息
        assert record.source_name == "langtaosha"
        assert record.platform == "langtaosha"
        assert record.source_record_id == "181"

        # 验证核心元数据（从 meta 数组中提取）
        assert record.core.title == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_title"][0]
        assert record.core.abstract == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_abstract"][0]
        assert record.core.language == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_language"][0]
        assert record.core.publisher == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_publisher"][0]
        assert record.core.submitted_at == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_date"][0]
        assert record.core.online_at == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_online_date"][0]
        assert record.core.published_at == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_publication_date"][0]

        # 验证标识符
        assert record.identifiers.doi == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_doi"][0]
        assert record.identifiers.langtaosha == "181"

        # 验证 URL
        assert record.source_url == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_abstract_html_url"][0]
        assert record.abstract_url == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_abstract_html_url"][0]
        assert record.pdf_url == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_pdf_url"][0]

        # 验证作者
        assert len(record.authors) == 3
        assert record.authors[0].name == "Alice Zhang"
        assert record.authors[0].sequence == 1
        assert record.authors[1].name == "Bob Li"
        assert record.authors[1].sequence == 2

        # 验证机构
        assert len(record.institutions) == 3
        assert record.institutions[0].name == "Tsinghua University"

        # 验证关键词
        assert len(record.keywords) == 3
        assert record.keywords[0].keyword == "LLM"
        assert record.keywords[0].keyword_type == "concept"
        assert record.keywords[0].source == "langtaosha"

        # 验证引用
        assert len(record.references) == 2
        assert record.references[0].reference_raw == "Ref A: Previous work on LLM reasoning"

        # 验证原始元数据被保留
        assert record.raw_metadata == LANGTAOSHA_JSONL_SAMPLE

    def test_extract_meta_field_helper(self):
        """测试 _extract_meta_field 辅助方法"""
        adapter = LangtaoshaSourceAdapter()

        # 测试从 meta 字典中提取（JSONL 格式）
        value = adapter._extract_meta_field(LANGTAOSHA_JSONL_SAMPLE, "citation_title")
        assert value == LANGTAOSHA_JSONL_SAMPLE["meta"]["citation_title"][0]

        # 测试从扁平格式中提取
        value = adapter._extract_meta_field(LANGTAOSHA_SAMPLE, "citation_title")
        assert value == LANGTAOSHA_SAMPLE["citation_title"]

        # 测试不存在的字段
        value = adapter._extract_meta_field(LANGTAOSHA_JSONL_SAMPLE, "nonexistent_field")
        assert value is None

    def test_transform_with_real_json_file(self):
        """测试使用真实的 JSON 文件进行转换"""
        from docset_hub.metadata.input_adapters import JSONInputAdapter

        # 使用实际的测试数据文件（JSON 格式）
        input_adapter = JSONInputAdapter()
        source_adapter = LangtaoshaSourceAdapter()

        # 解析第一个测试文件
        raw_metadata = input_adapter.parse(
            "/home/wnlab/langtaosha/Langtaosha_PD/test_data/langtaosha/article_184.json"
        )

        # 转换为 NormalizedRecord
        record = source_adapter.transform(raw_metadata)

        # 验证基本信息
        assert record.source_name == "langtaosha"
        assert record.source_record_id == "184"
        assert record.core.title is not None
        assert len(record.core.title) > 0

        # 验证作者信息存在
        assert len(record.authors) > 0

        # 验证机构信息存在
        assert len(record.institutions) > 0

        # 验证 DOI 格式
        if record.identifiers.doi:
            assert "10." in record.identifiers.doi

    def test_transform_with_real_jsonl_file(self):
        """测试使用真实的 JSONL 文件进行转换"""
        from docset_hub.metadata.input_adapters import JSONLInputAdapter

        # 使用实际的 JSONL 测试数据文件
        input_adapter = JSONLInputAdapter()
        source_adapter = LangtaoshaSourceAdapter()

        # 解析 JSONL 文件的第一行
        raw_metadata = input_adapter.parse(
            "/home/wnlab/langtaosha/Langtaosha_PD/local_data/langtaosha/raw/articles_raw.jsonl",
            line_number=0
        )

        # 转换为 NormalizedRecord
        record = source_adapter.transform(raw_metadata)

        # 验证基本信息
        assert record.source_name == "langtaosha"
        assert record.source_record_id == "184"
        assert record.core.title is not None
        assert len(record.core.title) > 0

        # 验证作者信息存在
        assert len(record.authors) > 0

        # 验证机构信息存在
        assert len(record.institutions) > 0

        # 验证 DOI 格式
        if record.identifiers.doi:
            assert "10." in record.identifiers.doi


class TestBiorxivSourceAdapter:
    """测试 bioRxiv Source Adapter"""

    def test_transform_success(self):
        """测试成功转换"""
        adapter = BiorxivSourceAdapter()

        record = adapter.transform(BIORXIV_SAMPLE)

        # 验证基本信息
        assert record.source_name == "biorxiv"
        assert record.platform == "bioRxiv"
        assert record.source_record_id == BIORXIV_SAMPLE["doi"]

        # 验证核心元数据
        assert record.core.title == BIORXIV_SAMPLE["title"]
        assert record.core.abstract == BIORXIV_SAMPLE["abstract"]
        assert record.core.publisher == "bioRxiv"
        assert record.core.online_at == BIORXIV_SAMPLE["date"]
        assert record.core.updated_at_source == BIORXIV_SAMPLE["date"]
        assert record.core.is_preprint is True
        assert record.core.is_published is False

        # 验证标识符
        assert record.identifiers.doi == BIORXIV_SAMPLE["doi"]
        assert record.identifiers.biorxiv == BIORXIV_SAMPLE["doi"]

        # 验证 URL
        assert record.source_url == f"https://www.biorxiv.org/content/{BIORXIV_SAMPLE['doi']}"
        assert record.pdf_url == f"https://www.biorxiv.org/content/{BIORXIV_SAMPLE['doi']}.full.pdf"

        # 验证作者
        assert len(record.authors) == 10
        assert record.authors[0].name == "Zhang, J."
        assert record.authors[0].sequence == 1

        # 验证机构
        assert len(record.institutions) == 2
        assert "Peng Cheng Laboratory" in record.institutions[0].name

        # 验证关键词（从 category 转换）
        assert len(record.keywords) == 1
        assert record.keywords[0].keyword == "neuroscience"
        assert record.keywords[0].keyword_type == "category"
        assert record.keywords[0].source == "biorxiv"

        # 验证引用为空（bioRxiv 不提供）
        assert len(record.references) == 0

        # 验证原始元数据被保留
        assert record.raw_metadata == BIORXIV_SAMPLE

    def test_transform_missing_title(self):
        """测试缺少标题时抛出异常"""
        adapter = BiorxivSourceAdapter()

        with pytest.raises(ValueError, match="title is required"):
            adapter.transform({"abstract": "Test"})

    def test_transform_published_paper(self):
        """测试已发表的论文"""
        adapter = BiorxivSourceAdapter()

        metadata = BIORXIV_SAMPLE.copy()
        metadata["published"] = "10.1234/published.2026.04.08"

        record = adapter.transform(metadata)

        assert record.core.is_published is True
        assert record.core.is_preprint is False
