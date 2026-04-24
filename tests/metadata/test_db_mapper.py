"""测试 db_mapper 模块"""
import pytest
from docset_hub.metadata.db_mapper import (
    MetadataDBMapper,
    DBPayload,
    PapersPayload,
    PaperSourcesPayload,
    PaperSourceMetadataPayload,
    PaperAuthorAffiliationPayload,
    PaperKeywordsPayload,
    PaperReferencesPayload,
    DBMapperError,
)
from docset_hub.metadata.contracts import (
    NormalizedRecord,
    CoreMetadata,
    Identifiers,
    Author,
    Institution,
    Keyword,
    Reference,
)
from docset_hub.metadata.normalizer import MetadataNormalizer


class TestMetadataDBMapper:
    """测试 MetadataDBMapper"""

    def setup_method(self):
        """测试前准备"""
        self.mapper = MetadataDBMapper(
            parser_version="1.0.0",
            source_schema_version="2025-04-13",
        )

    def test_map_to_db_payload_langtaosha(self):
        """测试映射 Langtaosha 记录"""
        # 创建一个 Langtaosha 记录
        record = NormalizedRecord(
            source_name="langtaosha",
            platform="langtaosha",
            source_record_id="181",
            source_url="https://langtaosha.org.cn/lts/en/preprint/view/181",
            abstract_url="https://langtaosha.org.cn/lts/en/preprint/view/181",
            pdf_url="https://langtaosha.org.cn/lts/en/preprint/download/181",
            raw_metadata={
                "citation_title": "A Study on Large Language Model Reasoning",
                "citation_author": ["Alice Zhang", "Bob Li"],
            },
            core=CoreMetadata(
                title="A Study on Large Language Model Reasoning",
                abstract="This paper proposes a novel approach to improve reasoning capabilities.",
                language="en",
                publisher="Langtaosha",
                submitted_at="2026-04-01",
                online_at="2026-04-05",
                published_at="2026-04-10",
                is_preprint=True,
                is_published=False,
            ),
            identifiers=Identifiers(
                doi="10.1234/langtaosha.001",
                langtaosha="181",
            ),
            authors=[
                Author(name="Alice Zhang", sequence=1, affiliations=["Tsinghua University"]),
                Author(name="Bob Li", sequence=2, affiliations=["Peking University"]),
            ],
            institutions=[
                Institution(name="Tsinghua University"),
                Institution(name="Peking University"),
            ],
            keywords=[
                Keyword(keyword_type="concept", keyword="LLM", source="langtaosha"),
                Keyword(keyword_type="concept", keyword="reasoning", source="langtaosha"),
            ],
            references=[
                Reference(reference_raw="Ref A: Previous work on LLM reasoning"),
                Reference(reference_raw="Ref B: Recent advances in AI"),
            ],
        )

        # 映射为 DB payload
        payload = self.mapper.map_to_db_payload(record)

        # 验证类型
        assert isinstance(payload, DBPayload)

        # 验证 papers payload
        assert isinstance(payload.papers, PapersPayload)
        assert payload.papers.work_id is None
        assert payload.papers.canonical_title == "A Study on Large Language Model Reasoning"
        assert payload.papers.canonical_language == "en"
        assert payload.papers.canonical_publisher == "Langtaosha"

        # 验证 paper_sources payload
        assert isinstance(payload.paper_sources, PaperSourcesPayload)
        assert payload.paper_sources.source_name == "langtaosha"
        assert payload.paper_sources.source_record_id == "181"
        assert payload.paper_sources.doi == "10.1234/langtaosha.001"

        # 验证 paper_source_metadata payload
        assert isinstance(payload.paper_source_metadata, PaperSourceMetadataPayload)
        assert payload.paper_source_metadata.raw_metadata_json == record.raw_metadata
        assert "common_normalized" in payload.paper_source_metadata.normalized_json
        assert "source_specific" in payload.paper_source_metadata.normalized_json

        # 验证 paper_author_affiliation payload
        assert isinstance(payload.paper_author_affiliation, PaperAuthorAffiliationPayload)
        assert len(payload.paper_author_affiliation.authors) == 2
        assert payload.paper_author_affiliation.authors[0]["name"] == "Alice Zhang"
        assert payload.paper_author_affiliation.authors[0]["sequence"] == 1

        # 验证 paper_keywords payload
        assert isinstance(payload.paper_keywords, PaperKeywordsPayload)
        assert len(payload.paper_keywords.to_list()) == 2
        keywords_list = payload.paper_keywords.to_list()
        assert keywords_list[0]["keyword"] == "LLM"
        assert keywords_list[0]["source"] == "langtaosha"

        # 验证 paper_references payload
        assert isinstance(payload.paper_references, PaperReferencesPayload)
        assert len(payload.paper_references.to_list()) == 2
        references_list = payload.paper_references.to_list()
        assert references_list[0]["reference_order"] == 1
        assert "Ref A" in references_list[0]["reference_text"]

    def test_map_to_db_payload_biorxiv(self):
        """测试映射 bioRxiv 记录"""
        record = NormalizedRecord(
            source_name="biorxiv",
            platform="bioRxiv",
            source_record_id="10.1101/2021.11.22.469359",
            raw_metadata={
                "title": "Neural computations in the foveal and peripheral visual fields",
                "authors": "Zhang, J.; Zhu, X.; Ma, Z.",
                "category": "neuroscience",
            },
            core=CoreMetadata(
                title="Neural computations in the foveal and peripheral visual fields",
                abstract="Active vision requires coordinated attentional processing ...",
                publisher="bioRxiv",
                online_at="2026-04-08",
                is_preprint=True,
            ),
            identifiers=Identifiers(
                doi="10.1101/2021.11.22.469359",
                biorxiv="10.1101/2021.11.22.469359",
            ),
            authors=[
                Author(name="Zhang, J.", sequence=1, affiliations=[]),
                Author(name="Zhu, X.", sequence=2, affiliations=[]),
                Author(name="Ma, Z.", sequence=3, affiliations=[]),
            ],
            keywords=[
                Keyword(keyword_type="domain", keyword="neuroscience", source="biorxiv"),
            ],
        )

        # 映射为 DB payload
        payload = self.mapper.map_to_db_payload(record)

        # 验证
        assert payload.papers.work_id is None
        assert payload.papers.canonical_title == "Neural computations in the foveal and peripheral visual fields"
        assert payload.paper_sources.source_name == "biorxiv"
        assert payload.paper_sources.doi == "10.1101/2021.11.22.469359"
        assert len(payload.paper_author_affiliation.authors) == 3
        assert len(payload.paper_keywords.to_list()) == 1

    def test_get_upsert_key_with_source_record_id(self):
        """测试获取 identity bundle（使用 source_record_id）"""
        record = NormalizedRecord(
            source_name="langtaosha",
            source_record_id="181",
            raw_metadata={},
            core=CoreMetadata(title="Test Paper"),
        )

        upsert_key = self.mapper.get_upsert_key(record)
        assert upsert_key["source_name"] == "langtaosha"
        assert upsert_key["source_identifiers"]["langtaosha"] == "181"
        assert "source_record_id" not in upsert_key  # 不再有顶层 source_record_id

    def test_get_upsert_key_with_doi(self):
        """测试获取 identity bundle（包含 DOI）"""
        record = NormalizedRecord(
            source_name="biorxiv",
            source_record_id="10.1101/2021.11.22.469359",
            raw_metadata={},
            core=CoreMetadata(title="Test Paper"),
            identifiers=Identifiers(doi="10.1101/2021.11.22.469359"),
        )

        upsert_key = self.mapper.get_upsert_key(record)
        assert upsert_key["source_name"] == "biorxiv"
        assert upsert_key["doi"] == "10.1101/2021.11.22.469359"
        assert upsert_key["source_identifiers"]["biorxiv"] == "10.1101/2021.11.22.469359"

    def test_get_upsert_key_with_multiple_identifiers(self):
        """测试获取 identity bundle（包含多个标识符）"""
        record = NormalizedRecord(
            source_name="langtaosha",
            source_record_id="181",
            raw_metadata={},
            core=CoreMetadata(title="Test Paper"),
            identifiers=Identifiers(
                doi="10.1234/test.001",
                arxiv="2301.12345",
                pubmed="12345678"
            ),
        )

        upsert_key = self.mapper.get_upsert_key(record)
        assert upsert_key["source_name"] == "langtaosha"
        assert upsert_key["doi"] == "10.1234/test.001"
        assert upsert_key["arxiv_id"] == "2301.12345"
        assert upsert_key["pubmed_id"] == "12345678"
        assert upsert_key["source_identifiers"]["langtaosha"] == "181"
        assert upsert_key["source_identifiers"]["arxiv"] == "2301.12345"
        assert upsert_key["source_identifiers"]["pubmed"] == "12345678"

    def test_get_upsert_key_failure(self):
        """测试获取 identity bundle 失败（没有任何标识符）"""
        record = NormalizedRecord(
            source_name="unknown",
            raw_metadata={},
            core=CoreMetadata(title="Test Paper"),
        )

        with pytest.raises(DBMapperError) as exc_info:
            self.mapper.get_upsert_key(record)

        assert "source_identifiers" in str(exc_info.value)

    def test_normalized_json_structure(self):
        """测试 normalized_json 的结构是否符合数据库 schema"""
        record = NormalizedRecord(
            source_name="langtaosha",
            source_record_id="181",
            raw_metadata={"citation_title": "Test Paper"},
            core=CoreMetadata(
                title="Test Paper",
                abstract="Test Abstract",
                language="en",
            ),
            authors=[Author(name="Alice Zhang", sequence=1, affiliations=[])],
            keywords=[Keyword(keyword_type="concept", keyword="LLM", source="langtaosha")],
        )

        payload = self.mapper.map_to_db_payload(record)
        normalized_json = payload.paper_source_metadata.normalized_json

        # 验证顶层结构
        assert "common_normalized" in normalized_json
        assert "source_specific" in normalized_json

        common = normalized_json["common_normalized"]
        source_specific = normalized_json["source_specific"]

        # 验证 common_normalized 包含的字段
        assert "title" in common
        assert "abstract" in common
        assert "authors" in common
        assert "keywords" in common
        assert "pub_info" in common
        assert "versions" in common
        assert "citations" in common
        assert "fields" in common

        # 验证 source_specific 包含的字段
        assert "platform" in source_specific
        assert "source_record_id" in source_specific
        assert "identifiers" in source_specific
        assert "institutions" in source_specific
        assert "references" in source_specific

    def test_to_dict_conversion(self):
        """测试转换为字典格式"""
        record = NormalizedRecord(
            source_name="langtaosha",
            source_record_id="181",
            raw_metadata={},
            core=CoreMetadata(title="Test Paper"),
        )

        payload = self.mapper.map_to_db_payload(record)
        payload_dict = payload.to_dict()

        # 验证字典结构
        assert "papers" in payload_dict
        assert "paper_sources" in payload_dict
        assert "paper_source_metadata" in payload_dict
        assert "paper_author_affiliation" in payload_dict
        assert "paper_keywords" in payload_dict
        assert "paper_references" in payload_dict

        # 验证 papers 子字典
        assert isinstance(payload_dict["papers"], dict)
        assert payload_dict["papers"]["canonical_title"] == "Test Paper"

        # 验证 paper_keywords 是列表
        assert isinstance(payload_dict["paper_keywords"], list)

        # 验证 paper_references 是列表
        assert isinstance(payload_dict["paper_references"], list)

    def test_minimal_record(self):
        """测试最小记录的映射（只有必填字段）"""
        record = NormalizedRecord(
            source_name="test",
            raw_metadata={"title": "Minimal Paper"},
            core=CoreMetadata(title="Minimal Paper"),
        )

        payload = self.mapper.map_to_db_payload(record)

        # 验证基本字段
        assert payload.papers.canonical_title == "Minimal Paper"
        assert payload.paper_sources.source_name == "test"
        assert payload.paper_sources.title == "Minimal Paper"

        # 验证可选字段为空
        assert payload.papers.canonical_abstract is None
        assert payload.papers.canonical_language is None
        assert payload.paper_sources.version is None

        # 验证列表为空
        assert len(payload.paper_author_affiliation.authors) == 0
        assert len(payload.paper_keywords.to_list()) == 0
        assert len(payload.paper_references.to_list()) == 0

    def test_record_with_version(self):
        """测试带版本号的记录映射"""
        record = NormalizedRecord(
            source_name="langtaosha",
            source_record_id="181",
            version="1.0.1",
            raw_metadata={"citation_title": "Test Paper"},
            core=CoreMetadata(title="Test Paper"),
        )

        payload = self.mapper.map_to_db_payload(record)

        # 验证 version 字段
        assert payload.paper_sources.version == "1.0.1"
        assert payload.paper_sources.source_name == "langtaosha"


class TestMetadataDBMapperIntegration:
    """集成测试：与 normalizer 配合使用"""

    def test_full_pipeline_with_normalizer(self):
        """测试完整的流水线：record -> normalizer -> db_mapper"""
        # 创建原始记录
        record = NormalizedRecord(
            source_name="langtaosha",
            source_record_id="181",
            raw_metadata={
                "citation_title": "  A Study on LLM Reasoning  ",
                "citation_doi": "HTTPS://doi.org/10.1234/LANGTAOSHA.001",
                "citation_date": "2026/04/01",
            },
            core=CoreMetadata(
                title="  A Study on LLM Reasoning  ",
                abstract=None,
                language="EN",
                submitted_at="2026/04/01",
            ),
            identifiers=Identifiers(
                doi="HTTPS://doi.org/10.1234/LANGTAOSHA.001",
            ),
            authors=[
                Author(name="  Alice Zhang  ", sequence=1, affiliations=[]),
            ],
            keywords=[
                Keyword(keyword_type="concept", keyword="  LLM  ", source=None),
            ],
        )

        # 归一化
        normalizer = MetadataNormalizer()
        normalized_record = normalizer.normalize(record)

        # 映射到数据库
        mapper = MetadataDBMapper()
        payload = mapper.map_to_db_payload(normalized_record)

        # 验证归一化效果
        assert normalized_record.identifiers.doi == "10.1234/langtaosha.001"
        assert normalized_record.core.language == "en"
        assert normalized_record.core.submitted_at == "2026-04-01"
        assert normalized_record.authors[0].name == "Alice Zhang"
        assert normalized_record.keywords[0].keyword == "LLM"
        assert normalized_record.keywords[0].source == "langtaosha"

        # 验证数据库 payload
        assert payload.papers.canonical_title == "A Study on LLM Reasoning"
        assert payload.paper_sources.doi == "10.1234/langtaosha.001"
        assert payload.paper_sources.language == "en"
