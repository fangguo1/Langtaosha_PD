"""Langtaosha Source Adapter

处理 Langtaosha 页面的 citation_* 元数据字段。
"""
import re
from typing import Dict, Any, List, Optional

from .base import BaseSourceAdapter
from ..contracts import (
    NormalizedRecord,
    CoreMetadata,
    Identifiers,
    Author,
    Institution,
    Keyword,
    Reference,
)


class LangtaoshaSourceAdapter(BaseSourceAdapter):
    """Langtaosha 来源适配器

    解析 Langtaosha 页面的 citation_* 元数据字段，映射到统一的 NormalizedRecord。
    """

    # Langtaosha 字段映射规则
    FIELD_MAPPING = {
        "citation_title": "title",
        "citation_abstract": "abstract",
        "citation_language": "language",
        "citation_publisher": "publisher",
        "citation_date": "submitted_at",
        "citation_online_date": "online_at",
        "citation_publication_date": "published_at",
        "citation_doi": "doi",
        "citation_abstract_html_url": "abstract_url",
        "citation_pdf_url": "pdf_url",
        "citation_author": "authors",
        "citation_author_institution": "institutions",
        "citation_keywords": "keywords",
        "citation_reference": "references",
    }

    def __init__(self, source_name: str = "langtaosha"):
        """初始化 Langtaosha 来源适配器

        Args:
            source_name: 来源名称
        """
        super().__init__(source_name=source_name)
        self.platform = "langtaosha"

    def _extract_meta_field(self, raw_metadata: Dict[str, Any], field_name: str) -> Any:
        """提取 meta 字段，支持两种格式：

        1. JSONL 格式：meta 字典中的字段（值为数组）
        2. 扁平格式：直接的字段（值为字符串或数组）

        Args:
            raw_metadata: 原始元数据
            field_name: 字段名（如 "citation_title"）

        Returns:
            字段值（如果是数组且只有一个元素，返回该元素；否则返回原值）
        """
        # 先尝试从 meta 字典中获取（JSONL 格式）
        if "meta" in raw_metadata:
            meta = raw_metadata.get("meta", {})
            if field_name in meta:
                value = meta[field_name]
                # 如果是数组且只有一个元素，返回该元素
                if isinstance(value, list) and len(value) == 1:
                    return value[0]
                # 如果是数组且有多个元素，返回数组
                elif isinstance(value, list):
                    return value
                # 否则返回原值
                return value

        # 回退到直接从根级别获取（扁平格式）
        if field_name in raw_metadata:
            value = raw_metadata[field_name]
            # 如果是数组且只有一个元素，返回该元素
            if isinstance(value, list) and len(value) == 1:
                return value[0]
            return value

        return None

    def transform(self, raw_metadata: Dict[str, Any]) -> NormalizedRecord:
        """转换 Langtaosha 元数据到 NormalizedRecord

        Args:
            raw_metadata: Langtaosha 页面的 citation_* 字段

        Returns:
            NormalizedRecord: 统一中间结构的记录

        Raises:
            ValueError: 如果缺少必填字段（如 citation_title）
        """
        if not raw_metadata:
            raise ValueError("raw_metadata is empty")

        # 提取核心元数据
        core = self._extract_core_metadata(raw_metadata)

        # 提取标识符
        identifiers = self._extract_identifiers(raw_metadata)

        # 提取作者
        authors = self._extract_authors(raw_metadata)

        # 提取机构
        institutions = self._extract_institutions(raw_metadata)

        # 提取关键词
        keywords = self._extract_keywords(raw_metadata)

        # 提取引用
        references = self._extract_references(raw_metadata)

        # 提取 URL
        source_record_id = self.extract_source_record_id(raw_metadata)
        source_url = self.extract_source_url(raw_metadata)
        abstract_url = self.extract_abstract_url(raw_metadata)
        pdf_url = self.extract_pdf_url(raw_metadata)

        # 提取版本号（如果存在）
        version = self._extract_version(raw_metadata)

        # 构建 NormalizedRecord
        record = NormalizedRecord(
            source_name=self.source_name,
            platform=self.platform,
            source_record_id=source_record_id,
            source_url=source_url,
            abstract_url=abstract_url,
            pdf_url=pdf_url,
            version=version,
            raw_metadata=raw_metadata,
            core=core,
            identifiers=identifiers,
            authors=authors,
            institutions=institutions,
            keywords=keywords,
            references=references,
        )

        # 校验记录
        record.validate()

        return record

    def _extract_core_metadata(self, raw_metadata: Dict[str, Any]) -> CoreMetadata:
        """提取核心元数据"""
        title = self._extract_meta_field(raw_metadata, "citation_title")
        if not title:
            raise ValueError("citation_title is required for Langtaosha metadata")

        publisher = self._extract_meta_field(raw_metadata, "citation_publisher") or "Langtaosha"
        language = self._extract_meta_field(raw_metadata, "citation_language")
        abstract = self._extract_meta_field(raw_metadata, "citation_abstract")
        submitted_at = self._extract_meta_field(raw_metadata, "citation_date")
        online_at = self._extract_meta_field(raw_metadata, "citation_online_date")
        published_at = self._extract_meta_field(raw_metadata, "citation_publication_date")

        return CoreMetadata(
            title=title,
            abstract=abstract,
            language=language,
            publisher=publisher,
            submitted_at=submitted_at,
            online_at=online_at,
            published_at=published_at,
            updated_at_source=None,  # Langtaosha 没有这个字段
            is_preprint=True,         # Langtaosha 默认为预印本平台
            is_published=False,       # 默认未正式发表（可根据 citation_publication_date 判断）
        )

    def _extract_identifiers(self, raw_metadata: Dict[str, Any]) -> Identifiers:
        """提取标识符"""
        doi = self._extract_meta_field(raw_metadata, "citation_doi")
        langtaosha_id = self.extract_source_record_id(raw_metadata)

        return Identifiers(
            doi=doi,
            arxiv=None,
            pubmed=None,
            semantic_scholar=None,
            langtaosha=langtaosha_id,
            biorxiv=None,
        )

    def _extract_version(self, raw_metadata: Dict[str, Any]) -> Optional[str]:
        """提取版本号

        Langtaosha 可能提供版本信息，如果没有则返回 None。
        """
        # 尝试从常见的版本字段中提取
        version_fields = ["version", "citation_version", "article_version"]

        for field in version_fields:
            value = self._extract_meta_field(raw_metadata, field)
            if value:
                return str(value).strip()

        return None

    def _extract_authors(self, raw_metadata: Dict[str, Any]) -> List[Author]:
        """提取作者列表"""
        authors_raw = self._extract_meta_field(raw_metadata, "citation_author")

        if not authors_raw:
            return []

        # citation_author 可能是字符串、列表或分号分隔的字符串
        if isinstance(authors_raw, str):
            # 尝试按分号或逗号分隔
            if ";" in authors_raw:
                author_names = [name.strip() for name in authors_raw.split(";")]
            elif "," in authors_raw and ", " not in authors_raw:
                # 简单逗号分隔（不是 "姓, 名" 格式）
                author_names = [name.strip() for name in authors_raw.split(",")]
            else:
                # 单个作者或复杂格式
                author_names = [authors_raw.strip()]
        elif isinstance(authors_raw, list):
            author_names = [str(name).strip() for name in authors_raw if name]
        else:
            author_names = [str(authors_raw).strip()]

        # 构建 Author 对象
        authors = []
        for seq, name in enumerate(author_names, start=1):
            if name:  # 过滤空字符串
                authors.append(Author(name=name, sequence=seq))

        return authors

    def _extract_institutions(self, raw_metadata: Dict[str, Any]) -> List[Institution]:
        """提取机构列表"""
        institutions_raw = self._extract_meta_field(raw_metadata, "citation_author_institution")

        if not institutions_raw:
            return []

        # citation_author_institution 可能是字符串或列表
        if isinstance(institutions_raw, str):
            # 尝试按分号或逗号分隔
            if ";" in institutions_raw:
                institution_names = [name.strip() for name in institutions_raw.split(";")]
            elif "," in institutions_raw:
                institution_names = [name.strip() for name in institutions_raw.split(",")]
            else:
                institution_names = [institutions_raw.strip()]
        elif isinstance(institutions_raw, list):
            institution_names = [str(name).strip() for name in institutions_raw if name]
        else:
            institution_names = [str(institutions_raw).strip()]

        # 构建 Institution 对象
        institutions = []
        for name in institution_names:
            if name:  # 过滤空字符串
                institutions.append(Institution(name=name))

        return institutions

    def _extract_keywords(self, raw_metadata: Dict[str, Any]) -> List[Keyword]:
        """提取关键词列表"""
        keywords_raw = self._extract_meta_field(raw_metadata, "citation_keywords")

        if not keywords_raw:
            return []

        # citation_keywords 可能是字符串或列表
        if isinstance(keywords_raw, str):
            # 尝试按分号或逗号分隔
            if ";" in keywords_raw:
                keyword_list = [kw.strip() for kw in keywords_raw.split(";")]
            elif "," in keywords_raw:
                keyword_list = [kw.strip() for kw in keywords_raw.split(",")]
            else:
                keyword_list = [keywords_raw.strip()]
        elif isinstance(keywords_raw, list):
            keyword_list = [str(kw).strip() for kw in keywords_raw if kw]
        else:
            keyword_list = [str(keywords_raw).strip()]

        # 构建 Keyword 对象
        keywords = []
        for kw in keyword_list:
            if kw:  # 过滤空字符串
                keywords.append(Keyword(
                    keyword_type="concept",
                    keyword=kw,
                    source="langtaosha",
                ))

        return keywords

    def _extract_references(self, raw_metadata: Dict[str, Any]) -> List[Reference]:
        """提取引用列表"""
        references_raw = self._extract_meta_field(raw_metadata, "citation_reference")

        if not references_raw:
            return []

        # citation_reference 可能是字符串或列表
        if isinstance(references_raw, str):
            # 尝试按分号分隔
            if ";" in references_raw:
                reference_list = [ref.strip() for ref in references_raw.split(";")]
            elif "\n" in references_raw:
                reference_list = [ref.strip() for ref in references_raw.split("\n")]
            else:
                reference_list = [references_raw.strip()]
        elif isinstance(references_raw, list):
            reference_list = [str(ref).strip() for ref in references_raw if ref]
        else:
            reference_list = [str(references_raw).strip()]

        # 构建 Reference 对象
        references = []
        for ref in reference_list:
            if ref:  # 过滤空字符串
                references.append(Reference(reference_raw=ref))

        return references

    def extract_source_record_id(self, raw_metadata: Dict[str, Any]) -> str | None:
        """从 URL 中提取 Langtaosha 记录 ID

        从 citation_abstract_html_url 中提取 ID，如：
        https://langtaosha.org.cn/lts/en/preprint/view/181 -> 181
        """
        url = self._extract_meta_field(raw_metadata, "citation_abstract_html_url") or self._extract_meta_field(raw_metadata, "citation_pdf_url")

        if not url:
            return None

        # 尝试从 URL 中提取 ID（/view/{id} 或 /download/{id}）
        match = re.search(r"/(?:view|download)/(\d+)", url)
        if match:
            return match.group(1)

        return None

    def extract_source_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """提取来源 URL"""
        return self._extract_meta_field(raw_metadata, "citation_abstract_html_url")

    def extract_abstract_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """提取摘要 URL"""
        return self._extract_meta_field(raw_metadata, "citation_abstract_html_url")

    def extract_pdf_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """提取 PDF URL"""
        return self._extract_meta_field(raw_metadata, "citation_pdf_url")
