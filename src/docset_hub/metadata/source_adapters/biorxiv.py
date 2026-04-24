"""bioRxiv Source Adapter

处理 bioRxiv API 返回的元数据字段。
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


class BiorxivSourceAdapter(BaseSourceAdapter):
    """bioRxiv 来源适配器

    解析 bioRxiv API 返回的元数据字段，映射到统一的 NormalizedRecord。
    """

    # bioRxiv 字段映射规则
    FIELD_MAPPING = {
        "title": "title",
        "abstract": "abstract",
        "date": "online_at",
        "doi": "doi",
        "authors": "authors",
        "category": "category",
        "jatsxml": "jatsxml",
        "version": "version",
        "license": "license",
        "type": "type",
        "funder": "funder",
        "published": "published",
    }

    def __init__(self, source_name: str = "biorxiv"):
        """初始化 bioRxiv 来源适配器

        Args:
            source_name: 来源名称（biorxiv, biorxiv_history, biorxiv_daily 等）
        """
        super().__init__(source_name=source_name)
        self.platform = "bioRxiv"

    def transform(self, raw_metadata: Dict[str, Any]) -> NormalizedRecord:
        """转换 bioRxiv 元数据到 NormalizedRecord

        Args:
            raw_metadata: bioRxiv API 返回的字段

        Returns:
            NormalizedRecord: 统一中间结构的记录

        Raises:
            ValueError: 如果缺少必填字段（如 title）
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

        # 提取引用（bioRxiv 通常不提供引用信息）
        references = self._extract_references(raw_metadata)

        # 提取 URL
        source_record_id = self.extract_source_record_id(raw_metadata)
        source_url = self.extract_source_url(raw_metadata)
        abstract_url = self.extract_abstract_url(raw_metadata)
        pdf_url = self.extract_pdf_url(raw_metadata)

        # 提取版本号（bioRxiv 通常有 version 字段）
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
        title = raw_metadata.get("title")
        if not title:
            raise ValueError("title is required for bioRxiv metadata")

        # 判断是否已发表
        published = raw_metadata.get("published", "NA")
        is_published = published != "NA"

        return CoreMetadata(
            title=title,
            abstract=raw_metadata.get("abstract"),
            language=None,  # bioRxiv 通常不提供语言信息
            publisher="bioRxiv",
            submitted_at=None,  # bioRxiv 不提供提交日期
            online_at=raw_metadata.get("date"),
            published_at=None,  # bioRxiv 的 'published' 字段是发表后的 DOI，不是日期
            updated_at_source=raw_metadata.get("date"),  # 使用 date 作为更新时间
            is_preprint=not is_published,  # bioRxiv 是预印本平台
            is_published=is_published,
        )

    def _extract_identifiers(self, raw_metadata: Dict[str, Any]) -> Identifiers:
        """提取标识符"""
        doi = raw_metadata.get("doi")

        # bioRxiv 的 DOI 格式：10.1101/2021.11.22.469359
        biorxiv_id = doi if doi else None

        return Identifiers(
            doi=doi,
            arxiv=None,
            pubmed=None,
            semantic_scholar=None,
            langtaosha=None,
            biorxiv=biorxiv_id,
        )

    def _extract_authors(self, raw_metadata: Dict[str, Any]) -> List[Author]:
        """提取作者列表"""
        authors_raw = raw_metadata.get("authors")

        if not authors_raw:
            return []

        # bioRxiv 的 authors 字段通常是分号分隔的字符串
        # 格式：Zhang, J.; Zhu, X.; Ma, Z.; Wang, S.
        if isinstance(authors_raw, str):
            # 按分号分隔
            author_names = [name.strip() for name in authors_raw.split(";")]
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
        """提取机构列表

        注意：bioRxiv 只提供 author_corresponding_institution，这是通讯作者的机构，
        不能盲目分配给所有作者。因此，这里只提取机构名称，但不与作者关联。
        """
        institution_raw = raw_metadata.get("author_corresponding_institution")

        if not institution_raw:
            return []

        # author_corresponding_institution 可能是字符串或列表
        if isinstance(institution_raw, str):
            # 可能包含 "&" 分隔多个机构
            if " & " in institution_raw:
                institution_names = [name.strip() for name in institution_raw.split(" & ")]
            elif ";" in institution_raw:
                institution_names = [name.strip() for name in institution_raw.split(";")]
            else:
                institution_names = [institution_raw.strip()]
        elif isinstance(institution_raw, list):
            institution_names = [str(name).strip() for name in institution_raw if name]
        else:
            institution_names = [str(institution_raw).strip()]

        # 构建 Institution 对象
        institutions = []
        for name in institution_names:
            if name:  # 过滤空字符串
                institutions.append(Institution(name=name))

        return institutions

    def _extract_keywords(self, raw_metadata: Dict[str, Any]) -> List[Keyword]:
        """提取关键词列表

        bioRxiv 的 category 字段表示论文的领域分类（如 neuroscience）。
        这里将其转换为关键词。
        """
        category = raw_metadata.get("category")

        if not category:
            return []

        # category 可能是字符串或列表
        if isinstance(category, str):
            categories = [category.strip()]
        elif isinstance(category, list):
            categories = [str(cat).strip() for cat in category if cat]
        else:
            categories = [str(category).strip()]

        # 构建 Keyword 对象
        keywords = []
        for cat in categories:
            if cat:  # 过滤空字符串
                keywords.append(Keyword(
                    keyword_type="category",
                    keyword=cat,
                    source="biorxiv",
                ))

        return keywords

    def _extract_references(self, raw_metadata: Dict[str, Any]) -> List[Reference]:
        """提取引用列表

        bioRxiv API 通常不提供引用信息，因此返回空列表。
        """
        return []

    def _extract_version(self, raw_metadata: Dict[str, Any]) -> Optional[str]:
        """提取版本号

        bioRxiv 通常提供版本信息（如 "1", "2", "3" 等）。
        """
        version = raw_metadata.get("version")
        if version:
            return str(version).strip()

        return None

    def extract_source_record_id(self, raw_metadata: Dict[str, Any]) -> str | None:
        """提取 bioRxiv 记录 ID

        使用 DOI 作为 source_record_id。
        """
        return raw_metadata.get("doi")

    def extract_source_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """提取来源 URL

        bioRxiv 的 jatsxml 字段是 XML 源文件地址，不是页面 URL。
        如果有 DOI，可以构造页面 URL。
        """
        doi = raw_metadata.get("doi")
        if doi:
            # bioRxiv DOI 格式：10.1101/2021.11.22.469359
            # 页面 URL 格式：https://www.biorxiv.org/content/10.1101/2021.11.22.469359
            return f"https://www.biorxiv.org/content/{doi}"

        return None

    def extract_abstract_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """提取摘要 URL

        bioRxiv 的摘要页面与来源页面相同。
        """
        return self.extract_source_url(raw_metadata)

    def extract_pdf_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """提取 PDF URL

        bioRxiv API 通常不直接提供 PDF URL。
        如果有 DOI，可以构造 PDF 下载 URL。
        """
        doi = raw_metadata.get("doi")
        if doi:
            # PDF 下载 URL 格式：https://www.biorxiv.org/content/10.1101/2021.11.22.469359.full.pdf
            return f"https://www.biorxiv.org/content/{doi}.full.pdf"

        return None
