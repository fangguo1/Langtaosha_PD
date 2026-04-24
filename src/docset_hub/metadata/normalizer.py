"""元数据值格式归一化

将 source_adapters 输出的 NormalizedRecord 中的值格式统一化。
"""
import re
from datetime import datetime
from typing import Optional, List

from .contracts import (
    NormalizedRecord,
    CoreMetadata,
    Identifiers,
    Author,
    Keyword,
)


class NormalizerError(Exception):
    """归一化处理异常"""
    pass


class MetadataNormalizer:
    """元数据归一化处理器

    统一值格式，包括：
    - DOI：去前缀、去空格、转小写
    - arXiv ID：去版本号
    - PubMed ID：提取数字
    - 日期：统一为 YYYY-MM-DD
    - 语言：统一为小写 ISO 代码
    - 作者：统一为标准格式
    - 关键词：转换为结构化对象
    """

    def __init__(self, default_language: str = "en"):
        """初始化

        Args:
            default_language: 默认语言代码
        """
        self.default_language = default_language

    def normalize(self, record: NormalizedRecord) -> NormalizedRecord:
        """归一化处理

        Args:
            record: 待归一化的记录

        Returns:
            归一化后的记录

        Raises:
            NormalizerError: 如果归一化失败
        """
        # 归一化标识符
        normalized_identifiers = self._normalize_identifiers(record.identifiers)

        # 归一化日期
        normalized_core = self._normalize_core_metadata(record.core)

        # 归一化作者
        normalized_authors = self._normalize_authors(record.authors)

        # 归一化关键词
        normalized_keywords = self._normalize_keywords(record.keywords, record.source_name)

        # 创建新的记录（避免修改原记录）
        normalized_record = NormalizedRecord(
            source_name=record.source_name,
            platform=record.platform,
            source_record_id=record.source_record_id,
            source_url=record.source_url,
            abstract_url=record.abstract_url,
            pdf_url=record.pdf_url,
            version=record.version,  # 保留 version 字段
            raw_metadata=record.raw_metadata,
            core=normalized_core,
            identifiers=normalized_identifiers,
            authors=normalized_authors,
            institutions=record.institutions,
            keywords=normalized_keywords,
            references=record.references,
        )

        return normalized_record

    def _normalize_identifiers(self, identifiers: Identifiers) -> Identifiers:
        """归一化标识符"""
        return Identifiers(
            doi=self._normalize_doi(identifiers.doi),
            arxiv=self._normalize_arxiv_id(identifiers.arxiv),
            pubmed=self._normalize_pubmed_id(identifiers.pubmed),
            semantic_scholar=self._normalize_semantic_scholar_id(identifiers.semantic_scholar),
            langtaosha=identifiers.langtaosha,  # langtaosha ID 不需要归一化
            biorxiv=self._normalize_doi(identifiers.biorxiv),  # bioRxiv 使用 DOI 格式
        )

    @staticmethod
    def _normalize_doi(doi: Optional[str]) -> Optional[str]:
        """归一化 DOI

        规则：
        - 去除 https://doi.org/ 等前缀
        - 转小写
        - 去除空格

        Args:
            doi: 原始 DOI 字符串

        Returns:
            归一化后的 DOI

        示例:
            'HTTPS://doi.org/10.1145/XXX' -> '10.1145/xxx'
            '10.1145/XXX' -> '10.1145/xxx'
        """
        if not doi:
            return None

        # 去除前后空格
        doi = doi.strip()
        if not doi:
            return None

        # 去除 DOI URL 前缀
        doi = re.sub(r'^https?://(dx\.)?doi\.org/', '', doi, flags=re.IGNORECASE)
        doi = re.sub(r'^doi\.org/', '', doi, flags=re.IGNORECASE)
        doi = re.sub(r'^doi:', '', doi, flags=re.IGNORECASE)

        # 转小写并去除所有空格
        doi = doi.lower().replace(' ', '')

        return doi if doi else None

    @staticmethod
    def _normalize_arxiv_id(arxiv_id: Optional[str]) -> Optional[str]:
        """归一化 arXiv ID

        规则：
        - 去掉版本号（如 v1, v2）
        - 去除前后空格

        Args:
            arxiv_id: 原始 arXiv ID 字符串

        Returns:
            归一化后的 base arXiv ID

        示例:
            '2301.12345v1' -> '2301.12345'
            'cs/2301012v3' -> 'cs/2301012'
        """
        if not arxiv_id:
            return None

        # 去除前后空格
        arxiv_id = arxiv_id.strip()
        if not arxiv_id:
            return None

        # 去掉版本号
        arxiv_id = re.sub(r'[vV]\d+$', '', arxiv_id)

        return arxiv_id if arxiv_id else None

    @staticmethod
    def _normalize_pubmed_id(pubmed_id: Optional[str]) -> Optional[str]:
        """归一化 PubMed ID

        规则：
        - 保留纯数字字符串
        - 去除前导空格和后导空格

        Args:
            pubmed_id: 原始 PubMed ID 字符串

        Returns:
            归一化后的 PubMed ID

        示例:
            ' 12345678 ' -> '12345678'
        """
        if not pubmed_id:
            return None

        # 去除前后空格
        pubmed_id = pubmed_id.strip()
        if not pubmed_id:
            return None

        # 确保是纯数字
        if not pubmed_id.isdigit():
            # 如果不是纯数字，尝试提取数字部分
            match = re.search(r'\d+', pubmed_id)
            if match:
                pubmed_id = match.group()
            else:
                return None

        return pubmed_id

    @staticmethod
    def _normalize_semantic_scholar_id(semantic_scholar_id: Optional[str]) -> Optional[str]:
        """归一化 Semantic Scholar ID

        规则：
        - 转小写
        - 去除前后空格

        Args:
            semantic_scholar_id: 原始 Semantic Scholar ID 字符串

        Returns:
            归一化后的 Semantic Scholar ID
        """
        if not semantic_scholar_id:
            return None

        # 去除前后空格并转小写
        semantic_scholar_id = semantic_scholar_id.strip().lower()

        return semantic_scholar_id if semantic_scholar_id else None

    def _normalize_core_metadata(self, core) -> CoreMetadata:
        """归一化核心元数据中的日期和语言"""
        return CoreMetadata(
            title=self._normalize_text(core.title),
            abstract=self._normalize_text(core.abstract),
            language=self._normalize_language(core.language),
            publisher=self._normalize_text(core.publisher),
            submitted_at=self._normalize_date(core.submitted_at),
            online_at=self._normalize_date(core.online_at),
            published_at=self._normalize_date(core.published_at),
            updated_at_source=self._normalize_date(core.updated_at_source),
            is_preprint=core.is_preprint,
            is_published=core.is_published,
        )

    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        """归一化日期

        规则：
        - 兼容 YYYY-MM-DD、YYYY/MM/DD、ISO datetime
        - 统一为 YYYY-MM-DD

        Args:
            date_str: 原始日期字符串

        Returns:
            归一化后的日期字符串（YYYY-MM-DD）
        """
        if not date_str:
            return None

        date_str = date_str.strip()
        if not date_str:
            return None

        # 尝试多种日期格式
        date_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y%m%d',
        ]

        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue

        # 如果无法解析，返回 None
        return None

    def _normalize_language(self, language: Optional[str]) -> Optional[str]:
        """归一化语言代码

        规则：
        - 统一为小写 ISO 代码（如 en, zh）

        Args:
            language: 原始语言代码

        Returns:
            归一化后的语言代码
        """
        if not language:
            return self.default_language

        language = language.strip().lower()

        # 简单的映射表
        language_mapping = {
            'english': 'en',
            'chinese': 'zh',
            '中文': 'zh',
            '英文': 'en',
        }

        return language_mapping.get(language, language)

    @staticmethod
    def _normalize_text(text: Optional[str]) -> Optional[str]:
        """归一化文本

        规则：
        - 去除前后空格
        - 将内部多个空格合并为一个

        Args:
            text: 原始文本

        Returns:
            归一化后的文本
        """
        if not text:
            return None

        # 去除前后空格，合并内部多个空格
        return ' '.join(text.split())

    def _normalize_authors(self, authors: List[Author]) -> List[Author]:
        """归一化作者列表

        规则：
        - 确保序号连续且从1开始
        - 清理作者姓名中的多余空格

        Args:
            authors: 原始作者列表

        Returns:
            归一化后的作者列表
        """
        if not authors:
            return []

        normalized = []
        for seq, author in enumerate(authors, start=1):
            # 清理姓名中的多余空格
            name = ' '.join(author.name.split())

            normalized.append(Author(
                name=name,
                sequence=seq,
                affiliations=[aff.strip() for aff in author.affiliations if aff.strip()],
            ))

        return normalized

    def _normalize_keywords(
        self,
        keywords: List[Keyword],
        source_name: str,
    ) -> List[Keyword]:
        """归一化关键词列表

        规则：
        - 确保有关键词类型
        - 清理关键词文本中的多余空格
        - 确保有来源标识

        Args:
            keywords: 原始关键词列表
            source_name: 来源名称

        Returns:
            归一化后的关键词列表
        """
        if not keywords:
            return []

        normalized = []
        for keyword in keywords:
            # 清理关键词文本
            keyword_text = ' '.join(keyword.keyword.split())

            if not keyword_text:
                continue

            normalized.append(Keyword(
                keyword_type=keyword.keyword_type or 'concept',
                keyword=keyword_text,
                source=keyword.source or source_name,
                weight=keyword.weight,
            ))

        return normalized
