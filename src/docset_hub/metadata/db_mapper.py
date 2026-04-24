"""数据库映射模块

将 NormalizedRecord 映射为数据库写入 payload，支持：
- papers: 统一作品主表
- paper_sources: 来源记录表
- paper_source_metadata: 来源元数据沉淀表
- paper_author_affiliation: 作者机构表
- paper_keywords: 关键词表
- paper_references: 参考文献表
"""
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime

from .contracts import NormalizedRecord


class DBMapperError(Exception):
    """数据库映射异常"""
    pass


@dataclass
class PapersPayload:
    """papers 表的写入 payload"""
    work_id: Optional[str]  # 预留字段；由 MetadataDB 在新建 paper 时分配
    canonical_title: str
    canonical_abstract: Optional[str] = None
    canonical_language: Optional[str] = None
    canonical_publisher: Optional[str] = None
    submitted_at: Optional[str] = None
    online_at: Optional[str] = None
    published_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "work_id": self.work_id,  # ← 新增
            "canonical_title": self.canonical_title,
            "canonical_abstract": self.canonical_abstract,
            "canonical_language": self.canonical_language,
            "canonical_publisher": self.canonical_publisher,
            "submitted_at": self.submitted_at,
            "online_at": self.online_at,
            "published_at": self.published_at,
        }


@dataclass
class PaperSourcesPayload:
    """paper_sources 表的写入 payload"""
    source_name: str
    platform: Optional[str] = None
    source_record_id: Optional[str] = None
    source_url: Optional[str] = None
    abstract_url: Optional[str] = None
    pdf_url: Optional[str] = None
    title: Optional[str] = None
    abstract: Optional[str] = None
    publisher: Optional[str] = None
    language: Optional[str] = None
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    pubmed_id: Optional[str] = None
    semantic_scholar_id: Optional[str] = None
    submitted_at: Optional[str] = None
    online_at: Optional[str] = None
    published_at: Optional[str] = None
    updated_at_source: Optional[str] = None
    version: Optional[str] = None                # 版本号
    is_preprint: Optional[bool] = None
    is_published: Optional[bool] = None
    is_primary_source: bool = True
    sync_status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "source_name": self.source_name,
            "platform": self.platform,
            "source_record_id": self.source_record_id,
            "source_url": self.source_url,
            "abstract_url": self.abstract_url,
            "pdf_url": self.pdf_url,
            "title": self.title,
            "abstract": self.abstract,
            "publisher": self.publisher,
            "language": self.language,
            "doi": self.doi,
            "arxiv_id": self.arxiv_id,
            "pubmed_id": self.pubmed_id,
            "semantic_scholar_id": self.semantic_scholar_id,
            "submitted_at": self.submitted_at,
            "online_at": self.online_at,
            "published_at": self.published_at,
            "updated_at_source": self.updated_at_source,
            "version": self.version,
            "is_preprint": self.is_preprint,
            "is_published": self.is_published,
            "is_primary_source": self.is_primary_source,
            "sync_status": self.sync_status,
        }


@dataclass
class PaperSourceMetadataPayload:
    """paper_source_metadata 表的写入 payload"""
    raw_metadata_json: Dict[str, Any]
    normalized_json: Dict[str, Any]
    parser_version: Optional[str] = None
    source_schema_version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "raw_metadata_json": self.raw_metadata_json,
            "normalized_json": self.normalized_json,
            "parser_version": self.parser_version,
            "source_schema_version": self.source_schema_version,
        }


@dataclass
class PaperAuthorAffiliationPayload:
    """paper_author_affiliation 表的写入 payload"""
    authors: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "authors": self.authors,
        }


@dataclass
class PaperKeywordsPayload:
    """paper_keywords 表的写入 payload（多条记录）"""
    keywords: List[Dict[str, Any]] = field(default_factory=list)

    def to_list(self) -> List[Dict[str, Any]]:
        """转换为列表格式，用于批量插入"""
        return self.keywords


@dataclass
class PaperReferencesPayload:
    """paper_references 表的写入 payload（多条记录）"""
    references: List[Dict[str, Any]] = field(default_factory=list)

    def to_list(self) -> List[Dict[str, Any]]:
        """转换为列表格式，用于批量插入"""
        return self.references


@dataclass
class DBPayload:
    """完整的数据库写入 payload

    包含所有需要写入数据库的表数据。
    """
    papers: PapersPayload
    paper_sources: PaperSourcesPayload
    paper_source_metadata: PaperSourceMetadataPayload
    paper_author_affiliation: PaperAuthorAffiliationPayload
    paper_keywords: PaperKeywordsPayload
    paper_references: PaperReferencesPayload

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "papers": self.papers.to_dict(),
            "paper_sources": self.paper_sources.to_dict(),
            "paper_source_metadata": self.paper_source_metadata.to_dict(),
            "paper_author_affiliation": self.paper_author_affiliation.to_dict(),
            "paper_keywords": self.paper_keywords.to_list(),
            "paper_references": self.paper_references.to_list(),
        }


class MetadataDBMapper:
    """元数据数据库映射器

    将 NormalizedRecord 映射为数据库写入 payload。

    映射策略：
    - 使用 (source_name, source_record_id) 作为主要 upsert key
    - 标识符回退策略：source_record_id -> doi -> arxiv_id -> pubmed_id
    - 保留原始元数据在 raw_metadata_json 中
    - 归一化后的数据存储在 normalized_json 中
    """

    def __init__(
        self,
        parser_version: str = "1.0.0",
        source_schema_version: Optional[str] = None,
    ):
        """初始化

        Args:
            parser_version: 解析器版本号
            source_schema_version: 来源 schema 版本号（可选）
        """
        self.parser_version = parser_version
        self.source_schema_version = source_schema_version

    def map_to_db_payload(self, record: NormalizedRecord) -> DBPayload:
        """将 NormalizedRecord 映射为数据库写入 payload

        Args:
            record: 归一化后的记录

        Returns:
            DBPayload: 数据库写入 payload

        Raises:
            DBMapperError: 如果映射失败
        """
        # 1. 映射 papers 表
        papers_payload = self._map_to_papers_payload(record)

        # 2. 映射 paper_sources 表
        paper_sources_payload = self._map_to_paper_sources_payload(record)

        # 3. 映射 paper_source_metadata 表
        paper_source_metadata_payload = self._map_to_paper_source_metadata_payload(record)

        # 4. 映射 paper_author_affiliation 表
        paper_author_affiliation_payload = self._map_to_paper_author_affiliation_payload(record)

        # 5. 映射 paper_keywords 表
        paper_keywords_payload = self._map_to_paper_keywords_payload(record)

        # 6. 映射 paper_references 表
        paper_references_payload = self._map_to_paper_references_payload(record)

        return DBPayload(
            papers=papers_payload,
            paper_sources=paper_sources_payload,
            paper_source_metadata=paper_source_metadata_payload,
            paper_author_affiliation=paper_author_affiliation_payload,
            paper_keywords=paper_keywords_payload,
            paper_references=paper_references_payload,
        )

    def _map_to_papers_payload(self, record: NormalizedRecord) -> PapersPayload:
        """映射到 papers 表"""
        return PapersPayload(
            # work_id 由 MetadataDB 在 INSERT_NEW_PAPER / UPSERT_NEW_PAPER 时生成
            work_id=None,
            canonical_title=record.core.title,
            canonical_abstract=record.core.abstract,
            canonical_language=record.core.language,
            canonical_publisher=record.core.publisher,
            submitted_at=self._parse_timestamp(record.core.submitted_at),
            online_at=self._parse_timestamp(record.core.online_at),
            published_at=self._parse_timestamp(record.core.published_at),
        )

    def _map_to_paper_sources_payload(self, record: NormalizedRecord) -> PaperSourcesPayload:
        """映射到 paper_sources 表"""
        return PaperSourcesPayload(
            source_name=record.source_name,
            platform=record.platform,
            source_record_id=record.source_record_id,
            source_url=record.source_url,
            abstract_url=record.abstract_url,
            pdf_url=record.pdf_url,
            title=record.core.title,
            abstract=record.core.abstract,
            publisher=record.core.publisher,
            language=record.core.language,
            doi=record.identifiers.doi,
            arxiv_id=record.identifiers.arxiv,
            pubmed_id=record.identifiers.pubmed,
            semantic_scholar_id=record.identifiers.semantic_scholar,
            submitted_at=self._parse_timestamp(record.core.submitted_at),
            online_at=self._parse_timestamp(record.core.online_at),
            published_at=self._parse_timestamp(record.core.published_at),
            updated_at_source=self._parse_timestamp(record.core.updated_at_source),
            version=record.version,
            is_preprint=record.core.is_preprint,
            is_published=record.core.is_published,
        )

    def _map_to_paper_source_metadata_payload(self, record: NormalizedRecord) -> PaperSourceMetadataPayload:
        """映射到 paper_source_metadata 表"""
        # 构建 normalized_json 结构
        normalized_json = self._build_normalized_json(record)

        return PaperSourceMetadataPayload(
            raw_metadata_json=record.raw_metadata,
            normalized_json=normalized_json,
            parser_version=self.parser_version,
            source_schema_version=self.source_schema_version,
        )

    def _build_normalized_json(self, record: NormalizedRecord) -> Dict[str, Any]:
        """构建 normalized_json 结构

        按照数据库 schema 的要求，构建包含 common_normalized 和 source_specific 的结构。
        """
        # 构建 common_normalized 部分
        common_normalized = {
            "title": record.core.title,
            "abstract": record.core.abstract,
            "language": record.core.language,
            "publisher": record.core.publisher,
            "authors": [
                {
                    "name": author.name,
                    "sequence": author.sequence,
                    "affiliations": author.affiliations,
                }
                for author in record.authors
            ],
            "keywords": [
                {
                    "keyword_type": keyword.keyword_type,
                    "keyword": keyword.keyword,
                    "source": keyword.source,
                    "weight": keyword.weight,
                }
                for keyword in record.keywords
            ],
            "categories": [],  # 暂时为空，后续可扩展
            "pub_info": {
                "submitted_at": record.core.submitted_at,
                "online_at": record.core.online_at,
                "published_at": record.core.published_at,
                "updated_at_source": record.core.updated_at_source,
                "is_preprint": record.core.is_preprint,
                "is_published": record.core.is_published,
            },
            "versions": [],  # 暂时为空，后续可扩展版本信息
            "citations": {
                "cited_by_count": None,  # 暂时为空，后续可扩展引用统计
                "update_time": None,
            },
            "fields": [],  # 暂时为空，后续可扩展领域信息
        }

        # 构建 source_specific 部分
        source_specific = {
            "platform": record.platform,
            "source_record_id": record.source_record_id,
            "source_url": record.source_url,
            "abstract_url": record.abstract_url,
            "pdf_url": record.pdf_url,
            "identifiers": {
                "doi": record.identifiers.doi,
                "arxiv": record.identifiers.arxiv,
                "pubmed": record.identifiers.pubmed,
                "semantic_scholar": record.identifiers.semantic_scholar,
                "langtaosha": record.identifiers.langtaosha,
                "biorxiv": record.identifiers.biorxiv,
            },
            "institutions": [
                {"name": inst.name}
                for inst in record.institutions
            ],
            "references": [
                {"reference_raw": ref.reference_raw}
                for ref in record.references
            ],
        }

        return {
            "common_normalized": common_normalized,
            "source_specific": source_specific,
        }

    def _map_to_paper_author_affiliation_payload(self, record: NormalizedRecord) -> PaperAuthorAffiliationPayload:
        """映射到 paper_author_affiliation 表"""
        authors_json = [
            {
                "name": author.name,
                "sequence": author.sequence,
                "affiliations": author.affiliations,
            }
            for author in record.authors
        ]

        return PaperAuthorAffiliationPayload(authors=authors_json)

    def _map_to_paper_keywords_payload(self, record: NormalizedRecord) -> PaperKeywordsPayload:
        """映射到 paper_keywords 表"""
        keywords_list = [
            {
                "keyword_type": keyword.keyword_type,
                "keyword": keyword.keyword,
                "weight": keyword.weight if keyword.weight is not None else 1.0,
                "source": keyword.source if keyword.source else record.source_name,
            }
            for keyword in record.keywords
        ]

        return PaperKeywordsPayload(keywords=keywords_list)

    def _map_to_paper_references_payload(self, record: NormalizedRecord) -> PaperReferencesPayload:
        """映射到 paper_references 表"""
        references_list = [
            {
                "reference_order": idx + 1,
                "reference_text": ref.reference_raw,
                "reference_raw_json": {"reference_raw": ref.reference_raw},
            }
            for idx, ref in enumerate(record.references)
        ]

        return PaperReferencesPayload(references=references_list)

    @staticmethod
    def _parse_timestamp(date_str: Optional[str]) -> Optional[str]:
        """解析日期字符串为 TIMESTAMP 格式

        Args:
            date_str: YYYY-MM-DD 格式的日期字符串

        Returns:
            TIMESTAMP 格式的字符串或 None
        """
        if not date_str:
            return None

        # 如果已经是 YYYY-MM-DD 格式，转换为 TIMESTAMP
        try:
            # 尝试解析为日期
            datetime.strptime(date_str, "%Y-%m-%d")
            # 返回完整的时间戳格式
            return f"{date_str} 00:00:00"
        except ValueError:
            # 如果无法解析，返回 None
            return None

    def get_upsert_key(self, record: NormalizedRecord) -> Dict[str, Any]:
        """获取 upsert 操作的全量 identity bundle

        返回包含所有可用标识符的完整 bundle，不再使用回退单键模式。

        Args:
            record: 归一化后的记录

        Returns:
            Dict: 包含全量 identity bundle 的字典，结构为：
                {
                    "source_name": str,
                    "doi": Optional[str],
                    "arxiv_id": Optional[str],
                    "pubmed_id": Optional[str],
                    "semantic_scholar_id": Optional[str],
                    "source_identifiers": {
                        "langtaosha": Optional[str],
                        "biorxiv": Optional[str],
                        ...
                    }
                }

        Raises:
            DBMapperError: 如果无法确定任何可用标识符
        """
        # 构建 source_identifiers 字典
        source_identifiers = {
            "langtaosha": record.identifiers.langtaosha,
            "biorxiv": record.identifiers.biorxiv,
            "arxiv": record.identifiers.arxiv,
            "pubmed": record.identifiers.pubmed,
            "semantic_scholar": record.identifiers.semantic_scholar,
        }

        # 校验：source_identifiers[source_name] 必填
        if source_identifiers.get(record.source_name) is None:
            # 如果在 identifiers 中没有找到，尝试使用 source_record_id
            if record.source_record_id:
                source_identifiers[record.source_name] = record.source_record_id
            else:
                raise DBMapperError(
                    f"Cannot determine upsert key for record: {record.core.title}. "
                    f"source_identifiers['{record.source_name}'] is required "
                    f"(found in record.identifiers.{record.source_name} or record.source_record_id)"
                )

        # 构建完整的 identity bundle
        identity_bundle = {
            "source_name": record.source_name,
            "doi": record.identifiers.doi,
            "arxiv_id": record.identifiers.arxiv,
            "pubmed_id": record.identifiers.pubmed,
            "semantic_scholar_id": record.identifiers.semantic_scholar,
            "source_identifiers": source_identifiers,
        }

        return identity_bundle
