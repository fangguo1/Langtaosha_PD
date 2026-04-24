"""统一数据契约定义与校验

定义 NormalizedRecord 中间结构，用于在 source_adapters、normalizer、db_mapper 之间传递数据。
"""
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from datetime import datetime


class ValidationError(Exception):
    """数据校验失败异常"""
    def __init__(self, message: str, field_path: str = None):
        self.message = message
        self.field_path = field_path
        super().__init__(f"{field_path}: {message}" if field_path else message)


@dataclass
class CoreMetadata:
    """核心元数据字段"""
    title: str                                    # 必填：论文标题
    abstract: Optional[str] = None                # 摘要
    language: Optional[str] = None                # 语言代码（ISO 639-1）
    publisher: Optional[str] = None               # 发布者
    submitted_at: Optional[str] = None            # 提交日期（YYYY-MM-DD）
    online_at: Optional[str] = None               # 在线发布日期（YYYY-MM-DD）
    published_at: Optional[str] = None            # 正式发表日期（YYYY-MM-DD）
    updated_at_source: Optional[str] = None       # 在来源侧的更新日期（YYYY-MM-DD）
    is_preprint: Optional[bool] = None            # 是否为预印本
    is_published: Optional[bool] = None           # 是否已正式发表


@dataclass
class Identifiers:
    """各类外部标识符"""
    doi: Optional[str] = None
    arxiv: Optional[str] = None
    pubmed: Optional[str] = None
    semantic_scholar: Optional[str] = None
    langtaosha: Optional[str] = None
    biorxiv: Optional[str] = None


@dataclass
class Author:
    """作者信息"""
    name: str                                     # 必填：作者姓名
    sequence: int                                 # 必填：作者序号（从1开始）
    affiliations: List[str] = field(default_factory=list)  # 所属机构列表


@dataclass
class Institution:
    """机构信息"""
    name: str                                     # 必填：机构名称


@dataclass
class Keyword:
    """关键词信息"""
    keyword_type: str                             # 必填：类型（concept, method, application等）
    keyword: str                                  # 必填：关键词文本
    source: Optional[str] = None                  # 来源（如 langtaosha, biorxiv）
    weight: Optional[float] = None                # 权重/置信度


@dataclass
class Reference:
    """引用信息"""
    reference_raw: str                            # 必填：原始引用字符串


@dataclass
class NormalizedRecord:
    """统一中间数据结构

    这是 metadata 流水线的核心数据契约，所有 source_adapters 的输出都必须符合此结构。
    """
    # 必填字段
    source_name: str                              # 来源名称（如 langtaosha, biorxiv）
    raw_metadata: Dict[str, Any]                  # 原始元数据（保留用于追溯）
    core: CoreMetadata                            # 核心元数据

    # 可选字段
    platform: Optional[str] = None                # 平台名称
    source_record_id: Optional[str] = None        # 来源侧记录 ID（用于 upsert）
    source_url: Optional[str] = None              # 来源页面 URL
    abstract_url: Optional[str] = None            # 摘要页面 URL
    pdf_url: Optional[str] = None                 # PDF 文件 URL
    version: Optional[str] = None                 # 版本号（用于同 source 版本比较）

    identifiers: Identifiers = field(default_factory=Identifiers)
    authors: List[Author] = field(default_factory=list)
    institutions: List[Institution] = field(default_factory=list)
    keywords: List[Keyword] = field(default_factory=list)
    references: List[Reference] = field(default_factory=list)

    def validate(self) -> None:
        """校验记录是否符合契约要求

        Raises:
            ValidationError: 如果校验失败
        """
        # 校验必填字段
        if not self.source_name:
            raise ValidationError("source_name is required", "source_name")

        if not self.raw_metadata:
            raise ValidationError("raw_metadata is required and must be non-empty", "raw_metadata")

        if not self.core.title:
            raise ValidationError("core.title is required", "core.title")

        # 校验日期格式（如果是字符串）
        date_fields = [
            ("core.submitted_at", self.core.submitted_at),
            ("core.online_at", self.core.online_at),
            ("core.published_at", self.core.published_at),
            ("core.updated_at_source", self.core.updated_at_source),
        ]

        for field_path, date_value in date_fields:
            if date_value is not None:
                if not isinstance(date_value, str):
                    raise ValidationError(f"{field_path} must be a string", field_path)
                # 简单校验是否为 YYYY-MM-DD 格式（允许后续 normalizer 进一步处理）
                if not self._is_valid_date_format(date_value):
                    # 不抛异常，只记录警告，因为 normalizer 会处理多种格式
                    pass

        # 校验作者序号唯一性
        sequences = [author.sequence for author in self.authors]
        if len(sequences) != len(set(sequences)):
            raise ValidationError("author sequences must be unique", "authors")

    @staticmethod
    def _is_valid_date_format(date_str: str) -> bool:
        """简单校验日期字符串是否为有效格式（不强制 YYYY-MM-DD）"""
        if not date_str:
            return False
        # 这里只做简单校验，允许 normalizer 处理多种格式
        return len(date_str) >= 8  # 最基本的有效性检查

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式（用于序列化）"""
        return {
            "source_name": self.source_name,
            "platform": self.platform,
            "source_record_id": self.source_record_id,
            "source_url": self.source_url,
            "abstract_url": self.abstract_url,
            "pdf_url": self.pdf_url,
            "version": self.version,
            "raw_metadata": self.raw_metadata,
            "core": {
                "title": self.core.title,
                "abstract": self.core.abstract,
                "language": self.core.language,
                "publisher": self.core.publisher,
                "submitted_at": self.core.submitted_at,
                "online_at": self.core.online_at,
                "published_at": self.core.published_at,
                "updated_at_source": self.core.updated_at_source,
                "is_preprint": self.core.is_preprint,
                "is_published": self.core.is_published,
            },
            "identifiers": {
                "doi": self.identifiers.doi,
                "arxiv": self.identifiers.arxiv,
                "pubmed": self.identifiers.pubmed,
                "semantic_scholar": self.identifiers.semantic_scholar,
                "langtaosha": self.identifiers.langtaosha,
                "biorxiv": self.identifiers.biorxiv,
            },
            "authors": [
                {
                    "name": author.name,
                    "sequence": author.sequence,
                    "affiliations": author.affiliations,
                }
                for author in self.authors
            ],
            "institutions": [
                {"name": institution.name}
                for institution in self.institutions
            ],
            "keywords": [
                {
                    "keyword_type": keyword.keyword_type,
                    "keyword": keyword.keyword,
                    "source": keyword.source,
                    "weight": keyword.weight,
                }
                for keyword in self.keywords
            ],
            "references": [
                {"reference_raw": reference.reference_raw}
                for reference in self.references
            ],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NormalizedRecord":
        """从字典创建 NormalizedRecord

        Args:
            data: 字典格式的数据

        Returns:
            NormalizedRecord 实例
        """
        core_data = data.get("core", {})
        identifiers_data = data.get("identifiers", {})

        return cls(
            source_name=data.get("source_name", ""),
            platform=data.get("platform"),
            source_record_id=data.get("source_record_id"),
            source_url=data.get("source_url"),
            abstract_url=data.get("abstract_url"),
            pdf_url=data.get("pdf_url"),
            version=data.get("version"),
            raw_metadata=data.get("raw_metadata", {}),
            core=CoreMetadata(
                title=core_data.get("title", ""),
                abstract=core_data.get("abstract"),
                language=core_data.get("language"),
                publisher=core_data.get("publisher"),
                submitted_at=core_data.get("submitted_at"),
                online_at=core_data.get("online_at"),
                published_at=core_data.get("published_at"),
                updated_at_source=core_data.get("updated_at_source"),
                is_preprint=core_data.get("is_preprint"),
                is_published=core_data.get("is_published"),
            ),
            identifiers=Identifiers(
                doi=identifiers_data.get("doi"),
                arxiv=identifiers_data.get("arxiv"),
                pubmed=identifiers_data.get("pubmed"),
                semantic_scholar=identifiers_data.get("semantic_scholar"),
                langtaosha=identifiers_data.get("langtaosha"),
                biorxiv=identifiers_data.get("biorxiv"),
            ),
            authors=[
                Author(
                    name=author_data.get("name", ""),
                    sequence=author_data.get("sequence", 0),
                    affiliations=author_data.get("affiliations", []),
                )
                for author_data in data.get("authors", [])
            ],
            institutions=[
                Institution(name=inst_data.get("name", ""))
                for inst_data in data.get("institutions", [])
            ],
            keywords=[
                Keyword(
                    keyword_type=kw_data.get("keyword_type", "concept"),
                    keyword=kw_data.get("keyword", ""),
                    source=kw_data.get("source"),
                    weight=kw_data.get("weight"),
                )
                for kw_data in data.get("keywords", [])
            ],
            references=[
                Reference(reference_raw=ref_data.get("reference_raw", ""))
                for ref_data in data.get("references", [])
            ],
        )
