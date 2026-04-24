"""
metadata 流水线包

流水线顺序：
    input_adapters -> router -> source_adapters -> normalizer -> db_mapper

子模块：
    input_adapters  - 按输入介质解析原始 payload（JSON / HTML / API dump）
    source_adapters - 按来源语义映射字段到统一中间契约
    contracts       - NormalizedRecord 定义与校验
    router          - 来源识别与 source_adapter 分发
    normalizer      - 统一值格式清洗（ID / 日期 / 语言 / 作者 / 关键词）
    db_mapper       - 映射到数据库写入 payload（papers / paper_sources / metadata）
    transformer     - 完整流水线封装（一键式转换）
"""

from .contracts import (
    NormalizedRecord,
    CoreMetadata,
    Identifiers,
    Author,
    Institution,
    Keyword,
    Reference,
    ValidationError,
)
from .router import MetadataRouter, RouteResult, RoutingError
from .normalizer import MetadataNormalizer, NormalizerError
from .db_mapper import (
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
from .transformer import (
    MetadataTransformer,
    TransformResult,
    TransformStats,
    TransformerError,
)

__all__ = [
    # Contracts
    "NormalizedRecord",
    "CoreMetadata",
    "Identifiers",
    "Author",
    "Institution",
    "Keyword",
    "Reference",
    "ValidationError",
    # Router
    "MetadataRouter",
    "RouteResult",
    "RoutingError",
    # Normalizer
    "MetadataNormalizer",
    "NormalizerError",
    # DB Mapper
    "MetadataDBMapper",
    "DBPayload",
    "PapersPayload",
    "PaperSourcesPayload",
    "PaperSourceMetadataPayload",
    "PaperAuthorAffiliationPayload",
    "PaperKeywordsPayload",
    "PaperReferencesPayload",
    "DBMapperError",
    # Transformer
    "MetadataTransformer",
    "TransformResult",
    "TransformStats",
    "TransformerError",
]
