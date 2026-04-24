"""元数据转换器 - 完整流水线封装

提供一键式转换功能，将输入文件转换为数据库写入 payload。
封装完整的流水线流程：input_adapter -> router -> source_adapter -> normalizer -> db_mapper

使用示例：
    from docset_hub.metadata.transformer import MetadataTransformer

    # 基本使用
    transformer = MetadataTransformer()
    result = transformer.transform_file(
        input_path="/path/to/langtaosha.json",
        source_name="langtaosha"
    )

    # 批量处理
    results = transformer.transform_batch([
        {"input_path": "/path/to/paper1.json", "source_name": "langtaosha"},
        {"input_path": "/path/to/paper2.json", "source_name": "biorxiv"},
    ])

    # 自定义配置
    transformer = MetadataTransformer(
        parser_version="1.0.0",
        source_schema_version="2025-04-13",
        default_language="en"
    )
    result = transformer.transform_file(input_path, source_name)
"""

from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field

from .input_adapters import BaseInputAdapter, JSONInputAdapter, JSONLInputAdapter
from .router import MetadataRouter, RoutingError
from .source_adapters import (
    BaseSourceAdapter,
    LangtaoshaSourceAdapter,
    BiorxivSourceAdapter,
)
from .normalizer import MetadataNormalizer, NormalizerError
from .db_mapper import MetadataDBMapper, DBMapperError
from .contracts import NormalizedRecord


class TransformerError(Exception):
    """转换器异常"""
    pass


@dataclass
class TransformResult:
    """转换结果

    Attributes:
        success: 是否成功
        input_path: 输入文件路径
        source_name: 来源名称
        db_payload: 数据库 payload（成功时）
        upsert_key: upsert 键（成功时）
        work_id: 论文全局唯一标识符（由 MetadataDB 写入阶段分配，转换阶段可能为 None）
        error: 错误信息（失败时）
        execution_time: 执行时间（秒）
    """
    success: bool
    input_path: str
    source_name: str
    db_payload: Optional[Dict[str, Any]] = None
    upsert_key: Optional[Dict[str, str]] = None
    work_id: Optional[str] = None  # 由 MetadataDB 分配；transform 阶段可能为空
    error: Optional[str] = None
    execution_time: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "success": self.success,
            "input_path": self.input_path,
            "source_name": self.source_name,
            "db_payload": self.db_payload,
            "upsert_key": self.upsert_key,
            "work_id": self.work_id,  # ← 新增
            "error": self.error,
            "execution_time": self.execution_time,
        }


@dataclass
class TransformStats:
    """转换统计信息

    Attributes:
        total: 总数
        successful: 成功数
        failed: 失败数
        success_rate: 成功率
    """
    total: int = 0
    successful: int = 0
    failed: int = 0

    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total == 0:
            return 0.0
        return (self.successful / self.total) * 100

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "total": self.total,
            "successful": self.successful,
            "failed": self.failed,
            "success_rate": f"{self.success_rate:.2f}%",
        }


class MetadataTransformer:
    """元数据转换器

    封装完整的元数据处理流水线，提供一键式转换功能。

    流水线流程：
        1. input_adapter: 解析输入文件（JSON/JSONL等）
        2. router: 验证来源名称
        3. source_adapter: 字段映射到统一契约
        4. normalizer: 值格式归一化
        5. db_mapper: 映射到数据库 payload

    支持的输入格式：
        - JSON: .json 文件
        - JSONL: .jsonl 文件（每行一个 JSON 对象）

    支持的来源：
        - langtaosha: 龙淘沙预印本平台
        - biorxiv: bioRxiv 预印本平台
        - arxiv: arXiv 预印本平台（未来）
        - pubmed: PubMed 数据库（未来）
    """

    # Input adapter 映射（根据文件扩展名自动选择）
    INPUT_ADAPTERS = {
        ".json": JSONInputAdapter,
        ".jsonl": JSONLInputAdapter,
    }

    # Source adapter 映射（根据来源名称自动选择）
    SOURCE_ADAPTERS = {
        "langtaosha": LangtaoshaSourceAdapter,
        "biorxiv_history": BiorxivSourceAdapter,
        "biorxiv_daily": BiorxivSourceAdapter,
    }

    def __init__(
        self,
        parser_version: str = "1.0.0",
        source_schema_version: str = "2025-04-13",
        default_language: str = "en",
    ):
        """初始化转换器

        Args:
            parser_version: 解析器版本号
            source_schema_version: 来源 schema 版本
            default_language: 默认语言代码
        """
        self.parser_version = parser_version
        self.source_schema_version = source_schema_version
        self.default_language = default_language

        # 初始化各个模块
        self._router = MetadataRouter()
        self._normalizer = MetadataNormalizer(default_language=default_language)
        self._db_mapper = MetadataDBMapper(
            parser_version=parser_version,
            source_schema_version=source_schema_version
        )

    def transform_file(
        self,
        input_path: Union[str, Path],
        source_name: str,
        input_adapter: Optional[BaseInputAdapter] = None,
        source_adapter: Optional[BaseSourceAdapter] = None,
    ) -> TransformResult:
        """转换单个文件

        Args:
            input_path: 输入文件路径
            source_name: 来源名称（langtaosha, biorxiv 等）
            input_adapter: 可选的 input_adapter（如果不提供则自动选择）
            source_adapter: 可选的 source_adapter（如果不提供则自动选择）

        Returns:
            TransformResult: 转换结果

        Raises:
            TransformerError: 转换过程中的任何错误
        """
        import time
        start_time = time.time()

        input_path = str(input_path)

        try:
            # Step 1: Input Adapter - 解析输入文件
            if input_adapter is None:
                input_adapter = self._get_input_adapter(input_path)

            raw_payload = input_adapter.parse(input_path)

            # Step 2: Router - 验证来源名称
            route_result = self._router.route(raw_payload, source_name=source_name)

            # Step 3: Source Adapter - 字段映射
            if source_adapter is None:
                source_adapter = self._get_source_adapter(route_result.source_adapter)

            record = source_adapter.transform(raw_payload)

            # Step 4: Normalizer - 值格式归一化
            normalized_record = self._normalizer.normalize(record)

            # Step 5: DB Mapper - 映射到数据库 payload
            db_payload = self._db_mapper.map_to_db_payload(normalized_record)
            upsert_key = self._db_mapper.get_upsert_key(normalized_record)

            # 转换为字典格式
            payload_dict = {
                "papers": db_payload.papers.to_dict() if db_payload.papers else None,
                "paper_sources": db_payload.paper_sources.to_dict() if db_payload.paper_sources else None,
                "paper_source_metadata": db_payload.paper_source_metadata.to_dict() if db_payload.paper_source_metadata else None,
                "paper_author_affiliation": db_payload.paper_author_affiliation.to_dict() if db_payload.paper_author_affiliation else None,
                "paper_keywords": db_payload.paper_keywords.to_list() if db_payload.paper_keywords else [],
                "paper_references": db_payload.paper_references.to_list() if db_payload.paper_references else [],
            }

            execution_time = time.time() - start_time

            # 提取 work_id
            work_id = None
            if db_payload.papers:
                work_id = db_payload.papers.work_id

            return TransformResult(
                success=True,
                input_path=input_path,
                source_name=source_name,
                db_payload=payload_dict,
                upsert_key=upsert_key,
                work_id=work_id,  # ← 新增：全局唯一标识符
                execution_time=execution_time,
            )

        except (FileNotFoundError, ValueError, RoutingError, NormalizerError, DBMapperError) as e:
            execution_time = time.time() - start_time
            return TransformResult(
                success=False,
                input_path=input_path,
                source_name=source_name,
                error=str(e),
                execution_time=execution_time,
            )
        except Exception as e:
            execution_time = time.time() - start_time
            return TransformResult(
                success=False,
                input_path=input_path,
                source_name=source_name,
                error=f"Unexpected error: {str(e)}",
                execution_time=execution_time,
            )

    def transform_batch(
        self,
        batch: List[Dict[str, Any]],
        continue_on_error: bool = True,
    ) -> tuple[List[TransformResult], TransformStats]:
        """批量转换多个文件

        Args:
            batch: 批量任务列表，每个任务是一个字典：
                {"input_path": str, "source_name": str}
                或
                {"input_path": str, "source_name": str, "input_adapter": BaseInputAdapter, "source_adapter": BaseSourceAdapter}
            continue_on_error: 遇到错误是否继续处理

        Returns:
            (List[TransformResult], TransformStats): 转换结果列表和统计信息
        """
        results = []
        stats = TransformStats(total=len(batch))

        for task in batch:
            input_path = task.get("input_path")
            source_name = task.get("source_name")
            input_adapter = task.get("input_adapter")
            source_adapter = task.get("source_adapter")

            if not input_path or not source_name:
                results.append(TransformResult(
                    success=False,
                    input_path=input_path or "unknown",
                    source_name=source_name or "unknown",
                    error="Missing required field: input_path or source_name",
                ))
                stats.failed += 1
                continue

            result = self.transform_file(
                input_path=input_path,
                source_name=source_name,
                input_adapter=input_adapter,
                source_adapter=source_adapter,
            )

            results.append(result)

            if result.success:
                stats.successful += 1
            else:
                stats.failed += 1
                if not continue_on_error:
                    break

        return results, stats

    def transform_dict(
        self,
        raw_payload: Dict[str, Any],
        source_name: str,
        source_adapter: Optional[BaseSourceAdapter] = None,
    ) -> TransformResult:
        """转换原始字典（跳过 input_adapter）

        适用于已经解析好的字典数据，或从 API 直接获取的响应。

        Args:
            raw_payload: 原始元数据字典
            source_name: 来源名称
            source_adapter: 可选的 source_adapter（如果不提供则自动选择）

        Returns:
            TransformResult: 转换结果
        """
        import time
        start_time = time.time()

        try:
            # Step 1: Router - 验证来源名称
            route_result = self._router.route(raw_payload, source_name=source_name)

            # Step 2: Source Adapter - 字段映射
            if source_adapter is None:
                source_adapter = self._get_source_adapter(route_result.source_adapter)

            record = source_adapter.transform(raw_payload)

            # Step 3: Normalizer - 值格式归一化
            normalized_record = self._normalizer.normalize(record)

            # Step 4: DB Mapper - 映射到数据库 payload
            db_payload = self._db_mapper.map_to_db_payload(normalized_record)
            upsert_key = self._db_mapper.get_upsert_key(normalized_record)

            # 转换为字典格式
            payload_dict = {
                "papers": db_payload.papers.to_dict() if db_payload.papers else None,
                "paper_sources": db_payload.paper_sources.to_dict() if db_payload.paper_sources else None,
                "paper_source_metadata": db_payload.paper_source_metadata.to_dict() if db_payload.paper_source_metadata else None,
                "paper_author_affiliation": db_payload.paper_author_affiliation.to_dict() if db_payload.paper_author_affiliation else None,
                "paper_keywords": db_payload.paper_keywords.to_list() if db_payload.paper_keywords else [],
                "paper_references": db_payload.paper_references.to_list() if db_payload.paper_references else [],
            }

            execution_time = time.time() - start_time

            # 提取 work_id
            work_id = None
            if db_payload.papers:
                work_id = db_payload.papers.work_id

            return TransformResult(
                success=True,
                input_path="<dict>",
                source_name=source_name,
                db_payload=payload_dict,
                upsert_key=upsert_key,
                work_id=work_id,  # ← 新增：全局唯一标识符
                execution_time=execution_time,
            )

        except (RoutingError, NormalizerError, DBMapperError) as e:
            execution_time = time.time() - start_time
            return TransformResult(
                success=False,
                input_path="<dict>",
                source_name=source_name,
                error=str(e),
                execution_time=execution_time,
            )
        except Exception as e:
            execution_time = time.time() - start_time
            return TransformResult(
                success=False,
                input_path="<dict>",
                source_name=source_name,
                error=f"Unexpected error: {str(e)}",
                execution_time=execution_time,
            )

    def _get_input_adapter(self, input_path: str) -> BaseInputAdapter:
        """根据文件扩展名获取 input_adapter

        Args:
            input_path: 输入文件路径

        Returns:
            BaseInputAdapter: 对应的 input_adapter 实例

        Raises:
            TransformerError: 不支持的文件格式
        """
        ext = Path(input_path).suffix.lower()

        adapter_class = self.INPUT_ADAPTERS.get(ext)
        if adapter_class is None:
            supported = ", ".join(self.INPUT_ADAPTERS.keys())
            raise TransformerError(
                f"Unsupported file format: {ext}. Supported formats: {supported}"
            )

        return adapter_class()

    def _get_source_adapter(self, source_name: str) -> BaseSourceAdapter:
        """根据来源名称获取 source_adapter

        Args:
            source_name: 来源名称

        Returns:
            BaseSourceAdapter: 对应的 source_adapter 实例

        Raises:
            TransformerError: 不支持的来源
        """
        adapter_class = self.SOURCE_ADAPTERS.get(source_name)
        if adapter_class is None:
            supported = ", ".join(self.SOURCE_ADAPTERS.keys())
            raise TransformerError(
                f"Unsupported source: {source_name}. Supported sources: {supported}"
            )

        # 传入 source_name 参数，而不是使用默认值
        return adapter_class(source_name=source_name)
