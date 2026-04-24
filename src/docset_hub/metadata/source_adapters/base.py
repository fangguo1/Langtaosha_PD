"""Source Adapter 基类"""
from abc import ABC, abstractmethod
from typing import Dict, Any

from ..contracts import NormalizedRecord


class BaseSourceAdapter(ABC):
    """Source Adapter 基类

    所有 source_adapters 都应该继承这个基类，并实现 transform 方法。
    """

    def __init__(self, source_name: str):
        """初始化

        Args:
            source_name: 来源名称（如 langtaosha, biorxiv）
        """
        self.source_name = source_name

    @abstractmethod
    def transform(self, raw_metadata: Dict[str, Any]) -> NormalizedRecord:
        """将原始元数据转换为 NormalizedRecord

        Args:
            raw_metadata: 来源侧的原始元数据字典

        Returns:
            NormalizedRecord: 统一中间结构的记录

        Raises:
            ValueError: 如果原始数据格式不正确或缺少必填字段
        """
        pass

    def extract_source_record_id(self, raw_metadata: Dict[str, Any]) -> str | None:
        """从原始元数据中提取来源记录 ID

        子类可以覆盖此方法以提供自定义提取逻辑。

        Args:
            raw_metadata: 原始元数据字典

        Returns:
            来源记录 ID，如果无法提取则返回 None
        """
        return None

    def extract_source_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """从原始元数据中提取来源 URL

        子类可以覆盖此方法以提供自定义提取逻辑。

        Args:
            raw_metadata: 原始元数据字典

        Returns:
            来源 URL，如果无法提取则返回 None
        """
        return None

    def extract_abstract_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """从原始元数据中提取摘要 URL

        子类可以覆盖此方法以提供自定义提取逻辑。

        Args:
            raw_metadata: 原始元数据字典

        Returns:
            摘要 URL，如果无法提取则返回 None
        """
        return None

    def extract_pdf_url(self, raw_metadata: Dict[str, Any]) -> str | None:
        """从原始元数据中提取 PDF URL

        子类可以覆盖此方法以提供自定义提取逻辑。

        Args:
            raw_metadata: 原始元数据字典

        Returns:
            PDF URL，如果无法提取则返回 None
        """
        return None
