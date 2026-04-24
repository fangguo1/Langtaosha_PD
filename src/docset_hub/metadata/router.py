"""元数据来源路由器

验证 source_name 是否在支持的来源列表中。
"""
from typing import Dict, Any, List


class RouteResult:
    """路由结果"""
    def __init__(self, source_adapter: str, reason: str, confidence: float = 1.0):
        self.source_adapter = source_adapter
        self.reason = reason
        self.confidence = confidence

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_adapter": self.source_adapter,
            "reason": self.reason,
            "confidence": self.confidence,
        }

    def __repr__(self) -> str:
        return f"RouteResult(adapter={self.source_adapter}, reason={self.reason}, confidence={self.confidence})"


class RoutingError(Exception):
    """路由失败异常"""
    pass


class MetadataRouter:
    """元数据来源路由器

    验证 source_name 是否在支持的来源列表中。
    不再支持自动识别，必须显式指定来源名称。
    """

    # 支持的来源列表
    SUPPORTED_SOURCES = [
        "langtaosha",
        "biorxiv_history",
        "biorxiv_daily",
        "arxiv",
        "pubmed",
    ]

    def __init__(self):
        """初始化路由器"""
        pass

    def route(
        self,
        payload: Dict[str, Any],
        source_name: str,
    ) -> RouteResult:
        """路由到对应的 source_adapter

        注意：此方法要求必须显式指定 source_name，不再支持自动识别。

        Args:
            payload: 原始元数据字典
            source_name: 必须显式指定的来源名称

        Returns:
            RouteResult: 路由结果，包含选中的 adapter 和原因

        Raises:
            RoutingError: 当 source_name 不在 SUPPORTED_SOURCES 中时
        """
        if source_name not in self.SUPPORTED_SOURCES:
            raise RoutingError(
                f"Unsupported source: '{source_name}'. "
                f"Supported sources: {', '.join(self.SUPPORTED_SOURCES)}"
            )

        return RouteResult(
            source_adapter=source_name,
            reason=f"explicitly specified as {source_name}",
            confidence=1.0,
        )

    def batch_route(
        self,
        payloads: List[Dict[str, Any]],
        source_names: List[str],
    ) -> List[RouteResult]:
        """批量路由

        Args:
            payloads: 原始元数据字典列表
            source_names: 必须显式指定的来源名称列表

        Returns:
            路由结果列表

        Raises:
            ValueError: 当 payloads 和 source_names 长度不匹配时
            RoutingError: 当任何 source_name 不在 SUPPORTED_SOURCES 中时
        """
        if len(payloads) != len(source_names):
            raise ValueError("payloads and source_names must have the same length")

        results = []
        for payload, source_name in zip(payloads, source_names):
            result = self.route(payload, source_name)
            results.append(result)

        return results
