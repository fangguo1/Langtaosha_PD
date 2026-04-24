"""版本比较工具模块

提供版本号比较功能，用于支持同 source 的版本覆盖策略。
"""
from typing import Optional, Tuple
from packaging import version
import logging
from datetime import datetime, date


def compare_versions(
    version1: Optional[str],
    version2: Optional[str]
) -> Tuple[int, str]:
    """比较两个版本号

    Args:
        version1: 第一个版本号
        version2: 第二个版本号

    Returns:
        Tuple[int, str]: (比较结果, 比较方法)
        - 比较结果: -1 (version1 < version2), 0 (version1 == version2), 1 (version1 > version2)
        - 比较方法: "version" (使用版本号比较) 或 "equal" (版本号相同)

    Raises:
        ValueError: 如果版本号不可比较
    """
    # 如果两个版本号都为 None 或相同，返回相等
    if version1 == version2:
        return (0, "equal")

    # 如果任一版本号为 None，无法比较
    if version1 is None or version2 is None:
        raise ValueError(
            f"Cannot compare versions: version1={version1}, version2={version2}. "
            "Both versions must be non-None for comparison."
        )

    try:
        # 使用 packaging.version 进行语义化版本比较
        v1 = version.parse(version1)
        v2 = version.parse(version2)

        if v1 < v2:
            return (-1, "version")
        elif v1 > v2:
            return (1, "version")
        else:
            return (0, "equal")
    except version.InvalidVersion as e:
        # 版本号格式无效，抛出异常
        raise ValueError(
            f"Invalid version format: version1={version1}, version2={version2}. "
            f"Error: {str(e)}"
        )


def should_update_by_version(
    new_version: Optional[str],
    existing_version: Optional[str],
    new_online_at: Optional[str],
    existing_online_at: Optional[str]
) -> Tuple[bool, str]:
    """判断是否应该根据版本号更新记录

    策略：
    1. 比较 version（可比较且新版本更高 → 覆盖）
    2. 若 version 不可比较或相同，则比较 online_at（更晚的覆盖更早的）
    3. 若均不可比较，默认不更新

    Args:
        new_version: 新记录的版本号
        existing_version: 现有记录的版本号
        new_online_at: 新记录的在线日期
        existing_online_at: 现有记录的在线日期

    Returns:
        Tuple[bool, str]: (是否应该更新, 原因)
    """
    try:
        # 尝试比较版本号
        comparison, method = compare_versions(new_version, existing_version)

        if method == "version":
            if comparison > 0:
                # 新版本更高，应该更新
                return (True, f"New version {new_version} > existing version {existing_version}")
            elif comparison < 0:
                # 新版本更低，不应该更新
                return (False, f"New version {new_version} < existing version {existing_version}")
            else:
                # 版本号相同，比较 online_at
                pass

        # 版本号相同或不可比较，比较 online_at
        if new_online_at and existing_online_at:
            new_dt = _normalize_online_at(new_online_at)
            existing_dt = _normalize_online_at(existing_online_at)
            if new_dt and existing_dt:
                if new_dt > existing_dt:
                    return (True, f"Same version, new online_at {_format_online_at_for_reason(new_dt)} > existing {_format_online_at_for_reason(existing_dt)}")
                return (False, f"Same version, new online_at {_format_online_at_for_reason(new_dt)} <= existing {_format_online_at_for_reason(existing_dt)}")

            # 解析失败时降级为原始字符串比较（不截断）
            new_online_at_str = str(new_online_at)
            existing_online_at_str = str(existing_online_at)
            if new_online_at_str > existing_online_at_str:
                return (True, f"Same version (fallback string), new online_at {new_online_at_str} > existing {existing_online_at_str}")
            return (False, f"Same version (fallback string), new online_at {new_online_at_str} <= existing {existing_online_at_str}")

        # online_at 也不可比较，默认不更新
        return (False, "Version and online_at are not comparable, keeping existing record")

    except ValueError as e:
        # 版本比较失败，记录日志并降级到 online_at 比较
        logging.warning(f"Version comparison failed: {str(e)}. Falling back to online_at comparison.")

        if new_online_at and existing_online_at:
            new_dt = _normalize_online_at(new_online_at)
            existing_dt = _normalize_online_at(existing_online_at)
            if new_dt and existing_dt:
                if new_dt > existing_dt:
                    return (True, f"Version comparison failed, new online_at {_format_online_at_for_reason(new_dt)} > existing {_format_online_at_for_reason(existing_dt)}")
                return (False, f"Version comparison failed, new online_at {_format_online_at_for_reason(new_dt)} <= existing {_format_online_at_for_reason(existing_dt)}")

            new_online_at_str = str(new_online_at)
            existing_online_at_str = str(existing_online_at)
            if new_online_at_str > existing_online_at_str:
                return (True, f"Version comparison failed (fallback string), new online_at {new_online_at_str} > existing {existing_online_at_str}")
            return (False, f"Version comparison failed (fallback string), new online_at {new_online_at_str} <= existing {existing_online_at_str}")

        # 都不可比较，默认不更新
        return (False, "Version comparison failed and online_at not available, keeping existing record")


def _normalize_online_at(value) -> Optional[datetime]:
    """将 online_at 统一转换为 datetime 以做可靠比较。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    text = str(value).strip()
    if not text:
        return None

    # 兼容 ISO8601 的 Z 后缀
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    # 依次尝试 datetime / date 解析
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return None


def _format_online_at_for_reason(value: datetime) -> str:
    """为日志/原因字符串生成更紧凑的时间表示。"""
    if value.tzinfo is None and value.time() == datetime.min.time():
        return value.date().isoformat()
    return value.isoformat()
