from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RankedDocument:
    work_id: str
    rank: int
    score: float | None = None
    retrieval_debug: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TestbedQuery:
    query_id: int
    query_text: str
    judgments: dict[str, int]
    judgment_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
