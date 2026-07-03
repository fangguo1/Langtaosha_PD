from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Optional

from .contracts import RankedDocument


def normalize_results(rows: Iterable[Mapping[str, Any]]) -> list[RankedDocument]:
    documents: list[RankedDocument] = []
    seen: set[str] = set()
    for row in rows:
        work_id = str(row.get("work_id") or "")
        if not work_id:
            raise ValueError("search result is missing work_id")
        if work_id in seen:
            continue
        seen.add(work_id)
        score = row.get("similarity", row.get("score"))
        documents.append(
            RankedDocument(
                work_id=work_id,
                rank=len(documents) + 1,
                score=None if score is None else float(score),
                retrieval_debug=dict(row.get("retrieval_debug") or {}),
            )
        )
    return documents


class PaperIndexerSearchStrategy:
    def __init__(
        self,
        *,
        indexer: Any,
        search_type: str,
        source_list: Optional[list[str]] = None,
    ) -> None:
        self.indexer = indexer
        self.search_type = search_type
        self.source_list = source_list
        self.name = search_type

    def search(self, query: str, top_k: int) -> list[RankedDocument]:
        rows = self.indexer.search(
            query=query,
            source_list=self.source_list,
            top_k=top_k,
            hydrate=False,
            search_type=self.search_type,
        )
        return normalize_results(rows)


class HybridRetrievalSearchStrategy:
    def __init__(self, *, indexer: Any, source_list: Optional[list[str]] = None) -> None:
        self.indexer = indexer
        self.source_list = source_list
        self.name = "hybrid_retrieval"

    def search(self, query: str, top_k: int) -> list[RankedDocument]:
        rows = self.indexer.hybrid_retrieval_search(
            query=query,
            source_list=self.source_list,
            top_k=top_k,
            hydrate=False,
        )
        return normalize_results(rows)
