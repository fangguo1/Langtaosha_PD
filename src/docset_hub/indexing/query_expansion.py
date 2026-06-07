"""Optional LLM query expansion for offline/replay evaluation.

The service is disabled unless LANGTAOSHA_ENABLE_LLM_QUERY_EXPANSION=1. It is
designed to enrich traces, not to replace the deterministic query route.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen


@dataclass
class QueryExpansionResult:
    enabled: bool
    status: str
    expanded_queries: List[str] = field(default_factory=list)
    must_have_terms: List[str] = field(default_factory=list)
    optional_terms: List[str] = field(default_factory=list)
    excluded_terms: List[str] = field(default_factory=list)
    reason: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class LLMQueryExpansionService:
    """Minimal OpenAI-compatible JSON expansion client."""

    def __init__(
        self,
        enabled: Optional[bool] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = 20,
    ) -> None:
        self.enabled = (
            enabled
            if enabled is not None
            else os.environ.get("LANGTAOSHA_ENABLE_LLM_QUERY_EXPANSION", "").lower() in {"1", "true", "yes"}
        )
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.model = model or os.environ.get("LANGTAOSHA_QUERY_EXPANSION_MODEL", "gpt-4.1-mini")
        self.timeout = timeout

    def expand(self, query: str) -> QueryExpansionResult:
        if not self.enabled:
            return QueryExpansionResult(enabled=False, status="disabled")
        if not self.api_key:
            return QueryExpansionResult(enabled=True, status="missing_api_key")

        try:
            payload = self._call_openai(query)
            result = self._sanitize(payload)
            result.enabled = True
            return result
        except Exception as exc:  # pragma: no cover - network dependent
            return QueryExpansionResult(enabled=True, status="error", error=str(exc))

    def _call_openai(self, query: str) -> Dict[str, Any]:
        system_prompt = (
            "You expand biomedical literature search queries. Return strict JSON "
            "with keys expanded_queries, must_have_terms, optional_terms, "
            "excluded_terms, reason. Do not add unrelated entities."
        )
        user_prompt = (
            "Expand this search query for recall. Keep at most 3 expanded queries "
            f"and avoid changing the user's intent.\nQuery: {query}"
        )
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
        }
        request = Request(
            "https://api.openai.com/v1/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        with urlopen(request, timeout=self.timeout) as response:
            raw = json.loads(response.read().decode("utf-8"))
        content = raw["choices"][0]["message"]["content"]
        return json.loads(content)

    @staticmethod
    def _clean_terms(values: Any, limit: int) -> List[str]:
        if not isinstance(values, list):
            return []
        cleaned = []
        for value in values:
            text = re.sub(r"\s+", " ", str(value or "").strip())
            if text and text not in cleaned:
                cleaned.append(text)
            if len(cleaned) >= limit:
                break
        return cleaned

    def _sanitize(self, payload: Dict[str, Any]) -> QueryExpansionResult:
        expanded = self._clean_terms(payload.get("expanded_queries"), 3)
        return QueryExpansionResult(
            enabled=True,
            status="ok",
            expanded_queries=expanded,
            must_have_terms=self._clean_terms(payload.get("must_have_terms"), 8),
            optional_terms=self._clean_terms(payload.get("optional_terms"), 12),
            excluded_terms=self._clean_terms(payload.get("excluded_terms"), 8),
            reason=str(payload.get("reason") or "")[:500] or None,
        )
