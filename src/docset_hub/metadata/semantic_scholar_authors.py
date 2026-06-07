"""Semantic Scholar based author-name enrichment.

This module is intentionally conservative. Semantic Scholar is not version
aware for bioRxiv records, so enriched author names are only accepted when the
returned author count matches the current article author count.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


DEFAULT_FIELDS = ",".join(
    [
        "paperId",
        "externalIds",
        "title",
        "publicationDate",
        "authors",
        "authors.authorId",
        "authors.name",
        "authors.externalIds",
        "authors.affiliations",
    ]
)


DEFAULT_API_KEY_TEST_PAPER_ID = "arXiv:1706.03762"
DEFAULT_API_KEY_TEST_FIELDS = "paperId,title,year,authors,authors.name"


@dataclass
class SemanticScholarAuthor:
    name: str
    author_id: Optional[str] = None
    external_ids: Dict[str, Any] = field(default_factory=dict)
    affiliations: List[Any] = field(default_factory=list)

    @property
    def url(self) -> Optional[str]:
        if not self.author_id:
            return None
        return f"https://www.semanticscholar.org/author/{self.author_id}"


@dataclass
class SemanticScholarPaper:
    paper_id: Optional[str]
    external_ids: Dict[str, Any]
    title: Optional[str]
    publication_date: Optional[str]
    authors: List[SemanticScholarAuthor]
    raw: Dict[str, Any]


class SemanticScholarClient:
    """Small Graph API client with retry/backoff and no third-party dependency."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        sleep_seconds: float = 1.0,
        user_agent: str = "langtaosha-semantic-scholar-author-enrichment/1.0",
    ) -> None:
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.sleep_seconds = sleep_seconds
        self.user_agent = user_agent

    def fetch_paper_by_doi(self, doi: str, fields: str = DEFAULT_FIELDS) -> SemanticScholarPaper:
        if not doi:
            raise ValueError("doi is required")

        encoded_doi = quote(doi, safe="")
        url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{encoded_doi}?fields={fields}"
        raw = self._fetch_json(url)
        return self._paper_from_raw(raw)

    def fetch_papers_by_ids(
        self,
        paper_ids: List[str],
        fields: str = DEFAULT_FIELDS,
    ) -> List[Optional[SemanticScholarPaper]]:
        """Fetch multiple papers in one request using the Graph paper batch endpoint."""

        if not paper_ids:
            return []

        url = f"https://api.semanticscholar.org/graph/v1/paper/batch?fields={fields}"
        raw_items = self._fetch_json(url, method="POST", payload={"ids": paper_ids})
        if not isinstance(raw_items, list):
            raise RuntimeError("Semantic Scholar batch endpoint returned a non-list response")
        return [self._paper_from_raw(item) if item else None for item in raw_items]

    def fetch_paper_by_id(
        self,
        paper_id: str,
        fields: str = DEFAULT_API_KEY_TEST_FIELDS,
    ) -> SemanticScholarPaper:
        if not paper_id:
            raise ValueError("paper_id is required")

        encoded_paper_id = quote(paper_id, safe=":")
        url = f"https://api.semanticscholar.org/graph/v1/paper/{encoded_paper_id}?fields={fields}"
        raw = self._fetch_json(url)
        return self._paper_from_raw(raw)

    def validate_api_key(
        self,
        paper_id: str = DEFAULT_API_KEY_TEST_PAPER_ID,
        fields: str = DEFAULT_API_KEY_TEST_FIELDS,
    ) -> Dict[str, Any]:
        """Make one authenticated request and return a small validation summary."""

        if not self.api_key:
            return {
                "ok": False,
                "status": "missing_api_key",
                "message": "SEMANTIC_SCHOLAR_API_KEY is not set.",
            }

        paper = self.fetch_paper_by_id(paper_id, fields=fields)
        return {
            "ok": True,
            "status": "ok",
            "paper_id": paper.paper_id,
            "title": paper.title,
            "publication_date": paper.publication_date,
            "author_count": len(paper.authors),
        }

    @staticmethod
    def _paper_from_raw(raw: Dict[str, Any]) -> SemanticScholarPaper:
        return SemanticScholarPaper(
            paper_id=raw.get("paperId"),
            external_ids=raw.get("externalIds") or {},
            title=raw.get("title"),
            publication_date=raw.get("publicationDate"),
            authors=[
                SemanticScholarAuthor(
                    name=str(author.get("name") or "").strip(),
                    author_id=author.get("authorId"),
                    external_ids=author.get("externalIds") or {},
                    affiliations=author.get("affiliations") or [],
                )
                for author in raw.get("authors") or []
            ],
            raw=raw,
        )

    def _fetch_json(
        self,
        url: str,
        method: str = "GET",
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                headers = {
                    "Accept": "application/json",
                    "User-Agent": self.user_agent,
                }
                if self.api_key:
                    headers["x-api-key"] = self.api_key
                data = None
                if payload is not None:
                    headers["Content-Type"] = "application/json"
                    data = json.dumps(payload).encode("utf-8")
                request = Request(url, data=data, headers=headers, method=method)
                with urlopen(request, timeout=self.timeout) as response:
                    charset = response.headers.get_content_charset() or "utf-8"
                    return json.loads(response.read().decode(charset))
            except HTTPError as exc:
                last_error = exc
                if exc.code in {400, 401, 403, 404}:
                    raise
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                if retry_after:
                    try:
                        time.sleep(max(float(retry_after), self.sleep_seconds))
                        continue
                    except ValueError:
                        pass
            except (URLError, TimeoutError) as exc:
                last_error = exc

            if attempt < self.max_retries:
                time.sleep(self.sleep_seconds * attempt)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Semantic Scholar request failed")


def author_name(author: Any) -> str:
    if isinstance(author, dict):
        for key in ("name", "full_name", "fullName", "author", "raw_name", "enriched_name"):
            value = author.get(key)
            if value:
                return str(value).strip()
        given = author.get("given_names") or author.get("given") or author.get("firstName")
        surname = author.get("surname") or author.get("lastName")
        if given or surname:
            return " ".join(str(part).strip() for part in (given, surname) if part)
        return ""
    return str(author or "").strip()


def enrich_author_list(
    current_authors: List[Any],
    semantic_paper: SemanticScholarPaper,
) -> Dict[str, Any]:
    """Return a conservative enrichment result for a paper author list."""

    current_count = len(current_authors or [])
    semantic_count = len(semantic_paper.authors or [])

    if current_count == 0:
        return {
            "status": "no_current_authors",
            "should_update": False,
            "authors": current_authors or [],
            "current_author_count": current_count,
            "semantic_author_count": semantic_count,
        }

    if semantic_count == 0:
        return {
            "status": "semantic_scholar_no_authors",
            "should_update": False,
            "authors": current_authors,
            "current_author_count": current_count,
            "semantic_author_count": semantic_count,
        }

    if current_count != semantic_count:
        return {
            "status": "author_count_mismatch",
            "should_update": False,
            "authors": current_authors,
            "current_author_count": current_count,
            "semantic_author_count": semantic_count,
        }

    enriched: List[Dict[str, Any]] = []
    for index, current in enumerate(current_authors):
        current_dict = dict(current) if isinstance(current, dict) else {"name": author_name(current)}
        semantic_author = semantic_paper.authors[index]
        raw_name = current_dict.get("raw_name") or author_name(current)
        current_dict.setdefault("raw_name", raw_name)
        current_dict["enriched_name"] = semantic_author.name or raw_name
        current_dict["semantic_scholar_author_id"] = semantic_author.author_id
        current_dict["semantic_scholar_author_url"] = semantic_author.url
        current_dict["semantic_scholar_external_ids"] = semantic_author.external_ids
        current_dict["semantic_scholar_affiliations"] = semantic_author.affiliations
        current_dict["author_enrichment_source"] = "semantic_scholar"
        current_dict["author_enrichment_status"] = "matched_author_count"
        current_dict["author_enrichment_order"] = index + 1
        enriched.append(current_dict)

    return {
        "status": "matched_author_count",
        "should_update": True,
        "authors": enriched,
        "current_author_count": current_count,
        "semantic_author_count": semantic_count,
    }
