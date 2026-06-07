"""Span-level concept matchers for query candidates.

This module turns extractor candidates into concept evidence. Ontology matches
can come from a long-lived service, while keyword matches continue to use the
existing local phrase lexicon.

To see examples, please use bash scripts/run_span_matcher_db.sh
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple

try:
    import requests
except ImportError:  # pragma: no cover - requests is a runtime dependency
    requests = None  # type: ignore[assignment]

from .query_phrase_analyzer import (
    GENERIC_HEADS,
    PhraseCandidate,
    PhraseLexicon,
    PhraseLexiconMatcher,
    QueryPhraseNormalizer,
    STOPWORDS,
)


SOURCE_PRIORITY = {
    "umls": 300,
    "mesh": 200,
    "keyword": 100,
}

EVIDENCE_STRUCTURED_FIELDS = {"aliases", "semantic_types", "types"}

LOW_VALUE_SINGLE_TOKEN_TERMS = GENERIC_HEADS | {
    "automation",
    "cell",
    "cells",
    "deep",
    "learning",
    "solution",
    "solutions",
}


class SpanMatcherError(RuntimeError):
    """Base error for span matching failures."""


class OntologyLinkerServiceUnavailable(SpanMatcherError):
    """Raised when a remote ontology linker cannot be reached."""


class OntologyLinkerConfigurationError(SpanMatcherError):
    """Raised when a remote ontology linker rejects the request shape."""


@dataclass
class ConceptMatchEvidence:
    """A single concept-level match for one candidate span."""

    candidate_text: str
    normalized_text: str
    start: int
    end: int
    candidate_kind: str
    source: str
    canonical: str
    concept_id: Optional[str] = None
    confidence: float = 0.0
    match_type: str = "unknown"
    aliases: List[str] = field(default_factory=list)
    semantic_types: List[str] = field(default_factory=list)
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SpanMatchResult:
    """All evidence collected for a candidate span."""

    candidate: PhraseCandidate
    evidence: List[ConceptMatchEvidence] = field(default_factory=list)

    @property
    def primary_evidence(self) -> Optional[ConceptMatchEvidence]:
        return self.evidence[0] if self.evidence else None

    @property
    def is_matched(self) -> bool:
        return bool(self.evidence)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "candidate": self.candidate.to_dict(),
            "evidence": [item.to_dict() for item in self.evidence],
        }


@dataclass
class SelectedConcept:
    """A non-overlapping matched concept selected from span match results."""

    candidate: PhraseCandidate
    evidence: List[ConceptMatchEvidence]

    @property
    def primary_evidence(self) -> ConceptMatchEvidence:
        return self.evidence[0]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "candidate": self.candidate.to_dict(),
            "primary_evidence": self.primary_evidence.to_dict(),
            "evidence": [item.to_dict() for item in self.evidence],
        }


class SpanMatcher(Protocol):
    """Protocol for candidate span matchers."""

    def match(self, candidate: PhraseCandidate) -> List[ConceptMatchEvidence]:
        ...

    def match_many(self, candidates: Sequence[PhraseCandidate]) -> List[List[ConceptMatchEvidence]]:
        ...


class KeywordSurfaceSpanMatcher:
    """Match candidates against the existing keyword surface lexicon."""

    source = "keyword"

    def __init__(
        self,
        lexicon: PhraseLexicon,
        normalizer: Optional[QueryPhraseNormalizer] = None,
    ):
        self.normalizer = normalizer or QueryPhraseNormalizer()
        self.matcher = PhraseLexiconMatcher(lexicon=lexicon, normalizer=self.normalizer)

    def match(self, candidate: PhraseCandidate) -> List[ConceptMatchEvidence]:
        return self.match_many([candidate])[0]

    def match_many(self, candidates: Sequence[PhraseCandidate]) -> List[List[ConceptMatchEvidence]]:
        matches = self.matcher.match(candidates)
        buckets: List[List[ConceptMatchEvidence]] = []
        for match in matches:
            if not match.is_matched or not match.canonical:
                buckets.append([])
                continue

            normalized_canonical = self.normalizer.normalize_phrase(match.canonical)
            evidence = ConceptMatchEvidence(
                candidate_text=match.text,
                normalized_text=match.normalized_text,
                start=match.start,
                end=match.end,
                candidate_kind=match.kind,
                source=self.source,
                concept_id=f"keyword:{normalized_canonical}",
                canonical=match.canonical,
                confidence=match.confidence,
                match_type=self._keyword_match_type(match.match_type),
                payload={
                    "doc_count": match.doc_count,
                    "variant_count": match.variant_count,
                    "matched_phrase_count": match.matched_phrase_count,
                    "is_generic": match.is_generic,
                    "surface_match_type": match.match_type,
                },
            )
            buckets.append([evidence])
        return buckets

    @staticmethod
    def _keyword_match_type(match_type: str) -> str:
        if match_type == "exact":
            return "keyword_exact"
        if match_type == "alias":
            return "keyword_alias"
        if match_type in {"normalized", "hyphen_space_variant", "plural_variant"}:
            return "keyword_normalized"
        return "keyword_surface"


class RemoteOntologySpanMatcher:
    """Call a long-lived ontology linker API for UMLS/MeSH evidence."""

    def __init__(
        self,
        base_url: str,
        sources: Sequence[str] = ("umls", "mesh"),
        top_k: int = 3,
        threshold: float = 0.9,
        timeout: float = 20.0,
        session: Optional[Any] = None,
    ):
        if not base_url:
            raise OntologyLinkerConfigurationError("base_url is required")
        if session is None and requests is None:
            raise OntologyLinkerConfigurationError("requests is required for RemoteOntologySpanMatcher")

        self.base_url = base_url.rstrip("/")
        self.sources = tuple(sources)
        self.top_k = top_k
        self.threshold = threshold
        self.timeout = timeout
        self.session = session if session is not None else requests.Session()
        if session is None:
            self.session.trust_env = False

    def match(self, candidate: PhraseCandidate) -> List[ConceptMatchEvidence]:
        return self.match_many([candidate])[0]

    def match_many(self, candidates: Sequence[PhraseCandidate]) -> List[List[ConceptMatchEvidence]]:
        if not candidates:
            return []

        candidate_ids = [f"c{index}" for index in range(len(candidates))]
        payload = {
            "sources": list(self.sources),
            "top_k": self.top_k,
            "threshold": self.threshold,
            "candidates": [
                {
                    "id": candidate_id,
                    "text": candidate.text,
                    "normalized_text": candidate.normalized_text,
                    "kind": candidate.kind,
                    "start": candidate.start,
                    "end": candidate.end,
                }
                for candidate_id, candidate in zip(candidate_ids, candidates)
            ],
        }

        response_payload = self._post(payload)
        results_by_id = self._results_by_candidate_id(response_payload)
        buckets: List[List[ConceptMatchEvidence]] = []
        for candidate_id, candidate in zip(candidate_ids, candidates):
            evidence_items = results_by_id.get(candidate_id, [])
            buckets.append([self._to_evidence(candidate, item) for item in evidence_items])
        return buckets

    def _post(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        url = f"{self.base_url}/v1/link"
        try:
            response = self.session.post(url, json=dict(payload), timeout=self.timeout)
        except Exception as exc:  # pragma: no cover - exercised with fake sessions
            raise OntologyLinkerServiceUnavailable(f"ontology linker request failed: {exc}") from exc

        status_code = int(getattr(response, "status_code", 200))
        if status_code >= 400:
            message = getattr(response, "text", "") or f"HTTP {status_code}"
            if status_code == 400:
                raise OntologyLinkerConfigurationError(message)
            raise OntologyLinkerServiceUnavailable(message)

        try:
            data = response.json()
        except Exception as exc:
            raise OntologyLinkerServiceUnavailable(f"ontology linker returned invalid JSON: {exc}") from exc
        if not isinstance(data, Mapping):
            raise OntologyLinkerServiceUnavailable("ontology linker returned a non-object response")
        return data

    @staticmethod
    def _results_by_candidate_id(response_payload: Mapping[str, Any]) -> Dict[str, List[Mapping[str, Any]]]:
        results: Dict[str, List[Mapping[str, Any]]] = {}
        raw_results = response_payload.get("results", [])
        if not isinstance(raw_results, list):
            return results

        for item in raw_results:
            if not isinstance(item, Mapping):
                continue
            candidate_id = str(item.get("candidate_id") or "")
            if not candidate_id:
                continue
            raw_evidence = item.get("evidence", [])
            if not isinstance(raw_evidence, list):
                raw_evidence = []
            results[candidate_id] = [e for e in raw_evidence if isinstance(e, Mapping)]
        return results

    @staticmethod
    def _to_evidence(candidate: PhraseCandidate, item: Mapping[str, Any]) -> ConceptMatchEvidence:
        source = str(item.get("source") or "ontology").lower()
        canonical = str(
            item.get("canonical")
            or item.get("canonical_name")
            or item.get("name")
            or candidate.text
        )
        confidence = _safe_float(item.get("confidence", item.get("score", 0.0)))
        return ConceptMatchEvidence(
            candidate_text=candidate.text,
            normalized_text=candidate.normalized_text,
            start=candidate.start,
            end=candidate.end,
            candidate_kind=candidate.kind,
            source=source,
            concept_id=_optional_str(item.get("concept_id") or item.get("cui") or item.get("mesh_id")),
            canonical=canonical,
            confidence=confidence,
            match_type=str(item.get("match_type") or f"{source}_entity_link"),
            aliases=_string_list(_evidence_value(item, "aliases", default=[])),
            semantic_types=_string_list(
                _evidence_value(item, "semantic_types", "types", default=[])
            ),
            payload=_evidence_payload(item),
        )


class CompositeSpanMatcher:
    """Combine ontology and keyword matchers with deterministic priority sorting."""

    def __init__(
        self,
        matchers: Sequence[SpanMatcher],
        source_priority: Optional[Mapping[str, int]] = None,
    ):
        self.matchers = list(matchers)
        self.source_priority = dict(SOURCE_PRIORITY)
        if source_priority:
            self.source_priority.update({str(key).lower(): int(value) for key, value in source_priority.items()})

    def match(self, candidate: PhraseCandidate) -> List[ConceptMatchEvidence]:
        return self.match_many([candidate])[0]

    def match_many(self, candidates: Sequence[PhraseCandidate]) -> List[List[ConceptMatchEvidence]]:
        buckets: List[List[ConceptMatchEvidence]] = [[] for _ in candidates]
        for matcher in self.matchers:
            matcher_buckets = matcher.match_many(candidates)
            for index, evidence_items in enumerate(matcher_buckets[: len(buckets)]):
                buckets[index].extend(evidence_items)

        for bucket in buckets:
            bucket.sort(key=self._sort_key)
        return buckets

    def _sort_key(self, evidence: ConceptMatchEvidence) -> Tuple[int, float, str, str]:
        priority = self.source_priority.get(evidence.source.lower(), 0)
        return (-priority, -evidence.confidence, evidence.canonical.lower(), evidence.concept_id or "")


class SubphraseCandidateGenerator:
    """Generate scoped n-gram candidates inside trusted non-full-query spans.

    ``full_query`` is intentionally excluded from the default parent kinds, so
    callers need scispaCy or connector-split spans when they expect subphrases
    such as ``genome`` from ``genome interaction``.
    """

    TOKEN_RE = re.compile(r"\S+")
    DEFAULT_PARENT_KINDS = {"scispacy_entity", "scispacy_noun_chunk", "connector_split"}

    def __init__(
        self,
        min_tokens: int = 1,
        max_tokens: int = 3,
        parent_kinds: Optional[Sequence[str]] = None,
        normalizer: Optional[QueryPhraseNormalizer] = None,
    ):
        self.min_tokens = min_tokens
        self.max_tokens = max_tokens
        self.parent_kinds = set(parent_kinds or self.DEFAULT_PARENT_KINDS)
        self.normalizer = normalizer or QueryPhraseNormalizer()

    def expand(self, candidates: Sequence[PhraseCandidate]) -> List[PhraseCandidate]:
        expanded: List[PhraseCandidate] = []
        seen: Dict[Tuple[str, int, int], PhraseCandidate] = {}

        def add(candidate: PhraseCandidate) -> None:
            key = (candidate.normalized_text, candidate.start, candidate.end)
            if key not in seen:
                seen[key] = candidate
                expanded.append(candidate)

        for candidate in candidates:
            add(candidate)
        for candidate in candidates:
            for subphrase in self.generate(candidate):
                add(subphrase)
        return expanded

    def generate(self, candidate: PhraseCandidate) -> List[PhraseCandidate]:
        if candidate.kind not in self.parent_kinds:
            return []

        tokens = self._token_offsets(candidate.text)
        if len(tokens) < 2:
            return []

        generated: List[PhraseCandidate] = []
        max_tokens = min(self.max_tokens, len(tokens))
        for token_count in range(max_tokens, self.min_tokens - 1, -1):
            for start_index in range(0, len(tokens) - token_count + 1):
                window = tokens[start_index : start_index + token_count]
                subphrase = self._candidate_from_window(candidate, window)
                if subphrase is not None:
                    generated.append(subphrase)
        return generated

    def _candidate_from_window(
        self,
        parent: PhraseCandidate,
        window: Sequence[Tuple[str, int, int]],
    ) -> Optional[PhraseCandidate]:
        rel_start = window[0][1]
        rel_end = window[-1][2]
        text = parent.text[rel_start:rel_end].strip()
        normalized = self.normalizer.normalize_phrase(text)
        if not self._is_usable(normalized):
            return None
        if parent.start + rel_start == parent.start and parent.start + rel_end == parent.end:
            return None

        return PhraseCandidate(
            text=text,
            normalized_text=normalized,
            kind="subphrase_ngram",
            start=parent.start + rel_start,
            end=parent.start + rel_end,
        )

    @classmethod
    def _token_offsets(cls, text: str) -> List[Tuple[str, int, int]]:
        return [(match.group(0), match.start(), match.end()) for match in cls.TOKEN_RE.finditer(text)]

    @staticmethod
    def _is_usable(normalized: str) -> bool:
        if not normalized:
            return False
        if normalized in STOPWORDS:
            return False
        if all(token in STOPWORDS for token in normalized.split()):
            return False
        return len(normalized) >= 3


class SpanMatcherExecutor:
    """Expand candidates, run matchers, and return aligned match results."""

    def __init__(
        self,
        matcher: SpanMatcher,
        subphrase_generator: Optional[SubphraseCandidateGenerator] = None,
        include_subphrases: bool = True,
    ):
        self.matcher = matcher
        self.subphrase_generator = subphrase_generator or SubphraseCandidateGenerator()
        self.include_subphrases = include_subphrases

    def expand_candidates(self, candidates: Sequence[PhraseCandidate]) -> List[PhraseCandidate]:
        if not self.include_subphrases:
            return list(candidates)
        return self.subphrase_generator.expand(candidates)

    def match_candidates(self, candidates: Sequence[PhraseCandidate]) -> List[SpanMatchResult]:
        expanded_candidates = self.expand_candidates(candidates)
        evidence_buckets = self.matcher.match_many(expanded_candidates)
        return [
            SpanMatchResult(candidate=candidate, evidence=list(evidence))
            for candidate, evidence in zip(expanded_candidates, evidence_buckets)
        ]


class MaximalConceptSelector:
    """Greedy selector for maximal non-overlapping matched concept spans."""

    def __init__(
        self,
        source_priority: Optional[Mapping[str, int]] = None,
        candidate_kind_bonus: Optional[Mapping[str, float]] = None,
        low_value_single_token_terms: Optional[Sequence[str]] = None,
        normalizer: Optional[QueryPhraseNormalizer] = None,
    ):
        self.source_priority = dict(SOURCE_PRIORITY)
        if source_priority:
            self.source_priority.update({str(key).lower(): int(value) for key, value in source_priority.items()})
        self.normalizer = normalizer or QueryPhraseNormalizer()
        self.candidate_kind_bonus = {
            "full_query": 0.4,
            "scispacy_entity": 0.3,
            "scispacy_noun_chunk": 0.25,
            "connector_split": 0.2,
            "subphrase_ngram": 0.0,
        }
        if candidate_kind_bonus:
            self.candidate_kind_bonus.update({str(key): float(value) for key, value in candidate_kind_bonus.items()})
        self.low_value_single_token_terms = set(LOW_VALUE_SINGLE_TOKEN_TERMS)
        if low_value_single_token_terms:
            self.low_value_single_token_terms.update(
                self.normalizer.normalize_phrase(term) for term in low_value_single_token_terms
            )

    def select(self, results: Sequence[SpanMatchResult]) -> List[SelectedConcept]:
        matched = [
            result
            for result in results
            if result.is_matched and result.primary_evidence is not None and self._is_selectable(result)
        ]
        ranked = sorted(matched, key=self._selection_key)
        selected_results: List[SpanMatchResult] = []
        selected_ranges: List[Tuple[int, int]] = []

        for result in ranked:
            span = (result.candidate.start, result.candidate.end)
            if self._overlaps_any(span, selected_ranges):
                continue
            selected_results.append(result)
            selected_ranges.append(span)

        selected_results.sort(key=lambda result: (result.candidate.start, result.candidate.end))
        return [SelectedConcept(candidate=result.candidate, evidence=result.evidence) for result in selected_results]

    def filter_effective_results(
        self,
        results: Sequence[SpanMatchResult],
        selected_concepts: Sequence[SelectedConcept],
    ) -> List[SpanMatchResult]:
        """Hide exploratory subphrases that lost to selected longer concepts."""

        selected_keys = {
            (
                concept.candidate.start,
                concept.candidate.end,
                concept.candidate.normalized_text,
            )
            for concept in selected_concepts
        }
        effective: List[SpanMatchResult] = []
        for result in results:
            candidate = result.candidate
            key = (candidate.start, candidate.end, candidate.normalized_text)
            if candidate.kind == "subphrase_ngram" and key not in selected_keys:
                continue
            effective.append(result)
        return effective

    def _selection_key(self, result: SpanMatchResult) -> Tuple[float, float, float, float, int, str]:
        evidence = result.primary_evidence
        assert evidence is not None
        token_count = len(result.candidate.normalized_text.split())
        source_priority = self.source_priority.get(evidence.source.lower(), 0)
        kind_bonus = self.candidate_kind_bonus.get(result.candidate.kind, 0.0)
        generic_penalty = 1.0 if self._is_generic_single_token(result.candidate.normalized_text) else 0.0
        confidence = evidence.confidence
        return (
            -float(token_count),
            -float(source_priority),
            -(confidence + kind_bonus - generic_penalty),
            -float(len(result.evidence)),
            result.candidate.start,
            result.candidate.normalized_text,
        )

    def _is_selectable(self, result: SpanMatchResult) -> bool:
        evidence = result.primary_evidence
        if evidence is None:
            return False

        if self._is_partial_ontology_parent(result):
            return False

        token_count = len(result.candidate.normalized_text.split())
        if token_count == 1 and result.candidate.normalized_text in self.low_value_single_token_terms:
            return False
        if token_count == 1 and self._is_ontology_only(result) and not self._looks_like_specific_code(result):
            return False
        return True

    def _is_partial_ontology_parent(self, result: SpanMatchResult) -> bool:
        evidence = result.primary_evidence
        if evidence is None or evidence.source.lower() not in {"umls", "mesh"}:
            return False
        if self._has_keyword_evidence(result):
            return False

        candidate_text = self.normalizer.normalize_phrase(result.candidate.normalized_text)
        canonical_text = self.normalizer.normalize_phrase(evidence.canonical)
        if not candidate_text or not canonical_text or candidate_text == canonical_text:
            return False
        return canonical_text in candidate_text

    @staticmethod
    def _has_keyword_evidence(result: SpanMatchResult) -> bool:
        return any(evidence.source.lower() == "keyword" for evidence in result.evidence)

    def _is_ontology_only(self, result: SpanMatchResult) -> bool:
        return not self._has_keyword_evidence(result) and all(
            evidence.source.lower() in {"umls", "mesh"} for evidence in result.evidence
        )

    @staticmethod
    def _looks_like_specific_code(result: SpanMatchResult) -> bool:
        text = result.candidate.normalized_text
        return any(character.isdigit() for character in text) or "-" in text or "/" in text

    @staticmethod
    def _overlaps_any(span: Tuple[int, int], selected_ranges: Sequence[Tuple[int, int]]) -> bool:
        start, end = span
        return any(start < selected_end and end > selected_start for selected_start, selected_end in selected_ranges)

    @staticmethod
    def _is_generic_single_token(normalized_text: str) -> bool:
        tokens = normalized_text.split()
        return len(tokens) == 1 and tokens[0] in GENERIC_HEADS


def _evidence_value(item: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in item and item.get(key) is not None:
            return item.get(key)

    nested_payload = item.get("payload")
    if isinstance(nested_payload, Mapping):
        for key in keys:
            if key in nested_payload and nested_payload.get(key) is not None:
                return nested_payload.get(key)

    return default


def _evidence_payload(item: Mapping[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for key, value in dict(item).items():
        if key in EVIDENCE_STRUCTURED_FIELDS:
            continue
        if key == "payload" and isinstance(value, Mapping):
            nested_payload = {
                nested_key: nested_value
                for nested_key, nested_value in dict(value).items()
                if nested_key not in EVIDENCE_STRUCTURED_FIELDS
            }
            if nested_payload:
                payload[key] = nested_payload
            continue
        payload[key] = value
    return payload


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _string_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return [str(item) for item in value if item is not None]
    return [str(value)]
