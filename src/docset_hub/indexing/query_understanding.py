"""Query understanding MVP for search routing.

Phase 3 intentionally keeps this layer deterministic: normalize the incoming
query, verify high-confidence author candidates from MetadataDB, and otherwise
fall back to semantic/vector search.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple


TOPIC_HINT_WORDS = {
    "learning",
    "model",
    "models",
    "gene",
    "protein",
    "cell",
    "cells",
    "disease",
    "cancer",
    "algorithm",
    "algorithms",
    "analysis",
    "using",
    "with",
    "for",
    "in",
    "therapy",
    "genomics",
}


EDGE_PUNCTUATION = " \t\r\n\"'`“”‘’!?;:()[]{}<>"


PHRASE_CONNECTORS = {
    "and",
    "or",
    "for",
    "with",
    "in",
    "using",
    "via",
    "by",
    "of",
    "on",
    "to",
    "from",
}


PHRASE_STOPWORDS = PHRASE_CONNECTORS | {
    "a",
    "an",
    "the",
    "this",
    "that",
    "these",
    "those",
}


PHRASE_SOURCE_PRIORITY = {
    "scispacy_entity": 0,
    "scispacy_noun_chunk": 1,
    "rule_split": 2,
    "ngram": 3,
}


@dataclass
class QueryNormalizationResult:
    """Normalized query plus original text for display."""

    original_query: str
    normalized_query: str
    is_valid: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class QueryUnderstandingResult:
    """Serializable query-understanding result for API/indexer routing."""

    original_query: str
    normalized_query: str
    intent: str
    route: str
    corrected_query: Optional[str] = None
    matched_author: Optional[str] = None
    suggested_author: Optional[str] = None
    confidence: float = 0.0
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    corrections: List[Dict[str, Any]] = field(default_factory=list)
    reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class QueryNormalizer:
    """Lightweight deterministic query normalizer."""

    def normalize(self, query: str) -> QueryNormalizationResult:
        original_query = "" if query is None else str(query)
        normalized_query = self._normalize_text(original_query)
        return QueryNormalizationResult(
            original_query=original_query,
            normalized_query=normalized_query,
            is_valid=bool(normalized_query),
        )

    @staticmethod
    def _normalize_text(query: str) -> str:
        stripped = (query or "").strip()
        stripped = stripped.strip(EDGE_PUNCTUATION)
        return re.sub(r"\s+", " ", stripped).strip()


def normalize_query(query: str) -> Dict[str, Any]:
    """Normalize a raw query and return a dict suitable for callers/tests."""

    return QueryNormalizer().normalize(query).to_dict()


def normalize_author_name(name: str) -> str:
    """Normalize author names for deterministic comparison."""

    if not name:
        return ""
    return " ".join(
        str(name)
        .lower()
        .replace(",", " ")
        .replace(".", " ")
        .split()
    )


@dataclass(frozen=True)
class PhraseSpan:
    """A candidate query phrase with offsets in the normalized query."""

    text: str
    start: int
    end: int
    source: str
    token_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class PhraseSegmenter:
    """Segment sentence-like queries into candidate phrases for correction."""

    def __init__(
        self,
        nlp: Optional[Any] = None,
        max_ngram: int = 5,
        min_chars: int = 4,
    ):
        self.nlp = nlp
        self.max_ngram = max_ngram
        self.min_chars = min_chars

    def segment(self, query: str) -> List[PhraseSpan]:
        normalized_query = QueryNormalizer().normalize(query).normalized_query
        if not normalized_query:
            return []

        spans: List[PhraseSpan] = []
        spans.extend(self._scispacy_spans(normalized_query))
        spans.extend(self._rule_split_spans(normalized_query))
        spans.extend(self._ngram_spans(normalized_query))
        return self._dedupe_and_sort(spans, normalized_query)

    def _scispacy_spans(self, query: str) -> List[PhraseSpan]:
        if self.nlp is None:
            return []

        doc = self.nlp(query)
        spans: List[PhraseSpan] = []
        for ent in getattr(doc, "ents", []) or []:
            spans.append(self._span_from_offsets(query, ent.start_char, ent.end_char, "scispacy_entity"))

        try:
            noun_chunks = list(doc.noun_chunks)
        except Exception:
            noun_chunks = []

        for chunk in noun_chunks:
            spans.append(self._span_from_offsets(query, chunk.start_char, chunk.end_char, "scispacy_noun_chunk"))
        return [span for span in spans if span is not None]

    def _rule_split_spans(self, query: str) -> List[PhraseSpan]:
        spans = []
        connector_pattern = "|".join(re.escape(word) for word in sorted(PHRASE_CONNECTORS, key=len, reverse=True))
        split_pattern = re.compile(rf"\s+(?:{connector_pattern})\s+|[,;/]+", re.IGNORECASE)

        start = 0
        for match in split_pattern.finditer(query):
            spans.append(self._span_from_offsets(query, start, match.start(), "rule_split"))
            start = match.end()
        spans.append(self._span_from_offsets(query, start, len(query), "rule_split"))
        return [span for span in spans if span is not None]

    def _ngram_spans(self, query: str) -> List[PhraseSpan]:
        token_matches = list(re.finditer(r"[A-Za-z0-9][A-Za-z0-9+_/-]*", query))
        spans: List[PhraseSpan] = []
        for start_idx in range(len(token_matches)):
            for end_idx in range(start_idx + 1, min(len(token_matches), start_idx + self.max_ngram) + 1):
                start = token_matches[start_idx].start()
                end = token_matches[end_idx - 1].end()
                spans.append(self._span_from_offsets(query, start, end, "ngram"))
        return [span for span in spans if span is not None]

    def _span_from_offsets(
        self,
        query: str,
        start: int,
        end: int,
        source: str,
    ) -> Optional[PhraseSpan]:
        start, end = self._trim_offsets(query, start, end)
        if start >= end:
            return None
        text = query[start:end]
        if not self._is_usable_phrase(text):
            return None
        return PhraseSpan(
            text=text,
            start=start,
            end=end,
            source=source,
            token_count=len(QueryCorrector._normalize_term(text).split()),
        )

    @staticmethod
    def _trim_offsets(query: str, start: int, end: int) -> Tuple[int, int]:
        while start < end and query[start].isspace():
            start += 1
        while end > start and query[end - 1].isspace():
            end -= 1
        while start < end and query[start] in EDGE_PUNCTUATION + ",./":
            start += 1
        while end > start and query[end - 1] in EDGE_PUNCTUATION + ",./":
            end -= 1
        return start, end

    def _is_usable_phrase(self, text: str) -> bool:
        normalized = QueryCorrector._normalize_term(text)
        if len(normalized) < self.min_chars:
            return False
        tokens = normalized.split()
        if not tokens or len(tokens) > self.max_ngram:
            return False
        if all(token in PHRASE_STOPWORDS for token in tokens):
            return False
        return True

    def _dedupe_and_sort(self, spans: List[PhraseSpan], query: str) -> List[PhraseSpan]:
        best_by_key: Dict[Tuple[int, int, str], PhraseSpan] = {}
        for span in spans:
            key = (span.start, span.end, QueryCorrector._normalize_term(span.text))
            existing = best_by_key.get(key)
            if existing is None or self._span_rank(span) < self._span_rank(existing):
                best_by_key[key] = span

        candidates = sorted(
            best_by_key.values(),
            key=lambda span: (
                span.start,
                -(span.end - span.start),
                self._span_rank(span),
                span.text.lower(),
            ),
        )

        selected: List[PhraseSpan] = []
        for span in candidates:
            if any(self._overlaps(span, existing) and self._span_rank(existing) <= self._span_rank(span) for existing in selected):
                continue
            selected = [
                existing
                for existing in selected
                if not (self._overlaps(span, existing) and self._span_rank(span) < self._span_rank(existing))
            ]
            selected.append(span)
        return sorted(selected, key=lambda span: (span.start, span.end))

    @staticmethod
    def _span_rank(span: PhraseSpan) -> Tuple[int, int]:
        return (PHRASE_SOURCE_PRIORITY.get(span.source, 99), -(span.end - span.start))

    @staticmethod
    def _overlaps(left: PhraseSpan, right: PhraseSpan) -> bool:
        return left.start < right.end and right.start < left.end


class AuthorMatcher:
    """Classify a query as an author name only after DB-backed verification."""

    HIGH_CONFIDENCE_THRESHOLD = 0.92
    MIDDLE_CONFIDENCE_THRESHOLD = 0.80
    AMBIGUOUS_SCORE_GAP = 0.03

    def __init__(self, metadata_db: Any):
        self.metadata_db = metadata_db

    def match(self, query: str) -> Dict[str, Any]:
        normalized_query = QueryNormalizer().normalize(query).normalized_query
        if not normalized_query:
            return self._result(False, None, 0.0, [], "empty_query")

        tokens = normalize_author_name(normalized_query).split()
        if not tokens:
            return self._result(False, None, 0.0, [], "empty_query")

        too_many_tokens = len(tokens) > 4
        has_topic_hint = any(token in TOPIC_HINT_WORDS for token in tokens)

        raw_candidates = self.metadata_db.suggest_author_names(
            query=normalized_query,
            limit=5,
        )
        candidates = [self._normalize_candidate(item) for item in raw_candidates]
        candidates = sorted(
            candidates,
            key=lambda item: (-item["score"], -item.get("paper_count", 0), item["name"].lower()),
        )
        if not candidates:
            return self._result(False, None, 0.0, [], "no_author_candidates")

        best = candidates[0]
        best_score = float(best.get("score") or 0.0)
        query_author_norm = normalize_author_name(normalized_query)
        best_author_norm = best.get("normalized_name") or normalize_author_name(best["name"])
        exact_match = query_author_norm == best_author_norm

        if not exact_match:
            if too_many_tokens:
                best_score *= 0.7
            if has_topic_hint:
                best_score *= 0.7

        ambiguous = self._is_ambiguous(tokens, candidates, best_score, exact_match)
        if exact_match and not too_many_tokens:
            return self._result(
                True,
                best["name"],
                1.0,
                candidates,
                "author_candidate_exact_match",
            )

        if best_score >= self.HIGH_CONFIDENCE_THRESHOLD and not ambiguous:
            return self._result(
                True,
                best["name"],
                best_score,
                candidates,
                "author_candidate_high_confidence",
            )

        if best_score >= self.MIDDLE_CONFIDENCE_THRESHOLD:
            reason = "author_candidate_ambiguous" if ambiguous else "author_candidate_middle_confidence"
            return self._result(False, None, best_score, candidates, reason)

        reason = "topic_hint_reduced_confidence" if has_topic_hint else "author_candidate_low_confidence"
        return self._result(False, None, best_score, candidates, reason)

    def _normalize_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        name = str(candidate.get("name") or "")
        normalized_name = candidate.get("normalized_name") or normalize_author_name(name)
        return {
            "name": name,
            "normalized_name": normalized_name,
            "score": float(candidate.get("score") or 0.0),
            "paper_count": int(candidate.get("paper_count") or 0),
        }

    def _is_ambiguous(
        self,
        tokens: List[str],
        candidates: List[Dict[str, Any]],
        best_score: float,
        exact_match: bool,
    ) -> bool:
        if exact_match:
            return False
        if len(tokens) == 1 and len(candidates) > 1:
            return True
        if len(candidates) < 2:
            return False
        second_score = float(candidates[1].get("score") or 0.0)
        return best_score - second_score < self.AMBIGUOUS_SCORE_GAP

    @staticmethod
    def _result(
        is_author: bool,
        matched_author: Optional[str],
        confidence: float,
        candidates: List[Dict[str, Any]],
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "is_author": is_author,
            "matched_author": matched_author,
            "confidence": confidence,
            "candidates": candidates,
            "reason": reason,
        }


class QueryCorrector:
    """Keyword-backed query corrector for semantic search terms."""

    AUTO_APPLY_THRESHOLD = 0.88
    SUGGEST_THRESHOLD = 0.75

    def __init__(self, metadata_db: Any):
        self.metadata_db = metadata_db

    def correct(self, query: str) -> Dict[str, Any]:
        normalized_query = QueryNormalizer().normalize(query).normalized_query
        if not self._should_correct(normalized_query):
            return self._result(None, 0.0, False, [], "query_correction_not_applicable")

        candidates = self._candidate_terms(normalized_query)
        if not candidates:
            return self._result(None, 0.0, False, [], "no_query_term_candidates")

        scored = []
        for candidate in candidates:
            keyword = candidate["keyword"]
            score = self._score(normalized_query, keyword)
            scored.append({**candidate, "score": score})
        scored = sorted(
            scored,
            key=lambda item: (
                -item["score"],
                -item.get("doc_count", 0),
                -float(item.get("avg_weight") or 0.0),
                item["keyword"].lower(),
            ),
        )
        best = scored[0]
        best_score = float(best["score"])
        if self._normalize_term(normalized_query) == self._normalize_term(best["keyword"]):
            return self._result(best["keyword"], 1.0, False, scored[:5], "query_term_exact_match")
        if best_score >= self.AUTO_APPLY_THRESHOLD:
            corrected = self._match_query_casing(normalized_query, best["keyword"])
            return self._result(corrected, best_score, True, scored[:5], "query_term_high_confidence")
        if best_score >= self.SUGGEST_THRESHOLD:
            corrected = self._match_query_casing(normalized_query, best["keyword"])
            return self._result(corrected, best_score, False, scored[:5], "query_term_middle_confidence")
        return self._result(None, best_score, False, scored[:5], "query_term_low_confidence")

    def _candidate_terms(self, query: str) -> List[Dict[str, Any]]:
        suggest = getattr(self.metadata_db, "suggest_query_terms", None)
        if not suggest:
            return []
        return list(suggest(query=query, limit=20))

    @classmethod
    def _score(cls, query: str, keyword: str) -> float:
        q = cls._normalize_term(query)
        k = cls._normalize_term(keyword)
        if not q or not k:
            return 0.0
        if q == k:
            return 1.0
        try:
            from rapidfuzz import fuzz

            return fuzz.WRatio(q, k) / 100.0
        except ImportError:
            from difflib import SequenceMatcher

            return SequenceMatcher(None, q, k).ratio()

    @staticmethod
    def _normalize_term(value: str) -> str:
        return " ".join(re.sub(r"[^a-z0-9+_-]+", " ", (value or "").lower()).split())

    @classmethod
    def _should_correct(cls, query: str) -> bool:
        normalized = cls._normalize_term(query)
        if len(normalized) < 4:
            return False
        return bool(re.search(r"[a-zA-Z]", normalized))

    @staticmethod
    def _match_query_casing(query: str, candidate: str) -> str:
        """Adapt candidate display casing to the user's query phrase."""
        query_text = query or ""
        candidate_text = candidate or ""
        if not query_text or not candidate_text:
            return candidate_text

        query_letters = [char for char in query_text if char.isalpha()]
        if query_letters and not any(char.isupper() for char in query_letters):
            return candidate_text.lower()
        return candidate_text

    @staticmethod
    def _result(
        corrected_query: Optional[str],
        confidence: float,
        auto_apply: bool,
        candidates: List[Dict[str, Any]],
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "corrected_query": corrected_query,
            "confidence": confidence,
            "auto_apply": auto_apply,
            "candidates": candidates,
            "reason": reason,
        }


class PhraseAwareQueryCorrector:
    """Correct multiple phrase spans in sentence-like semantic queries."""

    AUTO_APPLY_THRESHOLD = 0.88
    SUGGEST_THRESHOLD = 0.82

    def __init__(
        self,
        metadata_db: Any,
        phrase_segmenter: Optional[PhraseSegmenter] = None,
        whole_query_corrector: Optional[QueryCorrector] = None,
    ):
        self.metadata_db = metadata_db
        self.phrase_segmenter = phrase_segmenter or PhraseSegmenter()
        self.whole_query_corrector = whole_query_corrector or QueryCorrector(metadata_db)

    def correct(self, query: str) -> Dict[str, Any]:
        normalized_query = QueryNormalizer().normalize(query).normalized_query
        if not normalized_query:
            return self._with_corrections(
                self.whole_query_corrector.correct(query),
                corrections=[],
            )
        if not self._should_use_phrase_correction(normalized_query):
            return self._with_corrections(
                self.whole_query_corrector.correct(normalized_query),
                corrections=[],
            )

        try:
            phrase_result = self._correct_phrase_spans(normalized_query)
        except Exception:
            fallback = self.whole_query_corrector.correct(normalized_query)
            return self._with_corrections(
                fallback,
                corrections=[],
                fallback_reason="phrase_segmentation_failed",
            )

        if phrase_result["auto_apply"] or phrase_result["corrections"]:
            return phrase_result

        return self._with_corrections(
            self.whole_query_corrector.correct(normalized_query),
            corrections=[],
        )

    def _correct_phrase_spans(self, query: str) -> Dict[str, Any]:
        spans = self.phrase_segmenter.segment(query)
        scored_corrections: List[Dict[str, Any]] = []
        scored_candidates: List[Dict[str, Any]] = []

        for span in spans:
            if not QueryCorrector._should_correct(span.text) or self._looks_like_author_name(span.text):
                continue

            candidates = self._candidate_terms(span.text)
            if not candidates:
                continue

            scored = []
            for candidate in candidates:
                keyword = candidate["keyword"]
                score = QueryCorrector._score(span.text, keyword)
                scored.append({**candidate, "score": score})
            scored = sorted(
                scored,
                key=lambda item: (
                    -item["score"],
                    -item.get("doc_count", 0),
                    -float(item.get("avg_weight") or 0.0),
                    item["keyword"].lower(),
                ),
            )
            scored_candidates.extend(scored[:3])
            best = scored[0]
            best_score = float(best["score"])
            if QueryCorrector._normalize_term(span.text) == QueryCorrector._normalize_term(best["keyword"]):
                continue
            if best_score < self.SUGGEST_THRESHOLD:
                continue

            scored_corrections.append(
                {
                    "original": span.text,
                    "corrected": QueryCorrector._match_query_casing(span.text, best["keyword"]),
                    "start": span.start,
                    "end": span.end,
                    "confidence": best_score,
                    "auto_apply": best_score >= self.AUTO_APPLY_THRESHOLD,
                    "source": span.source,
                    "candidate_source": best.get("source"),
                    "keyword_type": best.get("keyword_type"),
                    "candidates": scored[:5],
                }
            )

        selected = self._select_non_overlapping(scored_corrections)
        auto_corrections = [item for item in selected if item["auto_apply"]]
        if not auto_corrections:
            confidence = max((item["confidence"] for item in selected), default=0.0)
            return {
                "corrected_query": None,
                "confidence": confidence,
                "auto_apply": False,
                "candidates": scored_candidates[:5],
                "corrections": selected,
                "reason": "phrase_query_terms_middle_confidence" if selected else "no_phrase_query_term_candidates",
            }

        corrected_query = self._apply_corrections(query, auto_corrections)
        confidence = min(item["confidence"] for item in auto_corrections)
        return {
            "corrected_query": corrected_query,
            "confidence": confidence,
            "auto_apply": True,
            "candidates": scored_candidates[:5],
            "corrections": selected,
            "reason": "phrase_query_terms_high_confidence",
        }

    def _candidate_terms(self, query: str) -> List[Dict[str, Any]]:
        suggest = getattr(self.metadata_db, "suggest_query_terms", None)
        if not suggest:
            return []
        return list(suggest(query=query, limit=20))

    @staticmethod
    def _should_use_phrase_correction(query: str) -> bool:
        normalized = QueryCorrector._normalize_term(query)
        tokens = normalized.split()
        if len(tokens) >= 5:
            return True
        if re.search(r"[,;/]", query):
            return True
        return any(token in PHRASE_CONNECTORS for token in tokens)

    @staticmethod
    def _looks_like_author_name(text: str) -> bool:
        tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", text) if token]
        if len(tokens) < 2 or len(tokens) > 4:
            return False
        normalized_tokens = [token.lower().strip(".") for token in tokens]
        if any(token in TOPIC_HINT_WORDS for token in normalized_tokens):
            return False
        return all(token[:1].isupper() for token in tokens)

    @staticmethod
    def _select_non_overlapping(corrections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        selected: List[Dict[str, Any]] = []
        for correction in sorted(
            corrections,
            key=lambda item: (
                not item["auto_apply"],
                -item["confidence"],
                -(item["end"] - item["start"]),
                item["start"],
            ),
        ):
            if any(correction["start"] < existing["end"] and existing["start"] < correction["end"] for existing in selected):
                continue
            selected.append(correction)
        return sorted(selected, key=lambda item: (item["start"], item["end"]))

    @staticmethod
    def _apply_corrections(query: str, corrections: List[Dict[str, Any]]) -> str:
        parts = []
        cursor = 0
        for correction in sorted(corrections, key=lambda item: item["start"]):
            parts.append(query[cursor:correction["start"]])
            parts.append(correction["corrected"])
            cursor = correction["end"]
        parts.append(query[cursor:])
        return "".join(parts)

    @staticmethod
    def _with_corrections(
        result: Dict[str, Any],
        corrections: List[Dict[str, Any]],
        fallback_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = dict(result)
        payload.setdefault("corrections", corrections)
        if fallback_reason:
            payload["fallback_reason"] = fallback_reason
        return payload


class QueryUnderstandingService:
    """Analyze a query and select the current MVP search route."""

    def __init__(self, metadata_db: Any):
        self.metadata_db = metadata_db
        self.normalizer = QueryNormalizer()
        self.author_matcher = AuthorMatcher(metadata_db)
        self.query_corrector = PhraseAwareQueryCorrector(metadata_db)

    def analyze(self, query: str) -> QueryUnderstandingResult:
        normalized = self.normalizer.normalize(query)
        if not normalized.is_valid:
            return QueryUnderstandingResult(
                original_query=normalized.original_query,
                normalized_query=normalized.normalized_query,
                intent="invalid",
                route="none",
                reason="empty_query",
            )

        author_match = self.author_matcher.match(normalized.normalized_query)
        if author_match["is_author"]:
            return QueryUnderstandingResult(
                original_query=normalized.original_query,
                normalized_query=normalized.normalized_query,
                intent="author_name",
                route="metadata_author",
                matched_author=author_match["matched_author"],
                confidence=author_match["confidence"],
                candidates=author_match["candidates"],
                reason=author_match["reason"],
            )

        if (
            author_match["candidates"]
            and float(author_match["confidence"]) >= AuthorMatcher.MIDDLE_CONFIDENCE_THRESHOLD
            and float(author_match["confidence"]) < AuthorMatcher.HIGH_CONFIDENCE_THRESHOLD
        ):
            return QueryUnderstandingResult(
                original_query=normalized.original_query,
                normalized_query=normalized.normalized_query,
                intent="author_name",
                route="author_suggestion",
                suggested_author=author_match["candidates"][0]["name"],
                confidence=author_match["confidence"],
                candidates=author_match["candidates"],
                reason=author_match["reason"],
            )

        correction = self.query_corrector.correct(normalized.normalized_query)
        corrected_query = correction["corrected_query"] if correction["auto_apply"] else None
        semantic_confidence = max(float(author_match["confidence"]), float(correction["confidence"]))
        semantic_candidates = correction["candidates"] or author_match["candidates"]
        semantic_reason = correction["reason"] if correction["reason"] != "no_query_term_candidates" else author_match["reason"]

        return QueryUnderstandingResult(
            original_query=normalized.original_query,
            normalized_query=normalized.normalized_query,
            intent="semantic_search",
            route="vector",
            corrected_query=corrected_query,
            confidence=semantic_confidence,
            candidates=semantic_candidates,
            corrections=correction.get("corrections", []),
            reason=semantic_reason,
        )
