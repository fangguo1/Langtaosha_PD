"""Deterministic query phrase analysis for keyword-aware retrieval.

This module implements the Step 2 MVP for query-side keyword matching:
normalize a raw query, extract atomic phrase candidates, validate them against
a keyword/alias lexicon, and classify the query into a retrieval policy.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


EDGE_PUNCTUATION = " \t\r\n\"'`!?;:()[]{}<>"

CONNECTORS = {
    "and",
    "or",
    "in",
    "with",
    "for",
    "of",
    "via",
    "between",
    "among",
    "using",
    "by",
    "on",
    "to",
    "from",
}

STOPWORDS = CONNECTORS | {
    "a",
    "an",
    "the",
    "this",
    "that",
    "these",
    "those",
}

GENERIC_HEADS = {
    "interaction",
    "interactions",
    "regulation",
    "regulations",
    "structure",
    "structures",
    "function",
    "functions",
    "mechanism",
    "mechanisms",
    "effect",
    "effects",
    "role",
    "roles",
    "model",
    "models",
    "disease",
    "diseases",
    "disorder",
    "disorders",
}

STRONG_MATCH_TYPES = {
    "exact",
    "normalized",
    "alias",
    "hyphen_space_variant",
    "plural_variant",
}

MATCH_CONFIDENCE = {
    "exact": 1.0,
    "normalized": 1.0,
    "alias": 0.95,
    "hyphen_space_variant": 0.95,
    "plural_variant": 0.95,
    "none": 0.0,
}

GREEK_LETTER_MAP = {
    "α": "alpha",
    "β": "beta",
    "γ": "gamma",
    "δ": "delta",
    "ε": "epsilon",
    "κ": "kappa",
    "λ": "lambda",
    "μ": "mu",
    "π": "pi",
    "τ": "tau",
}


@dataclass
class NormalizedQuery:
    original_query: str
    display_query: str
    normalized_query: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PhraseCandidate:
    text: str
    normalized_text: str
    kind: str
    start: int = 0
    end: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PhraseMatch:
    text: str
    normalized_text: str
    canonical: Optional[str]
    match_type: str
    is_matched: bool
    confidence: float
    doc_count: int = 0
    variant_count: int = 0
    matched_phrase_count: int = 0
    is_generic: bool = False
    kind: str = "connector_split"
    start: int = 0
    end: int = 0

    @property
    def is_strong(self) -> bool:
        return self.is_matched and self.match_type in STRONG_MATCH_TYPES

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AtomicPhrase:
    text: str
    canonical: str
    match_type: str
    doc_count: int
    variant_count: int
    confidence: float
    is_generic: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class QueryPhraseAnalysisResult:
    original_query: str
    normalized_query: str
    query_type: str
    atomic_phrases: List[AtomicPhrase]
    full_query_match: PhraseMatch
    phrase_integrity_keep: bool
    coverage_required_count: int
    retrieval_policy: str
    warnings: List[str]

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["atomic_phrases"] = [phrase.to_dict() for phrase in self.atomic_phrases]
        payload["full_query_match"] = self.full_query_match.to_dict()
        return payload


class QueryPhraseNormalizer:
    """Normalize queries and phrase candidates without semantic rewriting."""

    DASH_PATTERN = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2212]")

    def normalize_query(self, query: str) -> NormalizedQuery:
        original_query = "" if query is None else str(query)
        display_query = self._clean_display_text(original_query)
        normalized_query = self.normalize_phrase(display_query)
        return NormalizedQuery(
            original_query=original_query,
            display_query=display_query,
            normalized_query=normalized_query,
        )

    def normalize_phrase(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKC", "" if text is None else str(text))
        normalized = self.DASH_PATTERN.sub("-", normalized)
        for letter, replacement in GREEK_LETTER_MAP.items():
            normalized = normalized.replace(letter, replacement)
            normalized = normalized.replace(letter.upper(), replacement)
        normalized = normalized.strip(EDGE_PUNCTUATION + ",.")
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.lower().strip()

    def generate_variants(self, text: str) -> List[Tuple[str, str]]:
        """Return phrase variants paired with the match type they represent."""

        normalized = self.normalize_phrase(text)
        variants: List[Tuple[str, str]] = []
        seen: Set[Tuple[str, str]] = set()

        def add(phrase: str, match_type: str) -> None:
            phrase = self.normalize_phrase(phrase)
            if not phrase or phrase == normalized:
                return
            key = (phrase, match_type)
            if key not in seen:
                seen.add(key)
                variants.append(key)

        hyphen_space = self._hyphen_space_variants(normalized)
        for phrase in hyphen_space:
            add(phrase, "hyphen_space_variant")

        plural_inputs = {normalized} | hyphen_space
        for phrase in plural_inputs:
            for variant in self._head_plural_variants(phrase):
                if variant in hyphen_space or phrase in hyphen_space:
                    add(variant, "hyphen_space_variant")
                else:
                    add(variant, "plural_variant")

        return variants

    @staticmethod
    def _clean_display_text(text: str) -> str:
        cleaned = "" if text is None else str(text)
        cleaned = QueryPhraseNormalizer.DASH_PATTERN.sub("-", cleaned)
        cleaned = cleaned.strip(EDGE_PUNCTUATION + ",.")
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    @staticmethod
    def _hyphen_space_variants(phrase: str) -> Set[str]:
        variants: Set[str] = set()
        if "-" in phrase:
            variants.add(re.sub(r"\s+", " ", phrase.replace("-", " ")).strip())

        tokens = phrase.split()
        if len(tokens) >= 2:
            for index in range(len(tokens) - 1):
                hyphenated = list(tokens)
                hyphenated[index : index + 2] = [f"{tokens[index]}-{tokens[index + 1]}"]
                variants.add(" ".join(hyphenated))

        return {variant for variant in variants if variant and variant != phrase}

    @staticmethod
    def _head_plural_variants(phrase: str) -> Set[str]:
        tokens = phrase.split()
        if not tokens:
            return set()

        head = tokens[-1]
        variants = {phrase}
        if head.endswith("s") and len(head) > 3:
            variants.add(" ".join(tokens[:-1] + [head[:-1]]))
        else:
            variants.add(" ".join(tokens[:-1] + [head + "s"]))
        return {variant for variant in variants if variant != phrase}


class AtomicPhraseExtractor:
    """Extract full-query, scispaCy, and connector-split phrase candidates."""

    def __init__(self, normalizer: Optional[QueryPhraseNormalizer] = None):
        self.normalizer = normalizer or QueryPhraseNormalizer()
        connector_pattern = "|".join(re.escape(word) for word in sorted(CONNECTORS, key=len, reverse=True))
        self._connector_re = re.compile(rf"\b(?:{connector_pattern})\b", re.IGNORECASE)

    def extract(self, normalized_query: str, scispacy_doc: Optional[Any] = None) -> List[PhraseCandidate]:
        query = self.normalizer.normalize_phrase(normalized_query)
        if not query:
            return []

        candidates: List[PhraseCandidate] = [
            PhraseCandidate(
                text=query,
                normalized_text=query,
                kind="full_query",
                start=0,
                end=len(query),
            )
        ]

        candidates.extend(self._scispacy_candidates(query, scispacy_doc))
        candidates.extend(self._connector_split_candidates(query))
        return self._dedupe(candidates)

    def _scispacy_candidates(self, query: str, doc: Optional[Any]) -> List[PhraseCandidate]:
        if doc is None:
            return []

        candidates: List[PhraseCandidate] = []
        for ent in getattr(doc, "ents", []) or []:
            candidate = self._candidate_from_offsets(query, ent.start_char, ent.end_char, "scispacy_entity")
            if candidate is not None:
                candidates.append(candidate)

        try:
            noun_chunks = list(doc.noun_chunks)
        except Exception:
            noun_chunks = []

        for chunk in noun_chunks:
            candidate = self._candidate_from_offsets(query, chunk.start_char, chunk.end_char, "scispacy_noun_chunk")
            if candidate is not None:
                candidates.append(candidate)
        return candidates

    def _connector_split_candidates(self, query: str) -> List[PhraseCandidate]:
        candidates: List[PhraseCandidate] = []
        start = 0
        for match in self._connector_re.finditer(query):
            candidates.append(self._candidate_from_offsets(query, start, match.start(), "connector_split"))
            start = match.end()
        candidates.append(self._candidate_from_offsets(query, start, len(query), "connector_split"))
        return [candidate for candidate in candidates if candidate is not None]

    def _candidate_from_offsets(
        self,
        query: str,
        start: int,
        end: int,
        kind: str,
    ) -> Optional[PhraseCandidate]:
        start, end = self._trim_offsets(query, start, end)
        if start >= end:
            return None

        text = query[start:end]
        normalized = self.normalizer.normalize_phrase(text)
        if not self._is_usable_phrase(normalized, kind=kind):
            return None

        return PhraseCandidate(
            text=text,
            normalized_text=normalized,
            kind=kind,
            start=start,
            end=end,
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

    @staticmethod
    def _is_usable_phrase(normalized: str, kind: str) -> bool:
        if not normalized:
            return False
        if normalized in STOPWORDS:
            return False
        if kind != "full_query" and len(normalized) < 3:
            return False
        return True

    @staticmethod
    def _dedupe(candidates: Sequence[PhraseCandidate]) -> List[PhraseCandidate]:
        by_text: Dict[str, PhraseCandidate] = {}
        for candidate in candidates:
            existing = by_text.get(candidate.normalized_text)
            if existing is None:
                by_text[candidate.normalized_text] = candidate
                continue
            if existing.kind != "full_query" and candidate.kind == "full_query":
                by_text[candidate.normalized_text] = candidate
            elif existing.kind == candidate.kind and (candidate.end - candidate.start) > (existing.end - existing.start):
                by_text[candidate.normalized_text] = candidate
        return list(by_text.values())


class PhraseLexicon:
    """Minimal lexicon interface for phrase validation."""

    def lookup(self, normalized_phrase: str) -> Optional[Dict[str, Any]]:
        return None

    def lookup_many(self, normalized_phrases: List[str]) -> Dict[str, Dict[str, Any]]:
        return {
            phrase: record
            for phrase in normalized_phrases
            for record in [self.lookup(phrase)]
            if record is not None
        }

    def lookup_alias(self, normalized_phrase: str) -> Optional[Dict[str, Any]]:
        return None


class InMemoryPhraseLexicon(PhraseLexicon):
    """Simple lexicon implementation for unit tests and local prototypes."""

    def __init__(
        self,
        entries: Optional[Dict[str, Dict[str, Any]]] = None,
        aliases: Optional[Dict[str, Any]] = None,
        normalizer: Optional[QueryPhraseNormalizer] = None,
    ):
        self.normalizer = normalizer or QueryPhraseNormalizer()
        self.entries: Dict[str, Dict[str, Any]] = {}
        for phrase, record in (entries or {}).items():
            normalized = self.normalizer.normalize_phrase(phrase)
            self.entries[normalized] = self._record(normalized, record)

        self.aliases: Dict[str, Dict[str, Any]] = {}
        for alias, target in (aliases or {}).items():
            normalized_alias = self.normalizer.normalize_phrase(alias)
            if isinstance(target, str):
                normalized_target = self.normalizer.normalize_phrase(target)
                target_record = self.entries.get(normalized_target)
                if target_record is None:
                    target_record = self._record(normalized_target, {"canonical": target})
                    self.entries[normalized_target] = target_record
                self.aliases[normalized_alias] = dict(target_record)
            elif isinstance(target, dict):
                self.aliases[normalized_alias] = self._record(normalized_alias, target)

    def lookup(self, normalized_phrase: str) -> Optional[Dict[str, Any]]:
        record = self.entries.get(self.normalizer.normalize_phrase(normalized_phrase))
        return dict(record) if record is not None else None

    def lookup_alias(self, normalized_phrase: str) -> Optional[Dict[str, Any]]:
        record = self.aliases.get(self.normalizer.normalize_phrase(normalized_phrase))
        return dict(record) if record is not None else None

    @staticmethod
    def _record(normalized: str, record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record)
        payload.setdefault("canonical", record.get("canonical") or normalized)
        payload.setdefault("doc_count", 0)
        payload.setdefault("variant_count", 0)
        payload.setdefault("matched_phrase_count", payload["doc_count"])
        return payload


class MetadataDBPhraseLexicon(PhraseLexicon):
    """Phrase lexicon backed by ``paper_keywords`` in MetadataDB.

    The lookup is intentionally exact at the canonical phrase level: a phrase
    must exist as a keyword after lowercase normalization. Related keyword rows
    that contain the phrase are only used as broadness statistics.
    """

    def __init__(
        self,
        metadata_db: Any,
        paper_source_names: Optional[Sequence[str]] = None,
        keyword_sources: Optional[Sequence[str]] = None,
        normalizer: Optional[QueryPhraseNormalizer] = None,
    ):
        self.metadata_db = metadata_db
        self.normalizer = normalizer or QueryPhraseNormalizer()
        if paper_source_names is None:
            paper_source_names = getattr(metadata_db, "default_sources", None)
        self.paper_source_names = list(paper_source_names or [])
        self.keyword_sources = list(keyword_sources or [])
        self._lookup_cache: Dict[str, Optional[Dict[str, Any]]] = {}

    def lookup(self, normalized_phrase: str) -> Optional[Dict[str, Any]]:
        normalized = self.normalizer.normalize_phrase(normalized_phrase)
        if not normalized:
            return None
        if normalized not in self._lookup_cache:
            self._lookup_cache[normalized] = self._lookup_db(normalized)
        record = self._lookup_cache[normalized]
        return dict(record) if record is not None else None

    def lookup_alias(self, normalized_phrase: str) -> Optional[Dict[str, Any]]:
        return None

    def _lookup_db(self, normalized_phrase: str) -> Optional[Dict[str, Any]]:
        from sqlalchemy import text

        params: Dict[str, Any] = {
            "normalized_phrase": normalized_phrase,
            "contains_phrase": f"%{normalized_phrase}%",
        }
        filters = [
            "(lower(pk.keyword) = :normalized_phrase OR lower(pk.keyword) LIKE :contains_phrase)"
        ]

        keyword_source_filter = self._in_filter("pk.source", "keyword_source", self.keyword_sources, params)
        if keyword_source_filter:
            filters.append(keyword_source_filter)

        paper_source_filter = self._paper_source_exists_filter(params)
        if paper_source_filter:
            filters.append(paper_source_filter)

        where_clause = " AND ".join(filters)
        sql = text(
            f"""
            WITH scoped_keywords AS (
                SELECT
                    pk.paper_id,
                    pk.keyword,
                    lower(pk.keyword) AS normalized_keyword,
                    pk.source,
                    COALESCE(pk.weight, 1.0) AS weight
                FROM paper_keywords pk
                WHERE {where_clause}
            ),
            exact_keywords AS (
                SELECT *
                FROM scoped_keywords
                WHERE normalized_keyword = :normalized_phrase
            ),
            display_keyword AS (
                SELECT keyword
                FROM exact_keywords
                GROUP BY keyword
                ORDER BY COUNT(DISTINCT paper_id) DESC, AVG(weight) DESC, keyword ASC
                LIMIT 1
            )
            SELECT
                (SELECT keyword FROM display_keyword) AS canonical,
                (SELECT COUNT(DISTINCT paper_id) FROM exact_keywords) AS doc_count,
                (SELECT COUNT(DISTINCT normalized_keyword) FROM scoped_keywords) AS variant_count,
                (SELECT COUNT(*) FROM scoped_keywords) AS matched_phrase_count,
                (SELECT COUNT(DISTINCT paper_id) FROM scoped_keywords) AS related_doc_count
            """
        )

        with self.metadata_db.engine.connect() as conn:
            row = conn.execute(sql, params).mappings().fetchone()

        if not row or int(row["doc_count"] or 0) <= 0:
            return None

        canonical = row["canonical"] or normalized_phrase
        return {
            "canonical": canonical,
            "doc_count": int(row["doc_count"] or 0),
            "variant_count": int(row["variant_count"] or 0),
            "matched_phrase_count": int(row["matched_phrase_count"] or 0),
            "related_doc_count": int(row["related_doc_count"] or 0),
            "source": "metadata_db.paper_keywords",
        }

    @staticmethod
    def _in_filter(
        column: str,
        prefix: str,
        values: Sequence[str],
        params: Dict[str, Any],
    ) -> str:
        if not values:
            return ""
        placeholders = []
        for index, value in enumerate(values):
            key = f"{prefix}_{index}"
            placeholders.append(f":{key}")
            params[key] = value
        return f"{column} IN ({', '.join(placeholders)})"

    def _paper_source_exists_filter(self, params: Dict[str, Any]) -> str:
        if not self.paper_source_names:
            return ""

        placeholders = []
        for index, source_name in enumerate(self.paper_source_names):
            key = f"paper_source_{index}"
            placeholders.append(f":{key}")
            params[key] = source_name

        return (
            "EXISTS ("
            "SELECT 1 FROM paper_sources ps "
            "WHERE ps.paper_id = pk.paper_id "
            f"AND ps.source_name IN ({', '.join(placeholders)})"
            ")"
        )


class PhraseLexiconMatcher:
    """Validate phrase candidates against exact, alias, and variant matches."""

    def __init__(
        self,
        lexicon: PhraseLexicon,
        normalizer: Optional[QueryPhraseNormalizer] = None,
    ):
        self.lexicon = lexicon
        self.normalizer = normalizer or QueryPhraseNormalizer()

    def match(self, candidates: Sequence[PhraseCandidate]) -> List[PhraseMatch]:
        return [self._match_candidate(candidate) for candidate in candidates]

    def _match_candidate(self, candidate: PhraseCandidate) -> PhraseMatch:
        normalized = self.normalizer.normalize_phrase(candidate.normalized_text)

        direct = self.lexicon.lookup(normalized)
        if direct is not None:
            return self._to_match(candidate, direct, "exact")

        alias = self.lexicon.lookup_alias(normalized)
        if alias is not None:
            return self._to_match(candidate, alias, "alias")

        for variant, match_type in self.normalizer.generate_variants(normalized):
            record = self.lexicon.lookup(variant)
            if record is not None:
                return self._to_match(candidate, record, match_type)
            alias_record = self.lexicon.lookup_alias(variant)
            if alias_record is not None:
                return self._to_match(candidate, alias_record, "alias")

        return PhraseMatch(
            text=candidate.text,
            normalized_text=normalized,
            canonical=None,
            match_type="none",
            is_matched=False,
            confidence=0.0,
            is_generic=self._is_generic(normalized),
            kind=candidate.kind,
            start=candidate.start,
            end=candidate.end,
        )

    def _to_match(
        self,
        candidate: PhraseCandidate,
        record: Dict[str, Any],
        match_type: str,
    ) -> PhraseMatch:
        normalized = self.normalizer.normalize_phrase(candidate.normalized_text)
        canonical = str(record.get("canonical") or candidate.text)
        return PhraseMatch(
            text=candidate.text,
            normalized_text=normalized,
            canonical=canonical,
            match_type=match_type,
            is_matched=True,
            confidence=float(record.get("confidence") or MATCH_CONFIDENCE[match_type]),
            doc_count=int(record.get("doc_count") or 0),
            variant_count=int(record.get("variant_count") or 0),
            matched_phrase_count=int(record.get("matched_phrase_count") or record.get("doc_count") or 0),
            is_generic=self._is_generic(normalized) or self._is_generic(self.normalizer.normalize_phrase(canonical)),
            kind=candidate.kind,
            start=candidate.start,
            end=candidate.end,
        )

    @staticmethod
    def _is_generic(normalized: str) -> bool:
        if not normalized:
            return False
        tokens = normalized.split()
        if normalized in GENERIC_HEADS:
            return True
        return len(tokens) == 1 and tokens[-1] in GENERIC_HEADS


class QueryTypeClassifier:
    """Classify query phrase matches into the Step 2 query types."""

    def __init__(
        self,
        broad_doc_count_threshold: int = 100,
        broad_variant_threshold: int = 20,
        broad_phrase_threshold: int = 50,
    ):
        self.broad_doc_count_threshold = broad_doc_count_threshold
        self.broad_variant_threshold = broad_variant_threshold
        self.broad_phrase_threshold = broad_phrase_threshold

    def classify(
        self,
        original_query: str,
        normalized_query: str,
        matches: Sequence[PhraseMatch],
    ) -> QueryPhraseAnalysisResult:
        full_query_match = self._full_query_match(normalized_query, matches)
        phrase_matches = [match for match in matches if match.kind != "full_query"]
        selected_phrase_matches = self._select_non_overlapping_matches(
            [match for match in phrase_matches if match.is_matched]
        )

        warnings: List[str] = []
        if full_query_match.is_strong:
            atomic_phrases = [self._atomic_phrase(full_query_match)]
            if self.is_broad(full_query_match):
                query_type = "broad_concept"
                retrieval_policy = "diversified"
            else:
                query_type = "atomic_phrase"
                retrieval_policy = "phrase_strict"
            return self._result(
                original_query,
                normalized_query,
                query_type,
                atomic_phrases,
                full_query_match,
                phrase_integrity_keep=True,
                retrieval_policy=retrieval_policy,
                warnings=warnings,
            )

        if not full_query_match.is_matched:
            warnings.append("full_query_unmatched")

        if self._looks_ambiguous(full_query_match, selected_phrase_matches):
            if any(match.is_generic for match in selected_phrase_matches):
                warnings.append("generic_head")
            warnings.append("avoid_auto_rewrite")
            return self._result(
                original_query,
                normalized_query,
                "ambiguous",
                [self._atomic_phrase(match) for match in selected_phrase_matches],
                full_query_match,
                phrase_integrity_keep=False,
                retrieval_policy="soft_rerank" if selected_phrase_matches else "semantic_fallback",
                warnings=warnings,
            )

        if len(selected_phrase_matches) > 1:
            return self._result(
                original_query,
                normalized_query,
                "compositional_relation",
                [self._atomic_phrase(match) for match in selected_phrase_matches],
                full_query_match,
                phrase_integrity_keep=True,
                retrieval_policy="coverage_rerank",
                warnings=warnings,
            )

        if len(selected_phrase_matches) == 1:
            match = selected_phrase_matches[0]
            atomic_phrases = [self._atomic_phrase(match)]
            if self.is_broad(match):
                query_type = "broad_concept"
                retrieval_policy = "diversified"
            else:
                query_type = "atomic_phrase"
                retrieval_policy = "phrase_strict"
            return self._result(
                original_query,
                normalized_query,
                query_type,
                atomic_phrases,
                full_query_match,
                phrase_integrity_keep=True,
                retrieval_policy=retrieval_policy,
                warnings=warnings,
            )

        warnings.append("avoid_auto_rewrite")
        return self._result(
            original_query,
            normalized_query,
            "ambiguous",
            [],
            full_query_match,
            phrase_integrity_keep=False,
            retrieval_policy="semantic_fallback",
            warnings=warnings,
        )

    def is_broad(self, match: PhraseMatch) -> bool:
        return (
            match.doc_count >= self.broad_doc_count_threshold
            or match.variant_count >= self.broad_variant_threshold
            or match.matched_phrase_count >= self.broad_phrase_threshold
        )

    @staticmethod
    def _full_query_match(normalized_query: str, matches: Sequence[PhraseMatch]) -> PhraseMatch:
        for match in matches:
            if match.kind == "full_query":
                return match
        return PhraseMatch(
            text=normalized_query,
            normalized_text=normalized_query,
            canonical=None,
            match_type="none",
            is_matched=False,
            confidence=0.0,
            kind="full_query",
            start=0,
            end=len(normalized_query),
        )

    def _looks_ambiguous(
        self,
        full_query_match: PhraseMatch,
        phrase_matches: Sequence[PhraseMatch],
    ) -> bool:
        if full_query_match.is_strong:
            return False
        if not phrase_matches:
            return True

        generic_matches = [match for match in phrase_matches if match.is_generic]
        if not generic_matches:
            return False

        non_generic_matches = [match for match in phrase_matches if not match.is_generic]
        if not non_generic_matches:
            return True

        if len(phrase_matches) <= 2 and all(self.is_broad(match) for match in non_generic_matches):
            return True

        return False

    @staticmethod
    def _select_non_overlapping_matches(matches: Sequence[PhraseMatch]) -> List[PhraseMatch]:
        sorted_matches = sorted(
            matches,
            key=lambda match: (
                -match.confidence,
                -(match.end - match.start),
                match.start,
                match.normalized_text,
            ),
        )
        selected: List[PhraseMatch] = []
        occupied: List[Tuple[int, int]] = []
        for match in sorted_matches:
            if match.start == match.end == 0:
                selected.append(match)
                continue
            if any(not (match.end <= start or match.start >= end) for start, end in occupied):
                continue
            selected.append(match)
            occupied.append((match.start, match.end))
        return sorted(selected, key=lambda match: (match.start, match.end, match.normalized_text))

    @staticmethod
    def _atomic_phrase(match: PhraseMatch) -> AtomicPhrase:
        return AtomicPhrase(
            text=match.text,
            canonical=match.canonical or match.normalized_text,
            match_type=match.match_type,
            doc_count=match.doc_count,
            variant_count=match.variant_count,
            confidence=match.confidence,
            is_generic=match.is_generic,
        )

    @staticmethod
    def _result(
        original_query: str,
        normalized_query: str,
        query_type: str,
        atomic_phrases: List[AtomicPhrase],
        full_query_match: PhraseMatch,
        phrase_integrity_keep: bool,
        retrieval_policy: str,
        warnings: List[str],
    ) -> QueryPhraseAnalysisResult:
        return QueryPhraseAnalysisResult(
            original_query=original_query,
            normalized_query=normalized_query,
            query_type=query_type,
            atomic_phrases=atomic_phrases,
            full_query_match=full_query_match,
            phrase_integrity_keep=phrase_integrity_keep,
            coverage_required_count=len(atomic_phrases) if query_type == "compositional_relation" else min(len(atomic_phrases), 1),
            retrieval_policy=retrieval_policy,
            warnings=list(dict.fromkeys(warnings)),
        )


class QueryPhraseAnalyzer:
    """Facade for Step 2 query phrase analysis.

    ``analyze()`` runs the optional scispaCy pipeline automatically. Lower-level
    callers that use ``extractor.extract(...)`` directly must pass the
    ``scispacy_doc`` themselves if they want scispaCy entity / noun chunk spans.
    """

    def __init__(
        self,
        lexicon: Optional[PhraseLexicon] = None,
        scispacy_pipeline: Optional[Any] = None,
        normalizer: Optional[QueryPhraseNormalizer] = None,
        extractor: Optional[AtomicPhraseExtractor] = None,
        matcher: Optional[PhraseLexiconMatcher] = None,
        classifier: Optional[QueryTypeClassifier] = None,
    ):
        self.normalizer = normalizer or QueryPhraseNormalizer()
        self.lexicon = lexicon or InMemoryPhraseLexicon(normalizer=self.normalizer)
        self.scispacy_pipeline = scispacy_pipeline
        self.extractor = extractor or AtomicPhraseExtractor(normalizer=self.normalizer)
        self.matcher = matcher or PhraseLexiconMatcher(self.lexicon, normalizer=self.normalizer)
        self.classifier = classifier or QueryTypeClassifier()

    def analyze(self, query: str, scispacy_doc: Optional[Any] = None) -> QueryPhraseAnalysisResult:
        normalized = self.normalizer.normalize_query(query)
        doc = scispacy_doc
        if doc is None and self.scispacy_pipeline is not None and normalized.normalized_query:
            doc = self.scispacy_pipeline(normalized.normalized_query)

        candidates = self.extractor.extract(normalized.normalized_query, scispacy_doc=doc)

        matches = self.matcher.match(candidates)
        return self.classifier.classify(
            original_query=normalized.original_query,
            normalized_query=normalized.normalized_query,
            matches=matches,
        )


def normalize_query(query: str) -> Dict[str, Any]:
    """Normalize a query for query phrase analysis."""

    return QueryPhraseNormalizer().normalize_query(query).to_dict()


def extract_atomic_phrase_candidates(query: str, scispacy_doc: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Return atomic phrase candidates for a query."""

    normalizer = QueryPhraseNormalizer()
    normalized = normalizer.normalize_query(query)
    return [
        candidate.to_dict()
        for candidate in AtomicPhraseExtractor(normalizer=normalizer).extract(
            normalized.normalized_query,
            scispacy_doc=scispacy_doc,
        )
    ]


def lookup_phrase_candidates(
    candidates: Iterable[Any],
    keyword_lexicon: PhraseLexicon,
) -> List[Dict[str, Any]]:
    """Validate phrase candidates against a keyword lexicon."""

    phrase_candidates = [_coerce_candidate(candidate) for candidate in candidates]
    return [
        match.to_dict()
        for match in PhraseLexiconMatcher(keyword_lexicon).match(phrase_candidates)
    ]


def classify_query_type(
    full_query_match: Any,
    atomic_phrase_matches: Sequence[Any],
    original_query: str = "",
    normalized_query: str = "",
) -> Dict[str, Any]:
    """Classify already matched phrases into a query phrase result."""

    full_match = _coerce_match(full_query_match)
    phrase_matches = [_coerce_match(match) for match in atomic_phrase_matches]
    matches = [full_match] + phrase_matches
    if not normalized_query:
        normalized_query = full_match.normalized_text
    if not original_query:
        original_query = normalized_query
    return QueryTypeClassifier().classify(original_query, normalized_query, matches).to_dict()


def _coerce_candidate(candidate: Any) -> PhraseCandidate:
    if isinstance(candidate, PhraseCandidate):
        return candidate
    if isinstance(candidate, dict):
        return PhraseCandidate(
            text=str(candidate.get("text") or ""),
            normalized_text=str(candidate.get("normalized_text") or candidate.get("text") or ""),
            kind=str(candidate.get("kind") or "connector_split"),
            start=int(candidate.get("start") or 0),
            end=int(candidate.get("end") or 0),
        )
    raise TypeError(f"Unsupported phrase candidate type: {type(candidate)!r}")


def _coerce_match(match: Any) -> PhraseMatch:
    if isinstance(match, PhraseMatch):
        return match
    if isinstance(match, dict):
        return PhraseMatch(
            text=str(match.get("text") or ""),
            normalized_text=str(match.get("normalized_text") or match.get("text") or ""),
            canonical=match.get("canonical"),
            match_type=str(match.get("match_type") or "none"),
            is_matched=bool(match.get("is_matched")),
            confidence=float(match.get("confidence") or 0.0),
            doc_count=int(match.get("doc_count") or 0),
            variant_count=int(match.get("variant_count") or 0),
            matched_phrase_count=int(match.get("matched_phrase_count") or 0),
            is_generic=bool(match.get("is_generic")),
            kind=str(match.get("kind") or "connector_split"),
            start=int(match.get("start") or 0),
            end=int(match.get("end") or 0),
        )
    raise TypeError(f"Unsupported phrase match type: {type(match)!r}")
