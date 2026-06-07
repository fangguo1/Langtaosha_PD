"""Keyword enrichment services and audit-only keyword pipeline."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
import html
import logging
import re
from pathlib import Path
import unicodedata
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    from config.config_loader import load_config_from_yaml
except Exception:  # pragma: no cover - keeps audit utilities usable without config deps.
    load_config_from_yaml = None  # type: ignore[assignment]


DEFAULT_MODEL_NAMES = ("en_core_sci_lg", "en_ner_bionlp13cg_md")
DEFAULT_MODEL_NAME = "+".join(DEFAULT_MODEL_NAMES)
DEFAULT_KEYWORD_SOURCES = {
    "en_core_sci_lg": "scispacy-en_core_sci_lg-generated",
    "en_ner_bionlp13cg_md": "scispacy-en_ner_bionlp13cg_md-generated",
}
DEFAULT_KEYWORD_SOURCE = "scispacy-generated"

ALLOWED_KEYWORD_TYPES = {
    "domain",
    "concept",
    "method",
    "task",
    "disease",
    "gene",
    "protein",
    "model",
    "dataset",
    "metric",
    "organism",
    "chemical",
}

SCISPACY_LABEL_TO_KEYWORD_TYPE = {
    "CHEMICAL": "chemical",
    "DISEASE": "disease",
    "GENE": "gene",
    "GGP": "gene",
    "GENE_OR_GENE_PRODUCT": "gene",
    "ORGANISM": "organism",
    "ORGANISM_SUBDIVISION": "organism",
    "ORGANISM_SUBSTANCE": "organism",
    "ORGAN": "concept",
    "CELL": "concept",
    "CELL_LINE": "concept",
    "CELL_TYPE": "concept",
    "CELLULAR_COMPONENT": "concept",
    "DEVELOPING_ANATOMICAL_STRUCTURE": "concept",
    "IMMATERIAL_ANATOMICAL_ENTITY": "concept",
    "ANATOMICAL_SYSTEM": "concept",
    "DNA": "gene",
    "RNA": "gene",
    "PROTEIN": "protein",
    "AMINO_ACID": "chemical",
    "SIMPLE_CHEMICAL": "chemical",
    "CANCER": "disease",
    "PATHOLOGICAL_FORMATION": "disease",
    "MULTI_TISSUE_STRUCTURE": "concept",
    "TISSUE": "concept",
    "ENTITY": "concept",
}

GENERIC_KEYWORDS = {
    "study",
    "analysis",
    "result",
    "results",
    "paper",
    "research",
    "method",
    "methods",
    "data",
    "approach",
}

PIPELINE_V2_SOURCE = "keyword-pipeline-v2"

DASH_PATTERN = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2212]")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
HTML_ALT_PATTERN = re.compile(r"\bALT\s*=\s*\"[^\"]*\"?", re.IGNORECASE)

HARD_DROP_KEYWORDS = GENERIC_KEYWORDS | {
    "finding",
    "findings",
    "effect",
    "effects",
    "role",
    "roles",
    "mechanism",
    "mechanisms",
    "model",
    "models",
    "change",
    "changes",
    "feature",
    "features",
    "level",
    "levels",
    "function",
    "functions",
    "development",
    "evolution",
    "activation",
    "adaptation",
    "detection",
    "identification",
    "formation",
    "influence",
    "inhibition",
    "binding",
    "complex",
    "complexes",
    "lineage",
    "variant",
    "variants",
    "trait",
    "traits",
    "exposure",
    "increase",
    "induced",
    "year",
    "years",
    "age",
    "dynamic",
    "efficacy",
    "dataset",
    "datasets",
    "potential",
    "evidence",
    "framework",
    "performance",
    "information",
    "identified",
    "accurate",
    "early",
    "diverse",
    "diversity",
    "production",
    "response",
    "responses",
    "impact",
    "target",
    "treatment",
    "selection",
}

HARD_DROP_PHRASES = {
    "associated with",
    "increased",
    "reduced",
    "functional",
    "biological",
}

NOISE_PATTERNS = (
    re.compile(r"\bALT\s*=", re.IGNORECASE),
    re.compile(r"<[^>]+>"),
    re.compile(r"^figure\s+\d+", re.IGNORECASE),
    re.compile(r"^fig\.?\s*\d+", re.IGNORECASE),
    re.compile(r"^[\"']?figure\s+\d+", re.IGNORECASE),
)

def normalize_keyword_text(value: str) -> str:
    """Normalize keyword text for matching while preserving semantic symbols."""
    normalized = unicodedata.normalize("NFKC", "" if value is None else str(value))
    normalized = DASH_PATTERN.sub("-", normalized)
    normalized = html.unescape(normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip(" \t\r\n.,;:()[]{}").lower()


def clean_display_keyword(value: str) -> str:
    """Clean keyword text for display without semantic recasing."""
    cleaned = unicodedata.normalize("NFKC", "" if value is None else str(value))
    cleaned = DASH_PATTERN.sub("-", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" \t\r\n.,;:()[]{}")


@dataclass
class KeywordExtractionInput:
    """Input passed through the audit-only keyword pipeline."""

    text: str
    title: str = ""
    abstract: str = ""
    source_keywords: List[Dict[str, Any]] = field(default_factory=list)
    paper_id: Optional[int] = None


@dataclass
class KeywordCandidate:
    """A candidate keyword anchored in source text or source metadata."""

    text: str
    normalized_text: str
    keyword_type: str
    source: str
    extractor_name: str
    start: Optional[int] = None
    end: Optional[int] = None
    canonical: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    semantic_types: List[str] = field(default_factory=list)
    confidence: float = 1.0
    evidence: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_text(
        cls,
        text: str,
        keyword_type: str,
        source: str,
        extractor_name: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        canonical: Optional[str] = None,
        aliases: Optional[Sequence[str]] = None,
        semantic_types: Optional[Sequence[str]] = None,
        confidence: float = 1.0,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> "KeywordCandidate":
        display_text = clean_display_keyword(text)
        return cls(
            text=display_text,
            normalized_text=normalize_keyword_text(display_text),
            keyword_type=keyword_type,
            source=source,
            extractor_name=extractor_name,
            start=start,
            end=end,
            canonical=clean_display_keyword(canonical) if canonical else None,
            aliases=[clean_display_keyword(alias) for alias in aliases or [] if clean_display_keyword(alias)],
            semantic_types=[str(item) for item in semantic_types or [] if item],
            confidence=float(confidence),
            evidence=dict(evidence or {}),
        )

    def copy_with(self, **updates: Any) -> "KeywordCandidate":
        payload = {
            "text": self.text,
            "normalized_text": self.normalized_text,
            "keyword_type": self.keyword_type,
            "source": self.source,
            "extractor_name": self.extractor_name,
            "start": self.start,
            "end": self.end,
            "canonical": self.canonical,
            "aliases": list(self.aliases),
            "semantic_types": list(self.semantic_types),
            "confidence": self.confidence,
            "evidence": dict(self.evidence),
        }
        payload.update(updates)
        return KeywordCandidate(**payload)

    def to_keyword_dict(self, source: Optional[str] = None) -> Dict[str, Any]:
        return {
            "keyword_type": self.keyword_type,
            "keyword": self.canonical or self.text,
            "weight": max(0.0, min(1.0, float(self.confidence))),
            "source": source or self.source,
        }

    def to_audit_dict(self) -> Dict[str, Any]:
        return {
            "text": self.text,
            "normalized_text": self.normalized_text,
            "keyword_type": self.keyword_type,
            "source": self.source,
            "extractor_name": self.extractor_name,
            "start": self.start,
            "end": self.end,
            "canonical": self.canonical,
            "aliases": list(self.aliases),
            "semantic_types": list(self.semantic_types),
            "confidence": self.confidence,
            "evidence": dict(self.evidence),
        }


@dataclass
class KeywordPruneDecision:
    """Decision emitted by KeywordPruner for one candidate."""

    candidate: KeywordCandidate
    action: str
    reasons: List[str] = field(default_factory=list)

    def to_audit_dict(self, paper_id: Optional[int] = None) -> Dict[str, Any]:
        payload = self.candidate.to_audit_dict()
        payload.update(
            {
                "paper_id": paper_id,
                "action": self.action,
                "reasons": list(self.reasons),
            }
        )
        return payload


@dataclass
class KeywordPipelineResult:
    """Audit-only pipeline output."""

    candidates: List[KeywordCandidate] = field(default_factory=list)
    kept: List[KeywordCandidate] = field(default_factory=list)
    dropped: List[KeywordPruneDecision] = field(default_factory=list)
    reviewed: List[KeywordPruneDecision] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self, paper_id: Optional[int] = None) -> Dict[str, Any]:
        return {
            "paper_id": paper_id,
            "candidates": [candidate.to_audit_dict() for candidate in self.candidates],
            "kept": [candidate.to_audit_dict() for candidate in self.kept],
            "dropped": [decision.to_audit_dict(paper_id=paper_id) for decision in self.dropped],
            "reviewed": [decision.to_audit_dict(paper_id=paper_id) for decision in self.reviewed],
            "errors": list(self.errors),
        }


class TextPreprocessor:
    """Conservative text cleanup before candidate generation."""

    def preprocess(
        self,
        title: Optional[str] = None,
        abstract: Optional[str] = None,
        text: Optional[str] = None,
    ) -> KeywordExtractionInput:
        cleaned_title = self.clean(title or "")
        cleaned_abstract = self.clean(abstract or "")
        if text is not None:
            cleaned_text = self.clean(text)
        else:
            cleaned_text = "\n\n".join(part for part in (cleaned_title, cleaned_abstract) if part)
        return KeywordExtractionInput(
            text=cleaned_text,
            title=cleaned_title,
            abstract=cleaned_abstract,
        )

    @classmethod
    def clean(cls, value: str) -> str:
        cleaned = unicodedata.normalize("NFKC", "" if value is None else str(value))
        cleaned = DASH_PATTERN.sub("-", cleaned)
        cleaned = html.unescape(cleaned)
        cleaned = HTML_ALT_PATTERN.sub(" ", cleaned)
        cleaned = HTML_TAG_PATTERN.sub(" ", cleaned)
        cleaned = re.sub(r"\b(?:Figure|Fig\.?|Table)\s+\d+[:.\-]?", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()


class KeywordExtractorBase(ABC):
    """Base class for keyword candidate extractors."""

    name = "keyword_extractor"
    source = "keyword-extractor"

    @abstractmethod
    def extract(self, item: KeywordExtractionInput) -> List[KeywordCandidate]:
        """Extract keyword candidates from prepared input."""


class SourceKeywordExtractor(KeywordExtractorBase):
    """Use source-provided keywords as anchored metadata candidates."""

    name = "source_keywords"
    source = "source-provided"

    def extract(self, item: KeywordExtractionInput) -> List[KeywordCandidate]:
        candidates: List[KeywordCandidate] = []
        for raw in item.source_keywords or []:
            if isinstance(raw, str):
                raw_item: Dict[str, Any] = {"keyword": raw}
            else:
                raw_item = dict(raw or {})
            keyword = raw_item.get("keyword") or raw_item.get("text")
            if not keyword:
                continue
            candidates.append(
                KeywordCandidate.from_text(
                    text=str(keyword),
                    keyword_type=str(raw_item.get("keyword_type") or raw_item.get("type") or "concept"),
                    source=str(raw_item.get("source") or self.source),
                    extractor_name=self.name,
                    confidence=_safe_float(raw_item.get("weight"), default=1.0),
                    evidence={"source_provided": True},
                )
            )
        return candidates


class ScispacyEntityExtractor(KeywordExtractorBase):
    """Extract candidates from ``doc.ents`` for one scispaCy model."""

    def __init__(
        self,
        model_name: str,
        nlp_loader: Callable[[str], Any],
        source: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.model_name = model_name
        self.nlp_loader = nlp_loader
        self.source = source or DEFAULT_KEYWORD_SOURCES.get(model_name) or f"scispacy-{model_name}-generated"
        self.name = name or f"scispacy:{model_name}:entity"

    def extract(self, item: KeywordExtractionInput) -> List[KeywordCandidate]:
        nlp = self.nlp_loader(self.model_name)
        doc = nlp(item.text)
        candidates: List[KeywordCandidate] = []
        for ent in getattr(doc, "ents", []) or []:
            keyword = clean_display_keyword(getattr(ent, "text", ""))
            if not keyword:
                continue
            label = getattr(ent, "label_", "") or "ENTITY"
            candidates.append(
                KeywordCandidate.from_text(
                    text=keyword,
                    keyword_type=KeywordEnrichmentService._keyword_type_for_label(label),
                    source=self.source,
                    extractor_name=self.name,
                    start=getattr(ent, "start_char", None),
                    end=getattr(ent, "end_char", None),
                    evidence={
                        "model_name": self.model_name,
                        "scispacy_label": label,
                        "candidate_kind": "scispacy_entity",
                    },
                )
            )
        return candidates


class ScispacyNounChunkExtractor(KeywordExtractorBase):
    """Extract professional phrase candidates from ``doc.noun_chunks``."""

    def __init__(
        self,
        model_name: str,
        nlp_loader: Callable[[str], Any],
        source: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.model_name = model_name
        self.nlp_loader = nlp_loader
        self.source = source or f"scispacy-{model_name}-noun-chunk"
        self.name = name or f"scispacy:{model_name}:noun_chunk"

    def extract(self, item: KeywordExtractionInput) -> List[KeywordCandidate]:
        nlp = self.nlp_loader(self.model_name)
        doc = nlp(item.text)
        try:
            noun_chunks = list(doc.noun_chunks)
        except Exception:
            noun_chunks = []

        candidates: List[KeywordCandidate] = []
        for chunk in noun_chunks:
            keyword = clean_display_keyword(getattr(chunk, "text", ""))
            if not keyword:
                continue
            candidates.append(
                KeywordCandidate.from_text(
                    text=keyword,
                    keyword_type="concept",
                    source=self.source,
                    extractor_name=self.name,
                    start=getattr(chunk, "start_char", None),
                    end=getattr(chunk, "end_char", None),
                    evidence={
                        "model_name": self.model_name,
                        "candidate_kind": "scispacy_noun_chunk",
                    },
                )
            )
        return candidates


class AbbreviationExtractor(KeywordExtractorBase):
    """Extract abbreviation pairs anchored in the original text."""

    name = "abbreviation_detector"
    source = "abbreviation-detected"
    LONG_FORM_FIRST_RE = re.compile(
        r"(?P<long>[A-Za-z][A-Za-z0-9α-ωΑ-Ω,\-/ ]{3,120}?)\s*\((?P<abbr>[A-Za-z][A-Za-z0-9\-\/]{1,15})\)"
    )
    ABBR_FIRST_RE = re.compile(
        r"(?P<abbr>[A-Za-z][A-Za-z0-9\-\/]{1,15})\s*\((?P<long>[A-Za-z][A-Za-z0-9α-ωΑ-Ω,\-/ ]{3,120}?)\)"
    )

    def extract(self, item: KeywordExtractionInput) -> List[KeywordCandidate]:
        candidates: List[KeywordCandidate] = []
        seen: set[Tuple[str, str]] = set()
        for pattern in (self.LONG_FORM_FIRST_RE, self.ABBR_FIRST_RE):
            for match in pattern.finditer(item.text):
                long_form = clean_display_keyword(match.group("long"))
                abbreviation = clean_display_keyword(match.group("abbr"))
                long_form = self._shortest_matching_long_form(long_form, abbreviation)
                if not self._looks_like_abbreviation_pair(long_form, abbreviation):
                    continue
                key = (normalize_keyword_text(long_form), normalize_keyword_text(abbreviation))
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(
                    KeywordCandidate.from_text(
                        text=long_form,
                        keyword_type="concept",
                        source=self.source,
                        extractor_name=self.name,
                        start=match.start("long"),
                        end=match.end("long"),
                        canonical=long_form,
                        aliases=[abbreviation],
                        evidence={
                            "abbreviation_pair": True,
                            "abbreviation": abbreviation,
                            "long_form": long_form,
                            "candidate_kind": "abbreviation_long_form",
                        },
                    )
                )
                candidates.append(
                    KeywordCandidate.from_text(
                        text=abbreviation,
                        keyword_type="concept",
                        source=self.source,
                        extractor_name=self.name,
                        start=match.start("abbr"),
                        end=match.end("abbr"),
                        canonical=long_form,
                        aliases=[abbreviation],
                        evidence={
                            "abbreviation_pair": True,
                            "abbreviation": abbreviation,
                            "long_form": long_form,
                            "candidate_kind": "abbreviation_short_form",
                        },
                    )
                )
        return candidates

    @staticmethod
    def _looks_like_abbreviation_pair(long_form: str, abbreviation: str) -> bool:
        if not long_form or not abbreviation:
            return False
        if len(abbreviation) < 2 or len(abbreviation) > 15:
            return False
        if abbreviation.lower() == long_form.lower():
            return False
        if not any(char.isupper() for char in abbreviation) and not any(char.isdigit() for char in abbreviation):
            return False
        abbr_letters = re.sub(r"[^A-Za-z0-9]", "", abbreviation).lower()
        long_letters = re.sub(r"[^A-Za-z0-9]", "", long_form).lower()
        if not abbr_letters or not long_letters:
            return False
        position = 0
        for char in abbr_letters:
            found = long_letters.find(char, position)
            if found < 0:
                return False
            position = found + 1
        return True

    @classmethod
    def _shortest_matching_long_form(cls, long_form: str, abbreviation: str) -> str:
        tokens = long_form.split()
        if len(tokens) <= 1:
            return long_form
        for index in range(len(tokens) - 1, -1, -1):
            candidate = " ".join(tokens[index:])
            if cls._looks_like_abbreviation_pair(candidate, abbreviation):
                return candidate
        return long_form


class KeywordConceptEnricher:
    """Base interface for anchored candidate enrichment."""

    def enrich(self, candidates: List[KeywordCandidate]) -> List[KeywordCandidate]:
        return candidates


class AbbreviationConceptEnricher(KeywordConceptEnricher):
    """Propagate abbreviation pair aliases to matching same-paper candidates."""

    def enrich(self, candidates: List[KeywordCandidate]) -> List[KeywordCandidate]:
        long_by_abbr: Dict[str, Tuple[str, str]] = {}
        for candidate in candidates:
            if not candidate.evidence.get("abbreviation_pair"):
                continue
            abbreviation = candidate.evidence.get("abbreviation")
            long_form = candidate.evidence.get("long_form")
            if abbreviation and long_form:
                long_by_abbr[normalize_keyword_text(str(abbreviation))] = (
                    clean_display_keyword(str(long_form)),
                    clean_display_keyword(str(abbreviation)),
                )

        if not long_by_abbr:
            return candidates

        enriched: List[KeywordCandidate] = []
        for candidate in candidates:
            pair = long_by_abbr.get(candidate.normalized_text)
            if not pair:
                enriched.append(candidate)
                continue
            long_form, abbreviation = pair
            aliases = _unique_strings([*candidate.aliases, abbreviation])
            evidence = dict(candidate.evidence)
            evidence["abbreviation_pair"] = True
            evidence["long_form"] = long_form
            evidence["abbreviation"] = abbreviation
            enriched.append(candidate.copy_with(canonical=long_form, aliases=aliases, evidence=evidence))
        return enriched


class OntologyConceptEnricher(KeywordConceptEnricher):
    """Enhance anchored candidates with UMLS / MeSH evidence.

    The implementation intentionally mirrors ``RemoteOntologySpanMatcher``'s
    batch contract without creating any unanchored keywords.
    """

    def __init__(
        self,
        matcher: Optional[Any] = None,
        base_url: Optional[str] = None,
        sources: Sequence[str] = ("umls", "mesh"),
        top_k: int = 3,
        threshold: float = 0.9,
        timeout: float = 20.0,
    ):
        self.sources = tuple(sources)
        self.top_k = top_k
        self.threshold = threshold
        self.timeout = timeout
        self.matcher = matcher
        if self.matcher is None and base_url:
            from .span_matcher import RemoteOntologySpanMatcher

            self.matcher = RemoteOntologySpanMatcher(
                base_url=base_url,
                sources=sources,
                top_k=top_k,
                threshold=threshold,
                timeout=timeout,
            )

    def enrich(self, candidates: List[KeywordCandidate]) -> List[KeywordCandidate]:
        if not self.matcher or not candidates:
            return candidates

        phrase_candidates = [
            _OntologyPhraseCandidate(
                text=candidate.text,
                normalized_text=candidate.normalized_text,
                kind=candidate.evidence.get("candidate_kind") or candidate.extractor_name,
                start=candidate.start or 0,
                end=candidate.end or len(candidate.text),
            )
            for candidate in candidates
        ]
        try:
            evidence_buckets = self.matcher.match_many(phrase_candidates)
        except Exception as exc:
            logging.warning("ontology enrichment failed: %s", exc)
            return [
                candidate.copy_with(
                    evidence={**candidate.evidence, "ontology_error": str(exc)}
                )
                for candidate in candidates
            ]

        enriched: List[KeywordCandidate] = []
        for candidate, evidence_items in zip(candidates, evidence_buckets):
            evidence_payloads = [
                payload
                for payload in (_evidence_to_dict(item) for item in evidence_items or [])
                if _safe_float(payload.get("confidence"), default=0.0) >= self.threshold
            ]
            if not evidence_payloads:
                enriched.append(candidate)
                continue
            primary = evidence_payloads[0]
            aliases = _unique_strings([*candidate.aliases, *primary.get("aliases", [])])
            semantic_types = _unique_strings([*candidate.semantic_types, *primary.get("semantic_types", [])])
            evidence = dict(candidate.evidence)
            evidence["ontology"] = evidence_payloads
            enriched.append(
                candidate.copy_with(
                    canonical=clean_display_keyword(primary.get("canonical") or candidate.canonical or candidate.text),
                    aliases=aliases,
                    semantic_types=semantic_types,
                    confidence=max(candidate.confidence, _safe_float(primary.get("confidence"), default=0.0)),
                    evidence=evidence,
                )
            )
        return enriched


@dataclass
class _OntologyPhraseCandidate:
    text: str
    normalized_text: str
    kind: str
    start: int = 0
    end: int = 0


class KeywordPruner:
    """Deterministic pruning rules for audit-only keyword candidates."""

    def __init__(
        self,
        hard_drop_keywords: Optional[Iterable[str]] = None,
        hard_drop_phrases: Optional[Iterable[str]] = None,
        noise_patterns: Optional[Sequence[re.Pattern[str]]] = None,
    ):
        self.hard_drop_keywords = {
            normalize_keyword_text(item) for item in (hard_drop_keywords or HARD_DROP_KEYWORDS)
        }
        self.hard_drop_phrases = {
            normalize_keyword_text(item) for item in (hard_drop_phrases or HARD_DROP_PHRASES)
        }
        self.noise_patterns = tuple(noise_patterns or NOISE_PATTERNS)

    def prune(self, candidates: List[KeywordCandidate]) -> Tuple[List[KeywordCandidate], List[KeywordPruneDecision], List[KeywordPruneDecision]]:
        kept: List[KeywordCandidate] = []
        dropped: List[KeywordPruneDecision] = []
        reviewed: List[KeywordPruneDecision] = []
        for candidate in candidates:
            decision = self.decide(candidate)
            if decision.action == "keep":
                kept.append(candidate)
            elif decision.action == "review":
                reviewed.append(decision)
            else:
                dropped.append(decision)
        return kept, dropped, reviewed

    def decide(self, candidate: KeywordCandidate) -> KeywordPruneDecision:
        reasons: List[str] = []
        text = clean_display_keyword(candidate.text)
        normalized = candidate.normalized_text or normalize_keyword_text(text)

        if not text or len(text) < 2:
            return KeywordPruneDecision(candidate, "drop", ["empty_or_too_short"])
        if len(text) > 200:
            return KeywordPruneDecision(candidate, "drop", ["too_long"])
        if any(pattern.search(text) for pattern in self.noise_patterns):
            return KeywordPruneDecision(candidate, "drop", ["noise_pattern"])
        if normalized in self.hard_drop_keywords:
            return KeywordPruneDecision(candidate, "drop", ["hard_drop_keyword"])
        if normalized in self.hard_drop_phrases:
            return KeywordPruneDecision(candidate, "drop", ["hard_drop_phrase"])
        if self._is_stopword_only(normalized):
            return KeywordPruneDecision(candidate, "drop", ["stopword_only"])

        return KeywordPruneDecision(candidate, "keep", reasons)

    @staticmethod
    def _is_stopword_only(normalized: str) -> bool:
        return normalized in {"a", "an", "the", "and", "or", "of", "in", "on", "for", "with", "using"}


class CandidateGenerator:
    """Run a set of extractors and keep extractor failures isolated."""

    def __init__(self, extractors: Sequence[KeywordExtractorBase]):
        self.extractors = list(extractors)

    def generate(self, item: KeywordExtractionInput) -> Tuple[List[KeywordCandidate], List[Dict[str, Any]]]:
        candidates: List[KeywordCandidate] = []
        errors: List[Dict[str, Any]] = []
        for extractor in self.extractors:
            try:
                candidates.extend(extractor.extract(item))
            except Exception as exc:
                logging.error("keyword extractor failed: %s", getattr(extractor, "name", extractor), exc_info=True)
                errors.append(
                    {
                        "extractor": getattr(extractor, "name", extractor.__class__.__name__),
                        "source": getattr(extractor, "source", None),
                        "error": str(exc),
                    }
                )
        return candidates, errors


class KeywordEnrichmentPipeline:
    """Audit-only v2 pipeline: generate candidates, enrich, and prune."""

    def __init__(
        self,
        extractors: Sequence[KeywordExtractorBase],
        enrichers: Optional[Sequence[KeywordConceptEnricher]] = None,
        pruner: Optional[KeywordPruner] = None,
        preprocessor: Optional[TextPreprocessor] = None,
    ):
        self.generator = CandidateGenerator(extractors)
        self.enrichers = list(enrichers or [])
        self.pruner = pruner or KeywordPruner()
        self.preprocessor = preprocessor or TextPreprocessor()

    def run(
        self,
        title: Optional[str] = None,
        abstract: Optional[str] = None,
        text: Optional[str] = None,
        source_keywords: Optional[List[Dict[str, Any]]] = None,
        paper_id: Optional[int] = None,
    ) -> KeywordPipelineResult:
        item = self.preprocessor.preprocess(title=title, abstract=abstract, text=text)
        item.source_keywords = list(source_keywords or [])
        item.paper_id = paper_id
        candidates, errors = self.generator.generate(item)
        candidates = self._dedupe_exact(candidates)
        for enricher in self.enrichers:
            candidates = enricher.enrich(candidates)
        kept, dropped, reviewed = self.pruner.prune(candidates)
        return KeywordPipelineResult(
            candidates=candidates,
            kept=kept,
            dropped=dropped,
            reviewed=reviewed,
            errors=errors,
        )

    @staticmethod
    def _dedupe_exact(candidates: Sequence[KeywordCandidate]) -> List[KeywordCandidate]:
        deduped: List[KeywordCandidate] = []
        seen: set[Tuple[str, str, str, str]] = set()
        for candidate in candidates:
            key = (
                candidate.normalized_text,
                candidate.keyword_type.lower(),
                candidate.source.lower(),
                candidate.extractor_name.lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(candidate)
        return deduped


def _unique_strings(values: Iterable[Any]) -> List[str]:
    seen = set()
    unique: List[str] = []
    for value in values:
        text = clean_display_keyword(str(value))
        key = normalize_keyword_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(text)
    return unique


def _safe_float(value: Any, default: float = 1.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _evidence_to_dict(item: Any) -> Dict[str, Any]:
    if hasattr(item, "to_dict"):
        return dict(item.to_dict())
    if isinstance(item, Mapping):
        return dict(item)
    payload: Dict[str, Any] = {}
    for key in ("source", "concept_id", "canonical", "confidence", "match_type", "aliases", "semantic_types"):
        if hasattr(item, key):
            payload[key] = getattr(item, key)
    return payload


@dataclass
class KeywordExtractionResult:
    success: bool
    keywords: List[Dict[str, Any]] = field(default_factory=list)
    source: str = DEFAULT_KEYWORD_SOURCE
    model_name: str = DEFAULT_MODEL_NAME
    prompt_version: str = "scispacy-v1"
    error: Optional[str] = None
    raw_response: Optional[str] = None
    skipped: bool = False
    skip_reason: Optional[str] = None
    model_results: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "keywords": self.keywords,
            "source": self.source,
            "model_name": self.model_name,
            "prompt_version": self.prompt_version,
            "error": self.error,
            "raw_response": self.raw_response,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "model_results": self.model_results,
        }


class KeywordEnrichmentService:
    """Extract structured paper keywords with local scispaCy models.

    Each configured scispaCy model is treated as an independent keyword source.
    By default the service runs:

    - ``en_core_sci_lg`` for high-recall biomedical mention candidates.
    - ``en_ner_bionlp13cg_md`` for typed life-science entities.
    """

    def __init__(
        self,
        config_path: Optional[Path] = None,
        model_name: Optional[str] = None,
        model_names: Optional[Sequence[str]] = None,
        source: Optional[str] = None,
        sources: Optional[Dict[str, str]] = None,
        max_keywords: int = 12,
        timeout: int = 60,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        if config_path and load_config_from_yaml is not None:
            config = load_config_from_yaml(config_path)
        else:
            if config_path and load_config_from_yaml is None:
                logging.warning("config loader is unavailable; using default keyword enrichment config")
            config = {}
        keyword_config = config.get("keyword_enrichment") or {}

        configured_models = keyword_config.get("models") or keyword_config.get("model_names")
        if isinstance(configured_models, str):
            configured_models = [configured_models]

        if model_names is not None:
            selected_models = list(model_names)
        elif model_name:
            selected_models = [model_name]
        elif configured_models:
            selected_models = list(configured_models)
        else:
            selected_models = list(DEFAULT_MODEL_NAMES)

        configured_sources = keyword_config.get("sources") or {}
        if not isinstance(configured_sources, dict):
            configured_sources = {}
        self.source_by_model = self._build_source_map(
            selected_models,
            explicit_source=source or keyword_config.get("source"),
            explicit_sources=sources or configured_sources,
        )
        self.model_names = selected_models
        self.model_name = "+".join(selected_models)
        self.source = "+".join(self.source_by_model[model] for model in selected_models)
        self.sources = [self.source_by_model[model] for model in selected_models]
        self.max_keywords = int(keyword_config.get("max_keywords") or max_keywords)
        self.timeout = int(keyword_config.get("timeout") or timeout)
        self._nlp_by_model: Dict[str, Any] = {}

        # Kept for backwards-compatible construction by older callers. The
        # scispaCy implementation is local and does not use remote credentials.
        self.api_key = api_key
        self.base_url = base_url

    def build_pipeline_v2(
        self,
        include_source_keywords: bool = True,
        include_entity_extractors: bool = True,
        include_noun_chunks: bool = True,
        include_abbreviations: bool = True,
        ontology_matcher: Optional[Any] = None,
        ontology_base_url: Optional[str] = None,
        ontology_threshold: float = 0.9,
    ) -> KeywordEnrichmentPipeline:
        """Build the audit-only v2 keyword pipeline.

        The legacy ``extract_keywords`` path is intentionally unchanged. This
        method exposes the new candidate/enrich/prune pipeline for audit runs
        and staged rollout.
        """
        extractors: List[KeywordExtractorBase] = []
        if include_source_keywords:
            extractors.append(SourceKeywordExtractor())
        if include_entity_extractors:
            for model in self.model_names:
                extractors.append(
                    ScispacyEntityExtractor(
                        model_name=model,
                        nlp_loader=self._load_model,
                        source=self.source_by_model[model],
                    )
                )
        if include_noun_chunks and "en_core_sci_lg" in self.model_names:
            extractors.append(
                ScispacyNounChunkExtractor(
                    model_name="en_core_sci_lg",
                    nlp_loader=self._load_model,
                    source="scispacy-en_core_sci_lg-noun-chunk",
                )
            )
        if include_abbreviations:
            extractors.append(AbbreviationExtractor())

        enrichers: List[KeywordConceptEnricher] = [AbbreviationConceptEnricher()]
        if ontology_matcher is not None or ontology_base_url:
            enrichers.append(
                OntologyConceptEnricher(
                    matcher=ontology_matcher,
                    base_url=ontology_base_url,
                    threshold=ontology_threshold,
                )
            )
        return KeywordEnrichmentPipeline(
            extractors=extractors,
            enrichers=enrichers,
            pruner=KeywordPruner(),
        )

    def extract_keywords(
        self,
        title: Optional[str],
        abstract: Optional[str],
    ) -> KeywordExtractionResult:
        """Extract and normalize keywords for one paper from all models."""
        title = (title or "").strip()
        abstract = (abstract or "").strip()
        text = "\n\n".join(part for part in (title, abstract) if part)
        if not text:
            return KeywordExtractionResult(
                success=False,
                source=self.source,
                model_name=self.model_name,
                skipped=True,
                skip_reason="empty_title_and_abstract",
            )

        all_keywords: List[Dict[str, Any]] = []
        model_results: List[Dict[str, Any]] = []
        errors: List[str] = []

        for model in self.model_names:
            source = self.source_by_model[model]
            try:
                nlp = self._load_model(model)
                doc = nlp(text)
                keywords = self._normalize_entities(doc.ents, source=source)
                all_keywords.extend(keywords)
                model_results.append(
                    {
                        "success": True,
                        "model_name": model,
                        "source": source,
                        "keyword_count": len(keywords),
                    }
                )
            except Exception as exc:
                logging.error("scispaCy keyword extraction failed for %s: %s", model, exc, exc_info=True)
                errors.append(f"{model}: {exc}")
                model_results.append(
                    {
                        "success": False,
                        "model_name": model,
                        "source": source,
                        "error": str(exc),
                    }
                )

        if not all_keywords:
            return KeywordExtractionResult(
                success=False,
                source=self.source,
                model_name=self.model_name,
                error="; ".join(errors) if errors else "no keywords extracted",
                model_results=model_results,
            )

        return KeywordExtractionResult(
            success=True,
            keywords=all_keywords,
            source=self.source,
            model_name=self.model_name,
            error="; ".join(errors) if errors else None,
            model_results=model_results,
        )

    def _load_model(self, model_name: str):
        if model_name in self._nlp_by_model:
            return self._nlp_by_model[model_name]
        try:
            import spacy
        except ImportError as exc:
            raise RuntimeError("spacy/scispacy is not installed") from exc

        try:
            self._nlp_by_model[model_name] = spacy.load(model_name)
        except OSError as exc:
            raise RuntimeError(
                f"scispaCy model '{model_name}' is not installed. "
                "Install the compatible model package before enabling keyword enrichment."
            ) from exc
        return self._nlp_by_model[model_name]

    def _normalize_entities(self, entities, source: str) -> List[Dict[str, Any]]:
        counts: Counter[tuple[str, str]] = Counter()
        display_text: Dict[tuple[str, str], str] = {}

        for ent in entities:
            keyword = self._clean_keyword(ent.text)
            if not self._is_usable_keyword(keyword):
                continue
            keyword_type = self._keyword_type_for_label(getattr(ent, "label_", ""))
            key = (keyword_type, keyword.lower())
            counts[key] += 1
            display_text.setdefault(key, keyword)

        max_count = max(counts.values(), default=1)
        keywords = []
        for (keyword_type, keyword_lower), count in counts.most_common():
            keyword = display_text[(keyword_type, keyword_lower)]
            keywords.append(
                {
                    "keyword_type": keyword_type,
                    "keyword": keyword,
                    "weight": round(count / max_count, 3),
                    "source": source,
                }
            )
            if len(keywords) >= self.max_keywords:
                break
        return keywords

    @staticmethod
    def _build_source_map(
        model_names: Sequence[str],
        explicit_source: Optional[str],
        explicit_sources: Dict[str, str],
    ) -> Dict[str, str]:
        if explicit_source and len(model_names) == 1:
            return {model_names[0]: explicit_source}

        source_by_model = {}
        for model in model_names:
            source_by_model[model] = (
                explicit_sources.get(model)
                or DEFAULT_KEYWORD_SOURCES.get(model)
                or f"scispacy-{model}-generated"
            )
        return source_by_model

    @staticmethod
    def _clean_keyword(value: str) -> str:
        return re.sub(r"\s+", " ", (value or "").strip(" \t\r\n.,;:()[]{}"))

    @classmethod
    def _is_usable_keyword(cls, keyword: str) -> bool:
        if not keyword or len(keyword) < 2 or len(keyword) > 200:
            return False
        if keyword.lower() in GENERIC_KEYWORDS:
            return False
        return True

    @staticmethod
    def _keyword_type_for_label(label: str) -> str:
        keyword_type = SCISPACY_LABEL_TO_KEYWORD_TYPE.get((label or "").upper(), "concept")
        return keyword_type if keyword_type in ALLOWED_KEYWORD_TYPES else "concept"
