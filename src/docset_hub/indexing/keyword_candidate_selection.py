"""Select retrieval-grade keywords from keyword pipeline candidates."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .keyword_enrichment import KeywordCandidate, normalize_keyword_text


ONTOLOGY_SOURCES = {"umls", "mesh"}
SOURCE_KEYWORD_SOURCES = {"biorxiv", "langtaosha", "source-provided"}
TYPED_ENTITY_TYPES = {"gene", "protein", "chemical", "disease", "organism"}
LOW_INFORMATION_KEYWORDS = {
    "we",
    "our",
    "it",
    "its",
    "that",
    "this",
    "these",
    "those",
    "they",
    "them",
    "these findings",
    "these results",
    "this study",
    "our findings",
    "our results",
    "consistent with",
    "associated with",
    "cell",
    "cells",
    "human",
    "humans",
    "patient",
    "patients",
    "participant",
    "participants",
    "sample",
    "samples",
    "disease",
    "diseases",
    "gene",
    "genes",
    "protein",
    "proteins",
    "expression",
    "activity",
    "data",
    "model",
    "models",
    "network",
    "networks",
}


@dataclass
class KeywordSelectionDecision:
    """Selection decision for one merged keyword candidate cluster."""

    action: str
    keyword: str
    normalized_keyword: str
    keyword_type: str
    reasons: List[str]
    candidates: List[KeywordCandidate] = field(default_factory=list)
    canonical: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    source_details: List[str] = field(default_factory=list)
    ontology_sources: List[str] = field(default_factory=list)
    ontology_matches: List[Dict[str, Any]] = field(default_factory=list)
    confidence: float = 1.0
    rank: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "rank": self.rank,
            "keyword": self.keyword,
            "normalized_keyword": self.normalized_keyword,
            "keyword_type": self.keyword_type,
            "canonical": self.canonical,
            "aliases": list(self.aliases),
            "confidence": round(float(self.confidence), 4),
            "reasons": list(self.reasons),
            "sources": list(self.sources),
            "source_details": list(self.source_details),
            "ontology_sources": list(self.ontology_sources),
            "ontology_matches": list(self.ontology_matches),
            "candidates": [candidate.to_audit_dict() for candidate in self.candidates],
        }


@dataclass
class KeywordCandidateSelectionResult:
    """Selected and unselected keyword clusters."""

    selected: List[KeywordSelectionDecision]
    unselected: List[KeywordSelectionDecision]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "selected_keyword_count": len(self.selected),
            "unselected_keyword_count": len(self.unselected),
            "selected_keywords": [decision.to_dict() for decision in self.selected],
            "unselected_keywords": [decision.to_dict() for decision in self.unselected],
        }


@dataclass
class _CandidateCluster:
    key: str
    candidates: List[KeywordCandidate] = field(default_factory=list)

    @property
    def representative(self) -> KeywordCandidate:
        return sorted(self.candidates, key=_candidate_priority, reverse=True)[0]


class KeywordCandidateSelector:
    """Choose retrieval-grade keywords while preserving rejected candidates."""

    def __init__(
        self,
        max_keywords: int = 40,
        ontology_cap: int = 15,
        typed_entity_cap: int = 10,
        abbreviation_cap: int = 8,
        noun_chunk_cap: int = 5,
        ontology_threshold: float = 0.9,
    ):
        self.max_keywords = max_keywords
        self.ontology_cap = ontology_cap
        self.typed_entity_cap = typed_entity_cap
        self.abbreviation_cap = abbreviation_cap
        self.noun_chunk_cap = noun_chunk_cap
        self.ontology_threshold = ontology_threshold

    def select(
        self,
        candidates: Sequence[KeywordCandidate],
        title: str = "",
        abstract: str = "",
    ) -> KeywordCandidateSelectionResult:
        clusters = list(self._cluster(candidates).values())
        selected_keys: set[str] = set()
        selected_reasons: Dict[str, List[str]] = {}
        selected_order: List[str] = []
        cap_counts = {
            "ontology": 0,
            "typed": 0,
            "abbreviation": 0,
            "noun_chunk": 0,
        }

        def select_cluster(cluster: _CandidateCluster, reason: str, bucket: Optional[str] = None) -> None:
            if cluster.key in selected_keys:
                selected_reasons[cluster.key].append(reason)
                return
            if len(selected_order) >= self.max_keywords:
                return
            if bucket and cap_counts[bucket] >= self._cap_for(bucket):
                return
            selected_keys.add(cluster.key)
            selected_order.append(cluster.key)
            selected_reasons[cluster.key] = [reason]
            if bucket:
                cap_counts[bucket] += 1

        for cluster in sorted(clusters, key=lambda item: _source_keyword_score(item), reverse=True):
            if self._is_source_keyword(cluster):
                select_cluster(cluster, "source_keyword_or_category")

        for cluster in self._ranked(clusters, title, reason="abbreviation"):
            if self._is_abbreviation_pair(cluster) and not self._is_low_information(cluster):
                select_cluster(cluster, "abbreviation_pair", bucket="abbreviation")

        for cluster in self._ranked(clusters, title, reason="ontology"):
            if self._is_ontology_backed(cluster) and not self._is_low_information(cluster):
                select_cluster(cluster, "ontology_backed", bucket="ontology")

        for cluster in self._ranked(clusters, title, reason="typed"):
            if self._is_bionlp_typed_entity(cluster) and not self._is_low_information(cluster):
                select_cluster(cluster, "bionlp_typed_entity", bucket="typed")

        for cluster in self._ranked(clusters, title, reason="noun_chunk"):
            if not self._is_noun_chunk(cluster) or self._is_low_information(cluster):
                continue
            if self._is_title_match(cluster, title) or self._has_professional_shape(cluster):
                select_cluster(cluster, "qualified_noun_chunk", bucket="noun_chunk")

        selected = []
        unselected = []
        clusters_by_key = {cluster.key: cluster for cluster in clusters}
        for rank, key in enumerate(selected_order, start=1):
            selected.append(
                self._decision(
                    clusters_by_key[key],
                    action="selected",
                    reasons=selected_reasons[key],
                    rank=rank,
                )
            )

        for cluster in sorted(clusters, key=lambda item: self._ranking_score(item, title), reverse=True):
            if cluster.key in selected_keys:
                continue
            unselected.append(
                self._decision(
                    cluster,
                    action="unselected",
                    reasons=self._unselected_reasons(cluster, title),
                )
            )

        return KeywordCandidateSelectionResult(selected=selected, unselected=unselected)

    def _cap_for(self, bucket: str) -> int:
        return {
            "ontology": self.ontology_cap,
            "typed": self.typed_entity_cap,
            "abbreviation": self.abbreviation_cap,
            "noun_chunk": self.noun_chunk_cap,
        }[bucket]

    def _cluster(self, candidates: Sequence[KeywordCandidate]) -> Dict[str, _CandidateCluster]:
        clusters: Dict[str, _CandidateCluster] = {}
        seen = set()
        for candidate in candidates:
            unique_key = (
                candidate.source,
                candidate.keyword_type,
                candidate.normalized_text,
                candidate.canonical or "",
            )
            if unique_key in seen:
                continue
            seen.add(unique_key)
            key = self._merge_key(candidate)
            clusters.setdefault(key, _CandidateCluster(key=key)).candidates.append(candidate)
        return clusters

    def _merge_key(self, candidate: KeywordCandidate) -> str:
        if candidate.keyword_type == "category" or self._candidate_is_source_keyword(candidate):
            return f"source:{candidate.normalized_text}"
        if candidate.evidence.get("abbreviation_pair") and candidate.canonical:
            return f"abbr:{normalize_keyword_text(candidate.canonical)}"
        if self._candidate_ontology_evidence(candidate) and candidate.canonical:
            return f"ontology:{normalize_keyword_text(candidate.canonical)}"
        return f"text:{candidate.normalized_text}"

    def _ranked(self, clusters: Sequence[_CandidateCluster], title: str, reason: str) -> List[_CandidateCluster]:
        return sorted(
            clusters,
            key=lambda cluster: self._ranking_score(cluster, title, reason=reason),
            reverse=True,
        )

    def _ranking_score(self, cluster: _CandidateCluster, title: str, reason: str = "") -> float:
        rep = cluster.representative
        score = 0.0
        if self._is_title_match(cluster, title):
            score += 50.0
        if self._has_professional_shape(cluster):
            score += 25.0
        if self._is_ontology_backed(cluster):
            score += 30.0
        if self._has_mesh_evidence(cluster):
            score += 8.0
        if self._is_bionlp_typed_entity(cluster):
            score += 20.0
        if self._is_abbreviation_pair(cluster):
            score += 18.0
        score += min(_token_count(self._display_keyword(cluster)), 6) * 2.0
        score += self._best_confidence(cluster)
        if reason == "ontology":
            score += self._best_ontology_confidence(cluster) * 10.0
        if rep.source == "scispacy-en_core_sci_lg-noun-chunk":
            score -= 5.0
        if self._is_low_information(cluster):
            score -= 100.0
        return score

    def _decision(
        self,
        cluster: _CandidateCluster,
        action: str,
        reasons: List[str],
        rank: Optional[int] = None,
    ) -> KeywordSelectionDecision:
        rep = cluster.representative
        keyword = self._display_keyword(cluster)
        ontology_matches = self._ontology_matches(cluster)
        sources = _unique_strings(candidate.source for candidate in cluster.candidates)
        source_details = _unique_strings(
            f"{candidate.source} | {candidate.extractor_name} | {candidate.keyword_type}"
            for candidate in cluster.candidates
        )
        aliases = _unique_strings(
            alias
            for candidate in cluster.candidates
            for alias in candidate.aliases
        )
        ontology_sources = _unique_strings(
            str(match.get("source") or "ontology").lower()
            for match in ontology_matches
        )
        return KeywordSelectionDecision(
            action=action,
            rank=rank,
            keyword=keyword,
            normalized_keyword=normalize_keyword_text(keyword),
            keyword_type=rep.keyword_type,
            canonical=rep.canonical,
            aliases=aliases,
            confidence=self._best_confidence(cluster),
            reasons=_unique_strings(reasons),
            candidates=list(cluster.candidates),
            sources=sources,
            source_details=source_details,
            ontology_sources=ontology_sources,
            ontology_matches=ontology_matches,
        )

    def _display_keyword(self, cluster: _CandidateCluster) -> str:
        source_candidate = next(
            (
                candidate
                for candidate in cluster.candidates
                if candidate.keyword_type == "category" or self._candidate_is_source_keyword(candidate)
            ),
            None,
        )
        if source_candidate is not None:
            return source_candidate.text

        abbreviation_candidate = next(
            (
                candidate
                for candidate in cluster.candidates
                if candidate.evidence.get("abbreviation_pair") and candidate.canonical
            ),
            None,
        )
        if abbreviation_candidate is not None:
            return abbreviation_candidate.canonical or abbreviation_candidate.text

        rep = cluster.representative
        return rep.canonical or rep.text

    def _unselected_reasons(self, cluster: _CandidateCluster, title: str) -> List[str]:
        reasons = []
        if self._is_low_information(cluster):
            reasons.append("low_information_surface")
        if self._is_noun_chunk(cluster) and not self._is_title_match(cluster, title) and not self._has_professional_shape(cluster):
            reasons.append("noun_chunk_without_title_or_professional_shape")
        if self._is_ontology_backed(cluster):
            reasons.append("ontology_cap_or_lower_priority")
        elif self._is_bionlp_typed_entity(cluster):
            reasons.append("typed_entity_cap_or_lower_priority")
        elif self._is_abbreviation_pair(cluster):
            reasons.append("abbreviation_cap_or_duplicate")
        else:
            reasons.append("not_selected_by_retrieval_rules")
        return reasons

    def _is_source_keyword(self, cluster: _CandidateCluster) -> bool:
        return any(self._candidate_is_source_keyword(candidate) for candidate in cluster.candidates)

    @staticmethod
    def _candidate_is_source_keyword(candidate: KeywordCandidate) -> bool:
        return (
            candidate.keyword_type == "category"
            or candidate.source in SOURCE_KEYWORD_SOURCES
            or bool(candidate.evidence.get("source_provided"))
        )

    def _is_abbreviation_pair(self, cluster: _CandidateCluster) -> bool:
        return any(candidate.evidence.get("abbreviation_pair") for candidate in cluster.candidates)

    def _is_ontology_backed(self, cluster: _CandidateCluster) -> bool:
        return bool(self._ontology_matches(cluster))

    def _has_mesh_evidence(self, cluster: _CandidateCluster) -> bool:
        return any(match.get("source") == "mesh" for match in self._ontology_matches(cluster))

    @staticmethod
    def _is_bionlp_typed_entity(cluster: _CandidateCluster) -> bool:
        return any(
            candidate.source == "scispacy-en_ner_bionlp13cg_md-generated"
            and candidate.keyword_type in TYPED_ENTITY_TYPES
            for candidate in cluster.candidates
        )

    @staticmethod
    def _is_noun_chunk(cluster: _CandidateCluster) -> bool:
        return any(candidate.source.endswith("-noun-chunk") for candidate in cluster.candidates)

    def _is_title_match(self, cluster: _CandidateCluster, title: str) -> bool:
        title_key = normalize_keyword_text(title)
        if not title_key:
            return False
        for value in [self._display_keyword(cluster), *(candidate.text for candidate in cluster.candidates)]:
            key = normalize_keyword_text(value)
            if key and key in title_key:
                return True
        return False

    def _has_professional_shape(self, cluster: _CandidateCluster) -> bool:
        return any(_has_professional_shape(value) for value in [self._display_keyword(cluster), *(candidate.text for candidate in cluster.candidates)])

    def _is_low_information(self, cluster: _CandidateCluster) -> bool:
        return normalize_keyword_text(self._display_keyword(cluster)) in LOW_INFORMATION_KEYWORDS

    def _best_confidence(self, cluster: _CandidateCluster) -> float:
        return max([candidate.confidence for candidate in cluster.candidates] + [self._best_ontology_confidence(cluster), 0.0])

    def _best_ontology_confidence(self, cluster: _CandidateCluster) -> float:
        confidences = [
            _safe_float(match.get("confidence"), default=0.0)
            for match in self._ontology_matches(cluster)
        ]
        return max(confidences, default=0.0)

    def _ontology_matches(self, cluster: _CandidateCluster) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        seen = set()
        for candidate in cluster.candidates:
            for evidence in self._candidate_ontology_evidence(candidate):
                key = (
                    str(evidence.get("source") or ""),
                    str(evidence.get("concept_id") or ""),
                    str(evidence.get("canonical") or ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                matches.append(evidence)
        return sorted(
            matches,
            key=lambda item: (
                1 if str(item.get("source") or "").lower() == "mesh" else 0,
                _safe_float(item.get("confidence"), default=0.0),
            ),
            reverse=True,
        )

    def _candidate_ontology_evidence(self, candidate: KeywordCandidate) -> List[Dict[str, Any]]:
        evidence_items = candidate.evidence.get("ontology") or []
        matches = []
        for item in evidence_items if isinstance(evidence_items, list) else []:
            if not isinstance(item, dict):
                continue
            source = str(item.get("source") or "").lower()
            if source not in ONTOLOGY_SOURCES:
                continue
            if _safe_float(item.get("confidence"), default=0.0) < self.ontology_threshold:
                continue
            matches.append(dict(item))
        return matches


def _candidate_priority(candidate: KeywordCandidate) -> Tuple[float, int, int, str]:
    ontology_confidence = max(
        [
            _safe_float(item.get("confidence"), default=0.0)
            for item in candidate.evidence.get("ontology", [])
            if isinstance(item, dict)
        ],
        default=0.0,
    )
    return (
        max(float(candidate.confidence), ontology_confidence),
        1 if candidate.source == "scispacy-en_ner_bionlp13cg_md-generated" else 0,
        len(candidate.text),
        candidate.text,
    )


def _source_keyword_score(cluster: _CandidateCluster) -> float:
    score = 0.0
    for candidate in cluster.candidates:
        if candidate.keyword_type == "category":
            score += 100.0
        if candidate.source in SOURCE_KEYWORD_SOURCES:
            score += 50.0
        if candidate.evidence.get("source_provided"):
            score += 25.0
    return score


def _has_professional_shape(value: str) -> bool:
    text = value or ""
    if re.search(r"\d|[-/+]", text):
        return True
    if re.search(r"\b[A-Z]{2,}[A-Za-z0-9-]*\b", text):
        return True
    if re.search(r"\b[A-Z]\s+cell\b", text):
        return True
    if re.search(r"[α-ωΑ-Ω]", text):
        return True
    lower = text.lower()
    return any(marker in lower for marker in ("rna-seq", "dna-seq", "scrna", "crispr", "single-cell", "multi-omics"))


def _token_count(value: str) -> int:
    return len([token for token in re.split(r"\s+", value.strip()) if token])


def _unique_strings(values: Iterable[Any]) -> List[str]:
    seen = set()
    unique: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(text)
    return unique


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
