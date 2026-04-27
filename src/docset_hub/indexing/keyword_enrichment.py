"""scispaCy-based keyword enrichment service."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from config.config_loader import load_config_from_yaml


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
        config = load_config_from_yaml(config_path) if config_path else {}
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
