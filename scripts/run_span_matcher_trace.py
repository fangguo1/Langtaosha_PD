#!/usr/bin/env python3
"""Trace SpanMatcher step-by-step against the ontology linker service."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SRC_ROOT))

from docset_hub.indexing.entity_filter_policy import filter_ontology_evidence_items  # noqa: E402
from docset_hub.indexing.query_phrase_analyzer import (  # noqa: E402
    MetadataDBPhraseLexicon,
    PhraseCandidate,
    QueryPhraseAnalyzer,
)
from docset_hub.indexing.span_matcher import (  # noqa: E402
    CompositeSpanMatcher,
    ConceptMatchEvidence,
    KeywordSurfaceSpanMatcher,
    MaximalConceptSelector,
    RemoteOntologySpanMatcher,
    SpanMatcherExecutor,
    SpanMatchResult,
)
from docset_hub.storage.metadata_db import MetadataDB  # noqa: E402


DEFAULT_CONFIG_PATH = PROJECT_ROOT / "src" / "config" / "config_tecent_backend_server_mimic.yaml"
DEFAULT_PAPER_SOURCES = ("langtaosha", "biorxiv_history", "biorxiv_daily")
DEFAULT_SCISPACY_MODEL = "en_core_sci_lg"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trace SpanMatcher stages against the ontology linker service.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--query", action="append", default=[], help="Run one query non-interactively.")
    parser.add_argument(
        "--ontology-linker-url",
        default=os.environ.get("ONTOLOGY_LINKER_URL", "http://127.0.0.1:8765"),
        help="Ontology linker API base URL.",
    )
    parser.add_argument(
        "--ontology-source-list",
        default=os.environ.get("ONTOLOGY_SOURCE_LIST", "umls,mesh"),
        help="Comma-separated ontology sources.",
    )
    parser.add_argument(
        "--ontology-top-k",
        type=int,
        default=int(os.environ.get("ONTOLOGY_TOP_K", "2")),
        help="Top-k ontology evidence per source.",
    )
    parser.add_argument(
        "--ontology-threshold",
        type=float,
        default=float(os.environ.get("ONTOLOGY_THRESHOLD", "0.7")),
        help="Minimum ontology linker confidence.",
    )
    parser.add_argument(
        "--scispacy-model",
        default=DEFAULT_SCISPACY_MODEL,
        help="Local spaCy/scispaCy model used only for candidate extraction.",
    )
    parser.add_argument(
        "--skip-scispacy",
        action="store_true",
        help="Disable local scispaCy candidate extraction and use rule-based candidates only.",
    )
    parser.add_argument("--no-subphrase-ngram", action="store_true", help="Disable subphrase expansion.")
    parser.add_argument(
        "--use-db-keywords",
        action="store_true",
        help="Also run KeywordSurfaceSpanMatcher backed by MetadataDB.",
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Config path used when --use-db-keywords is enabled.",
    )
    parser.add_argument(
        "--paper-source-list",
        default=",".join(DEFAULT_PAPER_SOURCES),
        help="Comma-separated paper source scope used when --use-db-keywords is enabled.",
    )
    return parser.parse_args()


def parse_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def load_scispacy(model_name: str):
    try:
        import spacy
    except ImportError:
        print("spaCy is not installed; continuing with rule-only candidates.", file=sys.stderr)
        return None

    try:
        return spacy.load(model_name)
    except OSError:
        print(
            f"Could not load '{model_name}'; continuing with rule-only candidates.",
            file=sys.stderr,
        )
        return None


def build_analyzer(args: argparse.Namespace) -> QueryPhraseAnalyzer:
    scispacy_pipeline = None if args.skip_scispacy else load_scispacy(args.scispacy_model)
    return QueryPhraseAnalyzer(lexicon=None, scispacy_pipeline=scispacy_pipeline)


def build_keyword_matcher(args: argparse.Namespace) -> Optional[KeywordSurfaceSpanMatcher]:
    if not args.use_db_keywords:
        return None
    metadata_db = MetadataDB(config_path=args.config_path)
    lexicon = MetadataDBPhraseLexicon(
        metadata_db=metadata_db,
        paper_source_names=parse_csv(args.paper_source_list),
    )
    return KeywordSurfaceSpanMatcher(lexicon=lexicon)


def collect_ontology_trace(
    matcher: RemoteOntologySpanMatcher,
    candidates: Sequence[PhraseCandidate],
) -> tuple[Dict[str, List[Mapping[str, object]]], Dict[str, List[ConceptMatchEvidence]]]:
    candidate_ids = [f"c{index}" for index in range(len(candidates))]
    payload = {
        "sources": list(matcher.sources),
        "top_k": matcher.top_k,
        "threshold": matcher.threshold,
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
    response_payload = matcher._post(payload)
    raw_results = matcher._results_by_candidate_id(response_payload)

    raw_by_surface: Dict[str, List[Mapping[str, object]]] = {}
    filtered_by_surface: Dict[str, List[ConceptMatchEvidence]] = {}
    for candidate_id, candidate in zip(candidate_ids, candidates):
        raw_items = raw_results.get(candidate_id, [])
        label = candidate_label(candidate)
        raw_by_surface[label] = list(raw_items)
        filtered_items = filter_ontology_evidence_items(raw_items)
        filtered_by_surface[label] = [
            matcher._to_evidence(candidate, item)
            for item in filtered_items
        ]
    return raw_by_surface, filtered_by_surface


def render_trace_report(
    *,
    query: str,
    normalized_query: str,
    extractor_candidates: Sequence[PhraseCandidate],
    expanded_candidates: Sequence[PhraseCandidate],
    raw_ontology_items: Mapping[str, Sequence[Mapping[str, object]]],
    filtered_ontology_evidence: Mapping[str, Sequence[ConceptMatchEvidence]],
    keyword_evidence: Mapping[str, Sequence[ConceptMatchEvidence]],
    span_results: Sequence[SpanMatchResult],
    selected_concepts,
) -> str:
    lines: List[str] = []
    lines.append(f"> {query}")
    lines.append("")
    lines.append("=== Normalized Query ===")
    lines.append(normalized_query or "-")
    lines.append("")

    lines.append("=== Extractor Candidates ===")
    for index, candidate in enumerate(extractor_candidates, start=1):
        lines.append(_format_candidate(index, candidate))
    if not extractor_candidates:
        lines.append("(none)")
    lines.append("")

    lines.append("=== Expanded Candidates ===")
    for index, candidate in enumerate(expanded_candidates, start=1):
        lines.append(_format_candidate(index, candidate))
    if not expanded_candidates:
        lines.append("(none)")
    lines.append("")

    lines.append("=== Raw Ontology Evidence ===")
    lines.extend(_format_raw_buckets(raw_ontology_items))
    lines.append("")

    lines.append("=== Filtered Ontology Evidence ===")
    lines.extend(_format_evidence_buckets(filtered_ontology_evidence))
    lines.append("")

    if keyword_evidence:
        lines.append("=== Keyword Evidence ===")
        lines.extend(_format_evidence_buckets(keyword_evidence))
        lines.append("")

    lines.append("=== Final Span Results ===")
    lines.extend(_format_span_results(span_results))
    lines.append("")

    lines.append("=== Selected Concepts ===")
    lines.extend(_format_selected_concepts(selected_concepts))
    lines.append("")
    return "\n".join(lines)


def _format_candidate(index: int, candidate: PhraseCandidate) -> str:
    return (
        f"{index}. {candidate.text} "
        f"(normalized={candidate.normalized_text}, kind={candidate.kind}, span={candidate.start}:{candidate.end})"
    )


def _format_raw_buckets(raw_ontology_items: Mapping[str, Sequence[Mapping[str, object]]]) -> List[str]:
    lines: List[str] = []
    for surface, items in raw_ontology_items.items():
        if not items:
            lines.append(f"- {surface}: none")
            continue
        lines.append(f"- {surface}:")
        for item in items:
            lines.append(
                "  "
                + f"{item.get('source', '-')}:"
                + f"{item.get('concept_id', '-')}:"
                + f"{item.get('canonical', '-')}:"
                + f"{float(item.get('confidence', 0.0)):.3f}"
            )
    if not lines:
        lines.append("(none)")
    return lines


def _format_evidence_buckets(evidence_by_surface: Mapping[str, Sequence[ConceptMatchEvidence]]) -> List[str]:
    lines: List[str] = []
    for surface, items in evidence_by_surface.items():
        if not items:
            lines.append(f"- {surface}: none")
            continue
        lines.append(f"- {surface}:")
        for evidence in items:
            lines.append(
                "  "
                + f"{evidence.source}:{evidence.concept_id or '-'}:{evidence.canonical}:{evidence.confidence:.3f}"
                + f" filter_status={evidence.payload.get('filter_status', '-')}"
                + f" filter_reason={evidence.payload.get('filter_reason', '-')}"
            )
    if not lines:
        lines.append("(none)")
    return lines


def _format_span_results(span_results: Sequence[SpanMatchResult]) -> List[str]:
    lines: List[str] = []
    for result in span_results:
        if not result.evidence:
            lines.append(
                f"- {result.candidate.normalized_text} "
                f"(kind={result.candidate.kind}, span={result.candidate.start}:{result.candidate.end}) -> none"
            )
            continue
        summary = "; ".join(
            f"{item.source}:{item.concept_id or '-'}:{item.canonical}:{item.confidence:.3f}"
            for item in result.evidence
        )
        lines.append(
            f"- {result.candidate.normalized_text} "
            f"(kind={result.candidate.kind}, span={result.candidate.start}:{result.candidate.end}) -> {summary}"
        )
    if not lines:
        lines.append("(none)")
    return lines


def _format_selected_concepts(selected_concepts: Sequence[object]) -> List[str]:
    lines: List[str] = []
    for concept in selected_concepts:
        evidence = concept.primary_evidence
        candidate = concept.candidate
        lines.append(
            f"- {candidate.normalized_text} "
            f"(span={candidate.start}:{candidate.end}, source={evidence.source}, "
            f"concept_id={evidence.concept_id or '-'}, canonical={evidence.canonical})"
        )
    if not lines:
        lines.append("(none)")
    return lines


def candidate_label(candidate: PhraseCandidate) -> str:
    return (
        f"{candidate.normalized_text} "
        f"(kind={candidate.kind}, span={candidate.start}:{candidate.end})"
    )


def build_final_results(
    expanded_candidates: Sequence[PhraseCandidate],
    ontology_evidence: Mapping[str, Sequence[ConceptMatchEvidence]],
    keyword_matcher: Optional[KeywordSurfaceSpanMatcher],
) -> tuple[List[SpanMatchResult], List[object], Dict[str, List[ConceptMatchEvidence]]]:
    keyword_buckets: Dict[str, List[ConceptMatchEvidence]] = {}
    keyword_items = keyword_matcher.match_many(expanded_candidates) if keyword_matcher is not None else [[] for _ in expanded_candidates]
    composite = CompositeSpanMatcher([])
    span_results: List[SpanMatchResult] = []
    for candidate, keyword_evidence in zip(expanded_candidates, keyword_items):
        label = candidate_label(candidate)
        bucket = list(ontology_evidence.get(label, [])) + list(keyword_evidence)
        bucket.sort(key=composite._sort_key)
        keyword_buckets[label] = list(keyword_evidence)
        span_results.append(SpanMatchResult(candidate=candidate, evidence=bucket))
    selector = MaximalConceptSelector()
    selected = selector.select(span_results)
    return span_results, selected, keyword_buckets


def run_trace(args: argparse.Namespace, query: str) -> str:
    analyzer = build_analyzer(args)
    keyword_matcher = build_keyword_matcher(args)
    ontology_matcher = RemoteOntologySpanMatcher(
        base_url=args.ontology_linker_url,
        sources=parse_csv(args.ontology_source_list),
        top_k=args.ontology_top_k,
        threshold=args.ontology_threshold,
    )
    executor = SpanMatcherExecutor(
        matcher=CompositeSpanMatcher([ontology_matcher] + ([keyword_matcher] if keyword_matcher else [])),
        include_subphrases=not args.no_subphrase_ngram,
    )

    normalized = analyzer.normalizer.normalize_query(query)
    scispacy_doc = None
    if analyzer.scispacy_pipeline is not None and normalized.normalized_query:
        scispacy_doc = analyzer.scispacy_pipeline(normalized.normalized_query)
    extractor_candidates = analyzer.extractor.extract(
        normalized.normalized_query,
        scispacy_doc=scispacy_doc,
    )
    expanded_candidates = executor.expand_candidates(extractor_candidates)
    raw_ontology_items, filtered_ontology_evidence = collect_ontology_trace(ontology_matcher, expanded_candidates)
    span_results, selected_concepts, keyword_evidence = build_final_results(
        expanded_candidates,
        filtered_ontology_evidence,
        keyword_matcher,
    )
    return render_trace_report(
        query=query,
        normalized_query=normalized.normalized_query,
        extractor_candidates=extractor_candidates,
        expanded_candidates=expanded_candidates,
        raw_ontology_items=raw_ontology_items,
        filtered_ontology_evidence=filtered_ontology_evidence,
        keyword_evidence=keyword_evidence,
        span_results=span_results,
        selected_concepts=selected_concepts,
    )


def interactive_loop(args: argparse.Namespace) -> int:
    print("SpanMatcher trace. Type q, quit, or exit to stop.")
    while True:
        try:
            query = input("\nquery> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if query.lower() in {"q", "quit", "exit"}:
            return 0
        if not query:
            continue
        print(run_trace(args, query))


def main() -> int:
    args = parse_args()
    if args.query:
        for query in args.query:
            print(run_trace(args, query.strip()))
        return 0
    return interactive_loop(args)


if __name__ == "__main__":
    raise SystemExit(main())
