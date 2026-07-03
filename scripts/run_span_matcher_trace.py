#!/usr/bin/env python3
"""Trace SpanMatcher step-by-step against the ontology linker service."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SRC_ROOT))

from docset_hub.indexing.query_phrase_analyzer import (  # noqa: E402
    PhraseCandidate,
)
from docset_hub.indexing.query_semantic_plan import QuerySemanticPlan  # noqa: E402
from docset_hub.indexing.span_matcher import (  # noqa: E402
    ConceptMatchEvidence,
    SpanMatchResult,
)
from docset_hub.indexing.span_matcher_pipeline import (  # noqa: E402
    SpanMatcherPipeline,
    SpanMatcherProfile,
    SpanMatcherTrace,
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
        default=float(os.environ.get("ONTOLOGY_THRESHOLD", "0.9")),
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
    semantic_plan: QuerySemanticPlan,
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

    lines.append("=== Query Semantic Plan ===")
    lines.extend(_format_semantic_plan(semantic_plan))
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


def _format_semantic_plan(semantic_plan: QuerySemanticPlan) -> List[str]:
    lines: List[str] = []
    for span in semantic_plan.spans:
        lines.append(
            f"- {span.span_id}: {span.surface_text} "
            f"(canonical={span.canonical_text}, span={span.start}:{span.end})"
        )
        lines.append(f"  own.tier1={_format_semantic_terms(span.own_terms.tier1)}")
        lines.append(f"  own.tier2={_format_semantic_terms(span.own_terms.tier2)}")
        if not span.children:
            lines.append("  children=-")
            continue
        lines.append("  children:")
        for child in span.children:
            lines.append(
                f"    - {child.span_id}: {child.surface_text} "
                f"(canonical={child.canonical_text}, span={child.start}:{child.end})"
            )
            lines.append(f"      own.tier1={_format_semantic_terms(child.own_terms.tier1)}")
            lines.append(f"      own.tier2={_format_semantic_terms(child.own_terms.tier2)}")
    if not lines:
        lines.append("(none)")
    return lines


def _format_semantic_terms(terms: Sequence[object]) -> str:
    if not terms:
        return "-"
    formatted: List[str] = []
    for term in terms:
        formatted.append(f"{getattr(term, 'text', '-')} [{getattr(term, 'match_mode', 'exact')}]")
    return ", ".join(formatted)


def candidate_label(candidate: PhraseCandidate) -> str:
    return (
        f"{candidate.normalized_text} "
        f"(kind={candidate.kind}, span={candidate.start}:{candidate.end})"
    )


def build_profile(args: argparse.Namespace) -> SpanMatcherProfile:
    factory = SpanMatcherProfile.ontology_plus_keyword if args.use_db_keywords else SpanMatcherProfile.ontology_only
    return factory(
        enable_scispacy=not args.skip_scispacy,
        scispacy_model=args.scispacy_model,
        ontology_base_url=args.ontology_linker_url,
        ontology_sources=tuple(parse_csv(args.ontology_source_list)),
        ontology_top_k=args.ontology_top_k,
        ontology_threshold=args.ontology_threshold,
        paper_sources=tuple(parse_csv(args.paper_source_list)),
        include_subphrases=not args.no_subphrase_ngram,
    )


def run_trace(args: argparse.Namespace, query: str) -> str:
    profile = build_profile(args)
    metadata_db = MetadataDB(config_path=args.config_path) if profile.enable_keyword else None
    result = SpanMatcherPipeline.from_profile(
        profile=profile,
        metadata_db=metadata_db,
    ).run(query, trace=True)
    trace = result.trace or SpanMatcherTrace()
    semantic_plan = result.semantic_plan or QuerySemanticPlan(
        original_query=result.query,
        normalized_query=result.normalized_query,
        spans=[],
    )
    return render_trace_report(
        query=result.query,
        normalized_query=result.normalized_query,
        extractor_candidates=result.extractor_candidates,
        expanded_candidates=result.expanded_candidates,
        raw_ontology_items=trace.raw_ontology_items,
        filtered_ontology_evidence=trace.filtered_ontology_evidence,
        keyword_evidence=trace.keyword_evidence,
        span_results=result.span_results,
        selected_concepts=result.selected_concepts,
        semantic_plan=semantic_plan,
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
