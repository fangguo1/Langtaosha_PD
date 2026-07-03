from __future__ import annotations

from src.docset_hub.evaluation.feedback_importer import (
    FeedbackRecord,
    TargetIdentityIndex,
    resolve_document_identity,
    resolve_feedback_with_report,
    select_topic_feedback,
)


def _feedback(
    *,
    route: str | None = "vector",
    work_id: str = "W1",
    label: int = 1,
    annotator: str = "user-a",
    event_id: int = 1,
    created_at: str = "2026-06-08T10:00:00+08:00",
    doi: str | None = None,
    source_name: str | None = None,
    source_record_id: str | None = None,
) -> FeedbackRecord:
    return FeedbackRecord(
        query_text="synapse",
        normalized_query="synapse",
        route=route,
        feedback_label=label,
        annotator_id=annotator,
        source_event_id=event_id,
        source_search_event_id=100,
        created_at=created_at,
        origin_environment="mimic",
        origin_work_id=work_id,
        origin_rank=1,
        origin_search_mode="study",
        origin_search_query="synapse",
        doi=doi,
        source_name=source_name,
        source_record_id=source_record_id,
        title="Synapse signaling",
    )


def test_importer_excludes_author_and_unknown_routes_by_default():
    selected = select_topic_feedback(
        [
            _feedback(route="vector", work_id="W1"),
            _feedback(route="metadata_author", work_id="W2"),
            _feedback(route=None, work_id="W3"),
        ]
    )

    assert [row.origin_work_id for row in selected] == ["W1"]


def test_latest_feedback_wins_per_annotator_then_majority_vote_wins():
    resolved, report = resolve_feedback_with_report(
        [
            _feedback(annotator="a", work_id="W1", label=1, event_id=1, created_at="2026-06-08T10:00:00+08:00"),
            _feedback(annotator="a", work_id="W1", label=0, event_id=2, created_at="2026-06-08T10:05:00+08:00"),
            _feedback(annotator="b", work_id="W1", label=0, event_id=3, created_at="2026-06-08T10:02:00+08:00"),
        ]
    )

    assert len(resolved) == 1
    assert resolved[0].relevance == 0
    assert resolved[0].annotator_count == 2
    assert resolved[0].annotator_ids == ("a", "b")
    assert report.conflict_count == 0


def test_tied_participant_votes_are_excluded_as_conflict():
    resolved, report = resolve_feedback_with_report(
        [
            _feedback(annotator="a", work_id="W1", label=1),
            _feedback(annotator="b", work_id="W1", label=0, event_id=2),
        ]
    )

    assert resolved == []
    assert report.conflict_count == 1


def test_identity_resolution_prefers_exact_work_id_then_doi_then_source_identity():
    index = TargetIdentityIndex(
        existing_work_ids={"MIMIC-W1"},
        doi_to_work_ids={"10.1/example": ["USE-W2"]},
        source_identity_to_work_ids={("biorxiv_history", "bio-3"): ["USE-W3"]},
    )

    exact = resolve_document_identity(_feedback(work_id="MIMIC-W1"), index)
    by_doi = resolve_document_identity(_feedback(work_id="MIMIC-W2", doi="10.1/example"), index)
    by_source = resolve_document_identity(
        _feedback(work_id="MIMIC-W3", source_name="biorxiv_history", source_record_id="bio-3"),
        index,
    )

    assert (exact.resolved_work_id, exact.match_type) == ("MIMIC-W1", "exact_work_id")
    assert (by_doi.resolved_work_id, by_doi.match_type) == ("USE-W2", "doi")
    assert (by_source.resolved_work_id, by_source.match_type) == ("USE-W3", "source_identity")


def test_identity_resolution_excludes_ambiguous_and_title_only_matches():
    ambiguous_index = TargetIdentityIndex(doi_to_work_ids={"10.1/example": ["USE-W1", "USE-W2"]})
    unresolved_index = TargetIdentityIndex(title_to_work_ids={"Synapse signaling": ["USE-W9"]})

    ambiguous = resolve_document_identity(_feedback(work_id="MIMIC-W4", doi="10.1/example"), ambiguous_index)
    title_only = resolve_document_identity(_feedback(work_id="MIMIC-W5"), unresolved_index)

    assert ambiguous.resolved_work_id is None
    assert ambiguous.match_type == "ambiguous"
    assert title_only.resolved_work_id is None
    assert title_only.match_type == "unresolved"
