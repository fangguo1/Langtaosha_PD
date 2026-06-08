from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import scripts.backfill_sparse_collections as sparse_backfill
from scripts.backfill_sparse_collections import (
    build_index_text,
    build_sparse_documents,
    load_state,
    parse_sources,
    run_backfill,
    save_state,
)


class FakeMetadataDB:
    pass


class FakeVectorDB:
    allowed_sources = ["biorxiv_history", "langtaosha"]

    def __init__(self):
        self.ensured = []
        self.upserts = []

    def _validate_source(self, source_name):
        if source_name not in self.allowed_sources:
            raise ValueError(source_name)

    def ensure_sparse_collection(self, source_name):
        self.ensured.append(source_name)
        return True

    def add_sparse_documents(self, source_name, documents):
        self.upserts.append((source_name, documents))
        return {"success": True, "document_count": len(documents)}


def _args(**overrides):
    defaults = {
        "config_path": Path("src/config/config_tecent_backend_server_test.yaml"),
        "sources": "biorxiv_history",
        "batch_size": 2,
        "limit": 2,
        "resume": False,
        "state_file": Path("/tmp/sparse-bm25-test-state.json"),
        "dry_run": False,
        "no_progress": True,
    }
    defaults.update(overrides)
    return Namespace(**defaults)


def test_parse_sources():
    assert parse_sources("biorxiv_history, langtaosha") == ["biorxiv_history", "langtaosha"]
    assert parse_sources(None) is None


def test_build_index_text_matches_vectorization_policy():
    assert build_index_text("Title", "Abstract") == {
        "should_index": True,
        "text": "Title\nAbstract",
        "text_type": "abstract",
    }
    assert build_index_text("Title", "") == {
        "should_index": True,
        "text": "Title",
        "text_type": "title",
    }
    assert build_index_text("", "")["should_index"] is False


def test_build_sparse_documents_skips_empty_text():
    batch = build_sparse_documents(
        [
            {
                "paper_id": 1,
                "work_id": "W1",
                "canonical_title": "CRISPR-Cas9",
                "canonical_abstract": "Gene editing",
            },
            {
                "paper_id": 2,
                "work_id": "W2",
                "canonical_title": "",
                "canonical_abstract": "",
            },
        ]
    )

    assert batch["skipped"] == 1
    assert batch["documents"] == [
        {
            "work_id": "W1",
            "paper_id": 1,
            "text": "CRISPR-Cas9\nGene editing",
            "text_type": "abstract",
        }
    ]


def test_run_backfill_dry_run_does_not_write_sparse_collection(tmp_path):
    fake_vector_db = FakeVectorDB()
    state_file = tmp_path / "state.json"

    def fake_fetcher(_metadata_db, source_name, limit, after_paper_id):
        assert source_name == "biorxiv_history"
        assert limit == 1
        assert after_paper_id == 0
        return [
            {
                "paper_id": 1,
                "work_id": "W1",
                "canonical_title": "p53 mutation",
                "canonical_abstract": "Tumor suppressor",
                "source_name": source_name,
            }
        ]

    summary = run_backfill(
        _args(dry_run=True, resume=True, limit=1, state_file=state_file),
        metadata_db=FakeMetadataDB(),
        vector_db=fake_vector_db,
        fetcher=fake_fetcher,
    )

    assert summary.fetched == 1
    assert summary.indexed == 1
    assert fake_vector_db.ensured == []
    assert fake_vector_db.upserts == []
    assert not state_file.exists()
    assert summary.by_source["biorxiv_history"] == {
        "fetched": 1,
        "indexed": 1,
        "skipped": 0,
        "failed": 0,
        "last_paper_id": 1,
    }


def test_save_state_replaces_file_atomically(tmp_path, monkeypatch):
    state_file = tmp_path / "state.json"
    state_file.write_text('{"old": true}\n', encoding="utf-8")
    replacements = []
    real_replace = sparse_backfill.os.replace

    def record_replace(source, destination):
        replacements.append((Path(source), Path(destination)))
        real_replace(source, destination)

    monkeypatch.setattr(sparse_backfill.os, "replace", record_replace)

    save_state(state_file, {"biorxiv_history": {"last_paper_id": 42}})

    assert load_state(state_file) == {"biorxiv_history": {"last_paper_id": 42}}
    assert len(replacements) == 1
    assert replacements[0][0].parent == state_file.parent
    assert replacements[0][1] == state_file
    assert list(tmp_path.glob("*.tmp")) == []


def test_run_backfill_resume_starts_after_saved_paper_id(tmp_path):
    state_file = tmp_path / "state.json"
    save_state(state_file, {"biorxiv_history": {"last_paper_id": 9}})
    seen_after_ids = []

    def fake_fetcher(_metadata_db, source_name, limit, after_paper_id):
        seen_after_ids.append(after_paper_id)
        if after_paper_id > 9:
            return []
        return [
            {
                "paper_id": 10,
                "work_id": "W10",
                "canonical_title": "single-cell RNA-seq",
                "canonical_abstract": "",
                "source_name": source_name,
            }
        ]

    run_backfill(
        _args(resume=True, limit=None, state_file=state_file),
        metadata_db=FakeMetadataDB(),
        vector_db=FakeVectorDB(),
        fetcher=fake_fetcher,
    )

    assert seen_after_ids == [9, 10]
    assert load_state(state_file) == {"biorxiv_history": {"last_paper_id": 10}}


def test_run_backfill_upserts_batches():
    fake_vector_db = FakeVectorDB()

    def fake_fetcher(_metadata_db, source_name, limit, after_paper_id):
        if after_paper_id:
            return []
        return [
            {
                "paper_id": 10,
                "work_id": "W10",
                "canonical_title": "single-cell RNA-seq",
                "canonical_abstract": "",
                "source_name": source_name,
            }
        ]

    summary = run_backfill(
        _args(),
        metadata_db=FakeMetadataDB(),
        vector_db=fake_vector_db,
        fetcher=fake_fetcher,
    )

    assert summary.fetched == 1
    assert summary.indexed == 1
    assert fake_vector_db.ensured == ["biorxiv_history"]
    assert fake_vector_db.upserts == [
        (
            "biorxiv_history",
            [
                {
                    "work_id": "W10",
                    "paper_id": 10,
                    "text": "single-cell RNA-seq",
                    "text_type": "title",
                }
            ],
        )
    ]


def test_run_backfill_updates_tqdm_progress(monkeypatch):
    created_progress = []

    class FakeTqdm:
        def __init__(self, iterable=None, total=None, desc=None, unit=None, dynamic_ncols=None, leave=None):
            self.iterable = iterable
            self.total = total
            self.desc = desc
            self.unit = unit
            self.updates = []
            self.postfixes = []
            self.closed = False
            created_progress.append(self)

        def __iter__(self):
            return iter(self.iterable)

        def update(self, value):
            self.updates.append(value)

        def set_postfix(self, **values):
            self.postfixes.append(values)

        def close(self):
            self.closed = True

    fake_vector_db = FakeVectorDB()

    def fake_fetcher(_metadata_db, source_name, limit, after_paper_id):
        if after_paper_id:
            return []
        return [
            {
                "paper_id": 10,
                "work_id": "W10",
                "canonical_title": "single-cell RNA-seq",
                "canonical_abstract": "",
                "source_name": source_name,
            },
            {
                "paper_id": 11,
                "work_id": "W11",
                "canonical_title": "CRISPR-Cas9",
                "canonical_abstract": "Gene editing",
                "source_name": source_name,
            },
        ]

    monkeypatch.setattr(sparse_backfill, "tqdm", FakeTqdm)

    summary = run_backfill(
        _args(limit=2, no_progress=False),
        metadata_db=FakeMetadataDB(),
        vector_db=fake_vector_db,
        fetcher=fake_fetcher,
    )

    assert summary.fetched == 2
    assert summary.indexed == 2
    assert [progress.desc for progress in created_progress] == [
        "Sparse sources",
        "sparse:biorxiv_history",
    ]
    assert created_progress[1].updates == [2]
    assert created_progress[1].closed is True
    assert created_progress[1].postfixes[-1]["indexed"] == 2
