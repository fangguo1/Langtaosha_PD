from __future__ import annotations

from src.docset_hub.storage.sparse_encoder import BM25SparseEncoder


class FakeBM25Encoder:
    def __init__(self):
        self.text_batches = []
        self.query_inputs = []

    def encode_texts(self, texts):
        self.text_batches.append(list(texts))
        return [
            [(index + 1, 0.5), (index + 2, "0.25")]
            for index, _text in enumerate(texts)
        ]

    def encode_queries(self, query):
        self.query_inputs.append(query)
        return [(101, 0.9), ("102", "0.4")]


def test_encode_document_uses_encode_texts():
    fake = FakeBM25Encoder()
    encoder = BM25SparseEncoder(language="en", encoder=fake)

    sparse_vector = encoder.encode_document("CRISPR-Cas9 gene editing")

    assert fake.text_batches == [["CRISPR-Cas9 gene editing"]]
    assert sparse_vector == [[1, 0.5], [2, 0.25]]


def test_encode_query_uses_encode_queries():
    fake = FakeBM25Encoder()
    encoder = BM25SparseEncoder(language="en", encoder=fake)

    sparse_vector = encoder.encode_query("p53 mutation")

    assert fake.query_inputs == ["p53 mutation"]
    assert sparse_vector == [[101, 0.9], [102, 0.4]]


def test_empty_inputs_return_empty_vectors_without_encoder_calls():
    fake = FakeBM25Encoder()
    encoder = BM25SparseEncoder(language="en", encoder=fake)

    assert encoder.encode_document("   ") == []
    assert encoder.encode_documents(["", "  "]) == [[], []]
    assert encoder.encode_query("") == []
    assert fake.text_batches == []
    assert fake.query_inputs == []


def test_encode_documents_preserves_empty_positions():
    fake = FakeBM25Encoder()
    encoder = BM25SparseEncoder(language="en", encoder=fake)

    sparse_vectors = encoder.encode_documents(["alpha", "", "beta"])

    assert fake.text_batches == [["alpha", "beta"]]
    assert sparse_vectors == [
        [[1, 0.5], [2, 0.25]],
        [],
        [[2, 0.5], [3, 0.25]],
    ]


def test_sparse_vector_is_capped_to_max_non_zero_by_weight():
    fake = FakeBM25Encoder()
    encoder = BM25SparseEncoder(language="en", max_non_zero=2, encoder=fake)

    sparse_vector = encoder._normalize_sparse_vector(
        [(1, 0.1), (2, -0.9), (3, 0.4)]
    )

    assert sparse_vector == [[2, -0.9], [3, 0.4]]
