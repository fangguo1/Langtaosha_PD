"""Sparse vector encoders for Tencent VectorDB BM25 search."""

from __future__ import annotations

from typing import Any, Iterable, List, Optional, Sequence


SparseVector = List[List[float]]


class SparseEncoderError(RuntimeError):
    """Raised when sparse vector encoding fails."""


class BM25SparseEncoder:
    """Small wrapper around Tencent's tcvdb-text BM25 encoder.

    Tencent VectorDB uses different BM25 calls for indexing documents and
    encoding queries. Keeping that detail behind this wrapper prevents callers
    from accidentally using document encoding for query-time sparse search.
    """

    def __init__(
        self,
        language: str = "en",
        max_non_zero: int = 1024,
        encoder: Optional[Any] = None,
    ) -> None:
        self.language = language or "en"
        self.max_non_zero = max_non_zero
        self._encoder = encoder or self._load_encoder(self.language)

    @staticmethod
    def _load_encoder(language: str) -> Any:
        try:
            from tcvdb_text.encoder import BM25Encoder
        except ImportError as exc:
            raise SparseEncoderError(
                "缺少稀疏向量依赖 tcvdb-text。请先安装 requirements.txt 中的 tcvdb-text。"
            ) from exc

        try:
            return BM25Encoder.default(language)
        except Exception as exc:
            raise SparseEncoderError(f"初始化 BM25Encoder 失败: {exc}") from exc

    def encode_document(self, text: str) -> SparseVector:
        """Encode one document text for sparse-vector upsert."""
        if not text or not text.strip():
            return []

        try:
            encoded = self._encoder.encode_texts([text])
        except Exception as exc:
            raise SparseEncoderError(f"BM25 document encoding failed: {exc}") from exc

        if not encoded:
            return []
        return self._normalize_sparse_vector(encoded[0])

    def encode_documents(self, texts: Sequence[str]) -> List[SparseVector]:
        """Encode many document texts for sparse-vector upsert."""
        if not texts:
            return []

        encoded_results: List[SparseVector] = []
        pending_positions: List[int] = []
        pending_texts: List[str] = []

        for text in texts:
            encoded_results.append([])
            if text and text.strip():
                pending_positions.append(len(encoded_results) - 1)
                pending_texts.append(text)

        if not pending_texts:
            return encoded_results

        try:
            encoded = self._encoder.encode_texts(pending_texts)
        except Exception as exc:
            raise SparseEncoderError(f"BM25 batch document encoding failed: {exc}") from exc

        for position, sparse_vector in zip(pending_positions, encoded):
            encoded_results[position] = self._normalize_sparse_vector(sparse_vector)

        return encoded_results

    def encode_query(self, query: str) -> SparseVector:
        """Encode one query for sparse-vector search."""
        if not query or not query.strip():
            return []

        encode_queries = getattr(self._encoder, "encode_queries", None)
        if encode_queries is None:
            # Some Tencent docs contain a historical typo. This keeps the wrapper
            # tolerant without exposing the typo to callers.
            encode_queries = getattr(self._encoder, "encode_quires", None)
        if encode_queries is None:
            raise SparseEncoderError("BM25 encoder does not provide encode_queries")

        try:
            encoded = encode_queries(query)
        except Exception as exc:
            raise SparseEncoderError(f"BM25 query encoding failed: {exc}") from exc

        return self._normalize_encoded_query(encoded)

    def _normalize_encoded_query(self, encoded: Any) -> SparseVector:
        if self._looks_like_sparse_vector(encoded):
            return self._normalize_sparse_vector(encoded)

        if isinstance(encoded, Sequence) and encoded:
            first = encoded[0]
            if self._looks_like_sparse_vector(first):
                return self._normalize_sparse_vector(first)

        return self._normalize_sparse_vector(encoded)

    def _normalize_sparse_vector(self, sparse_vector: Any) -> SparseVector:
        pairs: SparseVector = []
        for item in self._iter_sparse_pairs(sparse_vector):
            if len(item) != 2:
                continue
            index, weight = item
            try:
                pairs.append([int(index), float(weight)])
            except (TypeError, ValueError):
                continue

        if len(pairs) > self.max_non_zero:
            pairs = sorted(pairs, key=lambda pair: abs(pair[1]), reverse=True)[: self.max_non_zero]

        return pairs

    @staticmethod
    def _looks_like_sparse_vector(value: Any) -> bool:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            return False
        if not value:
            return True
        first = value[0]
        return (
            isinstance(first, Sequence)
            and not isinstance(first, (str, bytes))
            and len(first) == 2
            and not isinstance(first[0], Sequence)
        )

    @staticmethod
    def _iter_sparse_pairs(sparse_vector: Any) -> Iterable[Any]:
        if sparse_vector is None:
            return []

        if isinstance(sparse_vector, dict):
            if "indices" in sparse_vector and "values" in sparse_vector:
                return zip(sparse_vector["indices"], sparse_vector["values"])
            if "sparse_vector" in sparse_vector:
                return sparse_vector["sparse_vector"]

        if hasattr(sparse_vector, "to_list"):
            return sparse_vector.to_list()
        if hasattr(sparse_vector, "tolist"):
            return sparse_vector.tolist()

        return sparse_vector
