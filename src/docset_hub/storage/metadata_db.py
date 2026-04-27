"""元数据库操作类"""
import json
import re
from difflib import SequenceMatcher
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError

from config.config_loader import init_config, get_db_engine
from ..metadata.transformer import MetadataTransformer
from ..metadata.utils import generate_work_id

import logging

class MetadataDB:
    """元数据库操作类 - 新架构（多源支持）

    负责将DocSet格式的数据存储到PostgreSQL数据库
    支持多数据库配置（PubMed 和 arXiv）
    """

    def __init__(self, config_path: Optional[Path] = None, db_key: str = 'metadata_db'):
        """初始化元数据库操作器

        Args:
            config_path: 配置文件路径，如果为 None 则自动查找
            db_key: 数据库配置键名，默认为 'metadata_db'
        """
        # 确保配置已初始化
        if config_path is None:
            raise ValueError("未找到配置文件 config.yaml，请指定 config_path")
        init_config(config_path)
        self.db_key = db_key
        self.engine = get_db_engine(db_key=db_key)

        # 新增：缓存 default_sources（source 合法性校验依据）
        from config.config_loader import get_default_sources
        self.default_sources = get_default_sources()
        logging.info(f"✅ MetadataDB 初始化完成，default_sources={self.default_sources}")

    # =========================================================================
    # 新架构方法 - 多源支持
    # =========================================================================

    GENERATED_KEYWORD_SOURCE = "scispacy-en_core_sci_lg-generated"
    ALLOWED_GENERATED_KEYWORD_TYPES = {
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

    @staticmethod
    def normalize_author_name(name: str) -> str:
        """Normalize author names for matching and scoring."""
        if not name:
            return ""
        return " ".join(
            name.lower()
            .replace(",", " ")
            .replace(".", " ")
            .split()
        )

    @classmethod
    def author_match_score(cls, query: str, author_name: str) -> float:
        """Score query against an author candidate in the 0.0-1.0 range."""
        q = cls.normalize_author_name(query)
        a = cls.normalize_author_name(author_name)

        if not q or not a:
            return 0.0
        if q == a:
            return 1.0
        if q in a:
            return min(0.95, 0.75 + 0.20 * len(q) / max(len(a), 1))

        try:
            from rapidfuzz import fuzz
            return float(fuzz.WRatio(q, a)) / 100.0
        except Exception:
            return SequenceMatcher(None, q, a).ratio()

    @staticmethod
    def _normalize_keyword(keyword: str) -> str:
        """Normalize generated keyword text before DB insertion."""
        return re.sub(r"\s+", " ", (keyword or "").strip())

    @staticmethod
    def _source_filter_sql(
        source_list: Optional[List[str]],
        params: Dict[str, Any],
        table_alias: str = "ps",
    ) -> str:
        """Build a SQLAlchemy text() compatible source_name IN filter."""
        if not source_list:
            return ""
        placeholders = []
        for idx, source_name in enumerate(source_list):
            param_name = f"source_filter_{idx}"
            placeholders.append(f":{param_name}")
            params[param_name] = source_name
        return f" AND {table_alias}.source_name IN ({', '.join(placeholders)})"

    @staticmethod
    def _build_write_result(
        mode: str,
        status_code: str,
        match_result: Dict[str, Any],
        apply_action: str,
        apply_reason: Optional[str],
        paper_id: Optional[int],
        paper_source_id: Optional[int],
        canonical_strategy: Optional[str],
        canonical_before: Optional[int],
        canonical_after: Optional[int],
        work_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """构建统一写入返回结果。"""
        return {
            "ok": True,
            "mode": mode,
            "status_code": status_code,
            "paper_id": paper_id,
            "work_id": work_id,
            "paper_source_id": paper_source_id,
            "resolve": {
                "match_type": match_result.get("match_type"),
                "matched_paper_id": match_result.get("paper_id"),
                "matched_paper_source_id": match_result.get("paper_source_id"),
            },
            "apply": {
                "action": apply_action,
                "reason": apply_reason,
            },
            "canonical": {
                "strategy": canonical_strategy,
                "before_canonical_source_id": canonical_before,
                "canonical_source_id": canonical_after,
                "changed": canonical_before != canonical_after
            }
        }

    def _get_canonical_source_id(self, conn: Connection, paper_id: int) -> Optional[int]:
        """查询当前 canonical_source_id。"""
        row = conn.execute(
            text("SELECT canonical_source_id FROM papers WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        ).fetchone()
        return row[0] if row else None

    def _get_work_id_by_paper_id(self, conn: Connection, paper_id: Optional[int]) -> Optional[str]:
        """根据 paper_id 查询 work_id。"""
        if paper_id is None:
            return None
        row = conn.execute(
            text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        ).fetchone()
        return row[0] if row else None

    def insert_paper(
        self,
        db_payload: Dict[str, Any],
        upsert_key: Dict[str, Any]
    ) -> Dict[str, Any]:
        """确保论文存在（如果已存在则按同 source 策略决定覆盖或跳过）。

        Returns:
            Dict[str, Any]: 统一状态返回结构（包含 status_code / paper_id / resolve / apply / canonical）。
        """
        return self._resolve_and_apply(
            db_payload=db_payload,
            upsert_key=upsert_key,
            mode="insert",
            canonical_source_id=None,
            auto_select_canonical=True
        )

    def update_paper(
        self,
        db_payload: Dict[str, Any],
        upsert_key: Dict[str, Any],
        canonical_source_id: Optional[int] = None,
        auto_select_canonical: bool = True
    ) -> Dict[str, Any]:
        """更新已存在的论文（仅同 source 命中时生效）。

        Returns:
            Dict[str, Any]: 统一状态返回结构。非 same_source 命中时返回 reject 状态。
        """
        return self._resolve_and_apply(
            db_payload=db_payload,
            upsert_key=upsert_key,
            mode="update",
            canonical_source_id=canonical_source_id,
            auto_select_canonical=auto_select_canonical
        )

    def upsert_paper(
        self,
        db_payload: Dict[str, Any],
        upsert_key: Dict[str, Any],
        canonical_source_id: Optional[int] = None,
        auto_select_canonical: bool = True
    ) -> Dict[str, Any]:
        """插入或更新论文（命中同 source 时强制更新，未命中时执行插入）。

        Returns:
            Dict[str, Any]: 统一状态返回结构。
        """
        return self._resolve_and_apply(
            db_payload=db_payload,
            upsert_key=upsert_key,
            mode="upsert",
            canonical_source_id=canonical_source_id,
            auto_select_canonical=auto_select_canonical
        )

    def _resolve_and_apply(
        self,
        db_payload: Dict[str, Any],
        upsert_key: Dict[str, Any],
        mode: str,
        canonical_source_id: Optional[int],
        auto_select_canonical: bool
    ) -> Dict[str, Any]:
        """统一写入执行主流程。

        执行顺序：
        1. 校验 source_name 合法性与 payload/upsert_key 一致性。
        2. 调用 `_resolve_match_by_identity` 判断命中类型：
           - `same_source`：同 source 命中（返回 paper_id/paper_source_id/version/online_at）
           - `cross_source`：跨 source 命中（返回 paper_id）
           - `no_match`：未命中
        3. 根据 `mode` 分流执行业务语义：
           - `insert`：同 source 按 version/online_at 策略决定覆盖或跳过；跨 source/未命中执行追加或新建。
           - `update`：仅 `same_source` 命中时更新，否则返回 None。
           - `upsert`：`same_source` 命中强制更新，否则插入。
        4. 写入完成后执行 canonical 规则（自动或手动）。

        Args:
            db_payload: Transformer 输出的标准化 DB payload。
            upsert_key: identity bundle（包含 source_name/source_identifiers/通用标识符）。
            mode: `insert` / `update` / `upsert`。
            canonical_source_id: 用户手动指定 canonical source（可选）。
            auto_select_canonical: 是否按 online_at 自动重算 canonical。

        Returns:
            Dict[str, Any]: 统一状态返回结构。
        """
        from .version_utils import should_update_by_version

        with self.engine.connect() as conn:
            try:
                self._validate_source_consistency(db_payload, upsert_key)
                match_result = self._resolve_match_by_identity(conn, upsert_key)
                match_type = match_result["match_type"]

                if mode == "update":
                    if match_type != "same_source":
                        logging.warning(
                            f"⚠️ 论文不存在，无法更新: source={upsert_key.get('source_name')}, "
                            f"source_identifier={self._get_current_source_identifier(upsert_key)}"
                        )
                        conn.commit()
                        return self._build_write_result(
                            mode="update",
                            status_code="UPDATE_NOT_ALLOWED_NON_SAME_SOURCE",
                            match_result=match_result,
                            apply_action="reject",
                            apply_reason="cross_source_or_no_match",
                            paper_id=None,
                            paper_source_id=None,
                            canonical_strategy=None,
                            canonical_before=None,
                            canonical_after=None,
                            work_id=None
                        )

                    paper_id = match_result["paper_id"]
                    paper_source_id = match_result["paper_source_id"]
                    canonical_before = self._get_canonical_source_id(conn, paper_id)
                    self._apply_source_update(conn, paper_id, paper_source_id, db_payload)
                    self._apply_canonical_strategy(conn, paper_id, canonical_source_id, auto_select_canonical)
                    canonical_after = self._get_canonical_source_id(conn, paper_id)
                    conn.commit()
                    logging.info(f"✅ 更新论文成功: paper_id={paper_id}")
                    canonical_strategy = (
                        "manual"
                        if canonical_source_id is not None
                        else ("auto_online_at" if auto_select_canonical else None)
                    )
                    return self._build_write_result(
                        mode="update",
                        status_code="UPDATE_SAME_SOURCE",
                        match_result=match_result,
                        apply_action="update",
                        apply_reason="same_source_update",
                        paper_id=paper_id,
                        paper_source_id=paper_source_id,
                        canonical_strategy=canonical_strategy,
                        canonical_before=canonical_before,
                        canonical_after=canonical_after,
                        work_id=self._get_work_id_by_paper_id(conn, paper_id)
                    )

                if match_type == "same_source":
                    paper_id = match_result["paper_id"]
                    paper_source_id = match_result["paper_source_id"]
                    canonical_before = self._get_canonical_source_id(conn, paper_id)

                    if mode == "upsert":
                        self._apply_source_update(conn, paper_id, paper_source_id, db_payload)
                        self._apply_canonical_strategy(conn, paper_id, canonical_source_id, auto_select_canonical)
                        canonical_after = self._get_canonical_source_id(conn, paper_id)
                        conn.commit()
                        logging.info(f"✅ upsert 更新论文成功: paper_id={paper_id}")
                        canonical_strategy = (
                            "manual"
                            if canonical_source_id is not None
                            else ("auto_online_at" if auto_select_canonical else None)
                        )
                        return self._build_write_result(
                            mode="upsert",
                            status_code="UPSERT_UPDATE_SAME_SOURCE",
                            match_result=match_result,
                            apply_action="update",
                            apply_reason="same_source_force_update",
                            paper_id=paper_id,
                            paper_source_id=paper_source_id,
                            canonical_strategy=canonical_strategy,
                            canonical_before=canonical_before,
                            canonical_after=canonical_after,
                            work_id=self._get_work_id_by_paper_id(conn, paper_id)
                        )

                    new_version = db_payload.get("paper_sources", {}).get("version")
                    new_online_at = db_payload.get("paper_sources", {}).get("online_at")
                    should_update, reason = should_update_by_version(
                        new_version,
                        match_result.get("version"),
                        new_online_at,
                        match_result.get("online_at")
                    )
                    if should_update:
                        self._apply_source_update(conn, paper_id, paper_source_id, db_payload)
                        self._set_canonical_source_by_online_at(conn, paper_id)
                        canonical_after = self._get_canonical_source_id(conn, paper_id)
                        conn.commit()
                        logging.info(f"✅ insert 覆盖现有论文成功: paper_id={paper_id}, reason={reason}")
                        return self._build_write_result(
                            mode="insert",
                            status_code="INSERT_UPDATE_SAME_SOURCE",
                            match_result=match_result,
                            apply_action="update",
                            apply_reason=reason,
                            paper_id=paper_id,
                            paper_source_id=paper_source_id,
                            canonical_strategy="auto_online_at",
                            canonical_before=canonical_before,
                            canonical_after=canonical_after,
                            work_id=self._get_work_id_by_paper_id(conn, paper_id)
                        )

                    # insert 场景下同 source 命中但不覆盖：保持幂等，返回现有 paper_id
                    self._set_canonical_source_by_online_at(conn, paper_id)
                    canonical_after = self._get_canonical_source_id(conn, paper_id)
                    conn.commit()
                    logging.info(f"✅ 论文已存在，跳过更新: paper_id={paper_id}, reason={reason}")
                    return self._build_write_result(
                        mode="insert",
                        status_code="INSERT_SKIP_SAME_SOURCE",
                        match_result=match_result,
                        apply_action="skip",
                        apply_reason=reason,
                        paper_id=paper_id,
                        paper_source_id=paper_source_id,
                        canonical_strategy="auto_online_at",
                        canonical_before=canonical_before,
                        canonical_after=canonical_after,
                        work_id=self._get_work_id_by_paper_id(conn, paper_id)
                    )

                # cross_source / no_match 都走新增 source 记录
                status_code = "INSERT_NEW_PAPER"
                if match_type == "cross_source":
                    paper_id = match_result["paper_id"]
                    status_code = "INSERT_APPEND_SOURCE" if mode == "insert" else "UPSERT_APPEND_SOURCE"
                    logging.info(f"🔗 跨 source 匹配到现有论文: paper_id={paper_id}")
                else:
                    status_code = "INSERT_NEW_PAPER" if mode == "insert" else "UPSERT_NEW_PAPER"
                    paper_id = self._get_or_create_paper_from_payload(conn, db_payload)

                canonical_before = self._get_canonical_source_id(conn, paper_id)
                paper_source_id = self._insert_source_record_from_payload(conn, paper_id, db_payload)
                self._apply_insert_side_effects(conn, paper_id, paper_source_id, db_payload)
                self._set_canonical_source_by_online_at(conn, paper_id)
                canonical_after = self._get_canonical_source_id(conn, paper_id)
                conn.commit()

                if mode == "upsert":
                    logging.info(f"✅ upsert 插入论文成功: paper_id={paper_id}")
                else:
                    logging.info(f"✅ 插入新论文成功: paper_id={paper_id}")
                return self._build_write_result(
                    mode=mode,
                    status_code=status_code,
                    match_result=match_result,
                    apply_action="insert",
                    apply_reason="cross_source_append" if match_type == "cross_source" else "no_match_insert",
                    paper_id=paper_id,
                    paper_source_id=paper_source_id,
                    canonical_strategy="auto_online_at",
                    canonical_before=canonical_before,
                    canonical_after=canonical_after,
                    work_id=self._get_work_id_by_paper_id(conn, paper_id)
                )
            except Exception as e:
                conn.rollback()
                logging.error(f"❌ {mode} 论文失败: {str(e)}")
                raise e

    def _resolve_match_by_identity(
        self,
        conn: Connection,
        upsert_key: Dict[str, Any]
    ) -> Dict[str, Any]:
        """基于 identity bundle 一次性完成同 source 与跨 source 判定。

        判定策略：
        1. 先查 `same_source`：在相同 source 下，使用 source_record_id/doi/arxiv_id/
           pubmed_id/semantic_scholar_id 任一命中。
        2. 再查 `cross_source`：仅使用通用标识符（doi/arxiv_id/pubmed_id/
           semantic_scholar_id）在不同 source 中匹配。
        3. 若都未命中，返回 `no_match`。

        Returns:
            Dict[str, Any]:
            - `{"match_type": "same_source", "paper_id": ..., "paper_source_id": ..., "version": ..., "online_at": ...}`
            - `{"match_type": "cross_source", "paper_id": ...}`
            - `{"match_type": "no_match"}`
        """
        source_name = upsert_key.get("source_name")
        source_identifiers = upsert_key.get("source_identifiers", {}) or {}
        self._validate_source_name(source_name)

        # 1) 先查同 source
        same_source_conditions = []
        same_source_params = {"source_name": source_name}

        current_source_id = source_identifiers.get(source_name)
        if current_source_id:
            same_source_conditions.append("ps.source_record_id = :source_record_id")
            same_source_params["source_record_id"] = current_source_id

        if upsert_key.get("doi"):
            same_source_conditions.append("ps.doi = :doi")
            same_source_params["doi"] = upsert_key["doi"]
        if upsert_key.get("arxiv_id"):
            same_source_conditions.append("ps.arxiv_id = :arxiv_id")
            same_source_params["arxiv_id"] = upsert_key["arxiv_id"]
        if upsert_key.get("pubmed_id"):
            same_source_conditions.append("ps.pubmed_id = :pubmed_id")
            same_source_params["pubmed_id"] = upsert_key["pubmed_id"]
        if upsert_key.get("semantic_scholar_id"):
            same_source_conditions.append("ps.semantic_scholar_id = :semantic_scholar_id")
            same_source_params["semantic_scholar_id"] = upsert_key["semantic_scholar_id"]

        if same_source_conditions:
            same_source_query = f"""
                SELECT ps.paper_id, ps.paper_source_id, ps.version, ps.online_at
                FROM paper_sources ps
                WHERE ps.source_name = :source_name
                AND ({' OR '.join(same_source_conditions)})
                LIMIT 1
            """
            row = conn.execute(text(same_source_query), same_source_params).fetchone()
            if row:
                return {
                    "match_type": "same_source",
                    "paper_id": row[0],
                    "paper_source_id": row[1],
                    "version": row[2],
                    "online_at": row[3]
                }

        # 2) 再查跨 source（仅通用标识符参与）
        cross_conditions = []
        cross_params = {"source_name": source_name}
        for field_name in ("doi", "arxiv_id", "pubmed_id", "semantic_scholar_id"):
            field_value = upsert_key.get(field_name)
            if field_value:
                cross_conditions.append(f"ps.{field_name} = :{field_name}")
                cross_params[field_name] = field_value

        if cross_conditions:
            cross_query = f"""
                SELECT DISTINCT ps.paper_id
                FROM paper_sources ps
                WHERE ps.source_name != :source_name
                AND ({' OR '.join(cross_conditions)})
                LIMIT 1
            """
            row = conn.execute(text(cross_query), cross_params).fetchone()
            if row:
                return {"match_type": "cross_source", "paper_id": row[0]}

        return {"match_type": "no_match"}

    def _apply_source_update(
        self,
        conn: Connection,
        paper_id: int,
        paper_source_id: int,
        db_payload: Dict[str, Any]
    ) -> None:
        """更新同 source 记录及关联数据。"""
        self._update_source_record_from_payload(conn, paper_source_id, db_payload)
        conn.execute(text("DELETE FROM paper_author_affiliation WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        conn.execute(text("DELETE FROM paper_keywords WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        conn.execute(text("DELETE FROM paper_references WHERE paper_id = :paper_id"), {"paper_id": paper_id})
        self._insert_author_affiliation_from_payload(conn, paper_id, db_payload)
        self._insert_keywords_from_payload(conn, paper_id, db_payload)
        self._insert_references_from_payload(conn, paper_id, paper_source_id, db_payload)
        self._upsert_source_metadata_from_payload(conn, paper_source_id, db_payload)

    def _apply_insert_side_effects(
        self,
        conn: Connection,
        paper_id: int,
        paper_source_id: int,
        db_payload: Dict[str, Any]
    ) -> None:
        """插入 source 后的关联写入。"""
        self._insert_author_affiliation_from_payload(conn, paper_id, db_payload)
        self._insert_keywords_from_payload(conn, paper_id, db_payload)
        self._insert_references_from_payload(conn, paper_id, paper_source_id, db_payload)
        self._upsert_source_metadata_from_payload(conn, paper_source_id, db_payload)

    def _apply_canonical_strategy(
        self,
        conn: Connection,
        paper_id: int,
        canonical_source_id: Optional[int],
        auto_select_canonical: bool
    ) -> None:
        """canonical 选择策略：用户指定优先，否则可选自动。"""
        if canonical_source_id is not None:
            self._set_canonical_source_by_user(conn, paper_id, canonical_source_id)
        elif auto_select_canonical:
            self._set_canonical_source_by_online_at(conn, paper_id)

    def _get_current_source_identifier(self, upsert_key: Dict[str, Any]) -> Optional[str]:
        """从 identity bundle 中提取当前 source 的标识符（用于日志）。"""
        source_name = upsert_key.get("source_name")
        source_identifiers = upsert_key.get("source_identifiers", {}) or {}
        return source_identifiers.get(source_name)

    def _validate_source_name(self, source_name: str) -> None:
        """校验 source_name 是否合法

        Args:
            source_name: 待校验的 source 名称

        Raises:
            ValueError: 在以下情况下抛出：
                - source_name 不是字符串
                - source_name 为空字符串
                - source_name 不在 default_sources 中
        """
        # 1. 类型检查
        if not isinstance(source_name, str):
            raise ValueError(
                f"source_name 必须是字符串类型，当前类型: {type(source_name)}, 值: {source_name}"
            )

        # 2. 非空检查
        if not source_name or not source_name.strip():
            raise ValueError("source_name 不能为空字符串")

        # 3. 合法性检查（是否在 default_sources 中）
        if source_name not in self.default_sources:
            raise ValueError(
                f"source_name '{source_name}' 不在 default_sources 中。"
                f"合法的 sources: {self.default_sources}"
            )

    def _validate_source_consistency(
        self,
        db_payload: Dict[str, Any],
        upsert_key: Dict[str, Any]
    ) -> str:
        """校验 db_payload 和 upsert_key 中的 source_name 一致性

        Args:
            db_payload: 数据库 payload（从 MetadataTransformer 获取）
            upsert_key: upsert 键（从 MetadataTransformer 获取）

        Returns:
            str: 校验通过的 source_name

        Raises:
            ValueError: 在以下情况下抛出：
                - upsert_key 缺少 source_name
                - db_payload.paper_sources 缺少 source_name
                - 两者 source_name 值不一致
                - source_name 不合法
        """
        # 1. 提取 source_name
        upsert_source_name = upsert_key.get('source_name')
        paper_sources_data = db_payload.get('paper_sources', {})
        payload_source_name = paper_sources_data.get('source_name')

        # 2. 检查存在性
        if not upsert_source_name:
            raise ValueError("upsert_key 缺少 source_name 字段")

        if not payload_source_name:
            raise ValueError("db_payload.paper_sources 缺少 source_name 字段")

        # 3. 检查一致性
        if upsert_source_name != payload_source_name:
            raise ValueError(
                f"source_name 不一致: upsert_key.source_name='{upsert_source_name}', "
                f"db_payload.paper_sources.source_name='{payload_source_name}'"
            )

        # 4. 校验 source_name 合法性
        self._validate_source_name(upsert_source_name)

        # 5. 返回校验通过的 source_name
        return upsert_source_name

    # =========================================================================
    # 辅助方法
    # =========================================================================

    def _set_canonical_source_by_online_at(
        self,
        conn: Connection,
        paper_id: int
    ) -> None:
        """根据 online_at 时间法则设置 canonical_source

        法则: 选择 online_at 最晚的 source 作为 canonical

        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文 ID
        """
        result = conn.execute(
            text("""
                UPDATE papers
                SET canonical_source_id = subquery.paper_source_id
                FROM (
                    SELECT paper_source_id
                    FROM paper_sources
                    WHERE paper_id = :paper_id
                    AND online_at IS NOT NULL
                    ORDER BY online_at DESC  -- 最晚的排在前面
                    LIMIT 1
                ) AS subquery
                WHERE papers.paper_id = :paper_id
            """),
            {"paper_id": paper_id}
        )

    def _set_canonical_source_by_user(
        self,
        conn: Connection,
        paper_id: int,
        canonical_source_id: int
    ) -> None:
        """根据用户指定设置 canonical_source

        验证: canonical_source_id 必须属于该 paper_id

        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文 ID
            canonical_source_id: 用户指定的 source ID

        Raises:
            ValueError: canonical_source_id 不属于该 paper_id
        """
        # 验证 canonical_source_id 是否属于该 paper_id
        result = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM paper_sources
                WHERE paper_source_id = :canonical_source_id
                AND paper_id = :paper_id
            """),
            {
                "canonical_source_id": canonical_source_id,
                "paper_id": paper_id
            }
        )
        count = result.scalar()

        if count == 0:
            raise ValueError(
                f"canonical_source_id={canonical_source_id} "
                f"不属于 paper_id={paper_id}"
            )

        # 设置 canonical_source
        conn.execute(
            text("""
                UPDATE papers
                SET canonical_source_id = :canonical_source_id
                WHERE paper_id = :paper_id
            """),
            {
                "canonical_source_id": canonical_source_id,
                "paper_id": paper_id
            }
        )

    def _get_or_create_paper_from_payload(
        self,
        conn: Connection,
        db_payload: Dict[str, Any]
    ) -> int:
        """创建新的 papers 记录

        Args:
            conn: SQLAlchemy 连接对象
            db_payload: 数据库 payload

        Returns:
            int: paper_id
        """
        papers_data = db_payload.get('papers', {})
        work_id = papers_data.get('work_id') or generate_work_id()

        result = conn.execute(
            text("""
                INSERT INTO papers (
                    work_id, canonical_title, canonical_abstract, canonical_language,
                    canonical_publisher, submitted_at, online_at, published_at,
                    created_at, updated_at
                ) VALUES (
                    :work_id, :canonical_title, :canonical_abstract, :canonical_language,
                    :canonical_publisher, :submitted_at, :online_at, :published_at,
                    :created_at, :updated_at
                )
                RETURNING paper_id
            """),
            {
                "work_id": work_id,
                "canonical_title": papers_data.get('canonical_title'),
                "canonical_abstract": papers_data.get('canonical_abstract'),
                "canonical_language": papers_data.get('canonical_language'),
                "canonical_publisher": papers_data.get('canonical_publisher'),
                "submitted_at": papers_data.get('submitted_at'),
                "online_at": papers_data.get('online_at'),
                "published_at": papers_data.get('published_at'),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        )

        return result.scalar()

    def _insert_source_record_from_payload(
        self,
        conn: Connection,
        paper_id: int,
        db_payload: Dict[str, Any]
    ) -> int:
        """插入新的 paper_sources 记录

        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文 ID
            db_payload: 数据库 payload

        Returns:
            int: paper_source_id
        """
        sources_data = db_payload.get('paper_sources', {})

        # 新增：兜底校验 source_name 合法性
        source_name = sources_data.get('source_name')
        self._validate_source_name(source_name)

        result = conn.execute(
            text("""
                INSERT INTO paper_sources (
                    paper_id, source_name, platform, source_record_id,
                    source_url, abstract_url, pdf_url, title, abstract,
                    publisher, language, doi, arxiv_id, pubmed_id,
                    semantic_scholar_id, submitted_at, online_at, published_at,
                    updated_at_source, version, is_preprint, is_published,
                    created_at, updated_at
                ) VALUES (
                    :paper_id, :source_name, :platform, :source_record_id,
                    :source_url, :abstract_url, :pdf_url, :title, :abstract,
                    :publisher, :language, :doi, :arxiv_id, :pubmed_id,
                    :semantic_scholar_id, :submitted_at, :online_at, :published_at,
                    :updated_at_source, :version, :is_preprint, :is_published,
                    :created_at, :updated_at
                )
                RETURNING paper_source_id
            """),
            {
                "paper_id": paper_id,
                "source_name": sources_data.get('source_name'),
                "platform": sources_data.get('platform'),
                "source_record_id": sources_data.get('source_record_id'),
                "source_url": sources_data.get('source_url'),
                "abstract_url": sources_data.get('abstract_url'),
                "pdf_url": sources_data.get('pdf_url'),
                "title": sources_data.get('title'),
                "abstract": sources_data.get('abstract'),
                "publisher": sources_data.get('publisher'),
                "language": sources_data.get('language'),
                "doi": sources_data.get('doi'),
                "arxiv_id": sources_data.get('arxiv_id'),
                "pubmed_id": sources_data.get('pubmed_id'),
                "semantic_scholar_id": sources_data.get('semantic_scholar_id'),
                "submitted_at": sources_data.get('submitted_at'),
                "online_at": sources_data.get('online_at'),
                "published_at": sources_data.get('published_at'),
                "updated_at_source": sources_data.get('updated_at'),
                "version": sources_data.get('version'),
                "is_preprint": sources_data.get('is_preprint'),
                "is_published": sources_data.get('is_published'),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        )

        return result.scalar()

    def _update_source_record_from_payload(
        self,
        conn: Connection,
        paper_source_id: int,
        db_payload: Dict[str, Any]
    ) -> None:
        """更新 paper_sources 记录

        Args:
            conn: SQLAlchemy 连接对象
            paper_source_id: 来源记录 ID
            db_payload: 数据库 payload
        """
        sources_data = db_payload.get('paper_sources', {})
        papers_data = db_payload.get('papers', {})

        # 首先获取 paper_id
        result = conn.execute(
            text("SELECT paper_id FROM paper_sources WHERE paper_source_id = :paper_source_id"),
            {"paper_source_id": paper_source_id}
        )
        paper_id = result.scalar()

        # 更新 paper_sources 表
        conn.execute(
            text("""
                UPDATE paper_sources SET
                    platform = :platform,
                    source_url = :source_url,
                    abstract_url = :abstract_url,
                    pdf_url = :pdf_url,
                    title = :title,
                    abstract = :abstract,
                    publisher = :publisher,
                    language = :language,
                    doi = :doi,
                    arxiv_id = :arxiv_id,
                    pubmed_id = :pubmed_id,
                    semantic_scholar_id = :semantic_scholar_id,
                    submitted_at = :submitted_at,
                    online_at = :online_at,
                    published_at = :published_at,
                    updated_at_source = :updated_at_source,
                    version = :version,
                    is_preprint = :is_preprint,
                    is_published = :is_published,
                    updated_at = :updated_at
                WHERE paper_source_id = :paper_source_id
            """),
            {
                "paper_source_id": paper_source_id,
                "platform": sources_data.get('platform'),
                "source_url": sources_data.get('source_url'),
                "abstract_url": sources_data.get('abstract_url'),
                "pdf_url": sources_data.get('pdf_url'),
                "title": sources_data.get('title'),
                "abstract": sources_data.get('abstract'),
                "publisher": sources_data.get('publisher'),
                "language": sources_data.get('language'),
                "doi": sources_data.get('doi'),
                "arxiv_id": sources_data.get('arxiv_id'),
                "pubmed_id": sources_data.get('pubmed_id'),
                "semantic_scholar_id": sources_data.get('semantic_scholar_id'),
                "submitted_at": sources_data.get('submitted_at'),
                "online_at": sources_data.get('online_at'),
                "published_at": sources_data.get('published_at'),
                "updated_at_source": sources_data.get('updated_at'),
                "version": sources_data.get('version'),
                "is_preprint": sources_data.get('is_preprint'),
                "is_published": sources_data.get('is_published'),
                "updated_at": datetime.now()
            }
        )

        # 如果这个 source 是 canonical_source，同步更新 papers 表的 canonical 字段
        result = conn.execute(
            text("SELECT canonical_source_id FROM papers WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        canonical_source_id = result.scalar()

        if canonical_source_id == paper_source_id:
            # 这个 source 是 canonical，更新 papers 表
            conn.execute(
                text("""
                    UPDATE papers SET
                        canonical_title = :canonical_title,
                        canonical_abstract = :canonical_abstract,
                        canonical_language = :canonical_language,
                        canonical_publisher = :canonical_publisher,
                        submitted_at = :submitted_at,
                        online_at = :online_at,
                        published_at = :published_at,
                        updated_at = :updated_at
                    WHERE paper_id = :paper_id
                """),
                {
                    "paper_id": paper_id,
                    "canonical_title": papers_data.get('canonical_title'),
                    "canonical_abstract": papers_data.get('canonical_abstract'),
                    "canonical_language": papers_data.get('canonical_language'),
                    "canonical_publisher": papers_data.get('canonical_publisher'),
                    "submitted_at": papers_data.get('submitted_at'),
                    "online_at": papers_data.get('online_at'),
                    "published_at": papers_data.get('published_at'),
                    "updated_at": datetime.now()
                }
            )

    def _upsert_source_metadata_from_payload(
        self,
        conn: Connection,
        paper_source_id: int,
        db_payload: Dict[str, Any]
    ) -> None:
        """插入或更新 paper_source_metadata 记录

        Args:
            conn: SQLAlchemy 连接对象
            paper_source_id: 来源记录 ID
            db_payload: 数据库 payload
        """
        metadata_data = db_payload.get('paper_source_metadata', {})

        conn.execute(
            text("""
                INSERT INTO paper_source_metadata (
                    paper_source_id, raw_metadata_json, normalized_json,
                    parser_version, source_schema_version, created_at, updated_at
                ) VALUES (
                    :paper_source_id, :raw_metadata_json, :normalized_json,
                    :parser_version, :source_schema_version, :created_at, :updated_at
                )
                ON CONFLICT (paper_source_id) DO UPDATE SET
                    raw_metadata_json = EXCLUDED.raw_metadata_json,
                    normalized_json = EXCLUDED.normalized_json,
                    parser_version = EXCLUDED.parser_version,
                    source_schema_version = EXCLUDED.source_schema_version,
                    updated_at = EXCLUDED.updated_at
            """),
            {
                "paper_source_id": paper_source_id,
                "raw_metadata_json": json.dumps(metadata_data.get('raw_metadata_json')),
                "normalized_json": json.dumps(metadata_data.get('normalized_json')),
                "parser_version": metadata_data.get('parser_version'),
                "source_schema_version": metadata_data.get('source_schema_version'),
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        )

    def _insert_author_affiliation_from_payload(
        self,
        conn: Connection,
        paper_id: int,
        db_payload: Dict[str, Any]
    ) -> None:
        """插入 paper_author_affiliation 记录

        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文 ID
            db_payload: 数据库 payload
        """
        authors_data = db_payload.get('paper_author_affiliation', {})
        if authors_data.get('authors'):
            conn.execute(
                text("""
                    INSERT INTO paper_author_affiliation (paper_id, authors)
                    VALUES (:paper_id, CAST(:authors AS jsonb))
                    ON CONFLICT (paper_id) DO UPDATE SET
                        authors = EXCLUDED.authors
                """),
                {
                    "paper_id": paper_id,
                    "authors": json.dumps(authors_data['authors'])
                }
            )

    def _insert_keywords_from_payload(
        self,
        conn: Connection,
        paper_id: int,
        db_payload: Dict[str, Any]
    ) -> None:
        """插入 paper_keywords 记录

        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文 ID
            db_payload: 数据库 payload
        """
        keywords_data = db_payload.get('paper_keywords', [])
        for keyword_data in keywords_data:
            keyword = self._normalize_keyword(keyword_data.get('keyword'))
            keyword_type = keyword_data.get('keyword_type')
            source = keyword_data.get('source') or "paper_metadata"
            if not keyword or not keyword_type:
                continue
            self._upsert_keyword_case_insensitive(
                conn=conn,
                paper_id=paper_id,
                keyword_type=keyword_type,
                keyword=keyword,
                weight=keyword_data.get('weight', 1.0),
                source=source,
            )

    def _upsert_keyword_case_insensitive(
        self,
        conn: Connection,
        paper_id: int,
        keyword_type: str,
        keyword: str,
        weight: float,
        source: str,
    ) -> str:
        """Upsert keyword rows case-insensitively within paper/type/source.

        PostgreSQL text primary keys are case-sensitive, so we first update any
        existing lower(keyword) match. This keeps the original display casing
        of the first row while preventing CRISPR/crispr duplicate inserts.
        """
        update_result = conn.execute(
            text("""
                UPDATE paper_keywords
                SET weight = :weight
                WHERE paper_id = :paper_id
                  AND lower(keyword_type) = lower(:keyword_type)
                  AND lower(keyword) = lower(:keyword)
                  AND source = :source
            """),
            {
                "paper_id": paper_id,
                "keyword_type": keyword_type,
                "keyword": keyword,
                "weight": weight,
                "source": source,
            },
        )
        if update_result.rowcount:
            return "updated"

        inserted = conn.execute(
            text("""
                INSERT INTO paper_keywords (
                    paper_id, keyword_type, keyword, weight, source
                ) VALUES (
                    :paper_id, :keyword_type, :keyword, :weight, :source
                )
                ON CONFLICT (paper_id, keyword_type, keyword, source)
                DO UPDATE SET weight = EXCLUDED.weight
                RETURNING (xmax = 0) AS inserted
            """),
            {
                "paper_id": paper_id,
                "keyword_type": keyword_type,
                "keyword": keyword,
                "weight": weight,
                "source": source,
            },
        ).fetchone()
        return "inserted" if inserted and inserted[0] else "updated"

    def upsert_generated_keywords(
        self,
        paper_id: int,
        keywords: List[Dict[str, Any]],
        source: str = GENERATED_KEYWORD_SOURCE,
        allowed_types: Optional[set] = None,
    ) -> Dict[str, Any]:
        """Upsert model-generated keywords into paper_keywords.

        The table primary key is expected to be
        (paper_id, keyword_type, keyword, source), so generated keywords do not
        overwrite metadata/native keywords from other sources.
        """
        if not paper_id:
            raise ValueError("paper_id is required")

        allowed = allowed_types or self.ALLOWED_GENERATED_KEYWORD_TYPES
        result = {
            "success": True,
            "paper_id": paper_id,
            "source": source,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "errors": [],
        }

        seen = set()
        normalized_rows = []
        for item in keywords or []:
            keyword_type = (item.get("keyword_type") or item.get("type") or "").strip()
            keyword = self._normalize_keyword(item.get("keyword"))
            if not keyword_type or not keyword:
                result["skipped"] += 1
                continue
            if keyword_type not in allowed:
                result["skipped"] += 1
                result["errors"].append(f"unknown keyword_type: {keyword_type}")
                continue

            try:
                weight = float(item.get("weight", 1.0))
            except (TypeError, ValueError):
                weight = 1.0
            weight = max(0.0, min(1.0, weight))

            dedupe_key = (keyword_type.lower(), keyword.lower(), source.lower())
            if dedupe_key in seen:
                result["skipped"] += 1
                continue
            seen.add(dedupe_key)
            normalized_rows.append(
                {
                    "keyword_type": keyword_type,
                    "keyword": keyword,
                    "weight": weight,
                    "source": source,
                }
            )

        if not normalized_rows:
            return result

        with self.engine.connect() as conn:
            try:
                exists = conn.execute(
                    text("SELECT 1 FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                ).fetchone()
                if not exists:
                    raise ValueError(f"paper_id does not exist: {paper_id}")

                for row in normalized_rows:
                    action = self._upsert_keyword_case_insensitive(
                        conn=conn,
                        paper_id=paper_id,
                        keyword_type=row["keyword_type"],
                        keyword=row["keyword"],
                        weight=row["weight"],
                        source=row["source"],
                    )
                    if action == "inserted":
                        result["inserted"] += 1
                    else:
                        result["updated"] += 1

                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def has_keywords_from_source(
        self,
        paper_id: int,
        source: str = GENERATED_KEYWORD_SOURCE,
    ) -> bool:
        """Return whether a paper already has keywords from the given source."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT 1
                    FROM paper_keywords
                    WHERE paper_id = :paper_id
                      AND source = :source
                    LIMIT 1
                """),
                {"paper_id": paper_id, "source": source}
            ).fetchone()
            return row is not None

    def suggest_query_terms(
        self,
        query: str,
        limit: int = 20,
        sources: Optional[List[str]] = None,
        min_weight: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Return keyword candidates for query correction from paper_keywords."""
        normalized_query = self._normalize_keyword(query)
        if not normalized_query:
            return []

        tokens = [
            token.lower()
            for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9+_-]*", normalized_query)
            if len(token) >= 3
        ]
        if not tokens:
            return []

        source_values = sources or [
            "scispacy-en_core_sci_lg-generated",
            "scispacy-en_ner_bionlp13cg_md-generated",
            "scispacy-en_core_sci_lg-generated-test",
            "scispacy-en_ner_bionlp13cg_md-generated-test",
        ]
        params: Dict[str, Any] = {
            "limit": max(limit * 5, 50),
            "min_weight": min_weight,
        }

        source_placeholders = []
        for idx, source in enumerate(source_values):
            param_name = f"source_{idx}"
            source_placeholders.append(f":{param_name}")
            params[param_name] = source

        token_conditions = []
        for idx, token in enumerate(sorted(tokens, key=len, reverse=True)[:3]):
            param_name = f"token_{idx}"
            token_conditions.append(f"lower(keyword) LIKE :{param_name}")
            params[param_name] = f"%{token}%"

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    WITH candidates AS (
                        SELECT
                            lower(keyword) AS normalized_keyword,
                            MIN(keyword) AS display_keyword,
                            keyword_type,
                            source,
                            paper_id,
                            MAX(COALESCE(weight, 1.0)) AS paper_weight
                        FROM paper_keywords
                        WHERE source IN ({", ".join(source_placeholders)})
                          AND COALESCE(weight, 1.0) >= :min_weight
                          AND ({" OR ".join(token_conditions)})
                        GROUP BY lower(keyword), keyword_type, source, paper_id
                    )
                    SELECT
                        MIN(display_keyword) AS keyword,
                        keyword_type,
                        source,
                        COUNT(DISTINCT paper_id) AS doc_count,
                        AVG(paper_weight) AS avg_weight
                    FROM candidates
                    GROUP BY normalized_keyword, keyword_type, source
                    ORDER BY doc_count DESC, avg_weight DESC, lower(MIN(display_keyword)) ASC
                    LIMIT :limit
                """),
                params,
            ).fetchall()

        return [
            {
                "keyword": row[0],
                "keyword_type": row[1],
                "source": row[2],
                "doc_count": int(row[3] or 0),
                "avg_weight": float(row[4] or 0.0),
            }
            for row in rows
        ]

    def _insert_references_from_payload(
        self,
        conn: Connection,
        paper_id: int,
        paper_source_id: int,
        db_payload: Dict[str, Any]
    ) -> None:
        """插入 paper_references 记录

        Args:
            conn: SQLAlchemy 连接对象
            paper_id: 论文 ID
            paper_source_id: 来源记录 ID
            db_payload: 数据库 payload
        """
        references_data = db_payload.get('paper_references', [])

        if not references_data:
            return  # 空列表则跳过

        for ref_data in references_data:
            try:
                conn.execute(
                    text("""
                        INSERT INTO paper_references (
                            paper_id, paper_source_id, reference_order,
                            reference_text, reference_raw_json, created_at
                        ) VALUES (
                            :paper_id, :paper_source_id, :reference_order,
                            :reference_text, :reference_raw_json, :created_at
                        )
                        ON CONFLICT DO NOTHING
                    """),
                    {
                        "paper_id": paper_id,
                        "paper_source_id": paper_source_id,
                        "reference_order": ref_data.get('reference_order'),
                        "reference_text": ref_data.get('reference_text'),
                        "reference_raw_json": json.dumps(ref_data.get('reference_raw_json')),
                        "created_at": datetime.now()
                    }
                )
            except Exception as e:
                logging.warning(f"插入参考文献失败 (paper_id={paper_id}): {str(e)}")

    # =========================================================================
    # 更新的查询方法
    # =========================================================================

    def get_paper_info_by_paper_id(self, paper_id: int) -> Optional[Dict[str, Any]]:
        """根据 paper_id 获取论文完整信息

        Args:
            paper_id: 论文 ID

        Returns:
            Optional[Dict]: 论文完整信息，包含所有 source 记录
        """
        with self.engine.connect() as conn:
            # 获取 papers 表数据
            result = conn.execute(
                text("""
                    SELECT
                        paper_id, work_id, canonical_title, canonical_abstract,
                        canonical_language, canonical_publisher,
                        submitted_at, online_at, published_at,
                        canonical_source_id, merge_status,
                        created_at, updated_at
                    FROM papers
                    WHERE paper_id = :paper_id
                """),
                {"paper_id": paper_id}
            )
            row = result.fetchone()

            if not row:
                return None

            paper_info = {
                'paper_id': row[0],
                'work_id': row[1],
                'canonical_title': row[2],
                'canonical_abstract': row[3],
                'canonical_language': row[4],
                'canonical_publisher': row[5],
                'submitted_at': row[6].isoformat() if row[6] else None,
                'online_at': row[7].isoformat() if row[7] else None,
                'published_at': row[8].isoformat() if row[8] else None,
                'canonical_source_id': row[9],
                'merge_status': row[10],
                'created_at': row[11].isoformat() if row[11] else None,
                'updated_at': row[12].isoformat() if row[12] else None,
                'sources': [],
                'metadata': []
            }

            # 获取所有 source 记录
            result = conn.execute(
                text("""
                    SELECT
                        paper_source_id, source_name, platform, source_record_id,
                        source_url, abstract_url, pdf_url, title, abstract,
                        publisher, language, doi, arxiv_id, pubmed_id,
                        semantic_scholar_id, submitted_at, online_at, published_at,
                        version, is_preprint, is_published
                    FROM paper_sources
                    WHERE paper_id = :paper_id
                    ORDER BY online_at DESC
                """),
                {"paper_id": paper_id}
            )

            for row in result.fetchall():
                source_info = {
                    'paper_source_id': row[0],
                    'source_name': row[1],
                    'platform': row[2],
                    'source_record_id': row[3],
                    'source_url': row[4],
                    'abstract_url': row[5],
                    'pdf_url': row[6],
                    'title': row[7],
                    'abstract': row[8],
                    'publisher': row[9],
                    'language': row[10],
                    'doi': row[11],
                    'arxiv_id': row[12],
                    'pubmed_id': row[13],
                    'semantic_scholar_id': row[14],
                    'submitted_at': row[15].isoformat() if row[15] else None,
                    'online_at': row[16].isoformat() if row[16] else None,
                    'published_at': row[17].isoformat() if row[17] else None,
                    'version': row[18],
                    'is_preprint': row[19],
                    'is_published': row[20]
                }
                paper_info['sources'].append(source_info)

            # 获取 metadata 记录
            for source in paper_info['sources']:
                result = conn.execute(
                    text("""
                        SELECT
                            raw_metadata_json, normalized_json,
                            parser_version, source_schema_version
                        FROM paper_source_metadata
                        WHERE paper_source_id = :paper_source_id
                    """),
                    {"paper_source_id": source['paper_source_id']}
                )
                row = result.fetchone()
                if row:
                    source['metadata'] = {
                        'raw_metadata_json': row[0],
                        'normalized_json': row[1],
                        'parser_version': row[2],
                        'source_schema_version': row[3]
                    }

            # 获取作者信息
            result = conn.execute(
                text("SELECT authors FROM paper_author_affiliation WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            author_row = result.fetchone()
            if author_row and author_row[0]:
                paper_info['authors'] = author_row[0]
            else:
                paper_info['authors'] = []

            # 获取关键词信息
            result = conn.execute(
                text("""
                    SELECT keyword_type, keyword, weight, source
                    FROM paper_keywords
                    WHERE paper_id = :paper_id
                    ORDER BY keyword_type, keyword
                """),
                {"paper_id": paper_id}
            )
            paper_info['keywords'] = [
                {
                    'keyword_type': row[0],
                    'keyword': row[1],
                    'weight': row[2],
                    'source': row[3]
                }
                for row in result.fetchall()
            ]

            # 获取参考文献信息
            result = conn.execute(
                text("""
                    SELECT reference_id, reference_order, reference_text, reference_raw_json
                    FROM paper_references
                    WHERE paper_id = :paper_id
                    ORDER BY reference_order
                """),
                {"paper_id": paper_id}
            )
            paper_info['references'] = [
                {
                    'reference_id': row[0],
                    'reference_order': row[1],
                    'reference_text': row[2],
                    'reference_raw_json': row[3]
                }
                for row in result.fetchall()
            ]

            return paper_info

    def read_paper(self, paper_id: int) -> Optional[Dict[str, Any]]:
        """读取完整论文数据（包含所有关联信息）

        Args:
            paper_id: 论文 ID

        Returns:
            Optional[Dict]: 完整论文数据
        """
        return self.get_paper_info_by_paper_id(paper_id)

    def search_by_author(
        self,
        author_name: str,
        limit: int = 100,
        source_list: Optional[List[str]] = None,
        fuzzy: bool = True,
    ) -> List[Dict[str, Any]]:
        """Search papers by author name from paper_author_affiliation.authors.

        MVP implementation expands the JSONB authors array and matches only
        authors[].name. It intentionally avoids authors::text matching so that
        affiliations or JSON field names cannot produce false positives.
        """
        normalized = (author_name or "").strip()
        if not normalized:
            return []

        params: Dict[str, Any] = {
            "author_pattern": f"%{normalized}%" if fuzzy else normalized,
            "limit": limit,
        }
        source_filter = self._source_filter_sql(source_list, params, table_alias="ps")
        source_join = "JOIN paper_sources ps ON ps.paper_id = p.paper_id"

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT
                        p.paper_id,
                        MAX(COALESCE(p.online_at, ps.online_at)) AS sort_online_at
                    FROM papers p
                    JOIN paper_author_affiliation paa ON paa.paper_id = p.paper_id
                    {source_join}
                    WHERE EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(paa.authors) AS author
                        WHERE author->>'name' IS NOT NULL
                          AND lower(author->>'name') {"ILIKE" if fuzzy else "="} lower(:author_pattern)
                    )
                    {source_filter}
                    GROUP BY p.paper_id
                    ORDER BY sort_online_at DESC NULLS LAST, p.paper_id DESC
                    LIMIT :limit
                """),
                params
            ).fetchall()

        results = []
        for row in rows:
            paper_info = self.get_paper_info_by_paper_id(row[0])
            if paper_info:
                results.append(paper_info)
        return results

    def suggest_author_names(
        self,
        query: str,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Return ranked author-name candidates from the current JSONB author pool."""
        normalized_query = self.normalize_author_name(query)
        if not normalized_query:
            return []

        tokens = [token for token in normalized_query.split() if len(token) >= 2]
        if not tokens:
            return []

        recall_tokens = sorted(tokens, key=len, reverse=True)[:2]
        conditions = []
        params: Dict[str, Any] = {
            "candidate_limit": max(limit * 20, 50),
        }
        for idx, token in enumerate(recall_tokens):
            param_name = f"pattern_{idx}"
            conditions.append(f"lower(author_name) ILIKE lower(:{param_name})")
            params[param_name] = f"%{token}%"

        where_clause = " OR ".join(conditions)

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    WITH authors AS (
                        SELECT
                            author->>'name' AS author_name,
                            paa.paper_id
                        FROM paper_author_affiliation paa,
                             jsonb_array_elements(paa.authors) AS author
                        WHERE author->>'name' IS NOT NULL
                          AND btrim(author->>'name') != ''
                    )
                    SELECT
                        author_name,
                        COUNT(DISTINCT paper_id) AS paper_count
                    FROM authors
                    WHERE {where_clause}
                    GROUP BY author_name
                    ORDER BY paper_count DESC, author_name ASC
                    LIMIT :candidate_limit
                """),
                params
            ).fetchall()

        best_by_normalized: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            name = row[0]
            normalized_name = self.normalize_author_name(name)
            score = self.author_match_score(normalized_query, name)
            candidate = {
                "name": name,
                "normalized_name": normalized_name,
                "score": score,
                "paper_count": int(row[1] or 0),
            }
            existing = best_by_normalized.get(normalized_name)
            if (
                existing is None
                or candidate["score"] > existing["score"]
                or (
                    candidate["score"] == existing["score"]
                    and candidate["paper_count"] > existing["paper_count"]
                )
            ):
                best_by_normalized[normalized_name] = candidate

        candidates = sorted(
            best_by_normalized.values(),
            key=lambda item: (-item["score"], -item["paper_count"], item["name"].lower())
        )
        return candidates[:limit]

    # =========================================================================
    # Embedding 状态管理（简化三态：pending/succeeded/failed）
    # =========================================================================

    def get_source_name_by_paper_source_id(self, paper_source_id: int) -> Optional[str]:
        """根据 paper_source_id 获取 source_name。"""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT source_name
                    FROM paper_sources
                    WHERE paper_source_id = :paper_source_id
                """),
                {"paper_source_id": paper_source_id}
            ).fetchone()
            return row[0] if row else None

    def upsert_embedding_status_pending(
        self,
        paper_id: int,
        work_id: str,
        canonical_source_id: Optional[int],
        source_name: Optional[str],
        text_type: str
    ) -> None:
        """写入/更新 embedding_status 为 pending。"""
        with self.engine.connect() as conn:
            try:
                conn.execute(
                    text("""
                        INSERT INTO embedding_status (
                            paper_id, work_id, canonical_source_id, source_name,
                            text_type, status, last_error_message, last_attempt_at
                        )
                        VALUES (
                            :paper_id, :work_id, :canonical_source_id, :source_name,
                            :text_type, 'pending', NULL, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (paper_id)
                        DO UPDATE SET
                            work_id = EXCLUDED.work_id,
                            canonical_source_id = EXCLUDED.canonical_source_id,
                            source_name = EXCLUDED.source_name,
                            text_type = EXCLUDED.text_type,
                            status = 'pending',
                            last_error_message = NULL,
                            last_attempt_at = CURRENT_TIMESTAMP
                    """),
                    {
                        "paper_id": paper_id,
                        "work_id": work_id,
                        "canonical_source_id": canonical_source_id,
                        "source_name": source_name,
                        "text_type": text_type
                    }
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logging.error(
                    "写入 embedding_status=pending 失败: "
                    f"paper_id={paper_id}, work_id={work_id}, error={str(e)}"
                )
                raise e

    def mark_embedding_succeeded(self, paper_id: int) -> bool:
        """将 embedding_status 标记为 succeeded。"""
        with self.engine.connect() as conn:
            try:
                result = conn.execute(
                    text("""
                        UPDATE embedding_status
                        SET
                            status = 'succeeded',
                            attempt_count = attempt_count + 1,
                            last_error_message = NULL,
                            last_success_at = CURRENT_TIMESTAMP
                        WHERE paper_id = :paper_id
                    """),
                    {"paper_id": paper_id}
                )
                conn.commit()
                return result.rowcount > 0
            except Exception as e:
                conn.rollback()
                logging.error(
                    f"更新 embedding_status=succeeded 失败: paper_id={paper_id}, error={str(e)}"
                )
                raise e

    def mark_embedding_failed(self, paper_id: int, error_message: str) -> bool:
        """将 embedding_status 标记为 failed，并记录最近错误。"""
        with self.engine.connect() as conn:
            try:
                result = conn.execute(
                    text("""
                        UPDATE embedding_status
                        SET
                            status = 'failed',
                            attempt_count = attempt_count + 1,
                            last_error_message = :error_message,
                            last_attempt_at = CURRENT_TIMESTAMP
                        WHERE paper_id = :paper_id
                    """),
                    {
                        "paper_id": paper_id,
                        "error_message": error_message[:2000] if error_message else None
                    }
                )
                conn.commit()
                return result.rowcount > 0
            except Exception as e:
                conn.rollback()
                logging.error(
                    f"更新 embedding_status=failed 失败: paper_id={paper_id}, error={str(e)}"
                )
                raise e

    def list_embedding_candidates(
        self,
        source_name: Optional[str] = None,
        statuses: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """查询待向量化候选（供 backfill 批处理使用）。"""
        if statuses is None:
            statuses = ['pending', 'failed']

        if not statuses:
            return []

        with self.engine.connect() as conn:
            status_placeholders = ", ".join([f":status_{i}" for i in range(len(statuses))])
            params: Dict[str, Any] = {"limit": limit, "offset": offset}
            for i, status in enumerate(statuses):
                params[f"status_{i}"] = status

            source_clause = ""
            if source_name:
                source_clause = "AND es.source_name = :source_name"
                params["source_name"] = source_name

            query = f"""
                SELECT
                    es.paper_id,
                    es.work_id,
                    es.canonical_source_id,
                    es.source_name,
                    es.text_type,
                    es.status,
                    es.attempt_count,
                    es.last_error_message,
                    es.last_attempt_at,
                    es.last_success_at
                FROM embedding_status es
                WHERE es.status IN ({status_placeholders})
                  {source_clause}
                ORDER BY es.updated_at ASC
                LIMIT :limit OFFSET :offset
            """

            rows = conn.execute(text(query), params).fetchall()
            return [
                {
                    "paper_id": row[0],
                    "work_id": row[1],
                    "canonical_source_id": row[2],
                    "source_name": row[3],
                    "text_type": row[4],
                    "status": row[5],
                    "attempt_count": row[6],
                    "last_error_message": row[7],
                    "last_attempt_at": row[8],
                    "last_success_at": row[9]
                }
                for row in rows
            ]

    def delete_paper_by_paper_id(self, paper_id: int) -> bool:
        """删除论文（级联删除所有关联数据）

        Args:
            paper_id: 论文 ID

        Returns:
            bool: 成功返回 True，不存在返回 False
        """
        with self.engine.connect() as conn:
            try:
                # 检查是否存在
                result = conn.execute(
                    text("SELECT paper_id FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )
                row = result.fetchone()

                if not row:
                    logging.warning(f"论文不存在: paper_id={paper_id}")
                    return False

                # 删除论文（级联删除关联数据）
                conn.execute(
                    text("DELETE FROM papers WHERE paper_id = :paper_id"),
                    {"paper_id": paper_id}
                )

                conn.commit()
                logging.info(f"成功删除论文: paper_id={paper_id}")
                return True

            except Exception as e:
                conn.rollback()
                logging.error(f"删除论文失败: paper_id={paper_id}, error={str(e)}")
                raise e

    def search_by_condition(
        self,
        title: Optional[str] = None,
        author: Optional[str] = None,
        category: Optional[str] = None,
        year: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """基于条件搜索论文

        Args:
            title: 标题关键词（模糊匹配）
            author: 作者名称（模糊匹配）
            category: 分类（精确匹配）
            year: 年份（精确匹配）
            limit: 返回数量限制

        Returns:
            List[Dict]: 论文列表
        """
        with self.engine.connect() as conn:
            conditions = []
            params = {"limit": limit}

            if title:
                conditions.append("canonical_title ILIKE :title")
                params['title'] = f"%{title}%"

            if author:
                # TODO: 实现作者搜索（需要查询 paper_author_affiliation）
                pass

            if category:
                # TODO: 实现分类搜索（需要查询 paper_categories）
                pass

            if year:
                conditions.append("EXTRACT(YEAR FROM online_at) = :year")
                params['year'] = year

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            query = f"""
                SELECT paper_id
                FROM papers
                WHERE {where_clause}
                ORDER BY online_at DESC
                LIMIT :limit
            """

            result = conn.execute(text(query), params)
            paper_ids = [row[0] for row in result.fetchall()]

            # 获取完整信息
            results = []
            for pid in paper_ids:
                paper_info = self.get_paper_info_by_paper_id(pid)
                if paper_info:
                    results.append(paper_info)

            return results

    # =========================================================================
    # 新增方法：通过 work_id 查询和管理论文
    # =========================================================================

    def get_paper_info_by_work_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        """根据 work_id 获取论文完整信息

        Args:
            work_id: 论文的全局唯一标识符（UUID v7 格式）

        Returns:
            Optional[Dict]: 论文完整信息，包含所有 source 记录
            如果不存在则返回 None

        Example:
            >>> paper_info = metadata_db.get_paper_info_by_work_id(
            ...     "W019b73d6-1634-77d3-9574-b6014f85b118"
            ... )
            >>> print(paper_info['canonical_title'])
        """
        with self.engine.connect() as conn:
            # 通过 work_id 查询 paper_id
            result = conn.execute(
                text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
                {"work_id": work_id}
            )
            row = result.fetchone()

            if not row:
                return None

            paper_id = row[0]

            # 使用现有的 get_paper_info_by_paper_id 方法
            return self.get_paper_info_by_paper_id(paper_id)

    def read_paper_by_work_id(self, work_id: str) -> Optional[Dict[str, Any]]:
        """读取完整论文数据（通过 work_id）

        这是 get_paper_info_by_work_id 的简化别名方法

        Args:
            work_id: 论文的 work_id

        Returns:
            Optional[Dict]: 完整论文数据
        """
        return self.get_paper_info_by_work_id(work_id)

    def get_authors_by_paper_id(self, paper_id: int) -> List[Dict[str, Any]]:
        """获取论文的作者信息

        Args:
            paper_id: 论文 ID

        Returns:
            List[Dict]: 作者列表，包含姓名、排序、机构等信息
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT authors FROM paper_author_affiliation WHERE paper_id = :paper_id"),
                {"paper_id": paper_id}
            )
            row = result.fetchone()
            if row and row[0]:
                return row[0]  # 返回 JSONB 数组
            return []

    def get_keywords_by_paper_id(self, paper_id: int) -> List[Dict[str, Any]]:
        """获取论文的关键词信息

        Args:
            paper_id: 论文 ID

        Returns:
            List[Dict]: 关键词列表，包含类型、关键词、权重、来源
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT keyword_type, keyword, weight, source
                    FROM paper_keywords
                    WHERE paper_id = :paper_id
                    ORDER BY keyword_type, keyword
                """),
                {"paper_id": paper_id}
            )
            return [
                {
                    'keyword_type': row[0],
                    'keyword': row[1],
                    'weight': row[2],
                    'source': row[3]
                }
                for row in result.fetchall()
            ]

    def get_references_by_paper_id(self, paper_id: int) -> List[Dict[str, Any]]:
        """获取论文的参考文献信息

        Args:
            paper_id: 论文 ID

        Returns:
            List[Dict]: 参考文献列表，按顺序排列
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT reference_id, reference_order, reference_text,
                           reference_raw_json, paper_source_id
                    FROM paper_references
                    WHERE paper_id = :paper_id
                    ORDER BY reference_order
                """),
                {"paper_id": paper_id}
            )
            return [
                {
                    'reference_id': row[0],
                    'reference_order': row[1],
                    'reference_text': row[2],
                    'reference_raw_json': row[3],
                    'paper_source_id': row[4]
                }
                for row in result.fetchall()
            ]

    def delete_paper_by_work_id(self, work_id: str) -> bool:
        """删除论文（通过 work_id）

        注意：由于外键约束设置了 ON DELETE CASCADE，
        删除 papers 表的记录会自动级联删除所有关联数据。

        Args:
            work_id: 论文的 work_id

        Returns:
            bool: 成功返回 True，不存在返回 False

        Example:
            >>> success = metadata_db.delete_paper_by_work_id(
            ...     "W019b73d6-1634-77d3-9574-b6014f85b118"
            ... )
        """
        with self.engine.connect() as conn:
            try:
                # 先检查论文是否存在
                result = conn.execute(
                    text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
                    {"work_id": work_id}
                )
                row = result.fetchone()

                if not row:
                    logging.warning(f"论文不存在: work_id={work_id}")
                    return False

                # 删除论文（级联删除关联数据）
                result = conn.execute(
                    text("DELETE FROM papers WHERE work_id = :work_id"),
                    {"work_id": work_id}
                )

                deleted_count = result.rowcount
                conn.commit()

                if deleted_count > 0:
                    logging.info(f"成功删除论文: work_id={work_id}")
                    return True
                else:
                    logging.warning(f"删除论文失败: work_id={work_id}")
                    return False

            except Exception as e:
                conn.rollback()
                logging.error(f"删除论文时发生错误: work_id={work_id}, error={str(e)}")
                raise e

    def get_papers_by_work_ids(
        self,
        work_ids: List[str],
        include_sources: bool = True
    ) -> List[Dict[str, Any]]:
        """批量获取论文信息（通过 work_id 列表）

        Args:
            work_ids: work_id 列表
            include_sources: 是否包含 source 记录（默认 True）

        Returns:
            List[Dict]: 论文列表

        Example:
            >>> work_ids = ["Wxxx", "Wyyy", "Wzzz"]
            >>> papers = metadata_db.get_papers_by_work_ids(work_ids)
        """
        if not work_ids:
            return []

        with self.engine.connect() as conn:
            # 查询所有匹配的 paper_id
            result = conn.execute(
                text("""
                    SELECT paper_id
                    FROM papers
                    WHERE work_id = ANY(:work_ids)
                """),
                {"work_ids": work_ids}
            )
            paper_ids = [row[0] for row in result.fetchall()]

            # 批量获取完整信息
            papers = []
            for paper_id in paper_ids:
                if include_sources:
                    paper_info = self.get_paper_info_by_paper_id(paper_id)
                else:
                    # 只返回 papers 表数据
                    result = conn.execute(
                        text("""
                            SELECT paper_id, work_id, canonical_title,
                                   canonical_abstract, canonical_source_id
                            FROM papers
                            WHERE paper_id = :paper_id
                        """),
                        {"paper_id": paper_id}
                    )
                    row = result.fetchone()
                    if row:
                        paper_info = {
                            'paper_id': row[0],
                            'work_id': row[1],
                            'canonical_title': row[2],
                            'canonical_abstract': row[3],
                            'canonical_source_id': row[4]
                        }
                    else:
                        paper_info = None

                if paper_info:
                    papers.append(paper_info)

            return papers
