"""腾讯云 VectorDB HTTP 客户端

负责与腾讯云 VectorDB API 交互，处理认证、请求封装和错误处理。
支持服务端自动 embedding 模式。

参考文档: docs/tencent_vectordb_embedding_manual.md
"""

import logging
import requests
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class VectorDBConfig:
    """向量数据库配置"""
    url: str
    account: str
    api_key: str
    embedding_source: str
    embedding_model: str


class VectorDBError(Exception):
    """向量数据库错误基类"""
    pass


class VectorDBClientError(VectorDBError):
    """客户端请求错误"""
    pass


class VectorDBServerError(VectorDBError):
    """服务端返回错误"""
    pass


class VectorDBClient:
    """腾讯云 VectorDB HTTP 客户端

    负责封装所有与腾讯云 VectorDB 的 HTTP 交互。

    Attributes:
        config: 向量数据库配置
        session: requests 会话对象
    """

    def __init__(self, url: str, account: str, api_key: str):
        """初始化客户端

        Args:
            url: VectorDB 服务 URL
            account: 账户名
            api_key: API 密钥
        """
        self.url = url.rstrip('/')
        self.account = account
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Authorization': f'Bearer account={account}&api_key={api_key}'
        })

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """发送 HTTP 请求

        Args:
            method: HTTP 方法 (GET, POST, DELETE)
            endpoint: API 端点
            data: 请求体数据

        Returns:
            Dict: API 响应数据

        Raises:
            VectorDBClientError: 请求失败
            VectorDBServerError: 服务端返回错误
        """
        url = f"{self.url}/{endpoint.lstrip('/')}"

        try:
            if method.upper() == 'GET':
                response = self.session.get(url, params=data)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data)
            elif method.upper() == 'DELETE':
                response = self.session.delete(url, json=data)
            else:
                raise VectorDBClientError(f"不支持的 HTTP 方法: {method}")

            # 检查 HTTP 状态码
            response.raise_for_status()

            result = response.json()

            # 检查业务状态码
            if result.get('code') != 0:
                error_msg = result.get('msg', '未知错误')
                raise VectorDBServerError(
                    f"API 返回错误: code={result.get('code')}, msg={error_msg}"
                )

            return result

        except requests.exceptions.RequestException as e:
            # 尝试从响应中获取更详细的错误信息
            error_details = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_json = e.response.json()
                    if 'msg' in error_json:
                        error_details = f"HTTP {e.response.status_code}: {error_json.get('msg', error_json)}"
                    elif 'message' in error_json:
                        error_details = f"HTTP {e.response.status_code}: {error_json.get('message', error_json)}"
                    else:
                        error_details = f"HTTP {e.response.status_code}: {error_json}"
                except ValueError:
                    # 如果不是 JSON 响应，使用原始错误信息
                    error_details = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
            raise VectorDBClientError(f"HTTP 请求失败: {error_details}")
        except ValueError as e:
            raise VectorDBClientError(f"JSON 解析失败: {str(e)}")

    def create_database(self, database: str) -> Dict[str, Any]:
        """创建数据库

        Args:
            database: 数据库名称

        Returns:
            Dict: API 响应

        Example:
            >>> client.create_database("langtaosha_test")
        """
        logging.info(f"创建数据库: {database}")
        return self._request('POST', '/database/create', {'database': database})

    def drop_database(self, database: str) -> Dict[str, Any]:
        """删除数据库

        Args:
            database: 数据库名称

        Returns:
            Dict: API 响应
        """
        logging.warning(f"删除数据库: {database}")
        return self._request('POST', '/database/drop', {'database': database})

    def list_databases(self) -> List[str]:
        """列出所有数据库

        Returns:
            List[str]: 数据库名称列表
        """
        result = self._request('GET', '/database/list')
        databases = result.get('databases', [])
        logging.info(f"数据库列表: {databases}")
        return databases

    def create_collection(
        self,
        database: str,
        collection: str,
        embedding_field: str,
        embedding_model: str,
        replica_num: int = 1,
        shard_num: int = 1
    ) -> Dict[str, Any]:
        """创建带 Embedding 的 Collection

        使用腾讯云服务端自动 embedding 模式。

        Args:
            database: 数据库名称
            collection: Collection 名称
            embedding_field: 用于 embedding 的文本字段名
            embedding_model: Embedding 模型名称
            replica_num: 副本数量
            shard_num: 分片数量

        Returns:
            Dict: API 响应

        Example:
            >>> client.create_collection(
            ...     database="langtaosha_test",
            ...     collection="lt_biorxiv_history",
            ...     embedding_field="text",
            ...     embedding_model="BAAI/bge-m3"
            ... )
        """
        logging.info(f"创建 collection: {database}.{collection}")

        request_data = {
            "database": database,
            "collection": collection,
            "replicaNum": replica_num,
            "shardNum": shard_num,
            "embedding": {
                "field": embedding_field,
                "vectorField": "vector",
                "model": embedding_model
            },
            "indexes": [
                {
                    "fieldName": "id",
                    "fieldType": "string",
                    "indexType": "primaryKey"
                },
                {
                    "fieldName": "vector",
                    "fieldType": "vector",
                    "indexType": "HNSW",
                    "metricType": "COSINE",
                    "params": {"M": 16, "efConstruction": 200}
                },
                {
                    "fieldName": "work_id",
                    "fieldType": "string",
                    "indexType": "filter"
                },
                {
                    "fieldName": "paper_id",
                    "fieldType": "string",
                    "indexType": "filter"
                },
                {
                    "fieldName": "source_name",
                    "fieldType": "string",
                    "indexType": "filter"
                },
                {
                    "fieldName": "text_type",
                    "fieldType": "string",
                    "indexType": "filter"
                }
            ]
        }

        return self._request('POST', '/collection/create', request_data)

    def drop_collection(self, database: str, collection: str) -> Dict[str, Any]:
        """删除 Collection

        Args:
            database: 数据库名称
            collection: Collection 名称

        Returns:
            Dict: API 响应
        """
        logging.warning(f"删除 collection: {database}.{collection}")
        return self._request(
            'POST',
            '/collection/drop',
            {'database': database, 'collection': collection}
        )

    def list_collections(self, database: str) -> List[str]:
        """列出数据库中的所有 Collection

        Args:
            database: 数据库名称

        Returns:
            List[str]: Collection 名称列表
        """
        # 根据腾讯云文档，list 应该使用 POST 请求
        result = self._request('POST', '/collection/list', {'database': database})
        collections_data = result.get('collections', [])

        # API 返回的是字典列表，每个字典包含 collection 的完整信息
        # 提取 collection 名称
        collections = [col.get('collection', '') if isinstance(col, dict) else col for col in collections_data]

        logging.info(f"Collection 列表 ({database}): {collections}")
        return collections

    def list_collections_with_info(self, database: str) -> List[Dict[str, Any]]:
        """列出数据库中的所有 Collection 并返回完整信息

        Args:
            database: 数据库名称

        Returns:
            List[Dict]: Collection 详细信息列表，每个包含：
                - collection: Collection 名称
                - documentCount: 文档数量
                - indexes: 索引信息
                - indexStatus: 索引状态
                - replicaNum: 副本数
                - shardNum: 分片数
                - createTime: 创建时间
        """
        result = self._request('POST', '/collection/list', {'database': database})
        collections = result.get('collections', [])

        logging.info(f"Collection 详细信息 ({database}): {len(collections)} 个")
        return collections

    def describe_collection(self, database: str, collection: str) -> Dict[str, Any]:
        """查询指定 Collection 的详细信息

        Args:
            database: 数据库名称
            collection: Collection 名称

        Returns:
            Dict: Collection 详细信息，包括文档数量、索引状态等

        Example:
            >>> info = client.describe_collection(
            ...     database="langtaosha_test",
            ...     collection="lt_biorxiv_history"
            ... )
            >>> print(info['documentCount'])
        """
        logging.info(f"查询 Collection 信息: {database}.{collection}")

        request_data = {
            "database": database,
            "collection": collection
        }

        result = self._request('POST', '/collection/describe', request_data)
        collection_info = result.get('collection', {})

        logging.info(
            f"Collection {collection}: 文档数={collection_info.get('documentCount', 0)}, "
            f"状态={collection_info.get('indexStatus', {}).get('status', 'unknown')}"
        )

        return collection_info

    def upsert_documents(
        self,
        database: str,
        collection: str,
        documents: List[Dict[str, Any]],
        build_index: bool = True
    ) -> Dict[str, Any]:
        """插入或更新文档

        Args:
            database: 数据库名称
            collection: Collection 名称
            documents: 文档列表，每个文档必须包含 id 和文本字段
            build_index: 是否构建索引

        Returns:
            Dict: API 响应，包含插入的文档数量等

        Example:
            >>> documents = [
            ...     {
            ...         "id": "biorxiv_history:work_123:abstract",
            ...         "text": "这是论文摘要",
            ...         "work_id": "work_123",
            ...         "paper_id": "12345",
            ...         "source_name": "biorxiv_history",
            ...         "text_type": "abstract"
            ...     }
            ... ]
            >>> client.upsert_documents("langtaosha_test", "lt_biorxiv_history", documents)
        """
        logging.info(f"插入 {len(documents)} 个文档到 {database}.{collection}")

        request_data = {
            "database": database,
            "collection": collection,
            "buildIndex": build_index,
            "documents": documents
        }

        result = self._request('POST', '/document/upsert', request_data)
        affected_count = result.get('affectedCount', result.get('insertCount', 0))
        logging.info(f"成功插入 {affected_count} 个文档")
        return result

    def delete_documents(
        self,
        database: str,
        collection: str,
        ids: Optional[List[str]] = None,
        filter: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """删除文档

        Args:
            database: 数据库名称
            collection: Collection 名称
            ids: 文档 ID 列表（可选）
            filter: 过滤表达式（可选），例如 "work_id=\"work_123\""
            limit: 删除文档数量限制（可选，不设置则最大删除 16384 条）

        Returns:
            Dict: API 响应，包含删除的文档数量

        Example:
            >>> # 按 ID 删除
            >>> client.delete_documents(
            ...     "langtaosha_test",
            ...     "lt_biorxiv_history",
            ...     ids=["biorxiv_history:work_123:abstract"]
            ... )
            >>> # 按过滤条件删除
            >>> client.delete_documents(
            ...     "langtaosha_test",
            ...     "lt_biorxiv_history",
            ...     filter="work_id=\"work_123\"",
            ...     limit=10
            ... )
        """
        if not ids and not filter:
            raise VectorDBClientError("删除文档必须提供 ids 或 filter 参数")

        # 构建查询对象
        query_obj = {}
        if ids:
            query_obj["documentIds"] = ids
        if filter:
            query_obj["filter"] = filter
        if limit:
            query_obj["limit"] = limit

        logging.info(f"从 {database}.{collection} 删除文档（条件: ids={len(ids) if ids else 0}, filter={filter}）")

        request_data = {
            "database": database,
            "collection": collection,
            "query": query_obj
        }

        result = self._request('POST', '/document/delete', request_data)
        affected_count = result.get('affectedCount', 0)
        logging.info(f"成功删除 {affected_count} 个文档")
        return result

    def search_documents(
        self,
        database: str,
        collection: str,
        query_text: str,
        limit: int = 10,
        output_fields: Optional[List[str]] = None,
        retrieve_vector: bool = False
    ) -> Dict[str, Any]:
        """搜索文档（使用腾讯云服务端 Embedding）

        Args:
            database: 数据库名称
            collection: Collection 名称
            query_text: 查询文本
            limit: 返回结果数量
            output_fields: 需要返回的字段列表
            retrieve_vector: 是否返回向量

        Returns:
            Dict: 搜索结果

        Example:
            >>> results = client.search_documents(
            ...     database="langtaosha_test",
            ...     collection="lt_biorxiv_history",
            ...     query_text="机器学习算法",
            ...     limit=5,
            ...     output_fields=["work_id", "paper_id", "source_name", "text_type"]
            ... )
        """
        if output_fields is None:
            output_fields = ["work_id", "paper_id", "source_name", "text_type"]

        logging.info(f"在 {database}.{collection} 中搜索: '{query_text}'")

        request_data = {
            "database": database,
            "collection": collection,
            "search": {
                "embeddingItems": [query_text],
                "limit": limit,
                "retrieveVector": retrieve_vector,
                "outputFields": output_fields
            }
        }

        result = self._request('POST', '/document/search', request_data)

        # 提取搜索结果
        # API 返回格式: {"documents": [[{doc1}, {doc2}, ...]]}
        # documents 是一个二维数组，需要提取第一层
        raw_documents = result.get('documents', [[]])
        search_results = raw_documents[0] if raw_documents and len(raw_documents) > 0 else []

        logging.info(f"搜索返回 {len(search_results)} 个结果")

        # 返回原始结果，但添加提取的文档列表便于访问
        result['_extracted_documents'] = search_results
        return result

    def query_documents(
        self,
        database: str,
        collection: str,
        ids: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        output_fields: Optional[List[str]] = None,
        retrieve_vector: bool = False,
        read_consistency: str = "eventualConsistency"
    ) -> Dict[str, Any]:
        """查询文档（通过 ID 或过滤条件）

        Args:
            database: 数据库名称
            collection: Collection 名称
            ids: 文档 ID 列表（可选）
            filter: 过滤条件（可选），例如 {"work_id": "work_123"}
            limit: 返回结果数量（可选）
            output_fields: 需要返回的字段列表
            retrieve_vector: 是否返回向量
            read_consistency: 读取一致性级别（可选，默认 "eventualConsistency"）
                            - "eventualConsistency": 最终一致性（默认，性能更好）
                            - "strongConsistency": 强一致性（确保读到最新数据）

        Returns:
            Dict: 查询结果，包含 documents 列表

        Example:
            >>> # 按 ID 查询
            >>> result = client.query_documents(
            ...     database="langtaosha_test",
            ...     collection="lt_biorxiv_history",
            ...     ids=["biorxiv_history:work_123:abstract"],
            ...     output_fields=["work_id", "text"]
            ... )
            >>> # 按条件查询（使用强一致性）
            >>> result = client.query_documents(
            ...     database="langtaosha_test",
            ...     collection="lt_biorxiv_history",
            ...     filter={"work_id": "work_123"},
            ...     limit=10,
            ...     read_consistency="strongConsistency"
            ... )
        """
        if output_fields is None:
            output_fields = ["work_id", "paper_id", "source_name", "text_type", "text"]

        logging.info(f"查询 {database}.{collection} 中的文档")

        # 构建查询对象（腾讯云 API 要求所有查询参数在 query 对象内）
        query_obj = {
            "retrieveVector": retrieve_vector,
            "outputFields": output_fields,
            "offset": 0  # 默认从第一条开始
        }

        # 构建查询条件
        if ids:
            # 使用主键过滤（腾讯云 API 使用 documentIds）
            query_obj["documentIds"] = ids
            logging.info(f"按 ID 查询: {ids}")

        if filter:
            # 将字典过滤条件转换为腾讯云 Filter 表达式格式
            # 简单实现：支持单个字段相等
            filter_parts = []
            for key, value in filter.items():
                if isinstance(value, str):
                    filter_parts.append(f'{key}="{value}"')
                else:
                    filter_parts.append(f'{key}={value}')

            if filter_parts:
                query_obj["filter"] = " and ".join(filter_parts)
                logging.info(f"按条件查询: {query_obj['filter']}")

        if limit:
            query_obj["limit"] = limit
            logging.info(f"限制返回数量: {limit}")

        # 构建请求数据（按照腾讯云官方文档格式）
        request_data = {
            "database": database,
            "collection": collection,
            "readConsistency": read_consistency,  # 使用传入的一致性级别
            "query": query_obj
        }

        result = self._request('POST', '/document/query', request_data)

        # 提取查询结果
        documents = result.get('documents', [])
        logging.info(f"查询返回 {len(documents)} 个文档")

        return result
