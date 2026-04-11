"""向量数据库操作类 - 基于AIgnite实现，适配PD_TEST项目"""
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
import faiss
import logging
import os
from dataclasses import dataclass
import torch
from pathlib import Path
from sqlalchemy import text
import threading

from src.config.config_loader import get_db_engine as get_engine

# LangChain imports
from langchain_community.vectorstores import FAISS
from langchain_core.embeddings import Embeddings
from langchain_core.documents import Document

# 设置日志
logger = logging.getLogger(__name__)

@dataclass
class VectorEntry:
    """Class for storing vector database entries."""
    work_id: str  # 使用 work_id 而不是 doc_id
    text: str
    text_type: str  # 'abstract' or 'chunk' or 'combined'
    chunk_id: Optional[str] = None
    vector: Optional[np.ndarray] = None


class GritLMEmbeddings(Embeddings):
    """GritLM embedding model wrapper for LangChain compatibility."""
    
    def __init__(self, model_name: str = 'GritLM/GritLM-7B', model_path: Optional[str] = None, query_instruction: str = "Given a scientific paper title, retrieve the paper's abstract"):
        """Initialize GritLM embeddings.
        
        Args:
            model_name: Name of the GritLM model to use (HuggingFace model name)
            model_path: Optional local path to the model (takes precedence over model_name)
            query_instruction: Instruction to use for query embeddings
        """
        self.model_name = model_path if model_path else model_name
        self.query_instruction = query_instruction
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(os.environ)
        try:
            # 清理可能存在的错误代理配置
            '''
            import os
            for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
                proxy_value = os.environ.get(proxy_var)
                if proxy_value:
                    # 检查是否是 Markdown 格式的链接（包含 [ 和 ]）
                    if '[' in proxy_value and ']' in proxy_value:
                        logger.warning(f"检测到格式错误的代理配置 {proxy_var}: {proxy_value}")
                        logger.warning(f"临时取消 {proxy_var} 设置以修复问题")
                        os.environ.pop(proxy_var, None)
                    # 检查是否是有效的 URL 格式
                    elif not proxy_value.startswith(('http://', 'https://')):
                        logger.warning(f"检测到格式错误的代理配置 {proxy_var}: {proxy_value}")
                        logger.warning(f"临时取消 {proxy_var} 设置以修复问题")
                        os.environ.pop(proxy_var, None)
            
            # 设置正确的 HuggingFace 缓存路径
            user_home = os.path.expanduser('~')
            user_cache = os.path.join(user_home, '.cache', 'huggingface')
            
            
            # 清理可能指向其他用户路径的环境变量（静默清理）
            for key in list(os.environ.keys()):
                if key.startswith('HF_') or 'HUGGINGFACE' in key.upper():
                    env_value = str(os.environ.get(key, ''))
                    # 如果路径不属于当前用户，静默清除
                    if env_value and not user_home in env_value and '/data3/' in env_value:
                        del os.environ[key]
            
            
            # 强制设置正确的缓存路径
            os.environ['HF_HOME'] = user_cache
            # 设置 transformers 缓存路径
            os.environ['TRANSFORMERS_CACHE'] = user_cache
            # 设置 huggingface_hub 缓存路径
            os.environ['HF_HUB_CACHE'] = os.path.join(user_cache, 'hub')
            
            logger.info(f"使用 HuggingFace 缓存路径: {user_cache}")
            '''

            from gritlm import GritLM
            # Initialize using GritLM official library
            # If model_path is provided, use it; otherwise use model_name (HuggingFace name)
            model_to_load = self.model_name

            logger.info(f"Loading GritLM model from: {model_to_load}")


            print(model_to_load)
            
            self.model = GritLM(model_to_load, torch_dtype="auto")
            
            # Set use_cache to False for better performance
            try:
                self.model.model.config.use_cache = False
            except AttributeError:
                self.model.config.use_cache = False
                
            logger.info(f"Successfully loaded GritLM model: {model_to_load}")
        except Exception as e:
            error_msg = f"Failed to load GritLM model {self.model_name}: {str(e)}"
            logger.error(error_msg)
            if "Network is unreachable" in str(e) or "couldn't connect" in str(e).lower():
                logger.error("网络连接失败，无法从 HuggingFace 下载模型。")
                logger.error("解决方案：")
                logger.error("1. 设置环境变量 GRITLM_MODEL_PATH 指向本地模型路径")
                logger.error("2. 或者确保网络可以访问 HuggingFace")
                logger.error("3. 或者使用离线模式（需要先下载模型）")
            raise
        
    def gritlm_instruction(self, instruction: str) -> str:
        """Format instruction for GritLM.
        
        Args:
            instruction: Instruction text
            
        Returns:
            Formatted instruction string
        """
        return "<|user|>\n" + instruction + "\n<|embed|>\n" if instruction else "<|embed|>\n"
    
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of documents.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors
        """
        try:
            # Clean and normalize texts
            cleaned_texts = [text.strip() for text in texts]
            
            # Get embeddings using GritLM's encode method with empty instruction for documents
            embeddings = self.model.encode(cleaned_texts, instruction=self.gritlm_instruction(""))
            
            # Convert to numpy array for normalization
            embeddings = np.array(embeddings, dtype=np.float32)
            
            # Normalize the vectors
            faiss.normalize_L2(embeddings)
            
            # Convert to list of lists
            return embeddings.tolist()
            
        except Exception as e:
            logger.error(f"Failed to embed documents: {str(e)}")
            raise
    
    def embed_query(self, text: str) -> List[float]:
        """Embed a single query text.
        
        Args:
            text: Text to embed
            
        Returns:
            Embedding vector
        """
        try:
            # Clean and normalize the text
            text = text.strip()
            
            # Get embedding using GritLM's encode method with query instruction
            embedding = self.model.encode([text], instruction=self.gritlm_instruction(self.query_instruction))[0]
            
            # Convert to numpy array for normalization
            embedding = np.array(embedding, dtype=np.float32).reshape(1, -1)
            
            # Normalize the vector
            faiss.normalize_L2(embedding)
            
            return embedding[0].tolist()
            
        except Exception as e:
            logger.error(f"Failed to embed query: {str(e)}")
            raise
    
    def __del__(self):
        """Cleanup method."""
        if hasattr(self, 'model'):
            try:
                del self.model
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except:
                pass
            self.model = None


class VectorDB:
    """Vector database implementation using LangChain FAISS."""
    
    def __init__(self, db_path: str, model_name: str = 'GritLM/GritLM-7B', model_path: Optional[str] = None, vector_dim: int = 4096):
        """Initialize vector database with embedding model.
        
        Args:
            db_path: Path to save/load the vector database. Will try to load existing DB from this path first.
            model_name: Name of the embedding model to use (HuggingFace model name)
            model_path: Optional local path to the model (takes precedence over model_name)
            vector_dim: Dimension of the embedding vectors (default: 4096 for GritLM-7B)
            
        Raises:
            ValueError: If db_path is not provided
        """
        if not db_path:
            raise ValueError("db_path must be provided for VectorDB initialization")
            
        self.db_path = db_path
        self.vector_dim = vector_dim
        self.model_name = model_name
        self.model_path = model_path
        
        # Multi-shard cache for performance optimization (P0)
        self._multi_shard_cache: Optional[Dict[str, Any]] = None
        self._multi_shard_cache_lock = threading.Lock()
        
        # Initialize embedding model
        if "gritlm" in model_name.lower() or (model_path and "gritlm" in model_path.lower()):
            self.embeddings = GritLMEmbeddings(model_name=model_name, model_path=model_path)
        else:
            raise ValueError(f"Unsupported model: {model_name}. Only GritLM is supported.")
        
        logger.info(f"Initialized embeddings with model: {model_name}")
        logger.info(f"Vector database path: {db_path}")
        
        # Try to load existing database first
        if self.exists():
            logger.info(f"Found existing vector database at {db_path}")
            if self.load():
                logger.info("Successfully loaded existing vector database")
                return
            else:
                logger.warning("Failed to load existing vector database, initializing new one")
        else:
            logger.info("No existing vector database found, initializing new one")
            # Create directory for new database if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if db_dir:  # Only create directory if there's a directory path
                os.makedirs(db_dir, exist_ok=True)
                logger.info(f"Created directory for new vector database at {db_dir}")
            else:
                logger.info(f"Using current directory for vector database: {self.db_path}")
        
        # Initialize new FAISS index if loading failed or database doesn't exist
        self.faiss_store = FAISS.from_texts(
            texts=["dummy"],  # Start with dummy text
            embedding=self.embeddings,
            metadatas=[{"work_id": "dummy", "text_type": "dummy"}],  # 使用 work_id
            ids=["dummy"]  # Specify custom ID for the dummy document
        )
        # Remove the dummy entry
        self.faiss_store.delete(["dummy"])

    def __del__(self):
        """Cleanup method."""
        if hasattr(self, 'embeddings'):
            self.embeddings = None
        if hasattr(self, 'faiss_store'):
            self.faiss_store = None

    def save(self) -> bool:
        """Save the vector database to disk.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if db_dir:  # Only create directory if there's a directory path
                os.makedirs(db_dir, exist_ok=True)
            
            # Save using LangChain FAISS save_local
            self.faiss_store.save_local(self.db_path, index_name="index")
            logger.info(f"Successfully saved vector database to {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save vector database: {str(e)}")
            return False

    def load(self) -> bool:
        """Load the vector database from disk.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if files exist
            if not self.exists():
                logger.info("Vector database files don't exist")
                return False
                
            # Load using LangChain FAISS load_local
            self.faiss_store = FAISS.load_local(
                folder_path=self.db_path,
                embeddings=self.embeddings,
                index_name="index",
                allow_dangerous_deserialization=True
            )
            logger.info(f"Successfully loaded vector database from {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to load vector database: {str(e)}")
            return False

    def exists(self) -> bool:
        """Check if vector database files exist.
        
        Returns:
            bool: True if database files exist, False otherwise
        """
        index_file = os.path.join(self.db_path, "index.pkl")
        return os.path.exists(index_file)

    def _document_to_vector_entry(self, doc: Document) -> VectorEntry:
        """Convert LangChain Document to VectorEntry.
        
        Args:
            doc: LangChain Document object
            
        Returns:
            VectorEntry object
        """
        return VectorEntry(
            work_id=doc.metadata.get("work_id", ""),  # 使用 work_id
            text=doc.page_content,
            text_type=doc.metadata.get("text_type", ""),
            chunk_id=doc.metadata.get("chunk_id")
        )

    def add_document(self, work_id: str, text_to_emb: str, text_type: str = "abstract", auto_save: bool = False) -> bool:
        """Add a document to the vector database.
        
        Args:
            work_id: Work ID (used as vector database ID)
            text_to_emb: Text content to embed and store (usually abstract)
            text_type: Type of text (default: "abstract")
            auto_save: Whether to automatically save after adding (default: False, for performance)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if document with this work_id already exists
            if work_id in self.faiss_store.docstore._dict:
                logger.warning(f"Document with work_id {work_id} already exists in vector database. Skipping addition.")
                return False
            
            # Create Document object with work_id in metadata
            document = Document(
                page_content=text_to_emb,
                metadata={"work_id": work_id, "text_type": text_type}  # 使用 work_id
            )
            
            # Add document to FAISS store (use work_id as the ID)
            self.faiss_store.add_documents([document], ids=[work_id])
            
            # Save to disk if auto_save is enabled
            if auto_save:
                self.save()
            
            logger.info(f"Successfully added document with work_id: {work_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add document with work_id {work_id} to vector database: {str(e)}")
            return False

    def delete_document(self, work_id: str, auto_save: bool = False) -> bool:
        """Delete a document from the vector database.
        
        Args:
            work_id: Work ID to delete
            auto_save: Whether to automatically save after deleting (default: False, for performance)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if document exists
            if work_id not in self.faiss_store.docstore._dict:
                logger.warning(f"Document with work_id {work_id} not found in vector database.")
                return False
            
            # Delete the document
            self.faiss_store.delete([work_id])
            
            # Save to disk if auto_save is enabled
            if auto_save:
                self.save()
            
            logger.info(f"Successfully deleted document with work_id: {work_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete document with work_id {work_id} from vector database: {str(e)}")
            return False

    def get_all_work_ids(self) -> List[str]:
        """Get all work_ids from the vector database.
        
        Returns:
            List of work_ids stored in the vector database
        """
        try:
            if not hasattr(self.faiss_store, 'docstore') or not hasattr(self.faiss_store.docstore, '_dict'):
                logger.warning("Vector database docstore not accessible")
                return []
            
            # Extract work_ids from docstore
            work_ids = []
            for doc_id, doc in self.faiss_store.docstore._dict.items():
                if hasattr(doc, 'metadata') and 'work_id' in doc.metadata:
                    work_ids.append(doc.metadata['work_id'])
            
            work_ids_list = sorted(list(set(work_ids)))  # Remove duplicates and sort
            logger.info(f"Retrieved {len(work_ids_list)} work_ids from vector database")
            return work_ids_list
            
        except Exception as e:
            logger.error(f"Failed to get all work_ids from vector database: {str(e)}")
            return []

    def search(
        self,
        query: str,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[VectorEntry, float]]:
        """Search for similar vectors.
        
        Args:
            query: Search query text
            filters: Optional filters (currently not implemented, reserved for future use)
            top_k: Number of results to return
            
        Returns:
            List of tuples containing (VectorEntry, similarity_score)
        """
        try:
            # Use LangChain wrapper for search
            docs_with_scores = self.faiss_store.similarity_search_with_score(
                query=query,
                k=top_k
            )
            
            results = []
            for doc, score in docs_with_scores:
                entry = self._document_to_vector_entry(doc)
                # For normalized vectors with inner product, distance is similarity
                # FAISS returns distance, we convert to similarity
                #similarity_score = float(1.0 - score) if score <= 1.0 else float(score)
                similarity_score = float(score)
                results.append((entry, similarity_score))
            
            logger.info(f"Search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return []

    @staticmethod
    def load_from_multi_shards(shards_dir: str, use_mmap: bool = True) -> Dict[str, Any]:
        """Load multiple FAISS shard indices from a directory.
        
        Args:
            shards_dir: Path to the shards directory (e.g., '{base_path}/shards')
            use_mmap: Whether to use memory mapping for loading indices (P1 optimization, default: True)
            
        Returns:
            Dict containing:
                - 'indices': List of FAISS indices
                - 'id_mappings': List of paper_id numpy arrays (one per shard)
                - 'shard_ids': List of shard IDs
                - 'shards_dir': The shards directory path (for cache validation)
        """
        shards_path = Path(shards_dir)
        if not shards_path.exists():
            logger.warning(f"Shards directory does not exist: {shards_dir}")
            return {
                'indices': [],
                'id_mappings': [],
                'shard_ids': []
            }
        
        # Find all shard index files
        index_files = sorted(shards_path.glob("shard_*.index"))
        if not index_files:
            logger.warning(f"No shard index files found in {shards_dir}")
            return {
                'indices': [],
                'id_mappings': [],
                'shard_ids': []
            }
        
        indices = []
        id_mappings = []
        shard_ids = []
        
        for index_file in index_files:
            try:
                # Extract shard_id from filename (e.g., "shard_000.index" -> 0)
                shard_id_str = index_file.stem.replace('shard_', '')
                shard_id = int(shard_id_str)
                
                # Load FAISS index (P1: use mmap for better performance on shared storage)
                try:
                    if use_mmap:
                        # Try to use memory mapping for faster loading and lower memory usage
                        try:
                            index = faiss.read_index(str(index_file), faiss.IO_FLAG_MMAP | faiss.IO_FLAG_READ_ONLY)
                            logger.debug(f"Loaded shard {shard_id} using mmap")
                        except (AttributeError, TypeError):
                            # Fallback if IO_FLAG_MMAP is not available in this FAISS version
                            index = faiss.read_index(str(index_file))
                            logger.debug(f"Loaded shard {shard_id} using standard read (mmap not available)")
                    else:
                        index = faiss.read_index(str(index_file))
                except Exception as e:
                    # Fallback to standard read if mmap fails
                    logger.warning(f"Failed to load shard {shard_id} with mmap, falling back to standard read: {e}")
                    index = faiss.read_index(str(index_file))
                
                indices.append(index)
                
                # Load corresponding ID mapping file
                ids_file = shards_path / f"shard_{shard_id_str}.ids.npy"
                if ids_file.exists():
                    paper_ids = np.load(str(ids_file))
                    id_mappings.append(paper_ids)
                else:
                    logger.warning(f"ID mapping file not found for shard {shard_id}: {ids_file}")
                    # Create empty array if IDs file doesn't exist
                    id_mappings.append(np.array([], dtype=np.int64))
                
                shard_ids.append(shard_id)
                logger.info(f"Loaded shard {shard_id}: {index.ntotal} vectors")
                
            except Exception as e:
                logger.error(f"Failed to load shard from {index_file}: {e}", exc_info=True)
                # Continue loading other shards
                continue
        
        logger.info(f"Successfully loaded {len(indices)} shards from {shards_dir}")
        return {
            'indices': indices,
            'id_mappings': id_mappings,
            'shard_ids': shard_ids,
            'shards_dir': shards_dir  # Store for cache validation
        }

    def _paper_ids_to_vector_entries(
        self,
        paper_ids: List[int],
        similarities: List[float]
    ) -> List[Tuple[VectorEntry, float]]:
        """Convert paper_ids to VectorEntry objects by querying the database.
        
        Args:
            paper_ids: List of paper_id values
            similarities: List of similarity scores (corresponding to paper_ids)
            
        Returns:
            List of tuples containing (VectorEntry, similarity_score)
        """
        if not paper_ids:
            return []
        
        try:
            engine = get_engine()
            with engine.connect() as conn:
                # Batch query database for work_id and abstract
                # Use parameterized query with IN clause
                result = conn.execute(
                    text("""
                        SELECT paper_id, work_id, title, abstract
                        FROM papers
                        WHERE paper_id = ANY(:paper_ids)
                    """),
                    {"paper_ids": paper_ids}
                )
                
                # Create a mapping from paper_id to (work_id, title,abstract)
                paper_info_map = {}
                for row in result:
                    paper_info_map[row[0]] = (row[1], row[2], row[3] or "")
                
                # Build results list
                results = []
                for paper_id, similarity in zip(paper_ids, similarities):
                    if paper_id in paper_info_map:
                        work_id, title, abstract = paper_info_map[paper_id]
                        if work_id:  # Only add if work_id exists
                            entry = VectorEntry(
                                work_id=str(work_id),
                                text=f"{title} {abstract}",
                                text_type="abstract",
                                chunk_id=None
                            )
                            results.append((entry, float(similarity)))
                    else:
                        logger.warning(f"Paper ID {paper_id} not found in database")
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to convert paper_ids to VectorEntry: {e}", exc_info=True)
            return []

    def search_from_multi_shards(
        self,
        query: str,
        shards_dir: str,
        top_k: int = 10,
        shard_top_k: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[VectorEntry, float]]:
        """Search across multiple shard indices and merge results.
        
        Args:
            query: Search query text
            shards_dir: Path to the shards directory (e.g., '{base_path}/shards')
            top_k: Number of final results to return
            shard_top_k: Number of results to retrieve from each shard (default: top_k * 2)
            filters: Optional filters (currently not implemented, reserved for future use)
            
        Returns:
            List of tuples containing (VectorEntry, similarity_score), sorted by similarity
        """
        if shard_top_k is None:
            shard_top_k = top_k * 2  # Get more candidates from each shard for better merging
        
        try:
            # P0: Use cached shard data if available
            with self._multi_shard_cache_lock:
                if self._multi_shard_cache is not None:
                    # Use cached data
                    shard_data = self._multi_shard_cache
                    logger.debug(f"Using cached shard data for {shards_dir}")
                else:
                    # Cache not loaded - need to call load_multi_shard_cache() first
                    raise RuntimeError(
                        f"Multi-shard cache not loaded. Please call load_multi_shard_cache('{shards_dir}') "
                        "before using search_from_multi_shards(). This should be done once at service startup."
                    )
            
            indices = shard_data['indices']
            id_mappings = shard_data['id_mappings']
            shard_ids = shard_data['shard_ids']
            
            if not indices:
                logger.warning(f"No shard indices loaded from {shards_dir}")
                return []
            
            # Generate query embedding
            query_vector_list = self.embeddings.embed_query(query)
            query_vector = np.array([query_vector_list], dtype=np.float32)
            
            # Ensure query vector is normalized (embeddings.embed_query already normalizes)
            faiss.normalize_L2(query_vector)
            
            # Search in each shard
            all_results = []  # List of (paper_id, similarity_score) tuples
            
            for idx, (index, id_mapping, shard_id) in enumerate(zip(indices, id_mappings, shard_ids)):
                try:
                    # Search in this shard
                    distances, indices_array = index.search(query_vector, min(shard_top_k, index.ntotal))
                    
                    # Convert FAISS indices to paper_ids using the ID mapping
                    for i in range(len(indices_array[0])):
                        faiss_idx = indices_array[0][i]
                        if faiss_idx >= 0 and faiss_idx < len(id_mapping):
                            paper_id = int(id_mapping[faiss_idx])
                            similarity = float(distances[0][i])
                            all_results.append((paper_id, similarity))
                    
                    logger.debug(f"Shard {shard_id}: found {len(indices_array[0])} candidates")
                    
                except Exception as e:
                    logger.error(f"Search failed in shard {shard_id}: {e}", exc_info=True)
                    continue
            
            if not all_results:
                logger.warning("No results found in any shard")
                return []
            
            # Remove duplicates (keep highest similarity)
            paper_id_to_score = {}
            for paper_id, similarity in all_results:
                if paper_id not in paper_id_to_score or similarity > paper_id_to_score[paper_id]:
                    paper_id_to_score[paper_id] = similarity
            
            # Sort by similarity (descending) and take top_k
            sorted_results = sorted(
                paper_id_to_score.items(),
                key=lambda x: x[1],
                reverse=True
            )[:top_k]
            
            # Convert to paper_ids and similarities lists
            final_paper_ids = [paper_id for paper_id, _ in sorted_results]
            final_similarities = [similarity for _, similarity in sorted_results]
            
            # Convert to VectorEntry objects
            results = self._paper_ids_to_vector_entries(final_paper_ids, final_similarities)
            
            logger.info(f"Multi-shard search returned {len(results)} results from {len(indices)} shards")
            return results
            
        except Exception as e:
            logger.error(f"Multi-shard search failed: {e}", exc_info=True)
            return []

    def load_multi_shard_cache(self, shards_dir: str, use_mmap: bool = True) -> bool:
        """Load multi-shard indices into cache (P0: call this at service startup).
        
        This method should be called once at service startup or before the first search
        to load shard indices into memory cache. This moves the expensive loading operation
        out of the search hot path.
        
        Args:
            shards_dir: Path to the shards directory (e.g., '{base_path}/shards')
            use_mmap: Whether to use memory mapping for loading indices (P1 optimization, default: True)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Loading multi-shard cache from {shards_dir}")
            shard_data = self.load_from_multi_shards(shards_dir, use_mmap=use_mmap)
            
            with self._multi_shard_cache_lock:
                self._multi_shard_cache = shard_data
            
            logger.info(f"Successfully loaded {len(shard_data['indices'])} shards into cache")
            return True
        except Exception as e:
            logger.error(f"Failed to load multi-shard cache: {e}", exc_info=True)
            return False

    def clear_multi_shard_cache(self) -> None:
        """Clear the multi-shard cache.
        
        This method can be used to force reloading of shard indices on the next search.
        Useful when shard indices are updated or when memory needs to be freed.
        """
        with self._multi_shard_cache_lock:
            if self._multi_shard_cache is not None:
                logger.info("Clearing multi-shard cache")
                self._multi_shard_cache = None

    def is_multi_shard_cache_loaded(self) -> bool:
        """Check if multi-shard cache is loaded.
        
        Returns:
            bool: True if cache is loaded, False otherwise
        """
        with self._multi_shard_cache_lock:
            return self._multi_shard_cache is not None

