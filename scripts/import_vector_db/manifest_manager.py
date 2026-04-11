#!/usr/bin/env python3
"""
Manifest 管理器

管理全局 manifest.json 文件，记录所有 shard 的元信息。
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ManifestManager:
    """Manifest 管理器
    
    管理 FAISS 索引的全局 manifest 文件。
    """
    
    def __init__(self, manifest_path: Path):
        """初始化 Manifest 管理器
        
        Args:
            manifest_path: Manifest 文件路径（例如 faiss/manifest.json）
        """
        self.manifest_path = Path(manifest_path)
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 加载或创建 manifest
        if self.manifest_path.exists():
            self.manifest = self._load_manifest()
        else:
            self.manifest = self._create_empty_manifest()
    
    def _create_empty_manifest(self) -> Dict[str, Any]:
        """创建空的 manifest
        
        Returns:
            Dict: 空的 manifest 结构
        """
        return {
            "version": "1.0",
            "embedding_model": "GritLM/GritLM-7B",
            "vector_dim": 4096,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "shards": []
        }
    
    def _load_manifest(self) -> Dict[str, Any]:
        """加载 manifest 文件
        
        Returns:
            Dict: Manifest 数据
        """
        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载 manifest 失败: {e}")
            return self._create_empty_manifest()
    
    def save_manifest(self):
        """保存 manifest 到文件"""
        self.manifest["updated_at"] = datetime.now().isoformat()
        
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            json.dump(self.manifest, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Manifest 已保存: {self.manifest_path}")
    
    def add_shard(
        self,
        shard_id: str,
        paper_id_range: List[int],
        index_path: str,
        ids_path: str,
        total_vectors: int,
        status: str = "ready"
    ):
        """添加或更新 shard 信息
        
        Args:
            shard_id: Shard ID（例如 "000"）
            paper_id_range: Paper ID 范围 [start, end]
            index_path: FAISS 索引文件路径（相对路径）
            ids_path: Paper ID 映射文件路径（相对路径）
            total_vectors: 总向量数
            status: 状态（"ready", "building", "error"）
        """
        shard_info = {
            "shard_id": shard_id,
            "paper_id_range": paper_id_range,
            "index_path": index_path,
            "ids_path": ids_path,
            "total_vectors": total_vectors,
            "status": status,
            "updated_at": datetime.now().isoformat()
        }
        
        # 查找是否已存在
        shard_idx = None
        for i, shard in enumerate(self.manifest["shards"]):
            if shard["shard_id"] == shard_id:
                shard_idx = i
                break
        
        if shard_idx is not None:
            # 更新现有 shard
            self.manifest["shards"][shard_idx] = shard_info
            logger.info(f"更新 shard {shard_id} 信息")
        else:
            # 添加新 shard
            self.manifest["shards"].append(shard_info)
            logger.info(f"添加 shard {shard_id} 信息")
        
        self.save_manifest()
    
    def update_shard_status(self, shard_id: str, status: str):
        """更新 shard 状态
        
        Args:
            shard_id: Shard ID
            status: 新状态
        """
        for shard in self.manifest["shards"]:
            if shard["shard_id"] == shard_id:
                shard["status"] = status
                shard["updated_at"] = datetime.now().isoformat()
                self.save_manifest()
                logger.info(f"更新 shard {shard_id} 状态为: {status}")
                return
        
        logger.warning(f"Shard {shard_id} 不存在")
    
    def get_shard_info(self, shard_id: str) -> Optional[Dict[str, Any]]:
        """获取 shard 信息
        
        Args:
            shard_id: Shard ID
            
        Returns:
            Dict: Shard 信息，如果不存在返回 None
        """
        for shard in self.manifest["shards"]:
            if shard["shard_id"] == shard_id:
                return shard
        return None
    
    def get_all_shards(self) -> List[Dict[str, Any]]:
        """获取所有 shard 信息
        
        Returns:
            List[Dict]: 所有 shard 信息列表
        """
        return self.manifest.get("shards", [])
    
    def get_manifest(self) -> Dict[str, Any]:
        """获取完整 manifest
        
        Returns:
            Dict: 完整 manifest 数据
        """
        return self.manifest.copy()


def main():
    """主函数（测试用）"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manifest 管理器工具')
    parser.add_argument('--manifest-path', type=str, required=True, help='Manifest 文件路径')
    parser.add_argument('--list', action='store_true', help='列出所有 shard')
    
    args = parser.parse_args()
    
    manager = ManifestManager(Path(args.manifest_path))
    
    if args.list:
        shards = manager.get_all_shards()
        print(f"共有 {len(shards)} 个 shard:")
        for shard in shards:
            print(f"  Shard {shard['shard_id']}: {shard['total_vectors']} 向量, 状态: {shard['status']}")


if __name__ == '__main__':
    main()



