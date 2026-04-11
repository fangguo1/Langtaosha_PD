#!/usr/bin/env python3
"""
数据导入脚本：将 JSON 文件导入到 PostgreSQL 数据库
使用 docset_hub 架构完成数据验证、转换和存储
"""

import sys
import os
import argparse
import time
import shutil
import tempfile
import json
import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from collections import deque
from itertools import islice

# 添加项目根目录与 src 到路径（仓库根 = scripts 的上级）
_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "src"))

# 加载配置（必须在导入其他模块之前）
# 明确指定配置文件路径，确保能找到配置文件
from config.config_loader import set_env_from_config


# 导入 docset_hub 模块
from docset_hub.input_adapters import JSONAdapter
from docset_hub.metadata import MetadataValidator
from docset_hub.storage import MetadataDB


class ErrorLogger:
    """错误日志记录器，使用缓冲批量写入文件（同步模式）
    
    使用缓冲机制减少 I/O 操作，当缓冲区达到指定大小时自动刷新到文件。
    支持 JSON Lines 格式，每行一个 JSON 对象。
    """
    
    def __init__(self, error_log_path: Path, json_dir: Optional[Path] = None, buffer_size: int = 100):
        """初始化错误日志记录器
        
        Args:
            error_log_path: 错误日志文件路径
            json_dir: JSON 文件根目录（用于计算相对路径，None 表示不转换）
            buffer_size: 缓冲大小（累积多少个错误后刷新到文件，默认: 100）
        """
        self.error_log_path = error_log_path
        self.json_dir = json_dir.resolve() if json_dir else None
        self.buffer_size = buffer_size
        self.buffer = []
        self.file_handle = None
        
        # 确保目录存在
        self.error_log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 打开文件（追加模式，支持中断恢复）
        self.file_handle = open(self.error_log_path, 'a', encoding='utf-8')
    
    def _normalize_file_path(self, file_path: str) -> str:
        """将文件路径转换为相对路径格式（json-dir/json_file）
        
        Args:
            file_path: 文件路径（可能是绝对路径）
            
        Returns:
            str: 相对路径格式，例如 subdir/file.json
        """
        if not self.json_dir:
            return file_path
        
        try:
            file_path_str = str(file_path)
            json_dir_str = str(self.json_dir)
            

            print(f"file_path_str: {file_path_str}")
            print(f"json_dir_str: {json_dir_str}")
            # 如果路径以 json_dir 开头，提取相对部分
            if file_path_str.startswith(json_dir_str):
                relative_part = file_path_str[len(json_dir_str):].lstrip('/')
                return relative_part if relative_part else file_path_str
            
            # 尝试使用 Path 对象计算相对路径
            file_path_obj = Path(file_path)
            json_dir_obj = Path(json_dir_str)
            
            relative_path = file_path_obj.relative_to(json_dir_obj.resolve())
            return str(relative_path)
            
            return file_path_str
        except Exception:
            # 任何异常都返回原始路径
            return str(file_path)
    
    def log_error(self, error_info: Dict[str, Any]) -> None:
        """记录错误到缓冲区
        
        Args:
            error_info: 错误信息字典
        """
        # 复制错误信息
        error_info_with_timestamp = error_info.copy()
                
        # 添加时间戳
        error_info_with_timestamp['timestamp'] = datetime.datetime.now().isoformat()
        
        self.buffer.append(error_info_with_timestamp)
        
        # 达到缓冲区大小时自动刷新
        if len(self.buffer) >= self.buffer_size:
            self.flush()
    
    def flush(self) -> None:
        """立即刷新缓冲区到文件"""
        if not self.buffer or not self.file_handle:
            return
        
        for error_info in self.buffer:
            json_line = json.dumps(error_info, ensure_ascii=False)
            self.file_handle.write(json_line + '\n')
        
        # 立即刷新到磁盘
        self.file_handle.flush()
        self.buffer.clear()
    
    def close(self) -> None:
        """关闭日志记录器，刷新所有剩余数据"""
        # 刷新剩余数据
        self.flush()
        
        # 关闭文件
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None


def _find_json_files_generator(directory: Path, progress_interval: int = 10000):
    """使用 os.scandir 创建 JSON 文件生成器（避免 rglob 在百万级文件目录中卡住）
    
    使用 os.scandir 替代 os.walk，性能更好，能更快找到第一个文件。
    
    Args:
        directory: 要搜索的目录路径
        progress_interval: 每找到多少个文件输出一次进度提示
        
    Yields:
        Path: JSON 文件路径
    """
    found_count = 0
    dirs_to_process = [str(directory)]
    dirs_processed = 0
    
    print("  开始遍历目录...", flush=True)
    
    while dirs_to_process:
        current_dir = dirs_to_process.pop(0)
        dirs_processed += 1
        
        # 每处理 1000 个目录输出一次进度
        if dirs_processed % 1000 == 0:
            print(f"  已遍历 {dirs_processed} 个目录，找到 {found_count} 个 JSON 文件...", flush=True)
        
        try:
            with os.scandir(current_dir) as entries:
                for entry in entries:
                    if entry.is_file() and entry.name.endswith('.json'):
                        found_count += 1
                        if found_count == 1:
                            print(f"  ✓ 找到第一个 JSON 文件: {entry.name}", flush=True)
                        elif found_count % progress_interval == 0:
                            print(f"  已找到 {found_count} 个 JSON 文件...", flush=True)
                        yield Path(entry.path)
                    elif entry.is_dir():
                        # 将子目录加入待处理列表
                        dirs_to_process.append(entry.path)
        except PermissionError:
            # 跳过无权限访问的目录
            continue
        except Exception as e:
            # 跳过其他错误（如符号链接问题等）
            continue


def _copy_files_to_local_cache(
    source_files: List[Path], 
    cache_dir: Path, 
    batch_idx: int,
    progress_interval: int = 1000
) -> List[Tuple[Path, Path]]:
    """批量复制文件到本地缓存目录
    
    Args:
        source_files: 源文件路径列表
        cache_dir: 本地缓存目录
        batch_idx: 批次索引（用于创建子目录）
        progress_interval: 每复制多少个文件输出一次进度（默认: 1000）
        
    Returns:
        List[Tuple[Path, Path]]: (缓存文件路径, 原始文件路径) 元组列表
    """
    batch_cache_dir = cache_dir / f"batch_{batch_idx}"
    batch_cache_dir.mkdir(parents=True, exist_ok=True)
    
    cached_files = []
    copy_failures = 0
    total_files = len(source_files)
    
    print(f"  📋 开始复制 {total_files} 个文件到本地缓存...", flush=True)
    
    for idx, source_file in enumerate(source_files, 1):
        try:
            # 进度输出
            if idx % progress_interval == 0 or idx == total_files:
                print(f"  ⏳ 复制进度: {idx}/{total_files} ({idx*100//total_files}%)...", flush=True)
            
            # 保持相对路径结构，避免文件名冲突
            # 使用文件名的hash或完整相对路径作为目标文件名
            target_file = batch_cache_dir / source_file.name
            
            # 如果文件名已存在，添加序号
            counter = 1
            original_target = target_file
            while target_file.exists():
                stem = original_target.stem
                suffix = original_target.suffix
                target_file = batch_cache_dir / f"{stem}_{counter}{suffix}"
                counter += 1
            
            shutil.copy2(source_file, target_file)
            cached_files.append((target_file, source_file))
        except Exception as e:
            copy_failures += 1
            # 记录错误但不中断流程（只显示前10个错误，避免刷屏）
            if copy_failures <= 10:
                print(f"  ⚠️  复制文件失败 {source_file.name}: {str(e)}", flush=True)
    
    if copy_failures > 0:
        print(f"  ⚠️  批次 {batch_idx}: {copy_failures} 个文件复制失败", flush=True)
    
    return cached_files


def _cleanup_local_cache(cached_files: List[Tuple[Path, Path]]) -> None:
    """清理已处理的本地缓存文件
    
    Args:
        cached_files: (缓存文件路径, 原始文件路径) 元组列表
    """
    for cached_file, _ in cached_files:
        try:
            if cached_file.exists():
                cached_file.unlink()
        except Exception as e:
            # 静默处理删除失败（记录日志但不抛出异常）
            pass
    
    # 尝试删除空目录
    dirs_to_clean = set()
    for cached_file, _ in cached_files:
        if cached_file.parent.exists():
            dirs_to_clean.add(cached_file.parent)
    
    for dir_path in dirs_to_clean:
        try:
            if dir_path.exists() and not any(dir_path.iterdir()):
                dir_path.rmdir()
        except Exception:
            # 静默处理删除失败
            pass


def _check_file_already_processed(
    file_path: Path,
    adapter,
    metadata_db
) -> bool:
    """检查文件是否已经处理过
    
    Args:
        file_path: JSON文件路径
        adapter: JSONAdapter 实例
        metadata_db: MetadataDB 实例
        
    Returns:
        bool: True表示已处理，False表示未处理
    """
    try:
        # 快速解析文件获取数据（只解析，不验证）
        doc_data = adapter.parse(str(file_path))
        
        # 获取规范化后的 external IDs
        normalized_ids = metadata_db.transformer.get_normalized_external_ids(doc_data)
        
        # 检查数据库中是否已存在
        with metadata_db.engine.connect() as conn:
            existing = metadata_db.check_paper_existence(conn, normalized_ids)
            if existing:
                return True
        return False
    except Exception:
        # 如果解析失败或检查失败，返回False（继续处理，让后续流程处理错误）
        return False


def _process_cached_files(
    cached_files: List[Tuple[Path, Path]],
    adapter,
    validator,
    metadata_db,
    mock: bool,
    start_idx: int,
    total: int,
    time_stats: Dict[str, List[float]],
    errors: deque,
    error_logger: Optional[ErrorLogger] = None,
    skip_existing: bool = True
) -> Tuple[int, int, int]:
    """处理本地缓存文件
    
    Args:
        cached_files: (缓存文件路径, 原始文件路径) 元组列表
        adapter: JSONAdapter 实例
        validator: MetadataValidator 实例
        metadata_db: MetadataDB 实例
        mock: 是否模拟模式
        start_idx: 起始索引（用于进度显示）
        total: 总文件数
        time_stats: 耗时统计字典
        errors: 错误列表
        error_logger: 错误日志记录器（可选）
        skip_existing: 是否跳过已存在的文件（默认: True）
        
    Returns:
        Tuple[int, int, int]: (success_count, fail_count, skip_count)
    """
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    for local_idx, (cached_file, original_file) in enumerate(cached_files, 1):
        idx = start_idx + local_idx - 1
        try:
            # 如果启用跳过已存在文件，先检查
            if skip_existing and not mock:
                if _check_file_already_processed(cached_file, adapter, metadata_db):
                    skip_count += 1
                    print(f"⏭️  [{idx}/{total}] 跳过已处理 {cached_file.name}")
                    continue
            
            # 1. 使用 JSONAdapter 解析文件（包含基础验证）
            start_time = time.time()
            doc_data = adapter.parse(str(cached_file))
            parse_time = time.time() - start_time
            time_stats['parse'].append(parse_time)
            
            # 2. 使用 MetadataValidator 验证 DocSet 格式
            start_time = time.time()
            is_valid, validation_errors = validator.validate(doc_data)
            validate_time = time.time() - start_time
            time_stats['validate'].append(validate_time)
            
            if not is_valid:
                error_msg = f"数据验证失败: {', '.join(validation_errors)}"
                fail_count += 1
                error_info = {
                    'file': str(original_file),
                    'error': error_msg,
                    'error_type': 'ValidationError',
                    'error_detail': ', '.join(validation_errors)
                }
                errors.append(error_info)
                if error_logger:
                    error_logger.log_error(error_info)
                print(f"❌ [{idx}/{total}] 验证失败 {cached_file.name}: {error_msg}")
                continue
            
            # 3. 使用 MetadataDB.insert_paper() 存储到 PostgreSQL
            start_time = time.time()
            if mock:
                # 模拟模式：不实际插入数据库，只模拟耗时
                time.sleep(0.001)  # 模拟最小耗时
                print(f"✅ [{idx}/{total}] 模拟导入 {cached_file.name}")
                success_count += 1
            else:
                paper_id = metadata_db.insert_paper(doc_data)
                print(f"✅ [{idx}/{total}] 成功插入 {cached_file.name}，paper_id: {paper_id}")
                success_count += 1
            insert_time = time.time() - start_time
            time_stats['insert'].append(insert_time)
            
            # 每处理 100 个文件显示进度
            if idx % 100 == 0:
                print(f"✅ 已处理 {idx}/{total} 个文件 (成功: {success_count}, 失败: {fail_count}, 跳过: {skip_count})")
                
        except FileNotFoundError as e:
            fail_count += 1
            error_info = {
                'file': str(original_file),
                'error': f"文件不存在: {str(e)}",
                'error_type': 'FileNotFoundError'
            }
            errors.append(error_info)
            if error_logger:
                error_logger.log_error(error_info)
            print(f"❌ [{idx}/{total}] 文件不存在 {cached_file.name}: {e}")
            
        except ValueError as e:
            # JSONAdapter 解析或验证错误
            fail_count += 1
            error_info = {
                'file': str(original_file),
                'error': f"解析或验证失败: {str(e)}",
                'error_type': 'ValueError',
                'error_detail': str(e)
            }
            errors.append(error_info)
            if error_logger:
                error_logger.log_error(error_info)
            print(f"❌ [{idx}/{total}] 解析失败 {cached_file.name}: {e}")
            
        except Exception as e:
            # 其他错误（数据库插入失败等）
            fail_count += 1
            error_info = {
                'file': str(original_file),
                'error': f"导入失败: {str(e)}",
                'error_type': type(e).__name__,
                'error_detail': str(e)
            }
            errors.append(error_info)
            if error_logger:
                error_logger.log_error(error_info)
            print(f"❌ [{idx}/{total}] 导入失败 {cached_file.name}: {e}")
    
    return success_count, fail_count, skip_count


def import_json_files(
    json_dir: str, 
    limit: Optional[int] = None, 
    mock: bool = True,
    cache_batch_size: int = 1000,
    cache_dir: Optional[str] = None,
    error_log_file: Optional[Path] = None,
    error_buffer_size: int = 100,
    skip_existing: bool = True
) -> Dict[str, Any]:
    """导入目录下的所有 JSON 文件
    
    使用 docset_hub 架构：
    1. JSONAdapter 解析 JSON 文件并验证基础格式
    2. MetadataValidator 验证 DocSet 格式
    3. MetadataDB.insert_paper() 存储到 PostgreSQL（内部调用 transform_to_db_format）
    
    Args:
        json_dir: JSON 文件目录路径
        limit: 导入数量限制，None 表示不限制
        mock: 是否模拟导入
        cache_batch_size: 缓存批次大小（每批复制的文件数，0表示禁用缓存）
        cache_dir: 本地缓存目录路径（None表示使用系统临时目录）
        error_log_file: 错误日志文件路径（None表示不记录到文件）
        error_buffer_size: 错误缓冲大小（达到此数量后写入文件，默认: 100）
        skip_existing: 是否跳过已存在的文件（默认: True）
        
    Returns:
        dict: 导入结果统计
        {
            'total': int,      # 总文件数
            'success': int,    # 成功数
            'fail': int,      # 失败数
            'skip': int,      # 跳过数
            'errors': list    # 错误详情列表
        }
    """
    json_path = Path(json_dir)
    
    if not json_path.exists():
        raise FileNotFoundError(f"目录不存在: {json_dir}")
    
    # 使用自定义生成器获取 JSON 文件（避免 rglob 在百万级文件目录中卡住）
    # 使用 os.walk 手动实现，找到文件立即 yield，避免卡住
    if limit:
        # 如果设置了 limit，直接使用生成器并限制数量
        print(f"📊 正在查找 JSON 文件（最多处理 {limit} 个，使用生成器模式，节省内存）...")
        json_files_generator = _find_json_files_generator(json_path)
        json_files_generator = islice(json_files_generator, limit)
        total = limit  # 如果设置了 limit，总数就是 limit
        print(f"📊 将处理最多 {total} 个 JSON 文件")
    else:
        # 如果没有设置 limit，先统计总数（使用生成器计数，不存储路径）
        print("📊 正在统计 JSON 文件数量（使用生成器模式，节省内存）...")
        total = sum(1 for _ in _find_json_files_generator(json_path))
        # 重新创建生成器用于后续处理
        json_files_generator = _find_json_files_generator(json_path)
        print(f"📊 找到 {total} 个 JSON 文件")
    
    if total == 0:
        print("⚠️  未找到任何 JSON 文件")
        return {
            'total': 0,
            'success': 0,
            'fail': 0,
            'errors': []
        }
    
    # 初始化 docset_hub 组件
    adapter = JSONAdapter()
    validator = MetadataValidator()
    metadata_db = MetadataDB()
    
    # 初始化错误日志记录器
    error_logger = None
    if error_log_file:
        error_logger = ErrorLogger(error_log_file, json_dir=json_path, buffer_size=error_buffer_size)
        print(f"📝 错误日志将写入: {error_log_file}")
    
    success_count = 0
    fail_count = 0
    skip_count = 0
    # 使用 deque 限制错误列表大小，最多保留 1000 个错误（避免内存无限增长）
    errors = deque(maxlen=1000)
    
    # 耗时统计
    time_stats = {
        'parse': [],      # 步骤1：JSONAdapter 解析文件耗时
        'validate': [],   # 步骤2：MetadataValidator 验证耗时
        'insert': [],     # 步骤3：MetadataDB.insert_paper() 耗时
        'copy': [],       # 步骤0：文件复制到本地缓存耗时
        'cleanup': []     # 步骤4：清理本地缓存耗时
    }
    
    # 判断是否启用缓存模式
    use_cache = cache_batch_size > 0
    
    if use_cache:
        # 设置本地缓存目录
        if cache_dir:
            local_cache_dir = Path(cache_dir)
            local_cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            # 使用系统临时目录
            local_cache_dir = Path(tempfile.mkdtemp(prefix="json_import_cache_"))
            print(f"📁 使用临时缓存目录: {local_cache_dir}")
        
        print(f"🚀 开始批量缓存处理（批次大小: {cache_batch_size}）...")
        
        # 批量收集文件路径并处理
        batch_files = []
        batch_idx = 0
        processed_count = 0
        
        for json_file in json_files_generator:
            batch_files.append(json_file)
            
            # 达到批次大小时处理
            if len(batch_files) >= cache_batch_size:
                batch_idx += 1
                print(f"\n📦 处理批次 {batch_idx} ({len(batch_files)} 个文件)...")
                
                # 复制到本地缓存
                start_time = time.time()
                cached_files = _copy_files_to_local_cache(batch_files, local_cache_dir, batch_idx)
                copy_time = time.time() - start_time
                time_stats['copy'].append(copy_time)
                print(f"  ✓ 复制完成 ({copy_time:.2f} 秒)")
                
                if cached_files:
                    # 处理本地缓存文件
                    batch_success, batch_fail, batch_skip = _process_cached_files(
                        cached_files, adapter, validator, metadata_db, mock,
                        processed_count + 1, total, time_stats, errors, error_logger,
                        skip_existing=skip_existing
                    )
                    success_count += batch_success
                    fail_count += batch_fail
                    skip_count += batch_skip
                    processed_count += len(cached_files)
                    
                    # 清理已处理的缓存文件
                    start_time = time.time()
                    _cleanup_local_cache(cached_files)
                    cleanup_time = time.time() - start_time
                    time_stats['cleanup'].append(cleanup_time)
                    print(f"  ✓ 清理完成 ({cleanup_time:.2f} 秒)")
                
                # 清空批次列表
                batch_files = []
        
        # 处理剩余文件
        if batch_files:
            batch_idx += 1
            print(f"\n📦 处理最后批次 {batch_idx} ({len(batch_files)} 个文件)...")
            
            # 复制到本地缓存
            start_time = time.time()
            cached_files = _copy_files_to_local_cache(batch_files, local_cache_dir, batch_idx)
            copy_time = time.time() - start_time
            time_stats['copy'].append(copy_time)
            print(f"  ✓ 复制完成 ({copy_time:.2f} 秒)")
            
            if cached_files:
                # 处理本地缓存文件
                batch_success, batch_fail, batch_skip = _process_cached_files(
                    cached_files, adapter, validator, metadata_db, mock,
                    processed_count + 1, total, time_stats, errors, error_logger,
                    skip_existing=skip_existing
                )
                success_count += batch_success
                fail_count += batch_fail
                skip_count += batch_skip
                processed_count += len(cached_files)
                
                # 清理已处理的缓存文件
                start_time = time.time()
                _cleanup_local_cache(cached_files)
                cleanup_time = time.time() - start_time
                time_stats['cleanup'].append(cleanup_time)
                print(f"  ✓ 清理完成 ({cleanup_time:.2f} 秒)")
        
        # 清理临时缓存目录（如果是自动创建的）
        if not cache_dir and local_cache_dir.exists():
            try:
                shutil.rmtree(local_cache_dir)
                print(f"  ✓ 临时缓存目录已清理: {local_cache_dir}")
            except Exception as e:
                print(f"  ⚠️  清理临时缓存目录失败: {e}")
    
    else:
        # 非缓存模式：直接处理挂载点文件（原有逻辑）
        print("🚀 开始处理文件（直接处理模式，未启用缓存）...")
        for idx, json_file in enumerate(json_files_generator, 1):
            try:
                # 如果启用跳过已存在文件，先检查
                if skip_existing and not mock:
                    if _check_file_already_processed(json_file, adapter, metadata_db):
                        skip_count += 1
                        print(f"⏭️  [{idx}/{total}] 跳过已处理 {json_file.name}")
                        continue
                
                # 1. 使用 JSONAdapter 解析文件（包含基础验证）
                start_time = time.time()
                doc_data = adapter.parse(str(json_file))
                parse_time = time.time() - start_time
                time_stats['parse'].append(parse_time)

                
                # 2. 使用 MetadataValidator 验证 DocSet 格式
                start_time = time.time()
                is_valid, validation_errors = validator.validate(doc_data)
                validate_time = time.time() - start_time
                time_stats['validate'].append(validate_time)
                
                if not is_valid:
                    error_msg = f"数据验证失败: {', '.join(validation_errors)}"
                    fail_count += 1
                    error_info = {
                        'file': str(json_file),
                        'error': error_msg,
                        'error_type': 'ValidationError',
                        'error_detail': ', '.join(validation_errors)
                    }
                    errors.append(error_info)
                    if error_logger:
                        error_logger.log_error(error_info)
                    print(f"❌ [{idx}/{total}] 验证失败 {json_file.name}: {error_msg}")
                    continue
                
                # 3. 使用 MetadataDB.insert_paper() 存储到 PostgreSQL
                #    内部会自动调用 MetadataTransformer.transform_to_db_format() 进行转换
                start_time = time.time()
                if mock:
                    # 模拟模式：不实际插入数据库，只模拟耗时
                    time.sleep(0.001)  # 模拟最小耗时
                    print(f"✅ [{idx}/{total}] 模拟导入 {json_file.name}")
                    success_count += 1
                else:
                    paper_id = metadata_db.insert_paper(doc_data)
                    print(f"✅ [{idx}/{total}] 成功插入 {json_file.name}，paper_id: {paper_id}")
                    success_count += 1
                insert_time = time.time() - start_time
                time_stats['insert'].append(insert_time)


                
                # 每处理 100 个文件显示进度
                if idx % 100 == 0:
                    print(f"✅ 已处理 {idx}/{total} 个文件 (成功: {success_count}, 失败: {fail_count}, 跳过: {skip_count})")
                    
            except FileNotFoundError as e:
                fail_count += 1
                error_info = {
                    'file': str(json_file),
                    'error': f"文件不存在: {str(e)}",
                    'error_type': 'FileNotFoundError'
                }
                errors.append(error_info)
                if error_logger:
                    error_logger.log_error(error_info)
                print(f"❌ [{idx}/{total}] 文件不存在 {json_file.name}: {e}")
                
            except ValueError as e:
                # JSONAdapter 解析或验证错误
                fail_count += 1
                error_info = {
                    'file': str(json_file),
                    'error': f"解析或验证失败: {str(e)}",
                    'error_type': 'ValueError',
                    'error_detail': str(e)
                }
                errors.append(error_info)
                if error_logger:
                    error_logger.log_error(error_info)
                print(f"❌ [{idx}/{total}] 解析失败 {json_file.name}: {e}")
                
            except Exception as e:
                # 其他错误（数据库插入失败等）
                fail_count += 1
                error_info = {
                    'file': str(json_file),
                    'error': f"导入失败: {str(e)}",
                    'error_type': type(e).__name__,
                    'error_detail': str(e)
                }
                errors.append(error_info)
                if error_logger:
                    error_logger.log_error(error_info)
                print(f"❌ [{idx}/{total}] 导入失败 {json_file.name}: {e}")
    
    # 打印最终统计
    print(f"\n{'='*60}")
    print(f"✅ 导入完成！")
    print(f"{'='*60}")
    print(f"   总计: {total}")
    print(f"   成功: {success_count}")
    print(f"   失败: {fail_count}")
    if skip_count > 0:
        print(f"   跳过: {skip_count}")
    
    # 打印耗时统计
    print(f"\n{'='*60}")
    print(f"⏱️  耗时统计")
    print(f"{'='*60}")
    
    def print_time_stats(stats_list, step_name):
        """打印单个步骤的耗时统计"""
        if not stats_list:
            print(f"   {step_name}: 无数据")
            return
        
        total_time = sum(stats_list)
        avg_time = total_time / len(stats_list)
        min_time = min(stats_list)
        max_time = max(stats_list)
        
        print(f"   {step_name}:")
        print(f"      总耗时: {total_time:.3f} 秒")
        print(f"      平均耗时: {avg_time:.3f} 秒")
        print(f"      最小耗时: {min_time:.3f} 秒")
        print(f"      最大耗时: {max_time:.3f} 秒")
        print(f"      执行次数: {len(stats_list)}")
    
    # 打印耗时统计（按顺序显示）
    if use_cache and time_stats['copy']:
        print_time_stats(time_stats['copy'], "0. 文件复制到本地缓存")
    print_time_stats(time_stats['parse'], "1. JSONAdapter 解析文件")
    print_time_stats(time_stats['validate'], "2. MetadataValidator 验证")
    print_time_stats(time_stats['insert'], "3. MetadataDB.insert_paper()")
    if use_cache and time_stats['cleanup']:
        print_time_stats(time_stats['cleanup'], "4. 清理本地缓存")
    
    # 计算总耗时
    total_processing_time = sum(time_stats['parse']) + sum(time_stats['validate']) + sum(time_stats['insert'])
    if use_cache:
        total_processing_time += sum(time_stats.get('copy', [])) + sum(time_stats.get('cleanup', []))
    print(f"\n   总处理耗时: {total_processing_time:.3f} 秒")
    if success_count > 0:
        print(f"   平均每文件耗时: {total_processing_time / success_count:.3f} 秒")
    
    # 显示错误详情（最多前 10 个）
    # 注意：errors 是 deque，需要转换为 list 才能切片
    errors_list = list(errors)
    if errors_list:
        print(f"\n错误详情（显示前 {min(10, len(errors_list))} 个）:")
        for i, error in enumerate(errors_list[:10], 1):
            print(f"  {i}. {Path(error['file']).name}:")
            print(f"     错误: {error['error']}")
            if error.get('error_type'):
                print(f"     类型: {error['error_type']}")
            if error.get('error_detail'):
                print(f"     详情: {error['error_detail']}")
        
        if len(errors_list) > 10:
            print(f"  ... 还有 {len(errors_list) - 10} 个错误未显示")
        if len(errors_list) >= 1000:
            print(f"  ⚠️  注意：由于内存优化，只保留了最近 1000 个错误详情")
    
    # 关闭错误日志记录器
    if error_logger:
        error_logger.close()
        print(f"\n📝 错误日志已保存: {error_logger.error_log_path}")
    
    return {
        'total': total,
        'success': success_count,
        'fail': fail_count,
        'skip': skip_count,
        'errors': list(errors)  # 转换为 list 返回（deque 不能直接序列化）
    }





def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='将 JSON 文件导入到 PostgreSQL 数据库（使用 docset_hub 架构）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用默认路径导入所有文件（启用缓存，批次大小1000）
  python import_json_pubmed.py
  
  # 指定目录和限制数量
  python import_json_pubmed.py --json-dir /path/to/json --limit 1000
  
  # 使用自定义缓存目录和批次大小
  python import_json_pubmed.py --json-dir /path/to/json --cache-dir /tmp/my_cache --cache-batch-size 2000
  
  # 禁用缓存模式（直接处理挂载点文件）
  python import_json_pubmed.py --json-dir /path/to/json --no-cache
        """
    )
    
    parser.add_argument(
        '--json-dir',
        type=str,
        default='/data3/guofang/remote_storage_home_10.0.4.7/pubmed_json/12',
        help='JSON 文件目录路径（默认: /data3/guofang/remote_storage_home_10.0.4.7/pubmed_json/12）'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='导入数量限制（默认: 不限制，导入所有文件）'
    )
    
    parser.add_argument(
        '--config-path',
        type=str,
        default=str(_REPO_ROOT / 'tests' / 'db' / 'config_backend_server_test.yaml'),
        help='配置文件路径（默认: 从 config.yaml 读取）'
    )

    parser.add_argument(
        '--mock',
        action='store_true',
        help='是否模拟导入（默认: 模拟导入）'
    )

    parser.add_argument(
        '--cache-batch-size',
        type=int,
        default=10000,
        help='缓存批次大小（每批复制的文件数，默认: 1000，设置为0禁用缓存）'
    )

    parser.add_argument(
        '--cache-dir',
        type=str,
        default=str(_REPO_ROOT / 'scripts' / 'cache'),
        help='本地缓存目录路径（默认: 使用系统临时目录）'
    )

    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='禁用缓存模式（直接处理挂载点文件）'
    )

    parser.add_argument(
        '--error-log-dir',
        type=str,
        default=str(_REPO_ROOT / 'scripts' / 'import_json_pubmed_error_logs'),
        help='错误日志目录路径（默认: 脚本所在目录下的 error_logs）'
    )

    parser.add_argument(
        '--error-buffer-size',
        type=int,
        default=100,
        help='错误缓冲大小（达到此数量后写入文件，默认: 100）'
    )

    parser.add_argument(
        '--skip-existing',
        action='store_true',
        default=True,
        help='跳过已处理的文件（默认: 启用）'
    )

    parser.add_argument(
        '--no-skip-existing',
        action='store_false',
        dest='skip_existing',
        help='不跳过已处理的文件（重新处理所有文件）'
    )

    args = parser.parse_args()
    
    # 如果指定了 --no-cache，将 cache_batch_size 设置为 0
    if args.no_cache:
        args.cache_batch_size = 0
    
    # 如果用户指定了不同的配置文件路径，重新加载配置
    if args.config_path:
        config_path = Path(args.config_path)
        if config_path.exists():
            print(f"📝 使用指定的配置文件: {config_path}")
            set_env_from_config(override=True, config_path=config_path)
        else:
            print(f"⚠️  警告: 指定的配置文件不存在: {config_path}")

    # 创建错误日志文件路径
    error_log_file = None
    if args.error_log_dir or True:  # 默认启用错误日志
        if args.error_log_dir:
            error_log_dir = Path(args.error_log_dir)
        else:
            # 默认使用脚本所在目录下的 error_logs
            error_log_dir = Path(__file__).parent / 'error_logs'
        
        # 确保目录存在
        error_log_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成带时间戳的日志文件名
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        error_log_file = error_log_dir / f'errors_{timestamp}.jsonl'
    
    # 显示配置信息
    print("="*60)
    print("🚀 开始导入数据（使用 docset_hub 架构）")
    print("="*60)
    print(f"   数据目录: {args.json_dir}")
    print(f"   限制数量: {args.limit if args.limit else '不限制'}")
    print(f"   数据库配置: 从 config.yaml 读取")
    if args.cache_batch_size > 0:
        print(f"   缓存模式: 启用（批次大小: {args.cache_batch_size}）")
        if args.cache_dir:
            print(f"   缓存目录: {args.cache_dir}")
        else:
            print(f"   缓存目录: 自动创建临时目录")
    else:
        print(f"   缓存模式: 禁用（直接处理挂载点文件）")
    if error_log_file:
        print(f"   错误日志: {error_log_file}")
        print(f"   错误缓冲大小: {args.error_buffer_size}")
    print(f"   跳过已存在: {'是' if args.skip_existing else '否'}")
    print()
    
    # 执行导入
    try:
        result = import_json_files(
            args.json_dir, 
            limit=args.limit, 
            mock=args.mock,
            cache_batch_size=args.cache_batch_size,
            cache_dir=args.cache_dir,
            error_log_file=error_log_file,
            error_buffer_size=args.error_buffer_size,
            skip_existing=args.skip_existing
        )
        
        # 根据结果设置退出码
        if result['fail'] == 0:
            print("\n✅ 所有文件导入成功！")
            sys.exit(0)
        else:
            print(f"\n⚠️  有 {result['fail']} 个文件导入失败")
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f"❌ 错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 发生未预期的错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
