#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
解压指定目录下所有 .xml.gz 文件为 .xml 文件
支持指定输出目录，如果未指定则输出到源文件所在目录
"""

import argparse
import gzip
import sys
from pathlib import Path
from tqdm import tqdm


def decompress_gz_file(gz_path: Path, xml_path: Path) -> None:
    """解压单个 .xml.gz 文件
    
    Args:
        gz_path: .xml.gz 文件路径
        xml_path: 输出 .xml 文件路径
        
    Raises:
        gzip.BadGzipFile: 当文件不是有效的 gzip 文件时
        IOError: 当发生 I/O 错误时
        OSError: 当发生系统错误时
    """
    tmp = xml_path.with_suffix(".xml.part")
    try:
        with gzip.open(gz_path, "rb") as src, open(tmp, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        tmp.replace(xml_path)
    except (gzip.BadGzipFile, IOError, OSError) as e:
        # 清理临时文件
        if tmp.exists():
            tmp.unlink()
        raise


def main() -> None:
    """主函数"""
    parser = argparse.ArgumentParser(
        description="解压指定目录下所有 .xml.gz 文件为 .xml 文件"
    )
    parser.add_argument(
        "--dir", "-d",
        type=Path,
        required=True,
        help="包含 .xml.gz 文件的目录路径"
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        required=False,
        default=None,
        help="输出目录路径（可选，未指定时输出到源文件所在目录）"
    )
    args = parser.parse_args()
    
    # 验证输入目录是否存在
    target_dir = args.dir
    if not target_dir.exists():
        print(f"错误: 目录不存在: {target_dir}", file=sys.stderr)
        return
    if not target_dir.is_dir():
        print(f"错误: 路径不是目录: {target_dir}", file=sys.stderr)
        return
    
    # 处理输出目录
    output_dir = args.output_dir
    if output_dir is not None:
        if output_dir.exists() and not output_dir.is_dir():
            print(f"错误: 输出路径不是目录: {output_dir}", file=sys.stderr)
            return
        # 创建输出目录（如果不存在）
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # 查找所有 .xml.gz 文件
    gz_files = sorted(target_dir.glob("*.xml.gz"))
    
    if not gz_files:
        print(f"在目录 {target_dir} 中未找到 .xml.gz 文件")
        return
    
    # 统计信息
    success_count = 0
    fail_count = 0
    
    # 使用 tqdm 显示进度
    for gz_path in tqdm(gz_files, desc="解压文件", unit="文件"):
        # 构建输出路径
        if output_dir is not None:
            xml_path = output_dir / gz_path.with_suffix("").name
        else:
            xml_path = gz_path.with_suffix("")  # 移除 .gz 后缀
        try:
            decompress_gz_file(gz_path, xml_path)
            success_count += 1
        except Exception as e:
            fail_count += 1
            print(f"\n错误: 解压失败 {gz_path.name}: {e}", file=sys.stderr)
    
    # 输出统计信息
    total = len(gz_files)
    print(f"\n完成: 总计 {total} 个文件, 成功 {success_count} 个, 失败 {fail_count} 个")


if __name__ == "__main__":
    main()
