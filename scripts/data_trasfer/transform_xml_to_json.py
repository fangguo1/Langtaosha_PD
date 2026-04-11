import argparse
import json
from pathlib import Path
import sys
from collections import defaultdict

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tqdm.contrib.concurrent import process_map
from mapping import build_record, iter_pubmed_articles, make_work_id


def collect_work_ids(xml_path: Path, limit: int):
    pmid_to_work_id = {}
    count = 0
    for article_elem in iter_pubmed_articles(xml_path):
        medline = article_elem.find("MedlineCitation")
        pmid = medline.findtext("PMID") if medline is not None else None
        if pmid and pmid not in pmid_to_work_id:
            pmid_to_work_id[pmid] = make_work_id()
        count += 1
        article_elem.clear()
        if limit > 0 and count >= limit:
            break
    return pmid_to_work_id


def extract_to_dir(xml_path: Path, batch_dir: Path, limit: int):
    batch_dir.mkdir(parents=True, exist_ok=True)
    pmid_to_work_id = collect_work_ids(xml_path, limit)

    processed = 0
    for article_elem in iter_pubmed_articles(xml_path):
        medline = article_elem.find("MedlineCitation")
        pmid = medline.findtext("PMID") if medline is not None else None
        if pmid not in pmid_to_work_id:
            article_elem.clear()
            continue
        record, _pubmed_data, work_id, _pmid = build_record(article_elem, pmid_to_work_id)
        out_path = batch_dir / f"{work_id}.json"
        out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        processed += 1
        article_elem.clear()
        if limit > 0 and processed >= limit:
            break
    return processed


def process_single_file(args_tuple):
    """处理单个 XML 文件的包装函数，用于多进程"""
    xml_file, batch_dir, limit, batch_id, xml_file_path = args_tuple
    processed = extract_to_dir(xml_file, batch_dir, limit)
    return (processed, batch_id, str(xml_file_path))



#将单个/多个xml文件转换为json文件


#python3 scripts/transform_xml_to_json.py --xml-dir /data3/guofang/remote_pubmed_10.0.4.7/ 
#--out-dir /data3/guofang/remote_storage_home_10.0.4.7/pubmed_json/ --limit 0

#python3 scripts/transform_xml_to_json.py \
#  --xml /data3/guofang/remote_pubmed_10.0.4.7/pubmed25n0001.xml \
#  --out-dir /data3/guofang/remote_storage_home_10.0.4.7/pubmed_json/ \
#  --limit 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--xml", type=Path, required=False, help="Single XML file path")
    parser.add_argument("--xml-dir", type=Path, required=False, help="Directory containing XML files")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=1000, help="Limit articles per file (0 = all)")
    parser.add_argument("--workers", type=int, default=100, help="Number of worker processes")
    args = parser.parse_args()

    # 确定要处理的 XML 文件列表
    xml_files = []
    if args.xml_dir:
        xml_files = sorted(args.xml_dir.glob("*.xml"))
    elif args.xml:
        xml_files = [args.xml]
    else:
        parser.error("Either --xml or --xml-dir must be provided")

    # 批次大小
    batch_size = 100

    # 准备参数元组列表，为每个XML文件分配批次号
    file_args = []
    for idx, xml_file in enumerate(xml_files):
        batch_id = idx // batch_size
        batch_dir = args.out_dir / str(batch_id)
        file_args.append((xml_file, batch_dir, args.limit, batch_id, xml_file))

    # 使用 process_map 并行处理
    results = process_map(
        process_single_file,
        file_args,
        max_workers=args.workers,
        chunksize=1,
        desc="Processing XML files"
    )

    # 收集批次完成情况
    batch_xml_files = defaultdict(list)
    total_processed = 0
    for processed, batch_id, xml_file_path in results:
        total_processed += processed
        batch_xml_files[batch_id].append(xml_file_path)

    # 为每个批次创建 xml_info.txt
    for batch_id in sorted(batch_xml_files.keys()):
        batch_dir = args.out_dir / str(batch_id)
        xml_info_path = batch_dir / "xml_info.txt"
        with open(xml_info_path, 'w', encoding='utf-8') as f:
            for xml_file_path in sorted(batch_xml_files[batch_id]):
                f.write(f"{Path(xml_file_path).name}\n")

    print(f"done. total_files={len(xml_files)}, total_articles={total_processed}, batches={len(batch_xml_files)}")


if __name__ == "__main__":
    main()
