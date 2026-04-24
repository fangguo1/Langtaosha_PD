"""测试数据 fixtures"""
from typing import Dict, Any, List
from pathlib import Path
import json


# =============================================================================
# 从test_data目录读取真实数据
# =============================================================================

def load_biorxiv_test_data(limit: int = 14) -> List[Dict[str, Any]]:
    """从test_data/bio_arxiv目录加载bioarxiv测试数据

    Args:
        limit: 最多加载的文件数量（默认14个）

    Returns:
        List[Dict]: bioarxiv原始数据列表
    """
    test_data_dir = Path(__file__).parent.parent.parent / "test_data" / "bio_arxiv"
    files = sorted(test_data_dir.glob("*.json"))[:limit]

    data_list = []
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            data_list.append(data)

    return data_list


def load_langtaosha_test_data(limit: int = 5) -> List[Dict[str, Any]]:
    """从test_data/langtaosha目录加载langtaosha测试数据

    Args:
        limit: 最多加载的文件数量（默认5个）

    Returns:
        List[Dict]: langtaosha原始数据列表
    """
    test_data_dir = Path(__file__).parent.parent.parent / "test_data" / "langtaosha"
    files = sorted(test_data_dir.glob("*.json"))[:limit]

    data_list = []
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            data_list.append(data)

    return data_list


def load_all_test_data(
    biorxiv_limit: int = 14,
    langtaosha_limit: int = 5
) -> Dict[str, List[Dict[str, Any]]]:
    """加载所有测试数据

    Args:
        biorxiv_limit: bioarxiv文件数量限制（默认14个）
        langtaosha_limit: langtaosha文件数量限制（默认5个）

    Returns:
        Dict: {
            "bioarxiv": [...],
            "langtaosha": [...]
        }
    """
    return {
        "bioarxiv": load_biorxiv_test_data(biorxiv_limit),
        "langtaosha": load_langtaosha_test_data(langtaosha_limit)
    }


# =============================================================================
# 简化的单样本数据（用于快速测试）
# =============================================================================

# Langtaosha 测试数据（单样本）
LANGTAOSHA_SAMPLE: Dict[str, Any] = {
    "citation_title": "A Study on Large Language Model Reasoning",
    "citation_abstract": "This paper proposes a novel approach to improve reasoning capabilities of large language models.",
    "citation_language": "en",
    "citation_publisher": "Langtaosha",
    "citation_date": "2026-04-01",
    "citation_online_date": "2026-04-05",
    "citation_publication_date": "2026-04-10",
    "citation_doi": "https://doi.org/10.1234/LANGTAOSHA.001",
    "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/181",
    "citation_pdf_url": "https://langtaosha.org.cn/lts/en/preprint/download/181",
    "citation_author": ["Alice Zhang", "Bob Li", "Charlie Wang"],
    "citation_author_institution": [
        "Tsinghua University",
        "Peking University",
        "Chinese Academy of Sciences",
    ],
    "citation_keywords": ["LLM", "reasoning", "artificial intelligence"],
    "citation_reference": [
        "Ref A: Previous work on LLM reasoning",
        "Ref B: Recent advances in AI",
    ],
}

# Langtaosha JSONL 格式测试数据（单样本）
LANGTAOSHA_JSONL_SAMPLE: Dict[str, Any] = {
    "url": "https://langtaosha.org.cn/lts/en/preprint/view/181",
    "sitemap_lastmod": "2026-04-13",
    "meta": {
        "citation_title": ["A Study on Large Language Model Reasoning"],
        "citation_abstract": ["This paper proposes a novel approach to improve reasoning capabilities of large language models."],
        "citation_language": ["en"],
        "citation_publisher": ["LangTaoSha Preprint Server"],
        "citation_date": ["2026/04/01"],
        "citation_online_date": ["2026/04/05"],
        "citation_publication_date": ["2026/04/10"],
        "citation_doi": ["10.1234/LANGTAOSHA.001"],
        "citation_abstract_html_url": ["https://langtaosha.org.cn/lts/en/preprint/view/181"],
        "citation_pdf_url": ["https://langtaosha.org.cn/lts/en/preprint/download/181"],
        "citation_author": ["Alice Zhang", "Bob Li", "Charlie Wang"],
        "citation_author_institution": [
            "Tsinghua University",
            "Peking University",
            "Chinese Academy of Sciences",
        ],
        "citation_keywords": ["LLM", "reasoning", "artificial intelligence"],
        "citation_reference": [
            "Ref A: Previous work on LLM reasoning",
            "Ref B: Recent advances in AI",
        ],
    },
    "fetched_at": "2026-04-14T04:00:00Z"
}


# bioRxiv 测试数据（单样本）
BIORXIV_SAMPLE: Dict[str, Any] = {
    "title": "Neural computations in the foveal and peripheral visual fields during active search",
    "authors": "Zhang, J.; Zhu, X.; Ma, Z.; Wang, S.; Wang, Y.; Esteky, H.; Tian, Y.; Desimone, R.; Wang, S.; Zhou, H.",
    "author_corresponding": "Huihui Zhou",
    "author_corresponding_institution": "Peng Cheng Laboratory, Shenzhen 518000, China. & Shenzhen Institute of Advanced Technology, Chinese Academy of Sciences, Shenzhen 518055, China.",
    "doi": "10.1101/2021.11.22.469359",
    "date": "2026-04-08",
    "version": "5",
    "type": "new results",
    "license": "cc_by",
    "category": "neuroscience",
    "jatsxml": "https://www.biorxiv.org/content/early/2026/04/08/2021.11.22.469359.source.xml",
    "abstract": "Active vision requires coordinated attentional processing ...",
    "funder": [
        {
            "name": "The National Natural Science Foundation of China",
            "id": "",
            "id-type": "ROR",
            "award": "62027804;62206141;31671108;",
        }
    ],
    "published": "NA",
    "server": "bioRxiv",
}


# 用于测试路由的数据
ROUTING_SAMPLES = {
    "langtaosha_by_domain": {
        "citation_title": "Test Paper",
        "citation_abstract_html_url": "https://langtaosha.org.cn/lts/en/preprint/view/123",
    },
    "langtaosha_by_fields": {
        "citation_title": "Test Paper",
        "citation_author": "Test Author",
    },
    "biorxiv_by_doi": {
        "title": "Test Paper",
        "doi": "10.1101/2021.11.22.469359",
    },
    "biorxiv_by_domain": {
        "title": "Test Paper",
        "jatsxml": "https://www.biorxiv.org/content/early/2026/04/08/2021.11.22.469359.source.xml",
    },
    "unknown_source": {
        "title": "Test Paper",
        "unknown_field": "unknown_value",
    },
}


# 用于测试归一化的数据
NORMALIZATION_SAMPLES = {
    "doi": {
        "input": "HTTPS://doi.org/10.1145/XXX",
        "expected": "10.1145/xxx",
    },
    "arxiv": {
        "input": "2301.12345v1",
        "expected": "2301.12345",
    },
    "pubmed": {
        "input": " 12345678 ",
        "expected": "12345678",
    },
    "date": {
        "inputs": [
            "2026-04-01",
            "2026/04/01",
            "2026-04-01T00:00:00",
        ],
        "expected": "2026-04-01",
    },
}


# 不完整的测试数据（用于测试错误处理]
INCOMPLETE_SAMPLES = {
    "missing_title": {
        "citation_abstract": "This is an abstract",
        "citation_author": "Test Author",
    },
    "empty_metadata": {},
    "missing_required_field": {
        "citation_author": "Test Author",
    },
}

