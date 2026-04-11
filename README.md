# PD_TEST_Langtaosha

本目录是从 `PD_TEST` 整理的 **浪淘沙 / 文献服务** 侧工程副本：`local_data/` 仅保留空目录（运行配置请自行放入并设置 `PD_TEST_CONFIG`），测试用向量与 JSON 样例在 `tests/fixtures/`。

## 运行 Web（Flask）

```bash
cd PD_TEST_Langtaosha
pip install -r requirements.txt
pip install -e .
# 可选：使用本机配置
# export PD_TEST_CONFIG=$PWD/local_data/your.yaml
python app/main.py
```

---


**PD_TEST** is a lightweight backend framework for **scientific paper storage, indexing, and retrieval**, designed to support downstream tasks such as literature search, analysis, and scientific discovery workflows.

The project provides a modular structure for managing paper metadata, embeddings, and indexing pipelines, and can serve as a foundation for research-oriented systems or experimental platforms.

---

## Features

- 📄 **Paper Storage**  
  Structured storage of scientific papers and metadata.

- 🔍 **Indexing & Search**  
  Support for building searchable indices for efficient retrieval.

- 🧪 **Research-Oriented Design**  
  Designed to be easily extended for experiments in recommendation, novelty detection, and scientific discovery.

- 🧩 **Modular Architecture**  
  Clear separation between data, database, scripts, and core source code.

---

## Project Structure

```
PD_TEST_Langtaosha/
├── app/              # Flask 入口（main.py）
├── local_data/       # 空目录：本机配置放此处，并用 PD_TEST_CONFIG 指向
├── database/         # schema 与 migrations
├── docs/             # 文档（含 docs/langtaosha）
├── scripts/          # 导入与运维脚本
├── src/              # 可安装 Python 包（config、docset_hub）
├── templates/        # Web 模板
├── templates_qinlin/
├── tests/            # 测试；fixtures 在 tests/fixtures/
├── requirements.txt
└── setup.py
```

---

## Installation (Development)

It is recommended to install the project in editable mode for development.

```bash
pip install -r requirements.txt
pip install -e .
```

---

## Usage

After installation, you can import and extend the core modules in your own pipelines or scripts.

Typical use cases include:
- Building paper ingestion pipelines
- Creating searchable indices over large-scale literature collections
- Prototyping scientific discovery or recommendation systems

Refer to the `notebooks/` and `scripts/` directories for examples.

---

## License

This project is intended for research and experimental use.
Please add a license file if redistribution or open-source release is planned.
