# 配置管理说明

本项目支持两种配置方式：`config.yaml` 和 `.env` 文件。

## 配置优先级

配置加载的优先级（从高到低）：
1. **已存在的环境变量**（如果 `override=False`）
2. **config.yaml 中的配置**
3. **.env 文件中的配置**

## 使用 config.yaml（推荐）

### 1. 创建配置文件

复制示例文件并填入实际值：

```bash
cp config.yaml.example config.yaml
```

### 2. 编辑 config.yaml

```yaml
# 数据库配置（注意：键名必须是 metadata_db，不是 db）
metadata_db:
  host: 10.0.4.7
  port: 5432
  user: your_username
  password: your_password
  name: your_database

storage:
  json: /path/to/json
  pdf: /path/to/pdf
  html: /path/to/html
  images: /path/to/images

# 向量数据库配置（注意：键名必须是 vector_db，不是 vector）
vector_db:
  db: /path/to/vector_db
  gritlm_model: null  # 或具体路径
  gritlm_model_name: GritLM/GritLM-7B
  hf_home: null
```

### 3. 键名映射

配置加载器会自动将嵌套的 YAML 键名映射为环境变量：

| YAML 路径 | 环境变量名 |
|-----------|-----------|
| `metadata_db.host` | `DB_HOST` |
| `metadata_db.port` | `DB_PORT` |
| `metadata_db.user` | `DB_USER` |
| `metadata_db.password` | `DB_PASSWORD` |
| `metadata_db.name` | `DB_NAME` |
| `storage.json` | `JSON_STORAGE_PATH` |
| `storage.pdf` | `PDF_STORAGE_PATH` |
| `storage.html` | `HTML_STORAGE_PATH` |
| `storage.images` | `IMAGE_STORAGE_PATH` |
| `vector_db.db` | `VECTOR_DB_PATH` |
| `vector_db.gritlm_model` | `GRITLM_MODEL_PATH` |
| `vector_db.gritlm_model_name` | `GRITLM_MODEL_NAME` |
| `vector_db.hf_home` | `HF_HOME` |

## 使用 .env 文件（传统方式）

如果更喜欢使用 `.env` 文件，可以继续使用：

```env
DB_HOST=10.0.4.7
DB_PORT=5432
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database

JSON_STORAGE_PATH=/path/to/json
PDF_STORAGE_PATH=/path/to/pdf
# ...
```

## 配置加载机制

配置在 `config/db_config.py` 导入时自动加载：

```python
from .config_loader import set_env_from_config
set_env_from_config(override=True)
```

之后所有代码都通过 `os.getenv()` 读取环境变量，无需修改现有代码。

## 安全注意事项

- `config.yaml` 和 `.env` 文件都包含敏感信息（如数据库密码）
- 这两个文件都已添加到 `.gitignore`，不会被提交到版本控制
- 请妥善保管配置文件，不要泄露给他人



