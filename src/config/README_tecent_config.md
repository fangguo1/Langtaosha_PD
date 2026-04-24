# 腾讯云配置使用指南

本文档展示如何使用更新后的 `config_loader.py` 来访问腾讯云配置。

## 配置文件约定

- 仓库提交的示例配置：`src/config/config_tecent_backend_server_example.yaml`
- 本地私有配置：自行复制示例配置并填写真实凭据
- 私有配置不要提交到 Git

推荐流程：

```bash
cp src/config/config_tecent_backend_server_example.yaml \
   src/config/config_tecent_backend_server_use.yaml
```

然后只在本机修改 `config_tecent_backend_server_use.yaml`、`config_tecent_backend_server_test.yaml` 或 `config_tecent_backend_server_mimic.yaml` 这类私有文件。

## 快速开始

### 1. 基本使用

```python
from pathlib import Path
from config.config_loader import (
    init_config,
    get_cvm_server_config,
    get_gpu_server_config
)

# 初始化配置
config_path = Path("src/config/config_tecent_backend_server_example.yaml")
init_config(config_path)

# 获取 CVM 服务器配置
cvm_config = get_cvm_server_config()
print(f"CVM 服务器: {cvm_config['public_host']}")

# 获取 GPU 服务器配置
gpu_config = get_gpu_server_config()
print(f"GPU 服务器: {gpu_config['public_host']}")
```

### 2. 服务器连接示例

```python
import paramiko
from config.config_loader import get_cvm_server_config

def connect_to_cvm_server():
    """连接到 CVM 服务器"""
    config = get_cvm_server_config()

    # 创建 SSH 客户端
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # 连接服务器
    ssh.connect(
        hostname=config['public_host'],
        port=config['ssh_port'],
        username=config['user'],
        password=config['password']
    )

    return ssh

# 使用示例
ssh = connect_to_cvm_server()
stdin, stdout, stderr = ssh.exec_command('ls -la')
print(stdout.read().decode())
ssh.close()
```

### 3. 向量数据库配置

```python
from config.config_loader import (
    get_vector_db_config,
    is_remote_vector_db
)

# 获取向量数据库配置
vector_db_config = get_vector_db_config()

if is_remote_vector_db():
    # 远程向量数据库模式
    print(f"远程向量数据库 URL: {vector_db_config['url']}")
    print(f"账户: {vector_db_config['account']}")
    print(f"API 密钥: {vector_db_config['api_key']}")
else:
    # 本地向量数据库模式
    print(f"本地向量数据库路径: {vector_db_config['db']}")
```

### 4. 数据库连接

```python
from config.config_loader import get_db_engine
from sqlalchemy import text

# 获取数据库引擎
engine = get_db_engine()

# 执行查询
with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    version = result.scalar()
    print(f"PostgreSQL 版本: {version}")
```

### 5. 环境变量访问

```python
import os
from config.config_loader import init_config
from pathlib import Path

# 初始化配置（自动设置环境变量）
config_path = Path("src/config/config_tecent_backend_server_example.yaml")
init_config(config_path)

# 直接访问环境变量
cvm_host = os.getenv('CVM_SERVER_PUBLIC_HOST')
gpu_host = os.getenv('GPU_SERVER_PUBLIC_HOST')
db_host = os.getenv('DB_HOST')

print(f"CVM 主机: {cvm_host}")
print(f"GPU 主机: {gpu_host}")
print(f"数据库主机: {db_host}")
```

## 配置文件结构

腾讯云配置文件包含以下主要部分：

```yaml
# CVM 通用计算节点
cvm_server:
  public_host: 43.143.246.163      # 公网地址
  private_host: 172.21.0.16         # 私网地址
  ssh_port: 52622                    # SSH 端口
  user: wnlab                        # 用户名
  password: ***                      # 密码

# GPU 计算节点
gpu_server:
  public_host: 49.232.190.174       # 公网地址
  private_host: 172.21.16.16         # 私网地址
  ssh_port: 52622                    # SSH 端口
  user: wnlab                        # 用户名
  password: ***                      # 密码

# 向量数据库服务
vector_db:
  url: "http://172.21.0.3:80"       # 服务 URL
  account: root                      # 账户
  api_key: ***                       # API 密钥

# PostgreSQL 数据库
metadata_db:
  host: 172.21.0.9                   # 数据库主机
  port: 5432                         # 端口
  user: root                         # 用户名
  password: ***                      # 密码
  name: langtaosha_test              # 数据库名称
```

## 新增函数说明

### `get_cvm_server_config()`

获取 CVM 服务器配置。

**返回:**
- `public_host`: 公网主机地址
- `private_host`: 私网主机地址
- `ssh_port`: SSH 端口
- `user`: 用户名
- `password`: 密码

### `get_gpu_server_config()`

获取 GPU 服务器配置。

**返回:**
- `public_host`: 公网主机地址
- `private_host`: 私网主机地址
- `ssh_port`: SSH 端口
- `user`: 用户名
- `password`: 密码

### `is_remote_vector_db()`

检查是否使用远程向量数据库。

**返回:**
- `True`: 远程模式（配置包含 `url` 字段）
- `False`: 本地模式（配置包含 `db` 字段）

## 环境变量映射

配置会自动映射到以下环境变量：

| 配置路径 | 环境变量 |
|---------|----------|
| `cvm_server.public_host` | `CVM_SERVER_PUBLIC_HOST` |
| `cvm_server.private_host` | `CVM_SERVER_PRIVATE_HOST` |
| `cvm_server.ssh_port` | `CVM_SERVER_SSH_PORT` |
| `cvm_server.user` | `CVM_SERVER_USER` |
| `cvm_server.password` | `CVM_SERVER_PASSWORD` |
| `gpu_server.public_host` | `GPU_SERVER_PUBLIC_HOST` |
| `gpu_server.private_host` | `GPU_SERVER_PRIVATE_HOST` |
| `gpu_server.ssh_port` | `GPU_SERVER_SSH_PORT` |
| `gpu_server.user` | `GPU_SERVER_USER` |
| `gpu_server.password` | `GPU_SERVER_PASSWORD` |
| `vector_db.url` | `VECTOR_DB_URL` |
| `vector_db.account` | `VECTOR_DB_ACCOUNT` |
| `vector_db.api_key` | `VECTOR_DB_API_KEY` |
| `metadata_db.host` | `DB_HOST` |
| `metadata_db.port` | `DB_PORT` |
| `metadata_db.user` | `DB_USER` |
| `metadata_db.password` | `DB_PASSWORD` |
| `metadata_db.name` | `DB_NAME` |

## 测试

运行测试脚本验证配置：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/src/config
python test_config_loader_tecent.py
```

测试脚本会验证：
- ✅ 配置文件加载
- ✅ CVM 服务器配置
- ✅ GPU 服务器配置
- ✅ 向量数据库配置
- ✅ 元数据库配置
- ✅ 环境变量设置
- ✅ 数据库引擎创建

## 注意事项

1. **安全性**: 真实配置文件包含敏感信息（密码、API 密钥），请勿提交到版本控制系统
2. **网络**: 确保网络可以访问腾讯云内网地址
3. **兼容性**: 更新后的 `config_loader.py` 向后兼容原有的配置格式
4. **初始化**: 使用配置前必须先调用 `init_config(config_path)`

## 更新日志

### 2024-04-14
- ✅ 添加 CVM 服务器配置支持
- ✅ 添加 GPU 服务器配置支持
- ✅ 更新向量数据库配置（支持远程服务）
- ✅ 扩展环境变量映射
- ✅ 添加 `is_remote_vector_db()` 函数
- ✅ 更新文档和测试
