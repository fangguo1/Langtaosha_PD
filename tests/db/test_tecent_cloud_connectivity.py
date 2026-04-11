"""腾讯云后端服务连通性测试

测试三项腾讯云资源的可访问性：
- test_1_gpu_ssh: GPU Server SSH 连通性
- test_2_vector_db_http: 向量数据库 HTTP API 连通性
- test_3_postgresql: PostgreSQL 数据库连通性

运行前请确保：
1. src/config/config_tecent_backend_server.yaml 中已填入真实凭据
2. 本机可访问腾讯云公网 IP（GPU SSH）
3. 本机处于腾讯云内网或已通过 CVM/GPU 做跳板（VectorDB、PostgreSQL）
"""

import unittest
import yaml
import json
from pathlib import Path
from urllib.parse import quote_plus

# 加载配置文件
CONFIG_PATH = Path(__file__).resolve().parents[2] / 'src' / 'config' / 'config_tecent_backend_server.yaml'

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"配置文件不存在: {CONFIG_PATH}")

with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    CONFIG = yaml.safe_load(f)


def _is_placeholder(value: str) -> bool:
    """检查配置值是否仍为占位符"""
    if not value:
        return True
    return "请查看" in value or value.strip() == ""


class TestGCPServerConnectivity(unittest.TestCase):
    """GPU Server SSH 连通性测试"""

    @classmethod
    def setUpClass(cls):
        cls.gpu_config = CONFIG.get('gpu', {})
        cls.host = cls.gpu_config.get('public_host', '')
        cls.port = cls.gpu_config.get('ssh_port', 52622)
        cls.user = cls.gpu_config.get('user', '')
        cls.password = cls.gpu_config.get('password', '')

    def setUp(self):
        if _is_placeholder(self.password):
            self.skipTest("GPU SSH 密码未配置，请在 config_tecent_backend_server.yaml 中填入真实凭据")

    def test_1_gpu_ssh(self):
        """测试 GPU Server SSH 连通性"""
        import paramiko

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                timeout=15,
                banner_timeout=15,
                auth_timeout=15,
            )
            stdin, stdout, stderr = client.exec_command("hostname && uname -a", timeout=10)
            output = stdout.read().decode().strip()
            self.assertTrue(output, "hostname 命令应返回非空输出")
            print(f"\n  GPU SSH 连接成功: {self.user}@{self.host}:{self.port}")
            print(f"  主机信息: {output.splitlines()[0]}")
        finally:
            client.close()


class TestVectorDBConnectivity(unittest.TestCase):
    """向量数据库 HTTP API 连通性测试"""

    @classmethod
    def setUpClass(cls):
        cls.vdb_config = CONFIG.get('vector_db_tecent', {})
        cls.url = cls.vdb_config.get('url', '')
        cls.account = cls.vdb_config.get('account', 'root')
        cls.api_key = cls.vdb_config.get('api_key', '')

    def setUp(self):
        if _is_placeholder(self.api_key):
            self.skipTest("VectorDB API Key 未配置，请在 config_tecent_backend_server.yaml 中填入真实凭据")

    def _auth_header(self):
        return {"Authorization": f"Bearer account={self.account}&api_key={self.api_key}"}

    def test_2_vector_db_http(self):
        """测试 VectorDB HTTP API 连通性（列出数据库）"""
        import urllib.request
        import urllib.error

        endpoint = f"{self.url}/database/list"
        req = urllib.request.Request(endpoint, method="GET", headers={
            "Content-Type": "application/json",
            **self._auth_header(),
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read().decode()
                data = json.loads(body)
                self.assertIsNotNone(data, "API 应返回非空响应")
                print(f"\n  VectorDB 连接成功: {self.url}")
                print(f"  响应: {json.dumps(data, ensure_ascii=False)[:300]}")
        except urllib.error.HTTPError as e:
            # 即使返回非 200，只要能连上也说明服务可达
            self.fail(f"VectorDB 返回 HTTP {e.code}: {e.reason}")
        except urllib.error.URLError as e:
            self.fail(f"VectorDB 不可达: {e.reason}")


class TestPostgreSQLConnectivity(unittest.TestCase):
    """PostgreSQL 数据库连通性测试"""

    @classmethod
    def setUpClass(cls):
        cls.db_config = CONFIG.get('metadata_db', {})
        cls.host = cls.db_config.get('host', '')
        cls.port = cls.db_config.get('port', 5432)
        cls.user = cls.db_config.get('user', '')
        cls.password = cls.db_config.get('password', '')
        cls.database = cls.db_config.get('name', 'postgres')

    def setUp(self):
        if _is_placeholder(self.password):
            self.skipTest("PostgreSQL 密码未配置，请在 config_tecent_backend_server.yaml 中填入真实凭据")

    def test_3_postgresql(self):
        """测试 PostgreSQL 连通性（列出数据库）"""
        import psycopg2

        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=10,
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;"
            )
            databases = [row[0] for row in cur.fetchall()]
            self.assertGreater(len(databases), 0, "应至少有一个非模板数据库")
            print(f"\n  PostgreSQL 连接成功: {self.user}@{self.host}:{self.port}/{self.database}")
            print(f"  数据库列表: {databases}")
            cur.close()
            conn.close()
        except psycopg2.OperationalError as e:
            self.fail(f"PostgreSQL 连接失败: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
