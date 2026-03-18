# Shared DB Package

统一的数据库读写抽象，集中封装 MySQL / PostgreSQL 连接、批量写入、CSV 读取和字段清洗能力。

## 目录结构
- `common.py`: 通用工具（字段名校验、CSV 分隔符/编码探测、数据清洗）
- `mysql.py`: MySQL 配置与读写能力
- `postgres.py`: PostgreSQL 配置与读写能力
- `dataframe_io.py`: 表格文件读取与 DataFrame 归一化

## 快速使用
```python
from subprojects._shared.db import MySQLDatabase, PostgresDatabase

mysql_db = MySQLDatabase()
rows = mysql_db.execute_query("SELECT 1")

pg_db = PostgresDatabase()
pg_db.connect()
pg_rows = pg_db.execute_query("SELECT 1")
pg_db.close()
```

## 兼容性
- 旧接口 `subprojects._shared.core.db_client` 与 `subprojects._shared.core.pg_db` 仍可继续使用。
- 旧脚本无需立即修改 import；可分批迁移到 `subprojects._shared.db` 新包。
