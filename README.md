# 数据库迁移数据一致性校验工具

数据库迁移后对比源表和转换后表数据一致性的 Python 工具。

## 功能特性

- **灵活的字段映射**: 支持直接映射、字段转换、条件映射等多种方式
- **多种比较规则**: 精确比较、忽略大小写、忽略空白、数值容差、跳过比较
- **多数据库支持**: MySQL、PostgreSQL、SQLite（可扩展 Oracle、SQL Server）
- **丰富的转换函数**: 字符串拼接、大小写转换、日期格式化、JSON 提取等
- **并行校验**: 支持多表并行校验，提高效率
- **详细报告**: JSON、Markdown、HTML 多种格式报告

## 安装

```bash
# 克隆或下载项目
cd db-migration-validator

# 安装依赖（根据需要选择）
pip install mysql-connector-python  # MySQL
pip install psycopg2-binary        # PostgreSQL
pip install cx_Oracle              # Oracle
pip install pyodbc                 # SQL Server
```

## 快速开始

```bash
# 运行校验
python validator.py mapping_example.json

# 并行校验
python validator.py mapping_example.json --parallel
```

---

## 配置文件说明

配置文件使用 JSON 格式，定义源数据库、目标数据库和表字段映射关系。

### 配置文件结构

```json
{
  "version": "1.0",
  "source_db": { ... },
  "target_db": { ... },
  "tables": [ ... ],
  "global_settings": { ... }
}
```

### 1. 数据库连接配置

#### MySQL

```json
{
  "type": "mysql",
  "host": "192.168.1.100",
  "port": 3306,
  "database": "mydb",
  "user": "readonly_user",
  "password": "password123"
}
```

**安全方式（推荐）**：使用环境变量存储密码

```json
{
  "type": "mysql",
  "host": "192.168.1.100",
  "port": 3306,
  "database": "mydb",
  "user": "readonly_user",
  "password_env": "SOURCE_DB_PASSWORD"
}
```

然后设置环境变量：
```bash
export SOURCE_DB_PASSWORD="your_password"
```

#### PostgreSQL

```json
{
  "type": "postgresql",
  "host": "192.168.1.200",
  "port": 5432,
  "database": "newdb",
  "user": "validator",
  "password_env": "TARGET_DB_PASSWORD"
}
```

#### SQLite

```json
{
  "type": "sqlite",
  "database": "/path/to/database.db"
}
```

### 2. 表映射配置

每个表的映射配置包含以下字段：

| 字段 | 必填 | 说明 |
|------|------|------|
| `source_table` | 是 | 源表名 |
| `target_table` | 是 | 目标表名 |
| `source_schema` | 否 | 源表 schema（PostgreSQL） |
| `target_schema` | 否 | 目标表 schema |
| `description` | 否 | 映射说明 |
| `field_mappings` | 是 | 字段映射列表（见下文） |
| `filters` | 否 | 过滤条件 |
| `sample_size` | 否 | 抽样数量，0 表示全量 |
| `batch_size` | 否 | 批量大小，默认 1000 |

示例：

```json
{
  "source_table": "users",
  "target_table": "user_profile",
  "target_schema": "public",
  "description": "用户表迁移",
  "field_mappings": [ ... ],
  "filters": {
    "source_filter": "deleted_at IS NULL",
    "target_filter": "is_deleted = false"
  },
  "sample_size": 10000
}
```

### 3. 字段映射配置

字段映射定义目标表字段如何从源表获取数据。

#### 3.1 直接映射

源表字段直接映射到目标表字段：

```json
{
  "target_field": "user_id",
  "source_field": "id",
  "is_primary_key": true,
  "description": "用户ID"
}
```

| 字段 | 必填 | 说明 |
|------|------|------|
| `target_field` | 是 | 目标字段名 |
| `source_field` | 二选一 | 源字段名（与 transform 二选一） |
| `is_primary_key` | 否 | 是否为主键，默认 false |
| `nullable` | 否 | 是否允许空值，默认 true |
| `compare_rule` | 否 | 比较规则，默认 "exact" |
| `tolerance` | 否 | 数值容差（compare_rule=numeric_tolerance 时） |
| `description` | 否 | 字段说明 |

#### 3.2 转换映射

使用 `transform` 对源数据进行转换：

```json
{
  "target_field": "full_name",
  "transform": {
    "type": "concat",
    "fields": ["first_name", "last_name"],
    "separator": " "
  }
}
```

### 4. 转换类型

#### 4.1 concat - 字符串拼接

将多个字段拼接成一个字符串：

```json
{
  "type": "concat",
  "fields": ["first_name", "last_name"],
  "separator": " "
}
```

#### 4.2 upper / lower - 大小写转换

```json
{
  "type": "upper",
  "fields": ["email"]
}
```

#### 4.3 trim - 去除空白

```json
{
  "type": "trim",
  "fields": ["username"]
}
```

#### 4.4 substring - 子字符串

```json
{
  "type": "substring",
  "fields": ["phone"],
  "start": 1,
  "length": 3
}
```

#### 4.5 replace - 字符串替换

```json
{
  "type": "replace",
  "fields": ["code"],
  "pattern": "-",
  "replacement": ""
}
```

#### 4.6 constant - 常量值

```json
{
  "type": "constant",
  "value": "active"
}
```

#### 4.7 coalesce - 空值处理

返回第一个非空值：

```json
{
  "type": "coalesce",
  "fields": ["nickname", "username"],
  "default": "Anonymous"
}
```

#### 4.8 case - 条件映射

类似 CASE WHEN 语句：

```json
{
  "type": "case",
  "fields": ["status"],
  "cases": [
    { "when": "active", "then": "1" },
    { "when": "inactive", "then": "0" },
    { "when": "pending", "then": "2" }
  ],
  "else": "0"
}
```

#### 4.9 date_format - 日期格式化

```json
{
  "type": "date_format",
  "fields": ["create_time"],
  "format": "%Y-%m-%d %H:%M:%S"
}
```

#### 4.10 date_add - 日期计算

```json
{
  "type": "date_add",
  "fields": ["expire_date"],
  "interval": "-7 days"
}
```

#### 4.11 json_extract - JSON 提取

从 JSON 字段中提取值：

```json
{
  "type": "json_extract",
  "fields": ["metadata"],
  "pattern": "$.user.tags"
}
```

#### 4.12 cast - 类型转换

```json
{
  "type": "cast",
  "fields": ["price"],
  "cast_type": "float"
}
```

支持类型：`string`、`int`、`float`、`bool`、`date`、`datetime`

#### 4.13 math - 数学运算

```json
{
  "type": "math",
  "fields": ["price", "tax", "shipping"],
  "expression": "price + tax + shipping"
}
```

#### 4.14 greatest / least - 最大/最小值

```json
{
  "type": "greatest",
  "fields": ["price_a", "price_b", "price_c"]
}
```

### 5. 比较规则 (compare_rule)

| 规则 | 说明 |
|------|------|
| `exact` | 精确比较（默认） |
| `ignore_case` | 忽略大小写 |
| `ignore_whitespace` | 忽略空白字符 |
| `numeric_tolerance` | 数值容差比较，需配合 `tolerance` 字段 |
| `skip` | 跳过该字段的比较 |

**数值容差示例**：

```json
{
  "target_field": "balance",
  "source_field": "account_balance",
  "compare_rule": "numeric_tolerance",
  "tolerance": 0.01
}
```

### 6. 过滤条件 (filters)

可以分别对源表和目标表设置过滤条件：

```json
{
  "filters": {
    "source_filter": "deleted_at IS NULL AND status = 'active'",
    "target_filter": "is_deleted = false AND status_code = 1"
  }
}
```

### 7. 全局设置 (global_settings)

```json
{
  "global_settings": {
    "parallel_workers": 4,
    "timeout_seconds": 3600,
    "output_dir": "./reports",
    "report_format": ["json", "html", "markdown"]
  }
}
```

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `parallel_workers` | 4 | 并行线程数 |
| `timeout_seconds` | 3600 | 超时时间（秒） |
| `output_dir` | ./reports | 报告输出目录 |
| `report_format` | ["json", "html"] | 报告格式 |

---

## 完整配置示例

```json
{
  "version": "1.0",
  "source_db": {
    "type": "mysql",
    "host": "192.168.1.100",
    "port": 3306,
    "database": "legacy_db",
    "user": "readonly_user",
    "password_env": "SOURCE_DB_PASSWORD"
  },
  "target_db": {
    "type": "postgresql",
    "host": "192.168.1.200",
    "port": 5432,
    "database": "new_db",
    "user": "validator_user",
    "password_env": "TARGET_DB_PASSWORD"
  },
  "tables": [
    {
      "source_table": "users",
      "target_table": "user_profile",
      "target_schema": "public",
      "description": "用户表迁移映射",
      "field_mappings": [
        {
          "target_field": "user_id",
          "source_field": "id",
          "is_primary_key": true
        },
        {
          "target_field": "full_name",
          "transform": {
            "type": "concat",
            "fields": ["first_name", "last_name"],
            "separator": " "
          }
        },
        {
          "target_field": "email_address",
          "source_field": "email",
          "compare_rule": "ignore_case"
        },
        {
          "target_field": "status_code",
          "transform": {
            "type": "case",
            "fields": ["status"],
            "cases": [
              { "when": "active", "then": "1" },
              { "when": "inactive", "then": "0" }
            ],
            "else": "0"
          }
        },
        {
          "target_field": "balance",
          "source_field": "account_balance",
          "compare_rule": "numeric_tolerance",
          "tolerance": 0.01
        }
      ],
      "filters": {
        "source_filter": "deleted_at IS NULL",
        "target_filter": "is_deleted = false"
      },
      "sample_size": 10000
    }
  ],
  "global_settings": {
    "parallel_workers": 4,
    "output_dir": "./validation_reports",
    "report_format": ["json", "markdown"]
  }
}
```

---

## 命令行参数

```bash
python validator.py <config.json> [--parallel]
```

| 参数 | 说明 |
|------|------|
| `config.json` | 配置文件路径（必填） |
| `--parallel` | 启用并行校验 |

---

## 输出示例

### 控制台输出

```
=== 校验摘要 ===
users -> user_profile: completed
  匹配: 9850/10000
  不匹配: 120
  目标表缺失: 30

orders -> order_main: completed
  匹配: 50000/50000

校验完成！
```

### JSON 报告

```json
{
  "generated_at": "2026-03-14T18:00:00",
  "summary": {
    "total_tables": 2,
    "completed": 2,
    "errors": 0,
    "total_mismatched": 120,
    "total_missing_source": 0,
    "total_missing_target": 30
  },
  "results": [...]
}
```

---

## 扩展开发

### 添加新数据库支持

继承 `DatabaseConnection` 类并实现必要方法：

```python
class OracleConnection(DatabaseConnection):
    def connect(self):
        # 实现 Oracle 连接
        pass
    
    def close(self):
        # 关闭连接
        pass
    
    def execute_query(self, query: str, params: tuple = None):
        # 执行查询
        pass
    
    def get_table_count(self, table: str, schema: str = None, filter_clause: str = None):
        # 获取记录数
        pass
    
    def get_primary_keys(self, table: str, schema: str = None):
        # 获取主键
        pass
```

然后在 `DatabaseConnectionFactory` 中注册。

### 添加新转换类型

在 `DataValidator.transform_value` 方法中添加新的转换逻辑。

---

## 注意事项

1. **密码安全**: 建议使用 `password_env` 通过环境变量传递密码
2. **主键配置**: 每个表至少配置一个主键字段，用于记录匹配
3. **性能优化**: 大表建议设置 `sample_size` 进行抽样校验
4. **过滤条件**: 确保源表和目标表的过滤条件逻辑等价

---

## License

MIT License