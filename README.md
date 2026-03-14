# 数据库迁移数据一致性校验工具

数据库迁移后对比源表和转换后表数据一致性的 Python 工具。

## 版本

| 版本 | 文件 | 适用场景 |
|------|------|----------|
| 单机版 | `validator.py` | 中小数据量，无需集群 |
| Spark版 | `validator_spark.py` | 大数据量，分布式计算 |

## 功能特性

- **灵活的字段映射**: 支持直接映射、字段转换、条件映射等多种方式
- **多种比较规则**: 精确比较、忽略大小写、忽略空白、数值容差、跳过比较
- **多数据库支持**: MySQL、PostgreSQL、SQLite（可扩展 Oracle、SQL Server）
- **丰富的转换函数**: 字符串拼接、大小写转换、日期格式化、JSON 提取等
- **并行校验**: 单机版支持多线程，Spark版支持分布式计算
- **详细报告**: JSON、Markdown、HTML 多种格式报告

## 安装

```bash
# 克隆项目
git clone https://github.com/gzhdev/db-migration-validator.git
cd db-migration-validator

# 安装依赖（单机版）
pip install mysql-connector-python psycopg2-binary

# 安装依赖（Spark版）
pip install pyspark mysql-connector-python psycopg2-binary
```

## 快速开始

### 单机版

```bash
python validator.py mapping_example.json
python validator.py mapping_example.json --parallel
```

### Spark版

```bash
# 本地模式
python validator_spark.py mapping_example.json

# 指定 Spark Master
python validator_spark.py mapping_example.json --master spark://localhost:7077

# YARN 模式
spark-submit validator_spark.py mapping_example.json --master yarn
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

| 类型 | 说明 | 示例 |
|------|------|------|
| `concat` | 字符串拼接 | `concat` fields with separator |
| `upper` | 转大写 | `upper` of field |
| `lower` | 转小写 | `lower` of field |
| `trim` | 去除空白 | `trim` of field |
| `substring` | 子字符串 | substring from start with length |
| `replace` | 字符串替换 | replace pattern with replacement |
| `constant` | 常量值 | fixed value |
| `coalesce` | 空值处理 | first non-null value |
| `case` | 条件映射 | CASE WHEN equivalent |
| `date_format` | 日期格式化 | format date string |
| `json_extract` | JSON 提取 | extract from JSON field |
| `cast` | 类型转换 | cast to int/float/string/bool |
| `math` | 数学运算 | arithmetic expression |

#### 4.1 concat - 字符串拼接

```json
{
  "type": "concat",
  "fields": ["first_name", "last_name"],
  "separator": " "
}
```

#### 4.2 case - 条件映射

```json
{
  "type": "case",
  "fields": ["status"],
  "cases": [
    { "when": "active", "then": "1" },
    { "when": "inactive", "then": "0" }
  ],
  "else": "0"
}
```

#### 4.3 coalesce - 空值处理

```json
{
  "type": "coalesce",
  "fields": ["nickname", "username"],
  "default": "Anonymous"
}
```

#### 4.4 json_extract - JSON 提取

```json
{
  "type": "json_extract",
  "fields": ["metadata"],
  "pattern": "$.user.tags"
}
```

#### 4.5 math - 数学运算

```json
{
  "type": "math",
  "fields": ["price", "tax", "shipping"],
  "expression": "price + tax + shipping"
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

### 单机版

```bash
python validator.py <config.json> [--parallel]
```

### Spark版

```bash
python validator_spark.py <config.json> [--master <spark-master>]
```

---

## 执行逻辑

```
1. 加载配置
   ↓
2. 连接源库和目标库
   ↓
3. 遍历每个表映射
   ├── 构建源表查询（提取涉及字段）
   ├── 构建目标表查询
   ├── 拉取数据为 DataFrame
   ├── 应用 transform 计算"期望值"
   ├── 用主键 JOIN 两边数据
   └── 逐条比对
       ├── 源有目标无 → missing_in_target
       ├── 目标有源无 → missing_in_source
       └── 都有 → 按 compare_rule 比较字段
   ↓
4. 生成报告（JSON/Markdown）
```

---

## 输出示例

### 控制台输出

```
=== 校验摘要 ===
users -> user_profile: completed
  匹配: 9850/10000
  不匹配: 120
  目标表缺失: 30

校验完成！
```

---

## License

MIT License