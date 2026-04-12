# 数据库迁移数据一致性校验工具

数据库迁移后对比源表和转换后表数据一致性的 Python 工具。

## 版本

| 版本 | 文件 | 适用场景 |
|------|------|----------|
| 单机版 | `validator.py` | 中小数据量，无需集群 |
| Spark版 | `validator_spark.py` | 大数据量，分布式计算 |

## 功能特性

- **灵活的字段映射**: 支持直接映射、字段转换、条件映射等多种方式
- **多表 JOIN 支持**: Spark版支持多张源表 JOIN 后与目标表比对
- **条件分段来源**: 目标表数据来自不同条件下的不同源表时，拆分为多个映射条目分别校验
- **多种比较规则**: 精确比较、忽略大小写、忽略空白、数值容差、跳过比较
- **取值范围校验**: 支持最小/最大值、允许值列表、正则表达式、自定义表达式校验
- **多数据库支持**: MySQL、PostgreSQL、Oracle、SQLite
- **丰富的转换函数**: 字符串拼接、大小写转换、日期格式化、JSON 提取等
- **并行校验**: 单机版支持多线程，Spark版支持分布式计算
- **详细报告**: JSON、Markdown、HTML 多种格式报告，包含错误样本和差异详情
- **可视化配置生成器**: Web 界面可视化编辑并生成配置文件，支持 CSV 批量导入

## 安装

```bash
# 克隆项目
git clone https://github.com/gzhdev/db-migration-validator.git
cd db-migration-validator

# 安装依赖（推荐使用 uv）
uv sync
```

所有依赖声明在 `pyproject.toml` 中，`uv sync` 会自动创建 `.venv`。

## 快速开始

### 单机版

```bash
uv run python validator.py mapping_example.json
uv run python validator.py mapping_example.json --parallel
```

### Spark版

```bash
# 本地模式
uv run python validator_spark.py mapping_example.json

# 指定 Spark Master
uv run python validator_spark.py mapping_example.json --master spark://localhost:7077

# YARN 模式
spark-submit validator_spark.py mapping_example.json --master yarn
```

### Web 服务器（配置生成器 + Debug 日志查看器）

```bash
cd web && uv run python app.py
# 访问 http://127.0.0.1:5000
```

| 页面 | 地址 | 说明 |
|------|------|------|
| Debug 日志查看器 | `/` | 导入并浏览校验运行记录 |
| 映射配置生成器 | `/mapping-generator` | 可视化生成配置文件 |

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
  "password_env": "SOURCE_DB_PASSWORD"
}
```

**安全方式（推荐）**：使用环境变量存储密码，然后设置：
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

#### Oracle (使用 python-oracledb)

python-oracledb 支持两种模式：

**Thin 模式（推荐）** - 纯 Python 实现，无需安装 Oracle Client：

```json
{
  "type": "oracle",
  "host": "192.168.1.50",
  "port": 1521,
  "service_name": "ORCL",
  "user": "scott",
  "password_env": "ORACLE_PASSWORD"
}
```

**Thick 模式** - 需要 Oracle Client，支持更多特性：

```json
{
  "type": "oracle",
  "host": "192.168.1.50",
  "port": 1521,
  "service_name": "ORCL",
  "user": "scott",
  "password_env": "ORACLE_PASSWORD",
  "thick_mode": true,
  "oracle_client": "/opt/oracle/instantclient_21_1"
}
```

| 参数 | 说明 |
|------|------|
| `service_name` | Oracle 服务名（推荐） |
| `sid` | Oracle SID（与 service_name 二选一） |
| `thick_mode` | 是否启用 Thick 模式，默认 false |
| `oracle_client` | Oracle Client 库路径（thick_mode=true 时） |

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
| `source_table` | 二选一 | 源表名（单表模式，与 source_tables 二选一） |
| `source_tables` | 二选一 | 多表 JOIN 配置（多表模式，与 source_table 二选一） |
| `target_table` | 是 | 目标表名 |
| `source_schema` | 否 | 源表 schema（PostgreSQL，单表模式） |
| `target_schema` | 否 | 目标表 schema |
| `description` | 否 | 映射说明 |
| `field_mappings` | 是 | 字段映射列表（见下文） |
| `filters` | 否 | 过滤条件（单表模式） |
| `table_filters` | 否 | 各表过滤条件（多表模式） |
| `sample_size` | 否 | 抽样数量，0 表示全量 |
| `batch_size` | 否 | 批量大小，默认 1000 |

#### 2.1 单表模式

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

#### 2.2 多表 JOIN 模式（Spark版）

当目标表数据来自多张源表 JOIN 时，使用 `source_tables` 配置：

```json
{
  "source_tables": [
    { "table_name": "orders", "alias": "o", "join_type": "primary" },
    { "table_name": "users", "alias": "u", "join_type": "left", "join_condition": "o.user_id = u.id" },
    { "table_name": "products", "alias": "p", "join_type": "inner", "join_condition": "o.product_id = p.id" }
  ],
  "table_filters": {
    "o": "o.deleted_at IS NULL",
    "u": "u.status = 'active'"
  },
  "target_table": "order_detail",
  "field_mappings": [
    { "target_field": "order_id", "source_field": "o.id", "is_primary_key": true },
    { "target_field": "customer_name", "source_field": "u.name", "nullable": true },
    {
      "target_field": "total_amount",
      "transform": { "type": "math", "fields": ["o.price", "o.quantity"], "expression": "o.price * o.quantity" }
    }
  ]
}
```

**多表 JOIN 配置说明**：

| 字段 | 必填 | 说明 |
|------|------|------|
| `table_name` | 是 | 源表名 |
| `alias` | 是 | 表别名，用于字段引用（如 `o.id`） |
| `schema` | 否 | 源表 schema |
| `join_type` | 否 | JOIN 类型：`primary`（第一个表）、`inner`、`left`、`right`、`full`、`cross` |
| `join_condition` | 条件 | JOIN 条件（第一个表除外，必需） |

在多表模式下，字段使用 `alias.field` 格式引用：`"o.id"`、`"u.name"`、`"o.price * o.quantity"`。

**注意事项**：
- 多表 JOIN 模式不支持并行分区读取（仅 Spark 版）
- Oracle 数据库不支持 `AS` 别名语法，工具已自动处理
- LEFT/FULL JOIN 可能产生 NULL 值，需正确配置 `nullable`
- 复杂 JOIN 建议在数据库层面创建视图

#### 2.3 条件分段来源（多源目标表）

当目标表的数据**根据不同条件来自不同源表**时（如 UNION 场景），将其拆分为多个互斥的表映射条目，每个条目用 `source_filter` / `target_filter` 限定各自的数据分区，校验器独立运行每个条目。

```json
{
  "tables": [
    {
      "description": "条件1满足：数据来自 table_a",
      "source_table": "table_a",
      "target_table": "target",
      "filters": {
        "source_filter": "type = 'X'",
        "target_filter": "type = 'X'"
      },
      "field_mappings": [ ... ]
    },
    {
      "description": "条件1不满足且条件2满足：数据来自 table_a JOIN table_b",
      "source_tables": [
        { "table_name": "table_a", "alias": "a", "join_type": "primary" },
        { "table_name": "table_b", "alias": "b", "join_type": "left", "join_condition": "a.id = b.ref_id" }
      ],
      "table_filters": { "a": "a.type != 'X' AND a.category = 'Y'" },
      "target_table": "target",
      "filters": { "target_filter": "type != 'X' AND category = 'Y'" },
      "field_mappings": [ ... ]
    }
  ]
}
```

**关键原则**：各条目的 `source_filter` 与 `target_filter` 必须互斥且完整覆盖，避免数据重复或遗漏。

另一种方案：在源库创建封装了 UNION 逻辑的视图，配置中直接引用视图名作为 `source_table`。

### 3. 字段映射配置

字段映射定义目标表字段如何从源表获取数据。

#### 3.1 直接映射

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
| `nullable` | 否 | 是否允许空值，默认 true（Spark版取值范围校验使用） |
| `compare_rule` | 否 | 比较规则，默认 "exact" |
| `tolerance` | 否 | 数值容差（compare_rule=numeric_tolerance 时） |
| `description` | 否 | 字段说明 |
| `min_value` | 否 | 最小值（Spark版取值范围校验） |
| `max_value` | 否 | 最大值（Spark版取值范围校验） |
| `allowed_values` | 否 | 允许值列表（Spark版取值范围校验） |
| `pattern` | 否 | 正则表达式（Spark版取值范围校验） |
| `value_check_expr` | 否 | 自定义 Spark SQL 表达式（Spark版取值范围校验） |

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

| 类型 | 说明 | 关键参数 |
|------|------|----------|
| `concat` | 字符串拼接 | `fields`, `separator` |
| `upper` / `lower` / `trim` | 大小写/去空白 | `fields` |
| `substring` | 子字符串 | `fields`, `start`, `length` |
| `replace` | 字符串替换 | `fields`, `pattern`, `replacement` |
| `constant` | 常量值 | `value` |
| `coalesce` | 空值处理（取第一个非空） | `fields`, `default` |
| `case` | 条件映射（CASE WHEN） | `fields`, `cases`, `else` |
| `date_format` | 日期格式化 | `fields`, `format` |
| `json_extract` | JSON 提取 | `fields`, `pattern` |
| `cast` | 类型转换 | `fields`, `cast_type` |
| `math` | 数学运算 | `fields`, `expression` |

**示例**：

```json
{ "type": "case", "fields": ["status"],
  "cases": [{"when": "active", "then": "1"}, {"when": "inactive", "then": "0"}],
  "else": "0" }

{ "type": "math", "fields": ["price", "tax", "shipping"],
  "expression": "price + tax + shipping" }

{ "type": "json_extract", "fields": ["metadata"], "pattern": "$.user.tags" }
```

### 5. 比较规则 (compare_rule)

| 规则 | 说明 |
|------|------|
| `exact` | 精确比较（默认） |
| `ignore_case` | 忽略大小写 |
| `ignore_whitespace` | 忽略空白字符 |
| `numeric_tolerance` | 数值容差比较，需配合 `tolerance` 字段 |
| `skip` | 跳过该字段的比较 |

### 6. 取值范围校验 (Spark版)

取值范围校验用于验证目标表字段值是否符合预期约束。

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `min_value` | number | 最小值 |
| `max_value` | number | 最大值 |
| `allowed_values` | array | 允许的值列表（枚举） |
| `pattern` | string | 正则表达式 |
| `value_check_expr` | string | 自定义 Spark SQL 表达式 |

```json
{ "target_field": "age", "source_field": "user_age", "min_value": 0, "max_value": 150 }

{ "target_field": "status", "source_field": "status_code",
  "allowed_values": ["active", "inactive", "pending"] }

{ "target_field": "email", "source_field": "email_address",
  "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$" }

{ "target_field": "price", "source_field": "unit_price",
  "value_check_expr": "price > 0 AND price < 1000000" }
```

**NULL 值处理**：

| `nullable` 值 | NULL 值行为 |
|---------------|-------------|
| `true`（默认） | NULL 值通过校验，不检查其他条件 |
| `false` | NULL 值校验失败，报告显示"字段不允许为空" |

### 7. 过滤条件 (filters)

```json
{
  "filters": {
    "source_filter": "deleted_at IS NULL AND status = 'active'",
    "target_filter": "is_deleted = false AND status_code = 1"
  }
}
```

### 8. 全局设置 (global_settings)

```json
{
  "global_settings": {
    "parallel_workers": 4,
    "timeout_seconds": 3600,
    "output_dir": "./reports",
    "report_format": ["json", "html", "markdown"],
    "debug": {
      "enabled": true,
      "output_dir": "./debug",
      "formats": ["jsonl", "html"],
      "max_records": 10000,
      "record_types": {
        "matched": false,
        "mismatched": true,
        "missing_in_source": true,
        "missing_in_target": true,
        "value_check_failed": true
      },
      "data_content": {
        "include_raw_source": true,
        "include_expected": true,
        "include_target": true
      }
    }
  }
}
```

### 9. Debug 模式（Spark版）

启用 debug 模式可以记录详细的比对过程，包含**三层数据**（原始数据、期望值、目标值），便于排查数据不一致问题。

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `enabled` | bool | false | 是否启用 debug 模式 |
| `output_dir` | string | `./debug` | debug 输出目录 |
| `formats` | array | `["jsonl", "html"]` | 输出格式 |
| `max_records` | int | 10000 | 最多记录条数，**0 表示不限制** |
| `buffer_size` | int | 1000 | JSONL 写入缓冲区大小 |

**输出目录结构**：

```
./debug/
├── debug_report_20260317.html      # HTML 交互式报告
├── users_to_profile/
│   ├── records.jsonl               # JSONL 数据文件
│   └── summary.json
└── orders_to_order/
    ├── records.jsonl
    └── summary.json
```

注意事项：
- `record_types.matched: true` 会记录所有匹配记录，数据量可能很大
- debug 模式影响性能，建议仅排查问题时启用
- debug 输出可能含敏感数据，注意文件权限

---

## 完整配置示例

```json
{
  "version": "1.1",
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
        { "target_field": "user_id", "source_field": "id", "is_primary_key": true },
        {
          "target_field": "full_name",
          "transform": { "type": "concat", "fields": ["first_name", "last_name"], "separator": " " }
        },
        { "target_field": "email_address", "source_field": "email", "compare_rule": "ignore_case" },
        {
          "target_field": "status_code",
          "transform": {
            "type": "case", "fields": ["status"],
            "cases": [{"when": "active", "then": "1"}, {"when": "inactive", "then": "0"}],
            "else": "0"
          }
        },
        { "target_field": "balance", "source_field": "account_balance",
          "compare_rule": "numeric_tolerance", "tolerance": 0.01 }
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
uv run python validator.py <config.json> [--parallel]
```

### Spark版

```bash
uv run python validator_spark.py <config.json> [--master <spark-master>]
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
4. 取值范围校验（Spark版）
   ├── 检查 min_value / max_value
   ├── 检查 allowed_values
   ├── 检查 pattern 正则
   └── 检查 value_check_expr
   ↓
5. 生成报告（JSON/Markdown/HTML）
```

---

## 输出示例

```
=== 校验摘要 ===
users -> user_profile: completed
  匹配: 9850/10000
  不匹配: 120
  目标表缺失: 30
  取值范围失败: 15

校验完成！
```

---

## License

MIT License