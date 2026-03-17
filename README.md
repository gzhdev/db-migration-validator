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
- **多种比较规则**: 精确比较、忽略大小写、忽略空白、数值容差、跳过比较
- **取值范围校验**: 支持最小/最大值、允许值列表、正则表达式、自定义表达式校验
- **多数据库支持**: MySQL、PostgreSQL、SQLite（可扩展 Oracle、SQL Server）
- **丰富的转换函数**: 字符串拼接、大小写转换、日期格式化、JSON 提取等
- **并行校验**: 单机版支持多线程，Spark版支持分布式计算
- **详细报告**: JSON、Markdown、HTML 多种格式报告，包含错误样本和差异详情

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

或使用 SID：

```json
{
  "type": "oracle",
  "host": "192.168.1.50",
  "port": 1521,
  "sid": "ORCL",
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

#### 2.1 单表模式示例

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
    {
      "table_name": "orders",
      "alias": "o",
      "join_type": "primary"
    },
    {
      "table_name": "users",
      "alias": "u",
      "join_type": "left",
      "join_condition": "o.user_id = u.id"
    },
    {
      "table_name": "products",
      "alias": "p",
      "join_type": "inner",
      "join_condition": "o.product_id = p.id"
    }
  ],
  "table_filters": {
    "o": "o.deleted_at IS NULL",
    "u": "u.status = 'active'"
  },
  "target_table": "order_detail",
  "field_mappings": [
    {
      "target_field": "order_id",
      "source_field": "o.id",
      "is_primary_key": true
    },
    {
      "target_field": "customer_name",
      "source_field": "u.name",
      "nullable": true
    },
    {
      "target_field": "total_amount",
      "transform": {
        "type": "math",
        "fields": ["o.price", "o.quantity"],
        "expression": "o.price * o.quantity"
      }
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

**字段引用格式**：在多表模式下，字段使用 `alias.field` 格式引用：
- `source_field`: `"o.id"`, `"u.name"`
- `transform.fields`: `["o.price", "o.quantity"]`
- `transform.expression`: `"o.price * o.quantity"`

**注意事项**：
- 多表 JOIN 模式不支持并行分区读取
- Oracle 数据库不支持 `AS` 别名语法，工具已自动处理
- LEFT/FULL JOIN 可能产生 NULL 值，需正确配置 `nullable`
- 复杂 JOIN 建议在数据库层面创建视图

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

### 6. 取值范围校验 (Spark版)

取值范围校验用于验证目标表字段值是否符合预期约束，可配置多个校验条件。

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `min_value` | number | 最小值（数值类型） |
| `max_value` | number | 最大值（数值类型） |
| `allowed_values` | array | 允许的值列表（枚举） |
| `pattern` | string | 正则表达式（字符串类型） |
| `value_check_expr` | string | 自定义 Spark SQL 表达式 |

#### 6.1 数值范围校验

```json
{
  "target_field": "age",
  "source_field": "user_age",
  "min_value": 0,
  "max_value": 150
}
```

#### 6.2 枚举值校验

```json
{
  "target_field": "status",
  "source_field": "status_code",
  "allowed_values": ["active", "inactive", "pending"]
}
```

#### 6.3 正则表达式校验

```json
{
  "target_field": "email",
  "source_field": "email_address",
  "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
}
```

#### 6.4 自定义表达式校验

支持 Spark SQL 表达式，可用于复杂的校验逻辑：

```json
{
  "target_field": "price",
  "source_field": "unit_price",
  "value_check_expr": "price > 0 AND price < 1000000"
}
```

```json
{
  "target_field": "discount",
  "source_field": "discount_rate",
  "value_check_expr": "discount >= 0 AND discount <= price"
}
```

#### 6.5 组合校验

可以同时配置多个校验条件：

```json
{
  "target_field": "score",
  "source_field": "exam_score",
  "min_value": 0,
  "max_value": 100,
  "value_check_expr": "score % 1 = 0"
}
```

#### 6.6 NULL 值处理

通过 `nullable` 配置项控制 NULL 值的处理方式：

```json
{
  "target_field": "nickname",
  "source_field": "nick_name",
  "allowed_values": ["Alice", "Bob", "Charlie"],
  "nullable": true
}
```

| `nullable` 值 | NULL 值行为 |
|---------------|-------------|
| `true`（默认） | NULL 值通过校验，不检查其他条件 |
| `false` | NULL 值校验失败，报告显示"字段不允许为空" |

**注意事项**：
- Spark SQL 的 `isin()`、`rlike()` 等函数对 NULL 返回 NULL（非 TRUE/FALSE）
- 工具内部已处理此问题，`nullable: true` 时 NULL 值会跳过所有取值范围检查
- 报告中 NULL 值显示为 `NULL` 而非 `None`

#### 6.7 报告输出示例

校验失败时，报告中会显示详细错误信息：

```markdown
**取值范围校验失败:**

1. 主键: `{user_id: 123}`
   - `age`: 值 `200` - 大于最大值 150
   - `status`: 值 `unknown` - 不在允许值列表中 ['active', 'inactive', 'pending']

2. 主键: `{user_id: 456}`
   - `email`: 值 `NULL` - 字段不允许为空
```

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

#### 9.1 配置说明

```json
{
  "global_settings": {
    "debug": {
      "enabled": true,
      "output_dir": "./debug",
      "formats": ["jsonl", "html"],
      "max_records": 10000,
      "buffer_size": 1000,
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

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `enabled` | bool | false | 是否启用 debug 模式 |
| `output_dir` | string | `./debug` | debug 输出目录 |
| `formats` | array | `["jsonl", "html"]` | 输出格式 |
| `max_records` | int | 10000 | 最多记录的比对条数，**0 表示不限制** |
| `buffer_size` | int | 1000 | JSONL 写入缓冲区大小 |

**记录类型配置** (`record_types`):

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `matched` | false | 完全匹配的记录 |
| `mismatched` | true | 字段不匹配的记录 |
| `missing_in_source` | true | 源表缺失的记录 |
| `missing_in_target` | true | 目标表缺失的记录 |
| `value_check_failed` | true | 取值范围校验失败的记录 |

**数据内容配置** (`data_content`):

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `include_raw_source` | true | 包含原始源表数据 |
| `include_expected` | true | 包含转换后的期望值 |
| `include_target` | true | 包含目标表实际值 |

#### 9.2 输出格式

**JSONL 格式** (数据导出，每行一个 JSON 对象):

```jsonl
{"record_id":"001_000001","primary_key":{"id":123},"match_type":"mismatched","raw_source":{"id":123,"first_name":"John","last_name":"Doe"},"expected_values":{"user_id":123,"full_name":"John Doe"},"target_values":{"user_id":123,"full_name":"Johnny Doe"},"comparison_result":{"full_name":{"match":false,"expected":"John Doe","actual":"Johnny Doe"}}}
```

**HTML 格式** (交互式报告):
- 三列并排展示：原始数据 | 期望值 | 目标值
- 不匹配字段高亮显示
- 支持按类型过滤（mismatched/missing/value_check_failed）
- 支持关键词搜索

#### 9.3 输出目录结构

```
./debug/
├── debug_report_20260317.html      # HTML 交互式报告
├── users_to_profile/               # 每个表一个目录
│   ├── records.jsonl               # JSONL 数据文件
│   └── summary.json                # 表级统计
└── orders_to_order/
    ├── records.jsonl
    └── summary.json
```

#### 9.4 HTML 报告示例

HTML 报告提供交互式界面：
- **总览面板**: 显示校验表数、debug 记录数、各类统计
- **表详情**: 可折叠展开每个表的详细记录
- **三列对比**: 原始数据（蓝色）、期望值（绿色）、目标值（橙色）
- **差异高亮**: 不匹配字段在 comparison_result 区域显示
- **过滤搜索**: 按记录类型过滤，支持关键词搜索

#### 9.5 注意事项

- `max_records` 限制记录的最大条数，**设为 0 表示不限制**（可能导致文件过大）
- `record_types.matched: true` 会记录所有完全匹配的记录，数据量可能很大
- debug 模式会影响性能，建议仅在排查问题时启用
- debug 输出可能包含敏感数据，注意文件权限和存储位置
- JSONL 格式支持流式写入，大数据量时内存效率更高

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
    "report_format": ["json", "markdown"],
    "debug": {
      "enabled": false,
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
4. 取值范围校验（Spark版）
   ├── 检查 min_value / max_value
   ├── 检查 allowed_values
   ├── 检查 pattern 正则
   └── 检查 value_check_expr
   ↓
5. 生成报告（JSON/Markdown）
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
  取值范围失败: 15

校验完成！
```

---

## License

MIT License