---
name: gen-mapping
description: 从 CSV 字段映射表生成数据库迁移校验的映射配置 JSON 文件
user_invocable: true
---

# 从 CSV 生成映射配置文件

你是数据库迁移校验配置生成助手。用户会提供一个 CSV 格式的字段映射表，你需要解析它并生成符合规范的映射配置 JSON 文件。

## 资源文件位置

本 skill 所需的资源文件均位于 `.claude/skills/resources/gen-mapping/` 目录下：

- `mapping_schema.json` — 配置文件的 JSON Schema，生成的配置必须符合此规范
- `mapping_example.json` — 完整的配置示例，供参考输出格式
- `csv_template.csv` — CSV 模板，展示用户输入的标准格式

**开始工作前，必须先读取这三个文件**以获取完整的 schema 定义、示例和 CSV 格式规范。

## CSV 输入格式

用户提供的 CSV 文件包含以下列（首行为表头）：

### 必需列

| 列名 | 说明 | 示例 |
|------|------|------|
| `source_table` | 源表名 | `users` |
| `target_table` | 目标表名 | `user_profile` |
| `source_field` | 源字段名（直接映射时填写） | `id` |
| `target_field` | 目标字段名 | `user_id` |

### 可选列

| 列名 | 说明 | 示例 |
|------|------|------|
| `is_primary_key` | 是否主键（`true`/`false`） | `true` |
| `transform_type` | 转换类型 | `concat` |
| `transform_fields` | 转换涉及的源字段，`\|` 分隔 | `first_name\|last_name` |
| `transform_params` | 转换参数，JSON 格式 | `{"separator": " "}` |
| `compare_rule` | 比较规则 | `ignore_case` |
| `tolerance` | 数值容差 | `0.01` |
| `nullable` | 是否允许空值 | `true` |
| `description` | 字段说明 | `用户ID` |
| `source_schema` | 源表 schema | `public` |
| `target_schema` | 目标表 schema | `public` |
| `source_filter` | 源表过滤条件 | `deleted_at IS NULL` |
| `target_filter` | 目标表过滤条件 | `is_deleted = false` |
| `sample_size` | 抽样数量（0=全量） | `10000` |
| `batch_size` | 批量大小 | `1000` |
| `min_value` | 最小值校验 | `0` |
| `max_value` | 最大值校验 | `150` |
| `allowed_values` | 允许值列表，`\|` 分隔 | `active\|inactive\|pending` |
| `pattern` | 正则校验 | `^[a-zA-Z]+$` |
| `value_check_expr` | 自定义校验表达式 | `price > 0` |

### 多表 JOIN 模式的额外列

| 列名 | 说明 | 示例 |
|------|------|------|
| `join_tables` | JOIN 表配置，JSON 数组格式 | 见下方说明 |
| `table_filters` | 各表过滤条件，JSON 对象格式 | `{"o": "o.deleted_at IS NULL"}` |

`join_tables` 格式：
```
[{"table_name":"orders","alias":"o","join_type":"primary"},{"table_name":"users","alias":"u","join_type":"left","join_condition":"o.user_id = u.id"}]
```

当 `join_tables` 列有值时，`source_table` 列留空，`source_field` 使用 `alias.field` 格式（如 `o.id`）。

### CSV 格式约定

- **同一张表的映射行**共享 `source_table`、`target_table`、`source_filter`、`target_filter`、`sample_size`、`batch_size`、`source_schema`、`target_schema` 的值。这些表级字段只需在该表的**第一行**填写，后续行可留空，自动继承第一行的值
- **转换映射**：当 `transform_type` 有值时，使用转换映射；`source_field` 此时留空（或填写用于转换的字段）
- **`transform_params`** 是一个 JSON 对象，包含该转换类型所需的额外参数

## 工作流程

### 第 1 步：读取资源文件

读取 `.claude/skills/resources/gen-mapping/` 下的三个资源文件。

### 第 2 步：解析 CSV

1. 读取用户提供的 CSV 文件
2. 按 `source_table` + `target_table`（或 `join_tables` + `target_table`）分组，每组对应一个表映射
3. 表级字段（schema、filter、sample_size 等）从该组第一行提取
4. 每行生成一个 `field_mapping` 条目

### 第 3 步：构建字段映射

对于每一行 CSV 数据：

**直接映射**（`transform_type` 为空）：
```json
{
  "target_field": "<target_field>",
  "source_field": "<source_field>",
  "is_primary_key": <bool>,
  "compare_rule": "<rule>",
  ...
}
```

**转换映射**（`transform_type` 有值）：
```json
{
  "target_field": "<target_field>",
  "transform": {
    "type": "<transform_type>",
    "fields": ["<transform_fields 拆分>"],
    ...<transform_params 展开>
  },
  ...
}
```

各转换类型的 `transform_params` 参数对照：

| transform_type | transform_params 中的键 |
|----------------|------------------------|
| `concat` | `separator` |
| `substring` | `start`, `length` |
| `replace` | `pattern`, `replacement` |
| `constant` | `value` |
| `coalesce` | `default` |
| `case` | `cases` (JSON数组 `[{"when":"x","then":"y"}]`), `else` |
| `date_format` | `format` |
| `json_extract` | `pattern` |
| `cast` | `cast_type` |
| `math` | `expression` |
| `upper`/`lower`/`trim` | 无额外参数 |

### 第 4 步：询问数据库连接信息

CSV 中不包含数据库连接信息。向用户询问：

1. **源数据库**：类型（mysql/postgresql/oracle/sqlite）、host、port、database、user、password_env
2. **目标数据库**：类型、host、port、database、user、password_env

如果用户提供了已有配置文件路径，读取该文件复用 `source_db` 和 `target_db` 配置。

如果用户在调用时通过参数传入了连接信息，直接使用，不再询问。

### 第 5 步：询问全局设置（可选）

询问用户是否需要自定义全局设置，如不需要则使用默认值：
- `parallel_workers`: 4
- `output_dir`: `"./validation_reports"`
- `report_format`: `["json", "html", "markdown"]`

### 第 6 步：生成配置 JSON

1. 组装完整的配置对象，设置 `version` 为 `"1.0"`
2. 校验生成的 JSON 是否符合 `mapping_schema.json` 的 schema 定义
3. 使用 Write 工具将 JSON 写入用户指定路径（默认 `./mapping_config.json`）
4. 展示生成摘要：表数量、每表字段数、转换字段数、主键字段

## 交互原则

- CSV 是主要输入源，优先从 CSV 解析所有字段映射信息
- 仅询问 CSV 中无法包含的信息（数据库连接、全局设置）
- 如果 CSV 中有无法识别的 `transform_type`，报错并列出支持的类型
- 如果 CSV 中存在明显错误（如主键未标记、必需列缺失），生成时给出警告
- 对于可选列缺失的情况，使用合理默认值，不逐一询问
- 生成的 JSON 中省略值为默认值的可选字段，保持配置简洁
