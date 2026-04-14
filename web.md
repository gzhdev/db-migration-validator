# Web 模块说明

## 概述

Web 模块是一个基于 Flask 的本地 Web 应用，提供两项功能：

| 功能 | 路径 | 说明 |
|------|------|------|
| **映射配置生成器** | `/mapping-generator` | 可视化生成校验配置文件，支持 Excel 批量导入 |
| **Debug 日志查看器** | `/` | 导入并浏览校验运行产生的 Debug 输出 |

Web 模块作为独立仓库维护，通过 git submodule 引入：https://github.com/gzhdev/db-migration-validator-web.git

---

---

## 安装与启动

```bash
# 拉取 submodule
git submodule update --init

# 安装依赖并启动
cd web
uv sync
uv run python app.py
```

默认监听 `http://127.0.0.1:5000`，浏览器打开即可使用。

---

## 映射配置生成器

访问 `/mapping-generator`，可视化编辑并生成校验配置 JSON，无需手写配置文件。

### 主要功能

- **数据库连接配置**：支持 MySQL、PostgreSQL、Oracle、SQLite
- **表映射编辑**：支持单表模式和多表 JOIN 模式，可添加任意数量的表映射条目
- **字段映射编辑**：支持所有转换类型（concat、case、math 等）和比较规则，字段详细设置（取值范围校验）通过弹窗配置
- **JSON 预览/下载/复制**：实时生成预览，一键下载
- **导入已有 JSON**：上传现有配置文件，在界面中继续编辑
- **Excel 批量导入**：上传字段映射 Excel 文件，自动生成完整配置

### Excel 批量导入

点击"下载 Excel 模板"获取模板文件（`mapping_template.xlsx`），包含 4 个 Sheet：

| Sheet | 内容 |
|-------|------|
| 单表示例 | 基本的单表字段映射示例 |
| 多表JOIN示例 | 多表 JOIN 场景示例 |
| 多源插入示例 | 3 个条目从不同源表插入同一目标表（对应存储过程多 INSERT 场景） |
| 填写说明 | 每列的含义、用途和填写规范 |

**填写规则**：

- 每个表映射的**第一行**填写 `source_table`（或 `join_tables`）和 `target_table`，后续行留空代表同一表映射的字段继续
- 多表 JOIN 模式：`join_tables` 填 JSON 数组，`table_filters` 填 JSON 对象
- 多个 Sheet 的数据会合并导入（"填写说明" Sheet 自动跳过）

**多源插入同一目标表**（存储过程多 INSERT 场景）：

在 Excel 中创建多个表映射条目，每个条目的 `target_table` 相同，但配置不同的源表和互斥的 `target_filter`：

```
条目1: source_table=table_a, target_table=target, target_filter="type='X'"
条目2: join_tables=[...],   target_table=target, target_filter="type!='X' AND category='Y'"
条目3: source_table=table_c, target_table=target, target_filter="type!='X' AND category!='Y'"
```

> **注意**：各条目的 `target_filter` 必须互斥且完整覆盖目标表全部行，否则会产生重复计数或静默遗漏。

---

## 前置：开启 Debug 输出

Web 查看器的数据来源是校验工具的 Debug 输出目录。需在校验配置文件的 `global_settings` 中开启：

```json
{
  "global_settings": {
    "debug": {
      "enabled": true,
      "output_dir": "./debug",
      "formats": ["jsonl"],
      "max_records": 10000,
      "record_types": {
        "matched": false,
        "mismatched": true,
        "missing_in_source": true,
        "missing_in_target": true,
        "value_check_failed": true
      }
    }
  }
}
```

运行校验后，`output_dir` 下会生成如下结构：

```
./debug/
├── orders_to_order_detail/
│   ├── summary.json      # 该表的统计数据
│   └── records.jsonl     # 异常记录（每行一个 JSON 对象）
└── users_to_user_info/
    ├── summary.json
    └── records.jsonl
```

---

## 功能说明

### 首页 `/`

展示所有已导入的校验运行列表，每条显示：

- 导入时间
- Debug 目录路径
- 表数量 / 总记录数
- 状态（`done` / `error`）

**导入新运行：** 在首页表单中填入 Debug 目录的绝对路径，点击导入。

**删除运行：** 点击运行右侧的删除按钮，会同时删除该运行的所有记录。

### 运行详情 `/runs/<id>`

展示该次校验各表的汇总统计：

| 列 | 说明 |
|----|------|
| 表名 | 源表 → 目标表 |
| 总行数 | 校验的数据总量 |
| 匹配 | 完全一致的行数 |
| 不匹配 | 字段值存在差异 |
| 源表缺失 | 目标有、源表无 |
| 目标缺失 | 源表有、目标无 |
| 取值校验失败 | 违反 `value_check` 规则 |

点击表行可跳转到该表的记录列表。

### 记录列表 `/runs/<id>/records`

支持按表名和记录类型筛选，分页展示每条异常记录的详细信息：

- **主键**：标识该记录的主键值
- **原始源数据**：源表读取到的原始字段值
- **期望值**：经过 transform 计算后的期望目标值
- **目标实际值**：目标表中实际存储的值
- **字段对比结果**：逐字段展示是否匹配
- **取值校验失败详情**：违反哪条规则、实际值是什么

---

## API 接口

Web 查看器同时提供 JSON API，可供脚本集成。

### 查询记录

```
GET /api/runs/<run_id>/records
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | string | 按表名过滤（可选） |
| `match_type` | string | 按类型过滤：`mismatched` / `missing_in_source` / `missing_in_target` / `value_check_failed`（可选） |
| `page` | int | 页码，默认 `1` |
| `page_size` | int | 每页条数，默认 `20` |

**响应示例：**

```json
{
  "total": 152,
  "page": 1,
  "page_size": 20,
  "total_pages": 8,
  "records": [
    {
      "id": 1,
      "table_name": "orders_to_order_detail",
      "match_type": "mismatched",
      "primary_key": {"order_id": 1001},
      "expected_values": {"total": "199.00"},
      "target_values": {"total": "200.00"},
      "comparison_result": {"total": false}
    }
  ]
}
```

### 删除运行

```
DELETE /runs/<run_id>
```

响应：`{"ok": true}`

---

## 数据存储

导入的数据存储在 `web/debug_viewer.db`（SQLite），包含三张表：

| 表 | 内容 |
|----|------|
| `runs` | 每次导入的运行元数据 |
| `table_summaries` | 各表统计数据 |
| `records` | 详细异常记录 |

数据库文件在 `.gitignore` 中，不会提交到版本库。如需清空所有数据，直接删除该文件即可（重启后自动重建）。

---

## 目录结构

```
web/                          # git submodule → db-migration-validator-web
├── pyproject.toml            # uv 项目配置，依赖 flask>=3.0.0, openpyxl>=3.1.0
├── uv.lock                   # 锁定依赖版本
├── .python-version           # 指定 Python 版本
├── .gitignore
├── app.py                    # Flask 路由
├── db.py                     # SQLite 操作
├── importer.py               # Debug 目录导入逻辑
├── templates/                # Jinja2 模板
│   ├── base.html
│   ├── index.html
│   ├── run_detail.html
│   ├── records.html
│   └── mapping_generator.html
└── static/                   # CSS / JS 静态资源
    ├── css/style.css
    └── js/
        ├── app.js
        └── mapping_generator.js
```
