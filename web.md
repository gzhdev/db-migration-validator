# Web 查看器说明

## 概述

Web 查看器是一个基于 Flask 的本地 Web 应用，用于可视化浏览校验工具产生的 Debug 输出。支持导入多次校验运行，对异常记录进行筛选、翻页查看。

---

## 安装

```bash
pip install flask>=3.0.0
```

---

## 启动

```bash
cd web
python app.py
```

默认监听 `http://127.0.0.1:5000`，浏览器打开即可使用。

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
web/
├── app.py          # Flask 路由
├── db.py           # SQLite 操作
├── importer.py     # Debug 目录导入逻辑
├── templates/      # Jinja2 模板
│   ├── base.html
│   ├── index.html
│   ├── run_detail.html
│   └── records.html
└── static/         # CSS / JS 静态资源
```
