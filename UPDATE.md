# 更新记录

## 2026-04-14

### FIX: `case` transform 中 `when_expr` 字段引用解析失败

**文件：** `validator/core.py` — `apply_transform()` 方法

**问题描述：**

在多表 JOIN 模式下，使用 `case` transform 且条件写成 `when_expr` 形式时，
Spark 抛出 `UNRESOLVED_COLUMN` 异常，提示无法解析形如 `o.si_dq_radg` 的列名。

**根本原因：**

多表 JOIN 后，DataFrame 的列名已被扁平化为 `alias_field` 格式（如 `O_si_dq_radg`），
不再存在表别名作用域。`when_expr` 中的字段引用（如 `o.si_dq_radg = 0`）被直接传入
`expr()`，Spark 将 `o.si_dq_radg` 解析为"表 `o` 的列 `si_dq_radg`"，导致解析失败。

同样的问题在 `math` transform 中早已修复（通过字符串替换将字段引用换成实际列名），
但 `case` transform 的 `when_expr` 分支遗漏了这一处理。

**修复方式：**

在 `when_expr` 传入 `expr()` 前，先做两层字段名替换：

1. 将 `transform.fields` 中声明的字段引用替换为 `field_alias_map` 中对应的实际列名
2. 兜底扫描 `field_alias_map`，替换 `when_expr` 中其他未经声明的 `alias.field` 引用

与 `math` transform 的处理逻辑保持一致。

**影响范围：** 仅影响多表 JOIN 模式下，`case` transform 使用 `when_expr` 写法的字段校验。

### FIX: `case` transform 修复引入的 `PARSE_SYNTAX_ERROR` 回归

**文件：** `validator/core.py` — `apply_transform()` 方法

**问题描述：**

上一轮修复 `when_expr` 字段引用时，`field_alias_map` 兜底替换逻辑在单表模式下
导致字段名被包裹双反引号（如 ` ``SA_PSBK_STS`` `），Spark 抛出 `PARSE_SYNTAX_ERROR`。

**根本原因：**

两个缺陷叠加：

1. **单表模式误替换：** 单表模式下 `field_alias_map` 中 key == value（如 `SA_PSBK_STS` → `SA_PSBK_STS`），
   匹配后无意义地添加了反引号
2. **重复替换：** `actual_fields` 循环已将字段替换为 `` `actual_col` `` 形式，
   但 `field_alias_map` 循环做子串匹配时再次命中，产生双反引号

**修复方式：**

在 `field_alias_map` 兜底循环中增加两个跳过条件：

1. `alias_field in handled_keys` — 跳过已被 `actual_fields` 循环处理过的字段
2. `alias_field == actual_col` — 跳过 key 与 value 相同的映射（单表模式）

**影响范围：** 所有使用 `case` transform + `when_expr` 的表校验（单表和多表模式）。