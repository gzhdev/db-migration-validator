# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a database migration data consistency validator tool with two implementations:

| Version | File | Use Case |
|---------|------|----------|
| Single-machine | `validator.py` | Small to medium data volumes, no cluster needed |
| Spark distributed | `validator_spark.py` | Large datasets, distributed computing |

The tool validates data consistency between source and target tables after database migration, supporting flexible field mappings with transforms and multiple comparison rules.

## Running the Validator

### Single-machine version
```bash
# Basic run
python validator.py mapping_example.json

# Parallel execution
python validator.py mapping_example.json --parallel
```

### Spark version
```bash
# Local mode
python validator_spark.py mapping_example.json

# Specify Spark master
python validator_spark.py mapping_example.json --master spark://localhost:7077

# YARN mode
spark-submit validator_spark.py mapping_example.json --master yarn
```

### Installing dependencies
```bash
# Single-machine version
pip install mysql-connector-python psycopg2-binary oracledb

# Spark version
pip install pyspark mysql-connector-python psycopg2-binary
```

## Architecture

### Core Components

**Data Classes** (Spark version):
- `SourceTable`: Defines a source table in multi-table JOIN (table_name, alias, join_type, join_condition)
- `FieldMapping`: Defines how a target field maps from source (direct field or transform)
- `TableMapping`: Contains source/target table info, field mappings, filters; supports single-table and multi-table modes
- `ValidationResult`: Stores validation outcomes (matched, mismatched, missing counts)

**Multi-Table JOIN Support** (Spark version only):
- `source_tables` array defines multiple source tables with aliases
- `table_filters` defines per-table WHERE conditions
- Field references use `alias.field` format (e.g., `o.id`, `u.name`)
- `is_multi_source()` method on `TableMapping` determines mode

**Database Connection Pattern** (validator.py):
- Abstract `DatabaseConnection` class with concrete implementations for MySQL, PostgreSQL, SQLite, Oracle
- `DatabaseConnectionFactory` creates appropriate connection based on config `type`
- Supports `password_env` for secure password retrieval from environment variables

**Oracle Specifics**:
- Uses `python-oracledb` library (not `cx_Oracle`)
- Thin mode is default (no Oracle Client required)
- Thick mode available via `thick_mode: true` and `oracle_client` path
- Supports both `service_name` and `sid` for DSN construction

### Validation Flow

1. **Load config** - JSON mapping file with source_db, target_db, tables array
2. **Connect databases** - Establish connections via JDBC (Spark) or drivers (single-machine)
3. **For each table mapping**:
   - Validate configuration (alias uniqueness, JOIN conditions, field references)
   - Build source query:
     - Single-table: `(SELECT fields FROM table WHERE filter) AS subq`
     - Multi-table: `(SELECT alias_fields FROM table1 alias1 JOIN_TYPE table2 alias2 ON condition WHERE filters) subq`
   - Build target query
   - Load data into DataFrame/list of dicts
   - Apply transforms to calculate "expected values" for target fields
   - JOIN data on primary keys
   - Compare fields using configured `compare_rule`
4. **Generate report** - JSON and Markdown formats to output directory

### Key Design Patterns

**Transform Types** (how source data becomes expected target values):
- `concat` - String concatenation with separator
- `upper`/`lower`/`trim` - String manipulation
- `case` - Conditional WHEN/THEN logic
- `coalesce` - First non-null value with default
- `json_extract` - JSONPath extraction
- `cast` - Type conversion (int/float/bool/string)
- `math` - Arithmetic expression evaluation
- `constant` - Fixed value
- `substring`, `replace`, `date_format` - Various utilities

**Compare Rules** (how expected vs actual values are compared):
- `exact` - Default, string equality
- `ignore_case` - Case-insensitive comparison
- `ignore_whitespace` - Strip whitespace before compare
- `numeric_tolerance` - Allow numeric difference within `tolerance`
- `skip` - Don't compare this field

**Value Check Rules** (Spark version only - validate target field values):
- `min_value` / `max_value` - Numeric range validation
- `allowed_values` - Enum validation (list of allowed values)
- `pattern` - Regex pattern validation
- `value_check_expr` - Custom Spark SQL expression
- `nullable` - If `true` (default), NULL values pass all value checks; if `false`, NULL values fail

**NULL Value Handling** (important for Spark version):
- Spark SQL `isin()`, `rlike()`, and comparisons return `NULL` (not `TRUE`/`FALSE`) when input is `NULL`
- The code handles this by using `col.isNull() | non_null_checks` pattern for nullable fields
- This ensures NULL values pass validation when `nullable: true`, avoiding false positives
- Reports display `NULL` (not `None`) for null values in error messages

**Value Check Rules** (Spark version only, validates target field values):
- `min_value` / `max_value` - Numeric range validation
- `allowed_values` - Enum validation (list of allowed values)
- `pattern` - Regex pattern validation
- `value_check_expr` - Custom Spark SQL expression
- `nullable` - If `true` (default), NULL values pass validation; if `false`, NULL values fail

### Spark Optimization (validator_spark.py)

- **Predicate pushdown**: Uses subquery `(SELECT ... WHERE filter) AS subq` to push filters to database
- **Parallel JDBC reads**: Partitions data using primary key ranges when `batch_size > 0`
- **Single FULL JOIN**: Optimized validation uses one `full_outer` join with aggregation instead of multiple passes
- **Caching**: Persists DataFrames before triggering actions
- **Oracle AS syntax**: Oracle doesn't support `AS` alias, handled with conditional `alias_prefix`

### Code Differences Between Versions

| Aspect | validator.py | validator_spark.py |
|--------|--------------|---------------------|
| Data loading | `execute_query()` returns list of dicts | `spark.read.jdbc()` returns DataFrame |
| Transform application | Python row-by-row | Spark SQL expressions (`col()`, `when()`, etc.) |
| Comparison logic | Python loops | Spark expressions + aggregation |
| Parallelism | `ThreadPoolExecutor` | Spark distributed execution |
| Join | In-memory dict indexing | DataFrame `full_outer` join |

## Configuration File Structure

### Single-Table Mode (Backward Compatible)

```json
{
  "version": "1.1",
  "source_db": { "type": "mysql|postgresql|oracle|sqlite", "host": "...", "database": "...", "user": "...", "password_env": "..." },
  "target_db": { ... },
  "tables": [
    {
      "source_table": "...",
      "target_table": "...",
      "source_schema": "public",  // Optional (PostgreSQL)
      "target_schema": "...",     // Optional
      "field_mappings": [
        {
          "target_field": "...",
          "source_field": "...",    // OR use transform
          "transform": { "type": "...", ... },
          "is_primary_key": true,
          "compare_rule": "exact|ignore_case|numeric_tolerance|skip",
          "tolerance": 0.01
        }
      ],
      "filters": { "source_filter": "deleted_at IS NULL", "target_filter": "..." },
      "sample_size": 10000  // 0 = full validation
    }
  ],
  "global_settings": {
    "parallel_workers": 4,
    "output_dir": "./reports",
    "report_format": ["json", "markdown"]
  }
}
```

### Multi-Table JOIN Mode (Spark version only)

```json
{
  "version": "1.1",
  "tables": [
    {
      "source_tables": [
        { "table_name": "orders", "alias": "o", "join_type": "primary" },
        { "table_name": "users", "alias": "u", "join_type": "left", "join_condition": "o.user_id = u.id" }
      ],
      "table_filters": { "o": "o.deleted_at IS NULL" },
      "target_table": "order_detail",
      "field_mappings": [
        { "target_field": "order_id", "source_field": "o.id", "is_primary_key": true },
        { "target_field": "customer", "source_field": "u.name", "nullable": true },
        { "target_field": "total", "transform": { "type": "math", "fields": ["o.price", "o.qty"], "expression": "o.price * o.qty" } }
      ]
    }
  ]
}
```

## Important Implementation Notes

- **Primary key handling**: If no `is_primary_key: true` fields are configured, all fields are used for joining
- **Field mapping**: Fields with transforms are computed from source data before comparison
- **Error sampling**: Only first 100 errors are stored in reports to prevent memory issues
- **Oracle column names**: Oracle returns column names uppercase, converted to lowercase in `execute_query()`
- **Report output**: Reports go to `./validation_reports/` by default (gitignored)
- **Spark 4.x compatibility**: Row objects in Spark 4.x don't support `get()` method; use `row[field]` with `__fields__` check instead
- **Column ambiguity**: After DataFrame join with alias, use `t.field_name` format to avoid ambiguity errors
- **NULL display**: Reports show `NULL` (not `None`) for null field values
- **Multi-table JOIN limitations**:
  - Not supported in single-machine version (`validator.py`)
  - Partition reads disabled for multi-table mode (no parallel JDBC)
  - Column name conflicts resolved with `alias_field` format (e.g., `o_id`, `u_name`)
- **Field alias resolution**:
  - `alias.field` in config → `alias_field` in DataFrame column
  - `field_alias_map` tracks this mapping for transform processing