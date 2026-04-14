# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Spark-based database migration data consistency validator tool. It uses PySpark for distributed computing and validates data consistency between source and target tables after database migration, supporting flexible field mappings with transforms and multiple comparison rules.

### Package Structure

| Module | Contents |
|--------|----------|
| `validator/__init__.py` | Spark conditional imports, logging setup |
| `validator/models.py` | Data classes: SourceTable, FieldMapping, TableMapping, ValidationResult, DebugConfig, DebugRecord |
| `validator/debug.py` | JsonlWriter, DebugExporter, `_fmt_value()` utility |
| `validator/core.py` | SparkDataValidator main class |
| `validator/__main__.py` | CLI entry point, `load_config()` |

## Running the Validator

### Install dependencies (uv)
```bash
uv sync
```

All dependencies are declared in `pyproject.toml`. `uv sync` creates `.venv` automatically.

### Run validation
```bash
uv run python -m validator mapping_example.json
uv run python -m validator mapping_example.json --master spark://localhost:7077
```

### Web server (submodule)
```bash
git submodule update --init
cd web && uv sync && uv run python app.py
# Serves at http://127.0.0.1:5000
```

## Architecture

### Core Components

**Data Classes** (`validator/models.py`):
- `SourceTable`: Defines a source table in multi-table JOIN (table_name, alias, join_type, join_condition)
- `FieldMapping`: Defines how a target field maps from source (direct field or transform)
- `TableMapping`: Contains source/target table info, field mappings, filters; supports single-table and multi-table modes
- `ValidationResult`: Stores validation outcomes (matched, mismatched, missing counts)

**Multi-Table JOIN Support**:
- `source_tables` array defines multiple source tables with aliases
- `table_filters` defines per-table WHERE conditions
- Field references use `alias.field` format (e.g., `o.id`, `u.name`)
- `is_multi_source()` method on `TableMapping` determines mode

### Validation Flow

1. **Load config** - JSON mapping file with source_db, target_db, tables array
2. **Connect databases** - Establish JDBC connections via Spark
3. **For each table mapping**:
   - Validate configuration (alias uniqueness, JOIN conditions, field references)
   - Build source query:
     - Single-table: `(SELECT fields FROM table WHERE filter) AS subq`
     - Multi-table: `(SELECT alias_fields FROM table1 alias1 JOIN_TYPE table2 alias2 ON condition WHERE filters) subq`
   - Build target query
   - Load data into DataFrame
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

**Value Check Rules** (validate target field values):
- `min_value` / `max_value` - Numeric range validation
- `allowed_values` - Enum validation (list of allowed values)
- `pattern` - Regex pattern validation
- `value_check_expr` - Custom Spark SQL expression
- `nullable` - If `true` (default), NULL values pass all value checks; if `false`, NULL values fail

**NULL Value Handling**:
- Spark SQL `isin()`, `rlike()`, and comparisons return `NULL` (not `TRUE`/`FALSE`) when input is `NULL`
- The code handles this by using `col.isNull() | non_null_checks` pattern for nullable fields
- This ensures NULL values pass validation when `nullable: true`, avoiding false positives
- Reports display `NULL` (not `None`) for null values in error messages

### Spark Optimization (`validator/core.py`)

- **Predicate pushdown**: Uses subquery `(SELECT ... WHERE filter) AS subq` to push filters to database
- **Parallel JDBC reads**: Partitions data using primary key ranges when `batch_size > 0`
- **Single FULL JOIN**: Optimized validation uses one `full_outer` join with aggregation instead of multiple passes
- **Caching**: Persists DataFrames before triggering actions
- **Oracle AS syntax**: Oracle doesn't support `AS` alias, handled with conditional `alias_prefix`

## Web Server Architecture (`web/` submodule)

Separate repository: https://github.com/gzhdev/db-migration-validator-web.git (added as git submodule)

Flask-based web application with two features:

| Feature | Route | File |
|---------|-------|------|
| Debug log viewer | `GET /` | `templates/index.html` |
| Mapping config generator | `GET /mapping-generator` | `templates/mapping_generator.html` |

**Routes:**
- `GET /` — List imported validation runs
- `POST /import` — Import a debug directory
- `GET /runs/<id>` — Run detail page
- `GET /runs/<id>/records` — Record browser (AJAX)
- `GET /mapping-generator` — Visual mapping config editor
- `GET /api/csv-template` — Download CSV template for mapping generator
- `GET /api/runs/<id>/records` — JSON API for records
- `DELETE /runs/<id>` — Delete a run

**Mapping Generator** (`web/static/js/mapping_generator.js`):
- Visual editor for DB connections, table mappings, field mappings
- CSV import: upload field mapping spreadsheet → auto-populate editor
- Supports single-table and multi-table JOIN modes
- JSON preview, download, and load-from-existing-JSON
- All logic is client-side; no server round-trip needed to generate JSON

**Database:** SQLite (`web/debug_viewer.db`, auto-created). Schema: `runs`, `table_summaries`, `records`.

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

### Multi-Table JOIN Mode

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

### Conditional UNION Pattern (multi-source target table)

When target table rows come from **different source tables depending on conditions**, split into multiple table mapping entries — each with mutually exclusive `source_filter` / `target_filter`. The validator runs each entry independently.

```json
{
  "tables": [
    {
      "description": "条件1: 数据来自A表",
      "source_table": "table_a",
      "target_table": "target",
      "filters": { "source_filter": "type = 'X'", "target_filter": "type = 'X'" },
      "field_mappings": [...]
    },
    {
      "description": "条件2: 数据来自A JOIN B",
      "source_tables": [
        { "table_name": "table_a", "alias": "a", "join_type": "primary" },
        { "table_name": "table_b", "alias": "b", "join_type": "left", "join_condition": "a.id = b.ref_id" }
      ],
      "table_filters": { "a": "a.type != 'X' AND a.category = 'Y'" },
      "target_table": "target",
      "filters": { "target_filter": "type != 'X' AND category = 'Y'" },
      "field_mappings": [...]
    }
  ]
}
```

Alternatively, create a database view that encapsulates the UNION logic and use it as `source_table`.

## Important Implementation Notes

- **Primary key handling**: If no `is_primary_key: true` fields are configured, all fields are used for joining
- **Field mapping**: Fields with transforms are computed from source data before comparison
- **Error sampling**: Only first 100 errors are stored in reports to prevent memory issues
- **Report output**: Reports go to `./validation_reports/` by default (gitignored)
- **Spark 4.x compatibility**: Row objects in Spark 4.x don't support `get()` method; use `row[field]` with `__fields__` check instead
- **Column ambiguity**: After DataFrame join with alias, use `t.field_name` format to avoid ambiguity errors
- **NULL display**: Reports show `NULL` (not `None`) for null field values
- **Multi-table JOIN limitations**:
  - Partition reads disabled for multi-table mode (no parallel JDBC)
  - Column name conflicts resolved with `alias_field` format (e.g., `o_id`, `u_name`)
- **Field alias resolution**:
  - `alias.field` in config → `alias_field` in DataFrame column
  - `field_alias_map` tracks this mapping for transform processing

<!-- rtk-instructions v2 -->
# RTK (Rust Token Killer) - Token-Optimized Commands

## Golden Rule

**Always prefix commands with `rtk`**. If RTK has a dedicated filter, it uses it. If not, it passes through unchanged. This means RTK is always safe to use.

**Important**: Even in command chains with `&&`, use `rtk`:
```bash
# ❌ Wrong
git add . && git commit -m "msg" && git push

# ✅ Correct
rtk git add . && rtk git commit -m "msg" && rtk git push
```

## RTK Commands by Workflow

### Build & Compile (80-90% savings)
```bash
rtk cargo build         # Cargo build output
rtk cargo check         # Cargo check output
rtk cargo clippy        # Clippy warnings grouped by file (80%)
rtk tsc                 # TypeScript errors grouped by file/code (83%)
rtk lint                # ESLint/Biome violations grouped (84%)
rtk prettier --check    # Files needing format only (70%)
rtk next build          # Next.js build with route metrics (87%)
```

### Test (90-99% savings)
```bash
rtk cargo test          # Cargo test failures only (90%)
rtk vitest run          # Vitest failures only (99.5%)
rtk playwright test     # Playwright failures only (94%)
rtk test <cmd>          # Generic test wrapper - failures only
```

### Git (59-80% savings)
```bash
rtk git status          # Compact status
rtk git log             # Compact log (works with all git flags)
rtk git diff            # Compact diff (80%)
rtk git show            # Compact show (80%)
rtk git add             # Ultra-compact confirmations (59%)
rtk git commit          # Ultra-compact confirmations (59%)
rtk git push            # Ultra-compact confirmations
rtk git pull            # Ultra-compact confirmations
rtk git branch          # Compact branch list
rtk git fetch           # Compact fetch
rtk git stash           # Compact stash
rtk git worktree        # Compact worktree
```

Note: Git passthrough works for ALL subcommands, even those not explicitly listed.

### GitHub (26-87% savings)
```bash
rtk gh pr view <num>    # Compact PR view (87%)
rtk gh pr checks        # Compact PR checks (79%)
rtk gh run list         # Compact workflow runs (82%)
rtk gh issue list       # Compact issue list (80%)
rtk gh api              # Compact API responses (26%)
```

### JavaScript/TypeScript Tooling (70-90% savings)
```bash
rtk pnpm list           # Compact dependency tree (70%)
rtk pnpm outdated       # Compact outdated packages (80%)
rtk pnpm install        # Compact install output (90%)
rtk npm run <script>    # Compact npm script output
rtk npx <cmd>           # Compact npx command output
rtk prisma              # Prisma without ASCII art (88%)
```

### Files & Search (60-75% savings)
```bash
rtk ls <path>           # Tree format, compact (65%)
rtk read <file>         # Code reading with filtering (60%)
rtk grep <pattern>      # Search grouped by file (75%)
rtk find <pattern>      # Find grouped by directory (70%)
```

### Analysis & Debug (70-90% savings)
```bash
rtk err <cmd>           # Filter errors only from any command
rtk log <file>          # Deduplicated logs with counts
rtk json <file>         # JSON structure without values
rtk deps                # Dependency overview
rtk env                 # Environment variables compact
rtk summary <cmd>       # Smart summary of command output
rtk diff                # Ultra-compact diffs
```

### Infrastructure (85% savings)
```bash
rtk docker ps           # Compact container list
rtk docker images       # Compact image list
rtk docker logs <c>     # Deduplicated logs
rtk kubectl get         # Compact resource list
rtk kubectl logs        # Deduplicated pod logs
```

### Network (65-70% savings)
```bash
rtk curl <url>          # Compact HTTP responses (70%)
rtk wget <url>          # Compact download output (65%)
```

### Meta Commands
```bash
rtk gain                # View token savings statistics
rtk gain --history      # View command history with savings
rtk discover            # Analyze Claude Code sessions for missed RTK usage
rtk proxy <cmd>         # Run command without filtering (for debugging)
rtk init                # Add RTK instructions to CLAUDE.md
rtk init --global       # Add RTK to ~/.claude/CLAUDE.md
```

## Token Savings Overview

| Category | Commands | Typical Savings |
|----------|----------|-----------------|
| Tests | vitest, playwright, cargo test | 90-99% |
| Build | next, tsc, lint, prettier | 70-87% |
| Git | status, log, diff, add, commit | 59-80% |
| GitHub | gh pr, gh run, gh issue | 26-87% |
| Package Managers | pnpm, npm, npx | 70-90% |
| Files | ls, read, grep, find | 60-75% |
| Infrastructure | docker, kubectl | 85% |
| Network | curl, wget | 65-70% |

Overall average: **60-90% token reduction** on common development operations.
<!-- /rtk-instructions -->