import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "debug_viewer.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    debug_dir     TEXT NOT NULL,
    imported_at   TEXT NOT NULL,
    table_count   INTEGER NOT NULL DEFAULT 0,
    total_records INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS table_summaries (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id             INTEGER NOT NULL REFERENCES runs(id),
    table_name         TEXT NOT NULL,
    total_rows         INTEGER DEFAULT 0,
    matched_rows       INTEGER DEFAULT 0,
    mismatched_rows    INTEGER DEFAULT 0,
    missing_in_source  INTEGER DEFAULT 0,
    missing_in_target  INTEGER DEFAULT 0,
    value_check_failed INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ts_run_id ON table_summaries(run_id);

CREATE TABLE IF NOT EXISTS records (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id               INTEGER NOT NULL REFERENCES runs(id),
    table_name           TEXT NOT NULL,
    record_id            TEXT NOT NULL,
    match_type           TEXT NOT NULL,
    primary_key          TEXT NOT NULL,
    raw_source           TEXT,
    expected_values      TEXT,
    target_values        TEXT,
    comparison_result    TEXT,
    value_check_failures TEXT
);
CREATE INDEX IF NOT EXISTS idx_rec_run_table ON records(run_id, table_name);
CREATE INDEX IF NOT EXISTS idx_rec_match_type ON records(match_type);
"""


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def db_conn():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with db_conn() as conn:
        conn.executescript(SCHEMA)


# ---------------------------------------------------------------------------
# Run CRUD
# ---------------------------------------------------------------------------

def create_run(debug_dir: str, imported_at: str) -> int:
    with db_conn() as conn:
        cur = conn.execute(
            "INSERT INTO runs (debug_dir, imported_at, status) VALUES (?, ?, 'pending')",
            (debug_dir, imported_at),
        )
        return cur.lastrowid


def update_run(run_id: int, table_count: int, total_records: int, status: str):
    with db_conn() as conn:
        conn.execute(
            "UPDATE runs SET table_count=?, total_records=?, status=? WHERE id=?",
            (table_count, total_records, status, run_id),
        )


def get_run(run_id: int):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None


def list_runs():
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM runs ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]


def delete_run(run_id: int):
    with db_conn() as conn:
        conn.execute("DELETE FROM records WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM table_summaries WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM runs WHERE id=?", (run_id,))


# ---------------------------------------------------------------------------
# Table summaries CRUD
# ---------------------------------------------------------------------------

def insert_table_summary(run_id: int, table_name: str, stats: dict):
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO table_summaries
               (run_id, table_name, total_rows, matched_rows, mismatched_rows,
                missing_in_source, missing_in_target, value_check_failed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                table_name,
                stats.get("total_rows", 0),
                stats.get("matched_rows", 0),
                stats.get("mismatched_rows", 0),
                stats.get("missing_in_source", 0),
                stats.get("missing_in_target", 0),
                stats.get("value_check_failed", 0),
            ),
        )


def get_table_summaries(run_id: int):
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM table_summaries WHERE run_id=? ORDER BY table_name",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Records CRUD
# ---------------------------------------------------------------------------

def insert_records_batch(rows: list):
    """Bulk-insert a list of record dicts."""
    with db_conn() as conn:
        conn.executemany(
            """INSERT INTO records
               (run_id, table_name, record_id, match_type, primary_key,
                raw_source, expected_values, target_values,
                comparison_result, value_check_failures)
               VALUES (:run_id, :table_name, :record_id, :match_type, :primary_key,
                       :raw_source, :expected_values, :target_values,
                       :comparison_result, :value_check_failures)""",
            rows,
        )


def query_records(run_id: int, table_name: str = None, match_type: str = None,
                  pk_search: str = None, page: int = 1, page_size: int = 20) -> dict:
    conditions = ["run_id = ?"]
    params = [run_id]

    if table_name:
        conditions.append("table_name = ?")
        params.append(table_name)
    if match_type:
        conditions.append("match_type = ?")
        params.append(match_type)
    if pk_search:
        conditions.append("primary_key LIKE ?")
        params.append(f"%{pk_search}%")

    where = " AND ".join(conditions)
    offset = (page - 1) * page_size

    with db_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM records WHERE {where}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"SELECT * FROM records WHERE {where} ORDER BY id LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()

    import json
    records = []
    for r in rows:
        rec = dict(r)
        for field in ("primary_key", "raw_source", "expected_values",
                      "target_values", "comparison_result", "value_check_failures"):
            if rec.get(field):
                try:
                    rec[field] = json.loads(rec[field])
                except (ValueError, TypeError):
                    pass
        records.append(rec)

    import math
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": math.ceil(total / page_size) if total else 0,
        "records": records,
    }


def get_table_names(run_id: int):
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT table_name FROM records WHERE run_id=? ORDER BY table_name",
            (run_id,),
        ).fetchall()
        return [r[0] for r in rows]
