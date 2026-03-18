import json
import logging
from datetime import datetime
from pathlib import Path

from db import (
    create_run,
    update_run,
    insert_table_summary,
    insert_records_batch,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def import_debug_dir(debug_dir: str) -> int:
    """
    Scan *debug_dir*, import all table sub-directories into SQLite.

    Each sub-directory is expected to contain:
      - summary.json   – validation statistics for that table
      - records.jsonl  – one JSON object per line

    Returns the new run_id.
    Raises ValueError if the directory does not exist.
    """
    base = Path(debug_dir)
    if not base.exists() or not base.is_dir():
        raise ValueError(f"目录不存在: {debug_dir}")

    imported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_id = create_run(str(base.resolve()), imported_at)

    table_count = 0
    total_records = 0

    try:
        for table_dir in sorted(base.iterdir()):
            if not table_dir.is_dir():
                continue

            table_name = table_dir.name
            summary_path = table_dir / "summary.json"
            jsonl_path = table_dir / "records.jsonl"

            # --- summary.json ---
            stats = {}
            if summary_path.exists():
                try:
                    with open(summary_path, encoding="utf-8") as f:
                        stats = json.load(f)
                except Exception as e:
                    logger.warning(f"读取 summary.json 失败 [{table_name}]: {e}")

            insert_table_summary(run_id, table_name, stats)
            table_count += 1

            # --- records.jsonl ---
            if not jsonl_path.exists():
                logger.warning(f"未找到 records.jsonl [{table_name}]，跳过")
                continue

            batch = []
            file_records = 0
            with open(jsonl_path, encoding="utf-8") as f:
                for lineno, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON 解析失败 [{table_name}] 第 {lineno} 行: {e}")
                        continue

                    batch.append(_build_record_row(run_id, table_name, obj))
                    file_records += 1

                    if len(batch) >= BATCH_SIZE:
                        insert_records_batch(batch)
                        batch = []

            if batch:
                insert_records_batch(batch)

            total_records += file_records
            logger.info(f"导入表 [{table_name}]: {file_records} 条记录")

        update_run(run_id, table_count, total_records, "done")
        logger.info(f"导入完成: run_id={run_id}, 表数={table_count}, 记录数={total_records}")

    except Exception as e:
        update_run(run_id, table_count, total_records, "error")
        logger.error(f"导入失败: {e}")
        raise

    return run_id


def _build_record_row(run_id: int, table_name: str, obj: dict) -> dict:
    """Convert a JSONL record dict to a DB row dict."""
    return {
        "run_id": run_id,
        "table_name": table_name,
        "record_id": obj.get("record_id", ""),
        "match_type": obj.get("match_type", "unknown"),
        "primary_key": json.dumps(obj.get("primary_key", {}), ensure_ascii=False, default=str),
        "raw_source": json.dumps(obj.get("raw_source"), ensure_ascii=False, default=str) if obj.get("raw_source") is not None else None,
        "expected_values": json.dumps(obj.get("expected_values"), ensure_ascii=False, default=str) if obj.get("expected_values") is not None else None,
        "target_values": json.dumps(obj.get("target_values"), ensure_ascii=False, default=str) if obj.get("target_values") is not None else None,
        "comparison_result": json.dumps(obj.get("comparison_result"), ensure_ascii=False, default=str) if obj.get("comparison_result") else None,
        "value_check_failures": json.dumps(obj.get("value_check_failures"), ensure_ascii=False, default=str) if obj.get("value_check_failures") else None,
    }
