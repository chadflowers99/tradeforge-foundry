from __future__ import annotations

import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_FILE = DATA_DIR / "stock_trades.db"

REQUIRED_SCHEMA = {
    "trades": {
        "id",
        "symbol",
        "action",
        "quantity",
        "price",
        "timestamp",
        "realized_pl",
        "notes",
        "row_hash",
    },
    "portfolio": {
        "symbol",
        "quantity",
        "avg_price",
        "last_updated",
    },
    "raw_trades": {
        "id",
        "batch_id",
        "ingested_at",
        "source_file",
        "row_num",
        "raw_payload",
    },
    "ingest_manifest": {
        "run_id",
        "timestamp",
        "source_file",
        "rows_ingested",
        "rows_rejected",
        "status",
        "rows_written",
        "batch_id",
    },
    "ingest_rejections": {
        "rejection_id",
        "run_id",
        "row_index",
        "error_message",
        "raw_data_fragment",
    },
}


def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _table_columns(cursor: sqlite3.Cursor, table_name: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {str(row[1]) for row in cursor.fetchall()}


def print_recent_rejection_summary(cursor: sqlite3.Cursor) -> None:
    if not _table_exists(cursor, "ingest_manifest"):
        return

    cursor.execute(
        """
        SELECT run_id, timestamp, source_file, rows_ingested, rows_written, rows_rejected, status
        FROM ingest_manifest
        ORDER BY timestamp DESC
        LIMIT 5
        """
    )
    recent_runs = cursor.fetchall()

    if not recent_runs:
        print("Recent ingest diagnostics: no manifest runs found.")
        return

    print("Recent ingest diagnostics (last 5 runs):")
    total_ingested = 0
    total_written = 0
    total_rejected = 0
    for run_id, ts, source_file, rows_ingested, rows_written, rows_rejected, status in recent_runs:
        total_ingested += int(rows_ingested)
        total_written += int(rows_written)
        total_rejected += int(rows_rejected)
        print(
            f"- run_id={run_id} | ts={ts} | source={source_file} | "
            f"ingested={rows_ingested} | written={rows_written} | rejected={rows_rejected} | status={status}"
        )

    if total_ingested > 0:
        accepted_pct = (total_written / total_ingested) * 100.0
        rejected_pct = (total_rejected / total_ingested) * 100.0
    else:
        accepted_pct = 0.0
        rejected_pct = 0.0
    print(
        "Data health score (last 5 runs): "
        f"accepted={accepted_pct:.2f}% | rejected={rejected_pct:.2f}% | "
        f"ingested={total_ingested}"
    )

    if not _table_exists(cursor, "ingest_rejections"):
        return

    run_ids = [run[0] for run in recent_runs]
    placeholders = ",".join("?" for _ in run_ids)
    cursor.execute(
        f"""
        SELECT run_id, row_index, error_message
        FROM ingest_rejections
        WHERE run_id IN ({placeholders})
        ORDER BY rejection_id DESC
        LIMIT 10
        """,
        run_ids,
    )
    recent_rejections = cursor.fetchall()

    if not recent_rejections:
        print("Recent ingest diagnostics: no rejections recorded in the last 5 runs.")
        return

    print("Recent rejection samples (up to 10):")
    for run_id, row_index, error_message in recent_rejections:
        print(f"- run_id={run_id} | row_index={row_index} | reason={error_message}")


def validate_schema(db_file: Path = DB_FILE) -> tuple[bool, list[str]]:
    if not db_file.exists():
        return False, [f"Database not found: {db_file}"]

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    issues: list[str] = []

    for table_name, expected_columns in REQUIRED_SCHEMA.items():
        if not _table_exists(c, table_name):
            issues.append(f"Missing required table: {table_name}")
            continue

        actual_columns = _table_columns(c, table_name)
        missing_columns = expected_columns.difference(actual_columns)
        if missing_columns:
            missing_sorted = ", ".join(sorted(missing_columns))
            issues.append(f"Table '{table_name}' is missing columns: {missing_sorted}")

    # Basic quality checks that catch common data integrity drift.
    if _table_exists(c, "trades"):
        c.execute(
            """
            SELECT COUNT(*)
            FROM trades
            WHERE symbol IS NULL
               OR TRIM(symbol) = ''
               OR action NOT IN ('buy', 'sell')
               OR quantity IS NULL
               OR quantity <= 0
               OR price IS NULL
               OR price < 0
            """
        )
        bad_trade_rows = int(c.fetchone()[0])
        if bad_trade_rows > 0:
            issues.append(f"Found {bad_trade_rows} invalid rows in trades table")

        c.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT row_hash
                FROM trades
                WHERE row_hash IS NOT NULL
                GROUP BY row_hash
                HAVING COUNT(*) > 1
            )
            """
        )
        duplicate_hashes = int(c.fetchone()[0])
        if duplicate_hashes > 0:
            issues.append(f"Found {duplicate_hashes} duplicate row_hash values in trades table")

    if _table_exists(c, "ingest_manifest"):
        c.execute(
            """
            SELECT COUNT(*)
            FROM ingest_manifest
            WHERE run_id IS NULL
               OR TRIM(run_id) = ''
               OR source_file IS NULL
               OR TRIM(source_file) = ''
               OR rows_ingested < 0
               OR rows_rejected < 0
               OR rows_written < 0
               OR status NOT IN ('success', 'partial_success', 'failed')
            """
        )
        bad_manifest_rows = int(c.fetchone()[0])
        if bad_manifest_rows > 0:
            issues.append(f"Found {bad_manifest_rows} invalid rows in ingest_manifest table")

    if _table_exists(c, "ingest_rejections"):
        c.execute(
            """
            SELECT COUNT(*)
            FROM ingest_rejections
            WHERE run_id IS NULL
               OR TRIM(run_id) = ''
               OR row_index < 1
               OR error_message IS NULL
               OR TRIM(error_message) = ''
               OR raw_data_fragment IS NULL
               OR TRIM(raw_data_fragment) = ''
            """
        )
        bad_rejection_rows = int(c.fetchone()[0])
        if bad_rejection_rows > 0:
            issues.append(f"Found {bad_rejection_rows} invalid rows in ingest_rejections table")

    print_recent_rejection_summary(c)
    conn.close()
    return len(issues) == 0, issues


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ok, issues = validate_schema(DB_FILE)

    if ok:
        print("Integrity check passed.")
        return 0

    print("Integrity check failed:")
    for issue in issues:
        print(f"- {issue}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
