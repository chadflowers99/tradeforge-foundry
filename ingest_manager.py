from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_FILE = DATA_DIR / "stock_trades.db"

SYMBOL_ALIASES = {
    "BITCOIN": "BTC-USD",
    "BTCUSD": "BTC-USD",
    "BTC-USD": "BTC-USD",
}

ACTION_ALIASES = {
    "BUY": "buy",
    "B": "buy",
    "COVER": "buy",
    "SELL": "sell",
    "S": "sell",
    "SHORT": "sell",
}

TIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def ensure_schema(conn: sqlite3.Connection) -> None:
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            quantity INTEGER,
            price REAL,
            timestamp TEXT,
            realized_pl REAL,
            notes TEXT,
            row_hash TEXT
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio (
            symbol TEXT PRIMARY KEY,
            quantity INTEGER,
            avg_price REAL,
            last_updated TEXT
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            source_file TEXT NOT NULL,
            row_num INTEGER NOT NULL,
            raw_payload TEXT NOT NULL
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_manifest (
            run_id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            source_file TEXT NOT NULL,
            rows_ingested INTEGER NOT NULL,
            rows_rejected INTEGER NOT NULL,
            status TEXT NOT NULL,
            rows_written INTEGER NOT NULL,
            batch_id TEXT NOT NULL
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_rejections (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            error_message TEXT NOT NULL,
            raw_data_fragment TEXT NOT NULL
        )
        """
    )

    c.execute("PRAGMA table_info(trades)")
    columns = {row[1] for row in c.fetchall()}
    if "row_hash" not in columns:
        c.execute("ALTER TABLE trades ADD COLUMN row_hash TEXT")

    c.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_row_hash ON trades(row_hash)"
    )

    conn.commit()


def log_rejection(
    cursor: sqlite3.Cursor,
    run_id: str,
    row_index: int,
    error_message: str,
    raw_data_fragment: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO ingest_rejections (run_id, row_index, error_message, raw_data_fragment)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, row_index, error_message, raw_data_fragment[:2000]),
    )


def normalize_symbol(raw_symbol: str) -> str:
    candidate = raw_symbol.strip().upper()
    return SYMBOL_ALIASES.get(candidate, candidate)


def normalize_action(raw_action: str) -> str | None:
    candidate = raw_action.strip().upper()
    return ACTION_ALIASES.get(candidate)


def normalize_quantity(raw_quantity: str) -> int | None:
    try:
        quantity = int(str(raw_quantity).replace(",", "").strip())
        if quantity <= 0:
            return None
        return quantity
    except (TypeError, ValueError):
        return None


def normalize_price(raw_price: str) -> float | None:
    try:
        price = float(str(raw_price).replace("$", "").replace(",", "").strip())
        if price < 0:
            return None
        return price
    except (TypeError, ValueError):
        return None


def normalize_timestamp(raw_timestamp: str) -> str | None:
    candidate = str(raw_timestamp or "").strip()
    if not candidate:
        return None

    iso_candidate = candidate.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_candidate)
        return dt.isoformat()
    except ValueError:
        pass

    for fmt in TIME_FORMATS:
        try:
            dt = datetime.strptime(candidate, fmt)
            return dt.isoformat()
        except ValueError:
            continue

    return None


def canonical_row_hash(
    symbol: str,
    action: str,
    quantity: int,
    price: float,
    timestamp: str,
    notes: str,
) -> str:
    canonical = "|".join(
        [
            symbol,
            action,
            str(quantity),
            f"{price:.8f}",
            timestamp,
            notes,
        ]
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _get_first(row: dict[str, str], keys: list[str]) -> str:
    for key in keys:
        if key in row and row[key] is not None:
            return str(row[key])
    return ""


def parse_clean_row(row: dict[str, str]) -> tuple[dict[str, object] | None, str | None]:
    raw_symbol = _get_first(row, ["symbol", "ticker", "asset", "instrument"])
    raw_action = _get_first(row, ["action", "side", "trade_type"])
    raw_quantity = _get_first(row, ["quantity", "qty", "shares", "size"])
    raw_price = _get_first(row, ["price", "fill_price", "execution_price", "cost"])
    raw_timestamp = _get_first(row, ["timestamp", "time", "datetime", "trade_time", "date"])
    notes = _get_first(row, ["notes", "note", "comment", "memo"]).strip()

    symbol = normalize_symbol(raw_symbol)
    action = normalize_action(raw_action)
    quantity = normalize_quantity(raw_quantity)
    price = normalize_price(raw_price)
    timestamp = normalize_timestamp(raw_timestamp)

    if not symbol:
        return None, "missing symbol"
    if action is None:
        return None, "invalid action"
    if quantity is None:
        return None, "invalid quantity"
    if price is None:
        return None, "invalid price"
    if timestamp is None:
        return None, "invalid timestamp"

    row_hash = canonical_row_hash(symbol, action, quantity, price, timestamp, notes)

    return {
        "symbol": symbol,
        "action": action,
        "quantity": quantity,
        "price": price,
        "timestamp": timestamp,
        "notes": notes,
        "row_hash": row_hash,
    }, None


def ingest_csv(source_file: Path) -> int:
    if not source_file.exists():
        print(f"Source file not found: {source_file}")
        return 1

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    batch_id = str(uuid.uuid4())
    run_timestamp = _now_iso()

    rows_ingested = 0
    rows_rejected = 0
    rows_written = 0

    conn = sqlite3.connect(DB_FILE)
    try:
        ensure_schema(conn)
        c = conn.cursor()

        with source_file.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                print("CSV has no header row.")
                return 1

            for row_num, row in enumerate(reader, start=2):
                rows_ingested += 1

                try:
                    raw_payload = json.dumps(row, ensure_ascii=True, sort_keys=True)
                    c.execute(
                        """
                        INSERT INTO raw_trades (batch_id, ingested_at, source_file, row_num, raw_payload)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (batch_id, run_timestamp, source_file.name, row_num, raw_payload),
                    )

                    clean_row, error = parse_clean_row(row)
                    if error is not None:
                        rows_rejected += 1
                        log_rejection(c, run_id, row_num, error, raw_payload)
                        continue

                    c.execute(
                        """
                        INSERT OR IGNORE INTO trades (symbol, action, quantity, price, timestamp, notes, row_hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            clean_row["symbol"],
                            clean_row["action"],
                            clean_row["quantity"],
                            clean_row["price"],
                            clean_row["timestamp"],
                            clean_row["notes"],
                            clean_row["row_hash"],
                        ),
                    )

                    if c.rowcount == 1:
                        rows_written += 1
                    else:
                        rows_rejected += 1
                        log_rejection(c, run_id, row_num, "duplicate_row_hash", raw_payload)
                except Exception as row_exc:
                    rows_rejected += 1
                    fallback_payload = json.dumps(row, ensure_ascii=True, sort_keys=True)
                    log_rejection(c, run_id, row_num, f"unexpected_row_error: {row_exc}", fallback_payload)

        status = "success" if rows_rejected == 0 else "partial_success"
        c.execute(
            """
            INSERT INTO ingest_manifest (
                run_id, timestamp, source_file, rows_ingested, rows_rejected, status, rows_written, batch_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                run_timestamp,
                source_file.name,
                rows_ingested,
                rows_rejected,
                status,
                rows_written,
                batch_id,
            ),
        )

        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"Ingest failed: {exc}")
        return 1
    finally:
        conn.close()

    print("Ingest complete.")
    print(f"run_id: {run_id}")
    print(f"batch_id: {batch_id}")
    print(f"rows_ingested: {rows_ingested}")
    print(f"rows_written: {rows_written}")
    print(f"rows_rejected: {rows_rejected}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest CSV data into raw_trades (append-only) and trades (clean validated)."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to source CSV file",
    )
    args = parser.parse_args()

    return ingest_csv(Path(args.source).resolve())


if __name__ == "__main__":
    raise SystemExit(main())
