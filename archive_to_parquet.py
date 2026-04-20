from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ARCHIVE_DIR = BASE_DIR / "archive"
DB_FILE = DATA_DIR / "stock_trades.db"


def main() -> int:
    try:
        import pandas as pd
    except ImportError:
        print("Missing dependency: pandas")
        print("Install with: pip install pandas pyarrow")
        return 1

    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return 1

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_dir = ARCHIVE_DIR / f"snapshot_{stamp}"
    target_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_FILE)
    try:
        trades_df = pd.read_sql_query("SELECT * FROM trades ORDER BY id ASC", conn)
        portfolio_df = pd.read_sql_query("SELECT * FROM portfolio ORDER BY symbol ASC", conn)
    finally:
        conn.close()

    trades_path = target_dir / "trades.parquet"
    portfolio_path = target_dir / "portfolio.parquet"

    try:
        trades_df.to_parquet(trades_path, index=False)
        portfolio_df.to_parquet(portfolio_path, index=False)
    except Exception as exc:
        print("Parquet export failed.")
        print(f"Reason: {exc}")
        print("Tip: ensure pyarrow is installed: pip install pyarrow")
        return 1

    print(f"Parquet archive created: {target_dir}")
    print(f"- {trades_path.name}: {len(trades_df)} rows")
    print(f"- {portfolio_path.name}: {len(portfolio_df)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
