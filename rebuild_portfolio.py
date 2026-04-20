from __future__ import annotations

import sqlite3
from pathlib import Path
from shutil import copy2
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_FILE = DATA_DIR / "stock_trades.db"


def rebuild_portfolio_from_trades() -> tuple[int, int]:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute(
        """
        SELECT id, symbol, action, quantity, price, timestamp
        FROM trades
        ORDER BY id ASC
        """
    )
    trades = c.fetchall()

    state: dict[str, dict[str, float | int | str]] = {}

    for _trade_id, symbol, action, qty, price, ts in trades:
        if not symbol or not action:
            continue

        symbol = symbol.strip().upper()
        qty = int(qty)
        price = float(price)

        lot = state.get(symbol, {"quantity": 0, "avg_price": 0.0, "last_updated": ts or ""})
        current_qty = int(lot["quantity"])
        current_avg = float(lot["avg_price"])

        if action == "buy":
            new_qty = current_qty + qty
            if new_qty > 0:
                new_avg = ((current_qty * current_avg) + (qty * price)) / new_qty
            else:
                new_avg = 0.0
            state[symbol] = {"quantity": new_qty, "avg_price": new_avg, "last_updated": ts or ""}
        elif action == "sell":
            new_qty = current_qty - qty
            if new_qty > 0:
                state[symbol] = {"quantity": new_qty, "avg_price": current_avg, "last_updated": ts or ""}
            else:
                state.pop(symbol, None)

    c.execute("SELECT COUNT(*) FROM portfolio")
    old_rows = c.fetchone()[0]

    c.execute("DELETE FROM portfolio")
    rows_to_insert = [
        (symbol, int(values["quantity"]), float(values["avg_price"]), str(values["last_updated"]))
        for symbol, values in sorted(state.items())
        if int(values["quantity"]) > 0
    ]

    c.executemany(
        "INSERT INTO portfolio (symbol, quantity, avg_price, last_updated) VALUES (?, ?, ?, ?)",
        rows_to_insert,
    )

    conn.commit()
    conn.close()

    return old_rows, len(rows_to_insert)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not DB_FILE.exists():
        raise FileNotFoundError(f"Database not found: {DB_FILE}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = DB_FILE.with_name(f"stock_trades_backup_{timestamp}.db")
    copy2(DB_FILE, backup_file)

    old_count, new_count = rebuild_portfolio_from_trades()

    print(f"Backup created: {backup_file.name}")
    print(f"Portfolio rows before rebuild: {old_count}")
    print(f"Portfolio rows after rebuild:  {new_count}")
    print("Portfolio rebuild complete.")


if __name__ == "__main__":
    main()
