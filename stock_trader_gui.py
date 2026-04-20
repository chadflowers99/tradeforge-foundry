# -*- coding: utf-8 -*-
"""
Created on Wed Jan 14 13:20:58 2026

@author: busin
"""

# stock_trader_gui.py

import tkinter as tk
from tkinter import messagebox
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
DB_FILE = DATA_DIR / "stock_trades.db"


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Trade log
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT, -- 'buy' or 'sell'
            quantity INTEGER,
            price REAL,
            timestamp TEXT,
            realized_pl REAL,
            notes TEXT,
            row_hash TEXT
        )
    """)

    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_row_hash ON trades(row_hash)")

    c.execute("PRAGMA table_info(trades)")
    trade_columns = {row[1] for row in c.fetchall()}
    if "row_hash" not in trade_columns:
        c.execute("ALTER TABLE trades ADD COLUMN row_hash TEXT")

    # Portfolio tracker
    c.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            symbol TEXT PRIMARY KEY,
            quantity INTEGER,
            avg_price REAL,
            last_updated TEXT
        )
    """)

    # Append-only raw ingest landing zone
    c.execute("""
        CREATE TABLE IF NOT EXISTS raw_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            source_file TEXT NOT NULL,
            row_num INTEGER NOT NULL,
            raw_payload TEXT NOT NULL
        )
    """)

    # Pipeline execution manifest
    c.execute("""
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
    """)

    # Row-level ingest diagnostics for rejected input records
    c.execute("""
        CREATE TABLE IF NOT EXISTS ingest_rejections (
            rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            error_message TEXT NOT NULL,
            raw_data_fragment TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def compute_fifo_realized_pl(symbol, sell_qty, sell_price, trade_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Get all BUY lots before this trade
    c.execute("""
        SELECT quantity, price
        FROM trades
        WHERE symbol = ? AND action = 'buy' AND id < ?
        ORDER BY id ASC
    """, (symbol, trade_id))
    buys = c.fetchall()

    # Get all SELL trades before this one
    c.execute("""
        SELECT quantity
        FROM trades
        WHERE symbol = ? AND action = 'sell' AND id < ?
        ORDER BY id ASC
    """, (symbol, trade_id))
    sells = c.fetchall()

    conn.close()

    # Convert buys to mutable list
    buy_lots = [[qty, price] for qty, price in buys]
    total_sold_before = sum(qty for qty, in sells)

    # Consume previous sells FIFO
    remaining_to_remove = total_sold_before
    for lot in buy_lots:
        if remaining_to_remove <= 0:
            break
        if lot[0] <= remaining_to_remove:
            remaining_to_remove -= lot[0]
            lot[0] = 0
        else:
            lot[0] -= remaining_to_remove
            remaining_to_remove = 0

    # Compute realized P/L for THIS sell
    remaining_to_sell = sell_qty
    realized_pl = 0.0

    for lot_qty, lot_price in buy_lots:
        if remaining_to_sell <= 0:
            break
        if lot_qty == 0:
            continue

        take = min(lot_qty, remaining_to_sell)
        realized_pl += (sell_price - lot_price) * take
        remaining_to_sell -= take

    return realized_pl


def log_trade(action):
    symbol = symbol_entry.get().strip().upper()
    qty_str = quantity_entry.get().strip()
    price_str = price_entry.get().strip()

    if not symbol or not qty_str or not price_str:
        messagebox.showwarning("Missing Fields", "Please fill in all fields.")
        return

    try:
        quantity = int(qty_str.replace(",", "").strip())
        price = float(price_str.replace("$", "").replace(",", "").strip())
    except ValueError:
        messagebox.showerror("Input Error", f"Invalid input -> Qty: '{qty_str}', Price: '{price_str}'")
        return

    now = datetime.now().isoformat()

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Log trade
    c.execute("""
        INSERT INTO trades (symbol, action, quantity, price, timestamp)
        VALUES (?, ?, ?, ?, ?)
    """, (symbol, action, quantity, price, now))
    trade_id = c.lastrowid

    # Update portfolio
    c.execute("SELECT quantity, avg_price FROM portfolio WHERE symbol = ?", (symbol,))
    row = c.fetchone()

    if action == "buy":
        if row:
            old_qty, old_avg = row
            new_qty = old_qty + quantity
            new_avg = ((old_qty * old_avg) + (quantity * price)) / new_qty
            c.execute(
                "UPDATE portfolio SET quantity = ?, avg_price = ?, last_updated = ? WHERE symbol = ?",
                (new_qty, new_avg, now, symbol),
            )
        else:
            c.execute(
                "INSERT INTO portfolio (symbol, quantity, avg_price, last_updated) VALUES (?, ?, ?, ?)",
                (symbol, quantity, price, now),
            )

    elif action == "sell":
        if row:
            old_qty, old_avg = row
            new_qty = old_qty - quantity
            if new_qty <= 0:
                c.execute("DELETE FROM portfolio WHERE symbol = ?", (symbol,))
            else:
                c.execute(
                    "UPDATE portfolio SET quantity = ?, last_updated = ? WHERE symbol = ?",
                    (new_qty, now, symbol),
                )

            # Compute FIFO realized P/L
            realized_pl_value = compute_fifo_realized_pl(symbol, quantity, price, trade_id)

            # Store it in the trade row
            c.execute(
                "UPDATE trades SET realized_pl = ? WHERE id = ?",
                (realized_pl_value, trade_id),
            )

        else:
            messagebox.showwarning("Sell Error", f"No holdings found for {symbol}")
            conn.commit()
            conn.close()
            return

    conn.commit()
    conn.close()

    messagebox.showinfo("Trade Logged", f"{action.title()} {quantity} shares of {symbol} @ ${price:.2f}")
    symbol_entry.delete(0, tk.END)
    quantity_entry.delete(0, tk.END)
    price_entry.delete(0, tk.END)


def summarize_portfolio():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Fetch holdings with avg price
    c.execute("SELECT symbol, quantity, avg_price FROM portfolio ORDER BY symbol")
    rows = c.fetchall()

    if not rows:
        conn.close()
        messagebox.showinfo("Portfolio Summary", "No holdings found.")
        return

    # GUI setup
    summary_window = tk.Toplevel(root)
    summary_window.title("Portfolio Summary")

    text = tk.Text(summary_window, width=80, height=15)
    text.pack(padx=10, pady=10)

    markdown_lines = ["# Portfolio Summary\n"]

    for symbol, qty, avg_price in rows:
        summary = f"{symbol:<6} | Qty: {qty:<5} | Avg Price: ${avg_price:.2f}"
        markdown = f"- **{symbol}**: {qty} shares, avg ${avg_price:.2f}"

        text.insert(tk.END, summary + "\n")
        markdown_lines.append(markdown)

    conn.close()

    # Write markdown summary
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    week_id = datetime.now().strftime("%Y-W%V")
    filename = OUTPUT_DIR / f"portfolio_summary_{week_id}.md"
    with open(filename, "w", encoding="utf-8") as md:
        md.write("\n".join(markdown_lines))


root = None
symbol_entry = None
quantity_entry = None
price_entry = None


def build_gui():
    global root, symbol_entry, quantity_entry, price_entry

    root = tk.Tk()
    root.title("Stock Trader GUI")
    root.geometry("600x200")

    tk.Label(root, text="Stock Symbol").grid(row=0, column=0, padx=10, pady=5, sticky="e")
    symbol_entry = tk.Entry(root, width=30)
    symbol_entry.grid(row=0, column=1, padx=10, pady=5)

    tk.Label(root, text="Quantity").grid(row=1, column=0, padx=10, pady=5, sticky="e")
    quantity_entry = tk.Entry(root, width=30)
    quantity_entry.grid(row=1, column=1, padx=10, pady=5)

    tk.Label(root, text="Price per Share").grid(row=2, column=0, padx=10, pady=5, sticky="e")
    price_entry = tk.Entry(root, width=30)
    price_entry.grid(row=2, column=1, padx=10, pady=5)

    button_frame = tk.Frame(root)
    button_frame.grid(row=3, column=0, columnspan=2, pady=10)
    tk.Button(button_frame, text="Buy", command=lambda: log_trade("buy")).grid(row=0, column=0, padx=5)
    tk.Button(button_frame, text="Sell", command=lambda: log_trade("sell")).grid(row=0, column=1, padx=5)

    tk.Button(root, text="Summarize Portfolio", command=summarize_portfolio).grid(
        row=4, column=0, columnspan=2, pady=15
    )


if __name__ == "__main__":
    init_db()
    build_gui()
    root.mainloop()
