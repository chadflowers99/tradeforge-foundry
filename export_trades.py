# -*- coding: utf-8 -*-
"""
Simple export script for trades table.
"""

import sqlite3
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
DB_FILE = DATA_DIR / "stock_trades.db"
EXPORT_FILE = OUTPUT_DIR / "trades_export.csv"


conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

c.execute(
    """
    SELECT id, symbol, action, quantity, price, realized_pl, timestamp, notes
    FROM trades
    ORDER BY id ASC
    """
)
rows = c.fetchall()

column_names = [desc[0] for desc in c.description]

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

with open(EXPORT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(column_names)
    writer.writerows(rows)

conn.close()

print("Export complete.")
