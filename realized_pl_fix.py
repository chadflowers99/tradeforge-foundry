import sqlite3
from stock_trader_gui import compute_fifo_realized_pl, DB_FILE


conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# Get all sell trades in order
c.execute(
    """
    SELECT id, symbol, quantity, price
    FROM trades
    WHERE action = 'sell'
    ORDER BY id ASC
    """
)
sells = c.fetchall()

for trade_id, symbol, qty, price in sells:
    new_pl = compute_fifo_realized_pl(symbol, qty, price, trade_id)
    c.execute("UPDATE trades SET realized_pl = ? WHERE id = ?", (new_pl, trade_id))

conn.commit()
conn.close()

print("Recalculation complete.")
