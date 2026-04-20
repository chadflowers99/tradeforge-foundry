import sqlite3

conn = sqlite3.connect("data/stock_trades.db")
c = conn.cursor()

c.execute("SELECT COUNT(*) FROM trades")
silver_count = c.fetchone()[0]

c.execute("SELECT COUNT(*) FROM raw_trades WHERE source_file = 'real_trades.csv'")
bronze_count = c.fetchone()[0]

c.execute("SELECT COUNT(*) FROM portfolio")
portfolio_count = c.fetchone()[0]

c.execute("SELECT symbol, quantity, avg_price FROM portfolio ORDER BY symbol")
holdings = c.fetchall()

print(f"Bronze (raw_trades real_trades.csv): {bronze_count} rows")
print(f"Silver (trades table total):         {silver_count} rows")
print(f"Portfolio holdings:                  {portfolio_count} symbols")
print()
print("Current holdings:")
for symbol, qty, avg in holdings:
    print(f"  {symbol:<6}  qty={qty}  avg_price=${avg:.4f}")

conn.close()
