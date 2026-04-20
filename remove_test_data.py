import sqlite3

conn = sqlite3.connect("data/stock_trades.db")
c = conn.cursor()

c.execute("DELETE FROM trades WHERE symbol = 'AAPL'")
c.execute("DELETE FROM portfolio WHERE symbol = 'AAPL'")
c.execute("DELETE FROM ingest_rejections WHERE run_id IN (SELECT run_id FROM ingest_manifest WHERE source_file = 'dirty_trades.csv')")
c.execute("DELETE FROM raw_trades WHERE source_file = 'dirty_trades.csv'")
c.execute("DELETE FROM ingest_manifest WHERE source_file = 'dirty_trades.csv'")

conn.commit()
conn.close()
print("Test data cleaned. dirty_trades.csv entries removed from all tables.")
