from __future__ import annotations

import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
DB_FILE = DATA_DIR / "stock_trades.db"
REPORT_FILE = OUTPUT_DIR / "metrics_summary.txt"


def _safe_div(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return num / den


def compute_metrics(db_file: Path = DB_FILE) -> dict[str, float | int]:
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM trades")
    total_trades = int(c.fetchone()[0])

    c.execute("SELECT COUNT(*) FROM trades WHERE action = 'buy'")
    total_buys = int(c.fetchone()[0])

    c.execute("SELECT COUNT(*) FROM trades WHERE action = 'sell'")
    total_sells = int(c.fetchone()[0])

    c.execute(
        """
        SELECT COUNT(*)
        FROM trades
        WHERE action = 'sell' AND realized_pl IS NOT NULL
        """
    )
    realized_sell_rows = int(c.fetchone()[0])

    c.execute(
        """
        SELECT COALESCE(SUM(realized_pl), 0.0)
        FROM trades
        WHERE action = 'sell' AND realized_pl IS NOT NULL
        """
    )
    total_realized_pl = float(c.fetchone()[0])

    c.execute(
        """
        SELECT COALESCE(SUM(realized_pl), 0.0)
        FROM trades
        WHERE action = 'sell' AND realized_pl > 0
        """
    )
    gross_profit = float(c.fetchone()[0])

    c.execute(
        """
        SELECT COALESCE(SUM(ABS(realized_pl)), 0.0)
        FROM trades
        WHERE action = 'sell' AND realized_pl < 0
        """
    )
    gross_loss = float(c.fetchone()[0])

    c.execute(
        """
        SELECT COUNT(*)
        FROM trades
        WHERE action = 'sell' AND realized_pl > 0
        """
    )
    winning_trades = int(c.fetchone()[0])

    c.execute(
        """
        SELECT COUNT(*)
        FROM trades
        WHERE action = 'sell' AND realized_pl < 0
        """
    )
    losing_trades = int(c.fetchone()[0])

    c.execute(
        """
        SELECT COALESCE(AVG(realized_pl), 0.0)
        FROM trades
        WHERE action = 'sell' AND realized_pl > 0
        """
    )
    avg_win = float(c.fetchone()[0])

    c.execute(
        """
        SELECT COALESCE(AVG(ABS(realized_pl)), 0.0)
        FROM trades
        WHERE action = 'sell' AND realized_pl < 0
        """
    )
    avg_loss = float(c.fetchone()[0])

    c.execute("SELECT COUNT(DISTINCT symbol) FROM trades")
    unique_symbols = int(c.fetchone()[0])

    c.execute("SELECT COUNT(*) FROM portfolio")
    open_positions = int(c.fetchone()[0])

    conn.close()

    win_rate = _safe_div(winning_trades, realized_sell_rows) * 100.0
    profit_factor = _safe_div(gross_profit, gross_loss) if gross_loss > 0 else 0.0
    expectancy = (_safe_div(winning_trades, realized_sell_rows) * avg_win) - (
        _safe_div(losing_trades, realized_sell_rows) * avg_loss
    )

    return {
        "total_trades": total_trades,
        "total_buys": total_buys,
        "total_sells": total_sells,
        "realized_sell_rows": realized_sell_rows,
        "unique_symbols": unique_symbols,
        "open_positions": open_positions,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "win_rate_pct": win_rate,
        "total_realized_pl": total_realized_pl,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy_per_sell": expectancy,
    }


def main() -> int:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        print("Tip: ingest demo data first with: python run_toolbox.py ingest --source data/demo_trades.csv")
        return 1

    metrics = compute_metrics(DB_FILE)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        "TradeForge Metrics Summary",
        "==========================",
        f"Total trades:          {metrics['total_trades']}",
        f"Buys / Sells:          {metrics['total_buys']} / {metrics['total_sells']}",
        f"Realized sell rows:    {metrics['realized_sell_rows']}",
        f"Unique symbols:        {metrics['unique_symbols']}",
        f"Open positions:        {metrics['open_positions']}",
        f"Winning / Losing:      {metrics['winning_trades']} / {metrics['losing_trades']}",
        f"Win rate:              {metrics['win_rate_pct']:.2f}%",
        f"Total realized P/L:    ${metrics['total_realized_pl']:.2f}",
        f"Gross profit/loss:     ${metrics['gross_profit']:.2f} / ${metrics['gross_loss']:.2f}",
        f"Profit factor:         {metrics['profit_factor']:.4f}",
        f"Avg win / Avg loss:    ${metrics['avg_win']:.2f} / ${metrics['avg_loss']:.2f}",
        f"Expectancy per sell:   ${metrics['expectancy_per_sell']:.2f}",
    ]

    REPORT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")

    for line in lines:
        print(line)
    print(f"\nReport written: {REPORT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
