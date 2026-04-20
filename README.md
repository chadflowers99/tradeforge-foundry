# TradeForge Foundry

A modular Python suite for trade lifecycle management with deterministic maintenance tasks.

## Included scripts

- stock_trader_gui.py: Main GUI for buy/sell logging and portfolio summary.
- rebuild_portfolio.py: Rebuilds portfolio table from trades with backup creation.
- realized_pl_fix.py: Recalculates FIFO realized P/L for historical sell trades.
- export_trades.py: Exports trades table to trades_export.csv.
- integrity_check.py: Gatekeeper validation for required schema and basic data quality checks.
- archive_to_parquet.py: Creates research-ready Parquet snapshots from SQLite history.
- ingest_manager.py: Bronze/Silver ingest pipeline with append-only raw landing and manifest logging.
- metrics_module.py: Research metrics summary for realized trading performance and open-risk posture.
- run_toolbox.py: Single command runner for common tasks.

## Project layout

- data/: active SQLite database (stock_trades.db).
- output/: generated CSV and markdown summary outputs.
- archive/: timestamped Parquet research snapshots.

## Two-zone architecture

- Bronze (raw_trades): append-only source payload storage with batch_id and ingested_at.
- Silver (trades): deterministic normalized records used by portfolio and P/L logic.
- Audit (ingest_manifest): per-run run_id, row counts, and status trail.
- Manifest detail (ingest_rejections): row-level rejection reasons and raw data fragments.

## Data Lineage & Forensics

- Immutable Bronze: every source row is preserved in raw_trades as raw JSON payload, including rows that later fail validation.
- Deterministic Silver: only normalized, valid, and deduplicated rows are written into trades.
- Audit Manifest: each ingest run is assigned a unique run_id with rows_ingested, rows_written, rows_rejected, status, and batch_id.
- Manifest Detail: each rejected row is logged with row_index, error_message, and raw_data_fragment for exact row-level debugging.

### Example forensic run

- Input file: dirty_trades.csv
- Result: ingested=4, written=1, rejected=3, status=partial_success
- Data health score: accepted=25.00%, rejected=75.00%
- Rejection reasons captured: invalid timestamp, invalid quantity, duplicate_row_hash

This behavior is intentional: rejection is treated as a protective control that preserves Silver-layer integrity for downstream FIFO P/L and portfolio rebuild logic.

## Quick start

1. Open a terminal in this folder.
2. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

3. Run the turnkey demo workflow (single command):

```powershell
.\run_client_demo.ps1
```

If PowerShell blocks script execution, run this once in the current shell and then retry:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
```

4. Or run toolbox commands directly:

```powershell
python run_toolbox.py demo-reset
python run_toolbox.py demo-run
```

5. Optional: run steps individually:

```powershell
python run_toolbox.py check-integrity
python run_toolbox.py recalc
python run_toolbox.py rebuild
python run_toolbox.py metrics
```

6. Run the GUI:

```powershell
python stock_trader_gui.py
```

## Data migration

For client deployments, ingest source CSV files through the Bronze/Silver pipeline instead of copying private SQLite databases.

## Runner commands

```powershell
python run_toolbox.py gui
.\run_client_demo.ps1
.\run_client_demo.ps1 -Source "data/new_trades.csv"
python run_toolbox.py ingest --source "data/new_trades.csv"
python run_toolbox.py demo-reset
python run_toolbox.py demo-run
python run_toolbox.py demo-run --source "data/new_trades.csv"
python run_toolbox.py check-integrity
python run_toolbox.py rebuild
python run_toolbox.py recalc
python run_toolbox.py export
python run_toolbox.py archive
python run_toolbox.py metrics
```

## Notes

- rebuild, recalc, export, and archive tasks run schema integrity checks first.
- metrics runs integrity checks first and writes output/metrics_summary.txt.
- ingest writes to raw_trades first, then validates/transforms to trades deterministically.
- check-integrity prints the last 5 ingest runs and recent rejection reasons for fast diagnosis.
- If data/stock_trades.db does not exist, launching the GUI will initialize the required tables.

## Scheduling guidance

- Recommended production mode: hybrid.
- Market-close automation: schedule ingest, integrity check, recalc, and archive after close.
- Human-in-the-loop safeguard: review check-integrity output and rejection reasons before publishing reports.
