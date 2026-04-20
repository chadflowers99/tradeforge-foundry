from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
ARCHIVE_DIR = BASE_DIR / "archive"


def run_script(script_name: str, extra_args: list[str] | None = None) -> int:
    if extra_args is None:
        extra_args = []
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        print(f"Script not found: {script_path}")
        return 1
    return subprocess.call([sys.executable, str(script_path), *extra_args], cwd=str(BASE_DIR))


def run_integrity_check() -> int:
    return run_script("integrity_check.py")


def run_demo_pipeline(source: str) -> int:
    print("[1/5] Ingesting source data...")
    code = run_script("ingest_manager.py", ["--source", source])
    if code != 0:
        print("Demo pipeline aborted during ingest.")
        return code

    print("[2/5] Running integrity checks...")
    code = run_integrity_check()
    if code != 0:
        print("Demo pipeline aborted because integrity check failed.")
        return code

    print("[3/5] Recalculating realized P/L...")
    code = run_script("realized_pl_fix.py")
    if code != 0:
        print("Demo pipeline aborted during P/L recalculation.")
        return code

    print("[4/5] Rebuilding portfolio state...")
    code = run_script("rebuild_portfolio.py")
    if code != 0:
        print("Demo pipeline aborted during portfolio rebuild.")
        return code

    print("[5/5] Generating metrics summary...")
    code = run_script("metrics_module.py")
    if code != 0:
        print("Demo pipeline aborted during metrics generation.")
        return code

    print("Demo pipeline completed successfully.")
    return 0


def run_demo_reset() -> int:
    removed_files: list[Path] = []

    for db_file in DATA_DIR.glob("stock_trades*.db"):
        if db_file.is_file():
            db_file.unlink(missing_ok=True)
            removed_files.append(db_file)

    for output_file in OUTPUT_DIR.glob("*"):
        if output_file.is_file():
            output_file.unlink(missing_ok=True)
            removed_files.append(output_file)

    for archive_file in ARCHIVE_DIR.rglob("*"):
        if archive_file.is_file():
            archive_file.unlink(missing_ok=True)
            removed_files.append(archive_file)

    print("Demo environment reset complete.")
    print(f"Removed files: {len(removed_files)}")
    print("Next step: python run_toolbox.py demo-run")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="TradeForge Foundry helper runner for common maintenance scripts."
    )
    parser.add_argument(
        "task",
        choices=[
            "gui",
            "rebuild",
            "recalc",
            "export",
            "check-integrity",
            "archive",
            "ingest",
            "metrics",
            "demo-run",
            "demo-reset",
        ],
        help=(
            "Task to run: gui (UI), rebuild (portfolio table), recalc (FIFO realized P/L), "
            "export (CSV export), check-integrity (schema/quality gate), "
            "archive (SQLite -> Parquet snapshot), ingest (CSV -> raw+clean tables), "
            "metrics (research metrics summary), demo-run (ingest+integrity+recalc+rebuild+metrics), "
            "demo-reset (clear DB/output/archive for a clean demo)"
        ),
    )
    parser.add_argument(
        "--source",
        default="",
        help="Source CSV path used by the ingest task",
    )
    args = parser.parse_args()

    if args.task == "check-integrity":
        return run_integrity_check()

    if args.task == "demo-run":
        source = args.source if args.source else "data/demo_trades.csv"
        return run_demo_pipeline(source)

    if args.task == "demo-reset":
        return run_demo_reset()

    if args.task in {"rebuild", "recalc", "export", "archive", "metrics"}:
        integrity_code = run_integrity_check()
        if integrity_code != 0:
            print("Aborting task because integrity check failed.")
            return integrity_code

    if args.task == "ingest":
        if not args.source:
            print("Ingest requires --source <path-to-csv>")
            return 1
        return run_script("ingest_manager.py", ["--source", args.source])

    if args.task == "gui":
        return run_script("stock_trader_gui.py")
    if args.task == "rebuild":
        return run_script("rebuild_portfolio.py")
    if args.task == "recalc":
        return run_script("realized_pl_fix.py")
    if args.task == "export":
        return run_script("export_trades.py")
    if args.task == "archive":
        return run_script("archive_to_parquet.py")
    if args.task == "metrics":
        return run_script("metrics_module.py")

    print("Unknown task.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
