"""
main.py
=======
CLI Pipeline Orchestrator — Excel Report Automation.

Accepts any mix of file types, APIs, and databases.

Examples
--------
  python main.py --demo
  python main.py --file sales.csv
  python main.py --file sales.csv --file returns.xlsx --file events.jsonl
  python main.py --file archive.zip
  python main.py --file sales.csv --file products.json --file data.db
  python main.py --api https://jsonplaceholder.typicode.com/posts
  python main.py --db crm.db "SELECT * FROM customers"
  python main.py --file sales.csv --group-col region --value-col revenue
"""

import argparse
import logging
import random
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from data_loader      import CSVLoader, ExcelLoader, APILoader, DatabaseLoader, MultiSourceLoader
from data_processor   import DataCleaner, DataAnalyzer
from smart_detect import smart_detect, explain_detection
from file_reader      import read_file_path, file_icon
from report_generator import ReportGenerator

# ── Logging ────────────────────────────────────────────────────────────────────

def setup_logging(log_dir: str = "logs") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fmt = "%(asctime)s  %(levelname)-8s — %(message)s"
    logging.basicConfig(
        level=logging.INFO, format=fmt,
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("matplotlib").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ── Demo data ──────────────────────────────────────────────────────────────────

REGIONS    = ["North","South","East","West","Central"]
PRODUCTS   = ["Laptop","Tablet","Phone","Monitor","Keyboard","Mouse"]
CATEGORIES = ["Electronics","Accessories","Peripherals"]
REPS       = [f"Rep_{i:02d}" for i in range(1, 11)]


def _rdate(s, e):
    return (s + timedelta(days=random.randint(0, (e-s).days))).strftime("%Y-%m-%d")


def make_demo_frames() -> list[pd.DataFrame]:
    """Return three realistic demo DataFrames (CSV, Excel, SQLite simulation)."""
    random.seed(42)
    s, e = datetime(2024, 1, 1), datetime(2024, 12, 31)

    rows = []
    for i in range(1, 201):
        qty, price = random.randint(1, 50), round(random.uniform(50, 2000), 2)
        disc = round(random.uniform(0, 0.3), 2)
        rows.append({
            "order_id":  f"ORD-{i:04d}", "date": _rdate(s, e),
            "product":   random.choice(PRODUCTS), "region": random.choice(REGIONS),
            "sales_rep": random.choice(REPS),     "quantity": qty,
            "unit_price": price, "discount": disc,
            "revenue":   round(qty * price * (1 - disc), 2),
        })
    rows[10]["revenue"] = None
    rows[20]["region"]  = "  north  "

    random.seed(7)
    returns_df = pd.DataFrame([{
        "order_id":      f"ORD-{random.randint(1,200):04d}",
        "return_reason": random.choice(["Defective","Wrong item","Changed mind"]),
        "return_amount": round(random.uniform(50, 1500), 2),
        "category":      random.choice(CATEGORIES),
    } for _ in range(80)])

    random.seed(99)
    nps_df = pd.DataFrame([{
        "order_id":   f"ORD-{random.randint(1,200):04d}",
        "region":     random.choice(REGIONS),
        "nps_score":  random.randint(0, 10),
        "csat_score": round(random.uniform(1, 5), 1),
    } for _ in range(120)])

    return [pd.DataFrame(rows), returns_df, nps_df]


# ── Column auto-detection ──────────────────────────────────────────────────────


# ── Core pipeline ──────────────────────────────────────────────────────────────

def run_pipeline(frames: list, report_title: str,
                 group_col=None, value_col=None, date_col=None,
                 output_dir: str = "output") -> Path:
    """
    Clean → Analyse → Generate report.
    Returns the path to the saved .xlsx file.
    """
    logger.info("Stacking %d source(s) …", len(frames))
    raw_df = pd.concat(frames, ignore_index=True)
    logger.info("Combined → %d rows × %d cols", *raw_df.shape)

    # Detect on raw df first, then normalise col names to match post-cleaning names
    # (DataCleaner lowercases all column names, so we must do the same here)
    cols = smart_detect(raw_df)
    def _norm(c):
        import re
        if c is None: return None
        return re.sub(r'[^\w]', '', re.sub(r'[\s\-]+', '_', c.lower()))
    cols = {k: _norm(v) for k, v in cols.items()}
    if group_col: cols["group_col"] = _norm(group_col)
    if value_col: cols["value_col"] = _norm(value_col)
    if date_col:  cols["date_col"]  = _norm(date_col)

    # Guard checks run AFTER cleaning (DataCleaner normalises column names to lowercase)

    # Clean
    num_cols = raw_df.select_dtypes(include="number").columns.tolist()
    clean_df = DataCleaner(raw_df).clean(
        date_columns    = [cols["date_col"]] if cols["date_col"] else [],
        numeric_columns = num_cols,
        fill_strategy   = {c: "mean" for c in num_cols},
    )

    # Analyse
    results = DataAnalyzer(clean_df).analyze(
        group_by     = cols["group_col"],
        value_col    = cols["value_col"],
        date_col     = cols["date_col"],
        category_col = cols["cat_col"],
    )

    # Generate
    out_path = ReportGenerator(
        analysis_results = results,
        raw_df           = clean_df,
        report_title     = report_title,
        group_col        = cols["group_col"],
        value_col        = cols["value_col"],
        output_dir       = output_dir,
    ).generate()

    return out_path


# ── CLI entry-point ────────────────────────────────────────────────────────────

def run(args) -> Path:
    setup_logging()
    logger.info("═" * 62)
    logger.info("  Excel Report Automation — CLI")
    logger.info("═" * 62)

    Path("data").mkdir(exist_ok=True)
    Path("output").mkdir(exist_ok=True)

    frames: list[pd.DataFrame] = []

    no_sources = not any([args.file, args.api, args.db])
    if args.demo or no_sources:
        logger.info("── Demo mode ─────────────────────────────────────────────────")
        frames = make_demo_frames()
    else:
        logger.info("── Loading sources ───────────────────────────────────────────")

        # ── Universal file inputs (any format) ────────────────────────────
        for filepath in (args.file or []):
            try:
                results = read_file_path(filepath)
                for label, df in results:
                    frames.append(df)
                    logger.info("Loaded %s  %s → %d rows × %d cols",
                                file_icon(filepath), label, *df.shape)
            except Exception as exc:
                logger.error("Could not read '%s': %s", filepath, exc)

        # ── API sources ───────────────────────────────────────────────────
        for entry in (args.api or []):
            parts    = entry.split("::")
            url      = parts[0]
            data_key = parts[1] if len(parts) > 1 else None
            headers  = {}
            for h in (args.api_header or []):
                k, _, v = h.partition(":"); headers[k.strip()] = v.strip()
            try:
                df = APILoader(url, data_key=data_key, headers=headers).load()
                frames.append(df)
                logger.info("Loaded 🌐 %s → %d rows × %d cols", url, *df.shape)
            except Exception as exc:
                logger.error("API '%s' failed: %s", url, exc)

        # ── Database sources ──────────────────────────────────────────────
        db_pairs = args.db or []
        if len(db_pairs) % 2 != 0:
            raise ValueError("--db requires pairs: <connection> <SQL query>")
        for conn_str, query in zip(db_pairs[::2], db_pairs[1::2]):
            try:
                df = DatabaseLoader(conn_str, query).load()
                frames.append(df)
                logger.info("Loaded 🗄️ %s → %d rows × %d cols", conn_str, *df.shape)
            except Exception as exc:
                logger.error("DB '%s' failed: %s", conn_str, exc)

    if not frames:
        raise RuntimeError("No data loaded. Check your sources and try again.")

    title = args.title or f"Report — {datetime.now().strftime('%d %b %Y')}"
    out   = run_pipeline(
        frames, title,
        group_col  = args.group_col or None,
        value_col  = args.value_col or None,
        date_col   = args.date_col  or None,
        output_dir = "output",
    )

    logger.info("═" * 62)
    logger.info("  ✅  Report saved → %s", out)
    logger.info("═" * 62)
    return out


# ── Argument parser ────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Excel Report Automation — universal multi-source pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Source flags ──────────────────────────────────────────────────────
    p.add_argument(
        "--file", metavar="PATH", action="append",
        help=(
            "Any data file — CSV, TSV, TXT, Excel, JSON, JSONL, "
            "XML, SQLite (.db), Parquet, ZIP. Repeatable."
        ),
    )
    p.add_argument(
        "--api", metavar="URL[::key]", action="append",
        help="REST API URL. Optionally append ::dot.path for nested JSON key.",
    )
    p.add_argument(
        "--api-header", metavar="Key:Value", action="append",
        help="HTTP header to send with API requests. Repeatable.",
    )
    p.add_argument(
        "--db", metavar="CONN", action="append",
        help="Pairs: --db <connection_or_path> <SQL query>.",
    )
    p.add_argument(
        "--demo", action="store_true",
        help="Run on auto-generated sample data (no files needed).",
    )

    # ── Column overrides ──────────────────────────────────────────────────
    p.add_argument("--group-col", metavar="COL",
                   help="Column to group by (auto-detected if omitted).")
    p.add_argument("--value-col", metavar="COL",
                   help="Numeric column to analyse (auto-detected if omitted).")
    p.add_argument("--date-col",  metavar="COL",
                   help="Date column for trend analysis (auto-detected if omitted).")
    p.add_argument("--title", metavar="TEXT", default="",
                   help="Report title shown on the Executive Summary sheet.")

    return p.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args())
    except Exception as exc:
        logging.getLogger(__name__).critical("Pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)
