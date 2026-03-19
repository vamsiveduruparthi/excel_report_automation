"""
file_reader.py
==============
Universal File Reader — converts ANY uploaded file into a pandas DataFrame.

Supported formats
-----------------
  CSV         .csv
  TSV         .tsv
  Plain text  .txt          (auto-detects separator: , ; | tab)
  Excel       .xlsx .xls .xlsm   (all sheets stacked if multiple)
  JSON        .json         (root list OR dict with data/results/items key)
  JSON Lines  .jsonl .ndjson     (one JSON object per line)
  XML         .xml
  SQLite      .db .sqlite .sqlite3  (all tables stacked)
  Parquet     .parquet       (requires: pip install pyarrow)
  ORC         .orc           (requires: pip install pyarrow)
  ZIP         .zip           (extracts every supported file inside)

Usage
-----
  # With a Streamlit UploadedFile
  results = read_any_file(uploaded_file)   # → list of (label, DataFrame)

  # With a plain file path (CLI usage)
  results = read_file_path("data/sales.json")
"""

import io
import json
import logging
import os
import sqlite3
import tempfile
import zipfile
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ── Extension metadata ─────────────────────────────────────────────────────────
# Maps extension → (display_icon, display_label)
FILE_TYPE_META: dict[str, tuple[str, str]] = {
    "csv":     ("📄", "CSV"),
    "tsv":     ("📄", "TSV"),
    "txt":     ("📄", "Text"),
    "xlsx":    ("📊", "Excel"),
    "xls":     ("📊", "Excel"),
    "xlsm":    ("📊", "Excel"),
    "json":    ("🔷", "JSON"),
    "jsonl":   ("🔷", "JSON Lines"),
    "ndjson":  ("🔷", "NDJSON"),
    "xml":     ("📋", "XML"),
    "db":      ("🗄️", "SQLite"),
    "sqlite":  ("🗄️", "SQLite"),
    "sqlite3": ("🗄️", "SQLite"),
    "parquet": ("⚡", "Parquet"),
    "orc":     ("⚡", "ORC"),
    "zip":     ("🗜️", "ZIP"),
}

# All extensions the system can handle (used for uploader filter hints)
SUPPORTED_EXTENSIONS: set[str] = set(FILE_TYPE_META.keys())


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL — single-file reader
# ══════════════════════════════════════════════════════════════════════════════

def _read_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Parse raw bytes for a single file into a DataFrame.
    Raises ValueError / ImportError on failure.
    """
    ext = Path(filename).suffix.lstrip(".").lower()
    buf = io.BytesIO(file_bytes)

    # ── CSV / plain text ───────────────────────────────────────────────────
    if ext in ("csv", "txt"):
        # Try common separators in order; pick the one producing most columns
        best_df = None
        for sep in [",", ";", "\t", "|"]:
            try:
                buf.seek(0)
                df = pd.read_csv(buf, sep=sep, engine="python")
                if best_df is None or len(df.columns) > len(best_df.columns):
                    best_df = df
            except Exception:
                continue
        if best_df is not None:
            return best_df
        buf.seek(0)
        return pd.read_csv(buf)   # pandas fallback

    # ── TSV ────────────────────────────────────────────────────────────────
    if ext == "tsv":
        return pd.read_csv(buf, sep="\t")

    # ── Excel ──────────────────────────────────────────────────────────────
    if ext in ("xlsx", "xls", "xlsm"):
        all_sheets: dict = pd.read_excel(buf, sheet_name=None)
        if len(all_sheets) == 1:
            return list(all_sheets.values())[0]
        # Multiple sheets → stack vertically
        logger.info("Excel '%s': stacking %d sheets.", filename, len(all_sheets))
        return pd.concat(all_sheets.values(), ignore_index=True)

    # ── JSON ───────────────────────────────────────────────────────────────
    if ext == "json":
        raw = json.loads(file_bytes.decode("utf-8", errors="replace"))
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if isinstance(raw, dict):
            # Search common wrapper keys
            for key in ("data", "results", "items", "records", "rows", "response"):
                if key in raw and isinstance(raw[key], list):
                    logger.info("JSON '%s': using key '%s'.", filename, key)
                    return pd.DataFrame(raw[key])
            # Flat single-record dict
            return pd.DataFrame([raw])
        raise ValueError(f"JSON root must be a list or dict, got {type(raw).__name__}.")

    # ── JSON Lines ─────────────────────────────────────────────────────────
    if ext in ("jsonl", "ndjson"):
        lines = [
            json.loads(line)
            for line in file_bytes.decode("utf-8", errors="replace").splitlines()
            if line.strip()
        ]
        return pd.DataFrame(lines)

    # ── XML ────────────────────────────────────────────────────────────────
    if ext == "xml":
        try:
            buf.seek(0)
            return pd.read_xml(buf)
        except Exception:
            try:
                buf.seek(0)
                return pd.read_xml(buf, xpath=".//record")
            except Exception:
                buf.seek(0)
                return pd.read_xml(buf, xpath=".//*")

    # ── SQLite ─────────────────────────────────────────────────────────────
    if ext in ("db", "sqlite", "sqlite3"):
        # Write bytes to a temp file (sqlite3 needs a real file path)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            tables = pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type='table'", conn
            )
            if tables.empty:
                raise ValueError(f"SQLite '{filename}' contains no tables.")
            frames = [pd.read_sql(f"SELECT * FROM [{tbl}]", conn)
                      for tbl in tables["name"]]
            conn.close()
            return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        finally:
            os.unlink(tmp_path)

    # ── Parquet ────────────────────────────────────────────────────────────
    if ext == "parquet":
        try:
            return pd.read_parquet(buf)
        except ImportError:
            raise ImportError(
                "Install pyarrow to read Parquet files:\n  pip install pyarrow"
            )

    # ── ORC ────────────────────────────────────────────────────────────────
    if ext == "orc":
        try:
            return pd.read_orc(buf)
        except ImportError:
            raise ImportError(
                "Install pyarrow to read ORC files:\n  pip install pyarrow"
            )

    raise ValueError(
        f"Unsupported file type: .{ext}\n"
        f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def read_any_file(uploaded_file) -> list[tuple[str, pd.DataFrame]]:
    """
    Accept a Streamlit UploadedFile (or any object with .name and .read()).
    Returns a list of (label, DataFrame) tuples.

    Most files return one tuple. ZIP archives and multi-sheet Excel files
    can return multiple tuples — one per inner file / sheet group.

    Parameters
    ----------
    uploaded_file : object with  .name (str)  and  .read() → bytes

    Returns
    -------
    list of (label: str, df: pd.DataFrame)
    """
    name  = uploaded_file.name
    ext   = Path(name).suffix.lstrip(".").lower()
    data  = uploaded_file.read()
    out: list[tuple[str, pd.DataFrame]] = []

    if ext == "zip":
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            # Filter to supported inner files, skip macOS metadata
            members = [
                m for m in zf.namelist()
                if Path(m).suffix.lstrip(".").lower() in (SUPPORTED_EXTENSIONS - {"zip"})
                and not m.startswith("__MACOSX")
                and not Path(m).name.startswith(".")
            ]
            if not members:
                raise ValueError(
                    f"ZIP '{name}' contains no supported files.\n"
                    f"Found: {zf.namelist()}"
                )
            for member in members:
                inner_bytes = zf.read(member)
                inner_name  = Path(member).name
                try:
                    df = _read_bytes(inner_bytes, inner_name)
                    label = f"{name}/{inner_name}"
                    out.append((label, df))
                    logger.info("ZIP member '%s': %d rows × %d cols.", label, *df.shape)
                except Exception as exc:
                    logger.warning("Skipping ZIP member '%s': %s", member, exc)
    else:
        df = _read_bytes(data, name)
        out.append((name, df))
        logger.info("Read '%s': %d rows × %d cols.", name, *df.shape)

    return out


def read_file_path(filepath: str) -> list[tuple[str, pd.DataFrame]]:
    """
    CLI convenience wrapper — reads a file from disk and calls read_any_file().

    Parameters
    ----------
    filepath : path to any supported file on disk

    Returns
    -------
    list of (label: str, df: pd.DataFrame)
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    class _DiskFile:
        name = path.name
        def read(self):
            return path.read_bytes()

    return read_any_file(_DiskFile())


# ── Metadata helpers ───────────────────────────────────────────────────────────

def file_icon(filename: str) -> str:
    """Return the display icon for a given filename."""
    ext = Path(filename).suffix.lstrip(".").lower()
    return FILE_TYPE_META.get(ext, ("📎", "File"))[0]


def file_label(filename: str) -> str:
    """Return the display label for a given filename."""
    ext = Path(filename).suffix.lstrip(".").lower()
    return FILE_TYPE_META.get(ext, ("📎", "File"))[1]
