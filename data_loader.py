"""
data_loader.py
==============
Multi-Source Data Ingestion Engine
Supports: CSV, Excel, REST APIs, and SQLite/SQL Databases
"""

import os
import logging
import sqlite3
import requests
import pandas as pd
from pathlib import Path

# ── Logger ────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# BASE LOADER
# ══════════════════════════════════════════════════════════════════════════════

class BaseLoader:
    """Abstract base: every loader must implement load() → pd.DataFrame."""

    source_type: str = "base"

    def load(self) -> pd.DataFrame:
        raise NotImplementedError

    def _validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Shared post-load guard: reject empty frames."""
        if df is None or df.empty:
            raise ValueError(f"[{self.source_type}] Loaded dataset is empty.")
        logger.info("[%s] Loaded %d rows × %d cols.", self.source_type, *df.shape)
        return df


# ══════════════════════════════════════════════════════════════════════════════
# CSV LOADER
# ══════════════════════════════════════════════════════════════════════════════

class CSVLoader(BaseLoader):
    """Load data from a CSV file."""

    source_type = "CSV"

    def __init__(self, filepath: str, **read_csv_kwargs):
        self.filepath = Path(filepath)
        self.kwargs = read_csv_kwargs

    def load(self) -> pd.DataFrame:
        if not self.filepath.exists():
            raise FileNotFoundError(f"CSV not found: {self.filepath}")
        logger.info("Reading CSV → %s", self.filepath)
        df = pd.read_csv(self.filepath, **self.kwargs)
        return self._validate(df)


# ══════════════════════════════════════════════════════════════════════════════
# EXCEL LOADER
# ══════════════════════════════════════════════════════════════════════════════

class ExcelLoader(BaseLoader):
    """Load data from an Excel file (.xlsx / .xls).

    Parameters
    ----------
    sheet_name : str | int | None
        Sheet to read.  Pass None to load ALL sheets and concatenate them.
    """

    source_type = "Excel"

    def __init__(self, filepath: str, sheet_name=0, **read_excel_kwargs):
        self.filepath = Path(filepath)
        self.sheet_name = sheet_name
        self.kwargs = read_excel_kwargs

    def load(self) -> pd.DataFrame:
        if not self.filepath.exists():
            raise FileNotFoundError(f"Excel file not found: {self.filepath}")
        logger.info("Reading Excel → %s  (sheet=%s)", self.filepath, self.sheet_name)

        if self.sheet_name is None:
            # Load every sheet and stack vertically
            sheets: dict = pd.read_excel(
                self.filepath, sheet_name=None, **self.kwargs
            )
            df = pd.concat(sheets.values(), ignore_index=True)
        else:
            df = pd.read_excel(
                self.filepath, sheet_name=self.sheet_name, **self.kwargs
            )
        return self._validate(df)


# ══════════════════════════════════════════════════════════════════════════════
# API LOADER
# ══════════════════════════════════════════════════════════════════════════════

class APILoader(BaseLoader):
    """Fetch tabular data from a REST/JSON API endpoint.

    Parameters
    ----------
    url        : full endpoint URL
    data_key   : dot-path into the JSON response where the list lives,
                 e.g. "results" or "data.items".  Leave None if the root
                 is already a list.
    params     : query-string params dict
    headers    : HTTP headers dict (auth tokens, etc.)
    timeout    : request timeout in seconds
    """

    source_type = "API"

    def __init__(
        self,
        url: str,
        data_key: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        timeout: int = 30,
    ):
        self.url = url
        self.data_key = data_key
        self.params = params or {}
        self.headers = headers or {}
        self.timeout = timeout

    def load(self) -> pd.DataFrame:
        logger.info("Fetching API → %s", self.url)
        try:
            resp = requests.get(
                self.url,
                params=self.params,
                headers=self.headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ConnectionError(f"API request failed: {exc}") from exc

        payload = resp.json()

        # Navigate to the nested key if provided
        if self.data_key:
            for key in self.data_key.split("."):
                if not isinstance(payload, dict) or key not in payload:
                    raise KeyError(
                        f"Key '{key}' not found in API response. "
                        f"Available keys: {list(payload.keys()) if isinstance(payload, dict) else 'N/A'}"
                    )
                payload = payload[key]

        if not isinstance(payload, list):
            raise TypeError(
                f"Expected a list at data_key='{self.data_key}', got {type(payload).__name__}."
            )

        df = pd.DataFrame(payload)
        return self._validate(df)


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE LOADER
# ══════════════════════════════════════════════════════════════════════════════

class DatabaseLoader(BaseLoader):
    """Load data from a SQL database via a raw SQL query.

    Supports any SQLAlchemy-compatible connection string, plus raw
    sqlite3 paths for zero-dependency SQLite usage.

    Parameters
    ----------
    connection  : SQLAlchemy URL  (e.g. "sqlite:///sales.db",
                  "postgresql://user:pass@host/db")
                  OR a raw file path ending in .db / .sqlite for SQLite.
    query       : SQL SELECT statement to execute.
    """

    source_type = "Database"

    def __init__(self, connection: str, query: str):
        self.connection = connection
        self.query = query

    def load(self) -> pd.DataFrame:
        logger.info("Querying DB → %s", self.connection)

        # ── SQLite shortcut (no SQLAlchemy needed) ─────────────────────────
        if self.connection.endswith((".db", ".sqlite")):
            if not Path(self.connection).exists():
                raise FileNotFoundError(f"SQLite DB not found: {self.connection}")
            conn = sqlite3.connect(self.connection)
            try:
                df = pd.read_sql_query(self.query, conn)
            finally:
                conn.close()
        else:
            # ── SQLAlchemy path ────────────────────────────────────────────
            try:
                from sqlalchemy import create_engine, text
            except ImportError as exc:
                raise ImportError(
                    "Install sqlalchemy: pip install sqlalchemy"
                ) from exc
            engine = create_engine(self.connection)
            with engine.connect() as conn:
                df = pd.read_sql_query(text(self.query), conn)

        return self._validate(df)


# ══════════════════════════════════════════════════════════════════════════════
# MULTI-SOURCE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

class MultiSourceLoader:
    """Collect data from multiple heterogeneous sources and merge them.

    Usage
    -----
    loader = MultiSourceLoader()
    loader.add_source("csv_sales",  CSVLoader("data/sales.csv"))
    loader.add_source("api_rates",  APILoader("https://api.example.com/rates", data_key="data"))
    loader.add_source("db_returns", DatabaseLoader("sales.db", "SELECT * FROM returns"))

    combined_df = loader.load_all(merge_on="order_id", how="left")
    # – OR –
    combined_df = loader.load_all()          # simple vertical stack
    """

    def __init__(self):
        self._sources: dict[str, BaseLoader] = {}

    # ── Registration ──────────────────────────────────────────────────────────
    def add_source(self, name: str, loader: BaseLoader) -> "MultiSourceLoader":
        """Register a named loader.  Returns self for chaining."""
        if not isinstance(loader, BaseLoader):
            raise TypeError(f"Loader for '{name}' must subclass BaseLoader.")
        self._sources[name] = loader
        logger.debug("Registered source: %s (%s)", name, loader.source_type)
        return self

    # ── Loading ───────────────────────────────────────────────────────────────
    def load_all(
        self,
        merge_on: str | list | None = None,
        how: str = "outer",
    ) -> pd.DataFrame:
        """Load every registered source, then combine.

        Parameters
        ----------
        merge_on : column(s) to join on.  If None, DataFrames are stacked
                   vertically (pd.concat).
        how      : merge strategy ('left', 'right', 'inner', 'outer').
        """
        if not self._sources:
            raise RuntimeError("No data sources registered. Call add_source() first.")

        frames: dict[str, pd.DataFrame] = {}
        errors: list[str] = []

        for name, loader in self._sources.items():
            try:
                frames[name] = loader.load()
            except Exception as exc:  # noqa: BLE001
                logger.error("Source '%s' failed: %s", name, exc)
                errors.append(f"{name}: {exc}")

        if not frames:
            raise RuntimeError(
                f"All sources failed to load:\n" + "\n".join(errors)
            )
        if errors:
            logger.warning(
                "%d source(s) skipped due to errors:\n%s",
                len(errors),
                "\n".join(errors),
            )

        if merge_on is None:
            # Vertical stack – align columns, fill gaps with NaN
            combined = pd.concat(frames.values(), ignore_index=True)
            logger.info(
                "Stacked %d source(s) → %d rows × %d cols.",
                len(frames),
                *combined.shape,
            )
        else:
            # Sequential left-join / outer-join on a common key
            frames_list = list(frames.values())
            combined = frames_list[0]
            for df in frames_list[1:]:
                combined = combined.merge(df, on=merge_on, how=how, suffixes=("", "_dup"))
                # Drop accidental duplicate columns
                dup_cols = [c for c in combined.columns if c.endswith("_dup")]
                combined.drop(columns=dup_cols, inplace=True)
            logger.info(
                "Merged %d source(s) on '%s' → %d rows × %d cols.",
                len(frames),
                merge_on,
                *combined.shape,
            )

        return combined

    # ── Convenience ───────────────────────────────────────────────────────────
    def source_names(self) -> list[str]:
        return list(self._sources.keys())

    def __repr__(self) -> str:
        sources = ", ".join(
            f"{n}({l.source_type})" for n, l in self._sources.items()
        )
        return f"MultiSourceLoader([{sources}])"
