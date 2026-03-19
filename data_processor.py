"""
data_processor.py
=================
Data Cleaning, Preprocessing, and Analysis Engine.

Responsibilities
----------------
1. Clean and standardise raw data coming from any source.
2. Compute KPIs: totals, averages, growth, rankings.
3. Build aggregated summary tables ready for the report.
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# DATA CLEANER
# ══════════════════════════════════════════════════════════════════════════════

class DataCleaner:
    """Apply a configurable cleaning pipeline to a raw DataFrame."""

    def __init__(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("DataCleaner received an empty DataFrame.")
        self.df = df.copy()

    def clean(
        self,
        date_columns: list[str] | None = None,
        numeric_columns: list[str] | None = None,
        drop_duplicate_subset: list[str] | None = None,
        fill_strategy: dict | None = None,
    ) -> pd.DataFrame:
        """Run the full cleaning pipeline.

        Parameters
        ----------
        date_columns          : columns to coerce to datetime.
        numeric_columns       : columns to coerce to float.
        drop_duplicate_subset : subset of columns for duplicate detection.
        fill_strategy         : {col: value_or_"mean"/"median"/"mode"}.
        """
        logger.info("── Cleaning pipeline started ──────────────────────────────")
        self._strip_whitespace()
        self._normalise_column_names()
        self._drop_fully_empty_rows()
        self._coerce_dates(date_columns or [])
        self._coerce_numerics(numeric_columns or [])
        self._handle_duplicates(drop_duplicate_subset)
        self._fill_missing(fill_strategy or {})
        logger.info("── Cleaning complete: %d rows × %d cols ────────────────────", *self.df.shape)
        return self.df

    def _strip_whitespace(self):
        self.df.columns = self.df.columns.str.strip()
        str_cols = self.df.select_dtypes(include="object").columns
        self.df[str_cols] = self.df[str_cols].apply(
            lambda s: s.str.strip() if hasattr(s, "str") else s
        )

    def _normalise_column_names(self):
        self.df.columns = (
            self.df.columns
            .str.lower()
            .str.replace(r"[\s\-]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )

    def _drop_fully_empty_rows(self):
        before = len(self.df)
        self.df.dropna(how="all", inplace=True)
        dropped = before - len(self.df)
        if dropped:
            logger.warning("Dropped %d fully-empty rows.", dropped)

    def _coerce_dates(self, cols: list[str]):
        for col in cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce")
                nulls = self.df[col].isna().sum()
                if nulls:
                    logger.warning("'%s': %d values could not be parsed as dates.", col, nulls)

    def _coerce_numerics(self, cols: list[str]):
        for col in cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
                nulls = self.df[col].isna().sum()
                if nulls:
                    logger.warning("'%s': %d non-numeric values → NaN.", col, nulls)

    def _handle_duplicates(self, subset: list[str] | None):
        before = len(self.df)
        self.df.drop_duplicates(subset=subset, inplace=True)
        dropped = before - len(self.df)
        if dropped:
            logger.warning("Removed %d duplicate rows.", dropped)

    def _fill_missing(self, strategy: dict):
        """strategy = {col: "mean" | "median" | "mode" | scalar}"""
        for col, method in strategy.items():
            if col not in self.df.columns:
                continue
            if method == "mean":
                fill_val = self.df[col].mean()
            elif method == "median":
                fill_val = self.df[col].median()
            elif method == "mode":
                fill_val = self.df[col].mode().iloc[0]
            else:
                fill_val = method
            count = self.df[col].isna().sum()
            self.df[col] = self.df[col].fillna(fill_val)
            if count:
                logger.info("'%s': filled %d NaN(s) with %s=%s.", col, count, method, fill_val)


# ══════════════════════════════════════════════════════════════════════════════
# DATA ANALYZER
# ══════════════════════════════════════════════════════════════════════════════

class DataAnalyzer:
    """Compute KPIs and aggregated summaries from a clean DataFrame."""

    def __init__(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("DataAnalyzer received an empty DataFrame.")
        self.df = df
        self.results: dict = {}

    def analyze(
        self,
        group_by: str,
        value_col: str,
        date_col: str | None = None,
        category_col: str | None = None,
    ) -> dict:
        """Run the full analysis pipeline.

        Returns dict with keys:
            summary_by_group, overall_kpis, top_performers,
            bottom_performers, monthly_trend (opt), category_breakdown (opt)
        """
        logger.info("── Analysis pipeline started ───────────────────────────────")
        self.results["summary_by_group"]  = self._summary_by_group(group_by, value_col)
        self.results["overall_kpis"]      = self._overall_kpis(value_col)
        self.results["top_performers"]    = self._top_performers(group_by, value_col)
        self.results["bottom_performers"] = self._bottom_performers(group_by, value_col)

        if date_col and date_col in self.df.columns:
            self.results["monthly_trend"] = self._monthly_trend(date_col, value_col)

        if category_col and category_col in self.df.columns:
            self.results["category_breakdown"] = self._category_breakdown(
                group_by, category_col, value_col
            )
        logger.info("── Analysis complete ───────────────────────────────────────")
        return self.results

    def _summary_by_group(self, group_by: str, value_col: str) -> pd.DataFrame:
        agg = (
            self.df.groupby(group_by)[value_col]
            .agg(Total="sum", Average="mean", Count="count", Min="min", Max="max")
            .reset_index()
        )
        agg["% of Total"] = (agg["Total"] / agg["Total"].sum() * 100).round(2)
        agg["Total"]   = agg["Total"].round(2)
        agg["Average"] = agg["Average"].round(2)
        agg.sort_values("Total", ascending=False, inplace=True)
        return agg

    def _overall_kpis(self, value_col: str) -> dict:
        series = self.df[value_col].dropna()
        kpis = {
            "Grand Total":     round(float(series.sum()), 2),
            "Overall Average": round(float(series.mean()), 2),
            "Median":          round(float(series.median()), 2),
            "Std Dev":         round(float(series.std()), 2),
            "Min":             round(float(series.min()), 2),
            "Max":             round(float(series.max()), 2),
            "Record Count":    int(len(series)),
        }
        logger.info("KPIs → %s", kpis)
        return kpis

    def _top_performers(self, group_by: str, value_col: str, n: int = 5) -> pd.DataFrame:
        return self._summary_by_group(group_by, value_col).head(n).reset_index(drop=True)

    def _bottom_performers(self, group_by: str, value_col: str, n: int = 5) -> pd.DataFrame:
        return self._summary_by_group(group_by, value_col).tail(n).reset_index(drop=True)

    def _monthly_trend(self, date_col: str, value_col: str) -> pd.DataFrame:
        tmp = self.df[[date_col, value_col]].copy()
        tmp[date_col] = pd.to_datetime(tmp[date_col], errors="coerce")
        tmp.dropna(subset=[date_col], inplace=True)
        tmp["Month"] = tmp[date_col].dt.to_period("M").astype(str)
        trend = (
            tmp.groupby("Month")[value_col]
            .agg(Total="sum", Count="count")
            .reset_index()
        )
        trend["Total"] = trend["Total"].round(2)
        trend["MoM Growth %"] = (trend["Total"].pct_change() * 100).round(2)
        return trend

    def _category_breakdown(self, group_by: str, category_col: str,
                             value_col: str) -> pd.DataFrame:
        pivot = (
            self.df.pivot_table(
                index=group_by, columns=category_col,
                values=value_col, aggfunc="sum", fill_value=0,
            ).round(2)
        )
        pivot.reset_index(inplace=True)
        return pivot
