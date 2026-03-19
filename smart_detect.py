"""
smart_detect.py
===============
Intelligent Column Detection Engine.

Analyses the ACTUAL DATA — not just column names — to reliably detect:
  - group_col  : best categorical column to group by
  - value_col  : best numeric column to analyse
  - date_col   : date/time column for trend analysis
  - cat_col    : secondary category for cross-tab pivot

Works on ANY dataset regardless of column names:
  weather data, sales data, student records, medical data,
  IoT sensor data, survey results, e-commerce, HR data — everything.

Detection Strategy (in priority order)
---------------------------------------
1. Name hints       — known keywords like "revenue", "region", "date"
2. Data-type scan   — object columns = candidates for group_col
3. Cardinality check— low unique-count columns = good grouping columns
4. Content analysis — looks at actual cell values (dates, numbers in strings)
5. Smart scoring    — scores every column and picks the best fit
"""

import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)

# ── Name-hint dictionaries (broad, covers most real-world datasets) ────────────

_VALUE_HINTS = {
    # finance / business
    "revenue","sales","amount","total","price","value","income","profit",
    "cost","expense","budget","salary","wage","fee","payment","turnover",
    "gross","net","margin","earning","spend","expenditure","quantity","qty",
    # science / weather / IoT
    "temperature","temp","humidity","pressure","wind","rainfall","precipitation",
    "speed","distance","weight","height","depth","area","volume","density",
    # analytics / web
    "score","rate","ratio","percentage","percent","pct","count","number",
    "views","clicks","sessions","conversions","engagement","rating","rank",
    # generic
    "num","val","measure","metric","stat","figure","size","age","duration",
    "hours","days","units","items","purchases","orders",
}

_GROUP_HINTS = {
    # geography
    "region","area","zone","territory","country","nation","state","province",
    "city","town","district","location","place","continent","market",
    # business
    "department","division","team","branch","store","shop","outlet","channel",
    "segment","category","type","class","group","tier","level","grade",
    "product","item","sku","brand","model","variant","series",
    # people / demographics
    "gender","sex","education","occupation","role","position",
    "status","label","tag","source","platform","device","os","browser",
    # generic
    "name","title","kind","mode",
}

_DATE_HINTS = {
    "date","time","datetime","timestamp","created","updated","modified",
    "created_at","updated_at","order_date","sale_date","purchase_date",
    "recorded","reported","logged","day","week","month","year","period",
    "quarter","fiscal","event_time","transaction_date",
}

# Date pattern regexes for content sniffing
_DATE_RE = [re.compile(p) for p in [
    r"^\d{4}-\d{2}-\d{2}",          # 2024-01-15
    r"^\d{2}/\d{2}/\d{4}",          # 01/15/2024
    r"^\d{2}-\d{2}-\d{4}",          # 01-15-2024
    r"^\d{4}/\d{2}/\d{2}",          # 2024/01/15
    r"^\d{1,2}\s+\w+\s+\d{4}",      # 15 Jan 2024
    r"^\w+\s+\d{1,2},?\s+\d{4}",    # January 15, 2024
    r"^\d{4}-\d{2}-\d{2}T\d{2}",    # ISO datetime
]]


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL SCORERS
# ══════════════════════════════════════════════════════════════════════════════

def _looks_like_date(series: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    if series.dtype != object:
        return False
    sample = series.dropna().head(10).astype(str)
    hits = sum(any(p.match(v) for p in _DATE_RE) for v in sample)
    return hits >= max(1, len(sample) * 0.6)


def _score_as_group(col: str, series: pd.Series, total: int) -> float:
    score = 0.0
    col_l = col.lower().replace(" ","_").replace("-","_")

    # Name hint
    if any(h in col_l for h in _GROUP_HINTS):
        score += 40

    # Object dtype preferred
    if series.dtype == object:
        score += 25

    # Cardinality scoring — independent checks so multiple penalties can stack
    n = series.nunique()
    if   2  <= n <= 10:  score += 30
    elif 11 <= n <= 30:  score += 20
    elif 31 <= n <= 50:  score += 10
    elif n == 1:         score -= 50   # single value = useless

    # Apply near-unique and all-unique penalties SEPARATELY (not elif)
    if n > total * 0.8:
        score -= 40                    # near-unique = likely an ID or free-text
    if n == total and series.dtype == object:
        score -= 50                    # every value is unique = name / free-text field

    # Penalise ID-like names
    if any(k in col_l for k in ("_id","uuid","key","hash","index","row_num","sku","code","ref","no_","_no","_nr","_num","_key","_ref")):
        score -= 40
    # Penalise if col name IS exactly a known ID pattern
    if col_l in ("id","sku","uuid","code","ref","key","no","nr","num","index","hash"):
        score -= 50
    # Penalise numeric-aggregate column names (these are measures, not categories)
    if any(k in col_l for k in ("_cap","market_cap","volume","vol_","_vol","amount",
                                 "revenue","salary","price","spend","cost","profit",
                                 "income","total","score","rate","ratio","percent",
                                 "pct","count","avg","mean","sum","min","max")):
        score -= 40
    # Penalise percentage-like columns (0-100 range floats) — they are measures not groups
    if pd.api.types.is_float_dtype(series):
        vals = series.dropna()
        if len(vals) > 0 and vals.between(0, 100).all() and "percent" in col_l or "pct" in col_l or "ratio" in col_l:
            score -= 30

    # Penalise numeric dtype
    if pd.api.types.is_numeric_dtype(series):
        score -= 20

    # Penalise date-like content
    if _looks_like_date(series):
        score -= 40

    return score


def _score_as_value(col: str, series: pd.Series, total: int) -> float:
    # Must be numeric — hard gate
    if not pd.api.types.is_numeric_dtype(series):
        return -9999.0

    score = 0.0
    col_l = col.lower().replace(" ","_").replace("-","_")

    # Primary name hints get a bigger bonus (most unambiguous measure words)
    _PRIMARY_VALUE = {"revenue","sales","amount","price","salary","spend","cost",
                      "profit","income","temperature","temp","score","rating"}
    if any(h in col_l for h in _PRIMARY_VALUE):
        score += 60
    elif any(h in col_l for h in _VALUE_HINTS):
        score += 40

    # High coefficient of variation = rich signal
    try:
        mean = series.mean()
        if mean and mean != 0:
            score += min((series.std() / abs(mean)) * 10, 20)
    except Exception:
        pass

    # Penalise ID / index columns
    if any(k in col_l for k in ("_id","uuid","index","row","key","code")):
        score -= 30

    # Penalise boolean-like (0/1 only)
    if set(series.dropna().unique()).issubset({0, 1}):
        score -= 20

    # Penalise year-like columns (integers in 1900–2100)
    try:
        if series.dropna().between(1900, 2100).all() and series.nunique() < 200:
            score -= 25
    except Exception:
        pass

    # Higher cardinality = likely a continuous measure
    if series.nunique() > total * 0.3:
        score += 10

    return score


def _score_as_date(col: str, series: pd.Series) -> float:
    if pd.api.types.is_datetime64_any_dtype(series):
        return 100.0
    score = 0.0
    col_l = col.lower().replace(" ","_").replace("-","_")
    if any(h in col_l for h in _DATE_HINTS):
        score += 50
    if _looks_like_date(series):
        score += 40
    return score


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def smart_detect(df: pd.DataFrame) -> dict:
    """
    Intelligently detect the best columns for group_by, value, date, category.

    Combines name-hint matching + dtype analysis + cardinality scoring
    + content sniffing to work on ANY dataset regardless of column names.

    Returns
    -------
    dict: {group_col, value_col, date_col, cat_col}  — any may be None
    """
    if df.empty or len(df.columns) == 0:
        return {"group_col":None,"value_col":None,"date_col":None,"cat_col":None}

    total = len(df)
    cols  = list(df.columns)

    # Score every column for every role
    g_scores = {c: _score_as_group(c, df[c], total) for c in cols}
    v_scores = {c: _score_as_value(c, df[c], total) for c in cols}
    d_scores = {c: _score_as_date(c,  df[c])        for c in cols}

    # ── 1. date_col — pick first, least ambiguous ────────────────────────────
    date_col = max(d_scores, key=d_scores.get)
    date_col = date_col if d_scores[date_col] >= 20 else None

    # ── 2. value_col — must be numeric, exclude date_col ────────────────────
    v_candidates = {c:s for c,s in v_scores.items()
                    if s > -9999 and c != date_col}
    # Stable pick: on equal scores, prefer the column that appears first in the DataFrame
    value_col = (
        max(v_candidates, key=lambda c: (v_candidates[c], -list(df.columns).index(c)))
        if v_candidates else None
    )

    # ── 3. group_col — exclude value_col and date_col ────────────────────────
    g_candidates = {c:s for c,s in g_scores.items()
                    if c != value_col and c != date_col}

    if g_candidates:
        group_col = max(g_candidates, key=lambda c: (g_candidates[c], -list(df.columns).index(c)))

        # Safety: if winner is high-cardinality numeric, prefer an object column
        if (pd.api.types.is_numeric_dtype(df[group_col])
                and df[group_col].nunique() > 20):
            obj_fallbacks = [c for c in g_candidates
                             if df[c].dtype == object and c != value_col]
            if obj_fallbacks:
                group_col = max(obj_fallbacks, key=lambda c: g_candidates[c])
    else:
        group_col = None

    # ── 4. cat_col — second-best object column ───────────────────────────────
    cat_candidates = {c:s for c,s in g_candidates.items()
                      if c != group_col and df[c].dtype == object}
    cat_col = max(cat_candidates, key=cat_candidates.get) if cat_candidates else None

    result = {"group_col":group_col, "value_col":value_col,
              "date_col":date_col,   "cat_col":cat_col}

    logger.info("smart_detect → %s", result)
    logger.debug("value scores (top5): %s",
                 sorted(v_scores.items(), key=lambda x:-x[1])[:5])
    logger.debug("group scores (top5): %s",
                 sorted(g_scores.items(), key=lambda x:-x[1])[:5])
    return result


def explain_detection(df: pd.DataFrame) -> str:
    """Return a markdown string explaining what was detected and why."""
    result = smart_detect(df)
    total  = len(df)
    lines  = ["**Auto-detected columns:**\n"]
    for role, col in result.items():
        if col is None:
            lines.append(f"- **{role}**: *not found*")
        else:
            n = df[col].nunique()
            d = str(df[col].dtype)
            lines.append(f"- **{role}**: `{col}` &nbsp;(dtype=`{d}`, unique={n}/{total})")
    return "\n".join(lines)
