"""
streamlit_app.py
================
Recruiter-facing live demo — Excel Report Automation.
Run with:  streamlit run streamlit_app.py
"""

import logging
import random
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import seaborn as sns
import streamlit as st

from data_loader      import APILoader, DatabaseLoader
from data_processor   import DataCleaner, DataAnalyzer
from smart_detect import smart_detect, explain_detection
from file_reader      import read_any_file, file_icon, SUPPORTED_EXTENSIONS
from report_generator import ReportGenerator

# ══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG + CSS
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Excel Report Automation",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');
html,body,[class*="css"]{font-family:'DM Sans',sans-serif}

[data-testid="stSidebar"]{background:#0f1117;border-right:1px solid #1e2130}
[data-testid="stSidebar"] *{color:#e2e8f0!important}
[data-testid="stSidebar"] .stRadio label{font-size:13px!important}

.hero{background:linear-gradient(135deg,#1F3864 0%,#2E75B6 100%);
      border-radius:12px;padding:2rem 2.5rem;margin-bottom:1.5rem;
      display:flex;align-items:center;justify-content:space-between}
.hero-title{font-size:28px;font-weight:600;color:#fff;margin:0}
.hero-sub  {font-size:14px;color:#93c5fd;margin:.3rem 0 0}
.hero-badge{background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.2);
            border-radius:20px;padding:6px 16px;font-size:12px;color:#fff;font-weight:500}

.kpi-card {background:#fff;border:1px solid #e2e8f0;border-radius:10px;
           padding:1.1rem 1.25rem;text-align:center;height:100%}
.kpi-label{font-size:11px;color:#64748b;text-transform:uppercase;
           letter-spacing:.06em;font-weight:500}
.kpi-value{font-size:26px;font-weight:600;color:#1F3864;margin:.2rem 0 0}

.step-pill{display:inline-flex;align-items:center;gap:6px;background:#EFF6FF;
           border:1px solid #BFDBFE;border-radius:20px;padding:4px 12px;
           font-size:12px;color:#1d4ed8;font-weight:500;margin:0 4px 6px 0}

.tech-badge{background:#f1f5f9;border:1px solid #e2e8f0;border-radius:6px;
            padding:4px 10px;font-size:12px;color:#334155;font-weight:500;
            display:inline-block;margin:2px}

.fmt-chip {background:#1e2130;border:1px solid #2d3748;border-radius:6px;
           padding:3px 9px;font-size:11px;color:#94a3b8;font-family:'DM Mono',monospace;
           display:inline-block;margin:2px}

.log-box  {background:#0f1117;border-radius:8px;padding:1rem 1.25rem;
           font-family:'DM Mono',monospace;font-size:12px;color:#4ade80;
           max-height:220px;overflow-y:auto;line-height:1.7}

.stDownloadButton>button{background:#1F3864!important;color:#fff!important;
                         border:none!important;border-radius:8px!important;
                         font-weight:500!important;font-size:14px!important}
.stTabs [aria-selected="true"]{background:#1F3864!important;color:#fff!important}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# DEMO DATA
# ══════════════════════════════════════════════════════════════════════════════

REGIONS    = ["North","South","East","West","Central"]
PRODUCTS   = ["Laptop","Tablet","Phone","Monitor","Keyboard","Mouse"]
CATEGORIES = ["Electronics","Accessories","Peripherals"]
REPS       = [f"Rep_{i:02d}" for i in range(1, 11)]


def _rdate(s, e):
    return (s + timedelta(days=random.randint(0,(e-s).days))).strftime("%Y-%m-%d")


@st.cache_data(show_spinner=False)
def make_demo_frames():
    random.seed(42)
    s, e = datetime(2024,1,1), datetime(2024,12,31)
    rows = []
    for i in range(1, 201):
        qty, price = random.randint(1,50), round(random.uniform(50,2000),2)
        disc = round(random.uniform(0,0.3),2)
        rows.append({
            "order_id": f"ORD-{i:04d}", "date": _rdate(s,e),
            "product": random.choice(PRODUCTS), "region": random.choice(REGIONS),
            "sales_rep": random.choice(REPS), "quantity": qty,
            "unit_price": price, "discount": disc,
            "revenue": round(qty*price*(1-disc),2),
        })
    rows[10]["revenue"] = None; rows[20]["region"] = "  north  "
    csv_df = pd.DataFrame(rows)

    random.seed(7)
    xl_df = pd.DataFrame([{
        "order_id":      f"ORD-{random.randint(1,200):04d}",
        "return_reason": random.choice(["Defective","Wrong item","Changed mind"]),
        "return_amount": round(random.uniform(50,1500),2),
        "category":      random.choice(CATEGORIES),
    } for _ in range(80)])

    random.seed(99)
    db_df = pd.DataFrame([{
        "order_id":   f"ORD-{random.randint(1,200):04d}",
        "region":     random.choice(REGIONS),
        "nps_score":  random.randint(0,10),
        "csat_score": round(random.uniform(1,5),1),
    } for _ in range(120)])

    return csv_df, xl_df, db_df


# ══════════════════════════════════════════════════════════════════════════════
# SHARED PIPELINE UTILITIES
# ══════════════════════════════════════════════════════════════════════════════



def _frames_have_overlap(frames: list) -> bool:
    """Check if multiple DataFrames share enough columns to be meaningfully stacked."""
    if len(frames) <= 1:
        return True
    col_sets = [set(df.columns.str.lower()) for df in frames]
    # Check overlap between first frame and all others
    base = col_sets[0]
    for other in col_sets[1:]:
        overlap = base & other
        # Need at least 2 common columns (not just an ID column)
        if len(overlap) >= 2:
            return True
    return False


def _best_frame(frames: list) -> pd.DataFrame:
    """Pick the best single DataFrame when frames cannot be meaningfully stacked.
    Prefers: most numeric columns, then most rows."""
    def score(df):
        num_cols = len(df.select_dtypes(include="number").columns)
        return (num_cols, len(df))
    return max(frames, key=score)


def run_pipeline(frames, log_fn, title, group_col=None, value_col=None, date_col=None):
    if len(frames) > 1 and not _frames_have_overlap(frames):
        log_fn(f"⚠️  {len(frames)} files have no common columns — analysing each separately")
        # Generate one combined report using the best frame for analysis
        # but stack all for the Raw Data sheet
        best_df  = _best_frame(frames)
        raw_df   = best_df
        all_raw  = pd.concat(frames, ignore_index=True)
        log_fn(f"Best file selected: {raw_df.shape[0]:,} rows × {raw_df.shape[1]} cols")
        log_fn(f"(Raw Data sheet will show all {all_raw.shape[0]:,} rows combined)")
    else:
        log_fn(f"Stacking {len(frames)} source(s) …")
        raw_df  = pd.concat(frames, ignore_index=True)
        all_raw = raw_df
        log_fn(f"Combined → {raw_df.shape[0]:,} rows × {raw_df.shape[1]} cols")

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
    log_fn(f"Columns  → group={cols['group_col']}  value={cols['value_col']}  date={cols['date_col']}")

    # Guard checks run AFTER cleaning (DataCleaner normalises column names to lowercase)

    log_fn("Running DataCleaner …")
    num_cols = raw_df.select_dtypes(include="number").columns.tolist()
    clean_df = DataCleaner(raw_df).clean(
        date_columns    = [cols["date_col"]] if cols["date_col"] else [],
        numeric_columns = num_cols,
        fill_strategy   = {c: "mean" for c in num_cols},
    )
    log_fn(f"Cleaned  → {clean_df.shape[0]:,} rows (NaNs filled, dupes removed)")

    # Post-clean validation (columns are now lowercased by DataCleaner)
    if not cols["value_col"] or cols["value_col"] not in clean_df.columns:
        raise ValueError(
            f"Value column '{cols['value_col']}' not found after cleaning. "
            f"Available: {list(clean_df.columns)}"
        )
    if not cols["group_col"] or cols["group_col"] not in clean_df.columns:
        cols["group_col"] = clean_df.columns[0]

    log_fn("Running DataAnalyzer …")
    results = DataAnalyzer(clean_df).analyze(
        group_by     = cols["group_col"],
        value_col    = cols["value_col"],
        date_col     = cols["date_col"],
        category_col = cols["cat_col"],
    )
    kpis = results.get("overall_kpis", {})
    log_fn(f"KPIs     → Total={kpis.get('Grand Total','?')}  Avg={kpis.get('Overall Average','?')}")

    log_fn("Building Excel report …")
    with tempfile.TemporaryDirectory() as tmp:
        path = ReportGenerator(
            analysis_results=results,
            raw_df=clean_df,
            report_title=title, group_col=cols["group_col"],
            value_col=cols["value_col"], output_dir=tmp,
        ).generate()
        report_bytes = open(path, "rb").read()

    log_fn(f"✅  Report ready  ({len(report_bytes)//1024} KB, 5 sheets)")
    return report_bytes, clean_df, results, cols, all_raw


# ══════════════════════════════════════════════════════════════════════════════
# CHART HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def chart_bar(summary, group_col):
    fig, ax = plt.subplots(figsize=(6,4))
    palette = sns.color_palette("Blues_r", len(summary))
    bars = ax.barh(summary[group_col], summary["Total"], color=palette, edgecolor="white")
    ax.set_xlabel("Total", fontsize=9)
    ax.set_title(f"Revenue by {group_col.title()}", fontsize=11, fontweight="bold")
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.spines[["top","right"]].set_visible(False)
    ax.bar_label(bars, fmt="${:,.0f}", padding=3, fontsize=8)
    fig.tight_layout(); return fig


def chart_pie(summary, group_col):
    fig, ax = plt.subplots(figsize=(5,4))
    colors = sns.color_palette("Set2", len(summary))
    ax.pie(summary["Total"], labels=summary[group_col], autopct="%1.1f%%",
           colors=colors, startangle=140, wedgeprops=dict(linewidth=0.8, edgecolor="white"))
    ax.set_title("Share of Total", fontsize=11, fontweight="bold")
    fig.tight_layout(); return fig


def chart_line(trend):
    fig, ax = plt.subplots(figsize=(7,4))
    ax.plot(trend["Month"], trend["Total"], marker="o", linewidth=2.5,
            color="#2E75B6", markerfacecolor="#E8612C", markersize=7)
    ax.fill_between(trend["Month"], trend["Total"], alpha=0.1, color="#2E75B6")
    ax.set_title("Monthly Revenue Trend", fontsize=11, fontweight="bold")
    ax.set_xlabel("Month", fontsize=9); ax.set_ylabel("Revenue ($)", fontsize=9)
    plt.xticks(rotation=45, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.spines[["top","right"]].set_visible(False)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout(); return fig


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### 📊 Report Automation")
    st.markdown(
        "<div style='font-size:12px;color:#64748b;margin-bottom:1rem'>"
        "Configure your data sources below</div>",
        unsafe_allow_html=True,
    )

    mode = st.radio(
        "Mode",
        ["🎲  Demo data", "📁  Upload files", "🌐  API / Database"],
        label_visibility="collapsed",
    )
    st.markdown("---")

    any_files = []
    api_url = api_key_path = db_conn = db_query = None

    if "Upload" in mode:
        # Show accepted formats
        st.markdown(
            "<div style='font-size:11px;color:#94a3b8;margin-bottom:.5rem'>"
            "Accepted: CSV · TSV · Excel · JSON · JSONL · XML · SQLite · Parquet · ZIP"
            "</div>",
            unsafe_allow_html=True,
        )
        any_files = st.file_uploader(
            "Drop any data files here — mix formats freely",
            accept_multiple_files=True,   # ← no type= restriction
            label_visibility="collapsed",
            key="any_files",
        )

    elif "API" in mode:
        st.markdown("**REST API**")
        api_url      = st.text_input("Endpoint URL", placeholder="https://api.example.com/data")
        api_key_path = st.text_input("JSON data key", placeholder="results  (blank = root list)")
        st.markdown("**SQL Database**")
        db_conn  = st.text_input("Connection", placeholder="path/to/file.db")
        db_query = st.text_area("SQL Query", placeholder="SELECT * FROM sales", height=80)

    st.markdown("---")
    st.markdown("**Column overrides** *(auto-detected if blank)*")
    ov_group = st.text_input("Group by column",  placeholder="e.g. region")
    ov_value = st.text_input("Value column",      placeholder="e.g. revenue")
    ov_date  = st.text_input("Date column",       placeholder="e.g. date")
    ov_title = st.text_input("Report title",      placeholder="Sales Report — FY 2024")

    st.markdown("---")
    run_btn = st.button("⚡  Generate Report", use_container_width=True, type="primary")


# ══════════════════════════════════════════════════════════════════════════════
# HERO BANNER
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<div class="hero">
  <div>
    <div class="hero-title">📊 Excel Report Automation</div>
    <div class="hero-sub">Multi-source · Auto-clean · Professional Excel output · Live demo</div>
  </div>
  <div class="hero-badge">🟢 Live Demo</div>
</div>""", unsafe_allow_html=True)

st.markdown("""
<div style="margin-bottom:1rem">
  <span class="tech-badge">Python 3.11</span>
  <span class="tech-badge">Pandas</span>
  <span class="tech-badge">OpenPyXL</span>
  <span class="tech-badge">Matplotlib · Seaborn</span>
  <span class="tech-badge">Streamlit</span>
  <span class="tech-badge">SQLite · SQLAlchemy</span>
  <span class="tech-badge">REST APIs</span>
  <span class="tech-badge">Modular Architecture</span>
</div>
<div style="margin-bottom:1.5rem">
  <span class="step-pill">① Universal File Reader</span> →
  <span class="step-pill">② Multi-Source Loader</span> →
  <span class="step-pill">③ Data Cleaner</span> →
  <span class="step-pill">④ KPI Analyzer</span> →
  <span class="step-pill">⑤ Excel Generator</span> →
  <span class="step-pill">⑥ Download .xlsx</span>
</div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE — runs on button click
# ══════════════════════════════════════════════════════════════════════════════

if run_btn:
    frames       = []
    log_lines    = []
    sources_used = []

    def log(msg):
        ts = datetime.now().strftime("%H:%M:%S")
        log_lines.append(f"[{ts}]  {msg}")

    with st.spinner("Loading data sources …"):

        # ── Demo ──────────────────────────────────────────────────────────
        if "Demo" in mode:
            csv_df, xl_df, db_df = make_demo_frames()
            frames = [csv_df, xl_df, db_df]
            sources_used = [
                "📄 demo_sales.csv (200 rows · CSV)",
                "📊 demo_returns.xlsx (80 rows · Excel)",
                "🗄️ demo_nps.db (120 rows · SQLite)",
            ]
            log("Demo mode — 3 sources loaded")

        # ── Universal file upload ─────────────────────────────────────────
        elif "Upload" in mode:
            if not any_files:
                st.warning("⚠️  Please upload at least one file.")
                st.stop()

            for uploaded in any_files:
                try:
                    file_results = read_any_file(uploaded)
                    for label, df in file_results:
                        frames.append(df)
                        icon = file_icon(uploaded.name)
                        tag  = f"{icon} {label} ({len(df):,} rows × {len(df.columns)} cols)"
                        sources_used.append(tag)
                        log(f"Loaded {label}: {len(df):,} rows × {len(df.columns)} cols")
                except Exception as ex:
                    st.error(f"❌  Could not read **{uploaded.name}**: {ex}")
                    log(f"ERROR reading {uploaded.name}: {ex}")

        # ── API / Database ────────────────────────────────────────────────
        elif "API" in mode:
            if api_url:
                try:
                    df = APILoader(api_url, data_key=api_key_path or None).load()
                    frames.append(df)
                    sources_used.append(f"🌐 {api_url} ({len(df):,} rows)")
                    log(f"API loaded: {len(df):,} rows")
                except Exception as ex:
                    st.error(f"API error: {ex}")
            if db_conn and db_query:
                try:
                    df = DatabaseLoader(db_conn, db_query).load()
                    frames.append(df)
                    sources_used.append(f"🗄️ {db_conn} ({len(df):,} rows)")
                    log(f"DB loaded: {len(df):,} rows")
                except Exception as ex:
                    st.error(f"DB error: {ex}")

    if not frames:
        st.warning("⚠️  No data loaded. Please check your sources.")
        st.stop()

    title = ov_title.strip() or f"Report — {datetime.now().strftime('%d %b %Y')}"

    try:
        with st.spinner("Running pipeline …"):
            report_bytes, clean_df, results, cols, all_raw = run_pipeline(
                frames, log, title,
                group_col = ov_group.strip() or None,
                value_col = ov_value.strip() or None,
                date_col  = ov_date.strip()  or None,
            )
        detection_info = explain_detection(pd.concat(frames, ignore_index=True))
        st.session_state.update({
            "report_bytes": report_bytes, "clean_df": clean_df,
            "results": results, "cols": cols, "all_raw": all_raw,
            "sources_used": sources_used, "log_lines": log_lines,
            "detection_info": detection_info, "ran": True,
        })
    except Exception as ex:
        st.error(f"Pipeline error: {ex}")
        st.stop()


# ══════════════════════════════════════════════════════════════════════════════
# RESULTS — shown after pipeline runs
# ══════════════════════════════════════════════════════════════════════════════

if st.session_state.get("ran"):
    report_bytes = st.session_state["report_bytes"]
    clean_df     = st.session_state["clean_df"]
    results      = st.session_state["results"]
    cols         = st.session_state["cols"]
    sources_used   = st.session_state["sources_used"]
    log_lines      = st.session_state["log_lines"]
    detection_info = st.session_state.get("detection_info", "")
    all_raw        = st.session_state.get("all_raw", clean_df)

    # ── Success + download ────────────────────────────────────────────────
    c1, c2 = st.columns([3, 1])
    with c1:
        st.success(f"✅  Report generated — {len(report_bytes)//1024} KB · 5 sheets · charts embedded")
    with c2:
        st.download_button(
            "⬇  Download .xlsx", data=report_bytes,
            file_name=f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # ── Sources used ──────────────────────────────────────────────────────
    st.markdown("**Sources loaded:**")
    src_cols = st.columns(max(len(sources_used), 1))
    for i, s in enumerate(sources_used):
        src_cols[i % len(src_cols)].info(s)

    # ── Auto-detection info ──────────────────────────────────────────────
    with st.expander("🔍 Auto-detected columns — click to see why", expanded=False):
        st.markdown(detection_info)
        st.markdown(
            "<div style='font-size:12px;color:#64748b;margin-top:.5rem'>"
            "Override any column using the <b>Column overrides</b> in the sidebar.</div>",
            unsafe_allow_html=True
        )

    # ── KPI cards ─────────────────────────────────────────────────────────
    st.markdown("---")
    kpis = results.get("overall_kpis", {})
    kpi_cols = st.columns(len(kpis))
    for i, (label, value) in enumerate(kpis.items()):
        fmt = f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
        with kpi_cols[i]:
            st.markdown(f"""
            <div class="kpi-card">
              <div class="kpi-label">{label}</div>
              <div class="kpi-value">{fmt}</div>
            </div>""", unsafe_allow_html=True)

    # ── Result tabs ───────────────────────────────────────────────────────
    st.markdown("---")
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Group Analysis", "📈 Monthly Trend",
        "🗂 Category Pivot", "🔢 Raw Data", "📋 Pipeline Log",
    ])

    with tab1:
        summary = results.get("summary_by_group", pd.DataFrame())
        if not summary.empty:
            ca, cb = st.columns(2)
            with ca:
                st.markdown("**Performance by Group**")
                st.dataframe(summary, use_container_width=True, height=320)
            with cb:
                fig = chart_bar(summary, cols["group_col"])
                st.pyplot(fig, use_container_width=True); plt.close(fig)
            fig2 = chart_pie(summary, cols["group_col"])
            st.pyplot(fig2, use_container_width=True); plt.close(fig2)

    with tab2:
        trend = results.get("monthly_trend")
        if trend is not None and not trend.empty:
            ca, cb = st.columns([1, 2])
            with ca:
                st.markdown("**Monthly Trend Table**")
                st.dataframe(trend, use_container_width=True, height=340)
            with cb:
                fig = chart_line(trend)
                st.pyplot(fig, use_container_width=True); plt.close(fig)
        else:
            st.info("No date column detected — trend analysis skipped.")

    with tab3:
        cb_df = results.get("category_breakdown")
        if cb_df is not None and not cb_df.empty:
            st.markdown("**Cross-tab: Group × Category**")
            st.dataframe(cb_df, use_container_width=True)
        else:
            st.info("No secondary category column — cross-tab skipped.")

    with tab4:
        display_df = all_raw if all_raw is not None else clean_df
        st.markdown(f"**Raw dataset — {len(display_df):,} rows × {len(display_df.columns)} cols**")
        st.dataframe(display_df.head(200), use_container_width=True, height=420)
        if len(display_df) > 200:
            st.caption(f"Showing first 200 of {len(display_df):,} rows.")

    with tab5:
        st.markdown("**Live pipeline execution log**")
        st.markdown(
            '<div class="log-box">' + "<br>".join(log_lines) + "</div>",
            unsafe_allow_html=True,
        )

else:
    # ── Landing state (before first run) ──────────────────────────────────
    st.markdown("### How it works")
    c1, c2, c3, c4 = st.columns(4)
    for col, icon, title, desc in zip(
        [c1, c2, c3, c4],
        ["📂", "🧹", "📐", "📥"],
        ["Upload Any File", "Auto-Clean", "KPI Analysis", "Download Report"],
        [
            "CSV, TSV, Excel, JSON, JSONL, XML, SQLite, Parquet, ZIP — mix freely",
            "Strips whitespace, fixes types, fills NaN, removes duplicates",
            "Totals, averages, trends, rankings, category pivots",
            "Formatted 5-sheet Excel with embedded bar, pie & line charts",
        ],
    ):
        with col:
            st.markdown(f"""
            <div class="kpi-card" style="text-align:left">
              <div style="font-size:24px;margin-bottom:.5rem">{icon}</div>
              <div style="font-size:14px;font-weight:600;color:#1F3864;margin-bottom:.3rem">{title}</div>
              <div style="font-size:12px;color:#64748b">{desc}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Show all supported formats
    st.markdown("**Supported file formats:**")
    fmt_html = " ".join(
        f'<span class="fmt-chip">.{ext}</span>'
        for ext in sorted(SUPPORTED_EXTENSIONS)
    )
    st.markdown(fmt_html, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.info("👈  Choose a mode in the sidebar and click **⚡ Generate Report** to run the live demo.")


# ══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown(
    "<div style='font-size:12px;color:#94a3b8;display:flex;"
    "justify-content:space-between'>"
    "<span>Excel Report Automation · Python · Pandas · OpenPyXL · Streamlit</span>"
    "<span>📊 Portfolio Project</span></div>",
    unsafe_allow_html=True,
)
