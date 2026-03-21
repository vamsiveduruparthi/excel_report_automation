# 📊 Excel Report Automation

Automatically reads raw data from **multiple sources** (CSV, Excel, APIs, Databases), cleans it, analyses it, and produces a professionally formatted multi-sheet Excel report with charts — no manual work required.

---

## Problem Statement

Most organisations still create Excel reports by hand: copy-pasting data, applying formatting, drawing charts. This is slow, error-prone, and impossible to scale. This project replaces that workflow with a single Python pipeline.

**Unique feature:** unlike most scripts that only accept one data format, this system ingests CSV, Excel, REST APIs, and SQL databases simultaneously and merges them automatically.

---

## Tools Used

| Tool | Purpose |
|------|---------|
| Python 3.11+ | Core language |
| Pandas | Data manipulation & aggregation |
| OpenPyXL | Excel file creation & formatting |
| Matplotlib / Seaborn | Chart generation |
| Requests | REST API data fetching |
| SQLAlchemy / sqlite3 | Database connectivity |
| logging | Audit trail |

---

## Project Architecture

```
User / Scheduler
      │
      ▼
  main.py  ──── orchestrates ──────────────────────────────────────┐
      │                                                             │
      ▼                                                             ▼
data_loader.py                                          report_generator.py
  ├── CSVLoader          ┐                               ├── Executive Summary sheet
  ├── ExcelLoader        ├── MultiSourceLoader           ├── Detailed Analysis sheet
  ├── APILoader          ┘   (merges/stacks)             ├── Monthly Trend sheet
  └── DatabaseLoader                                     ├── Category Breakdown sheet
                                                         └── Raw Data sheet
      │
      ▼
data_processor.py
  ├── DataCleaner   (strip, coerce, dedup, fill NaN)
  └── DataAnalyzer  (KPIs, summaries, trends, pivots)
```

---

## Folder Structure

```
excel_report_automation/
├── data_loader.py       # Multi-source ingestion engine
├── data_processor.py    # Cleaning & analysis engine
├── report_generator.py  # Excel report builder
├── main.py              # Pipeline orchestrator + demo data generator
├── requirements.txt     # Python dependencies
├── README.md
├── data/                # Auto-created — raw source files
│   ├── sales_data.csv
│   ├── returns_data.xlsx
│   └── satisfaction.db
├── output/              # Auto-created — final reports land here
│   └── report_YYYYMMDD_HHMMSS.xlsx
└── logs/                # Auto-created — run logs
    └── run_YYYYMMDD_HHMMSS.log
```

---

## How to Run

```bash
# 1. Clone / copy the project folder
cd excel_report_automation

# 2. Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the pipeline
python main.py
```

The report appears in the `output/` folder.

---

## Data Sources Supported

### CSV
```

### Excel


### REST API


### SQL Database


### Combining sources


**## Example Output**

The generated report contains 5 sheets:

| Sheet | Contents |
|-------|---------|
| Executive Summary | KPI scorecards + horizontal bar chart |
| Detailed Analysis | Group-by table (totals, avg, %) + pie chart |
| Monthly Trend | Month-over-month table + line chart |
| Category Breakdown | Pivot cross-tab |
| Raw Data | Full cleaned dataset with frozen headers |

---

## Future Improvements

- **Scheduling** — wrap `main.py` with `APScheduler` or a cron job for daily auto-reports.
- **Email delivery** — attach the report and send via `smtplib` or SendGrid.
- **Config file** — move all parameters (paths, column names, filters) to a `config.yaml`.
- **Cloud storage** — push the final `.xlsx` to S3, Azure Blob, or Google Drive.
- **Dashboard** — stream KPIs to a Streamlit or Dash web app alongside the Excel report.
- **More chart types** — waterfall, scatter, heat-map correlation matrix.
- **Delta reports** — compare current period vs prior period automatically.
- **Unit tests** — `pytest` suite covering each module.
