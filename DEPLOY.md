# 🚀 Deployment Guide — Excel Report Automation

## Final folder structure before deploying

```
excel_report_automation/
├── streamlit_app.py       ← Streamlit entry point
├── data_loader.py
├── data_processor.py
├── report_generator.py
├── main.py
├── requirements.txt       ← includes streamlit
├── packages.txt           ← libgomp1 (for matplotlib on Linux)
├── .streamlit/
│   └── config.toml        ← theme + upload size config
└── README.md
```

---

## Option 1 — Streamlit Community Cloud (FREE · Recommended)

The easiest way to get a public shareable URL for recruiters in under 5 minutes.

### Step 1 — Push to GitHub

```bash
# Inside your project folder
git init
git add .
git commit -m "Initial commit — Excel Report Automation"

# Create a new repo on github.com, then:
git remote add origin https://github.com/YOUR_USERNAME/excel-report-automation.git
git branch -M main
git push -u origin main
```

### Step 2 — Deploy on Streamlit Cloud

1. Go to **https://share.streamlit.io**
2. Sign in with your GitHub account
3. Click **"New app"**
4. Fill in:
   - Repository: `YOUR_USERNAME/excel-report-automation`
   - Branch: `main`
   - Main file path: `streamlit_app.py`
5. Click **"Deploy!"**

Your app will be live at:
```
https://YOUR_USERNAME-excel-report-automation-streamlit-app-XXXX.streamlit.app
```

**That URL is what you share with recruiters.**

---

## Option 2 — Run locally (for interviews / screen shares)

```bash
# Clone your repo
git clone https://github.com/YOUR_USERNAME/excel-report-automation.git
cd excel-report-automation

# Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run
streamlit run streamlit_app.py
```

App opens at **http://localhost:8501**

---

## Option 3 — Deploy on Render (free tier)

1. Go to **https://render.com** and sign in
2. New → Web Service → connect your GitHub repo
3. Settings:
   - Build command: `pip install -r requirements.txt`
   - Start command: `streamlit run streamlit_app.py --server.port $PORT --server.address 0.0.0.0`
4. Click **"Create Web Service"**

---

## Option 4 — Deploy on Railway

```bash
npm install -g @railway/cli
railway login
railway init
railway up
```

Add env variable: `PORT=8501`
Start command: `streamlit run streamlit_app.py --server.port $PORT --server.address 0.0.0.0`

---

## Sharing with Recruiters

### What to include in your portfolio / resume

```
Live Demo:   https://your-app.streamlit.app
GitHub:      https://github.com/YOUR_USERNAME/excel-report-automation
```

### What to say in interviews

> "I built an end-to-end data pipeline that reads from multiple sources
> simultaneously — CSV files, Excel workbooks, REST APIs, and SQL databases.
> It automatically cleans the data, computes KPIs, and generates a
> professionally formatted multi-sheet Excel report with embedded charts.
> The Streamlit UI lets anyone run it without touching code.
> You can try the live demo right now at this URL."

### Demo walkthrough (2 minutes)

1. Open the app → show the landing page (architecture overview)
2. Click **Demo mode** → hit **Generate Report** → walk through the pipeline log
3. Show the KPI cards updating in real time
4. Click through the tabs: Group Analysis chart → Monthly Trend → Category Pivot → Raw Data
5. Hit **Download .xlsx** → open the file → show the 5 formatted sheets with charts
6. Switch to **Upload mode** → drag in a real CSV → show it processes automatically

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: streamlit` | `pip install streamlit` |
| `libgomp` error on Linux | Already handled by `packages.txt` |
| Upload fails on Cloud | Check `maxUploadSize = 50` in `.streamlit/config.toml` |
| Matplotlib blank charts | Already using `matplotlib.use("Agg")` at top of files |
| Port already in use | `streamlit run streamlit_app.py --server.port 8502` |
