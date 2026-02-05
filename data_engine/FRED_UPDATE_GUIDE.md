# FRED Data Update Guide

The FRED fetcher automatically updates and uploads to BigQuery.

### Quick Start

```bash
# Run the complete update pipeline (fetch → update Excel → upload to BigQuery)
python fetch_fred.py
```

### Options

```bash
# Full update (default) - fetch, update Excel, and upload
python fetch_fred.py --mode full-update

# Fetch only - just download to JSON files without updating Excel
python fetch_fred.py --mode fetch-only
```

---

## Manual Update (Alternative)

If you prefer to update the Excel file manually:

1. Update `historic_fred/fred_macro_data.xlsx` with new data
2. Run preprocessing: `python preprocess_fred.py`
3. Upload to BigQuery: `python upload_fred_to_bigquery.py`

---

## How It Works

### Incremental Updates
The fetcher is smart about updates:
- Tracks the last observation date for each series
- Only fetches new data since the last update
- Merges new data with existing Excel data
- Avoids duplicates

### Series Tracked
- **MORTGAGE30US**: 30-Year Mortgage Rate (weekly)
- **CPIAUCSL**: Consumer Price Index (monthly)
- **RHORUSQ156N**: Homeownership Rate (quarterly)
- **HPIPONM226S_PCH**: House Price Index (monthly)
- **GDPC1CTM**: GDP Growth (annual)

---

## Scheduling Automated Updates

### Option 1: Manual Weekly Run
```bash
# Run this once a week
python fetch_fred.py
```

### Option 2: Windows Task Scheduler
1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Weekly (e.g., every Monday)
4. Action: Start a program
   - Program: `python`
   - Arguments: `fetch_fred.py`
   - Start in: `C:\Users\dvolosh\Dev\housing_dashboard\data_engine`

### Option 3: GitHub Actions (Future)
Create `.github/workflows/update-fred.yml` for automated cloud updates

---

## Troubleshooting

**"FRED_API_KEY not found"**
- Make sure your `.env` file has `FRED_API_KEY=your_key_here`

**"Excel file not found"**
- Verify `historic_fred/fred_macro_data.xlsx` exists

**"No new observations"**
- This is normal if FRED hasn't published new data since your last update

---

## Files

- **fetch_fred.py**: Main update script
- **fred_raw/**: Downloaded JSON data from API
- **historic_fred/fred_macro_data.xlsx**: Your Excel file (auto-updated)
- **fred_processed/fred_combined.csv**: Processed data (auto-generated)
- **BigQuery**: `vant-486316.db.fred_metrics` (auto-uploaded)
