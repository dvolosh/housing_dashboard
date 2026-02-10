# Google Trends Data Pipeline - Topic-Based Analysis

## Overview

Tracks **4 validated Google Trends topics** using MIDs for clean housing market signals, aggregated to weekly granularity.

## Tracked Topics

All 4 metrics use **validated topic MIDs** for reliable signal:

| Category | Topic | MID | Signal |
|----------|-------|-----|--------|
| **Involuntary Supply** | Estate Sales | `/m/02rmp0` | Housing supply from inheritance/death |
| **Distress Signal** | Foreclosure Auctions | `/m/02tp2m` | Forced selling/market stress |
| **Financial Friction** | Home Insurance | `/m/01v8_f` | Rising homeowner carry costs |
| **Market Access** | Mortgage Assumption | `/m/0ddkfg` | Buyers seeking rate relief |

## Quick Start

```powershell
cd data_engine

# Fetch 5 years of data (first time)
python fetch_trends.py --full

# Process to weekly aggregation
python preprocess_trends.py

# Upload to BigQuery
python upload_trends_to_bigquery.py
```

## Regular Updates

```powershell
# Incremental fetch
python fetch_trends.py

# Reprocess
python preprocess_trends.py

# Upload
python upload_trends_to_bigquery.py
```

## Output Schema

**Weekly data** in `google_search_trends` table:
- `week_start_date` - Sunday start date
- `search_term` - Display name (e.g. "Estate Sales")
- `category` - Involuntary Supply, Distress Signal, Financial Friction, or Market Access
- `avg_interest_score` - Weekly average (0-100)
- `region` - US

## Why Topics?

Topics (MIDs) aggregate related searches for cleaner signals:
- `/m/02rmp0` captures "estate sale", "estate sales near me", "estate auction", etc.
- More stable than exact search strings
- Better trend identification

## Environment

Required in `.env`:
```
GCP_PROJECT_ID=your-project-id
GCP_DATASET_ID=db
```
