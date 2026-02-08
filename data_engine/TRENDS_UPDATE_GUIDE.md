# Google Trends Data Pipeline Guide

This guide explains how to use the Google Trends pipeline to fetch, process, and upload search interest data for housing-related terms to BigQuery.

## Overview

The pipeline consists of three main scripts:
1. **fetch_trends.py** - Fetches search interest data using pytrends
2. **preprocess_trends.py** - Normalizes and combines data
3. **upload_trends_to_bigquery.py** - Uploads to BigQuery

## Prerequisites

- Python 3.8+
- Google Cloud project with BigQuery enabled
- Service account with BigQuery permissions
- Environment variables configured in `.env`:
  ```
  GCP_PROJECT_ID=your-project-id
  BQ_DATASET_ID=housing_dashboard
  ```

## Quick Start

### Initial Setup (First Time)

```bash
# 1. Fetch last 30 days of data (test mode)
python fetch_trends.py --test

# 2. Process the data
python preprocess_trends.py

# 3. Upload to BigQuery
python upload_trends_to_bigquery.py
```

### Regular Updates (Incremental)

```bash
# Fetch only new data since last update
python fetch_trends.py --incremental

# Process new data
python preprocess_trends.py

# Upload to BigQuery
python upload_trends_to_bigquery.py
```

## Detailed Usage

### fetch_trends.py

**Fetch all terms (incremental)**:
```bash
python fetch_trends.py --incremental
```

**Fetch specific term**:
```bash
python fetch_trends.py --term mortgage_rate --incremental
```

**Full historical fetch (last 5 years)**:
```bash
python fetch_trends.py --full
```

**Custom date range**:
```bash
python fetch_trends.py --full --start-date 2020-01-01 --end-date 2024-12-31
```

**Test mode (last 30 days)**:
```bash
python fetch_trends.py --test
```

**Options**:
- `--term` - Which term to fetch (see tracked terms below, or 'all')
- `--incremental` - Fetch only new data since last update (default)
- `--full` - Full fetch from start-date
- `--start-date` - Start date in YYYY-MM-DD format
- `--end-date` - End date in YYYY-MM-DD format
- `--geo` - Geographic region (default: US)
- `--test` - Test mode: fetch last 30 days only

**Output**:
- CSV files in `trends_raw/` (one per term)
- Metadata files tracking last fetch date

### Tracked Search Terms

The pipeline tracks these housing-related search terms:

| Term Key | Search Term | Description |
|----------|-------------|-------------|
| mortgage_rate | "mortgage rate" | Interest in mortgage rates |
| foreclosure | "foreclosure" | Interest in foreclosures |
| house_hunting | "house hunting" | Interest in house hunting |
| first_time_home_buyer | "first time home buyer" | Interest in first-time buying |
| housing_market_crash | "housing market crash" | Interest in market crash |

### preprocess_trends.py

**Process all data**:
```bash
python preprocess_trends.py
```

This script:
- Loads raw CSV files from `trends_raw/`
- Normalizes interest scores (0-100)
- Combines all terms into single CSV
- Saves to `trends_processed/trends_data.csv`

**Output**:
- `trends_processed/trends_data.csv` - Combined data ready for BigQuery

### upload_trends_to_bigquery.py

**Upload processed data**:
```bash
python upload_trends_to_bigquery.py
```

**Verify upload only**:
```bash
python upload_trends_to_bigquery.py --verify-only
```

This script:
- Creates `trends_metrics` table if it doesn't exist
- Deduplicates based on (date, search_term, region)
- Uploads new records to BigQuery
- Runs verification queries

## Data Schema

### trends_metrics Table

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Date of measurement |
| search_term | STRING | Housing-related search term |
| interest_score | INTEGER | Google Trends interest score (0-100) |
| region | STRING | Geographic region (e.g., US) |
| updated_at | TIMESTAMP | When record was inserted |

**Partitioning**: By `date` (daily)  
**Clustering**: By `search_term`

## Understanding Interest Scores

Google Trends interest scores are normalized to a 0-100 scale:
- **100** = Peak interest for the term in the time period
- **50** = Half the peak interest
- **0** = Very low interest (less than 1% of peak)

**Important Notes**:
- Scores are relative to the term's own history
- Different terms cannot be directly compared (each has its own scale)
- Use the `trends_normalized` view for cross-term comparison

## Rate Limits and Best Practices

### Google Trends Rate Limits

Google Trends (via pytrends) has strict rate limits:
- **Limit**: ~100 requests per hour
- **Blocking**: IP-based temporary blocks if exceeded
- **Recovery**: Usually 1-4 hours

### Best Practices

1. **Use incremental updates** - Only fetch new data
2. **Space out requests** - Default 2-second delay between requests
3. **Avoid full refetches** - Only do full fetch when necessary
4. **Run during off-hours** - Less likely to hit rate limits

### If You Get Blocked

If you see errors like "429 Too Many Requests" or "The request failed":
1. Wait 1-4 hours before retrying
2. Increase `RATE_LIMIT_DELAY` in `fetch_trends.py`
3. Consider using a VPN to change IP address (last resort)

## Troubleshooting

### pytrends Installation Issues

If pytrends fails to install:
```bash
pip install --upgrade pytrends
```

### Empty Data Returned

If no data is returned:
- Check that the search term exists in Google Trends
- Try a different date range
- Verify you're not blocked (wait a few hours)

### BigQuery Upload Errors

If upload fails:
- Check that `GCP_PROJECT_ID` is set in `.env`
- Verify service account has BigQuery Data Editor permissions
- Check that dataset `housing_dashboard` exists

## Example Queries

### Recent interest scores
```sql
SELECT date, search_term, interest_score
FROM `housing_dashboard.trends_metrics`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY date DESC, search_term;
```

### High-interest periods (score >= 75)
```sql
SELECT date, search_term, interest_score
FROM `housing_dashboard.trends_metrics`
WHERE interest_score >= 75
ORDER BY date DESC, interest_score DESC
LIMIT 100;
```

### Weekly averages (smoother trends)
```sql
SELECT * 
FROM `housing_dashboard.trends_weekly`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
ORDER BY week_start DESC, search_term;
```

### Normalized comparison across terms
```sql
SELECT date, search_term, normalized_score
FROM `housing_dashboard.trends_normalized`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY date DESC, search_term;
```

## Automation

To automate the pipeline, create a script or cron job:

```bash
#!/bin/bash
# update_trends.sh

cd /path/to/data_engine

# Fetch new data
python fetch_trends.py --incremental

# Process
python preprocess_trends.py

# Upload
python upload_trends_to_bigquery.py

echo "Google Trends data update complete!"
```

Schedule with cron (daily at 3 AM, after Reddit update):
```
0 3 * * * /path/to/update_trends.sh
```

**Note**: Due to rate limits, it's recommended to run this once per day at most.

## Adding New Search Terms

To track additional search terms:

1. Edit `fetch_trends.py` and add to `SEARCH_TERMS` dict:
```python
SEARCH_TERMS = {
    # ... existing terms ...
    'new_term_key': {
        'term': 'your search term',
        'description': 'What this term indicates'
    }
}
```

2. Edit `preprocess_trends.py` and add to `TERM_KEYS` list:
```python
TERM_KEYS = [
    # ... existing terms ...
    'new_term_key'
]
```

3. Run full fetch for the new term:
```bash
python fetch_trends.py --term new_term_key --full
```
