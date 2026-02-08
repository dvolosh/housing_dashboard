# Reddit Data Pipeline Guide

This guide explains how to use the Reddit data pipeline to fetch, process, and upload posts from r/FirstTimeHomeBuyer and r/SameGrassButGreener to BigQuery.

## Overview

The pipeline consists of three main scripts:
1. **fetch_reddit.py** - Fetches posts from PullPush API
2. **preprocess_reddit.py** - Extracts metadata (location, price, cities)
3. **upload_reddit_to_bigquery.py** - Uploads to BigQuery

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
# 1. Fetch last 30 days of data
python fetch_reddit.py --test

# 2. Process the data
python preprocess_reddit.py

# 3. Upload to BigQuery
python upload_reddit_to_bigquery.py
```

### Regular Updates (Incremental)

```bash
# Fetch only new posts since last update
python fetch_reddit.py --incremental

# Process new data
python preprocess_reddit.py

# Upload to BigQuery
python upload_reddit_to_bigquery.py
```

## Detailed Usage

### fetch_reddit.py

**Fetch all subreddits (incremental)**:
```bash
python fetch_reddit.py --incremental
```

**Fetch specific subreddit**:
```bash
python fetch_reddit.py --subreddit FirstTimeHomeBuyer --incremental
```

**Full historical fetch**:
```bash
python fetch_reddit.py --full --start-date 2023-01-01
```

**Test mode (last 7 days)**:
```bash
python fetch_reddit.py --test
```

**Options**:
- `--subreddit` - Which subreddit to fetch (FirstTimeHomeBuyer, SameGrassButGreener, or all)
- `--incremental` - Fetch only new posts since last update (default)
- `--full` - Full fetch from start-date
- `--start-date` - Start date in YYYY-MM-DD format
- `--end-date` - End date in YYYY-MM-DD format
- `--test` - Test mode: fetch last 7 days only

**Output**:
- Raw JSON files in `reddit_raw/`
- Metadata files tracking last fetch date

### preprocess_reddit.py

**Process all data**:
```bash
python preprocess_reddit.py
```

This script:
- Loads raw JSON from `reddit_raw/`
- Extracts location and purchase price from r/FirstTimeHomeBuyer posts
- Extracts city mentions from r/SameGrassButGreener posts
- Saves processed CSV to `reddit_processed/reddit_posts.csv`

**Output**:
- `reddit_processed/reddit_posts.csv` - Post-level data ready for BigQuery

### upload_reddit_to_bigquery.py

**Upload processed data**:
```bash
python upload_reddit_to_bigquery.py
```

**Verify upload only**:
```bash
python upload_reddit_to_bigquery.py --verify-only
```

This script:
- Creates `reddit_posts` table if it doesn't exist
- Deduplicates based on post_id
- Uploads new posts to BigQuery
- Runs verification queries

## Data Schema

### reddit_posts Table

| Column | Type | Description |
|--------|------|-------------|
| post_id | STRING | Unique Reddit post ID |
| subreddit | STRING | FirstTimeHomeBuyer or SameGrassButGreener |
| created_utc | TIMESTAMP | Post creation time |
| created_date | DATE | Post creation date (partition key) |
| title | STRING | Post title |
| selftext | STRING | Full post body text |
| score | INTEGER | Upvotes - downvotes |
| num_comments | INTEGER | Number of comments |
| author | STRING | Reddit username |
| location | STRING | Extracted location (FirstTimeHomeBuyer only) |
| purchase_price | FLOAT | Extracted price (FirstTimeHomeBuyer only) |
| city_mentions | STRING | Pipe-separated cities (SameGrassButGreener only) |
| permalink | STRING | Reddit post URL |
| updated_at | TIMESTAMP | When record was inserted |

**Partitioning**: By `created_date` (daily)  
**Clustering**: By `subreddit`, `location`

## Extraction Patterns

### Location Extraction (FirstTimeHomeBuyer)

Extracts patterns like:
- "Austin, TX"
- "Phoenix, AZ"
- "San Francisco, CA"

### Purchase Price Extraction (FirstTimeHomeBuyer)

Extracts patterns like:
- $450,000
- $450K
- $1.2M
- 450k

### City Mentions (SameGrassButGreener)

Extracts mentions of major US cities from a predefined list.

## Troubleshooting

### PullPush API Rate Limits

If you hit rate limits (429 errors):
- The script automatically retries with exponential backoff
- Default rate limit: 1 request per second
- Adjust `RATE_LIMIT_DELAY` in `fetch_reddit.py` if needed

### No Data Extracted

If location/price extraction rates are low:
- This is normal - not all posts contain this information
- Typical extraction rates:
  - Location: 20-40% of posts
  - Purchase price: 10-30% of posts

### BigQuery Upload Errors

If upload fails:
- Check that `GCP_PROJECT_ID` is set in `.env`
- Verify service account has BigQuery Data Editor permissions
- Check that dataset `housing_dashboard` exists

## Example Queries

### Posts with location and price
```sql
SELECT created_date, location, purchase_price, title, permalink
FROM `housing_dashboard.reddit_posts`
WHERE subreddit = 'FirstTimeHomeBuyer'
  AND location IS NOT NULL
  AND purchase_price IS NOT NULL
ORDER BY created_date DESC
LIMIT 100;
```

### City mentions in SameGrassButGreener
```sql
SELECT created_date, city_mentions, title, score
FROM `housing_dashboard.reddit_posts`
WHERE subreddit = 'SameGrassButGreener'
  AND city_mentions LIKE '%Austin%'
ORDER BY score DESC
LIMIT 50;
```

### Daily post volume
```sql
SELECT 
  created_date,
  subreddit,
  COUNT(*) as post_count
FROM `housing_dashboard.reddit_posts`
GROUP BY created_date, subreddit
ORDER BY created_date DESC;
```

## Automation

To automate the pipeline, create a script or cron job:

```bash
#!/bin/bash
# update_reddit.sh

cd /path/to/data_engine

# Fetch new posts
python fetch_reddit.py --incremental

# Process
python preprocess_reddit.py

# Upload
python upload_reddit_to_bigquery.py

echo "Reddit data update complete!"
```

Schedule with cron (daily at 2 AM):
```
0 2 * * * /path/to/update_reddit.sh
```
