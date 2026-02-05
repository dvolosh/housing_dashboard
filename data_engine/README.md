# Data Engine - Setup Guide

## Overview
This directory contains scripts for processing Zillow housing data and uploading it to BigQuery.

## Setup Instructions

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy the example environment file:
```powershell
cp .env.example .env
```

Edit `.env` and set your credentials:
```env
# Required for BigQuery upload
GCP_PROJECT_ID=your-gcp-project-id
GCP_DATASET_ID=housing_data
GCP_TABLE_ID=zillow_metrics

# Optional: Service account key path
# GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json

# For future FRED API integration
FRED_API_KEY=your_fred_api_key_here

# For future Reddit API integration
REDDIT_CLIENT_ID=your_reddit_client_id_here
REDDIT_CLIENT_SECRET=your_reddit_client_secret_here
```

### 3. Authenticate with Google Cloud

**Option 1: Using gcloud CLI (Recommended)**
```powershell
# Install gcloud CLI from: https://cloud.google.com/sdk/docs/install

# Authenticate
gcloud auth application-default login

# Set your project
gcloud config set project your-gcp-project-id
```

**Option 2: Using Service Account Key**
```powershell
# Download service account key from GCP Console
# Set the path in .env file:
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json
```

## Usage

### Preprocess Zillow Data
Transform raw CSV files into normalized Parquet format:
```powershell
python preprocess_zillow.py
```

**Output:** `zillow_processed/zillow_combined.parquet`

### Upload to BigQuery
Upload processed data to BigQuery:
```powershell
python upload_to_bigquery.py
```

This will:
- Create the BigQuery table (if it doesn't exist)
- Upload all data with partitioning and clustering
- Run validation queries

## File Structure

```
data_engine/
├── .env                    # Your credentials (not in git)
├── .env.example            # Template for credentials
├── .gitignore              # Prevents committing secrets
├── requirements.txt        # Python dependencies
├── README.md               # This file
├── preprocess_zillow.py    # Data transformation script
├── upload_to_bigquery.py   # BigQuery upload script
├── create_bq_schema.sql    # Schema definition
├── zillow_raw/             # Original CSV files
└── zillow_processed/       # Processed Parquet files
```

## Security Notes

⚠️ **Never commit `.env` or service account keys to git!**

The `.gitignore` file is configured to exclude:
- `.env` files
- `*.json` credential files
- Python cache files

## Troubleshooting

### Authentication Errors
If you see `DefaultCredentialsError`:
1. Make sure you've run `gcloud auth application-default login`
2. Or set `GOOGLE_APPLICATION_CREDENTIALS` in `.env`

### Missing Dependencies
```powershell
pip install -r requirements.txt
```

### Environment Variables Not Loading
Make sure your `.env` file is in the `data_engine/` directory and contains valid values.
