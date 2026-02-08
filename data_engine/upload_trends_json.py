"""Upload Google Trends data to BigQuery using JSON format"""
from google.cloud import bigquery
import pandas as pd
from pathlib import Path
import json

client = bigquery.Client(project='vant-486316')

# Load CSV
csv_file = Path(__file__).parent / 'trends_processed' / 'trends_data.csv'
df = pd.read_csv(csv_file, parse_dates=['date'])

print(f"Loaded {len(df)} records from CSV")

# Convert to JSON Lines format
json_file = Path(__file__).parent / 'trends_processed' / 'trends_data.jsonl'

with open(json_file, 'w', encoding='utf-8') as f:
    for _, row in df.iterrows():
        # Convert row to dict, handling NaN values and dates
        row_dict = row.to_dict()
        # Replace NaN with None and convert date to string
        row_dict = {k: (None if pd.isna(v) else (v.strftime('%Y-%m-%d') if k == 'date' else v)) 
                   for k, v in row_dict.items()}
        f.write(json.dumps(row_dict) + '\n')

print(f"Converted to JSON Lines: {json_file}")

# Delete existing table
table_id = 'vant-486316.db.trends_metrics'
try:
    client.delete_table(table_id)
    print(f"Deleted existing table: {table_id}")
except:
    pass

# Configure job for JSON
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

# Upload from JSON file
with open(json_file, 'rb') as f:
    job = client.load_table_from_file(f, table_id, job_config=job_config)

print(f"Starting upload job: {job.job_id}")
job.result()  # Wait for completion

print(f"Job state: {job.state}")
print(f"Output rows: {job.output_rows}")
if job.errors:
    print(f"Errors: {job.errors}")
else:
    print("No errors!")

# Verify
table = client.get_table(table_id)
print(f"\nTable {table_id} now has {table.num_rows} rows")

# Query to double-check
query = f"SELECT COUNT(*) as count FROM `{table_id}`"
result = client.query(query).result()
for row in result:
    print(f"Query confirms: {row.count} rows")

print("\nSuccess! Google Trends data uploaded via JSON format")
