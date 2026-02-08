"""Upload Reddit data to BigQuery using JSON format (more reliable than CSV)"""
from google.cloud import bigquery
import pandas as pd
from pathlib import Path
import json

client = bigquery.Client(project='vant-486316')

# Load CSV
csv_file = Path(__file__).parent / 'reddit_processed' / 'reddit_posts.csv'
df = pd.read_csv(csv_file)

print(f"Loaded {len(df)} posts from CSV")

# Convert to JSON Lines format (newline-delimited JSON)
json_file = Path(__file__).parent / 'reddit_processed' / 'reddit_posts.jsonl'

# Convert DataFrame to JSON Lines
with open(json_file, 'w', encoding='utf-8') as f:
    for _, row in df.iterrows():
        # Convert row to dict, handling NaN values
        row_dict = row.to_dict()
        # Replace NaN with None for JSON
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
        f.write(json.dumps(row_dict) + '\n')

print(f"Converted to JSON Lines: {json_file}")

# Delete existing table
table_id = 'vant-486316.db.reddit_posts'
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

print("\nSuccess! Reddit data uploaded via JSON format")
