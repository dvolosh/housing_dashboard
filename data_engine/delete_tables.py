from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))
dataset_id = os.getenv('GCP_DATASET_ID', 'db')

# Delete existing tables if they exist
for table_name in ['zillow_city_latest', 'zillow_state_aggregated']:
    table_ref = f"{client.project}.{dataset_id}.{table_name}"
    try:
        client.delete_table(table_ref)
        print(f"Deleted table: {table_ref}")
    except Exception as e:
        print(f"Table {table_name} doesn't exist or couldn't be deleted: {e}")

print("Done!")
