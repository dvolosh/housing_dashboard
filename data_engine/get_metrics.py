"""
Get all available metrics from BigQuery
"""
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))

query = """
SELECT DISTINCT metric_type
FROM `vant-486316.db.zillow_metrics`
WHERE region_type = 'msa'
ORDER BY metric_type
"""

df = client.query(query).to_dataframe()
print("Available metrics:")
for metric in df['metric_type'].tolist():
    print(f"  - {metric}")
