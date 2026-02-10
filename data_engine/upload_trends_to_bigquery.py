"""
Google Trends Data BigQuery Uploader

Uploads processed Google Trends weekly data to BigQuery.
Handles deduplication on (week_start_date, search_term, region).
"""

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pathlib import Path
import logging
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrendsBigQueryUploader:
    """Handles uploading Google Trends data to BigQuery"""
    
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        """
        Initialize uploader
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        
        # Full table reference
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    def table_exists(self) -> bool:
        """Check if table exists"""
        try:
            self.client.get_table(self.table_ref)
            return True
        except NotFound:
            return False
    
    def create_table(self):
        """Create table with schema"""
        logger.info(f"Creating table {self.table_ref}...")
        
        schema = [
            bigquery.SchemaField("week_start_date", "DATE", mode="REQUIRED", description="Start date of the week (Sunday)"),
            bigquery.SchemaField("search_term", "STRING", mode="REQUIRED", description="Search term"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED", description="Term category: Distress, Affordability, or Inventory"),
            bigquery.SchemaField("avg_interest_score", "INTEGER", mode="REQUIRED", description="Weekly average interest score (0-100)"),
            bigquery.SchemaField("region", "STRING", mode="REQUIRED", description="Geographic region"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="When record was inserted"),
        ]
        
        table = bigquery.Table(self.table_ref, schema=schema)
        
        # Configure partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="week_start_date"
        )
        
        # Configure clustering
        table.clustering_fields = ["category", "search_term"]
        
        # Set description
        table.description = "Google Trends weekly average interest scores for housing-related search terms"
        
        # Create table
        table = self.client.create_table(table)
        logger.info(f"✅ Created table {self.table_ref}")
    
    def get_existing_records(self) -> set:
        """Get set of existing (week_start_date, search_term, region) tuples to avoid duplicates"""
        if not self.table_exists():
            return set()
        
        query = f"""
        SELECT DISTINCT week_start_date, search_term, region
        FROM `{self.table_ref}`
        """
        
        logger.info("Fetching existing records...")
        
        try:
            result = self.client.query(query).result()
            records = {(row.week_start_date, row.search_term, row.region) for row in result}
            logger.info(f"Found {len(records)} existing records")
            return records
        except Exception as e:
            logger.warning(f"Could not fetch existing records: {e}")
            return set()
    
    def upload_data(self, csv_file: str, deduplicate: bool = True):
        """
        Upload data from CSV to BigQuery
        
        Args:
            csv_file: Path to CSV file
            deduplicate: If True, skip records that already exist
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Uploading Google Trends data to BigQuery")
        logger.info(f"{'='*60}")
        
        # Load CSV
        csv_path = Path(csv_file)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_file}")
            return
        
        logger.info(f"Loading data from {csv_file}...")
        df = pd.read_csv(csv_file, parse_dates=['week_start_date'])
        logger.info(f"Loaded {len(df)} records")
        
        # Create table if it doesn't exist
        if not self.table_exists():
            self.create_table()
        
        # Deduplicate if requested
        if deduplicate:
            existing_records = self.get_existing_records()
            
            if existing_records:
                original_count = len(df)
                
                # Create tuple for comparison
                df['_key'] = list(zip(df['week_start_date'].dt.date, df['search_term'], df['region']))
                df = df[~df['_key'].isin(existing_records)]
                df = df.drop(columns=['_key'])
                
                logger.info(f"Filtered out {original_count - len(df)} duplicate records")
                logger.info(f"Uploading {len(df)} new records")
        
        if len(df) == 0:
            logger.info("No new data to upload")
            return
        
        # Add updated_at timestamp
        df['updated_at'] = pd.Timestamp.now()
        
        # Ensure proper data types
        df['avg_interest_score'] = df['avg_interest_score'].astype('Int64')
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )
        
        # Upload to BigQuery
        logger.info(f"Starting upload to {self.table_ref}...")
        
        try:
            job = self.client.load_table_from_dataframe(
                df,
                self.table_ref,
                job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"✅ Upload complete!")
            logger.info(f"Uploaded {len(df)} records")
            
            # Get table info
            table = self.client.get_table(self.table_ref)
            logger.info(f"Total rows in table: {table.num_rows:,}")
            
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise
    
    def verify_upload(self):
        """Verify upload with sample queries"""
        logger.info(f"\n{'='*60}")
        logger.info("Verifying upload...")
        logger.info(f"{'='*60}")
        
        # Query 1: Total records by category and search term
        query1 = f"""
        SELECT 
            category,
            search_term,
            COUNT(*) as record_count,
            MIN(week_start_date) as earliest_week,
            MAX(week_start_date) as latest_week,
            AVG(avg_interest_score) as avg_interest
        FROM `{self.table_ref}`
        GROUP BY category, search_term
        ORDER BY category, search_term
        """
        
        logger.info("\n📊 Records by category and search term:")
        result = self.client.query(query1).result()
        current_category = None
        for row in result:
            if row.category != current_category:
                logger.info(f"\n  [{row.category}]")
                current_category = row.category
            logger.info(f"    {row.search_term}:")
            logger.info(f"      Records: {row.record_count:,}")
            logger.info(f"      Week range: {row.earliest_week} to {row.latest_week}")
            logger.info(f"      Avg interest: {row.avg_interest:.1f}")
        
        # Query 2: Recent high-interest weeks
        query2 = f"""
        SELECT 
            week_start_date,
            category,
            search_term,
            avg_interest_score
        FROM `{self.table_ref}`
        WHERE week_start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
          AND avg_interest_score >= 75
        ORDER BY week_start_date DESC, avg_interest_score DESC
        LIMIT 10
        """
        
        logger.info("\n📊 Recent high-interest weeks (score >= 75):")
        result = self.client.query(query2).result()
        count = 0
        for row in result:
            logger.info(f"  {row.week_start_date}: [{row.category}] {row.search_term} = {row.avg_interest_score}")
            count += 1
        
        if count == 0:
            logger.info("  No high-interest weeks in last 12 weeks")
        
        logger.info("\n✅ Verification complete!")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload Google Trends data to BigQuery')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only run verification queries')
    
    args = parser.parse_args()
    
    # Load configuration from environment
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    table_id = 'google_search_trends'
    
    if not project_id:
        logger.error("GCP_PROJECT_ID not found in environment variables")
        logger.error("Please set GCP_PROJECT_ID in your .env file")
        return
    
    # Define paths
    processed_dir = Path(__file__).parent / 'trends_processed'
    csv_file = processed_dir / 'trends_data.csv'
    
    # Create uploader
    uploader = TrendsBigQueryUploader(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id
    )
    
    if args.verify_only:
        uploader.verify_upload()
    else:
        # Upload data
        uploader.upload_data(str(csv_file), deduplicate=True)
        
        # Verify
        uploader.verify_upload()


if __name__ == '__main__':
    main()
