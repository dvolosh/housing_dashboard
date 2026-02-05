"""
BigQuery Upload Script

Uploads preprocessed Zillow data to BigQuery with proper schema,
partitioning, and clustering configuration.
"""

from google.cloud import bigquery
from pathlib import Path
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BigQueryUploader:
    """Handles uploading data to BigQuery"""
    
    def __init__(self, project_id: str, dataset_id: str):
        """
        Initialize BigQuery uploader
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        
    def create_table(self, table_id: str) -> bigquery.Table:
        """
        Create BigQuery table with proper schema and configuration
        
        Args:
            table_id: Table ID (name)
            
        Returns:
            Created table object
        """
        # Define schema
        schema = [
            bigquery.SchemaField("region_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("region_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("region_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("state_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("metric_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("value", "FLOAT", mode="REQUIRED"),
        ]
        
        # Create table reference
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        table = bigquery.Table(table_ref, schema=schema)
        
        # Configure partitioning (by date, monthly)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="date"
        )
        
        # Configure clustering
        table.clustering_fields = ["region_name", "metric_type"]
        
        # Set table description
        table.description = "Zillow housing market metrics in normalized long format"
        
        # Create table
        try:
            table = self.client.create_table(table)
            logger.info(f"Created table {table_ref}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.warning(f"Table {table_ref} already exists, will append data")
                table = self.client.get_table(table_ref)
            else:
                raise
        
        return table
    
    def upload_from_parquet(self, table_id: str, parquet_file: str, write_disposition: str = "WRITE_TRUNCATE"):
        """
        Upload data from Parquet file to BigQuery
        
        Args:
            table_id: Target table ID
            parquet_file: Path to Parquet file
            write_disposition: How to handle existing data (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
        """
        logger.info(f"Uploading {parquet_file} to {table_id}...")
        
        # Create table if it doesn't exist
        table = self.create_table(table_id)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            # Parquet files are self-describing, enable autodetect
            autodetect=True
        )
        
        # Load data
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        with open(parquet_file, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config
            )
        
        # Wait for job to complete
        logger.info("Waiting for upload to complete...")
        job.result()
        
        # Get table info
        table = self.client.get_table(table_ref)
        logger.info(f"✅ Upload complete!")
        logger.info(f"  Table: {table_ref}")
        logger.info(f"  Rows: {table.num_rows:,}")
        logger.info(f"  Size: {table.num_bytes / (1024**2):.2f} MB")
        
        return table
    
    def run_validation_queries(self, table_id: str):
        """
        Run validation queries to verify data quality
        
        Args:
            table_id: Table to validate
        """
        logger.info("\n" + "="*60)
        logger.info("Running validation queries...")
        logger.info("="*60)
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        # Query 1: Row count by metric
        query1 = f"""
        SELECT 
            metric_type,
            COUNT(*) as row_count,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM `{table_ref}`
        GROUP BY metric_type
        ORDER BY row_count DESC
        """
        
        logger.info("\n📊 Row count by metric:")
        results = self.client.query(query1).result()
        for row in results:
            logger.info(f"  {row.metric_type}: {row.row_count:,} rows ({row.earliest_date} to {row.latest_date})")
        
        # Query 2: National median sale price trend (last 12 months)
        query2 = f"""
        SELECT 
            date,
            value as median_sale_price
        FROM `{table_ref}`
        WHERE region_name = 'United States'
          AND metric_type = 'median_sale_price'
          AND date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))
        ORDER BY date DESC
        LIMIT 12
        """
        
        logger.info("\n📈 National median sale price (last 12 months):")
        results = self.client.query(query2).result()
        for row in results:
            logger.info(f"  {row.date}: ${row.median_sale_price:,.0f}")
        
        # Query 3: Top 10 MSAs by ZHVI
        query3 = f"""
        SELECT 
            region_name,
            value as zhvi
        FROM `{table_ref}`
        WHERE metric_type = 'zhvi'
          AND region_type = 'msa'
          AND date = (SELECT MAX(date) FROM `{table_ref}` WHERE metric_type = 'zhvi')
        ORDER BY value DESC
        LIMIT 10
        """
        
        logger.info("\n🏆 Top 10 MSAs by ZHVI (most recent):")
        results = self.client.query(query3).result()
        for i, row in enumerate(results, 1):
            logger.info(f"  {i}. {row.region_name}: ${row.zhvi:,.0f}")


def main():
    """Main execution function"""
    # Load configuration from environment variables
    PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    DATASET_ID = os.getenv('GCP_DATASET_ID', 'housing_data')
    TABLE_ID = os.getenv('GCP_TABLE_ID', 'zillow_metrics')
    
    # Validate required environment variables
    if not PROJECT_ID:
        logger.error("GCP_PROJECT_ID not found in environment variables")
        logger.error("Please set GCP_PROJECT_ID in your .env file")
        return
    
    logger.info(f"Using GCP Project: {PROJECT_ID}")
    logger.info(f"Dataset: {DATASET_ID}, Table: {TABLE_ID}")
    
    # Paths
    processed_dir = Path(__file__).parent / 'zillow_processed'
    parquet_file = processed_dir / 'zillow_combined.parquet'
    
    # Verify file exists
    if not parquet_file.exists():
        logger.error(f"Parquet file not found: {parquet_file}")
        logger.error("Please run preprocess_zillow.py first")
        return
    
    # Create uploader
    uploader = BigQueryUploader(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )
    
    # Upload data
    uploader.upload_from_parquet(
        table_id=TABLE_ID,
        parquet_file=str(parquet_file),
        write_disposition="WRITE_TRUNCATE"  # Replace existing data
    )
    
    # Run validation queries
    uploader.run_validation_queries(TABLE_ID)
    
    logger.info("\n✅ All done!")


if __name__ == '__main__':
    main()
