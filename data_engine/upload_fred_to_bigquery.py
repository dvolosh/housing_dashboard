"""
FRED BigQuery Upload Script

Uploads preprocessed FRED data to BigQuery with proper schema,
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


class FREDBigQueryUploader:
    """Handles uploading FRED data to BigQuery"""
    
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
        
    def create_fred_table(self, table_id: str) -> bigquery.Table:
        """
        Create BigQuery table for FRED data with proper schema
        
        Args:
            table_id: Table ID (name)
            
        Returns:
            Created table object
        """
        # Define schema
        schema = [
            bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("series_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("value", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("frequency", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("units", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
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
        table.clustering_fields = ["series_id", "date"]
        
        # Set table description
        table.description = "Federal Reserve Economic Data (FRED) time series metrics"
        
        # Set labels
        table.labels = {
            "source": "fred",
            "data_type": "economic_indicators"
        }
        
        # Create table
        try:
            table = self.client.create_table(table)
            logger.info(f"Created table {table_ref}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.warning(f"Table {table_ref} already exists, will append/replace data")
                table = self.client.get_table(table_ref)
            else:
                raise
        
        return table
    
    def upload_from_parquet(
        self, 
        table_id: str, 
        parquet_file: str, 
        write_disposition: str = "WRITE_TRUNCATE"
    ):
        """
        Upload FRED data from Parquet file to BigQuery
        
        Args:
            table_id: Target table ID
            parquet_file: Path to Parquet file
            write_disposition: How to handle existing data (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
        """
        logger.info(f"Uploading {parquet_file} to {table_id}...")
        
        # Create table if it doesn't exist
        table = self.create_fred_table(table_id)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            # Use explicit schema to ensure all data is loaded
            schema=[
                bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("series_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("value", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("frequency", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("units", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
            ]
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
    
    def upload_from_csv(
        self, 
        table_id: str, 
        csv_file: str, 
        write_disposition: str = "WRITE_TRUNCATE"
    ):
        """
        Upload FRED data from CSV file to BigQuery
        
        Args:
            table_id: Target table ID
            csv_file: Path to CSV file
            write_disposition: How to handle existing data (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)
        """
        logger.info(f"Uploading {csv_file} to {table_id}...")
        
        # Don't pre-create table - let autodetect handle it
        # Pre-creating with partitioning/clustering conflicts with autodetect
        
        # Configure load job with autodetect
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=write_disposition,
            skip_leading_rows=1,  # Skip header row
            autodetect=True,  # Let BigQuery detect schema from data
        )
        
        # Load data
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        with open(csv_file, "rb") as source_file:
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
        Run validation queries to verify FRED data quality
        
        Args:
            table_id: Table to validate
        """
        logger.info("\n" + "="*60)
        logger.info("Running validation queries...")
        logger.info("="*60)
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        
        # Query 1: Observations by series
        query1 = f"""
        SELECT 
            series_id,
            series_name,
            frequency,
            COUNT(*) as observation_count,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            ROUND(MIN(value), 2) as min_value,
            ROUND(MAX(value), 2) as max_value
        FROM `{table_ref}`
        GROUP BY series_id, series_name, frequency
        ORDER BY series_id
        """
        
        logger.info("\n📊 Observations by series:")
        results = self.client.query(query1).result()
        for row in results:
            logger.info(f"\n  {row.series_id} ({row.frequency}):")
            logger.info(f"    Name: {row.series_name}")
            logger.info(f"    Observations: {row.observation_count:,}")
            logger.info(f"    Date range: {row.earliest_date} to {row.latest_date}")
            logger.info(f"    Value range: {row.min_value} to {row.max_value}")
        
        # Query 2: Latest values for each series
        query2 = f"""
        WITH latest_obs AS (
            SELECT 
                series_id,
                series_name,
                date,
                value,
                units,
                ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY date DESC) as rn
            FROM `{table_ref}`
        )
        SELECT 
            series_id,
            series_name,
            date as latest_date,
            ROUND(value, 2) as latest_value,
            units
        FROM latest_obs
        WHERE rn = 1
        ORDER BY series_id
        """
        
        logger.info("\n📈 Latest values:")
        results = self.client.query(query2).result()
        for row in results:
            logger.info(f"  {row.series_id}: {row.latest_value} {row.units} (as of {row.latest_date})")
        
        # Query 3: Mortgage rate trend (last 12 weeks)
        query3 = f"""
        SELECT 
            date,
            ROUND(value, 2) as mortgage_rate_30yr
        FROM `{table_ref}`
        WHERE series_id = 'MORTGAGE30US'
        ORDER BY date DESC
        LIMIT 12
        """
        
        logger.info("\n🏠 30-Year Mortgage Rate (last 12 weeks):")
        results = self.client.query(query3).result()
        for row in results:
            logger.info(f"  {row.date}: {row.mortgage_rate_30yr}%")
        
        # Query 4: CPI year-over-year change
        query4 = f"""
        WITH cpi_data AS (
            SELECT 
                date,
                value as cpi,
                LAG(value, 12) OVER (ORDER BY date) as cpi_year_ago
            FROM `{table_ref}`
            WHERE series_id = 'CPIAUCSL'
        )
        SELECT 
            date,
            ROUND(cpi, 1) as cpi,
            ROUND(((cpi - cpi_year_ago) / cpi_year_ago) * 100, 2) as yoy_change_pct
        FROM cpi_data
        WHERE cpi_year_ago IS NOT NULL
        ORDER BY date DESC
        LIMIT 12
        """
        
        logger.info("\n💰 CPI Year-over-Year Change (last 12 months):")
        results = self.client.query(query4).result()
        for row in results:
            logger.info(f"  {row.date}: {row.cpi} (YoY: {row.yoy_change_pct:+.2f}%)")


def main():
    """Main execution function"""
    # Load configuration from environment variables
    PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    DATASET_ID = os.getenv('GCP_DATASET_ID', 'db')
    TABLE_ID = os.getenv('GCP_FRED_TABLE_ID', 'fred_metrics')
    
    # Validate required environment variables
    if not PROJECT_ID:
        logger.error("GCP_PROJECT_ID not found in environment variables")
        logger.error("Please set GCP_PROJECT_ID in your .env file")
        return
    
    logger.info(f"Using GCP Project: {PROJECT_ID}")
    logger.info(f"Dataset: {DATASET_ID}, Table: {TABLE_ID}")
    
    # Paths
    processed_dir = Path(__file__).parent / 'fred_processed'
    csv_file = processed_dir / 'fred_combined.csv'
    
    # Verify file exists
    if not csv_file.exists():
        logger.error(f"CSV file not found: {csv_file}")
        logger.error("Please run preprocess_fred.py first")
        return
    
    # Create uploader
    uploader = FREDBigQueryUploader(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID
    )
    
    # Upload data from CSV
    uploader.upload_from_csv(
        table_id=TABLE_ID,
        csv_file=str(csv_file),
        write_disposition="WRITE_TRUNCATE"  # Replace existing data
    )
    
    # Run validation queries
    uploader.run_validation_queries(TABLE_ID)
    
    logger.info("\n✅ All done!")


if __name__ == '__main__':
    main()
