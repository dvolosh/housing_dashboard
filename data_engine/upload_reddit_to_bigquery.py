"""
Reddit Data BigQuery Uploader

Uploads processed Reddit posts to BigQuery.
Handles post-level data with deduplication on post_id.
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


class RedditBigQueryUploader:
    """Handles uploading Reddit data to BigQuery"""
    
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
            bigquery.SchemaField("post_id", "STRING", mode="REQUIRED", description="Unique Reddit post ID"),
            bigquery.SchemaField("subreddit", "STRING", mode="REQUIRED", description="Subreddit name"),
            bigquery.SchemaField("created_utc", "TIMESTAMP", mode="REQUIRED", description="Post creation time (UTC)"),
            bigquery.SchemaField("created_date", "DATE", mode="REQUIRED", description="Post creation date (partition key)"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE", description="Post title"),
            bigquery.SchemaField("selftext", "STRING", mode="NULLABLE", description="Full post body text"),
            bigquery.SchemaField("score", "INTEGER", mode="NULLABLE", description="Upvotes minus downvotes"),
            bigquery.SchemaField("num_comments", "INTEGER", mode="NULLABLE", description="Number of comments"),
            bigquery.SchemaField("author", "STRING", mode="NULLABLE", description="Reddit username"),
            bigquery.SchemaField("location", "STRING", mode="NULLABLE", description="Extracted location (FirstTimeHomeBuyer)"),
            bigquery.SchemaField("purchase_price", "FLOAT", mode="NULLABLE", description="Extracted purchase price (FirstTimeHomeBuyer)"),
            bigquery.SchemaField("city_mentions", "STRING", mode="NULLABLE", description="Pipe-separated cities (SameGrassButGreener)"),
            bigquery.SchemaField("permalink", "STRING", mode="NULLABLE", description="Reddit post URL"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE", description="When record was inserted"),
        ]
        
        table = bigquery.Table(self.table_ref, schema=schema)
        
        # Configure partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_date"
        )
        
        # Configure clustering
        table.clustering_fields = ["subreddit", "location"]
        
        # Set description
        table.description = "Post-level Reddit data from r/FirstTimeHomeBuyer and r/SameGrassButGreener"
        
        # Create table
        table = self.client.create_table(table)
        logger.info(f"✅ Created table {self.table_ref}")
    
    def get_existing_post_ids(self) -> set:
        """Get set of existing post IDs to avoid duplicates"""
        if not self.table_exists():
            return set()
        
        query = f"""
        SELECT DISTINCT post_id
        FROM `{self.table_ref}`
        """
        
        logger.info("Fetching existing post IDs...")
        
        try:
            result = self.client.query(query).result()
            post_ids = {row.post_id for row in result}
            logger.info(f"Found {len(post_ids)} existing posts")
            return post_ids
        except Exception as e:
            logger.warning(f"Could not fetch existing post IDs: {e}")
            return set()
    
    def upload_data(self, csv_file: str, deduplicate: bool = True):
        """
        Upload data from CSV to BigQuery
        
        Args:
            csv_file: Path to CSV file
            deduplicate: If True, skip posts that already exist
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Uploading Reddit data to BigQuery")
        logger.info(f"{'='*60}")
        
        # Load CSV
        csv_path = Path(csv_file)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_file}")
            return
        
        logger.info(f"Loading data from {csv_file}...")
        df = pd.read_csv(csv_file)
        logger.info(f"Loaded {len(df)} posts")
        
        # Create table if it doesn't exist
        if not self.table_exists():
            self.create_table()
        
        # Deduplicate if requested
        if deduplicate:
            existing_ids = self.get_existing_post_ids()
            
            if existing_ids:
                original_count = len(df)
                df = df[~df['post_id'].isin(existing_ids)]
                logger.info(f"Filtered out {original_count - len(df)} duplicate posts")
                logger.info(f"Uploading {len(df)} new posts")
        
        if len(df) == 0:
            logger.info("No new data to upload")
            return
        
        # Convert created_utc to timestamp
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
        
        # Add updated_at timestamp
        df['updated_at'] = pd.Timestamp.now()
        
        # Ensure proper data types
        df['score'] = df['score'].astype('Int64')
        df['num_comments'] = df['num_comments'].astype('Int64')
        
        # Handle NaN values in string columns
        string_cols = ['title', 'selftext', 'author', 'location', 'city_mentions', 'permalink']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna('')
        
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
            logger.info(f"Uploaded {len(df)} posts")
            
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
        
        # Query 1: Total posts by subreddit
        query1 = f"""
        SELECT 
            subreddit,
            COUNT(*) as post_count,
            MIN(created_date) as earliest_post,
            MAX(created_date) as latest_post
        FROM `{self.table_ref}`
        GROUP BY subreddit
        ORDER BY subreddit
        """
        
        logger.info("\n📊 Posts by subreddit:")
        result = self.client.query(query1).result()
        for row in result:
            logger.info(f"  r/{row.subreddit}: {row.post_count:,} posts ({row.earliest_post} to {row.latest_post})")
        
        # Query 2: FirstTimeHomeBuyer extraction stats
        query2 = f"""
        SELECT 
            COUNT(*) as total_posts,
            COUNTIF(location IS NOT NULL) as posts_with_location,
            COUNTIF(purchase_price IS NOT NULL) as posts_with_price,
            ROUND(AVG(purchase_price), 0) as avg_price
        FROM `{self.table_ref}`
        WHERE subreddit = 'FirstTimeHomeBuyer'
        """
        
        logger.info("\n📊 FirstTimeHomeBuyer extraction stats:")
        result = self.client.query(query2).result()
        for row in result:
            logger.info(f"  Total posts: {row.total_posts:,}")
            logger.info(f"  Posts with location: {row.posts_with_location:,} ({row.posts_with_location/row.total_posts*100:.1f}%)")
            logger.info(f"  Posts with price: {row.posts_with_price:,} ({row.posts_with_price/row.total_posts*100:.1f}%)")
            if row.avg_price:
                logger.info(f"  Average price: ${row.avg_price:,.0f}")
        
        # Query 3: SameGrassButGreener city mentions
        query3 = f"""
        SELECT 
            COUNT(*) as total_posts,
            COUNTIF(city_mentions IS NOT NULL) as posts_with_cities
        FROM `{self.table_ref}`
        WHERE subreddit = 'SameGrassButGreener'
        """
        
        logger.info("\n📊 SameGrassButGreener extraction stats:")
        result = self.client.query(query3).result()
        for row in result:
            logger.info(f"  Total posts: {row.total_posts:,}")
            logger.info(f"  Posts with city mentions: {row.posts_with_cities:,} ({row.posts_with_cities/row.total_posts*100:.1f}%)")
        
        logger.info("\n✅ Verification complete!")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload Reddit data to BigQuery')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only run verification queries')
    
    args = parser.parse_args()
    
    # Load configuration from environment
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = os.getenv('BQ_DATASET_ID', 'housing_dashboard')
    table_id = 'reddit_posts'
    
    if not project_id:
        logger.error("GCP_PROJECT_ID not found in environment variables")
        logger.error("Please set GCP_PROJECT_ID in your .env file")
        return
    
    # Define paths
    processed_dir = Path(__file__).parent / 'reddit_processed'
    csv_file = processed_dir / 'reddit_posts.csv'
    
    # Create uploader
    uploader = RedditBigQueryUploader(
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
