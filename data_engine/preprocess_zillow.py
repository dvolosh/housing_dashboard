"""
Zillow Data Preprocessing Script

Transforms wide-format Zillow CSVs into normalized long-format data
suitable for BigQuery upload.

Input: CSV files in zillow_raw/ with dates as columns
Output: Parquet files in zillow_processed/ with normalized schema
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ZillowPreprocessor:
    """Handles transformation of Zillow raw data to normalized format"""
    
    # Mapping of filenames to metric types
    METRIC_MAPPING = {
        'median_sale_price.csv': 'median_sale_price',
        'zhvi_home_value.csv': 'zhvi',
        'active_listings.csv': 'active_listings',
        'market_heat_index.csv': 'market_heat_index',
        'new_construction_median_sale_price.csv': 'new_construction_median_sale_price',
        'new_construction_sales_count.csv': 'new_construction_sales_count',
        'new_homeowner_affordability.csv': 'new_homeowner_affordability',
        'new_listings.csv': 'new_listings',
        'sales_count.csv': 'sales_count'
    }
    
    # Metadata columns (not date columns)
    METADATA_COLS = ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']
    
    def __init__(self, raw_dir: str, processed_dir: str):
        """
        Initialize preprocessor
        
        Args:
            raw_dir: Path to directory containing raw CSV files
            processed_dir: Path to output directory for processed files
        """
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        
        # Create processed directory if it doesn't exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def process_file(self, filename: str) -> pd.DataFrame:
        """
        Process a single Zillow CSV file
        
        Args:
            filename: Name of the CSV file to process
            
        Returns:
            DataFrame in long format with normalized schema
        """
        logger.info(f"Processing {filename}...")
        
        # Read CSV
        filepath = self.raw_dir / filename
        df = pd.read_csv(filepath)
        
        logger.info(f"  Loaded {len(df)} regions with {len(df.columns)} columns")
        
        # Get metric type from filename
        metric_type = self.METRIC_MAPPING.get(filename)
        if not metric_type:
            raise ValueError(f"Unknown file: {filename}")
        
        # Identify date columns (all columns except metadata)
        date_cols = [col for col in df.columns if col not in self.METADATA_COLS]
        
        logger.info(f"  Found {len(date_cols)} date columns from {date_cols[0]} to {date_cols[-1]}")
        
        # Transform from wide to long format
        df_long = pd.melt(
            df,
            id_vars=self.METADATA_COLS,
            value_vars=date_cols,
            var_name='date',
            value_name='value'
        )
        
        # Add metric type column
        df_long['metric_type'] = metric_type
        
        # Convert date strings to datetime
        df_long['date'] = pd.to_datetime(df_long['date'])
        
        # Clean column names (lowercase, snake_case)
        df_long.columns = [col.lower().replace(' ', '_') for col in df_long.columns]
        
        # Rename columns to match BigQuery schema
        df_long = df_long.rename(columns={
            'regionid': 'region_id',
            'sizerank': 'size_rank',
            'regionname': 'region_name',
            'regiontype': 'region_type',
            'statename': 'state_name'
        })
        
        # Reorder columns
        df_long = df_long[[
            'region_id', 'region_name', 'region_type', 'state_name',
            'metric_type', 'date', 'value'
        ]]
        
        # Remove rows with null values
        initial_rows = len(df_long)
        df_long = df_long.dropna(subset=['value'])
        removed_rows = initial_rows - len(df_long)
        
        if removed_rows > 0:
            logger.info(f"  Removed {removed_rows:,} rows with null values")
        
        logger.info(f"  Transformed to {len(df_long):,} rows")
        
        return df_long
    
    def process_all(self, save_format: str = 'parquet') -> pd.DataFrame:
        """
        Process all Zillow CSV files and combine into single dataset
        
        Args:
            save_format: Output format ('parquet' or 'csv')
            
        Returns:
            Combined DataFrame with all metrics
        """
        logger.info("Starting Zillow data preprocessing...")
        logger.info(f"Raw data directory: {self.raw_dir}")
        logger.info(f"Processed data directory: {self.processed_dir}")
        
        all_data = []
        
        # Process each file
        for filename in self.METRIC_MAPPING.keys():
            try:
                df = self.process_file(filename)
                all_data.append(df)
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                raise
        
        # Combine all dataframes
        logger.info("Combining all metrics...")
        combined_df = pd.concat(all_data, ignore_index=True)
        
        logger.info(f"Total rows: {len(combined_df):,}")
        logger.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
        logger.info(f"Unique regions: {combined_df['region_id'].nunique():,}")
        logger.info(f"Metrics: {combined_df['metric_type'].unique().tolist()}")
        
        # Save to file
        output_filename = f"zillow_combined.{save_format}"
        output_path = self.processed_dir / output_filename
        
        if save_format == 'parquet':
            combined_df.to_parquet(output_path, index=False, compression='snappy')
        elif save_format == 'csv':
            combined_df.to_csv(output_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {save_format}")
        
        logger.info(f"Saved to {output_path}")
        
        # Print sample data
        logger.info("\nSample data:")
        print(combined_df.head(10).to_string())
        
        # Print data quality summary
        logger.info("\nData Quality Summary:")
        logger.info(f"  Total rows: {len(combined_df):,}")
        logger.info(f"  Null values: {combined_df.isnull().sum().sum()}")
        logger.info(f"  Duplicate rows: {combined_df.duplicated().sum()}")
        
        # Print value statistics by metric
        logger.info("\nValue statistics by metric:")
        for metric in combined_df['metric_type'].unique():
            metric_data = combined_df[combined_df['metric_type'] == metric]['value']
            logger.info(f"  {metric}:")
            logger.info(f"    Count: {len(metric_data):,}")
            logger.info(f"    Mean: ${metric_data.mean():,.2f}")
            logger.info(f"    Min: ${metric_data.min():,.2f}")
            logger.info(f"    Max: ${metric_data.max():,.2f}")
        
        return combined_df
    
    def create_city_latest_table(self, df_long: pd.DataFrame) -> pd.DataFrame:
        """
        Create table with latest metric values for each city
        
        Args:
            df_long: Long-format DataFrame with all metrics
            
        Returns:
            Wide-format DataFrame with latest values for each city-metric
        """
        logger.info("Creating city latest table...")
        
        # Sort by date and get latest for each city-metric combination
        df_sorted = df_long.sort_values('date')
        df_latest = df_sorted.groupby(
            ['region_name', 'state_name', 'metric_type', 'region_type']
        ).last().reset_index()
        
        logger.info(f"  Found {len(df_latest)} city-metric combinations")
        
        # Pivot to wide format (one row per city, columns for each metric)
        df_pivot = df_latest.pivot_table(
            index=['region_name', 'state_name', 'region_type'],
            columns='metric_type',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Add latest_date column (max date across all metrics for each city)
        latest_dates = df_latest.groupby(['region_name', 'state_name'])['date'].max().reset_index()
        df_pivot = df_pivot.merge(latest_dates, on=['region_name', 'state_name'])
        
        logger.info(f"  Created table with {len(df_pivot)} cities and {len(df_pivot.columns)} columns")
        
        return df_pivot
    
    def create_state_aggregated_table(self, df_city_latest: pd.DataFrame) -> pd.DataFrame:
        """
        Create pre-aggregated state-level metrics
        
        Args:
            df_city_latest: Wide-format DataFrame with city-level data
            
        Returns:
            DataFrame with aggregated state-level metrics
        """
        logger.info("Creating state aggregated table...")
        
        # Filter to MSA (city-level) only
        df_msa = df_city_latest[df_city_latest['region_type'] == 'msa'].copy()
        
        # Define which metrics to sum vs average
        sum_metrics = [
            'active_listings', 'new_listings', 'sales_count', 
            'new_construction_sales_count'
        ]
        
        # Build aggregation dictionary
        agg_dict = {}
        for col in df_msa.columns:
            if col == 'state_name':
                continue  # Grouping column
            elif col == 'region_name':
                agg_dict[col] = 'count'  # Will become city_count
            elif col == 'latest_date':
                agg_dict[col] = 'max'
            elif col in sum_metrics:
                agg_dict[col] = 'sum'
            elif col not in ['region_type', 'date']:
                agg_dict[col] = 'mean'
        
        # Perform aggregation
        df_state = df_msa.groupby('state_name').agg(agg_dict).reset_index()
        df_state.rename(columns={'region_name': 'city_count'}, inplace=True)
        
        logger.info(f"  Created table with {len(df_state)} states and {len(df_state.columns)} columns")
        
        return df_state


def main():
    """Main execution function"""
    # Define paths
    raw_dir = Path(__file__).parent / 'zillow_raw'
    processed_dir = Path(__file__).parent / 'zillow_processed'
    
    # Create preprocessor
    preprocessor = ZillowPreprocessor(
        raw_dir=str(raw_dir),
        processed_dir=str(processed_dir)
    )
    
    # Process all files to get long-format data
    df_long = preprocessor.process_all(save_format='parquet')
    
    logger.info("\n" + "="*60)
    logger.info("Creating pre-computed aggregation tables...")
    logger.info("="*60)
    
    # Create city latest table (wide format with latest values)
    df_city_latest = preprocessor.create_city_latest_table(df_long)
    city_output_path = processed_dir / 'zillow_city_latest.parquet'
    df_city_latest.to_parquet(city_output_path, index=False)
    logger.info(f"✅ Saved city latest table to: {city_output_path}")
    
    # Create state aggregated table
    df_state_agg = preprocessor.create_state_aggregated_table(df_city_latest)
    state_output_path = processed_dir / 'zillow_state_aggregated.parquet'
    df_state_agg.to_parquet(state_output_path, index=False)
    logger.info(f"✅ Saved state aggregated table to: {state_output_path}")
    
    logger.info("\n" + "="*60)
    logger.info("✅ All preprocessing complete!")
    logger.info("="*60)
    logger.info(f"Long-format data: {processed_dir / 'zillow_combined.parquet'}")
    logger.info(f"City latest data: {city_output_path}")
    logger.info(f"State aggregated data: {state_output_path}")




if __name__ == '__main__':
    main()
