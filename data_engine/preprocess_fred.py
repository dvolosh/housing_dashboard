"""
FRED Data Preprocessing Script

Transforms FRED Excel data (organized by frequency sheets) into normalized 
long-format data suitable for BigQuery upload.

Input: Excel file in historic_fred/ with sheets by frequency
Output: Parquet file in fred_processed/ with normalized schema
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


class FREDPreprocessor:
    """Handles transformation of FRED Excel data to normalized format"""
    
    # Mapping of FRED series IDs to metadata
    SERIES_METADATA = {
        'MORTGAGE30US': {
            'name': '30-Year Fixed Rate Mortgage Average',
            'units': 'percent',
            'description': 'Average 30-year fixed mortgage rate in the United States'
        },
        'CPIAUCSL': {
            'name': 'Consumer Price Index for All Urban Consumers',
            'units': 'index',
            'description': 'CPI for all urban consumers, all items (1982-1984=100)'
        },
        'RHORUSQ156N': {
            'name': 'Homeownership Rate in the United States',
            'units': 'percent',
            'description': 'Percentage of households that are owner-occupied'
        },
        'HPIPONM226S_PCH': {
            'name': 'Purchase Only House Price Index',
            'units': 'percent_change',
            'description': 'House price index for purchase-only transactions (percent change)'
        },
        'GDPC1': {
            'name': 'Real Gross Domestic Product',
            'units': 'billions_chained_2017_dollars',
            'description': 'Real GDP in billions of chained 2017 dollars'
        }
    }
    
    # Sheet name to frequency mapping
    SHEET_FREQUENCY_MAP = {
        'Annual': 'annual',
        'Monthly': 'monthly',
        'Quarterly': 'quarterly',
        'Weekly, Ending Thursday': 'weekly'
    }
    
    def __init__(self, excel_file: str, processed_dir: str):
        """
        Initialize preprocessor
        
        Args:
            excel_file: Path to Excel file with FRED data
            processed_dir: Path to output directory for processed files
        """
        self.excel_file = Path(excel_file)
        self.processed_dir = Path(processed_dir)
        
        # Create processed directory if it doesn't exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        if not self.excel_file.exists():
            raise FileNotFoundError(f"Excel file not found: {self.excel_file}")
    
    def process_sheet(self, sheet_name: str, frequency: str) -> pd.DataFrame:
        """
        Process a single sheet from the Excel file
        
        Args:
            sheet_name: Name of the Excel sheet
            frequency: Frequency of the data (weekly/monthly/quarterly/annual)
            
        Returns:
            DataFrame in long format with normalized schema
        """
        logger.info(f"Processing sheet: {sheet_name} (frequency: {frequency})...")
        
        # Read the sheet
        df = pd.read_excel(self.excel_file, sheet_name=sheet_name)
        
        logger.info(f"  Loaded {len(df)} rows with {len(df.columns)} columns")
        logger.info(f"  Columns: {df.columns.tolist()}")
        
        # Get series columns (all except observation_date)
        series_cols = [col for col in df.columns if col != 'observation_date']
        
        if not series_cols:
            logger.warning(f"  No data columns found in {sheet_name}, skipping")
            return pd.DataFrame()
        
        logger.info(f"  Found {len(series_cols)} series: {series_cols}")
        
        # Transform from wide to long format
        df_long = pd.melt(
            df,
            id_vars=['observation_date'],
            value_vars=series_cols,
            var_name='series_id',
            value_name='value'
        )
        
        # Add frequency column
        df_long['frequency'] = frequency
        
        # Convert date to datetime
        df_long['observation_date'] = pd.to_datetime(df_long['observation_date'])
        
        # Add metadata for each series
        df_long['series_name'] = df_long['series_id'].map(
            lambda x: self.SERIES_METADATA.get(x, {}).get('name', x)
        )
        df_long['units'] = df_long['series_id'].map(
            lambda x: self.SERIES_METADATA.get(x, {}).get('units', 'unknown')
        )
        
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
        Process all sheets in the Excel file and combine into single dataset
        
        Args:
            save_format: Output format ('parquet' or 'csv')
            
        Returns:
            Combined DataFrame with all metrics
        """
        logger.info("Starting FRED data preprocessing...")
        logger.info(f"Excel file: {self.excel_file}")
        logger.info(f"Processed data directory: {self.processed_dir}")
        
        all_data = []
        
        # Process each frequency sheet
        for sheet_name, frequency in self.SHEET_FREQUENCY_MAP.items():
            try:
                df = self.process_sheet(sheet_name, frequency)
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                logger.error(f"Error processing {sheet_name}: {e}")
                # Continue with other sheets
        
        if not all_data:
            raise ValueError("No data was processed from any sheet")
        
        # Combine all dataframes
        logger.info("\nCombining all sheets...")
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Reorder columns to match BigQuery schema
        combined_df = combined_df[[
            'series_id', 'series_name', 'observation_date', 'value', 
            'frequency', 'units'
        ]]
        
        # Rename columns to match BigQuery schema
        combined_df = combined_df.rename(columns={
            'observation_date': 'date'
        })
        
        # Convert date to string format (YYYY-MM-DD) to avoid Parquet INT64/INT32 mismatch
        # BigQuery will parse this as DATE type
        combined_df['date'] = combined_df['date'].dt.strftime('%Y-%m-%d')
        
        # Sort by date first, then by series_id to interleave series
        # This ensures all series appear in the first 100 rows for BigQuery schema detection
        combined_df = combined_df.sort_values(['date', 'series_id'])
        
        # Add last_updated timestamp in BigQuery-compatible format (without microseconds)
        # Use ISO 8601 format: YYYY-MM-DD HH:MM:SS
        combined_df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"\nTotal rows: {len(combined_df):,}")
        logger.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
        logger.info(f"Unique series: {combined_df['series_id'].nunique()}")
        logger.info(f"Series: {combined_df['series_id'].unique().tolist()}")
        
        # Save to file
        output_filename = f"fred_combined.{save_format}"
        output_path = self.processed_dir / output_filename
        
        if save_format == 'parquet':
            combined_df.to_parquet(output_path, index=False, compression='snappy')
        elif save_format == 'csv':
            # Write CSV with explicit parameters for BigQuery compatibility
            combined_df.to_csv(
                output_path, 
                index=False,
                encoding='utf-8',
                quoting=3,  # QUOTE_NONE - don't quote any fields
                escapechar='\\',  # Use backslash for escaping
                lineterminator='\n'  # Use Unix line endings
            )
        else:
            raise ValueError(f"Unsupported format: {save_format}")
        
        logger.info(f"\nSaved to {output_path}")
        
        # Print sample data
        logger.info("\nSample data:")
        print(combined_df.head(10).to_string())
        
        # Print data quality summary
        logger.info("\nData Quality Summary:")
        logger.info(f"  Total rows: {len(combined_df):,}")
        logger.info(f"  Null values: {combined_df.isnull().sum().sum()}")
        logger.info(f"  Duplicate rows: {combined_df.duplicated().sum()}")
        
        # Print statistics by series
        logger.info("\nObservations by series:")
        for series_id in combined_df['series_id'].unique():
            series_data = combined_df[combined_df['series_id'] == series_id]
            series_name = series_data['series_name'].iloc[0]
            frequency = series_data['frequency'].iloc[0]
            # Date is now a string, so just use min/max directly
            date_range = f"{series_data['date'].min()} to {series_data['date'].max()}"
            
            logger.info(f"  {series_id} ({frequency}):")
            logger.info(f"    Name: {series_name}")
            logger.info(f"    Count: {len(series_data):,} observations")
            logger.info(f"    Date range: {date_range}")
            logger.info(f"    Value range: {series_data['value'].min():.2f} to {series_data['value'].max():.2f}")
        
        return combined_df


def main():
    """Main execution function"""
    # Define paths
    excel_file = Path(__file__).parent / 'historic_fred' / 'fred_macro_data.xlsx'
    processed_dir = Path(__file__).parent / 'fred_processed'
    
    # Create preprocessor
    preprocessor = FREDPreprocessor(
        excel_file=str(excel_file),
        processed_dir=str(processed_dir)
    )
    
    # Process all sheets - use CSV format to avoid Parquet type issues
    df = preprocessor.process_all(save_format='csv')
    
    logger.info("\n✅ Preprocessing complete!")
    logger.info(f"Output saved to: {processed_dir / 'fred_combined.parquet'}")


if __name__ == '__main__':
    main()
