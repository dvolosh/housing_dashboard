"""
Google Trends Data Preprocessor

Transforms raw Google Trends CSV data into normalized format for BigQuery.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrendsPreprocessor:
    """Handles preprocessing of raw Google Trends data"""
    
    # Term keys (must match fetch_trends.py)
    TERM_KEYS = [
        'mortgage_rate',
        'foreclosure',
        'house_hunting',
        'first_time_home_buyer',
        'housing_market_crash'
    ]
    
    def __init__(self, raw_dir: str, processed_dir: str):
        """
        Initialize preprocessor
        
        Args:
            raw_dir: Directory with raw CSV files
            processed_dir: Directory to save processed CSV
        """
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def process_term(self, term_key: str) -> pd.DataFrame:
        """
        Process data for a single term
        
        Args:
            term_key: Term key
            
        Returns:
            DataFrame with processed data
        """
        logger.info(f"Processing {term_key}...")
        
        # Load raw data
        data_file = self.raw_dir / f"{term_key}_data.csv"
        metadata_file = self.raw_dir / f"{term_key}_metadata.json"
        
        if not data_file.exists():
            logger.warning(f"No data file found: {data_file}")
            return pd.DataFrame()
        
        # Load data
        df = pd.read_csv(data_file, parse_dates=['date'])
        
        # Load metadata
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        search_term = metadata['search_term']
        geo = metadata['geo']
        
        logger.info(f"  Loaded {len(df)} records")
        
        # Ensure interest_score is integer (0-100)
        df['interest_score'] = df['interest_score'].fillna(0).astype(int)
        
        # Add metadata columns
        df['search_term'] = search_term
        df['region'] = geo
        
        # Select and order columns
        df = df[['date', 'search_term', 'interest_score', 'region']]
        
        # Sort by date
        df = df.sort_values('date')
        
        logger.info(f"  Processed {len(df)} records")
        logger.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"  Interest score range: {df['interest_score'].min()} to {df['interest_score'].max()}")
        
        return df
    
    def process_all(self) -> pd.DataFrame:
        """
        Process all terms and combine into single DataFrame
        
        Returns:
            Combined DataFrame with all trends data
        """
        logger.info("Starting Google Trends data preprocessing...")
        logger.info(f"{'='*60}")
        
        all_dfs = []
        
        for term_key in self.TERM_KEYS:
            try:
                df = self.process_term(term_key)
                if not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Error processing {term_key}: {e}")
        
        if not all_dfs:
            logger.warning("No data to process")
            return pd.DataFrame()
        
        # Combine all terms
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Sort by date and term
        combined_df = combined_df.sort_values(['date', 'search_term'])
        
        # Save to CSV
        output_file = self.processed_dir / 'trends_data.csv'
        combined_df.to_csv(output_file, index=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("✅ Preprocessing complete!")
        logger.info(f"Total records: {len(combined_df)}")
        logger.info(f"Terms: {combined_df['search_term'].nunique()}")
        logger.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
        logger.info(f"Output: {output_file}")
        
        return combined_df


def main():
    """Main execution function"""
    # Define paths
    raw_dir = Path(__file__).parent / 'trends_raw'
    processed_dir = Path(__file__).parent / 'trends_processed'
    
    # Create preprocessor
    preprocessor = TrendsPreprocessor(
        raw_dir=str(raw_dir),
        processed_dir=str(processed_dir)
    )
    
    # Process all data
    preprocessor.process_all()


if __name__ == '__main__':
    main()
