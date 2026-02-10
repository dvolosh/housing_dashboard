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
        'estate_sale',
        'foreclosure_auction',
        'home_insurance',
        'mortgage_assumption'
    ]
    
    # Category mapping
    CATEGORY_MAP = {
        'estate_sale': 'Involuntary Supply',
        'foreclosure_auction': 'Distress Signal',
        'home_insurance': 'Financial Friction',
        'mortgage_assumption': 'Market Access'
    }
    
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
        
        # Use display_name if available, fallback to term
        search_term = metadata.get('display_name', metadata.get('search_term', metadata['term']))
        geo = metadata['geo']
        
        logger.info(f"  Loaded {len(df)} records")
        
        # Ensure interest_score is integer (0-100)
        df['interest_score'] = df['interest_score'].fillna(0).astype(int)
        
        # Add metadata columns
        df['search_term'] = search_term
        df['region'] = geo
        df['category'] = self.CATEGORY_MAP.get(term_key, 'Unknown')
        
        # Select and order columns
        df = df[['date', 'search_term', 'category', 'interest_score', 'region']]
        
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
        
        logger.info(f"\nDaily data collected: {len(combined_df)} records")
        
        # Aggregate to weekly level to reduce noise
        logger.info("Aggregating to weekly level...")
        
        # Group by week and search_term, calculate average interest score
        weekly_dfs = []
        for term in combined_df['search_term'].unique():
            term_df = combined_df[combined_df['search_term'] == term].copy()
            
            # Set date as index for resampling
            term_df = term_df.set_index('date')
            
            # Resample to weekly (Sunday start), taking mean of interest_score
            # label='left' makes week_start_date the actual Sunday the week starts, not the end boundary
            weekly = term_df.resample('W-SUN', label='left').agg({
                'interest_score': 'mean',
                'category': 'first',  # Category is constant per term
                'region': 'first'     # Region is constant
            }).reset_index()
            
            # Rename columns for clarity
            weekly = weekly.rename(columns={
                'date': 'week_start_date',
                'interest_score': 'avg_interest_score'
            })
            
            # Round average to integer
            weekly['avg_interest_score'] = weekly['avg_interest_score'].round().astype(int)
            
            # Add search term back
            weekly['search_term'] = term
            
            weekly_dfs.append(weekly)
        
        # Combine all weekly data
        combined_df = pd.concat(weekly_dfs, ignore_index=True)
        
        # Sort by date and term
        combined_df = combined_df.sort_values(['week_start_date', 'search_term'])
        
        # Reorder columns
        combined_df = combined_df[['week_start_date', 'search_term', 'category', 'avg_interest_score', 'region']]
        
        # Save to CSV
        output_file = self.processed_dir / 'trends_data.csv'
        combined_df.to_csv(output_file, index=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("✅ Preprocessing complete!")
        logger.info(f"Total weekly records: {len(combined_df)}")
        logger.info(f"Terms: {combined_df['search_term'].nunique()}")
        logger.info(f"Categories: {', '.join(combined_df['category'].unique())}")
        logger.info(f"Week range: {combined_df['week_start_date'].min()} to {combined_df['week_start_date'].max()}")
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
