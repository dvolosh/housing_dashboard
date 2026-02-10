"""
Google Trends Data Fetcher using pytrends

Fetches search interest data for housing-related terms.
Uses pytrends library (unofficial Google Trends API).

Documentation: https://pypi.org/project/pytrends/
"""

from pytrends.request import TrendReq
import pandas as pd
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import json
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


class TrendsFetcher:
    """Handles fetching Google Trends data"""
    
    # Housing market topics using validated Google Trends MIDs and search terms
    # Mix of topics (MIDs) and search terms based on availability/validation
    SEARCH_TERMS = {
        # Involuntary Supply - Homes coming to market from life events
        'estate_sale': {
            'term': '/m/02rmp0',  # Topic: Estate Sale (VALIDATED)
            'display_name': 'Estate Sales',
            'category': 'Involuntary Supply',
            'description': 'Estate liquidation - indicates housing supply from inheritance/death',
            'is_topic': True
        },
        
        # Distress Signal - Market stress indicators
        'foreclosure_auction': {
            'term': '/m/02tp2m',  # Topic: Foreclosure Auction (VALIDATED)
            'display_name': 'Foreclosure Auctions',
            'category': 'Distress Signal',
            'description': 'Foreclosure activity - indicates forced selling/market distress',
            'is_topic': True
        },
        
        # Financial Friction - Homeownership cost pressures
        'home_insurance': {
            'term': 'home insurance',  # Search term (MID /m/01v8_f not found)
            'display_name': 'Home Insurance',
            'category': 'Financial Friction',
            'description': 'Insurance concerns - indicates rising carry costs/affordability stress',
            'is_topic': False
        },
        
        # Market Access - Alternative financing pathways
        'mortgage_assumption': {
            'term': '/m/0ddkfg',  # Topic: Mortgage Assumption (trying MID, can fallback to 'mortgage payments')
            'display_name': 'Mortgage Assumption',
            'category': 'Market Access',
            'description': 'Assumable mortgage interest - indicates buyers seeking rate relief',
            'is_topic': True
        }
    }
    
    RATE_LIMIT_DELAY = 2.0  # 2 seconds between requests to avoid blocking
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 3
    
    def __init__(self, raw_dir: str, geo: str = 'US'):
        """
        Initialize Trends fetcher
        
        Args:
            raw_dir: Directory to save raw data
            geo: Geographic region (default: US)
        """
        self.raw_dir = Path(raw_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.geo = geo
        
        # Initialize pytrends
        self.pytrends = TrendReq(hl='en-US', tz=360)
        
        # Track last request time for rate limiting
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Enforce rate limiting between API requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def get_last_fetch_date(self, term_key: str) -> Optional[str]:
        """
        Get the date of the last fetched data for a term
        
        Args:
            term_key: Term key from SEARCH_TERMS
            
        Returns:
            Last date (YYYY-MM-DD) or None if no data exists
        """
        metadata_file = self.raw_dir / f"{term_key}_metadata.json"
        
        if not metadata_file.exists():
            return None
        
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                return metadata.get('last_date')
        except Exception as e:
            logger.warning(f"Could not read metadata for {term_key}: {e}")
            return None
    
    def save_term_data(self, term_key: str, data: pd.DataFrame, metadata: Dict):
        """
        Save term data and metadata
        
        Args:
            term_key: Term key from SEARCH_TERMS
            data: DataFrame with trends data
            metadata: Fetch metadata
        """
        # Save data as CSV
        data_file = self.raw_dir / f"{term_key}_data.csv"
        
        # Load existing data if file exists
        if data_file.exists():
            try:
                existing_data = pd.read_csv(data_file, parse_dates=['date'])
                
                # Merge and deduplicate
                data['date'] = pd.to_datetime(data.index)
                data = data.reset_index(drop=True)
                
                combined = pd.concat([existing_data, data], ignore_index=True)
                combined = combined.drop_duplicates(subset=['date'], keep='last')
                combined = combined.sort_values('date')
                
                data = combined
            except Exception as e:
                logger.warning(f"Could not load existing data: {e}")
        else:
            data['date'] = pd.to_datetime(data.index)
            data = data.reset_index(drop=True)
        
        # Save merged data
        data.to_csv(data_file, index=False)
        
        # Save metadata
        metadata_file = self.raw_dir / f"{term_key}_metadata.json"
        
        term_info = self.SEARCH_TERMS[term_key]
        metadata_to_save = {
            'term_key': term_key,
            'term': term_info['term'],  # MID or search string
            'display_name': term_info.get('display_name', term_info['term']),
            'category': term_info['category'],
            'is_topic': term_info.get('is_topic', False),
            'description': term_info['description'],
            'geo': self.geo,
            'last_date': data['date'].max().strftime('%Y-%m-%d'),
            'total_records': len(data),
            'fetched_at': datetime.now().isoformat(),
            'fetch_metadata': metadata
        }
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata_to_save, f, indent=2)
        
        logger.info(f"  Saved {len(data)} records to {data_file}")
    
    def fetch_term(
        self,
        term_key: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        incremental: bool = True,
        retry_count: int = 0
    ):
        """
        Fetch trends data for a single term
        
        Args:
            term_key: Term key from SEARCH_TERMS
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            incremental: If True, fetch only new data since last update
            retry_count: Current retry attempt
        """
        term_info = self.SEARCH_TERMS[term_key]
        display_name = term_info.get('display_name', term_info['term'])
        is_topic = term_info.get('is_topic', False)
        term_type = "Topic" if is_topic else "Search Term"
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Fetching: {display_name} ({term_type})")
        logger.info(f"{'='*60}")
        
        # Determine time range
        if incremental:
            last_date = self.get_last_fetch_date(term_key)
            if last_date:
                # Fetch from day after last date
                start_dt = datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)
                timeframe_start = start_dt.strftime('%Y-%m-%d')
                logger.info(f"Incremental update from {timeframe_start}")
            else:
                # No previous data, fetch last 90 days
                timeframe_start = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                logger.info(f"No previous data, fetching last 90 days")
        else:
            # Full fetch
            if start_date:
                timeframe_start = start_date
            else:
                # Default: last 5 years
                timeframe_start = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
            logger.info(f"Full fetch from {timeframe_start}")
        
        # End date
        if end_date:
            timeframe_end = end_date
        else:
            timeframe_end = datetime.now().strftime('%Y-%m-%d')
        
        timeframe = f"{timeframe_start} {timeframe_end}"
        
        # Rate limit
        self._rate_limit()
        
        try:
            # Build payload
            search_term = self.SEARCH_TERMS[term_key]['term']
            self.pytrends.build_payload(
                kw_list=[search_term],
                cat=0,  # All categories
                timeframe=timeframe,
                geo=self.geo,
                gprop=''  # Web search
            )
            
            # Get interest over time
            logger.info(f"  Fetching data for timeframe: {timeframe}")
            data = self.pytrends.interest_over_time()
            
            if data.empty:
                logger.info("  No data returned")
                return
            
            # Remove 'isPartial' column if present
            if 'isPartial' in data.columns:
                data = data.drop(columns=['isPartial'])
            
            # Rename column to 'interest_score'
            data.columns = ['interest_score']
            
            logger.info(f"  Retrieved {len(data)} data points")
            
            # Save data
            metadata = {
                'timeframe': timeframe,
                'geo': self.geo,
                'records_fetched': len(data)
            }
            
            self.save_term_data(term_key, data, metadata)
            
        except Exception as e:
            if retry_count < self.MAX_RETRIES:
                wait_time = self.BACKOFF_FACTOR ** retry_count
                logger.warning(f"Request failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                self.fetch_term(term_key, start_date, end_date, incremental, retry_count + 1)
            else:
                logger.error(f"Failed after {self.MAX_RETRIES} retries: {e}")
                raise
    
    def fetch_all(self, incremental: bool = True):
        """
        Fetch all configured search terms
        
        Args:
            incremental: If True, only fetch new data since last update
        """
        logger.info("Starting Google Trends data fetch...")
        logger.info(f"Mode: {'Incremental' if incremental else 'Full download'}")
        logger.info(f"Terms to fetch: {len(self.SEARCH_TERMS)}")
        logger.info(f"Geographic region: {self.geo}")
        
        for term_key in self.SEARCH_TERMS.keys():
            try:
                self.fetch_term(term_key, incremental=incremental)
            except Exception as e:
                logger.error(f"Error fetching {term_key}: {e}")
                # Continue with other terms
        
        logger.info("\n✅ Google Trends data fetch complete!")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch Google Trends data')
    parser.add_argument('--term', choices=list(TrendsFetcher.SEARCH_TERMS.keys()) + ['all'],
                       default='all', help='Search term to fetch')
    parser.add_argument('--incremental', action='store_true', default=True,
                       help='Fetch only new data since last update')
    parser.add_argument('--full', action='store_true',
                       help='Full fetch (override incremental)')
    parser.add_argument('--start-date', type=str,
                       help='Start date (YYYY-MM-DD) for full fetch')
    parser.add_argument('--end-date', type=str,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--geo', type=str, default='US',
                       help='Geographic region (default: US)')
    parser.add_argument('--test', action='store_true',
                       help='Test mode: fetch last 30 days only')
    
    args = parser.parse_args()
    
    # Define paths
    raw_dir = Path(__file__).parent / 'trends_raw'
    
    # Create fetcher
    fetcher = TrendsFetcher(raw_dir=str(raw_dir), geo=args.geo)
    
    # Test mode
    if args.test:
        logger.info("TEST MODE: Fetching last 30 days")
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if args.term == 'all':
            for term_key in fetcher.SEARCH_TERMS.keys():
                fetcher.fetch_term(term_key, start_date=start_date, incremental=False)
        else:
            fetcher.fetch_term(args.term, start_date=start_date, incremental=False)
        return
    
    # Determine incremental vs full
    incremental = args.incremental and not args.full
    
    # Fetch data
    if args.term == 'all':
        fetcher.fetch_all(incremental=incremental)
    else:
        fetcher.fetch_term(
            args.term,
            start_date=args.start_date,
            end_date=args.end_date,
            incremental=incremental
        )


if __name__ == '__main__':
    main()
