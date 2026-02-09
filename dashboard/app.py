"""
Housing Market Dashboard - Streamlit Application

A comprehensive dashboard for analyzing housing market data from multiple sources:
- FRED: Macro economic indicators
- Zillow: Housing market metrics
- Reddit: Social sentiment data
- Google Trends: Search interest data
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Housing Market Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for additional styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #A7D129;
        margin-bottom: 0.5rem;
    }
    
    .sub-header {
        font-size: 1.2rem;
        color: #ededed;
        margin-bottom: 2rem;
        opacity: 0.8;
    }
    
    .metric-card {
        background: #1a1a1a;
        padding: 1.5rem;
        border-radius: 8px;
        border-left: 4px solid #A7D129;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 1rem 2rem;
        font-weight: 500;
    }
    
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        color: #A7D129;
    }
</style>
""", unsafe_allow_html=True)

# Initialize BigQuery client
@st.cache_resource
def get_bigquery_client():
    """Initialize and cache BigQuery client"""
    try:
        project_id = os.getenv('GCP_PROJECT_ID', 'vant-486316')
        return bigquery.Client(project=project_id)
    except Exception as e:
        st.error(f"Failed to initialize BigQuery client: {e}")
        st.info("Please ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly")
        return None

# Data fetching functions with caching
@st.cache_data(ttl=3600)
def fetch_fred_data():
    """Fetch FRED macro indicators"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    query = f"""
    SELECT 
        series_id,
        series_name,
        date,
        value,
        units
    FROM `{client.project}.{dataset_id}.fred_metrics`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
    ORDER BY series_id, date
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching FRED data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_zillow_data(states=None):
    """Fetch Zillow housing metrics"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Build state filter
    state_filter = ""
    if states and len(states) > 0:
        state_list = "', '".join(states)
        state_filter = f"AND state_name IN ('{state_list}')"
    
    query = f"""
    SELECT 
        region_id,
        region_name,
        region_type,
        state_name,
        metric_type,
        date,
        value
    FROM `{client.project}.{dataset_id}.zillow_metrics`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
    {state_filter}
    ORDER BY metric_type, region_name, date
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching Zillow data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_reddit_data(states=None):
    """Fetch Reddit posts data"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Build state filter
    state_filter = ""
    if states and len(states) > 0:
        state_list = "', '".join(states)
        state_filter = f"AND location IN ('{state_list}')"
    
    query = f"""
    SELECT 
        post_id,
        subreddit,
        created_date,
        title,
        score,
        num_comments,
        location,
        purchase_price
    FROM `{client.project}.{dataset_id}.reddit_posts`
    WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
    {state_filter}
    ORDER BY created_date DESC
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching Reddit data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_trends_data(states=None):
    """Fetch Google Trends data"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Build region filter
    region_filter = ""
    if states and len(states) > 0:
        # Convert state names to state codes if needed
        state_list = "', '".join(states)
        region_filter = f"AND region IN ('{state_list}')"
    
    query = f"""
    SELECT 
        date,
        search_term,
        interest_score,
        region
    FROM `{client.project}.{dataset_id}.trends_metrics`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
    {region_filter}
    ORDER BY search_term, date
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching Trends data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_available_states():
    """Get list of available states from Zillow data"""
    client = get_bigquery_client()
    if not client:
        return []
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    query = f"""
    SELECT DISTINCT state_name
    FROM `{client.project}.{dataset_id}.zillow_metrics`
    WHERE state_name IS NOT NULL
    ORDER BY state_name
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df['state_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching states: {e}")
        return []

# Visualization functions
def plot_fred_series(df, series_id, title):
    """Plot a FRED time series"""
    series_data = df[df['series_id'] == series_id].copy()
    
    if series_data.empty:
        st.warning(f"No data available for {series_id}")
        return
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=series_data['date'],
        y=series_data['value'],
        mode='lines',
        name=series_data['series_name'].iloc[0],
        line=dict(color='#A7D129', width=2)
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=series_data['units'].iloc[0] if not series_data.empty else "Value",
        template="plotly_dark",
        hovermode='x unified',
        plot_bgcolor='#000000',
        paper_bgcolor='#000000',
        font=dict(color='#ededed')
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_zillow_metric(df, metric_type, title, region_type='state'):
    """Plot Zillow metrics"""
    metric_data = df[(df['metric_type'] == metric_type) & (df['region_type'] == region_type)].copy()
    
    if metric_data.empty:
        st.warning(f"No data available for {metric_type}")
        return
    
    fig = px.line(
        metric_data,
        x='date',
        y='value',
        color='region_name',
        title=title,
        labels={'value': 'Value', 'date': 'Date', 'region_name': 'Region'}
    )
    
    fig.update_layout(
        template="plotly_dark",
        hovermode='x unified',
        plot_bgcolor='#000000',
        paper_bgcolor='#000000',
        font=dict(color='#ededed')
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_reddit_volume(df):
    """Plot Reddit post volume over time"""
    if df.empty:
        st.warning("No Reddit data available")
        return
    
    # Aggregate by date and subreddit
    volume_data = df.groupby(['created_date', 'subreddit']).size().reset_index(name='post_count')
    
    fig = px.line(
        volume_data,
        x='created_date',
        y='post_count',
        color='subreddit',
        title='Reddit Post Volume Over Time',
        labels={'post_count': 'Number of Posts', 'created_date': 'Date', 'subreddit': 'Subreddit'}
    )
    
    fig.update_layout(
        template="plotly_dark",
        hovermode='x unified',
        plot_bgcolor='#000000',
        paper_bgcolor='#000000',
        font=dict(color='#ededed')
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_trends_interest(df):
    """Plot Google Trends interest scores"""
    if df.empty:
        st.warning("No Trends data available")
        return
    
    fig = px.line(
        df,
        x='date',
        y='interest_score',
        color='search_term',
        title='Google Trends Interest Over Time',
        labels={'interest_score': 'Interest Score (0-100)', 'date': 'Date', 'search_term': 'Search Term'}
    )
    
    fig.update_layout(
        template="plotly_dark",
        hovermode='x unified',
        plot_bgcolor='#000000',
        paper_bgcolor='#000000',
        font=dict(color='#ededed')
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Main application
def main():
    # Header
    st.markdown('<div class="main-header">🏠 Housing Market Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Market Intelligence Platform</div>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("Filters")
        
        # Get available states
        available_states = get_available_states()
        
        # State filter
        selected_states = st.multiselect(
            "Select States",
            options=available_states,
            default=None,
            help="Filter data by state. Leave empty for all states."
        )
        
        st.divider()
        
        st.markdown("### About")
        st.markdown("""
        This dashboard provides comprehensive housing market analysis combining:
        - **FRED**: Economic indicators
        - **Zillow**: Housing metrics
        - **Reddit**: Social sentiment
        - **Google Trends**: Search interest
        """)
    
    # Main content tabs
    tab1, tab2 = st.tabs(["📊 Quantitative Metrics", "💬 Alternative Data"])
    
    with tab1:
        st.header("Quantitative Metrics")
        st.markdown("Macro economic indicators and housing market fundamentals")
        
        # FRED Metrics
        st.subheader("FRED Macro Indicators")
        
        with st.spinner("Loading FRED data..."):
            fred_df = fetch_fred_data()
        
        if not fred_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                plot_fred_series(fred_df, 'MORTGAGE30US', '30-Year Mortgage Rate')
            
            with col2:
                plot_fred_series(fred_df, 'CPIAUCSL', 'Consumer Price Index (CPI)')
        else:
            st.info("No FRED data available. Please check your BigQuery connection.")
        
        st.divider()
        
        # Zillow Metrics
        st.subheader("Zillow Housing Metrics")
        
        with st.spinner("Loading Zillow data..."):
            zillow_df = fetch_zillow_data(selected_states)
        
        if not zillow_df.empty:
            # Show summary metrics
            latest_date = zillow_df['date'].max()
            latest_data = zillow_df[zillow_df['date'] == latest_date]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                zhvi_data = latest_data[latest_data['metric_type'] == 'zhvi']
                if not zhvi_data.empty:
                    avg_zhvi = zhvi_data['value'].mean()
                    st.metric("Average ZHVI", f"${avg_zhvi:,.0f}")
            
            with col2:
                median_price = latest_data[latest_data['metric_type'] == 'median_sale_price']
                if not median_price.empty:
                    avg_price = median_price['value'].mean()
                    st.metric("Avg Median Sale Price", f"${avg_price:,.0f}")
            
            with col3:
                listings = latest_data[latest_data['metric_type'] == 'active_listings']
                if not listings.empty:
                    total_listings = listings['value'].sum()
                    st.metric("Total Active Listings", f"{total_listings:,.0f}")
            
            # Charts
            col1, col2 = st.columns(2)
            
            with col1:
                plot_zillow_metric(zillow_df, 'zhvi', 'Zillow Home Value Index (ZHVI)', 'state')
            
            with col2:
                plot_zillow_metric(zillow_df, 'median_sale_price', 'Median Sale Price', 'state')
        else:
            st.info("No Zillow data available. Please check your BigQuery connection or adjust filters.")
    
    with tab2:
        st.header("Alternative Data")
        st.markdown("Social sentiment and search interest trends")
        
        # Reddit Data
        st.subheader("Reddit Social Sentiment")
        
        with st.spinner("Loading Reddit data..."):
            reddit_df = fetch_reddit_data(selected_states)
        
        if not reddit_df.empty:
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Posts", f"{len(reddit_df):,}")
            
            with col2:
                avg_score = reddit_df['score'].mean()
                st.metric("Avg Post Score", f"{avg_score:.1f}")
            
            with col3:
                posts_with_location = reddit_df['location'].notna().sum()
                pct = (posts_with_location / len(reddit_df)) * 100
                st.metric("Posts with Location", f"{pct:.1f}%")
            
            # Post volume chart
            plot_reddit_volume(reddit_df)
            
            # Recent posts sample
            st.subheader("Recent Posts Sample")
            recent_posts = reddit_df.head(10)[['created_date', 'subreddit', 'title', 'score', 'location']]
            st.dataframe(recent_posts, use_container_width=True, hide_index=True)
        else:
            st.info("No Reddit data available. Please check your BigQuery connection or adjust filters.")
        
        st.divider()
        
        # Google Trends Data
        st.subheader("Google Trends Search Interest")
        
        with st.spinner("Loading Google Trends data..."):
            trends_df = fetch_trends_data(selected_states)
        
        if not trends_df.empty:
            # Summary metrics
            col1, col2 = st.columns(2)
            
            with col1:
                avg_interest = trends_df['interest_score'].mean()
                st.metric("Avg Interest Score", f"{avg_interest:.1f}")
            
            with col2:
                unique_terms = trends_df['search_term'].nunique()
                st.metric("Search Terms Tracked", unique_terms)
            
            # Interest chart
            plot_trends_interest(trends_df)
        else:
            st.info("No Google Trends data available. Please check your BigQuery connection or adjust filters.")

if __name__ == "__main__":
    main()
