"""
Housing Market Dashboard - Plotly Dash Application

A professional dashboard for analyzing housing market data from multiple sources:
- FRED: Macro economic indicators
- Zillow: Housing market metrics with US map visualization
"""

import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Dash app with Bootstrap theme
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)

# Custom CSS for professional dark theme
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --primary-accent: #A7D129;
                --bg-dark: #000000;
                --bg-card: #1a1a1a;
                --text-primary: #ededed;
                --text-secondary: rgba(237, 237, 237, 0.8);
            }
            
            * {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            }
            
            body {
                background-color: var(--bg-dark);
                color: var(--text-primary);
                margin: 0;
                padding: 0;
            }
            
            .main-container {
                max-width: 1400px;
                margin: 0 auto;
                padding: 2rem;
            }
            
            .header {
                text-align: center;
                margin-bottom: 3rem;
                padding: 2rem 0;
            }
            
            .header h1 {
                font-size: 2.5rem;
                font-weight: 700;
                color: var(--primary-accent);
                margin-bottom: 0.5rem;
            }
            
            .header p {
                font-size: 1.2rem;
                color: var(--text-secondary);
                margin: 0;
            }
            
            .metric-card {
                background: var(--bg-card);
                border-radius: 8px;
                padding: 1.5rem;
                margin-bottom: 1.5rem;
                border-left: 4px solid var(--primary-accent);
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
            }
            
            .metric-card h3 {
                color: var(--text-primary);
                font-size: 1.1rem;
                font-weight: 600;
                margin-bottom: 1rem;
            }
            
            .section-title {
                font-size: 1.8rem;
                font-weight: 600;
                color: var(--primary-accent);
                margin: 2rem 0 1rem 0;
                padding-bottom: 0.5rem;
                border-bottom: 2px solid var(--primary-accent);
            }
            
            .filter-section {
                background: var(--bg-card);
                border-radius: 8px;
                padding: 1.5rem;
                margin-bottom: 2rem;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
            }
            
            .Select-control, .Select-menu-outer {
                background-color: var(--bg-card) !important;
                color: var(--text-primary) !important;
            }
            
            .chart-container {
                background: var(--bg-card);
                border-radius: 8px;
                padding: 1rem;
                margin-bottom: 1.5rem;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
            }
            
            /* Modern Dash Dropdown styling - black text on white background */
            .dash-dropdown .Select-control,
            #city-search .Select-control,
            #map-metric .Select-control,
            div[class*="css-"][class*="control"] {
                background-color: #ffffff !important;
                border: 1px solid #cccccc !important;
            }
            
            /* Single value and placeholder - DARK GRAY TEXT */
            .dash-dropdown .Select-value-label,
            .dash-dropdown .Select-placeholder,
            .dash-dropdown .Select-value,
            #city-search .Select-value-label,
            #city-search .Select-placeholder,
            #city-search .Select-value,
            #map-metric .Select-value-label,
            #map-metric .Select-placeholder,
            #map-metric .Select-value,
            div[class*="css-"][class*="singleValue"],
            div[class*="css-"][class*="placeholder"],
            div[class*="css-"] div[class*="Value"],
            .Select--single > .Select-control .Select-value {
                color: #333333 !important;
            }
            
            /* Input field when typing */
            .dash-dropdown input,
            #city-search input,
            #map-metric input {
                color: #333333 !important;
            }
            
            /* Dropdown menu - WHITE BACKGROUND */
            .dash-dropdown .Select-menu-outer,
            #city-search .Select-menu-outer,
            #map-metric .Select-menu-outer,
            div[class*="css-"][class*="menu"] {
                background-color: #ffffff !important;
                border: 1px solid #cccccc !important;
            }
            
            /* Menu options - WHITE BACKGROUND, DARK GRAY TEXT */
            .dash-dropdown .Select-option,
            #city-search .Select-option,
            #map-metric .Select-option,
            div[class*="css-"][class*="option"] {
                background-color: #ffffff !important;
                color: #333333 !important;
            }
            
            /* Focused/hover option - LIGHT GRAY BACKGROUND */
            .dash-dropdown .Select-option.is-focused,
            #city-search .Select-option.is-focused,
            #map-metric .Select-option.is-focused,
            div[class*="css-"][class*="option"]:hover {
                background-color: #f0f0f0 !important;
                color: #333333 !important;
            }
            
            /* Selected option - LIGHTER GRAY */
            .dash-dropdown .Select-option.is-selected,
            #city-search .Select-option.is-selected,
            #map-metric .Select-option.is-selected {
                background-color: #e0e0e0 !important;
                color: #333333 !important;
            }
            
            /* Arrow indicator */
            .dash-dropdown svg,
            #city-search svg,
            #map-metric svg {
                fill: #333333 !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Initialize BigQuery client
def get_bigquery_client():
    """Initialize BigQuery client"""
    try:
        project_id = os.getenv('GCP_PROJECT_ID', 'vant-486316')
        return bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return None

# Data fetching functions
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
        print(f"Error fetching FRED data: {e}")
        return pd.DataFrame()

def fetch_zillow_data(states=None):
    """Fetch Zillow housing metrics - FIXED: Cast TIMESTAMP to DATE"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Build state filter
    state_filter = ""
    if states and len(states) > 0:
        state_list = "', '".join(states)
        state_filter = f"AND state_name IN ('{state_list}')"
    
    # FIX: Cast date to DATE type to avoid TIMESTAMP vs DATE comparison error
    query = f"""
    SELECT 
        region_id,
        region_name,
        region_type,
        state_name,
        metric_type,
        CAST(date AS DATE) as date,
        value
    FROM `{client.project}.{dataset_id}.zillow_metrics`
    WHERE CAST(date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
    {state_filter}
    ORDER BY metric_type, region_name, date
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error fetching Zillow data: {e}")
        return pd.DataFrame()

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
        print(f"Error fetching states: {e}")
        return []

def get_state_aggregated_data():
    """Get pre-computed state-level data (OPTIMIZED)"""
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Simple query - no window functions, no aggregation!
    # Data is pre-computed during preprocessing
    query = f"""
    SELECT *
    FROM `{client.project}.{dataset_id}.zillow_state_aggregated`
    """
    
    try:
        df = client.query(query).to_dataframe()
        
        # Fill NaN with 0
        for col in df.columns:
            if col not in ['state_name', 'city_count', 'latest_date']:
                df[col] = df[col].fillna(0)
        
        return df
    except Exception as e:
        print(f"Error fetching state data: {e}")
        return pd.DataFrame()

def get_city_data_with_coords():
    """Get pre-computed city data with coordinates for all MSAs"""
    client = get_bigquery_client()
    if not client:
        print("ERROR: BigQuery client initialization failed")
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Query for all cities with MSA data
    query = f"""
    SELECT *
    FROM `{client.project}.{dataset_id}.zillow_city_latest`
    WHERE region_type = 'msa'
    AND zhvi IS NOT NULL
    """
    
    try:
        print("INFO: Querying zillow_city_latest for top 5 cities per state...")
        zillow_df = client.query(query).to_dataframe()
        print(f"INFO: Retrieved {len(zillow_df)} cities from BigQuery")
        
        if zillow_df.empty:
            print("WARNING: No city data returned from BigQuery!")
            return pd.DataFrame()
        
        # Load coordinates from CSV
        cities_csv_path = Path(__file__).parent.parent / 'data_engine' / 'uscities.csv'
        if not cities_csv_path.exists():
            print(f"ERROR: Cities CSV file not found at {cities_csv_path}")
            return pd.DataFrame()
            
        coords_df = pd.read_csv(cities_csv_path, usecols=['city', 'state_id', 'lat', 'lng'])
        print(f"INFO: Loaded {len(coords_df)} cities with coordinates from CSV")
        
        # Show sample data for debugging
        print(f"SAMPLE Zillow data:")
        print(f"  region_name: {zillow_df['region_name'].iloc[0]}")
        print(f"  state_name: {zillow_df['state_name'].iloc[0]}")
        print(f"SAMPLE CSV data:")
        print(f"  city: {coords_df['city'].iloc[0]}")
        print(f"  state_id: {coords_df['state_id'].iloc[0]}")
        
        # Extract city name from "City, ST" format
        zillow_df['city_clean'] = zillow_df['region_name'].str.split(',').str[0].str.strip()
        print(f"SAMPLE cleaned city: {zillow_df['city_clean'].iloc[0]}")
        
        # Merge on city name and state code
        # Zillow: city_clean (e.g., "Anchorage"), state_name (e.g., "AK")
        # CSV: city (e.g., "Anchorage"), state_id (e.g., "AK")
        merged_df = zillow_df.merge(
            coords_df,
            left_on=['city_clean', 'state_name'],
            right_on=['city', 'state_id'],
            how='left'
        )
        
        # Check merge results
        total_rows = len(merged_df)
        rows_with_coords = merged_df['lat'].notna().sum()
        rows_without_coords = merged_df['lat'].isna().sum()
        
        print(f"MERGE RESULTS:")
        print(f"  Total rows: {total_rows}")
        print(f"  Rows with coordinates: {rows_with_coords}")
        print(f"  Rows WITHOUT coordinates: {rows_without_coords}")
        
        if rows_without_coords > 0:
            print(f"WARNING: {rows_without_coords} cities failed to match coordinates!")
            print("Sample unmatched cities:")
            unmatched = merged_df[merged_df['lat'].isna()][['city_clean', 'state_name']].head(10)
            for _, row in unmatched.iterrows():
                print(f"  - {row['city_clean']}, {row['state_name']}")
        
        # Drop rows without coordinates
        merged_df = merged_df.dropna(subset=['lat', 'lng'])
        print(f"INFO: Final dataset has {len(merged_df)} cities with coordinates")
        
        if merged_df.empty:
            print("ERROR: No cities have coordinates after merge!")
            return pd.DataFrame()
        
        # Fill NaN for metrics
        metric_cols = [col for col in merged_df.columns if col not in 
                      ['region_name', 'state_name', 'city_clean', 'city', 
                       'state_id', 'lat', 'lng', 'date', 'region_type', 'region_id']]
        for col in metric_cols:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].fillna(0)
        
        # Select final columns
        result_cols = ['region_name', 'state_name', 'lat', 'lng'] + [col for col in metric_cols if col in merged_df.columns]
        return merged_df[result_cols]
        
    except Exception as e:
        print(f"ERROR fetching city data: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


# Layout
app.layout = html.Div(children=[
    # Location component for page load detection
    dcc.Location(id='url', refresh=False),
    
    # Hidden stores
    html.Div(style={'display': 'none'}, children=[
        dcc.Store(id='fred-data-store'),
        dcc.Store(id='zillow-data-store'),
        dcc.Store(id='city-data-store'),
        dcc.Store(id='data-date-store'),  # Store for data release date
    ]),

    # Main visible content
    html.Div(className='main-container', children=[
    
    # Header
    html.Div(className='header', children=[
        html.H1('Vant'),
        html.P('Housing Market Intelligence Platform'),
        html.P(id='data-date-display', style={'fontSize': '0.9rem', 'color': 'rgba(237, 237, 237, 0.6)', 'marginTop': '0.5rem'})
    ]),
    
    # Filters
    html.Div(className='filter-section', children=[
        dbc.Row([
            dbc.Col([
                html.Label('Search City:', style={'color': '#ededed', 'fontWeight': '500', 'marginBottom': '0.5rem'}),
                dcc.Dropdown(
                    id='city-search',
                    options=[],
                    placeholder='Search for a city...',
                    className='dash-dropdown',
                    clearable=True,
                    style={'color': '#333333', 'backgroundColor': '#ffffff'},
                )
            ], md=6),
            dbc.Col([
                html.Label('Map Metric:', style={'color': '#ededed', 'fontWeight': '500', 'marginBottom': '0.5rem'}),
                dcc.Dropdown(
                    id='map-metric',
                    options=[
                        {'label': 'Home Value Index (ZHVI)', 'value': 'zhvi'},
                        {'label': 'Median Sale Price', 'value': 'median_sale_price'},
                        {'label': 'Active Listings', 'value': 'active_listings'},
                        {'label': 'Market Heat Index', 'value': 'market_heat_index'},
                        {'label': 'New Listings', 'value': 'new_listings'},
                        {'label': 'Sales Count', 'value': 'sales_count'},
                        {'label': 'New Construction - Median Sale Price', 'value': 'new_construction_median_sale_price'},
                        {'label': 'New Construction - Sales Count', 'value': 'new_construction_sales_count'},
                        {'label': 'New Homeowner Affordability', 'value': 'new_homeowner_affordability'}
                    ],
                    value='zhvi',
                    clearable=False,
                    className='dash-dropdown',
                    style={'color': '#333333', 'backgroundColor': '#ffffff'},
                )
            ], md=6)
        ])
    ]),
    
    # US Map Section
    html.Div(className='section-title', children='Housing Market by State & City'),
    html.Div(className='chart-container', children=[
        dcc.Loading(
            id='loading-map',
            type='default',
            children=dcc.Graph(id='us-map', config={'displayModeBar': False})
        )
    ]),
    
    
    # City Time Series Section
    html.Div(className='section-title', children='City Historical Trends', style={'marginTop': '3rem'}),
    
    # Time Series Filters
    html.Div(className='filter-section', children=[
        dbc.Row([
            dbc.Col([
                html.Label('Select City', style={'marginBottom': '0.5rem', 'display': 'block'}),
                dcc.Dropdown(
                    id='timeseries-city-search',
                    options=[],
                    placeholder='Search for a city...',
                    className='dash-dropdown',
                    style={'color': '#333333', 'backgroundColor': '#ffffff'},
                )
            ], md=6),
            dbc.Col([
                html.Label('Select Metric', style={'marginBottom': '0.5rem', 'display': 'block'}),
                dcc.Dropdown(
                    id='timeseries-metric',
                    options=[
                        {'label': 'ZHVI (Home Value Index)', 'value': 'zhvi'},
                        {'label': 'Median Sale Price', 'value': 'median_sale_price'},
                        {'label': 'Median List Price', 'value': 'median_list_price'},
                        {'label': 'Active Listing Count', 'value': 'active_listing_count'},
                        {'label': 'Median Days to Pending', 'value': 'median_days_to_pending'},
                        {'label': 'New Homeowner Affordability', 'value': 'new_homeowner_affordability'},
                    ],
                    value='zhvi',
                    clearable=False,
                    className='dash-dropdown',
                    style={'color': '#333333', 'backgroundColor': '#ffffff'},
                )
            ], md=6)
        ])
    ]),
    
    # Time Series Chart
    html.Div(className='chart-container', children=[
        dcc.Graph(id='city-timeseries', config={'displayModeBar': False})
    ]),
    
    # Economic Indicators Section
    html.Div(className='section-title', children='Economic Indicators', style={'marginTop': '3rem'}),
    dbc.Row([
        dbc.Col([
            html.Div(className='chart-container', children=[
                dcc.Loading(
                    id='loading-mortgage',
                    type='default',
                    children=dcc.Graph(id='mortgage-chart')
                )
            ])
        ], md=6),
        dbc.Col([
            html.Div(className='chart-container', children=[
                dcc.Loading(
                    id='loading-cpi',
                    type='default',
                    children=dcc.Graph(id='cpi-chart')
                )
            ])
        ], md=6)
    ]),

    ]),  # End of main-container
])


# Callbacks
@app.callback(
    Output('city-search', 'options'),
    Input('city-search', 'id')
)
def update_city_options(_):
    """Populate city search dropdown with all available cities"""
    client = get_bigquery_client()
    if not client:
        return []
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    query = f"""
    SELECT DISTINCT region_name, state_name
    FROM `{client.project}.{dataset_id}.zillow_metrics`
    WHERE region_type = 'msa'
    ORDER BY region_name
    """
    
    try:
        df = client.query(query).to_dataframe()
        return [{'label': f"{row['region_name']}", 'value': row['region_name']} 
                for _, row in df.iterrows()]
    except Exception as e:
        print(f"Error fetching cities: {e}")
        return []

@app.callback(
    [Output('fred-data-store', 'data'),
     Output('zillow-data-store', 'data'),
     Output('city-data-store', 'data'),
     Output('data-date-store', 'data')],
    Input('url', 'pathname')  # Changed from city-search to url - fetch once on page load
)
def fetch_data_on_load(pathname):
    """Fetch all data once on page load - city filtering happens client-side"""
    fred_df = fetch_fred_data()
    zillow_df = fetch_zillow_data(None)
    
    # Get both state aggregated data and city data with coordinates (top 5 per state)
    state_df = get_state_aggregated_data()
    city_df = get_city_data_with_coords()
    
    # Store data without selected_city - filtering happens in map callback
    combined_data = {
        'state': state_df.to_json(date_format='iso', orient='split') if not state_df.empty else None,
        'city': city_df.to_json(date_format='iso', orient='split') if not city_df.empty else None,
    }
    
    # Extract latest date for display
    data_date = None
    if not city_df.empty and 'date' in city_df.columns:
        data_date = pd.to_datetime(city_df['date']).max().isoformat()
    
    return (
        fred_df.to_json(date_format='iso', orient='split') if not fred_df.empty else None,
        zillow_df.to_json(date_format='iso', orient='split') if not zillow_df.empty else None,
        combined_data,
        data_date
    )

@app.callback(
    Output('data-date-display', 'children'),
    Input('data-date-store', 'data')
)
def update_data_date(date_str):
    """Display the data release date"""
    if not date_str:
        return ''
    try:
        date_obj = pd.to_datetime(date_str)
        return f"Data as of {date_obj.strftime('%B %Y')}"
    except:
        return ''


@app.callback(
    Output('us-map', 'figure'),
    [Input('city-data-store', 'data'),
     Input('map-metric', 'value'),
     Input('city-search', 'value')]  # Added city-search for filtering
)
def update_map(combined_data, metric, selected_city):
    """Update US map with state choropleth and city scatter overlay, with city zoom"""
    if not combined_data or not combined_data.get('state'):
        # Return empty map
        fig = go.Figure()
        fig.update_layout(
            title='No data available',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed'),
            geo=dict(
                scope='usa',
                projection_type='albers usa',
                showland=True,
                landcolor='#2a2a2a',
                coastlinecolor='#444',
                showlakes=True,
                lakecolor='#1a1a1a'
            )
        )
        return fig
    
    # Load state data
    state_df = pd.read_json(combined_data['state'], orient='split')
    
    # Check if metric exists in state data
    has_state_data = metric in state_df.columns
    
    # Filter out states with 0 values (from NaN filling) only if we have the metric
    if has_state_data:
        state_df_filtered = state_df[state_df[metric] > 0].copy()
    else:
        state_df_filtered = pd.DataFrame()  # No state data for this metric
    
    # Create figure with state choropleth
    fig = go.Figure()
    
    # Add state choropleth layer only if we have data
    if not state_df_filtered.empty and has_state_data:
        fig.add_trace(go.Choropleth(
            locations=state_df_filtered['state_name'],
            z=state_df_filtered[metric],
            locationmode='USA-states',
            colorscale=[[0, '#1a1a1a'], [0.5, '#A7D129'], [1, '#d4ff00']],
            colorbar=dict(
                title=metric.replace('_', ' ').title(),
                tickprefix='$' if ('price' in metric or 'zhvi' in metric) and 'affordability' not in metric else '',
                tickformat=',.0%' if 'affordability' in metric else ',.0f',
                bgcolor='#1a1a1a',
                tickfont=dict(color='#ededed')
            ),
            hovertemplate='<b>%{location}</b><br>' +
                          f'{metric.replace("_", " ").title()}: ' +
                          ('%{z:.1%}' if 'affordability' in metric else ('$%{z:,.0f}' if 'price' in metric or 'zhvi' in metric else '%{z:,.0f}')) +
                          '<br>Cities: %{customdata[0]}<extra></extra>',
            customdata=state_df_filtered[['city_count']].values,
            marker_line_color='#444',
            marker_line_width=0.5,
            showscale=True
        ))
    
    # Add city scatter layer if coordinates are available
    if combined_data.get('city'):
        city_df = pd.read_json(combined_data['city'], orient='split')
        
        # Check if metric exists in city data
        has_city_data = metric in city_df.columns
        
        if has_city_data:
            # Filter out cities with 0 values to avoid black markers
            # But always include selected city if one is chosen
            if selected_city:
                # Include selected city + cities with values > 0
                city_df_filtered = city_df[
                    (city_df['region_name'] == selected_city) | (city_df[metric] > 0)
                ].copy()
            else:
                # Only show cities with values > 0 (this prevents black markers)
                city_df_filtered = city_df[city_df[metric] > 0].copy()
        else:
            city_df_filtered = pd.DataFrame()
        
        if not city_df_filtered.empty and has_city_data:
            # Normalize sizes for better visualization
            max_val = city_df_filtered[metric].max()
            min_val = city_df_filtered[metric].min()
            if max_val > min_val:
                city_df_filtered['size'] = 5 + 15 * (city_df_filtered[metric] - min_val) / (max_val - min_val)
            else:
                city_df_filtered['size'] = 10
            
            # Highlight selected city in gold
            if selected_city:
                # Create separate color column for display
                city_df_filtered['marker_color'] = city_df_filtered.apply(
                    lambda row: '#FFD700' if row['region_name'] == selected_city else None,
                    axis=1
                )
                city_df_filtered['size'] = city_df_filtered.apply(
                    lambda row: 25 if row['region_name'] == selected_city else row['size'],
                    axis=1
                )
                # For non-selected cities, use the metric value for color
                mask = city_df_filtered['marker_color'].isna()
                city_df_filtered.loc[mask, 'marker_color'] = city_df_filtered.loc[mask, metric]
            else:
                city_df_filtered['marker_color'] = city_df_filtered[metric]
            
            # Create custom hover data with metric values
            city_df_filtered['hover_value'] = city_df_filtered[metric]
            
            fig.add_trace(go.Scattergeo(
                lon=city_df_filtered['lng'],
                lat=city_df_filtered['lat'],
                text=city_df_filtered['region_name'],
                mode='markers',
                marker=dict(
                    size=city_df_filtered['size'],
                    color=city_df_filtered['marker_color'],
                    colorscale=[[0, '#A7D129'], [1, '#d4ff00']],
                    line=dict(width=0.5, color='#000'),
                    sizemode='diameter',
                    showscale=False,
                    cmin=city_df_filtered[metric].min() if not selected_city else None,
                    cmax=city_df_filtered[metric].max() if not selected_city else None
                ),
                customdata=city_df_filtered[['hover_value']].values,
                hovertemplate='<b>%{text}</b><br>' +
                              f'{metric.replace("_", " ").title()}: ' +
                              ('%{customdata[0]:.1%}' if 'affordability' in metric else ('$%{customdata[0]:,.0f}' if 'price' in metric or 'zhvi' in metric else '%{customdata[0]:,.0f}')) +
                              '<extra></extra>',
                name='Cities'
            ))
    
    # Get latest date for title
    latest_date_str = ''
    if combined_data.get('city'):
        city_df = pd.read_json(combined_data['city'], orient='split')
        if 'date' in city_df.columns and not city_df.empty:
            latest_date = pd.to_datetime(city_df['date']).max()
            latest_date_str = f" (Data as of {latest_date.strftime('%B %Y')})"
    
    # Set zoom and center if city is selected
    geo_settings = dict(
        scope='usa',
        projection_type='albers usa',
        showland=True,
        landcolor='#2a2a2a',
        coastlinecolor='#444',
        showlakes=True,
        lakecolor='#1a1a1a',
        bgcolor='#000000'
    )
    
    if selected_city and combined_data.get('city'):
        city_df = pd.read_json(combined_data['city'], orient='split')
        selected_city_data = city_df[city_df['region_name'] == selected_city]
        if not selected_city_data.empty:
            lat = selected_city_data.iloc[0]['lat']
            lon = selected_city_data.iloc[0]['lng']
            geo_settings.update({
                'center': {'lat': lat, 'lon': lon},
                'projection_scale': 8  # Reduced zoom for better view
            })
    
    fig.update_layout(
        title=f'US Housing Market - {metric.replace("_", " ").title()}' + 
              (f' (Focused on {selected_city})' if selected_city else '') +
              latest_date_str,
        template='plotly_dark',
        plot_bgcolor='#000000',
        paper_bgcolor='#1a1a1a',
        font=dict(color='#ededed'),
        geo=geo_settings,
        height=600,
        showlegend=False
    )
    
    return fig

@app.callback(
    Output('mortgage-chart', 'figure'),
    Input('fred-data-store', 'data')
)
def update_mortgage_chart(fred_data_json):
    """Update mortgage rate chart"""
    if not fred_data_json:
        return go.Figure().update_layout(
            title='No FRED data available',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed')
        )
    
    fred_df = pd.read_json(fred_data_json, orient='split')
    mortgage_data = fred_df[fred_df['series_id'] == 'MORTGAGE30US'].copy()
    
    if mortgage_data.empty:
        return go.Figure().update_layout(
            title='No mortgage rate data available',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed')
        )
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=mortgage_data['date'],
        y=mortgage_data['value'],
        mode='lines',
        name='30-Year Mortgage Rate',
        line=dict(color='#A7D129', width=2),
        fill='tozeroy',
        fillcolor='rgba(167, 209, 41, 0.1)'
    ))
    
    fig.update_layout(
        title='30-Year Mortgage Rate',
        xaxis_title='Date',
        yaxis_title='Rate (%)',
        template='plotly_dark',
        hovermode='x unified',
        plot_bgcolor='#000000',
        paper_bgcolor='#1a1a1a',
        font=dict(color='#ededed'),
        xaxis=dict(gridcolor='#333'),
        yaxis=dict(gridcolor='#333')
    )
    
    return fig

@app.callback(
    Output('cpi-chart', 'figure'),
    Input('fred-data-store', 'data')
)
def update_cpi_chart(fred_data_json):
    """Update CPI chart"""
    if not fred_data_json:
        return go.Figure().update_layout(
            title='No FRED data available',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed')
        )
    
    fred_df = pd.read_json(fred_data_json, orient='split')
    cpi_data = fred_df[fred_df['series_id'] == 'CPIAUCSL'].copy()
    
    if cpi_data.empty:
        return go.Figure().update_layout(
            title='No CPI data available',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed')
        )
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=cpi_data['date'],
        y=cpi_data['value'],
        mode='lines',
        name='Consumer Price Index',
        line=dict(color='#A7D129', width=2),
        fill='tozeroy',
        fillcolor='rgba(167, 209, 41, 0.1)'
    ))
    
    fig.update_layout(
        title='Consumer Price Index (CPI)',
        xaxis_title='Date',
        yaxis_title='Index Value',
        template='plotly_dark',
        hovermode='x unified',
        plot_bgcolor='#000000',
        paper_bgcolor='#1a1a1a',
        font=dict(color='#ededed'),
        xaxis=dict(gridcolor='#333'),
        yaxis=dict(gridcolor='#333')
    )
    
    return fig

@app.callback(
    Output('timeseries-city-search', 'options'),
    Input('timeseries-city-search', 'id')
)
def update_timeseries_city_options(_):
    """Populate time series city search dropdown with all available cities"""
    client = get_bigquery_client()
    if not client:
        return []
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    query = f"""
    SELECT DISTINCT region_name, state_name
    FROM `{client.project}.{dataset_id}.zillow_metrics`
    WHERE region_type = 'msa'
    ORDER BY region_name
    """
    
    try:
        df = client.query(query).to_dataframe()
        return [{'label': f"{row['region_name']}", 'value': row['region_name']}
                for _, row in df.iterrows()]
    except Exception as e:
        print(f"Error fetching cities: {e}")
        return []

@app.callback(
    Output('city-timeseries', 'figure'),
    [Input('timeseries-city-search', 'value'),
     Input('timeseries-metric', 'value')]
)
def update_city_timeseries(selected_city, selected_metric):
    """Update time series chart for selected city and metric"""
    
    # Return empty chart if no city selected
    if not selected_city:
        fig = go.Figure()
        fig.update_layout(
            title='Select a city to view historical data',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed'),
            height=400
        )
        return fig
    
    # Fetch data from BigQuery
    client = get_bigquery_client()
    if not client:
        return go.Figure()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Map UI metric names to database metric_type values
    metric_mapping = {
        'zhvi': 'zhvi',
        'median_sale_price': 'median_sale_price',
        'median_list_price': 'median_list_price',
        'active_listing_count': 'active_listing_count',
        'median_days_to_pending': 'median_days_to_pending',
        'new_homeowner_affordability': 'new_homeowner_affordability'
    }
    
    metric_type = metric_mapping.get(selected_metric, selected_metric)
    
    query = f"""
    SELECT date, value
    FROM `{client.project}.{dataset_id}.zillow_metrics`
    WHERE region_name = @city_name
    AND metric_type = @metric_type
    AND region_type = 'msa'
    ORDER BY date ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("city_name", "STRING", selected_city),
            bigquery.ScalarQueryParameter("metric_type", "STRING", metric_type)
        ]
    )
    
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                title=f'No data available for {selected_city} - {selected_metric.replace("_", " ").title()}',
                template='plotly_dark',
                plot_bgcolor='#000000',
                paper_bgcolor='#1a1a1a',
                font=dict(color='#ededed'),
                height=400
            )
            return fig
        
        # Convert timestamp to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Create line chart
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['value'],
            mode='lines',
            line=dict(color='#A7D129', width=2),
            fill='tozeroy',
            fillcolor='rgba(167, 209, 41, 0.1)',
            name=selected_metric.replace('_', ' ').title(),
            hovertemplate='<b>%{x|%b %Y}</b><br>' +
                         ('Value: %{y:.1%}' if 'affordability' in selected_metric else 
                          ('Value: $%{y:,.0f}' if 'price' in selected_metric or 'zhvi' in selected_metric 
                           else 'Value: %{y:,.0f}')) +
                         '<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'{selected_city} - {selected_metric.replace("_", " ").title()}',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed'),
            xaxis=dict(
                title='Date',
                gridcolor='#333',
                showgrid=True
            ),
            yaxis=dict(
                title=selected_metric.replace('_', ' ').title(),
                gridcolor='#333',
                showgrid=True,
                tickprefix='$' if ('price' in selected_metric or 'zhvi' in selected_metric) and 'affordability' not in selected_metric else '',
                tickformat=',.0%' if 'affordability' in selected_metric else ',.0f'
            ),
            height=400,
            hovermode='x unified',
            margin=dict(l=60, r=20, t=60, b=60)
        )
        
        return fig
        
    except Exception as e:
        print(f"Error fetching time series data: {e}")
        fig = go.Figure()
        fig.update_layout(
            title=f'Error loading data for {selected_city}',
            template='plotly_dark',
            plot_bgcolor='#000000',
            paper_bgcolor='#1a1a1a',
            font=dict(color='#ededed'),
            height=400
        )
        return fig

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8050)
