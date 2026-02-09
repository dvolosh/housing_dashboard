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
            
            /* Dropdown styling */
            .dash-dropdown .Select-control {
                background-color: #2a2a2a !important;
                border-color: #444 !important;
            }
            
            .dash-dropdown .Select-menu-outer {
                background-color: #2a2a2a !important;
                border-color: #444 !important;
            }
            
            .dash-dropdown .Select-value-label,
            .dash-dropdown .Select-placeholder {
                color: var(--text-primary) !important;
            }
            
            .dash-dropdown .Select-option {
                background-color: #2a2a2a !important;
                color: var(--text-primary) !important;
            }
            
            .dash-dropdown .Select-option.is-focused {
                background-color: #3a3a3a !important;
            }
            
            .dash-dropdown .Select-option.is-selected {
                background-color: var(--primary-accent) !important;
                color: #000 !important;
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

def get_city_coordinates():
    """Get city coordinates for map visualization"""
    # Sample city coordinates - in production, this would come from your data
    # For now, we'll extract from Zillow data and use approximate coordinates
    client = get_bigquery_client()
    if not client:
        return pd.DataFrame()
    
    dataset_id = os.getenv('GCP_DATASET_ID', 'db')
    
    # Get city-level data with latest values
    query = f"""
    WITH LatestData AS (
        SELECT 
            region_name,
            state_name,
            metric_type,
            value,
            ROW_NUMBER() OVER (PARTITION BY region_name, state_name, metric_type ORDER BY date DESC) as rn
        FROM `{client.project}.{dataset_id}.zillow_metrics`
        WHERE region_type = 'city'
        AND metric_type IN ('zhvi', 'median_sale_price')
    )
    SELECT 
        region_name as city,
        state_name as state,
        MAX(CASE WHEN metric_type = 'zhvi' THEN value END) as zhvi,
        MAX(CASE WHEN metric_type = 'median_sale_price' THEN value END) as median_price
    FROM LatestData
    WHERE rn = 1
    GROUP BY region_name, state_name
    HAVING MAX(CASE WHEN metric_type = 'zhvi' THEN value END) IS NOT NULL
    """
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error fetching city data: {e}")
        return pd.DataFrame()

# Layout
app.layout = html.Div(className='main-container', children=[
    # Header
    html.Div(className='header', children=[
        html.H1('🏠 Housing Market Dashboard'),
        html.P('Market Intelligence Platform')
    ]),
    
    # Filters
    html.Div(className='filter-section', children=[
        dbc.Row([
            dbc.Col([
                html.Label('Select States:', style={'color': '#ededed', 'fontWeight': '500', 'marginBottom': '0.5rem'}),
                dcc.Dropdown(
                    id='state-filter',
                    options=[],
                    multi=True,
                    placeholder='All States',
                    className='dash-dropdown'
                )
            ], md=6),
            dbc.Col([
                html.Label('Map Metric:', style={'color': '#ededed', 'fontWeight': '500', 'marginBottom': '0.5rem'}),
                dcc.Dropdown(
                    id='map-metric',
                    options=[
                        {'label': 'Home Value Index (ZHVI)', 'value': 'zhvi'},
                        {'label': 'Median Sale Price', 'value': 'median_price'}
                    ],
                    value='zhvi',
                    clearable=False,
                    className='dash-dropdown'
                )
            ], md=6)
        ])
    ]),
    
    # US Map Section
    html.Div(className='section-title', children='Housing Market by City'),
    html.Div(className='chart-container', children=[
        dcc.Loading(
            id='loading-map',
            type='default',
            children=dcc.Graph(id='us-map', style={'height': '600px'})
        )
    ]),
    
    # FRED Time Series Section
    html.Div(className='section-title', children='Economic Indicators (FRED)'),
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
    
    # Store components for data
    dcc.Store(id='fred-data-store'),
    dcc.Store(id='zillow-data-store'),
    dcc.Store(id='city-data-store')
])

# Callbacks
@app.callback(
    Output('state-filter', 'options'),
    Input('state-filter', 'id')
)
def update_state_options(_):
    """Populate state filter options"""
    states = get_available_states()
    return [{'label': state, 'value': state} for state in states]

@app.callback(
    [Output('fred-data-store', 'data'),
     Output('zillow-data-store', 'data'),
     Output('city-data-store', 'data')],
    Input('state-filter', 'value')
)
def fetch_data(selected_states):
    """Fetch all data when filters change"""
    fred_df = fetch_fred_data()
    zillow_df = fetch_zillow_data(selected_states)
    city_df = get_city_coordinates()
    
    return (
        fred_df.to_json(date_format='iso', orient='split') if not fred_df.empty else None,
        zillow_df.to_json(date_format='iso', orient='split') if not zillow_df.empty else None,
        city_df.to_json(date_format='iso', orient='split') if not city_df.empty else None
    )

@app.callback(
    Output('us-map', 'figure'),
    [Input('city-data-store', 'data'),
     Input('map-metric', 'value')]
)
def update_map(city_data_json, metric):
    """Update US map with city data"""
    if not city_data_json:
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
    
    city_df = pd.read_json(city_data_json, orient='split')
    
    # For demonstration, we'll use a scatter plot on the map
    # In production, you'd have actual lat/lon coordinates
    fig = px.scatter_geo(
        city_df,
        locations='state',
        locationmode='USA-states',
        color=metric,
        hover_name='city',
        hover_data={'state': True, 'zhvi': ':$,.0f', 'median_price': ':$,.0f'},
        size=metric,
        color_continuous_scale=[[0, '#1a1a1a'], [0.5, '#A7D129'], [1, '#d4ff00']],
        title=f'US Housing Market - {metric.replace("_", " ").title()}'
    )
    
    fig.update_layout(
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
            lakecolor='#1a1a1a',
            bgcolor='#000000'
        ),
        coloraxis_colorbar=dict(
            title=metric.replace('_', ' ').title(),
            tickprefix='$',
            tickformat=',.0f'
        )
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

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8050)
