-- BigQuery Schema for FRED Economic Data
-- Creates a table to store Federal Reserve Economic Data (FRED) time series

CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.fred_metrics` (
  -- Series identification
  series_id STRING NOT NULL,
  series_name STRING NOT NULL,
  
  -- Time series data
  date DATE NOT NULL,
  value FLOAT64 NOT NULL,
  
  -- Metadata
  frequency STRING NOT NULL,  -- weekly, monthly, quarterly, annual
  units STRING NOT NULL,      -- percent, index, percent_change, etc.
  
  -- Tracking
  last_updated TIMESTAMP NOT NULL
)
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY series_id, date
OPTIONS(
  description="Federal Reserve Economic Data (FRED) time series metrics",
  labels=[("source", "fred"), ("data_type", "economic_indicators")]
);

-- Example queries:

-- 1. Get latest mortgage rates
SELECT date, value as mortgage_rate_30yr
FROM `{project_id}.{dataset_id}.fred_metrics`
WHERE series_id = 'MORTGAGE30US'
  AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
ORDER BY date DESC;

-- 2. Get CPI trend (year-over-year change)
WITH cpi_data AS (
  SELECT 
    date,
    value as cpi,
    LAG(value, 12) OVER (ORDER BY date) as cpi_year_ago
  FROM `{project_id}.{dataset_id}.fred_metrics`
  WHERE series_id = 'CPIAUCSL'
)
SELECT 
  date,
  cpi,
  ROUND(((cpi - cpi_year_ago) / cpi_year_ago) * 100, 2) as yoy_change_pct
FROM cpi_data
WHERE cpi_year_ago IS NOT NULL
ORDER BY date DESC
LIMIT 12;

-- 3. Get all latest values for each series
SELECT 
  series_id,
  series_name,
  frequency,
  MAX(date) as latest_date,
  ARRAY_AGG(value ORDER BY date DESC LIMIT 1)[OFFSET(0)] as latest_value,
  units
FROM `{project_id}.{dataset_id}.fred_metrics`
GROUP BY series_id, series_name, frequency, units
ORDER BY series_id;

-- 4. Homeownership rate trend
SELECT 
  date,
  value as homeownership_rate_pct
FROM `{project_id}.{dataset_id}.fred_metrics`
WHERE series_id = 'RHORUSQ156N'
ORDER BY date DESC
LIMIT 20;
