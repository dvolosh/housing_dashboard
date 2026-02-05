-- BigQuery Table Schema for Zillow Metrics
-- This DDL creates the zillow_metrics table with proper partitioning and clustering

CREATE TABLE IF NOT EXISTS `housing-dashboard-452100.housing_data.zillow_metrics`
(
  region_id INT64 NOT NULL OPTIONS(description="Zillow Region ID"),
  region_name STRING NOT NULL OPTIONS(description="Name of the region (e.g., 'United States', 'New York, NY')"),
  region_type STRING NOT NULL OPTIONS(description="Type of region: 'country', 'msa', etc."),
  state_name STRING OPTIONS(description="State name (null for national data)"),
  metric_type STRING NOT NULL OPTIONS(description="Type of metric: 'median_sale_price', 'zhvi', 'active_listings', etc."),
  date DATE NOT NULL OPTIONS(description="Date of the metric value (monthly)"),
  value FLOAT64 NOT NULL OPTIONS(description="Metric value")
)
PARTITION BY DATE_TRUNC(date, MONTH)
CLUSTER BY region_name, metric_type
OPTIONS(
  description="Zillow housing market metrics in normalized long format. Data includes median sale prices, ZHVI, active listings, market heat index, and more across various geographic regions from 2000-2025.",
  labels=[("source", "zillow"), ("data_type", "housing_metrics")]
);

-- Sample queries to validate the table

-- 1. Check row count by metric
SELECT 
  metric_type,
  COUNT(*) as row_count,
  MIN(date) as earliest_date,
  MAX(date) as latest_date
FROM `housing-dashboard-452100.housing_data.zillow_metrics`
GROUP BY metric_type
ORDER BY row_count DESC;

-- 2. National median sale price trend (last 24 months)
SELECT 
  date,
  value as median_sale_price
FROM `housing-dashboard-452100.housing_data.zillow_metrics`
WHERE region_name = 'United States'
  AND metric_type = 'median_sale_price'
  AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
ORDER BY date DESC;

-- 3. Top 10 MSAs by ZHVI (most recent)
SELECT 
  region_name,
  state_name,
  value as zhvi
FROM `housing-dashboard-452100.housing_data.zillow_metrics`
WHERE metric_type = 'zhvi'
  AND region_type = 'msa'
  AND date = (SELECT MAX(date) FROM `housing-dashboard-452100.housing_data.zillow_metrics` WHERE metric_type = 'zhvi')
ORDER BY value DESC
LIMIT 10;

-- 4. Correlation between active listings and median sale price (national)
SELECT 
  a.date,
  a.value as active_listings,
  b.value as median_sale_price
FROM `housing-dashboard-452100.housing_data.zillow_metrics` a
JOIN `housing-dashboard-452100.housing_data.zillow_metrics` b
  ON a.region_id = b.region_id 
  AND a.date = b.date
WHERE a.region_name = 'United States'
  AND a.metric_type = 'active_listings'
  AND b.metric_type = 'median_sale_price'
ORDER BY a.date DESC
LIMIT 100;
