-- BigQuery Schema for Google Trends Data
-- Table: trends_metrics
-- Purpose: Store Google Trends interest scores for housing-related search terms

CREATE TABLE IF NOT EXISTS `housing_dashboard.trends_metrics` (
  -- Date
  date DATE NOT NULL OPTIONS(description="Date of interest score measurement"),
  
  -- Search term
  search_term STRING NOT NULL OPTIONS(description="Housing-related search term"),
  
  -- Interest score
  interest_score INT64 NOT NULL OPTIONS(description="Google Trends interest score (0-100 scale)"),
  
  -- Geographic region
  region STRING NOT NULL OPTIONS(description="Geographic region (e.g., US)"),
  
  -- Metadata
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="When record was inserted/updated")
)
PARTITION BY date
CLUSTER BY search_term
OPTIONS(
  description="Google Trends interest scores for housing-related search terms to track market sentiment and buyer behavior",
  labels=[("source", "google_trends"), ("data_type", "search_interest")]
);

-- Create a view for recent trends (last 90 days)
CREATE OR REPLACE VIEW `housing_dashboard.trends_recent` AS
SELECT 
  date,
  search_term,
  interest_score,
  region
FROM `housing_dashboard.trends_metrics`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY date DESC, search_term;

-- Create a view for normalized comparison (all terms on same scale)
CREATE OR REPLACE VIEW `housing_dashboard.trends_normalized` AS
WITH term_stats AS (
  SELECT 
    search_term,
    MAX(interest_score) as max_score,
    MIN(interest_score) as min_score
  FROM `housing_dashboard.trends_metrics`
  GROUP BY search_term
)
SELECT 
  t.date,
  t.search_term,
  t.interest_score,
  -- Normalize to 0-1 scale within each term's historical range
  CASE 
    WHEN s.max_score = s.min_score THEN 0.5
    ELSE (t.interest_score - s.min_score) / (s.max_score - s.min_score)
  END as normalized_score,
  t.region
FROM `housing_dashboard.trends_metrics` t
JOIN term_stats s ON t.search_term = s.search_term
ORDER BY t.date DESC, t.search_term;

-- Create a view for weekly aggregates (smoother trends)
CREATE OR REPLACE VIEW `housing_dashboard.trends_weekly` AS
SELECT 
  DATE_TRUNC(date, WEEK) as week_start,
  search_term,
  AVG(interest_score) as avg_interest_score,
  MIN(interest_score) as min_interest_score,
  MAX(interest_score) as max_interest_score,
  region
FROM `housing_dashboard.trends_metrics`
GROUP BY week_start, search_term, region
ORDER BY week_start DESC, search_term;
