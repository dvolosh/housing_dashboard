-- BigQuery Schema for Google Trends Data
-- Table: google_search_trends
-- Purpose: Store Google Trends interest scores for housing-related search terms (weekly aggregation)

-- Drop old table if it exists
DROP TABLE IF EXISTS `housing_dashboard.trends_metrics`;

CREATE TABLE IF NOT EXISTS `housing_dashboard.google_search_trends` (
  -- Week start date (Sunday)
  week_start_date DATE NOT NULL OPTIONS(description="Start date of the week (Sunday)"),
  
  -- Search term
  search_term STRING NOT NULL OPTIONS(description="Housing-related search term"),
  
  -- Category
  category STRING NOT NULL OPTIONS(description="Term category: Distress, Affordability, or Inventory"),
  
  -- Average interest score for the week
  avg_interest_score INT64 NOT NULL OPTIONS(description="Weekly average Google Trends interest score (0-100 scale)"),
  
  -- Geographic region
  region STRING NOT NULL OPTIONS(description="Geographic region (e.g., US)"),
  
  -- Metadata
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="When record was inserted/updated")
)
PARTITION BY week_start_date
CLUSTER BY category, search_term
OPTIONS(
  description="Google Trends weekly average interest scores for housing-related search terms to track market sentiment and buyer behavior",
  labels=[("source", "google_trends"), ("data_type", "search_interest"), ("granularity", "weekly")]
);

-- Create a view for recent trends (last 90 days)
CREATE OR REPLACE VIEW `housing_dashboard.google_search_trends_recent` AS
SELECT 
  week_start_date,
  search_term,
  category,
  avg_interest_score,
  region
FROM `housing_dashboard.google_search_trends`
WHERE week_start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY week_start_date DESC, category, search_term;

-- Create a view for normalized comparison (all terms on same scale)
CREATE OR REPLACE VIEW `housing_dashboard.google_search_trends_normalized` AS
WITH term_stats AS (
  SELECT 
    search_term,
    MAX(avg_interest_score) as max_score,
    MIN(avg_interest_score) as min_score
  FROM `housing_dashboard.google_search_trends`
  GROUP BY search_term
)
SELECT 
  t.week_start_date,
  t.search_term,
  t.category,
  t.avg_interest_score,
  -- Normalize to 0-1 scale within each term's historical range
  CASE 
    WHEN s.max_score = s.min_score THEN 0.5
    ELSE (t.avg_interest_score - s.min_score) / (s.max_score - s.min_score)
  END as normalized_score,
  t.region
FROM `housing_dashboard.google_search_trends` t
JOIN term_stats s ON t.search_term = s.search_term
ORDER BY t.week_start_date DESC, t.category, t.search_term;

-- Create a view grouped by category
CREATE OR REPLACE VIEW `housing_dashboard.google_search_trends_by_category` AS
SELECT 
  week_start_date,
  category,
  AVG(avg_interest_score) as category_avg_score,
  MIN(avg_interest_score) as category_min_score,
  MAX(avg_interest_score) as category_max_score,
  COUNT(DISTINCT search_term) as num_terms,
  region
FROM `housing_dashboard.google_search_trends`
GROUP BY week_start_date, category, region
ORDER BY week_start_date DESC, category;

