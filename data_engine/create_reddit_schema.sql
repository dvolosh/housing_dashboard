-- BigQuery Schema for Reddit Posts
-- Table: reddit_posts
-- Purpose: Store post-level Reddit data for LLM analysis

CREATE TABLE IF NOT EXISTS `housing_dashboard.reddit_posts` (
  -- Primary identifiers
  post_id STRING NOT NULL OPTIONS(description="Unique Reddit post ID"),
  subreddit STRING NOT NULL OPTIONS(description="Subreddit name: FirstTimeHomeBuyer or SameGrassButGreener"),
  
  -- Timestamps
  created_utc TIMESTAMP NOT NULL OPTIONS(description="Post creation time (UTC)"),
  created_date DATE NOT NULL OPTIONS(description="Post creation date (partition key)"),
  
  -- Post content
  title STRING OPTIONS(description="Post title"),
  selftext STRING OPTIONS(description="Full post body text for LLM analysis"),
  
  -- Engagement metrics
  score INT64 OPTIONS(description="Upvotes minus downvotes"),
  num_comments INT64 OPTIONS(description="Number of comments"),
  
  -- Author
  author STRING OPTIONS(description="Reddit username"),
  
  -- Extracted metadata for FirstTimeHomeBuyer
  location STRING OPTIONS(description="Extracted location (City, ST) from FirstTimeHomeBuyer posts"),
  purchase_price FLOAT64 OPTIONS(description="Extracted purchase price from FirstTimeHomeBuyer posts"),
  
  -- Extracted metadata for SameGrassButGreener
  city_mentions STRING OPTIONS(description="Pipe-separated list of cities mentioned in SameGrassButGreener posts"),
  
  -- Reference
  permalink STRING OPTIONS(description="Reddit post URL"),
  
  -- Metadata
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="When record was inserted/updated")
)
PARTITION BY created_date
CLUSTER BY subreddit, location
OPTIONS(
  description="Post-level Reddit data from r/FirstTimeHomeBuyer and r/SameGrassButGreener for housing market analysis and LLM-powered insights",
  labels=[("source", "pullpush_api"), ("data_type", "social_media")]
);

-- Create a view for FirstTimeHomeBuyer posts with extracted data
CREATE OR REPLACE VIEW `housing_dashboard.reddit_ftb_with_data` AS
SELECT 
  created_date,
  location,
  purchase_price,
  title,
  score,
  num_comments,
  permalink
FROM `housing_dashboard.reddit_posts`
WHERE subreddit = 'FirstTimeHomeBuyer'
  AND (location IS NOT NULL OR purchase_price IS NOT NULL)
ORDER BY created_date DESC;

-- Create a view for SameGrassButGreener posts with city mentions
CREATE OR REPLACE VIEW `housing_dashboard.reddit_sgg_cities` AS
SELECT 
  created_date,
  city_mentions,
  title,
  selftext,
  score,
  num_comments,
  permalink
FROM `housing_dashboard.reddit_posts`
WHERE subreddit = 'SameGrassButGreener'
  AND city_mentions IS NOT NULL
ORDER BY created_date DESC;

-- Create a view for daily post volume (time series indicator)
CREATE OR REPLACE VIEW `housing_dashboard.reddit_daily_volume` AS
SELECT 
  created_date,
  subreddit,
  COUNT(*) as post_count,
  AVG(score) as avg_score,
  AVG(num_comments) as avg_comments,
  SUM(score + num_comments) as total_engagement
FROM `housing_dashboard.reddit_posts`
GROUP BY created_date, subreddit
ORDER BY created_date DESC, subreddit;
