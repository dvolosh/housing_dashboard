# Housing Market Dashboard

This project outlines the development of the Housing Market Dashboard, a full-stack platform that bridges the gap between official macroeconomic data and real-time buyer behavior. This architecture leverages a high-performance web shell for professional presentation and a heavy-duty data engine for complex analytics.

## The Vision: Value vs. Vibe
The platform identifies market inflections by correlating "Hard Data" (Zillow/FRED) with "Social Heat" (Reddit). It provides a high-utility view for buyers and researchers by revealing when the national conversation in communities like `r/FirstTimeHomeBuyer` begins to lead or lag behind official price reports.

## 1. Technical Architecture (GCP + Vercel)
The platform utilizes a decoupled architecture to ensure both a polished user interface and a robust data pipeline.

| Component | Technology | Role |
|Data Engine| Google BigQuery | Centralized hub for structured Zillow CSVs & semi-structured social metrics. |
|Ingestion (ETL)| Cloud Functions | Python-based serverless functions that automate data collection. |
|Sentiment ML| Vertex AI (Gemini)| LLM-driven engine extracting "Primary Friction Points" from social discourse. |
|Web Shell | Next.js (Vercel) | Professional landing page hosting the embedded dashboard. |
|Dashboard | Streamlit Cloud | Interactive, data-heavy portal embedded via iframe. |

## 2. Data Engine & Machine Learning Logic

### Market Fundamentals (The Value)
*   **Zillow Research**: Automatically ingests monthly ZHVI (Home Value Index) and ZORI (Rent Index) CSVs. Ingestion scripts clean and load these massive files into BigQuery.
*   **FRED API**: Tracks 30-Year Fixed Rate Mortgage Average and Housing Inventory (Active Listings) to provide immediate financial context.

### Social Intelligence (The Vibe)
*   **Volume Tracking**: Uses Reddit API to log daily post volume in `r/FirstTimeHomeBuyer`. Spikes often signal market anxiety shifts before pricing changes.
*   **Topic Extraction**: Weekly extraction of active thread titles/comments to Vertex AI. The LLM identifies the week’s "Primary Friction Point" (e.g., “Competition,” “Insurance Hikes,” “Rate Paralysis”).

## 3. Dashboard Features & Interaction
The embedded Streamlit dashboard features three high-impact modules:
1.  **The Lead-Lag Divergence Plot**: Dual-axis time-series chart showing Zillow National Price Index vs. Reddit Post Volume.
2.  **The "Primary Friction" Feed**: Real-time cards showcasing top three extracted topics, linking social complaints to economic metrics.
3.  **Metro Benchmark Tool**: Interactive table for selecting cities (Top 100 MSAs) to compare local growth vs. the National Sentiment Index.

## 4. Implementation Requirements

### GCP Setup
*   Enable BigQuery and Cloud Functions.
*   Service account with "BigQuery Data Editor" permissions.

### API Keys
*   Secure access to FRED API and Reddit (PRAW).
*   Store in "Secrets" manager of both GCP and Streamlit Cloud.

### BigQuery Schema
*   `fact_zillow_prices`: Keyed by `MSA_ID` and `Month`.
*   `fact_reddit_sentiment`: Keyed by `Date`, `Post_Count`, and `Top_Topic`.

### Next.js Embedding
*   Deploy Next.js shell on Vercel.
*   Embed Streamlit app using iframe with `?embed=true` for a seamless look.
