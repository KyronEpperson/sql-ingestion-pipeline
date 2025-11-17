from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5651636
table_name = "Builder_Codes_Revenue"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH json_data AS (
  SELECT
    JSON_PARSE(
      HTTP_GET(
        '
https://flowscan-builder-data-production.up.railway.app/api/builders/all-daily-revenue?startDate=2024-10-27&endDate=' || CAST(CURRENT_DATE AS VARCHAR)
      )
    ) AS parsed_data
),

-- Extract the dailyRevenue object
daily_revenue_raw AS (
  SELECT
    JSON_EXTRACT(parsed_data, '$.data.dailyRevenue') AS daily_revenue
  FROM json_data
),
-- Unnest dates (keys of dailyRevenue)
unnested_dates AS (
  SELECT
    t.date_key,
    t.date_value
  FROM daily_revenue_raw
  CROSS JOIN UNNEST(CAST(JSON_EXTRACT(daily_revenue, '$') AS MAP(VARCHAR, JSON))) AS t(date_key, date_value)
),
-- Unnest builder revenues for each date
unnested_builders AS (
  SELECT
    unnested_dates.date_key,
    builder,
    CAST(JSON_EXTRACT_SCALAR(revenue, '$') AS DOUBLE) AS daily_revenue
  FROM unnested_dates
  CROSS JOIN UNNEST(CAST(JSON_EXTRACT(unnested_dates.date_value, '$') AS MAP(VARCHAR, JSON))) AS t(builder, revenue)
),
-- Clean: convert date string to DATE type
daily_builder_revenue AS (
  SELECT
    DATE(date_key) AS day,
    builder,
    daily_revenue
  FROM unnested_builders
),
-- Aggregate total revenue across all builders per day
daily_total_revenue AS (
  SELECT
    day,
    SUM(daily_revenue) AS total_daily_revenue
  FROM daily_builder_revenue
  GROUP BY day
)
-- Final joined result: pivoted per builder + daily totals
SELECT
  d.day,
  d.total_daily_revenue,
  b.builder,
  b.daily_revenue
FROM daily_total_revenue d
JOIN daily_builder_revenue b ON d.day = b.day
ORDER BY d.day DESC, b.daily_revenue DESC;
'''