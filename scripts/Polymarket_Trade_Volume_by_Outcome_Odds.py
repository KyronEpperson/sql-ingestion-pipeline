from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 6058140
table_name = "Polymarket_Trade_Volume_by_Outcome_Odds"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''-- Daily volume distribution by implied probability (price) bands

WITH base AS (
  SELECT
    DATE_TRUNC('day', block_time) AS day,
    price,
    amount
  FROM polymarket_polygon.market_trades
  WHERE block_time >= date'2024-01-01'
    AND amount > 0
),

binned AS (
  SELECT
    day,
    CASE
      WHEN price < 0.01 THEN '0-1%'
      WHEN price < 0.05 THEN '1-5%'
      WHEN price < 0.10 THEN '5-10%'
      WHEN price < 0.20 THEN '10-20%'
      WHEN price < 0.30 THEN '20-30%'
      WHEN price < 0.40 THEN '30-40%'
      WHEN price < 0.60 THEN '40-60%'
      WHEN price < 0.70 THEN '60-70%'
      WHEN price < 0.80 THEN '70-80%'
      WHEN price < 0.90 THEN '80-90%'
      WHEN price < 0.95 THEN '90-95%'
      WHEN price < 0.99 THEN '95-99%'
      ELSE '99-100%'
    END AS price_band,
    amount AS volume_usd
  FROM base
),

daily AS (
  SELECT
    day,
    price_band,
    SUM(volume_usd) AS volume_usd
  FROM binned
  GROUP BY 1, 2
)

SELECT
  day,
  price_band,
  volume_usd,
  ROUND(
    100 * volume_usd / NULLIF(SUM(volume_usd) OVER (PARTITION BY day), 0),
    2
  ) AS pct_of_day
FROM daily
ORDER BY day, price_band;
'''