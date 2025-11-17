from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5922860
table_name = "Weekly_Prediction_Market_Notional_Sports_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH tags as (
WITH base AS (
  SELECT
    *,
    TRANSFORM(SPLIT(tags, ','), x -> TRIM(LOWER(x))) AS tags_arr_lc
  FROM polymarket_polygon.market_details
)

SELECT
DISTINCT
condition_id,
CASE
WHEN array_position(tags_arr_lc, 'trump')   > 0  THEN 'Trump'
WHEN array_position(tags_arr_lc, 'sports')   > 0  THEN 'Sports'
WHEN array_position(tags_arr_lc, 'crypto')   > 0 THEN 'Crypto'
WHEN array_position(tags_arr_lc, 'politics') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'geopolitics') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'nyc mayor') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'openai') > 0 THEN 'Tech'
WHEN array_position(tags_arr_lc, 'ai') > 0 THEN 'Tech'
WHEN array_position(tags_arr_lc, 'nba') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'weather') > 0 THEN 'Weather'
WHEN array_position(tags_arr_lc, 'economy') > 0 THEN 'Economy'
WHEN array_position(tags_arr_lc, 'culture') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'elections') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'ipo') > 0 THEN 'Earnings'
WHEN array_position(tags_arr_lc, 'esports') > 0 THEN 'Esports'
WHEN array_position(tags_arr_lc, 'celebrities') > 0 THEN 'Celebrities'
WHEN array_position(tags_arr_lc, 'global elections') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'bitcoin') > 0 THEN 'Crypto'
WHEN array_position(tags_arr_lc, 'la liga') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'youtube') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'pope') > 0 THEN 'Religion'
WHEN array_position(tags_arr_lc, 'cfb') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'crypto prices') > 0 THEN 'Crypto'
WHEN array_position(tags_arr_lc, 'tech') > 0 THEN 'Tech'
WHEN array_position(tags_arr_lc, 'world') > 0 THEN 'World'
WHEN array_position(tags_arr_lc, 'religion') > 0 THEN 'Religion'
WHEN array_position(tags_arr_lc, 'economic policy') > 0 THEN 'Economy'
WHEN array_position(tags_arr_lc, 'nfl') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'finance') > 0 THEN 'Economy'
WHEN array_position(tags_arr_lc, 'business') > 0 THEN 'Economy'
WHEN array_position(tags_arr_lc, 'soccer') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'biden') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'science') > 0 THEN 'World'
WHEN array_position(tags_arr_lc, 'geology') > 0 THEN 'World'
WHEN array_position(tags_arr_lc, 'music') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'spacex') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'jobs') > 0 THEN 'Economy'
WHEN array_position(tags_arr_lc, 'chess') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'movies') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'social media') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'technology') > 0 THEN 'Tech'
WHEN array_position(tags_arr_lc, 'nfts') > 0 THEN 'Crypto'
WHEN array_position(tags_arr_lc, 'mlb') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'us politics') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'champions league') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'inflation') > 0 THEN 'Economy'
WHEN array_position(tags_arr_lc, 'video games') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'entertainment') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'blockchain') > 0 THEN 'Crypto'
WHEN array_position(tags_arr_lc, 'u.s. politics') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'tweet markets') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'airdrops') > 0 THEN 'Crypto'
WHEN array_position(tags_arr_lc, 'awards') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'boxing') > 0 THEN 'Culture'
WHEN array_position(tags_arr_lc, 'fed rates') > 0 THEN 'Economy'
WHEN array_position(tags_arr_lc, 'f1') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'gaza') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'russia') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'ukraine') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'israel') > 0 THEN 'Politics'
WHEN array_position(tags_arr_lc, 'golf') > 0 THEN 'Sports'
WHEN array_position(tags_arr_lc, 'memecoins') > 0 THEN 'Crypto'
ELSE 'Other'
END AS custom_tag
FROM base
)

SELECT DATE_TRUNC('week', block_time) as "Week", 'Polymarket' as "Platform", SUM(shares)/2 as "Notional Volume" FROM polymarket_polygon.market_trades p
INNER JOIN tags t ON p.condition_id = t.condition_id
WHERE DATE_TRUNC('week', block_time) >= CAST('2024-04-08' AS DATE)
AND DATE_TRUNC('week', block_time) < DATE_TRUNC('week', NOW())
AND custom_tag = 'Sports'
GROUP BY 1,2
UNION ALL
SELECT 
DATE_TRUNC('week', DATE(date)) as week,
'Kalshi' as "Platform",
SUM(value) as "Volume"
FROM dune.datadashboardsapi.dataset_kalshi_daily_metrics
WHERE "Type" = 'Volume'
AND DATE_TRUNC('week', DATE(date)) >= CAST('2024-04-08' AS DATE)
AND DATE_TRUNC('week', DATE(date)) < DATE_TRUNC('week', NOW())
AND category = 'Sports'
GROUP BY 1,2
'''