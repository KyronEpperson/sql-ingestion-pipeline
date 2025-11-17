from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5175131
table_name = "NEAR_Intents_Token_Volume_30_Days"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH static AS (
  SELECT split('{{symbol}}', ',') AS selected_symbols
),

symbols AS (
  SELECT symbol
  FROM static
  CROSS JOIN UNNEST(selected_symbols) AS t(symbol)
),

static_1 AS (
  SELECT split('{{blockchain}}', ',') AS selected_blockchains
),

blockchains AS (
  SELECT blockchain
  FROM static_1
  CROSS JOIN UNNEST(selected_blockchains) AS t(blockchain)
),

t1 as (
SELECT
  date_at,
  UPPER(blockchain) as blockchain,
  symbol,
  volume_amount_usd
  FROM dune.near.dataset_near_intents_metrics 
)

SELECT
  date_at,
  symbol,
  SUM(CAST(volume_amount_usd AS DOUBLE)) AS volume_usd
FROM t1
WHERE
  CAST(date_at AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
  AND IF(
    'ALL' IN (SELECT symbol FROM symbols),
    TRUE,
    symbol IN (SELECT symbol FROM symbols)
  )
  AND IF(
    'ALL' IN (SELECT blockchain FROM blockchains),
    TRUE,
    UPPER(blockchain) IN (SELECT blockchain FROM blockchains)
  )
GROUP BY date_at, symbol
HAVING SUM(CAST(volume_amount_usd AS DOUBLE)) > 0
ORDER BY date_at desc
'''