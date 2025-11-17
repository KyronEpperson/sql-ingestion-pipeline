from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5468582
table_name = "Weekly_Launchpad_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH bonk_tokens AS (
  SELECT DISTINCT account_arguments[7] AS token_mint_address
  FROM solana.instruction_calls
  WHERE executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
      AND (
      varbinary_starts_with(data, 0xafaf6d1f0d989bed)
      OR varbinary_starts_with(data, 0x4399af27)
      )
    AND tx_success = TRUE
    AND account_arguments[4] IN (
      'FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1',
      'BuM6KDpWiTcxvrpXywWFiw45R2RNH8WURdvqoTDV1BW4'
    )
),
bonk_volume_old AS (
  SELECT 
    DATE_TRUNC('week', block_time) AS dt,
    'bonk' AS category,
    SUM(amount_usd) AS volume_usd
  FROM dex_solana.trades
  WHERE block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', block_time) < DATE_TRUNC('week', DATE '2025-08-27')
    AND project = 'raydium_launchlab'
    AND (
      token_bought_mint_address IN (SELECT token_mint_address FROM bonk_tokens)
      OR token_sold_mint_address IN (SELECT token_mint_address FROM bonk_tokens)
    )
  GROUP BY 1, 2
),
bonk_volume_new AS (
  SELECT 
    DATE_TRUNC('week', block_time) AS dt,
    'bonk' AS category,
    SUM(volume_usd) AS volume_usd
  FROM query_5808441
  WHERE block_time >= DATE '2025-08-27'
    AND block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', block_time) < DATE_TRUNC('week', NOW())
  GROUP BY 1, 2
),
bonk_volume AS (
  SELECT * FROM bonk_volume_old
  UNION ALL
  SELECT * FROM bonk_volume_new
),
raydium_launchlab_volume AS (
  SELECT 
    DATE_TRUNC('week', block_time) AS dt,
    'raydium_launchlab' AS category,
    SUM(amount_usd) AS volume_usd
  FROM dex_solana.trades
  WHERE block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', block_time) < DATE_TRUNC('week', NOW())
    AND project = 'raydium_launchlab'
    AND token_bought_mint_address NOT IN (SELECT token_mint_address FROM bonk_tokens)
    AND token_sold_mint_address NOT IN (SELECT token_mint_address FROM bonk_tokens)
  GROUP BY 1, 2
),
pumpdotfun_volume AS (
  SELECT 
    DATE_TRUNC('week', block_time) AS dt,
    'pumpdotfun' AS category,
    SUM(amount_usd) AS volume_usd
  FROM dex_solana.trades
  WHERE block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', block_time) < DATE_TRUNC('week', NOW())
    AND project = 'pumpdotfun'
  GROUP BY 1, 2
),
bags_volume AS (
  SELECT 
    DATE_TRUNC('week', q.block_time) AS dt,
    'bags' AS category,
    SUM(q.amount_usd) AS volume_usd
  FROM query_5610453 q
  WHERE q.amount_usd > 0 
    AND q.block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', q.block_time) < DATE_TRUNC('week', NOW())
  GROUP BY 1, 2
),
moonshot_volume AS (
  SELECT 
    DATE_TRUNC('week', q.block_time) AS dt,
    'moonshot' AS category,
    SUM(q.amount_usd) AS volume_usd
  FROM query_5615005 q
  WHERE q.amount_usd > 0 
    AND q.block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', q.block_time) < DATE_TRUNC('week', NOW())
  GROUP BY 1, 2
),
believe_volume AS (
  SELECT 
    DATE_TRUNC('week', q.block_time) AS dt,
    'believe' AS category,
    SUM(q.amount_usd) AS volume_usd
  FROM query_5630602 q
  WHERE q.amount_usd > 0 
    AND q.block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', q.block_time) < DATE_TRUNC('week', NOW())
  GROUP BY 1, 2
),
sugar_volume AS (
  SELECT 
    DATE_TRUNC('week', q.block_time) AS dt,
    'Sugar' AS category,
    SUM(q.amount_usd) AS volume_usd
  FROM query_5672325 q
  WHERE q.amount_usd > 0 
    AND q.block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', q.block_time) < DATE_TRUNC('week', NOW())
  GROUP BY 1, 2
),
heaven_volume AS (
  SELECT 
    DATE_TRUNC('week', q.block_time) AS dt,
    'heaven' AS category,
    SUM(q.amount_usd) AS volume_usd
  FROM query_5686944 q
  WHERE q.amount_usd > 0 
    AND q.trading_phase = 'bonding_curve'
    AND q.block_time >= DATE_ADD('week', -20, NOW())
    AND DATE_TRUNC('week', q.block_time) < DATE_TRUNC('week', NOW())
  GROUP BY 1, 2
)
SELECT * FROM bonk_volume
UNION ALL
SELECT * FROM raydium_launchlab_volume
UNION ALL
SELECT * FROM pumpdotfun_volume
UNION ALL
SELECT * FROM bags_volume
UNION ALL
SELECT * FROM moonshot_volume
UNION ALL
SELECT * FROM believe_volume
UNION ALL
SELECT * FROM sugar_volume
UNION ALL
SELECT * FROM heaven_volume
ORDER BY dt DESC, category;
'''