from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4929624
table_name = "5_Hour_Trending_on_PumpSwap"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH decoded_swap AS (
  SELECT * FROM query_4893450
),

decoded_pool AS (
  SELECT 
    pool,
    quoteMint,
    baseMint AS contract_address  
  FROM query_4922901
),

avg_price AS (
  SELECT AVG(price) AS avg_sol_to_usd
  FROM prices.usd
  WHERE symbol = 'SOL'
),

pumpswap_trades AS (
  SELECT 
    s.pool,
    p.contract_address,
    CASE
      WHEN p.quoteMint IN (
        'So11111111111111111111111111111111111111112', -- SOL
        'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'  -- mSOL
      ) THEN (s.quoteAmountOutorIn / 1e9) * pr.avg_sol_to_usd  

      WHEN p.quoteMint IN (
        'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', -- USDC
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', -- USDT
        'DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT'  -- USDE
      ) THEN s.quoteAmountOutorIn / 1e6  

      ELSE NULL
    END AS volume_usd
  FROM decoded_swap s
  JOIN decoded_pool p ON s.pool = p.pool
  CROSS JOIN avg_price pr
  WHERE s.block_time >= NOW() - INTERVAL '5' HOUR
    AND p.quoteMint IN (
      'So11111111111111111111111111111111111111112',
      'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
      'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
      'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
      'DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT'
    )
)

SELECT
  SUM(volume_usd) AS volume_usd,
  CONCAT(
'<a href="https://axiom.trade/t/',
    contract_address,
'/@adamtehc" target="_blank">',
    contract_address,
    '</a>'
  ) AS contract_address,
    pool
FROM pumpswap_trades
WHERE volume_usd IS NOT NULL
GROUP BY pool, contract_address
ORDER BY volume_usd DESC
LIMIT 20;
'''