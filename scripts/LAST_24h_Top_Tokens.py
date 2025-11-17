from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5138002
table_name = "LAST_24h_Top_Tokens"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH pumpfun_graduates AS (
  SELECT account_arguments[3] AS token_address, 'Pumpfun' AS platform
  FROM solana.instruction_calls
  WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    AND bytearray_substring(data, 1, 8) = 0x9beae792ec9ea21e
    AND block_time >= NOW() - INTERVAL '7' DAY
    AND (cardinality(inner_instructions) > 0 OR is_inner = true)
    AND tx_success = TRUE
),

moonshot_recent_tokens AS (
  SELECT DISTINCT
    token_mint_address AS token_address
  FROM tokens_solana.transfers
  WHERE block_time >= NOW() - INTERVAL '7' DAY
    AND action = 'mint'
    AND outer_executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
    AND tx_signer = '7rtiKSUDLBm59b1SBmD9oajcP8xE64vAGSMbAN5CXy1q'
    AND token_mint_address != '8WiiPLXoUoDytn7saXfCFaoUmDL38NnQYNK13Pvvmoon'
),

moonshot_graduates AS (
  SELECT token_address, 'Moonshot' AS platform
  FROM moonshot_recent_tokens
),

all_bonk_tokens AS (
  SELECT DISTINCT account_arguments[7] AS token_address
  FROM solana.instruction_calls
  WHERE block_time >= NOW() - INTERVAL '7' DAY
    AND executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
      AND (
      varbinary_starts_with(data, 0xafaf6d1f0d989bed)
      OR varbinary_starts_with(data, 0x4399af27)
      )
    AND tx_success = TRUE
    AND (
      account_arguments[4] = 'FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1'
      OR account_arguments[4] = 'BuM6KDpWiTcxvrpXywWFiw45R2RNH8WURdvqoTDV1BW4'
    )
),

launchlab_grads_raw AS (
  SELECT
    CASE 
      WHEN inner_executing_account = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' 
        AND account_arguments[6] = 'So11111111111111111111111111111111111111112' THEN account_arguments[5]
      WHEN inner_executing_account = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' THEN account_arguments[6]
      WHEN inner_executing_account = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' 
        AND account_arguments[9] = 'So11111111111111111111111111111111111111112' THEN account_arguments[10]
      ELSE account_arguments[9]
    END AS token_address
  FROM solana.instruction_calls
  WHERE outer_executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
    AND (
      (inner_executing_account = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' AND bytearray_substring(data, 1, 8) = 0xafaf6d1f0d989bed) OR
      (inner_executing_account = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' AND bytearray_substring(data, 1, 3) = 0x01fe00)
    )
    AND block_time >= NOW() - INTERVAL '7' DAY
),

bonk_graduates AS (
  SELECT token_address, 'BonkFun' AS platform
  FROM all_bonk_tokens
  WHERE token_address IN (SELECT token_address FROM all_bonk_tokens)
),

launchlab_graduates AS (
  SELECT token_address, 'LaunchLabs' AS platform
  FROM launchlab_grads_raw
  WHERE token_address NOT IN (SELECT token_address FROM all_bonk_tokens)
),

believe_graduates AS (
  SELECT account_arguments[4] AS token_address, 'Believe' AS platform
  FROM solana.instruction_calls
  WHERE block_time >= NOW() - INTERVAL '7' DAY
    AND executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
    AND tx_signer = '5qWya6UjwWnGVhdSBL3hyZ7B45jbk6Byt1hwd7ohEGXE'
    AND account_arguments[4] <> 'So11111111111111111111111111111111111111112'
    AND tx_success
    AND NOT is_inner
),

jup_studio_graduates AS (
  WITH jup_authority_calls AS (
    SELECT
        tx_id,
        block_time
    FROM solana.instruction_calls
    WHERE block_time >= NOW() - INTERVAL '7' DAY
      AND CARDINALITY(account_arguments) >= 9
      AND account_arguments[9] = '8rE9CtCjwhSmbwL5fbJBtRFsS3ohfMcDFeTTC7t4ciUA'
      AND executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
      AND tx_success = true
  ),
  token_address_calls AS (
    SELECT
        tx_id,
        account_arguments[4] as token_address  
    FROM solana.instruction_calls
    WHERE block_time >= NOW() - INTERVAL '7' DAY
      AND executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
      AND bytearray_substring(data, 1, 6) = 0x8c55d7b06636
      AND CARDINALITY(account_arguments) >= 5
      AND tx_success = true
  )
  SELECT 
      t.token_address,
      'Jup Studio' AS platform
  FROM jup_authority_calls j
  INNER JOIN token_address_calls t ON j.tx_id = t.tx_id
),

wavebreak_graduates AS (
  SELECT 
    account_arguments[9] AS token_address, 
    'Wavebreak' AS platform
  FROM solana.instruction_calls
  WHERE executing_account = 'waveQX2yP3H1pVU8djGvEHmYg8uamQ84AuyGtpsrXTF'
    AND bytearray_substring(data, 1, 1) = 0x20
    AND CARDINALITY(account_arguments) >= 9
    AND block_time >= NOW() - INTERVAL '7' DAY
    AND tx_success = true
),

bags_graduates AS (
  SELECT 
    account_arguments[4] AS token_address, 
    'Bags' AS platform
  FROM solana.instruction_calls
  WHERE executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
    AND tx_signer = 'BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv'
    AND bytearray_substring(data, 1, 4) = 0x8c55d7b0
    AND block_time >= NOW() - INTERVAL '7' DAY
    AND tx_success = true
),
heaven_tokens AS (
  SELECT 
    account_arguments[6] AS token_address, 
    'Heaven' AS platform
  FROM solana.instruction_calls
  WHERE executing_account = 'HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o'
    AND bytearray_substring(data, 1, 4) = 0x2a2b7e38
    AND block_time >= NOW() - INTERVAL '7' DAY
    AND tx_success = true
),

sugar_graduates AS (
  SELECT 
    account_arguments[4] AS token_address, 
    'Sugar' AS platform
  FROM solana.instruction_calls
  WHERE bytearray_substring(data, 1, 4) = 0x181ec828
    AND executing_account = 'deus4Bvftd5QKcEkE5muQaWGWDoma8GrySvPFrBPjhS'
    AND tx_success = true
    AND block_time >= NOW() - INTERVAL '7' DAY
),

all_graduates AS (
  SELECT * FROM pumpfun_graduates
  UNION ALL SELECT * FROM launchlab_graduates
  UNION ALL SELECT * FROM bonk_graduates
  UNION ALL SELECT * FROM believe_graduates
  UNION ALL SELECT * FROM moonshot_graduates
  UNION ALL SELECT * FROM jup_studio_graduates
  UNION ALL SELECT * FROM wavebreak_graduates
  UNION ALL SELECT * FROM bags_graduates
  UNION ALL SELECT * FROM heaven_tokens
  UNION ALL SELECT * FROM sugar_graduates

),

token_prices AS (
  SELECT 
    COALESCE(
      CASE
        WHEN token_bought_mint_address IN (SELECT token_address FROM all_graduates) THEN token_bought_mint_address
        WHEN token_sold_mint_address IN (SELECT token_address FROM all_graduates) THEN token_sold_mint_address
      END, NULL
    ) AS token_address,
    CASE
      WHEN token_bought_mint_address IN (SELECT token_address FROM all_graduates) THEN token_bought_symbol
      WHEN token_sold_mint_address IN (SELECT token_address FROM all_graduates) THEN token_sold_symbol
      ELSE 'Unknown'
    END AS asset,
    amount_usd / NULLIF(
      CASE 
        WHEN token_bought_mint_address IN (SELECT token_address FROM all_graduates) THEN token_bought_amount
        WHEN token_sold_mint_address IN (SELECT token_address FROM all_graduates) THEN token_sold_amount
        ELSE 0
      END, 0
    ) AS token_price,
    CASE 
      WHEN token_bought_mint_address IN (SELECT token_address FROM all_graduates) THEN token_bought_amount
      WHEN token_sold_mint_address IN (SELECT token_address FROM all_graduates) THEN token_sold_amount
      ELSE 0
    END AS token_amount,
    block_time,
    ROW_NUMBER() OVER (PARTITION BY 
      CASE 
        WHEN token_bought_mint_address IN (SELECT token_address FROM all_graduates) THEN token_bought_mint_address
        WHEN token_sold_mint_address IN (SELECT token_address FROM all_graduates) THEN token_sold_mint_address
      END
      ORDER BY block_time DESC
    ) AS rn
  FROM dex_solana.trades
  WHERE amount_usd >= 1
    AND block_time >= NOW() - INTERVAL '24' HOUR
    AND (
      token_bought_mint_address IN (SELECT token_address FROM all_graduates)
      OR token_sold_mint_address IN (SELECT token_address FROM all_graduates)
    )
),

ranked_prices AS (
  SELECT token_address, asset, token_price, token_amount
  FROM token_prices
  WHERE rn <= 7 AND asset != 'Unknown'
),

trade_counts AS (
  SELECT
    COALESCE(token_bought_mint_address, token_sold_mint_address) AS token_address,
    COUNT(*) AS trade_count
  FROM dex_solana.trades
  WHERE amount_usd >= 1
    AND block_time >= NOW() - INTERVAL '7' DAY
    AND (
      token_bought_mint_address IN (SELECT token_address FROM all_graduates)
      OR token_sold_mint_address IN (SELECT token_address FROM all_graduates)
    )
  GROUP BY 1
),
query_5707806_supplies AS (
  SELECT 
    token,
    supply
  FROM query_5707806
  WHERE token IN (SELECT token_address FROM all_graduates)
),

query_5707803_supplies AS (
  SELECT 
    token,
    supply
  FROM query_5707803
  WHERE token IN (SELECT token_address FROM all_graduates)
),

token_supplies AS (
  SELECT 
    g.token_address,
    CASE 
      WHEN g.platform IN ('Sugar', 'Heaven', 'Wavebreak', 'Pumpfun') THEN 1000000000 
      WHEN g.platform IN ('Moonshot', 'Believe', 'Jup Studio', 'Bags') THEN COALESCE(q1.supply, 1000000000)
      WHEN g.platform IN ('LaunchLabs', 'BonkFun') THEN COALESCE(q2.supply, 1000000000)
      ELSE 1000000000
    END as total_supply
  FROM all_graduates g
  LEFT JOIN query_5707806_supplies q1 ON g.token_address = q1.token
  LEFT JOIN query_5707803_supplies q2 ON g.token_address = q2.token
)

SELECT
  RANK() OVER (ORDER BY (SUM(r.token_price * r.token_amount) / SUM(r.token_amount) * ts.total_supply) DESC) AS rank,
  CONCAT('<a href="https://axiom.trade/t/', r.token_address, '/@adamtehc" target="_blank">', r.asset, '</a>') AS asset_with_chart,
  CONCAT('<a href="https://axiom.trade/t/', r.token_address, '/@adamtehc" target="_blank">', r.token_address, '</a>') AS token_address_with_chart,
  r.asset,
  SUM(r.token_price * r.token_amount) / SUM(r.token_amount) AS vwap_token_price,
  ts.total_supply,
  (SUM(r.token_price * r.token_amount) / SUM(r.token_amount) * ts.total_supply) AS market_cap,
  tc.trade_count,
  g.platform
FROM ranked_prices r
JOIN trade_counts tc ON r.token_address = tc.token_address
JOIN all_graduates g ON r.token_address = g.token_address
JOIN token_supplies ts ON r.token_address = ts.token_address
WHERE tc.trade_count >= 100
GROUP BY r.token_address, r.asset, tc.trade_count, g.platform, ts.total_supply
ORDER BY market_cap DESC;
'''