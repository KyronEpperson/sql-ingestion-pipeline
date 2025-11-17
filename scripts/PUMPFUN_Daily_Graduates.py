from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5131612
table_name = "PUMPFUN_Daily_Graduates"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH boop_graduates AS (
  SELECT
    DATE(block_time) AS block_date,
    'Boop' AS platform,
    COUNT(DISTINCT tx_id) AS daily_graduates
  FROM tokens_solana.transfers
  WHERE action = 'mint'
    AND outer_executing_account = 'boop8hVGQGqehUK2iVEMEnMrL5RbjywRzHKBmBE7ry4'
    AND inner_instruction_index = 17
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
  GROUP BY 1, 2
),

all_bonk_tokens AS (
  SELECT DISTINCT account_arguments[7] AS token_address
  FROM solana.instruction_calls
  WHERE block_time >= DATE '2025-04-26'
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

launchlab_graduates_old AS (
  SELECT
    block_time,
    tx_id,
    CASE 
      WHEN inner_executing_account = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' 
        AND account_arguments[6] = 'So11111111111111111111111111111111111111112' THEN account_arguments[5]
      WHEN inner_executing_account = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' THEN account_arguments[6]
      WHEN inner_executing_account = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' 
        AND account_arguments[9] = 'So11111111111111111111111111111111111111112' THEN account_arguments[10]
      ELSE account_arguments[9]
    END AS token_address
  FROM solana.instruction_calls
  WHERE block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < DATE '2025-08-20'
    AND outer_executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
    AND (
      (inner_executing_account = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' AND bytearray_substring(data, 1, 8) = 0xafaf6d1f0d989bed) OR
      (inner_executing_account = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' AND bytearray_substring(data, 1, 3) = 0x01fe00)
    )
),

launchlab_graduates_new AS (
  SELECT 
    block_time,
    tx_id,
    CASE 
      WHEN account_arguments[7] = 'USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB' THEN account_arguments[6]
      ELSE account_arguments[7]
    END AS token_address
  FROM solana.instruction_calls
    WHERE block_time >= DATE '2025-08-19'
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
    AND tx_signer = 'RAYpQbFNq9i3mu6cKpTKKRwwHFDeK5AuZz8xvxUrCgw'
    AND bytearray_substring(data, 1, 4) = 0x3f37fe41
    AND executing_account = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'
    AND tx_success = true
),

launchlab_graduates_raw AS (
  SELECT block_time, tx_id, token_address FROM launchlab_graduates_old
  UNION ALL
  SELECT block_time, tx_id, token_address FROM launchlab_graduates_new
),

bonk_graduates AS (
  SELECT
    DATE(ll.block_time) AS block_date,
    'LetsBonk' AS platform,
    COUNT(DISTINCT ll.tx_id) AS daily_graduates
  FROM launchlab_graduates_raw ll
  INNER JOIN all_bonk_tokens bonk
    ON ll.token_address = bonk.token_address
  GROUP BY 1, 2
),

launchlab_graduates AS (
  SELECT
    DATE(block_time) AS block_date,
    'LaunchLab' AS platform,
    COUNT(DISTINCT tx_id) AS daily_graduates
  FROM launchlab_graduates_raw
  WHERE token_address NOT IN (SELECT token_address FROM all_bonk_tokens)
  GROUP BY 1, 2
),

pumpfun_graduates AS (
  SELECT
    DATE(block_time) AS block_date,
    'Pumpfun' AS platform,
    COUNT(DISTINCT account_arguments[3]) AS daily_graduates
  FROM solana.instruction_calls
  WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    AND bytearray_substring(data, 1, 8) = 0x9beae792ec9ea21e
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
    AND (cardinality(inner_instructions) > 0 OR is_inner = true)
    AND tx_success = TRUE
  GROUP BY 1, 2
),

moonshot_graduates_raw AS (
  SELECT
    account_arguments[7] AS token_address,
    block_time
  FROM solana.instruction_calls
  WHERE inner_executing_account = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
    AND outer_executing_account = '6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma'
    AND cardinality(account_arguments) > 7
    AND account_arguments[7] LIKE '%moon'
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
    AND tx_success = true
),

moonshot_graduates_deduped AS (
  SELECT
    token_address,
    MIN(block_time) AS first_graduation_time
  FROM moonshot_graduates_raw
  GROUP BY token_address
),

moonshot_graduates AS (
  SELECT
    DATE(first_graduation_time) AS block_date,
    'Moonshot' AS platform,
    COUNT(*) AS daily_graduates
  FROM moonshot_graduates_deduped fg
  JOIN query_5370809 c ON fg.token_address = c.token_address
  GROUP BY 1, 2
),

all_believe_tokens AS (
  SELECT DISTINCT account_arguments[4] AS token_address
  FROM solana.instruction_calls
  WHERE executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
    AND tx_signer = '5qWya6UjwWnGVhdSBL3hyZ7B45jbk6Byt1hwd7ohEGXE'
    AND bytearray_substring(data, 1, 4) = 0x8c55d7b0
    AND tx_success = true
),

believe_graduates AS (
  SELECT
    DATE(block_time) AS block_date,
    'Believe' AS platform,
    COUNT(DISTINCT account_arguments[14]) AS daily_graduates
  FROM solana.instruction_calls
  WHERE executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
    AND bytearray_substring(data, 1, 4) = 0x9ca9e667
    AND is_inner = true
    AND tx_success = true
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
    AND CARDINALITY(account_arguments) >= 15
    AND account_arguments[14] IN (SELECT token_address FROM all_believe_tokens)
  GROUP BY 1, 2
),

wavebreak_graduates AS (
  SELECT
    DATE(block_time) AS block_date,
    'Wavebreak' AS platform,
    COUNT(DISTINCT account_arguments[9]) AS daily_graduates
  FROM solana.instruction_calls
  WHERE executing_account = 'waveQX2yP3H1pVU8djGvEHmYg8uamQ84AuyGtpsrXTF'
    AND bytearray_substring(data, 1, 1) = 0x20
    AND CARDINALITY(account_arguments) >= 9
    AND block_time >= DATE('2025-07-29')
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
    AND tx_success = true
  GROUP BY 1, 2
),

bags_launched_tokens AS (
  SELECT DISTINCT account_arguments[4] AS token_address
  FROM solana.instruction_calls
  WHERE executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
    AND tx_signer = 'BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv'
    AND bytearray_substring(data, 1, 4) = 0x8c55d7b0
    AND tx_success = true
),

bags_graduates AS (
  SELECT 
    DATE(block_time) AS block_date,
    'Bags' AS platform,
    COUNT(DISTINCT account_arguments[14]) AS daily_graduates
  FROM solana.instruction_calls
  WHERE tx_signer IN ('CQdrEsYAxRqkwmpycuTwnMKggr3cr9fqY8Qma4J9TudY', 'DeQ8dPv6ReZNQ45NfiWwS5CchWpB2BVq1QMyNV8L2uSW')
    AND executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
    AND bytearray_substring(data, 1, 8) = 0x9ca9e66735e45040
    AND tx_success = true
    AND block_time >= DATE('2025-05-11')
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
    AND account_arguments[14] IN (SELECT token_address FROM bags_launched_tokens)
  GROUP BY 1, 2
),
sugar_graduates AS (
  SELECT 
    DATE(block_time) AS block_date,
    'Sugar' AS platform,
    COUNT(DISTINCT account_arguments[2]) AS daily_graduates
  FROM solana.instruction_calls
  WHERE bytearray_substring(data, 1, 8) = 0x60e65b8c8b28eb8e
    AND executing_account = 'deus4Bvftd5QKcEkE5muQaWGWDoma8GrySvPFrBPjhS'
    AND tx_success = true
    AND block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
  GROUP BY 1, 2
),
heaven_graduates AS (
  SELECT
    DATE(block_time) AS block_date,
    'Heaven' AS platform,
    COUNT(DISTINCT tx_id) AS daily_graduates
  FROM query_5683308
  WHERE block_time >= NOW() - INTERVAL '90' day
    AND DATE(block_time) < CURRENT_DATE
  GROUP BY 1, 2
)

SELECT * FROM boop_graduates
UNION ALL
SELECT * FROM bonk_graduates
UNION ALL
SELECT * FROM launchlab_graduates
UNION ALL
SELECT * FROM pumpfun_graduates
UNION ALL
SELECT * FROM moonshot_graduates
UNION ALL
SELECT * FROM believe_graduates
UNION ALL
SELECT * FROM wavebreak_graduates
UNION ALL
SELECT * FROM bags_graduates
UNION ALL
SELECT * FROM sugar_graduates
UNION ALL
SELECT * FROM heaven_graduates
ORDER BY block_date DESC, platform;
'''