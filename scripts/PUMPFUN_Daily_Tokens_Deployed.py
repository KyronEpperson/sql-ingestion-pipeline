from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4010816
table_name = "PUMPFUN_Daily_Tokens_Deployed"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH
pumpdotfun_tokens AS (
  SELECT
      MIN(block_time) AS time,
      token_mint_address,
      'Pumpdotfun' AS platform
  FROM tokens_solana.transfers
  WHERE action = 'mint'
    AND outer_executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    AND block_time >= CURRENT_DATE - INTERVAL '120' DAY
  GROUP BY token_mint_address
),

mint_transfers AS (
  SELECT
      block_time,
      token_mint_address,
      outer_executing_account,
      tx_signer,
      inner_instruction_index
  FROM tokens_solana.transfers
  WHERE block_time >= CURRENT_DATE - INTERVAL '120' DAY
    AND action = 'mint'
    AND outer_executing_account IN (
          'MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG',
          'boop8hVGQGqehUK2iVEMEnMrL5RbjywRzHKBmBE7ry4',
          'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
        )
),
xfer_tokens AS (
  SELECT
      MIN(time) AS time,
      token_mint_address,
      platform
  FROM (
    SELECT
        block_time AS time,
        token_mint_address,
        CASE
          WHEN outer_executing_account = 'MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG'
            THEN 'Moon.it'
          WHEN outer_executing_account = 'boop8hVGQGqehUK2iVEMEnMrL5RbjywRzHKBmBE7ry4'
               AND inner_instruction_index = 4
            THEN 'Boop'
          WHEN outer_executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
               AND tx_signer = 'BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv'
            THEN 'Bags'
          WHEN outer_executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
               AND tx_signer = '7rtiKSUDLBm59b1SBmD9oajcP8xE64vAGSMbAN5CXy1q'
            THEN 'Moonshot'
        END AS platform
    FROM mint_transfers
    WHERE
         outer_executing_account = 'MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG'
      OR (outer_executing_account = 'boop8hVGQGqehUK2iVEMEnMrL5RbjywRzHKBmBE7ry4' AND inner_instruction_index = 4)
      OR (outer_executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
          AND tx_signer IN ('BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv','7rtiKSUDLBm59b1SBmD9oajcP8xE64vAGSMbAN5CXy1q'))
  ) s
  WHERE platform IS NOT NULL
  GROUP BY token_mint_address, platform
),

calls_filtered AS (
  SELECT
      block_time,
      tx_id,
      executing_account,
      tx_signer,
      account_arguments,
      inner_instructions,
      data
  FROM solana.instruction_calls
  WHERE block_time >= CURRENT_DATE - INTERVAL '120' DAY
    AND tx_success = TRUE
    AND executing_account IN (
          'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj',
          'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN',
          'waveQX2yP3H1pVU8djGvEHmYg8uamQ84AuyGtpsrXTF',
          'HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o',
          'deus4Bvftd5QKcEkE5muQaWGWDoma8GrySvPFrBPjhS'
        )
),
call_tokens AS (
  SELECT
      MIN(time) AS time,
      token_mint_address,
      platform
  FROM (
    SELECT
        block_time AS time,
        CASE
          WHEN executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
               AND (varbinary_starts_with(data, 0xafaf6d1f0d989bed) OR varbinary_starts_with(data, 0x4399af27))
            THEN tx_id
          WHEN executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
               AND tx_signer = '5qWya6UjwWnGVhdSBL3hyZ7B45jbk6Byt1hwd7ohEGXE'
               AND varbinary_starts_with(data, 0x8c55d7b0)
            THEN account_arguments[4]
          WHEN executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
               AND cardinality(account_arguments) >= 9
               AND account_arguments[9] = '8rE9CtCjwhSmbwL5fbJBtRFsS3ohfMcDFeTTC7t4ciUA'
            THEN tx_id
          WHEN executing_account = 'waveQX2yP3H1pVU8djGvEHmYg8uamQ84AuyGtpsrXTF'
               AND cardinality(inner_instructions) > 0
               AND inner_instructions[1].data = '11114XtYk9gGfZoo968fyjNUYQJKf9gdmkGoaoBpzFv4vyaSMBn3VKxZdv7mZLzoyX5YNC'
            THEN account_arguments[3]
          WHEN executing_account = 'HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o'
               AND varbinary_starts_with(data, 0x2a2b7e38)
            THEN account_arguments[6]
          WHEN executing_account = 'deus4Bvftd5QKcEkE5muQaWGWDoma8GrySvPFrBPjhS'
               AND varbinary_starts_with(data, 0x181ec828)
            THEN account_arguments[4]
        END AS token_mint_address,
        CASE
          WHEN executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
               AND (varbinary_starts_with(data, 0xafaf6d1f0d989bed) OR varbinary_starts_with(data, 0x4399af27))
            THEN CASE
                   WHEN account_arguments[4] IN (
                        'FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1',
                        'BuM6KDpWiTcxvrpXywWFiw45R2RNH8WURdvqoTDV1BW4'
                      )
                     THEN 'LetsBonk'
                   ELSE 'LaunchLabs'
                 END
          WHEN executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
               AND tx_signer = '5qWya6UjwWnGVhdSBL3hyZ7B45jbk6Byt1hwd7ohEGXE'
               AND varbinary_starts_with(data, 0x8c55d7b0)
            THEN 'Believeapp'
          WHEN executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
               AND cardinality(account_arguments) >= 9
               AND account_arguments[9] = '8rE9CtCjwhSmbwL5fbJBtRFsS3ohfMcDFeTTC7t4ciUA'
            THEN 'Jup Studio'
          WHEN executing_account = 'waveQX2yP3H1pVU8djGvEHmYg8uamQ84AuyGtpsrXTF'
               AND cardinality(inner_instructions) > 0
               AND inner_instructions[1].data = '11114XtYk9gGfZoo968fyjNUYQJKf9gdmkGoaoBpzFv4vyaSMBn3VKxZdv7mZLzoyX5YNC'
            THEN 'Wavebreak'
          WHEN executing_account = 'HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o'
               AND varbinary_starts_with(data, 0x2a2b7e38)
            THEN 'Heaven'
          WHEN executing_account = 'deus4Bvftd5QKcEkE5muQaWGWDoma8GrySvPFrBPjhS'
               AND varbinary_starts_with(data, 0x181ec828)
            THEN 'Sugar'
        END AS platform
    FROM calls_filtered
    WHERE
         (executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
          AND (varbinary_starts_with(data, 0xafaf6d1f0d989bed) OR varbinary_starts_with(data, 0x4399af27)))
      OR (executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
          AND tx_signer = '5qWya6UjwWnGVhdSBL3hyZ7B45jbk6Byt1hwd7ohEGXE'
          AND varbinary_starts_with(data, 0x8c55d7b0))
      OR (executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
          AND cardinality(account_arguments) >= 9
          AND account_arguments[9] = '8rE9CtCjwhSmbwL5fbJBtRFsS3ohfMcDFeTTC7t4ciUA')
      OR (executing_account = 'waveQX2yP3H1pVU8djGvEHmYg8uamQ84AuyGtpsrXTF'
          AND cardinality(inner_instructions) > 0
          AND inner_instructions[1].data = '11114XtYk9gGfZoo968fyjNUYQJKf9gdmkGoaoBpzFv4vyaSMBn3VKxZdv7mZLzoyX5YNC')
      OR (executing_account = 'HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o'
          AND varbinary_starts_with(data, 0x2a2b7e38))
      OR (executing_account = 'deus4Bvftd5QKcEkE5muQaWGWDoma8GrySvPFrBPjhS'
          AND varbinary_starts_with(data, 0x181ec828))
  ) s
  WHERE token_mint_address IS NOT NULL
    AND platform IS NOT NULL
  GROUP BY token_mint_address, platform
),

all_tokens AS (
  SELECT * FROM pumpdotfun_tokens
  UNION ALL
  SELECT * FROM xfer_tokens
  UNION ALL
  SELECT * FROM call_tokens
),

daily_tokens AS (
  SELECT
      DATE_TRUNC('day', time) AS date_time,
      platform,
      COUNT(*) AS daily_token_count
  FROM all_tokens
  GROUP BY 1, 2
)

SELECT
    date_time,
    platform,
    daily_token_count,
    SUM(daily_token_count) OVER (PARTITION BY platform ORDER BY date_time ASC) AS cumulative_token_count
FROM daily_tokens
WHERE date_time >= CURRENT_DATE - INTERVAL '120' DAY
  AND date_time < CURRENT_DATE
ORDER BY date_time DESC, platform ASC;
'''