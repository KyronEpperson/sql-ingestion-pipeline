from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5660681
table_name = "LAUNCHPAD_Token_Creators"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH all_platforms AS (
    SELECT
        DATE_TRUNC('day', block_time) AS date,
        tx_signer AS creator,
        CASE
            WHEN account_arguments[4] IN (
                'FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1',
                'BuM6KDpWiTcxvrpXywWFiw45R2RNH8WURdvqoTDV1BW4'
            )
            THEN 'LetsBonk'
            ELSE 'LaunchLabs'
        END AS platform
    FROM solana.instruction_calls
    WHERE block_time >= CURRENT_DATE - INTERVAL '120' DAY
      AND block_time < CURRENT_DATE
      AND executing_account = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj'
      AND tx_success = true
        AND (
      varbinary_starts_with(data, 0xafaf6d1f0d989bed)
      OR varbinary_starts_with(data, 0x4399af27)
      )
      
    UNION ALL
    SELECT
        DATE_TRUNC('day', call_block_time) AS date,
        account_user AS creator,
        'Pump.fun' AS platform
    FROM pumpdotfun_solana.pump_call_create
    WHERE call_block_time >= CURRENT_DATE - INTERVAL '120' DAY
      AND call_block_time < DATE '2025-11-09'

    UNION ALL
    SELECT
        DATE_TRUNC('day', block_time) AS date,
        account_arguments[6] AS creator,
        'Pump.fun' AS platform
    FROM solana.instruction_calls
    WHERE bytearray_substring(data, 1, 7) = 0xd6904cec5f8b31
      AND executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
      AND outer_executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
      AND tx_success = true
      AND block_time >= DATE '2025-11-11'
      AND block_time < CURRENT_DATE

    UNION ALL
    SELECT
        DATE_TRUNC('day', block_time) AS date,
        tx_signer AS creator,
        'Jup Studio' AS platform
    FROM solana.instruction_calls
    WHERE block_time >= CURRENT_DATE - INTERVAL '120' DAY
      AND block_time < CURRENT_DATE
      AND executing_account = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN'
      AND tx_success = true
      AND CARDINALITY(account_arguments) >= 9
      AND account_arguments[9] = '8rE9CtCjwhSmbwL5fbJBtRFsS3ohfMcDFeTTC7t4ciUA'
    UNION ALL
    SELECT
        DATE_TRUNC('day', block_time) AS date,
        tx_signer AS creator,
        'Heaven' AS platform
    FROM solana.instruction_calls
    WHERE block_time >= CURRENT_DATE - INTERVAL '120' DAY
      AND block_time < CURRENT_DATE
      AND executing_account = 'HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o'
      AND bytearray_substring(data, 1, 4) = 0x2a2b7e38
      AND tx_success = true
      
    UNION ALL
    
    SELECT
        DATE_TRUNC('day', block_time) AS date,
        account_arguments[7] AS creator,
        'Sugar' AS platform
    FROM solana.instruction_calls
    WHERE block_time >= CURRENT_DATE - INTERVAL '120' DAY
        AND block_time < CURRENT_DATE
        AND bytearray_substring(data, 1, 4) = 0x181ec828
        AND executing_account = 'deus4Bvftd5QKcEkE5muQaWGWDoma8GrySvPFrBPjhS'
        AND tx_success = true
)
SELECT 
    date,
    platform,
    COUNT(DISTINCT creator) AS unique_creators,
    COUNT(*) AS total_tokens_created
FROM all_platforms
WHERE creator IS NOT NULL
  AND creator != ''
GROUP BY date, platform
ORDER BY date DESC, unique_creators DESC;
'''