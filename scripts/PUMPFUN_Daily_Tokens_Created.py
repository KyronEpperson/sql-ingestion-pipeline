from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4861426
table_name = "PUMPFUN_Daily_Tokens_Created"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH pumpdotfun_tokens AS (
    SELECT
        MIN(block_time) AS time,
        token_mint_address
    FROM tokens_solana.transfers
    WHERE action = 'mint'
        AND outer_executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
        AND block_time >= DATE_ADD('day', -365, NOW())
    GROUP BY token_mint_address
),

graduated_tokens AS (
    SELECT 
        date_time,
        COUNT(DISTINCT token_address) AS daily_graduated_token_count
    FROM (
        SELECT
            DATE_TRUNC('day', block_date) AS date_time,
            account_arguments[3] AS token_address
        FROM solana.instruction_calls
        WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
            AND bytearray_substring(data, 1, 8) = 0xb712469c946da122
            AND tx_success = true
            AND block_slot >= 266793916
            AND block_date < DATE '2025-03-21'
        
        UNION ALL

        SELECT
            DATE_TRUNC('day', block_date) AS date_time,
            account_arguments[2] AS token_address
        FROM solana.instruction_calls
        WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
            AND bytearray_substring(data, 1, 8) = 0xb712469c946da122
            AND tx_success = true
            AND block_slot < 266793916
            AND block_date < DATE '2025-03-19'

        UNION ALL

        SELECT
            DATE_TRUNC('day', block_time) AS date_time,
            account_arguments[3] AS token_address
        FROM solana.instruction_calls
        WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
            AND bytearray_substring(data, 1, 8) = 0x9beae792ec9ea21e
            AND block_time >= DATE '2025-03-20'
            AND tx_success = TRUE
            AND (cardinality(inner_instructions) > 0 OR is_inner = true)
    ) all_grads
    GROUP BY date_time
),

daily_tokens AS (
    SELECT
        DATE_TRUNC('day', time) AS date_time,
        COUNT(*) AS daily_token_count,
        'Pumpdotfun' AS platform
    FROM pumpdotfun_tokens
    GROUP BY DATE_TRUNC('day', time)
),

final_data AS (
    SELECT
        dt.date_time,
        dt.platform,
        dt.daily_token_count,
        COALESCE(gt.daily_graduated_token_count, 0) AS daily_graduated_token_count,
        dt.daily_token_count - COALESCE(gt.daily_graduated_token_count, 0) AS daily_nongrad_token_count
    FROM daily_tokens dt
    LEFT JOIN graduated_tokens gt ON dt.date_time = gt.date_time
)

SELECT
    date_time,
    platform,
    daily_token_count,
    daily_graduated_token_count,
    daily_nongrad_token_count,
    SUM(daily_token_count) OVER (PARTITION BY platform ORDER BY date_time ASC) AS cumulative_token_count,
    SUM(daily_nongrad_token_count) OVER (PARTITION BY platform ORDER BY date_time ASC) AS cumulative_nongrad_token_count
FROM final_data
WHERE date_time >= DATE_ADD('day', -365, NOW())
ORDER BY date_time DESC, platform ASC;
'''