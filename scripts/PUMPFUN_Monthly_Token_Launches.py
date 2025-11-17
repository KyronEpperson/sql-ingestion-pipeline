from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4048901
table_name = "PUMPFUN_Monthly_Token_Launches"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH pump_tokens AS (
    SELECT
        DATE_TRUNC('month', block_time) AS date,
        token_mint_address
    FROM tokens_solana.transfers
    WHERE action = 'mint'
        AND outer_executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
        AND DATE(block_time) != DATE '2024-05-16'  
    GROUP BY token_mint_address, block_time
),

monthly_pump_tokens AS (
    SELECT
        date,
        COUNT(*) AS monthly_token_launches
    FROM pump_tokens
    GROUP BY date
),

graduated_tokens AS (
    SELECT 
        DATE_TRUNC('month', block_time) AS date,
        account_arguments[3] AS token_address
    FROM solana.instruction_calls
    WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
      AND bytearray_substring(data, 1, 8) = 0xb712469c946da122
      AND tx_success = TRUE
      AND DATE(block_time) <= DATE '2025-03-21'
      AND DATE(block_time) != DATE '2024-05-16'

    UNION ALL

    SELECT 
        DATE_TRUNC('month', block_time) AS date,
        account_arguments[2] AS token_address
    FROM solana.instruction_calls
    WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
      AND bytearray_substring(data, 1, 8) = 0xb712469c946da122
      AND tx_success = TRUE
      AND DATE(block_time) <= DATE '2025-03-21'
      AND DATE(block_time) != DATE '2024-05-16'

    UNION ALL

    SELECT 
        DATE_TRUNC('month', block_time) AS date,
        account_arguments[3] AS token_address
    FROM solana.instruction_calls
    WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
      AND bytearray_substring(data, 1, 8) = 0x9beae792ec9ea21e
      AND tx_success = TRUE
      AND cardinality(inner_instructions) > 0
      AND DATE(block_time) >= DATE '2025-03-19'
      AND DATE(block_time) != DATE '2024-05-16'
),

monthly_graduations AS (
    SELECT
        date,
        COUNT(DISTINCT token_address) AS monthly_graduations
    FROM graduated_tokens
    GROUP BY date
)

SELECT
    dpt.date,
    COALESCE(dpt.monthly_token_launches, 0) AS monthly_token_launches,
    COALESCE(dg.monthly_graduations, 0) AS monthly_graduations,
    CASE 
        WHEN COALESCE(dpt.monthly_token_launches, 0) = 0 THEN NULL
        ELSE COALESCE(CAST(dg.monthly_graduations AS DECIMAL(16, 4)), 0) / CAST(dpt.monthly_token_launches AS DECIMAL(16, 4))
    END AS graduation_to_launch_ratio
FROM
    monthly_pump_tokens dpt
LEFT JOIN
    monthly_graduations dg ON dpt.date = dg.date
WHERE
    dpt.date < DATE_TRUNC('month', CURRENT_DATE)  
ORDER BY
    dpt.date DESC;

'''