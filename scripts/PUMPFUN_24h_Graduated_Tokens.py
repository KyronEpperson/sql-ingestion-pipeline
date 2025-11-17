from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4124453
table_name = "PUMPFUN_24h_Graduated_Tokens"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH withdraws AS (
    SELECT
        account_arguments[3] AS token_address
    FROM solana.instruction_calls
    WHERE executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
      AND bytearray_substring(data, 1, 8) = 0x9beae792ec9ea21e 
      AND block_time >= NOW() - INTERVAL '24' HOUR
      AND (cardinality(inner_instructions) > 0 OR is_inner = true)
      AND tx_success = TRUE
),
token_prices AS (
    SELECT 
        COALESCE(
            CASE
                WHEN token_bought_mint_address IN (SELECT token_address FROM withdraws) THEN token_bought_mint_address
                WHEN token_sold_mint_address IN (SELECT token_address FROM withdraws) THEN token_sold_mint_address
            END, NULL
        ) AS token_address,
        CASE
            WHEN token_bought_mint_address IN (SELECT token_address FROM withdraws) THEN token_bought_symbol
            WHEN token_sold_mint_address IN (SELECT token_address FROM withdraws) THEN token_sold_symbol
            ELSE 'Unknown'
        END AS asset,
        amount_usd / NULLIF(
            CASE 
                WHEN token_bought_mint_address IN (SELECT token_address FROM withdraws) THEN token_bought_amount
                WHEN token_sold_mint_address IN (SELECT token_address FROM withdraws) THEN token_sold_amount
                ELSE 0
            END, 0
        ) AS token_price,
        CASE 
            WHEN token_bought_mint_address IN (SELECT token_address FROM withdraws) THEN token_bought_amount
            WHEN token_sold_mint_address IN (SELECT token_address FROM withdraws) THEN token_sold_amount
            ELSE 0
        END AS token_amount,
        block_time,
        ROW_NUMBER() OVER (PARTITION BY 
            CASE 
                WHEN token_bought_mint_address IN (SELECT token_address FROM withdraws) THEN token_bought_mint_address
                WHEN token_sold_mint_address IN (SELECT token_address FROM withdraws) THEN token_sold_mint_address
            END
            ORDER BY block_time DESC
        ) AS rn
    FROM dex_solana.trades
    WHERE amount_usd >= 1
      AND block_time >= NOW() - INTERVAL '24' HOUR
      AND (
          token_bought_mint_address IN (SELECT token_address FROM withdraws)
          OR token_sold_mint_address IN (SELECT token_address FROM withdraws)
      )
),
ranked_prices AS (
    SELECT
        token_address, 
        asset,
        token_price,
        token_amount
    FROM 
        token_prices
    WHERE rn <= 7
      AND asset != 'Unknown'
),
trade_counts AS (
    SELECT
        COALESCE(token_bought_mint_address, token_sold_mint_address) AS token_address,
        COUNT(*) AS trade_count
    FROM dex_solana.trades
    WHERE 
        amount_usd >= 1
        AND block_time >= NOW() - INTERVAL '24' HOUR
        AND (
            token_bought_mint_address IN (SELECT token_address FROM withdraws)
            OR token_sold_mint_address IN (SELECT token_address FROM withdraws)
        )
    GROUP BY COALESCE(token_bought_mint_address, token_sold_mint_address)
)

SELECT
    RANK() OVER (ORDER BY SUM(r.token_price * r.token_amount) / SUM(r.token_amount) * 1000000000 DESC) AS rank,
    CONCAT(
        '<a href="https://axiom.trade/t/',
        r.token_address,
        '/@adamtehc" target="_blank">',
        r.asset,
        '</a>'
    ) AS asset_with_chart,
    CONCAT(
        '<a href="https://axiom.trade/t/',
        r.token_address,
        '/@adamtehc" target="_blank">',
        r.token_address,
        '</a>'
    ) AS token_address_with_chart,
    r.asset,
    SUM(r.token_price * r.token_amount) / SUM(r.token_amount) AS vwap_token_price,
    SUM(r.token_price * r.token_amount) / SUM(r.token_amount) * 1000000000 AS market_cap,
    tc.trade_count,
    r.token_address
FROM
    ranked_prices r
JOIN
    trade_counts tc
ON
    r.token_address = tc.token_address
WHERE
    tc.trade_count >= 100
GROUP BY
    r.token_address, r.asset, tc.trade_count
ORDER BY
    market_cap DESC;

'''