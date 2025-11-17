from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4840709
table_name = "24_Hour_Trending_on_Raydium_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH stablecoins AS (
    SELECT * FROM (
        VALUES 
            ('So11111111111111111111111111111111111111112'), -- SOL
            ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'), -- USDC
            ('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'), -- USDT
            ('mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So') -- mSOL
    ) AS t(stablecoin_mint)
),

raydium_trades AS (
    SELECT 
        block_time,
        token_bought_mint_address,
        token_sold_mint_address,
        token_bought_symbol,
        token_sold_symbol,
        amount_usd
    FROM dex_solana.trades
    WHERE 
        block_time >= NOW() - INTERVAL '24' HOUR
        AND project = 'raydium'  
),

token_volumes AS (
    SELECT 
        COALESCE(rt.token_bought_mint_address, rt.token_sold_mint_address) AS token_address,
        MAX(COALESCE(rt.token_bought_symbol, rt.token_sold_symbol, 'Unknown')) AS asset_name,
        SUM(rt.amount_usd) AS total_volume_24h
    FROM raydium_trades rt
    GROUP BY COALESCE(rt.token_bought_mint_address, rt.token_sold_mint_address)
),

filtered_tokens AS (
    SELECT * FROM token_volumes
    WHERE token_address NOT IN (SELECT stablecoin_mint FROM stablecoins)
)

SELECT 
    ft.token_address,
    CONCAT(
'<a href="https://axiom.trade/t/',
        ft.token_address,
'/@adamtehc" target="_blank">',
        ft.asset_name,
        '</a>'
    ) AS asset_with_chart,
    CONCAT(
'<a href="https://axiom.trade/t/',
        ft.token_address,
'/@adamtehc" target="_blank">',
        ft.token_address,
        '</a>'
    ) AS token_address_with_chart,
    ft.total_volume_24h
FROM filtered_tokens ft
ORDER BY ft.total_volume_24h DESC  
LIMIT 100;

'''