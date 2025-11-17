from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4830187
table_name = "Telegram_Bots_Top_Traded_Tokens"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH relevant_transfers AS (
    SELECT DISTINCT tx_id
    FROM tokens_solana.transfers
    WHERE to_owner IN (
        '47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF', -- Banana Gun
        '4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7', -- Banana Gun
        '8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2', -- Banana Gun
        '9cSuF94JWPb1HQzWMcifJzkoggwAtfjsojcUqny5XuJy', -- Shuriken
        '9yMwSPk9mrXSN7yDHUuZurAh1sjbJsfpUqjZ7SvVtdco', -- Trojan
        'G9PhF9C9H83mAjjkdJz4MDqkufiTPMJkx7TnKE1kFyCp', -- Pepe Boost
        'F34kcgMgCF7mYWkwLN3WN7KrFprr2NbwxuLvXx4fbztj', -- Sol Trading Bot
        'K1LRSA1DSoKBtC5DkcvnermRQ62YxogWSCZZPWQrdG5', -- Sol Trading Bot
        'HEPL5rTb6n1Ax6jt9z2XMPFJcDe9bSWvWQpsK7AMcbZg', -- Sol Trading Bot
        '96aFQc9qyqpjMfqdUeurZVYRrrwPJG2uPV6pceu4B1yb', -- Sol Trading Bot
        'FRMxAnZgkW58zbYcE7Bxqsg99VWpJh6sMP5xLzAWNabN' -- Maestro
    )
    AND block_time >= NOW() - INTERVAL '12' HOUR
),
filtered_trades AS (
    SELECT
        dt.tx_id,
        dt.block_time,
        dt.token_bought_symbol,
        dt.token_bought_mint_address,
        dt.token_bought_amount,
        dt.amount_usd
    FROM dex_solana.trades dt
    JOIN relevant_transfers rt ON dt.tx_id = rt.tx_id
    WHERE dt.token_bought_symbol NOT IN ('USDC', 'SOL', '') 
),
mostTradedTokens AS (
    SELECT
        ft.token_bought_symbol AS token_symbol,
        ft.token_bought_mint_address AS token_mint_address,
        SUM(ft.token_bought_amount) AS total_token_volume,
        SUM(ft.amount_usd) AS total_volume_usd,
        COUNT(*) AS total_trades
    FROM filtered_trades ft
    GROUP BY ft.token_bought_symbol, ft.token_bought_mint_address
)
SELECT
    RANK() OVER (ORDER BY total_volume_usd DESC) AS rank,
    CONCAT(
        '<a href="https://axiom.trade/t/', 
        token_mint_address,
        '/@adamtehc" target="_blank">',
        token_symbol,
        '</a>'
    ) AS token_link,
    total_volume_usd,
    total_token_volume,
    total_trades,
    token_mint_address,
    CONCAT(
        '<a href="https://axiom.trade/t/', 
        token_mint_address,
        '/@adamtehc" target="_blank">',
        token_mint_address,
        '</a>'
    ) AS contract_address_link
FROM mostTradedTokens
ORDER BY total_volume_usd DESC
LIMIT 100;

'''