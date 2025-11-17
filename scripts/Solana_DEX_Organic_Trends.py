from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3131956
table_name = "Solana_DEX_Organic_Trends"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
WITH 
    bots as (
        SELECT * FROM dune.dune.result_solana_dex_bot_detection
    )
    
SELECT 
date_trunc('week',block_time) as week
, case when bots.signer is not null then 'bot'
    else 'person' 
    end as trader_type
, sum(amount_usd) as volume
, count(*) as swaps
, count(distinct trader_id) as traders
FROM dex_solana.trades sp
LEFT JOIN bots ON bots.signer = sp.trader_id
WHERE token_bought_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi' and token_sold_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi'
group by 1,2
'''