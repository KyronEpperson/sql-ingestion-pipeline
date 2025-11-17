from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3084533
table_name = "All_Chains_DEX_Volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
SELECT 
    cast(date_trunc('week', block_time) as timestamp) week
    , tr.blockchain
    , sum(amount_usd) as volume_usd
    , count(distinct taker) as traders
FROM dex.trades tr
WHERE block_time > timestamp '{{start date}}'
and tr.blockchain != 'solana'
group by 1,2

UNION ALL 

SELECT 
    cast(date_trunc('week', block_time) as timestamp) week
    , tr.blockchain
    , sum(amount_usd) as volume_usd
    , count(distinct trader_id) as traders
FROM dex_solana.trades tr
WHERE block_time > timestamp '{{start date}}'
AND token_bought_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi' and token_sold_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi'
group by 1,2
'''