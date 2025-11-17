from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3830470
table_name = "Solana_DEX_New_Tokens_By_Project"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with 
    pairs as (
        SELECT 
            project
            , token_bought_mint_address
            , min(block_time) as first_trade_time
        FROM dex_solana.trades tr
        GROUP BY 1,2
    )

SELECT 
    date_trunc('week',first_trade_time) as week_created
    , project
    , count(*) as pairs
FROM pairs
WHERE first_trade_time > timestamp '{{start date}}'
GROUP BY 1,2
'''