from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3084529
table_name = "Solana_DEX_Project_Stats"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
SELECT
    case when project = 'whirlpool' then 'orca' else project end as project
    , COALESCE(sum(case when block_time > now() - interval '1' day then amount_usd else 0 end),0) as one_day_volume
    , COALESCE(sum(case when block_time > now() - interval '7' day then amount_usd else 0 end),0) as seven_day_volume
    , COALESCE(sum(case when block_time > now() - interval '30' day then amount_usd else 0 end),0) as thirty_day_volume
    , COALESCE(sum(amount_usd),0) as all_time_volume
FROM dex_solana.trades dx
group by 1
order by 2 desc
'''