from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 6162721
table_name = "NEAR_Total_generated_fees_by_channel"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with t1 as (
select
date_at,
'protocol fee' as referral,
cast(amount_fee as double) as fee
from dune.near.dataset_near_intents_protocol_fees 

UNION ALL

select
date_at,
referral,
cast(fee as double) as fee
from dune.near.dataset_near_intents_fees
WHERE CAST(from_iso8601_timestamp(date_at) AS DATE)<current_date
)

select
referral,
sum(cast(fee as double)) as fees
from t1
where cast(fee as double)>0 and referral!=''
group by referral
having sum(cast(fee as double))>10
order by fees desc
limit 100
'''