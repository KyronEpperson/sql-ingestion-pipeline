from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3130352
table_name = "Solana_DEX_Trade_Source_120D"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
with 
    jupiter_invoked as (
        SELECT 
            distinct tx_id
        FROM solana.instruction_calls
        WHERE executing_account like '%JUP%'
        and block_time > now() - interval '120' day
    )
    
    , bots as (
        SELECT * FROM dune.dune.result_solana_dex_bot_detection --https://dune.com/queries/3797652
    )
    

SELECT 
    date_trunc('week', block_time) as time
    , case when trade_source like '%JUP%' OR ji.tx_id is not null then '♻️ Jupiter'
        when trade_source = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH' then '♻️ Drift'
        when trade_source = 'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS' then '🎯 raydium'
        -- when trade_source != 'direct' and p.namespace is not null then p.namespace
        when trade_source != 'direct' then '♻️ Other'
        else '🎯 ' || project
        end as trade_source
    , sum(amount_usd) as amount_usd
    , sum(case when bots.signer is null and trade_source 
            NOT IN ('2nAAsYdXF3eTQzaeUQS3fr4o782dDg8L28mX39Wr5j8N'
                    ,'Bt2WPMmbwHPk36i4CRucNDyLcmoGdC7xEdrVuxgJaNE6'
                    ,'92J8nGdH9h6QNiZV35nJjqGMAGF9s2xjZ6AyJR7crf3Q' --arcs
                    ) 
            then amount_usd else null 
            end) as amount_usd_organic
FROM dex_solana.trades tr 
-- LEFT JOIN solana.programs p ON p.program_id = tr.trade_source
LEFT JOIN jupiter_invoked ji ON ji.tx_id = tr.tx_id
LEFT JOIN bots ON bots.signer = tr.trader_id
WHERE block_time > now() - interval '120' day
AND token_bought_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi' and token_sold_mint_address != '4PfN9GDeF9yQ37qt9xCPsQ89qktp1skXfbsZ5Azk82Xi'
group by 1, 2
HAVING sum(amount_usd) > 500000
'''