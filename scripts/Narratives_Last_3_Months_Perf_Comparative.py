from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3318044
table_name = "Narratives_Last_3_Months_Perf_Comparative"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''
SELECT  
      narrative
    , mtd_perf as quarter_perf --TODO: Rename all mtd to quarter
    , APPROX_PERCENTILE(mtd_perf,0.5) OVER() *100 AS avg_quarter_perf_across_all_narratives
FROM
(
    -- SELECT 'LRT' AS narrative,  AVG("$ YTD (%)") as ytd_perf FROM query_3314935
    -- UNION ALL
    -- SELECT 'Bitcoin Narrative' AS narrative, AVG("$ YTD (%)") AS ytd_perf FROM query_3312009
    -- UNION ALL
    SELECT 'L1s' AS narrative,  AVG("$ MTD (%)") as mtd_perf FROM query_3317408
    UNION ALL 
    SELECT 'L2s' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317416
    UNION ALL 
    SELECT 'DeFi 1.0' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317867
    UNION ALL 
    SELECT 'DeFi 2.0' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317858
    UNION ALL 
    -- SELECT 'Money Markets' AS narrative,  AVG("$ YTD (%)")  as ytd_perf FROM query_3250906
    -- UNION ALL 
    SELECT 'AI' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317433
    UNION ALL 
    -- SELECT 'LSD L1' AS narrative,  AVG("$ YTD (%)")  as ytd_perf FROM query_3234777
    -- UNION ALL 
    -- SELECT 'LSD L2' AS narrative,  AVG("$ YTD (%)")  as ytd_perf FROM query_3241718
    -- UNION ALL 
    SELECT 'Real Yield' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317876
    UNION ALL 
    SELECT 'Memes' AS narrative, AVG("$ MTD (%)")  as mtd_perf FROM query_3317431
    --UNION ALL 
    --SELECT 'Gamble-Fi' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317436
    UNION ALL 
    SELECT 'Game-Fi' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317419
    UNION ALL 
    -- SELECT 'DEX' AS narrative,  AVG("$ YTD (%)")  as ytd_perf FROM query_3234839
    -- UNION ALL 
    -- SELECT 'Perps Derivatives' AS narrative,  AVG("$ YTD (%)")  as ytd_perf FROM query_3235633
    -- UNION ALL 
    -- SELECT 'Options' AS narrative,  AVG("$ YTD (%)")  as ytd_perf FROM query_3235663
    -- UNION ALL 
    SELECT 'RWA' AS narrative,  AVG("$ MTD (%)")  as mtd_perf FROM query_3317860
    UNION ALL 
    SELECT 'Oracles' AS narrative,  AVG("$ MTD (%)") as mtd_perf FROM query_3317447
    -- UNION ALL 
    -- SELECT 'Telegram Bots' AS narrative,   AVG("$ YTD (%)") as ytd_perf FROM query_3235698
    -- UNION ALL 
    -- SELECT 'DePIN' AS narrative, AVG("$ YTD (%)") as ytd_perf FROM query_3240471
    -- UNION ALL 
    -- SELECT 'DeSCI' AS narrative, AVG("$ YTD (%)") as ytd_perf FROM query_3256523
    -- UNION ALL 
    -- SELECT 'Wallets' AS narrative,  AVG("$ YTD (%)") as ytd_perf FROM query_3236506
    -- UNION ALL 
    -- SELECT 'Account Abstractions' AS narrative,  AVG("$ YTD (%)") as ytd_perf FROM query_3242713
    -- UNION ALL 
    -- SELECT 'NFTs' AS narrative,  AVG("$ YTD (%)") as ytd_perf FROM query_3235002
    -- UNION ALL
    -- SELECT 'Appchain/Modular' AS narrative, AVG("$ YTD (%)") AS ytd_perf FROM query_3287674
    UNION ALL
    SELECT 'Privacy' AS narrative, AVG("$ MTD (%)") AS ytd_perf FROM query_3332006
    UNION ALL
    SELECT 'CEX' AS narrative, AVG("$ MTD (%)") AS ytd_perf FROM query_3486061
)
ORDER BY 2 DESC
'''