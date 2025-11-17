from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 3253229
table_name = "History_of_Narratives"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

#commented out SQL query for reference
'''
WITH narrative_performance AS (
    SELECT main.month, "Narrative", "Monthly Performance (%)"
    FROM (
        SELECT month, 'Bitcoin narrative' AS "Narrative", "Monthly Performance (%)" FROM query_3312019
        UNION ALL
        SELECT month, 'L1' AS "Narrative", "Monthly Performance (%)" FROM query_3249148
        UNION ALL
        SELECT month, 'Appchain/Modular' AS "Narrative", "Monthly Performance (%)" FROM query_3287852
        UNION ALL 
        SELECT month, 'L2' AS "Narrative", "Monthly Performance (%)" FROM query_3249171
        UNION ALL 
        SELECT month, 'DeFi 1.0' AS "Narrative", "Monthly Performance (%)" FROM query_3249203
        UNION ALL 
        SELECT month, 'DeFi 2.0' AS "Narrative", "Monthly Performance (%)" FROM query_3250678
        UNION ALL 
        SELECT month, 'Money Markets' AS "Narrative", "Monthly Performance (%)" FROM query_3250910
        UNION ALL 
        SELECT month, 'AI' AS "Narrative", "Monthly Performance (%)" FROM query_3249218
        UNION ALL 
        SELECT month, 'LSD L1' AS "Narrative", "Monthly Performance (%)" FROM query_3249227
        UNION ALL 
        SELECT month, 'LSD L2' AS "Narrative", "Monthly Performance (%)" FROM query_3249236
        UNION ALL 
        SELECT month, 'Real Yield' AS "Narrative", "Monthly Performance (%)" FROM query_3249240
        UNION ALL 
        SELECT month, 'Memes' AS "Narrative", "Monthly Performance (%)" FROM query_3249258
        -- UNION ALL 
        -- SELECT month, 'Gamble-Fi' AS "Narrative", "Monthly Performance (%)" FROM query_3249266
        UNION ALL 
        SELECT month, 'Game-Fi' AS "Narrative", "Monthly Performance (%)" FROM query_3249270
        UNION ALL 
        SELECT month, 'DEX' AS "Narrative", "Monthly Performance (%)" FROM query_3249313
        UNION ALL 
        SELECT month, 'Perps' AS "Narrative", "Monthly Performance (%)" FROM query_3249329
        UNION ALL 
        SELECT month, 'Options' AS "Narrative", "Monthly Performance (%)" FROM query_3249337
        UNION ALL 
        SELECT month, 'RWA' AS "Narrative", "Monthly Performance (%)" FROM query_3249341
        UNION ALL 
        SELECT month, 'Oracles' AS "Narrative", "Monthly Performance (%)" FROM query_3249342
        -- UNION ALL 
        -- SELECT month, 'Telegram Bots' AS "Narrative", "Monthly Performance (%)" FROM query_3249344
        UNION ALL 
        SELECT month, 'DePIN' AS "Narrative", "Monthly Performance (%)" FROM query_3249349
        UNION ALL 
        SELECT month, 'DeSCI' AS "Narrative", "Monthly Performance (%)" FROM query_3256607
        UNION ALL 
        SELECT month, 'Wallets' AS "Narrative", "Monthly Performance (%)" FROM query_3249353
        UNION ALL 
        SELECT month, 'Account Abstraction' AS "Narrative", "Monthly Performance (%)" FROM query_3249355
        UNION ALL 
        SELECT month, 'NFTs' AS "Narrative", "Monthly Performance (%)" FROM query_3254585
        UNION ALL 
        SELECT month, 'LRT' AS "Narrative", "Monthly Performance (%)" FROM query_3314997
        UNION ALL
        SELECT month, 'Privacy' AS "Narrative", "Monthly Performance (%)" FROM query_3332011
        UNION ALL 
        SELECT month, 'Ordinals' AS "Narrative", "Monthly Performance (%)" FROM query_3357674
        UNION ALL 
        SELECT month, 'Identity' AS "Narrative", "Monthly Performance (%)" FROM query_3449811
        UNION ALL 
        SELECT month, 'CEX' AS "Narrative", "Monthly Performance (%)" FROM query_3486067
    ) AS main
), ranked_narratives AS (
    SELECT 
         month,
        "Narrative",
        "Monthly Performance (%)",
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY "Monthly Performance (%)" DESC) AS best_rank,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY "Monthly Performance (%)" ASC) AS worst_rank,
        AVG("Monthly Performance (%)") OVER (PARTITION BY month) AS avg_monthly_performance
    FROM narrative_performance
    WHERE month >=  DATE'2020-05-01'
) 
SELECT
    RANK() OVER (ORDER BY AVG(avg_monthly_performance) FILTER (WHERE best_rank = 1) DESC) AS "Overall Month Rank",
    TO_CHAR(month, 'yyyy-mm') AS date_str,
    month as date,
    AVG(avg_monthly_performance) FILTER (WHERE best_rank = 1) AS "Overall Avg",
    -- Best narratives
    MAX("Narrative") FILTER (WHERE best_rank = 1) AS "1st Best Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE best_rank = 1) AS "1st Best Narrative Performance %",
    MAX("Narrative") FILTER (WHERE best_rank = 2) AS "2nd Best Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE best_rank = 2) AS "2nd Best Narrative Performance %",
    MAX("Narrative") FILTER (WHERE best_rank = 3) AS "3rd Best Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE best_rank = 3) AS "3rd Best Narrative Performance %",
    MAX("Narrative") FILTER (WHERE best_rank = 4) AS "4th Best Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE best_rank = 4) AS "4th Best Narrative Performance %",
    MAX("Narrative") FILTER (WHERE best_rank = 5) AS "5th Best Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE best_rank = 5) AS "5th Best Narrative Performance %",
    
    -- Worst narratives
    MAX("Narrative") FILTER (WHERE worst_rank = 1) AS "1st Worst Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE worst_rank = 1) AS "1st Worst Narrative Performance %",
    MAX("Narrative") FILTER (WHERE worst_rank = 2) AS "2nd Worst Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE worst_rank = 2) AS "2nd Worst Narrative Performance %",
    MAX("Narrative") FILTER (WHERE worst_rank = 3) AS "3rd Worst Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE worst_rank = 3) AS "3rd Worst Narrative Performance %",
    MAX("Narrative") FILTER (WHERE worst_rank = 4) AS "4th Worst Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE worst_rank = 4) AS "4th Worst Narrative Performance %",
    MAX("Narrative") FILTER (WHERE worst_rank = 5) AS "5th Worst Narrative",
    MAX("Monthly Performance (%)") FILTER (WHERE worst_rank = 5) AS "5th Worst Narrative Performance %"
FROM ranked_narratives
WHERE month <> DATE_TRUNC('month', NOW()) 
GROUP BY 2,3
ORDER BY 3 DESC
'''