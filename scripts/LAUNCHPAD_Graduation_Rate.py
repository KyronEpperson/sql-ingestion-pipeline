from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 5129526
table_name = "LAUNCHPAD_Graduation_Rate"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH launches AS (
    SELECT
        DATE_TRUNC('day', date_time) AS block_date,
        platform,
        SUM(daily_token_count) AS daily_token_launches
    FROM query_4010816
    WHERE platform IN ('Pumpdotfun', 'Boop', 'LetsBonk', 'Bags', 'Wavebreak', 'Believe', 'Sugar')
    GROUP BY 1, 2
),
graduates AS (
    SELECT
        DATE_TRUNC('day', block_date) AS block_date,
        CASE
            WHEN platform = 'Pumpfun' THEN 'Pumpdotfun'
            ELSE platform
        END AS platform,
        SUM(daily_graduates) AS daily_graduations
    FROM query_5131612
    WHERE platform IN ('Pumpfun', 'Boop', 'LetsBonk', 'Bags', 'Wavebreak', 'Believe', 'Sugar')
    GROUP BY 1, 2
),
combined AS (
    SELECT
        l.block_date,
        l.platform,
        COALESCE(l.daily_token_launches, 0) AS daily_token_launches,
        COALESCE(g.daily_graduations, 0) AS daily_graduations
    FROM launches l
    LEFT JOIN graduates g
        ON l.block_date = g.block_date AND l.platform = g.platform
)
SELECT
    block_date,
    platform,
    daily_token_launches,
    daily_graduations,
    CASE 
        WHEN daily_token_launches = 0 THEN NULL
        ELSE 
            CASE 
                WHEN (CAST(daily_graduations AS DOUBLE) / CAST(daily_token_launches AS DOUBLE)) > 0.04 THEN NULL
                ELSE ROUND(CAST(daily_graduations AS DOUBLE) / CAST(daily_token_launches AS DOUBLE), 4)
            END
    END AS graduation_rate
FROM combined
WHERE block_date >= CURRENT_DATE - INTERVAL '27' DAY
ORDER BY block_date DESC, platform;
'''