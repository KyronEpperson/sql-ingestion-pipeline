from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4061600
table_name = "Polymarket_User_Segments_by_Average_Bet_Size"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''WITH NegRisk AS (
    SELECT
        takerOrderMaker,
        COUNT(evt_tx_hash) AS negrisk_txns,
        AVG(makerAmountFilled/1e6) AS avg_negrisk_amount
    FROM polymarket_polygon.NegRiskCtfExchange_evt_OrdersMatched
    GROUP BY takerOrderMaker
),
CTF AS (
    SELECT
        takerOrderMaker,
        COUNT(evt_tx_hash) AS ctf_txns,
        AVG(makerAmountFilled/1e6) AS avg_ctf_amount
    FROM polymarket_polygon.CTFExchange_evt_OrdersMatched
    GROUP BY takerOrderMaker
),
CombinedData AS (
    SELECT
        COALESCE(n.takerOrderMaker, c.takerOrderMaker) AS takerOrderMaker,
        COALESCE(n.negrisk_txns, 0) AS negrisk_txns,
        COALESCE(n.avg_negrisk_amount, 0) AS avg_negrisk_amount,
        COALESCE(c.ctf_txns, 0) AS ctf_txns,
        COALESCE(c.avg_ctf_amount, 0) AS avg_ctf_amount,
        -- Calculate the weighted average bet size
        (COALESCE(n.negrisk_txns, 0) * COALESCE(n.avg_negrisk_amount, 0) + COALESCE(c.ctf_txns, 0) * COALESCE(c.avg_ctf_amount, 0)) 
        / NULLIF((COALESCE(n.negrisk_txns, 0) + COALESCE(c.ctf_txns, 0)), 0) AS weighted_avg_bet
    FROM NegRisk n
    FULL JOIN CTF c ON n.takerOrderMaker = c.takerOrderMaker
),
SegmentedUsers AS (
    SELECT
        takerOrderMaker,
        weighted_avg_bet,
        CASE
            WHEN weighted_avg_bet <= 10 THEN '$0-$10'
            WHEN weighted_avg_bet > 10 AND weighted_avg_bet <= 50 THEN '$10-$50'
            WHEN weighted_avg_bet > 50 AND weighted_avg_bet <= 100 THEN '$50-$100'
            WHEN weighted_avg_bet > 100 AND weighted_avg_bet <= 500 THEN '$100-$500'
            WHEN weighted_avg_bet > 500 AND weighted_avg_bet <= 1000 THEN '$500-$1,000'
            WHEN weighted_avg_bet > 1000 AND weighted_avg_bet <= 5000 THEN '$1,000-$5,000'
            WHEN weighted_avg_bet > 5000 AND weighted_avg_bet <= 10000 THEN '$5,000-$10,000'
            WHEN weighted_avg_bet > 10000 AND weighted_avg_bet <= 50000 THEN '$10,000-$50,000'
            WHEN weighted_avg_bet > 50000 AND weighted_avg_bet <= 100000 THEN '$50,000-$100,000'
            ELSE '$100,000+'
        END AS bet_size
    FROM CombinedData
),
TotalUsers AS (
    SELECT
        COUNT(takerOrderMaker) AS total_users
    FROM SegmentedUsers
)
SELECT
    s.bet_size,
    COUNT(s.takerOrderMaker) AS user_count,
    ROUND(COUNT(s.takerOrderMaker) * 1.0000 / t.total_users, 4) AS percentage
FROM SegmentedUsers s
CROSS JOIN TotalUsers t
GROUP BY s.bet_size, t.total_users
ORDER BY user_count DESC;
'''