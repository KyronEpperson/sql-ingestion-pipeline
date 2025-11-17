from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime


query_id = 4344649
table_name = "Polymarket_Top_100_unresolved_markets_by_total_bet_volume"
database_name = "DUNE_METRICS"
# Add and Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"
df = ingest_dune_query(query_id, table_name, database_name)
load_dataframe_to_sql(df, table_name)

'''--Error: Unable to fully debug query 
WITH buys AS (
  SELECT
    takerAssetId,
    SUM(makerAmountFilled) / 1e6 AS bet_amount_buy
  FROM TABLE(
    DECODE_EVM_EVENT(
      abi => '{ "anonymous": false, "inputs": [ { "indexed": true, "internalType": "bytes32", "name": "takerOrderHash", "type": "bytes32" }, { "indexed": true, "internalType": "address", "name": "takerOrderMaker", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "makerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "makerAmountFilled", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAmountFilled", "type": "uint256" } ], "name": "OrdersMatched", "type": "event" }',
      input => TABLE(
        SELECT
          *
        FROM polygon.logs
        WHERE
          1 = 1
          AND topic0 = 0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c /* Orders Matched */
      )
    )
  )
  WHERE
    takerAssetId <> 0
  GROUP BY
    1
), sells AS (
  SELECT
    makerAssetId,
    SUM(takerAmountFilled) / 1e6 AS bet_amount_sell
  FROM TABLE(
    DECODE_EVM_EVENT(
      abi => '{ "anonymous": false, "inputs": [ { "indexed": true, "internalType": "bytes32", "name": "takerOrderHash", "type": "bytes32" }, { "indexed": true, "internalType": "address", "name": "takerOrderMaker", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "makerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAssetId", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "makerAmountFilled", "type": "uint256" }, { "indexed": false, "internalType": "uint256", "name": "takerAmountFilled", "type": "uint256" } ], "name": "OrdersMatched", "type": "event" }',
      input => TABLE(
        SELECT
          *
        FROM polygon.logs
        WHERE
          1 = 1
          AND topic0 = 0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c /* Orders Matched */
      )
    )
  )
  WHERE
    makerAssetId <> 0
  GROUP BY
    1
), markets AS (
  SELECT
    token_outcome_name,
    token_id,
    outcome
  FROM polymarket_polygon.market_details
)
SELECT
  b.takerAssetId AS token_id,
  m.token_outcome_name,
  b.bet_amount_buy,
  s.bet_amount_sell,
  b.bet_amount_buy + s.bet_amount_sell AS total_bet
FROM buys AS b
LEFT JOIN sells AS s
  ON b.takerAssetId = s.makerAssetId
LEFT JOIN markets AS m
  ON b.takerAssetId = m.token_id
WHERE
  m.outcome = 'unresolved'
ORDER BY
  total_bet DESC
LIMIT 100
'''