from ingest_dune_data import ingest_dune_query
from load_to_sql import load_dataframe_to_sql
from datetime import datetime

query_id = 3970986
table_name = "blockchain_metrics"
database_name = "dune_metrics"
# Construct timestamp and source_key
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
source_key = f"{database_name}.{table_name}.{timestamp}"

#initialize dataframe to ingest
df = ingest_dune_query(query_id, table_name, database_name)

# Add metadata to DataFrame
#df["source_key"] = source_key
#df["source_date"] = datetime.now().date()

load_dataframe_to_sql(df, table_name)