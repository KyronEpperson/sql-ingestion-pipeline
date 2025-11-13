from dune_utils import fetch_dune_data
from datetime import datetime
import os
from dotenv import load_dotenv

def ingest_dune_query(query_id, table_name, database_name):
    load_dotenv()
    api_key = os.getenv("DUNE_API_KEY")
    df = fetch_dune_data(query_id, api_key)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df["source_key"] = f"{database_name}.{table_name}.{timestamp}"
    df["source_date"] = datetime.now().date()

    return df