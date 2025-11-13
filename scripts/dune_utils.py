from dune_client.client import DuneClient
import pandas as pd

def fetch_dune_data(query_id: int, api_key: str) -> pd.DataFrame:
    dune = DuneClient(api_key)
    result = dune.get_latest_result(query_id).result
    return pd.DataFrame(result.rows, columns=result.metadata.column_names)