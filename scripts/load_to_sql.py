from sqlalchemy import create_engine
from sql_connection import get_sql_engine

def load_dataframe_to_sql(df, table_name, if_exists="replace"):
    """
    Pushes a DataFrame to SQL Server using the provided table name.
    
    Parameters:
    - df: pandas DataFrame to load
    - table_name: name of the SQL table
    - if_exists: 'replace', 'append', or 'fail'
    """
    engine = get_sql_engine()
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)

    print(f"✅ Loaded DataFrame to SQL table: {table_name}")