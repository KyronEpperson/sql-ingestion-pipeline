from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def get_sql_engine():
    # Use Windows Authentication
    conn_str = (
        "mssql+pyodbc://@DESKTOP-U6HGS39\SQLEXPRESS?"
        "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes&database=dune_metrics"
    )
    # Or use SQL Authentication (uncomment and edit below)
    # conn_str = (
    #     "mssql+pyodbc://PLATO\epper:StrongPassword123!@PLATO/MSSQLSERVER?"
    #     "driver=ODBC+Driver+17+for+SQL+Server"
    # )

    return create_engine(conn_str)