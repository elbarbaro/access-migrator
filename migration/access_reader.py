import pyodbc
import re
import pandas as pd
from typing import List, Tuple

def connect_access_db(access_file_path: str) -> pyodbc.Connection:
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={access_file_path};"
    )
    return pyodbc.connect(conn_str)

def list_tables(conn: pyodbc.Connection, pattern: str = None) -> List[str]:
    cursor = conn.cursor()
    tables = [row.table_name for row in cursor.tables(tableType='TABLE')]
    if pattern:
        regex = re.compile(pattern)
        tables = [t for t in tables if regex.match(t)]
    return tables

def get_table_schema(conn: pyodbc.Connection, table_name: str) -> List[Tuple[str, str]]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM [{table_name}] WHERE 1=0")
    return [(column[0], column[1]) for column in cursor.description]

def read_table(conn: pyodbc.Connection, table_name: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM [{table_name}]", conn) 