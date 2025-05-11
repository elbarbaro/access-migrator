import sqlalchemy
import pandas as pd
from sqlalchemy.engine import Engine
from typing import List, Tuple

def connect_postgres(user: str, password: str, host: str, port: int, dbname: str) -> Engine:
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    engine = sqlalchemy.create_engine(url)
    return engine

def create_unified_table(engine: Engine, table_name: str, schema: List[Tuple[str, str]], year_col: str = 'year'):
    # schema: list of (col_name, col_type)
    type_map = {
        'VARCHAR': 'TEXT',
        'CHAR': 'TEXT',
        'TEXT': 'TEXT',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'DOUBLE PRECISION',
        'DATETIME': 'TIMESTAMP',
        'DATE': 'DATE',
        'BIT': 'BOOLEAN',
        # Add more as needed
    }
    cols = []
    for col, typ in schema:
        pg_type = type_map.get(typ.upper(), 'TEXT')
        cols.append(f'"{col}" {pg_type}')
    cols.append(f'"{year_col}" INTEGER')
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(create_sql))

def insert_dataframe(engine: Engine, table_name: str, df: pd.DataFrame):
    df.to_sql(table_name, engine, if_exists='append', index=False) 