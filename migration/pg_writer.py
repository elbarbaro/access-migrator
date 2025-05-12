import sqlalchemy
import pandas as pd
from sqlalchemy.engine import Engine
from typing import List, Tuple
import re

def to_snake_case(name: str) -> str:
    """Convert a string to snake_case"""
    # Eliminar espacios y caracteres especiales
    name = re.sub(r'[^\w\s]', '', name)
    # Reemplazar espacios con guiones bajos
    name = re.sub(r'\s+', '_', name)
    # Convertir a minúsculas
    name = name.lower()
    # Eliminar guiones bajos múltiples
    name = re.sub(r'_+', '_', name)
    # Eliminar guiones bajos al inicio y final
    name = name.strip('_')
    return name

def connect_postgres(user: str, password: str, host: str, port: int, dbname: str) -> Engine:
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    engine = sqlalchemy.create_engine(url)
    return engine

def create_unified_table(engine: Engine, table_name: str, schema: List[Tuple[str, str]], year_col: str = 'year'):
    # Asegurarnos de que el nombre de la tabla esté en minúsculas y snake_case
    table_name = to_snake_case(table_name)
    
    # schema: list of (col_name, col_type)
    type_map = {
        'VARCHAR': 'TEXT',
        'CHAR': 'TEXT',
        'TEXT': 'TEXT',
        'STRING': 'TEXT',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'LONG': 'INTEGER',
        'FLOAT': 'DOUBLE PRECISION',
        'DOUBLE': 'DOUBLE PRECISION',
        'NUMBER': 'DOUBLE PRECISION',
        'NUMERIC': 'DOUBLE PRECISION',
        'DECIMAL': 'DOUBLE PRECISION',
        'DATETIME': 'TIMESTAMP',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'BIT': 'BOOLEAN',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'YES/NO': 'BOOLEAN',
        'TRUE/FALSE': 'BOOLEAN',
        'CURRENCY': 'DOUBLE PRECISION',
        'MEMO': 'TEXT',
        'OLEOBJECT': 'BYTEA',
        'BINARY': 'BYTEA',
        'VARBINARY': 'BYTEA',
        'IMAGE': 'BYTEA',
        'GUID': 'UUID',
        'UNIQUEIDENTIFIER': 'UUID',
        'XML': 'XML',
        'JSON': 'JSONB',
        'ARRAY': 'TEXT[]',
        'BLOB': 'BYTEA',
        'CLOB': 'TEXT',
        'NCLOB': 'TEXT',
        'ROWID': 'TEXT',
        'UROWID': 'TEXT',
        'RAW': 'BYTEA',
        'LONG RAW': 'BYTEA',
        'BFILE': 'BYTEA',
        'BINARY_FLOAT': 'DOUBLE PRECISION',
        'BINARY_DOUBLE': 'DOUBLE PRECISION',
        'INTERVAL': 'INTERVAL',
        'TIMESTAMP': 'TIMESTAMP',
        'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP WITH TIME ZONE',
        'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMP WITH TIME ZONE',
        'SDO_GEOMETRY': 'GEOMETRY',
        'SDO_TOPO_GEOMETRY': 'GEOMETRY',
        'SDO_GEORASTER': 'BYTEA',
        'SDO_RASTER': 'BYTEA',
        'SDO_TIN': 'BYTEA',
        'SDO_POINT': 'GEOMETRY',
        'SDO_POINT_TYPE': 'GEOMETRY',
        'SDO_ELEM_INFO_ARRAY': 'INTEGER[]',
        'SDO_ORDINATE_ARRAY': 'DOUBLE PRECISION[]',
        'SDO_GEOMETRY_ARRAY': 'GEOMETRY[]',
        'SDO_TOPO_GEOMETRY_ARRAY': 'GEOMETRY[]',
        'SDO_GEORASTER_ARRAY': 'BYTEA[]',
        'SDO_RASTER_ARRAY': 'BYTEA[]',
        'SDO_TIN_ARRAY': 'BYTEA[]',
        'SDO_POINT_ARRAY': 'GEOMETRY[]',
        'SDO_POINT_TYPE_ARRAY': 'GEOMETRY[]',
        'SDO_ELEM_INFO_ARRAY_ARRAY': 'INTEGER[][]',
        'SDO_ORDINATE_ARRAY_ARRAY': 'DOUBLE PRECISION[][]',
        'SDO_GEOMETRY_ARRAY_ARRAY': 'GEOMETRY[][]',
        'SDO_TOPO_GEOMETRY_ARRAY_ARRAY': 'GEOMETRY[][]',
        'SDO_GEORASTER_ARRAY_ARRAY': 'BYTEA[][]',
        'SDO_RASTER_ARRAY_ARRAY': 'BYTEA[][]',
        'SDO_TIN_ARRAY_ARRAY': 'BYTEA[][]',
        'SDO_POINT_ARRAY_ARRAY': 'GEOMETRY[][]',
        'SDO_POINT_TYPE_ARRAY_ARRAY': 'GEOMETRY[][]',
        'SDO_ELEM_INFO_ARRAY_ARRAY_ARRAY': 'INTEGER[][][]',
        'SDO_ORDINATE_ARRAY_ARRAY_ARRAY': 'DOUBLE PRECISION[][][]',
        'SDO_GEOMETRY_ARRAY_ARRAY_ARRAY': 'GEOMETRY[][][]',
        'SDO_TOPO_GEOMETRY_ARRAY_ARRAY_ARRAY': 'GEOMETRY[][][]',
        'SDO_GEORASTER_ARRAY_ARRAY_ARRAY': 'BYTEA[][][]',
        'SDO_RASTER_ARRAY_ARRAY_ARRAY': 'BYTEA[][][]',
        'SDO_TIN_ARRAY_ARRAY_ARRAY': 'BYTEA[][][]',
        'SDO_POINT_ARRAY_ARRAY_ARRAY': 'GEOMETRY[][][]',
        'SDO_POINT_TYPE_ARRAY_ARRAY_ARRAY': 'GEOMETRY[][][]',
    }
    
    cols = []
    for col, typ in schema:
        # Asegurarnos de que typ sea una cadena
        if not isinstance(typ, str):
            typ = str(typ)
            
        # Convertir el tipo a mayúsculas y eliminar espacios
        typ = typ.upper().strip()
        
        # Obtener el tipo de PostgreSQL correspondiente
        pg_type = type_map.get(typ, 'TEXT')
        
        # Convertir el nombre de la columna a snake_case
        col = to_snake_case(col)
        
        # Agregar la columna con su tipo
        cols.append(f'"{col}" {pg_type}')
    
    # Agregar la columna de año
    cols.append(f'"{year_col}" INTEGER')
    
    # Crear la tabla
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'
    
    try:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(create_sql))
            conn.commit()
            print(f"Tabla {table_name} creada exitosamente")
    except Exception as e:
        print(f"Error creando tabla {table_name}: {str(e)}")
        print(f"SQL: {create_sql}")
        raise

def insert_dataframe(engine: Engine, table_name: str, df: pd.DataFrame):
    # Asegurarnos de que el nombre de la tabla esté en minúsculas y snake_case
    table_name = to_snake_case(table_name)
    
    # Convertir los nombres de las columnas a snake_case
    df.columns = [to_snake_case(col) for col in df.columns]
    
    print(f"\n=== Iniciando inserción en tabla {table_name} ===")
    print(f"Número de filas a insertar: {len(df)}")
    print("Columnas en el DataFrame:", df.columns.tolist())
    
    # Crear una copia del DataFrame para no modificar el original
    df = df.copy()
    
    try:
        # Obtener los tipos de datos de las columnas en PostgreSQL
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
            """))
            pg_types = {row[0]: row[1] for row in result}
            print("Tipos de datos en PostgreSQL:", pg_types)
        
        # Convertir los tipos de datos según corresponda
        for col in df.columns:
            if col in pg_types:
                pg_type = pg_types[col]
                try:
                    print(f"\nProcesando columna: {col}")
                    print(f"Tipo en PostgreSQL: {pg_type}")
                    print(f"Tipo actual: {df[col].dtype}")
                    
                    if pg_type == 'integer':
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    elif pg_type == 'double precision':
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    elif pg_type == 'timestamp':
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif pg_type == 'date':
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                    elif pg_type == 'boolean':
                        df[col] = df[col].astype(bool)
                    else:  # Para tipos text y otros
                        df[col] = df[col].astype(str)
                    
                    print(f"Nuevo tipo: {df[col].dtype}")
                except Exception as e:
                    print(f"Error convirtiendo columna {col} a tipo {pg_type}: {str(e)}")
                    # Si hay error en la conversión, convertimos a string
                    df[col] = df[col].astype(str)
        
        # Insertar los datos
        print("\nInsertando datos en la tabla...")
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print("Datos insertados exitosamente")
        
        # Verificar la inserción
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            print(f"Total de registros en la tabla: {count}")
            
    except Exception as e:
        print(f"Error durante la inserción: {str(e)}")
        raise 