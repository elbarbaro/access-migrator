import streamlit as st
import re
import pandas as pd
import tempfile
import os
import json
from migration import access_reader, pg_writer, transformer, config as migration_config
import sqlalchemy

st.set_page_config(page_title="Access to PostgreSQL Migrator", layout="wide")

PAGES = {
    "Configuration": "Configure database connections and migration settings.",
    "Migration": "Run the migration process and view logs.",
    "Query": "Query the unified table in PostgreSQL."
}

st.sidebar.title("Navigation")
selection = st.sidebar.radio("Go to", list(PAGES.keys()))

st.title("Access to Barbaro Migrator")

if selection == "Configuration":
    st.header("Configuration")
    st.info(PAGES[selection])
    st.warning("Configuration UI not implemented yet. Please use the Migration page for now.")

elif selection == "Migration":
    st.header("Migration")
    st.info(PAGES[selection])

    with st.form("migration_form"):
        st.subheader("Source: Access Databases (Multiple Years)")
        access_files = st.file_uploader("Upload Access files (.mdb/.accdb) for each year", type=["mdb", "accdb"], accept_multiple_files=True)
        
        # Mostrar las tablas de los archivos cargados
        if access_files:
            st.subheader("Tablas encontradas en los archivos")
            for uploaded_file in access_files:
                # Guardar el archivo temporalmente
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, uploaded_file.name)
                
                try:
                    # Guardar el contenido del archivo
                    with open(temp_path, 'wb') as f:
                        f.write(uploaded_file.getvalue())
                    
                    # Verificar que el archivo existe y tiene contenido
                    if not os.path.exists(temp_path):
                        st.error(f"No se pudo crear el archivo temporal para {uploaded_file.name}")
                        continue
                        
                    acc_conn = access_reader.connect_access_db(temp_path)
                    tables = access_reader.list_tables(acc_conn)
                    year_match = re.search(r'(\d{4})', uploaded_file.name)
                    year = year_match.group(1) if year_match else "N/A"
                    
                    with st.expander(f"📁 {uploaded_file.name} (Año: {year})"):
                        for table in tables:
                            st.write(f"• {table}")
                    
                    acc_conn.close()
                except Exception as e:
                    st.error(f"Error al leer {uploaded_file.name}: {e}")
                finally:
                    # Limpiar el archivo temporal
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception as e:
                        st.warning(f"No se pudo eliminar el archivo temporal {temp_path}: {e}")
        
        st.subheader("Target: PostgreSQL")
        pg_host = st.text_input("PostgreSQL Host", value="localhost")
        pg_port = st.number_input("Port", value=5432)
        pg_db = st.text_input("Database Name")
        pg_user = st.text_input("User")
        pg_pass = st.text_input("Password", type="password")
        
        st.subheader("Transformation Rules (JSON, applies to all tables)")
        rules_json = st.text_area("Rules (e.g. {\"col1\": {\"trim\": true}})", value="{}")
        submitted = st.form_submit_button("Run Migration")

    if submitted:
        st.write("---")
        st.write("**Migration Log:**")
        log = st.empty()
        try:
            rules = {}
            if rules_json.strip():
                rules = json.loads(rules_json)
        except Exception as e:
            st.error(f"Invalid transformation rules: {e}")
            st.stop()
        if not access_files:
            st.error("Please upload at least one Access file.")
            st.stop()
        try:
            log.text("Connecting to PostgreSQL...")
            pg_engine = pg_writer.connect_postgres(pg_user, pg_pass, pg_host, pg_port, pg_db)
            
            # Diccionario para almacenar los esquemas de las tablas
            table_schemas = {}
            
            for i, uploaded_file in enumerate(access_files):
                # Guardar el archivo temporalmente
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, uploaded_file.name)
                
                try:
                    # Guardar el contenido del archivo
                    with open(temp_path, 'wb') as f:
                        f.write(uploaded_file.getvalue())
                    
                    # Verificar que el archivo existe y tiene contenido
                    if not os.path.exists(temp_path):
                        st.error(f"No se pudo crear el archivo temporal para {uploaded_file.name}")
                        continue
                        
                    year_match = re.search(r'(\d{4})', uploaded_file.name)
                    if not year_match:
                        st.warning(f"Could not extract year from filename: {uploaded_file.name}")
                        continue
                    year = int(year_match.group(1))
                    log.text(f"[{uploaded_file.name}] Opening Access DB for year {year}...")
                    acc_conn = access_reader.connect_access_db(temp_path)
                    tables = access_reader.list_tables(acc_conn)
                    
                    # Procesar cada tabla encontrada
                    for table_name in tables:
                        log.text(f"[{uploaded_file.name}] Processing table: {table_name}")
                        
                        # Leer la tabla
                        df = access_reader.read_table(acc_conn, table_name)
                        df['year'] = year
                        
                        # Aplicar transformaciones
                        df = transformer.apply_transformations(df, rules)
                        
                        # Obtener el esquema si no lo tenemos
                        if table_name not in table_schemas:
                            table_schemas[table_name] = access_reader.get_table_schema(acc_conn, table_name)
                        
                        # Crear tabla unificada si no existe
                        unified_table_name = f"{table_name}_unified"
                        pg_writer.create_unified_table(pg_engine, unified_table_name, table_schemas[table_name], year_col='year')
                        
                        # Insertar datos
                        pg_writer.insert_dataframe(pg_engine, unified_table_name, df)
                        log.text(f"[{uploaded_file.name}] Inserted {len(df)} rows into {unified_table_name}.")
                    
                    acc_conn.close()
                except Exception as e:
                    st.error(f"Error al procesar {uploaded_file.name}: {e}")
                finally:
                    # Limpiar el archivo temporal
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception as e:
                        st.warning(f"No se pudo eliminar el archivo temporal {temp_path}: {e}")
            
            log.text("Migration complete!")
            st.success("Migration complete!")
        except Exception as e:
            st.error(f"Migration failed: {e}")

elif selection == "Query":
    st.header("Query")
    st.info(PAGES[selection])

    st.subheader("PostgreSQL Connection")
    pg_host = st.text_input("PostgreSQL Host", value="localhost", key="query_pg_host")
    pg_port = st.number_input("Port", value=5432, key="query_pg_port")
    pg_db = st.text_input("Database Name", key="query_pg_db")
    pg_user = st.text_input("User", key="query_pg_user")
    pg_pass = st.text_input("Password", type="password", key="query_pg_pass")

    st.subheader("SQL Query")
    default_query = "SELECT * FROM students_unified LIMIT 100"
    query = st.text_area("Enter SQL query", value=default_query, height=100)
    run_query = st.button("Run Query")

    if run_query:
        try:
            # Conectar a PostgreSQL
            engine = pg_writer.connect_postgres(pg_user, pg_pass, pg_host, pg_port, pg_db)
            
            # Ejecutar la consulta
            with engine.connect() as conn:
                # Usar text() para la consulta SQL
                result = conn.execute(sqlalchemy.text(query))
                # Convertir el resultado a DataFrame
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            # Mostrar los resultados
            st.dataframe(df)
            st.success(f"Returned {len(df)} rows.")
            
        except Exception as e:
            st.error(f"Query failed: {str(e)}")
            st.error("Error details:", e) 