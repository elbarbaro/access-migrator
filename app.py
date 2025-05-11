import streamlit as st
import re
import pandas as pd
import tempfile
import os
from migration import access_reader, pg_writer, transformer, config as migration_config

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
        
        st.subheader("Target: PostgreSQL")
        pg_host = st.text_input("PostgreSQL Host", value="localhost")
        pg_port = st.number_input("Port", value=5432)
        pg_db = st.text_input("Database Name")
        pg_user = st.text_input("User")
        pg_pass = st.text_input("Password", type="password")
        subjects_unified = st.text_input("Unified Subjects Table Name", value="subjects_unified")
        professors_unified = st.text_input("Unified Professors Table Name", value="professors_unified")

        st.subheader("Transformation Rules (JSON, applies to both tables)")
        rules_json = st.text_area("Rules (e.g. {\"col1\": {\"trim\": true}})", value="{}")
        submitted = st.form_submit_button("Run Migration")

    if submitted:
        st.write("---")
        st.write("**Migration Log:**")
        log = st.empty()
        try:
            rules = {}
            if rules_json.strip():
                rules = pd.io.json.loads(rules_json)
        except Exception as e:
            st.error(f"Invalid transformation rules: {e}")
            st.stop()
        if not access_files:
            st.error("Please upload at least one Access file.")
            st.stop()
        try:
            log.text("Connecting to PostgreSQL...")
            pg_engine = pg_writer.connect_postgres(pg_user, pg_pass, pg_host, pg_port, pg_db)
            subjects_schema = None
            professors_schema = None
            for i, uploaded_file in enumerate(access_files):
                # Save uploaded file to a temp file
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
                    tmp.write(uploaded_file.read())
                    tmp_path = tmp.name
                # Extract year from filename (e.g., db_2015.accdb -> 2015)
                year_match = re.search(r'(\d{4})', uploaded_file.name)
                if not year_match:
                    st.warning(f"Could not extract year from filename: {uploaded_file.name}")
                    os.unlink(tmp_path)
                    continue
                year = int(year_match.group(1))
                log.text(f"[{uploaded_file.name}] Opening Access DB for year {year}...")
                acc_conn = access_reader.connect_access_db(tmp_path)
                tables = access_reader.list_tables(acc_conn)
                for table_type, unified_table in [("Subjects", subjects_unified), ("Professors", professors_unified)]:
                    if table_type in tables:
                        log.text(f"[{uploaded_file.name}] Processing table: {table_type}")
                        df = access_reader.read_table(acc_conn, table_type)
                        df['year'] = year
                        df = transformer.apply_transformations(df, rules)
                        # Get schema from first occurrence
                        if table_type == "Subjects" and subjects_schema is None:
                            subjects_schema = access_reader.get_table_schema(acc_conn, table_type)
                        if table_type == "Professors" and professors_schema is None:
                            professors_schema = access_reader.get_table_schema(acc_conn, table_type)
                        # Create unified table if not exists
                        schema = subjects_schema if table_type == "Subjects" else professors_schema
                        pg_writer.create_unified_table(pg_engine, unified_table, schema, year_col='year')
                        pg_writer.insert_dataframe(pg_engine, unified_table, df)
                        log.text(f"[{uploaded_file.name}] Inserted {len(df)} rows into {unified_table}.")
                    else:
                        log.text(f"[{uploaded_file.name}] Table {table_type} not found.")
                acc_conn.close()
                os.unlink(tmp_path)
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
    default_query = "SELECT * FROM subjects_unified LIMIT 100"
    query = st.text_area("Enter SQL query", value=default_query, height=100)
    run_query = st.button("Run Query")

    if run_query:
        try:
            engine = pg_writer.connect_postgres(pg_user, pg_pass, pg_host, pg_port, pg_db)
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
            st.dataframe(df)
            st.success(f"Returned {len(df)} rows.")
        except Exception as e:
            st.error(f"Query failed: {e}") 