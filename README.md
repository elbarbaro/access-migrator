# Migrator App

A Streamlit-based application to migrate and consolidate tables from Microsoft Access to PostgreSQL, with data transformation and query capabilities.

## Features
- Connect to Access and PostgreSQL databases
- Discover and merge year-based tables (e.g., subjects_2015, subjects_2016, ...)
- Add a `year` column to the unified table
- Apply configurable data transformation rules
- Query the unified table from the UI

## Setup
### Using [uv](https://github.com/astral-sh/uv) for Dependency & Virtual Environment Management
1. **Install uv** (if not already):
   ```bash
   pip install uv
   ```
2. **Create and activate a virtual environment:**
   ```bash
   uv venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. **Install dependencies:**
   ```bash
   uv pip install -r requirements.txt
   ```
4. **Run the app:**
   ```bash
   streamlit run app.py
   ```

## Configuration
- You will need an Access ODBC driver installed on your system.
- Provide your Access file path and PostgreSQL credentials in the app UI. 