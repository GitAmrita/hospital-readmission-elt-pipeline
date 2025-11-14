import duckdb

DB_PATH = "ehr.duckdb"
RAW_TABLE = "raw.all_encounters"

# Create DuckDB connection
def get_connection():
    con = duckdb.connect(DB_PATH)
    return con

# Ensure the raw schema exists
def create_raw_schema(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

def create_raw_table_if_not_exists(con, sample_df):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} AS
        SELECT * FROM sample_df LIMIT 0
    """)

