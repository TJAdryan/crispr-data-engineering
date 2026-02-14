from dagster import asset, Definitions
import duckdb
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def _get_pg_conn():
    pg_password = os.environ.get("POSTGRES_PASSWORD")
    if not pg_password:
        raise RuntimeError("POSTGRES_PASSWORD is not set")
    return f"host=localhost user=postgres password={pg_password} dbname=crispr_db"

@asset(group_name="Ingestion")
def gene_effects_postgres():
    """
    The normalized 21M row dataset. 
    Transforming from Wide CSV to Long Postgres format.
    """
    # Using your proven DuckDB logic
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    
    # Connect to your Podman-managed Postgres
    pg_conn = _get_pg_conn()
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")
    
    # The 'Soul' of the transformation
    con.execute("""
        CREATE TABLE IF NOT EXISTS pg.gene_effects AS 
        SELECT "column00000" AS model_id, gene_symbol, dependency_score
        FROM (UNPIVOT (SELECT * FROM read_csv_auto('CRISPRGeneEffect.csv'))
              ON COLUMNS(* EXCLUDE "column00000")
              INTO NAME gene_symbol VALUE dependency_score);
    """)
    return "21 Million Rows Synchronized"

# Project Scope:
defs = Definitions(
    assets=[gene_effects_table],
    asset_checks=[check_no_null_symbols], 
    sensors=[watch_crispr_csv],
)