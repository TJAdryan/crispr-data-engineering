from dagster import asset, Definitions
import duckdb
import os

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
    pg_conn = "host=localhost user=postgres password=self_assured_complexity dbname=crispr_db"
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

# This defines the scope of your project for the Dagster UI
defs = Definitions(
    assets=[gene_effects_postgres],
)