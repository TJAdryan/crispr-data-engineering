import duckdb
import os
from dotenv import load_dotenv


"""
ARCHIVED: 2026-02-14
REPLACED BY: assets.py (Dagster Orchestration)

DESCRIPTION:
This was the original standalone ETL script. It used DuckDB to unpivot 21M+ rows 
from the CRISPR Gene Effect CSV and load them into a local Postgres instance.

Reason:
The logic was later refactored into Dagster assets and sensors for better orchestration.
"""

load_dotenv()

def _get_pg_conn():
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_db = os.getenv("POSTGRES_DB", "crispr_db")
    return f"host=127.0.0.1 user={pg_user} password={pg_password} dbname={pg_db}"

def run_transform():
    csv_path = os.getenv("CRISPR_CSV_PATH")
    con = duckdb.connect()
    
    print(f"Starting cleaned unpivot of {csv_path}...")

    pg_conn = _get_pg_conn()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

    # This SQL handles the 21M rows and cleans the gene names
    con.execute(f"""
        CREATE TABLE pg.gene_effects AS 
        SELECT 
            "column00000" AS model_id, 
            split_part(gene_symbol_raw, ' (', 1) AS gene_symbol,
            regexp_extract(gene_symbol_raw, '\\((.*)\\)', 1) AS entrez_id,
            dependency_score
        FROM (
            UNPIVOT (SELECT * FROM read_csv_auto('{csv_path}'))
            ON COLUMNS(* EXCLUDE "column00000")
            INTO NAME gene_symbol_raw VALUE dependency_score
        );
    """)
    print("Success! 21 million rows cleaned and loaded.")

if __name__ == "__main__":
    run_transform()