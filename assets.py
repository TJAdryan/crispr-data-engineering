import os
import duckdb
from dagster import asset, Output
from dotenv import load_dotenv

load_dotenv()

@asset
def gene_effects_table():
    # 1. Configuration
    csv_path = os.getenv("CRISPR_CSV_PATH")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_db = os.getenv("POSTGRES_DB", "crispr_db")
    pg_host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    
    # 2. Connection
    pg_conn = f"host={pg_host} user={pg_user} password={pg_password} dbname={pg_db}"
    con = duckdb.connect()
    
  # 3. Initialization
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

    # 4. Clean the Registry (Pre-Transaction)
    # We do this OUTSIDE the transaction to force Postgres to release the index name
    print("Pre-clearing index and table...")
    con.execute("DROP INDEX IF EXISTS pg.idx_gene_symbol;")
    con.execute("DROP TABLE IF EXISTS pg.gene_effects CASCADE;")

    # 5. Transactional Block
    con.execute("BEGIN TRANSACTION;")
    
    print(f"Loading 21M rows from {csv_path}...")
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

    print("Building search index...")
    con.execute("CREATE INDEX idx_gene_symbol ON pg.gene_effects(gene_symbol);")
    
    con.execute("COMMIT;")
    con.close()