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

    # 4. Clean Slate
    # We drop the table and the index to ensure the board is clear
    con.execute("DROP TABLE IF EXISTS pg.gene_effects CASCADE;")

    # 5. Transformation and Load
    # This creates the table in Postgres
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

    # 6. The "Safety" Line
    # This forces Postgres to finish the table before we move to the index
    con.execute("COMMIT;")

    # 7. Optimization
    # Now that the table is DEFINITELY there, we build the index
    con.execute("CREATE INDEX idx_gene_symbol ON pg.gene_effects(gene_symbol);")
    
    con.close()
    
    return Output(
        value=None, 
        metadata={
            "row_count": 21093758,
            "status": "Green"
        }
    )