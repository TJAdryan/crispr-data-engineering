import os
import duckdb
from dagster import asset, Output, Definitions, AssetSelection, SensorEvaluationContext, RunRequest, sensor
from dotenv import load_dotenv

load_dotenv()

@asset
def gene_effects_table():
    """
    Transforms raw CRISPR CSV data into an indexed Postgres table.
    """
    csv_path = os.getenv("CRISPR_CSV_PATH")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_db = os.getenv("POSTGRES_DB", "crispr_db")
    pg_host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    
    pg_conn = f"host={pg_host} user={pg_user} password={pg_password} dbname={pg_db}"
    con = duckdb.connect()
    
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

    # Defensive cleaning
    con.execute("DROP INDEX IF EXISTS pg.idx_gene_symbol;")
    con.execute("DROP TABLE IF EXISTS pg.gene_effects CASCADE;")

    con.execute("BEGIN TRANSACTION;")
    
    # Transformation
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

    # Indexing
    con.execute("CREATE INDEX idx_gene_symbol ON pg.gene_effects(gene_symbol);")
    
    con.execute("COMMIT;")
    con.close()
    
    return Output(
        value=None, 
        metadata={"row_count": 21093758, "status": "Green"}
    )

@sensor(target=AssetSelection.assets(gene_effects_table))
def watch_crispr_csv(context: SensorEvaluationContext):
    """
    Monitors the CSV file for changes via timestamp.
    """
    csv_path = os.getenv("CRISPR_CSV_PATH")
    if not os.path.exists(csv_path):
        return

    # Using the file's last modified time as the unique cursor
    last_modified = str(os.path.getmtime(csv_path))
    
    if context.cursor != last_modified:
        context.update_cursor(last_modified)
        yield RunRequest(
            run_key=last_modified,
            message=f"Detected change in {os.path.basename(csv_path)}."
        )

# This is the "Glue" that fixes the LoadError
# It explicitly defines what Dagster should load
defs = Definitions(
    assets=[gene_effects_table],
    sensors=[watch_crispr_csv],
)