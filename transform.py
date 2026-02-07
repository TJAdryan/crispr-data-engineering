import duckdb
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Define your connection to the Podman Postgres instance
# Note: Since it's local, 'localhost' works
pg_password = os.environ.get("POSTGRES_PASSWORD")
if pg_password:
    pg_conn = f"host=localhost user=postgres password={pg_password} dbname=crispr_db"
else:
    pg_conn = "host=localhost user=postgres dbname=crispr_db"

def run_transform():
    con = duckdb.connect()
    print("Connecting to Postgres and unpivoting data...")

    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

    # Your optimized unpivot logic
    con.execute("""
        CREATE TABLE IF NOT EXISTS pg.gene_effects AS 
        SELECT 
            "column00000" AS model_id, 
            gene_symbol, 
            dependency_score
        FROM (
            UNPIVOT (SELECT * FROM read_csv_auto('CRISPRGeneEffect.csv'))
            ON COLUMNS(* EXCLUDE "column00000")
            INTO NAME gene_symbol VALUE dependency_score
        );
    """)
    print("Success! 21 million rows processed.")

if __name__ == "__main__":
    run_transform()
import duckdb
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Define your connection to the Podman Postgres instance
# Note: Since it's local, 'localhost' works
pg_password = os.environ.get("POSTGRES_PASSWORD", "self_assured_complexity")
pg_conn = f"host=localhost user=postgres password={pg_password} dbname=crispr_db"

def run_transform():
    con = duckdb.connect()
    print("Connecting to Postgres and unpivoting data...")

    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

    # Your optimized unpivot logic
    con.execute("""
        CREATE TABLE IF NOT EXISTS pg.gene_effects AS 
        SELECT 
            "column00000" AS model_id, 
            gene_symbol, 
            dependency_score
        FROM (
            UNPIVOT (SELECT * FROM read_csv_auto('CRISPRGeneEffect.csv'))
            ON COLUMNS(* EXCLUDE "column00000")
            INTO NAME gene_symbol VALUE dependency_score
        );
    """)
    print("Success! 21 million rows processed.")

if __name__ == "__main__":
    run_transform()
import duckdb

# Define your connection to the Podman Postgres instance
# Note: Since it's local, 'localhost' works
pg_conn = "host=localhost user=postgres password=self_assured_complexity dbname=crispr_db"

def run_transform():
    con = duckdb.connect()
    print("Connecting to Postgres and unpivoting data...")
    
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")
    
    # Your optimized unpivot logic
    con.execute("""
        CREATE TABLE IF NOT EXISTS pg.gene_effects AS 
        SELECT 
            "column00000" AS model_id, 
            import duckdb
            import os

            try:
                from dotenv import load_dotenv
                load_dotenv()
            except Exception:
                pass

            # Define your connection to the Podman Postgres instance
            # Note: Since it's local, 'localhost' works
            pg_password = os.environ.get("POSTGRES_PASSWORD", "self_assured_complexity")
            pg_conn = f"host=localhost user=postgres password={pg_password} dbname=crispr_db"

            def run_transform():
                con = duckdb.connect()
                print("Connecting to Postgres and unpivoting data...")

                con.execute("INSTALL postgres; LOAD postgres;")
                con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE POSTGRES);")

                # Your optimized unpivot logic
                con.execute("""
                    CREATE TABLE IF NOT EXISTS pg.gene_effects AS 
                    SELECT 
                        "column00000" AS model_id, 
                        gene_symbol, 
                        dependency_score
                    FROM (
                        UNPIVOT (SELECT * FROM read_csv_auto('CRISPRGeneEffect.csv'))
                        ON COLUMNS(* EXCLUDE "column00000")
                        INTO NAME gene_symbol VALUE dependency_score
                    );
                """)
                print("Success! 21 million rows processed.")

            if __name__ == "__main__":
                run_transform()