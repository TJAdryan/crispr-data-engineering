import os
import sys
import psycopg2
from dotenv import load_dotenv

# 1. Load environment variables from your .env file
load_dotenv()

def get_connection():
    """Establishes a secure connection to the Podman Postgres instance."""
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "crispr_db"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
    except psycopg2.Error as e:
        print(f"CRITICAL: Unable to connect to database. {e}")
        sys.exit(1)

def search_gene(gene_name):
    """
    Queries the 21M row table using the idx_gene_symbol index.
    Expects the 'Senior Clean' format (e.g., 'AAVS1' instead of 'AAVS1 (107080)').
    """
    conn = get_connection()
    cur = conn.cursor()

    # Optimized SQL Query: Uses parameterized input to prevent SQL Injection
    # We order by dependency_score to find the most 'essential' cell lines first
    query = """
        SELECT model_id, entrez_id, dependency_score 
        FROM gene_effects 
        WHERE gene_symbol = %s 
        ORDER BY dependency_score ASC 
        LIMIT 10;
    """

    try:
        cur.execute(query, (gene_name.upper(),))
        rows = cur.fetchall()

        if not rows:
            print(f"\n[!] No results found for '{gene_name.upper()}'.")
            print("Tip: Ensure you have re-run transform.py with the 'split_part' cleaning logic.")
            return

        print(f"\n--- Top 10 Dependencies for {gene_name.upper()} ---")
        print(f"{'Model ID':<15} | {'Entrez ID':<10} | {'Score':<10}")
        print("-" * 45)

        for model_id, entrez_id, score in rows:
            # Highlighting: Scores < -1.0 usually indicate high essentiality
            status = " (Essential)" if score <= -1.0 else ""
            print(f"{model_id:<15} | {entrez_id:<10} | {score:<10.4f}{status}")

    except Exception as e:
        print(f"ERROR during search: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print("CRISPR Gene Effect Search Tool")
    target = input("Enter Gene Symbol (e.g., TP53, AAVS1): ").strip()
    
    if target:
        search_gene(target)
    else:
        print("No input detected. Exiting.")