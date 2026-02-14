CRISPR Gene Effect Pipeline
Project Intent and Utility
This project serves as a high-performance data engineering bridge between raw genomic screening data and operational research insights. It processes the Broad Institute’s DepMap CRISPR (Gene Effect) dataset, which contains over 21 million records representing the dependency scores of approximately 17,000 genes across 1,100+ cancer cell lines.

The primary importance of this system is computational efficiency for therapeutic discovery. Raw genomic data is typically distributed in wide-format CSVs that are difficult to query or join with other datasets. This pipeline transforms that data into a cleaned, indexed, and searchable relational "Gold" layer. This allows researchers to instantly identify "Achilles' heels"—specific genes that cancer cells depend on for survival—without the overhead of parsing massive flat files.

Architecture and Engineering Specs
The system utilizes a Medallion Architecture to ensure data integrity and performance:

Compute: DuckDB performs in-memory, multi-threaded UNPIVOT operations, processing the 21M row dataset in approximately 60 seconds.

Orchestration: Dagster manages the asset lifecycle, providing a persistent run history and event-driven automation.

Persistence: A dedicated DAGSTER_HOME directory ensures pipeline logs and sensor states persist across system reboots.
Storage: Containerized PostgreSQL (via Podman) provides persistent, relational storage.

Optimization: B-Tree indexing on gene_symbol enables sub-second query performance.

Data Normalization: Raw headers (e.g., A1BG (1)) are parsed into distinct gene_symbol and entrez_id columns to support relational joins with other biomedical databases.

Installation and Usage
1. Environment Setup
This project uses uv for deterministic dependency management.

Bash
uv sync
2. Secret Management
Configuration is decoupled from code via environment variables. Create a .env file in the root directory:

Plaintext
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=crispr_db
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
CRISPR_CSV_PATH=/home/username/Downloads/CRISPRGeneEffect.csv
3. Infrastructure
Ensure Podman is active and the PostgreSQL service is running:

Bash
# Example if using a systemd-managed container
systemctl --user start postgres
4. Execution and Automation
The pipeline is fully orchestrated. Once the environment is set up, launch the Dagster development server:

Bash
# Set the local Dagster home for persistent logging
export DAGSTER_HOME=$(pwd)/.dagster_home

# Launch the orchestrator
uv run dagster dev -f assets.py
Manual Trigger: Navigate to the Assets tab in the UI and click Materialize.

Automated Trigger: In the Automation tab, toggle the watch_crispr_csv sensor to ON. The system will now automatically refresh the "Gold" layer whenever the source CSV timestamp changes.

Bash
uv run python transform.py
B. Indexing: Apply indexing to the Gold layer for high-speed retrieval.

Bash
psql -h 127.0.0.1 -U postgres -d crispr_db -c "CREATE INDEX idx_gene_symbol ON gene_effects(gene_symbol);"
C. Genomic Search: Utility script for sub-second gene dependency lookups.

Bash
uv run python gene_search.py
Engineering Notes
Security: Password prompts are bypassed using a .pgpass file with 0600 permissions, ensuring credentials are never exposed in system process lists.

Data Cleaning: The pipeline utilizes regex and string splitting within the DuckDB layer to ensure symbols match standard NCBI nomenclature.

Scalability: The architecture is designed for vertical scaling, utilizing all available CPU cores on the host machine for data ingestion.

Sample Project Structure:
crispr_project1/
├── .dagster_home/      # Persistent run history, logs, and sensor cursors
├── archive/            # Historical scripts and legacy logic
│   └── transform.py    # Original standalone ETL script (Archived)
├── .env                # Local secrets and path configurations (Git-ignored)
├── .gitignore          # Prevents environment and home data from being tracked
├── assets.py           # Core Dagster definitions (Assets, Sensors, Jobs)
├── gene_search.py      # CLI utility for sub-second gene dependency lookups
├── pyproject.toml      # Dependency management via uv
└── README.md           # Project documentation and engineering specs