# CRISPR Gene Effect Pipeline

## Overview
A production-grade data engineering pipeline that transforms the Broad Institute's DepMap CRISPR (Gene Effect) dataset into a scalable, queryable lakehouse for cancer research. Processes 21M+ genomic screening records across 17,000+ genes and 1,100+ cancer cell lines using Apache Spark, Delta Lake, and Dagster orchestration.

**Key Features:**
- 🔬 Normalizes wide-format CRISPR screening data into relational format
- ⚡ Apache Spark for distributed processing with Delta Lake persistence
- 🎯 Dagster orchestration with sensors and observability
- 🧬 Identifies genetic dependencies ("Achilles' heels") in cancer cells
- 📊 ACID transactions and time-travel versioning for data integrity

## Project Intent and Utility
This project serves as a high-performance data engineering bridge between raw genomic screening data and operational research insights. It processes the Broad Institute’s DepMap CRISPR (Gene Effect) dataset, containing over 21 million records representing the dependency scores of approximately 17,000 genes across 1,100+ cancer cell lines.

The primary importance of this system is computational efficiency for therapeutic discovery. By transforming wide-format CSVs into a cleaned, indexed, and searchable Lakehouse architecture, researchers can instantly identify "Achilles' heels"—specific genes that cancer cells depend on for survival—without the overhead of parsing massive flat files.

## Evolution of the Architecture
This project followed a phased engineering approach to ensure data integrity and scalability:

**Phase 1 (Validation):** Utilized DuckDB and PostgreSQL to perform initial in-memory UNPIVOT operations and relational storage. This phase served as the "Proof of Concept," validating that the 21M row dataset could be normalized while maintaining biological accuracy (benchmarked against known essential genes like SNRPD3 and RPL15).

**Phase 2 (Scalability):** Upon validation, the compute layer was migrated to Apache Spark and the storage layer to Delta Lake. This shift enables ACID transactions, time-travel versioning, and the scalability required for joining with other massive biomedical datasets.

## Architecture and Engineering Specs
The system utilizes a Medallion Architecture managed by modern orchestration:

- **Compute:** Apache Spark performs distributed UNPIVOT operations, utilizing the stack expression to normalize 17,000+ gene columns into relational rows.

- **Orchestration:** Dagster manages the asset lifecycle, providing observability, persistent run history, and event-driven automation via sensors.

- **Storage (Lakehouse):** Delta Lake provides the persistence layer, offering high-speed Parquet-based storage with schema enforcement and versioning.

- **Data Normalization:** Raw headers (e.g., A1BG (1)) are parsed into distinct gene_symbol and entrez_id columns to support relational joins with databases like TCGA or DGIdb.

## Installation and Usage
### 1. Environment Setup
This project uses uv for deterministic dependency management.
```bash
uv sync
```

### 2. Local Network Notebook Access
A custom shell script is provided to launch a Jupyter Lab instance. It automatically configures the JVM flags required for Apache Arrow zero-copy memory transfers.
```bash
chmod +x start_jupyter.sh
./start_jupyter.sh
```

### 3. Execution and Automation
Launch the Dagster development server to materialize the Delta tables:
```bash
# Set the local Dagster home for persistent logging
export DAGSTER_HOME=$(pwd)/.dagster_home

# Launch the orchestrator
uv run dagster dev -f assets.py
```

## Engineering Notes
- **JVM Tuning:** To support high-speed data transfer between Spark and Python (via Arrow), the system utilizes specific JVM "add-opens" flags: `--add-opens=java.base/java.nio=ALL-UNNAMED`.
- **Memory Management:** For exploratory analysis in Jupyter, the pipeline is configured to use standard serialization or limited sampling to maintain stability on host machines with Java 17+.
- **Persistence:** A dedicated `.dagster_home` directory ensures pipeline logs and sensor states (watching for CSV updates) persist across system reboots.

## Sample Project Structure
```plaintext
crispr_project1/
├── .dagster_home/      # Persistent run history, logs, and sensor cursors
├── archive/            # Phase 1: DuckDB & Postgres legacy logic
│   └── transform.py    # Original standalone ETL script
├── data/
│   └── delta/          # Managed Delta Lake storage (Silver/Gold Layers)
├── .env                # Local secrets and path configurations (Git-ignored)
├── assets.py           # Core Dagster definitions (Spark & Delta logic)
├── notebooks/          # EDA and Data Quality validation (01_crispr_eda.ipynb)
├── start_jupyter.sh    # Network-enabled Jupyter launch script with JVM flags
├── pyproject.toml      # Dependency management via uv
└── README.md           # Project documentation and engineering specs
```
