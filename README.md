Installation and Usage
1. Environment Setup
This project uses uv for extremely fast, reproducible dependency management. To sync your environment:

Bash
uv sync
2. Infrastructure
Ensure Podman is active and start the PostgreSQL container:

Bash
podman-compose up -d
3. Execution
The project contains three primary entry points:

Core Pipeline: Executes the DuckDB-to-PostgreSQL ETL process for the 21M row dataset.

Bash
uv run main.py
Data Transformation: Specific logic for CRISPR genomic data cleaning and reformatting.

Bash
uv run transform.py
Genomic Search: Utility script for querying specific gene sequences within the processed data.

Bash
uv run gene_search.py
Engineering Notes
definitions.py: Contains centralized configuration for database schemas, Podman container credentials, and file paths to ensure consistency across the pipeline.

uv.lock: Guarantees deterministic builds across different environments, critical for pipeline stability in production.
