CRISPR Data Transformation Pipeline
Scalable Processing for 21M+ Rows
Overview
This project implements a high-performance ETL pipeline designed to process and analyze CRISPR genomic data. The primary objective was to architect a solution capable of handling a 21-million-row dataset on local hardware while maintaining low latency for analytical queries and data integrity during the transfer process.

Technical Stack
Database Engine: DuckDB (In-process OLAP database for vectorized transformations)

Persistent Storage: PostgreSQL (Managed via Podman for relational persistence)

Orchestration: Python 3.x

Infrastructure: Fedora Linux / VS Code Remote Tunnels

Architecture
The pipeline follows a three-stage architecture:

Ingestion: Raw CRISPR data is ingested from large-scale CSV/Parquet sources using DuckDB’s zero-copy integration.

Transformation: Data is processed using DuckDB to perform vectorized operations on the 21M row set, minimizing memory overhead compared to standard row-based processing.

Load: Transformed datasets are loaded into a containerized PostgreSQL instance for persistent storage and relational querying.

Engineering Challenges and Solutions
Memory Management: Leveraged DuckDB's out-of-core processing to transform a 21M row dataset within the constraints of the Fedora host's physical RAM.

Environment Parity: Utilized Podman to containerize the PostgreSQL instance, ensuring the database environment is reproducible and isolated from the host operating system.

Remote Development: Managed the development lifecycle via VS Code Tunnels, enabling consistent access to the Fedora-based compute resources.

Installation and Usage
Clone the repository: git clone https://github.com/tjadryan/crispr-data-engineering.git

Initialize containers: podman-compose up -d

Execute pipeline: python src/transform.py