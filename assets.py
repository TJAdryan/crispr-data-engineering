import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, expr
from dagster import (
    asset, 
    sensor, 
    AssetSelection, 
    AssetKey, 
    RunRequest, 
    Output, 
    SensorEvaluationContext, 
    Definitions,
    file_relative_path
)
from dotenv import load_dotenv

load_dotenv()

# --- SPARK CONFIGURATION ---
def get_spark_session(app_name="CRISPR_Pipeline"):
    return (SparkSession.builder
            .appName(app_name)
            .master("local[2]") 
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") 
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.extraJavaOptions", 
                    "-XX:+IgnoreUnrecognizedVMOptions "
                    "--add-opens=java.base/java.nio=ALL-UNNAMED "
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                    "--add-opens=java.base/java.lang=ALL-UNNAMED")
            .getOrCreate())

# --- ASSET 1: RAW METADATA INGESTION ---
@asset(group_name="raw")
def raw_model_metadata(context):
    """Ingests Model.csv from local data directory."""
    metadata_path = os.path.join(os.getcwd(), "data/Model.csv")
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Model.csv not found at {metadata_path}. Please move it to the data/ folder.")
    
    spark = get_spark_session("Metadata_Ingest")
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(metadata_path)
        output_path = os.path.join(os.getcwd(), "data/delta/model_metadata")
        df.write.format("delta").mode("overwrite").save(output_path)
        
        context.log.info(f"Metadata ingested to {output_path}")
        return Output(value=output_path, metadata={"path": output_path, "rows": df.count()})
    finally:
        spark.stop()

# --- ASSET 2: GENE EFFECTS TRANSFORMATION (Original Logic) ---
@asset(group_name="silver")
def gene_effects_delta(context):
    """Processes the 21M row CRISPR CSV into a normalized Delta table."""
    csv_path = os.getenv("CRISPR_CSV_PATH")
    spark = get_spark_session("Gene_Effects_Unpivot")

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        first_col = df.columns[0] 
        gene_cols = [c for c in df.columns if c != first_col]
        
        stack_parts = ", ".join([f"'{c}', `{c}`" for c in gene_cols])
        stack_expr = f"stack({len(gene_cols)}, {stack_parts}) as (gene_symbol_raw, dependency_score)"

        final_df = df.select(
            col(first_col).alias("model_id"),
            expr(stack_expr)
        ).withColumn(
            "gene_symbol", split(col("gene_symbol_raw"), r" \(").getItem(0)
        ).withColumn(
            "entrez_id", regexp_extract(col("gene_symbol_raw"), r"\((\d+)\)", 1)
        ).select("model_id", "gene_symbol", "entrez_id", "dependency_score")

        output_path = os.path.join(os.getcwd(), "data/delta/gene_effects")
        final_df.write.format("delta").mode("overwrite").save(output_path)
        
        context.log.info(f"Unpivot complete: {output_path}")
        return Output(value=output_path, metadata={"rows": "21M+", "path": output_path})
    finally:
        spark.stop()

# --- ASSET 3: ENRICHED JOIN ---
@asset(
    deps=[gene_effects_delta, raw_model_metadata],
    group_name="silver"
)
def enriched_gene_effects(context):
    """Joins CRISPR scores with lineage/disease metadata."""
    spark = get_spark_session("Enrichment_Join")
    try:
        effects_path = os.path.join(os.getcwd(), "data/delta/gene_effects")
        meta_path = os.path.join(os.getcwd(), "data/delta/model_metadata")
        
        effects_df = spark.read.format("delta").load(effects_path)
        meta_df = spark.read.format("delta").load(meta_path)

        # Select target columns from Model.csv
        refined_meta = meta_df.select(
            col("ModelID").alias("meta_model_id"),
            "OncotreeLineage",
            "OncotreePrimaryDisease",
            "OncotreeSubtype"
        )

        joined_df = effects_df.join(
            refined_meta,
            effects_df.model_id == refined_meta.meta_model_id,
            "left"
        ).drop("meta_model_id")

        output_path = os.path.join(os.getcwd(), "data/delta/enriched_gene_effects")
        joined_df.write.format("delta").mode("overwrite").save(output_path)
        
        context.log.info(f"Final Enriched Table count: {joined_df.count()}")
        return Output(value=output_path, metadata={"path": output_path})
    finally:
        spark.stop()

# --- SENSOR ---
@sensor(target=AssetSelection.keys(AssetKey("gene_effects_delta")))
def watch_crispr_csv(context: SensorEvaluationContext):
    csv_path = os.getenv("CRISPR_CSV_PATH")
    if not csv_path or not os.path.exists(csv_path):
        return
    last_modified = str(os.path.getmtime(csv_path))
    if context.cursor != last_modified:
        context.update_cursor(last_modified)
        yield RunRequest(run_key=last_modified)

# --- DEFINITIONS ---
defs = Definitions(
    assets=[raw_model_metadata, gene_effects_delta, enriched_gene_effects],
    sensors=[watch_crispr_csv],
)