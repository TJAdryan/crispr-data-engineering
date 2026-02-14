import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, expr
from dagster import asset, sensor, AssetSelection, AssetKey, RunRequest, Output, SensorEvaluationContext, Definitions, DefaultSensorStatus
from dotenv import load_dotenv

load_dotenv()

def get_spark_session():
    return (SparkSession.builder
            .appName("CRISPR_Pipeline")
            .master("local[2]") 
            # Changed _2.13 to _2.12 to match your environment
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") 
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.extraJavaOptions", 
                    "-XX:+IgnoreUnrecognizedVMOptions "
                    "--add-opens=java.base/java.nio=ALL-UNNAMED "
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                    "--add-opens=java.base/java.lang=ALL-UNNAMED")
            .getOrCreate())

            
# --- ADDED DECORATOR HERE ---
@asset
def gene_effects_delta():
    csv_path = os.getenv("CRISPR_CSV_PATH")
    spark = get_spark_session()

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
        
        return Output(value=None, metadata={"rows_processed": "Success", "path": output_path})
        
    finally:
        spark.stop()

# Using keys(AssetKey(...)) is more explicit and prevents the "function" type error
@sensor(target=AssetSelection.keys(AssetKey("gene_effects_delta")))
def watch_crispr_csv(context: SensorEvaluationContext):
    csv_path = os.getenv("CRISPR_CSV_PATH")
    if not csv_path or not os.path.exists(csv_path):
        return
    last_modified = str(os.path.getmtime(csv_path))
    if context.cursor != last_modified:
        context.update_cursor(last_modified)
        yield RunRequest(run_key=last_modified)

defs = Definitions(
    assets=[gene_effects_delta],
    sensors=[watch_crispr_csv],
)