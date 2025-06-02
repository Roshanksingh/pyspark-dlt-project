# src/bronze/ingest_bronze.py
import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    comment="Raw data from source into bronze table"
)
def bronze_table():
    return (
        spark.read.format("json")
        .load("abfss://bronze@<your-storage>.dfs.core.windows.net/source/")
        .withColumn("ingested_at", current_timestamp())
    )
