import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

# Create a DLT table in Unity Catalog schema `bronze`
@dlt.table(
    name="region",
    comment="Ingested region data from source to bronze",
    table_properties={"quality": "bronze"}
)
def load_region():
    file_name = dlt.current_session().widgets.get("file_name", "region")

    df = (
        spark.read.format("parquet")
        .load(f"abfss://source@endtoendadls.dfs.core.windows.net/{file_name}")
        .withColumn("file_name", input_file_name())
        .withColumn("ingestion_ts", current_timestamp())
    )

    return df
