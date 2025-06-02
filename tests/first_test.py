from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("dev").getOrCreate()