from databricks.connect.session import DatabricksSession as SparkSession
from databricks.sdk.core import Config

config = Config(profile="PROFILE", cluster_id="CLUSTER_ID")
spark = SparkSession.builder.sdkConfig(config).getOrCreate()