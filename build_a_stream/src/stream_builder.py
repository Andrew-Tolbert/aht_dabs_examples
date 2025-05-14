from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from databricks.sdk import WorkspaceClient

class Params:
    def __init__(self, catalog, schema, volume, updated_at):
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.updated_at = updated_at

    def __repr__(self):
        return (f"Params(catalog={self.catalog!r}, schema={self.schema!r}, "
                f"volume={self.volume!r}, updated_at={self.updated_at!r})")

# Create the WorkspaceClient and Params object at the module level
w = WorkspaceClient()

params = Params(
    catalog=w.dbutils.widgets.getArgument('catalog'),
    schema=w.dbutils.widgets.getArgument('schema'),
    volume='stream',
    updated_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
)

# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()

