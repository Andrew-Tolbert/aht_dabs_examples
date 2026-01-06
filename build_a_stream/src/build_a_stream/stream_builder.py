from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from databricks.sdk import WorkspaceClient

class Params:
    def __init__(self, catalog, schema, volume, updated_at,stream_length_m, files_per_m, reset_data, compression, streaming_mode, streaming_file_size_m):
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.stream_length_m = stream_length_m
        self.files_per_m = files_per_m
        self.reset_data = reset_data
        self.compression = compression
        self.streaming_mode = streaming_mode
        self.streaming_file_size_m = streaming_file_size_m
        self.updated_at = updated_at

# Create the WorkspaceClient and Params object at the module level
w = WorkspaceClient()

params = Params(
    catalog=w.dbutils.widgets.getArgument('catalog'),
    schema=w.dbutils.widgets.getArgument('schema'),
    volume='stream',
    stream_length_m=float(w.dbutils.widgets.getArgument("stream_length_m")),
    files_per_m=int(w.dbutils.widgets.getArgument("files_per_m")),
    reset_data=True if w.dbutils.widgets.getArgument("reset_data") == 'Y' else False,
    compression=True if w.dbutils.widgets.getArgument("compression") == 'Y' else False,
    streaming_mode=True if w.dbutils.widgets.getArgument("streaming_mode") == 'Y' else False,
    streaming_file_size_m=float(w.dbutils.widgets.getArgument("streaming_file_size_m")),
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

