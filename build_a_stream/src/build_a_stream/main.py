from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from databricks.sdk import WorkspaceClient


def get_taxis(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.nyctaxi.trips")


def main():
    get_taxis(get_spark()).show(5)


if __name__ == "__main__":
    main()