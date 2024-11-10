from pyspark.sql import SparkSession
from cowsay import cow
from dbruntime.databricks_repl_context import get_context
from databricks.sdk.runtime import dbutils #I have to import dbutils here so I can use client side https://github.com/databricks/databricks-sdk-py?tab=readme-ov-file#interaction-with-dbutils


def get_taxis():
  spark = SparkSession.builder.getOrCreate()
  return spark.read.table("samples.nyctaxi.trips")

def main():
  get_taxis().show(5)

def sentient_cow():

  spark = SparkSession.builder.getOrCreate()
  cow_env = ""
  cow_workspace = ""
  cow_catalog = ""

  try: 
      env = dbutils.widgets.getArgument("job_env")
      if len(env) > 0:
          cow_env = f"\u2713 I know that the DABS Variables are {env}"
      else: 
          cow_env = "x I don't know the DABS Variables"
  except: 
      cow_env = "x I don't know the DABS Variables"

  try: 
      workspaceId = get_context().workspaceId
      cow_workspace = f"\u2713 I know that the workspace id is {workspaceId}"
  except: 
      cow_workspace = "x I have no idea what workspace im in"

  try: 
      current_catalog = spark.catalog.currentCatalog()
      cow_catalog = f"\u2713 I know that I'm in the {current_catalog} catalog"
  except: 
      cow_catalog = f"x I have no idea what catalog I'm in"

  cow_knowledge = f"{cow_env}\n{cow_workspace}\n{cow_catalog}"

  return cow(cow_knowledge)

if __name__ == '__main__':
  main()
