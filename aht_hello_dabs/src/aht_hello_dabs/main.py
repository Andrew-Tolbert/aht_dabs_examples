from pyspark.sql import SparkSession
from cowsay import cow

def get_taxis():
  spark = SparkSession.builder.getOrCreate()
  return spark.read.table("samples.nyctaxi.trips")

def main():
  get_taxis().show(5)

def cow_say(string):
  return cow(string)
  
if __name__ == '__main__':
  main()
