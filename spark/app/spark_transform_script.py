import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("TransformData") \
    .getOrCreate() 

print("Spark Session created")
print(spark)



