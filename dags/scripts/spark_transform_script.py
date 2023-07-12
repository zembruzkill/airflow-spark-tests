import requests
import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from decouple import config

spark = SparkSession \
    .builder \
    .appName("TransformData") \
    .getOrCreate() 

