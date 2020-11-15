
from pyspark.sql import *


spark = SparkSession \
        .builder \
        .appName("DatabricksSample") \
        .master("local[2]") \
        .getOrCreate()

logDF = spark.read\
        .option("header",True)\
        .csv("/Users/rahulvenugopalan/Documents/bigDBFS.csv")\
        .sample(withReplacement=False, fraction=0.3, seed=3)

logDF.show(50)


spark.stop()
