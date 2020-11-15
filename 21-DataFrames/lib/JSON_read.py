
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("jsonread") \
        .master("local[2]") \
        .getOrCreate()


smartphoneDF = spark.read.json("/Users/rahulvenugopalan/Downloads/sampledb2.txt")
smartphoneDF.show()

smartphoneDF.printSchema()
smartphoneDFschema = smartphoneDF.schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType

schemaSMS = StructType([
    StructField("SMS", StringType(), True)
])




# Here is the full schema as well
fullSchema =  StructType([
    StructField("SMS", StructType([
    StructField("Address",StringType(),True),
    StructField("date",StringType(),True),
    StructField("metadata", StructType([
    StructField("name",StringType(), True)
    ]), True),
  ]), True)
])

## Filter using SQL expression
SMSDF = (spark.read
  .schema(schemaSMS)
  .json("/Users/rahulvenugopalan/Downloads/sampledb2.txt")
  .filter("SMS is not null")
)
# Filter using column
SMSDF2 = (spark.read
  .schema(fullSchema)
  .json("/Users/rahulvenugopalan/Downloads/sampledb2.txt")
  .filter(f.col("SMS").isNotNull())
)


SMSDF.show()
SMSDF2.show()


SMSDF2.select('SMS.Address','SMS.date','SMS.metadata.name').show(truncate=False)

spark.stop()
