
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.functions import spark_partition_id

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("DatabricksSample") \
        .master("local[2]") \
        .getOrCreate()

    logDF = spark.read\
        .option("header",True)\
        .csv("/Users/rahulvenugopalan/Documents/bigDBFS.csv")\
        ##.sample(withReplacement=False, fraction=0.3, seed=3)

    from pyspark.sql.functions import col

    serverErrorDF =  logDF.filter((col("code")>=500)& (col("code")<600)).select("date", "time", "extention", "code")
    logDF.show()
    logDF.createOrReplaceTempView("total")
    sqlDF = spark.sql("select ip,count(ip) from total group by ip order by count")

    #ipCountDF = spark.sql("select ip,count(ip) as count from total group by ip order by count desc")
    sqlDF.show()

    from pyspark.sql.functions import from_utc_timestamp, hour, col
    countvalue=serverErrorDF.count()
    print(countvalue)

    serverErrorDF.show(100)

    countsDF = (serverErrorDF
                .select(f.minute(from_utc_timestamp(col("time"), "GMT")).alias("hour"))
                .groupBy("hour")
                .count()
                .orderBy("hour")
                )

    countsDF.show()

    spark.stop()