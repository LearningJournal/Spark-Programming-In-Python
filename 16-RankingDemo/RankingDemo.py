from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    summary_df = spark.read.parquet("data/summary.parquet")

    summary_df.sort("Country", "WeekNumber").show()

    rank_window = Window.partitionBy("Country") \
        .orderBy(f.col("InvoiceValue").desc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df = summary_df.withColumn("Rank", f.dense_rank().over(rank_window)) \
        .where(f.col("Rank") == 1) \
        .sort("Country", "WeekNumber") \
        .show()
