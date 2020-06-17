from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("RowDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()
    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    new_df.printSchema()
    new_df.show()
