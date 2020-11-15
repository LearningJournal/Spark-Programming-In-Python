
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("dateFormatting") \
        .master("local[2]") \
        .getOrCreate()

    # Creating a function
    def cube(x):
        return x*x*x

    def strlen_nullsafe(x):
        return len(x)

    # Register the function as udf
    spark.udf.register("udfcube" ,cube,LongType())

    spark.range(1, 20).createOrReplaceTempView("test1")
    sqlDF=spark.sql("select id,udfcube(id) as qubedID from test1")
    sqlDF.show()

    spark.udf.register("strlen_nullsafe", lambda s: len(s) if not s is None else -1, "int")

    #spark.sql("select s from test1 where if(s is not null, strlen(s), null) > 1")

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row(None, "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.createOrReplaceTempView("test2")

    sqlDF = spark.sql("select id,strlen_nullsafe(id) as strlen_nullsafe from test2")

    print("lambdadf :")
    sqlDF.show()

    sqldf = spark.sql("select id ,strlen_nullsafe(id) from test2 where id is not null and strlen_nullsafe(id) > 1")
    print("sqldf :")
    sqldf.show()

    spark.stop()

