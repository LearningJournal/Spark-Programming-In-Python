
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("dateFormatting") \
        .master("local[2]") \
        .getOrCreate()


    my_schema_date = StructType([
        StructField("ID", StringType()),
        StructField("EventDatestr", StringType())
    ])

    my_schema_timestamp= StructType([
        StructField("ID", StringType()),
        StructField("EventTimestamp", StringType())
    ])

    my_rows_date = [Row("123", "04/05/2020"),
               Row("124", "3/05/2020" ),
               Row("125", "04/05/2020"),
               Row("126", "04/05/2020"),
               Row("127", "04/05/2020"),
               Row("128", "04/05/2020"),
               Row("129", "04/05/2020"),
               Row("130", "8/3/2020"),
               Row("131", "11/12/2020"),
               Row("132", "04/13/2020")]

    my_rows_timestamp = [Row("123", "2020-02-28 12:30:00"),
                         Row("234", "2020-02-28 12:30:00") ,
                         Row("233", "2020-02-28 10:30:00") ,
                         Row("343", "2020-02-28 11:30:00") ,
                         Row("434", "2020-02-28 14:30:00") ,
                         Row("343", "2020-02-28 10:30:00") ,
                         Row("353", "2020-02-28 10:30:00") ,
                         Row("453", "2020-02-28 23:30:00") ,
                         Row("137", "2020-02-28 14:30:00") ]

    my_schema_list = StructType([
        StructField("ID", IntegerType())
    ])

    my_row_list = [Row(123)]

    my_rdd_list = spark.sparkContext.parallelize(my_row_list)

    my_df_list= spark.createDataFrame(my_rdd_list, my_schema_list)

    my_df_list.show()

    print(type(my_df_list))

    mvv = my_df_list.select(max("ID")).rdd.flatMap(lambda x: x).collect()

    print(type(mvv))
    print(mvv)

    for i in mvv:
        xy=i
    print(xy)
    print(type(xy))

        #list(my_df_list.select('ID').toPandas()['ID'])  # =>

    #print(list)

    my_rdd_date = spark.sparkContext.parallelize(my_rows_date)

    my_df_date = spark.createDataFrame(my_rdd_date, my_schema_date)

    my_rdd2 = spark.sparkContext.parallelize(my_rows_timestamp)

    my_df_timestamp = spark.createDataFrame(my_rdd2, my_schema_timestamp)

    from pyspark.sql.functions import col

    dfwithDate = my_df_date.withColumn("EventDatetype",to_date(col("EventDatestr"), 'M/d/yyyy') )

    dfwithDate.show()

    dfwithTimestamp=my_df_timestamp.withColumn("EventTimestamptype", to_timestamp(col("EventTimestamp"), 'yyyy-MM-dd HH:mm:ss'))

    dfwithTimestamp2 = my_df_timestamp.select("ID",to_timestamp("EventTimestamp",'yyyy-MM-dd HH:mm:ss').alias("new"))

    my_df_timestamp.show()

    dfwithTimestamp2.show()


    dfwithTimestamp3 = my_df_timestamp.withColumn("Timeintimestamp", to_timestamp("EventTimestamp", 'yyyy-MM-dd HH:mm:ss')) \
                                     .withColumn("ID2", col("ID").cast(IntegerType()))\
                                     .withColumn("ID3", col("ID2")*2)\
                                     .withColumnRenamed("ID3","Transformed_Integer_column")\
                                     .drop("ID2")

    dfwithTimestamp3.show()

    cols = set(dfwithTimestamp3.columns)
    print(cols)

    print("dfwithTimestamp2")
    minmax = dfwithTimestamp3.select(min("Transformed_Integer_column")).collect().map(_(0)).toList


    print(type(minmax))

    dfwithTimestamp3.agg({'Transformed_Integer_column': 'max'}).show()

    newdf5=dfwithTimestamp3.select(hour(from_utc_timestamp(col("Timeintimestamp"), "GMT")).alias("hour"))\
                            .groupBy("hour").count().orderBy("hour")
    newdf5.show()


spark.stop()
