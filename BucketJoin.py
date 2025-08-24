from pyspark.sql.functions import expr, broadcast

from lib.logger import Log4J

from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    '''
    #BucketBy only works in a spark managed table
    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")

    #Coalesce is to Merge the partitions
    flight_time_df1.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data1")

    flight_time_df2.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.flight_data2")
    '''

    #Assuming the above is done in another application
    df3 = spark.read.table("MY_DB.flight_data1")
    df4 = spark.read.table("MY_DB.flight_data2")

    #prevents broadcast join since both tables are small
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    join_expr = df3.id == df4.id
    join_df = df3.join(df4, join_expr, "inner")

    #Action to trigger the join transformations and a spark job
    join_df.collect()

    #holding the spark application to view the Spark UI
    input("press key to stop...")
