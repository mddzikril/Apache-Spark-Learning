from pyspark.sql import *
from pyspark.sql.types import StructField, DateType, StringType, IntegerType, StructType

from lib.logger import Log4J

#enableHiveSupport connects Spark to a persistent Hive Metastore
#Metastore is needed to create a managed table
if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSQLTable") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    flight_time_parquet_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    #ORIGIN has too many unique values, will create alot of partitions
    #Can use bucketBy instead
    """
    flight_time_parquet_df.write \
        .mode("Overwrite") \
        .partitionBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")
    """

    #BucketBy instead of partitionBy
    #Create 5 bucket for the in which the partitions will be equally distributed
    #can think of it as doing partitionBy() first and then splitting the results equally into 5 buckets
    flight_time_parquet_df.write \
        .format("csv") \
        .mode("Overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))