from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("DataSinkDemo") \
        .getOrCreate()

    logger = Log4J(spark)

    flight_time_parquet_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flight_time_parquet_df.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()

    # Checking number of partitions
    logger.info("Num Partitions before: " + str(flight_time_parquet_df.rdd.getNumPartitions()))
    # Checking number of records per partition
    flight_time_parquet_df.groupBy(spark_partition_id()).count().show()

    """ Repartitioning to 5 partitions before writing to produce 5 output files
    
    partitioned_df = flight_time_parquet_df.repartition(5)

    # Checking number of partitions for the repartitioned file
    logger.info("Num Partitions after: " + str(partitioned_df.rdd.getNumPartitions()))
    # Checking number of records per partition
    partitioned_df.groupBy(spark_partition_id()).count().show()

    partitioned_df.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()
    
    """

    # Using partitionBy to write to JSON
    flight_time_parquet_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()
