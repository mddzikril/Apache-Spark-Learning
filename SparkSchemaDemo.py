from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4J(spark)

    """
    * is a wild card which targets any csv file starting with flight
    inferSchema is not reliable as date time field is inferred as a string data type
    1. Explicitly define schema
    2. Use a data file with implicitly defined schema
    """
    flight_time_csv_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.csv")

    flight_time_csv_df.show(5)
    logger.info("CSV Schema:" + flight_time_csv_df.schema.simpleString())

    """
    JSON has no header option
    JSON format will always inferSchema
    Schema still inaccurate as date field is inferred as a String
    """
    flight_time_Json_df = spark.read \
        .format("json") \
        .load("data/flight*.json")

    flight_time_Json_df.show(5)
    logger.info("JSON Schema:" + flight_time_Json_df.schema.simpleString())

    """
    Parquet has implicitly defined schema in the datafile
    Date fields is now correctly defined
    Parquet is the recommended and default file format for Apache Spark
    """
    flight_time_parquet_df = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flight_time_parquet_df.show(5)
    logger.info("Parquet Schema:" + flight_time_parquet_df.schema.simpleString())

