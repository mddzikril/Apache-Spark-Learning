from pyspark.sql import *
from pyspark.sql.types import StructField, DateType, StringType, IntegerType, StructType

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4J(spark)

    """
    Programmatically defining the Schema
    """
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    """
    DDL String defined schema
    """
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    """
    * is a wild card which targets any csv file starting with flight
    inferSchema is not reliable as date time field is inferred as a string data type
    mode is to throw an error if the types in the data do not match our own defined schema at runtime
    CSV requires us to define the date format pattern if we use our own defined schema
    """
    flight_time_csv_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")

    flight_time_csv_df.show(5)
    logger.info("CSV Schema:" + flight_time_csv_df.schema.simpleString())

    """
    JSON has no header option
    JSON format will always inferSchema
    inferred Schema still inaccurate as date field is inferred as a String
    We are using the DDL defined schema here but both programmatically defined and ddl defined are interchangeable
    Will also need the date format here
    """
    flight_time_Json_df = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
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

