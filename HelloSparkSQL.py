import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    survey_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(sys.argv[1])

    survey_df.createOrReplaceTempView("survey_table")  # survey_table is the new name of the table
    count_df = spark.sql("SELECT Country, Count(1) as Count from survey_table WHERE Age<40 GROUP BY Country")
    count_df.show()
