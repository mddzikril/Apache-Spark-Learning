import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
    """
    Spark conf can also be created this way but this is considered hard coding:
    conf = SparkConf()
    conf.setMaster("local[3]").setAppName("HelloSpark")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    """
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting Hello Spark")

    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)

    count_df = count_by_country(partitioned_survey_df)


    logger.info(count_df.collect())

    input("Insert") #ensure program does not end but use this only for debugging
    #can use to check Spark UI
    logger.info("Finished Hello Spark")

    spark.stop()  # need to stop at the end of every session
