import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting Hello Spark")
    #proccessing code
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())


    logger.info("Finished Hello Spark")

    spark.stop()  # need to stop at the end of every session
