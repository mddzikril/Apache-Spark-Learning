from lib.logger import Log4J

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("WindowAggFunctions") \
        .getOrCreate()

    logger = Log4J(spark)

    #parquet has schema defined in its metadata
    summary_df = spark.read.parquet("data/summary.parquet")

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("RunningTotal",
                          f.sum("InvoiceValue").over(running_total_window)) \
        .show(8)