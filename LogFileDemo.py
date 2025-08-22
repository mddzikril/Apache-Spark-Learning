from pyspark.sql import *
from pyspark.sql.functions import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4J(spark)

    file_df = spark.read.text("data/apache_logs.txt")
    file_df.printSchema()

    #Regular Expression
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    #3 Arguments for regexp_extract which will create a new dataframe
    # 1. Field Name - only "value" in this case
    # 2. regex
    # 3. field number
    logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                             regexp_extract('value', log_reg, 4).alias('date'),
                             regexp_extract('value', log_reg, 6).alias('request'),
                             regexp_extract('value', log_reg, 10).alias('referrer'))

    logs_df.printSchema()

    #can now proceed to do some analysis
    logs_df \
        .filter("trim(referrer) != '-' ") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(100, truncate=False)

    logs_df.withColumn("referrer", substring_index("referrer", "/", 3)) \
        .select("referrer") \
        .show(5)