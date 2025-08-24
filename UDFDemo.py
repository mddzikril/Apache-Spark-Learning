import re

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from lib.logger import Log4J
from lib.utils import to_date_df

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("UDFTesting") \
        .getOrCreate()

    logger = Log4J(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")

    survey_df.show(10)

    #Column object expression

    #Registering the UDF function in the spark session
    #Driver will serialise and send this function to the executors
    parse_gender_udf = udf(parse_gender, StringType())

    #Checking catalog entry
    logger.info("Catalog Entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    #SQL expression
    spark.udf.register("parse_gender_udf_sql", parse_gender, StringType())
    logger.info("Catalog Entry 2:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf_sql(Gender)"))
    survey_df3.show(10)