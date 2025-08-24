import re

from lib.logger import Log4J

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("MiscTransformations") \
        .getOrCreate()

    logger = Log4J(spark)

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

    #Adding an ID Column with the monotically_increasing_id() function
    #Id will be random but UNIQUE
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.show()

    #Creating SQL case expressions (Switch case) to reformat year
    df2 = df1.withColumn("year", expr("""
        CASE
            WHEN year < 21 THEN year + 2000
            WHEN year < 100 then year + 1900 
            ELSE year
        END
        """))
    df2.show()

    #Inline casting to prevent spark from auto promoting to an unwanted datatype
    #year column will remain as str after the operation
    df3 = df1.withColumn("year", expr("""
            CASE
                WHEN year < 21 THEN cast(year as int) + 2000
                WHEN year < 100 then cast(year as int) + 1900 
                ELSE year
            END
            """))
    df3.show()

    #Changing the schema
    #Will change the year column to int after the operation
    df4 = df1.withColumn("year", expr("""
                CASE
                    WHEN year < 21 THEN cast(year as int) + 2000
                    WHEN year < 100 then cast(year as int) + 1900 
                    ELSE year
                END
                """).cast(IntegerType()))
    df4.show()


    #casting as the start
    df5 = df1.withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType())) \

    df5.printSchema()

    #Column object expression version of Case expression
    df7 = df5.withColumn("year", when(col("year") < 21, col("year") + 2000) \
                         .when(col("year") < 100, col("year") + 1900) \
                         .otherwise((col("year"))))
    df7.show()

    #Adding columns - dob field
    df8 = df7.withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"), 'd/M/y'))
    df8.show()

    #removing column - day,month,year fields
    df9 = df8.drop("day", "month", "year")
    df9.show()

    #drop duplicates
    df10 = df9.dropDuplicates(["name", "dob"])
    df10.show()

    #sorting by DOB
    df11 = df10.sort(expr("dob desc"))
    df11.show()