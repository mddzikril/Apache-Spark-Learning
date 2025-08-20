from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from lib.logger import Log4J
from lib.utils import to_date_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("RowTesting") \
        .getOrCreate()

    logger = Log4J(spark)

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())
    ])

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()
    my_df_reformatted = to_date_df(my_df, "M/d/y", "EventDate")
    my_df_reformatted.printSchema()
    my_df_reformatted.show()