from pyspark.sql.functions import expr, broadcast

from lib.logger import Log4J

from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4J(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    #Setting the shuffle partition config
    #3 partitions after the shuffle meaning 3 reduce exchanges
    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    #broadcast join
    join_df_broadcast = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    #Dummy action to trigger transformation
    join_df.foreach(lambda f: None)
    join_df_broadcast.foreach(lambda f: None)

    #to keep the program running so that Spark UI is viewable
    input("INPUT")