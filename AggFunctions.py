from lib.logger import Log4J

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("AggFunctions") \
        .getOrCreate()

    logger = Log4J(spark)

    invoice_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("data/invoices.csv")

    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")).show(10)

    invoice_df.selectExpr(
        "COUNT(1) as `Count 1`",
        "COUNT(StockCode) as `Count Field`",
        "SUM(Quantity) as TotalQuantity",
        "AVG(UnitPrice) as AveragePrice"
    ).show(10)

    #Using sql api for grouping and aggregating
    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
          SELECT Country, InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice),2) as InvoiceValue
          FROM sales
          GROUP BY Country, InvoiceNo""")

    summary_sql.show(5)

    #using DataFrame API
    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(sum("Quantity").alias("TotalQuantity"),
             round(sum(expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )

    summary_df.show(5)

    """
    Grouping Data
    """

    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    summary_df2 = invoice_df \
        .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "d-M-y H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)

    summary_df2.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "dataSink/parquet/") \
        .save()

    summary_df2.sort("Country", "WeekNumber").show(5)

