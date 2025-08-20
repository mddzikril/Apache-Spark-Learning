from datetime import date
from unittest import TestCase
from pyspark.sql import *
from pyspark.sql.types import *

from lib.utils import load_survey_df, count_by_country, to_date_df


class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_datafile_loading(self): #test for load_survey_df method and validating the number of rows
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        result_count = sample_df.count() #count number of rows
        self.assertEqual(result_count, 9, "Record count should be 9") #assert

    def test_country_count(self): #test for count_by_country method and validating the number of countries
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        count_list = count_by_country(sample_df).collect() #will collect result in a list
        count_dict = dict() #should have 3 entries in the list with each entry being a country-count pair
        for row in count_list:
            count_dict[row["Country"]] = row["count"] #row["Country] is the key while row["Count] is the value
        self.assertEqual(count_dict["United States"], 4, "Count for United States should be 4")
        self.assertEqual(count_dict["Canada"], 2, "Count for United States should be 2")
        self.assertEqual(count_dict["United Kingdom"], 1, "Count for United States should be 1")


    #unit test for testing data type
    def test_data_type(self):
        #this to_date_df function will return a dataframe
        #actual data of dataframe is sitting at the executor, dataframe is just a reference to the real data
        #cannot assert the dataframe, if want to validate data, must bring it to the driver
        #by using collect() which will return a list of rows to the driver
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_date_value(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))

    # @classmethod
    # def tearDownClass(cls) -> None:
    #     cls.spark.stop()
