from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_survey_df, count_by_country


class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

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


    # @classmethod
    # def tearDownClass(cls) -> None:
    #     cls.spark.stop()
