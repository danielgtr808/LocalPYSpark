from local_pyspark.sql.row import Row
from local_pyspark.sql.session import SparkSession
import unittest

from local_pyspark.sql.types import LongType, StringType



class TestRow(unittest.TestCase):

    def test_builder_check_if_spark_app_name_is_set_through_appName(self):
        self.assertEqual(
            SparkSession.Builder().appName("Test APP").getOrCreate().conf.get("spark.app.name"),
            "Test APP"
        )

    def test_builder_check_if_spark_catalogImplementation_is_set_through_enableHiveSupport(self):
        self.assertEqual(
            SparkSession.Builder().enableHiveSupport().getOrCreate().conf.get("spark.sql.catalogImplementation"),
            "hive"
        )

    def test_builder_check_if_spark_master_is_set_through_master(self):
        self.assertEqual(
            SparkSession.Builder().master("master 1").getOrCreate().conf.get("spark.master"),
            "master 1"
        )

    def test_inferSchemaFromList_raises_on_empty_data(self):
        self.assertRaises(
            Exception,
            lambda: SparkSession.Builder().getOrCreate()._inferSchemaFromList([])
        )

    def test_inferSchemaFromList_args_row(self):
        first_inference = SparkSession.Builder().getOrCreate()._inferSchemaFromList([
            Row(1, 2),
            Row(1, 2)
        ])

        self.assertTrue(isinstance(first_inference["_1"].dataType, LongType))
        self.assertTrue(isinstance(first_inference["_2"].dataType, LongType))

        second_inference = SparkSession.Builder().getOrCreate()._inferSchemaFromList([
            Row("1", 2),
            Row(1, "2")
        ])

        self.assertTrue(isinstance(second_inference["_1"].dataType, StringType))
        self.assertTrue(isinstance(second_inference["_2"].dataType, StringType))


if __name__ == "__main__":
    unittest.main()