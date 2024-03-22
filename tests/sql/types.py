from decimal import Decimal
from local_pyspark.sql.row import Row
from local_pyspark.sql.types import DecimalType, DoubleType, LongType, StringType, _infer_schema, _merge_type
import unittest



class TestTypes(unittest.TestCase):

    def test_infer_schema_args_row(self):
        first_inference = _infer_schema(Row(1, 2))
        self.assertTrue(isinstance(first_inference[0].dataType, LongType))
        self.assertTrue(isinstance(first_inference[1].dataType, LongType))

        second_inference = _infer_schema(Row("1", Decimal(10)))
        self.assertTrue(isinstance(second_inference[0].dataType, StringType))
        self.assertTrue(isinstance(second_inference[1].dataType, DecimalType))

    def test_infer_schema_kwargs_row(self):
        first_inference = _infer_schema(Row(a = 1, b = 2))
        self.assertTrue(isinstance(first_inference["a"].dataType, LongType))
        self.assertTrue(isinstance(first_inference["b"].dataType, LongType))

        second_inference = _infer_schema(Row(a = "1", b = Decimal(10)))
        self.assertTrue(isinstance(second_inference["a"].dataType, StringType))
        self.assertTrue(isinstance(second_inference["b"].dataType, DecimalType))

    def test_merge_type_cannot_merge(self):
        self.assertRaises(
            Exception,
            lambda: _merge_type(LongType(), DecimalType())
        )

        self.assertRaises(
            Exception,
            lambda: _merge_type(DoubleType(), DecimalType())
        )


if __name__ == "__main__":
    unittest.main()