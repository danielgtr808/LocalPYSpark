from local_pyspark._local_api_.table import Table
from local_pyspark.sql.row import Row
import unittest




class TestTable(unittest.TestCase):

    def test_infer_schema_doesnt_allow_empty_list(self):
        self.assertRaises(
            Exception,
            lambda: Table().infer_schema([])
        )

    def test_infer_schema_(self):
        Table().infer_schema([
            Row(1, 2),
            Row(10, 20)
        ])



if __name__ == "__main__":
    unittest.main()