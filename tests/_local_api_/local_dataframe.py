import local_pyspark._local_api_ as lapi # lapi means "Local API"
import unittest

columns = [
    lapi.LocalColumn(None, { "id": 1, "name": "grp" }),
    lapi.LocalColumn(None, { "id": 2, "name": "sub_grp" }),
    lapi.LocalColumn(None, { "id": 3, "name": "quantity" }),
]

rows = [
    lapi.LocalRow({ columns[0]: 1, columns[1]: 1, columns[2]: 1 }),
    lapi.LocalRow({ columns[0]: 1, columns[1]: 1, columns[2]: 2 }),
    lapi.LocalRow({ columns[0]: 1, columns[1]: 2, columns[2]: 3 }),
    lapi.LocalRow({ columns[0]: 1, columns[1]: 2, columns[2]: 4 }),
    lapi.LocalRow({ columns[0]: 2, columns[1]: 1, columns[2]: 5 }),
    lapi.LocalRow({ columns[0]: 2, columns[1]: 1, columns[2]: 2 }),
    lapi.LocalRow({ columns[0]: 2, columns[1]: 2, columns[2]: 10 }),
    lapi.LocalRow({ columns[0]: 2, columns[1]: 2, columns[2]: 11 })
]

class TestLocalDataframe(unittest.TestCase):

    def test_avg_logic_with_subgroup(self):
        avg_column = columns[2].avg()
        result_rows = lapi.LocalDataframe((lambda: rows)).groupBy(columns[0], columns[1]).agg(avg_column).data_callback()

        self.assertEqual(result_rows[0][avg_column], 1.5)
        self.assertEqual(result_rows[1][avg_column], 3.5)
        self.assertEqual(result_rows[2][avg_column], 3.5)
        self.assertEqual(result_rows[3][avg_column], 10.5)

    def test_avg_logic_with_partial_column_selection(self):
        avg_column = columns[2].avg()
        result_rows = lapi.LocalDataframe((lambda: rows)).groupBy(columns[0]).agg(avg_column).data_callback()

        self.assertEqual(result_rows[0][avg_column], 2.5)
        self.assertEqual(result_rows[1][avg_column], 7)

    def test_max_logic_with_subgroup(self):
        max_column = columns[2].max()
        result_rows = lapi.LocalDataframe((lambda: rows)).groupBy(columns[0], columns[1]).agg(max_column).data_callback()

        self.assertEqual(result_rows[0][max_column], 2)
        self.assertEqual(result_rows[1][max_column], 4)
        self.assertEqual(result_rows[2][max_column], 5)
        self.assertEqual(result_rows[3][max_column], 11)

    def test_max_logic_with_partial_column_selection(self):
        max_column = columns[2].max()
        result_rows = lapi.LocalDataframe((lambda: rows)).groupBy(columns[0]).agg(max_column).data_callback()

        self.assertEqual(result_rows[0][max_column], 4)
        self.assertEqual(result_rows[1][max_column], 11)

    def test_min_logic_with_subgroup(self):
        min_column = columns[2].min()
        result_rows = lapi.LocalDataframe((lambda: rows)).groupBy(columns[0], columns[1]).agg(min_column).data_callback()

        self.assertEqual(result_rows[0][min_column], 1)
        self.assertEqual(result_rows[1][min_column], 3)
        self.assertEqual(result_rows[2][min_column], 2)
        self.assertEqual(result_rows[3][min_column], 10)

    def test_min_logic_with_partial_column_selection(self):
        min_column = columns[2].min()
        result_rows = lapi.LocalDataframe((lambda: rows)).groupBy(columns[0]).agg(min_column).data_callback()

        self.assertEqual(result_rows[0][min_column], 1)
        self.assertEqual(result_rows[1][min_column], 2)

if __name__ == "__main__":
    unittest.main()