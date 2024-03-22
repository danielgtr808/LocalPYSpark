from local_pyspark.sql.row import Row, _rows_are_equal
import unittest



class TestRow(unittest.TestCase):

    def test_access_value_trough_numeric_index(self):
        row = Row("a", "b")
        self.assertEqual(row[0], "a")
        self.assertEqual(row[1], "b")

    def test_access_value_trough_slice_index(self):
        row = Row("a", "b")
        self.assertEqual(row[0:2], ("a", "b"))

    def test_access_value_trough_string_index(self):
        row = Row(a=1, b=2)
        self.assertEqual(row["a"], 1)
        self.assertEqual(row["b"], 2)

    def test_asDict_cannot_transform_from_args_row(self):
        self.assertRaises(
            Exception,
            lambda: Row("a", "b").asDict()
        )

    def test_asDict_one_recursion_dictionary(self):
        self.assertEqual(Row(a = 1, b = 2, c = Row(d = 4, e = 5)).asDict(True), { "a": 1, "b": 2, "c": { "d": 4, "e": 5 } })

    def test_asDict_simple_dictionary(self):
        self.assertEqual(Row(a = 1, b = 2).asDict(), { "a": 1, "b": 2 })

    def test_asDict_with_dict(self):
        self.assertEqual(Row(a = 1, b = 2, c = { "a": 1, "b": 2 }).asDict(True), { "a": 1, "b": 2, "c":  { "a": 1, "b": 2 } })

    def test_asDict_with_list_and_tuple(self):
        self.assertEqual(Row(a = 1, b = 2, c = [1, 2, 3], d = (1, 2, 3)).asDict(True), { "a": 1, "b": 2, "c": [1, 2, 3], "d": (1, 2, 3) })

    def test_constructor_error_on_args_and_kwargs(self):
        self.assertRaises(
            Exception,
            lambda: Row("a", "b", c = 3, d = 4)
        )



class TestRowsAreEqual(unittest.TestCase):

    def test_with_args_rows(self):
        self.assertTrue(
            _rows_are_equal([
                Row("a", "b"),
                Row("a", "b")
            ])
        )

    def test_with_dict_list_and_tuple(self):
        self.assertTrue(
            _rows_are_equal([
                Row(a = 1, b = 2, c = { "a": 1 }, d = [1, 2, 3], e = (1, 2, 3)),
                Row(a = 1, b = 2, c = { "a": 1 }, d = [1, 2, 3], e = (1, 2, 3))
            ])
        )

    def test_with_simple_recursion(self):
        self.assertTrue(
            _rows_are_equal([
                Row(a = 1, b = 2, c = Row(c = 3, d = 4)),
                Row(a = 1, b = 2, c = Row(c = 3, d = 4)),
            ])
        )

    def test_with_simple_row(self):
        self.assertTrue(
            _rows_are_equal([
                Row(a = 1, b = 2),
                Row(a = 1, b = 2)
            ])
        )



if __name__ == "__main__":
    unittest.main()