import local_pyspark._local_api_ as lapi # lapi means "Local API"
import unittest



class TestLocalColumn(unittest.TestCase):

    def test_and_logic(self):
        columns = [
            lapi.LocalColumn(None, { 'id': 1, 'name': "ID" }),
            lapi.LocalColumn(None, { 'id': 2, 'name': "Name" }),
            lapi.LocalColumn(None, { 'id': 3, 'name': "Active" })
        ]
        rows = [
            lapi.LocalRow({ columns[0]: 1, columns[1]: "Daniel", columns[2]: 1 }),
            lapi.LocalRow({ columns[0]: 2, columns[1]: "Karen", columns[2]: 1 })
        ]

        conditional = columns[2].equal_to(1).and_(columns[1].equal_to("Karen"))
        self.assertEqual(conditional(rows[0]), False)
        self.assertEqual(conditional(rows[1]), True)

    def test_and_str(self):
        columns = [
            lapi.LocalColumn(None, { 'id': 1, 'name': "ID" }),
            lapi.LocalColumn(None, { 'id': 2, 'name': "Name" })
        ]
        self.assertEqual(str(columns[0].and_(columns[1])), "(ID AND Name)")

    def test_as_str(self):
        column = lapi.LocalColumn(None, { 'id': 1, 'name': "ID" }).as_("another name")
        self.assertEqual(str(column), "ID as another name")
        self.assertEqual(str(column.as_("another alias")), "ID as another name as another alias")

    def test_as_field_recovery(self):
        column = lapi.LocalColumn(None, { "id": 1, "name": "ID" })
        row = lapi.LocalRow({ column: 1 })

        self.assertEqual(column.as_("another name")(row), 1)




if __name__ == "__main__":
    unittest.main()