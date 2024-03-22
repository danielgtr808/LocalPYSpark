from ..sql.row import Row
from typing import Any, List, Union

# {
#     "metadata": {},
#     "name": "name",
#     "nullable": False,
#     "type": "string"
# }


_type_mappings = {
    type(None): NullType,
    bool: BooleanType,
    int: LongType,
    float: DoubleType,
    str: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,  # can be TimestampNTZType
    datetime.time: TimestampType,  # can be TimestampNTZType
    datetime.timedelta: DayTimeIntervalType,
    bytes: BinaryType,
}

class Table():
    
    def infer_schema(self, rows: List[Row]) -> None:
        if (len(rows) == 0):
            raise Exception("Cannot infer schema from empty dataset")

        columns: Union[List[int], List[str]] = []
        if (not hasattr(rows[0], "_fields")):
            columns = range(len(rows[0]._values))
        else:
            columns = rows[0]._fields

        for column in columns:
            values: List[Any] = list(map(lambda x: x[column], rows))
            print(values)
