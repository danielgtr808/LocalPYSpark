from .row import Row
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import datetime
import decimal



class DataType(): pass
class AtomicType(DataType): pass

class BinaryType(AtomicType): pass
class BooleanType(AtomicType): pass
class DateType(AtomicType): pass
class DayTimeIntervalType(AtomicType): pass
class DoubleType(AtomicType): pass
class LongType(AtomicType): pass
class NullType(DataType): pass
class StringType(AtomicType): pass
class TimestampType(AtomicType): pass

class DecimalType(AtomicType):

    def __init__(self, precision: int = 10, scale: int = 0):
        self.precision = precision
        self.scale = scale

class StructField(DataType):

    def __init__(self, name: str, dataType: DataType, nullable: bool = True, metadata: Optional[Dict[str, Any]] = None):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}

class StructType(DataType):

    def __init__(self, fields: Optional[List[StructField]] = None):
        if not fields:
            self.fields = []
            self.names = []
        else:
            self.fields = fields
            self.names = [f.name for f in fields]

    def __getitem__(self, key: Union[str, int]) -> StructField:
        if (isinstance(key, str)):
            return self.fields[self.names.index(key)]
        else:
            return self.fields[key]

    def __iter__(self) -> Iterator[StructField]:
        return iter(self.fields)

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

def _infer_type(obj: Any) -> DataType:
    if (obj is None):
        return NullType()
    
    data_type = _type_mappings.get(type(obj))

    if (data_type is DecimalType):
        return DecimalType(38, 18)
    
    return data_type()

def _infer_schema(row: Row):
    items: Iterable[Tuple[str, Any]]
    
    if (not hasattr(row, "_fields")): #args Row
        items = zip(
            map(lambda x: f"_{x}", range(1, len(row._values) + 1)),
            row._values
        )
    else:
        items = zip(row._fields, row._values)

    fields = []
    for k, v in items:
        fields.append(
            StructField(
                k,
                _infer_type(v),
                True
            )
        )
        
    return StructType(fields)
    
def _merge_type(
    a: Union[StructType, DataType],
    b: Union[StructType, DataType],
) -> Union[StructType, DataType]:

    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif isinstance(a, AtomicType) and isinstance(b, StringType):
        return b
    elif isinstance(a, StringType) and isinstance(b, AtomicType):
        return a
    elif type(a) is not type(b):
        raise Exception("CANNOT_MERGE_TYPE")

    return a



